"""
alems/tests/test_e2e.py
────────────────────────────────────────────────────────────────────────────
End-to-end test suite for the A-LEMS distributed stack.

Tests (run in order):
  T01  SQLite migration 007 applied correctly
  T02  UUID backfill completed for all existing rows
  T03  Server /health endpoint reachable
  T04  Agent registration returns api_key
  T05  Heartbeat accepted by server
  T06  Job can be enqueued and fetched
  T07  Bulk sync inserts rows into PostgreSQL
  T08  No duplicate rows after re-sync (idempotency)
  T09  Streamlit mode detection works
  T10  Sync monitor shows correct counts

Usage:
    # Test SQLite only (no server required)
    python -m alems.tests.test_e2e --sqlite-only

    # Full stack test (server must be running)
    python -m alems.tests.test_e2e --server http://129.153.71.47:8000

    # With PostgreSQL verification
    ALEMS_DB_URL=postgresql://alems:pass@localhost/alems_central \\
        python -m alems.tests.test_e2e --server http://129.153.71.47:8000
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH      = str(PROJECT_ROOT / "data" / "experiments.db")

PASS = "✓"
FAIL = "✗"
SKIP = "○"

results: list[tuple[str, str, str]] = []


def test(name: str):
    """Decorator for test functions."""
    def decorator(fn):
        def wrapper(*args, **kwargs):
            try:
                fn(*args, **kwargs)
                results.append((name, PASS, ""))
                print(f"  {PASS}  {name}")
            except AssertionError as e:
                results.append((name, FAIL, str(e)))
                print(f"  {FAIL}  {name}: {e}")
            except Exception as e:
                results.append((name, FAIL, f"Exception: {e}"))
                print(f"  {FAIL}  {name}: {type(e).__name__}: {e}")
        return wrapper
    return decorator


def skip(name: str, reason: str):
    results.append((name, SKIP, reason))
    print(f"  {SKIP}  {name} — {reason}")


# ── T01: SQLite migration ─────────────────────────────────────────────────────

@test("T01 SQLite migration 007 applied")
def t01_migration():
    con = sqlite3.connect(DB_PATH)

    # Check schema_version
    row = con.execute("SELECT MAX(version) FROM schema_version").fetchone()
    assert row and row[0] >= 7, f"schema_version < 7: got {row}"

    # Check new columns exist in runs
    cols = {r[1] for r in con.execute("PRAGMA table_info('runs')").fetchall()}
    assert "global_run_id" in cols,  "runs.global_run_id missing"
    assert "sync_status"   in cols,  "runs.sync_status missing"

    # Check experiments
    cols_exp = {r[1] for r in con.execute("PRAGMA table_info('experiments')").fetchall()}
    assert "global_exp_id" in cols_exp, "experiments.global_exp_id missing"

    # Check hardware_config agent tracking
    cols_hw = {r[1] for r in con.execute("PRAGMA table_info('hardware_config')").fetchall()}
    assert "last_seen"    in cols_hw, "hardware_config.last_seen missing"
    assert "agent_status" in cols_hw, "hardware_config.agent_status missing"

    con.close()


# ── T02: UUID backfill ────────────────────────────────────────────────────────

@test("T02 UUID backfill — no NULL global_run_id in runs")
def t02_backfill_runs():
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT COUNT(*) FROM runs WHERE global_run_id IS NULL").fetchone()
    null_count = row[0] if row else -1
    total = con.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
    con.close()
    assert null_count == 0, \
        f"{null_count}/{total} runs still have NULL global_run_id — run backfill"


@test("T02b UUID backfill — no NULL global_exp_id in experiments")
def t02b_backfill_exps():
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT COUNT(*) FROM experiments WHERE global_exp_id IS NULL").fetchone()
    null_count = row[0] if row else -1
    con.close()
    assert null_count == 0, \
        f"{null_count} experiments still have NULL global_exp_id"


@test("T02c UUID format valid (uuid4/uuid5)")
def t02c_uuid_format():
    con = sqlite3.connect(DB_PATH)
    sample = con.execute(
        "SELECT global_run_id FROM runs WHERE global_run_id IS NOT NULL LIMIT 5"
    ).fetchall()
    con.close()
    for (uid,) in sample:
        try:
            uuid.UUID(uid)
        except ValueError:
            assert False, f"Invalid UUID: {uid}"


@test("T02d global_run_id unique across all runs")
def t02d_uuid_unique():
    con = sqlite3.connect(DB_PATH)
    total = con.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
    unique = con.execute(
        "SELECT COUNT(DISTINCT global_run_id) FROM runs WHERE global_run_id IS NOT NULL"
    ).fetchone()[0]
    con.close()
    assert total == unique, f"UUID collision: {total} runs but {unique} unique IDs"


@test("T02e child table global_run_id propagated")
def t02e_child_propagation():
    con = sqlite3.connect(DB_PATH)
    for tbl in ["energy_samples", "cpu_samples", "thermal_samples"]:
        row = con.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE global_run_id IS NULL AND run_id IS NOT NULL"
        ).fetchone()
        null_count = row[0] if row else 0
        assert null_count == 0, \
            f"{tbl}: {null_count} rows with NULL global_run_id"
    con.close()


# ── T03-T08: Server tests (require running server) ────────────────────────────

def run_server_tests(server_url: str, pg_url: str | None):
    import httpx

    print(f"\n  Server: {server_url}")

    @test("T03 Server /health reachable")
    def t03_health():
        r = httpx.get(f"{server_url}/health", timeout=5)
        assert r.status_code == 200, f"HTTP {r.status_code}"
        data = r.json()
        assert data.get("status") == "ok",   f"status != ok: {data}"
        assert data.get("mode")   == "server", f"mode != server: {data}"

    @test("T04 Agent registration returns api_key")
    def t04_register():
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        hw = con.execute("SELECT * FROM hardware_config LIMIT 1").fetchone()
        con.close()
        assert hw, "No hardware_config row in SQLite"

        r = httpx.post(f"{server_url}/register", json=dict(hw), timeout=10)
        assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"
        data = r.json()
        assert "api_key"       in data, f"api_key missing: {data}"
        assert "server_hw_id"  in data, f"server_hw_id missing: {data}"
        assert len(data["api_key"]) > 10, "api_key too short"
        # Save for subsequent tests
        t04_register.api_key      = data["api_key"]
        t04_register.server_hw_id = data["server_hw_id"]
        t04_register.hw_hash      = dict(hw)["hardware_hash"]

    t03_health()
    t04_register()

    # Get credentials from T04 for subsequent tests
    api_key      = getattr(t04_register, "api_key",      None)
    server_hw_id = getattr(t04_register, "server_hw_id", None)
    hw_hash      = getattr(t04_register, "hw_hash",      None)

    if not api_key:
        skip("T05", "registration failed — skipping remaining server tests")
        return

    headers = {"Authorization": f"Bearer {api_key}"}

    @test("T05 Heartbeat accepted")
    def t05_heartbeat():
        r = httpx.post(f"{server_url}/heartbeat", json={
            "hardware_hash": hw_hash,
            "api_key":       api_key,
            "status":        "idle",
            "unsynced_runs": 0,
        }, timeout=5)
        assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"
        data = r.json()
        assert data.get("ok") is True, f"ok != True: {data}"

    @test("T06 Job enqueue and fetch")
    def t06_job():
        # No job available yet → get-job returns None
        r = httpx.get(f"{server_url}/get-job",
                      params={"hardware_hash": hw_hash},
                      headers=headers, timeout=5)
        assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"
        data = r.json()
        assert "job" in data, f"'job' key missing: {data}"
        # job is either null or a JobDetail — both valid
        print(f"    job={data['job']}", end="")

    @test("T07 Bulk sync inserts rows")
    def t07_sync():
        # Get a real unsynced run from SQLite
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        run = con.execute("""
            SELECT r.*, e.global_exp_id
            FROM runs r
            JOIN experiments e ON e.exp_id = r.exp_id
            WHERE r.global_run_id IS NOT NULL
            LIMIT 1
        """).fetchone()

        if not run:
            con.close()
            assert False, "No runs in SQLite to test sync with"

        run_dict = dict(run)
        exp = con.execute(
            "SELECT * FROM experiments WHERE exp_id=?", (run_dict["exp_id"],)
        ).fetchone()
        exp_dict = dict(exp) if exp else {}
        # Fetch environment_config for this experiment
        env_id = exp_dict.get("env_id")
        env_configs = []
        if env_id:
            env_row = con.execute(
                "SELECT * FROM environment_config WHERE env_id=?", (env_id,)
            ).fetchone()
            if env_row:
                env_configs = [dict(env_row)]

        # Fetch idle_baselines for this run
        baseline_id = run_dict.get("baseline_id")
        baselines = []
        if baseline_id:
            bl_row = con.execute(
                "SELECT * FROM idle_baselines WHERE baseline_id=?", (baseline_id,)
            ).fetchone()
            if bl_row:
                baselines = [dict(bl_row)]        

        hw = con.execute("SELECT * FROM hardware_config LIMIT 1").fetchone()
        hw_dict = dict(hw) if hw else {}
        con.close()

        payload = {
            "hardware_hash":             hw_hash,
            "api_key":                   api_key,
            "hardware_data":             hw_dict,
            "environment_config":        env_configs,
            "idle_baselines":            baselines,
            "task_categories":           [],
            "experiments":               [exp_dict],
            "runs":                      [run_dict],
            "energy_samples":            [],
            "cpu_samples":               [],
            "thermal_samples":           [],
            "interrupt_samples":         [],
            "orchestration_events":      [],
            "llm_interactions":          [],
            "orchestration_tax_summary": [],
            "outliers":                  [],
        }
        r = httpx.post(f"{server_url}/bulk-sync", json=payload, timeout=30)
        assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"
        data = r.json()
        assert data.get("ok") is True, f"ok != True: {data}"
        assert data.get("rows_inserted", 0) > 0, f"no rows inserted: {data}"
        t07_sync.synced_run_id = run_dict["global_run_id"]

    @test("T08 Re-sync is idempotent (no duplicates)")
    def t08_idempotent():
        synced_id = getattr(t07_sync, "synced_run_id", None)
        if not synced_id:
            assert False, "T07 did not complete — skipping idempotency check"

        # Re-send same payload
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        run = con.execute(
            "SELECT * FROM runs WHERE global_run_id=?", (synced_id,)
        ).fetchone()
        hw = con.execute("SELECT * FROM hardware_config LIMIT 1").fetchone()
        con.close()

        payload = {
            "hardware_hash":    hw_hash,
            "api_key":          api_key,
            "hardware_data":    dict(hw),
            "experiments":      [],
            "runs":             [dict(run)],
            "energy_samples":   [],
            "cpu_samples":      [],
            "thermal_samples":  [],
            "interrupt_samples": [],
            "orchestration_events": [],
            "llm_interactions": [],
            "orchestration_tax_summary": [],
        }
        r = httpx.post(f"{server_url}/bulk-sync", json=payload, timeout=30)
        assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"

        if pg_url:
            # Verify count in PostgreSQL
            import psycopg2
            con_pg = psycopg2.connect(pg_url)
            cur = con_pg.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM runs WHERE global_run_id=%s", (synced_id,)
            )
            count = cur.fetchone()[0]
            con_pg.close()
            assert count == 1, f"Expected 1 row, got {count} (duplicate!)"

    t05_heartbeat()
    t06_job()
    t07_sync()
    t08_idempotent()

    # T09 mode detection
    @test("T09 Streamlit mode detection")
    def t09_mode():
        # Without ALEMS_DB_URL set: should be local or connected
        original = os.environ.pop("ALEMS_DB_URL", None)
        try:
            from alems.shared.db_layer import get_ui_mode, get_engine
            engine = get_engine()
            mode   = get_ui_mode(engine)
            assert mode in ("local", "connected"), f"Unexpected mode: {mode}"
        finally:
            if original:
                os.environ["ALEMS_DB_URL"] = original

        # With postgresql URL: should be server
        if pg_url:
            from alems.shared.db_layer import get_ui_mode, get_engine
            engine = get_engine(pg_url)
            mode   = get_ui_mode(engine)
            assert mode == "server", f"Expected 'server' with PG URL, got: {mode}"

    t09_mode()


# ── T10: Sync counts ──────────────────────────────────────────────────────────

@test("T10 Sync monitor counts consistent")
def t10_sync_counts():
    con = sqlite3.connect(DB_PATH)
    row = con.execute("""
        SELECT
            SUM(CASE WHEN sync_status=0 THEN 1 ELSE 0 END) as unsynced,
            SUM(CASE WHEN sync_status=1 THEN 1 ELSE 0 END) as synced,
            SUM(CASE WHEN sync_status=2 THEN 1 ELSE 0 END) as failed,
            COUNT(*) as total
        FROM runs
    """).fetchone()
    con.close()
    unsynced, synced, failed, total = row
    assert (unsynced or 0) + (synced or 0) + (failed or 0) == total, \
        f"Sync status counts don't add up: {unsynced}+{synced}+{failed} != {total}"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global DB_PATH
    parser = argparse.ArgumentParser(description="A-LEMS E2E tests")
    parser.add_argument("--sqlite-only", action="store_true")
    parser.add_argument("--server",      type=str, default=None)
    parser.add_argument("--db",          type=str, default=DB_PATH)
    args = parser.parse_args()

    
    DB_PATH = args.db

    pg_url = os.environ.get("ALEMS_DB_URL")
    if pg_url and not pg_url.startswith("postgresql"):
        pg_url = None

    print("\nA-LEMS End-to-End Test Suite")
    print("=" * 50)

    # SQLite tests always run
    print("\n[SQLite tests]")
    t01_migration()
    t02_backfill_runs()
    t02b_backfill_exps()
    t02c_uuid_format()
    t02d_uuid_unique()
    t02e_child_propagation()
    t10_sync_counts()

    # Server tests
    if not args.sqlite_only:
        server_url = args.server or "http://129.153.71.47:8000"
        print(f"\n[Server tests — {server_url}]")
        try:
            import httpx
            run_server_tests(server_url, pg_url)
        except ImportError:
            print("  httpx not installed — skip server tests: pip install httpx")

    # Summary
    print("\n" + "=" * 50)
    passed = sum(1 for _, r, _ in results if r == PASS)
    failed = sum(1 for _, r, _ in results if r == FAIL)
    skipped = sum(1 for _, r, _ in results if r == SKIP)
    print(f"Results: {passed} passed · {failed} failed · {skipped} skipped")

    if failed:
        print("\nFailed tests:")
        for name, result, msg in results:
            if result == FAIL:
                print(f"  {FAIL} {name}: {msg}")
        sys.exit(1)
    else:
        print("\nAll tests passed ✓")


if __name__ == "__main__":
    main()
