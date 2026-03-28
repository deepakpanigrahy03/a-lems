"""
alems/agent/backfill.py
────────────────────────────────────────────────────────────────────────────
Assigns global_run_id / global_exp_id to all existing SQLite rows that
predate migration 007.

Rules:
  - Idempotent: rows that already have a global_*_id are skipped
  - Deterministic: same hw_id + local int id → always same UUID
  - Propagates global_run_id to all 7 child tables automatically
  - Prints a summary at the end

Run automatically by run_migrations.py after applying migration 007.
Can also be run standalone:
    python -m alems.agent.backfill
    python -m alems.agent.backfill --db /path/to/experiments.db
────────────────────────────────────────────────────────────────────────────
"""

import argparse
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent


def _default_db() -> Path:
    return PROJECT_ROOT / "data" / "experiments.db"


def _get_local_hw_id(con: sqlite3.Connection) -> int:
    """Return the single hw_id from local hardware_config."""
    row = con.execute("SELECT hw_id FROM hardware_config LIMIT 1").fetchone()
    if not row:
        print("[backfill] ERROR: no row in hardware_config — run A-LEMS at least once first")
        sys.exit(1)
    return int(row[0])


def backfill_global_ids(db_path: str) -> dict:
    """
    Main backfill function. Returns counts of rows updated.
    Safe to call multiple times — only fills NULL values.
    """
    from alems.shared.uuid_gen import exp_uuid, run_uuid

    path = Path(db_path)
    if not path.exists():
        print(f"[backfill] ERROR: {path} not found")
        sys.exit(1)

    con = sqlite3.connect(path)
    hw_id = _get_local_hw_id(con)
    print(f"[backfill] hw_id={hw_id}, db={path}")

    counts = {}

    # ── experiments ──────────────────────────────────────────────────────────
    rows = con.execute(
        "SELECT exp_id FROM experiments WHERE global_exp_id IS NULL"
    ).fetchall()
    for (exp_id,) in rows:
        uid = exp_uuid(hw_id, exp_id)
        con.execute(
            "UPDATE experiments SET global_exp_id=? WHERE exp_id=?",
            (uid, exp_id)
        )
    counts["experiments"] = len(rows)
    if rows:
        print(f"  experiments: {len(rows)} rows backfilled")

    # ── runs ─────────────────────────────────────────────────────────────────
    rows = con.execute(
        "SELECT run_id FROM runs WHERE global_run_id IS NULL"
    ).fetchall()
    for (run_id,) in rows:
        uid = run_uuid(hw_id, run_id)
        con.execute(
            "UPDATE runs SET global_run_id=? WHERE run_id=?",
            (uid, run_id)
        )
    counts["runs"] = len(rows)
    if rows:
        print(f"  runs: {len(rows)} rows backfilled")

    # ── child tables — propagate from runs ───────────────────────────────────
    child_tables = [
        "energy_samples",
        "cpu_samples",
        "thermal_samples",
        "interrupt_samples",
        "orchestration_events",
        "llm_interactions",
    ]
    for tbl in child_tables:
        result = con.execute(f"""
            UPDATE {tbl}
            SET global_run_id = (
                SELECT global_run_id FROM runs
                WHERE runs.run_id = {tbl}.run_id
            )
            WHERE global_run_id IS NULL
              AND run_id IS NOT NULL
        """)
        n = result.rowcount
        counts[tbl] = n
        if n:
            print(f"  {tbl}: {n} rows backfilled")

    # ── orchestration_tax_summary — uses linear_run_id ───────────────────────
    result = con.execute("""
        UPDATE orchestration_tax_summary
        SET global_run_id = (
            SELECT global_run_id FROM runs
            WHERE runs.run_id = orchestration_tax_summary.linear_run_id
        )
        WHERE global_run_id IS NULL
          AND linear_run_id IS NOT NULL
    """)
    counts["orchestration_tax_summary"] = result.rowcount
    if result.rowcount:
        print(f"  orchestration_tax_summary: {result.rowcount} rows backfilled")

    con.commit()
    con.close()

    total = sum(counts.values())
    print(f"[backfill] Complete — {total} total rows updated")
    return counts


def verify_backfill(db_path: str) -> bool:
    """
    Verify no NULL global_run_ids remain. Returns True if clean.
    """
    con = sqlite3.connect(db_path)
    ok = True

    tables = {
        "runs":        "global_run_id",
        "experiments": "global_exp_id",
    }
    for tbl, col in tables.items():
        row = con.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NULL"
        ).fetchone()
        n = row[0] if row else 0
        if n > 0:
            print(f"[verify] WARNING: {tbl} has {n} rows with NULL {col}")
            ok = False

    con.close()
    if ok:
        print("[verify] All global IDs present — backfill complete")
    return ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args()
    db = args.db or str(_default_db())
    backfill_global_ids(db)
    verify_backfill(db)
