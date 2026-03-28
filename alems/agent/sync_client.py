"""
alems/agent/sync_client.py
────────────────────────────────────────────────────────────────────────────
Reads unsynced rows from local SQLite and pushes to server via POST /bulk-sync.

Design (clean, no UUIDs):
  - Local SQLite uses run_id, exp_id, hw_id (natural integers)
  - Server assigns its own BIGSERIAL global_run_id / global_exp_id
  - Collision safety: UNIQUE(hw_id, run_id) in PostgreSQL
  - Idempotency: ON CONFLICT (hw_id, run_id) DO NOTHING
  - sync_status tracks what has been pushed (0=unsynced, 1=synced, 2=failed)

FK sync order:
  hardware_config → environment_config → idle_baselines → task_categories
  → experiments → runs → child tables
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import sqlite3
import time
from typing import Optional

import httpx

from alems.agent.mode_manager import get_api_key, get_server_url, get_sync_config

TIMEOUT = 60


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {get_api_key()}",
        "Content-Type":  "application/json",
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def sync_unsynced_runs(db_path: str, immediately: bool = False) -> dict:
    """
    Build sync payload from local SQLite and POST to server.
    Uses hw_id + run_id as natural composite key — no UUIDs needed.
    """
    cfg        = get_sync_config()
    batch_size = int(cfg.get("batch_size", 100))
    retry_max  = int(cfg.get("retry_max", 3))
    backoff    = int(cfg.get("retry_backoff_s", 30))

    summary = {"runs_synced": 0, "rows_total": 0, "status": "ok", "error": None}

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    rows = con.execute("""
        SELECT run_id, exp_id, hw_id
        FROM runs
        WHERE sync_status IN (0, 2)
        ORDER BY run_id ASC
        LIMIT ?
    """, (batch_size,)).fetchall()

    if not rows:
        con.close()
        return summary

    run_ids = [r["run_id"] for r in rows]
    exp_ids = list(set(r["exp_id"] for r in rows))

    payload = _build_payload(con, run_ids, exp_ids, db_path)
    con.close()

    for attempt in range(1, retry_max + 1):
        result = _post_sync(payload)
        if result and result.get("ok"):
            synced_ids = result.get("synced_run_ids", [])
            _mark_synced(db_path, synced_ids)
            summary["runs_synced"] = len(synced_ids)
            summary["rows_total"]  = result.get("rows_inserted", 0)
            summary["status"]      = "ok"
            print(f"[sync] Synced {len(synced_ids)} runs, "
                  f"{summary['rows_total']} rows total")
            return summary
        else:
            print(f"[sync] Attempt {attempt}/{retry_max} failed")
            if attempt < retry_max:
                time.sleep(backoff)

    _mark_failed(db_path, run_ids)
    summary["status"] = "failed"
    summary["error"]  = f"All {retry_max} sync attempts failed"
    return summary


# ── Payload builder ───────────────────────────────────────────────────────────

def _build_payload(
    con: sqlite3.Connection,
    run_ids: list[int],
    exp_ids: list[int],
    db_path: str,
) -> dict:
    """
    Build sync payload. All rows identified by natural integer IDs.
    Server resolves collisions via UNIQUE(hw_id, run_id).
    """

    def fetch(table: str, id_col: str, ids: list) -> list[dict]:
        if not ids:
            return []
        ph   = ",".join("?" * len(ids))
        rows = con.execute(
            f"SELECT * FROM {table} WHERE {id_col} IN ({ph})", ids
        ).fetchall()
        return [dict(r) for r in rows]

    hw_row  = con.execute("SELECT * FROM hardware_config LIMIT 1").fetchone()
    hw_data = dict(hw_row) if hw_row else {}

    # Fetch core rows
    exp_rows = fetch("experiments", "exp_id", exp_ids)
    run_rows = fetch("runs",        "run_id", run_ids)

    # Collect FK parent IDs
    env_ids      = list({e["env_id"]      for e in exp_rows if e.get("env_id")})
    baseline_ids = list({r["baseline_id"] for r in run_rows if r.get("baseline_id")})

    # Small reference tables — always full sync (idempotent)
    task_cats = [dict(r) for r in con.execute(
        "SELECT * FROM task_categories"
    ).fetchall()]

    return {
        "hardware_hash":    hw_data.get("hardware_hash", ""),
        "api_key":          get_api_key(),
        "hardware_data":    hw_data,
        # Parent tables (no FK deps) — must arrive first
        "environment_config":        fetch("environment_config", "env_id", env_ids),
        "idle_baselines":            fetch("idle_baselines", "baseline_id", baseline_ids),
        "task_categories":           task_cats,
        # Core tables
        "experiments":               exp_rows,
        "runs":                      run_rows,
        # Child tables (FK → runs, identified by run_id)
        "energy_samples":            fetch("energy_samples",    "run_id", run_ids),
        "cpu_samples":               fetch("cpu_samples",       "run_id", run_ids),
        "thermal_samples":           fetch("thermal_samples",   "run_id", run_ids),
        "interrupt_samples":         fetch("interrupt_samples", "run_id", run_ids),
        "orchestration_events":      fetch("orchestration_events", "run_id", run_ids),
        "llm_interactions":          fetch("llm_interactions",  "run_id", run_ids),
        "orchestration_tax_summary": fetch(
            "orchestration_tax_summary", "linear_run_id", run_ids),
        "outliers":                  fetch("outliers", "run_id", run_ids),
    }


def _post_sync(payload: dict) -> Optional[dict]:
    server_url = get_server_url()
    try:
        r = httpx.post(
            f"{server_url}/bulk-sync",
            json=payload,
            headers=_headers(),
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        return r.json()
    except httpx.TimeoutException:
        print("[sync] Timeout during bulk-sync")
    except httpx.HTTPStatusError as e:
        print(f"[sync] HTTP {e.response.status_code} during bulk-sync")
    except Exception as e:
        print(f"[sync] Error: {e}")
    return None


def _mark_synced(db_path: str, run_ids: list[int]) -> None:
    """Mark runs as synced using local integer run_id."""
    if not run_ids:
        return
    ph  = ",".join("?" * len(run_ids))
    con = sqlite3.connect(db_path)
    con.execute(
        f"UPDATE runs SET sync_status=1 WHERE run_id IN ({ph})",
        run_ids,
    )
    con.commit()
    con.close()


def _mark_failed(db_path: str, run_ids: list[int]) -> None:
    if not run_ids:
        return
    ph  = ",".join("?" * len(run_ids))
    con = sqlite3.connect(db_path)
    con.execute(
        f"UPDATE runs SET sync_status=2 WHERE run_id IN ({ph})",
        run_ids,
    )
    con.commit()
    con.close()


def count_unsynced(db_path: str) -> int:
    try:
        con = sqlite3.connect(db_path)
        row = con.execute(
            "SELECT COUNT(*) FROM runs WHERE sync_status IN (0, 2)"
        ).fetchone()
        con.close()
        return int(row[0]) if row else 0
    except Exception:
        return 0
