"""
alems/agent/sync_client.py
────────────────────────────────────────────────────────────────────────────
Reads unsynced rows from local SQLite and pushes them to the server
via POST /bulk-sync.

Design rules:
  - Runs AFTER a job completes (immediate trigger) and on a 60s timer
  - Batched: at most sync_config["batch_size"] runs per call
  - Retries: up to retry_max attempts with backoff
  - Idempotent: server uses ON CONFLICT DO NOTHING, so re-sending is safe
  - Server down: marks runs as sync_status=2 (failed), retries next cycle
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Optional

import httpx

from alems.agent.mode_manager import (
    get_api_key, get_server_url, get_sync_config,
)
from alems.shared.models import BulkSyncPayload, BulkSyncResponse

AGENT_VERSION = "1.0.0"
TIMEOUT = 60  # longer timeout for bulk sync


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {get_api_key()}",
        "Content-Type":  "application/json",
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def sync_unsynced_runs(db_path: str, immediately: bool = False) -> dict:
    """
    Build sync payload from local SQLite and POST to server.
    Returns summary dict with counts.

    immediately=True: skip the "nothing to sync" early exit (post-run trigger).
    """
    cfg = get_sync_config()
    batch_size = int(cfg.get("batch_size", 100))
    retry_max  = int(cfg.get("retry_max", 3))
    backoff    = int(cfg.get("retry_backoff_s", 30))

    summary = {"runs_synced": 0, "rows_total": 0, "status": "ok", "error": None}

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # Get unsynced runs (status=0) or failed runs to retry (status=2)
    rows = con.execute("""
        SELECT run_id, global_run_id, exp_id, global_exp_id, hw_id, sync_status
        FROM runs
        WHERE sync_status IN (0, 2)
          AND global_run_id IS NOT NULL
        ORDER BY run_id ASC
        LIMIT ?
    """, (batch_size,)).fetchall()

    if not rows and not immediately:
        con.close()
        return summary

    if not rows:
        con.close()
        return summary

    run_ids        = [r["run_id"]       for r in rows]
    global_run_ids = [r["global_run_id"] for r in rows]
    exp_ids        = list(set(r["exp_id"] for r in rows))

    # Build payload
    payload = _build_payload(con, run_ids, exp_ids, global_run_ids, db_path)
    con.close()

    # POST with retries
    for attempt in range(1, retry_max + 1):
        result = _post_sync(payload)
        if result:
            # Mark synced rows in SQLite
            _mark_synced(db_path, result.synced_run_ids)
            summary["runs_synced"] = len(result.synced_run_ids)
            summary["rows_total"]  = result.rows_inserted
            summary["status"]      = "ok"
            print(f"[sync] Synced {len(result.synced_run_ids)} runs, "
                  f"{result.rows_inserted} rows total")
            return summary
        else:
            print(f"[sync] Attempt {attempt}/{retry_max} failed")
            if attempt < retry_max:
                time.sleep(backoff)

    # All retries exhausted — mark as failed
    _mark_failed(db_path, global_run_ids)
    summary["status"] = "failed"
    summary["error"]  = f"All {retry_max} sync attempts failed"
    print(f"[sync] Sync failed for {len(global_run_ids)} runs — will retry next cycle")
    return summary


# ── Payload builder ───────────────────────────────────────────────────────────

def _build_payload(
    con: sqlite3.Connection,
    run_ids: list[int],
    exp_ids: list[int],
    global_run_ids: list[str],
    db_path: str,
) -> dict:
    """Build the full BulkSyncPayload dict from local SQLite."""

    def fetch_table(table: str, id_col: str, ids: list) -> list[dict]:
        if not ids:
            return []
        ph = ",".join("?" * len(ids))
        rows = con.execute(
            f"SELECT * FROM {table} WHERE {id_col} IN ({ph})", ids
        ).fetchall()
        return [dict(r) for r in rows]

    # Hardware
    hw_row = con.execute("SELECT * FROM hardware_config LIMIT 1").fetchone()
    hw_data = dict(hw_row) if hw_row else {}

    return {
        "hardware_hash":             hw_data.get("hardware_hash", ""),
        "api_key":                   get_api_key(),
        "hardware_data":             hw_data,
        "experiments":               fetch_table("experiments", "exp_id", exp_ids),
        "runs":                      fetch_table("runs", "run_id", run_ids),
        "energy_samples":            fetch_table("energy_samples", "run_id", run_ids),
        "cpu_samples":               fetch_table("cpu_samples", "run_id", run_ids),
        "thermal_samples":           fetch_table("thermal_samples", "run_id", run_ids),
        "interrupt_samples":         fetch_table("interrupt_samples", "run_id", run_ids),
        "orchestration_events":      fetch_table("orchestration_events", "run_id", run_ids),
        "llm_interactions":          fetch_table("llm_interactions", "run_id", run_ids),
        "orchestration_tax_summary": fetch_table(
            "orchestration_tax_summary", "linear_run_id", run_ids
        ),
    }


def _post_sync(payload: dict) -> Optional[BulkSyncResponse]:
    server_url = get_server_url()
    try:
        r = httpx.post(
            f"{server_url}/bulk-sync",
            json=payload,
            headers=_headers(),
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        return BulkSyncResponse(**r.json())
    except httpx.TimeoutException:
        print("[sync] Timeout during bulk-sync")
    except httpx.HTTPStatusError as e:
        print(f"[sync] HTTP {e.response.status_code} during bulk-sync")
    except Exception as e:
        print(f"[sync] Error during bulk-sync: {e}")
    return None


def _mark_synced(db_path: str, global_run_ids: list[str]) -> None:
    if not global_run_ids:
        return
    ph = ",".join("?" * len(global_run_ids))
    con = sqlite3.connect(db_path)
    con.execute(
        f"UPDATE runs SET sync_status = 1 WHERE global_run_id IN ({ph})",
        global_run_ids,
    )
    con.commit()
    con.close()


def _mark_failed(db_path: str, global_run_ids: list[str]) -> None:
    if not global_run_ids:
        return
    ph = ",".join("?" * len(global_run_ids))
    con = sqlite3.connect(db_path)
    con.execute(
        f"UPDATE runs SET sync_status = 2 WHERE global_run_id IN ({ph})",
        global_run_ids,
    )
    con.commit()
    con.close()


def count_unsynced(db_path: str) -> int:
    """Quick count of unsynced runs. Used by heartbeat payload."""
    try:
        con = sqlite3.connect(db_path)
        row = con.execute(
            "SELECT COUNT(*) FROM runs WHERE sync_status IN (0, 2)"
        ).fetchone()
        con.close()
        return int(row[0]) if row else 0
    except Exception:
        return 0
