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

#TIMEOUT = 60 -- coming from sync_config now, default 300s (5min) to allow for large batches and slow connections


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {get_api_key()}",
        "Content-Type":  "application/json",
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def sync_unsynced_runs(db_path: str, immediately: bool = False) -> dict:
    cfg        = get_sync_config()
    batch_size = int(cfg.get("batch_size", 50))  # larger now — metadata only
    retry_max  = int(cfg.get("retry_max", 3))
    backoff    = int(cfg.get("retry_backoff_s", 30))
    summary    = {"runs_synced": 0, "rows_total": 0, "status": "ok", "error": None}

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # Phase 1 — metadata only (runs + experiments, NO child tables)
    rows = con.execute("""
        SELECT run_id, exp_id, hw_id FROM runs
        WHERE sync_status IN (0, 2)
        ORDER BY run_id ASC LIMIT ?
    """, (batch_size,)).fetchall()

    if not rows:
        con.close()
        # Phase 2 — sync samples for runs already synced (sync_status=1)
        # but samples not yet synced (sync_samples_status=0)
        _sync_pending_samples(db_path, retry_max, backoff)
        return summary

    run_ids = [r["run_id"] for r in rows]
    exp_ids = list(set(r["exp_id"] for r in rows))

    # Build metadata-only payload
    payload = _build_payload(con, run_ids, exp_ids, db_path, include_samples=False)
    con.close()

    for attempt in range(1, retry_max + 1):
        result = _post_sync(payload)
        if result and result.get("ok"):
            synced_ids = result.get("synced_run_ids", [])
            _mark_synced(db_path, synced_ids)
            summary["runs_synced"] = len(synced_ids)
            summary["rows_total"]  = result.get("rows_inserted", 0)
            print(f"[sync] Phase 1: {len(synced_ids)} runs metadata synced")
            _write_sync_log(len(synced_ids), summary["rows_total"], "ok")
            return summary
        print(f"[sync] Attempt {attempt}/{retry_max} failed")
        if attempt < retry_max:
            time.sleep(backoff)

    _mark_failed(db_path, run_ids)
    summary["status"] = "failed"
    return summary


def sync_run_samples_now(db_path: str, run_id_from: int, retry_max: int = 3) -> None:
    """Immediately sync samples for all runs created >= run_id_from — bypasses backlog."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT run_id, exp_id FROM runs WHERE run_id >= ? AND sync_status=1 AND sync_samples_status=0",
        (run_id_from,)
    ).fetchall()
    if not rows:
        con.close()
        return
    run_ids = [r["run_id"] for r in rows]
    exp_ids = list(set(r["exp_id"] for r in rows))
    payload = _build_payload(con, run_ids, exp_ids, db_path, include_samples=True)
    con.close()
    for attempt in range(1, retry_max + 1):
        result = _post_sync(payload)
        if result and result.get("ok"):
            _mark_samples_synced(db_path, run_ids)
            print(f"[sync] Live sync: {len(run_ids)} run(s) samples synced (run_ids {run_ids[0]}–{run_ids[-1]})")
            return
        time.sleep(2)
    print(f"[sync] Live sync: failed after {retry_max} attempts")


def _sync_pending_samples(db_path: str, retry_max: int, backoff: int) -> None:
    """Phase 2 — sync child table samples for already-synced runs."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    rows = con.execute("""
        SELECT run_id, exp_id FROM runs
        WHERE sync_status = 1
          AND sync_samples_status = 0
        ORDER BY run_id DESC LIMIT 2
    """, ()).fetchall()

    if not rows:
        con.close()
        return

    run_ids = [r["run_id"] for r in rows]
    exp_ids = list(set(r["exp_id"] for r in rows))
    payload = _build_payload(con, run_ids, exp_ids, db_path, include_samples=True)
    con.close()

    for attempt in range(1, retry_max + 1):
        result = _post_sync(payload)
        if result and result.get("ok"):
            _mark_samples_synced(db_path, run_ids)
            print(f"[sync] Phase 2: {len(run_ids)} runs samples synced")
            return
        if attempt < retry_max:
            time.sleep(backoff)


def _mark_samples_synced(db_path: str, run_ids: list[int]) -> None:
    if not run_ids:
        return
    ph  = ",".join("?" * len(run_ids))
    con = sqlite3.connect(db_path)
    con.execute(
        f"UPDATE runs SET sync_samples_status=1 WHERE run_id IN ({ph})",
        run_ids,
    )
    con.commit()
    con.close()


# ── Payload builder ───────────────────────────────────────────────────────────

def _build_payload(con, run_ids, exp_ids, db_path, include_samples=True):
    def fetch(table, id_col, ids):
        if not ids: return []
        ph = ",".join("?" * len(ids))
        return [dict(r) for r in con.execute(
            f"SELECT * FROM {table} WHERE {id_col} IN ({ph})", ids
        ).fetchall()]

    hw_row  = con.execute("SELECT * FROM hardware_config LIMIT 1").fetchone()
    hw_data = dict(hw_row) if hw_row else {}
    exp_rows = fetch("experiments", "exp_id", exp_ids)
    run_rows = fetch("runs", "run_id", run_ids)
    env_ids   = list({e["env_id"] for e in exp_rows if e.get("env_id")})
    # Send ALL baselines and task_categories every batch — small reference tables,
    # idempotent upsert on server. Avoids FK violation when a run references a
    # baseline that was collected in a different sync batch.
    all_baselines = [dict(r) for r in con.execute("SELECT * FROM idle_baselines").fetchall()]
    task_cats     = [dict(r) for r in con.execute("SELECT * FROM task_categories").fetchall()]

    payload = {
        "hardware_hash":    hw_data.get("hardware_hash", ""),
        "api_key":          get_api_key(),
        "hardware_data":    hw_data,
        "environment_config":  fetch("environment_config", "env_id", env_ids),
        "idle_baselines":      all_baselines,
        "task_categories":     task_cats,
        "experiments":         exp_rows,
        "runs":                run_rows,
        # Child tables — only in Phase 2
        "energy_samples":            fetch("energy_samples",    "run_id", run_ids) if include_samples else [],
        "cpu_samples":               fetch("cpu_samples",       "run_id", run_ids) if include_samples else [],
        "thermal_samples":           fetch("thermal_samples",   "run_id", run_ids) if include_samples else [],
        "interrupt_samples":         fetch("interrupt_samples", "run_id", run_ids) if include_samples else [],
        "orchestration_events":      fetch("orchestration_events", "run_id", run_ids) if include_samples else [],
        "llm_interactions":          fetch("llm_interactions",  "run_id", run_ids) if include_samples else [],
        "orchestration_tax_summary": fetch("orchestration_tax_summary", "linear_run_id", run_ids) if include_samples else [],
        "outliers":                  fetch("outliers", "run_id", run_ids) if include_samples else [],
    }
    return payload


def _write_sync_log(runs_synced: int, rows_total: int, status: str) -> None:
    """POST sync result to server for audit log. Non-blocking — errors ignored."""
    try:
        server_url = get_server_url()
        import datetime
        httpx.post(
            f"{server_url}/sync-log",
            json={
                "api_key":         get_api_key(),
                "runs_synced":     runs_synced,
                "rows_total":      rows_total,
                "status":          status,
                "sync_completed_at": datetime.datetime.utcnow().isoformat(),
            },
            headers=_headers(),
            timeout=5,
        )
    except Exception:
        pass  # Never block sync for logging


def _post_sync(payload: dict) -> Optional[dict]:
    from alems.agent.mode_manager import get_sync_config
    timeout = int(get_sync_config().get("timeout_seconds", 300))
    server_url = get_server_url()
    try:
        r = httpx.post(
            f"{server_url}/bulk-sync",
            json=payload,
            headers=_headers(),
            timeout=timeout,
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
