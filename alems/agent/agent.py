"""
alems/agent/agent.py
────────────────────────────────────────────────────────────────────────────
A-LEMS Agent — main process.

Three daemon threads run in parallel:
  1. Heartbeat thread  — reports status to server every 30s (5s during run)
  2. Poll thread       — checks for new jobs every 10s when idle
  3. Sync thread       — pushes unsynced runs every 60s

The _active_run Event is the single source of truth for "is a run in progress".
Both the poll thread and heartbeat thread check it.

Usage:
    python -m alems.agent start
    python -m alems.agent start --mode local
    python -m alems.agent status
    python -m alems.agent set-mode connected
    python -m alems.agent set-mode local
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import argparse
import signal
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH      = str(PROJECT_ROOT / "data" / "experiments.db")

# ── Shared state ──────────────────────────────────────────────────────────────
_active_run   = threading.Event()   # SET when a run is executing
_shutdown     = threading.Event()   # SET to stop all threads
_current_job_id: Optional[str] = None
_current_global_run_id: Optional[str] = None
_current_run_id: Optional[int] = None  # local SQLite run_id during execution
_last_sync_at: Optional[str] = None


def _get_db_path() -> str:
    """Allow DB path override via env var for testing."""
    import os
    return os.environ.get("ALEMS_SQLITE_PATH", DB_PATH)


# ── Thread 1: Heartbeat ───────────────────────────────────────────────────────

def _heartbeat_loop():
    from alems.agent.heartbeat import send_heartbeat
    from alems.agent.mode_manager import get_execution_config, get_mode
    from alems.agent.sync_client import count_unsynced
    from alems.shared.models import LiveMetrics

    print("[agent] Heartbeat thread started")
    while not _shutdown.is_set():
        cfg      = get_execution_config()
        interval = (
            int(cfg.get("heartbeat_run_s", 5))
            if _active_run.is_set()
            else int(cfg.get("heartbeat_s", 30))
        )

        if get_mode() == "connected":
            status = "running" if _active_run.is_set() else "idle"
            live   = _build_live_metrics() if _active_run.is_set() else None

            action = send_heartbeat(
                status=status,
                db_path=_get_db_path(),
                live=live,
                unsynced_runs=count_unsynced(_get_db_path()),
                last_sync_at=_last_sync_at,
            )

            if action == "sync_now":
                print("[agent] Server requested immediate sync")
                _trigger_sync()
            elif action == "reregister":
                print("[agent] Server requested re-registration")
                _do_registration()

        _shutdown.wait(timeout=interval)

    print("[agent] Heartbeat thread stopped")


def _post_run_status_cache(run_id: int, job_id: str) -> None:
    """Write completed run metrics to server run_status_cache."""
    try:
        con = sqlite3.connect(_get_db_path())
        con.row_factory = sqlite3.Row
        row = con.execute("""
            SELECT r.run_id, r.exp_id, r.workflow_type,
                   r.total_energy_uj, r.avg_power_watts,
                   r.total_tokens, r.steps, r.duration_ns,
                   e.task_name, e.model_name
            FROM runs r JOIN experiments e ON e.exp_id = r.exp_id
            WHERE r.run_id = ? LIMIT 1
        """, (run_id,)).fetchone()
        con.close()
        if not row:
            return
        from alems.agent.heartbeat import send_heartbeat
        from alems.shared.models import LiveMetrics
        metrics = LiveMetrics(
            run_id=run_id,
            job_id=job_id,
            task_name=row["task_name"],
            model_name=row["model_name"],
            workflow_type=row["workflow_type"],
            elapsed_s=int((row["duration_ns"] or 0) / 1e9),
            energy_uj=row["total_energy_uj"],
            avg_power_watts=row["avg_power_watts"],
            total_tokens=row["total_tokens"],
            steps=row["steps"],
        )
        # Send as "running" so server writes to run_status_cache, then idle
        send_heartbeat("running", _get_db_path(), live=metrics)
        print(f"[agent] Posted run metrics to server: run_id={run_id}")
    except Exception as e:
        print(f"[agent] run_status_cache post error: {e}")


def _build_live_metrics():
    """Read latest metrics from SQLite for the current active run."""
    from alems.shared.models import LiveMetrics
    global _current_job_id, _current_global_run_id, _current_run_id

    if not _current_run_id:
        return None
    try:
        con = sqlite3.connect(_get_db_path())
        con.row_factory = sqlite3.Row
        row = con.execute("""
            SELECT r.run_id, r.exp_id, r.workflow_type,
                   r.total_energy_uj, r.avg_power_watts,
                   r.total_tokens, r.steps,
                   e.task_name, e.model_name,
                   (CAST(strftime('%s','now') AS INTEGER) - r.start_time_ns/1000000000) as elapsed_s
            FROM runs r
            JOIN experiments e ON e.exp_id = r.exp_id
            WHERE r.run_id = ?
            ORDER BY r.run_id DESC LIMIT 1
        """, (_current_run_id,)).fetchone()
        con.close()
        if not row:
            return None
        return LiveMetrics(
            run_id=row["run_id"],
            exp_id=row["exp_id"],
            global_run_id=_current_global_run_id,
            job_id=_current_job_id,
            task_name=row["task_name"],
            model_name=row["model_name"],
            workflow_type=row["workflow_type"],
            elapsed_s=int(row["elapsed_s"] or 0),
            energy_uj=row["total_energy_uj"],
            avg_power_watts=row["avg_power_watts"],
            total_tokens=row["total_tokens"],
            steps=row["steps"],
        )
    except Exception as e:
        print(f"[agent] Live metrics error: {e}")
        return None


# ── Thread 2: Job poll ────────────────────────────────────────────────────────

def _poll_loop():
    from alems.agent.heartbeat import fetch_job, report_job_status
    from alems.agent.job_executor import execute_job, build_command
    from alems.agent.mode_manager import get_execution_config, get_mode
    from alems.agent.sync_client import sync_unsynced_runs
    global _current_job_id, _current_global_run_id

    print("[agent] Poll thread started")
    while not _shutdown.is_set():
        cfg      = get_execution_config()
        interval = int(cfg.get("poll_interval_s", 10))

        if get_mode() == "connected" and not _active_run.is_set():
            job = fetch_job(_get_db_path())
            if job:
                print(f"[agent] Got job: {job.job_id}")
                _active_run.set()
                _current_job_id        = job.job_id
                _current_global_run_id = None
                _current_run_id        = None

                report_job_status(job.job_id, "started", _get_db_path())

                try:
                    cmd = job.command or build_command(job.exp_config)
                    # Pre-set expected run_id so heartbeat sends live metrics during run
                    try:
                        _con = sqlite3.connect(_get_db_path())
                        _row = _con.execute("SELECT COALESCE(MAX(run_id),0) FROM runs").fetchone()
                        _con.close()
                        _current_run_id = int(_row[0]) + 1
                    except Exception:
                        _current_run_id = None

                    new_run_id = execute_job(
                        command=cmd,
                        db_path=_get_db_path(),
                        job_id=job.job_id,
                    )
                    _current_run_id        = new_run_id
                    _current_global_run_id = str(new_run_id) if new_run_id else None

                    report_job_status(
                        job.job_id, "completed", _get_db_path(),
                        global_run_id=_current_global_run_id,
                    )
                    print(f"[agent] Job {job.job_id} completed — run_id={new_run_id}")
                    # Write final metrics to run_status_cache so server can see result
                    if new_run_id:
                        _post_run_status_cache(new_run_id, job.job_id)

                except Exception as e:
                    print(f"[agent] Job {job.job_id} error: {e}")
                    report_job_status(
                        job.job_id, "failed", _get_db_path(),
                        error_message=str(e),
                    )
                finally:
                    _active_run.clear()
                    _current_job_id = None
                    _current_run_id = None

                # Sync immediately after run completes — full (metadata + samples)
                _trigger_sync(full=True)

        _shutdown.wait(timeout=interval)

    print("[agent] Poll thread stopped")


# ── Thread 3: Background sync ─────────────────────────────────────────────────

def _sync_loop():
    from alems.agent.mode_manager import get_mode, get_sync_config
    global _last_sync_at

    print("[agent] Sync thread started")
    while not _shutdown.is_set():
        cfg      = get_sync_config()
        interval = int(cfg.get("interval_seconds", 60))

        if get_mode() == "connected":
            result = _trigger_sync()
            if result.get("status") == "ok" and result.get("runs_synced", 0) > 0:
                import datetime
                _last_sync_at = datetime.datetime.now().isoformat()

        _shutdown.wait(timeout=interval)

    print("[agent] Sync thread stopped")


def _trigger_sync(full: bool = False) -> dict:
    from alems.agent.sync_client import sync_unsynced_runs, _sync_pending_samples
    from alems.agent.mode_manager import get_sync_config
    result = sync_unsynced_runs(_get_db_path())
    if full:
        cfg = get_sync_config()
        _sync_pending_samples(_get_db_path(),
                              int(cfg.get("retry_max", 3)),
                              int(cfg.get("retry_backoff_s", 5)))
    return result


# ── Registration ──────────────────────────────────────────────────────────────

def _do_registration() -> bool:
    from alems.agent.heartbeat import register
    from alems.agent.mode_manager import is_registered
    if is_registered():
        return True
    print("[agent] Registering with server...")
    return register(_get_db_path())

def _sync_all_baselines():
    """
    Sync idle_baselines and task_categories to server on startup.
    These reference tables must exist in PostgreSQL before runs sync
    because SQL calculations use baseline power/temperature values.
    Idempotent — ON CONFLICT DO NOTHING on server side.
    """
    import sqlite3
    import httpx
    from alems.agent.mode_manager import get_api_key, get_server_url

    db_path    = _get_db_path()
    server_url = get_server_url()

    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        baselines = [dict(r) for r in con.execute(
            "SELECT * FROM idle_baselines").fetchall()]
        task_cats = [dict(r) for r in con.execute(
            "SELECT * FROM task_categories").fetchall()]
        hw_row    = con.execute(
            "SELECT * FROM hardware_config LIMIT 1").fetchone()
        hw_data   = dict(hw_row) if hw_row else {}
        con.close()

        payload = {
            "hardware_hash":             hw_data.get("hardware_hash", ""),
            "api_key":                   get_api_key(),
            "hardware_data":             hw_data,
            "environment_config":        [],
            "idle_baselines":            baselines,
            "task_categories":           task_cats,
            "experiments":               [],
            "runs":                      [],
            "energy_samples":            [],
            "cpu_samples":               [],
            "thermal_samples":           [],
            "interrupt_samples":         [],
            "orchestration_events":      [],
            "llm_interactions":          [],
            "orchestration_tax_summary": [],
            "outliers":                  [],
        }

        r = httpx.post(
            f"{server_url}/bulk-sync",
            json=payload,
            headers={"Authorization": f"Bearer {get_api_key()}",
                     "Content-Type": "application/json"},
            timeout=30,
        )
        if r.status_code == 200:
            print(f"[agent] Reference tables synced: "
                  f"{len(baselines)} baselines, "
                  f"{len(task_cats)} task categories")
        else:
            print(f"[agent] Reference sync warning: HTTP {r.status_code}")
    except Exception as e:
        print(f"[agent] Reference sync error (non-fatal): {e}")

# ── CLI entry point ───────────────────────────────────────────────────────────

def cmd_start(mode: Optional[str] = None):
    from alems.agent.backfill import backfill_global_ids, verify_backfill
    from alems.agent.mode_manager import (
        ensure_conf_exists, get_mode, set_mode, is_registered,
    )

    ensure_conf_exists()

    if mode:
        set_mode(mode)

    # Run backfill on every start — idempotent, fast if already done
    print("[agent] Checking UUID backfill...")
    backfill_global_ids(_get_db_path())

    current_mode = get_mode()
    print(f"[agent] Starting in {current_mode.upper()} mode")

    if current_mode == "connected":
        if not is_registered():
            if not _do_registration():
                print("[agent] WARNING: Registration failed — running without server connection")
                print("[agent] Will retry registration on next heartbeat")
        # Sync reference tables before any run sync
        # idle_baselines MUST exist in PostgreSQL before runs can sync
        _sync_all_baselines()
                

    # Signal handler for clean shutdown
    def _handle_signal(sig, frame):
        print("\n[agent] Shutting down...")
        _shutdown.set()

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Start threads
    threads = [
        threading.Thread(target=_heartbeat_loop, name="heartbeat", daemon=True),
        threading.Thread(target=_poll_loop,       name="poll",      daemon=True),
        threading.Thread(target=_sync_loop,        name="sync",      daemon=True),
    ]
    for t in threads:
        t.start()
    print(f"[agent] All threads running — db={_get_db_path()}")

    # Keep main thread alive
    try:
        while not _shutdown.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown.set()

    for t in threads:
        t.join(timeout=5)
    print("[agent] Agent stopped")


def cmd_status():
    from alems.agent.mode_manager import (
        get_mode, get_server_url, get_local_hw_id,
        get_server_hw_id, is_registered,
    )
    from alems.agent.heartbeat import check_server_health
    from alems.agent.sync_client import count_unsynced

    mode       = get_mode()
    registered = is_registered()
    server_ok  = check_server_health() if mode == "connected" else None
    unsynced   = count_unsynced(_get_db_path())

    print(f"\nA-LEMS Agent Status")
    print(f"  mode:         {mode}")
    print(f"  server_url:   {get_server_url()}")
    print(f"  registered:   {registered}")
    print(f"  server alive: {server_ok}")
    print(f"  local hw_id:  {get_local_hw_id()}")
    print(f"  server hw_id: {get_server_hw_id()}")
    print(f"  unsynced runs:{unsynced}")
    print(f"  db:           {_get_db_path()}\n")


def main():
    parser = argparse.ArgumentParser(description="A-LEMS Agent")
    sub = parser.add_subparsers(dest="cmd")

    p_start = sub.add_parser("start", help="Start the agent")
    p_start.add_argument("--mode", choices=["local", "connected"], default=None)

    sub.add_parser("status", help="Show agent status")

    p_mode = sub.add_parser("set-mode", help="Switch mode")
    p_mode.add_argument("mode", choices=["local", "connected"])

    args = parser.parse_args()

    if args.cmd == "start":
        cmd_start(mode=getattr(args, "mode", None))
    elif args.cmd == "status":
        cmd_status()
    elif args.cmd == "set-mode":
        from alems.agent.mode_manager import set_mode
        set_mode(args.mode)
        print(f"Mode set to: {args.mode}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
