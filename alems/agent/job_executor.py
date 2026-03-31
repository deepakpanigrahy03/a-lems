"""
alems/agent/job_executor.py
────────────────────────────────────────────────────────────────────────────
Wraps the existing test_harness execution pipeline.

The command string comes from the server job payload and is run as a
subprocess exactly as it would be from the Streamlit "Execute Run" button.

No UUID assignment — PostgreSQL assigns global IDs when synced.
Local SQLite uses natural run_id/exp_id integers.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import shlex
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).parent.parent.parent


def execute_job(
    command: str,
    db_path: str,
    job_id: Optional[str] = None,
    cwd: Optional[str] = None,
) -> Optional[int]:
    """
    Run test_harness command as subprocess.
    Returns the new run_id created in SQLite on success, None on failure.
    No UUID assignment — PostgreSQL assigns global IDs during sync.
    """
    working_dir = cwd or str(PROJECT_ROOT)

    # Replace python executable with local venv python — command may have been
    # built on a different machine with a different python path
    import re
    command = re.sub(r'^/[^ ]+/python[^ ]*', sys.executable, command)

    print(f"[executor] Executing: {command}")
    print(f"[executor] Working dir: {working_dir}")

    run_id_before = _get_max_run_id(db_path)

    start_time = time.time()
    try:
        result = subprocess.run(
            shlex.split(command),
            cwd=working_dir,
            capture_output=False,
            text=True,
            timeout=3600,
        )
    except subprocess.TimeoutExpired:
        print("[executor] ERROR: job timed out after 1 hour")
        return None
    except Exception as e:
        print(f"[executor] ERROR: {e}")
        return None

    elapsed = time.time() - start_time

    if result.returncode != 0:
        print(f"[executor] Job failed (rc={result.returncode}) after {elapsed:.1f}s")
        return None

    print(f"[executor] Job completed in {elapsed:.1f}s")

    # Find the new run_id created by this job
    new_run_id = _get_new_run_id(db_path, run_id_before)
    if new_run_id:
        print(f"[executor] New run created: run_id={new_run_id}")
    return new_run_id


def _get_max_run_id(db_path: str) -> int:
    try:
        con = sqlite3.connect(db_path)
        row = con.execute("SELECT MAX(run_id) FROM runs").fetchone()
        con.close()
        return int(row[0] or 0)
    except Exception:
        return 0


def _get_new_run_id(db_path: str, run_id_before: int) -> Optional[int]:
    """Return the highest run_id created after run_id_before."""
    try:
        con = sqlite3.connect(db_path)
        row = con.execute(
            "SELECT MAX(run_id) FROM runs WHERE run_id > ?",
            (run_id_before,)
        ).fetchone()
        con.close()
        return int(row[0]) if row and row[0] else None
    except Exception:
        return None


def build_command(exp_config: dict) -> str:
    """
    Build the test_harness CLI command from an experiment config dict.
    Mirrors what the Streamlit Execute page does.
    """
    # Support both old (task_id/provider) and new (tasks/providers) config keys
    tasks     = exp_config.get("tasks") or exp_config.get("task_id", "gsm8k_basic")
    providers = exp_config.get("providers") or exp_config.get("provider", "cloud")
    parts = [
        sys.executable, "-m", "core.execution.tests.run_experiment",
        "--tasks",       str(tasks),
        "--providers",   str(providers),
        "--repetitions", str(exp_config.get("repetitions", 3)),
        "--country",     str(exp_config.get("country", "US")),
        "--save-db",
    ]
    return " ".join(parts)
