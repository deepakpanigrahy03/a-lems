"""
alems/agent/job_executor.py
────────────────────────────────────────────────────────────────────────────
Wraps the existing test_harness execution pipeline.
The command string comes from the server's job payload and is run as a
subprocess exactly as it would be from the Streamlit "Execute Run" button.

Key behaviours:
  - Assigns global_run_id BEFORE the run starts (inserted into SQLite
    immediately so the heartbeat can report it live)
  - Does NOT modify test_harness in any way
  - Returns the global_run_id of the completed run (or None on failure)
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

from alems.shared.uuid_gen import new_run_uuid, new_exp_uuid

PROJECT_ROOT = Path(__file__).parent.parent.parent


def execute_job(
    command: str,
    db_path: str,
    job_id: Optional[str] = None,
    cwd: Optional[str] = None,
) -> Optional[str]:
    """
    Run test_harness command as subprocess.
    Assigns global_run_id to the resulting run row.
    Returns global_run_id on success, None on failure.
    """
    working_dir = cwd or str(PROJECT_ROOT)
    print(f"[executor] Executing: {command}")
    print(f"[executor] Working dir: {working_dir}")

    # Snapshot run count before execution
    run_id_before = _get_max_run_id(db_path)

    start_time = time.time()
    try:
        result = subprocess.run(
            shlex.split(command),
            cwd=working_dir,
            capture_output=False,   # let output stream to terminal
            text=True,
            timeout=3600,           # 1 hour hard limit
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

    # Find the new run(s) created by this job and assign global_run_ids
    global_run_id = _assign_global_ids_to_new_runs(db_path, run_id_before)
    return global_run_id


def _get_max_run_id(db_path: str) -> int:
    try:
        con = sqlite3.connect(db_path)
        row = con.execute("SELECT MAX(run_id) FROM runs").fetchone()
        con.close()
        return int(row[0] or 0)
    except Exception:
        return 0


def _assign_global_ids_to_new_runs(db_path: str, run_id_before: int) -> Optional[str]:
    """
    Find all runs with run_id > run_id_before and assign global_run_id.
    Also assigns global_exp_id to their experiments if missing.
    Returns the global_run_id of the last new run.
    """
    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row

        # New runs created by this job
        new_runs = con.execute("""
            SELECT run_id, exp_id, hw_id
            FROM runs
            WHERE run_id > ? AND global_run_id IS NULL
            ORDER BY run_id ASC
        """, (run_id_before,)).fetchall()

        last_global_run_id = None
        exp_ids_done = set()

        for row in new_runs:
            run_id = row["run_id"]
            exp_id = row["exp_id"]
            hw_id  = row["hw_id"] or 1

            # Assign global_run_id
            uid = new_run_uuid()
            con.execute(
                "UPDATE runs SET global_run_id=? WHERE run_id=?",
                (uid, run_id)
            )
            last_global_run_id = uid

            # Propagate to child tables
            for tbl in ["energy_samples", "cpu_samples", "thermal_samples",
                         "interrupt_samples", "orchestration_events",
                         "llm_interactions"]:
                con.execute(f"""
                    UPDATE {tbl} SET global_run_id=?
                    WHERE run_id=? AND global_run_id IS NULL
                """, (uid, run_id))

            # Assign global_exp_id if missing
            if exp_id not in exp_ids_done:
                exp_row = con.execute(
                    "SELECT global_exp_id FROM experiments WHERE exp_id=?",
                    (exp_id,)
                ).fetchone()
                if exp_row and not exp_row["global_exp_id"]:
                    con.execute(
                        "UPDATE experiments SET global_exp_id=? WHERE exp_id=?",
                        (new_exp_uuid(), exp_id)
                    )
                exp_ids_done.add(exp_id)

        con.commit()
        con.close()

        if new_runs:
            print(f"[executor] Assigned global_run_id to {len(new_runs)} new run(s)")

        return last_global_run_id

    except Exception as e:
        print(f"[executor] Error assigning global IDs: {e}")
        return None


def build_command(exp_config: dict) -> str:
    """
    Build the test_harness CLI command from an experiment config dict.
    Mirrors what the Streamlit Execute page does.
    """
    parts = [
        sys.executable, "-m", "core.execution.tests.test_harness",
        "--task-id",     str(exp_config.get("task_id", "gsm8k_basic")),
        "--provider",    str(exp_config.get("provider", "cloud")),
        "--repetitions", str(exp_config.get("repetitions", 3)),
        "--country",     str(exp_config.get("country", "US")),
        "--cool-down",   str(exp_config.get("cool_down", 5)),
        "--save-db",
    ]
    if exp_config.get("workflow_type"):
        parts += ["--workflow-type", exp_config["workflow_type"]]
    if exp_config.get("model_name"):
        parts += ["--model", exp_config["model_name"]]
    return " ".join(parts)
