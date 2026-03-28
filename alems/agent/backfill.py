"""
alems/agent/backfill.py
────────────────────────────────────────────────────────────────────────────
No UUID backfill needed — PostgreSQL assigns its own BIGSERIAL IDs.

This module only ensures sync_status=0 for any rows that need syncing.
Called by run_migrations.py after migration 007 for consistency.
────────────────────────────────────────────────────────────────────────────
"""

import argparse
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent


def _default_db() -> Path:
    return PROJECT_ROOT / "data" / "experiments.db"


def backfill_global_ids(db_path: str) -> dict:
    """
    No UUID backfill needed in clean design.
    Just verifies sync_status column exists and resets any NULL values.
    """
    path = Path(db_path)
    if not path.exists():
        print(f"[backfill] ERROR: {path} not found")
        sys.exit(1)

    con = sqlite3.connect(path)

    # Ensure sync_status has no NULLs (default should be 0)
    result = con.execute(
        "UPDATE runs SET sync_status=0 WHERE sync_status IS NULL"
    )
    fixed = result.rowcount

    # Count unsynced
    row = con.execute(
        "SELECT COUNT(*) FROM runs WHERE sync_status=0"
    ).fetchone()
    unsynced = row[0] if row else 0

    con.commit()
    con.close()

    if fixed:
        print(f"[backfill] Fixed {fixed} NULL sync_status rows")
    print(f"[backfill] {unsynced} runs pending sync to PostgreSQL")

    return {"unsynced": unsynced, "fixed": fixed}


def verify_backfill(db_path: str) -> bool:
    """Verify sync_status is clean — no NULLs."""
    con = sqlite3.connect(db_path)
    row = con.execute(
        "SELECT COUNT(*) FROM runs WHERE sync_status IS NULL"
    ).fetchone()
    nulls = row[0] if row else 0
    con.close()

    if nulls > 0:
        print(f"[verify] WARNING: {nulls} runs have NULL sync_status")
        return False
    print("[verify] sync_status clean — all rows have a status value")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args()
    db = args.db or str(_default_db())
    backfill_global_ids(db)
    verify_backfill(db)
