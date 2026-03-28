"""
alems/migrations/run_migrations.py
────────────────────────────────────────────────────────────────────────────
Run SQLite migration 007 on a local experiments.db.
Also validates PostgreSQL schema when ALEMS_DB_URL is set.

Usage:
    # Apply to default SQLite path
    python -m alems.migrations.run_migrations

    # Apply to specific SQLite file
    python -m alems.migrations.run_migrations --db /path/to/experiments.db

    # Apply PostgreSQL schema (requires ALEMS_DB_URL env var)
    python -m alems.migrations.run_migrations --postgres
────────────────────────────────────────────────────────────────────────────
"""

import argparse
import os
import sqlite3
import sys
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent
PROJECT_ROOT   = MIGRATIONS_DIR.parent.parent


def _default_sqlite_path() -> Path:
    return PROJECT_ROOT / "data" / "experiments.db"


def _get_sqlite_version(con: sqlite3.Connection) -> int:
    try:
        row = con.execute(
            "SELECT MAX(version) as v FROM schema_version"
        ).fetchone()
        return int(row[0] or 0)
    except Exception:
        return 0


def _column_exists(con: sqlite3.Connection, table: str, column: str) -> bool:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return any(r[1] == column for r in rows)


def _index_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?",
        (name,)
    ).fetchone()
    return bool(row and row[0])


def apply_sqlite_007(db_path: Path) -> None:
    """
    Apply migration 007 idempotently to SQLite.
    Checks each ALTER TABLE before running — safe to run multiple times.
    """
    print(f"\n[migration] SQLite target: {db_path}")

    if not db_path.exists():
        print(f"[migration] ERROR: {db_path} not found")
        sys.exit(1)

    con = sqlite3.connect(db_path)

    current_version = _get_sqlite_version(con)
    print(f"[migration] Current schema version: {current_version}")

    if current_version >= 7:
        print("[migration] Migration 007 already applied — skipping")
        con.close()
        return

    print("[migration] Applying migration 007 (distributed identity)...")

    # ── experiments ──────────────────────────────────────────────────────────
    if not _column_exists(con, "experiments", "global_exp_id"):
        con.execute("ALTER TABLE experiments ADD COLUMN global_exp_id TEXT")
        print("  + experiments.global_exp_id")

    # ── runs ─────────────────────────────────────────────────────────────────
    for col, defn in [
        ("global_run_id", "TEXT"),
        ("sync_status",   "INTEGER DEFAULT 0"),
    ]:
        if not _column_exists(con, "runs", col):
            con.execute(f"ALTER TABLE runs ADD COLUMN {col} {defn}")
            print(f"  + runs.{col}")

    # ── child tables ─────────────────────────────────────────────────────────
    child_tables = [
        "energy_samples",
        "cpu_samples",
        "thermal_samples",
        "interrupt_samples",
        "orchestration_events",
        "llm_interactions",
        "orchestration_tax_summary",
    ]
    for tbl in child_tables:
        if not _column_exists(con, tbl, "global_run_id"):
            con.execute(f"ALTER TABLE {tbl} ADD COLUMN global_run_id TEXT")
            print(f"  + {tbl}.global_run_id")

    # ── hardware_config agent tracking ───────────────────────────────────────
    for col, defn in [
        ("last_seen",     "TIMESTAMP"),
        ("agent_status",  "TEXT DEFAULT 'offline'"),
        ("agent_version", "TEXT"),
        ("server_hw_id",  "INTEGER"),
    ]:
        if not _column_exists(con, "hardware_config", col):
            con.execute(f"ALTER TABLE hardware_config ADD COLUMN {col} {defn}")
            print(f"  + hardware_config.{col}")

    # ── indexes ──────────────────────────────────────────────────────────────
    indexes = [
        ("idx_runs_sync_status", "CREATE INDEX IF NOT EXISTS idx_runs_sync_status ON runs(sync_status)"),
        ("idx_runs_global_id",   "CREATE INDEX IF NOT EXISTS idx_runs_global_id ON runs(global_run_id)"),
        ("idx_exp_global_id",    "CREATE INDEX IF NOT EXISTS idx_exp_global_id ON experiments(global_exp_id)"),
    ]
    for name, sql in indexes:
        if not _index_exists(con, name):
            con.execute(sql)
            print(f"  + index {name}")

    # ── version bump ─────────────────────────────────────────────────────────
    con.execute(
        "INSERT OR IGNORE INTO schema_version(version, description) VALUES (?, ?)",
        (7, "distributed identity: global_run_id, global_exp_id, sync_status, agent tracking")
    )

    con.commit()
    con.close()
    print("[migration] Migration 007 applied successfully")


def apply_postgres(pg_url: str) -> None:
    """Apply PostgreSQL initial schema using psycopg2."""
    try:
        import psycopg2
    except ImportError:
        print("[migration] ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
        sys.exit(1)

    sql_path = MIGRATIONS_DIR / "001_postgres_initial.sql"
    if not sql_path.exists():
        print(f"[migration] ERROR: {sql_path} not found")
        sys.exit(1)

    print(f"\n[migration] PostgreSQL target: {pg_url.split('@')[-1]}")
    sql = sql_path.read_text()

    con = psycopg2.connect(pg_url)
    cur = con.cursor()
    try:
        cur.execute(sql)
        con.commit()
        print("[migration] PostgreSQL schema applied successfully")
    except Exception as e:
        con.rollback()
        print(f"[migration] ERROR applying PostgreSQL schema: {e}")
        sys.exit(1)
    finally:
        cur.close()
        con.close()


def main():
    parser = argparse.ArgumentParser(description="A-LEMS migration runner")
    parser.add_argument("--db",       type=str, default=None,
                        help="Path to SQLite experiments.db")
    parser.add_argument("--postgres", action="store_true",
                        help="Apply PostgreSQL schema (reads ALEMS_DB_URL)")
    args = parser.parse_args()

    if args.postgres:
        pg_url = os.environ.get("ALEMS_DB_URL")
        if not pg_url:
            print("[migration] ERROR: ALEMS_DB_URL environment variable not set")
            sys.exit(1)
        apply_postgres(pg_url)
    else:
        db_path = Path(args.db) if args.db else _default_sqlite_path()
        apply_sqlite_007(db_path)
        # Always run backfill after migration
        print("\n[migration] Running UUID backfill for existing rows...")
        from alems.agent.backfill import backfill_global_ids
        backfill_global_ids(str(db_path))


if __name__ == "__main__":
    main()
