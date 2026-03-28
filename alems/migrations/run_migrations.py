"""
alems/migrations/run_migrations.py
────────────────────────────────────────────────────────────────────────────
Migration runner for A-LEMS distributed setup.

Migration 007 adds ONLY:
  - runs.sync_status          (track what's been pushed to PostgreSQL)
  - hardware_config agent tracking columns

No UUID columns. No backfill. PostgreSQL assigns its own sequential IDs.

Usage:
    # Apply SQLite migration (every local machine)
    python -m alems.migrations.run_migrations

    # Apply PostgreSQL schema (Oracle VM only, first time)
    python -m alems.migrations.run_migrations --postgres

    # Apply PostgreSQL UUID removal migration (Oracle VM, one time)
    python -m alems.migrations.run_migrations --postgres-migrate-002
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
    Adds sync_status and hardware_config agent tracking columns.
    NO UUID columns — PostgreSQL assigns its own sequential IDs.
    """
    print(f"\n[migration] SQLite target: {db_path}")

    if not db_path.exists():
        print(f"[migration] ERROR: {db_path} not found")
        sys.exit(1)

    con = sqlite3.connect(db_path)
    current_version = _get_sqlite_version(con)
    print(f"[migration] Current schema version: {current_version}")

    # Check if sync_status exists (the key column for migration 007)
    if current_version >= 7:
        missing = not _column_exists(con, "runs", "sync_status")
        if not missing:
            print("[migration] Migration 007 already applied — skipping")
            con.close()
            return
        print("[migration] Migration 007 version set but columns missing — reapplying...")

    print("[migration] Applying migration 007 (distributed identity)...")

    # ── runs: sync tracking ───────────────────────────────────────────────────
    if not _column_exists(con, "runs", "sync_status"):
        con.execute("ALTER TABLE runs ADD COLUMN sync_status INTEGER DEFAULT 0")
        print("  + runs.sync_status")

    # ── hardware_config: agent tracking ───────────────────────────────────────
    for col, defn in [
        ("last_seen",     "TIMESTAMP"),
        ("agent_status",  "TEXT DEFAULT 'offline'"),
        ("agent_version", "TEXT"),
        ("server_hw_id",  "INTEGER"),
        ("api_key",       "TEXT"),
    ]:
        if not _column_exists(con, "hardware_config", col):
            con.execute(f"ALTER TABLE hardware_config ADD COLUMN {col} {defn}")
            print(f"  + hardware_config.{col}")

    # ── indexes ───────────────────────────────────────────────────────────────
    if not _index_exists(con, "idx_runs_sync_status"):
        con.execute("CREATE INDEX IF NOT EXISTS idx_runs_sync_status ON runs(sync_status)")
        print("  + index idx_runs_sync_status")

    # ── version bump ──────────────────────────────────────────────────────────
    con.execute(
        "INSERT OR IGNORE INTO schema_version(version, description) VALUES (?, ?)",
        (7, "distributed identity: sync_status, agent tracking (no UUIDs)")
    )

    con.commit()
    con.close()
    print("[migration] Migration 007 applied successfully")


def apply_postgres_initial(pg_url: str) -> None:
    """Apply PostgreSQL initial schema (001_postgres_initial.sql)."""
    try:
        import psycopg2
    except ImportError:
        print("[migration] ERROR: psycopg2 not installed: pip install psycopg2-binary")
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
        print("[migration] PostgreSQL initial schema applied successfully")
    except Exception as e:
        con.rollback()
        print(f"[migration] ERROR: {e}")
        sys.exit(1)
    finally:
        cur.close()
        con.close()


def apply_postgres_002(pg_url: str) -> None:
    """Apply PostgreSQL migration 002 — remove UUID PKs, use BIGSERIAL."""
    try:
        import psycopg2
    except ImportError:
        print("[migration] ERROR: psycopg2 not installed: pip install psycopg2-binary")
        sys.exit(1)

    sql_path = MIGRATIONS_DIR / "002_remove_uuid_pks.sql"
    if not sql_path.exists():
        print(f"[migration] ERROR: {sql_path} not found")
        sys.exit(1)

    print(f"\n[migration] Applying PostgreSQL migration 002 (remove UUID PKs)...")
    sql = sql_path.read_text()

    con = psycopg2.connect(pg_url)
    cur = con.cursor()
    try:
        cur.execute(sql)
        con.commit()
        print("[migration] PostgreSQL migration 002 applied successfully")
    except Exception as e:
        con.rollback()
        print(f"[migration] ERROR: {e}")
        raise
    finally:
        cur.close()
        con.close()


def main():
    parser = argparse.ArgumentParser(description="A-LEMS migration runner")
    parser.add_argument("--db",                  type=str, default=None)
    parser.add_argument("--postgres",            action="store_true",
                        help="Apply PostgreSQL initial schema")
    parser.add_argument("--postgres-migrate-002", action="store_true",
                        help="Remove UUID PKs from PostgreSQL (run once)")
    args = parser.parse_args()

    pg_url = os.environ.get("ALEMS_DB_URL")

    if args.postgres:
        if not pg_url:
            print("[migration] ERROR: ALEMS_DB_URL not set")
            sys.exit(1)
        apply_postgres_initial(pg_url)

    elif getattr(args, "postgres_migrate_002", False):
        if not pg_url:
            print("[migration] ERROR: ALEMS_DB_URL not set")
            sys.exit(1)
        apply_postgres_002(pg_url)

    else:
        db_path = Path(args.db) if args.db else _default_sqlite_path()
        apply_sqlite_007(db_path)


if __name__ == "__main__":
    main()
