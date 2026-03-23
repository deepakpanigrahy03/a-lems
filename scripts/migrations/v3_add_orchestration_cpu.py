#!/usr/bin/env python3
"""
Migration v3: Add orchestration_cpu_ms column to runs table.
"""

import sqlite3
import shutil
from datetime import datetime
from pathlib import Path

DB_PATH = Path("data/experiments.db")
BACKUP_PATH = Path(f"data/experiments_backup_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")


def backup_database():
    """Create backup before migration."""
    if DB_PATH.exists():
        print(f"📦 Creating backup: {BACKUP_PATH}")
        shutil.copy2(DB_PATH, BACKUP_PATH)
        print(f"✅ Backup created")
    else:
        print(f"⚠️ Database not found at {DB_PATH}")


def migrate():
    print("=" * 60)
    print("A-LEMS Schema Migration v3: Add orchestration_cpu_ms")
    print("=" * 60)

    # Backup
    backup_database()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Check if column already exists
    cursor.execute("PRAGMA table_info(runs)")
    existing_cols = [col[1] for col in cursor.fetchall()]

    if "orchestration_cpu_ms" not in existing_cols:
        cursor.execute("ALTER TABLE runs ADD COLUMN orchestration_cpu_ms REAL")
        print("✅ Added column: orchestration_cpu_ms to runs table")
    else:
        print("ℹ️ Column orchestration_cpu_ms already exists")

    # Create schema_version table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        )
    """)

    # Insert version only if not exists
    cursor.execute("SELECT 1 FROM schema_version WHERE version = 3")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO schema_version (version, description) VALUES (3, ?)",
            ("Add orchestration_cpu_ms column to runs table for agentic orchestration overhead",)
        )
        print("✅ Recorded schema version 3")
    else:
        print("ℹ️ Schema version 3 already recorded")

    conn.commit()
    conn.close()
    print("=" * 60)
    print("✅ Migration complete!")
    print("=" * 60)


if __name__ == "__main__":
    migrate()
