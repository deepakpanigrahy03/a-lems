#!/usr/bin/env python3
"""
Migration v4: Add error_message and status columns to llm_interactions table.

This captures failed LLM calls with error details for failure analysis.
"""

import sqlite3
import shutil
from datetime import datetime
from pathlib import Path

DB_PATH = Path("data/experiments.db")
BACKUP_PATH = Path(f"data/experiments_backup_v3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")


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
    print("A-LEMS Schema Migration v4: Add error_message and status columns")
    print("=" * 60)

    # Backup
    backup_database()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Check existing columns
    cursor.execute("PRAGMA table_info(llm_interactions)")
    existing_cols = [col[1] for col in cursor.fetchall()]

    # Add error_message column
    if "error_message" not in existing_cols:
        cursor.execute("ALTER TABLE llm_interactions ADD COLUMN error_message TEXT")
        print("✅ Added column: error_message")
    else:
        print("ℹ️ Column error_message already exists")

    # Add status column
    if "status" not in existing_cols:
        cursor.execute("ALTER TABLE llm_interactions ADD COLUMN status TEXT")
        print("✅ Added column: status")
    else:
        print("ℹ️ Column status already exists")

    # Update existing rows to have default status
    cursor.execute("UPDATE llm_interactions SET status = 'success' WHERE status IS NULL")
    print(f"✅ Updated {cursor.rowcount} rows with status='success'")

    # Create schema_version table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        )
    """)

    # Record version
    cursor.execute("SELECT 1 FROM schema_version WHERE version = 4")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO schema_version (version, description) VALUES (4, ?)",
            ("Add error_message and status columns to llm_interactions for failure tracking",)
        )
        print("✅ Recorded schema version 4")
    else:
        print("ℹ️ Schema version 4 already recorded")

    conn.commit()
    conn.close()
    print("=" * 60)
    print("✅ Migration complete!")
    print("=" * 60)


if __name__ == "__main__":
    migrate()
