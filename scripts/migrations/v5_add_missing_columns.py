#!/usr/bin/env python3
"""
Migration v5: Add missing columns to llm_interactions table.

Columns to add:
- bytes_sent_approx, bytes_recv_approx, tcp_retransmits (network metrics)
- error_message, status (failure tracking)

This is the FINAL schema change. No more changes after this.
"""

import sqlite3
import shutil
from datetime import datetime
from pathlib import Path

DB_PATH = Path("data/experiments.db")
BACKUP_PATH = Path(f"data/experiments_backup_v4_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")


def backup_database():
    if DB_PATH.exists():
        print(f"📦 Creating backup: {BACKUP_PATH}")
        shutil.copy2(DB_PATH, BACKUP_PATH)
        print(f"✅ Backup created")
    else:
        print(f"⚠️ Database not found at {DB_PATH}")


def migrate():
    print("=" * 60)
    print("A-LEMS Schema Migration v5: Add missing columns (FINAL)")
    print("=" * 60)

    backup_database()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get existing columns
    cursor.execute("PRAGMA table_info(llm_interactions)")
    existing = [col[1] for col in cursor.fetchall()]

    # Define columns to add
    columns = {
        "bytes_sent_approx": "INTEGER",
        "bytes_recv_approx": "INTEGER",
        "tcp_retransmits": "INTEGER",
        "error_message": "TEXT",
        "status": "TEXT",
    }

    # Add missing columns
    for col_name, col_type in columns.items():
        if col_name not in existing:
            cursor.execute(f"ALTER TABLE llm_interactions ADD COLUMN {col_name} {col_type}")
            print(f"✅ Added column: {col_name}")
        else:
            print(f"ℹ️ Column already exists: {col_name}")

    # Update existing rows to have default status
    if "status" in columns and "status" not in existing:
        cursor.execute("UPDATE llm_interactions SET status = 'success' WHERE status IS NULL")
        print(f"✅ Updated rows with default status='success'")

    # Record version
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        )
    """)

    cursor.execute("SELECT 1 FROM schema_version WHERE version = 5")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO schema_version (version, description) VALUES (5, ?)",
            ("Add missing columns: bytes_sent_approx, bytes_recv_approx, tcp_retransmits, error_message, status",)
        )
        print("✅ Recorded schema version 5")
    else:
        print("ℹ️ Schema version 5 already recorded")

    conn.commit()
    conn.close()

    print("=" * 60)
    print("✅ Migration complete! Schema is now FINAL.")
    print("=" * 60)


if __name__ == "__main__":
    migrate()
