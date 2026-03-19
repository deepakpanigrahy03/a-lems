#!/usr/bin/env python3
"""
Sync task categories from YAML to database.
Run this whenever tasks.yaml changes.
"""

import os
import sqlite3
import sys
from pathlib import Path

import yaml

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def load_tasks_from_yaml():
    """Load all tasks with their categories from YAML"""
    yaml_path = Path(__file__).parent.parent / "config" / "tasks.yaml"

    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)

    tasks = []
    for task in config.get("tasks", []):
        if "id" in task and "category" in task:
            tasks.append((task["id"], task["category"]))

    return tasks


def sync_to_database(tasks):
    """Full refresh of task_categories table"""
    db_path = Path(__file__).parent.parent / "data" / "experiments.db"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Clear existing
    cursor.execute("DELETE FROM task_categories")

    # Insert all tasks
    cursor.executemany(
        "INSERT INTO task_categories (task_id, category) VALUES (?, ?)", tasks
    )

    # Insert fallback for custom queries
    cursor.execute(
        "INSERT INTO task_categories (task_id, category) VALUES (?, ?)",
        ("custom_query", "custom"),
    )

    conn.commit()

    # Show summary
    cursor.execute("""
        SELECT category, COUNT(*) as count 
        FROM task_categories 
        GROUP BY category 
        ORDER BY count DESC
    """)

    print("\n✅ Task categories synced successfully!")
    print("\n📊 Summary:")
    for cat, count in cursor.fetchall():
        print(f"   • {cat}: {count} tasks")

    conn.close()

    return len(tasks)


def main():
    print("🔄 Syncing task categories from YAML to database...")

    tasks = load_tasks_from_yaml()
    print(f"📋 Found {len(tasks)} tasks in YAML")

    count = sync_to_database(tasks)
    print(f"\n✅ Done! {count} tasks + 'custom' fallback synced.")


if __name__ == "__main__":
    main()
