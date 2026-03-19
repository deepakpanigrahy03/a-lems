#!/usr/bin/env python3
"""
Test SQLiteAdapter basic functionality.
"""

import sys
import tempfile
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.database.sqlite_adapter import SQLiteAdapter


def test_sqlite_adapter():
    """Test basic SQLiteAdapter operations."""
    print("\n🔍 Testing SQLiteAdapter...")
    print("=" * 50)

    # Create temp database
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
        print(f"📁 Temp database: {db_path}")

    try:
        # Configure adapter
        config = {"path": db_path, "journal_mode": "WAL", "timeout": 30}

        # Create and connect
        db = SQLiteAdapter(config)
        db.connect()
        print("✅ Connected to database")

        # Create tables
        db.create_tables()
        print("✅ Tables created")

        # Keep connection alive with a small delay
        time.sleep(0.1)

        # Insert test experiment
        exp_id = db.insert_experiment(
            {
                "name": "test_experiment",
                "description": "Test experiment",
                "workflow_type": "linear",
                "model_name": "test-model",
                "provider": "test",
                "task_name": "test-task",
                "country_code": "US",
            }
        )
        print(f"✅ Inserted experiment with ID: {exp_id}")

        # Keep connection alive
        time.sleep(0.1)

        # Query back - should still be connected
        runs = db.get_runs_by_experiment(exp_id)
        print(f"✅ Retrieved {len(runs)} runs")

        # Verify the run data
        if runs:
            print(f"   Run count: {len(runs)}")
        else:
            print("   No runs found (expected for new experiment)")

        db.close()
        print("\n✅ All tests passed!")
        return True

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        # Cleanup
        try:
            Path(db_path).unlink(missing_ok=True)
            print(f"🧹 Cleaned up temp database")
        except:
            pass


if __name__ == "__main__":
    success = test_sqlite_adapter()
    sys.exit(0 if success else 1)
