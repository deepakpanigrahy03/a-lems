#!/usr/bin/env python3
"""
Test DatabaseFactory functionality.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.config_loader import ConfigLoader
from core.database.factory import DatabaseFactory
from core.database.sqlite_adapter import SQLiteAdapter


def test_factory_with_config():
    """Test factory using real config."""
    print("\n🔍 Testing DatabaseFactory with config...")
    print("=" * 50)

    # Load config
    config_loader = ConfigLoader()
    db_config = config_loader.get_db_config()
    print(f"✅ Loaded config with engine: {db_config.get('engine')}")

    # Create adapter
    db = DatabaseFactory.create(db_config)
    print(f"✅ Created adapter: {type(db).__name__}")

    # Verify it's the right type
    if db_config.get("engine") == "sqlite":
        assert isinstance(db, SQLiteAdapter)
        print("✅ Correct adapter type: SQLiteAdapter")

    print("\n✅ Factory test passed!")
    return True


def test_factory_with_dict():
    """Test factory with direct dictionary config."""
    print("\n🔍 Testing DatabaseFactory with dict...")
    print("=" * 50)

    # Test SQLite config
    sqlite_config = {
        "engine": "sqlite",
        "sqlite": {"path": ":memory:", "journal_mode": "WAL"},
        "backup_enabled": True,
    }

    db = DatabaseFactory.create(sqlite_config)
    print(f"✅ Created SQLite adapter: {type(db).__name__}")
    assert isinstance(db, SQLiteAdapter)

    # Test invalid engine
    try:
        bad_config = {"engine": "oracle"}
        db = DatabaseFactory.create(bad_config)
        print("❌ Should have raised error for invalid engine")
        return False
    except Exception as e:
        print(f"✅ Correctly caught invalid engine: {e}")

    print("\n✅ Factory dict test passed!")
    return True


def test_list_engines():
    """Test listing supported engines."""
    print("\n🔍 Testing supported engines list...")
    print("=" * 50)

    engines = DatabaseFactory.list_supported_engines()
    print(f"✅ Supported engines: {engines}")
    assert "sqlite" in engines

    return True


if __name__ == "__main__":
    success = all(
        [test_factory_with_config(), test_factory_with_dict(), test_list_engines()]
    )
    sys.exit(0 if success else 1)
