#!/usr/bin/env python3
"""
Test database config loading from ConfigLoader.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.config_loader import ConfigLoader


def test_db_config():
    """Test loading database configuration."""
    print("\n🔍 Testing Database Config Loading...")
    print("=" * 50)

    # Create config loader instance
    config = ConfigLoader()

    # Try to get database config
    try:
        db_config = config.get_db_config()
        print("✅ Successfully loaded database config")
        print(f"\n📋 Database Config:")
        print(f"   Engine: {db_config.get('engine')}")

        if db_config["engine"] == "sqlite":
            sqlite = db_config.get("sqlite", {})
            print(f"\n   SQLite Settings:")
            print(f"      Path: {sqlite.get('path')}")
            print(f"      Journal Mode: {sqlite.get('journal_mode')}")
            print(f"      Timeout: {sqlite.get('timeout')}s")
        else:
            pg = db_config.get("postgresql", {})
            print(f"\n   PostgreSQL Settings:")
            print(f"      Host: {pg.get('host')}")
            print(f"      Port: {pg.get('port')}")
            print(f"      Database: {pg.get('database')}")
            print(f"      User: {pg.get('user')}")
            print(f"      Pool Size: {pg.get('pool_size')}")

        print(f"\n   Common Settings:")
        print(f"      Backup Enabled: {db_config.get('backup_enabled')}")
        print(f"      Backup Interval: {db_config.get('backup_interval_hours')}h")

        return True

    except Exception as e:
        print(f"❌ Failed to load database config: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_db_config()
    sys.exit(0 if success else 1)
