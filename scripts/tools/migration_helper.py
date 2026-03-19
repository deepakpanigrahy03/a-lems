#!/usr/bin/env python3
"""Run database migrations safely"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from core.database.migration_manager import migrate
    from scripts.tools.path_loader import config
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running from project root:")
    print("  cd ~/mydrive/a-lems")
    print("  python scripts/tools/migration_helper.py")
    sys.exit(1)

if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else str(config.DB_PATH)
    print(f"🔄 Migrating database: {db_path}")
    migrate(db_path)
    print("✅ Migration complete")