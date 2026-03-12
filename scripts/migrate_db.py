#!/usr/bin/env python3
"""Run database migrations safely"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.database.migration_manager import migrate

if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/experiments.db"
    print(f"🔄 Migrating database: {db_path}")
    migrate(db_path)
    print("✅ Migration complete")
