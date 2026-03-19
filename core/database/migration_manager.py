"""Handle database migrations safely"""

import glob
import sqlite3
from pathlib import Path


class MigrationManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)

    def get_current_version(self):
        """Get current schema version"""
        try:
            result = self.conn.execute(
                "SELECT MAX(version) FROM schema_version"
            ).fetchone()
            return result[0] if result[0] else 0
        except sqlite3.OperationalError:
            # schema_version table doesn't exist yet
            return 0

    def get_pending_migrations(self):
        """Get list of migrations not yet applied"""
        current = self.get_current_version()
        migration_files = sorted(glob.glob("core/database/migrations/v*.sql"))

        pending = []
        for file in migration_files:
            version = int(Path(file).stem.split("_")[0].replace("v", ""))
            if version > current:
                pending.append((version, file))

        return pending

    def apply_migrations(self, target_version=None):
        """Apply all pending migrations"""
        pending = self.get_pending_migrations()

        if not pending:
            print("✅ Database schema is up to date")
            return

        print(f"📦 Applying {len(pending)} migrations...")

        for version, file in pending:
            if target_version and version > target_version:
                break

            print(f"  Applying v{version}...")
            sql = Path(file).read_text()

            try:
                # Execute migration in transaction
                with self.conn:
                    self.conn.executescript(sql)
                print(f"    ✅ v{version} applied")
            except Exception as e:
                print(f"    ❌ Failed: {e}")
                raise

        self.conn.close()
        print("✅ All migrations applied successfully")


def migrate(db_path="data/experiments.db"):
    """Convenience function to run migrations"""
    manager = MigrationManager(db_path)
    manager.apply_migrations()
