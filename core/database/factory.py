#!/usr/bin/env python3
"""
================================================================================
DATABASE FACTORY – Creates appropriate database adapter based on configuration
================================================================================

PURPOSE:
    Factory pattern implementation that selects and creates the correct
    database adapter (SQLite, PostgreSQL, etc.) based on the 'engine'
    setting in the configuration file.

WHY THIS EXISTS:
    - Centralizes adapter selection logic
    - Makes it easy to add new database types
    - Decouples adapter creation from usage
    - Follows the factory design pattern

USAGE:
    config = config_loader.get_db_config()
    db = DatabaseFactory.create(config)
    db.connect()
    db.create_tables()

AUTHOR: Deepak Panigrahy
================================================================================
"""

from typing import Any, Dict

from .base import DatabaseError, DatabaseInterface
from .sqlite_adapter import SQLiteAdapter


class DatabaseFactory:
    """
    Factory class for creating database adapters.

    This factory examines the 'engine' key in the configuration
    and instantiates the appropriate adapter class.
    """

    # Registry of available adapters
    _adapters = {
        "sqlite": SQLiteAdapter,
        # 'postgresql': PostgreSQLAdapter,  # To be added in the future
        # 'mysql': MySQLAdapter,            # Future expansion
    }

    @classmethod
    def create(cls, config: Dict[str, Any]) -> DatabaseInterface:
        """
        Create and return an appropriate database adapter.

        Args:
            config: Database configuration dictionary containing:
                - engine: str - 'sqlite' or 'postgresql'
                - engine-specific config under the engine key
                - Common settings at top level

        Returns:
            An instance of a class implementing DatabaseInterface

        Raises:
            DatabaseError: If engine is not supported or config is invalid

        Example:
            config = {
                'engine': 'sqlite',
                'sqlite': {
                    'path': 'data/experiments.db',
                    'journal_mode': 'WAL'
                }
            }
            db = DatabaseFactory.create(config)
        """
        # Validate config
        if not isinstance(config, dict):
            raise DatabaseError(
                f"Invalid config type: expected dict, got {type(config)}"
            )

        # Get engine type
        engine = config.get("engine", "sqlite")
        if not engine:
            raise DatabaseError("No database engine specified in config")

        # Convert to lowercase for case-insensitive matching
        engine = engine.lower()

        # Check if adapter exists
        if engine not in cls._adapters:
            supported = ", ".join(cls._adapters.keys())
            raise DatabaseError(
                f"Unsupported database engine: '{engine}'. "
                f"Supported engines: {supported}"
            )

        # Get engine-specific config
        engine_config = config.get(engine, {})
        if not engine_config:
            print(f"⚠️ No specific config found for engine '{engine}', using defaults")

        # Merge with common settings
        # Common settings like backup_enabled, backup_interval_hours
        common_config = {
            k: v
            for k, v in config.items()
            if k not in ["engine", "sqlite", "postgresql", "mysql"]
        }

        # Combine engine config with common settings
        full_config = {**common_config, **engine_config}

        # Create and return adapter instance
        adapter_class = cls._adapters[engine]
        return adapter_class(full_config)

    @classmethod
    def register_adapter(cls, engine: str, adapter_class) -> None:
        """
        Register a new database adapter.

        This allows adding new database types without modifying the factory.

        Args:
            engine: Engine name (e.g., 'postgresql', 'mysql')
            adapter_class: Class implementing DatabaseInterface

        Example:
            from .postgres_adapter import PostgreSQLAdapter
            DatabaseFactory.register_adapter('postgresql', PostgreSQLAdapter)
        """
        cls._adapters[engine.lower()] = adapter_class

    @classmethod
    def list_supported_engines(cls) -> list:
        """Return list of supported database engines."""
        return list(cls._adapters.keys())
