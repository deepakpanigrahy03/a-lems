#!/usr/bin/env python3
"""
Simple ConfigLoader for testing
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

# Configure logging
logger = logging.getLogger(__name__)


class ConfigDict(dict):
    """A dictionary that also allows attribute access.

    This class enables both:
        dict-style: config['database']['path']
        object-style: config.database.path

    All nested dictionaries are automatically converted to ConfigDict.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self

    def __getattr__(self, name):
        """Allow attribute access to dictionary keys."""
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"'ConfigDict' object has no attribute '{name}'")


class ConfigLoader:
    """Simple config loader for testing LLM setup."""

    def __init__(self, config_dir: Optional[str] = None):
        """Initialize with config directory."""
        # Set configuration directory
        if config_dir is None:
            # __file__ is /home/dpani/mydrive/a-lems/core/config_loader.py
            # parent → /home/dpani/mydrive/a-lems/core/
            # parent.parent → /home/dpani/mydrive/a-lems/
            self.config_dir = Path(__file__).parent.parent / "config"
        else:
            self.config_dir = Path(config_dir)

        print(f"📁 Config directory: {self.config_dir}")

        # Load models config
        self._models_config = self._load_json("models.json")
        print(
            f"✅ Loaded models config: {list(self._models_config.keys()) if self._models_config else 'None'}"
        )

        # Load hardware config
        self._hardware_config = self._load_json("hw_config.json")
        print(
            f"✅ Loaded hardware config: {list(self._hardware_config.keys()) if self._hardware_config else 'None'}"
        )

        # ====================================================================
        # NEW: Load grid intensity config for sustainability calculator
        # ====================================================================
        self._grid_intensity = self._load_json("grid_intensity_2026.json")
        if self._grid_intensity:
            country_count = len(
                [k for k in self._grid_intensity.keys() if k != "metadata"]
            )
            print(f"✅ Loaded grid intensity data for {country_count} countries")
            # ===== ADD THIS =====
            print(f"🔍 RAW grid data keys: {list(self._grid_intensity.keys())}")
            if "IN" in self._grid_intensity:
                print(f"✅ India data found in config loader!")
                print(
                    f"   Carbon: {self._grid_intensity['IN'].get('carbon_intensity')}"
                )
            else:
                print(f"❌ India NOT found in config loader!")
            # ====================
        else:
            print("⚠️ No grid intensity data loaded")

    def _load_json(self, filename: str) -> Dict[str, Any]:
        """Load JSON file from config directory."""
        filepath = self.config_dir / filename
        print(f"📁 Loading {filepath}")

        if not filepath.exists():
            print(f"⚠️ Config file not found: {filepath}")
            return {}

        try:
            with open(filepath, "r") as f:
                data = json.load(f)
            print(f"✅ Successfully loaded {filename}")
            return data
        except Exception as e:
            print(f"⚠️ Error loading {filename}: {e}")
            return {}

    def get_hardware_config(self) -> Dict[str, Any]:
        """
        Get the hardware configuration.

        Returns:
            Dictionary containing hardware configuration with keys:
            - rapl: RAPL paths and domains
            - thermal: Thermal zone paths
            - msr: MSR device information
            - cpufreq: CPU frequency paths
            - cpu: CPU core information
            - turbostat: Turbostat column mappings
        """
        if self._hardware_config is None:
            logger.warning("Hardware config not loaded, returning empty dict")
            return {}

        # Return a copy of the dictionary
        return self._hardware_config.copy()

    def get_model_config(self, mode: str, workflow: str) -> Optional[Dict[str, Any]]:
        """
        Get model configuration for a specific mode and workflow.

        Args:
            mode: "local" or "cloud"
            workflow: "linear" or "agentic"

        Returns:
            Dictionary with model config or None if not found.
        """
        if self._models_config is None:
            print("⚠️ _models_config is None")
            return None

        mode_config = self._models_config.get(mode, {})
        if not mode_config:
            print(f"⚠️ Mode '{mode}' not found in config")
            return None

        workflow_config = mode_config.get(workflow)
        if not workflow_config:
            print(f"⚠️ Workflow '{workflow}' not found in mode '{mode}'")
            return None

        return workflow_config.copy()

    # ========================================================================
    # NEW METHOD: Get grid intensity data for sustainability calculator
    # ========================================================================
    def get_grid_intensity_data(self) -> Dict[str, Any]:
        """
        Load grid intensity data from JSON file.

        Returns:
            Dictionary with country codes as keys and grid factors as values,
            or empty dict if not available.
        """
        if self._grid_intensity is None:
            logger.warning("Grid intensity data not loaded, returning empty dict")
            return {}

        # Return a copy of the dictionary
        return self._grid_intensity.copy()

    # ========================================================================
    # NEW METHOD: Get grid factors for a specific country
    # ========================================================================
    def get_country_grid_factors(self, country_code: str) -> Optional[Dict[str, Any]]:
        """
        Get grid factors for a specific country.

        Args:
            country_code: ISO country code (e.g., "US", "IN", "FR")

        Returns:
            Dictionary with grid factors for the country, or None if not found.
        """
        if not self._grid_intensity:
            return None

        # Handle case-insensitive lookup
        country_code = country_code.upper()

        # Direct lookup
        if country_code in self._grid_intensity:
            return self._grid_intensity[country_code].copy()

        # Try to find in keys (some files might have lowercase)
        for code, data in self._grid_intensity.items():
            if code.upper() == country_code:
                return data.copy()

        logger.debug(f"Country {country_code} not found in grid intensity data")
        return None

    # ========================================================================
    # NEW METHOD: Get country metrics for ESI calculation
    # ========================================================================
    def get_country_metrics(self, country_code: str) -> Optional[Dict[str, Any]]:
        """
        Get country metrics (population, GDP, household energy) for a specific country.

        Args:
            country_code: ISO country code (e.g., "US", "IN", "FR")

        Returns:
            Dictionary with country metrics or None if not found.
        """
        try:
            # Try to load country_metrics.yaml
            import yaml

            metrics_path = self.config_dir / "country_metrics.yaml"

            if not metrics_path.exists():
                logger.warning(f"Country metrics file not found: {metrics_path}")
                return None

            with open(metrics_path, "r") as f:
                metrics_data = yaml.safe_load(f)

            countries = metrics_data.get("countries", {})

            # Handle case-insensitive lookup
            country_code = country_code.upper()

            # Direct lookup
            if country_code in countries:
                return countries[country_code].copy()

            # Try to find in keys
            for code, data in countries.items():
                if code.upper() == country_code:
                    return data.copy()

            logger.debug(f"Country {country_code} not found in country metrics")
            return None

        except ImportError:
            logger.error("PyYAML not installed. Run: pip install pyyaml")
            return None
        except Exception as e:
            logger.error(f"Failed to load country metrics: {e}")
            return None

    # ========================================================================
    # NEW METHOD: List all available countries in grid data
    # ========================================================================
    def list_available_countries(self) -> list:
        """Get list of all country codes available in grid intensity data."""
        if not self._grid_intensity:
            return []

        return [k for k in self._grid_intensity.keys() if k != "metadata"]

    def get_settings(self) -> ConfigDict:
        """
        Load settings from app_settings.yaml.

        Returns:
            ConfigDict that works with both:
            - Dict-style: settings['database']['path']
            - Object-style: settings.database.path
        """
        settings_path = self.config_dir / "app_settings.yaml"

        if not settings_path.exists():
            print(f"⚠️ Settings file not found: {settings_path}")
            return ConfigDict()

        try:
            import yaml

            with open(settings_path, "r") as f:
                data = yaml.safe_load(f)

            # Recursively convert nested dicts to ConfigDict
            return self._to_config_dict(data if data else {})

        except ImportError:
            print("⚠️ PyYAML not installed. Run: pip install pyyaml")
            return ConfigDict()
        except Exception as e:
            print(f"⚠️ Failed to load settings: {e}")
            return ConfigDict()

    def _to_config_dict(self, data):
        """Recursively convert dict to ConfigDict."""
        if isinstance(data, dict):
            return ConfigDict({k: self._to_config_dict(v) for k, v in data.items()})
        elif isinstance(data, list):
            return [self._to_config_dict(item) for item in data]
        else:
            return data

    def get_db_config(self) -> Dict[str, Any]:
        """
        Get database configuration from app_settings.yaml.

        Returns:
            Dictionary with database configuration including:
            - engine: 'sqlite' or 'postgresql'
            - engine-specific config under 'sqlite' or 'postgresql' keys
            - Common settings (backup_enabled, etc.)
        """
        settings = self.get_settings()

        # Handle both object and dict access patterns
        if hasattr(settings, "database"):
            # Object-style access (SimpleNamespace)
            db_config_obj = settings.database

            # Convert SimpleNamespace to dict
            if hasattr(db_config_obj, "__dict__"):
                db_config = db_config_obj.__dict__
            else:
                db_config = {}
                for key in dir(db_config_obj):
                    if not key.startswith("_"):
                        value = getattr(db_config_obj, key)
                        if not callable(value):
                            # If value is also SimpleNamespace, convert it
                            if hasattr(value, "__dict__"):
                                db_config[key] = value.__dict__
                            else:
                                db_config[key] = value
        else:
            # Dict-style access
            db_config = settings.get("database", {})

        # Ensure engine is set
        if "engine" not in db_config:
            db_config["engine"] = "sqlite"
            print("⚠️ No database engine specified, defaulting to 'sqlite'")

        # Set defaults for missing values
        if db_config["engine"] == "sqlite":
            # Handle both dict and SimpleNamespace for sqlite config
            sqlite_config = db_config.get("sqlite", {})
            if hasattr(sqlite_config, "__dict__"):
                # Convert SimpleNamespace to dict
                sqlite_config = sqlite_config.__dict__

            # Now ensure it's a dict and set defaults
            if not isinstance(sqlite_config, dict):
                sqlite_config = {}

            sqlite_config.setdefault("path", "data/experiments.db")
            sqlite_config.setdefault("journal_mode", "WAL")
            sqlite_config.setdefault("timeout", 30)
            db_config["sqlite"] = sqlite_config

        else:  # postgresql
            # Handle both dict and SimpleNamespace for postgresql config
            pg_config = db_config.get("postgresql", {})
            if hasattr(pg_config, "__dict__"):
                pg_config = pg_config.__dict__

            # Now ensure it's a dict and set defaults
            if not isinstance(pg_config, dict):
                pg_config = {}

            pg_config.setdefault("host", "localhost")
            pg_config.setdefault("port", 5432)
            pg_config.setdefault("database", "alems")
            pg_config.setdefault("user", "alems_user")
            pg_config.setdefault("password", "${DB_PASSWORD}")
            pg_config.setdefault("pool_size", 10)
            pg_config.setdefault("pool_timeout", 30)
            db_config["postgresql"] = pg_config

        # Common settings (ensure dict)
        if not isinstance(db_config, dict):
            db_config = {}

        db_config.setdefault("backup_enabled", True)
        db_config.setdefault("backup_interval_hours", 24)

        return db_config

    def sync_task_categories(self, db_connection):
        """Sync task categories from YAML to database using TaskLoader"""
        try:
            # Import here to avoid circular imports
            from core.utils.task_loader import load_tasks

            # Load tasks using existing task loader
            tasks = load_tasks()

            if not tasks:
                print("⚠️ No tasks loaded from YAML")
                return

            cursor = db_connection.cursor()

            # Clear existing
            cursor.execute("DELETE FROM task_categories")

            # Insert all tasks with categories
            count = 0
            for task in tasks:
                if "id" in task and "category" in task:
                    cursor.execute(
                        "INSERT INTO task_categories (task_id, category) VALUES (?, ?)",
                        (task["id"], task["category"]),
                    )
                    count += 1

            # Add fallback for custom queries
            cursor.execute(
                "INSERT OR IGNORE INTO task_categories (task_id, category) VALUES (?, ?)",
                ("custom_query", "custom"),
            )

            db_connection.commit()
            print(f"✅ Synced {count} task categories from YAML")

            # Optional: Show summary
            cursor.execute("""
                SELECT category, COUNT(*) FROM task_categories 
                GROUP BY category ORDER BY COUNT(*) DESC
            """)
            summary = cursor.fetchall()
            if summary:
                print("   Categories:")
                for cat, cnt in summary:
                    print(f"      • {cat}: {cnt}")

        except Exception as e:
            print(f"⚠️ Error syncing task categories: {e}")
            import traceback

            traceback.print_exc()
