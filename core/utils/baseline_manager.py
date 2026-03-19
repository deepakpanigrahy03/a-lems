#!/usr/bin/env python3
"""
================================================================================
BASELINE MANAGER – Handles storage and retrieval of baseline measurements
================================================================================

This module manages baseline data:
- Loading from cache
- Saving new baselines
- Getting the most recent baseline
- Database storage for experiment tracking

Author: Deepak Panigrahy
================================================================================
"""

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

# Fix Python path to find core modules
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.config_loader import ConfigLoader
from core.database.manager import DatabaseManager
from core.models.baseline_measurement import BaselineMeasurement
from core.readers.rapl_reader import RAPLReader
from core.utils.core_pinner import CorePinner
from core.utils.idle_baseline import measure_idle_baseline

logger = logging.getLogger(__name__)


class BaselineManager:
    """
    Manages baseline measurements – loading, saving, and retrieving.

    Baselines are stored in:
    1. `data/baselines/` as JSON files (backward compatibility)
    2. `idle_baselines` database table (for experiment tracking)
    """

    def __init__(self, base_dir: str = "data/baselines"):
        """
        Initialize baseline manager.

        Args:
            base_dir: Directory to store baseline JSON files
        """
        self.base_dir = Path(project_root) / base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # ====================================================================
        # NEW: Database connection for experiment tracking
        # ====================================================================
        self.config_loader = ConfigLoader()
        self.db_config = self.config_loader.get_db_config()
        self.db = DatabaseManager(self.db_config)

        logger.info(f"BaselineManager initialized with dir: {self.base_dir}")

    def save(self, baseline: BaselineMeasurement) -> str:
        """
        Save a baseline measurement to disk AND database.
        """
        print(f"🔍 DEBUG - save() method ENTERED for baseline {baseline.baseline_id}")
        print(f"🔍 DEBUG3 - save() entry - object ID: {id(baseline)}")
        print(f"🔍 DEBUG3 - save() entry - metadata: {baseline.metadata}")

        # Save to JSON file

        filename = f"{baseline.baseline_id}.json"
        filepath = self.base_dir / filename

        with open(filepath, "w") as f:
            json.dump(baseline.to_dict(), f, indent=2)

        logger.info(f"Saved baseline {baseline.baseline_id} to {filepath}")

        # Insert into database
        baseline_dict = baseline.to_dict()
        print(
            f"🔍 DEBUG - baseline_dict metadata before DB insert: {baseline_dict.get('metadata')}"
        )

        try:
            result = self.db.insert_baseline(baseline_dict)
            print(f"🔍 DEBUG - Insert result: {result}")
            logger.info(f"Saved baseline {baseline.baseline_id} to database")
        except Exception as e:
            print(f"🔍 DEBUG - Database insert EXCEPTION: {type(e).__name__}: {e}")
            import traceback

            traceback.print_exc()
            logger.warning(f"Failed to save baseline to database: {e}")

        return str(filepath)

    def load(self, baseline_id: str) -> Optional[BaselineMeasurement]:
        """
        Load a baseline by ID from JSON file.

        Args:
            baseline_id: Unique baseline identifier

        Returns:
            BaselineMeasurement or None if not found
        """
        filepath = self.base_dir / f"{baseline_id}.json"

        if not filepath.exists():
            logger.warning(f"Baseline {baseline_id} not found")
            return None

        try:
            with open(filepath, "r") as f:
                data = json.load(f)

            return BaselineMeasurement(
                baseline_id=data["baseline_id"],
                timestamp=data["timestamp"],
                power_watts=data["power_watts"],
                duration_seconds=data["duration_seconds"],
                sample_count=data["sample_count"],
                std_dev_watts=data.get("std_dev_watts", {}),
                cpu_temperature_c=data.get("cpu_temperature_c"),
                method=data.get("method", "loaded"),
                metadata=data.get("metadata", {}),
            )
        except Exception as e:
            logger.error(f"Error loading baseline {baseline_id}: {e}")
            return None

    def get_latest(self) -> Optional[BaselineMeasurement]:
        """
        Get the most recent baseline from database (preferred) or filesystem.

        Returns:
            Most recent BaselineMeasurement or None
        """
        # ====================================================================
        # NEW: Try database first
        # ====================================================================
        try:
            result = self.db.execute(
                "SELECT * FROM idle_baselines ORDER BY timestamp DESC LIMIT 1"
            )
            if result and len(result) > 0:
                row = result[0]

                power_watts = {
                    "package-0": row.get("package_power_watts", 0.0),
                    "core": row.get("core_power_watts", 0.0),
                    "uncore": row.get("uncore_power_watts", 0.0),
                    "dram": row.get("dram_power_watts", 0.0),
                }

                std_dev = {
                    "package-0": row.get("package_std", 0.0),
                    "core": row.get("core_std", 0.0),
                    "uncore": row.get("uncore_std", 0.0),
                    "dram": row.get("dram_std", 0.0),
                }

                return BaselineMeasurement(
                    baseline_id=row["baseline_id"],
                    timestamp=row["timestamp"],
                    power_watts=power_watts,
                    duration_seconds=row["duration_seconds"],
                    sample_count=row["sample_count"],
                    std_dev_watts=std_dev,
                    method=row.get("method", "database"),
                    metadata={
                        "governor": row.get("governor"),
                        "turbo": row.get("turbo"),
                        "background_cpu": row.get("background_cpu"),
                        "process_count": row.get("process_count"),
                    },
                )
        except Exception as e:
            logger.debug(f"No baseline in database: {e}")

        # ====================================================================
        # Fallback to filesystem (backward compatibility)
        # ====================================================================
        json_files = list(self.base_dir.glob("*.json"))
        if json_files:
            latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
            baseline_id = latest_file.stem
            return self.load(baseline_id)

        return None

    # ... rest of existing methods (load, list_baselines, measure_new) remain the same ...
