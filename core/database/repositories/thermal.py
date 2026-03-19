"""
Thermal repository – Handles thermal sample storage.
"""

import json
from typing import Any, Dict, List


class ThermalRepository:
    """Repository for thermal samples."""

    def __init__(self, db):
        self.db = db

    def insert_thermal_samples(
        self, run_id: int, thermal_samples: List[Dict[str, Any]]
    ) -> None:
        """Bulk insert thermal samples for a run."""
        if not thermal_samples:
            return

        rows = []
        for sample in thermal_samples:
            all_zones = sample.get("all_zones", {})
            rows.append(
                (
                    run_id,
                    sample["timestamp_ns"],
                    sample.get("sample_time_s", 0),
                    all_zones.get("cpu_package"),
                    all_zones.get("system"),
                    all_zones.get("wifi"),
                    sample.get("throttle_event", 0),
                    json.dumps(sample.get("all_zones", {})),
                    len(sample.get("all_zones", {})),
                )
            )

        self.db.execute_many(
            """
            INSERT INTO thermal_samples (
                run_id, timestamp_ns, sample_time_s,
                cpu_temp, system_temp, wifi_temp,
                throttle_event, all_zones_json, sensor_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            rows,
        )
