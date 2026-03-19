#!/usr/bin/env python3
"""
================================================================================
SAMPLES REPOSITORY – Handles high-frequency sample insertion
================================================================================

PURPOSE:
    Contains logic for inserting high-frequency samples:
    - Energy samples (RAPL)
    - CPU samples (utilization, frequency)
    - Interrupt samples

WHY THIS EXISTS:
    - Separates sample insertion from other database operations
    - Handles large batch inserts efficiently
    - Part of splitting the god object manager.py

AUTHOR: Deepak Panigrahy
================================================================================
"""

from typing import Any, Dict, List

from ..base import DatabaseInterface


class SamplesRepository:
    """
    Repository for high-frequency samples.

    Handles insertion of time-series data from various sensors.
    """

    def __init__(self, db: DatabaseInterface):
        """
        Initialize with database adapter.

        Args:
            db: DatabaseInterface instance
        """
        self.db = db

    def insert_energy_samples(self, run_id: int, samples: List[Dict[str, Any]]) -> None:
        """
        Insert high-frequency RAPL samples.

        Args:
            run_id: Foreign key to runs table
            samples: List of sample dictionaries
        """
        if not samples:
            return

        query = """
            INSERT INTO energy_samples
            (run_id, timestamp_ns, pkg_energy_uj, core_energy_uj, uncore_energy_uj, dram_energy_uj)
            VALUES (?, ?, ?, ?, ?, ?)
        """

        # No transaction wrapper - caller manages transactions
        for s in samples:
            self.db.conn.execute(
                query,
                (
                    run_id,
                    s.get("timestamp_ns"),
                    s.get("pkg_energy_uj"),
                    s.get("core_energy_uj"),
                    s.get("uncore_energy_uj"),
                    s.get("dram_energy_uj"),
                ),
            )

    def insert_cpu_samples(self, run_id: int, samples: List[Dict[str, Any]]) -> None:
        """Insert CPU samples."""
        if not samples:
            return

        # No transaction wrapper - caller manages transactions
        for s in samples:
            self.db.conn.execute(
                """
                INSERT INTO cpu_samples
                (run_id, timestamp_ns, 
                 cpu_util_percent, cpu_busy_mhz, cpu_avg_mhz,
                 c1_residency, c2_residency, c3_residency, c6_residency, c7_residency,
                 pkg_c8_residency, pkg_c9_residency, pkg_c10_residency,
                 package_power, dram_power,
                 gpu_rc6,
                 package_temp, ipc,
                 extra_metrics_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    run_id,
                    s.get("timestamp_ns"),
                    s.get("cpu_util_percent"),
                    s.get("cpu_busy_mhz"),
                    s.get("cpu_avg_mhz"),
                    s.get("c1_residency"),
                    s.get("c2_residency"),
                    s.get("c3_residency"),
                    s.get("c6_residency"),
                    s.get("c7_residency"),
                    s.get("pkg_c8_residency"),
                    s.get("pkg_c9_residency"),
                    s.get("pkg_c10_residency"),
                    s.get("package_power"),
                    s.get("dram_power"),
                    s.get("gpu_rc6"),
                    s.get("package_temp"),
                    s.get("ipc"),
                    s.get("extra_metrics_json"),
                ),
            )

    def insert_interrupt_samples(
        self, run_id: int, samples: List[Dict[str, Any]]
    ) -> None:
        """
        Insert interrupt samples.

        Args:
            run_id: Foreign key to runs table
            samples: List of sample dictionaries
        """
        if not samples:
            return

        query = """
            INSERT INTO interrupt_samples
            (run_id, timestamp_ns, interrupts_per_sec)
            VALUES (?, ?, ?)
        """

        # No transaction wrapper - caller manages transactions
        for s in samples:
            self.db.conn.execute(
                query, (run_id, s.get("timestamp_ns"), s.get("interrupts_per_sec"))
            )
