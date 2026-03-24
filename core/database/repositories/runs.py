#!/usr/bin/env python3
"""
================================================================================
RUNS REPOSITORY – Handles insertion of run data and derived metrics
================================================================================

PURPOSE:
    Contains all logic for inserting runs into the database, including:
    - Extracting data from harness output
    - Computing derived metrics (energy_per_instruction, thermal_delta, etc.)
    - Computing cryptographic run state hash
    - Inserting the main run record

WHY THIS EXISTS:
    - Separates run insertion logic from other database operations
    - Makes the 80+ column insert manageable
    - Centralizes derived metric calculations
    - Part of splitting the god object manager.py

AUTHOR: Deepak Panigrahy
================================================================================
"""

import hashlib
import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional

from ..base import DatabaseError, DatabaseInterface


class RunsRepository:
    """
    Repository for run-related database operations.

    This class handles all insertions and queries related to runs,
    including the complex 80+ column run table.
    """

    def __init__(self, db: DatabaseInterface):
        """
        Initialize with database adapter.

        Args:
            db: DatabaseInterface instance (SQLiteAdapter, etc.)
        """
        self.db = db

    def _compute_run_state_hash(
        self, run_data: Dict[str, Any], hw_id: Optional[int], baseline_id: Optional[str]
    ) -> str:
        """
        Compute a cryptographic hash of the system state for reproducibility.

        The hash includes:
        - governor
        - turbo_enabled
        - hw_id (which encodes microcode, kernel, etc.)
        - baseline_id

        Args:
            run_data: Dictionary containing run fields (ml_features)
            hw_id: hardware_config primary key (or None)
            baseline_id: idle_baselines primary key (or None)

        Returns:
            SHA‑256 hex digest string
        """
        state_str = (
            f"{run_data.get('governor', '')}|"
            f"{run_data.get('turbo_enabled', '')}|"
            f"{hw_id}|"
            f"{baseline_id}"
        )
        return hashlib.sha256(state_str.encode()).hexdigest()

    def _extract_from_ml_features(self, ml: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract and compute all fields from ml_features dictionary.

        Args:
            ml: ml_features dictionary from harness output

        Returns:
            Dictionary with all fields ready for database insertion
        """
        # Energy values (convert J to µJ)
        total_energy_j = ml.get("energy_j", 0)
        total_energy_uj = int(total_energy_j * 1_000_000)

        # Performance counters
        instructions = ml.get("instructions", 0)
        cycles = ml.get("cycles", 0)
        total_tokens = ml.get("total_tokens", 0)

        # Derived metrics
        energy_per_instruction = (
            (total_energy_j / instructions) if instructions else None
        )
        energy_per_cycle = (total_energy_j / cycles) if cycles else None
        energy_per_token = (total_energy_j / total_tokens) if total_tokens else None
        instructions_per_token = (instructions / total_tokens) if total_tokens else None

        duration_sec = ml.get("duration_sec", 0)
        energy_j = ml.get("energy_j", 0)
        avg_power_watts = energy_j / duration_sec if duration_sec > 0 else 0

        # Thermal delta
        start_temp = ml.get("start_temp_c")
        max_temp = ml.get("max_temp_c")
        thermal_delta = (
            (max_temp - start_temp)
            if (start_temp is not None and max_temp is not None)
            else None
        )

        return {
            # Core fields
            "run_number": ml.get("run_number"),
            "duration_ms": ml.get("duration_ms", 0),
            "total_energy_j": total_energy_j,
            "total_energy_uj": total_energy_uj,
            'avg_power_watts': avg_power_watts,
            # Performance counters
            "instructions": instructions,
            "cycles": cycles,
            "ipc": ml.get("ipc", 0),
            "cache_misses": ml.get("cache_misses", 0),
            "cache_references": ml.get("cache_references", 0),
            "cache_miss_rate": ml.get("cache_miss_rate", 0),
            "page_faults": ml.get("page_faults", 0),
            "major_page_faults": ml.get("major_page_faults", 0),
            "minor_page_faults": ml.get("minor_page_faults", 0),
            # Scheduler
            "context_switches_voluntary": ml.get("context_switches_voluntary", 0),
            "context_switches_involuntary": ml.get("context_switches_involuntary", 0),
            "total_context_switches": ml.get("total_context_switches", 0),
            "thread_migrations": ml.get("thread_migrations", 0),
            "run_queue_length": ml.get("run_queue_length", 0),
            "kernel_time_ms": ml.get("kernel_time_ms", 0),
            "user_time_ms": ml.get("user_time_ms", 0),
            # Frequency
            "frequency_mhz": ml.get("frequency_mhz", 0),
            "ring_bus_freq_mhz": ml.get("ring_bus_freq_mhz", 0),
            # Thermal
            "package_temp_celsius": ml.get("package_temp_celsius"),
            "baseline_temp_celsius": ml.get("baseline_temp_celsius"),
            "start_temp_c": ml.get("start_temp_c", 0.0),
            "max_temp_c": ml.get("max_temp_c", 0.0),
            "min_temp_c": ml.get("min_temp_c", 0.0),
            "thermal_delta_c": ml.get("thermal_delta_c", 0.0),
            "thermal_during_experiment": ml.get("thermal_during_experiment", False),
            "thermal_now_active": ml.get("thermal_now_active", False),
            "thermal_since_boot": ml.get("thermal_since_boot", False),
            "experiment_valid": ml.get("experiment_valid", True),
            # C-states
            "c2_time_seconds": ml.get("c2_time_seconds", 0),
            "c3_time_seconds": ml.get("c3_time_seconds", 0),
            "c6_time_seconds": ml.get("c6_time_seconds", 0),
            "c7_time_seconds": ml.get("c7_time_seconds", 0),
            # ... swap fields ...
            "swap_total_mb": ml.get("swap_total_mb"),
            "swap_end_free_mb": ml.get("swap_end_free_mb"),
            "swap_start_used_mb": ml.get("swap_start_used_mb"),
            "swap_end_used_mb": ml.get("swap_end_used_mb"),
            "swap_start_cached_mb": ml.get("swap_start_cached_mb"),
            "swap_end_cached_mb": ml.get("swap_end_cached_mb"),
            "swap_end_percent": ml.get("swap_end_percent"),
            # MSR
            "wakeup_latency_us": ml.get("wakeup_latency_us", 0),
            "interrupt_rate": ml.get("interrupt_rate", 0),
            "thermal_throttle_flag": ml.get("thermal_throttle_flag", 0),
            # Memory
            "rss_memory_mb": ml.get("rss_memory_mb", 0),
            "vms_memory_mb": ml.get("vms_memory_mb", 0),
            # Tokens
            "total_tokens": total_tokens,
            "prompt_tokens": ml.get("prompt_tokens", 0),
            "completion_tokens": ml.get("completion_tokens", 0),
            # Network
            "dns_latency_ms": ml.get("dns_latency_ms", 0),
            "api_latency_ms": ml.get("api_latency_ms", 0),
            "compute_time_ms": ml.get("compute_time_ms", 0),
            # System state
            "baseline_id": ml.get("baseline_id"),
            "governor": ml.get("governor", "unknown"),
            "turbo_enabled": ml.get("turbo_enabled", False),
            "is_cold_start": ml.get("is_cold_start", False),
            "background_cpu_percent": ml.get("background_cpu_percent", 0),
            "process_count": ml.get("process_count", 0),
            # Agentic-specific
            "planning_time_ms": ml.get("planning_time_ms"),
            "execution_time_ms": ml.get("execution_time_ms"),
            "synthesis_time_ms": ml.get("synthesis_time_ms"),
            "phase_planning_ratio": ml.get("phase_planning_ratio"),
            "phase_execution_ratio": ml.get("phase_execution_ratio"),
            "phase_synthesis_ratio": ml.get("phase_synthesis_ratio"),
            "llm_calls": ml.get("llm_calls"),
            "tool_calls": ml.get("tool_calls"),
            "tools_used": ml.get("tools_used"),
            "steps": ml.get("steps"),
            "avg_step_time_ms": ml.get("avg_step_time_ms"),
            "complexity_level": ml.get("complexity_level"),
            "complexity_score": ml.get("complexity_score"),
            'orchestration_cpu_ms': ml.get('orchestration_cpu_ms', 0),
            # Derived metrics
            "energy_per_instruction": energy_per_instruction,
            "energy_per_cycle": energy_per_cycle,
            "energy_per_token": energy_per_token,
            "instructions_per_token": instructions_per_token,
            "interrupts_per_second": ml.get("interrupt_rate", 0),
        }

    def insert_run(
        self, exp_id: int, hw_id: Optional[int], run_data: Dict[str, Any]
    ) -> int:
        """
        Insert a single run with all its metrics.

        Args:
            exp_id: Foreign key to experiments table
            hw_id: Foreign key to hardware_config (or None)
            run_data: Dictionary containing all data for one run

        Returns:
            run_id of the newly inserted run
        """
        if not hasattr(self.db, "conn") or self.db.conn is None:
            self.db.connect()

        # Extract ml_features
        ml = run_data.get("ml_features", {})
        sus = run_data.get("sustainability", {})

        pkg_raw_uj = ml.get("pkg_energy_uj", 0)
        core_raw_uj = ml.get("core_energy_uj", 0)
        uncore_raw_uj = ml.get("uncore_energy_uj", 0)
        dram_raw_uj = ml.get("dram_energy_uj", 0)
        baseline_energy_uj = ml.get("idle_energy_uj", 0)

        print(
            f"🔍 RUNS DEBUG - pkg_raw_uj: {pkg_raw_uj}, baseline_energy_uj: {baseline_energy_uj}"
        )
        print(
            f"🔍 RUNS DEBUG - dynamic will be: {max(pkg_raw_uj - baseline_energy_uj, 0)}"
        )

        # Extract and compute fields
        fields = self._extract_from_ml_features(ml)

        # ====================================================================
        # Get timestamps from ml_features (captured per run during execution)
        # ====================================================================
        start_time_ns = ml.get("start_time_ns")
        end_time_ns = ml.get("end_time_ns")
        duration_ns = int(fields["duration_ms"] * 1_000_000)

        # Fallback to old method if per-run timestamps missing
        if start_time_ns is None or end_time_ns is None:
            harness_ts_str = run_data.get(
                "harness_timestamp", datetime.now().isoformat()
            )
            try:
                dt = datetime.fromisoformat(harness_ts_str)
                start_time_s = dt.timestamp()
            except Exception:
                start_time_s = datetime.now().timestamp()
            start_time_ns = int(start_time_s * 1_000_000_000)
            end_time_ns = start_time_ns + duration_ns

        # Sustainability metrics
        carbon_g = None
        water_ml = None
        methane_mg = None
        if sus:
            carbon_g = sus.get("carbon", {}).get("grams")
            water_ml = sus.get("water", {}).get("milliliters")
            methane_mg = sus.get("methane", {}).get("grams")

        # Run state hash
        baseline_id = run_data.get("baseline_id")
        if baseline_id is None and "ml_features" in run_data:
            baseline_id = run_data["ml_features"].get("baseline_id")
        run_state_hash = self._compute_run_state_hash(ml, hw_id, baseline_id)

        # Insert the run
        query = """
            INSERT INTO runs (
                exp_id, hw_id, baseline_id, run_number, workflow_type,
                start_time_ns, end_time_ns, duration_ns,
                total_energy_uj, dynamic_energy_uj, baseline_energy_uj, avg_power_watts,
                pkg_energy_uj, core_energy_uj, uncore_energy_uj, dram_energy_uj,
                instructions, cycles, ipc, cache_misses, cache_references, cache_miss_rate,
                page_faults, major_page_faults, minor_page_faults,
                context_switches_voluntary, context_switches_involuntary, total_context_switches,
                thread_migrations, run_queue_length, kernel_time_ms, user_time_ms,
                frequency_mhz, ring_bus_freq_mhz,
                package_temp_celsius, baseline_temp_celsius, start_temp_c, max_temp_c, min_temp_c, thermal_delta_c,
                thermal_during_experiment, thermal_now_active, thermal_since_boot, experiment_valid,
                c2_time_seconds, c3_time_seconds, c6_time_seconds, c7_time_seconds,
                swap_total_mb, swap_end_free_mb, swap_start_used_mb,
                swap_end_used_mb, swap_start_cached_mb, swap_end_cached_mb, swap_end_percent,
                wakeup_latency_us, interrupt_rate, thermal_throttle_flag,
                rss_memory_mb, vms_memory_mb,
                total_tokens, prompt_tokens, completion_tokens,
                dns_latency_ms, api_latency_ms, compute_time_ms,
                governor, turbo_enabled, is_cold_start, background_cpu_percent, process_count,
                planning_time_ms, execution_time_ms, synthesis_time_ms,
                phase_planning_ratio, phase_execution_ratio, phase_synthesis_ratio,
                llm_calls, tool_calls, tools_used, steps, avg_step_time_ms, orchestration_cpu_ms,
                complexity_level, complexity_score,
                carbon_g, water_ml, methane_mg,
                energy_per_instruction, energy_per_cycle, energy_per_token,
                instructions_per_token, interrupts_per_second,bytes_sent, bytes_recv, tcp_retransmits,
                run_state_hash
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, 
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, 
                ?, ?, ?, 
                ?, ?, ?, 
                ?, ?, ?, ?, ?,
                ? , ?
            )
        """

        params = (
            exp_id,
            hw_id,
            baseline_id,
            fields["run_number"],
            ml.get("workflow_type", "unknown"),
            start_time_ns,
            end_time_ns,
            duration_ns,
            pkg_raw_uj,
            max(pkg_raw_uj - baseline_energy_uj, 0),
            baseline_energy_uj,
            fields.get("avg_power_watts", 0),
            pkg_raw_uj,
            core_raw_uj,
            uncore_raw_uj,
            dram_raw_uj,
            fields["instructions"],
            fields["cycles"],
            fields["ipc"],
            fields["cache_misses"],
            fields["cache_references"],
            fields["cache_miss_rate"],
            fields["page_faults"],
            fields["major_page_faults"],
            fields["minor_page_faults"],
            fields["context_switches_voluntary"],
            fields["context_switches_involuntary"],
            fields["total_context_switches"],
            fields["thread_migrations"],
            fields["run_queue_length"],
            fields["kernel_time_ms"],
            fields["user_time_ms"],
            fields["frequency_mhz"],
            fields["ring_bus_freq_mhz"],
            fields["package_temp_celsius"],
            fields["baseline_temp_celsius"],
            fields["start_temp_c"],
            fields["max_temp_c"],
            fields["min_temp_c"],
            fields["thermal_delta_c"],
            fields["thermal_during_experiment"],
            fields["thermal_now_active"],
            fields["thermal_since_boot"],
            fields["experiment_valid"],
            fields["c2_time_seconds"],
            fields["c3_time_seconds"],
            fields["c6_time_seconds"],
            fields["c7_time_seconds"],
            fields.get("swap_total_mb"),
            fields.get("swap_end_free_mb"),
            fields.get("swap_start_used_mb"),
            fields.get("swap_end_used_mb"),
            fields.get("swap_start_cached_mb"),
            fields.get("swap_end_cached_mb"),
            fields.get("swap_end_percent"),
            fields["wakeup_latency_us"],
            fields["interrupt_rate"],
            fields["thermal_throttle_flag"],
            fields["rss_memory_mb"],
            fields["vms_memory_mb"],
            fields["total_tokens"],
            fields["prompt_tokens"],
            fields["completion_tokens"],
            fields["dns_latency_ms"],
            fields["api_latency_ms"],
            fields["compute_time_ms"],
            fields["governor"],
            fields["turbo_enabled"],
            fields["is_cold_start"],
            fields["background_cpu_percent"],
            fields["process_count"],
            fields["planning_time_ms"],
            fields["execution_time_ms"],
            fields["synthesis_time_ms"],
            fields["phase_planning_ratio"],
            fields["phase_execution_ratio"],
            fields["phase_synthesis_ratio"],
            fields["llm_calls"],
            fields["tool_calls"],
            fields["tools_used"],
            fields["steps"],
            fields["avg_step_time_ms"],
            fields["orchestration_cpu_ms"],
            fields["complexity_level"],
            fields["complexity_score"],
            carbon_g,
            water_ml,
            methane_mg,
            fields["energy_per_instruction"],
            fields["energy_per_cycle"],
            fields["energy_per_token"],
            fields["instructions_per_token"],
            fields["interrupts_per_second"],
            ml.get("bytes_sent", 0),
            ml.get("bytes_recv", 0),
            ml.get("tcp_retransmits", 0),
            run_state_hash,
        )

        try:
            # Remove the transaction wrapper - let the caller control transactions
            cursor = self.db.conn.execute(query, params)
            return cursor.lastrowid
        except sqlite3.IntegrityError as e:
            # Print detailed debug info
            print(f"\n❌ FOREIGN KEY ERROR: {e}")
            print("\n🔍 Foreign key values:")
            print(f"   exp_id: {exp_id} (type: {type(exp_id)})")
            print(f"   hw_id: {hw_id} (type: {type(hw_id)})")
            print(f"   baseline_id: {baseline_id} (type: {type(baseline_id)})")

            # Verify each foreign key exists
            try:
                exp_check = self.db.execute(
                    "SELECT COUNT(*) as count FROM experiments WHERE exp_id = ?",
                    (exp_id,),
                )
                print(
                    f"   experiments count for exp_id {exp_id}: {exp_check[0]['count']}"
                )
            except Exception as ex:
                print(f"   ❌ Failed to check experiments: {ex}")

            try:
                hw_check = self.db.execute(
                    "SELECT COUNT(*) as count FROM hardware_config WHERE hw_id = ?",
                    (hw_id,),
                )
                print(
                    f"   hardware_config count for hw_id {hw_id}: {hw_check[0]['count']}"
                )
            except Exception as ex:
                print(f"   ❌ Failed to check hardware: {ex}")

            if baseline_id:
                try:
                    bl_check = self.db.execute(
                        "SELECT COUNT(*) as count FROM idle_baselines WHERE baseline_id = ?",
                        (baseline_id,),
                    )
                    print(
                        f"   idle_baselines count for baseline_id {baseline_id}: {bl_check[0]['count']}"
                    )
                except Exception as ex:
                    print(f"   ❌ Failed to check baseline: {ex}")
            else:
                print(f"   baseline_id is None (allowed)")

            # Re-raise the original error
            raise

    def update_run_stats(self, run_id: int, stats: Dict) -> None:
        """Update run with aggregated statistics from samples."""
        self.db.execute(
            """
            UPDATE runs SET
                cpu_busy_mhz = ?,
                cpu_avg_mhz = ?,
                frequency_mhz = ?,
                package_temp_celsius = ?,
                max_temp_c = ?,
                min_temp_c = ?,
                interrupt_rate = ?
            WHERE run_id = ?
        """,
            (
                stats.get("cpu_busy_mhz", 0),
                stats.get("cpu_avg_mhz", 0),
                stats.get("cpu_avg_mhz", 0),
                stats.get("package_temp_celsius", 0),
                stats.get("max_temp_c", 0),
                stats.get("min_temp_c", 0),
                stats.get("interrupt_rate", 0),
                run_id,
            ),
        )
