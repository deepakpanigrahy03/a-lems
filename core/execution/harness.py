#!/usr/bin/env python3
"""
================================================================================
EXPERIMENT HARNESS – Wraps AI execution with energy measurement
================================================================================

Purpose:
    This is the CRITICAL synchronization layer between energy measurement
    (Module 1) and AI execution (Module 3). It ensures perfect alignment
    between RAPL counters and code execution.

SCIENTIFIC NOTES:
    - Uses Module 1's 3‑layer architecture correctly:
        Layer 1: RawEnergyMeasurement (from EnergyEngine)
        Layer 2: BaselineMeasurement (idle power)
        Layer 3: DerivedEnergyMeasurement (computed metrics for analysis)
    - ALL THREE LAYERS are returned in results for complete transparency
    - Energy measurement wraps ENTIRE execution (not just API calls)
    - Warmup runs discard first execution to eliminate cache/initialization effects
    - Multiple repetitions (n>=30) for statistical power
    - Cool‑down period between linear and agentic ensures fair comparison
    - Network latency tracked separately for cloud models
    - CPU metrics come from Layer 3's performance counters

Why this exists:
    - Timestamps inside executors can drift from hardware counters
    - Even 50ms misalignment corrupts orchestration tax measurements
    - This harness guarantees perfect synchronization

Requirements:
    Req 3.6: Device Handoff Latency – precise timing alignment
    Req 1.46: High‑frequency sampling – must capture short agent steps
    Req 3.5: Cool‑down period between runs

Author: Deepak Panigrahy
================================================================================
"""

import json
import logging
import os
import socket
import subprocess
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import psutil
from scipy import stats as scipy_stats

from core.analysis.energy_analyzer import EnergyAnalyzer
from core.config_loader import ConfigLoader
from core.database.manager import DatabaseManager
from core.energy_engine import EnergyEngine
from core.execution.base import calc_stats
from core.execution.display_formatter import (display_hardware,
                                              display_ipc_analysis,
                                              display_sustainability_header,
                                              display_workflow_comparison)
from core.execution.hardware_collector import (_is_cold_start,
                                               _measure_network_latency,
                                               _read_interrupts,
                                               _read_temperature, _warmup_run,
                                               get_background_cpu,
                                               get_governor,
                                               get_interrupt_rate,
                                               get_process_count,
                                               get_process_memory,
                                               get_turbo_status)
from core.execution.sample_processor import (calculate_thermal_metrics,
                                             load_canonical_metrics,
                                             process_cpu_samples,
                                             process_energy_samples)
from core.sustainability.calculator import SustainabilityCalculator
from core.utils.baseline_manager import BaselineManager
from core.utils.debug import dprint

logger = logging.getLogger(__name__)


def get_workflow_order(rep_num):
    """Return (first, second) workflow order for repetition (1-based)."""
    if rep_num % 2 == 1:  # Odd: 1,3,5...
        return ("linear", "agentic")
    else:  # Even: 2,4,6...
        return ("agentic", "linear")


class ExperimentHarness:
    """
    Wraps AI execution with energy measurement for perfect synchronization.

    Critical design:
        energy_start()  ← RAPL starts recording
        executor.execute()  ← AI code runs
        energy_stop()   ← RAPL stops recording

    This ensures hardware counters align exactly with software execution.

    For cloud models, network latency is tracked separately to distinguish
    orchestration overhead from network delays.
    """

    def __init__(self, config_loader):
        """
        Initialize harness with energy measurement and analysis modules.

        Args:
            config_loader: Module 0 config loader
        """
        hw_config = config_loader.get_hardware_config()
        settings = config_loader.get_settings()

        # Convert settings to dict if needed
        if hasattr(settings, "__dict__"):
            settings_dict = settings.__dict__
        else:
            settings_dict = settings

        # Merge into single config dict for EnergyEngine
        engine_config = hw_config.copy()
        engine_config["settings"] = settings_dict

        self.energy_engine = EnergyEngine(engine_config)  # ← Now passing dict
        self.energy_analyzer = EnergyAnalyzer()
        self.sustainability = SustainabilityCalculator(
            config_loader
        )  # ← This stays as ConfigLoader

        # Load baseline if available (Layer 2)
        self.baseline_mgr = BaselineManager()
        self.baseline = self.baseline_mgr.get_latest()
        if self.baseline:
            logger.info(f"Loaded baseline: {self.baseline.baseline_id}")
        else:
            logger.warning("No baseline found. Will measure during experiment.")

        if not self.baseline:
            logger.warning("No baseline found. Run baseline measurement first.")
            dprint(
                "⚠️ No baseline – energy values will not be corrected for idle power"
            )

        logger.info("ExperimentHarness initialized")

    def save_to_database(
        self,
        results: Dict[str, Any],
        experiment_meta: Dict[str, Any],
        hardware_info: Optional[Dict] = None,
    ) -> Optional[int]:
        """
        Save experiment results to database.

        Args:
            results: Results from run_comparison
            experiment_meta: Experiment metadata
            hardware_info: Hardware configuration (optional)

        Returns:
            exp_id if successful, None otherwise
        """
        try:
            # Load database config
            config_loader = ConfigLoader()
            db_config = config_loader.get_db_config()

            # Create database manager
            db = DatabaseManager(db_config)

            # Ensure tables exist
            db.create_tables()

            # Get hardware ID if info provided
            hw_id = None
            if hardware_info:
                hw_id = db.insert_hardware(hardware_info)

            # Insert experiment
            exp_id = db.insert_experiment(experiment_meta)

            # Insert all runs (linear and agentic)
            all_runs = []
            if "all_runs" in results:
                # New format with all_runs dict
                all_runs.extend(results["all_runs"].get("linear", []))
                all_runs.extend(results["all_runs"].get("agentic", []))
            else:
                # Old format - single run
                all_runs.append(results)

            with db.transaction():
                for run in all_runs:
                    db.insert_run(exp_id, hw_id, run)

            # Create tax summaries
            db.create_tax_summaries(exp_id)

            db.close()
            logger.info(f"✅ Saved experiment {exp_id} to database")
            return exp_id

        except Exception as e:
            logger.error(f"❌ Failed to save to database: {e}")
            return None

    # Note: System-wide swap metrics are already in scheduler_monitor.py
    # This is handled by M3-6 (partial) - we only need RSS/VMS here
    def run_linear(
        self,
        executor,
        prompt: str,
        task_id: str = None,
        is_cloud: bool = True,
        country_code: str = "US",
        run_number: int = 1,
    ) -> Dict[str, Any]:
        """
        Run linear executor with synchronized energy measurement.

        This uses the 3‑layer architecture correctly and returns ALL THREE LAYERS:
            1. RawEnergyMeasurement from EnergyEngine (Layer 1)
            2. Baseline from BaselineManager (Layer 2)
            3. DerivedEnergyMeasurement from EnergyAnalyzer (Layer 3) ← USED FOR METRICS
        """
        dprint(f"\n{'='*70}")
        dprint(f"🔬 HARNESS: Starting LINEAR measurement")
        dprint(f"{'='*70}")
        self.energy_engine.scheduler.reset_interrupt_samples()
        # Measure network latency for cloud models
        network_metrics = {}
        if is_cloud:
            network_metrics = _measure_network_latency()
            dprint(f"📡 Network DNS: {network_metrics.get('dns_latency_ms', 0):.1f}ms")
        # ====================================================================
        # Capture system state BEFORE run (M3-1 through M3-6)
        # ====================================================================
        intr_before = _read_interrupts()
        temp_start = _read_temperature()
        governor = get_governor()
        turbo = get_turbo_status()
        background_cpu = get_background_cpu()
        process_count = get_process_count()
        memory_before = get_process_memory()
        is_cold = _is_cold_start(run_number)

        # ====================================================================
        # Step 1: Get Raw Energy Measurement (Layer 1)
        # ====================================================================
        run_start_dt = datetime.now()  # Human-readable start time
        run_start_perf = time.perf_counter()  # High-precision for duration
        self.energy_engine.start_measurement()
        exec_result = executor.execute(prompt)
        raw_energy = (
            self.energy_engine.stop_measurement()
        )  # RawEnergyMeasurement (Layer 1)
        if hasattr(raw_energy, 'perf'):
            dprint(f"🔍 PERF DEBUG - raw_energy.perf: {raw_energy.perf}")
            dprint(f"🔍 PERF DEBUG - minor_page_faults: {raw_energy.perf.minor_page_faults}")
            dprint(f"🔍 PERF DEBUG - major_page_faults: {raw_energy.perf.major_page_faults}")
        else:
            dprint(f"🔍 PERF DEBUG - raw_energy has no 'perf' attribute")
        run_end_dt = datetime.now()  # Human-readable end time
        run_end_perf = time.perf_counter()
        run_duration_sec = run_end_perf - run_start_perf

        dprint(
            f"🔍 DEBUG EXECUTION TIME - Linear compute: {exec_result.get('execution_time_ms', 0)} ms"
        )
        dprint(f"🔍 DEBUG TOTAL MEASUREMENT - Duration: {run_duration_sec*1000:.1f} ms")

        # ====================================================================
        # DEBUG: Check what's available in energy_engine
        # ====================================================================
        dprint(f"🔍 DEBUG - energy_engine attributes: {dir(self.energy_engine)}")
        if hasattr(self.energy_engine, "samples"):
            dprint(f"🔍 DEBUG - samples keys: {self.energy_engine.last_samples.keys()}")
        else:
            dprint("🔍 DEBUG - energy_engine has NO 'samples' attribute")

        # ====================================================================
        # Load canonical metrics from override file
        # ====================================================================
        from pathlib import Path

        import yaml

        canonical_metrics = {}
        store_extra = True

        override_path = Path("config/turbostat_override.yaml")
        if override_path.exists():
            try:
                with open(override_path, "r") as f:
                    override_config = yaml.safe_load(f)
                canonical_metrics = override_config.get("canonical_metrics", {})
                store_extra = override_config.get("store_extra_in_json", True)
                dprint(
                    f"📋 Loaded {len(canonical_metrics)} canonical metrics from override file"
                )
            except Exception as e:
                dprint(f"⚠️ Failed to load override file: {e}")

        # ====================================================================
        # Load canonical metrics from override file
        # ====================================================================
        from pathlib import Path

        import yaml

        canonical_metrics = {}
        store_extra = True

        override_path = Path("config/turbostat_override.yaml")
        if override_path.exists():
            try:
                with open(override_path, "r") as f:
                    override_config = yaml.safe_load(f)
                canonical_metrics = override_config.get("canonical_metrics", {})
                store_extra = override_config.get("store_extra_in_json", True)
                dprint(
                    f"📋 Loaded {len(canonical_metrics)} canonical metrics from override file"
                )
            except Exception as e:
                dprint(f"⚠️ Failed to load override file: {e}")

        # ====================================================================
        # Get samples from energy engine
        # ====================================================================
        energy_samples, interrupt_samples = process_energy_samples(self.energy_engine)
        cpu_samples = process_cpu_samples(raw_energy, canonical_metrics, store_extra)
        # Process thermal samples
        thermal_samples = []
        temps = []
        throttle_events = 0
        start_time_ns = int(run_start_dt.timestamp() * 1e9)

        if hasattr(raw_energy, "thermal_samples") and raw_energy.thermal_samples:
            for ts, readings, throttled in raw_energy.thermal_samples:
                time_since_start = (ts * 1e9 - start_time_ns) / 1e9
                thermal_samples.append(
                    {
                        "timestamp_ns": int(ts * 1e9),
                        "sample_time_s": time_since_start,
                        "cpu_temp": readings.get("cpu_temp"),
                        "system_temp": readings.get("system_temp"),
                        "wifi_temp": readings.get("wifi_temp"),
                        "throttle_event": 1 if throttled else 0,
                        "all_zones": readings,
                    }
                )
                if readings.get("cpu_temp"):
                    temps.append(readings["cpu_temp"])
                if throttled:
                    throttle_events += 1

        # Calculate thermal metrics
        thermal_area = 0
        if len(temps) > 1:
            # Approximate thermal area (integral approximation)
            thermal_area = sum(temps) / len(temps) * (max(temps) - min(temps))

        # ====================================================================
        # Calculate thermal metrics from CPU samples
        # ====================================================================
        start_temp_c, max_temp_c, min_temp_c, thermal_delta_c = (
            calculate_thermal_metrics(cpu_samples)
        )

        # ====================================================================
        # Step 2: Compute Derived Energy (Layer 3) using Baseline (Layer 2)
        # ====================================================================
        derived = self.energy_analyzer.compute(
            raw_energy, self.baseline
        )  # DerivedEnergyMeasurement (Layer 3)

        # ====================================================================
        # Step 3: Calculate sustainability (optional)
        # ====================================================================
        sustainability = self.sustainability.calculate_from_derived(
            derived, country_code=country_code, query_count=1
        )

        # ====================================================================
        # Step 4: Energy per token (using Layer 3)
        # ====================================================================
        if exec_result.get("tokens", {}).get("total", 0) > 0:
            energy_per_token = (
                derived.workload_energy_j / exec_result["tokens"]["total"]
            )
        else:
            energy_per_token = 0

        # ====================================================================
        # Step 5: CPU metrics from Layer 3's performance_counters
        # ====================================================================
        if hasattr(derived, "performance_counters") and derived.performance_counters:
            cpu_metrics = {
                "instructions": derived.performance_counters.instructions,
                "cycles": derived.performance_counters.cpu_cycles,
                "ipc": derived.performance_counters.instructions_per_cycle(),
                "cache_misses": derived.performance_counters.cache_misses,
                "context_switches": derived.performance_counters.total_context_switches(),
            }
        else:
            # Fallback if performance_counters not available
            cpu_metrics = {
                "instructions": 0,
                "cycles": 0,
                "ipc": 0,
                "cache_misses": 0,
                "context_switches": 0,
                "note": "Performance counters not available",
            }
        if hasattr(raw_energy, "scheduler_metrics") and raw_energy.scheduler_metrics:
            swap_metrics = raw_energy.scheduler_metrics.get("swap", {})
        else:
            swap_metrics = {}

        # Get scheduler metrics with start/end swap values
        scheduler_start = {}
        scheduler_end = {}
        if hasattr(raw_energy, "scheduler_start") and raw_energy.scheduler_start:
            scheduler_start = raw_energy.scheduler_start.get("swap", {})
        if hasattr(raw_energy, "scheduler_end") and raw_energy.scheduler_end:
            scheduler_end = raw_energy.scheduler_end.get("swap", {})

        # ====================================================================
        # Step 6: Return ALL THREE LAYERS with ML features
        # ====================================================================

        # ====================================================================
        # Capture system state AFTER run
        # ====================================================================
        intr_after = _read_interrupts()
        temp_max = max(temp_start, _read_temperature())
        memory_after = get_process_memory()
        duration = raw_energy.duration_seconds
        interrupt_rate = get_interrupt_rate(intr_before, intr_after, duration)

        dprint(f"🔍 LINEAR HARNESS - total_bytes_sent: {exec_result.get('total_bytes_sent', 'NOT FOUND')}")
        dprint(f"🔍 LINEAR HARNESS - total_bytes_recv: {exec_result.get('total_bytes_recv', 'NOT FOUND')}")
        dprint(f"🔍 LINEAR HARNESS - total_tcp_retransmits: {exec_result.get('total_tcp_retransmits', 'NOT FOUND')}")

        result = {
            "experiment_id": exec_result.get("experiment_id"),
            "task_id": task_id,
            "workflow": "linear",
            "country_code": country_code,
            "execution": exec_result,
             "pending_interactions": exec_result.get("pending_interactions", []),
            # THREE LAYERS – All available for analysis
            "layer1_raw": raw_energy.to_dict(),  # Raw hardware readings
            "layer2_baseline": (
                self.baseline.to_dict() if self.baseline else None
            ),  # Idle reference
            "layer3_derived": derived.to_dict(),  # Corrected metrics
            # Backward compatibility (keep old keys)
            "raw_energy": raw_energy.to_dict(),
            "derived_energy": derived.to_dict(),
            # Other metrics
            "sustainability": sustainability.to_dict() if sustainability else None,
            "network_metrics": network_metrics,
            "energy_per_token": energy_per_token,
            "cpu_metrics": cpu_metrics,
            # ====================================================================
            # NEW: ML Features Dictionary (ALL features for training)
            # ====================================================================
            "ml_features": {
                # Hardware metrics (from layer3_derived)
                "start_time_ns": int(run_start_dt.timestamp() * 1_000_000_000),
                "end_time_ns": int(run_end_dt.timestamp() * 1_000_000_000),
                "start_time_iso": run_start_dt.isoformat(),
                "end_time_iso": run_end_dt.isoformat(),
                "duration_sec": run_duration_sec,
                "duration_ms": run_duration_sec * 1000,
                "instructions": derived.instructions,
                "pkg_energy_uj": derived.package_energy_uj,
                "core_energy_uj": derived.core_energy_uj,
                "uncore_energy_uj": derived.uncore_energy_uj,
                "dram_energy_uj": (
                    derived.dram_energy_uj if hasattr(derived, "dram_energy_uj") else 0
                ),
                "idle_energy_uj": derived.idle_energy_uj,
                "orchestration_tax_uj": derived.orchestration_tax_uj,
                "cycles": derived.cycles,
                "ipc": derived.ipc,
                "cache_misses": derived.cache_misses,
                "cache_references": derived.cache_references,
                "cache_miss_rate": (
                    derived.cache_misses / derived.cache_references
                    if derived.cache_references > 0
                    else 0
                ),
                "page_faults": (
                     raw_energy.perf.minor_page_faults + raw_energy.perf.major_page_faults

                ),
                "major_page_faults": (
                    raw_energy.perf.major_page_faults
                ),
                "minor_page_faults": (
                    raw_energy.perf.minor_page_faults
                ),
                "context_switches_voluntary": derived.context_switches_voluntary,
                "context_switches_involuntary": derived.context_switches_involuntary,
                "total_context_switches": derived.total_context_switches,
                "thread_migrations": derived.thread_migrations,
                "run_queue_length": derived.run_queue_length,
                "kernel_time_ms": derived.kernel_time_ms,
                "user_time_ms": derived.user_time_ms,
                "frequency_mhz": cpu_metrics.get('cpu_avg_mhz', 0),
                "package_temp_celsius": derived.package_temp_celsius,
                "baseline_temp_celsius": (
                    self.baseline.cpu_temperature_c if self.baseline else None
                ),
                "thermal_metrics": {
                    "min_temp_c": min(temps) if temps else 0,
                    "max_temp_c": max(temps) if temps else 0,
                    "avg_temp_c": sum(temps) / len(temps) if temps else 0,
                    "thermal_area": thermal_area,
                    "throttle_events": throttle_events,
                    "throttle_ratio": (
                        throttle_events / len(thermal_samples) if thermal_samples else 0
                    ),
                },
                # C-state metrics
                "c2_time_seconds": derived.c2_time_seconds,
                "c3_time_seconds": derived.c3_time_seconds,
                "c6_time_seconds": derived.c6_time_seconds,
                "c7_time_seconds": derived.c7_time_seconds,
                # Swap metrics
                "swap_total_mb": swap_metrics.get("swap_total_mb"),
                "swap_end_free_mb": swap_metrics.get("swap_free_mb"),
                "swap_start_used_mb": scheduler_start.get("swap_used_mb"),
                "swap_end_used_mb": scheduler_end.get("swap_used_mb"),
                "swap_start_cached_mb": scheduler_start.get("swap_cached_mb"),
                "swap_end_cached_mb": scheduler_end.get("swap_cached_mb"),
                "swap_end_percent": swap_metrics.get("swap_percent"),
                # Ring bus
                "ring_bus_freq_mhz": derived.ring_bus_current_mhz,
                "wakeup_latency_us": derived.wakeup_latency_us,
                # Thermal validity
                "thermal_during_experiment": derived.thermal_during_experiment,
                "thermal_now_active": derived.thermal_now_active,
                "thermal_since_boot": derived.thermal_since_boot,
                "experiment_valid": (
                    derived.thermal_during_experiment == 0
                    and derived.thermal_now_active == 0
                ),
                # Token metrics (from execution)
                "total_tokens": exec_result.get("tokens", {}).get("total", 0),
                "prompt_tokens": exec_result.get("tokens", {}).get("prompt", 0),
                "completion_tokens": exec_result.get("tokens", {}).get("completion", 0),
                # Network metrics (for cloud models)
                "bytes_sent": exec_result.get("total_bytes_sent", 0),
                "bytes_recv": exec_result.get("total_bytes_recv", 0),
                "tcp_retransmits": exec_result.get("total_tcp_retransmits", 0),
                "total_non_local_ms": exec_result.get("total_workflow_non_local_ms", 0),
                "effective_throughput_kbps": exec_result.get("effective_throughput_kbps", 0),

                "dns_latency_ms": network_metrics.get("dns_latency_ms", 0),
                "api_latency_ms": exec_result.get("api_latency_ms", 0),
                "compute_time_ms": exec_result.get(
                    "compute_time_ms", exec_result.get("execution_time_ms", 0)
                ),
                # =============================================================
                # NEW: System State Metrics (ADD THESE)
                # =============================================================
                "governor": governor,
                "baseline_id": self.baseline.baseline_id if self.baseline else None,
                "turbo_enabled": 1 if turbo == "enabled" else 0,
                "interrupt_rate": interrupt_rate,
                "start_temp_c": start_temp_c,
                "max_temp_c": max_temp_c,
                "min_temp_c": min_temp_c,
                "thermal_delta_c": thermal_delta_c,
                "is_cold_start": 1 if is_cold else 0,
                "background_cpu_percent": background_cpu,
                "process_count": process_count,
                "rss_memory_mb": memory_after.get("rss_mb", 0),
                "vms_memory_mb": memory_after.get("vms_mb", 0),
                # Metadata
                "model_name": executor.config.get("model_id", "unknown"),
                "provider": executor.provider,
                "task_id": task_id,
                "country_code": country_code,
                "workflow_type": "linear",
                # ====================================================================
                # TARGETS (what we want to predict)
                # ====================================================================
                "energy_j": derived.workload_energy_j,
                "carbon_g": sustainability.carbon.grams if sustainability else 0,
                "duration_ms": derived.duration_seconds * 1000,
            },
            "energy_samples": energy_samples,
            "cpu_samples": cpu_samples,
            "interrupt_samples": interrupt_samples,
            "thermal_samples": thermal_samples,
            "harness_timestamp": datetime.now().isoformat(),
            "scientific_notes": {
                "measurement_scope": "client_side_orchestration_only",
                "layers": {
                    "layer1_raw": "RawEnergyMeasurement (archived)",
                    "layer2_baseline": self.baseline is not None,
                    "layer3_derived": "DerivedEnergyMeasurement (used for analysis)",
                },
                "includes": [
                    "cpu_energy",
                    "memory_energy",
                    "local_computation",
                    "performance_counters",
                ],
                "excludes": ["model_inference_on_cloud"] if is_cloud else [],
                "baseline_corrected": self.baseline is not None,
            },
        }

        # ====================================================================
        # Add high-frequency samples to result
        # ====================================================================
        if hasattr(self.energy_engine, "last_samples"):
            result["energy_samples"] = list(self.energy_engine.last_samples)
            dprint(
                f"📊 Added {len(self.energy_engine.last_samples)} energy samples to result"
            )
        else:
            dprint("⚠️ No last_samples attribute found in energy_engine")

        if hasattr(self.energy_engine, "last_interrupt_samples"):
            result["interrupt_samples"] = self.energy_engine.last_interrupt_samples
            dprint(
                f"📊 Added {len(self.energy_engine.last_interrupt_samples)} energy samples to result"
            )
        else:
            dprint("⚠️ No last_samples attribute found in energy_engine")

        dprint(f"✅ Harness complete: {derived.workload_energy_j:.4f}J workload energy")
        return result

    def run_agentic(
        self,
        executor,
        task: str,
        task_id: str = None,
        is_cloud: bool = True,
        country_code: str = "US",
        run_number: int = 1,
    ) -> Dict[str, Any]:
        """
        Run agentic executor with synchronized energy measurement.

        Same critical alignment as linear:
        Energy measurement wraps ENTIRE agentic pipeline.

        This captures ALL orchestration tax in one synchronized window:
        - Planning
        - Tool execution
        - Synthesis
        - Inter-step delays

        Uses the same 3‑layer architecture and returns ALL THREE LAYERS.
        """
        dprint(f"\n{'='*70}")
        dprint(f"🔬 HARNESS: Starting AGENTIC measurement")
        dprint(f"{'='*70}")
        self.energy_engine.scheduler.reset_interrupt_samples()
        # Measure network latency for cloud models
        network_metrics = {}
        if is_cloud:
            network_metrics = _measure_network_latency()
            dprint(f"📡 Network DNS: {network_metrics.get('dns_latency_ms', 0):.1f}ms")
        # ====================================================================
        # Capture system state BEFORE run (M3-1 through M3-6)
        # ====================================================================
        intr_before = _read_interrupts()
        temp_start = _read_temperature()
        governor = get_governor()
        turbo = get_turbo_status()
        background_cpu = get_background_cpu()
        process_count = get_process_count()
        memory_before = get_process_memory()
        is_cold = _is_cold_start(run_number)
        # ====================================================================
        # Step 1: Get Raw Energy Measurement (Layer 1)
        # ====================================================================
        run_start_dt = datetime.now()  # Human-readable start time
        run_start_perf = time.perf_counter()  # High-precision for duration
        self.energy_engine.start_measurement()
        exec_result = executor.execute_comparison(task)
        raw_energy = (
            self.energy_engine.stop_measurement()
        )  # RawEnergyMeasurement (Layer 1)
        if hasattr(raw_energy, 'perf'):
            dprint(f"🔍 PERF DEBUG - raw_energy.perf: {raw_energy.perf}")
            dprint(f"🔍 PERF DEBUG - minor_page_faults: {raw_energy.perf.minor_page_faults}")
            dprint(f"🔍 PERF DEBUG - major_page_faults: {raw_energy.perf.major_page_faults}")
        else:
            print(f"🔍 PERF DEBUG - raw_energy has no 'perf' attribute")

        run_end_dt = datetime.now()  # Human-readable end time
        run_end_perf = time.perf_counter()
        run_duration_sec = run_end_perf - run_start_perf
        # ====================================================================
        # DEBUG: Check what's available in energy_engine
        # ====================================================================
        dprint(f"🔍 DEBUG - energy_engine attributes: {dir(self.energy_engine)}")
        if hasattr(self.energy_engine, "last_samples"):
            dprint(f"🔍 DEBUG - samples keys: {self.energy_engine.last_samples}")
            if self.energy_engine.last_samples:
                dprint(
                    f"🔍 DEBUG - first sample type: {type(self.energy_engine.last_samples[0])}"
                )
                dprint(
                    f"🔍 DEBUG - first sample type: {type(self.energy_engine.last_samples[0])}"
                )
        else:
            dprint("🔍 DEBUG - energy_engine has NO 'samples' attribute")

        # ====================================================================
        # Load canonical metrics from override file
        # ====================================================================
        from pathlib import Path

        import yaml

        canonical_metrics = {}
        store_extra = True

        override_path = Path("config/turbostat_override.yaml")
        if override_path.exists():
            try:
                with open(override_path, "r") as f:
                    override_config = yaml.safe_load(f)
                canonical_metrics = override_config.get("canonical_metrics", {})
                store_extra = override_config.get("store_extra_in_json", True)
                dprint(
                    f"📋 Loaded {len(canonical_metrics)} canonical metrics from override file"
                )
            except Exception as e:
                dprint(f"⚠️ Failed to load override file: {e}")

        energy_samples, interrupt_samples = process_energy_samples(self.energy_engine)
        cpu_samples = process_cpu_samples(raw_energy, canonical_metrics, store_extra)

        # Process thermal samples
        thermal_samples = []
        temps = []
        throttle_events = 0
        start_time_ns = int(run_start_dt.timestamp() * 1e9)

        if hasattr(raw_energy, "thermal_samples") and raw_energy.thermal_samples:
            for ts, readings, throttled in raw_energy.thermal_samples:
                time_since_start = (ts * 1e9 - start_time_ns) / 1e9
                thermal_samples.append(
                    {
                        "timestamp_ns": int(ts * 1e9),
                        "sample_time_s": time_since_start,
                        "cpu_temp": readings.get("cpu_temp"),
                        "system_temp": readings.get("system_temp"),
                        "wifi_temp": readings.get("wifi_temp"),
                        "throttle_event": 1 if throttled else 0,
                        "all_zones": readings,
                    }
                )
                if readings.get("cpu_temp"):
                    temps.append(readings["cpu_temp"])
                if throttled:
                    throttle_events += 1

        # Calculate thermal metrics
        thermal_area = 0
        if len(temps) > 1:
            # Approximate thermal area (integral approximation)
            thermal_area = sum(temps) / len(temps) * (max(temps) - min(temps))
        # ====================================================================
        # Calculate thermal metrics from CPU samples
        # ====================================================================
        start_temp_c, max_temp_c, min_temp_c, thermal_delta_c = (
            calculate_thermal_metrics(cpu_samples)
        )

        # ====================================================================
        # Step 2: Compute Derived Energy (Layer 3) using Baseline (Layer 2)
        # ====================================================================
        derived = self.energy_analyzer.compute(
            raw_energy, self.baseline
        )  # DerivedEnergyMeasurement (Layer 3)

        # ====================================================================
        # Step 3: Calculate sustainability
        # ====================================================================
        sustainability = self.sustainability.calculate_from_derived(
            derived, country_code=country_code, query_count=1
        )

        # ====================================================================
        # Step 4: Energy per token
        # ====================================================================
        if exec_result.get("tokens", {}).get("total", 0) > 0:
            energy_per_token = (
                derived.workload_energy_j / exec_result["tokens"]["total"]
            )
        else:
            energy_per_token = 0

        # ====================================================================
        # Step 5: CPU metrics from Layer 3
        # ====================================================================
        if hasattr(derived, "performance_counters") and derived.performance_counters:
            cpu_metrics = {
                "instructions": derived.performance_counters.instructions,
                "cycles": derived.performance_counters.cpu_cycles,
                "ipc": derived.performance_counters.instructions_per_cycle(),
                "cache_misses": derived.performance_counters.cache_misses,
                "context_switches": derived.performance_counters.total_context_switches(),
            }
        else:
            cpu_metrics = {
                "instructions": 0,
                "cycles": 0,
                "ipc": 0,
                "cache_misses": 0,
                "context_switches": 0,
                "note": "Performance counters not available",
            }
        if hasattr(raw_energy, "scheduler_metrics") and raw_energy.scheduler_metrics:
            swap_metrics = raw_energy.scheduler_metrics.get("swap", {})
        else:
            swap_metrics = {}

        # Get scheduler metrics with start/end swap values
        scheduler_start = {}
        scheduler_end = {}
        if hasattr(raw_energy, "scheduler_start") and raw_energy.scheduler_start:
            scheduler_start = raw_energy.scheduler_start.get("swap", {})
        if hasattr(raw_energy, "scheduler_end") and raw_energy.scheduler_end:
            scheduler_end = raw_energy.scheduler_end.get("swap", {})

        # ====================================================================
        # Capture system state AFTER run
        # ====================================================================
        intr_after = _read_interrupts()
        temp_max = max(temp_start, _read_temperature())
        memory_after = get_process_memory()
        duration = raw_energy.duration_seconds
        interrupt_rate = get_interrupt_rate(intr_before, intr_after, duration)

        # ====================================================================
        # DEBUG: Check token data before building ml_features
        # ====================================================================
        dprint(f"🔍 DEBUG*** - agentic exec_result keys: {exec_result.keys()}")
        dprint(
            f"🔍 DEBUG ***- agentic tokens in exec_result: {exec_result.get('tokens', {})}"
        )
        dprint(
            f"🔍 DEBUG*** - agentic token total: {exec_result.get('tokens', {}).get('total')}"
        )
        dprint(
            f"🔍 DEBUG*** - agentic token prompt: {exec_result.get('tokens', {}).get('prompt')}"
        )
        dprint(
            f"🔍 DEBUG*** - agentic token completion: {exec_result.get('tokens', {}).get('completion')}"
        )
        # ====================================================================
        # NEW: Capture orchestration events from executor
        # ====================================================================
        orchestration_events = []
        if hasattr(executor, "_events") and executor._events:
            orchestration_events = (
                executor._events.copy()
            )  # Copy to prevent modification
            print(
                f"🔍 DEBUG - Captured {len(orchestration_events)} orchestration events from executor"
            )
            # Clear events to prevent mixing between runs
            executor._events = []
        else:
            print("🔍 DEBUG - No orchestration events found in executor")



        # ====================================================================
        # Step 6: Return ALL THREE LAYERS with ML features
        # ====================================================================
        result = {
            "experiment_id": exec_result.get("experiment_id"),
            "task_id": task_id,
            "workflow": "agentic",
            "country_code": country_code,
            "execution": exec_result,
            # THREE LAYERS – All available for analysis
            "layer1_raw": raw_energy.to_dict(),  # Raw hardware readings
            "layer2_baseline": (
                self.baseline.to_dict() if self.baseline else None
            ),  # Idle reference
            "layer3_derived": derived.to_dict(),  # Corrected metrics
            # Backward compatibility
            "raw_energy": raw_energy.to_dict(),
            "derived_energy": derived.to_dict(),
            # Other metrics
            "sustainability": sustainability.to_dict() if sustainability else None,
            "network_metrics": network_metrics,
            "energy_per_token": energy_per_token,
            "cpu_metrics": cpu_metrics,
            "orchestration_events": orchestration_events,
            "pending_interactions": exec_result.get("pending_interactions", []),
            # ====================================================================
            # NEW: ML Features Dictionary (ALL features for training)
            # ====================================================================
            "ml_features": {
                # Same hardware metrics as linear
                "start_time_ns": int(run_start_dt.timestamp() * 1_000_000_000),
                "end_time_ns": int(run_end_dt.timestamp() * 1_000_000_000),
                "start_time_iso": run_start_dt.isoformat(),
                "end_time_iso": run_end_dt.isoformat(),
                "duration_sec": run_duration_sec,
                "duration_ms": run_duration_sec * 1000,
                "instructions": derived.instructions,
                "pkg_energy_uj": derived.package_energy_uj,
                "core_energy_uj": derived.core_energy_uj,
                "uncore_energy_uj": derived.uncore_energy_uj,
                "dram_energy_uj": (
                    derived.dram_energy_uj if hasattr(derived, "dram_energy_uj") else 0
                ),
                "idle_energy_uj": derived.idle_energy_uj,
                "orchestration_tax_uj": derived.orchestration_tax_uj,
                "cycles": derived.cycles,
                "ipc": derived.ipc,
                "cache_misses": derived.cache_misses,
                "cache_references": derived.cache_references,
                "cache_miss_rate": (
                    derived.cache_misses / derived.cache_references
                    if derived.cache_references > 0
                    else 0
                ),
                "page_faults": (
                    raw_energy.perf.minor_page_faults + raw_energy.perf.major_page_faults
                ),
                "major_page_faults": (

                    raw_energy.perf.major_page_faults
                ),
                "minor_page_faults": (
                    raw_energy.perf.minor_page_faults
                ),
                "context_switches_voluntary": derived.context_switches_voluntary,
                "context_switches_voluntary": derived.context_switches_voluntary,
                "context_switches_involuntary": derived.context_switches_involuntary,
                "total_context_switches": derived.total_context_switches,
                "thread_migrations": derived.thread_migrations,
                "run_queue_length": derived.run_queue_length,
                "kernel_time_ms": derived.kernel_time_ms,
                "user_time_ms": derived.user_time_ms,
                "frequency_mhz": cpu_metrics.get('cpu_avg_mhz', 0),
                "package_temp_celsius": derived.package_temp_celsius,
                "baseline_temp_celsius": (
                    self.baseline.cpu_temperature_c if self.baseline else None
                ),
                "thermal_metrics": {
                    "min_temp_c": min(temps) if temps else 0,
                    "max_temp_c": max(temps) if temps else 0,
                    "avg_temp_c": sum(temps) / len(temps) if temps else 0,
                    "thermal_area": thermal_area,
                    "throttle_events": throttle_events,
                    "throttle_ratio": (
                        throttle_events / len(thermal_samples) if thermal_samples else 0
                    ),
                },
                # C-state metrics
                "c2_time_seconds": derived.c2_time_seconds,
                "c3_time_seconds": derived.c3_time_seconds,
                "c6_time_seconds": derived.c6_time_seconds,
                "c7_time_seconds": derived.c7_time_seconds,
                # Swap metrics
                # Swap metrics
                "swap_total_mb": swap_metrics.get("swap_total_mb"),
                "swap_end_free_mb": swap_metrics.get("swap_free_mb"),
                "swap_start_used_mb": scheduler_start.get("swap_used_mb"),
                "swap_end_used_mb": scheduler_end.get("swap_used_mb"),
                "swap_start_cached_mb": scheduler_start.get("swap_cached_mb"),
                "swap_end_cached_mb": scheduler_end.get("swap_cached_mb"),
                "swap_end_percent": swap_metrics.get("swap_percent"),
                # Ring bus
                "ring_bus_freq_mhz": derived.ring_bus_current_mhz,
                "wakeup_latency_us": derived.wakeup_latency_us,
                # Thermal validity
                "thermal_during_experiment": derived.thermal_during_experiment,
                "thermal_now_active": derived.thermal_now_active,
                "thermal_since_boot": derived.thermal_since_boot,
                "experiment_valid": (
                    derived.thermal_during_experiment == 0
                    and derived.thermal_now_active == 0
                ),
                # Token metrics
                "total_tokens": exec_result.get("tokens", {}).get("total", 0),
                "prompt_tokens": exec_result.get("tokens", {}).get("prompt", 0),
                "completion_tokens": exec_result.get("tokens", {}).get("completion", 0),
                "bytes_sent": exec_result.get("total_bytes_sent", 0),
                "bytes_recv": exec_result.get("total_bytes_recv", 0),
                "total_non_local_ms": exec_result.get("total_workflow_non_local_ms", 0),
                "effective_throughput_kbps": exec_result.get("effective_throughput_kbps", 0),
                "tcp_retransmits": exec_result.get("total_tcp_retransmits", 0),

                # ====================================================================
                # AGENTIC-SPECIFIC FEATURES
                # ====================================================================
                "planning_time_ms": exec_result.get("phase_times", {}).get(
                    "planning_ms", 0
                ),
                "execution_time_ms": exec_result.get("phase_times", {}).get(
                    "execution_ms", 0
                ),
                "synthesis_time_ms": exec_result.get("phase_times", {}).get(
                    "synthesis_ms", 0
                ),
                "phase_planning_ratio": (
                    exec_result.get("phase_ratios", {}).get("planning_ratio", 0)
                    if exec_result.get("phase_ratios")
                    else 0
                ),
                "phase_execution_ratio": (
                    exec_result.get("phase_ratios", {}).get("execution_ratio", 0)
                    if exec_result.get("phase_ratios")
                    else 0
                ),
                "phase_synthesis_ratio": (
                    exec_result.get("phase_ratios", {}).get("synthesis_ratio", 0)
                    if exec_result.get("phase_ratios")
                    else 0
                ),
                "llm_calls": exec_result.get("llm_calls", 1),
                "tool_calls": exec_result.get("tool_count", 0),
                "tools_used": len(exec_result.get("tools_used", [])),
                "steps": exec_result.get("steps", 1),
                "avg_step_time_ms": exec_result.get("avg_step_time_ms", 0),
                "orchestration_cpu_ms": exec_result.get("orchestration_cpu_ms", 0),
                "complexity_level": exec_result.get("complexity_level", 1),
                "complexity_score": exec_result.get("complexity_score", {}).get(
                    "raw_score", 0
                ),
                # Network
                "dns_latency_ms": network_metrics.get("dns_latency_ms", 0),
                "api_latency_ms": exec_result.get("api_latency_ms", 0),
                "compute_time_ms": exec_result.get(
                    "compute_time_ms", exec_result.get("total_time_ms", 0)
                ),
                # =============================================================
                # NEW: System State Metrics (ADD THESE)
                # =============================================================
                "governor": governor,
                "baseline_id": self.baseline.baseline_id if self.baseline else None,
                "turbo_enabled": 1 if turbo == "enabled" else 0,
                "interrupt_rate": interrupt_rate,
                "start_temp_c": start_temp_c,
                "max_temp_c": max_temp_c,
                "min_temp_c": min_temp_c,
                "thermal_delta_c": thermal_delta_c,
                "is_cold_start": 1 if is_cold else 0,
                "background_cpu_percent": background_cpu,
                "process_count": process_count,
                "rss_memory_mb": memory_after.get("rss_mb", 0),
                "vms_memory_mb": memory_after.get("vms_mb", 0),
                # Metadata
                "model_name": executor.config.get("model_id", "unknown"),
                "provider": executor.provider,
                "task_id": task_id,
                "country_code": country_code,
                "workflow_type": "agentic",
                # ====================================================================
                # TARGETS
                # ====================================================================
                "energy_j": derived.workload_energy_j,
                "orchestration_tax_j": derived.orchestration_tax_j,
                "carbon_g": sustainability.carbon.grams if sustainability else 0,
                "duration_ms": derived.duration_seconds * 1000,
            },
            "energy_samples": energy_samples,
            "cpu_samples": cpu_samples,
            "interrupt_samples": interrupt_samples,
            "thermal_samples": thermal_samples,
            "harness_timestamp": datetime.now().isoformat(),
            "scientific_notes": {
                "measurement_scope": "client_side_orchestration_only",
                "layers": {
                    "layer1_raw": "RawEnergyMeasurement (archived)",
                    "layer2_baseline": self.baseline is not None,
                    "layer3_derived": "DerivedEnergyMeasurement (used for analysis)",
                },
                "includes": [
                    "planning",
                    "tool_execution",
                    "synthesis",
                    "cpu_energy",
                    "performance_counters",
                ],
                "excludes": ["model_inference_on_cloud"] if is_cloud else [],
                "baseline_corrected": self.baseline is not None,
            },
        }

        # ====================================================================
        # Add high-frequency samples to result
        # ====================================================================
        if hasattr(self.energy_engine, "last_samples"):
            result["energy_samples"] = list(self.energy_engine.last_samples)
            dprint(
                f"📊 Added {len(self.energy_engine.last_samples)} energy samples to result"
            )
        else:
            dprint("⚠️ No last_samples attribute found in energy_engine")

        if hasattr(self.energy_engine, "last_interrupt_samples"):
            result["interrupt_samples"] = self.energy_engine.last_interrupt_samples
            dprint(
                f"📊 Added {len(self.energy_engine.last_interrupt_samples)} energy samples to result"
            )
        else:
            dprint("⚠️ No last_samples attribute found in energy_engine")

        # ====================================================================
        # DEBUG - Check orchestration events before returning
        # ====================================================================
        print(
            f"🔍 DEBUG HARNESS - orchestration_events in result: {'orchestration_events' in result}"
        )
        if "orchestration_events" in result:
            print(
                f"🔍 DEBUG HARNESS - Number of events: {len(result['orchestration_events'])}"
            )
            if len(result["orchestration_events"]) > 0:
                print(
                    f"🔍 DEBUG HARNESS - First event keys: {result['orchestration_events'][0].keys()}"
                )
        # Debug to check if thermal_samples is in result
        dprint(f"🔍 DEBUG - thermal_samples in result: {'thermal_samples' in result}")
        if "thermal_samples" in result:
            dprint(
                f"🔍 DEBUG - Number of thermal samples: {len(result['thermal_samples'])}"
            )
        print(f"✅ Harness complete: {derived.workload_energy_j:.4f}J workload energy")
        return result

    def run_comparison(
        self,
        linear_executor,
        agentic_executor,
        task: str,
        task_id: str = None,
        cool_down: Optional[int] = None,
        n_repetitions: int = 30,
        include_warmup: bool = True,
        country_code: str = "US",
        hardware_info: Optional[Dict] = None,
        save_to_db: bool = False,
    ) -> Dict[str, Any]:
        """
        Run multiple comparisons with warmup and statistical analysis.

        This is the publishable experimental protocol:
        1. Warmup run (discarded) to stabilize system
        2. N repetitions of (linear + cool‑down + agentic)
        3. Statistical analysis of all runs

        Returns results with ALL THREE LAYERS for each run.
        """
        dprint(f"\n{'#'*70}")
        dprint(f"📊 COMPARISON EXPERIMENT: Linear vs Agentic")
        dprint(f"   Task: {task[:100]}")
        dprint(f"   Repetitions: {n_repetitions}")
        dprint(f"{'#'*70}")

        # ====================================================================
        # Initialize results collection
        # ====================================================================
        self._collected_samples = {
            "energy_samples": [],  # Flat list (backward compatibility)
            "cpu_samples": [],  # Flat list
            "interrupt_samples": [],  # Flat list
            "energy_samples_by_run": [],  # NEW: list of lists
            "cpu_samples_by_run": [],  # NEW: list of lists
            "interrupt_samples_by_run": [],  # NEW: list of lists
        }

        # Determine if cloud model (check provider)
        is_cloud = linear_executor.provider != "ollama"

        # Set cool‑down period
        if cool_down is None:
            cool_down = self.config.get_settings().experiment.cool_down_seconds

        # Warmup run (optional, recommended)
        if include_warmup:
            dprint(f"\n🔥 Warmup phase (results discarded)")
            _warmup_run(linear_executor, task, is_agentic=False)
            time.sleep(cool_down)
            _warmup_run(agentic_executor, task, is_agentic=True)
            time.sleep(cool_down)

        # Storage for all runs
        all_linear = []
        all_agentic = []
        all_taxes = []

        for i in range(n_repetitions):
            dprint(f"\n{'─'*50}")
            dprint(f"📋 Repetition {i+1}/{n_repetitions}")
            dprint(f"{'─'*50}")

            # Determine workflow order for this repetition
            first_workflow, second_workflow = get_workflow_order(i + 1)

            # Execute first workflow
            if first_workflow == "linear":
                linear_result = self.run_linear(
                    linear_executor,
                    task,
                    task_id,
                    is_cloud,
                    country_code=country_code,
                    run_number=i + 1,
                )
                agentic_result = self.run_agentic(
                    agentic_executor,
                    task,
                    task_id,
                    is_cloud,
                    country_code=country_code,
                    run_number=i + 1,
                )
            else:
                agentic_result = self.run_agentic(
                    agentic_executor,
                    task,
                    task_id,
                    is_cloud,
                    country_code=country_code,
                    run_number=i + 1,
                )
                linear_result = self.run_linear(
                    linear_executor,
                    task,
                    task_id,
                    is_cloud,
                    country_code=country_code,
                    run_number=i + 1,
                )

            # Store results
            all_linear.append(linear_result)
            all_agentic.append(agentic_result)

            # Collect samples from both runs
            for result in [linear_result, agentic_result]:
                if "energy_samples" in result:
                    self._collected_samples["energy_samples"].extend(
                        result["energy_samples"]
                    )
                if "cpu_samples" in result:
                    self._collected_samples["cpu_samples"].extend(result["cpu_samples"])
                if "interrupt_samples" in result:
                    self._collected_samples["interrupt_samples"].extend(
                        result["interrupt_samples"]
                    )

            # ====================================================================
            # SAVE IMMEDIATELY if save_to_db is True
            # ====================================================================

            # Cool‑down between repetitions (except last)
            if i < n_repetitions - 1:
                dprint(f"⏳ Cool‑down: {cool_down}s")
                time.sleep(cool_down)

            # ====================================================================
            # YOUR EXISTING TAX CALCULATION CODE (PRESERVED)
            # ====================================================================
            ##temporary debug
            print(
                f"🔍 DEBUG: linear_result['layer3_derived'] keys = {linear_result['layer3_derived'].keys()}"
            )

            # Compute tax for this pair (using Layer 3 workload energy)
            linear_layer3 = linear_result["layer3_derived"]
            agentic_layer3 = agentic_result["layer3_derived"]

            # Debug to see actual structure
            print(f"🔍 DEBUG: linear_layer3 keys = {list(linear_layer3.keys())}")
            if "energy_uj" in linear_layer3:
                print(
                    f"🔍 DEBUG: energy_uj keys = {list(linear_layer3['energy_uj'].keys())}"
                )

            # Extract linear workload energy from the nested structure
            if (
                "energy_uj" in linear_layer3
                and "workload" in linear_layer3["energy_uj"]
            ):
                linear_energy = linear_layer3["energy_uj"]["workload"] / 1_000_000
            else:
                logger.warning(
                    f"Could not find workload energy. Keys: {list(linear_layer3.keys())}"
                )
                linear_energy = 0

            # Extract agentic workload energy
            if (
                "energy_uj" in agentic_layer3
                and "workload" in agentic_layer3["energy_uj"]
            ):
                agentic_energy = agentic_layer3["energy_uj"]["workload"] / 1_000_000
            else:
                logger.warning(
                    f"Could not find workload energy. Keys: {list(agentic_layer3.keys())}"
                )
                agentic_energy = 0

            tax = agentic_energy / linear_energy if linear_energy > 0 else 0
            all_taxes.append(tax)

            all_taxes.append(tax)

            # ====================================================================
            # Collect orchestration events from agentic run (with protection)
            # ====================================================================
            try:
                if agentic_result and "orchestration_events" in agentic_result:
                    if "orchestration_events_by_run" not in self._collected_samples:
                        self._collected_samples["orchestration_events_by_run"] = []
                    self._collected_samples["orchestration_events_by_run"].append(
                        agentic_result["orchestration_events"]
                    )
                    dprint(
                        f"   📝 Collected {len(agentic_result['orchestration_events'])} orchestration events"
                    )
                else:
                    dprint("   ⚠️ No orchestration events in agentic_result")
                    # Add empty list to maintain alignment (ADD THIS BACK)
                    if "orchestration_events_by_run" not in self._collected_samples:
                        self._collected_samples["orchestration_events_by_run"] = []
                    self._collected_samples["orchestration_events_by_run"].append([])
            except Exception as e:
                dprint(f"   ⚠️ Error collecting events: {e}")
                if "orchestration_events_by_run" not in self._collected_samples:
                    self._collected_samples["orchestration_events_by_run"] = []
                self._collected_samples["orchestration_events_by_run"].append([])

            # Cool‑down between repetitions (except last)
            if i < n_repetitions - 1:
                time.sleep(cool_down)
        # ====================================================================
        # Calculate heat flux for all runs (using baseline temperature)
        # ====================================================================
        # Get baseline temperature from the baseline measurement
        baseline_temp = None
        if self.baseline and hasattr(self.baseline, "cpu_temperature_c"):
            baseline_temp = self.baseline.cpu_temperature_c

        # Calculate heat flux for each run
        for run in all_linear + all_agentic:
            if "ml_features" in run:
                current_temp = run["ml_features"].get("package_temp_celsius")

                if current_temp and baseline_temp:
                    # Temperature rise from baseline
                    temp_rise = current_temp - baseline_temp
                    duration = (
                        run["ml_features"].get("duration_ms", 0) / 1000
                    )  # seconds

                    if duration > 0:
                        heat_flux = temp_rise / duration  # °C per second
                        run["ml_features"]["heat_flux"] = heat_flux
                        dprint(
                            f"🔥 Heat flux: {heat_flux:.2f}°C/s for run {run['ml_features'].get('run_number', '?')}"
                        )

        # ====================================================================
        # Build grouped samples in correct order (all linear first, then all agentic)
        # ====================================================================
        # Energy samples
        self._collected_samples["energy_samples_by_run"] = []
        for run in all_linear:
            if "energy_samples" in run:
                self._collected_samples["energy_samples_by_run"].append(
                    run["energy_samples"]
                )
        for run in all_agentic:
            if "energy_samples" in run:
                self._collected_samples["energy_samples_by_run"].append(
                    run["energy_samples"]
                )

        # CPU samples
        self._collected_samples["cpu_samples_by_run"] = []
        for run in all_linear:
            if "cpu_samples" in run:
                self._collected_samples["cpu_samples_by_run"].append(run["cpu_samples"])
        for run in all_agentic:
            if "cpu_samples" in run:
                self._collected_samples["cpu_samples_by_run"].append(run["cpu_samples"])

        # Interrupt samples
        self._collected_samples["interrupt_samples_by_run"] = []
        for run in all_linear:
            if "interrupt_samples" in run:
                self._collected_samples["interrupt_samples_by_run"].append(
                    run["interrupt_samples"]
                )
        for run in all_agentic:
            if "interrupt_samples" in run:
                self._collected_samples["interrupt_samples_by_run"].append(
                    run["interrupt_samples"]
                )

        dprint(
            f"📊 Grouped samples: energy={len(self._collected_samples['energy_samples_by_run'])}, "
            f"cpu={len(self._collected_samples['cpu_samples_by_run'])}, "
            f"interrupt={len(self._collected_samples['interrupt_samples_by_run'])}"
        )

        # ====================================================================
        # Statistical analysis
        # ====================================================================

        print(
            f"🔍 Agentic interrupt samples count: {len(agentic_result.get('interrupt_samples', []))}"
        )

        # Extract energy values from Layer 3 (DerivedEnergyMeasurement)

        linear_energies = [
            r["layer3_derived"]["energy_uj"]["workload"] / 1_000_000 for r in all_linear
        ]

        agentic_energies = [
            r["layer3_derived"]["energy_uj"]["workload"] / 1_000_000
            for r in all_agentic
        ]

        linear_times = [r["execution"]["execution_time_ms"] for r in all_linear]
        agentic_times = [r["execution"]["total_time_ms"] for r in all_agentic]

        # Energy per token from Layer 3
        linear_ept = [r["energy_per_token"] for r in all_linear]
        agentic_ept = [r["energy_per_token"] for r in all_agentic]

        # ====================================================================
        # Collect raw events from agentic runs (store temporarily)
        # ====================================================================
        raw_agentic_events = []
        for i, result in enumerate(all_agentic):
            if result and "orchestration_events" in result:
                raw_agentic_events.append(result["orchestration_events"])
                dprint(
                    f"🔍 DEBUG - Collected {len(result['orchestration_events'])} orchestration events from agentic run {i+1}"
                )
            else:
                raw_agentic_events.append([])
                dprint(f"🔍 DEBUG - No orchestration events in agentic run {i+1}")

        # ====================================================================
        # Build grouped samples after the loop
        # ====================================================================
        # Build energy_samples_by_run in order: [L1, L2, L3, A1, A2, A3]
        self._collected_samples["energy_samples_by_run"] = [
            run["energy_samples"] for run in all_linear if "energy_samples" in run
        ] + [run["energy_samples"] for run in all_agentic if "energy_samples" in run]

        self._collected_samples["cpu_samples_by_run"] = [
            run["cpu_samples"] for run in all_linear if "cpu_samples" in run
        ] + [run["cpu_samples"] for run in all_agentic if "cpu_samples" in run]

        self._collected_samples["interrupt_samples_by_run"] = [
            run["interrupt_samples"] for run in all_linear if "interrupt_samples" in run
        ] + [
            run["interrupt_samples"]
            for run in all_agentic
            if "interrupt_samples" in run
        ]

        # ====================================================================
        # Build orchestration events by run (AGENTIC ONLY)
        # ====================================================================
        # For orchestration events, we only have them for agentic runs
        # But we need to place them in the same order as runs:
        # [L1, L2, L3, A1, A2, A3] → So first n_repetitions entries are empty lists
        orchestration_events_by_run = []

        # Add empty lists for linear runs
        for _ in range(n_repetitions):
            orchestration_events_by_run.append([])

        # Add events for agentic runs (using raw_agentic_events collected above)
        orchestration_events_by_run.extend(raw_agentic_events)
        dprint(f"📊 Added {len(raw_agentic_events)} agentic event groups")

        results = {
            "task": task,
            "task_id": task_id,
            "n_repetitions": n_repetitions,
            "cool_down_seconds": cool_down,
            "is_cloud": is_cloud,
            "statistics": {
                "linear_energy_j": calc_stats(linear_energies),
                "agentic_energy_j": calc_stats(agentic_energies),
                "linear_time_ms": calc_stats(linear_times),
                "agentic_time_ms": calc_stats(agentic_times),
                "orchestration_tax": calc_stats(all_taxes),
                "linear_energy_per_token": calc_stats(linear_ept),
                "agentic_energy_per_token": calc_stats(agentic_ept),
            },
            "all_runs": {
                "linear": [r["experiment_id"] for r in all_linear],
                "agentic": [r["experiment_id"] for r in all_agentic],
                "taxes": all_taxes,
            },
            "scientific_notes": {
                "measurement_scope": "client_side_orchestration_only",
                "layers": {
                    "layer1_raw": "RawEnergyMeasurement (archived per run)",
                    "layer2_baseline": self.baseline is not None,
                    "layer3_derived": "DerivedEnergyMeasurement (used for analysis)",
                },
                "includes": [
                    "cpu_energy",
                    "memory_energy",
                    "local_computation",
                    "orchestration_overhead",
                ],
                "excludes": ["model_inference_on_cloud"] if is_cloud else [],
                "baseline_corrected": self.baseline is not None,
                "warmup_performed": include_warmup,
                "statistical_method": "Student's t-test, 95% CI",
            },
        }

        # ====================================================================
        # Add collected samples to results
        # ====================================================================
        if hasattr(self, "_collected_samples"):
            results["energy_samples"] = self._collected_samples.get(
                "energy_samples", []
            )
            results["cpu_samples"] = self._collected_samples.get("cpu_samples", [])
            results["interrupt_samples"] = self._collected_samples.get(
                "interrupt_samples", []
            )
            # NEW: Add per-run samples
            results["energy_samples_by_run"] = self._collected_samples.get(
                "energy_samples_by_run", []
            )
            results["cpu_samples_by_run"] = self._collected_samples.get(
                "cpu_samples_by_run", []
            )
            results["interrupt_samples_by_run"] = self._collected_samples.get(
                "interrupt_samples_by_run", []
            )
            results["orchestration_events_by_run"] = orchestration_events_by_run

            dprint(
                f"📊 Added samples to final results: energy={len(results['energy_samples'])}, "
                f"cpu={len(results['cpu_samples'])}, interrupt={len(results['interrupt_samples'])}"
            )

        # Print summary
        dprint(f"\n{'#'*70}")
        dprint(f"📊 EXPERIMENT SUMMARY")
        dprint(f"{'#'*70}")
        dprint(
            f"   Linear energy:   {results['statistics']['linear_energy_j']['mean']:.4f} ± {results['statistics']['linear_energy_j']['std']:.4f} J"
        )
        dprint(
            f"   Agentic energy:  {results['statistics']['agentic_energy_j']['mean']:.4f} ± {results['statistics']['agentic_energy_j']['std']:.4f} J"
        )
        dprint(
            f"   Orchestration tax: {results['statistics']['orchestration_tax']['mean']:.2f}x "
            f"[95% CI: {results['statistics']['orchestration_tax']['ci_lower']:.2f}, {results['statistics']['orchestration_tax']['ci_upper']:.2f}]"
        )
        dprint(
            f"   Energy per token: Linear={results['statistics']['linear_energy_per_token']['mean']:.6f} J/tok, Agentic={results['statistics']['agentic_energy_per_token']['mean']:.6f} J/tok"
        )
        dprint(f"{'#'*70}")
        # ====================================================================
        # Display ALL hardware parameters from Layer 1 (25 requirements)
        # ====================================================================
        print("\n" + "=" * 70)
        print("🔧 HARDWARE PARAMETERS DEEP DIVE (Layer 1 - All 25 Requirements)")
        print("=" * 70)

        # display_hardware(all_linear, "LINEAR")
        # display_hardware(all_agentic, "AGENTIC")
        for i in range(len(all_linear)):
            display_hardware([all_linear[i]], f"LINEAR Run {i+1}")
            if i < len(all_agentic):
                display_hardware([all_agentic[i]], f"AGENTIC Run {i+1}")

        display_ipc_analysis(all_linear, all_agentic)

        # ====================================================================
        # Helper function to calculate sustainability stats (ENHANCED with energy)
        # ====================================================================
        def calc_sustainability_stats(runs, label):
            """Calculate and display sustainability stats with energy for consistency check."""
            if not runs or not runs[0].get("sustainability"):
                return None

            carbon_total = 0
            water_total = 0
            methane_total = 0
            energy_total = 0  # ← NEW
            count = 0

            for run in runs:
                sus = run.get("sustainability", {})
                if sus:
                    if "carbon" in sus:
                        carbon_total += sus["carbon"].get("grams", 0)
                    if "water" in sus:
                        water_total += sus["water"].get("milliliters", 0)
                    if "methane" in sus:
                        methane_total += sus["methane"].get("grams", 0)

                    # ← NEW: Get energy from layer3_derived
                    derived = run.get("layer3_derived", {})
                    energy_uj = derived.get("energy_uj", {})
                    energy_total += energy_uj.get("workload", 0) / 1_000_000

                    count += 1

            if count > 0:
                carbon_mean = carbon_total / count
                energy_mean = energy_total / count

                print(f"\n   📊 {label} Workflow ({count} runs):")
                print(f"      Grid region: {runs[0].get('country_code', 'US')}")
                print(f"      Energy:  {energy_mean:.6f} J")  # ← NEW
                print(f"      Carbon:  {carbon_mean:.6f} g CO₂e")
                print(f"      Water:   {water_total/count:.6f} ml")
                print(f"      Methane: {methane_total/count:.6f} g CH₄")

            return {
                "energy": energy_total / count if count > 0 else 0,  # ← NEW
                "carbon": carbon_total / count if count > 0 else 0,
                "water": water_total / count if count > 0 else 0,
                "methane": methane_total / count if count > 0 else 0,
                "count": count,
            }

        # ====================================================================
        # SECTION 1: GRID FACTORS & SOURCES (Shown once)
        # ====================================================================

        display_sustainability_header(all_linear)
        # ====================================================================
        # SECTION 2: PER-WORKFLOW METRICS (Clean table format)
        # ====================================================================
        linear_stats = calc_sustainability_stats(all_linear, "LINEAR")
        agentic_stats = calc_sustainability_stats(all_agentic, "AGENTIC")

        display_workflow_comparison(linear_stats, agentic_stats)

        # ====================================================================
        # SECTION 3: DERIVED METRICS (Tax, Reasoning, Scarcity)
        # ====================================================================
        if linear_stats and agentic_stats:
            wait_tax_energy = agentic_stats["energy"] - linear_stats["energy"]
            reasoning_ratio = (
                (linear_stats["energy"] / agentic_stats["energy"]) * 100
                if agentic_stats["energy"] > 0
                else 0
            )

            print(f"\n   📈 DERIVED METRICS")
            print(f"   " + "-" * 50)
            print(f"   [2.8] Wait-Tax Per Query:")
            print(f"         Energy: {wait_tax_energy:.4f} J")
            print(
                f"         Carbon: {(agentic_stats['carbon'] - linear_stats['carbon'])*1000:.3f} mg"
            )

            print(f"\n   [2.11] Reasoning-to-Waste:")
            print(f"         Reasoning: {reasoning_ratio:.1f}%")
            print(f"         Waste:     {100-reasoning_ratio:.1f}%")

            # Energy Scarcity Index (Req 2.13)
            print(f"\n   [2.13] Energy Scarcity Index:")
            print(f"         Linear:  {linear_stats['energy']/3.6e6/10:.8f}")
            print(f"         Agentic: {agentic_stats['energy']/3.6e6/10:.8f}")

        # ====================================================================
        # SECTION 4: MODULE 3 EXECUTION METRICS
        # ====================================================================
        if all_agentic:
            print("\n   🤖 EXECUTION METRICS [Module 3]")
            print("   " + "-" * 50)

            total_llm = 0
            total_tools = 0
            total_steps = 0
            total_plan = 0
            total_exec = 0
            total_syn = 0
            complexity_sum = 0

            for run in all_agentic:
                exec_data = run.get("execution", {})
                total_llm += exec_data.get("llm_calls", 0)
                total_tools += exec_data.get("tool_count", 0)
                total_steps += exec_data.get("steps", 0)

                phase_times = exec_data.get("phase_times", {})
                total_plan += phase_times.get("planning_ms", 0)
                total_exec += phase_times.get("execution_ms", 0)
                total_syn += phase_times.get("synthesis_ms", 0)

                complexity = exec_data.get("complexity_score", {})
                if isinstance(complexity, dict):
                    complexity_sum += complexity.get("raw_score", 0)

            count = len(all_agentic)
            if count > 0:
                print(
                    f"\n      [3.2] Complexity Level: {agentic_stats.get('complexity_level', 1)}"
                )
                print(f"      [3.2] Complexity Score: {complexity_sum/count:.3f}")
                print(f"\n      [3.6] Phase Breakdown:")
                print(f"         Planning:  {total_plan/count:6.1f} ms")
                print(f"         Execution: {total_exec/count:6.1f} ms")
                print(f"         Synthesis: {total_syn/count:6.1f} ms")
                print(f"         {'─'*30}")
                print(
                    f"         TOTAL:     {(total_plan+total_exec+total_syn)/count:6.1f} ms"
                )

                print(f"\n      Workload Characteristics:")
                print(f"         LLM Calls:  {total_llm/count:.1f}")
                print(f"         Tool Calls: {total_tools/count:.1f}")
                print(f"         Steps:      {total_steps/count:.1f}")

        print("=" * 70)

        # ====================================================================
        # Create ML-ready dataset from all runs
        # ====================================================================
        ml_dataset = {"linear_runs": [], "agentic_runs": [], "all_runs": []}

        for i, run in enumerate(all_linear):
            if "ml_features" in run:
                run["ml_features"]["run_number"] = i + 1
                run["ml_features"]["experiment_id"] = run.get("experiment_id")
                ml_dataset["linear_runs"].append(run["ml_features"])
                ml_dataset["all_runs"].append(run["ml_features"])

        for i, run in enumerate(all_agentic):
            if "ml_features" in run:
                run["ml_features"]["run_number"] = i + 1
                run["ml_features"]["experiment_id"] = run.get("experiment_id")
                ml_dataset["agentic_runs"].append(run["ml_features"])
                ml_dataset["all_runs"].append(run["ml_features"])

        # Add to results
        results["ml_dataset"] = ml_dataset

        # Optional: Save to CSV for immediate use
        try:
            import pandas as pd

            df = pd.DataFrame(ml_dataset["all_runs"])
            csv_path = f"data/ml_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(csv_path, index=False)
            print(
                f"\n💾 ML dataset saved to: {csv_path} ({len(df)} runs, {len(df.columns)} features)"
            )
        except ImportError:
            print("\n⚠️ pandas not installed. Run: pip install pandas")
        except Exception as e:
            print(f"\n⚠️ Could not save CSV: {e}")

        if save_to_db:
            experiment_meta = {
                "name": f"{task_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "description": f"Task: {task[:100]}",
                "workflow_type": "comparison",
                "model_name": linear_executor.config.get("model_id", "unknown"),
                "provider": linear_executor.provider,
                "task_name": task_id or "custom",
                "country_code": country_code,
            }
            self.save_to_database(results, experiment_meta, hardware_info)

        return results
