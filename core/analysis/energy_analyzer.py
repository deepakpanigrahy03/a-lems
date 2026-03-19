#!/usr/bin/env python3
"""
================================================================================
ENERGY ANALYZER – Computes derived metrics from raw + baseline
================================================================================

Purpose:
    Takes raw energy measurements and computes workload energy,
    reasoning energy, and orchestration tax.

Why this exists:
    Raw measurements include idle energy. This module subtracts the baseline
    to get energy that actually belongs to your workload.

Simple math:
    workload = package - idle
    reasoning = core - idle_core
    tax = workload - reasoning

Author: Deepak Panigrahy
================================================================================
"""

import os
import sys
from pathlib import Path

# ============================================================================
# Fix Python path – add project root so 'core' module is found
# ============================================================================
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import logging
from typing import Optional

from core.models.baseline_measurement import BaselineMeasurement
from core.models.derived_energy_measurement import DerivedEnergyMeasurement
from core.models.raw_energy_measurement import RawEnergyMeasurement

logger = logging.getLogger(__name__)


class EnergyAnalyzer:
    """
    Computes derived metrics from raw measurements and baselines.

    This is where we calculate the orchestration tax:
        1. Subtract idle energy to get workload energy
        2. Subtract idle core energy to get reasoning energy
        3. Tax = workload - reasoning (overhead of coordination)
    """

    @staticmethod
    def compute(
        raw: RawEnergyMeasurement, baseline: Optional[BaselineMeasurement] = None
    ) -> DerivedEnergyMeasurement:
        """
        Compute derived metrics from raw measurement.

        Args:
            raw: RawEnergyMeasurement from Module 1
            baseline: Optional baseline for idle subtraction

        Returns:
            DerivedEnergyMeasurement with workload/reasoning/tax
        """
        # ====================================================================
        # Step 1: Basic energy calculations (always the same)
        # ====================================================================
        package_uj = raw.package_energy_uj  # Total CPU energy
        core_uj = raw.core_energy_uj  # Energy used by cores
        dram_uj = raw.dram_energy_uj or 0  # Memory energy (if available)

        # Uncore = package - core - dram (cache, memory controller, interconnect)
        if hasattr(raw, "uncore_uj") and raw.uncore_uj is not None:
            uncore_uj = raw.uncore_uj
        else:
            uncore_uj = max(0, package_uj - core_uj - dram_uj)

        # ====================================================================
        # Step 2: Subtract idle energy if baseline is available
        # ====================================================================
        idle_uj = 0
        idle_core_uj = 0
        idle_uncore_uj = 0
        baseline_id = None

        if baseline:
            # Use minimum baseline (2nd percentile) instead of mean
            min_energy = baseline.min_energy_uj(raw.duration_seconds)
            idle_uj = min_energy["package-0"]
            idle_core_uj = min_energy["core"]
            idle_uncore_uj = min_energy["uncore"]
            baseline_id = baseline.baseline_id

            print(
                f"🔍 DEBUG - Using MIN baseline: idle_uj={idle_uj/1e6:.3f}J, idle_core={idle_core_uj/1e6:.3f}J, idle_uncore={idle_uncore_uj/1e6:.3f}J"
            )

        # ====================================================================
        # Step 3: Calculate the three key metrics
        # ====================================================================
        # Workload energy = total energy - min baseline
        workload_uj = max(0, package_uj - idle_uj)

        # Reasoning energy = core energy - min core baseline
        reasoning_uj = max(0, core_uj - idle_core_uj)

        # Orchestration tax = uncore - min uncore baseline
        tax_uj = max(0, uncore_uj - idle_uncore_uj)

        # ====================================================================
        # Step 4: Get performance counters - handle both dict and object
        # ====================================================================
        instructions = 0
        cycles = 0
        cache_misses = 0
        cache_references = 0
        major_faults = 0
        minor_faults = 0
        page_faults = 0
        ipc = 0
        migrations = 0

        # Check if perf is a PerformanceCounters object or dict
        if hasattr(raw, "perf") and raw.perf is not None:
            perf_data = raw.perf

            # Handle PerformanceCounters object (has attributes)
            if hasattr(perf_data, "instructions_retired"):
                instructions = perf_data.instructions_retired
                cycles = perf_data.cpu_cycles
                cache_misses = perf_data.cache_misses
                cache_references = perf_data.cache_references
                major_faults = (
                    raw.perf.major_page_faults
                    if hasattr(raw.perf, "major_page_faults")
                    else 0
                )
                minor_faults = (
                    raw.perf.minor_page_faults
                    if hasattr(raw.perf, "minor_page_faults")
                    else 0
                )

                migrations = perf_data.thread_migrations
                ipc = perf_data.instructions_per_cycle()

            # Handle dictionary (has .get method)
            elif isinstance(perf_data, dict):
                instructions = perf_data.get("instructions", 0)
                cycles = perf_data.get("cycles", 0)
                cache_misses = perf_data.get("cache_misses", 0)
                cache_references = perf_data.get("cache_references", 0)
                major_faults = perf_data.get("major_page_faults", 0)
                minor_faults = perf_data.get("minor_page_faults", 0)

                migrations = perf_data.get("thread_migrations", 0)

                # Calculate IPC if cycles > 0
                if cycles > 0:
                    ipc = instructions / cycles

            page_faults = major_faults + minor_faults
            print(
                f"🔍 DEBUG - Page faults extracted: major={major_faults}, minor={minor_faults}, total={major_faults + minor_faults}"
            )

        # ====================================================================
        # Step 5: Get power states (C-states, frequencies) - handles both dict/object
        # ====================================================================
        # ====================================================================
        # Step 5: Get power states from turbostat data (where it actually is!)
        # ====================================================================
        c_states = {}
        freq = 0
        gpu_freq = 0
        gpu_rc6 = 0

        # Check if turbostat data exists (this is where the data is!)
        if hasattr(raw, "turbostat") and raw.turbostat:
            turbostat_data = raw.turbostat
            if isinstance(turbostat_data, dict):
                # If it has summary dict (from your simplified approach)
                if "summary" in turbostat_data:
                    summary = turbostat_data["summary"]

                    # Get frequency
                    freq = summary.get("frequency_mean", 0)

                    # Get C-states (keys like 'C1_mean', 'C2_mean', etc.)
                    for key, value in summary.items():
                        if key.endswith("_mean") and key[0] == "C" and key[1].isdigit():
                            c_state = key.replace("_mean", "")
                            c_states[c_state] = value

                    # Get temperature if available in summary
                    if "package_temp_mean" in summary:
                        # This will override thermal data if needed
                        pass

                # If it has the old format with power_state
                elif "power_state" in turbostat_data:
                    power = turbostat_data["power_state"]
                    if isinstance(power, dict):
                        c_states = power.get("c_state_residencies", {})
                        freqs = power.get("frequencies", {})
                        freq = next(iter(freqs.values())) if freqs else 0
                        gpu_freq = power.get("igpu_frequency_mhz", 0)
                        gpu_rc6 = power.get("igpu_rc6_percent", 0)

        # Fallback to power_state (kept for backward compatibility)
        elif hasattr(raw, "power_state") and raw.power_state:
            power = raw.power_state
            if isinstance(power, dict):
                c_states = power.get("c_state_residencies", {})
                freqs = power.get("frequencies", {})
                freq = next(iter(freqs.values())) if freqs else 0
                gpu_freq = power.get("igpu_frequency_mhz", 0)
                gpu_rc6 = power.get("igpu_rc6_percent", 0)

        # ====================================================================
        # Step 6: Get thermal data (temperatures)
        # ====================================================================
        if hasattr(raw, "thermal") and raw.thermal:
            thermal = raw.thermal

            if isinstance(thermal, dict):
                package_temp = thermal.get("package_celsius", 0)
                core_temps = thermal.get("core_temps", [])
            else:
                package_temp = getattr(thermal, "package_temperature_celsius", 0)
                core_temps = getattr(thermal, "core_temperatures_celsius", [])
        else:
            package_temp = 0
            core_temps = []

        # ====================================================================
        # Step 7: Get scheduler metrics (context switches, run queue)
        # ====================================================================
        if hasattr(raw, "scheduler_metrics") and raw.scheduler_metrics:
            sched = raw.scheduler_metrics

            if isinstance(sched, dict):
                voluntary = sched.get("voluntary_switches", 0)
                involuntary = sched.get("involuntary_switches", 0)
                run_queue = max(0, sched.get("runnable", 0))
                kernel = sched.get("system_time", 0)
                user = sched.get("user_time", 0)

            else:
                voluntary = getattr(sched, "voluntary_switches", 0)
                involuntary = getattr(sched, "involuntary_switches", 0)
                run_queue = max(0, getattr(sched, "run_queue_length", 0))
                kernel = getattr(sched, "kernel_time_ms", 0)
                user = getattr(sched, "user_time_ms", 0)

        else:
            voluntary = 0
            involuntary = 0
            run_queue = 0
            kernel = 0
            user = 0
            # migrations = 0
        # ====================================================================
        #  NEW Step 8: Extract MSR metrics (if available)
        # ====================================================================
        # Default values
        ring_bus_min_mhz = 0.0
        ring_bus_max_mhz = 0.0
        ring_bus_current_mhz = 0.0
        wakeup_latency_us = 0.0
        thermal_throttle = 0
        tsc_frequency_hz = 0
        c2_time_seconds = 0.0
        c3_time_seconds = 0.0
        c6_time_seconds = 0.0
        c7_time_seconds = 0.0
        c2_ticks = 0
        c3_ticks = 0
        c6_ticks = 0
        c7_ticks = 0
        thermal_during_experiment: int = 0  # 1 if throttling occurred DURING experiment
        thermal_now_active: int = 0  # 1 if throttling active at end
        thermal_since_boot: int = 0  # 1 if system has ever throttled since boot

        if hasattr(raw, "msr_metrics") and raw.msr_metrics:
            msr = raw.msr_metrics

            # Get baseline/dynamic structure from get_all_metrics()
            baseline_data = msr.get("baseline", {})
            dynamic_data = msr.get("dynamic", {})

            # Extract baseline values
            measurements = baseline_data.get("measurements", {})
            wakeup_latency_us = measurements.get("wakeup_latency_us", 0.0)

            # Extract dynamic values
            ring_bus_current_mhz = dynamic_data.get("ring_bus_frequency_mhz", 0.0)
            thermal_throttle = dynamic_data.get("thermal_throttle", 0)

            # ====================================================================
            # NEW: First check for per-run deltas (top level from get_all_metrics)
            # ====================================================================
            c2_time_seconds = msr.get("c2_time_seconds", 0.0)
            c3_time_seconds = msr.get("c3_time_seconds", 0.0)
            c6_time_seconds = msr.get("c6_time_seconds", 0.0)
            c7_time_seconds = msr.get("c7_time_seconds", 0.0)

            # ====================================================================
            # If deltas not found, fall back to cumulative averages
            # ====================================================================
            if c2_time_seconds == 0 and c3_time_seconds == 0:
                cstate_avgs = dynamic_data.get("cstate_averages", {})
                if cstate_avgs:
                    # Get raw ticks
                    raw_ticks = cstate_avgs.get("raw", {})
                    c2_ticks = raw_ticks.get("C2", 0)
                    c3_ticks = raw_ticks.get("C3", 0)
                    c6_ticks = raw_ticks.get("C6", 0)
                    c7_ticks = raw_ticks.get("C7", 0)

                    # Get seconds (cumulative)
                    seconds = cstate_avgs.get("seconds", {})
                    c2_time_seconds = seconds.get("C2", 0.0)
                    c3_time_seconds = seconds.get("C3", 0.0)
                    c6_time_seconds = seconds.get("C6", 0.0)
                    c7_time_seconds = seconds.get("C7", 0.0)

            # TSC frequency
            tsc_frequency_hz = msr.get("tsc_frequency_hz", 0)

        # ========== ADD THESE 2 LINES ==========
        print(
            f"🔴 ANALYZER_VALUE: c2={c2_time_seconds:.3f}s, c3={c3_time_seconds:.3f}s"
        )
        print(f"🔴 ANALYZER_KEYS: c2={c2_time_seconds}, c3={c3_time_seconds}")
        # ====================================================================
        # Step 9: Return everything in one clean object
        # ====================================================================
        return DerivedEnergyMeasurement(
            # Core energy fields
            measurement_id=raw.measurement_id,
            start_time=raw.start_time,
            end_time=raw.end_time,
            package_energy_uj=package_uj,
            core_energy_uj=core_uj,
            uncore_energy_uj=uncore_uj,
            idle_energy_uj=idle_uj,
            workload_energy_uj=workload_uj,
            reasoning_energy_uj=reasoning_uj,
            orchestration_tax_uj=tax_uj,
            duration_seconds=raw.duration_seconds,
            baseline_id=baseline_id,
            dram_energy_uj=dram_uj if dram_uj > 0 else None,
            # Performance counters (now from perf_dict)
            instructions=instructions,
            cycles=cycles,
            cache_misses=cache_misses,
            cache_references=cache_references,
            ipc=ipc,
            page_faults=page_faults,
            major_page_faults=major_faults,
            minor_page_faults=minor_faults,
            # Power states
            c_state_residencies=c_states,
            frequency_mhz=freq,
            gpu_frequency_mhz=gpu_freq,
            gpu_rc6_percent=gpu_rc6,
            # Thermal
            package_temp_celsius=package_temp,
            core_temps_celsius=core_temps if core_temps else None,
            # Scheduler
            context_switches_voluntary=voluntary,
            context_switches_involuntary=involuntary,
            run_queue_length=run_queue,
            kernel_time_ms=kernel,
            user_time_ms=user,
            thread_migrations=migrations,
            # ====================================================================
            # NEW: MSR Metrics (add these parameters)
            # ====================================================================
            ring_bus_min_mhz=ring_bus_min_mhz,
            ring_bus_max_mhz=ring_bus_max_mhz,
            ring_bus_current_mhz=ring_bus_current_mhz,
            wakeup_latency_us=wakeup_latency_us,
            thermal_throttle=thermal_throttle,
            tsc_frequency_hz=tsc_frequency_hz,
            c2_time_seconds=c2_time_seconds,
            c3_time_seconds=c3_time_seconds,
            c6_time_seconds=c6_time_seconds,
            c7_time_seconds=c7_time_seconds,
            c2_ticks=c2_ticks,
            c3_ticks=c3_ticks,
            c6_ticks=c6_ticks,
            c7_ticks=c7_ticks,
            # ====================================================================
            # NEW: Thermal derived metrics (add these lines)
            # ====================================================================
            thermal_during_experiment=thermal_during_experiment,
            thermal_now_active=thermal_now_active,
            thermal_since_boot=thermal_since_boot,
        )

    @staticmethod
    def compute_batch(raw_list, baseline=None):
        """Compute for multiple measurements."""
        return [EnergyAnalyzer.compute(r, baseline) for r in raw_list]
