#!/usr/bin/env python3
"""
================================================================================
DERIVED ENERGY MEASUREMENT – Layer 3: Analysis Results
================================================================================

This class represents computed/derived metrics from raw data and baseline.
NEVER stored permanently – computed on demand when needed.

Includes ALL 25 hardware parameters from Module 1:
- RAPL energy (package, core, dram, uncore)
- Performance counters (instructions, cycles, cache, context switches)
- Power states (C-state residencies, frequencies)
- Thermal data (temperatures)
- Scheduler metrics (run queue, context switches)

Author: Deepak Panigrahy
================================================================================
"""

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class DerivedEnergyMeasurement:
    """
    Layer 3 – Computed on demand, never stored permanently.

    Contains ALL 25 hardware parameters from Module 1 for complete analysis.

    Energy Attributes:
        measurement_id: Reference to raw measurement
        baseline_id: Optional reference to baseline used

        package_energy_uj: Total package energy (raw)
        core_energy_uj: Core energy (raw)
        dram_energy_uj: DRAM energy if available (raw)

        uncore_energy_uj: Uncore waste (package - core - dram)
        idle_energy_uj: Estimated idle energy from baseline
        workload_energy_uj: Energy attributed to workload (package - idle)
        reasoning_energy_uj: Energy for actual computation (core - idle_core)
        orchestration_tax_uj: Energy for overhead (uncore + extra)

    Performance Counters (Req 1.5, 1.6, 1.10):
        instructions: int = 0                  # Instructions retired
        cycles: int = 0                         # CPU cycles
        cache_misses: int = 0                    # LLC cache misses
        cache_references: int = 0                 # LLC references
        ipc: float = 0.0                          # Instructions per cycle
        page_faults: int = 0                       # Major + minor page faults

    Power States (Req 1.7, 1.41, 1.8):
        c_state_residencies: Dict[str, float] = None  # C1%, C6%, C7%, etc.
        frequency_mhz: float = 0.0                     # CPU frequency
        gpu_frequency_mhz: float = 0.0                  # iGPU frequency
        gpu_rc6_percent: float = 0.0                     # iGPU RC6 residency

    Thermal (Req 1.9):
        package_temp_celsius: float = 0.0                # Package temperature
        core_temps_celsius: List[float] = None           # Per-core temperatures

    Scheduler (Req 1.12, 1.23, 1.36):
        context_switches_voluntary: int = 0              # Voluntary context switches
        context_switches_involuntary: int = 0            # Involuntary context switches
        run_queue_length: float = 0.0                     # Run queue length
        kernel_time_ms: float = 0.0                       # Time in kernel mode
        user_time_ms: float = 0.0                         # Time in user mode

    Duration:
        duration_seconds: float
    """

    # Required fields (no defaults) must come FIRST
    measurement_id: str
    start_time: float
    end_time: float
    package_energy_uj: int
    core_energy_uj: int
    uncore_energy_uj: int
    idle_energy_uj: int
    workload_energy_uj: int
    reasoning_energy_uj: int
    orchestration_tax_uj: int
    duration_seconds: float

    # Optional fields (with defaults) come AFTER
    baseline_id: Optional[str] = None
    dram_energy_uj: Optional[int] = None

    # ========================================================================
    # Performance Counters (Req 1.5, 1.6, 1.10)
    # ========================================================================
    instructions: int = 0
    cycles: int = 0
    cache_misses: int = 0
    cache_references: int = 0
    ipc: float = 0.0
    page_faults: int = 0
    major_page_faults: int = 0
    minor_page_faults: int = 0

    # ========================================================================
    # Power States (Req 1.7, 1.41, 1.8)
    # ========================================================================
    c_state_residencies: Optional[Dict[str, float]] = None
    frequency_mhz: float = 0.0
    gpu_frequency_mhz: float = 0.0
    gpu_rc6_percent: float = 0.0

    # ========================================================================
    # Thermal (Req 1.9)
    # ========================================================================
    package_temp_celsius: float = 0.0
    core_temps_celsius: Optional[List[float]] = None

    # ========================================================================
    # Scheduler (Req 1.12, 1.23, 1.36)
    # ========================================================================
    context_switches_voluntary: int = 0
    context_switches_involuntary: int = 0
    run_queue_length: float = 0.0
    kernel_time_ms: float = 0.0
    user_time_ms: float = 0.0
    thread_migrations: int = 0
    # ========================================================================
    # NEW: MSR Metrics
    # ========================================================================
    ring_bus_min_mhz: float = 0.0
    ring_bus_max_mhz: float = 0.0
    ring_bus_current_mhz: float = 0.0
    wakeup_latency_us: float = 0.0
    thermal_throttle: int = 0
    tsc_frequency_hz: int = 0
    # ========================================================================
    # NEW: Thermal derived metrics (from EnergyEngine calculations)
    # ========================================================================
    thermal_during_experiment: int = 0  # 1 if throttling occurred DURING experiment
    thermal_now_active: int = 0  # 1 if throttling active at end
    thermal_since_boot: int = 0  # 1 if system has ever throttled since boot
    # C-state times (in seconds for easy analysis)
    c2_time_seconds: float = 0.0
    c3_time_seconds: float = 0.0
    c6_time_seconds: float = 0.0
    c7_time_seconds: float = 0.0

    # Raw TSC ticks (for advanced users)
    c2_ticks: int = 0
    c3_ticks: int = 0
    c6_ticks: int = 0
    c7_ticks: int = 0

    @property
    def package_energy_j(self) -> float:
        """Package energy in joules."""
        return self.package_energy_uj / 1_000_000

    @property
    def workload_energy_j(self) -> float:
        """Workload energy in joules."""
        return self.workload_energy_uj / 1_000_000

    @property
    def orchestration_tax_j(self) -> float:
        """Orchestration tax in joules."""
        return self.orchestration_tax_uj / 1_000_000

    @property
    def total_context_switches(self) -> int:
        """Total context switches."""
        return self.context_switches_voluntary + self.context_switches_involuntary

    @property
    def reasoning_ratio(self) -> float:
        """Percentage of energy used for actual reasoning."""
        if self.package_energy_uj == 0:
            return 0.0
        return (self.reasoning_energy_uj / self.package_energy_uj) * 100

    @property
    def tax_ratio(self) -> float:
        """Percentage of energy used for orchestration overhead."""
        if self.package_energy_uj == 0:
            return 0.0
        return (self.orchestration_tax_uj / self.package_energy_uj) * 100

    @property
    def exp_start_time(self) -> str:
        """Human-readable start time."""
        from datetime import datetime

        return datetime.fromtimestamp(self.start_time).strftime("%Y-%m-%d %H:%M:%S")

    @property
    def exp_end_time(self) -> str:
        """Human-readable end time."""
        from datetime import datetime

        return datetime.fromtimestamp(self.end_time).strftime("%Y-%m-%d %H:%M:%S")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for reporting."""
        result = {
            "measurement_id": self.measurement_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "exp_start_time": self.exp_start_time,
            "exp_end_time": self.exp_end_time,
            "baseline_id": self.baseline_id,
            "energy_uj": {
                "package": self.package_energy_uj,
                "core": self.core_energy_uj,
                "dram": self.dram_energy_uj,
                "uncore": self.uncore_energy_uj,
                "idle": self.idle_energy_uj,
                "workload": self.workload_energy_uj,
                "reasoning": self.reasoning_energy_uj,
                "orchestration_tax": self.orchestration_tax_uj,
            },
            "energy_j": {
                "package": self.package_energy_j,
                "workload": self.workload_energy_j,
                "orchestration_tax": self.orchestration_tax_j,
            },
            "ratios": {
                "reasoning_percent": self.reasoning_ratio,
                "tax_percent": self.tax_ratio,
            },
            # ====================================================================
            # Performance Counters
            # ====================================================================
            "performance": {
                "instructions": self.instructions,
                "cycles": self.cycles,
                "ipc": self.ipc,
                "cache_misses": self.cache_misses,
                "cache_references": self.cache_references,
                "cache_miss_rate": (
                    self.cache_misses / self.cache_references
                    if self.cache_references > 0
                    else 0
                ),
                "page_faults": self.page_faults,
                "major_page_faults": self.major_page_faults,
                "minor_page_faults": self.minor_page_faults,
            },
            # ====================================================================
            # Power States
            # ====================================================================
            "power_states": {
                "c_state_residencies": self.c_state_residencies,
                "frequency_mhz": self.frequency_mhz,
                "gpu_frequency_mhz": self.gpu_frequency_mhz,
                "gpu_rc6_percent": self.gpu_rc6_percent,
            },
            # ====================================================================
            # Thermal
            # ====================================================================
            "thermal": {
                "package_temp_celsius": self.package_temp_celsius,
                "core_temps_celsius": self.core_temps_celsius,
            },
            # ====================================================================
            # Scheduler
            # ====================================================================
            "scheduler": {
                "context_switches_voluntary": self.context_switches_voluntary,
                "context_switches_involuntary": self.context_switches_involuntary,
                "total_context_switches": self.total_context_switches,
                "run_queue_length": self.run_queue_length,
                "kernel_time_ms": self.kernel_time_ms,
                "user_time_ms": self.user_time_ms,
                "thread_migrations": self.thread_migrations,
            },
            # ====================================================================
            # NEW: Add MSR metrics section (add these lines exactly)
            # ====================================================================
            "msr": {
                "ring_bus": {
                    "min_mhz": self.ring_bus_min_mhz,
                    "max_mhz": self.ring_bus_max_mhz,
                    "current_mhz": self.ring_bus_current_mhz,
                },
                "wakeup_latency_us": self.wakeup_latency_us,
                "thermal_throttle": self.thermal_throttle,
                "tsc_frequency_hz": self.tsc_frequency_hz,
                "c_states": {
                    "c2": {
                        "seconds": self.c2_time_seconds,
                        "ticks": self.c2_ticks,
                    },
                    "c3": {
                        "seconds": self.c3_time_seconds,
                        "ticks": self.c3_ticks,
                    },
                    "c6": {
                        "seconds": self.c6_time_seconds,
                        "ticks": self.c6_ticks,
                    },
                    "c7": {
                        "seconds": self.c7_time_seconds,
                        "ticks": self.c7_ticks,
                    },
                },
            },
            "duration_seconds": self.duration_seconds,
        }
        return result

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(self.to_dict(), indent=2)
