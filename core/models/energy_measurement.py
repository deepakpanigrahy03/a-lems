#!/usr/bin/env python3
"""
================================================================================
ENERGY MEASUREMENT DATA MODELS
================================================================================

This module defines the data structures used throughout Module 1 to represent
energy measurements, performance counters, and hardware state.

All data classes are designed to be:
- Self-documenting (field names explain their purpose)
- Serializable (can be saved to JSON/database)
- Type-safe (using dataclasses with type hints)

Author: Deepak Panigrahy
================================================================================
"""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

# ============================================================================
# PERFORMANCE COUNTERS
# ============================================================================


@dataclass
class PerformanceCounters:
    """
    Hardware performance counter readings from perf_events.

    These counters provide insight into what the CPU was actually doing
    during the measurement interval. They help distinguish between:
    - Actual computation (instructions retired)
    - Memory bottlenecks (cache misses)
    - OS scheduler overhead (context switches)

    Req 1.5: Instruction density (IPC)
    Req 1.6: Cache behavior
    Req 1.10: Page faults
    Req 1.43: Thread migrations
    """

    # ===== Req 1.5: Instruction density =====
    instructions_retired: int = 0
    """Number of instructions retired (actual work done)."""

    cpu_cycles: int = 0
    """Number of CPU cycles elapsed."""

    # ===== Req 1.6: Cache behavior =====
    cache_references: int = 0
    """Number of cache references (LLC accesses)."""

    cache_misses: int = 0
    """Number of cache misses (LLC misses)."""

    # ===== Req 1.10: Memory pressure =====
    major_page_faults: int = 0
    """Major page faults (needed disk I/O)."""

    minor_page_faults: int = 0
    """Minor page faults (resolved in memory)."""

    # ===== Req 1.12, 1.43: Scheduler activity =====
    context_switches_voluntary: int = 0
    """Voluntary context switches (process yielded CPU)."""

    context_switches_involuntary: int = 0
    """Involuntary context switches (scheduler preempted)."""

    thread_migrations: int = 0
    """Thread migrations between CPU cores."""

    # ===== Additional perf data =====
    branches: int = 0
    """Branch instructions executed."""

    branch_misses: int = 0
    """Mispredicted branches."""

    cpu_clock_ms: float = 0.0
    """CPU time consumed in milliseconds."""

    task_clock_ms: float = 0.0
    """Task elapsed time in milliseconds."""

    def instructions_per_cycle(self) -> float:
        """
        Calculate Instructions Per Cycle (IPC).

        IPC tells us how efficiently the CPU was running:
        - High IPC (>1) means CPU was doing useful work
        - Low IPC (<0.5) suggests stalls (waiting for memory)

        Req 1.5: Instruction density

        Returns:
            float: Instructions per cycle, or 0.0 if no cycles
        """
        if self.cpu_cycles > 0:
            return self.instructions_retired / self.cpu_cycles
        return 0.0

    def cache_miss_rate(self) -> float:
        """
        Calculate cache miss rate.

        Higher miss rates indicate memory bandwidth pressure,
        which can increase energy consumption significantly.

        Req 1.6: Cache behavior

        Returns:
            float: Miss rate between 0.0 and 1.0
        """
        if self.cache_references > 0:
            return self.cache_misses / self.cache_references
        return 0.0

    def total_context_switches(self) -> int:
        """
        Total context switches (voluntary + involuntary).

        Req 1.12: Kernel context switches

        Returns:
            int: Total number of context switches
        """
        return self.context_switches_voluntary + self.context_switches_involuntary

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for serialization.

        Returns:
            Dict with all fields and derived metrics
        """
        result = asdict(self)
        result["ipc"] = self.instructions_per_cycle()
        result["cache_miss_rate"] = self.cache_miss_rate()
        result["total_context_switches"] = self.total_context_switches()
        return result


# ============================================================================
# THERMAL READINGS
# ============================================================================


@dataclass
class ThermalReadings:
    """
    Temperature sensor readings from various thermal zones.

    Temperature is critical for:
    - Detecting thermal throttling (Req 1.9)
    - Ensuring measurements aren't skewed by overheating
    - Validating cooldown periods between runs

    Req 1.9: Package thermal jitter
    """

    package_temperature_celsius: float = 0.0
    """CPU package temperature in Celsius."""

    core_temperatures_celsius: List[float] = field(default_factory=list)
    """Per-core temperatures (may vary significantly)."""

    gpu_temperature_celsius: float = 0.0
    """Integrated GPU temperature if available."""

    pch_temperature_celsius: float = 0.0
    """Platform Controller Hub temperature."""

    thermal_throttle_count: int = 0
    """Number of thermal throttling events during measurement."""

    prochot_events: int = 0
    """PROCHOT (processor hot) assertion count."""

    def is_throttling(self, threshold: float = 85.0) -> bool:
        """
        Check if CPU is thermal throttling.

        Most CPUs throttle around 85-100°C. If we're above threshold,
        measurements may be unreliable.

        Args:
            threshold: Temperature threshold in Celsius

        Returns:
            bool: True if temperature exceeds threshold
        """
        return self.package_temperature_celsius > threshold

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for serialization.

        Returns:
            Dict with all thermal data
        """
        return {
            "package_celsius": self.package_temperature_celsius,
            "core_temps": self.core_temperatures_celsius,
            "gpu_celsius": self.gpu_temperature_celsius,
            "pch_celsius": self.pch_temperature_celsius,
            "throttle_events": self.thermal_throttle_count,
            "prochot_events": self.prochot_events,
            "is_throttling": self.is_throttling(),
        }


# ============================================================================
# POWER STATE
# ============================================================================


@dataclass
class PowerState:
    """
    CPU power state information including frequencies and C-states.

    Modern CPUs save power by entering deeper sleep states (C-states)
    and lower frequencies (P-states). This data helps attribute energy
    to actual work vs. waiting.

    Req 1.4: DVFS attribution (frequency)
    Req 1.7: C-state residency
    Req 1.8: iGPU states
    Req 1.41: Core-specific C-states
    """

    # ===== Req 1.4: Frequency =====
    frequencies_mhz: Dict[int, float] = field(default_factory=dict)
    """Current frequency per core in MHz."""

    tsc_frequency_mhz: float = 0.0
    """Time Stamp Counter frequency in MHz (constant)."""

    # ===== Req 1.7, 1.41: C-states =====
    c_state_residencies: Dict[str, float] = field(default_factory=dict)
    """
    Time spent in each C-state as percentage.
    Keys: 'C1', 'C1E', 'C3', 'C6', 'C7', 'C8', 'C9', 'C10'
    """

    # ===== Req 1.8: iGPU =====
    igpu_rc6_percent: float = 0.0
    """Render C6 state residency for iGPU (power saving)."""

    igpu_frequency_mhz: float = 0.0
    """Integrated GPU frequency in MHz."""

    # ===== Req 1.9: Temperature =====
    package_temperature_celsius: float = 0.0
    """CPU package temperature in Celsius."""

    # ===== Power estimates =====
    package_power_watts: float = 0.0
    """Package power estimate in watts."""

    core_power_watts: float = 0.0
    """Core power estimate in watts."""

    gpu_power_watts: float = 0.0
    """GPU power estimate in watts."""

    ram_power_watts: float = 0.0
    """DRAM power estimate in watts."""

    def deepest_cstate(self) -> Optional[str]:
        """
        Identify the deepest C-state entered.

        Req 1.41: Deepest core C-state

        Returns:
            str: Name of deepest C-state (e.g., 'C7') or None
        """
        for state in ["C10", "C9", "C8", "C7", "C6", "C3", "C1E", "C1"]:
            if state in self.c_state_residencies:
                residency = self.c_state_residencies.get(state, 0)
                if residency > 0:
                    return state
        return None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for serialization.

        Returns:
            Dict with all power state data
        """
        return {
            "frequencies": self.frequencies_mhz,
            "tsc_mhz": self.tsc_frequency_mhz,
            "c_states": self.c_state_residencies,
            "deepest_cstate": self.deepest_cstate(),
            "igpu_rc6_percent": self.igpu_rc6_percent,
            "igpu_frequency_mhz": self.igpu_frequency_mhz,
            "package_temp_celsius": self.package_temperature_celsius,
            "power_watts": {
                "package": self.package_power_watts,
                "core": self.core_power_watts,
                "gpu": self.gpu_power_watts,
                "ram": self.ram_power_watts,
            },
        }


# ============================================================================
# ENERGY MEASUREMENT (MAIN DATA CLASS)
# ============================================================================


@dataclass
class EnergyMeasurement:
    """
    Complete energy measurement result from a single experiment run.

    This is the primary output of Module 1. It contains everything
    needed for sustainability calculations (Module 2) and orchestration
    tax analysis (Module 3).

    The object is designed to be:
    - Self-contained (all data in one place)
    - Serializable (can be saved to database)
    - Comparable (can compare linear vs agentic runs)

    Req 1.1 through 1.47 are all represented here.
    """

    # ===== Metadata =====
    measurement_id: str
    """Unique identifier for this measurement (e.g., 'meas_20260220_143022')."""

    start_time: float
    """Unix timestamp when measurement started."""

    end_time: float
    """Unix timestamp when measurement ended."""

    duration_seconds: float
    """Total duration of measurement in seconds."""

    # ===== Req 1.1, 1.3: RAPL energy (microjoules) =====
    package_energy_uj: int = 0
    """Total package energy in microjoules (PKG domain)."""

    core_energy_uj: int = 0
    """Core-only energy in microjoules."""

    dram_energy_uj: Optional[int] = None
    """DRAM energy in microjoules (may be None if unavailable)."""

    gpu_energy_uj: Optional[int] = None
    """GPU energy in microjoules (may be None if unavailable)."""

    uncore_waste_uj: int = 0
    """
    Energy consumed by uncore (cache, memory controller, interconnect).
    Calculated as: package - core - dram - gpu
    
    Req 1.3: This is the energy attributed to orchestration overhead.
    """

    # ===== Req 1.45: Idle baseline correction =====
    idle_baseline_energy_uj: float = 0.0
    """Energy the system would consume if completely idle (subtracted)."""

    # ===== Req 1.5-1.43: Performance counters =====
    performance_counters: PerformanceCounters = field(
        default_factory=PerformanceCounters
    )
    """Hardware performance counter readings."""

    # ===== Req 1.9: Thermal data =====
    thermal: ThermalReadings = field(default_factory=ThermalReadings)
    """Temperature sensor readings."""

    # ===== Req 1.4, 1.7, 1.8, 1.41: Power states =====
    power_state: PowerState = field(default_factory=PowerState)
    """CPU power state information."""

    # ===== Req 1.12, 1.23, 1.24, 1.36: Scheduler data =====
    run_queue_length: float = 0.0
    """Average number of processes in run queue (from /proc/loadavg)."""

    kernel_time_ms: float = 0.0
    """Time spent in kernel mode (milliseconds)."""

    user_time_ms: float = 0.0
    """Time spent in user mode (milliseconds)."""

    # ===== Req 1.46: Sampling metadata =====
    sample_count: int = 0
    """Number of RAPL samples taken during measurement."""

    sampling_rate_hz: float = 0.0
    """Actual sampling rate achieved (samples / second)."""

    # ===== Quality metrics =====
    measurement_valid: bool = True
    """Whether the measurement passed validation checks."""

    validation_issues: List[str] = field(default_factory=list)
    """List of issues found during validation (if any)."""

    # ===== Raw data for reproducibility =====
    raw_rapl_start: Optional[Dict[str, int]] = None
    """Raw RAPL readings at start (for debugging)."""

    raw_rapl_end: Optional[Dict[str, int]] = None
    """Raw RAPL readings at end (for debugging)."""

    def calculate_derived_metrics(self):
        """
        Calculate derived metrics from raw readings.

        This should be called after populating raw data.
        """
        # Convert to joules for easier reading (but keep microjoules for precision)
        self.package_energy_joules = self.package_energy_uj / 1_000_000
        self.core_energy_joules = self.core_energy_uj / 1_000_000

        # Req 1.3: Calculate uncore waste if we have all components
        accounted = self.core_energy_uj
        if self.dram_energy_uj:
            accounted += self.dram_energy_uj
        if self.gpu_energy_uj:
            accounted += self.gpu_energy_uj

        self.uncore_waste_uj = max(0, self.package_energy_uj - accounted)

    def inference_ratio(self) -> float:
        """
        Estimate ratio of energy used for actual inference vs. overhead.

        This is a heuristic based on uncore waste and core activity.

        Req 2.11: Reasoning-to-waste ratio

        Returns:
            float: Ratio between 0.0 and 1.0
        """
        if self.package_energy_uj == 0:
            return 0.0

        # Assume core energy is inference, uncore waste is overhead
        inference = self.core_energy_uj
        total = self.package_energy_uj

        return inference / total

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for serialization.

        Returns:
            Dict ready for JSON serialization
        """
        self.calculate_derived_metrics()

        return {
            "measurement_id": self.measurement_id,
            "timing": {
                "start": self.start_time,
                "end": self.end_time,
                "duration_seconds": self.duration_seconds,
            },
            "energy_uj": {
                "package": self.package_energy_uj,
                "core": self.core_energy_uj,
                "dram": self.dram_energy_uj,
                "gpu": self.gpu_energy_uj,
                "uncore_waste": self.uncore_waste_uj,
                "idle_baseline": self.idle_baseline_energy_uj,
            },
            "energy_joules": {
                "package": self.package_energy_uj / 1_000_000,
                "core": self.core_energy_uj / 1_000_000,
                "uncore_waste": self.uncore_waste_uj / 1_000_000,
            },
            "performance": self.performance_counters.to_dict(),
            "thermal": self.thermal.to_dict(),
            "power_state": self.power_state.to_dict(),
            "scheduler": {
                "run_queue_length": self.run_queue_length,
                "kernel_time_ms": self.kernel_time_ms,
                "user_time_ms": self.user_time_ms,
                "kernel_ratio": self.kernel_time_ms
                / (self.kernel_time_ms + self.user_time_ms + 1e-9),
            },
            "quality": {
                "valid": self.measurement_valid,
                "issues": self.validation_issues,
                "samples": self.sample_count,
                "sampling_rate_hz": self.sampling_rate_hz,
                "inference_ratio": self.inference_ratio(),
            },
        }

    def to_json(self) -> str:
        """
        Convert to JSON string for saving to file.

        Returns:
            JSON string with pretty formatting
        """
        return json.dumps(self.to_dict(), indent=2, default=str)


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    """
    Example: Creating and using an EnergyMeasurement object.

    This shows how the data classes will be used in practice.
    """
    print("\n🔧 Testing EnergyMeasurement data classes...")

    # Create a performance counters object
    perf = PerformanceCounters(
        instructions_retired=1234567,
        cpu_cycles=987654,
        cache_references=50000,
        cache_misses=5000,
        context_switches_voluntary=842,
        context_switches_involuntary=156,
    )

    print(f"✅ IPC: {perf.instructions_per_cycle():.2f}")
    print(f"✅ Cache miss rate: {perf.cache_miss_rate():.2%}")
    print(f"✅ Total context switches: {perf.total_context_switches()}")

    # Create thermal readings
    thermal = ThermalReadings(
        package_temperature_celsius=52.3,
        core_temperatures_celsius=[51.2, 52.1, 53.0, 52.8],
        thermal_throttle_count=0,
    )

    print(f"✅ Package temp: {thermal.package_temperature_celsius}°C")
    print(f"✅ Throttling? {thermal.is_throttling()}")

    # Create power state
    power = PowerState(
        frequencies_mhz={0: 2300, 1: 2300, 2: 1200, 3: 1200},
        c_state_residencies={"C1": 5.2, "C6": 42.1, "C7": 38.3},
        igpu_rc6_percent=95.2,
        igpu_frequency_mhz=300,
    )

    print(f"✅ Deepest C-state: {power.deepest_cstate()}")

    # Create complete measurement
    import time

    measurement = EnergyMeasurement(
        measurement_id="test_001",
        start_time=time.time() - 2.34,
        end_time=time.time(),
        duration_seconds=2.34,
        package_energy_uj=12400000,
        core_energy_uj=8200000,
        performance_counters=perf,
        thermal=thermal,
        power_state=power,
        sample_count=234,
    )

    measurement.calculate_derived_metrics()

    print(f"\n✅ Complete measurement created")
    print(f"   Package energy: {measurement.package_energy_uj / 1e6:.4f} J")
    print(f"   Uncore waste: {measurement.uncore_waste_uj / 1e6:.4f} J")
    print(f"   Inference ratio: {measurement.inference_ratio():.2%}")

    print("\n✅ All data classes working!")
