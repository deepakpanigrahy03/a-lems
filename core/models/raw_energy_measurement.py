#!/usr/bin/env python3
"""
================================================================================
RAW ENERGY MEASUREMENT – Layer 1: Ground Truth
================================================================================

This class represents pure raw measurement data from hardware counters.
NEVER modified after creation. Contains only raw readings, no corrections.

Author: Deepak Panigrahy
================================================================================
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class RawEnergyMeasurement:
    """
    Layer 1 – Raw measurement data. NEVER modified.

    This is your primary evidence. All fields are stored as read from hardware.
    No baseline correction, no derived values – just raw truth.

    Attributes:
        measurement_id: Unique identifier for this measurement
        start_time: Unix timestamp when measurement started
        end_time: Unix timestamp when measurement ended
        duration_seconds: Total duration in seconds

        rapl_start_uj: Raw RAPL readings at start (microjoules)
        rapl_end_uj: Raw RAPL readings at end (microjoules)

        perf: Performance counter readings (PerformanceCounters object)
        turbostat: Turbostat continuous monitoring data (dictionary with dataframe, summary)
        thermal: Temperature readings
        power_state: CPU power state data
        scheduler_metrics: Scheduler metrics with deltas

        samples: List of high-frequency samples (timestamp, energy)
        sampling_rate_hz: Actual sampling rate achieved

        metadata: Additional context (hostname, kernel, cpu_affinity, etc.)
    """

    # Metadata
    measurement_id: str
    start_time: float
    end_time: float
    duration_seconds: float

    # Raw RAPL readings (microjoules)
    rapl_start_uj: Dict[str, int]
    rapl_end_uj: Dict[str, int]

    # Other hardware readings
    perf: Any  # PerformanceCounters object
    turbostat: Dict[str, Any] = field(default_factory=dict)  # ADD THIS LINE
    thermal: Dict[str, Any] = field(default_factory=dict)
    power_state: Dict[str, Any] = field(default_factory=dict)
    ##to capture schedular swap memory start and end snapshot
    scheduler_start: Optional[Dict] = None
    scheduler_end: Optional[Dict] = None
    scheduler_metrics: Dict[str, Any] = field(default_factory=dict)
    msr_metrics: Dict[str, Any] = field(default_factory=dict)
    # ====================================================================
    # NEW: Thermal derived fields (from EnergyEngine calculations)
    # ====================================================================

    thermal_during_experiment: Optional[int] = (
        None  # 1 if throttling occurred during experiment
    )
    thermal_now_active: Optional[int] = None  # 1 if throttling active at end
    thermal_since_boot: Optional[int] = (
        None  # 1 if system has ever throttled since boot
    )
    # Sampling data
    samples: List[tuple] = field(default_factory=list)
    sampling_rate_hz: float = 0.0

    # Thermal samples (1Hz)
    thermal_samples: List[Tuple[float, Dict, bool]] = field(default_factory=list)

    # Additional context
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate raw data (but NEVER modify it)."""
        if self.duration_seconds <= 0:
            raise ValueError(f"Duration must be positive: {self.duration_seconds}")
        # ========== ADD THESE LINES ==========
        if hasattr(self, "msr_metrics") and self.msr_metrics:
            print(f"🟡 RAW_ID: {id(self.msr_metrics)}")
            print(f"🟡 RAW_VALUE: c2={self.msr_metrics.get('c2_time_seconds', 0):.3f}s")
            print(f"🟡 RAW_KEYS: {list(self.msr_metrics.keys())}")
        # =====================================

    @property
    def package_energy_uj(self) -> int:
        """Compute raw package energy delta in microjoules."""
        start = self.rapl_start_uj.get("package-0", 0)
        end = self.rapl_end_uj.get("package-0", 0)
        return max(0, end - start)

    @property
    def core_energy_uj(self) -> int:
        """Compute raw core energy delta in microjoules."""
        start = self.rapl_start_uj.get("core", 0)
        end = self.rapl_end_uj.get("core", 0)
        return max(0, end - start)

    @property
    def dram_energy_uj(self) -> Optional[int]:
        """Compute raw DRAM energy delta if available."""
        if "dram" in self.rapl_start_uj and "dram" in self.rapl_end_uj:
            start = self.rapl_start_uj["dram"]
            end = self.rapl_end_uj["dram"]
            return max(0, end - start)
        return None

    @property
    def package_energy_j(self) -> float:
        """Package energy in joules (for convenience)."""
        return self.package_energy_uj / 1_000_000

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "measurement_id": self.measurement_id,
            "timing": {
                "start": self.start_time,
                "end": self.end_time,
                "duration_seconds": self.duration_seconds,
                "start_iso": datetime.fromtimestamp(self.start_time).isoformat(),
                "end_iso": datetime.fromtimestamp(self.end_time).isoformat(),
            },
            "rapl_start_uj": self.rapl_start_uj,
            "rapl_end_uj": self.rapl_end_uj,
            "perf": self.perf.to_dict() if hasattr(self.perf, "to_dict") else self.perf,
            "turbostat": self.turbostat,  # ADD THIS LINE
            "thermal": self.thermal,
            "power_state": self.power_state,
            "scheduler_metrics": self.scheduler_metrics,
            "msr_metrics": self.msr_metrics,
            # NEW: Thermal derived fields
            "thermal_during_experiment": self.thermal_during_experiment,
            "thermal_now_active": self.thermal_now_active,
            "thermal_since_boot": self.thermal_since_boot,
            "samples": self.samples,
            "sampling_rate_hz": self.sampling_rate_hz,
            "metadata": self.metadata,
            "derived": {
                "package_energy_uj": self.package_energy_uj,
                "core_energy_uj": self.core_energy_uj,
                "dram_energy_uj": self.dram_energy_uj,
                "package_energy_j": self.package_energy_j,
            },
        }

    def to_json(self) -> str:
        """Serialize to JSON for permanent storage."""
        return json.dumps(self.to_dict(), indent=2, default=str)
