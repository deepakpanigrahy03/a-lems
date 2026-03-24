#!/usr/bin/env python3
"""
================================================================================
IDLE BASELINE MEASUREMENT UTILITY – Research-Grade Idle Power Measurement
with automatic caching and system state tracking
================================================================================
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import json
import logging
import statistics
import time
from typing import Any, Dict, List, Optional

import psutil  # NEW: For process and CPU monitoring

from core.models.baseline_measurement import \
    BaselineMeasurement  # NEW: Use proper model
from core.readers.rapl_reader import RAPLReader
from core.readers.scheduler_monitor import SchedulerMonitor
from core.utils.core_pinner import CorePinner
from core.utils.debug import dprint

logger = logging.getLogger(__name__)
from core.config_loader import ConfigLoader
config = ConfigLoader()
settings = config.get_settings()
cache_file = settings.get("experiment", {}).get("baseline", {}).get("cache_file", "data/idle_baseline.json")
DEFAULT_CACHE_FILE = project_root / cache_file


def get_system_state() -> Dict[str, Any]:
    """
    Get current system state for reproducibility.

    Captures:
    - CPU governor (performance/powersave)
    - Turbo boost status
    - Number of running processes
    - Background CPU usage

    Returns:
        Dictionary with system state information
    """
    state = {}

    # Get CPU governor (affects idle power)
    try:
        with open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor", "r") as f:
            state["governor"] = f.read().strip()
    except:
        state["governor"] = "unknown"

    # Get turbo boost status (affects max frequency)
    try:
        with open("/sys/devices/system/cpu/intel_pstate/no_turbo", "r") as f:
            turbo_val = f.read().strip()
            state["turbo"] = "disabled" if turbo_val == "1" else "enabled"
    except:
        state["turbo"] = "unknown"

    # Get background process count (noise indicator)
    state["processes"] = len(psutil.pids())

    # Get background CPU usage (noise indicator)
    state["background_cpu"] = psutil.cpu_percent(interval=1)

    return state


def measure_idle_baseline(
    rapl_reader: RAPLReader,
    core_pinner: CorePinner,
    duration_seconds: int = 10,
    num_samples: int = 10,
    pre_wait_seconds: int = 10,
    pin_cores: Optional[List[int]] = None,
    cache_file: Optional[Path] = None,
    force_remeasure: bool = False,
) -> BaselineMeasurement:  # CHANGED: Returns BaselineMeasurement, not dict
    """
    Measure system idle energy baseline using research-grade methodology.
    Automatically caches the result and reuses it on subsequent calls.

    NEW FEATURES:
    - Tracks system state (governor, turbo, processes, background CPU)
    - Returns proper BaselineMeasurement object with metadata
    - Cache validation ensures state matches before reuse

    Args:
        rapl_reader: Initialized RAPLReader instance
        core_pinner: CorePinner instance for CPU affinity
        duration_seconds: How long each idle sample lasts
        num_samples: Number of samples to take
        pre_wait_seconds: Time to wait before starting
        pin_cores: List of cores to pin to (None = use pinner's default)
        cache_file: Path to cache file (default: data/idle_baseline.json)
        force_remeasure: If True, ignore cache and force new measurement

    Returns:
        BaselineMeasurement object with power values and metadata
    """
    # Use default cache if none provided
    if cache_file is None:
        cache_file = DEFAULT_CACHE_FILE

    # Create cache directory if needed
    cache_file.parent.mkdir(parents=True, exist_ok=True)

    # Get current system state
    current_state = get_system_state()
    dprint(
        f"System state: governor={current_state['governor']}, "
        f"turbo={current_state['turbo']}, "
        f"processes={current_state['processes']}, "
        f"background_cpu={current_state['background_cpu']:.1f}%"
    )

    # Try to load from cache unless forced to remeasure
    if not force_remeasure and cache_file.exists():
        try:
            with open(cache_file, "r") as f:
                cache_data = json.load(f)

            # Verify cache matches current system state
            cached_state = cache_data.get("metadata", {})
            if (
                cached_state.get("governor") == current_state["governor"]
                and cached_state.get("turbo") == current_state["turbo"]
            ):

                dprint(f"✅ Loaded idle baseline from {cache_file}")

                # Convert cached dict back to BaselineMeasurement
                return BaselineMeasurement(
                    baseline_id=cache_data["baseline_id"],
                    timestamp=cache_data["timestamp"],
                    power_watts=cache_data["power_watts"],
                    duration_seconds=cache_data["duration_seconds"],
                    sample_count=cache_data["sample_count"],
                    std_dev_watts=cache_data["std_dev_watts"],
                    cpu_temperature_c=cache_data.get("cpu_temperature_c", 0),
                    method=cache_data.get("method", "idle_measurement"),
                    metadata=cache_data.get("metadata", {}),
                )
            else:
                dprint("⚠️ Cache invalid (system state changed), remeasuring...")
        except Exception as e:
            logger.warning(f"Failed to load cache, remeasuring: {e}")

    dprint(
        f"📝 Measuring idle baseline: {num_samples} samples of {duration_seconds}s each"
    )

    # ------------------------------------------------------------------------
    # Step 1: Pin to dedicated cores (Req 1.15)
    # ------------------------------------------------------------------------
    if pin_cores is not None:
        core_pinner.pin_to_cores(pin_cores)
        dprint(f"📌 Pinned to runtime-specified cores: {pin_cores}")
    else:
        core_pinner.pin_to_cores()  # Use pinner's default
        dprint(f"📌 Pinned to default cores: {core_pinner.default_cores}")

    # ------------------------------------------------------------------------
    # Step 2: Wait for system to enter deep idle states (Req 1.7)
    # ------------------------------------------------------------------------
    dprint(
        f"⏳ Waiting {pre_wait_seconds} seconds for system to enter deep idle states..."
    )
    time.sleep(pre_wait_seconds)

    # ------------------------------------------------------------------------
    # Step 3: Collect multiple samples
    # ------------------------------------------------------------------------
    sched_monitor = SchedulerMonitor({})
    all_samples = []
    all_powers = {}  # Store values per domain for statistics
    start_interrupts = (
        sched_monitor._read_total_interrupts()
    )  # You'll need to import this
    start_time = time.time()
    for sample_idx in range(num_samples):
        dprint(f"📊 Sample {sample_idx + 1}/{num_samples}")

        start_rapl = rapl_reader.read_energy()
        time.sleep(duration_seconds)
        end_rapl = rapl_reader.read_energy()

        sample_power = {}
        for domain in start_rapl:
            if domain in end_rapl:
                delta_uj = max(0, end_rapl[domain] - start_rapl[domain])
                delta_joules = delta_uj / 1_000_000
                power_watts = delta_joules / duration_seconds
                sample_power[domain] = power_watts

                # Store for statistics
                if domain not in all_powers:
                    all_powers[domain] = []
                all_powers[domain].append(power_watts)

        all_samples.append(sample_power)
        dprint(f"   Power: {sample_power}")

    # ------------------------------------------------------------------------
    # Step 4: Compute statistics (mean and standard deviation)
    # ------------------------------------------------------------------------
    avg_power = {}
    std_power = {}

    for domain, values in all_powers.items():
        avg_power[domain] = statistics.mean(values)
        if len(values) > 1:
            std_power[domain] = statistics.stdev(values)
        else:
            std_power[domain] = 0.0
        dprint(
            f"   Domain {domain}: mean={avg_power[domain]:.4f} W, "
            f"std={std_power[domain]:.4f} W"
        )

    # Record end interrupts
    end_interrupts = sched_monitor._read_total_interrupts()
    end_time = time.time()

    # Calculate average interrupt rate during baseline
    elapsed = end_time - start_time
    avg_interrupt_rate = (end_interrupts - start_interrupts) / elapsed

    # Add to current_state before it becomes metadata
    current_state["interrupt_rate"] = avg_interrupt_rate

    # ------------------------------------------------------------------------
    # Step 5: Create BaselineMeasurement object
    # ------------------------------------------------------------------------
    baseline = BaselineMeasurement(
        baseline_id=f"baseline_{int(time.time())}",
        timestamp=time.time(),
        power_watts=avg_power,
        duration_seconds=duration_seconds * num_samples,
        sample_count=num_samples,
        std_dev_watts=std_power,
        cpu_temperature_c=0,  # Add if you have temperature sensor
        method="idle_measurement",
        metadata=current_state,  # Store system state for validation
    )

    # ------------------------------------------------------------------------
    # Step 6: Save to cache (with metadata for validation)
    # ------------------------------------------------------------------------
    try:
        with open(cache_file, "w") as f:
            json.dump(
                {
                    "baseline_id": baseline.baseline_id,
                    "timestamp": baseline.timestamp,
                    "power_watts": baseline.power_watts,
                    "duration_seconds": baseline.duration_seconds,
                    "sample_count": baseline.sample_count,
                    "std_dev_watts": baseline.std_dev_watts,
                    "cpu_temperature_c": baseline.cpu_temperature_c,
                    "method": baseline.method,
                    "metadata": baseline.metadata,
                },
                f,
                indent=2,
            )
        dprint(f"💾 Saved idle baseline to {cache_file}")
    except Exception as e:
        logger.error(f"Failed to save baseline cache: {e}")

    dprint(
        "✅ Idle baseline complete:", **{k: f"{v:.4f} W" for k, v in avg_power.items()}
    )
    return baseline


def apply_baseline_correction(
    raw_energy_uj: Dict[str, int],
    baseline_power_watts: Dict[str, float],
    duration_seconds: float,
) -> Dict[str, int]:
    """
    Apply idle baseline correction to raw energy measurements.

    Args:
        raw_energy_uj: Raw energy readings in microjoules
        baseline_power_watts: Idle power in Watts per domain
        duration_seconds: Duration of the measurement

    Returns:
        Corrected energy in microjoules (raw - idle)
    """
    corrected = {}

    for domain, energy_uj in raw_energy_uj.items():
        if domain in baseline_power_watts:
            idle_joules = baseline_power_watts[domain] * duration_seconds
            idle_uj = int(idle_joules * 1_000_000)
            corrected[domain] = max(0, energy_uj - idle_uj)
        else:
            corrected[domain] = energy_uj

    return corrected


# ============================================================================
# Example usage (standalone test)
# ============================================================================
if __name__ == "__main__":
    import json

    # Set up logging
    logging.basicConfig(level=logging.INFO)

    print("\n" + "=" * 70)
    print("🔬 IDLE BASELINE MEASUREMENT TEST")
    print("=" * 70)

    # Load config
    with open(project_root / "config" / "hw_config.json") as f:
        config = json.load(f)

    # Initialize readers
    rapl = RAPLReader(config)
    pinner = CorePinner(default_cores=[0, 1])
    print(f"📊 {pinner}")

    # First call – should measure and save
    print("\n📝 First call (measuring, should save to cache)...")
    baseline1 = measure_idle_baseline(
        rapl_reader=rapl,
        core_pinner=pinner,
        duration_seconds=2,
        num_samples=2,
        pre_wait_seconds=2,
    )
    print(f"   Baseline: {baseline1.power_watts}")
    print(
        f"   Interrupt Rate: {baseline1.metadata.get('interrupt_rate', 'N/A'):.1f}/sec"
    )

    # Second call – should load from cache
    print("\n📝 Second call (should load from cache)...")
    baseline2 = measure_idle_baseline(rapl_reader=rapl, core_pinner=pinner)
    print(f"   Baseline: {baseline2.power_watts}")
    print(
        f"   Interrupt Rate: {baseline2.metadata.get('interrupt_rate', 'N/A'):.1f}/sec"
    )

    # Force remeasure
    print("\n📝 Force remeasure...")
    baseline3 = measure_idle_baseline(
        rapl_reader=rapl,
        core_pinner=pinner,
        force_remeasure=True,
        duration_seconds=2,
        num_samples=2,
    )
    print(f"   Baseline: {baseline3.power_watts}")
    print(
        f"   Interrupt Rate: {baseline3.metadata.get('interrupt_rate', 'N/A'):.1f}/sec"
    )

    print("\n" + "=" * 70)
    print("✅ Test complete!")
    print("=" * 70)
