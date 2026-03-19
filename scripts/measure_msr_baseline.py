#!/usr/bin/env python3
"""
================================================================================
MSR Baseline Collector – Measure stable hardware MSR properties once per system
================================================================================

This script measures MSR values that are invariant or change very slowly
(e.g., ring bus frequency, wake-up latency) and stores them in a baseline file.
Run this once after hardware changes or at initial setup.

Usage:
    python scripts/measure_msr_baseline.py [--force]

Options:
    --force    Remeasure even if baseline already exists
================================================================================
"""

#!/usr/bin/env python3
"""
================================================================================
MSR Baseline Collector – Measure stable hardware MSR properties once per system
================================================================================
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.config_loader import ConfigLoader
from core.readers.msr_reader import MSRReader
from core.utils.debug import init_debug_from_env

init_debug_from_env()
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("msr_baseline")

BASELINE_FILE = project_root / "data" / "msr_baseline.json"


def load_existing_baseline():
    if BASELINE_FILE.exists():
        with open(BASELINE_FILE, "r") as f:
            return json.load(f)
    return None


def save_baseline(data):
    BASELINE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(BASELINE_FILE, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"✅ Baseline saved to {BASELINE_FILE}")


def measure_baseline(config):
    # Create MSRReader WITHOUT baseline (to measure fresh)
    # It will automatically use the C helper if available
    reader = MSRReader(config, use_baseline=False)
    if not reader.rdmsr_available and not reader.helper_available:
        logger.error("MSR not available. Check permissions and msr-tools.")
        return None

    # Pin to CPU 0 for consistent measurements
    reader.pin_to_cpu(0)

    baseline = {
        "timestamp": datetime.now().isoformat(),
        "cpu_model": reader.cpu_model,
        "kernel_version": reader.kernel_version,
        "measurements": {},
    }

    # 1. Ring bus frequency (average of 5 samples)
    freqs = []
    for _ in range(5):
        f = reader.measure_ring_bus_frequency()
        if f is not None:
            freqs.append(f)
        time.sleep(0.01)
    if freqs:
        baseline["measurements"]["ring_bus_frequency_mhz"] = sum(freqs) / len(freqs)
    else:
        baseline["measurements"]["ring_bus_frequency_mhz"] = None
        logger.warning("Could not measure ring bus frequency.")

    # 2. Hardware wake-up latency
    iterations = (
        config.get("settings", {}).get("msr", {}).get("wakeup_latency_iterations", 500)
    )
    lat = reader.measure_hardware_wakeup_latency(iterations=iterations)
    baseline["measurements"]["wakeup_latency_us"] = lat

    # 3. Sample C7 counter (just for reference)
    c7 = reader.read_msr(reader.MSR_ADDRESSES["MSR_PKG_C7_RESIDENCY"], cpu=0)
    baseline["measurements"]["sample_c7_counter"] = c7 if c7 is not None else 0

    # 4. Store configuration-derived constants
    baseline["measurements"]["cstate_counter_max"] = reader.cstate_max
    baseline["measurements"]["base_clock_mhz"] = reader.ring_bus_base_clock

    reader.unpin()
    return baseline


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force", action="store_true", help="Remeasure even if baseline exists"
    )
    args = parser.parse_args()

    if not args.force and BASELINE_FILE.exists():
        logger.info(
            f"Baseline already exists at {BASELINE_FILE}. Use --force to remeasure."
        )
        return

    config_loader = ConfigLoader()
    hw_config = config_loader.get_hardware_config()
    settings = config_loader.get_settings()

    if hasattr(settings, "__dict__"):
        hw_config["settings"] = settings.__dict__
    else:
        hw_config["settings"] = settings

    baseline = measure_baseline(hw_config)
    if baseline:
        save_baseline(baseline)
    else:
        logger.error("Baseline measurement failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
