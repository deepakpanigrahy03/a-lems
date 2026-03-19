"""
================================================================================
SAMPLE PROCESSOR – Handle energy, CPU, and interrupt sample processing
================================================================================
"""

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

logger = logging.getLogger(__name__)


def process_energy_samples(energy_engine) -> tuple:
    """
    Process energy samples from energy_engine.

    Returns:
        Tuple of (energy_samples, interrupt_samples)
    """
    energy_samples = []
    interrupt_samples = []

    if hasattr(energy_engine, "last_samples"):
        samples = energy_engine.last_samples
        logger.debug(
            f"🔍 DEBUG - Found {len(samples)} samples in energy_engine.last_samples"
        )

        # Samples are tuples - identify them by structure
        for sample in samples:
            if len(sample) == 2 and isinstance(sample[1], dict):
                # This is an energy sample: (timestamp, {'core':..., 'package-0':..., 'uncore':...})
                timestamp, energy_dict = sample
                energy_samples.append(
                    {
                        "timestamp_ns": int(
                            timestamp * 1_000_000_000
                        ),  # Convert seconds to ns
                        "pkg_energy_uj": energy_dict.get("package-0", 0),
                        "core_energy_uj": energy_dict.get("core", 0),
                        "uncore_energy_uj": energy_dict.get("uncore", 0),
                        "dram_energy_uj": 0,  # DRAM not in samples
                    }
                )
            elif len(sample) == 2 and isinstance(sample[1], (int, float)):
                # This is an interrupt sample: (timestamp, value)
                interrupt_timestamp, interrupt_value = sample
                print(
                    f"🔍 INTERRUPT RAW - timestamp: {interrupt_timestamp}, type: {type(interrupt_timestamp)}"
                )
                print(f"🔍 INTERRUPT RAW - value: {interrupt_value}")
                print(
                    f"🔍 INTERRUPT CALC - divided by 1e9: {interrupt_timestamp / 1e9}"
                )
                print(f"🔍 INTERRUPT CALC - epoch time: {time.time()}")

                interrupt_samples.append(
                    {
                        "timestamp_ns": int(interrupt_timestamp),
                        "interrupts_per_sec": interrupt_value,
                    }
                )
            else:
                logger.debug(f"⚠️ Unknown sample format: {sample}")

        logger.debug(
            f"📊 Processed {len(energy_samples)} energy samples, {len(interrupt_samples)} interrupt samples"
        )

    return energy_samples, interrupt_samples


def process_cpu_samples(raw_energy, canonical_metrics, store_extra=True) -> list:
    """
    Extract CPU samples from turbostat continuous data.

    Returns:
        List of CPU sample dictionaries
    """
    cpu_samples = []

    if (
        hasattr(raw_energy, "turbostat")
        and raw_energy.turbostat.get("dataframe") is not None
    ):
        df = raw_energy.turbostat["dataframe"]

        # Get timing info from metadata
        start_ns = None
        interval_ns = 100_000_000  # Default 100ms
        if hasattr(raw_energy, "metadata"):
            start_ns = raw_energy.metadata.get("turbostat_start_ns")
            interval_ns = raw_energy.metadata.get("turbostat_interval_ns", 100_000_000)

        if not df.empty:
            for idx, row in df.iterrows():
                # Calculate timestamp using monotonic clock
                if start_ns is not None:
                    timestamp_ns = start_ns + (idx + 1) * interval_ns
                else:
                    # Fallback to old method
                    timestamp_ns = int((raw_energy.start_time + idx * 0.1) * 1e9)

                # Start with timestamp
                sample = {"timestamp_ns": timestamp_ns}

                # Extract canonical metrics
                for our_name, turbostat_col in canonical_metrics.items():
                    try:
                        val = float(row.get(turbostat_col, 0.0))

                        # Scale percentages (C-states, GPU RC6)
                        if our_name in [
                            "c1_residency",
                            "c2_residency",
                            "c3_residency",
                            "c6_residency",
                            "c7_residency",
                            "pkg_c8_residency",
                            "pkg_c9_residency",
                            "pkg_c10_residency",
                            "gpu_rc6",
                        ]:
                            val = val / 100.0

                        # IPC might need scaling if >10
                        if our_name == "ipc" and val > 10:
                            val = val / 10.0

                        sample[our_name] = val
                    except (TypeError, ValueError):
                        sample[our_name] = 0.0

                # Store all other columns in JSON
                if store_extra:
                    extra = {}
                    for col in df.columns:
                        if col not in canonical_metrics.values():
                            val = row.get(col)
                            if val is not None:
                                try:
                                    extra[col] = float(val)
                                except (TypeError, ValueError):
                                    extra[col] = str(val)
                    sample["extra_metrics_json"] = json.dumps(extra) if extra else "{}"

                cpu_samples.append(sample)

            logger.debug(
                f"📊 Extracted {len(cpu_samples)} CPU samples with {len(canonical_metrics)} canonical metrics"
            )
            if cpu_samples:
                print(f"🔍 First 3 CPU samples:")
                for i, sample in enumerate(cpu_samples[:3]):
                    print(f"   Sample {i}: {sample}")

    return cpu_samples


def calculate_thermal_metrics(cpu_samples) -> tuple:
    """
    Calculate thermal metrics from CPU samples.

    Returns:
        Tuple of (start_temp_c, max_temp_c, min_temp_c, thermal_delta_c)
    """
    if cpu_samples and len(cpu_samples) > 0:
        temps = [
            s.get("package_temp") for s in cpu_samples if s.get("package_temp", 0) > 10
        ]
        if temps:
            start_temp_c = temps[0]  # First sample
            max_temp_c = max(temps)  # Maximum during run
            min_temp_c = min(temps)  # Minimum during run
            thermal_delta_c = max_temp_c - start_temp_c
        else:
            start_temp_c = 0
            max_temp_c = 0
            min_temp_c = 0
            thermal_delta_c = 0
    else:
        start_temp_c = 0
        max_temp_c = 0
        min_temp_c = 0
        thermal_delta_c = 0

    return start_temp_c, max_temp_c, min_temp_c, thermal_delta_c


def load_canonical_metrics() -> tuple:
    """
    Load canonical metrics from turbostat_override.yaml.

    Returns:
        Tuple of (canonical_metrics dict, store_extra boolean)
    """
    canonical_metrics = {}
    store_extra = True

    override_path = Path("config/turbostat_override.yaml")
    if override_path.exists():
        try:
            with open(override_path, "r") as f:
                override_config = yaml.safe_load(f)
            canonical_metrics = override_config.get("canonical_metrics", {})
            store_extra = override_config.get("store_extra_in_json", True)
            logger.debug(
                f"📋 Loaded {len(canonical_metrics)} canonical metrics from override file"
            )
        except Exception as e:
            logger.debug(f"⚠️ Failed to load override file: {e}")

    return canonical_metrics, store_extra
