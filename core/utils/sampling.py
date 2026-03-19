#!/usr/bin/env python3
"""
================================================================================
HIGH-FREQUENCY SAMPLING UTILITY
================================================================================

Author: Deepak Panigrahy
Institution:
Contact:

Module: core/utils/sampling.py (Part of Module 1 - Energy Engine)

Purpose:
--------
This module provides a high-frequency sampling thread that reads RAPL energy
counters at a configurable rate (default 100 Hz). This enables:

1. Capturing energy consumption during short agent steps (Req 1.46)
2. Attributing energy to specific phases of agentic workflows
3. Detecting energy spikes from context switches or tool calls
4. Validating that sampling frequency is sufficient (Req 1.46)

Why a Dedicated Sampling Thread:
--------------------------------
- RAPL counters are cumulative; we need differences over short intervals.
- A background thread ensures consistent sampling without blocking the main
  experiment code.
- Samples are stored with timestamps for later correlation with workflow phases.

Requirements Covered:
--------------------
Req 1.46 – RAPL Sampling Frequency Validation – ensures we capture enough
           samples for short agent steps (default 100 Hz = 10ms intervals).

Design:
-------
- The sampler runs in a daemon thread so it exits when the main program exits.
- Samples are stored in a thread-safe queue to avoid locking issues.
- The main thread can retrieve samples after the measurement stops.

Usage Example:
-------------
    sampler = HighFrequencySampler(rapl_reader, sampling_rate_hz=100)
    sampler.start()
    # ... run experiment ...
    sampler.stop()
    samples = sampler.get_samples()

    # Or get samples since a specific timestamp
    recent = sampler.get_samples_since(start_time)

================================================================================
"""

import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class EnergySample:
    """
    A single energy sample from the high-frequency sampler.

    Attributes:
        timestamp (float): Unix timestamp when the sample was taken.
        package_uj (int): Package energy in microjoules.
        core_uj (int): Core energy in microjoules.
        dram_uj (int): DRAM energy in microjoules (may be None if unavailable).
        uncore_uj (int): Uncore energy in microjoules (may be None).
        raw_values (dict): Raw values from all available domains.
    """

    timestamp: float
    package_uj: int = 0
    core_uj: int = 0
    dram_uj: Optional[int] = None
    uncore_uj: Optional[int] = None
    raw_values: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert sample to dictionary for serialization."""
        return {
            "timestamp": self.timestamp,
            "package_uj": self.package_uj,
            "core_uj": self.core_uj,
            "dram_uj": self.dram_uj,
            "uncore_uj": self.uncore_uj,
            "raw": self.raw_values,
        }


class HighFrequencySampler:
    """
    High-frequency RAPL sampling thread.

    This class runs a background thread that reads RAPL energy counters at a
    specified rate (default 100 Hz = every 10ms). Samples are stored in a
    thread-safe queue for later retrieval.

    Attributes:
        rapl_reader: RAPLReader instance to read energy counters.
        sampling_rate_hz (int): Target sampling frequency.
        interval (float): Calculated interval between samples.
        running (bool): Whether the sampler thread is running.
        sample_queue (queue.Queue): Thread-safe queue of EnergySample objects.
        thread (threading.Thread): Background sampler thread.
    """

    def __init__(
        self, rapl_reader, sampling_rate_hz: int = 100, max_queue_size: int = 10000
    ):
        """
        Initialize the high-frequency sampler.

        Args:
            rapl_reader: Initialized RAPLReader instance.
            sampling_rate_hz: Target sampling frequency (default 100 Hz).
            max_queue_size: Maximum number of samples to queue (prevents memory issues).

        Req 1.46: Sampling frequency must be high enough for short agent steps.
        """
        self.rapl_reader = rapl_reader
        self.sampling_rate_hz = sampling_rate_hz
        self.interval = 1.0 / sampling_rate_hz
        self.max_queue_size = max_queue_size

        self.running = False
        self.sample_queue = queue.Queue(maxsize=max_queue_size)
        self.thread: Optional[threading.Thread] = None

        # For statistics
        self.samples_taken = 0
        self.samples_dropped = 0
        self.start_time: Optional[float] = None

        logger.debug(
            f"Sampler initialized: rate={sampling_rate_hz}Hz, "
            f"interval={self.interval*1000:.2f}ms"
        )

    def start(self) -> None:
        """
        Start the sampling thread.

        The thread runs in daemon mode, so it will automatically exit when the
        main program exits.
        """
        if self.running:
            logger.warning("Sampler already running")
            return

        self.running = True
        self.samples_taken = 0
        self.samples_dropped = 0
        self.start_time = time.time()

        # Clear any existing samples
        while not self.sample_queue.empty():
            try:
                self.sample_queue.get_nowait()
            except queue.Empty:
                break

        self.thread = threading.Thread(
            target=self._sampling_loop, name="RAPL-Sampler", daemon=True
        )
        self.thread.start()
        logger.info(f"Sampling thread started at {self.sampling_rate_hz} Hz")

    def stop(self) -> List[EnergySample]:
        """
        Stop the sampling thread and return all collected samples.

        Returns:
            List of EnergySample objects collected during the sampling period.
        """
        if not self.running:
            logger.warning("Sampler not running")
            return []

        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)

        # Collect all samples from queue
        samples = []
        while not self.sample_queue.empty():
            try:
                samples.append(self.sample_queue.get_nowait())
            except queue.Empty:
                break

        duration = time.time() - self.start_time if self.start_time else 0
        actual_rate = len(samples) / duration if duration > 0 else 0

        logger.info(
            f"Sampling stopped: {len(samples)} samples in {duration:.2f}s "
            f"({actual_rate:.1f} Hz actual, {self.samples_dropped} dropped)"
        )

        # Req 1.46: Validate sampling frequency
        if actual_rate < self.sampling_rate_hz * 0.8:
            logger.warning(
                f"Sampling rate too low: {actual_rate:.1f} Hz "
                f"(target {self.sampling_rate_hz} Hz)"
            )

        return samples

    def _sampling_loop(self) -> None:
        """
        Background sampling loop.

        Runs at the configured interval, reading RAPL counters and storing
        samples in the queue. If the queue fills up, oldest samples are dropped
        to prevent memory issues.
        """
        logger.debug("Sampling loop started")
        next_sample = time.time()

        while self.running:
            try:
                # Take sample
                timestamp = time.time()
                energy = self.rapl_reader.read_energy_safe()

                # Create sample object
                sample = EnergySample(
                    timestamp=timestamp,
                    package_uj=energy.get("package-0", 0),
                    core_uj=energy.get("core", 0),
                    dram_uj=energy.get("dram", None),
                    uncore_uj=energy.get("uncore", None),
                    raw_values=energy,
                )

                # Add to queue (non-blocking)
                try:
                    self.sample_queue.put_nowait(sample)
                    self.samples_taken += 1
                except queue.Full:
                    # Queue full - remove oldest and add new
                    try:
                        self.sample_queue.get_nowait()
                        self.sample_queue.put_nowait(sample)
                        self.samples_dropped += 1
                    except queue.Empty:
                        # Shouldn't happen, but handle gracefully
                        pass

                # Schedule next sample
                next_sample += self.interval
                sleep_time = max(0, next_sample - time.time())

                if sleep_time > 0:
                    time.sleep(sleep_time)
                else:
                    # We're falling behind – log warning but continue
                    logger.warning(
                        f"Sampling loop falling behind (interval "
                        f"{self.interval*1000:.1f}ms too short)"
                    )
                    next_sample = time.time()  # Reset timing

            except Exception as e:
                logger.error(f"Sampling error: {e}")
                time.sleep(0.01)  # Brief pause before retry

        logger.debug("Sampling loop stopped")

    def get_samples(self) -> List[EnergySample]:
        """
        Get all samples currently in the queue.

        Returns:
            List of EnergySample objects.
        """
        samples = []
        while not self.sample_queue.empty():
            try:
                samples.append(self.sample_queue.get_nowait())
            except queue.Empty:
                break
        return samples

    def get_samples_since(self, timestamp: float) -> List[EnergySample]:
        """
        Get all samples taken after a specific timestamp.

        Args:
            timestamp: Unix timestamp (samples after this time are returned).

        Returns:
            List of EnergySample objects with timestamp > given timestamp.

        Note:
            This method consumes samples from the queue. If you need to keep
            them, call get_samples() first.
        """
        all_samples = self.get_samples()
        return [s for s in all_samples if s.timestamp > timestamp]

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get sampling statistics.

        Returns:
            Dictionary with keys:
                - sampling_rate_hz: Target rate
                - interval_ms: Target interval in milliseconds
                - samples_taken: Total samples taken
                - samples_dropped: Samples dropped due to queue full
                - current_queue_size: Current number of samples in queue
        """
        return {
            "sampling_rate_hz": self.sampling_rate_hz,
            "interval_ms": self.interval * 1000,
            "samples_taken": self.samples_taken,
            "samples_dropped": self.samples_dropped,
            "current_queue_size": self.sample_queue.qsize(),
        }


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    """
    Example: Test the high-frequency sampler.

    Run with: python -m core.utils.sampling
    """
    import json
    import sys
    import time
    from pathlib import Path

    # Add project root to path
    project_root = Path(__file__).parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("\n" + "=" * 70)
    print("HIGH-FREQUENCY SAMPLER TEST")
    print("=" * 70)

    # Try to use real RAPL reader first
    try:
        from core.readers.rapl_reader import RAPLReader

        # Load config
        config_path = project_root / "config" / "hw_config.json"
        if not config_path.exists():
            print("❌ Config file not found!")
            print("⚠️ Falling back to dummy reader...")
            raise ImportError("No config file")

        with open(config_path) as f:
            config = json.load(f)

        # Get RAPL config
        rapl_config = config.get("rapl", {})
        print(f"🔍 DEBUG: rapl_config = {rapl_config}")

        if not rapl_config.get("paths"):
            print("❌ No RAPL paths in config!")
            print("⚠️ Falling back to dummy reader...")
            raise ImportError("No RAPL paths")

        # Initialize RAPL reader
        try:
            rapl = RAPLReader(rapl_config)

            # Test if it worked by trying to read
            test_read = rapl.read_energy()
            if test_read and any(v != 0 for v in test_read.values()):
                print(
                    f"✅ Using REAL RAPL reader with domains: {list(test_read.keys())}"
                )
                print(f"✅ Sample reading: {test_read}")
            else:
                print("❌ RAPL reader initialized but returned zeros")
                raise ImportError("RAPL read failed")

        except Exception as e:
            print(f"⚠️ Could not initialize real RAPL reader: {e}")
            print("⚠️ Using dummy RAPL reader (simulated data)")

            # Dummy reader
            class DummyRAPLReader:
                def read_energy_safe(self):
                    import random

                    return {
                        "package-0": random.randint(1000000, 2000000),
                        "core": random.randint(500000, 1000000),
                        "uncore": random.randint(100000, 500000),
                    }

                def read_energy(self):
                    return self.read_energy_safe()

                def get_available_domains(self):
                    return ["package-0", "core", "uncore"]

            rapl = DummyRAPLReader()

    except Exception as e:
        print(f"⚠️ Could not initialize real RAPL reader: {e}")
        print("⚠️ Using dummy RAPL reader (simulated data)")

        # Dummy reader
        class DummyRAPLReader:
            def read_energy_safe(self):
                import random

                return {
                    "package-0": random.randint(1000000, 2000000),
                    "core": random.randint(500000, 1000000),
                    "uncore": random.randint(100000, 500000),
                }

            def read_energy(self):
                return self.read_energy_safe()

            def get_available_domains(self):
                return ["package-0", "core", "uncore"]

        rapl = DummyRAPLReader()

    # Create sampler
    sampler = HighFrequencySampler(rapl, sampling_rate_hz=100, max_queue_size=1000)
    print(
        f"📊 Sampler: {sampler.sampling_rate_hz} Hz ({sampler.interval*1000:.2f} ms interval)"
    )

    # Start sampling
    print("\n▶️ Starting sampler for 2 seconds...")
    sampler.start()

    # Let it run for 2 seconds
    time.sleep(2)

    # Stop and get samples
    samples = sampler.stop()

    # Display results
    print(f"\n📈 Collected {len(samples)} samples")

    if samples:
        # Show first few samples
        print("\n📝 First 3 samples:")
        for i, s in enumerate(samples[:3]):
            print(
                f"   {i+1}: t={s.timestamp:.3f}, PKG={s.package_uj} µJ, "
                f"CORE={s.core_uj} µJ"
            )

        # Calculate statistics
        timestamps = [s.timestamp for s in samples]
        deltas = [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]
        if deltas:
            avg_interval = sum(deltas) / len(deltas)
            print(f"\n📊 Statistics:")
            print(f"   Average interval: {avg_interval*1000:.2f} ms")
            print(f"   Actual rate: {1/avg_interval:.1f} Hz")
            print(f"   Target rate: {sampler.sampling_rate_hz} Hz")

    print("\n" + "=" * 70)
    print("✅ Test complete!")
    print("=" * 70)
