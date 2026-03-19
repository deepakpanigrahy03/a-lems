#!/usr/bin/env python3
"""
================================================================================
PERFORMANCE COUNTER READER - Linux perf_events Interface
================================================================================

This module provides access to hardware performance counters via the Linux
perf_events interface. perf_events allows reading CPU performance monitoring
unit (PMU) counters, which provide detailed insight into what the CPU is
actually doing.

Why perf_events matters for AI energy research:
- Instructions/cycle tells us if CPU is doing useful work or stalled
- Cache misses indicate memory bandwidth pressure from large models
- Context switches reveal OS scheduler overhead (orchestration tax)
- Page faults show when models don't fit in RAM

Reference: perf_event_open(2) man page, Linux kernel documentation

Author: Deepak Panigrahy
================================================================================
"""

import logging
import os
import re
import select
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ============================================================================
# FIX PYTHON PATH - Add project root to sys.path
# ============================================================================
# This ensures the 'core' module is found whether running as script or module.
# When running as: python core/readers/perf_reader.py, the project root isn't
# in Python's path. This hack adds it dynamically.
#
# Why this is needed:
# - Python imports are relative to the current working directory or PYTHONPATH
# - Our module structure (core.readers, core.models) expects project root in path
# - This fix works both when run as script or as module with -m
#
# Req: None (infrastructure requirement)
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
    logger = logging.getLogger(__name__)
    logger.debug(f"Added {project_root} to Python path for module imports")

# Now import works regardless of how script is run
from core.models.performance_counters import PerformanceCounters

# Configure module logger
logger = logging.getLogger(__name__)


# ============================================================================
# PERF READER CLASS
# ============================================================================


class PerfReader:
    """
    Reads hardware performance counters using Linux perf_events.

    This class provides access to CPU performance counters via the 'perf stat'
    command. It parses the output and returns structured PerformanceCounters
    objects.

    The reader handles:
    - Checking perf availability and permissions
    - Running perf stat for specified durations
    - Parsing perf output reliably
    - Graceful degradation if perf is not available

    Req 1.5: Instructions, cycles, IPC
    Req 1.6: Cache references and misses
    Req 1.10: Page faults
    Req 1.43: Thread migrations
    """

    # Default perf events to monitor
    # These cover the requirements from the MLSys paper
    DEFAULT_EVENTS = [
        "instructions",  # Req 1.5 - Instructions retired
        "cycles",  # Req 1.5 - CPU cycles
        "cache-references",  # Req 1.6 - Last level cache references
        "cache-misses",  # Req 1.6 - Last level cache misses
        "context-switches",  # Req 1.12 - Context switches (both types)
        "migrations",  # Req 1.43 - Thread migrations
        "page-faults",  # Req 1.10 - Page faults (major+minor)
        "branches",  # Branch instructions
        "branch-misses",  # Mispredicted branches
        "cpu-clock",  # CPU time used
        "task-clock",  # Task elapsed time
    ]

    # Regex patterns for parsing perf output
    # perf stat output format: "value unit event"
    PERF_LINE_PATTERN = re.compile(r"^\s*([0-9,]+)\s+([a-zA-Z%/]+)\s+([a-zA-Z0-9_-]+)$")

    def __init__(self, config: Dict):
        """
        Initialize perf reader with configuration.

        Args:
            config: Hardware configuration from Module 0.
                   May contain 'perf_events' list to override defaults.

        The constructor checks if perf is available and logs the status.
        If perf is not available, the reader will return empty counters
        but won't crash – experiments can still run with simulated data.
        """
        # Get perf events from config or use defaults
        hw_config = config.get("hardware", config)  # Handle different config structures
        self.events = hw_config.get("perf_events", self.DEFAULT_EVENTS)

        # Check if perf is available
        self.perf_available = self._check_perf_availability()
        self.perf_path = self._find_perf_path()

        if self.perf_available:
            logger.info(
                "PerfReader initialized with %d events: %s",
                len(self.events),
                self.events[:5],
            )
            logger.debug("Full event list: %s", self.events)
        else:
            logger.warning("Perf not available - performance counters disabled")
            logger.info("To enable perf: ensure kernel.perf_event_paranoid <= 1")

    def _find_perf_path(self) -> Optional[str]:
        """
        Find the path to the perf executable.

        Returns:
            Path to perf or None if not found
        """
        try:
            result = subprocess.run(
                ["which", "perf"], capture_output=True, text=True, timeout=1
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return None

    def _check_perf_availability(self) -> bool:
        """
        Check if perf command is available and has necessary permissions.

        This checks:
        1. Whether the perf command exists
        2. Whether we can run a simple perf stat command

        Returns:
            True if perf can be used, False otherwise
        """
        # First check if command exists
        if not self._find_perf_path():
            logger.debug("perf command not found")
            return False

        # Try to run a simple perf stat command
        try:
            cmd = [
                "perf",
                "stat",
                "-e",
                "instructions",
                "--field-separator=,",
                "--no-big-num",
                "--timeout",
                "10",
                "sleep",
                "0.01",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1)

            # perf writes to stderr, so check if we got any output
            # Return code 0 means success, but even with error we might get data
            return len(result.stderr) > 0

        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logger.debug("perf check failed: %s", e)
            return False
        except Exception as e:
            logger.debug("perf check error: %s", e)
            return False

    def start_process_measurement(self):
        """
        Start perf monitoring - just record start time.
        Actual measurement happens in stop().
        """
        self._measurement_start = time.time()
        logger.debug("Perf measurement started")

    def stop_process_measurement(self) -> PerformanceCounters:
        """
        Stop perf monitoring by running a timed measurement.
        Uses the same reliable method as the test.
        """
        if not hasattr(self, "_measurement_start"):
            logger.warning("No measurement start time found")
            return PerformanceCounters()

        duration_ms = int((time.time() - self._measurement_start) * 1000)
        logger.debug(f"Measuring perf for {duration_ms}ms")

        # Use the working test method
        counters = self.read_counters(duration_ms=duration_ms)

        logger.info(f"Perf collected: {counters.instructions_retired} instructions")
        return counters

    def _drain_perf_output(self):
        """
        Background thread that continuously reads perf output.
        Uses select() with timeout to prevent blocking.
        Starts immediately to capture all output.
        """
        import select

        logger.debug("Perf output drainer thread started")

        if not self.perf_process or not self.perf_process.stderr:
            logger.error("No perf process or stderr pipe")
            return

        # Check if pipe is readable
        import os

        fd = self.perf_process.stderr.fileno()
        logger.debug(f"Perf stderr pipe FD: {fd}")

        lines_read = 0
        while not self._stop_perf_reader:
            try:
                # Check if data is available to read (with 100ms timeout)
                reads, _, _ = select.select([self.perf_process.stderr], [], [], 0.1)

                if reads:
                    # Data available, read it
                    line = self.perf_process.stderr.readline()
                    if line:
                        self._perf_buffer.append(line)
                        lines_read += 1

                        # Log first few lines to verify data is flowing
                        if lines_read <= 5:
                            logger.debug(f"✅ Perf line {lines_read}: {line.strip()}")
                        elif lines_read == 6:
                            logger.debug(f"Perf drainer continuing to collect...")

                        # Log progress occasionally
                        if lines_read % 50 == 0:
                            buffer_size = len("".join(self._perf_buffer)) // 1024
                            logger.debug(
                                f"Perf drainer collected {lines_read} lines, buffer: {buffer_size}KB"
                            )
                    else:
                        # EOF - process probably died
                        logger.warning("EOF reached on perf stderr")
                        break
                else:
                    # No data available - log occasionally to show thread is alive
                    if lines_read == 0 and time.time() % 2 < 0.1:  # Every ~2 seconds
                        logger.debug("Perf drainer waiting for data...")

            except Exception as e:
                logger.error(f"Error in perf drainer thread: {e}")
                time.sleep(0.1)

        buffer_size = (
            len("".join(self._perf_buffer)) // 1024 if self._perf_buffer else 0
        )
        logger.info(
            f"Perf output drainer stopped, collected {lines_read} lines, buffer: {buffer_size}KB"
        )

    def read_counters(self, duration_ms: int = 100) -> PerformanceCounters:
        """
        Read performance counters for specified duration.

        This runs 'perf stat' for the specified duration and parses
        the output into a PerformanceCounters object.

        Args:
            duration_ms: How long to sample (milliseconds)
                        Shorter durations give higher resolution but more noise.
                        100ms is a good balance for AI workloads.

        Returns:
            PerformanceCounters object with all counter values.
            Returns empty counters (all zeros) if perf is not available.

        Req 1.5: Instructions, cycles, IPC
        Req 1.6: Cache references and misses
        Req 1.10: Page faults
        Req 1.43: Thread migrations

        Example:
            >>> reader = PerfReader(config)
            >>> counters = reader.read_counters(duration_ms=100)
            >>> print(f"IPC: {counters.instructions_per_cycle():.2f}")
        """
        counters = PerformanceCounters()

        if not self.perf_available or not self.perf_path:
            logger.debug("perf not available - returning empty counters")
            return counters

        # Build perf command
        # --timeout runs for specified milliseconds, then stops
        # --field-separator=, makes output CSV format (easier to parse)
        # --no-big-num prevents comma formatting in numbers (e.g., 1,234 -> 1234)
        event_str = ",".join(self.events)
        cmd = [
            "perf",
            "stat",
            "-e",
            event_str,
            "--field-separator=,",
            "--no-big-num",
            "--timeout",
            str(duration_ms),
        ]

        logger.debug("Running perf: %s", " ".join(cmd))

        try:
            # Run perf with timeout slightly longer than requested
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=(duration_ms / 1000) + 1
            )

            # ===== DEBUG: Print raw output =====
            """
            print("\n" + "="*50)
            print("RAW PERF STDERR OUTPUT:")
            print("="*50)
            print(result.stderr)
            print("="*50)
            print("RAW PERF STDOUT OUTPUT:")
            print("="*50)
            print(result.stdout)
            print("="*50)
            """
            # ===== END DEBUG =====

            # perf writes counter data to stderr
            self._parse_perf_output(result.stderr, counters)

            # Add duration metadata
            counters.duration_ms = duration_ms

            logger.debug(
                "perf counters: %d instructions, %d cycles, %.2f IPC",
                counters.instructions_retired,
                counters.cpu_cycles,
                counters.instructions_per_cycle(),
            )

        except subprocess.TimeoutExpired:
            logger.warning("perf stat timeout - counters may be incomplete")
        except Exception as e:
            logger.error("perf read failed: %s", e)

        return counters

    def read_interval_counters(
        self, interval_ms: int = 100, samples: int = 10
    ) -> List[PerformanceCounters]:
        """
        Read multiple samples over time.

        This is useful for capturing variation in performance counters
        during longer experiments, such as tracking how IPC changes
        across different phases of agentic AI execution.

        Args:
            interval_ms: Time between samples in milliseconds
            samples: Number of samples to take

        Returns:
            List of PerformanceCounters objects, one per sample

        Example:
            >>> # Monitor performance every 50ms for 1 second
            >>> samples = reader.read_interval_counters(interval_ms=50, samples=20)
            >>> for i, c in enumerate(samples):
            ...     print(f"Sample {i}: IPC={c.instructions_per_cycle():.2f}")
        """
        results = []

        for i in range(samples):
            # Take a sample
            counters = self.read_counters(duration_ms=interval_ms)
            results.append(counters)

            # Log progress for long-running experiments
            if samples > 10 and (i + 1) % 10 == 0:
                logger.debug(f"Completed {i+1}/{samples} samples")

            # Don't sleep after the last sample
            if i < samples - 1:
                time.sleep(interval_ms / 1000.0)

        logger.debug(f"Collected {len(results)} samples at {interval_ms}ms intervals")
        return results

    def _parse_perf_output(self, output: str, counters: PerformanceCounters) -> None:
        """
        Parse perf stat output and populate counters object.

        perf stat output format (varies by version):
        - Some versions: value,event,unit,...
        - Some versions: value,,event,...

        We handle both by looking for the event name position.

        Args:
            output: stderr output from perf stat
            counters: PerformanceCounters object to populate
        """
        for line in output.split("\n"):
            line = line.strip()
            if not line or any(
                x in line.lower() for x in ["performance", "seconds", "cpus utilized"]
            ):
                continue

            # Split CSV line
            parts = line.split(",")
            if len(parts) < 2:
                continue

            try:
                # First part is always the value
                value_str = parts[0].strip().replace(",", "")
                value = int(value_str)

                # Find the event name - it's the first non-empty part after index 0
                event_name = None
                for part in parts[1:]:
                    if part.strip():  # First non-empty part is the event name
                        event_name = part.strip().lower()
                        break

                if event_name:
                    self._map_event_to_counter(event_name, value, counters)

            except (ValueError, IndexError) as e:
                logger.debug("Failed to parse perf line '%s': %s", line, e)
                continue

    def _map_event_to_counter(
        self, event_name: str, value: int, counters: PerformanceCounters
    ) -> None:
        """
        Map a perf event name to the appropriate counter field.

        Args:
            event_name: perf event name (e.g., 'instructions')
            value: counter value
            counters: PerformanceCounters object to update
        """
        # Instructions retired (Req 1.5)
        if "instructions" in event_name:
            counters.instructions_retired = value

        # CPU cycles (Req 1.5)
        elif "cycles" in event_name:
            counters.cpu_cycles = value

        # Cache references (Req 1.6)
        elif "cache-references" in event_name:
            counters.cache_references = value

        # Cache misses (Req 1.6)
        elif "cache-misses" in event_name:
            counters.cache_misses = value

        # Context switches (Req 1.12)
        # perf combines both voluntary and involuntary
        elif "context-switches" in event_name:
            counters.context_switches_voluntary = value
            # We can't separate voluntary/involuntary from perf alone
            # Module 1 will also read /proc/self/status for breakdown

        # Thread migrations (Req 1.43)
        elif "migrations" in event_name:
            counters.thread_migrations = value

        # Page faults (Req 1.10)
        # perf doesn't separate major/minor
        elif "page-faults" in event_name:
            counters.minor_page_faults = value
            # major faults are handled separately via /proc

        # Branch instructions
        elif "branches" in event_name:
            counters.branches = value

        # Branch misses
        elif "branch-misses" in event_name:
            counters.branch_misses = value

        # CPU clock (ms)
        elif "cpu-clock" in event_name:
            counters.cpu_clock_ms = value / 1000.0  # Convert to ms

        # Task clock (ms)
        elif "task-clock" in event_name:
            counters.task_clock_ms = value / 1000.0  # Convert to ms

    def check_paranoid_setting(self) -> Optional[int]:
        """
        Check the current kernel.perf_event_paranoid setting.

        The paranoid setting controls access to perf events:
        - -1: No restrictions (recommended for research)
        - 0: Allow access to CPU-specific data
        - 1: Allow kernel and user-space measurements
        - 2: Allow only user-space measurements (default on many systems)
        - 3: Disable all perf events

        Returns:
            Current paranoid value, or None if cannot read
        """
        try:
            with open("/proc/sys/kernel/perf_event_paranoid", "r") as f:
                return int(f.read().strip())
        except Exception:
            return None

    def __str__(self) -> str:
        """String representation of perf reader state."""
        status = "available" if self.perf_available else "unavailable"
        return f"PerfReader({status}, {len(self.events)} events)"

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"PerfReader(events={self.events}, path={self.perf_path})"


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    """
    Example: Testing perf_reader with simple measurements.
    """

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("\n" + "=" * 70)
    print("PERF READER TEST")
    print("=" * 70)
    print(f"Python path: {sys.path[0]}")
    print(f"Project root: {project_root}")
    print("=" * 70)

    # Try to load config from Module 0
    config_path = project_root / "config" / "hw_config.json"
    if config_path.exists():
        import json

        with open(config_path) as f:
            config = json.load(f)
        print(f"✅ Loaded config from {config_path}")
    else:
        print("⚠️ No config file found, using defaults")
        config = {}

    # Initialize reader
    reader = PerfReader(config)
    print(f"📊 {reader}")

    # Check paranoid setting
    paranoid = reader.check_paranoid_setting()
    print(f"🔧 perf_event_paranoid = {paranoid}")

    # ===== TEST 1: Short sample (might be zero on idle) =====
    print("\n📝 Taking 100ms sample (may be zero on idle system)...")
    counters = reader.read_counters(duration_ms=100)

    print("\n" + "=" * 70)
    print("100ms SAMPLE RESULTS")
    print("=" * 70)
    print(f"Instructions: {counters.instructions_retired:,}")
    print(f"Cycles:       {counters.cpu_cycles:,}")
    print(f"IPC:          {counters.instructions_per_cycle():.2f}")

    # ===== TEST 2: Longer sample with workload =====
    print("\n" + "=" * 70)
    print("1 SECOND SAMPLE WITH WORKLOAD")
    print("=" * 70)
    print("Running CPU workload for 1 second...")

    # Create a simple workload function
    def busy_work(duration_ms):
        import math
        import time

        end_time = time.time() + (duration_ms / 1000)
        result = 0
        while time.time() < end_time:
            for i in range(10000):
                result += math.sqrt(i)
        return result

    # Measure during workload
    import threading
    import time

    result_container = []

    def run_workload():
        result_container.append(busy_work(1000))

    # Start workload in background
    workload_thread = threading.Thread(target=run_workload)
    workload_thread.start()

    # Measure during workload
    counters = reader.read_counters(duration_ms=1000)
    workload_thread.join()

    print(f"Instructions: {counters.instructions_retired:,}")
    print(f"Cycles:       {counters.cpu_cycles:,}")
    print(f"IPC:          {counters.instructions_per_cycle():.2f}")
    print(f"Cache refs:   {counters.cache_references:,}")
    print(f"Cache misses: {counters.cache_misses:,}")
    print(f"Miss rate:    {counters.cache_miss_rate():.2%}")

    # ===== TEST 3: Multiple samples =====
    print("\n" + "=" * 70)
    print("3 SAMPLES AT 200ms INTERVALS DURING WORKLOAD")
    print("=" * 70)

    def background_workload():
        busy_work(2000)  # 2 second workload

    workload_thread = threading.Thread(target=background_workload)
    workload_thread.start()

    samples = reader.read_interval_counters(interval_ms=200, samples=3)
    workload_thread.join()

    for i, c in enumerate(samples):
        print(
            f"   Sample {i+1}: {c.instructions_retired:,} instructions, {c.instructions_per_cycle():.2f} IPC"
        )

    print("\n✅ Test complete!")
