#!/usr/bin/env python3
"""
================================================================================
SCHEDULER MONITOR – Reads Linux scheduler statistics from /proc
================================================================================

This module reads various scheduler and system metrics from the /proc filesystem:
- Context switches (voluntary/involuntary) from /proc/self/status
- Kernel and user CPU times from /proc/stat
- Run queue length from /proc/loadavg
- Thread migrations from /proc/self/status (if available)

These metrics help quantify OS‑level overhead that contributes to the
orchestration tax in agentic AI workflows.

Requirements implemented:
- Req 1.12: Kernel Context Switches
- Req 1.23: Kernel/User Ratio
- Req 1.24: Voluntary Switch Delay (proxy)
- Req 1.36: Run Queue Length

Author: Deepak Panigrahy
================================================================================
"""

import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# ============================================================================
# Fix Python path – ensure core modules are importable
# ============================================================================
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.utils.debug import dprint, init_debug_from_env, trace

init_debug_from_env()
logger = logging.getLogger(__name__)


class SchedulerMonitor:
    """
    Reads Linux scheduler metrics from /proc filesystem.

    This class provides methods to read:
    - Voluntary and involuntary context switches (Req 1.12)
    - Kernel and user CPU times (Req 1.23)
    - Run queue length (Req 1.36)
    - Thread migrations (optional, can be derived from perf_reader)

    All values are read at a single point in time. For rates or deltas,
    the caller should take two readings and compute differences.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the scheduler monitor.

        Args:
            config: Configuration dictionary (may contain paths, but typically not needed).
        """
        self.config = config
        self._page_size = os.sysconf("SC_PAGESIZE")  # for RSS, not used here
        dprint("SchedulerMonitor initialized")
        # NEW: Interrupt sampling state
        self._interrupt_sampling_active = False
        self._interrupt_samples = []
        self._last_interrupt_counts = None
        self._last_sample_time_ns = None
        self._start_epoch_ns = None
        self._start_monotonic_ns = None

    def read_context_switches(self) -> Tuple[int, int]:
        """
        Read voluntary and involuntary context switches from /proc/self/status.

        Returns:
            Tuple (voluntary, involuntary) as integers.
            If file cannot be read, returns (0, 0).

        Req 1.12: Kernel Context Switches.
        """
        path = "/proc/self/status"
        vol = 0
        invol = 0
        try:
            with open(path, "r") as f:
                for line in f:
                    if line.startswith("voluntary_ctxt_switches:"):
                        vol = int(line.split(":")[1].strip())
                    elif line.startswith("nonvoluntary_ctxt_switches:"):
                        invol = int(line.split(":")[1].strip())
        except Exception as e:
            logger.warning(f"Could not read {path}: {e}")
        dprint(f"Context switches: voluntary={vol}, involuntary={invol}")
        return vol, invol

    def read_cpu_times(self) -> Tuple[float, float]:
        """
        Read kernel and user CPU times from /proc/stat.

        Returns:
            Tuple (user_time_seconds, system_time_seconds) since boot.
            These are aggregate times across all CPUs.

        Note: To get time spent during a measurement interval, the caller must
        take two readings and compute the difference.

        Req 1.23: Kernel/User Ratio.
        """
        user = 0.0
        system = 0.0
        try:
            with open("/proc/stat", "r") as f:
                first_line = f.readline()
                if first_line.startswith("cpu "):
                    parts = first_line.split()
                    # user, nice, system, idle, iowait, irq, softirq, steal, guest, guest_nice
                    # indices: 1:user, 2:nice, 3:system
                    user = float(parts[1]) + float(parts[2])  # user + nice
                    system = float(parts[3])
        except Exception as e:
            logger.warning(f"Could not read /proc/stat: {e}")
        dprint(f"CPU times: user={user:.2f}, system={system:.2f}")
        return user, system

    def read_loadavg(self) -> Dict[str, float]:
        """
        Read load averages from /proc/loadavg.

        Returns:
            Dictionary with keys:
                - 'load1': 1‑minute average
                - 'load5': 5‑minute average
                - 'load15': 15‑minute average
                - 'runnable': number of currently runnable tasks
                - 'total_tasks': total number of tasks
                - 'last_pid': last used PID

        Req 1.36: Run Queue Length (approximated by the 1‑minute load average).
        """
        result = {
            "load1": 0.0,
            "load5": 0.0,
            "load15": 0.0,
            "runnable": 0,
            "total_tasks": 0,
            "last_pid": 0,
        }
        try:
            with open("/proc/loadavg", "r") as f:
                line = f.read().strip()
                parts = line.split()
                if len(parts) >= 5:
                    result["load1"] = float(parts[0])
                    result["load5"] = float(parts[1])
                    result["load15"] = float(parts[2])
                    runnable_total = parts[3].split("/")
                    if len(runnable_total) == 2:
                        result["runnable"] = int(runnable_total[0])
                        result["total_tasks"] = int(runnable_total[1])
                    result["last_pid"] = int(parts[4])
        except Exception as e:
            logger.warning(f"Could not read /proc/loadavg: {e}")
        dprint("Load averages:", **result)
        return result

    def read_all(self) -> Dict[str, Any]:
        """
        Convenience method to read all scheduler metrics at once.

        Returns:
            Dictionary containing all metrics:
                - voluntary_switches
                - involuntary_switches
                - user_time
                - system_time
                - load1, load5, load15, runnable, total_tasks
        """
        vol, invol = self.read_context_switches()
        user_time, system_time = self.read_cpu_times()
        load = self.read_loadavg()
        swap = self.get_swap_metrics()
        result = {
            "voluntary_switches": vol,
            "involuntary_switches": invol,
            "user_time": user_time,
            "system_time": system_time,
            **load,
            "swap": swap,
        }
        dprint("Scheduler snapshot:", **result)
        return result

    def __str__(self) -> str:
        return "SchedulerMonitor(procfs)"

    def get_swap_metrics(self) -> Dict[str, float]:
        """
        Get swap usage metrics from /proc/meminfo.

        Returns:
            Dictionary with swap metrics:
            - swap_total_mb: Total swap in MB
            - swap_free_mb: Free swap in MB
            - swap_used_mb: Used swap in MB
            - swap_percent: Percentage of swap used
            - swap_cached_mb: Swap cached in memory (if available)
        """
        swap_metrics = {
            "swap_total_mb": 0.0,
            "swap_free_mb": 0.0,
            "swap_used_mb": 0.0,
            "swap_percent": 0.0,
            "swap_cached_mb": 0.0,
        }

        try:
            with open("/proc/meminfo", "r") as f:
                for line in f:
                    line = line.strip()
                    if "SwapTotal:" in line:
                        kb = int(line.split()[1])
                        swap_metrics["swap_total_mb"] = kb / 1024.0
                    elif "SwapFree:" in line:
                        kb = int(line.split()[1])
                        swap_metrics["swap_free_mb"] = kb / 1024.0
                    elif "SwapCached:" in line:
                        kb = int(line.split()[1])
                        swap_metrics["swap_cached_mb"] = kb / 1024.0

            # Calculate used swap and percentage
            if swap_metrics["swap_total_mb"] > 0:
                swap_metrics["swap_used_mb"] = (
                    swap_metrics["swap_total_mb"] - swap_metrics["swap_free_mb"]
                )
                swap_metrics["swap_percent"] = (
                    swap_metrics["swap_used_mb"] / swap_metrics["swap_total_mb"]
                ) * 100.0

            logger.debug(
                f"Swap metrics: total={swap_metrics['swap_total_mb']:.1f}MB, "
                f"used={swap_metrics['swap_used_mb']:.1f}MB, "
                f"cached={swap_metrics['swap_cached_mb']:.1f}MB"
            )

        except Exception as e:
            logger.debug(f"Could not read swap metrics: {e}")

        return swap_metrics

    def start_interrupt_sampling(self):
        """Start collecting interrupt samples."""
        self._interrupt_sampling_active = True
        self._interrupt_samples = []
        self._last_interrupt_counts = self._read_total_interrupts()

        # Capture clock alignment ONCE at the beginning
        start_mono = time.monotonic_ns()
        start_epoch = time.time_ns()

        self._start_monotonic_ns = start_mono
        self._start_epoch_ns = start_epoch
        self._last_sample_time_ns = start_mono

        logger.debug(
            f"Interrupt sampling started - epoch: {start_epoch}, mono: {start_mono}"
        )

    def _read_total_interrupts(self) -> int:
        """Read total interrupt count from /proc/stat."""
        try:
            with open("/proc/stat", "r") as f:
                for line in f:
                    if line.startswith("intr "):
                        return int(line.split()[1])
        except Exception:
            pass
        return 0

    def reset_interrupt_samples(self):
        """Clear interrupt samples buffer for new run."""
        self._interrupt_samples = []
        self._last_interrupt_counts = self._read_total_interrupts()

        # DO NOT reset the start times here!
        # Only update the last sample time for delta calculation
        self._last_sample_time_ns = time.monotonic_ns()

        logger.debug("Interrupt samples reset for new run (start times preserved)")

    def sample_interrupts(self):
        """Take one interrupt sample (called from energy_engine sampling loop)."""
        if not self._interrupt_sampling_active:
            return

        monotonic_now = time.monotonic_ns()
        current_counts = self._read_total_interrupts()

        # Convert monotonic to epoch time
        if self._start_epoch_ns is not None and self._start_monotonic_ns is not None:
            epoch_ns = int(
                self._start_epoch_ns + (monotonic_now - self._start_monotonic_ns)
            )
        else:
            # Fallback if start times not set (should not happen)
            epoch_ns = monotonic_now
            logger.warning("Start times not set for interrupt sampling")

        logger.debug(
            f"🔍 INTERRUPT DEBUG: counts={current_counts}, last={self._last_interrupt_counts}"
        )

        if (
            self._last_interrupt_counts is not None
            and self._last_sample_time_ns is not None
        ):
            time_delta_s = (monotonic_now - self._last_sample_time_ns) / 1e9
            if time_delta_s > 0:
                count_delta = current_counts - self._last_interrupt_counts
                rate = count_delta / time_delta_s
                logger.debug(f"🔍 INTERRUPT RATE: {rate:.2f} IRQ/s")

                self._interrupt_samples.append(
                    {
                        "timestamp_ns": epoch_ns,  # Now in epoch time!
                        "interrupts_per_sec": rate,
                    }
                )

        self._last_interrupt_counts = current_counts
        self._last_sample_time_ns = monotonic_now

    def stop_interrupt_sampling(self) -> list:
        """Stop and return interrupt samples."""
        self._interrupt_sampling_active = False
        samples = self._interrupt_samples.copy()
        self._interrupt_samples = []
        logger.debug(f"🔍 INTERRUPT STOP: collected {len(samples)} samples")
        return samples


# ============================================================================
# Example usage (standalone test)
# ============================================================================
if __name__ == "__main__":
    import time

    logging.basicConfig(level=logging.INFO)
    monitor = SchedulerMonitor({})
    print("=" * 70)
    print("SCHEDULER MONITOR TEST")
    print("=" * 70)

    # First reading
    print("\n📊 Initial snapshot:")
    start = monitor.read_all()
    print(
        f"   Context switches: vol={start['voluntary_switches']}, invol={start['involuntary_switches']}"
    )
    print(
        f"   CPU times: user={start['user_time']:.2f}, system={start['system_time']:.2f}"
    )
    print(
        f"   Load avg: {start['load1']} (1min), {start['load5']} (5min), {start['load15']} (15min)"
    )
    print(f"   Runnable tasks: {start['runnable']}/{start['total_tasks']}")

    # NEW: Print swap metrics
    if "swap" in start:
        swap = start["swap"]
        print(f"\n💾 Swap metrics:")
        print(f"   Total: {swap['swap_total_mb']:.1f} MB")
        print(f"   Used:  {swap['swap_used_mb']:.1f} MB ({swap['swap_percent']:.1f}%)")
        print(f"   Free:  {swap['swap_free_mb']:.1f} MB")
        print(f"   Cached: {swap['swap_cached_mb']:.1f} MB")

    # Wait
    print("\n⏳ Waiting 2 seconds...")
    time.sleep(2)

    # Second reading
    print("\n📊 Final snapshot:")
    end = monitor.read_all()
    print(
        f"   Context switches: vol={end['voluntary_switches']}, invol={end['involuntary_switches']}"
    )
    print(f"   CPU times: user={end['user_time']:.2f}, system={end['system_time']:.2f}")

    # Deltas
    print("\n📈 Deltas:")
    vol_delta = end["voluntary_switches"] - start["voluntary_switches"]
    invol_delta = end["involuntary_switches"] - start["involuntary_switches"]
    user_delta = end["user_time"] - start["user_time"]
    system_delta = end["system_time"] - start["system_time"]

    print(f"   Context switches: vol=+{vol_delta}, invol=+{invol_delta}")
    print(f"   CPU time: user=+{user_delta:.2f}, system=+{system_delta:.2f}")
    print(f"   Load avg: {end['load1']}")

    # NEW: Swap delta (should be near zero in normal operation)
    if "swap" in end and "swap" in start:
        print(f"\n💾 Swap changes:")
        swap_start = start["swap"]
        swap_end = end["swap"]
        used_delta = swap_end["swap_used_mb"] - swap_start["swap_used_mb"]
        print(f"   Used memory change: {used_delta:+.2f} MB")

    print("\n" + "=" * 70)
    print("✅ Test complete!")
    print("=" * 70)

    # ===== NEW: Test interrupt sampling =====
    print("\n" + "=" * 70)
    print("INTERRUPT SAMPLING TEST")
    print("=" * 70)

    monitor.start_interrupt_sampling()
    print("Sampling interrupts for 2 seconds at 10 Hz...")

    # Simulate 10 Hz sampling for 2 seconds
    for _ in range(20):  # 20 samples at 0.1s intervals = 2 seconds
        time.sleep(0.1)
        monitor.sample_interrupts()
        print(".", end="", flush=True)
    print()  # newline

    samples = monitor.stop_interrupt_sampling()

    print(f"Collected {len(samples)} interrupt samples")
    if samples:
        print("First 3 samples:")
        for i, s in enumerate(samples[:3]):
            # Convert timestamp_ns to readable time if needed
            rate = s["interrupts_per_sec"]
            print(f"  {i+1}: rate={rate:.2f} IRQ/s")
    else:
        print(
            "❌ No samples collected – check that _read_total_interrupts() is working"
        )
    print("=" * 70)
