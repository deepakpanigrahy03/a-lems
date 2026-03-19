#!/usr/bin/env python3
"""
================================================================================
MSR READER - Direct MSR Access with C Helper and TSC Conversion
================================================================================

Author: Deepak Panigrahy

This module provides MSR access using a privileged C helper binary.
The C binary has CAP_SYS_RAWIO set, so Python can run without sudo.

Key Features:
- Uses C helper binary (no sudo needed for Python)
- Falls back to direct access if helper unavailable
- Reads TSC frequency from hw_config.json for accurate C-state conversion
- Converts C-state counters from TSC ticks to real time (seconds/microseconds)
- CPU pinning for consistent measurements
- Baseline loading (stable hardware properties)
- Dynamic measurement methods for per‑run counters

Requirements Covered:
- Req 1.21: Ring Bus Frequency
- Req 1.27: C-State Transition Counters (now properly converted to time)
- Req 1.47: Wake-up Latency
- Req 1.9: Thermal Throttling

================================================================================
"""

import json
import logging
import os
import platform
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ============================================================================
# Fix Python path
# ============================================================================
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.utils.debug import dprint, init_debug_from_env, trace

init_debug_from_env()
logger = logging.getLogger(__name__)


class MSRReader:
    """
    High-performance MSR reader using C helper binary with TSC conversion.
    """

    # Intel MSR addresses (from SDM)
    MSR_ADDRESSES = {
        # Ring bus (uncore) frequency
        "MSR_UNCORE_PERF_STATUS": 0x621,
        "MSR_PLATFORM_INFO": 0xCE,
        # C-state residency counters (time in TSC ticks)
        "MSR_PKG_C2_RESIDENCY": 0x60D,
        "MSR_PKG_C3_RESIDENCY": 0x3F8,
        "MSR_PKG_C6_RESIDENCY": 0x3F9,
        "MSR_PKG_C7_RESIDENCY": 0x3FA,
        "MSR_PKG_C8_RESIDENCY": 0x630,
        "MSR_PKG_C9_RESIDENCY": 0x631,
        "MSR_PKG_C10_RESIDENCY": 0x632,
        # Thermal throttling
        "MSR_IA32_THERM_STATUS": 0x19C,
        "MSR_IA32_PACKAGE_THERM_STATUS": 0x1B1,
        # APERF/MPERF for actual frequency
        "MSR_IA32_APERF": 0xE8,
        "MSR_IA32_MPERF": 0xE7,
    }

    BASELINE_PATH = project_root / "data" / "msr_baseline.json"
    HELPER_PATH = Path(__file__).parent.parent / "msr_helper" / "msr_read"

    def __init__(
        self,
        config: Dict[str, Any],
        use_baseline: bool = True,
        require_baseline: bool = False,
    ):
        """
        Initialize MSR reader.

        Args:
            config: Combined config (hw_config + settings)
            use_baseline: If True, attempt to load baseline from file.
            require_baseline: If True and baseline missing, raise FileNotFoundError.
        """
        self.msr_config = config.get("msr", {})
        self.cpu_count = self.msr_config.get("count", os.cpu_count() or 8)
        self.msr_fds = []
        self.rdmsr_available = False
        self.helper_available = False
        self.start_cstate_counters = None
        self.end_cstate_counters = None
        self._cstate_start = None
        self._cstate_end = None

        # Load hardware-specific parameters from config (fallbacks)
        self.cstate_max = self.msr_config.get("cstate_counter_max", 2**64 - 1)
        self.wakeup_idle_s = self.msr_config.get("wakeup_idle_ms", 1) / 1000.0
        self.ring_bus_base_clock = self.msr_config.get("ring_bus_base_clock_mhz", 100.0)

        # Load ring bus sysfs paths from config
        ring_bus_config = config.get("ring_bus", {})
        self.ring_bus_sysfs_paths = ring_bus_config.get("sysfs_paths", {})
        if self.ring_bus_sysfs_paths:
            logger.debug(
                f"Loaded ring bus sysfs paths: {list(self.ring_bus_sysfs_paths.keys())}"
            )

        # ====================================================================
        # NEW: Get TSC frequency from config (auto-detected by detect_hardware.py)
        # ====================================================================
        cpu_config = config.get("cpu", {})
        self.tsc_frequency_hz = cpu_config.get("tsc_frequency_hz")

        if self.tsc_frequency_hz:
            logger.info(
                f"TSC frequency: {self.tsc_frequency_hz/1e6:.0f} MHz (from config)"
            )
        else:
            logger.warning(
                "TSC frequency not in config - C-state conversions will be approximate"
            )
            # Fallback: estimate from CPU base frequency (rough approximation)
            self.tsc_frequency_hz = int(self.ring_bus_base_clock * 1_000_000 * 28)

        # Load experiment settings (from app_settings.yaml)
        settings = config.get("settings", {})
        msr_settings = settings.get("msr", {})
        self.enable_aperf_mperf = msr_settings.get("enable_aperf_mperf", False)
        self.wakeup_iterations = msr_settings.get("wakeup_latency_iterations", 500)
        self.measure_cores = msr_settings.get("measure_cores", [0])
        self.log_interval = settings.get("logging", {}).get(
            "msr_progress_interval", 100
        )

        # Record system info
        self.cpu_model = self._get_cpu_model()
        self.kernel_version = platform.release()
        logger.info(f"CPU model: {self.cpu_model}")
        logger.info(f"Kernel: {self.kernel_version}")

        # Baseline storage
        self.baseline = {}
        if use_baseline:
            self._load_baseline(require_baseline)

        # ====================================================================
        # Check for C helper binary
        # ====================================================================
        self._check_helper()

        # Open MSR devices as fallback
        self._open_msr_devices()

        self._pinned_cpu = None

    def __del__(self):
        """Clean up MSR file descriptors if they were opened."""
        if hasattr(self, "msr_fds") and self.msr_fds:
            for fd in self.msr_fds:
                try:
                    os.close(fd)
                except:
                    pass

    # ------------------------------------------------------------------------
    # Helper binary management
    # ------------------------------------------------------------------------
    def _check_helper(self):
        """Check if C helper binary exists and is executable."""

        if self.HELPER_PATH.exists() and os.access(self.HELPER_PATH, os.X_OK):
            self.helper_available = True
            logger.info(f"C helper found at {self.HELPER_PATH}")
        else:
            self.helper_available = False
            logger.warning(
                f"C helper not found at {self.HELPER_PATH}. Will use direct access (may need sudo)."
            )

    # ------------------------------------------------------------------------
    # Direct MSR access (fallback)
    # ------------------------------------------------------------------------
    def _open_msr_devices(self):
        """Open MSR devices as fallback access method."""
        for cpu in range(self.cpu_count):
            msr_path = f"/dev/cpu/{cpu}/msr"
            try:
                fd = os.open(msr_path, os.O_RDONLY)
                self.msr_fds.append(fd)
            except (FileNotFoundError, PermissionError, OSError) as e:
                logger.debug(f"Cannot open {msr_path}: {e}")
                for opened_fd in self.msr_fds:
                    os.close(opened_fd)
                self.msr_fds = []
                break

        if self.msr_fds:
            self.rdmsr_available = True
            logger.info(f"Direct MSR access available with {len(self.msr_fds)} CPUs")
        else:
            logger.debug("Direct MSR access not available")

    # ------------------------------------------------------------------------
    # Core MSR read (with helper priority)
    # ------------------------------------------------------------------------
    @trace
    def read_msr(self, msr_addr: int, cpu: int = 0, pin: bool = True) -> Optional[int]:
        """
        Read MSR value using C helper (preferred) or direct access (fallback).
        No sudo needed if helper is available.
        """
        was_pinned = self._pinned_cpu
        if pin and was_pinned != cpu:
            self.pin_to_cpu(cpu)

        try:
            # Method 1: Use C helper (no sudo needed)
            if self.helper_available:
                value = self._read_msr_helper(msr_addr, cpu)
                if value is not None:
                    return value

            # Method 2: Direct access (may need sudo)
            if self.rdmsr_available:
                value = self._read_msr_direct(msr_addr, cpu)
                if value is not None:
                    return value

            logger.debug(f"All MSR access methods failed for 0x{msr_addr:X}")
            return None

        finally:
            if pin and was_pinned != cpu and was_pinned is not None:
                self.pin_to_cpu(was_pinned)
            elif pin and was_pinned is None:
                self.unpin()

    def _read_msr_helper(self, msr_addr: int, cpu: int) -> Optional[int]:
        """Read MSR using C helper binary."""
        try:
            cmd = [str(self.HELPER_PATH), str(cpu), f"0x{msr_addr:X}"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1)

            if result.returncode == 0:
                value = int(result.stdout.strip())
                dprint(f"MSR[0x{msr_addr:X}] on CPU{cpu} = {value} (helper)")
                return value
            else:
                logger.debug(f"Helper failed: {result.stderr}")
                return None

        except Exception as e:
            logger.debug(f"Helper error: {e}")
            return None

    def _read_msr_direct(self, msr_addr: int, cpu: int) -> Optional[int]:
        """Read MSR directly via /dev/cpu/*/msr."""
        if cpu >= len(self.msr_fds):
            return None

        fd = self.msr_fds[cpu]
        try:
            data = os.pread(fd, 8, msr_addr)
            value = struct.unpack("<Q", data)[0]
            dprint(f"MSR[0x{msr_addr:X}] on CPU{cpu} = {value} (direct)")
            return value
        except Exception as e:
            logger.debug(f"Direct MSR read failed: {e}")
            return None

    def read_msr_all_cpus(self, msr_addr: int, pin: bool = True) -> Dict[int, int]:
        """Read an MSR from all CPUs."""
        results = {}
        for cpu in range(self.cpu_count):
            val = self.read_msr(msr_addr, cpu, pin=pin)
            if val is not None:
                results[cpu] = val
        return results

    # ------------------------------------------------------------------------
    # TSC tick conversion helpers
    # ------------------------------------------------------------------------
    def ticks_to_seconds(self, ticks: int) -> float:
        """Convert TSC ticks to seconds."""
        if self.tsc_frequency_hz and ticks > 0:
            return ticks / self.tsc_frequency_hz
        return 0.0

    def ticks_to_microseconds(self, ticks: int) -> float:
        """Convert TSC ticks to microseconds."""
        if self.tsc_frequency_hz and ticks > 0:
            return (ticks / self.tsc_frequency_hz) * 1_000_000
        return 0.0

    def ticks_to_human(self, ticks: int) -> str:
        """Convert TSC ticks to human-readable string."""
        seconds = self.ticks_to_seconds(ticks)
        if seconds < 60:
            return f"{seconds:.2f} seconds"
        elif seconds < 3600:
            return f"{seconds/60:.2f} minutes"
        elif seconds < 86400:
            return f"{seconds/3600:.2f} hours"
        else:
            return f"{seconds/86400:.2f} days"

    # ------------------------------------------------------------------------
    # Baseline handling
    # ------------------------------------------------------------------------
    def _load_baseline(self, require: bool = False):
        """Load baseline from JSON file."""
        if not self.BASELINE_PATH.exists():
            msg = f"MSR baseline file not found: {self.BASELINE_PATH}"
            if require:
                raise FileNotFoundError(msg)
            else:
                logger.warning(msg + " – continuing without baseline.")
                return
        try:
            with open(self.BASELINE_PATH, "r") as f:
                self.baseline = json.load(f)
            logger.info(f"Loaded MSR baseline from {self.BASELINE_PATH}")
            measurements = self.baseline.get("measurements", {})
            ring_freq = measurements.get("ring_bus_frequency_mhz")
            if ring_freq:
                logger.info(f"   Ring bus freq: {ring_freq} MHz")
            else:
                logger.info(f"   Ring bus freq: Not available")
            wake_lat = measurements.get("wakeup_latency_us")
            if wake_lat:
                logger.info(f"   Wake-up latency: {wake_lat:.2f} µs")
        except Exception as e:
            logger.error(f"Failed to load MSR baseline: {e}")
            if require:
                raise

    def get_baseline_dict(self) -> Dict[str, Any]:
        """Return a copy of the baseline dictionary."""
        return self.baseline.copy()

    def get_ring_bus_frequency(self) -> Optional[float]:
        """Return cached ring bus frequency from baseline."""
        return self.baseline.get("measurements", {}).get("ring_bus_frequency_mhz")

    def get_wakeup_latency(self) -> Optional[float]:
        """Return cached wake-up latency from baseline."""
        return self.baseline.get("measurements", {}).get("wakeup_latency_us")

    # ------------------------------------------------------------------------
    # CPU Wakeup Count - Track C-state exits
    # ------------------------------------------------------------------------
    def read_cstate_counters_for_wakeup(
        self, cpu: int = 0, pin: bool = True
    ) -> Dict[str, int]:
        """
        Read C-state counters needed for wakeup calculation.

        Returns:
            Dictionary with C6 and C7 counter values
        """
        counters = {}

        # Read C6 counter
        c6_val = self.read_msr(self.MSR_ADDRESSES["MSR_PKG_C6_RESIDENCY"], cpu, pin=pin)
        if c6_val is not None:
            counters["c6"] = c6_val

        # Read C7 counter
        c7_val = self.read_msr(self.MSR_ADDRESSES["MSR_PKG_C7_RESIDENCY"], cpu, pin=pin)
        if c7_val is not None:
            counters["c7"] = c7_val

        return counters

    def snapshot_cstate_counters(
        self, cpu: int = 0, pin: bool = True
    ) -> Dict[str, Any]:
        """
        Snapshot raw C-state residency MSR counters.

        Returns RAW counter values with timestamp. This is the SINGLE source of truth
        for C-state measurements. Unit conversion happens AFTER delta calculation
        using TSC frequency.

        Args:
            cpu: CPU core to read from (default 0)
            pin: Whether to pin thread to CPU

        Returns:
            Dictionary with timestamp and counters (NEVER contains None values)
        """
        # Initialize counters dictionary
        counters = {}
        # Track which reads succeeded for debugging
        read_status = {}

        # Map state names to MSR addresses
        msr_map = {
            "c2": 0x3F8,  # MSR_PKG_C2_RESIDENCY
            "c3": 0x3F9,  # MSR_PKG_C3_RESIDENCY
            "c6": 0x3FA,  # MSR_PKG_C6_RESIDENCY
            "c7": 0x3FB,  # MSR_PKG_C7_RESIDENCY
        }

        # Read each C-state counter
        for state, addr in msr_map.items():
            try:
                # Attempt to read MSR value
                value = self.read_msr(addr, cpu, pin=pin)

                # CRITICAL FIX: Convert None to 0 explicitly
                if value is None:
                    logger.debug(f"MSR read for {state} returned None, using 0")
                    counters[state] = 0
                    read_status[state] = False
                else:
                    counters[state] = value
                    read_status[state] = True

            except Exception as e:
                # Any exception means we couldn't read this counter
                logger.debug(f"Failed to read {state} MSR: {e}")
                counters[state] = 0
                read_status[state] = False

        # Return complete snapshot with metadata
        return {
            "timestamp": time.time(),
            "counters": counters,
            "read_status": read_status,  # For debugging failed reads
            "cpu": cpu,  # Record which CPU was used
            "method": "helper" if self.helper_available else "direct",
        }

    def calculate_wakeup_delta(
        self, start_counters: Dict[str, int], end_counters: Dict[str, int]
    ) -> int:
        """
        Calculate number of wakeups between two snapshots.

        Each time the CPU wakes from deep sleep (C6/C7), the counter increments.
        By comparing start and end values, we can count total wakeups during
        the experiment.

        Args:
            start_counters: C-state counters from start of experiment
            end_counters: C-state counters from end of experiment

        Returns:
            Estimated number of wakeups during the experiment
        """
        wakeups = 0

        # Calculate delta for C6
        if "c6" in start_counters and "c6" in end_counters:
            delta_c6 = end_counters["c6"] - start_counters["c6"]
            if delta_c6 < 0:
                delta_c6 += self.cstate_max  # Handle wrap-around
            wakeups += delta_c6

        # Calculate delta for C7
        if "c7" in start_counters and "c7" in end_counters:
            delta_c7 = end_counters["c7"] - start_counters["c7"]
            if delta_c7 < 0:
                delta_c7 += self.cstate_max  # Handle wrap-around
            wakeups += delta_c7

        logger.debug(
            f"CPU wakeup delta: {wakeups} (C6: {delta_c6 if 'c6' in locals() else 0}, "
            f"C7: {delta_c7 if 'c7' in locals() else 0})"
        )

        return wakeups

    # ------------------------------------------------------------------------
    # CPU pinning
    # ------------------------------------------------------------------------
    def pin_to_cpu(self, cpu: int) -> bool:
        """Pin the current thread to a specific CPU core."""
        if cpu < 0 or cpu >= self.cpu_count:
            logger.error(f"Invalid CPU index {cpu}")
            return False
        try:
            os.sched_setaffinity(0, {cpu})
            self._pinned_cpu = cpu
            logger.debug(f"Pinned to CPU {cpu}")
            return True
        except Exception as e:
            logger.error(f"Failed to pin to CPU {cpu}: {e}")
            return False

    def unpin(self):
        """Unpin (allow thread to run on any CPU)."""
        try:
            os.sched_setaffinity(0, set(range(self.cpu_count)))
            self._pinned_cpu = None
            logger.debug("Unpinned")
        except Exception as e:
            logger.error(f"Failed to unpin: {e}")

    # ------------------------------------------------------------------------
    # System info
    # ------------------------------------------------------------------------
    def _get_cpu_model(self) -> str:
        try:
            with open("/proc/cpuinfo", "r") as f:
                for line in f:
                    if line.startswith("model name"):
                        return line.split(":", 1)[1].strip()
        except:
            pass
        return "Unknown"

    # ------------------------------------------------------------------------
    # Ring bus frequency measurement
    # ------------------------------------------------------------------------
    def get_ring_limits(
        self, cpu: int = 0, pin: bool = True
    ) -> Optional[Dict[str, float]]:
        """
        Extract min and max ring bus frequencies from MSR 0x621.
        Returns dict with 'min_mhz' and 'max_mhz' in MHz.
        """
        val = self.read_msr(0x621, cpu, pin=pin)
        if val is None:
            return None

        # Bits 0-6: MIN ratio
        # Bits 8-14: MAX ratio
        min_ratio = val & 0x7F
        max_ratio = (val >> 8) & 0x7F

        base_clock = 100  # MHz

        return {"min_mhz": min_ratio * base_clock, "max_mhz": max_ratio * base_clock}

    def _read_uncore_from_sysfs(self) -> Optional[float]:
        """
        Read current uncore frequency from sysfs using paths from config.
        """
        if not self.ring_bus_sysfs_paths:
            dprint("❌ RING BUS DEBUG: No sysfs paths in config")
            logger.debug("No ring bus sysfs paths in config")
            return None

        # Try current_freq first (most accurate)
        current_path = self.ring_bus_sysfs_paths.get("current_freq")
        dprint(f"🔍 RING BUS DEBUG: Trying current_freq path: {current_path}")
        if current_path and os.path.exists(current_path):
            try:
                with open(current_path, "r") as f:
                    freq_khz = int(f.read().strip())
                    dprint(
                        f"✅ RING BUS DEBUG: Read {freq_khz} kHz from {current_path}"
                    )
                    logger.debug(
                        f"Current ring bus freq: {freq_khz} kHz from {current_path}"
                    )
                    return freq_khz / 1000.0
            except (IOError, OSError, ValueError) as e:
                dprint(f"❌ RING BUS DEBUG: Error reading {current_path}: {e}")
                logger.debug(f"Could not read {current_path}: {e}")
        else:
            dprint(f"❌ RING BUS DEBUG: Path doesn't exist: {current_path}")

        # Fallback to initial_max_freq (boot-time max)
        init_max_path = self.ring_bus_sysfs_paths.get("initial_max_freq")
        dprint(f"🔍 RING BUS DEBUG: Trying initial_max_freq path: {init_max_path}")
        if init_max_path and os.path.exists(init_max_path):
            try:
                with open(init_max_path, "r") as f:
                    freq_khz = int(f.read().strip())
                    dprint(
                        f"✅ RING BUS DEBUG: Read {freq_khz} kHz from {init_max_path}"
                    )
                    logger.debug(
                        f"Initial max ring bus freq: {freq_khz} kHz from {init_max_path}"
                    )
                    return freq_khz / 1000.0
            except (IOError, OSError, ValueError) as e:
                dprint(f"❌ RING BUS DEBUG: Error reading {init_max_path}: {e}")
                logger.debug(f"Could not read {init_max_path}: {e}")
        else:
            dprint(f"❌ RING BUS DEBUG: Path doesn't exist: {init_max_path}")

        return None

    def measure_ring_bus_frequency(self) -> Optional[float]:
        """
        Measure current ring bus frequency using best available method:
        1. sysfs current_freq_khz (most accurate)
        2. Average of min/max from MSR (fallback)
        3. None if unavailable
        """
        # Method 1: sysfs current frequency (live reading)
        dprint("🔍 RING BUS DEBUG: Attempting sysfs read...")
        freq = self._read_uncore_from_sysfs()
        if freq is not None:
            dprint(f"✅ RING BUS DEBUG: sysfs success! freq={freq:.1f} MHz")
            logger.debug(f"Ring bus frequency (sysfs): {freq:.1f} MHz")
            return freq
        else:
            dprint("❌ RING BUS DEBUG: sysfs returned None")

        # Method 2: Average of min/max from MSR
        dprint("🔍 RING BUS DEBUG: Falling back to MSR...")
        limits = self.get_ring_limits()
        if limits:
            avg_freq = (limits["min_mhz"] + limits["max_mhz"]) / 2
            dprint(
                f"⚠️ RING BUS DEBUG: MSR average = {avg_freq:.1f} MHz (min={limits['min_mhz']:.0f}, max={limits['max_mhz']:.0f})"
            )
            logger.debug(
                f"Ring bus frequency (MSR average): {avg_freq:.1f} MHz "
                f"[min={limits['min_mhz']:.0f}, max={limits['max_mhz']:.0f}]"
            )
            return avg_freq

        dprint("❌ RING BUS DEBUG: All methods failed")
        logger.warning("Ring bus frequency not available on this CPU")
        return None

    # ------------------------------------------------------------------------
    # C-state counters with proper TSC conversion
    # ------------------------------------------------------------------------
    def read_cstate_counters(self, cpu: int = 0, pin: bool = True) -> Dict[str, Any]:
        """
        Read C-state residency counters.
        Returns dict with both raw TSC ticks and converted times.
        """
        counters = {
            "raw": {},  # Raw TSC ticks
            "seconds": {},  # Converted to seconds
            "microseconds": {},  # Converted to microseconds
            "human": {},  # Human-readable string
        }

        # Try C2
        val = self.read_msr(self.MSR_ADDRESSES["MSR_PKG_C2_RESIDENCY"], cpu, pin=pin)
        if val is not None and val > 0:
            counters["raw"]["C2"] = val
            counters["seconds"]["C2"] = self.ticks_to_seconds(val)
            counters["microseconds"]["C2"] = self.ticks_to_microseconds(val)
            counters["human"]["C2"] = self.ticks_to_human(val)

        # Try C3
        val = self.read_msr(self.MSR_ADDRESSES["MSR_PKG_C3_RESIDENCY"], cpu, pin=pin)
        if val is not None and val > 0:
            counters["raw"]["C3"] = val
            counters["seconds"]["C3"] = self.ticks_to_seconds(val)
            counters["microseconds"]["C3"] = self.ticks_to_microseconds(val)
            counters["human"]["C3"] = self.ticks_to_human(val)

        # Try C6
        val = self.read_msr(self.MSR_ADDRESSES["MSR_PKG_C6_RESIDENCY"], cpu, pin=pin)
        if val is not None and val > 0:
            counters["raw"]["C6"] = val
            counters["seconds"]["C6"] = self.ticks_to_seconds(val)
            counters["microseconds"]["C6"] = self.ticks_to_microseconds(val)
            counters["human"]["C6"] = self.ticks_to_human(val)

        # Try C7
        val = self.read_msr(self.MSR_ADDRESSES["MSR_PKG_C7_RESIDENCY"], cpu, pin=pin)
        if val is not None and val > 0:
            counters["raw"]["C7"] = val
            counters["seconds"]["C7"] = self.ticks_to_seconds(val)
            counters["microseconds"]["C7"] = self.ticks_to_microseconds(val)
            counters["human"]["C7"] = self.ticks_to_human(val)

        # Try deeper C-states
        for state in ["C8", "C9", "C10"]:
            addr_name = f"MSR_PKG_{state}_RESIDENCY"
            if addr_name in self.MSR_ADDRESSES:
                val = self.read_msr(self.MSR_ADDRESSES[addr_name], cpu, pin=pin)
                if val is not None and val > 0:
                    counters["raw"][state] = val
                    counters["seconds"][state] = self.ticks_to_seconds(val)
                    counters["microseconds"][state] = self.ticks_to_microseconds(val)
                    counters["human"][state] = self.ticks_to_human(val)

        return counters

    def read_cstate_counters_all_cpus(
        self, pin: bool = True
    ) -> Dict[int, Dict[str, Any]]:
        """Read C-state counters for all CPUs."""
        results = {}
        for cpu in range(self.cpu_count):
            results[cpu] = self.read_cstate_counters(cpu, pin=pin)
        return results

    def average_cstate_counters(
        self, counters_per_cpu: Dict[int, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Compute average of each C-state counter across all CPUs.
        Returns dict with raw averages and converted values.
        """
        # First, collect all raw values
        raw_values = {}
        for cpu, data in counters_per_cpu.items():
            for state, val in data.get("raw", {}).items():
                if state not in raw_values:
                    raw_values[state] = []
                raw_values[state].append(val)

        # Calculate raw averages
        raw_averages = {}
        for state, values in raw_values.items():
            if values:
                raw_averages[state] = sum(values) / len(values)

        # Convert to seconds, microseconds, and human-readable
        result = {"raw": raw_averages, "seconds": {}, "microseconds": {}, "human": {}}

        for state, val in raw_averages.items():
            result["seconds"][state] = self.ticks_to_seconds(int(val))
            result["microseconds"][state] = self.ticks_to_microseconds(int(val))
            result["human"][state] = self.ticks_to_human(int(val))

        return result

    # ------------------------------------------------------------------------
    # Thermal throttle status - CORRECT BIT DEFINITIONS
    # ------------------------------------------------------------------------
    def read_thermal_throttle_status(
        self, cpu: int = 0, pin: bool = True
    ) -> Optional[Dict[str, int]]:
        """
        Read complete thermal throttle status from MSR_IA32_PACKAGE_THERM_STATUS.

        SCIENTIFIC NOTE:
            Bit 0: thermal_now - Currently throttling (1) or not (0)
            Bit 1: thermal_log - Has throttled since last reset (sticky bit)

        Returns:
            Dictionary with:
                - thermal_now: Current throttling status
                - thermal_log: Historical throttling indicator (sticky)
            Returns None if MSR read fails.
        """
        val = self.read_msr(
            self.MSR_ADDRESSES["MSR_IA32_PACKAGE_THERM_STATUS"], cpu, pin=pin
        )
        print(f"   Raw value: {val} (hex: {hex(val) if val else 'None'})")
        if val is None:
            return None

        result = {"thermal_now": (val >> 0) & 1, "thermal_log": (val >> 1) & 1}
        print(f"   Decoded: now={result['thermal_now']}, log={result['thermal_log']}")
        return result

    def read_core_thermal_status(
        self, cpu: int = 0, pin: bool = True
    ) -> Optional[Dict[str, int]]:
        """
        Read per-core thermal status from MSR_IA32_THERM_STATUS.

        Some throttling events are per-core and not visible in package status.
        Combine with package status using OR logic for complete picture.
        """
        val = self.read_msr(self.MSR_ADDRESSES["MSR_IA32_THERM_STATUS"], cpu, pin=pin)

        if val is None:
            return None

        return {
            "thermal_now": (val >> 0) & 1,  # Bit 0 - currently throttling
            "thermal_log": (val >> 1) & 1,  # Bit 1 - has throttled since last reset
        }

    def snapshot_thermal_state(
        self, cpu: int = 0, pin: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Take a complete thermal snapshot for before/after comparison.

        Combines both package and core thermal status for accurate throttling detection.

        CRITICAL SCIENTIFIC NOTES:
            - If both MSR reads fail, returns None (skip this experiment)
            - If one read fails, use defaults (0,0) for that domain
            - Throttling detection must use OR logic across domains

        Use this at START and END of experiment to detect if throttling
        occurred DURING the experiment (end_log > start_log).

        Returns:
            Dictionary with package and core thermal status, or None if both fail
        """
        package_status = self.read_thermal_throttle_status(cpu, pin)
        core_status = self.read_core_thermal_status(cpu, pin)

        # If both reads failed, return None (can't determine thermal state)
        if package_status is None and core_status is None:
            logger.warning(
                "Both package and core thermal MSRs failed - cannot determine thermal state"
            )
            return None

        # Use defaults (0,0) for any failed reads to prevent crashes
        safe_package = (
            package_status
            if package_status is not None
            else {"thermal_now": 0, "thermal_log": 0}
        )
        safe_core = (
            core_status
            if core_status is not None
            else {"thermal_now": 0, "thermal_log": 0}
        )
        from datetime import datetime

        return {
            "package": safe_package,
            "core": safe_core,
            "timestamp": time.time(),
            "human_time": datetime.fromtimestamp(time.time()).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }

    # ------------------------------------------------------------------------
    # APERF/MPERF frequency calculation - REAL CPU frequency
    # ------------------------------------------------------------------------
    def calculate_actual_frequency(
        self, aperf1: int, aperf2: int, mperf1: int, mperf2: int
    ) -> Optional[float]:
        """
        Calculate actual CPU frequency from APERF/MPERF deltas.

        Formula: actual_freq = TSC_freq * (ΔAPERF / ΔMPERF)

        SCIENTIFIC NOTES:
            - Ratio should be between 0 and 5 for valid CPU frequencies
            - Handles 64-bit counter wrap-around
            - Returns None if calculation is invalid

        Args:
            aperf1, aperf2: APERF values at start and end
            mperf1, mperf2: MPERF values at start and end

        Returns:
            Actual frequency in MHz, or None if invalid
        """
        if None in (aperf1, aperf2, mperf1, mperf2):
            return None

        delta_aperf = aperf2 - aperf1
        delta_mperf = mperf2 - mperf1

        if delta_mperf == 0:
            return None

        # Handle counter wrap-around (64-bit)
        if delta_aperf < 0:
            delta_aperf += 2**64
        if delta_mperf < 0:
            delta_mperf += 2**64

        # Calculate ratio with sanity check
        ratio = delta_aperf / delta_mperf

        # Sanity check: ratio should be between 0.1 and 5 for valid CPU frequencies
        # (0.1 = 280MHz, 5 = 14GHz on a 2.8GHz base system)
        if ratio <= 0.1 or ratio > 5:
            logger.warning(f"APERF/MPERF ratio out of bounds: {ratio}")
            return None

        actual_hz = self.tsc_frequency_hz * ratio
        return actual_hz / 1_000_000  # Convert to MHz

    def snapshot_aperf_mperf(self, cpu: int = 0, pin: bool = True) -> Dict[str, Any]:
        """
        Take a snapshot of APERF/MPERF for before/after comparison.

        Use at START and END of experiment to calculate actual CPU frequency
        during the measurement period.

        Returns:
            Dictionary with aperf, mperf values and timestamp
        """
        aperf, mperf = self.read_aperf_mperf(cpu, pin)
        return {"aperf": aperf, "mperf": mperf, "timestamp": time.time()}

    def take_cstate_snapshot(self, cpu: int = 0) -> Dict[str, Any]:
        """
        Take a snapshot of C-state counters for wakeup calculation.
        Call this at start and end of experiment.
        """
        return {
            "timestamp": time.time(),
            "counters": self.read_cstate_counters_for_wakeup(cpu),
        }

    def set_start_snapshot(self, snapshot: Dict[str, Any]):
        """Store the start snapshot."""
        self.start_cstate_counters = snapshot
        dprint(f"✅ Start C-state snapshot stored: {snapshot}")

    def set_end_snapshot(self, snapshot: Dict[str, Any]):
        """Store the end snapshot."""
        self.end_cstate_counters = snapshot
        dprint(f"✅ End C-state snapshot stored: {snapshot}")

    # ------------------------------------------------------------------------
    # Get ALL metrics in one call
    # ------------------------------------------------------------------------
    def get_all_metrics(self) -> Dict[str, Any]:
        """
        Get complete MSR metrics including both baseline and dynamic measurements.
        Now includes properly converted C-state times.
        """
        metrics = {
            "baseline": self.get_baseline_dict(),
            "dynamic": {},
            "tsc_frequency_hz": self.tsc_frequency_hz,  # Include for reference
        }

        if not self.helper_available and not self.rdmsr_available:
            logger.warning("MSR not available, returning only baseline")
            return metrics

        try:
            # Read current ring bus frequency
            ring_freq = self.measure_ring_bus_frequency()
            if ring_freq is not None:
                metrics["dynamic"]["ring_bus_frequency_mhz"] = ring_freq

            # Read C-state counters (now with proper conversion)
            cstate_all = self.read_cstate_counters_all_cpus()
            metrics["dynamic"]["cstate_counters"] = cstate_all
            # Calculate averages
            metrics["dynamic"]["cstate_averages"] = self.average_cstate_counters(
                cstate_all
            )

            # ====================================================================
            # Calculate per-run C-state deltas if start snapshot exists
            # ====================================================================
            print(
                f"🔵 MSR_READER - _cstate_start exists: {hasattr(self, '_cstate_start')}"
            )
            if hasattr(self, "_cstate_start"):
                print(f"🔵 MSR_READER - _cstate_start: {self._cstate_start}")
                current_cstates = self.snapshot_cstate_counters()
                print(f"🔵 MSR_READER - current_cstates: {current_cstates}")
            # ====================================================================
            # Calculate per-run C-state deltas if start snapshot exists
            # ====================================================================
            if hasattr(self, "_cstate_start") and self._cstate_start is not None:
                # Get current snapshot of C-state counters
                current_cstates = self.snapshot_cstate_counters()
                deltas = {}

                # Safely extract counters dictionaries with type checking
                if isinstance(self._cstate_start, dict):
                    start_counters = self._cstate_start.get("counters", {})
                else:
                    # Handle case where _cstate_start is not a dictionary
                    logger.warning(
                        "_cstate_start is not a dictionary, using empty counters"
                    )
                    start_counters = {}

                if isinstance(current_cstates, dict):
                    end_counters = current_cstates.get("counters", {})
                else:
                    logger.warning(
                        "current_cstates is not a dictionary, using empty counters"
                    )
                    end_counters = {}

                # Calculate delta for each C-state
                for state in ["c2", "c3", "c6", "c7"]:
                    # Step 1: Get values with defaults
                    start_val = start_counters.get(state, 0)
                    end_val = end_counters.get(state, 0)

                    # Step 2: Explicitly handle None values
                    if start_val is None:
                        start_val = 0
                        logger.debug(f"start_val for {state} was None, using 0")
                    if end_val is None:
                        end_val = 0
                        logger.debug(f"end_val for {state} was None, using 0")

                    # Step 3: Ensure values are integers
                    try:
                        start_val = int(start_val)
                    except (TypeError, ValueError):
                        start_val = 0
                        logger.debug(
                            f"start_val for {state} couldn't be converted, using 0"
                        )

                    try:
                        end_val = int(end_val)
                    except (TypeError, ValueError):
                        end_val = 0
                        logger.debug(
                            f"end_val for {state} couldn't be converted, using 0"
                        )

                    # Step 4: Safe subtraction
                    delta_ticks = max(0, end_val - start_val)

                    # Step 5: Convert to seconds using TSC frequency
                    if self.tsc_frequency_hz and delta_ticks > 0:
                        delta_seconds = delta_ticks / self.tsc_frequency_hz
                    else:
                        delta_seconds = delta_ticks / 2.8e9
                        if delta_ticks > 0:
                            logger.debug(f"Using fallback TSC frequency for {state}")

                    # Store in seconds
                    deltas[f"{state}_time_seconds"] = delta_seconds

                # Add deltas to top level for easy database access
                metrics.update(deltas)

                # Debug output
                print(f"🔵 MSR_READER_ID: {id(metrics)}")
                print(
                    f"🔵 MSR_READER_VALUE: c2={metrics.get('c2_time_seconds', 0):.3f}s"
                )
                print(f"🔵 MSR_READER_KEYS: {list(metrics.keys())}")

                # Store raw deltas in dynamic section
                metrics["dynamic"]["cstate_deltas"] = deltas

                logger.debug(f"C-state deltas calculated: {deltas}")

            # Read thermal status from both package and core
            thermal_pkg = self.read_thermal_throttle_status(cpu=0)
            thermal_core = self.read_core_thermal_status(cpu=0)

            # Calculate averages
            metrics["dynamic"]["cstate_averages"] = self.average_cstate_counters(
                cstate_all
            )

            # Read thermal status from both package and core
            thermal_pkg = self.read_thermal_throttle_status(cpu=0)
            thermal_core = self.read_core_thermal_status(cpu=0)
            metrics["dynamic"]["thermal"] = {
                "package": thermal_pkg,
                "core": thermal_core,
            }

            # Add wakeup counters if available
            if self.start_cstate_counters and self.end_cstate_counters:
                wakeups = self.calculate_wakeup_delta(
                    self.start_cstate_counters.get("counters", {}),
                    self.end_cstate_counters.get("counters", {}),
                )
                metrics["dynamic"]["cpu_wakeup_count"] = wakeups

                dprint(f"\n🔍 DEBUG - CPU Wakeup counters:")
                dprint(
                    f"   Start counters: {self.start_cstate_counters.get('counters', {})}"
                )
                dprint(
                    f"   End counters: {self.end_cstate_counters.get('counters', {})}"
                )
                dprint(f"   Wakeup count: {wakeups}")
            else:
                dprint(f"\n🔍 DEBUG - No wakeup snapshots available")
                dprint(f"   start_cstate_counters: {self.start_cstate_counters}")
                dprint(f"   end_cstate_counters: {self.end_cstate_counters}")

            dprint("\n🔍 DEBUG - Thermal data structure:")
            dprint(f"   thermal_pkg = {thermal_pkg}")
            dprint(f"   thermal_core = {thermal_core}")
            dprint(
                f"   metrics['dynamic']['thermal'] = {metrics['dynamic']['thermal']}"
            )

            # Read APERF/MPERF if enabled
            if self.enable_aperf_mperf:
                aperf, mperf = self.read_aperf_mperf(cpu=0)
                metrics["dynamic"]["aperf"] = aperf
                metrics["dynamic"]["mperf"] = mperf

        except Exception as e:
            logger.error(f"Error reading dynamic MSR metrics: {e}")

        return metrics

    # ------------------------------------------------------------------------
    # Safe counter access helper - prevents NoneType errors
    # ------------------------------------------------------------------------
    def safe_get_counter(
        self, counters_dict: Optional[Dict], state: str, default: int = 0
    ) -> int:
        """
        Safely extract a counter value, ensuring it's a number.

        Use this method whenever accessing C-state counters to avoid NoneType errors.
        This provides a single point of defense against None values, missing keys,
        and type conversion issues.

        Args:
            counters_dict: Dictionary possibly containing counter values
            state: State name ('c2', 'c3', 'c6', 'c7')
            default: Default value if not found or invalid

        Returns:
            Integer counter value (always a number, never None)
        """
        # Case 1: Dictionary is None
        if counters_dict is None:
            logger.debug(
                f"counters_dict is None for state {state}, using default {default}"
            )
            return default

        # Case 2: Get value with default
        value = counters_dict.get(state, default)

        # Case 3: Value is None
        if value is None:
            logger.debug(f"Value for state {state} is None, using default {default}")
            return default

        # Case 4: Value exists but might not be convertible to int
        try:
            return int(value)
        except (TypeError, ValueError) as e:
            logger.debug(
                f"Could not convert {state} value {value} to int: {e}, using default {default}"
            )
            return default

    # ------------------------------------------------------------------------
    # Debug helper - inspect C-state snapshot integrity
    # ------------------------------------------------------------------------
    def debug_cstate_snapshots(self):
        """
        Debug method to check C-state snapshot integrity.

        Call this when experiencing issues with C-state calculations to verify
        that snapshots contain expected data structures and no None values.
        """
        print("\n" + "=" * 60)
        print("🔍 C-STATE SNAPSHOT DEBUG")
        print("=" * 60)

        # Check if _cstate_start exists
        if not hasattr(self, "_cstate_start"):
            print("❌ No _cstate_start attribute found")
            return

        print(f"✓ _cstate_start exists")
        print(f"  Type: {type(self._cstate_start)}")

        # Examine _cstate_start structure
        if isinstance(self._cstate_start, dict):
            print(f"  Keys: {list(self._cstate_start.keys())}")

            # Check counters sub-dictionary
            counters = self._cstate_start.get("counters", {})
            print(f"  counters type: {type(counters)}")
            print(f"  counters keys: {list(counters.keys())}")

            # Check each expected C-state
            print("\n  C-state values:")
            for state in ["c2", "c3", "c6", "c7"]:
                val = counters.get(state)
                val_type = type(val).__name__ if val is not None else "NoneType"
                status = "✅" if val is not None and isinstance(val, int) else "❌"
                print(f"    {status} {state}: {val} (type: {val_type})")

            # Check timestamp
            ts = self._cstate_start.get("timestamp")
            if ts:
                print(f"\n  Timestamp: {ts}")

            # Check read_status if present
            read_status = self._cstate_start.get("read_status")
            if read_status:
                print(f"  Read status: {read_status}")
        else:
            print(f"❌ _cstate_start is not a dictionary: {self._cstate_start}")

        print("=" * 60)

    # ------------------------------------------------------------------------
    # Wake-up latency measurement (baseline only)
    # ------------------------------------------------------------------------
    def measure_hardware_wakeup_latency(
        self, cpu: int = 0, iterations: Optional[int] = None
    ) -> Optional[float]:
        """Measure wake-up latency using C-state counters (baseline only)."""
        if not self.helper_available and not self.rdmsr_available:
            logger.warning(
                "MSR not available, cannot measure hardware wake-up latency."
            )
            return None

        if iterations is None:
            iterations = self.wakeup_iterations

        self.pin_to_cpu(cpu)
        latencies = []
        log_interval = max(1, iterations // 10)

        for i in range(iterations):
            c7_before = self.read_msr(
                self.MSR_ADDRESSES["MSR_PKG_C7_RESIDENCY"], cpu, pin=False
            )
            time.sleep(self.wakeup_idle_s)
            start = time.perf_counter()
            x = 0
            for _ in range(100):
                x += 1
            end = time.perf_counter()
            c7_after = self.read_msr(
                self.MSR_ADDRESSES["MSR_PKG_C7_RESIDENCY"], cpu, pin=False
            )

            if c7_before is not None and c7_after is not None:
                delta = c7_after - c7_before
                if delta < 0:
                    delta += self.cstate_max
                if delta > 0:
                    # Convert from TSC ticks to microseconds
                    delta_us = self.ticks_to_microseconds(delta)
                    latencies.append(delta_us)
                else:
                    latencies.append((end - start) * 1_000_000)
            else:
                latencies.append((end - start) * 1_000_000)

            if (i + 1) % log_interval == 0:
                logger.debug(f"Wake-up latency iteration {i+1}/{iterations}")

        self.unpin()
        if latencies:
            avg = sum(latencies) / len(latencies)
            logger.info(f"Measured wake-up latency: {avg:.2f} µs")
            return avg
        return None

    # ------------------------------------------------------------------------
    # Optional: Set performance governor
    # ------------------------------------------------------------------------
    @staticmethod
    def set_performance_governor() -> bool:
        try:
            subprocess.run(
                ["cpupower", "frequency-set", "-g", "performance"],
                check=True,
                capture_output=True,
            )
            logger.info("CPU governor set to 'performance'")
            return True
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.warning(f"Could not set performance governor: {e}")
            return False

    def __str__(self) -> str:
        status = (
            "available"
            if (self.helper_available or self.rdmsr_available)
            else "unavailable"
        )
        method = (
            "C helper"
            if self.helper_available
            else "direct" if self.rdmsr_available else "none"
        )
        pinned = (
            f", pinned to CPU {self._pinned_cpu}"
            if self._pinned_cpu is not None
            else ""
        )
        baseline_info = f", baseline loaded: {'yes' if self.baseline else 'no'}"
        tsc_info = (
            f", TSC: {self.tsc_frequency_hz/1e6:.0f}MHz"
            if self.tsc_frequency_hz
            else ", TSC: unknown"
        )
        return f"MSRReader({status}, method={method}{pinned}{baseline_info}{tsc_info}, {self.cpu_count} CPUs)"


# ============================================================================
# STANDALONE TEST
# ============================================================================
if __name__ == "__main__":
    import json

    from core.config_loader import ConfigLoader

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("\n" + "=" * 70)
    print("MSR READER TEST (with C Helper)")
    print("=" * 70)

    # Load configuration
    config_loader = ConfigLoader()
    hw_config = config_loader.get_hardware_config()
    settings = config_loader.get_settings()

    if hasattr(settings, "__dict__"):
        hw_config["settings"] = settings.__dict__
    else:
        hw_config["settings"] = settings

    print("\n📁 Testing MSRReader with C helper...")
    reader = MSRReader(hw_config, use_baseline=True, require_baseline=False)
    print(f"📊 {reader}")

    if not reader.helper_available and not reader.rdmsr_available:
        print("\n❌ MSR not available. Check permissions and C helper.")
        sys.exit(1)

    # Test get_all_metrics()
    print("\n" + "=" * 70)
    print("TEST: get_all_metrics() - Complete MSR Data")
    print("=" * 70)
    metrics = reader.get_all_metrics()

    # Display baseline
    print("\n📁 Baseline Data:")
    baseline_meas = metrics.get("baseline", {}).get("measurements", {})
    ring_freq = baseline_meas.get("ring_bus_frequency_mhz")
    if ring_freq:
        print(f"   Ring Bus Frequency (baseline): {ring_freq} MHz")
    else:
        print(f"   Ring Bus Frequency: Not available in baseline")

    wake_lat = baseline_meas.get("wakeup_latency_us")
    if wake_lat:
        print(f"   Wake-up Latency: {wake_lat:.2f} µs")

    # Display dynamic data
    print("\n📊 Dynamic Data (current measurements):")
    dynamic = metrics.get("dynamic", {})

    current_ring = dynamic.get("ring_bus_frequency_mhz")
    if current_ring:
        print(f"   Current Ring Bus Frequency: {current_ring:.1f} MHz")

    # Display C-state averages with proper conversion
    cstate_avgs = dynamic.get("cstate_averages", {})
    if cstate_avgs:
        print("\n   C-State Averages (across all CPUs):")
        print("      Raw TSC ticks:")
        for state, val in cstate_avgs.get("raw", {}).items():
            print(f"         {state}: {val:.0f}")
        print("      Actual time:")
        for state, val in cstate_avgs.get("seconds", {}).items():
            print(f"         {state}: {val:.2f} seconds")
        for state, val in cstate_avgs.get("human", {}).items():
            print(f"         {state}: {val}")
    else:
        print("   No C-state data available")

    throttle = dynamic.get("thermal_throttle")
    if throttle is not None:
        print(f"\n   Thermal Throttle: {throttle}")

    print(f"\n   TSC Frequency: {metrics.get('tsc_frequency_hz', 0)/1e6:.0f} MHz")

    print("\n" + "=" * 70)
    print("✅ MSR Reader Test Complete!")
    print("=" * 70)
