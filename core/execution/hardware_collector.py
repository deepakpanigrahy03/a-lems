"""
================================================================================
HARDWARE COLLECTOR – System state monitoring functions
================================================================================

Functions moved from harness.py during refactoring.
"""

import glob
import logging
import socket
import time
from typing import Dict, Optional

import psutil

from core.utils.debug import dprint

logger = logging.getLogger(__name__)

# Store last good temperature as module-level variable
_last_good_temp = 0.0


def _measure_network_latency(hostname: str = "api.groq.com") -> Dict[str, float]:
    """
    Measure network latency to cloud provider.

    Purpose:
        Separate network delays from true orchestration overhead.
        This is critical for cloud experiments where network latency
        can dominate measurements.

    Args:
        hostname: Cloud provider hostname

    Returns:
        Dictionary with DNS and ping latencies
    """
    network_metrics = {}

    # Measure DNS resolution time
    dns_start = time.time()
    try:
        socket.gethostbyname(hostname)
        dns_latency = (time.time() - dns_start) * 1000
        network_metrics["dns_latency_ms"] = dns_latency
    except:
        network_metrics["dns_latency_ms"] = None

    return network_metrics


def _warmup_run(executor, prompt: str, is_agentic: bool = False) -> None:
    """
    Perform a warmup run to eliminate initialization effects.

    Scientific rationale:
        First run often includes:
        - Cold caches
        - API connection establishment
        - Python JIT compilation
        - Model loading (local models)

    These would skew energy measurements, so we discard warmup.

    Args:
        executor: Linear or Agentic executor
        prompt: The task prompt
        is_agentic: Whether this is agentic (uses comparison method)
    """
    dprint("🔥 Warmup run (results discarded)")
    if is_agentic:
        executor.execute_comparison(prompt)
    else:
        executor.execute(prompt)
    dprint("✅ Warmup complete")


# =========================================================================
# FIX M3-1: Governor/Turbo Control
# =========================================================================
def get_governor() -> str:
    """
    Get current CPU frequency governor.

    Returns:
        'performance', 'powersave', 'ondemand', etc., or 'unknown'
    """
    try:
        with open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor", "r") as f:
            return f.read().strip()
    except Exception as e:
        logger.debug(f"Could not read governor: {e}")
        return "unknown"


def get_turbo_status() -> str:
    """
    Get turbo boost status.

    Returns:
        'enabled' if turbo is on, 'disabled' if off, 'unknown' if can't read
    """
    try:
        # Intel p-state driver
        with open("/sys/devices/system/cpu/intel_pstate/no_turbo", "r") as f:
            # 0 = turbo enabled, 1 = turbo disabled
            return "disabled" if int(f.read().strip()) else "enabled"
    except:
        try:
            # Alternative location for some systems
            with open("/sys/devices/system/cpu/cpufreq/boost", "r") as f:
                return "enabled" if int(f.read().strip()) else "disabled"
        except Exception as e:
            logger.debug(f"Could not read turbo status: {e}")
            return "unknown"


# =========================================================================
# FIX M3-2: Interrupt Rate
# =========================================================================
def _read_interrupts() -> int:
    """
    Read total interrupt count from /proc/stat.

    Returns:
        Total interrupts since boot, or 0 if cannot read
    """
    try:
        with open("/proc/stat", "r") as f:
            for line in f:
                if line.startswith("intr"):
                    # Format: intr <total> <interrupts...>
                    return int(line.split()[1])
    except Exception as e:
        logger.warning(f"Could not read interrupts: {e}")
    return 0


def get_interrupt_rate(before: int, after: int, duration_seconds: float) -> float:
    """
    Calculate interrupt rate from start/end readings.

    Args:
        before: Interrupt count at start
        after: Interrupt count at end
        duration_seconds: Duration of measurement

    Returns:
        Interrupts per second
    """
    if duration_seconds <= 0:
        return 0.0
    return (after - before) / duration_seconds


# =========================================================================
# FIX M3-3: Temperature Tracking
# =========================================================================
def _read_temperature() -> float:
    """
    Read current package temperature in Celsius.

    Tries multiple sources:
    1. Thermal zone (most common)
    2. Coretemp hwmon (fallback)
    3. Any hwmon with temp1_input

    Returns:
        Temperature in °C, or last known good temperature if reading fails
    """
    # ========== START OF FIXES ==========

    # Store last good temperature as instance variable

    global _last_good_temp

    # Method 1: Try ALL thermal zones (not just zone0)
    for zone in range(10):  # ← CHANGED: Loop through zones 0-9
        try:
            path = f"/sys/class/thermal/thermal_zone{zone}/temp"
            with open(path, "r") as f:
                temp = int(f.read().strip()) / 1000.0
                # Sanity check: valid CPU temps are between 10°C and 100°C
                if 10 < temp < 100:  # ← NEW: Add sanity check
                    _last_good_temp = temp
                    return temp
        except:
            continue

    # Method 2: Try all hwmon devices (already does this)
    try:
        import glob

        for hwmon in glob.glob("/sys/class/hwmon/hwmon*/temp1_input"):
            with open(hwmon, "r") as f:
                temp = int(f.read().strip()) / 1000.0
                if 10 < temp < 100:  # ← NEW: Add sanity check
                    _last_good_temp = temp
                    return temp
    except:
        pass

    # ========== END OF FIXES ==========

    # Return last known good temperature instead of 0.0
    if _last_good_temp > 0:
        logger.debug(f"Returning last good temperature: {_last_good_temp}°C")
        return _last_good_temp

    logger.debug("Could not read temperature")
    return 0.0


# =========================================================================
# FIX M3-4: Cold Start Flag
# =========================================================================
def _is_cold_start(run_number: int) -> bool:
    """
    Determine if this is a cold start run.

    Args:
        run_number: Current run number (1-based)

    Returns:
        True if this is the first run in the batch
    """
    return run_number == 1


# =========================================================================
# FIX M3-5: Background Noise
# =========================================================================
def get_background_cpu() -> float:
    """
    Get current background CPU usage percent.

    Returns:
        CPU usage percentage (0-100)
    """
    try:
        # Short interval to get current usage
        return psutil.cpu_percent(interval=0.1)
    except Exception as e:
        logger.debug(f"Could not get CPU percent: {e}")
        return 0.0


def get_process_count() -> int:
    """
    Get number of running processes.

    Returns:
        Total number of processes
    """
    try:
        return len(psutil.pids())
    except Exception as e:
        logger.debug(f"Could not get process count: {e}")
        return 0


# =========================================================================
# FIX M3-6: Memory Metrics (RSS, VMS - process level)
# =========================================================================
def get_process_memory() -> Dict[str, float]:
    """
    Get memory usage for the current Python process.

    Returns:
        Dictionary with:
        - rss_mb: Resident Set Size in MB (physical memory)
        - vms_mb: Virtual Memory Size in MB (total virtual address space)
    """
    metrics = {"rss_mb": 0.0, "vms_mb": 0.0}
    try:
        process = psutil.Process()
        mem_info = process.memory_info()
        metrics["rss_mb"] = mem_info.rss / (1024 * 1024)  # Convert to MB
        metrics["vms_mb"] = mem_info.vms / (1024 * 1024)  # Convert to MB
        logger.debug(
            f"Process memory: RSS={metrics['rss_mb']:.1f}MB, VMS={metrics['vms_mb']:.1f}MB"
        )
    except Exception as e:
        logger.debug(f"Could not get process memory: {e}")
    return metrics
