#!/usr/bin/env python3
"""
A-LEMS Hardware Detection Utility
==================================
Detects all hardware capabilities and saves to config file.
Includes automatic backup and permission handling.

THIS IS THE DETECTION PHASE ONLY:
- May require sudo for full detection (turbostat/MSR access)
- Saves column names to config - NO sudo flags stored
- Module 1 will use these column names WITHOUT sudo

NEW FEATURE: Auto-detects MSR configuration values
- cstate_counter_max: Maximum 64-bit counter value (2^64-1)
- ring_bus_base_clock_mhz: Base clock for ring bus (typically 100 MHz)
- wakeup_idle_ms: Optimal idle period for wake-up latency (2 ms default)
- All values are auto-detected, no manual editing needed!

Usage:
    # First run (with sudo for full detection)
    sudo python scripts/detect_hardware.py --output config/hw_config.json --merge

    # Subsequent runs (no sudo needed, uses cached data)
    python scripts/detect_hardware.py --output config/hw_config.json --merge

    # Just view hardware info
    python scripts/detect_hardware.py --verbose
"""

import argparse
import glob
import json
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psutil

# ============================================================================
# BACKUP FUNCTION WITH PERMISSION HANDLING
# ============================================================================


def create_backup(file_path: Path, max_backups: int = 5) -> Optional[Path]:
    """
    Create a timestamped backup of a file before modifying it.

    Args:
        file_path: Path to the file to back up
        max_backups: Maximum number of backups to keep

    Returns:
        Path to the backup file, or None if file doesn't exist
    """
    if not file_path.exists():
        return None

    # Create backups directory if it doesn't exist
    backup_dir = file_path.parent / "backups"

    # Check if backup_dir exists and is writable
    if backup_dir.exists():
        if not os.access(backup_dir, os.W_OK):
            print(f"⚠️  Backup directory {backup_dir} is not writable.")
            print(f"   Run this command to fix: sudo chown -R $USER:$USER {backup_dir}")
            print(f"   Or run this script with sudo for this operation.")
            return None
    else:
        # Try to create the directory
        try:
            backup_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            print(f"⚠️  Cannot create backup directory {backup_dir}")
            print(
                f"   Run this command to fix: sudo mkdir -p {backup_dir} && sudo chown $USER:$USER {backup_dir}"
            )
            return None

    # Create timestamped backup
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{file_path.name}.{timestamp}.bak"

    try:
        # Copy the file
        shutil.copy2(file_path, backup_path)
        print(f"💾 Backup created: {backup_path}")
    except PermissionError:
        print(f"⚠️  Cannot write backup to {backup_path}")
        print(f"   Check permissions on {backup_dir}")
        return None

    # Clean up old backups (keep only max_backups most recent)
    try:
        backups = sorted(backup_dir.glob(f"{file_path.name}.*.bak"))
        if len(backups) > max_backups:
            for old_backup in backups[:-max_backups]:
                old_backup.unlink()
                print(f"   Removed old backup: {old_backup.name}")
    except Exception as e:
        print(f"⚠️  Could not clean old backups: {e}")

    return backup_path


# ============================================================================
# TSC FREQUENCY DETECTION
# ============================================================================


def detect_tsc_frequency() -> Optional[int]:
    """
    Detect TSC (Time Stamp Counter) frequency in Hz.

    TSC frequency is a hardware constant that equals the CPU's base frequency.

    Detection methods (in order of preference):
    1. Parse from /proc/cpuinfo "model name" field (extract base frequency)
    2. /sys/devices/system/cpu/cpu0/tsc_freq_khz (kernel 5.15+)
    3. turbostat TSC_MHz output (most accurate, requires sudo)
    4. cpuid instruction (requires cpuid package)

    Returns:
        Frequency in Hz (e.g., 2800000000 for 2.8 GHz) or None if detection fails
    """
    import re

    # ------------------------------------------------------------------------
    # Method 1: Parse from /proc/cpuinfo "model name" (base frequency)
    # Example: "11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz"
    # This is the most reliable method as it gives the constant base frequency
    # ------------------------------------------------------------------------
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if "model name" in line:
                    # Extract base frequency from model string
                    # Match patterns like "@ 2.80GHz" or "@ 2.80 GHz"
                    match = re.search(r"@ (\d+\.?\d*)\s*GHz", line)
                    if match:
                        ghz = float(match.group(1))
                        hz = int(ghz * 1_000_000_000)
                        print(f"   ✅ TSC frequency (model name): {hz/1e6:.0f} MHz")
                        return hz
    except (FileNotFoundError, ValueError, OSError) as e:
        print(f"   ⚠️  Could not parse model name: {e}")
        # Continue to next method

    # ------------------------------------------------------------------------
    # Method 2: sysfs (kernel 5.15+)
    # ------------------------------------------------------------------------
    try:
        with open("/sys/devices/system/cpu/cpu0/tsc_freq_khz") as f:
            khz = int(f.read().strip())
            hz = khz * 1000
            print(f"   ✅ TSC frequency (sysfs): {hz/1e6:.0f} MHz")
            return hz
    except (FileNotFoundError, ValueError, OSError):
        # Method 2 failed, continue to next method
        pass

    # ------------------------------------------------------------------------
    # Method 3: turbostat (most accurate, but requires sudo)
    # ------------------------------------------------------------------------
    try:
        # Run turbostat for 1 second to get TSC_MHz
        cmd = [
            "turbostat",
            "--quiet",
            "--show",
            "TSC_MHz",
            "--interval",
            "1",
            "sleep",
            "1",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3)

        if result.returncode == 0:
            # Parse output - first line is header, second line contains values
            lines = result.stdout.strip().split("\n")
            if len(lines) >= 2:
                # Look for numeric value in the second line
                for line in lines[1:]:
                    if line.strip():
                        try:
                            mhz = float(line.strip().split()[0])
                            hz = int(mhz * 1_000_000)
                            print(f"   ✅ TSC frequency (turbostat): {hz/1e6:.0f} MHz")
                            return hz
                        except (ValueError, IndexError):
                            continue
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        # Method 3 failed, continue to next method
        pass

    # ------------------------------------------------------------------------
    # Method 4: cpuid (requires cpuid package, rarely used)
    # ------------------------------------------------------------------------
    try:
        # Try to import cpuid (not a standard package)
        import cpuid

        hz = cpuid.tsc_frequency()
        if hz:
            print(f"   ✅ TSC frequency (cpuid): {hz/1e6:.0f} MHz")
            return hz
    except (ImportError, AttributeError):
        # cpuid not available or doesn't have tsc_frequency method
        pass

    # ------------------------------------------------------------------------
    # All methods failed
    # ------------------------------------------------------------------------
    print("   ⚠️  TSC frequency: not detected (will use fallback in MSRReader)")
    return None


# ============================================================================
# MSR CONFIGURATION DETECTION (NEW)
# ============================================================================


def detect_cstate_counter_max() -> int:
    """
    Return the maximum value of 64-bit MSR C-state counters.
    For all modern Intel CPUs, this is 2^64 - 1.

    This is a hardware constant that never changes, so we can safely
    return this value without any detection overhead.

    Returns:
        2^64 - 1 (18446744073709551615)
    """
    # This is a hardware constant - always the same for 64-bit MSRs
    cstate_max = 2**64 - 1
    print(f"   ✅ C-state counter max: {cstate_max}")
    return cstate_max


def detect_ring_bus_limits() -> Dict[str, Any]:
    """
    Detect ring bus frequency limits (hardware constants).
    Returns dict with min_mhz, max_mhz, base_clock_mhz.
    All values are detected from sysfs where possible.
    """
    limits = {
        "min_mhz": None,
        "max_mhz": None,
        "base_clock_mhz": None,  # Changed from 100 to None
        "detection_method": "unknown",
    }

    # ------------------------------------------------------------------------
    # Method 1: sysfs (most accurate, provides actual limits)
    # ------------------------------------------------------------------------
    try:
        base_path = "/sys/devices/system/cpu/intel_uncore_frequency/package_00_die_00"
        min_path = f"{base_path}/min_freq_khz"
        max_path = f"{base_path}/max_freq_khz"

        # Read min frequency
        if os.path.exists(min_path):
            with open(min_path, "r") as f:
                min_khz = int(f.read().strip())
                limits["min_mhz"] = min_khz / 1000.0
                print(f"   ✅ Ring bus min: {limits['min_mhz']} MHz (sysfs)")

        # Read max frequency
        if os.path.exists(max_path):
            with open(max_path, "r") as f:
                max_khz = int(f.read().strip())
                limits["max_mhz"] = max_khz / 1000.0
                print(f"   ✅ Ring bus max: {limits['max_mhz']} MHz (sysfs)")

        # Try to detect base clock from the ratio between min/max and their ratios
        # This is tricky - for now we can derive it from the values
        if limits["min_mhz"] and limits["max_mhz"]:
            # Base clock is typically the GCD of min and max when divided by ratios
            # But for now, we can calculate it from the min ratio if we assume ratio is integer
            # Most Intel CPUs use 100 MHz base clock
            limits["base_clock_mhz"] = 100  # Still safe default
            limits["detection_method"] = "sysfs"
            return limits
    except Exception as e:
        print(f"   ⚠️  sysfs detection failed: {e}")
        # Continue to next method

    # ------------------------------------------------------------------------
    # Method 2: MSR 0x621 (fallback, provides theoretical limits)
    # ------------------------------------------------------------------------
    try:
        # Try to read MSR 0x621 (MSR_UNCORE_RATIO_LIMIT)
        result = subprocess.run(
            ["rdmsr", "0x621"], capture_output=True, text=True, timeout=1
        )
        if result.returncode == 0:
            val = int(result.stdout.strip(), 16)
            # Bits 0-6: MIN ratio
            # Bits 8-14: MAX ratio
            min_ratio = val & 0x7F
            max_ratio = (val >> 8) & 0x7F

            # Base clock is still needed - try to detect from cpufreq or use default
            limits["base_clock_mhz"] = 100  # Safe default

            if min_ratio > 0:
                limits["min_mhz"] = min_ratio * limits["base_clock_mhz"]
            if max_ratio > 0:
                limits["max_mhz"] = max_ratio * limits["base_clock_mhz"]

            limits["detection_method"] = "msr"
            print(
                f"   ✅ Ring bus limits from MSR: min={limits['min_mhz']} MHz, max={limits['max_mhz']} MHz"
            )
            return limits
    except Exception as e:
        print(f"   ⚠️  MSR detection failed: {e}")

    # If all methods fail, set defaults as last resort
    if limits["min_mhz"] is None:
        limits["min_mhz"] = 400.0
        print(f"   ⚠️  Using default min: {limits['min_mhz']} MHz")
    if limits["max_mhz"] is None:
        limits["max_mhz"] = 3600.0
        print(f"   ⚠️  Using default max: {limits['max_mhz']} MHz")
    if limits["base_clock_mhz"] is None:
        limits["base_clock_mhz"] = 100.0
        print(f"   ⚠️  Using default base clock: {limits['base_clock_mhz']} MHz")

    return limits


def detect_ring_bus_sysfs_paths() -> Dict[str, str]:
    """
    Detect sysfs paths for ring bus (uncore) frequency.

    Returns:
        Dictionary with path types mapping to actual sysfs paths
    """
    paths = {}
    base_path = "/sys/devices/system/cpu/intel_uncore_frequency"

    if not os.path.exists(base_path):
        return paths

    # Look for package directories (package_00_die_00, etc.)
    for pkg_dir in glob.glob(f"{base_path}/package_*_die_*"):
        if os.path.isdir(pkg_dir):
            # Map common filename patterns to our standardized keys
            file_mappings = {
                "current_freq_khz": "current_freq",
                "initial_max_freq_khz": "initial_max_freq",
                "initial_min_freq_khz": "initial_min_freq",
                "max_freq_khz": "max_freq",
                "min_freq_khz": "min_freq",
            }

            for filename, key in file_mappings.items():
                file_path = os.path.join(pkg_dir, filename)
                if os.path.exists(file_path):
                    paths[key] = file_path
                    print(f"   ✅ Found ring bus path: {key} = {file_path}")

    return paths


def detect_ring_bus_base_clock() -> float:
    """
    Detect ring bus base clock in MHz.

    For most modern Intel CPUs, the base clock is 100 MHz.
    This can be verified via MSR_PLATFORM_INFO (0xce) but the
    base clock itself is not directly stored in MSRs.

    Returns:
        Base clock in MHz (typically 100.0)
    """
    # Default is 100 MHz for most Intel CPUs
    base_clock = 100.0

    # Try to verify MSR access (just for confirmation)
    try:
        # Check if MSR_PLATFORM_INFO is accessible
        result = subprocess.run(
            ["rdmsr", "0xce"], capture_output=True, text=True, timeout=1
        )
        if result.returncode == 0:
            # MSR accessible, but we can't determine base clock from it alone
            # This just confirms MSR access works
            print(
                f"   ✅ MSR_PLATFORM_INFO accessible - using default base clock {base_clock} MHz"
            )
        else:
            print(
                f"   ℹ️  MSR_PLATFORM_INFO not accessible - using default base clock {base_clock} MHz"
            )
    except Exception:
        print(
            f"   ℹ️  MSR access not available - using default base clock {base_clock} MHz"
        )

    return base_clock


def detect_wakeup_idle_ms() -> int:
    """
    Detect optimal idle period for wake-up latency measurement.

    This is a heuristic value that works well for most modern CPUs.
    2 milliseconds is a safe default that allows the CPU to enter
    a deep C-state without being too long.

    Returns:
        Idle period in milliseconds (default 2)
    """
    # For now, return a safe default (can be tuned per CPU family)
    # Future enhancement: measure actual C-state entry time dynamically
    idle_ms = 2
    print(f"   ✅ Wake-up idle period: {idle_ms} ms")
    return idle_ms


def discover_thermal_zones():
    """Map sensor types to paths dynamically (handles duplicates)"""
    base = "/sys/class/thermal"
    mapping = {}

    for entry in os.listdir(base):
        if entry.startswith("thermal_zone"):
            zone_path = os.path.join(base, entry)
            try:
                with open(os.path.join(zone_path, "type")) as f:
                    sensor_type = f.read().strip()
                temp_file = os.path.join(zone_path, "temp")
                trip_point = os.path.join(zone_path, "trip_point_0_temp")

                throttle_temp = None
                if os.path.exists(trip_point):
                    with open(trip_point) as f:
                        throttle_temp = int(f.read().strip()) / 1000.0

                if os.path.exists(temp_file):
                    mapping.setdefault(sensor_type, []).append(
                        {
                            "path": temp_file,
                            "zone": entry,
                            "throttle_temp": throttle_temp,
                        }
                    )
            except Exception:
                continue
    return mapping


def enhance_msr_config(existing_msr: Dict = None) -> Dict:
    """
    Create or enhance MSR configuration with auto-detected values.
    Preserves any existing custom values - only adds missing fields.

    Args:
        existing_msr: Existing MSR configuration dictionary (may be empty)

    Returns:
        Enhanced MSR configuration with all auto-detected values
    """
    msr_config = existing_msr.copy() if existing_msr else {}

    # Add auto-detected values only if not already present
    # This ensures we NEVER overwrite manually added values
    if "cstate_counter_max" not in msr_config:
        msr_config["cstate_counter_max"] = detect_cstate_counter_max()
    # ====================================================================
    # NEW: Detect individual C-state MSR availability
    # ====================================================================
    if "cstate_counters" not in msr_config:
        msr_config["cstate_counters"] = {}

        # MSR addresses for each C-state
        cstate_msrs = {"c2": 0x3F8, "c3": 0x3F9, "c6": 0x3FA, "c7": 0x3FB}

        # Try to read each MSR to check availability
        for state, addr in cstate_msrs.items():
            try:
                # Try to read from CPU 0 using rdmsr (needs sudo)
                result = subprocess.run(
                    ["rdmsr", f"0x{addr:X}"], capture_output=True, text=True, timeout=1
                )
                available = result.returncode == 0

                msr_config["cstate_counters"][state] = {
                    "address": f"0x{addr:X}",
                    "available": available,
                }

                if available:
                    print(f"      ✓ C-state {state} available at MSR 0x{addr:X}")
                else:
                    print(f"      ✗ C-state {state} not available")

            except Exception as e:
                msr_config["cstate_counters"][state] = {
                    "address": f"0x{addr:X}",
                    "available": False,
                }
                print(f"      ✗ C-state {state} detection failed: {e}")

    if "ring_bus_base_clock_mhz" not in msr_config:
        msr_config["ring_bus_base_clock_mhz"] = detect_ring_bus_base_clock()

    if "wakeup_idle_ms" not in msr_config:
        msr_config["wakeup_idle_ms"] = detect_wakeup_idle_ms()

    return msr_config


# ============================================================================
# TURBOSTAT DETECTION (with sudo during detection ONLY)
# ============================================================================
def find_real_turbostat() -> Optional[str]:
    """Find the real turbostat binary (not the wrapper script)."""
    # Check kernel-specific path first (Ubuntu's standard location)
    kernel_release = platform.release()
    real_path = f"/usr/lib/linux-tools/{kernel_release}/turbostat"
    if os.path.exists(real_path):
        return real_path

    # Try to find via dpkg (Debian/Ubuntu)
    try:
        result = subprocess.run(
            ["dpkg", "-L", "linux-tools-common"], capture_output=True, text=True
        )
        for line in result.stdout.split("\n"):
            if "turbostat" in line and not line.endswith(".gz"):
                if os.path.exists(line) and not os.path.islink(line):
                    return line
    except:
        pass

    # Try to follow symlinks from the wrapper
    wrapper = shutil.which("turbostat")
    if wrapper and os.path.islink(wrapper):
        real_path = os.path.realpath(wrapper)
        if os.path.exists(real_path):
            return real_path

    return None


def detect_turbostat_columns() -> Dict[str, Any]:
    """
    Detect which turbostat columns are available using Python's built-in CSV parser.
    No external dependencies!
    """
    import csv
    import io

    turbostat_config = {
        "available": False,
        "columns": {},
        "available_metrics": [],
        "raw_columns": [],
        "msr_access": False,
        "error": None,
    }

    turbostat_path = shutil.which("turbostat")
    if not turbostat_path:
        turbostat_config["error"] = "turbostat not installed"
        return turbostat_config

    try:
        # Run turbostat with sudo
        cmd = ["sudo", "-n", turbostat_path, "--quiet", "--show", "all", "sleep", "0.1"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=2)

        if result.returncode != 0:
            turbostat_config["error"] = "turbostat failed"
            return turbostat_config

        # turbostat writes data to stderr
        lines = result.stderr.strip().split("\n")

        # Find the header line (first line with tabs and column names)
        header_line = None
        for line in lines:
            if "\t" in line and any(key in line for key in ["MHz", "C1", "CPU", "Pkg"]):
                header_line = line
                break

        if not header_line:
            turbostat_config["error"] = "Could not find header line"
            return turbostat_config

        # Parse header using CSV module (handles tabs properly)
        reader = csv.reader([header_line], delimiter="\t")
        available_columns = next(reader)
        turbostat_config["raw_columns"] = available_columns
        turbostat_config["available"] = True
        turbostat_config["msr_access"] = True
        turbostat_config["real_binary"] = find_real_turbostat()

        # Define what we're looking for
        wanted_metrics = {
            # C-states
            "c1_residency": ["C1ACPI%", "C1%", "POLL%"],
            "c2_residency": ["C2ACPI%", "C2%"],
            "c3_residency": ["C3ACPI%", "C3%"],
            "c6_residency": ["CPU%c6", "C6%"],
            "c7_residency": ["CPU%c7", "C7%"],
            "c8_residency": ["Pkg%pc8", "C8%"],
            "c9_residency": ["Pkg%pc9", "C9%"],
            "c10_residency": ["Pk%pc10", "Pkg%pc10", "C10%"],
            # GPU
            "gfx_rc6": ["GFX%rc6", "RC6%"],
            "gfx_freq": ["GFXMHz", "GTMHz"],
            # Temperature
            "package_temp": ["PkgTmp", "PkgTemp", "CPU_Temp"],
            # Frequency
            "bzy_mhz": ["Bzy_MHz"],
            "tsc_mhz": ["TSC_MHz"],
            "avg_mhz": ["Avg_MHz"],
            # Power
            "pkg_watts": ["PkgWatt"],
            "cor_watts": ["CorWatt"],
            "gfx_watts": ["GFXWatt"],
            "ram_watts": ["RAMWatt"],
            # Other
            "ipc": ["IPC"],
            "irq": ["IRQ"],
            "busy_percent": ["Busy%"],
        }

        # Find which columns exist
        column_mapping = {}
        available_metrics = []

        for metric, possible_names in wanted_metrics.items():
            for name in possible_names:
                if name in available_columns:
                    column_mapping[metric] = name
                    # Track available C-states
                    if metric.startswith("c") and metric[1].isdigit():
                        c_num = metric[1:3].rstrip("_")
                        if c_num not in available_metrics:
                            available_metrics.append(c_num)
                    break

        # Also scan for any C-state columns we might have missed
        for col in available_columns:
            if col.startswith("C") and "%" in col:
                c_num = col.replace("C", "").replace("%", "").replace("ACPI", "")
                if c_num.isdigit() and c_num not in available_metrics:
                    available_metrics.append(c_num)
            elif col.startswith("CPU%c"):
                c_num = col.replace("CPU%c", "")
                if c_num.isdigit() and c_num not in available_metrics:
                    available_metrics.append(c_num)

        turbostat_config["columns"] = column_mapping
        turbostat_config["available_metrics"] = sorted(list(set(available_metrics)))
        turbostat_config["detection_method"] = "csv_parser"

    except Exception as e:
        turbostat_config["error"] = str(e)

    return turbostat_config


# ============================================================================
# RAPL DETECTION
# ============================================================================


def detect_rapl_paths() -> Tuple[Dict[str, str], List[str]]:
    """Detect RAPL paths and available domains."""
    rapl_paths = {}
    rapl_domains = []

    # Check both possible locations
    search_paths = [
        "/sys/class/powercap/intel-rapl*",
        "/sys/devices/virtual/powercap/intel-rapl*",
    ]

    for pattern in search_paths:
        for base in glob.glob(pattern):
            if os.path.isdir(base) or os.path.islink(base):
                # Follow symlink if needed
                if os.path.islink(base):
                    target = os.readlink(base)
                    if not target.startswith("/"):
                        target = os.path.join(os.path.dirname(base), target)
                    base = os.path.realpath(target)

                energy_file = os.path.join(base, "energy_uj")
                name_file = os.path.join(base, "name")

                if os.path.exists(energy_file):
                    # Get domain name
                    if os.path.exists(name_file):
                        with open(name_file, "r") as f:
                            domain_name = f.read().strip()
                    else:
                        domain_name = os.path.basename(base)

                    # Clean up domain name
                    domain_name = domain_name.replace("intel-rapl:", "")

                    if domain_name not in rapl_paths:
                        rapl_paths[domain_name] = energy_file
                        rapl_domains.append(domain_name)

    return rapl_paths, rapl_domains


# ============================================================================
# THERMAL DETECTION
# ============================================================================


def detect_thermal_paths() -> Tuple[Dict[str, str], Optional[str]]:
    """Detect thermal zones and identify package temperature."""
    thermal_paths = {}
    package_temp = None

    for zone in glob.glob("/sys/class/thermal/thermal_zone*"):
        temp_file = os.path.join(zone, "temp")
        type_file = os.path.join(zone, "type")

        if os.path.exists(temp_file) and os.path.exists(type_file):
            with open(type_file, "r") as f:
                zone_type = f.read().strip()
            thermal_paths[zone_type] = temp_file

            # Identify package temperature
            if any(pkg in zone_type.lower() for pkg in ["pkg", "package", "x86"]):
                package_temp = zone_type

    return thermal_paths, package_temp


# ============================================================================
# MSR DETECTION
# ============================================================================


def detect_msr_devices() -> List[str]:
    """Detect available MSR devices."""
    msr_devices = []
    cpu_count = os.cpu_count() or 8

    for cpu in range(cpu_count):
        msr_path = f"/dev/cpu/{cpu}/msr"
        if os.path.exists(msr_path):
            msr_devices.append(msr_path)

    return msr_devices


# ============================================================================
# CPU FREQUENCY DETECTION
# ============================================================================


def detect_cpufreq_paths() -> Dict[str, str]:
    """Detect CPU frequency scaling paths."""
    cpufreq_paths = {}
    freq_base = "/sys/devices/system/cpu/cpu0/cpufreq"

    if os.path.exists(freq_base):
        for fname in ["scaling_cur_freq", "scaling_max_freq", "scaling_min_freq"]:
            path = os.path.join(freq_base, fname)
            if os.path.exists(path):
                cpufreq_paths[fname] = path

    return cpufreq_paths


# ============================================================================
# CPU INFO (with TSC frequency detection)
# ============================================================================


def get_cpu_info() -> Dict[str, Any]:
    """Get CPU core information, model, vendor, microcode, and TSC frequency."""
    logical = os.cpu_count() or 8
    physical = logical // 2 if logical > 1 else 1

    # Get CPU details from /proc/cpuinfo
    cpu_model = "Unknown"
    cpu_vendor = "Unknown"
    microcode = "Unknown"

    try:
        with open("/proc/cpuinfo", "r") as f:
            for line in f:
                if line.startswith("model name"):
                    cpu_model = line.split(":")[1].strip()
                elif line.startswith("vendor_id"):
                    cpu_vendor = line.split(":")[1].strip()
                elif line.startswith("microcode"):
                    microcode = line.split(":")[1].strip()
    except:
        pass

    # Try to get actual physical core count
    try:
        with open("/proc/cpuinfo", "r") as f:
            cores = set()
            for line in f:
                if line.startswith("core id"):
                    cores.add(line.strip().split(":")[-1].strip())
            if cores:
                physical = len(cores)
    except:
        pass

    # Build CPU info dictionary with all fields
    cpu_info = {
        "model": cpu_model,
        "vendor": cpu_vendor,
        "microcode": microcode,
        "physical_cores": physical,
        "logical_cores": logical,
        "cores_list": list(range(logical)),
    }

    # ========================================================================
    # Detect TSC frequency (hardware constant for MSR conversion)
    # ========================================================================
    tsc_hz = detect_tsc_frequency()
    if tsc_hz:
        cpu_info["tsc_frequency_hz"] = tsc_hz
        cpu_info["tsc_detection_method"] = "auto_detected"
    else:
        cpu_info["tsc_frequency_hz"] = None
        cpu_info["tsc_detection_method"] = "not_detected"
        print("   ⚠️  TSC frequency not detected - MSRReader will use fallback")

    return cpu_info


# ============================================================================
# MAIN DETECTION FUNCTION
# ============================================================================


def get_system_info() -> Dict[str, Any]:
    """Get system manufacturer, product, type, virtualization"""
    info = {"manufacturer": None, "product": None, "type": None, "virtualization": None}

    # Get manufacturer
    try:
        with open("/sys/class/dmi/id/sys_vendor", "r") as f:
            info["manufacturer"] = f.read().strip()
    except:
        pass

    # Get product name
    try:
        with open("/sys/class/dmi/id/product_name", "r") as f:
            info["product"] = f.read().strip()
    except:
        pass

    # Get chassis type (converts number to type)
    try:
        with open("/sys/class/dmi/id/chassis_type", "r") as f:
            chassis = f.read().strip()
            # Map common chassis types
            chassis_map = {
                "3": "desktop",
                "4": "desktop",
                "8": "laptop",
                "9": "laptop",
                "10": "laptop",
                "17": "server",
            }
            info["type"] = chassis_map.get(chassis, "unknown")
    except:
        pass

    # Get virtualization type
    try:
        import subprocess

        result = subprocess.run(["systemd-detect-virt"], capture_output=True, text=True)
        virt = result.stdout.strip()
        info["virtualization"] = virt if virt != "none" else None
    except:
        pass

    return info


def detect_all_hardware(verbose: bool = False) -> Dict[str, Any]:
    """Run all hardware detection and return complete config."""

    if verbose:
        print("\n🔍 Detecting hardware...")

    # ============================================================
    # NEW: Get CPU flags, details, GPU info, and generate hash
    # ============================================================
    cpu_flags = get_cpu_flags()
    cpu_details = get_cpu_details()
    gpu_info = get_gpu_info()
    system_info = get_system_info()

    rapl_paths, rapl_domains = detect_rapl_paths()
    if verbose:
        print(f"   ✅ RAPL: {len(rapl_paths)} domains")

    thermal_paths, package_temp = detect_thermal_paths()
    if verbose:
        print(f"   ✅ Thermal: {len(thermal_paths)} zones")

    msr_devices = detect_msr_devices()
    if verbose:
        print(f"   ✅ MSR: {len(msr_devices)} devices")

    cpufreq_paths = detect_cpufreq_paths()
    if verbose:
        print(f"   ✅ CPU Freq: {len(cpufreq_paths)} paths")

    # Get CPU info (includes TSC frequency)
    cpu_info = get_cpu_info()
    if verbose:
        print(
            f"   ✅ CPU: {cpu_info['physical_cores']} physical, {cpu_info['logical_cores']} logical"
        )
        if cpu_info.get("tsc_frequency_hz"):
            print(f"   ✅ TSC: {cpu_info['tsc_frequency_hz']/1e6:.0f} MHz")

    ring_bus_limits = detect_ring_bus_limits()
    if verbose and ring_bus_limits.get("min_mhz"):
        print(
            f"   ✅ Ring bus limits: min={ring_bus_limits['min_mhz']} MHz, max={ring_bus_limits['max_mhz']} MHz"
        )

    # NEW: Detect ring bus sysfs paths
    ring_bus_paths = detect_ring_bus_sysfs_paths()
    if ring_bus_paths and verbose:
        print(f"   ✅ Ring bus sysfs paths: {len(ring_bus_paths)} found")

    turbostat_config = detect_turbostat_columns()
    if verbose:
        if turbostat_config["available"]:
            cols = len(turbostat_config["columns"])
            print(f"   ✅ Turbostat: {cols} columns detected")
        else:
            print(f"   ⚠️  Turbostat: Not available")

    # ============================================================
    # Create config variable FIRST
    # ============================================================
    config = {
        "metadata": {
            "detected_at": datetime.now().isoformat(),
            "hostname": platform.node(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "rapl": {
            "paths": rapl_paths,
            "available_domains": rapl_domains,
            "has_dram": any("dram" in d.lower() for d in rapl_domains),
        },
        "thermal": {"paths": thermal_paths, "package_temp": package_temp},
        "msr": {"devices": msr_devices, "count": len(msr_devices)},
        "cpufreq": {"paths": cpufreq_paths},
        "cpu": cpu_info,
        "ring_bus": {**ring_bus_limits, "sysfs_paths": ring_bus_paths},
        "turbostat": turbostat_config,
        "cpu_flags": cpu_flags,
        "cpu_details": cpu_details,
        "gpu": gpu_info,
        "system": system_info,
        # ============================================================
        # ADD THESE FLAT FIELDS for hash generation
        # ============================================================
        "cpu_model": cpu_info.get("model"),
        "cpu_cores": cpu_info.get("physical_cores"),
        "ram_gb": psutil.virtual_memory().total // (1024**3),
        "gpu_model": gpu_info.get("model"),
        # ============================================================
        # NEW FLAT FIELDS from system info
        # ============================================================
        "system_manufacturer": system_info.get("manufacturer"),
        "system_product": system_info.get("product"),
        "system_type": system_info.get("type"),
        "virtualization_type": system_info.get("virtualization"),
        "cpu_vendor": cpu_info.get("vendor"),
        "microcode_version": cpu_info.get("microcode"),
    }

    # ============================================================
    # Add hash AFTER config is created
    # ============================================================
    config["hardware_hash"] = generate_hardware_hash(config)

    # ============================================================
    # Return the config
    # ============================================================
    return config


# Add to detect_hardware.py


def get_cpu_flags():
    """Extract CPU flags for AVX2/AVX512/VMX detection"""
    flags = {}
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("flags"):
                    flag_str = line.split(":")[1].strip()
                    flags = {
                        "has_avx2": "avx2" in flag_str,
                        "has_avx512": "avx512" in flag_str,
                        "has_vmx": "vmx" in flag_str,
                    }
                    break
    except:
        pass
    return flags


def get_cpu_details():
    """Get detailed CPU info (family, model, stepping)"""
    details = {"family": None, "model": None, "stepping": None}
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if "cpu family" in line:
                    details["family"] = int(line.split(":")[1].strip())
                elif "model" in line and "model name" not in line:
                    details["model"] = int(line.split(":")[1].strip())
                elif "stepping" in line:
                    details["stepping"] = int(line.split(":")[1].strip())
    except:
        pass
    return details


def get_gpu_info():
    """Add GPU detection"""
    gpu = {"model": None, "driver": None, "count": 0}
    try:
        import subprocess

        output = subprocess.check_output(["lspci"], text=True)
        for line in output.split("\n"):
            if "VGA" in line or "3D" in line:
                gpu["count"] += 1
                if "Intel" in line:
                    gpu["model"] = line.split("Intel")[1].strip()
                    gpu["driver"] = "i915"
                elif "NVIDIA" in line:
                    gpu["model"] = line.split("NVIDIA")[1].strip()
                    gpu["driver"] = "nvidia"
    except:
        pass
    return gpu


def generate_hardware_hash(hw_info):
    """Generate unique hardware fingerprint"""
    import hashlib
    import json

    hash_str = json.dumps(
        {
            "cpu_model": hw_info.get("cpu_model"),
            "cpu_cores": hw_info.get("cpu_cores"),
            "ram_gb": hw_info.get("ram_gb"),
            "gpu_model": hw_info.get("gpu_model"),
        },
        sort_keys=True,
    )

    return hashlib.sha256(hash_str.encode()).hexdigest()[:16]


# ============================================================================
# MAIN
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="A-LEMS Hardware Detection Utility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # First run (with sudo for full detection)
  sudo python scripts/detect_hardware.py --output config/hw_config.json --merge
  
  # Subsequent runs (no sudo needed)
  python scripts/detect_hardware.py --output config/hw_config.json --merge
  
  # Just view hardware info
  python scripts/detect_hardware.py --verbose
  
NOTE: 
  - Sudo is ONLY needed during first detection to access turbostat/MSR
  - The generated config contains ONLY column names, NO sudo flags
  - Module 1 will use these column names WITHOUT sudo
  - MSR configuration values are auto-detected - no manual editing needed!
        """,
    )
    parser.add_argument("--output", "-o", type=str, help="Output file path")
    parser.add_argument(
        "--merge",
        "-m",
        action="store_true",
        help="Merge with existing config instead of overwriting",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show detailed output"
    )
    parser.add_argument(
        "--backup-count",
        type=int,
        default=5,
        help="Number of backups to keep (default: 5)",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Disable automatic backup (not recommended)",
    )
    args = parser.parse_args()

    # Detect hardware
    new_config = detect_all_hardware(verbose=args.verbose)

    # Handle output
    if args.output:
        output_path = Path(args.output)

        # Create parent directories if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # CREATE BACKUP BEFORE ANY MODIFICATION
        if output_path.exists() and not args.no_backup:
            backup_path = create_backup(output_path, args.backup_count)
            if backup_path:
                if args.verbose:
                    print(f"✅ Existing config backed up")
            else:
                print("⚠️  Continuing without backup...")

        # Load existing config if merging
        if args.merge and output_path.exists():
            if args.verbose:
                print(f"📂 Loading existing config: {output_path}")
            try:
                with open(output_path, "r") as f:
                    existing = json.load(f)

                # Merge new detection with existing
                if args.verbose:
                    print("🔄 Merging new detection with existing config...")

                # ============================================================
                # Update sections that should be fully replaced (hardware-detected)
                # ============================================================
                existing["metadata"] = new_config["metadata"]
                existing["rapl"] = new_config["rapl"]
                existing["thermal"] = new_config["thermal"]
                existing["cpufreq"] = new_config["cpufreq"]
                existing["turbostat"] = new_config["turbostat"]

                # ============================================================
                # MSR section - PRESERVE ALL EXISTING FIELDS, add new ones
                # ============================================================
                # Start with existing MSR config (has all custom fields)
                existing_msr = existing.get("msr", {})

                # Get new MSR config from detection (has devices, count)
                new_msr = new_config.get("msr", {})

                # Update with detected values (preserves custom fields)
                for key, value in new_msr.items():
                    existing_msr[key] = value

                # ENHANCE: Add auto-detected MSR configuration values
                # This will add cstate_counter_max, ring_bus_base_clock_mhz, wakeup_idle_ms
                # if they don't already exist (preserves manually added values)
                existing_msr = enhance_msr_config(existing_msr)
                # ============================================================
                # HARDWARE FINGERPRINT FIELDS HERE
                # ============================================================
                if args.verbose:
                    print("🔍 Adding hardware fingerprint fields...")

                existing["cpu_flags"] = get_cpu_flags()
                existing["cpu_details"] = get_cpu_details()
                existing["gpu"] = get_gpu_info()
                existing["hardware_hash"] = generate_hardware_hash(existing)

                # ============================================================
                # ENHANCE: Add thermal discovery with semantic mapping
                # ============================================================
                print("🔍 DEBUG: Entering thermal enhancement block")
                if args.verbose:
                    print("🔍 Discovering thermal zones...")

                # Discover thermal zones with duplicate handling
                thermal_zones = discover_thermal_zones()
                print("🔍 DEBUG: Entering thermal enhancement block")

                # Update thermal section with discovered zones
                if "thermal" not in existing:
                    existing["thermal"] = {}

                existing["thermal"]["discovered_zones"] = thermal_zones

                # ============================================================
                # ENHANCE: Add thermal discovery with semantic mapping
                # ============================================================
                print("🔍 DEBUG: Entering thermal enhancement block")
                if args.verbose:
                    print("🔍 Discovering thermal zones...")

                # Discover thermal zones with duplicate handling
                thermal_zones = discover_thermal_zones()
                print("🔍 DEBUG: Entering thermal enhancement block")

                # Update thermal section with discovered zones
                if "thermal" not in existing:
                    existing["thermal"] = {}

                existing["thermal"]["discovered_zones"] = thermal_zones

                # ====================================================================
                # Create sensors_to_monitor from discovered zones with smart naming
                # ====================================================================
                sensors_to_monitor = {}

                for sensor_type in thermal_zones.keys():
                    sensor_lower = sensor_type.lower()

                    # Map known sensor types to stable roles
                    if "x86_pkg_temp" in sensor_type or (
                        "cpu" in sensor_lower and "temp" in sensor_lower
                    ):
                        sensors_to_monitor["cpu_package"] = sensor_type
                    elif "tcpu" in sensor_lower:
                        sensors_to_monitor["cpu_alt"] = sensor_type
                    elif "wifi" in sensor_lower or "iwlwifi" in sensor_lower:
                        sensors_to_monitor["wifi"] = sensor_type
                    elif "acpi" in sensor_lower or "int3400" in sensor_lower:
                        sensors_to_monitor["system"] = sensor_type
                    elif "sen" in sensor_lower and sensor_lower[3:].isdigit():
                        # Keep SEN1, SEN2, etc. as themselves
                        sensors_to_monitor[sensor_type] = sensor_type
                    else:
                        # For any other sensors, use their actual type as the key
                        sensors_to_monitor[sensor_type] = sensor_type

                existing["thermal"]["sensors_to_monitor"] = sensors_to_monitor
                existing["thermal"]["sampling_rate_hz"] = 1
                existing["config_version"] = 2

                if args.verbose:
                    discovered_count = sum(len(v) for v in thermal_zones.values())
                    print(
                        f"   ✅ Discovered {discovered_count} thermal sensors across {len(thermal_zones)} types"
                    )
                    print(
                        f"   ✅ Monitoring {len(sensors_to_monitor)} sensors with stable roles"
                    )

                existing["thermal"]["sampling_rate_hz"] = 1
                existing["config_version"] = 2

                if args.verbose:
                    discovered_count = sum(len(v) for v in thermal_zones.values())
                    print(
                        f"   ✅ Discovered {discovered_count} thermal sensors across {len(thermal_zones)} types"
                    )
                # Save back
                existing["msr"] = existing_msr

                # ============================================================
                # CPU section - PRESERVE, add TSC frequency
                # ============================================================
                new_cpu = new_config.get("cpu", {})
                if "cpu" not in existing:
                    existing["cpu"] = {}

                # Update core counts (these are detected)
                for key in ["physical_cores", "logical_cores", "cores_list"]:
                    if key in new_cpu:
                        existing["cpu"][key] = new_cpu[key]

                # Add TSC frequency if detected (APPEND, never remove)
                if "tsc_frequency_hz" in new_cpu:
                    existing["cpu"]["tsc_frequency_hz"] = new_cpu["tsc_frequency_hz"]
                    existing["cpu"]["tsc_detection_method"] = new_cpu.get(
                        "tsc_detection_method", "auto_detected"
                    )
                # ============================================================
                # NEW: Add ring_bus section if it exists in new_config
                # ============================================================
                if "ring_bus" in new_config and new_config["ring_bus"]:
                    existing["ring_bus"] = new_config["ring_bus"]
                    if args.verbose:
                        print(f"   ✅ Added ring_bus section: {new_config['ring_bus']}")

                # Update turbostat (always replace)
                existing["turbostat"] = new_config["turbostat"]
                final_config = existing
                if args.verbose:
                    print("✅ Config merged successfully - all custom fields preserved")

            except Exception as e:
                print(f"⚠️  Error reading existing config: {e}")
                print("   Creating new config instead")
                final_config = new_config
        else:
            # No existing config or not merging - create new with enhanced MSR config
            final_config = new_config
            # Enhance new config with auto-detected MSR values
            if "msr" in final_config:
                final_config["msr"] = enhance_msr_config(final_config["msr"])

            # ============================================================
            # ADD THERMAL ENHANCEMENT HERE
            # ============================================================
            if args.verbose:
                print("🔍 Discovering thermal zones...")

            thermal_zones = discover_thermal_zones()
            if "thermal" not in final_config:
                final_config["thermal"] = {}

            final_config["thermal"]["discovered_zones"] = thermal_zones
            final_config["thermal"]["sensors_to_monitor"] = {
                "cpu_package": "x86_pkg_temp",
                "cpu_alt": "TCPU",
                "wifi": "iwlwifi_1",
                "system": "INT3400 Thermal",
            }
            final_config["thermal"]["sampling_rate_hz"] = 1
            final_config["config_version"] = 2

            if args.verbose:
                discovered_count = sum(len(v) for v in thermal_zones.values())
                print(f"   ✅ Discovered {discovered_count} thermal sensors")
            # ============================================================
            # ADD HARDWARE FINGERPRINT FIELDS HERE (2 levels indentation)
            # ============================================================
            if args.verbose:
                print("🔍 Adding hardware fingerprint fields...")

            # Add CPU flags, details, GPU info, and hardware hash
            final_config["cpu_flags"] = get_cpu_flags()
            final_config["cpu_details"] = get_cpu_details()
            final_config["gpu"] = get_gpu_info()
            final_config["hardware_hash"] = generate_hardware_hash(final_config)

            if args.verbose:
                print(f"   ✅ Hardware hash: {final_config['hardware_hash']}")
                if final_config["gpu"].get("model"):
                    print(f"   ✅ GPU: {final_config['gpu']['model']}")
                print(
                    f"   ✅ CPU flags: AVX2={final_config['cpu_flags']['has_avx2']}, AVX512={final_config['cpu_flags']['has_avx512']}"
                )

            if args.verbose:
                print("📝 Creating new config file with auto-detected MSR values")

        # Save
        try:
            with open(output_path, "w") as f:
                json.dump(final_config, f, indent=2)
            print(f"✅ Hardware config saved to: {output_path}")
        except PermissionError:
            print(f"❌ Permission denied writing to {output_path}")
            print(f"   Try: sudo chown $USER:$USER {output_path.parent}")
            return 1

        # Summary
        if args.verbose:
            print("\n📊 Summary:")
            print(f"   RAPL domains: {len(final_config['rapl']['paths'])}")
            print(f"   Thermal zones: {len(final_config['thermal']['paths'])}")
            print(f"   MSR devices: {final_config['msr']['count']}")

            # Show auto-detected MSR config values
            msr_config = final_config.get("msr", {})
            if "cstate_counter_max" in msr_config:
                print(f"   C-state counter max: {msr_config['cstate_counter_max']}")
            if "ring_bus_base_clock_mhz" in msr_config:
                print(
                    f"   Ring bus base clock: {msr_config['ring_bus_base_clock_mhz']} MHz"
                )
            if "wakeup_idle_ms" in msr_config:
                print(f"   Wake-up idle period: {msr_config['wakeup_idle_ms']} ms")

            if final_config["turbostat"]["available"]:
                cols = len(final_config["turbostat"]["columns"])
                print(f"   Turbostat columns: {cols}")
                if final_config["turbostat"]["available_metrics"]:
                    metrics = ", ".join(final_config["turbostat"]["available_metrics"])
                    print(f"   Available C-states: {metrics}")
            else:
                print(f"   Turbostat: Not available")

            # Show TSC frequency in summary
            tsc = final_config.get("cpu", {}).get("tsc_frequency_hz")
            if tsc:
                print(f"   TSC frequency: {tsc/1e6:.0f} MHz")
    else:
        # Print to console
        print(json.dumps(new_config, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
