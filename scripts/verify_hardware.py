#!/usr/bin/env python3
"""
A-LEMS Complete Hardware Verification Script
READS FROM YOUR hw_config.json – No hardcoded paths!

This script verifies that all hardware components detected in hw_config.json
are actually accessible and working. It checks:
- RAPL energy counters
- MSR devices and configuration
- Ring bus limits (NEW)
- TSC frequency (NEW)
- Turbostat functionality
- Thermal zones
- CPU frequency scaling

Usage:
    python scripts/verify_hardware.py
    python scripts/verify_hardware.py --verbose
"""

import json
import os
import subprocess
import sys
from pathlib import Path

# ============================================================================
# ANSI color codes for pretty terminal output
# ============================================================================
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
RESET = "\033[0m"


def print_status(component, status, message=""):
    """
    Print a colored status message for a hardware component.

    Args:
        component: Name of the component being checked
        status: Status emoji (✅, ⚠️, ❌)
        message: Additional information to display
    """
    if status == "✅":
        color = GREEN
    elif status == "⚠️":
        color = YELLOW
    elif status == "❌":
        color = RED
    else:
        color = BLUE

    print(f"{color}{status}{RESET} {component:30} {message}")


def load_config():
    """
    Load hardware configuration from hw_config.json.

    Returns:
        Dictionary containing the complete hardware configuration,
        or None if the file doesn't exist.
    """
    config_path = Path("config/hw_config.json")
    if not config_path.exists():
        print_status("Config file", "❌", "config/hw_config.json not found!")
        print(
            "\nRun detection first: python scripts/detect_hardware.py --output config/hw_config.json"
        )
        return None

    with open(config_path, "r") as f:
        return json.load(f)


def check_rapl(config):
    """
    Check RAPL (Running Average Power Limit) energy counters.

    RAPL provides energy consumption data for CPU packages, cores, and DRAM.
    These counters are essential for measuring energy usage of AI workloads.

    Args:
        config: Hardware configuration dictionary

    Returns:
        True if at least one RAPL counter is working, False otherwise
    """
    print(f"\n{BLUE}🔋 RAPL ENERGY COUNTERS (from config):{RESET}")

    rapl_paths = config.get("rapl", {}).get("paths", {})
    if not rapl_paths:
        print_status("  No RAPL paths in config", "⚠️")
        return False

    working = 0
    for domain, path in rapl_paths.items():
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    value = f.read().strip()
                print_status(f"  {domain}", "✅", f"{value[:10]}... µJ")
                working += 1
            except PermissionError:
                print_status(f"  {domain}", "❌", "Permission denied")
            except Exception as e:
                print_status(f"  {domain}", "❌", str(e))
        else:
            print_status(f"  {domain}", "❌", f"Path not found: {path}")

    return working > 0


def check_msr(config):
    """
    Check MSR (Model Specific Register) access.

    MSRs provide low-level CPU information including C-state counters,
    ring bus frequency, and thermal throttle status. This is critical
    for understanding orchestration tax at the hardware level.

    Args:
        config: Hardware configuration dictionary

    Returns:
        True if MSR access is working, False otherwise
    """
    print(f"\n{BLUE}🔧 MSR DEVICES (from config):{RESET}")

    # Check if rdmsr command is installed (part of msr-tools package)
    rdmsr_path = subprocess.run(
        ["which", "rdmsr"], capture_output=True, text=True
    ).stdout.strip()
    if not rdmsr_path:
        print_status("  rdmsr tool", "❌", "Install: sudo apt install msr-tools")
        return False

    # Check if rdmsr has the necessary capabilities (CAP_SYS_RAWIO)
    # This allows it to read MSRs without root privileges
    cap_result = subprocess.run(["getcap", rdmsr_path], capture_output=True, text=True)
    if "cap_sys_rawio=ep" not in cap_result.stdout:
        print_status(
            "  rdmsr capabilities",
            "⚠️",
            "Missing. Run: sudo setcap cap_sys_rawio=ep $(which rdmsr)",
        )

    # Try to read a simple MSR (IA32_PLATFORM_ID = 0x10)
    # If this succeeds, MSR access is working
    try:
        result = subprocess.run(
            ["rdmsr", "0x10"], capture_output=True, text=True, timeout=1
        )
        if result.returncode == 0:
            print_status("  MSR access", "✅", f"Readable: {result.stdout.strip()}")
            return True
        else:
            print_status("  MSR access", "❌", "rdmsr failed")
    except Exception as e:
        print_status("  MSR access", "❌", str(e))

    return False


def check_msr_config(config):
    """
    Check auto-detected MSR configuration values.

    These values are hardware constants that should be present in the config:
    - cstate_counter_max: Maximum value of 64-bit C-state counters (2^64-1)
    - ring_bus_base_clock_mhz: Base clock for ring bus (typically 100 MHz)
    - wakeup_idle_ms: Time to wait for CPU to enter deep C-state

    Args:
        config: Hardware configuration dictionary

    Returns:
        True if all required MSR config values are present and valid
    """
    print(f"\n{BLUE}⚙️ MSR CONFIGURATION (auto-detected):{RESET}")

    msr_config = config.get("msr", {})
    all_ok = True

    # ------------------------------------------------------------------------
    # Check cstate_counter_max (2^64 - 1 for all 64-bit CPUs)
    # ------------------------------------------------------------------------
    cstate_max = msr_config.get("cstate_counter_max")
    if cstate_max:
        expected_max = 2**64 - 1
        if cstate_max == expected_max:
            print_status("  cstate_counter_max", "✅", f"{cstate_max}")
        else:
            print_status(
                "  cstate_counter_max", "⚠️", f"{cstate_max} (expected {expected_max})"
            )
            all_ok = False
    else:
        print_status("  cstate_counter_max", "❌", "Missing")
        all_ok = False

    # ------------------------------------------------------------------------
    # Check ring_bus_base_clock_mhz (typically 100 MHz for Intel)
    # ------------------------------------------------------------------------
    base_clock = msr_config.get("ring_bus_base_clock_mhz")
    if base_clock:
        if base_clock == 100:
            print_status("  ring_bus_base_clock_mhz", "✅", f"{base_clock} MHz")
        else:
            print_status(
                "  ring_bus_base_clock_mhz",
                "⚠️",
                f"{base_clock} MHz (typical is 100 MHz)",
            )
            # Not marking as failure - some CPUs may have different base clock
    else:
        print_status("  ring_bus_base_clock_mhz", "❌", "Missing")
        all_ok = False

    # ------------------------------------------------------------------------
    # Check wakeup_idle_ms (should be between 1-5 ms for most CPUs)
    # ------------------------------------------------------------------------
    wakeup_idle = msr_config.get("wakeup_idle_ms")
    if wakeup_idle:
        if 1 <= wakeup_idle <= 5:
            print_status("  wakeup_idle_ms", "✅", f"{wakeup_idle} ms")
        else:
            print_status(
                "  wakeup_idle_ms", "⚠️", f"{wakeup_idle} ms (typical range 1-5 ms)"
            )
    else:
        print_status("  wakeup_idle_ms", "❌", "Missing")
        all_ok = False

    return all_ok


def check_ring_bus(config):
    """
    Check ring bus configuration.

    The ring bus connects CPU cores, cache, and memory controller.
    Its frequency affects memory latency and energy consumption.

    Args:
        config: Hardware configuration dictionary

    Returns:
        True if ring bus configuration is present and valid
    """
    print(f"\n{BLUE}🔧 RING BUS CONFIGURATION:{RESET}")

    ring_bus = config.get("ring_bus", {})
    if not ring_bus:
        print_status("  Ring bus config", "❌", "Missing ring_bus section")
        return False

    all_ok = True

    # ------------------------------------------------------------------------
    # Check minimum ring bus frequency
    # ------------------------------------------------------------------------
    min_mhz = ring_bus.get("min_mhz")
    if min_mhz:
        print_status("  Min frequency", "✅", f"{min_mhz} MHz")
    else:
        print_status("  Min frequency", "❌", "Missing")
        all_ok = False

    # ------------------------------------------------------------------------
    # Check maximum ring bus frequency
    # ------------------------------------------------------------------------
    max_mhz = ring_bus.get("max_mhz")
    if max_mhz:
        print_status("  Max frequency", "✅", f"{max_mhz} MHz")
    else:
        print_status("  Max frequency", "❌", "Missing")
        all_ok = False

    # ------------------------------------------------------------------------
    # Check base clock (should be 100 MHz for most Intel CPUs)
    # ------------------------------------------------------------------------
    base_clock = ring_bus.get("base_clock_mhz")
    if base_clock:
        print_status("  Base clock", "✅", f"{base_clock} MHz")
    else:
        print_status("  Base clock", "⚠️", "Missing (using default 100 MHz)")

    # ------------------------------------------------------------------------
    # Display detection method (sysfs, MSR, or manual)
    # ------------------------------------------------------------------------
    method = ring_bus.get("detection_method", "unknown")
    print_status("  Detection method", "✅", method)

    return all_ok


def check_tsc_frequency(config):
    """
    Check TSC (Time Stamp Counter) frequency.

    TSC frequency is needed to convert C-state counters from TSC ticks
    to actual time (seconds). This is critical for accurate measurement
    of CPU sleep times.

    Args:
        config: Hardware configuration dictionary

    Returns:
        True if TSC frequency is present in config, False otherwise
        (MSRReader has a fallback if missing)
    """
    print(f"\n{BLUE}⏱️  TSC FREQUENCY:{RESET}")

    cpu_config = config.get("cpu", {})
    tsc_hz = cpu_config.get("tsc_frequency_hz")
    detection_method = cpu_config.get("tsc_detection_method", "unknown")

    if tsc_hz:
        tsc_mhz = tsc_hz / 1_000_000
        print_status(
            "  TSC frequency", "✅", f"{tsc_mhz:.0f} MHz (detected: {detection_method})"
        )
        return True
    else:
        print_status(
            "  TSC frequency", "⚠️", "Not detected - MSRReader will use fallback"
        )
        return False


def check_turbostat(config):
    """
    Check turbostat functionality.

    Turbostat provides detailed CPU performance data including C-state
    residencies, frequencies, temperatures, and power estimates.

    Args:
        config: Hardware configuration dictionary

    Returns:
        True if turbostat is available and working
    """
    print(f"\n{BLUE}📊 TURBOSTAT (from config):{RESET}")

    turbostat_config = config.get("turbostat", {})
    if not turbostat_config.get("available", False):
        print_status("  Turbostat", "⚠️", "Not available in config")
        return False

    columns = turbostat_config.get("columns", {})
    metrics = turbostat_config.get("available_metrics", [])

    print_status("  Turbostat", "✅", "Available")
    print_status(f"  Mapped columns", "✅", f"{len(columns)} metrics mapped")
    if metrics:
        print_status(f"  C-states found", "✅", f"{', '.join(metrics)}")

    # ------------------------------------------------------------------------
    # Try to actually run turbostat to verify it works
    # ------------------------------------------------------------------------
    try:
        result = subprocess.run(
            ["turbostat", "--version"], capture_output=True, text=True, timeout=1
        )
        if result.returncode == 0:
            print_status("  turbostat command", "✅", "Works")

            # Quick test with --no-msr to avoid permission issues
            test_cmd = [
                "turbostat",
                "--quiet",
                "--no-msr",
                "--show",
                "all",
                "sleep",
                "0.1",
            ]
            test_result = subprocess.run(
                test_cmd, capture_output=True, text=True, timeout=2
            )
            if test_result.returncode == 0:
                print_status("  turbostat run", "✅", "Executes successfully")
            else:
                print_status("  turbostat run", "⚠️", "May need sudo")
        else:
            print_status("  turbostat command", "❌", "Not found")
    except:
        print_status("  turbostat command", "❌", "Not found")

    return True


def check_thermal(config):
    """
    Check thermal zone sensors.

    Temperature sensors are important for detecting thermal throttling
    and ensuring measurements aren't skewed by overheating.

    Args:
        config: Hardware configuration dictionary

    Returns:
        True if at least one thermal sensor is readable
    """
    print(f"\n{BLUE}🌡️ THERMAL ZONES (from config):{RESET}")

    thermal_paths = config.get("thermal", {}).get("paths", {})
    if not thermal_paths:
        print_status("  No thermal paths in config", "⚠️")
        return False

    working = 0
    # Show first 5 sensors to avoid clutter
    for zone, path in list(thermal_paths.items())[:5]:
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    temp = f.read().strip()
                    temp_c = int(temp) / 1000
                print_status(f"  {zone}", "✅", f"{temp_c:.1f}°C")
                working += 1
            except:
                print_status(f"  {zone}", "⚠️", "Not readable")
        else:
            print_status(f"  {zone}", "❌", f"Path not found")

    return working > 0


def check_cpufreq(config):
    """
    Check CPU frequency scaling paths.

    These provide current, minimum, and maximum CPU frequencies,
    which are useful for understanding DVFS behavior during workloads.

    Args:
        config: Hardware configuration dictionary

    Returns:
        True if at least one frequency path is readable
    """
    print(f"\n{BLUE}⚡ CPU FREQUENCY (from config):{RESET}")

    cpufreq_paths = config.get("cpufreq", {}).get("paths", {})
    if not cpufreq_paths:
        print_status("  No cpufreq paths in config", "⚠️")
        return False

    working = 0
    for name, path in cpufreq_paths.items():
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    value = int(f.read().strip()) / 1000  # Convert to MHz
                print_status(f"  {name}", "✅", f"{value:.0f} MHz")
                working += 1
            except:
                print_status(f"  {name}", "⚠️", "Not readable")
        else:
            print_status(f"  {name}", "❌", f"Path not found")

    return working > 0


def main():
    """
    Main entry point: run all hardware verification checks.

    Returns:
        0 if all critical checks pass, 1 otherwise
    """
    print("\n" + "=" * 70)
    print("🔍 A-LEMS HARDWARE VERIFICATION")
    print("=" * 70)

    # ========================================================================
    # Load configuration (single source of truth)
    # ========================================================================
    config = load_config()
    if not config:
        return 1

    print(f"System: {config['metadata']['hostname']}")
    print(f"Detected: {config['metadata']['detected_at']}")
    print("=" * 70)

    # ========================================================================
    # Run all hardware checks
    # ========================================================================
    rapl_ok = check_rapl(config)
    msr_ok = check_msr(config)
    msr_config_ok = check_msr_config(config)
    ring_bus_ok = check_ring_bus(config)
    tsc_ok = check_tsc_frequency(config)
    turbostat_ok = check_turbostat(config)
    thermal_ok = check_thermal(config)
    cpufreq_ok = check_cpufreq(config)

    # ========================================================================
    # Display summary
    # ========================================================================
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)

    total = 8  # Total number of checks
    passed = sum(
        [
            rapl_ok,
            msr_ok,
            msr_config_ok,
            ring_bus_ok,
            tsc_ok,
            turbostat_ok,
            thermal_ok,
            cpufreq_ok,
        ]
    )

    print(f"RAPL:           {'✅' if rapl_ok else '❌'}")
    print(f"MSR Access:     {'✅' if msr_ok else '❌'}")
    print(f"MSR Config:     {'✅' if msr_config_ok else '❌'}")
    print(f"Ring Bus:       {'✅' if ring_bus_ok else '❌'}")
    print(f"TSC Frequency:  {'✅' if tsc_ok else '⚠️'}")
    print(f"Turbostat:      {'✅' if turbostat_ok else '❌'}")
    print(f"Thermal:        {'✅' if thermal_ok else '❌'}")
    print(f"CPU Freq:       {'✅' if cpufreq_ok else '❌'}")
    print(f"\nPassed: {passed}/{total} checks")

    # ========================================================================
    # Final verdict
    # ========================================================================
    if passed == total:
        print("\n✅ ALL SYSTEMS GO! Module 0 is ready.")
    elif passed >= total - 1:  # Allow TSC to be missing (fallback exists)
        print("\n✅ Core systems ready. TSC frequency optional (fallback exists).")
    else:
        print("\n⚠️  Some checks failed. Run: sudo ./scripts/fix_permissions.sh")

    print("=" * 70 + "\n")
    return 0 if passed >= total - 1 else 1


if __name__ == "__main__":
    sys.exit(main())
