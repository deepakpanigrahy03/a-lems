#!/usr/bin/env python3
"""
================================================================================
RAPL ENERGY READER - Intel Running Average Power Limit Interface
================================================================================

This module provides access to Intel RAPL (Running Average Power Limit) energy
counters. RAPL is a hardware feature that provides energy consumption estimates
for different power domains within the CPU.

Why RAPL over external power meters:
- Microsecond-scale sampling possible
- Per-domain breakdown (critical for orchestration tax attribution)
- Available on all modern Intel/AMD systems
- No additional hardware required

RAPL provides energy in microjoules since system boot (cumulative counter).
To measure energy over an interval:
    1. Read counter at start (E1)
    2. Read counter at end (E2)
    3. Energy = E2 - E1 (handling wrap-around)

Reference: Intel Software Developer's Manual, Volume 3, Chapter 14.9

Author: Deepak Panigrahy
================================================================================
"""

import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Configure module logger
logger = logging.getLogger(__name__)


# ============================================================================
# RAPL READER CLASS
# ============================================================================


class RAPLReader:
    """
    Reads Intel RAPL (Running Average Power Limit) energy counters from sysfs.

    This class handles the low-level interaction with the RAPL interface,
    including:
    - Detecting available domains via name files
    - Reading energy values with retry logic
    - Handling permission issues gracefully
    - Supporting high-frequency sampling

    The reader is designed to be used by the EnergyEngine, not directly
    by end users.

    Req 1.1: RAPL energy measurement
    Req 1.3: Uncore waste calculation
    Req 1.46: High-frequency sampling support
    """

    # Standard domain names expected from the kernel
    # These are used for mapping, but we'll use whatever the system provides
    STANDARD_DOMAINS = {
        "package-0": "package",  # Primary package domain
        "package-1": "package",  # Second socket (if present)
        "core": "core",  # Core power plane
        "pp0": "core",  # Alternative name for core
        "uncore": "uncore",  # Uncore (cache, memory controller)
        "pp1": "uncore",  # Alternative name for uncore/graphics
        "dram": "dram",  # DRAM controller
        "psys": "psys",  # Platform (entire SoC)
        "gpu": "gpu",  # Integrated GPU (on some systems)
    }

    def __init__(self, config: Dict):
        """
        Initialize RAPL reader with hardware configuration.

        Args:
            config: Can be either:
                   - Full config with 'rapl' key: {'rapl': {'paths': {...}}}
                   - Direct rapl section: {'paths': {...}, 'available_domains': [...]}
        """
        # Handle both config formats
        if "rapl" in config:
            # Full config with nested rapl section (from main config)
            rapl_config = config["rapl"]
            logger.debug("Using nested rapl config format")
        else:
            # Already the rapl section (from sampling.py)
            rapl_config = config
            logger.debug("Using direct rapl config format")

        # Extract paths
        self.rapl_paths = rapl_config.get("paths", {})

        logger.info(
            "Initializing RAPLReader with %d configured domains", len(self.rapl_paths)
        )

        # Validate which paths are actually accessible
        self.available_paths = {}
        self.domain_map = {}  # Maps standard names to actual paths

        for domain_name, path in self.rapl_paths.items():
            if self._validate_path(path):
                self.available_paths[domain_name] = path

                # Map to standard domain name if possible
                std_name = self._get_standard_domain(domain_name)
                self.domain_map[std_name] = path

                logger.debug(
                    "RAPL domain available: %s -> %s (%s)", domain_name, std_name, path
                )
            else:
                logger.warning(
                    "RAPL domain not accessible: %s at %s", domain_name, path
                )

        # Log summary
        if self.available_paths:
            logger.info(
                "RAPLReader initialized with %d accessible domains: %s",
                len(self.available_paths),
                list(self.available_paths.keys()),
            )
        else:
            logger.error("No RAPL domains accessible! Check permissions.")

        # Initialize last readings for wrap-around detection
        self.last_readings = {}

    def _validate_path(self, path: str) -> bool:
        """
        Validate that a RAPL path exists and is readable.

        This method checks both file existence and read permissions.
        It's called during initialization to filter out inaccessible domains.

        Args:
            path: Sysfs path to validate

        Returns:
            True if path exists and is readable, False otherwise
        """
        path_obj = Path(path)

        # Check if file exists
        if not path_obj.exists():
            logger.debug("RAPL path does not exist: %s", path)
            return False

        # Check if we can read it (permissions)
        try:
            with open(path_obj, "r") as f:
                # Just read a small amount to test permissions
                f.read(10)
            return True
        except PermissionError:
            logger.debug("RAPL path permission denied: %s", path)
            return False
        except Exception as e:
            logger.debug("RAPL path error: %s - %s", path, e)
            return False

    def _get_standard_domain(self, domain_name: str) -> str:
        """
        Map a domain name to a standard type.

        Different systems use different naming conventions. This method
        attempts to map whatever the system provides to one of our
        standard domain types: 'package', 'core', 'uncore', 'dram', etc.

        Args:
            domain_name: Raw domain name from sysfs (e.g., 'package-0')

        Returns:
            Standardized domain name
        """
        domain_lower = domain_name.lower()

        # Check against known patterns
        for pattern, std_name in self.STANDARD_DOMAINS.items():
            if pattern in domain_lower:
                return std_name

        # If no match, return as-is (might be custom)
        logger.debug("Unknown RAPL domain type: %s", domain_name)
        return domain_name

    def read_energy(self) -> Dict[str, int]:
        """
        Read current energy values from all available RAPL domains.

        This method reads the cumulative energy counters from sysfs.
        The values are in microjoules and increase monotonically until
        they wrap around (typically after ~2^32 µJ, about 70 minutes on a 100W CPU).

        Returns:
            Dictionary mapping domain names to energy in microjoules.
            Example: {'package': 12345678, 'core': 8765432, 'uncore': 3580246}

        Raises:
            IOError: If a previously available path becomes inaccessible
        """
        readings = {}

        for domain_name, path in self.available_paths.items():
            try:
                with open(path, "r") as f:
                    value = int(f.read().strip())
                readings[domain_name] = value

                # Store for wrap-around detection
                self.last_readings[domain_name] = value

            except Exception as e:
                logger.error("Failed to read RAPL domain %s: %s", domain_name, e)
                # Re-raise as IOError to signal serious problem
                raise IOError(f"RAPL read failed for {domain_name}: {e}")

        return readings

    def read_energy_safe(self, max_retries: int = 3) -> Dict[str, int]:
        """
        Read energy with retry logic for transient failures.

        Args:
            max_retries: Number of retry attempts before giving up

        Returns:
            Dictionary mapping domain names to energy in microjoules
            (may be zeros if all retries fail)

        Req 1.46: Used by high-frequency sampler to ensure robust readings.
        """
        for attempt in range(max_retries):
            try:
                return self.read_energy()
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"RAPL read failed after {max_retries} attempts: {e}")
                    # Get domains via method
                    domains = self.get_available_domains()
                    return {domain: 0 for domain in domains}
                logger.debug(f"RAPL read attempt {attempt+1} failed, retrying...")
                time.sleep(0.01)  # Brief pause before retry
        return {}  # Fallback (shouldn't reach here)

    def read_with_retry(self, max_retries: int = 3) -> Dict[str, int]:
        """
        Read energy with retry logic for transient failures.

        Occasional read failures can happen due to kernel contention.
        This method retries a few times before giving up.

        Args:
            max_retries: Maximum number of retry attempts

        Returns:
            Dictionary of energy readings (may be partial if all retries fail)

        Note:
            If all retries fail, returns empty dict and logs error.
            This is better than crashing the entire experiment.
        """
        for attempt in range(max_retries):
            try:
                return self.read_energy()
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(
                        "RAPL read failed after %d attempts: %s", max_retries, e
                    )
                    return {}
                logger.debug("RAPL read attempt %d failed, retrying...", attempt + 1)
                time.sleep(0.01)  # Brief pause before retry

        return {}  # Should never reach here, but just in case

    def get_energy_delta(
        self, start_readings: Dict[str, int], end_readings: Dict[str, int]
    ) -> Dict[str, int]:
        """
        Calculate energy consumed between two readings.

        Handles counter wrap-around by assuming counters never wrap more than once
        in a measurement interval (safe assumption for typical experiment durations).

        Args:
            start_readings: Readings at start of interval
            end_readings: Readings at end of interval

        Returns:
            Dictionary mapping domains to energy consumed in microjoules
        """
        deltas = {}

        for domain, start_val in start_readings.items():
            if domain in end_readings:
                end_val = end_readings[domain]

                # Handle wrap-around (counter resets to 0 after reaching max)
                if end_val >= start_val:
                    delta = end_val - start_val
                else:
                    # Counter wrapped around
                    # Assume 32-bit counter (2^32 - 1 max)
                    max_val = 2**32 - 1
                    delta = (max_val - start_val) + end_val
                    logger.debug(
                        "RAPL counter wrapped for %s: %d -> %d, delta=%d",
                        domain,
                        start_val,
                        end_val,
                        delta,
                    )

                deltas[domain] = delta

        return deltas

    def get_available_domains(self) -> List[str]:
        """
        Get list of available RAPL domains.

        Returns:
            List of domain names that are currently accessible
        """
        return list(self.available_paths.keys())

    def has_dram(self) -> bool:
        """
        Check if DRAM energy counter is available.

        DRAM counter is only present on server CPUs and some high-end desktops.
        Mobile/laptop CPUs typically don't have it.

        Returns:
            True if DRAM domain is available
        """
        return any("dram" in domain.lower() for domain in self.available_paths)

    def get_package_path(self) -> Optional[str]:
        """
        Get the path to the package energy counter.

        The package domain is the most important – it measures total CPU energy.

        Returns:
            Path to package energy file, or None if not available
        """
        for domain, path in self.available_paths.items():
            if "package" in domain.lower():
                return path
        return None

    def get_core_path(self) -> Optional[str]:
        """
        Get the path to the core energy counter.

        Core energy is used to calculate uncore waste (package - core).

        Returns:
            Path to core energy file, or None if not available
        """
        for domain, path in self.available_paths.items():
            if domain.lower() in ["core", "pp0"]:
                return path
        return None

    def __str__(self) -> str:
        """
        String representation of RAPL reader state.

        Returns:
            Human-readable summary of available domains
        """
        domains = ", ".join(self.available_paths.keys())
        return f"RAPLReader(domains=[{domains}])"

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"RAPLReader(paths={self.available_paths})"


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    """
    Example: Testing RAPLReader with a simple measurement.

    This demonstrates basic usage and can be used to verify that
    RAPL is working correctly on your system.

    Run with: python -m core.readers.rapl_reader
    """
    import sys
    import time
    from pathlib import Path

    # Set up logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("\n" + "=" * 70)
    print("RAPL READER TEST")
    print("=" * 70)

    # Try to load config from Module 0
    config_path = Path("config/hw_config.json")
    if not config_path.exists():
        print("❌ No config file found. Using default paths for testing.")
        # Create minimal test config
        test_config = {
            "rapl": {
                "paths": {
                    "package-0": "/sys/class/powercap/intel-rapl:intel-rapl:0/energy_uj",
                    "core": "/sys/class/powercap/intel-rapl:intel-rapl:0:0/energy_uj",
                    "uncore": "/sys/class/powercap/intel-rapl:intel-rapl:0:1/energy_uj",
                }
            }
        }
        config = test_config
    else:
        import json

        with open(config_path) as f:
            config = json.load(f)
        print(f"✅ Loaded config from {config_path}")

    # Initialize reader
    try:
        reader = RAPLReader(config)
        print(f"\n📊 RAPL Reader: {reader}")
    except Exception as e:
        print(f"❌ Failed to initialize RAPLReader: {e}")
        sys.exit(1)

    # Take initial reading
    print("\n📝 Taking initial reading...")
    start_readings = reader.read_with_retry()
    if not start_readings:
        print("❌ Failed to read RAPL counters")
        sys.exit(1)

    for domain, value in start_readings.items():
        print(f"   {domain:10} = {value} µJ")

    # Wait a bit
    print("\n⏳ Waiting 2 seconds...")
    time.sleep(2)

    # Take final reading
    print("📝 Taking final reading...")
    end_readings = reader.read_with_retry()

    for domain, value in end_readings.items():
        print(f"   {domain:10} = {value} µJ")

    # Calculate delta
    deltas = reader.get_energy_delta(start_readings, end_readings)

    print("\n" + "=" * 70)
    print("ENERGY CONSUMED")
    print("=" * 70)
    for domain, delta in deltas.items():
        joules = delta / 1_000_000
        print(f"   {domain:10} = {delta:10} µJ ({joules:.6f} J)")

    # Calculate uncore waste if we have package and core
    if "package-0" in deltas and "core" in deltas:
        package = deltas.get("package-0", 0)
        core = deltas.get("core", 0)
        uncore_waste = max(0, package - core)
        print(f"\n📊 Uncore waste: {uncore_waste} µJ ({uncore_waste/1e6:.6f} J)")
        print("   (package - core) - Req 1.3")

    print("\n✅ Test complete!")
