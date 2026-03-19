#!/usr/bin/env python3
"""
================================================================================
CORE PINNER UTILITY – CPU Affinity Management
================================================================================

This module provides CPU core pinning functionality to ensure consistent
measurements by preventing thread migration across cores (Req 1.15).

Features:
- Pin current process to specific cores
- Get current affinity
- Support both default and runtime core specifications
- Cross-platform (Linux, macOS fallback)

Usage:
    from core.utils.core_pinner import CorePinner

    # With default cores
    pinner = CorePinner(default_cores=[0, 1])
    pinner.pin_to_cores()  # Uses default

    # Or specify at runtime
    pinner.pin_to_cores([2, 3])  # Override default

    # Get current affinity
    current = pinner.get_current_affinity()

Author: Deepak Panigrahy
================================================================================
"""

import logging
import os
import platform
from typing import List, Optional

logger = logging.getLogger(__name__)


class CorePinner:
    """
    Manages CPU core affinity for consistent measurements.

    This class provides methods to pin the current process to specific
    CPU cores, preventing the OS scheduler from migrating threads and
    causing measurement noise (Req 1.15).
    """

    def __init__(self, default_cores: Optional[List[int]] = None):
        """
        Initialize the core pinner with optional default cores.

        Args:
            default_cores: List of core indices to use when no cores
                          are specified at runtime. If None, uses [0, 1].
        """
        self.default_cores = default_cores or [0, 1]
        self._check_platform()

    def _check_platform(self) -> None:
        """Check if platform supports CPU affinity."""
        if platform.system() != "Linux":
            logger.warning(f"CPU affinity not fully supported on {platform.system()}")

    def pin_to_cores(self, cores: Optional[List[int]] = None) -> bool:
        """
        Pin the current process to specified CPU cores.

        Args:
            cores: List of core indices to pin to. If None, uses default cores.

        Returns:
            True if successful, False otherwise.

        Raises:
            ValueError: If cores list is empty or contains invalid core numbers.

        Req 1.15: Physical Core Affinity – eliminates noise from thread migration.
        """
        cores_to_pin = cores if cores is not None else self.default_cores

        if not cores_to_pin:
            raise ValueError("Core list cannot be empty")

        if platform.system() != "Linux":
            logger.warning("CPU affinity only supported on Linux")
            return False

        try:
            import psutil

            process = psutil.Process()

            # Get available cores
            available_cores = list(range(psutil.cpu_count()))

            # Validate requested cores
            for core in cores_to_pin:
                if core not in available_cores:
                    raise ValueError(
                        f"Core {core} not available (0-{max(available_cores)})"
                    )

            # Set affinity
            process.cpu_affinity(cores_to_pin)

            # Verify it worked
            new_affinity = process.cpu_affinity()
            logger.debug(f"Pinned to cores: {new_affinity}")
            return True

        except ImportError:
            logger.error("psutil not installed. Cannot set CPU affinity.")
            return False
        except Exception as e:
            logger.error(f"Failed to set CPU affinity: {e}")
            return False

    def get_current_affinity(self) -> Optional[List[int]]:
        """
        Get the current CPU affinity for this process.

        Returns:
            List of core indices this process is allowed to run on,
            or None if unable to determine.
        """
        if platform.system() != "Linux":
            return None

        try:
            import psutil

            process = psutil.Process()
            return process.cpu_affinity()
        except Exception as e:
            logger.error(f"Failed to get CPU affinity: {e}")
            return None

    def __str__(self) -> str:
        """String representation."""
        current = self.get_current_affinity()
        if current:
            return f"CorePinner(current={current}, default={self.default_cores})"
        return f"CorePinner(default={self.default_cores})"


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import time

    print("\n" + "=" * 70)
    print("CORE PINNER TEST")
    print("=" * 70)

    # Create pinner with default cores
    pinner = CorePinner(default_cores=[0, 1])
    print(f"📊 {pinner}")

    # Pin to default cores
    print("\n📝 Pinning to default cores [0, 1]...")
    success = pinner.pin_to_cores()
    print(f"   Success: {success}")
    print(f"   Current affinity: {pinner.get_current_affinity()}")

    # Pin to different cores at runtime
    print("\n📝 Pinning to cores [2, 3]...")
    success = pinner.pin_to_cores([2, 3])
    print(f"   Success: {success}")
    print(f"   Current affinity: {pinner.get_current_affinity()}")

    # Demonstrate that it persists
    print("\n📝 Running a small workload...")
    start = time.time()
    for i in range(1000000):
        _ = i * i
    end = time.time()
    print(f"   Workload completed in {(end-start)*1000:.2f} ms")
    print(f"   Affinity still: {pinner.get_current_affinity()}")

    print("\n" + "=" * 70)
    print("✅ Test complete!")
    print("=" * 70)
