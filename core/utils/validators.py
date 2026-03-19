#!/usr/bin/env python3
"""
================================================================================
VALIDATORS – Check measurement quality (Req 1.46, Req 1.9)
================================================================================

Provides functions to validate energy measurements for quality issues such as:
- Negative energy (should not happen)
- Thermal throttling (high temperature)
- Too few samples
- Zero energy

Author: Deepak Panigrahy
================================================================================
"""

import sys
from pathlib import Path
from typing import Any, List, Tuple

# ============================================================================
# Fix Python path
# ============================================================================
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.utils.debug import dprint


class MeasurementValidator:
    """
    Static methods to validate energy measurements.
    """

    @staticmethod
    def validate(measurement: Any) -> Tuple[bool, List[str]]:
        """
        Perform all validation checks on a measurement.

        Args:
            measurement: An EnergyMeasurement object (expected to have
                         attributes: package_energy_uj, sample_count,
                         thermal.package_temperature_celsius, etc.)

        Returns:
            Tuple (is_valid, list_of_issues).
        """
        issues = []

        # 1. Check for negative energy
        if measurement.package_energy_uj < 0:
            issues.append(f"Negative package energy: {measurement.package_energy_uj}")

        # 2. Thermal throttling (Req 1.9)
        if hasattr(measurement, "thermal"):
            temp = getattr(measurement.thermal, "package_temperature_celsius", 0)
            if temp > 85:
                issues.append(f"High temperature ({temp:.1f}°C) – possible throttling")
            elif temp > 90:
                issues.append(
                    f"Very high temperature ({temp:.1f}°C) – throttling likely"
                )

        # 3. Sample count too low (Req 1.46)
        if measurement.sample_count < 5:
            issues.append(
                f"Too few samples ({measurement.sample_count}) – measurement may be unreliable"
            )

        # 4. Zero energy on a non‑zero duration
        if measurement.duration_seconds > 0.1 and measurement.package_energy_uj == 0:
            issues.append("Zero energy measured – possible hardware failure")

        # 5. Duration too short
        if measurement.duration_seconds < 0.01:
            issues.append(
                f"Measurement too short ({measurement.duration_seconds*1000:.2f}ms)"
            )

        dprint("Validation issues:", issues)
        return len(issues) == 0, issues
