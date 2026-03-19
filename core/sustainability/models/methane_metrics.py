#!/usr/bin/env python3
"""
================================================================================
METHANE METRICS – Data classes for methane emissions
================================================================================

This module defines data structures for methane and CO₂e calculations.

Author: Deepak Panigrahy
================================================================================
"""

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class MethaneMetrics:
    """
    Methane emissions metrics for a single measurement.

    Attributes:
        kg: Methane emitted in kilograms
        co2e_20yr: CO₂ equivalent using 20-year GWP
        co2e_100yr: CO₂ equivalent using 100-year GWP
        source: Source of methane leakage data
        year: Data year
        gwp_source: Source for GWP values (IPCC AR6)
        calculation_method: How this was derived
    """

    kg: float
    co2e_20yr: Optional[float] = None
    co2e_100yr: Optional[float] = None
    source: str = "IEA Methane Tracker 2026"
    source_url: Optional[str] = (
        "https://www.iea.org/reports/global-methane-tracker-2026"
    )
    year: int = 2026
    gwp_source: str = "IPCC AR6"
    gwp_source_url: str = "https://www.ipcc.ch/report/ar6/wg1/chapter/chapter-7/"
    calculation_method: str = "energy_kwh × grid_methane_leakage"

    def __post_init__(self):
        """Validate values."""
        if self.kg < 0:
            raise ValueError(f"Methane cannot be negative: {self.kg}")

    @property
    def grams(self) -> float:
        """Methane in grams."""
        return self.kg * 1000

    @property
    def milligrams(self) -> float:
        """Methane in milligrams."""
        return self.kg * 1_000_000

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "kg": self.kg,
            "grams": self.grams,
            "milligrams": self.milligrams,
            "co2e_20yr": self.co2e_20yr,
            "co2e_100yr": self.co2e_100yr,
            "source": self.source,
            "source_url": self.source_url,
            "year": self.year,
            "gwp_source": self.gwp_source,
            "gwp_source_url": self.gwp_source_url,
            "calculation_method": self.calculation_method,
        }

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(self.to_dict(), indent=2)


@dataclass
class MethaneLeakageFactors:
    """
    Grid methane leakage factors for a country.

    Attributes:
        country_code: ISO 3166-1 alpha-2 code
        g_per_kwh: Methane leakage in grams CH₄/kWh
        source: Data source
        year: Data year
        data_quality: "high", "medium", "low"
    """

    country_code: str
    g_per_kwh: float
    source: str
    source_url: str
    year: int
    data_quality: str = "medium"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "country_code": self.country_code,
            "g_per_kwh": self.g_per_kwh,
            "source": self.source,
            "source_url": self.source_url,
            "year": self.year,
            "data_quality": self.data_quality,
        }
