#!/usr/bin/env python3
"""
================================================================================
WATER METRICS – Data classes for water consumption
================================================================================

This module defines data structures for water usage.

Author: Deepak Panigrahy
================================================================================
"""

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class WaterMetrics:
    """
    Water consumption metrics for a single measurement.

    Attributes:
        liters: Water consumed in liters
        per_query_ml: Water per query in milliliters (normalized)
        source: Source of water intensity data
        year: Data year
        calculation_method: How this was derived
    """

    liters: float
    per_query_ml: Optional[float] = None
    source: str = "UN-Water 2025"
    source_url: Optional[str] = (
        "https://www.unwater.org/publications/un-world-water-development-report-2025"
    )
    year: int = 2025
    calculation_method: str = "energy_kwh × grid_water_intensity"

    def __post_init__(self):
        """Validate values."""
        if self.liters < 0:
            raise ValueError(f"Water cannot be negative: {self.liters}")

    @property
    def milliliters(self) -> float:
        """Water in milliliters."""
        return self.liters * 1000

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "liters": self.liters,
            "milliliters": self.milliliters,
            "per_query_ml": self.per_query_ml,
            "source": self.source,
            "source_url": self.source_url,
            "year": self.year,
            "calculation_method": self.calculation_method,
        }

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(self.to_dict(), indent=2)


@dataclass
class WaterIntensityFactors:
    """
    Grid water intensity factors for a country.

    Attributes:
        country_code: ISO 3166-1 alpha-2 code
        liters_per_kwh: Water intensity in L/kWh
        source: Data source
        year: Data year
        data_quality: "high", "medium", "low"
    """

    country_code: str
    liters_per_kwh: float
    source: str
    source_url: str
    year: int
    data_quality: str = "medium"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "country_code": self.country_code,
            "liters_per_kwh": self.liters_per_kwh,
            "source": self.source,
            "source_url": self.source_url,
            "year": self.year,
            "data_quality": self.data_quality,
        }
