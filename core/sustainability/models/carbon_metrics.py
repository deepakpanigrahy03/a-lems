#!/usr/bin/env python3
"""
================================================================================
CARBON METRICS – Data classes for carbon footprint
================================================================================

This module defines data structures for carbon emissions with:
- Uncertainty quantification [Req 2.4]
- Confidence intervals
- Source attribution [Req 2.1]
- Time-aware factors (future)

Requirements Covered:
    Req 2.1: Country-Specific Carbon Intensity
    Req 2.4: Total Operational Carbon (CCI)
    Req 2.9: Carbon Receipt Per Task
    Req 2.16: Global Metadata Integrity

Author: Deepak Panigrahy
================================================================================
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class CarbonMetrics:
    """
    Carbon footprint metrics for a single measurement with uncertainty.

    Scientific Notes:
        - Carbon intensity varies by grid region and time of day
        - Uncertainty reflects grid reporting accuracy (±5-15%)
        - 95% confidence intervals are standard for GHG reporting
        - Per-query metrics require query_count validation

    Attributes:
        kg: Carbon dioxide equivalent in kilograms [Req 2.4]
        uncertainty_percent: Estimated uncertainty (±%) [Req 2.16]
        confidence_level: Statistical confidence level (default 0.95)
        per_query_mg: Carbon per query in milligrams [Req 2.9]
        source: Source of grid carbon intensity data [Req 2.1]
        source_url: Verifiable URL for data source [Req 2.16]
        year: Data year
        calculation_method: How this was derived
        timestamp: When calculation was performed
        metadata: Additional context (grid region, time of day)
    """

    # Core metric [Req 2.4]
    kg: float

    # Uncertainty quantification [Req 2.16]
    uncertainty_percent: float = 10.0  # Default ±10% for grid reporting
    confidence_level: float = 0.95  # 95% confidence interval

    # Per-query metrics [Req 2.9]
    per_query_mg: Optional[float] = None
    query_count_validated: bool = False  # Set True only after validation

    # Source attribution [Req 2.1, 2.16]
    source: str = "Ember 2026"
    source_url: Optional[str] = (
        "https://ember-energy.org/data/global-electricity-review-2026/"
    )
    year: int = 2026

    # Calculation metadata
    calculation_method: str = "energy_kwh × grid_carbon_intensity"
    timestamp: float = field(default_factory=lambda: datetime.now().timestamp())
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate values [Req 2.16]."""
        if self.kg < 0:
            raise ValueError(f"Carbon cannot be negative: {self.kg}")
        if self.uncertainty_percent < 0:
            raise ValueError(
                f"Uncertainty cannot be negative: {self.uncertainty_percent}"
            )
        if not 0 < self.confidence_level < 1:
            raise ValueError(
                f"Confidence level must be between 0 and 1: {self.confidence_level}"
            )

    # ========================================================================
    # Confidence Intervals [Req 2.16]
    # ========================================================================
    @property
    def kg_lower(self) -> float:
        """Lower bound of confidence interval."""
        return self.kg * (1 - self.uncertainty_percent / 100)

    @property
    def kg_upper(self) -> float:
        """Upper bound of confidence interval."""
        return self.kg * (1 + self.uncertainty_percent / 100)

    @property
    def interval_width_percent(self) -> float:
        """Width of confidence interval as percentage."""
        if self.kg == 0:
            return 0.0
        return ((self.kg_upper - self.kg_lower) / self.kg) * 100

    # ========================================================================
    # Unit Conversions
    # ========================================================================
    @property
    def grams(self) -> float:
        """Carbon in grams (for human readability)."""
        return self.kg * 1000

    @property
    def grams_lower(self) -> float:
        """Lower bound in grams."""
        return self.kg_lower * 1000

    @property
    def grams_upper(self) -> float:
        """Upper bound in grams."""
        return self.kg_upper * 1000

    @property
    def milligrams(self) -> float:
        """Carbon in milligrams (for per-query metrics)."""
        return self.kg * 1_000_000

    @property
    def milligrams_lower(self) -> float:
        """Lower bound in milligrams."""
        return self.kg_lower * 1_000_000

    @property
    def milligrams_upper(self) -> float:
        """Upper bound in milligrams."""
        return self.kg_upper * 1_000_000

    # ========================================================================
    # Validation Methods
    # ========================================================================
    def validate_query_count(self, query_count: int) -> None:
        """
        Validate that per-query metrics are meaningful.

        Args:
            query_count: Number of queries in measurement

        Raises:
            ValueError: If query_count <= 0

        Scientific Note:
            Per-query metrics assume energy measured corresponds exactly
            to the number of queries specified.
        """
        if query_count <= 0:
            raise ValueError(
                f"query_count must be positive (got {query_count}). "
                "Per-query metrics require valid query count."
            )
        self.query_count_validated = True

    # ========================================================================
    # Serialization
    # ========================================================================
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization [Req 2.16]."""
        return {
            "kg": self.kg,
            "kg_lower": self.kg_lower,
            "kg_upper": self.kg_upper,
            "uncertainty_percent": self.uncertainty_percent,
            "confidence_level": self.confidence_level,
            "interval_width_percent": self.interval_width_percent,
            "grams": self.grams,
            "grams_lower": self.grams_lower,
            "grams_upper": self.grams_upper,
            "milligrams": self.milligrams,
            "milligrams_lower": self.milligrams_lower,
            "milligrams_upper": self.milligrams_upper,
            "per_query_mg": self.per_query_mg,
            "query_count_validated": self.query_count_validated,
            "source": self.source,
            "source_url": self.source_url,
            "year": self.year,
            "calculation_method": self.calculation_method,
            "timestamp": self.timestamp,
            "timestamp_iso": datetime.fromtimestamp(self.timestamp).isoformat(),
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        """Serialize to JSON with pretty formatting."""
        return json.dumps(self.to_dict(), indent=2)

    def summary(self) -> str:
        """Human-readable summary with confidence intervals."""
        lines = [
            f"Carbon Footprint:",
            f"  {self.kg:.6f} kg CO₂e [{self.kg_lower:.6f}, {self.kg_upper:.6f}] (95% CI)",
            f"  Uncertainty: ±{self.uncertainty_percent:.1f}%",
            f"  {self.grams:.2f} g CO₂e [{self.grams_lower:.2f}, {self.grams_upper:.2f}]",
        ]
        if self.per_query_mg:
            lines.append(f"  Per query: {self.per_query_mg:.2f} mg CO₂e")
        lines.extend(
            [
                f"  Source: {self.source} ({self.year})",
                f"  Method: {self.calculation_method}",
            ]
        )
        return "\n".join(lines)


@dataclass
class CarbonIntensityFactors:
    """
    Grid carbon intensity factors for a country.

    Scientific Notes:
        - Values vary by country based on energy mix
        - Data quality indicates confidence in reporting
        - High quality: Official government statistics
        - Medium quality: Industry reports
        - Low quality: Estimates or interpolations

    Attributes:
        country_code: ISO 3166-1 alpha-2 code [Req 2.1]
        kg_per_kwh: Carbon intensity in kg CO₂e/kWh [Req 2.1]
        source: Data source name [Req 2.16]
        source_url: Verifiable URL [Req 2.16]
        year: Data year
        data_quality: "high", "medium", "low" [Req 2.16]
        uncertainty_percent: Reported uncertainty
        metadata: Additional context
    """

    country_code: str
    kg_per_kwh: float
    source: str
    source_url: str
    year: int
    data_quality: str = "medium"
    uncertainty_percent: float = 10.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate values."""
        if self.kg_per_kwh < 0:
            raise ValueError(f"Carbon intensity cannot be negative: {self.kg_per_kwh}")
        if self.data_quality not in ["high", "medium", "low"]:
            raise ValueError(f"Invalid data quality: {self.data_quality}")

    @property
    def quality_score(self) -> float:
        """Convert quality to numeric score (0-1)."""
        scores = {"high": 1.0, "medium": 0.7, "low": 0.4}
        return scores.get(self.data_quality, 0.5)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "country_code": self.country_code,
            "kg_per_kwh": self.kg_per_kwh,
            "source": self.source,
            "source_url": self.source_url,
            "year": self.year,
            "data_quality": self.data_quality,
            "quality_score": self.quality_score,
            "uncertainty_percent": self.uncertainty_percent,
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(self.to_dict(), indent=2)
