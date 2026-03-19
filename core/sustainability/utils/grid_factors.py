#!/usr/bin/env python3
"""
================================================================================
GRID FACTORS – Load grid intensity data
================================================================================

Author: Deepak Panigrahy
================================================================================
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, Optional

# Set up logger
logger = logging.getLogger(__name__)


@dataclass
class GridFactors:
    """Grid factors for a country."""

    country_code: str
    carbon_kg_per_kwh: float
    water_l_per_kwh: float
    methane_g_per_kwh: float
    data_quality: str = "medium"
    sources: Dict = field(default_factory=dict)


class GridFactorManager:
    """Manages grid intensity factors by reading from config file."""

    WORLD_AVERAGES = {"carbon": 0.475, "water": 2.0, "methane": 0.18}

    def __init__(self, config_loader):
        """Initialize with config loader and load data from file."""
        self.config = config_loader
        self.cache = {}
        self._load_from_config()

    def _load_from_config(self):
        """Load grid factors from the actual config file."""
        try:
            # Load grid_intensity_2026.json via config loader
            grid_data = self.config.get_grid_intensity_data()
            # ===== ADD THIS =====
            print(f"\n🔍 GRID MANAGER received data from config loader")
            print(f"   Type: {type(grid_data)}")
            if grid_data:
                print(f"   Keys: {list(grid_data.keys())}")
                if "IN" in grid_data:
                    print(f"   ✅ India data in grid_data!")
                    print(f"      Carbon: {grid_data['IN'].get('carbon_intensity')}")
                else:
                    print(f"   ❌ India NOT in grid_data!")
            else:
                print(f"   ❌ grid_data is None or empty")
            # ====================
            if not grid_data:
                logger.warning("No grid intensity data found, using sample data")
                self._load_sample_data()
                return

            # Skip metadata and load each country
            for country_code, data in grid_data.items():
                if country_code == "metadata":
                    continue

                # Extract values with fallbacks to world averages
                carbon = data.get("carbon_intensity")
                water = data.get("water_intensity")
                methane = data.get("methane_leakage")

                # Only add if country has at least some data
                if carbon is not None or water is not None or methane is not None:
                    self.cache[country_code] = GridFactors(
                        country_code=country_code,
                        carbon_kg_per_kwh=(
                            carbon
                            if carbon is not None
                            else self.WORLD_AVERAGES["carbon"]
                        ),
                        water_l_per_kwh=(
                            water if water is not None else self.WORLD_AVERAGES["water"]
                        ),
                        methane_g_per_kwh=(
                            methane
                            if methane is not None
                            else self.WORLD_AVERAGES["methane"]
                        ),
                        data_quality=data.get("data_quality", "medium"),
                        sources={
                            "carbon": {
                                "name": data.get("carbon_source", "Ember 2026"),
                                "year": 2026,
                            },
                            "water": {
                                "name": data.get("water_source", "UN-Water 2025"),
                                "year": 2025,
                            },
                            "methane": {
                                "name": data.get("methane_source", "IEA 2026"),
                                "year": 2026,
                            },
                        },
                    )
                    logger.debug(f"Loaded grid factors for {country_code}")

            logger.info(f"Loaded grid factors for {len(self.cache)} countries")

        except Exception as e:
            logger.error(f"Failed to load grid factors: {e}")
            self._load_sample_data()

    def _load_sample_data(self):
        """Fallback sample data if config loading fails."""
        logger.warning("Using sample data for testing")
        self.cache["US"] = GridFactors(
            country_code="US",
            carbon_kg_per_kwh=0.385,
            water_l_per_kwh=1.8,
            methane_g_per_kwh=0.15,
            data_quality="high",
            sources={
                "carbon": {"name": "Sample Data - US", "year": 2026},
                "water": {"name": "Sample Data - US", "year": 2025},
                "methane": {"name": "Sample Data - US", "year": 2026},
            },
        )

    def get_factors(self, country_code: str = "US") -> GridFactors:
        """Get grid factors for a country."""
        country_code = country_code.upper()

        if country_code in self.cache:
            return self.cache[country_code]

        # Return world averages with proper source attribution
        logger.warning(
            f"Country {country_code} not found in grid data, using world averages"
        )
        return GridFactors(
            country_code=country_code,
            carbon_kg_per_kwh=self.WORLD_AVERAGES["carbon"],
            water_l_per_kwh=self.WORLD_AVERAGES["water"],
            methane_g_per_kwh=self.WORLD_AVERAGES["methane"],
            data_quality="low",
            sources={
                "carbon": {"name": "World Average", "year": 2026},
                "water": {"name": "World Average", "year": 2025},
                "methane": {"name": "World Average", "year": 2026},
            },
        )

    def list_countries(self) -> list:
        """Get list of available countries."""
        return list(self.cache.keys())
