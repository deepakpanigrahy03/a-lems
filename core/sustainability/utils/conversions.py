#!/usr/bin/env python3
"""
================================================================================
SUSTAINABILITY CONVERSION UTILITIES
================================================================================

Author: Deepak Panigrahy
================================================================================
"""


class EnergyConverter:
    """Energy unit conversions."""

    JOULES_PER_KWH = 3_600_000

    @classmethod
    def joules_to_kwh(cls, joules: float) -> float:
        return joules / cls.JOULES_PER_KWH


class UnitConverter:
    """General unit conversions."""

    @classmethod
    def kg_to_mg(cls, kg: float) -> float:
        return kg * 1_000_000

    @classmethod
    def liters_to_ml(cls, liters: float) -> float:
        return liters * 1000


class GWPCalculator:
    """Global Warming Potential calculations."""

    DEFAULT_GWP = {"CH4": {"20_year": 81, "100_year": 28}}

    def __init__(self, config_loader=None):
        self.gwp_values = self.DEFAULT_GWP

    def methane_to_co2e(self, methane_kg: float, year: int = 100) -> float:
        gwp = self.gwp_values["CH4"][f"{year}_year"]
        return methane_kg * gwp
