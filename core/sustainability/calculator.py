#!/usr/bin/env python3
"""
================================================================================
SUSTAINABILITY CALCULATOR – Module 2 Main Entry Point
================================================================================

Requirements Covered:
---------------------
Req 2.1: Country-Specific Carbon Intensity
Req 2.2: Country-Specific Water Intensity
Req 2.3: Methane Leakage Factor
Req 2.4: Total Operational Carbon (CCI)
Req 2.5: Total Water Consumption (WCI)
Req 2.6: Methane Emission Impact (MCI)
Req 2.7: Energy Per Query (EPQ)
Req 2.8: Wait-Tax Per Query (TPQ)
Req 2.9: Carbon Receipt Per Task
Req 2.10: Water Receipt Per Task
Req 2.12: Geographic Arbitrage Potential
Req 2.13: Energy Scarcity Index (ESI)
Req 2.16: Global Metadata Integrity

Author: Deepak Panigrahy
================================================================================
"""

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

# Fix Python path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.models.derived_energy_measurement import DerivedEnergyMeasurement
# Module 1 imports
from core.models.raw_energy_measurement import RawEnergyMeasurement
# Module 2 imports
from core.sustainability.models.carbon_metrics import CarbonMetrics
from core.sustainability.models.methane_metrics import MethaneMetrics
from core.sustainability.models.water_metrics import WaterMetrics
from core.sustainability.utils import (EnergyConverter, GridFactorManager,
                                       GWPCalculator, UnitConverter)

logger = logging.getLogger(__name__)


@dataclass
class SustainabilityResult:
    """
    Complete sustainability results for a measurement.

    Attributes:
        measurement_id: Reference to raw measurement
        country_code: Grid region used
        carbon: CarbonMetrics object
        water: WaterMetrics object
        methane: MethaneMetrics object
        energy_j: Total energy in joules (NEW)
        energy_kwh: Total energy in kilowatt-hours
        carbon_intensity_g_per_kwh: Grid carbon intensity used (NEW)
        water_intensity_l_per_kwh: Grid water intensity used (NEW)
        methane_intensity_g_per_kwh: Grid methane intensity used (NEW)
        timestamp: When calculation was performed
    """

    measurement_id: str
    country_code: str
    carbon: CarbonMetrics
    water: WaterMetrics
    methane: MethaneMetrics
    energy_kwh: float
    timestamp: float
    energy_j: float = 0.0
    carbon_intensity_g_per_kwh: float = 0.0
    water_intensity_l_per_kwh: float = 0.0
    methane_intensity_g_per_kwh: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "measurement_id": self.measurement_id,
            "country_code": self.country_code,
            "energy_j": self.energy_j,
            "energy_kwh": self.energy_kwh,
            "carbon_intensity_g_per_kwh": self.carbon_intensity_g_per_kwh,
            "carbon_intensity_g_per_kwh": self.carbon_intensity_g_per_kwh,
            "carbon_intensity_g_per_kwh": self.carbon_intensity_g_per_kwh,
            "carbon": self.carbon.to_dict(),
            "water": self.water.to_dict(),
            "methane": self.methane.to_dict(),
            "timestamp": self.timestamp,
            "timestamp_iso": time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp)
            ),
        }

    def summary(self) -> str:
        """Human-readable summary."""
        lines = [
            f"Sustainability Report for {self.measurement_id}",
            f"Grid: {self.country_code}",
            f"Energy: {self.energy_kwh:.6f} kWh ({self.energy_j:.4f} J)",  # UPDATED
            f"Carbon Intensity: {self.carbon_intensity_g_per_kwh:.3f} g/kWh",  # NEW
            f"Water Intensity: {self.water_intensity_l_per_kwh:.3f} L/kWh",  # NEW
            f"Methane Intensity: {self.methane_intensity_g_per_kwh:.3f} g/kWh",  # NEW
            "",
            f"Carbon: {self.carbon.kg*1000:.6f} g CO₂e  [Req 2.4]",
            (
                f"  • {self.carbon.per_query_mg:.6f} mg/query  [Req 2.9]"
                if self.carbon.per_query_mg
                else ""
            ),
            f"  • Source: {self.carbon.source} ({self.carbon.year})  [Req 2.1]",
            "",
            f"Water: {self.water.liters*1000:.6f} ml  [Req 2.5]",
            (
                f"  • {self.water.per_query_ml:.6f} ml/query  [Req 2.10]"
                if self.water.per_query_ml
                else ""
            ),
            f"  • Source: {self.water.source} ({self.water.year})  [Req 2.2]",
            "",
            f"Methane: {self.methane.grams:.6f} g CH₄  [Req 2.6]",
            (
                f"  • CO₂e (20yr): {self.methane.co2e_20yr*1000:.6f} g"
                if self.methane.co2e_20yr
                else ""
            ),
            (
                f"  • CO₂e (100yr): {self.methane.co2e_100yr*1000:.6f} g"
                if self.methane.co2e_100yr
                else ""
            ),
            f"  • Source: {self.methane.source} ({self.methane.year})  [Req 2.3]",
        ]
        return "\n".join(filter(None, lines))


class SustainabilityCalculator:
    """
    Main sustainability calculator.

    Transforms energy measurements into environmental impacts using
    country-specific grid factors from Module 0.

    Requirements:
        Req 2.1-2.3: Grid intensity factors
        Req 2.4-2.6: Environmental impacts
        Req 2.7-2.10: Per-query metrics
        Req 2.12-2.13: Geographic context
        Req 2.16: Data integrity
    """

    def __init__(self, config_loader):
        """
        Initialize with Module 0 configuration.

        Args:
            config_loader: Module 0 ConfigLoader instance

        Req 2.16: Global Metadata Integrity – loads verified sources
        """
        self.config = config_loader
        self.grid_manager = GridFactorManager(config_loader)  # Req 2.1-2.3
        self.gwp_calc = GWPCalculator(config_loader)  # Req 2.6

        logger.info("SustainabilityCalculator initialized")

    def calculate_from_raw(
        self,
        raw: RawEnergyMeasurement,
        country_code: str = "US",  # <-- changed default to US for consistency
        query_count: int = 1,
    ) -> SustainabilityResult:
        """
        Calculate sustainability from raw measurement.

        Args:
            raw: RawEnergyMeasurement from Module 1
            country_code: ISO country code for grid factors [Req 2.1-2.3]
            query_count: Number of queries for per-query normalization [Req 2.7-2.10]

        Returns:
            SustainabilityResult object
        """
        # Convert energy to kWh [Req 2.4]
        energy_kwh = EnergyConverter.joules_to_kwh(raw.package_energy_j)

        # Get grid factors [Req 2.1-2.3]
        factors = self.grid_manager.get_factors(country_code)

        # ====================================================================
        # Req 2.4: Total Operational Carbon (CCI)
        # Req 2.9: Carbon Receipt Per Task
        # ====================================================================
        carbon_kg = energy_kwh * factors.carbon_kg_per_kwh
        carbon_per_query_mg = UnitConverter.kg_to_mg(carbon_kg / query_count)

        carbon = CarbonMetrics(
            kg=carbon_kg,
            per_query_mg=carbon_per_query_mg,
            source=factors.sources.get("carbon", {}).get("name", "Ember 2026"),
            source_url=factors.sources.get("carbon", {}).get("url"),
            year=factors.sources.get("carbon", {}).get("year", 2026),
        )

        # ====================================================================
        # Req 2.5: Total Water Consumption (WCI)
        # Req 2.10: Water Receipt Per Task
        # ====================================================================
        water_liters = energy_kwh * factors.water_l_per_kwh
        water_per_query_ml = UnitConverter.liters_to_ml(water_liters / query_count)

        water = WaterMetrics(
            liters=water_liters,
            per_query_ml=water_per_query_ml,
            source=factors.sources.get("water", {}).get("name", "UN-Water 2025"),
            source_url=factors.sources.get("water", {}).get("url"),
            year=factors.sources.get("water", {}).get("year", 2025),
        )

        # ====================================================================
        # Req 2.6: Methane Emission Impact (MCI)
        # ====================================================================
        methane_kg = energy_kwh * (factors.methane_g_per_kwh / 1000)  # Convert g to kg

        # Calculate CO₂e equivalents using GWP [Req 2.6]
        co2e_20yr = self.gwp_calc.methane_to_co2e(methane_kg, year=20)
        co2e_100yr = self.gwp_calc.methane_to_co2e(methane_kg, year=100)

        methane = MethaneMetrics(
            kg=methane_kg,
            co2e_20yr=co2e_20yr,
            co2e_100yr=co2e_100yr,
            source=factors.sources.get("methane", {}).get("name", "IEA 2026"),
            source_url=factors.sources.get("methane", {}).get("url"),
            year=factors.sources.get("methane", {}).get("year", 2026),
        )

        return SustainabilityResult(
            measurement_id=raw.measurement_id,
            country_code=country_code,
            carbon=carbon,
            water=water,
            methane=methane,
            energy_kwh=energy_kwh,
            timestamp=time.time(),
        )

    def calculate_from_derived(
        self,
        derived: DerivedEnergyMeasurement,
        country_code: str = "US",  # <-- consistent default
        query_count: int = 1,
    ) -> SustainabilityResult:
        """
        Calculate sustainability from derived measurement.

        This uses workload energy instead of total package energy,
        giving a more accurate picture of the environmental impact
        of the actual workload.

        Args:
            derived: DerivedEnergyMeasurement from Module 1 analyzer
            country_code: ISO country code for grid factors [Req 2.1-2.3]
            query_count: Number of queries for per-query normalization [Req 2.7-2.10]

        Returns:
            SustainabilityResult object
        """
        # Use workload energy (package - idle) instead of total [Req 2.8]
        energy_j = derived.workload_energy_j
        energy_kwh = energy_j / EnergyConverter.JOULES_PER_KWH

        # Get grid factors [Req 2.1-2.3]
        factors = self.grid_manager.get_factors(country_code)
        carbon_intensity_kg_per_kwh = factors.carbon_kg_per_kwh
        water_intensity_l_per_kwh = factors.water_l_per_kwh
        methane_intensity_g_per_kwh = factors.methane_g_per_kwh
        carbon_intensity_g_per_kwh = carbon_intensity_kg_per_kwh * 1000

        # ====================================================================
        # Req 2.4: Total Operational Carbon (CCI)
        # Req 2.9: Carbon Receipt Per Task
        # ====================================================================
        carbon_kg = energy_kwh * factors.carbon_kg_per_kwh
        carbon_per_query_mg = UnitConverter.kg_to_mg(carbon_kg / query_count)

        carbon = CarbonMetrics(
            kg=carbon_kg,
            per_query_mg=carbon_per_query_mg,
            source=factors.sources.get("carbon", {}).get("name", "Ember 2026"),
            source_url=factors.sources.get("carbon", {}).get("url"),
            year=factors.sources.get("carbon", {}).get("year", 2026),
            calculation_method="workload_energy_kwh × grid_carbon_intensity",
        )

        # ====================================================================
        # Req 2.5: Total Water Consumption (WCI)
        # Req 2.10: Water Receipt Per Task
        # ====================================================================
        water_liters = energy_kwh * factors.water_l_per_kwh
        water_per_query_ml = UnitConverter.liters_to_ml(water_liters / query_count)

        water = WaterMetrics(
            liters=water_liters,
            per_query_ml=water_per_query_ml,
            source=factors.sources.get("water", {}).get("name", "UN-Water 2025"),
            source_url=factors.sources.get("water", {}).get("url"),
            year=factors.sources.get("water", {}).get("year", 2025),
            calculation_method="workload_energy_kwh × grid_water_intensity",
        )

        # ====================================================================
        # Req 2.6: Methane Emission Impact (MCI)
        # ====================================================================
        methane_kg = energy_kwh * (factors.methane_g_per_kwh / 1000)

        co2e_20yr = self.gwp_calc.methane_to_co2e(methane_kg, year=20)
        co2e_100yr = self.gwp_calc.methane_to_co2e(methane_kg, year=100)

        methane = MethaneMetrics(
            kg=methane_kg,
            co2e_20yr=co2e_20yr,
            co2e_100yr=co2e_100yr,
            source=factors.sources.get("methane", {}).get("name", "IEA 2026"),
            source_url=factors.sources.get("methane", {}).get("url"),
            year=factors.sources.get("methane", {}).get("year", 2026),
            calculation_method="workload_energy_kwh × grid_methane_leakage",
        )

        return SustainabilityResult(
            measurement_id=derived.measurement_id,
            country_code=country_code,
            carbon=carbon,
            water=water,
            methane=methane,
            energy_j=energy_j,
            energy_kwh=energy_kwh,
            carbon_intensity_g_per_kwh=carbon_intensity_g_per_kwh,
            water_intensity_l_per_kwh=water_intensity_l_per_kwh,
            methane_intensity_g_per_kwh=methane_intensity_g_per_kwh,
            timestamp=time.time(),
        )

    # ========================================================================
    # Req 2.12: Geographic Arbitrage Potential
    # ========================================================================
    def calculate_arbitrage(
        self, energy_kwh: float, current_country: str = "US"
    ) -> Dict[str, Any]:
        """
        Calculate potential savings by running in different grid regions.

        Args:
            energy_kwh: Energy consumption in kWh
            current_country: Current grid region

        Returns:
            Dictionary with best country and potential savings
        """
        countries = self.grid_manager.list_countries()
        current_factors = self.grid_manager.get_factors(current_country)
        current_carbon = energy_kwh * current_factors.carbon_kg_per_kwh

        best_country = None
        best_carbon = float("inf")

        for country in countries[:10]:  # Limit to first 10 for performance
            factors = self.grid_manager.get_factors(country)
            carbon = energy_kwh * factors.carbon_kg_per_kwh
            if carbon < best_carbon:
                best_carbon = carbon
                best_country = country

        if best_carbon < float("inf") and current_carbon > 0:
            savings = ((current_carbon - best_carbon) / current_carbon) * 100
        else:
            savings = 0.0

        return {
            "current_country": current_country,
            "current_carbon_kg": current_carbon,
            "best_country": best_country,
            "best_carbon_kg": best_carbon,
            "potential_savings_percent": savings,
            "requirement": "2.12",
        }

    # ========================================================================
    # Req 2.13: Energy Scarcity Index (ESI)
    # ========================================================================
    def calculate_scarcity_index(
        self, energy_kwh: float, country_code: str = "US"
    ) -> float:
        """
        Calculate Energy Scarcity Index (ESI).

        ESI = energy_kwh / household_daily_kwh

        Args:
            energy_kwh: Energy consumption in kWh
            country_code: ISO country code

        Returns:
            ESI value (ratio of experiment energy to household daily)
        """
        # Get household energy from Module 0 config
        try:
            country_metrics = self.config.get_country_metrics(country_code)
            if country_metrics:
                household_kwh = getattr(country_metrics, "household_daily_kwh", 30.0)
            else:
                household_kwh = 30.0  # Global average fallback
        except:
            household_kwh = 30.0

        return energy_kwh / household_kwh

    def list_countries(self) -> list:
        """Get list of available countries."""
        return self.grid_manager.list_countries()


# ============================================================================
# EXAMPLE USAGE with command‑line configurability
# ============================================================================
if __name__ == "__main__":
    import argparse

    import requests  # for IP geolocation (install with `pip install requests`)

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # ------------------------------------------------------------------------
    # Parse command line arguments
    # ------------------------------------------------------------------------
    parser = argparse.ArgumentParser(
        description="Test the sustainability calculator with a dummy workload."
    )
    parser.add_argument(
        "--country",
        type=str,
        default=None,
        help="Two‑letter ISO country code (e.g., US, IN, FR). "
        "If not given, attempts IP detection and falls back to US.",
    )
    parser.add_argument(
        "--ip-detect",
        action="store_true",
        help="Force IP‑based country detection (overrides --country if both given).",
    )
    args = parser.parse_args()

    # ------------------------------------------------------------------------
    # Determine country code
    # ------------------------------------------------------------------------
    country_code = "US"  # ultimate fallback

    if args.ip_detect or (args.country is None and args.ip_detect):
        # Try IP geolocation
        print("🌍 Attempting to detect country from public IP...")
        try:
            resp = requests.get("https://ipapi.co/json/", timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                detected = data.get("country_code")
                if detected and len(detected) == 2:
                    country_code = detected.upper()
                    print(f"   Detected: {country_code}")
                else:
                    print("   Invalid response, falling back to US.")
            else:
                print(
                    f"   Geolocation API returned {resp.status_code}, falling back to US."
                )
        except Exception as e:
            print(f"   Geolocation failed: {e}, falling back to US.")
    elif args.country:
        country_code = args.country.upper()
        print(f"🌍 Using provided country: {country_code}")
    else:
        print(f"🌍 No country specified, using default: {country_code}")

    print("\n" + "=" * 70)
    print("SUSTAINABILITY CALCULATOR TEST")
    print("=" * 70)

    # ------------------------------------------------------------------------
    # Step 1: Load configuration
    # ------------------------------------------------------------------------
    print("\n📁 Loading configuration... [Req 2.16]")
    from core.config_loader import ConfigLoader

    config_loader = ConfigLoader()

    # ------------------------------------------------------------------------
    # Step 2: Get a real measurement
    # ------------------------------------------------------------------------
    print("\n⚙️ Getting real measurement from Energy Engine...")

    # Configure engine
    config = config_loader.get_hardware_config()
    settings = config_loader.get_settings()
    if hasattr(settings, "__dict__"):
        config["settings"] = settings.__dict__
    else:
        config["settings"] = settings

    from core.analysis.energy_analyzer import EnergyAnalyzer
    from core.energy_engine import EnergyEngine
    from core.utils.baseline_manager import BaselineManager

    engine = EnergyEngine(config)
    baseline_mgr = BaselineManager()

    # Get baseline
    baseline = baseline_mgr.get_latest()
    if not baseline:
        print("📝 Measuring new baseline...")
        power = engine.measure_idle_baseline(duration_seconds=5, num_samples=3)
        from core.models.baseline_measurement import BaselineMeasurement

        baseline = BaselineMeasurement(
            baseline_id=f"baseline_{int(time.time())}",
            timestamp=time.time(),
            power_watts=power,
            duration_seconds=15,
            sample_count=3,
        )
        baseline_mgr.save(baseline)

    # Run measurement
    def dummy_workload():
        import time

        time.sleep(1)
        return "done"

    with engine as m:
        result = dummy_workload()

    raw = engine.measurement
    derived = EnergyAnalyzer.compute(raw, baseline)

    print(f"   Raw package: {raw.package_energy_j:.4f} J")
    print(f"   Workload: {derived.workload_energy_j:.4f} J")

    # ------------------------------------------------------------------------
    # Step 3: Calculate sustainability
    # ------------------------------------------------------------------------
    print("\n🌍 Calculating sustainability impacts...")
    calculator = SustainabilityCalculator(config_loader)

    # Use the determined country code for all calculations
    result_raw = calculator.calculate_from_raw(
        raw, country_code=country_code, query_count=1
    )
    print("\n📊 From Raw (Total Energy):")
    print(result_raw.summary())

    result_derived = calculator.calculate_from_derived(
        derived, country_code=country_code, query_count=1
    )
    print("\n📊 From Derived (Workload Only):")
    print(result_derived.summary())

    # ------------------------------------------------------------------------
    # Step 4: Additional metrics (using the same country)
    # ------------------------------------------------------------------------
    print("\n📊 Geographic Arbitrage Potential [Req 2.12]:")
    arbitrage = calculator.calculate_arbitrage(
        derived.workload_energy_j / 3.6e6, country_code
    )
    print(f"   Current ({country_code}): {arbitrage['current_carbon_kg']:.6f} kg CO₂e")
    print(
        f"   Best ({arbitrage['best_country']}): {arbitrage['best_carbon_kg']:.6f} kg CO₂e"
    )
    print(f"   Potential savings: {arbitrage['potential_savings_percent']:.6f}%")

    print("\n📊 Energy Scarcity Index [Req 2.13]:")
    esi = calculator.calculate_scarcity_index(
        derived.workload_energy_j / 3.6e6, country_code
    )
    print(f"   ESI: {esi:.6f} (ratio of experiment to household daily energy)")

    # ------------------------------------------------------------------------
    # Step 5: Save results
    # ------------------------------------------------------------------------
    output_file = f"data/sustainability_{int(time.time())}.json"
    with open(output_file, "w") as f:
        import json

        json.dump(
            {
                "raw": result_raw.to_dict(),
                "derived": result_derived.to_dict(),
                "arbitrage": arbitrage,
                "scarcity_index": esi,
            },
            f,
            indent=2,
        )
    print(f"\n💾 Results saved to: {output_file}")

    print("\n" + "=" * 70)
    print("✅ Sustainability Calculator Test Complete!")
    print("=" * 70)
