#!/usr/bin/env python3
"""
Test to verify C3/C6 counters by measuring idle period.
"""

import sys
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.analysis.energy_analyzer import EnergyAnalyzer
from core.config_loader import ConfigLoader
from core.energy_engine import EnergyEngine


def main():
    print("\n" + "=" * 70)
    print("🔬 C-STATE IDLE TEST")
    print("=" * 70)

    # Load config
    config_loader = ConfigLoader()
    config = config_loader.get_hardware_config()
    settings = config_loader.get_settings()

    # Merge settings into config
    if hasattr(settings, "__dict__"):
        config["settings"] = settings.__dict__
    else:
        config["settings"] = settings

    # Create engine
    engine = EnergyEngine(config)

    print("\n📊 Measuring 10 seconds of IDLE time...")
    print("   (Don't touch mouse/keyboard during this test)")

    # Measure idle period
    with engine as m:
        print("   ⏳ Sleeping for 10 seconds...")
        time.sleep(10)

    # Get raw measurement
    raw = engine.measurement

    # Extract C-state values from msr_metrics
    if hasattr(raw, "msr_metrics") and raw.msr_metrics:
        print("\n📊 C-STATE RESULTS (10 second idle):")
        print("-" * 50)

        # Look for C-state times in msr_metrics
        for state in ["c2", "c3", "c6", "c7"]:
            time_key = f"{state}_time_seconds"
            if time_key in raw.msr_metrics:
                value = raw.msr_metrics[time_key]
                print(f"   {state.upper()}: {value:.3f} seconds")
            else:
                print(f"   {state.upper()}: Not available")

        # Also check dynamic section
        if "dynamic" in raw.msr_metrics:
            dyn = raw.msr_metrics["dynamic"]
            if "cstate_deltas" in dyn:
                print("\n   From cstate_deltas:")
                for state, value in dyn["cstate_deltas"].items():
                    print(f"      {state}: {value:.3f}s")

        print("-" * 50)
        print("\n✅ If you see C3/C6 > 0, your counters work!")
        print("   If they're still 0, your CPU just doesn't enter")
        print("   deep C-states even during idle (common on some laptops)")
    else:
        print("❌ No MSR metrics available")


if __name__ == "__main__":
    main()
