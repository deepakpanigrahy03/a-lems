#!/usr/bin/env python3
"""
Test Energy Engine directly to see if RAPL works
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import time

from core.config_loader import ConfigLoader
from core.energy_engine import EnergyEngine

print("\n" + "=" * 70)
print("🔍 TESTING ENERGY ENGINE DIRECTLY")
print("=" * 70)

# Load config
config = ConfigLoader()
print("\n📁 Loading config...")

# Get hardware config (should have RAPL paths)
hw_config = config.get_hardware_config()
print(f"📁 hw_config type: {type(hw_config)}")
if isinstance(hw_config, dict):
    print(f"📁 hw_config keys: {list(hw_config.keys())}")
    if "rapl" in hw_config:
        print(f"📁 rapl paths: {hw_config['rapl'].get('paths', {})}")
else:
    print(f"📁 hw_config dir: {dir(hw_config)[:10]}")

# Get settings
settings = config.get_settings()
if hasattr(settings, "__dict__"):
    settings_dict = settings.__dict__
else:
    settings_dict = settings

# Create full config for EnergyEngine
if isinstance(hw_config, dict):
    full_config = hw_config.copy()
else:
    # Try to convert to dict
    full_config = {}
    if hasattr(hw_config, "__dict__"):
        full_config = hw_config.__dict__.copy()

full_config["settings"] = settings_dict

print(f"\n🔧 Creating EnergyEngine with config keys: {list(full_config.keys())}")
print(f"🔧 'rapl' in config: {'rapl' in full_config}")

# Initialize EnergyEngine
try:
    engine = EnergyEngine(full_config)
    print("✅ EnergyEngine initialized")
except Exception as e:
    print(f"❌ Failed to initialize EnergyEngine: {e}")
    sys.exit(1)

# Try a simple measurement
print("\n📝 Taking a 1-second measurement...")
engine.start_measurement()
time.sleep(1)
raw = engine.stop_measurement()

print(f"\n📊 Raw measurement result:")
print(f"   Measurement ID: {raw.measurement_id}")
print(f"   Duration: {raw.duration_seconds:.2f}s")
print(f"   Package energy: {raw.package_energy_uj / 1e6:.6f} J")
print(f"   Core energy: {raw.core_energy_uj / 1e6:.6f} J")
if raw.dram_energy_uj:
    print(f"   DRAM energy: {raw.dram_energy_uj / 1e6:.6f} J")

print("\n✅ Test complete!")
