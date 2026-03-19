#!/usr/bin/env python3
"""
Debug RAPL readings
"""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from core.config_loader import ConfigLoader
from core.readers.rapl_reader import RAPLReader

print("\n🔍 RAPL DEBUG")
print("=" * 50)

# Load config
config = ConfigLoader()
hw_config = config.get_hardware_config()

# Convert to dict if needed
if hasattr(hw_config, "__dict__"):
    config_dict = hw_config.__dict__
else:
    config_dict = hw_config

print(f"\n📁 Hardware config keys: {list(config_dict.keys())}")
print(f"📁 RAPL config: {config_dict.get('rapl', {})}")

# Initialize RAPL reader
reader = RAPLReader(config_dict)

print(f"\n🔧 Available domains: {reader.available_domains}")

# Try reading multiple times
print("\n📊 Reading RAPL 5 times:")
for i in range(5):
    energy = reader.read_energy()
    print(f"  Reading {i+1}: {energy}")
