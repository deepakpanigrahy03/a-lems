#!/usr/bin/env python3
"""
Debug: Check what get_hardware_config() returns
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.config_loader import ConfigLoader

print("\n" + "=" * 70)
print("🔍 DEBUG: ConfigLoader Hardware Config")
print("=" * 70)

config = ConfigLoader()

# Get hardware config
hw_config = config.get_hardware_config()

print(f"\n📁 Type of hw_config: {type(hw_config)}")
print(f"📁 Dir of hw_config: {[m for m in dir(hw_config) if not m.startswith('_')]}")

# Try to access rapl
if hasattr(hw_config, "rapl"):
    print(f"\n✅ hw_config has 'rapl' attribute")
    print(f"   rapl type: {type(hw_config.rapl)}")
    if hasattr(hw_config.rapl, "paths"):
        print(f"   rapl.paths: {hw_config.rapl.paths}")
    else:
        print(
            f"   rapl attributes: {[m for m in dir(hw_config.rapl) if not m.startswith('_')]}"
        )
else:
    print(f"\n❌ hw_config does NOT have 'rapl' attribute")

# Try to convert to dict
if hasattr(hw_config, "__dict__"):
    hw_dict = hw_config.__dict__
    print(f"\n📁 __dict__ keys: {list(hw_dict.keys())}")
    if "rapl" in hw_dict:
        print(f"✅ 'rapl' found in __dict__")
    else:
        print(f"❌ 'rapl' NOT in __dict__")
