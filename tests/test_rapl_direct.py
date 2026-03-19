#!/usr/bin/env python3
import json
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.readers.rapl_reader import RAPLReader

# Load config
with open("config/hw_config.json") as f:
    config = json.load(f)

rapl_config = config.get("rapl", {})
print(f"Config being passed: {rapl_config}")

# Initialize reader
rapl = RAPLReader(rapl_config)

# Check what happened
print(f"Reader available domains: {rapl.get_available_domains()}")
print(f"Reader paths: {rapl.paths if hasattr(rapl, 'paths') else 'No paths attribute'}")

# Try reading
energy = rapl.read_energy()
print(f"Energy reading: {energy}")
