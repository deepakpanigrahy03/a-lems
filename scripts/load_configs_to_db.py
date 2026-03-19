#!/usr/bin/env python3
"""Load hardware.json and environment.json to database"""

import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config_loader import ConfigLoader
from core.database.manager import DatabaseManager


def main():
    config = ConfigLoader()
    db = DatabaseManager(config.get_db_config())

    # Load hardware
    hw_path = Path("config/hw_config.json")
    if hw_path.exists():
        print("📦 Loading hardware config...")
        with open(hw_path) as f:
            hw_data = json.load(f)

        # Flatten ALL fields from JSON (both top-level and nested)
        flat_hw = {
            # Top-level flat fields (already present)
            "hardware_hash": hw_data.get("hardware_hash"),
            "cpu_model": hw_data.get("cpu_model"),
            "cpu_cores": hw_data.get("cpu_cores"),
            "ram_gb": hw_data.get("ram_gb"),
            "gpu_model": hw_data.get("gpu_model"),
            "system_manufacturer": hw_data.get("system_manufacturer"),
            "system_product": hw_data.get("system_product"),
            "system_type": hw_data.get("system_type"),
            "virtualization_type": hw_data.get("virtualization_type"),
            "cpu_vendor": hw_data.get("cpu_vendor"),
            "microcode_version": hw_data.get("microcode_version"),
            # Metadata
            "hostname": hw_data.get("metadata", {}).get("hostname"),
            "kernel_version": hw_data.get("metadata", {}).get("release"),
            "detected_at": hw_data.get("metadata", {}).get("detected_at"),
            "cpu_architecture": hw_data.get("metadata", {}).get("machine"),
            # CPU nested
            "cpu_threads": hw_data.get("cpu", {}).get("logical_cores"),
            # CPU details nested
            "cpu_family": hw_data.get("cpu_details", {}).get("family"),
            "cpu_model_id": hw_data.get("cpu_details", {}).get("model"),
            "cpu_stepping": hw_data.get("cpu_details", {}).get("stepping"),
            # CPU flags nested
            "has_avx2": hw_data.get("cpu_flags", {}).get("has_avx2"),
            "has_avx512": hw_data.get("cpu_flags", {}).get("has_avx512"),
            "has_vmx": hw_data.get("cpu_flags", {}).get("has_vmx"),
            # GPU nested
            "gpu_driver": hw_data.get("gpu", {}).get("driver"),
            "gpu_count": hw_data.get("gpu", {}).get("count"),
            "gpu_power_available": hw_data.get("gpu", {}).get("power_available"),
            # RAPL
            "rapl_domains": str(hw_data.get("rapl", {}).get("available_domains")),
            "rapl_has_dram": hw_data.get("rapl", {}).get("has_dram"),
            "rapl_has_uncore": "uncore"
            in hw_data.get("rapl", {}).get("available_domains", []),
        }

        hw_id = db.insert_hardware(flat_hw)
        print(f"   ✅ Hardware ID: {hw_id}")
    else:
        print("⚠️  No hw_config.json found. Run scripts/detect_hardware.py first.")
        hw_id = None

    # Load environment
    env_path = Path("config/environment.json")
    if env_path.exists():
        print("📦 Loading environment config...")
        with open(env_path) as f:
            env_data = json.load(f)

        env_id = db.insert_environment_config(env_data)
        print(f"   ✅ Environment ID: {env_id}")
    else:
        print("⚠️  No environment.json found. Run scripts/detect_environment.py first.")
        env_id = None

    # Save session info
    if hw_id and env_id:
        session = {
            "hw_id": hw_id,
            "env_id": env_id,
            "session_start": datetime.now().isoformat(),
        }
        Path("config/current_session.json").write_text(json.dumps(session, indent=2))
        print(f"\n✅ Session saved to config/current_session.json")

    db.close()


if __name__ == "__main__":
    main()
