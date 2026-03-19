#!/usr/bin/env python3
"""Load hardware.json and environment.json to database"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config_loader import ConfigLoader
from core.database.manager import DatabaseManager


def load_hardware(db, path):
    """Load hardware config, return hw_id"""
    data = json.loads(path.read_text())

    # Flatten for DB insertion
    hw_data = {
        "hardware_hash": data.get("hardware_hash"),
        "cpu_model": data.get("cpu_model"),
        "cpu_cores": data.get("cpu_cores"),
        "cpu_threads": data.get("cpu_threads"),
        "cpu_architecture": data.get("cpu_architecture"),
        "cpu_vendor": data.get("cpu_vendor"),
        "cpu_family": data.get("cpu_family"),
        "cpu_model_id": data.get("cpu_model_id"),
        "cpu_stepping": data.get("cpu_stepping"),
        "has_avx2": data.get("has_avx2"),
        "has_avx512": data.get("has_avx512"),
        "has_vmx": data.get("has_vmx"),
        "gpu_model": data.get("gpu_model"),
        "gpu_driver": data.get("gpu_driver"),
        "gpu_count": data.get("gpu_count"),
        "ram_gb": data.get("ram_gb"),
        "detected_at": data.get("detected_at"),
    }

    return db.insert_hardware_config(hw_data)


def main():
    config = ConfigLoader()
    db = DatabaseManager(config.get_db_config())

    hw_id = None
    if Path("config/hardware.json").exists():
        hw_id = load_hardware(db, Path("config/hardware.json"))
        print(f"✅ Hardware ID: {hw_id}")

    # Save session
    if hw_id:
        Path("config/current_session.json").write_text(
            json.dumps({"hw_id": hw_id, "session_start": str(datetime.now())})
        )

    db.close()


if __name__ == "__main__":
    from datetime import datetime

    main()
