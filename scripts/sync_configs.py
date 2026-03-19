#!/usr/bin/env python3
"""
A-LEMS Configuration Sync Utility

Manually run this script to update configuration files with the latest data
from official sources. It creates backups and logs all changes.

Usage:
    python scripts/sync_configs.py [--dry-run] [--source SOURCE]
"""

import argparse
import json
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import requests
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(Path(__file__).parent.parent / "logs" / "sync.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("sync_configs")

from dotenv import load_dotenv

load_dotenv()


def fetch_json(url):
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


def backup_file(filepath):
    if not filepath.exists():
        return
    backup_dir = filepath.parent / "backups"
    backup_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{filepath.name}.{timestamp}.bak"
    shutil.copy2(filepath, backup_path)
    logger.info(f"Backup created: {backup_path}")


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)


def save_json(filepath, data):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)


def load_yaml(filepath):
    with open(filepath) as f:
        return yaml.safe_load(f)


def save_yaml(filepath, data):
    with open(filepath, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


# Parsers (simplified; real implementation would extract fields)
def parse_ember(data):
    result = {}
    for item in data:
        code = item.get("country_code")
        if code:
            result[code] = {
                "carbon_intensity": item.get("carbon_intensity_2026"),
                "generation_mix": item.get("generation", {}),
            }
    return result


def sync_grid_intensity(dry_run=False):
    path = Path("config/grid_intensity_2026.json")
    if not path.exists():
        logger.error("grid_intensity_2026.json not found")
        return
    current = load_json(path)
    updated = False

    # Ember
    logger.info("Fetching Ember data...")
    data = fetch_json("https://api.ember-energy.org/latest/electricity-review")
    if data:
        new_data = parse_ember(data)
        for code, vals in new_data.items():
            if code in current:
                # Update carbon intensity if changed
                old = current[code].get("carbon_intensity")
                new = vals.get("carbon_intensity")
                if old != new:
                    logger.info(f"  {code}: carbon_intensity {old} -> {new}")
                    current[code]["carbon_intensity"] = new
                    updated = True
                # Optionally update generation_mix
                if "generation_mix" in vals:
                    current[code]["generation_mix"] = vals["generation_mix"]
                    updated = True
            else:
                logger.warning(f"  New country {code} found, consider adding.")
    if updated and not dry_run:
        backup_file(path)
        save_json(path, current)
        logger.info("✅ grid_intensity_2026.json updated")
    else:
        logger.info("No changes to grid_intensity_2026.json")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--source", choices=["grid", "country", "gwp", "all"], default="all"
    )
    args = parser.parse_args()
    logger.info("=" * 60)
    logger.info("A-LEMS sync started")
    if args.source in ("grid", "all"):
        sync_grid_intensity(args.dry_run)
    logger.info("Sync completed")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
