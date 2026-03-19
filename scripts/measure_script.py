#!/usr/bin/env python3
"""
================================================================================
MEASURE SCRIPT – Run any Python script with energy measurement
================================================================================

This script wraps an arbitrary Python script (which must define a main() function)
and measures its energy consumption using the EnergyEngine (Module 1). It can run
the script multiple times and output results to console, a JSON file, and/or the
A‑LEMS database (Module 4).

Usage:
    ./scripts/measure_script.py path/to/script.py [options]

Options:
    --iterations N, -n N       Number of runs (default: 10)
    --cool-down SEC, -c SEC     Seconds between runs (default: from config or 30)
    --output FILE, -o FILE      Save results to JSON file
    --db                        Store results in the A‑LEMS database
    --verbose, -v               Enable debug output

The target script must contain a function named `main()` that takes no arguments
and returns a result (which is ignored). Example:

    def main():
        # your AI code here
        return "done"

Author: Deepak Panigrahy
================================================================================
"""

import argparse
import importlib.util
import json
import logging
import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.config_loader import ConfigLoader
from core.energy_engine import EnergyEngine

# ------------------------------------------------------------------------
# Logging setup
# ------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("measure_script")


def load_script(script_path: Path):
    """
    Dynamically import a Python script and return its module.

    Args:
        script_path: Path to the .py file.

    Returns:
        Module object.

    Raises:
        ImportError: if the script cannot be loaded.
    """
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    # Create a module spec from the file
    spec = importlib.util.spec_from_file_location("target_script", script_path)
    if spec is None:
        raise ImportError(f"Could not load spec from {script_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(
        description="Run a Python script with energy measurement.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "script", type=Path, help="Path to the Python script to measure"
    )
    parser.add_argument(
        "--iterations", "-n", type=int, default=10, help="Number of runs (default: 10)"
    )
    parser.add_argument(
        "--cool-down",
        "-c",
        type=int,
        default=None,
        help="Seconds between runs (default: from config or 30)",
    )
    parser.add_argument("--output", "-o", type=Path, help="Output JSON file")
    parser.add_argument("--db", action="store_true", help="Store results in database")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug output"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # ------------------------------------------------------------------------
    # Load target script
    # ------------------------------------------------------------------------
    try:
        script_module = load_script(args.script)
    except Exception as e:
        logger.error(f"Failed to load script: {e}")
        sys.exit(1)

    # Check that the script has a main() function
    if not hasattr(script_module, "main"):
        logger.error(f"Script {args.script} does not define a main() function")
        sys.exit(1)

    main_func = script_module.main

    # ------------------------------------------------------------------------
    # Load configuration from Module 0
    # ------------------------------------------------------------------------
    config_loader = ConfigLoader()
    config = config_loader.get_hardware_config()
    settings = config_loader.get_settings()

    # Merge settings into config (handles both dict and object)
    if hasattr(settings, "__dict__"):
        config["settings"] = settings.__dict__
    else:
        config["settings"] = settings

    # ------------------------------------------------------------------------
    # Create EnergyEngine and measure
    # ------------------------------------------------------------------------
    engine = EnergyEngine(config)

    # Optionally measure idle baseline? The user may want to skip if already cached.
    # We'll let the engine's measure_idle_baseline handle caching automatically.
    # For now, we just run the experiment; the engine will load baseline from cache.

    # Run multiple measurements
    stats = engine.run_multiple(
        func=main_func,
        iterations=args.iterations,
        cool_down=args.cool_down,
        output_file=str(args.output) if args.output else None,
    )

    # ------------------------------------------------------------------------
    # Print summary to console
    # ------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("MEASUREMENT SUMMARY")
    print("=" * 70)
    print(f"Script:       {args.script}")
    print(f"Iterations:   {args.iterations}")
    print(
        f"Cool-down:    {args.cool_down if args.cool_down else engine.cool_down_seconds} s"
    )
    print("\n--- Energy Statistics ---")
    for metric, values in stats["summary"].items():
        print(
            f"{metric:20} mean={values['mean']:10.2f} {values['unit']}  "
            f"stdev={values['stdev']:8.2f}"
        )

    # ------------------------------------------------------------------------
    # Database storage (if requested)
    # ------------------------------------------------------------------------
    if args.db:
        try:
            from core.database import DatabaseManager

            db = DatabaseManager()
            # For each run, save to database (simplified – you may want to store all)
            for run in stats["all_runs"]:
                db.save_experiment(run)
            print(f"\n✅ Stored {len(stats['all_runs'])} runs in database")
        except ImportError:
            logger.error("Database module not available (--db ignored)")
        except Exception as e:
            logger.error(f"Database error: {e}")

    print("\n" + "=" * 70)
    print("✅ Measurement complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
