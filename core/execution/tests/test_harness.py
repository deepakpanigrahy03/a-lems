#!/usr/bin/env python3
"""
================================================================================
TEST HARNESS – Quick verification that harness works
================================================================================
"""

import os
import sys
import time
from pathlib import Path
from core.utils.preflight import preflight

import requests  # Add this for IP geolocation
from dotenv import load_dotenv

load_dotenv()  # Looks for .env in the current directory or parent directories
from core.execution.experiment_runner import ExperimentRunner

# ============================================================================
# Add project root to Python path
# ============================================================================
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import argparse
import socket
from datetime import datetime

import psutil

from core.config_loader import ConfigLoader
from core.database.manager import DatabaseManager
from core.execution.agentic import AgenticExecutor
from core.execution.harness import ExperimentHarness
from core.execution.linear import LinearExecutor
from core.execution.optimizer_wrapper import OptimizedExecutorWrapper
from core.utils.debug import set_debug
from core.utils.task_loader import get_task_by_id, load_tasks


def get_country_from_ip():
    """
    Attempt to derive the country code from the current public IP address.
    Uses ipapi.co (free, no API key required for basic lookups).
    Returns a two‑letter country code, or None if it fails.
    """
    try:
        response = requests.get("https://ipapi.co/json/", timeout=5)
        if response.status_code == 200:
            data = response.json()
            country_code = data.get("country_code")
            if country_code and len(country_code) == 2:
                return country_code.upper()
        print(f"⚠️ IP geolocation returned status {response.status_code}")
    except Exception as e:
        print(f"⚠️ IP geolocation failed: {e}")
    return None


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Test experiment harness")
    parser.add_argument(
        "--repetitions",
        "-n",
        type=int,
        default=3,
        help="Number of repetitions (default: 3)",
    )
    parser.add_argument(
        "--cool-down", type=int, default=None, help="Seconds between runs"
    )
    parser.add_argument(
        "--task",
        "-t",
        type=str,
        default=None,
        help="Custom task prompt (overrides --task-id if provided)",
    )
    parser.add_argument(
        "--task-id",
        type=str,
        default="simple",
        help="Task ID from config/tasks.yaml (default: simple, ignored if --task is used)",
    )
    parser.add_argument(
        "--list-tasks", action="store_true", help="List all available tasks and exit"
    )
    parser.add_argument("--no-warmup", action="store_true", help="Skip warmup runs")
    parser.add_argument(
        "--provider",
        choices=["cloud", "local"],
        default="cloud",
        help="Model provider (default: cloud)",
    )
    parser.add_argument(
        "--country",
        type=str,
        default=None,
        help="Country code for sustainability (e.g., US, IN, FR). "
        "If not provided, derives from IP and falls back to US.",
    )
    parser.add_argument(
        "--ip-detect",
        action="store_true",
        help="Force IP‑based country detection (overrides --country if both given)",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug")
    parser.add_argument(
        "--save-db", action="store_true", help="Save results to database"
    )
    parser.add_argument(
        "--optimizer", action="store_true", help="Use optimizer wrapper"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Show detailed hardware output per pair"
    )
    return parser.parse_args()


def main():
    """Run harness test."""
    args = parse_arguments()

    if args.debug:
        set_debug(True)

    print("\n" + "=" * 70)
    print("🔬 TESTING EXPERIMENT HARNESS")
    print("=" * 70)

    # ========================================================================
    # Handle task listing if requested
    # ========================================================================
    if args.list_tasks:
        tasks = load_tasks()
        print("\n📋 Available tasks:")
        print("-" * 50)
        for t in tasks:
            print(
                f"  {t['id']:<15} | Level {t['level']} | {t['tool_calls']} tools | {t['name']}"
            )
        print("-" * 50)
        return 0

    # ========================================================================
    # Determine country code for sustainability
    # ========================================================================
    country_code = "US"  # Ultimate fallback

    if args.ip_detect or (args.country is None and args.ip_detect):
        print("\n🌍 Attempting to detect country from public IP...")
        detected = get_country_from_ip()
        if detected:
            country_code = detected
            print(f"   ✅ Detected country: {country_code}")
        else:
            print(f"   ⚠️ Detection failed, using default: {country_code}")
    elif args.country:
        country_code = args.country.upper()
        print(f"\n🌍 Using provided country: {country_code}")
    else:
        print(f"\n🌍 No country specified, using default: {country_code}")

    # ========================================================================
    # Load configuration
    # ========================================================================
    print("\n📁 Loading configuration...")
    config = ConfigLoader()

    # Get settings and hardware config
    settings = config.get_settings()
    hw_config = config.get_hardware_config()

    # Convert settings to dict if needed
    if hasattr(settings, "__dict__"):
        settings_dict = settings.__dict__
    else:
        settings_dict = settings

    # Merge into single config dict for EnergyEngine
    engine_config = hw_config.copy()
    engine_config["settings"] = settings_dict

    # Get experiment defaults
    if hasattr(settings, "experiment"):
        default_repetitions = getattr(settings.experiment, "default_iterations", 10)
        default_cool_down = getattr(settings.experiment, "cool_down_seconds", 30)
    elif isinstance(settings, dict):
        exp_settings = settings.get("experiment", {})
        default_repetitions = exp_settings.get("default_iterations", 10)
        default_cool_down = exp_settings.get("cool_down_seconds", 30)
    else:
        default_repetitions = 10
        default_cool_down = 30

    repetitions = (
        args.repetitions if args.repetitions is not None else default_repetitions
    )
    cool_down = args.cool_down if args.cool_down is not None else default_cool_down

    # ========================================================================
    # Determine task prompt (custom or from config)
    # ========================================================================
    if args.task:
        # User provided custom task string
        task_prompt = args.task
        task_id = "custom"
        task_name = "Custom Task"
        print(f"\n📝 Using custom task: {task_prompt[:50]}...")
    else:
        # Load from config
        task = get_task_by_id(args.task_id)
        if not task:
            print(
                f"\n❌ Task ID '{args.task_id}' not found. Use --list-tasks to see available tasks."
            )
            return 1
        task_prompt = task["prompt"]
        task_id = task["id"]
        task_name = task["name"]
        print(f"\n📋 Using predefined task: {task_name} (level {task['level']})")
        print(f"   Prompt: {task_prompt[:50]}...")

    print(f"\n📋 Configuration:")
    print(f"   Provider:     {args.provider}")
    print(f"   Country:      {country_code}")
    print(f"   Task:         {task_name}")
    print(f"   Repetitions:  {repetitions}")
    print(f"   Cool-down:    {cool_down}s")
    print(f"   Warmup:       {'Yes' if not args.no_warmup else 'No'}")

    # ========================================================================
    # Get model configs
    # ========================================================================
    print(f"\n🤖 Getting {args.provider} model configs...")

    linear_config = config.get_model_config(args.provider, "linear")
    agentic_config = config.get_model_config(args.provider, "agentic")

   
    dummy = type('obj', (object,), {'config': linear_config})()
    preflight(dummy, args.provider)

    if not linear_config or not agentic_config:
        print(f"❌ Failed to load {args.provider} model configurations")
        return 1

    print(f"   Linear model:  {linear_config.get('name')}")
    print(f"   Agentic model: {agentic_config.get('name')}")

    # ========================================================================
    # Create executors and harness
    # ========================================================================
    print("\n⚙️ Creating executors...")

    if args.optimizer:
        from core.execution.optimizer_wrapper import OptimizedExecutorWrapper

        agentic = OptimizedExecutorWrapper(agentic_config, "agentic")
        linear = OptimizedExecutorWrapper(linear_config, "linear")
    else:
        agentic = AgenticExecutor(agentic_config)
        linear = LinearExecutor(linear_config)

    print(f"   Optimizer:    {'Yes' if args.optimizer else 'No'}")

    # ========================================================================
    # Create proper config with BOTH hardware paths and settings
    # ========================================================================
    print("🔧 Creating harness...")

    # engine_config already has hardware + settings from earlier
    print(f"🔍 engine_config keys: {list(engine_config.keys())}")
    print(f"🔍 'rapl' in engine_config: {'rapl' in engine_config}")
    if "rapl" in engine_config:
        print(f"🔍 rapl paths: {engine_config['rapl'].get('paths', {})}")

    harness = ExperimentHarness(config)

    # ========================================================================
    # Create runner and ensure baseline
    # ========================================================================
    runner = ExperimentRunner(config, args)
    #runner.validate_experiment(linear_executor, args.provider)
    baseline = runner.ensure_baseline(harness)
    harness.baseline = baseline

    # ========================================================================
    # Run test
    # ========================================================================
    print(f"\n🚀 Running test with {repetitions} repetitions...")

    # ========================================================================
    # Setup database if saving
    # ========================================================================
    if args.save_db:
        # Create database connection and experiment
        db, hw_id, env_id = runner.setup_database()

        #runner.ensure_baseline_in_db(db, harness)
        config.sync_task_categories(db.db.conn)
        exp_id = runner.create_experiment(
            db,
            task_id,
            task_name,
            args.provider,
            linear_config,
            country_code,
            args.repetitions,
            hw_id,
            env_id,
            optimizer=args.optimizer,
        )

    # ========================================================================
    # OUR OWN LOOP - ONE LOOP DOES EVERYTHING
    # ========================================================================
    all_linear = []
    all_agentic = []
    all_taxes = []
    runs_completed = 0  # Add this line

    try:
        for rep in range(repetitions):
            print(f"\n{'─'*50}")
            print(f"📋 Repetition {rep+1}/{repetitions}")
            print(f"{'─'*50}")

            # Run linear
            linear_result = harness.run_linear(
                executor=linear,
                prompt=task_prompt,
                task_id=task_id,
                is_cloud=(args.provider == "cloud"),
                country_code=country_code,
                run_number=rep + 1,
            )

            # Run agentic
            agentic_result = harness.run_agentic(
                executor=agentic,
                task=task_prompt,
                task_id=task_id,
                is_cloud=(args.provider == "cloud"),
                country_code=country_code,
                run_number=rep + 1,
            )

            # Store for stats
            all_linear.append(linear_result)
            all_agentic.append(agentic_result)

            # Calculate tax
            linear_energy = linear_result["ml_features"]["energy_j"]
            agentic_energy = agentic_result["ml_features"]["energy_j"]
            tax = agentic_energy / linear_energy if linear_energy > 0 else 0
            all_taxes.append(tax)

            print(f"\n   📊 Pair {rep+1}:")
            print(f"      Linear:  {linear_energy:.4f} J")
            print(f"      Agentic: {agentic_energy:.4f} J")
            print(f"      Tax: {tax:.2f}x")

            # Insert to database
            if args.save_db:
                runner.save_pair(
                    db, exp_id, hw_id, linear_result, agentic_result, rep + 1
                )
                runs_completed += 2  # Update after successful pair

            # Cool down
            if rep < repetitions - 1:
                print(f"\n⏳ Cooling down for {cool_down}s...")
                time.sleep(cool_down)

    except (Exception, KeyboardInterrupt) as e:
        print(f"\n❌ Experiment failed: {e}")
        import traceback

        traceback.print_exc()

        if args.save_db:
            # Handle both regular exceptions and Ctrl+C
            error_msg = str(e) if str(e) else "KeyboardInterrupt (user cancelled)"
            runner.update_status(db, exp_id, "failed", runs_completed, error_msg)
            db.close()
        return 1

    # ========================================================================
    # UPDATE EXPERIMENT STATUS - COMPLETE SOLUTION
    # ========================================================================
    if args.save_db:
        # Calculate total runs completed
        runs_completed = len(all_linear) + len(all_agentic)  # Should be repetitions * 2

        # Update final status to completed
        runner.update_status(db, exp_id, "completed", runs_completed)
        print(f"\n✅ Experiment {exp_id} completed with {runs_completed} runs")

        # Close database connection
        db.close()

    # ========================================================================
    # DISPLAY HARDWARE PARAMETERS (verbose mode only)
    # ========================================================================
    if args.verbose:
        from core.execution.display_formatter import display_pair_hardware

        display_pair_hardware(
            all_linear, all_agentic, "HARDWARE PARAMETERS DEEP DIVE - PER PAIR"
        )
    # ========================================================================
    # CALCULATE FINAL STATISTICS
    # ========================================================================
    import numpy as np

    from core.execution.base import calc_stats

    linear_energies = [r["ml_features"]["energy_j"] for r in all_linear]
    agentic_energies = [r["ml_features"]["energy_j"] for r in all_agentic]

    stats = {
        "linear_energy_j": calc_stats(linear_energies),
        "agentic_energy_j": calc_stats(agentic_energies),
        "orchestration_tax": calc_stats(all_taxes),
    }

    print("\n" + "=" * 70)
    print("📊 FINAL STATISTICS")
    print("=" * 70)

    linear_mean = stats["linear_energy_j"]["mean"]
    linear_std = stats["linear_energy_j"]["std"]
    if not np.isnan(linear_std):
        print(f"   Linear energy:     {linear_mean:.4f} ± {linear_std:.4f} J")
    else:
        print(f"   Linear energy:     {linear_mean:.4f} J")

    agentic_mean = stats["agentic_energy_j"]["mean"]
    agentic_std = stats["agentic_energy_j"]["std"]
    if not np.isnan(agentic_std):
        print(f"   Agentic energy:    {agentic_mean:.4f} ± {agentic_std:.4f} J")
    else:
        print(f"   Agentic energy:    {agentic_mean:.4f} J")

    tax_mean = stats["orchestration_tax"]["mean"]
    ci_lower = stats["orchestration_tax"]["ci_lower"]
    ci_upper = stats["orchestration_tax"]["ci_upper"]

    if not np.isnan(ci_lower):
        print(
            f"   Orchestration tax: {tax_mean:.2f}x [95% CI: {ci_lower:.2f}, {ci_upper:.2f}]"
        )
    else:
        print(f"   Orchestration tax: {tax_mean:.2f}x")

    print("\n✅ Test complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
