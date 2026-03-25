#!/usr/bin/env python3
"""
================================================================================
REAL EXPERIMENT – Run statistically significant experiments
================================================================================
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from core.utils.preflight import preflight

import numpy as np
from dotenv import load_dotenv

load_dotenv()

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.config_loader import ConfigLoader
from core.execution.agentic import AgenticExecutor
from core.execution.base import calc_stats
from core.execution.display_formatter import display_pair_hardware
from core.execution.experiment_runner import ExperimentRunner
from core.execution.harness import ExperimentHarness
from core.execution.linear import LinearExecutor
from core.utils.task_loader import list_task_summary, load_tasks


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run A-LEMS experiments")
    parser.add_argument("--repetitions", "-n", type=int, default=None)
    parser.add_argument("--cool-down", type=int, default=None)
    parser.add_argument("--tasks", type=str, default="simple,capital")
    parser.add_argument("--list-tasks", action="store_true")
    parser.add_argument("--no-warmup", action="store_true")
    parser.add_argument("--provider", choices=["cloud", "local"], default="cloud")
    parser.add_argument("--country", type=str, default="US")
    parser.add_argument("--save-db", action="store_true")
    parser.add_argument("--providers", type=str, help="Comma-separated providers")
    parser.add_argument(
        "--verbose", action="store_true", help="Show detailed hardware output per pair"
    )
    parser.add_argument(
        "--optimizer", action="store_true", help="Use optimizer wrapper"
    )

    return parser.parse_args()


def run_provider_task(
    harness, runner, task, provider, repetitions, cool_down, args, config
):
    """
    Run all repetitions for ONE provider-task combination.
    Returns stats dict or None if failed.

    Args:
        harness: ExperimentHarness instance
        runner: ExperimentRunner instance
        task: Task dictionary
        provider: Provider name
        repetitions: Number of repetitions
        cool_down: Cool down seconds
        args: Command line arguments
        config: ConfigLoader instance
    """
    print(f"\n   {'='*60}")
    print(f"   📋 {provider} | {task['name']}")
    print(f"   {'='*60}")

    # ========================================================================
    # Setup executors for this provider
    # ========================================================================
    linear_config = runner.config.get_model_config(provider, "linear")
    agentic_config = runner.config.get_model_config(provider, "agentic")

    if not linear_config or not agentic_config:
        print(f"   ❌ Failed to load {provider} configs, skipping...")
        return None
    # ========================================================================
    # PRE-FLIGHT CHECKS - Validate before creating executors
    # ========================================================================
    from core.utils.preflight import preflight
    dummy = type('obj', (object,), {'config': linear_config})()
    preflight(dummy, provider)

    if args.optimizer:
        from core.execution.optimizer_wrapper import OptimizedExecutorWrapper

        linear = OptimizedExecutorWrapper(linear_config, "linear")
        agentic = OptimizedExecutorWrapper(agentic_config, "agentic")
    else:
        linear = LinearExecutor(linear_config)
        agentic = AgenticExecutor(agentic_config)

    print(f"   Optimizer:    {'Yes' if args.optimizer else 'No'}")

    # ========================================================================
    # Setup database for this provider-task
    # ========================================================================
    db = None
    exp_id = None
    hw_id = None
    runs_completed = 0

    try:
        if args.save_db:
            db, hw_id, env_id = runner.setup_database()

            config.sync_task_categories(db.db.conn)
            #runner.ensure_baseline_in_db(db, harness)
            exp_id = runner.create_experiment(
                db,
                task["id"],
                task["name"],
                provider,
                linear_config,
                args.country,
                repetitions,
                hw_id,
                env_id,
                optimizer=args.optimizer,
            )

        # Storage for results
        linear_results = []
        agentic_results = []
        taxes = []
        runs_completed = 0

        # ========================================================================
        # Repetition loop
        # ========================================================================
        try:
            for rep in range(repetitions):
                print(f"\n      {'─'*50}")
                print(f"      Repetition {rep+1}/{repetitions}")
                print(f"      {'─'*50}")

                # Run linear
                linear_result = harness.run_linear(
                    executor=linear,
                    prompt=task["prompt"],
                    task_id=task["id"],
                    is_cloud=(provider in ["cloud", "openrouter"]),
                    country_code=args.country,
                    run_number=rep + 1,
                )

                # Run agentic
                agentic_result = harness.run_agentic(
                    executor=agentic,
                    task=task["prompt"],
                    task_id=task["id"],
                    is_cloud=(provider in ["cloud", "openrouter"]),
                    country_code=args.country,
                    run_number=rep + 1,
                )

                # Store
                linear_results.append(linear_result)
                agentic_results.append(agentic_result)

                # Calculate tax
                linear_energy = linear_result["ml_features"]["energy_j"]
                agentic_energy = agentic_result["ml_features"]["energy_j"]
                tax = agentic_energy / linear_energy if linear_energy > 0 else 0
                taxes.append(tax)

                print(f"\n         Linear:  {linear_energy:.4f} J")
                print(f"         Agentic: {agentic_energy:.4f} J")
                print(f"         Tax: {tax:.2f}x")

                # Insert to database
                if args.save_db and db:
                    runner.save_pair(
                        db, exp_id, hw_id, linear_result, agentic_result, rep + 1
                    )
                    print(f"         ✅ Pair {rep+1}/{repetitions} saved")

                    # Update progress in real-time
                    runs_completed = (rep + 1) * 2
                    runner.update_progress(db, exp_id, runs_completed)

                # Cool down
                if rep < repetitions - 1:
                    print(f"\n         ⏳ Cooling down {cool_down}s...")
                    time.sleep(cool_down)

        except (Exception, KeyboardInterrupt) as e:
            print(f"\n   ❌ ERROR in {provider}/{task['name']}: {e}")
            import traceback

            traceback.print_exc()

            if db and exp_id:
                error_msg = str(e) if str(e) else "KeyboardInterrupt (user cancelled)"
                runner.update_status(
                    db, exp_id, "failed", runs_completed, error=error_msg
                )
                print(
                    f"   ⚠️ Experiment {exp_id} marked as failed after {runs_completed} runs"
                )

            return None

        # ========================================================================
        # UPDATE EXPERIMENT STATUS - SUCCESS
        # ========================================================================
        if db:
            runs_completed = len(linear_results) + len(agentic_results)
            runner.update_status(db, exp_id, "completed", runs_completed)
            print(f"\n   ✅ Experiment {exp_id} completed with {runs_completed} runs")

        # ========================================================================
        # DISPLAY HARDWARE PARAMETERS (verbose mode only)
        # ========================================================================
        if args.verbose:
            from core.execution.display_formatter import display_pair_hardware

            display_pair_hardware(
                linear_results,
                agentic_results,
                f"{provider} - {task['name']} HARDWARE DETAILS",
            )

        # ========================================================================
        # Calculate statistics for this provider-task
        # ========================================================================
        linear_energies = [r["ml_features"]["energy_j"] for r in linear_results]
        agentic_energies = [r["ml_features"]["energy_j"] for r in agentic_results]

        stats = {
            "provider": provider,
            "task": task["name"],
            "linear_energy_j": calc_stats(linear_energies),
            "agentic_energy_j": calc_stats(agentic_energies),
            "orchestration_tax": calc_stats(taxes),
        }

        return stats

    except Exception as e:
        # ========================================================================
        # ERROR HANDLING - Update experiment status with error
        # ========================================================================
        print(f"\n   ❌ ERROR in {provider}/{task['name']}: {e}")
        import traceback

        traceback.print_exc()

        if db and exp_id:
            runner.update_status(db, exp_id, "failed", runs_completed, error=str(e))
            print(
                f"   ⚠️ Experiment {exp_id} marked as failed after {runs_completed} runs"
            )

        return None

    finally:
        # ========================================================================
        # CLEANUP - Always close database
        # ========================================================================
        if db:
            db.close()


def run_task(harness, runner, task, providers, repetitions, cool_down, args, config):
    """
    Run one task across all providers.
    Returns list of stats for each provider.
    """
    results = []

    for provider in providers:
        stats = run_provider_task(
            harness, runner, task, provider, repetitions, cool_down, args, config
        )
        if stats:
            results.append(stats)
            print(f"\n   ✅ {provider} | {task['name']} complete:")
            print(
                f"      Tax: {stats['orchestration_tax']['mean']:.2f}x "
                f"[{stats['orchestration_tax']['ci_lower']:.2f}, {stats['orchestration_tax']['ci_upper']:.2f}]"
            )

    return results


def run_all_experiments(args):
    """
    Run all tasks with all providers.
    Returns list of all results.
    """
    print("\n" + "=" * 70)
    print("📊 A-LEMS REAL EXPERIMENT")
    print("=" * 70)

    # Load configuration
    config = ConfigLoader()
    settings = config.get_settings()

    # Get defaults
    default_repetitions = (
        getattr(settings.experiment, "default_iterations", 30)
        if hasattr(settings, "experiment")
        else 30
    )
    default_cool_down = (
        getattr(settings.experiment, "cool_down_seconds", 30)
        if hasattr(settings, "experiment")
        else 30
    )

    repetitions = (
        args.repetitions if args.repetitions is not None else default_repetitions
    )
    cool_down = args.cool_down if args.cool_down is not None else default_cool_down

    # Parse providers
    if args.providers:
        providers = [p.strip() for p in args.providers.split(",")]
    elif args.provider:
        providers = [args.provider]
    else:
        providers = ["cloud"]

    # Load tasks
    all_tasks = load_tasks()
    if args.tasks == "all":
        selected_tasks = all_tasks
    else:
        task_ids = [tid.strip() for tid in args.tasks.split(",")]
        selected_tasks = [t for t in all_tasks if t["id"] in task_ids]

    if not selected_tasks:
        print("\n❌ No valid tasks selected.")
        return []

    print(f"\n📋 Configuration:")
    print(f"   Providers:    {', '.join(providers)}")
    print(f"   Tasks:        {len(selected_tasks)}")
    print(f"   Repetitions:  {repetitions}")
    print(f"   Cool-down:    {cool_down}s")

    # Create harness and runner
    harness = ExperimentHarness(config)
    runner = ExperimentRunner(config, args)

    # Ensure baseline (once per session)
    baseline = runner.ensure_baseline(harness)
    harness.baseline = baseline

    # Run all experiments
    all_results = []

    for task in selected_tasks:
        print(f"\n{'='*70}")
        print(f"📋 TASK: {task['name']} (Level {task['level']})")
        print(f"{'='*70}")

        task_results = run_task(
            harness, runner, task, providers, repetitions, cool_down, args, config
        )
        all_results.extend(task_results)

    return all_results, config, args


def display_master_summary(all_results):
    """Display final summary table."""
    if not all_results:
        return

    print("\n" + "=" * 85)
    print("📊 MASTER SUMMARY")
    print("=" * 85)
    print(
        f"{'Provider':<12} {'Task':<20} {'Linear (J)':>12} {'Agentic (J)':>12} {'Tax (x)':>10} {'CI Range':>18}"
    )
    print("-" * 85)

    for r in all_results:
        provider = r["provider"]
        task = r["task"][:18]
        linear = f"{r['linear_energy_j']['mean']:.4f}"
        agentic = f"{r['agentic_energy_j']['mean']:.4f}"
        tax_mean = r["orchestration_tax"]["mean"]
        ci_lower = r["orchestration_tax"]["ci_lower"]
        ci_upper = r["orchestration_tax"]["ci_upper"]

        if not np.isnan(ci_lower):
            ci_display = f"[{ci_lower:.2f}, {ci_upper:.2f}]"
            tax_display = f"{tax_mean:.2f}x"
        else:
            ci_display = "N/A"
            tax_display = f"{tax_mean:.2f}x*"

        print(
            f"{provider:<12} {task:<20} {linear:>12} {agentic:>12} {tax_display:>10} {ci_display:>18}"
        )

    print("=" * 85)


def main():
    """Main entry point - minimal logic."""
    args = parse_arguments()

    if args.list_tasks:
        tasks = load_tasks()
        list_task_summary(tasks)
        return 0

    all_results, config, args = run_all_experiments(args)

    if all_results:
        display_master_summary(all_results)

        # Save to JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/experiment_{timestamp}.json"
        Path(filename).parent.mkdir(parents=True, exist_ok=True)

        with open(filename, "w") as f:
            json.dump(all_results, f, indent=2, default=str)
        print(f"\n💾 Results saved to: {filename}")

    print("\n✅ All experiments complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
