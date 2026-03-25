#!/usr/bin/env python3
"""
================================================================================
EXPERIMENT RUNNER – Shared logic for all experiment scripts
================================================================================

This module contains ONLY the code that is duplicated between test_harness.py
and run_experiment.py. All original features remain in each script.

NEW FEATURES ADDED:
- Session grouping (group_id)
- Status tracking (running/completed/partial/failed)
- Multi-provider support
- Progress tracking (runs_completed/runs_total)

Author: Deepak Panigrahy
================================================================================
"""

import json
import os
import socket
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from core.utils.preflight import preflight

import psutil

# Add project root to path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.config_loader import ConfigLoader
from core.database.manager import DatabaseManager
from core.models.baseline_measurement import BaselineMeasurement


class ExperimentRunner:
    """Shared experiment logic - ONLY duplicate code + new features"""

    def __init__(self, config_loader, args):
        self.config = config_loader
        self.args = args
        self.settings = config_loader.get_settings()
        self.group_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def validate_experiment(self, executor, provider):
        """Run pre-flight checks before experiment."""
        preflight(executor, provider)
        
    # ========================================================================
    # DUPLICATE CODE 1: Hardware info collection (identical in both scripts)
    # ========================================================================

    def get_hardware_info(self) -> Dict[str, Any]:
        """Get hardware info - loads from hw_config.json and flattens it"""
        hw_config_path = Path("config/hw_config.json")
        if hw_config_path.exists():
            with open(hw_config_path) as f:
                data = json.load(f)

                # Flatten nested structures using ONLY data from JSON
                flat_data = {
                    "hardware_hash": data.get("hardware_hash"),
                    "hostname": data.get("metadata", {}).get("hostname"),
                    "cpu_model": data.get("cpu_model"),
                    "cpu_cores": data.get("cpu_cores"),
                    "cpu_threads": data.get("cpu", {}).get("logical_cores"),
                    "cpu_architecture": data.get("metadata", {}).get("machine"),
                    "cpu_vendor": data.get("cpu_details", {}).get(
                        "vendor"
                    ),  # If exists
                    "cpu_family": data.get("cpu_details", {}).get("family"),
                    "cpu_model_id": data.get("cpu_details", {}).get("model"),
                    "cpu_stepping": data.get("cpu_details", {}).get("stepping"),
                    "has_avx2": data.get("cpu_flags", {}).get("has_avx2"),
                    "has_avx512": data.get("cpu_flags", {}).get("has_avx512"),
                    "has_vmx": data.get("cpu_flags", {}).get("has_vmx"),
                    "gpu_model": data.get("gpu_model"),
                    "gpu_driver": data.get("gpu", {}).get("driver"),
                    "gpu_count": data.get("gpu", {}).get("count"),
                    "gpu_power_available": data.get("gpu", {}).get("power_available"),
                    "ram_gb": data.get("ram_gb"),
                    "kernel_version": data.get("metadata", {}).get("release"),
                    "microcode_version": data.get("cpu", {}).get(
                        "microcode"
                    ),  # If exists
                    "rapl_domains": str(data.get("rapl", {}).get("available_domains")),
                    "rapl_has_dram": data.get("rapl", {}).get("has_dram"),
                    "rapl_has_uncore": "uncore"
                    in data.get("rapl", {}).get("available_domains", []),
                    "system_manufacturer": data.get("system", {}).get("manufacturer"),
                    "system_product": data.get("system", {}).get("product"),
                    "system_type": data.get("system", {}).get("type"),
                    "virtualization_type": data.get("system", {}).get("virtualization"),
                    "detected_at": data.get("metadata", {}).get("detected_at"),
                }
                return flat_data

    def get_environment_info(self) -> Dict[str, Any]:
        """Get environment information for reproducibility tracking"""
        import hashlib
        import json
        import platform
        import subprocess

        # Get git info
        git_commit = None
        git_branch = None
        git_dirty = None
        try:
            git_commit = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip()[:16]
            git_branch = subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True
            ).strip()
            git_dirty = bool(
                subprocess.check_output(
                    ["git", "status", "--porcelain"], text=True
                ).strip()
            )
        except:
            pass

        # Get dependency versions
        numpy_version = None
        torch_version = None
        transformers_version = None
        try:
            import numpy

            numpy_version = numpy.__version__
        except:
            pass
        try:
            import torch

            torch_version = torch.__version__
        except:
            torch_version = None
        try:
            import transformers

            transformers_version = transformers.__version__
        except:
            transformers_version = None

        # Build environment info
        env_info = {
            "python_version": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "os_name": platform.system(),
            "os_version": platform.version(),
            "kernel_version": platform.release(),
            "git_commit": git_commit,
            "git_branch": git_branch,
            "git_dirty": git_dirty,
            "numpy_version": numpy_version,
            "torch_version": torch_version,
            "transformers_version": transformers_version,
            "llm_framework": None,  # From experiment config
            "framework_version": None,  # Can be filled from model config
        }

        # Generate env_hash
        hash_input = json.dumps(
            {
                "python_version": env_info["python_version"],
                "git_commit": env_info["git_commit"],
                "numpy_version": env_info["numpy_version"],
            },
            sort_keys=True,
        )
        env_info["env_hash"] = hashlib.sha256(hash_input.encode()).hexdigest()[:16]

        return env_info

    # ========================================================================
    # Baseline measurement (from test_harness, add to run_experiment)
    # ========================================================================
    def ensure_baseline(self, harness) -> Optional[BaselineMeasurement]:
        """Get baseline (measure if needed) and insert to DB once."""
        baseline_config = self.settings.get("experiment", {}).get("baseline", {})
        force_remeasure = baseline_config.get("force_remeasure", False)
        cache_file = baseline_config.get("cache_file", "data/idle_baseline.json")
        
        # Check if cache file exists
        cache_path = Path(cache_file)
        cache_exists = cache_path.exists()
        
        # Determine if we need to measure
        needs_measure = force_remeasure or not cache_exists
        
        if needs_measure:
            print("\n" + "=" * 70)
            print("📏 MEASURING IDLE POWER BASELINE")
            print("=" * 70)

            duration = baseline_config.get("duration_seconds", 10)
            samples = baseline_config.get("num_samples", 3)
            pre_wait = baseline_config.get("pre_wait_seconds", 5)

            print(f"   Duration: {duration}s × {samples} samples = {duration * samples}s total")
            print("   Please don't use mouse/keyboard during this time.\n")

            try:
                harness.baseline = harness.energy_engine.measure_idle_baseline(
                    duration_seconds=duration,
                    num_samples=samples,
                    pre_wait_seconds=pre_wait,
                    force_remeasure=force_remeasure,
                )
                
                # Insert to DB (only once, when measured)
                harness.baseline_mgr.save(harness.baseline)

                print(f"\n   ✅ Baseline measured and saved!")
                print(f"      Baseline ID: {harness.baseline.baseline_id}")
                print(f"      Package idle power: {harness.baseline.power_watts.get('package-0', 0):.3f} W")
                print(f"      Core idle power:    {harness.baseline.power_watts.get('core', 0):.3f} W")

            except Exception as e:
                import traceback
                print(f"\n   ⚠️ Baseline measurement failed: {e}")
                traceback.print_exc()
                print("   Continuing without baseline")
                return None
        else:
            # Load from cache if not already in memory
            if not harness.baseline:
                try:
                    with open(cache_path, 'r') as f:
                        data = json.load(f)
                        harness.baseline = BaselineMeasurement.from_dict(data)
                    print(f"\n📏 Loaded baseline from cache: {harness.baseline.baseline_id}")
                except Exception as e:
                    print(f"\n⚠️ Failed to load baseline from cache: {e}")
                    return None
            else:
                print(f"\n📏 Using existing baseline: {harness.baseline.baseline_id}")
        
        return harness.baseline



    def aggregate_run_stats(
        self, run_id: int, cpu_samples: List[Dict], interrupt_samples: List[Dict]
    ) -> Dict:
        """
        Compute aggregated statistics for a run from samples.
        This will populate runs table with correct averages.
        """
        stats = {
            "run_id": run_id,
            "cpu_busy_mhz": 0.0,
            "cpu_avg_mhz": 0.0,
            "package_temp_celsius": 0.0,
            "max_temp_c": 0.0,
            "min_temp_c": 0.0,
            "interrupt_rate": 0.0,
        }

        # Aggregate CPU samples
        if cpu_samples:
            busy_freqs = [
                s.get("cpu_busy_mhz", 0) for s in cpu_samples if s.get("cpu_busy_mhz")
            ]
            avg_freqs = [
                s.get("cpu_avg_mhz", 0) for s in cpu_samples if s.get("cpu_avg_mhz")
            ]
            temps = [
                s.get("package_temp", 0) for s in cpu_samples if s.get("package_temp")
            ]

            if busy_freqs:
                stats["cpu_busy_mhz"] = sum(busy_freqs) / len(busy_freqs)
            if avg_freqs:
                stats["cpu_avg_mhz"] = sum(avg_freqs) / len(avg_freqs)
            if temps:
                stats["package_temp_celsius"] = sum(temps) / len(temps)
                stats["max_temp_c"] = max(temps)
                stats["min_temp_c"] = min(temps)

        # Aggregate interrupt samples
        if interrupt_samples:
            irq_rates = [
                s.get("interrupts_per_sec", 0)
                for s in interrupt_samples
                if s.get("interrupts_per_sec")
            ]
            if irq_rates:
                stats["interrupt_rate"] = sum(irq_rates) / len(irq_rates)

        return stats

    # ========================================================================
    # DUPLICATE CODE 3: Database setup (similar in both scripts)
    # ========================================================================
    def setup_database(self) -> Tuple[DatabaseManager, int]:
        """Database setup - similar in both scripts"""
        db_config = self.config.get_db_config()
        db = DatabaseManager(db_config)
        db.create_tables()
        hw_id = db.insert_hardware(self.get_hardware_info())
        env_id = self.setup_environment(db)
        return db, hw_id

    # ========================================================================
    # DUPLICATE CODE 4: Run data preparation (identical in both scripts)
    # ========================================================================
    def prepare_run_data(self, results, baseline_id=None) -> List[Dict]:
        """Extract run data from ml_dataset - identical in both scripts"""
        all_runs = []
        if "ml_dataset" in results:
            # Linear runs
            if "linear_runs" in results["ml_dataset"]:
                for rd in results["ml_dataset"]["linear_runs"]:
                    run_package = {
                        "ml_features": rd,
                        "sustainability": {
                            "carbon": {"grams": rd.get("carbon_g", 0)},
                            "water": {"milliliters": rd.get("water_ml", 0)},
                            "methane": {"grams": rd.get("methane_mg", 0)},
                        },
                        "baseline_id": baseline_id,
                        "harness_timestamp": datetime.now().isoformat(),
                    }
                    all_runs.append(run_package)
            # Agentic runs
            if "agentic_runs" in results["ml_dataset"]:
                for rd in results["ml_dataset"]["agentic_runs"]:
                    run_package = {
                        "ml_features": rd,
                        "sustainability": {
                            "carbon": {"grams": rd.get("carbon_g", 0)},
                            "water": {"milliliters": rd.get("water_ml", 0)},
                            "methane": {"grams": rd.get("methane_mg", 0)},
                        },
                        "baseline_id": baseline_id,
                        "harness_timestamp": datetime.now().isoformat(),
                    }
                    all_runs.append(run_package)
        return all_runs

    # ========================================================================
    # DUPLICATE CODE 5: Energy sample conversion (identical in both scripts)
    # ========================================================================
    def convert_energy_samples(self, results) -> List[Dict]:
        """Convert energy samples - identical in both scripts"""
        samples = []
        if "energy_samples" in results:
            for sample in results["energy_samples"]:
                if len(sample) == 2 and isinstance(sample[1], dict):
                    timestamp, energy_dict = sample
                    samples.append(
                        {
                            "timestamp_ns": int(timestamp * 1_000_000_000),
                            "pkg_energy_uj": energy_dict.get("package-0", 0),
                            "core_energy_uj": energy_dict.get("core", 0),
                            "uncore_energy_uj": energy_dict.get("uncore", 0),
                            "dram_energy_uj": 0,
                        }
                    )
        return samples

    def setup_database(
        self,
    ) -> Tuple[DatabaseManager, int, int]:  # ← Returns 3 values now
        """Setup database connection and return db, hw_id, env_id"""
        db = DatabaseManager(self.config.get_db_config())

        # Create tables if needed
        db.create_tables()

        # Get or create hardware record
        hw_id = db.insert_hardware(self.get_hardware_info())

        # Get or create environment record  ← ADD THIS
        env_id = self._get_or_create_environment(db)

        return db, hw_id, env_id  # ← Return both IDs

    def _get_or_create_environment(self, db) -> int:
        """Auto-detect if environment changed - insert if new, return existing if same"""
        env_info = self.get_environment_info()
        current_hash = env_info["env_hash"]

        # Check if this environment hash already exists
        result = db.db.execute(
            "SELECT env_id FROM environment_config WHERE env_hash = ?", (current_hash,)
        )
        if result:
            existing_id = result[0]["env_id"]
            print(
                f"   ✅ Using existing environment: {existing_id} (hash: {current_hash})"
            )
            return existing_id

        # New environment - insert it
        print(f"   📦 New environment detected (hash: {current_hash}) - inserting...")
        new_id = db.insert_environment_config(env_info)
        print(f"   ✅ Created new environment: {new_id}")
        return new_id

    def setup_environment(self, db) -> int:
        """Get or create environment record"""
        env_info = self.get_environment_info()
        return db.insert_environment_config(env_info)

    # ========================================================================
    # NEW FEATURE 1: Create experiment with group_id and status
    # ========================================================================
    def create_experiment(
        self,
        db,
        task_id,
        task_name,
        provider,
        linear_config,
        country_code,
        repetitions,
        hw_id,
        env_id,
        optimizer=False,
    ) -> int:
        """Create experiment with session tracking (NEW)"""
        experiment_meta = {
            "name": f"{task_id}_{provider}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "description": f"Task: {task_name}",
            "workflow_type": "comparison",
            "model_name": linear_config.get("name", "unknown"),
            "provider": provider,
            "task_name": task_id,
            "country_code": country_code,
            "group_id": self.group_id,  # NEW
            "status": "running",  # NEW
            "started_at": datetime.now().isoformat(),  # NEW
            "runs_total": repetitions * 2,  # linear + agentic
            "optimization_enabled": 1 if optimizer else 0,
            "hw_id": hw_id,
            "env_id": env_id,
        }
        return db.insert_experiment(experiment_meta)

    # ========================================================================
    # NEW FEATURE 2: Update experiment status
    # ========================================================================
    def update_status(
        self,
        db,
        exp_id: int,
        status: str,
        runs_completed: int = None,
        error: str = None,
    ):
        """Update experiment status (NEW)"""
        updates = {"status": status}
        if status in ["completed", "failed", "partial"]:
            updates["completed_at"] = datetime.now().isoformat()
        if runs_completed is not None:
            updates["runs_completed"] = runs_completed
        if error:
            updates["error_message"] = error

        set_clause = ", ".join([f"{k}=?" for k in updates.keys()])
        values = list(updates.values()) + [exp_id]
        db.db.execute(f"UPDATE experiments SET {set_clause} WHERE exp_id=?", values)

    def update_progress(self, db, exp_id: int, runs_completed: int):
        """
        Update progress of an experiment without changing status.

        Args:
            db: Database connection
            exp_id: Experiment ID
            runs_completed: Number of runs completed so far
        """
        db.db.execute(
            "UPDATE experiments SET runs_completed = ? WHERE exp_id = ?",
            (runs_completed, exp_id),
        )
        print(
            f"   📊 Progress: {runs_completed}/{self._get_total_runs(db, exp_id)} runs"
        )

    def _get_total_runs(self, db, exp_id: int) -> int:
        """Get total runs expected for experiment"""
        result = db.db.execute(
            "SELECT runs_total FROM experiments WHERE exp_id = ?", (exp_id,)
        )
        return result[0]["runs_total"] if result else 0

    # ========================================================================
    # NEW FEATURE 3: Multi-provider helper
    # ========================================================================
    def get_providers(self):
        """Get list of providers from args (NEW)"""
        if hasattr(self.args, "providers") and self.args.providers:
            return [p.strip() for p in self.args.providers.split(",")]
        elif hasattr(self.args, "provider") and self.args.provider:
            return [self.args.provider]
        else:
            return ["cloud"]

    # ========================================================================
    # CPU SAMPLES - Identical in both scripts
    # ========================================================================
    def get_cpu_samples(self, results) -> List[Dict]:
        """Get CPU samples - identical in test_harness and run_experiment"""
        samples = []
        if "cpu_samples" in results:
            samples = results["cpu_samples"]
            print(f"   Found {len(samples)} CPU samples (ready for insertion)")
            if samples and len(samples) > 0:
                print(f"   🔍 First CPU sample keys: {list(samples[0].keys())}")
                print(f"   🔍 First CPU sample values: {samples[0]}")
        return samples

    # ========================================================================
    # INTERRUPT SAMPLES - Identical in both scripts
    # ========================================================================
    def get_interrupt_samples(self, results) -> List[Dict]:
        """Get interrupt samples - identical in test_harness and run_experiment"""
        samples = []
        if "interrupt_samples" in results:
            samples = results["interrupt_samples"]
            print(f"   Found {len(samples)} interrupt samples (ready for insertion)")
        return samples

    def ensure_baseline_in_db(self, db, harness):
        """Save baseline to database once per experiment session."""
        if not harness.baseline:
            print("⚠️ No baseline available - foreign key constraints may fail")
            return False

        if hasattr(self, "_baseline_saved"):
            return True

        # Check if already in database
        try:
            result = db.execute(
                "SELECT 1 FROM idle_baselines WHERE baseline_id = ?",
                (harness.baseline.baseline_id,),
            )
            if result.fetchone():
                print(
                    f"✅ Baseline {harness.baseline.baseline_id} already exists in database"
                )
                self._baseline_saved = True
                return True
        except Exception:
            pass

        # Save baseline
        try:
            b = harness.baseline
            metadata = b.metadata or {}

            baseline_dict = {
                "baseline_id": b.baseline_id,
                "timestamp": b.timestamp,
                "package_power_watts": b.power_watts.get("package-0", 0),
                "core_power_watts": b.power_watts.get("core", 0),
                "uncore_power_watts": b.power_watts.get("uncore", 0),
                "dram_power_watts": b.power_watts.get("dram", 0),
                "duration_seconds": b.duration_seconds,
                "sample_count": b.sample_count,
                "package_std": b.std_dev_watts.get("package-0"),
                "core_std": b.std_dev_watts.get("core"),
                "uncore_std": b.std_dev_watts.get("uncore"),
                "dram_std": b.std_dev_watts.get("dram"),
                "governor": metadata.get("governor"),
                "turbo": metadata.get("turbo"),
                "background_cpu": metadata.get("background_cpu"),
                "process_count": metadata.get("process_count"),
                "method": b.method,
            }

            db.insert_baseline(baseline_dict)
            self._baseline_saved = True
            print(f"✅ Baseline {b.baseline_id} saved to database")
            return True

        except Exception as e:
            print(f"❌ Failed to save baseline: {e}")
            return False

    def save_pair(self, db, exp_id, hw_id, linear_result, agentic_result, rep_num):
        """Save one pair of runs with all samples."""

        # Set run_number
        linear_result["ml_features"]["run_number"] = rep_num
        agentic_result["ml_features"]["run_number"] = rep_num

        linear_copy = linear_result.copy()
        agentic_copy = agentic_result.copy()
        linear_copy["baseline_id"] = linear_copy["ml_features"].get("baseline_id")
        agentic_copy["baseline_id"] = agentic_copy["ml_features"].get("baseline_id")

        with db.transaction():
            # Insert linear run
            linear_id = db.insert_run(exp_id, hw_id, linear_result)

            # Linear energy samples
            if "energy_samples" in linear_result:
                converted = []
                for sample in linear_result["energy_samples"]:
                    if len(sample) == 2 and isinstance(sample[1], dict):
                        timestamp, energy_dict = sample
                        converted.append(
                            {
                                "timestamp_ns": int(timestamp * 1_000_000_000),
                                "pkg_energy_uj": energy_dict.get("package-0", 0),
                                "core_energy_uj": energy_dict.get("core", 0),
                                "uncore_energy_uj": energy_dict.get("uncore", 0),
                                "dram_energy_uj": 0,
                            }
                        )
                if converted:
                    db.insert_energy_samples(linear_id, converted)

            # Linear CPU samples
            if "cpu_samples" in linear_result:
                db.insert_cpu_samples(linear_id, linear_result["cpu_samples"])

            # Linear interrupt samples
            if "interrupt_samples" in linear_result:
                db.insert_interrupt_samples(
                    linear_id, linear_result["interrupt_samples"]
                )

            # Save thermal samples
            if "thermal_samples" in linear_result:
                db.insert_thermal_samples(linear_id, linear_result["thermal_samples"])
                print(
                    f"🔍 DEBUG - Saving {len(linear_result['thermal_samples'])} thermal samples for run {linear_id}"
                )

                # After inserting samples, update runs with aggregated stats
                linear_agg = self.aggregate_run_stats(
                    linear_id,
                    linear_result.get("cpu_samples", []),
                    linear_result.get("interrupt_samples", []),
                )
                db.update_run_stats(linear_id, linear_agg)

            # Insert agentic run
            agentic_id = db.insert_run(exp_id, hw_id, agentic_result)

            # Agentic energy samples
            if "energy_samples" in agentic_result:
                converted = []
                for sample in agentic_result["energy_samples"]:
                    if len(sample) == 2 and isinstance(sample[1], dict):
                        timestamp, energy_dict = sample
                        converted.append(
                            {
                                "timestamp_ns": int(timestamp * 1_000_000_000),
                                "pkg_energy_uj": energy_dict.get("package-0", 0),
                                "core_energy_uj": energy_dict.get("core", 0),
                                "uncore_energy_uj": energy_dict.get("uncore", 0),
                                "dram_energy_uj": 0,
                            }
                        )
                if converted:
                    db.insert_energy_samples(agentic_id, converted)

            # Agentic CPU samples
            if "cpu_samples" in agentic_result:
                db.insert_cpu_samples(agentic_id, agentic_result["cpu_samples"])

            # Agentic interrupt samples
            if "interrupt_samples" in agentic_result:
                db.insert_interrupt_samples(
                    agentic_id, agentic_result["interrupt_samples"]
                )

            if "thermal_samples" in agentic_result:
                db.insert_thermal_samples(agentic_id, agentic_result["thermal_samples"])
                print(
                    f"🔍 DEBUG - Saving {len(agentic_result['thermal_samples'])} thermal samples for run {agentic_id}"
                )

                agentic_agg = self.aggregate_run_stats(
                    agentic_id,
                    agentic_result.get("cpu_samples", []),
                    agentic_result.get("interrupt_samples", []),
                )
                db.update_run_stats(agentic_id, agentic_agg)
            # Agentic orchestration events
            if "orchestration_events" in agentic_result:
                db.insert_orchestration_events(
                    agentic_id, agentic_result["orchestration_events"]
                )

            print(
                f"🔍 DEBUG - linear pending_interactions count: {len(linear_result.get('pending_interactions', []))}"
            )
            print(
                f"🔍 DEBUG - agentic pending_interactions count: {len(agentic_result.get('pending_interactions', []))}"
            )

            # Save LLM interactions for linear run
            if (
                "pending_interactions" in linear_result
                and linear_result["pending_interactions"]
            ):
                print(
                    f"   💾 Saving {len(linear_result['pending_interactions'])} LLM interactions for linear run {linear_id}"
                )
                for interaction in linear_result["pending_interactions"]:
                    interaction["run_id"] = linear_id
                    db.insert_llm_interaction(interaction)

            # Save LLM interactions for agentic run
            if (
                "pending_interactions" in agentic_result
                and agentic_result["pending_interactions"]
            ):
                print(
                    f"   💾 Saving {len(agentic_result['pending_interactions'])} LLM interactions for agentic run {agentic_id}"
                )
                for interaction in agentic_result["pending_interactions"]:
                    interaction["run_id"] = agentic_id
                    db.insert_llm_interaction(interaction)
            # Tax summary for this pair
            linear_uj = linear_result["layer3_derived"]["energy_uj"]["workload"]
            agentic_uj = agentic_result["layer3_derived"]["energy_uj"]["workload"]
            linear_orchestration_uj = linear_result["ml_features"].get(
                "orchestration_tax_uj", 0
            )
            agentic_orchestration_uj = agentic_result["ml_features"].get(
                "orchestration_tax_uj", 0
            )

            print(
                f"🔍 DEBUG - linear_orchestration_uj from ml_features: {linear_result['ml_features'].get('orchestration_tax_uj')}"
            )
            print(
                f"🔍 DEBUG - agentic_orchestration_uj from ml_features: {agentic_result['ml_features'].get('orchestration_tax_uj')}"
            )

            linear_orchestration_uj = linear_result["layer3_derived"]["energy_uj"].get(
                "orchestration_tax", 0
            )
            agentic_orchestration_uj = agentic_result["layer3_derived"][
                "energy_uj"
            ].get("orchestration_tax", 0)
            print(
                f"🔍 DEBUG - linear energy_uj keys: {linear_result['layer3_derived']['energy_uj'].keys()}"
            )
            print(
                f"🔍 DEBUG - agentic energy_uj keys: {agentic_result['layer3_derived']['energy_uj'].keys()}"
            )
            print(
                f"🔍 DEBUG - linear energy_uj content: {linear_result['layer3_derived']['energy_uj']}"
            )
            print(
                f"🔍 DEBUG - linear energy_uj content: {agentic_result['layer3_derived']['energy_uj']}"
            )

            db.create_tax_summary_for_pair(
                linear_id,
                agentic_id,
                linear_uj,
                agentic_uj,
                linear_orchestration_uj,
                agentic_orchestration_uj,
            )

        print(
            f"   ✅ Pair {rep_num} saved (linear: {linear_id}, agentic: {agentic_id})"
        )
        return linear_id, agentic_id
