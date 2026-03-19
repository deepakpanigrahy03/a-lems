#!/usr/bin/env python3
"""
Test all repositories together with connection persistence.
"""

import sys
import tempfile
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.database.factory import DatabaseFactory
from core.database.repositories import (EventsRepository, RunsRepository,
                                        SamplesRepository, TaxRepository)


def test_repositories():
    """Test all repositories working together."""
    print("\n🔍 Testing Repositories...")
    print("=" * 50)

    # Create temp database
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
        print(f"📁 Temp database: {db_path}")

    db = None
    try:
        # Configure and create database
        config = {
            "engine": "sqlite",
            "sqlite": {"path": db_path, "journal_mode": "WAL", "timeout": 30},
        }

        # Create database adapter
        db = DatabaseFactory.create(config)
        db.connect()
        print("✅ Connected to database")

        # Create tables
        db.create_tables()
        print("✅ Tables created")

        # Create repositories
        runs_repo = RunsRepository(db)
        events_repo = EventsRepository(db)
        samples_repo = SamplesRepository(db)
        tax_repo = TaxRepository(db)
        print("✅ All repositories created")

        # ====================================================================
        # Use a single transaction for all inserts
        # ====================================================================
        with db:
            # Insert hardware
            hw_id = db.insert_hardware(
                {
                    "hostname": "test-host",
                    "cpu_model": "Intel Core Test",
                    "cpu_cores": 8,
                    "cpu_threads": 16,
                    "ram_gb": 32,
                    "kernel_version": "6.5.0",
                    "microcode_version": "0x123",
                    "rapl_domains": "package,core,uncore",
                }
            )
            print(f"✅ Inserted hardware with ID: {hw_id}")

            # Insert baseline
            baseline_id = db.insert_baseline(
                {
                    "timestamp": time.time(),
                    "package_power_watts": 10.5,
                    "core_power_watts": 5.2,
                    "uncore_power_watts": 3.1,
                    "dram_power_watts": 2.2,
                    "duration_seconds": 10,
                    "sample_count": 10,
                    "package_std": 0.1,
                    "core_std": 0.05,
                    "uncore_std": 0.02,
                    "dram_std": 0.01,
                    "governor": "powersave",
                    "turbo": "enabled",
                    "background_cpu": 2.5,
                    "process_count": 350,
                    "method": "test",
                }
            )
            print(f"✅ Inserted baseline with ID: {baseline_id}")

            # Insert experiment
            exp_id = db.insert_experiment(
                {
                    "name": "test_experiment",
                    "description": "Testing repositories",
                    "workflow_type": "linear",
                    "model_name": "test-model",
                    "provider": "test",
                    "task_name": "test-task",
                    "country_code": "US",
                }
            )
            print(f"✅ Inserted experiment with ID: {exp_id}")

        print("✅ Transaction committed")

        # Verify they exist in a separate transaction
        print("\n🔍 Verifying inserts...")
        exp_check = db.execute("SELECT COUNT(*) as count FROM experiments")
        print(f"   Experiments count: {exp_check[0]['count']}")

        hw_check = db.execute("SELECT COUNT(*) as count FROM hardware_config")
        print(f"   Hardware count: {hw_check[0]['count']}")

        bl_check = db.execute("SELECT COUNT(*) as count FROM idle_baselines")
        print(f"   Baselines count: {bl_check[0]['count']}")

        # ====================================================================
        # Now insert the run
        # ====================================================================
        dummy_run = {
            "ml_features": {
                "run_number": 1,
                "duration_ms": 1000.0,
                "energy_j": 0.5,
                "avg_power_watts": 0.5,
                "instructions": 1000000,
                "cycles": 2000000,
                "ipc": 0.5,
                "cache_misses": 10000,
                "cache_references": 100000,
                "cache_miss_rate": 0.1,
                "page_faults": 10,
                "major_page_faults": 0,
                "minor_page_faults": 10,
                "context_switches_voluntary": 50,
                "context_switches_involuntary": 5,
                "total_context_switches": 55,
                "thread_migrations": 10,
                "run_queue_length": 0.1,
                "kernel_time_ms": 10.0,
                "user_time_ms": 20.0,
                "frequency_mhz": 2000.0,
                "ring_bus_freq_mhz": 1800.0,
                "package_temp_celsius": 45.0,
                "baseline_temp_celsius": 40.0,
                "start_temp_c": 42.0,
                "max_temp_c": 46.0,
                "thermal_during_experiment": False,
                "thermal_now_active": False,
                "thermal_since_boot": False,
                "experiment_valid": True,
                "c2_time_seconds": 0.1,
                "c3_time_seconds": 0.2,
                "c6_time_seconds": 0.0,
                "c7_time_seconds": 0.0,
                "wakeup_latency_us": 5.0,
                "interrupt_rate": 1000.0,
                "thermal_throttle_flag": 0,
                "rss_memory_mb": 150.0,
                "vms_memory_mb": 750.0,
                "total_tokens": 100,
                "prompt_tokens": 40,
                "completion_tokens": 60,
                "dns_latency_ms": 10.0,
                "api_latency_ms": 50.0,
                "compute_time_ms": 40.0,
                "governor": "powersave",
                "turbo_enabled": True,
                "is_cold_start": True,
                "background_cpu_percent": 2.5,
                "process_count": 350,
                "planning_time_ms": None,
                "execution_time_ms": None,
                "synthesis_time_ms": None,
                "phase_planning_ratio": None,
                "phase_execution_ratio": None,
                "phase_synthesis_ratio": None,
                "llm_calls": None,
                "tool_calls": None,
                "tools_used": None,
                "steps": None,
                "avg_step_time_ms": None,
                "complexity_level": None,
                "complexity_score": None,
            },
            "sustainability": {
                "carbon": {"grams": 0.0001},
                "water": {"milliliters": 0.001},
                "methane": {"grams": 0.00001},
            },
            "harness_timestamp": "2024-01-01T00:00:00",
            "baseline_id": baseline_id,
        }

        print("\n🔄 Inserting run...")
        run_id = runs_repo.insert_run(exp_id, hw_id, dummy_run)
        print(f"✅ Inserted run with ID: {run_id}")

        # Verify run was inserted
        run = db.get_run(run_id)
        if run:
            print(f"✅ Verified run exists: {run['run_id']}")
            print(f"   Run number: {run['run_number']}")
            print(f"   Energy: {run['total_energy_uj']/1e6:.4f} J")
        else:
            print("❌ Run not found after insertion")
            return False

        print("\n✅ All repository tests passed!")
        return True

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        # Cleanup
        if db:
            db.close()
        Path(db_path).unlink(missing_ok=True)
        print("🧹 Cleaned up temp database")


if __name__ == "__main__":
    success = test_repositories()
    sys.exit(0 if success else 1)
