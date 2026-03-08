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

import os
import sys
import time
import socket
import psutil
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

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
        
    # ========================================================================
    # DUPLICATE CODE 1: Hardware info collection (identical in both scripts)
    # ========================================================================
    def get_hardware_info(self) -> Dict[str, Any]:
        """Hardware info - identical in both scripts"""
        return {
            'hostname': socket.gethostname(),
            'cpu_model': 'Unknown',
            'cpu_cores': psutil.cpu_count(logical=False),
            'cpu_threads': psutil.cpu_count(logical=True),
            'ram_gb': psutil.virtual_memory().total // (1024**3),
            'kernel_version': os.uname().release,
            'microcode_version': 'Unknown',
            'rapl_domains': 'package,core,uncore,dram'
        }
    
    # ========================================================================
    # DUPLICATE CODE 2: Baseline measurement (from test_harness, add to run_experiment)
    # ========================================================================
    def ensure_baseline(self, harness) -> Optional[BaselineMeasurement]:
        """Baseline measurement - add to run_experiment which currently lacks it"""
        baseline_config = self.settings.get('experiment', {}).get('baseline', {})
        force_remeasure = baseline_config.get('force_remeasure', False)
        
        if force_remeasure or not harness.baseline:
            print("\n" + "="*70)
            print("📏 MEASURING IDLE POWER BASELINE")
            print("="*70)
            
            duration = baseline_config.get('duration_seconds', 10)
            samples = baseline_config.get('num_samples', 3)
            pre_wait = baseline_config.get('pre_wait_seconds', 5)
            force = baseline_config.get('force_remeasure', True)
            
            print(f"   Duration: {duration}s × {samples} samples = {duration * samples}s total")
            print(f"   Force remeasure: {force}")
            print("   Please don't use mouse/keyboard during this time.\n")
            
            try:
                harness.baseline = harness.energy_engine.measure_idle_baseline(
                    duration_seconds=duration,
                    num_samples=samples,
                    pre_wait_seconds=pre_wait,
                    force_remeasure=force
                )
                harness.baseline_mgr.save(harness.baseline)
                
                print(f"\n   ✅ Baseline measured and saved!")
                print(f"      Baseline ID: {harness.baseline.baseline_id}")
                print(f"      Package idle power: {harness.baseline.power_watts.get('package-0', 0):.3f} W")
                print(f"      Core idle power:    {harness.baseline.power_watts.get('core', 0):.3f} W")
                return harness.baseline
                
            except Exception as e:
                print(f"\n   ⚠️ Baseline measurement failed: {e}")
                import traceback
                traceback.print_exc()
                print("   Continuing without baseline")
                return None
        else:
            print(f"\n📏 Using existing baseline: {harness.baseline.baseline_id}")
            return harness.baseline
    
    # ========================================================================
    # DUPLICATE CODE 3: Database setup (similar in both scripts)
    # ========================================================================
    def setup_database(self) -> Tuple[DatabaseManager, int]:
        """Database setup - similar in both scripts"""
        db_config = self.config.get_db_config()
        db = DatabaseManager(db_config)
        db.create_tables()
        hw_id = db.insert_hardware(self.get_hardware_info())
        return db, hw_id
    
    # ========================================================================
    # DUPLICATE CODE 4: Run data preparation (identical in both scripts)
    # ========================================================================
    def prepare_run_data(self, results, baseline_id=None) -> List[Dict]:
        """Extract run data from ml_dataset - identical in both scripts"""
        all_runs = []
        if 'ml_dataset' in results:
            # Linear runs
            if 'linear_runs' in results['ml_dataset']:
                for rd in results['ml_dataset']['linear_runs']:
                    run_package = {
                        'ml_features': rd,
                        'sustainability': {
                            'carbon': {'grams': rd.get('carbon_g', 0)},
                            'water': {'milliliters': rd.get('water_ml', 0)},
                            'methane': {'grams': rd.get('methane_mg', 0)}
                        },
                        'baseline_id': baseline_id,
                        'harness_timestamp': datetime.now().isoformat()
                    }
                    all_runs.append(run_package)
            # Agentic runs
            if 'agentic_runs' in results['ml_dataset']:
                for rd in results['ml_dataset']['agentic_runs']:
                    run_package = {
                        'ml_features': rd,
                        'sustainability': {
                            'carbon': {'grams': rd.get('carbon_g', 0)},
                            'water': {'milliliters': rd.get('water_ml', 0)},
                            'methane': {'grams': rd.get('methane_mg', 0)}
                        },
                        'baseline_id': baseline_id,
                        'harness_timestamp': datetime.now().isoformat()
                    }
                    all_runs.append(run_package)
        return all_runs
    
    # ========================================================================
    # DUPLICATE CODE 5: Energy sample conversion (identical in both scripts)
    # ========================================================================
    def convert_energy_samples(self, results) -> List[Dict]:
        """Convert energy samples - identical in both scripts"""
        samples = []
        if 'energy_samples' in results:
            for sample in results['energy_samples']:
                if len(sample) == 2 and isinstance(sample[1], dict):
                    timestamp, energy_dict = sample
                    samples.append({
                        'timestamp_ns': int(timestamp * 1_000_000_000),
                        'pkg_energy_uj': energy_dict.get('package-0', 0),
                        'core_energy_uj': energy_dict.get('core', 0),
                        'uncore_energy_uj': energy_dict.get('uncore', 0),
                        'dram_energy_uj': 0
                    })
        return samples
    
    # ========================================================================
    # NEW FEATURE 1: Create experiment with group_id and status
    # ========================================================================
    def create_experiment(self, db, task_id, task_name, provider, 
                          linear_config, country_code,  repetitions, optimizer=False) -> int:
        """Create experiment with session tracking (NEW)"""
        experiment_meta = {
            'name': f"{task_id}_{provider}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'description': f"Task: {task_name}",
            'workflow_type': 'comparison',
            'model_name': linear_config.get('name', 'unknown'),
            'provider': provider,
            'task_name': task_id,
            'country_code': country_code,
            'group_id': self.group_id,      # NEW
            'status': 'running',            # NEW
            'started_at': datetime.now().isoformat(),  # NEW
            'runs_total': repetitions * 2,  # linear + agentic
            'optimization_enabled': 1 if optimizer else 0
        }
        return db.insert_experiment(experiment_meta)
    
    # ========================================================================
    # NEW FEATURE 2: Update experiment status
    # ========================================================================
    def update_status(self, db, exp_id: int, status: str, 
                     runs_completed: int = None, error: str = None):
        """Update experiment status (NEW)"""
        updates = {'status': status}
        if status in ['completed', 'failed', 'partial']:
            updates['completed_at'] = datetime.now().isoformat()
        if runs_completed is not None:
            updates['runs_completed'] = runs_completed
        if error:
            updates['error_message'] = error
        
        set_clause = ', '.join([f"{k}=?" for k in updates.keys()])
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
            (runs_completed, exp_id)
        )
        print(f"   📊 Progress: {runs_completed}/{self._get_total_runs(db, exp_id)} runs")
    
    def _get_total_runs(self, db, exp_id: int) -> int:
        """Get total runs expected for experiment"""
        result = db.db.execute(
            "SELECT runs_total FROM experiments WHERE exp_id = ?",
            (exp_id,)
        )
        return result[0]['runs_total'] if result else 0


    # ========================================================================
    # NEW FEATURE 3: Multi-provider helper
    # ========================================================================
    def get_providers(self):
        """Get list of providers from args (NEW)"""
        if hasattr(self.args, 'providers') and self.args.providers:
            return [p.strip() for p in self.args.providers.split(',')]
        elif hasattr(self.args, 'provider') and self.args.provider:
            return [self.args.provider]
        else:
            return ['cloud']
    # ========================================================================
    # CPU SAMPLES - Identical in both scripts
    # ========================================================================
    def get_cpu_samples(self, results) -> List[Dict]:
        """Get CPU samples - identical in test_harness and run_experiment"""
        samples = []
        if 'cpu_samples' in results:
            samples = results['cpu_samples']
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
        if 'interrupt_samples' in results:
            samples = results['interrupt_samples']
            print(f"   Found {len(samples)} interrupt samples (ready for insertion)")
        return samples 
    def save_pair(self, db, exp_id, hw_id, linear_result, agentic_result, rep_num):
        """Save one pair of runs with all samples."""
        
        # Set run_number
        linear_result['ml_features']['run_number'] = rep_num
        agentic_result['ml_features']['run_number'] = rep_num

        linear_copy = linear_result.copy()
        agentic_copy = agentic_result.copy()
        linear_copy['baseline_id'] = linear_copy['ml_features'].get('baseline_id')
        agentic_copy['baseline_id'] = agentic_copy['ml_features'].get('baseline_id')
        
        with db.transaction():
            # Insert linear run
            linear_id = db.insert_run(exp_id, hw_id, linear_result)
            
            # Linear energy samples
            if 'energy_samples' in linear_result:
                converted = []
                for sample in linear_result['energy_samples']:
                    if len(sample) == 2 and isinstance(sample[1], dict):
                        timestamp, energy_dict = sample
                        converted.append({
                            'timestamp_ns': int(timestamp * 1_000_000_000),
                            'pkg_energy_uj': energy_dict.get('package-0', 0),
                            'core_energy_uj': energy_dict.get('core', 0),
                            'uncore_energy_uj': energy_dict.get('uncore', 0),
                            'dram_energy_uj': 0
                        })
                if converted:
                    db.insert_energy_samples(linear_id, converted)
            
            # Linear CPU samples
            if 'cpu_samples' in linear_result:
                db.insert_cpu_samples(linear_id, linear_result['cpu_samples'])
            
            # Linear interrupt samples
            if 'interrupt_samples' in linear_result:
                db.insert_interrupt_samples(linear_id, linear_result['interrupt_samples'])
            
            # Insert agentic run
            agentic_id = db.insert_run(exp_id, hw_id, agentic_result)
            
            # Agentic energy samples
            if 'energy_samples' in agentic_result:
                converted = []
                for sample in agentic_result['energy_samples']:
                    if len(sample) == 2 and isinstance(sample[1], dict):
                        timestamp, energy_dict = sample
                        converted.append({
                            'timestamp_ns': int(timestamp * 1_000_000_000),
                            'pkg_energy_uj': energy_dict.get('package-0', 0),
                            'core_energy_uj': energy_dict.get('core', 0),
                            'uncore_energy_uj': energy_dict.get('uncore', 0),
                            'dram_energy_uj': 0
                        })
                if converted:
                    db.insert_energy_samples(agentic_id, converted)
            
            # Agentic CPU samples
            if 'cpu_samples' in agentic_result:
                db.insert_cpu_samples(agentic_id, agentic_result['cpu_samples'])
            
            # Agentic interrupt samples
            if 'interrupt_samples' in agentic_result:
                db.insert_interrupt_samples(agentic_id, agentic_result['interrupt_samples'])
            
            # Agentic orchestration events
            if 'orchestration_events' in agentic_result:
                db.insert_orchestration_events(agentic_id, agentic_result['orchestration_events'])
            
            # Tax summary for this pair
            linear_uj = linear_result['layer3_derived']['energy_uj']['workload']
            agentic_uj = agentic_result['layer3_derived']['energy_uj']['workload']
            linear_orchestration_uj = linear_result['ml_features'].get('orchestration_tax_uj', 0)
            agentic_orchestration_uj = agentic_result['ml_features'].get('orchestration_tax_uj', 0)
            

            print(f"🔍 DEBUG - linear_orchestration_uj from ml_features: {linear_result['ml_features'].get('orchestration_tax_uj')}")
            print(f"🔍 DEBUG - agentic_orchestration_uj from ml_features: {agentic_result['ml_features'].get('orchestration_tax_uj')}")  

            linear_orchestration_uj = linear_result['layer3_derived']['energy_uj'].get('orchestration_tax', 0)
            agentic_orchestration_uj = agentic_result['layer3_derived']['energy_uj'].get('orchestration_tax', 0) 
            print(f"🔍 DEBUG - linear energy_uj keys: {linear_result['layer3_derived']['energy_uj'].keys()}")
            print(f"🔍 DEBUG - agentic energy_uj keys: {agentic_result['layer3_derived']['energy_uj'].keys()}")
            print(f"🔍 DEBUG - linear energy_uj content: {linear_result['layer3_derived']['energy_uj']}")
            print(f"🔍 DEBUG - linear energy_uj content: {agentic_result['layer3_derived']['energy_uj']}")
        

            db.create_tax_summary_for_pair(
            linear_id, agentic_id, 
            linear_uj, agentic_uj,
            linear_orchestration_uj, agentic_orchestration_uj  
        )
            

        
        print(f"   ✅ Pair {rep_num} saved (linear: {linear_id}, agentic: {agentic_id})")
        return linear_id, agentic_id


