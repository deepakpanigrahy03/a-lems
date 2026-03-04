#!/usr/bin/env python3
"""
================================================================================
EXPERIMENT HARNESS – Wraps AI execution with energy measurement
================================================================================

Purpose:
    This is the CRITICAL synchronization layer between energy measurement
    (Module 1) and AI execution (Module 3). It ensures perfect alignment
    between RAPL counters and code execution.

SCIENTIFIC NOTES:
    - Uses Module 1's 3‑layer architecture correctly:
        Layer 1: RawEnergyMeasurement (from EnergyEngine)
        Layer 2: BaselineMeasurement (idle power)
        Layer 3: DerivedEnergyMeasurement (computed metrics for analysis)
    - ALL THREE LAYERS are returned in results for complete transparency
    - Energy measurement wraps ENTIRE execution (not just API calls)
    - Warmup runs discard first execution to eliminate cache/initialization effects
    - Multiple repetitions (n>=30) for statistical power
    - Cool‑down period between linear and agentic ensures fair comparison
    - Network latency tracked separately for cloud models
    - CPU metrics come from Layer 3's performance counters

Why this exists:
    - Timestamps inside executors can drift from hardware counters
    - Even 50ms misalignment corrupts orchestration tax measurements
    - This harness guarantees perfect synchronization

Requirements:
    Req 3.6: Device Handoff Latency – precise timing alignment
    Req 1.46: High‑frequency sampling – must capture short agent steps
    Req 3.5: Cool‑down period between runs

Author: Deepak Panigrahy
================================================================================
"""

import time
import logging
import socket
import numpy as np
from typing import Dict, Any, Optional, List
from datetime import datetime
from scipy import stats as scipy_stats
import psutil
import json
import subprocess
import os

from core.energy_engine import EnergyEngine
from core.analysis.energy_analyzer import EnergyAnalyzer
from core.sustainability.calculator import SustainabilityCalculator
from core.utils.baseline_manager import BaselineManager
from core.utils.debug import dprint
from core.database.manager import DatabaseManager
from core.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class ExperimentHarness:
    """
    Wraps AI execution with energy measurement for perfect synchronization.
    
    Critical design:
        energy_start()  ← RAPL starts recording
        executor.execute()  ← AI code runs
        energy_stop()   ← RAPL stops recording
        
    This ensures hardware counters align exactly with software execution.
    
    For cloud models, network latency is tracked separately to distinguish
    orchestration overhead from network delays.
    """

    def __init__(self, config_loader):
        """
        Initialize harness with energy measurement and analysis modules.
        
        Args:
            config_loader: Module 0 config loader
        """
        hw_config = config_loader.get_hardware_config()
        settings = config_loader.get_settings()
        
        # Convert settings to dict if needed
        if hasattr(settings, '__dict__'):
            settings_dict = settings.__dict__
        else:
            settings_dict = settings
        
        # Merge into single config dict for EnergyEngine
        engine_config = hw_config.copy()
        engine_config['settings'] = settings_dict
        
        self.energy_engine = EnergyEngine(engine_config)  # ← Now passing dict
        self.energy_analyzer = EnergyAnalyzer()
        self.sustainability = SustainabilityCalculator(config_loader)  # ← This stays as ConfigLoader
        
        # Load baseline if available (Layer 2)
        self.baseline_mgr = BaselineManager()
        self.baseline = self.baseline_mgr.get_latest()
        if self.baseline:
            logger.info(f"Loaded baseline: {self.baseline.baseline_id}")
        else:
            logger.warning("No baseline found. Will measure during experiment.")
        
        
        if not self.baseline:
            logger.warning("No baseline found. Run baseline measurement first.")
            dprint("⚠️ No baseline – energy values will not be corrected for idle power")
        
        logger.info("ExperimentHarness initialized")

    def _measure_network_latency(self, hostname: str = "api.groq.com") -> Dict[str, float]:
        """
        Measure network latency to cloud provider.
        
        Purpose:
            Separate network delays from true orchestration overhead.
            This is critical for cloud experiments where network latency
            can dominate measurements.
            
        Args:
            hostname: Cloud provider hostname
            
        Returns:
            Dictionary with DNS and ping latencies
        """
        network_metrics = {}
        
        # Measure DNS resolution time
        dns_start = time.time()
        try:
            socket.gethostbyname(hostname)
            dns_latency = (time.time() - dns_start) * 1000
            network_metrics['dns_latency_ms'] = dns_latency
        except:
            network_metrics['dns_latency_ms'] = None
        
        return network_metrics

    def _warmup_run(self, executor, prompt: str, is_agentic: bool = False) -> None:
        """
        Perform a warmup run to eliminate initialization effects.
        
        Scientific rationale:
            First run often includes:
            - Cold caches
            - API connection establishment
            - Python JIT compilation
            - Model loading (local models)
            
        These would skew energy measurements, so we discard warmup.
        
        Args:
            executor: Linear or Agentic executor
            prompt: The task prompt
            is_agentic: Whether this is agentic (uses comparison method)
        """
        dprint("🔥 Warmup run (results discarded)")
        if is_agentic:
            executor.execute_comparison(prompt)
        else:
            executor.execute(prompt)
        dprint("✅ Warmup complete")

    # =========================================================================
    # FIX M3-1: Governor/Turbo Control
    # =========================================================================
    def get_governor(self) -> str:
        """
        Get current CPU frequency governor.
        
        Returns:
            'performance', 'powersave', 'ondemand', etc., or 'unknown'
        """
        try:
            with open('/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor', 'r') as f:
                return f.read().strip()
        except Exception as e:
            logger.debug(f"Could not read governor: {e}")
            return 'unknown'
    
    def get_turbo_status(self) -> str:
        """
        Get turbo boost status.
        
        Returns:
            'enabled' if turbo is on, 'disabled' if off, 'unknown' if can't read
        """
        try:
            # Intel p-state driver
            with open('/sys/devices/system/cpu/intel_pstate/no_turbo', 'r') as f:
                # 0 = turbo enabled, 1 = turbo disabled
                return 'disabled' if int(f.read().strip()) else 'enabled'
        except:
            try:
                # Alternative location for some systems
                with open('/sys/devices/system/cpu/cpufreq/boost', 'r') as f:
                    return 'enabled' if int(f.read().strip()) else 'disabled'
            except Exception as e:
                logger.debug(f"Could not read turbo status: {e}")
                return 'unknown'
    
    # =========================================================================
    # FIX M3-2: Interrupt Rate
    # =========================================================================
    def _read_interrupts(self) -> int:
        """
        Read total interrupt count from /proc/stat.
        
        Returns:
            Total interrupts since boot, or 0 if cannot read
        """
        try:
            with open('/proc/stat', 'r') as f:
                for line in f:
                    if line.startswith('intr'):
                        # Format: intr <total> <interrupts...>
                        return int(line.split()[1])
        except Exception as e:
            logger.warning(f"Could not read interrupts: {e}")
        return 0
    
    def get_interrupt_rate(self, before: int, after: int, duration_seconds: float) -> float:
        """
        Calculate interrupt rate from start/end readings.
        
        Args:
            before: Interrupt count at start
            after: Interrupt count at end
            duration_seconds: Duration of measurement
            
        Returns:
            Interrupts per second
        """
        if duration_seconds <= 0:
            return 0.0
        return (after - before) / duration_seconds
    
    # =========================================================================
    # FIX M3-3: Temperature Tracking
    # =========================================================================
    def _read_temperature(self) -> float:
        """
        Read current package temperature in Celsius.
        
        Tries multiple sources:
        1. Thermal zone (most common)
        2. Coretemp hwmon (fallback)
        3. Any hwmon with temp1_input
        
        Returns:
            Temperature in °C, or last known good temperature if reading fails
        """
        # ========== START OF FIXES ==========
        
        # Store last good temperature as instance variable
        if not hasattr(self, '_last_good_temp'):
            self._last_good_temp = 0.0
        
        # Method 1: Try ALL thermal zones (not just zone0)
        for zone in range(10):  # ← CHANGED: Loop through zones 0-9
            try:
                path = f'/sys/class/thermal/thermal_zone{zone}/temp'
                with open(path, 'r') as f:
                    temp = int(f.read().strip()) / 1000.0
                    # Sanity check: valid CPU temps are between 10°C and 100°C
                    if 10 < temp < 100:  # ← NEW: Add sanity check
                        self._last_good_temp = temp
                        return temp
            except:
                continue
        
        # Method 2: Try all hwmon devices (already does this)
        try:
            import glob
            for hwmon in glob.glob('/sys/class/hwmon/hwmon*/temp1_input'):
                with open(hwmon, 'r') as f:
                    temp = int(f.read().strip()) / 1000.0
                    if 10 < temp < 100:  # ← NEW: Add sanity check
                        self._last_good_temp = temp
                        return temp
        except:
            pass
        
        # ========== END OF FIXES ==========
        
        # Return last known good temperature instead of 0.0
        if self._last_good_temp > 0:
            logger.debug(f"Returning last good temperature: {self._last_good_temp}°C")
            return self._last_good_temp
        
        logger.debug("Could not read temperature")
        return 0.0
    
    # =========================================================================
    # FIX M3-4: Cold Start Flag
    # =========================================================================
    def _is_cold_start(self, run_number: int) -> bool:
        """
        Determine if this is a cold start run.
        
        Args:
            run_number: Current run number (1-based)
            
        Returns:
            True if this is the first run in the batch
        """
        return run_number == 1
    
    # =========================================================================
    # FIX M3-5: Background Noise
    # =========================================================================
    def get_background_cpu(self) -> float:
        """
        Get current background CPU usage percent.
        
        Returns:
            CPU usage percentage (0-100)
        """
        try:
            # Short interval to get current usage
            return psutil.cpu_percent(interval=0.1)
        except Exception as e:
            logger.debug(f"Could not get CPU percent: {e}")
            return 0.0
    
    def get_process_count(self) -> int:
        """
        Get number of running processes.
        
        Returns:
            Total number of processes
        """
        try:
            return len(psutil.pids())
        except Exception as e:
            logger.debug(f"Could not get process count: {e}")
            return 0
    
    # =========================================================================
    # FIX M3-6: Memory Metrics (RSS, VMS - process level)
    # =========================================================================
    def get_process_memory(self) -> Dict[str, float]:
        """
        Get memory usage for the current Python process.
        
        Returns:
            Dictionary with:
            - rss_mb: Resident Set Size in MB (physical memory)
            - vms_mb: Virtual Memory Size in MB (total virtual address space)
        """
        metrics = {'rss_mb': 0.0, 'vms_mb': 0.0}
        try:
            process = psutil.Process()
            mem_info = process.memory_info()
            metrics['rss_mb'] = mem_info.rss / (1024 * 1024)  # Convert to MB
            metrics['vms_mb'] = mem_info.vms / (1024 * 1024)  # Convert to MB
            logger.debug(f"Process memory: RSS={metrics['rss_mb']:.1f}MB, VMS={metrics['vms_mb']:.1f}MB")
        except Exception as e:
            logger.debug(f"Could not get process memory: {e}")
        return metrics

    def save_to_database(self, results: Dict[str, Any], experiment_meta: Dict[str, Any], hardware_info: Optional[Dict] = None) -> Optional[int]:
        """
        Save experiment results to database.
        
        Args:
            results: Results from run_comparison
            experiment_meta: Experiment metadata
            hardware_info: Hardware configuration (optional)
            
        Returns:
            exp_id if successful, None otherwise
        """
        try:
            # Load database config
            config_loader = ConfigLoader()
            db_config = config_loader.get_db_config()
            
            # Create database manager
            db = DatabaseManager(db_config)
            
            # Ensure tables exist
            db.create_tables()
            
            # Get hardware ID if info provided
            hw_id = None
            if hardware_info:
                hw_id = db.insert_hardware(hardware_info)
            
            # Insert experiment
            exp_id = db.insert_experiment(experiment_meta)
            
            # Insert all runs (linear and agentic)
            all_runs = []
            if 'all_runs' in results:
                # New format with all_runs dict
                all_runs.extend(results['all_runs'].get('linear', []))
                all_runs.extend(results['all_runs'].get('agentic', []))
            else:
                # Old format - single run
                all_runs.append(results)
            
            with db.transaction():
                for run in all_runs:
                    db.insert_run(exp_id, hw_id, run)
            
            # Create tax summaries
            db.create_tax_summaries(exp_id)
            
            db.close()
            logger.info(f"✅ Saved experiment {exp_id} to database")
            return exp_id
            
        except Exception as e:
            logger.error(f"❌ Failed to save to database: {e}")
            return None

    # Note: System-wide swap metrics are already in scheduler_monitor.py
    # This is handled by M3-6 (partial) - we only need RSS/VMS here
    def run_linear(self, executor, prompt: str, task_id: str = None, is_cloud: bool = True, country_code: str = "US", run_number: int = 1) -> Dict[str, Any]:
        """
        Run linear executor with synchronized energy measurement.
        
        This uses the 3‑layer architecture correctly and returns ALL THREE LAYERS:
            1. RawEnergyMeasurement from EnergyEngine (Layer 1)
            2. Baseline from BaselineManager (Layer 2)
            3. DerivedEnergyMeasurement from EnergyAnalyzer (Layer 3) ← USED FOR METRICS
        """
        dprint(f"\n{'='*70}")
        dprint(f"🔬 HARNESS: Starting LINEAR measurement")
        dprint(f"{'='*70}")
        self.energy_engine.scheduler.reset_interrupt_samples()
        # Measure network latency for cloud models
        network_metrics = {}
        if is_cloud:
            network_metrics = self._measure_network_latency()
            dprint(f"📡 Network DNS: {network_metrics.get('dns_latency_ms', 0):.1f}ms")
        # ====================================================================
        # Capture system state BEFORE run (M3-1 through M3-6)
        # ====================================================================
        intr_before = self._read_interrupts()
        temp_start = self._read_temperature()
        governor = self.get_governor()
        turbo = self.get_turbo_status()
        background_cpu = self.get_background_cpu()
        process_count = self.get_process_count()
        memory_before = self.get_process_memory()
        is_cold = self._is_cold_start(run_number)    

        # ====================================================================
        # Step 1: Get Raw Energy Measurement (Layer 1)
        # ====================================================================
        run_start_dt = datetime.now()  # Human-readable start time
        run_start_perf = time.perf_counter()  # High-precision for duration        
        self.energy_engine.start_measurement()
        exec_result = executor.execute(prompt)
        raw_energy = self.energy_engine.stop_measurement()  # RawEnergyMeasurement (Layer 1)
        run_end_dt = datetime.now()  # Human-readable end time
        run_end_perf = time.perf_counter()
        run_duration_sec = run_end_perf - run_start_perf

        # ====================================================================
        # DEBUG: Check what's available in energy_engine
        # ====================================================================
        dprint(f"🔍 DEBUG - energy_engine attributes: {dir(self.energy_engine)}")
        if hasattr(self.energy_engine, 'samples'):
            dprint(f"🔍 DEBUG - samples keys: {self.energy_engine.last_samples.keys()}")
        else:
            dprint("🔍 DEBUG - energy_engine has NO 'samples' attribute")

        # ====================================================================
        # Load canonical metrics from override file
        # ====================================================================
        import yaml
        from pathlib import Path
        
        canonical_metrics = {}
        store_extra = True
        
        override_path = Path("config/turbostat_override.yaml")
        if override_path.exists():
            try:
                with open(override_path, 'r') as f:
                    override_config = yaml.safe_load(f)
                canonical_metrics = override_config.get('canonical_metrics', {})
                store_extra = override_config.get('store_extra_in_json', True)
                dprint(f"📋 Loaded {len(canonical_metrics)} canonical metrics from override file")
            except Exception as e:
                dprint(f"⚠️ Failed to load override file: {e}")


        # ====================================================================
        # Get samples from energy engine
        # ====================================================================
        energy_samples = []
        cpu_samples = []
        interrupt_samples = []
        
        if hasattr(self.energy_engine, 'last_samples'):
            samples = self.energy_engine.last_samples
            dprint(f"🔍 DEBUG - Found {len(samples)} samples in energy_engine.last_samples")

            # DEBUG: Print first few samples to see structure
            for i, sample in enumerate(samples[:3]):
                dprint(f"🔍 DEBUG - Sample {i}: length={len(sample)}, content={sample}") 

            # Samples are tuples - we need to identify them by structure
            for sample in samples:
                if len(sample) == 2 and isinstance(sample[1], dict):
                    # This is an energy sample: (timestamp, {'core':..., 'package-0':..., 'uncore':...})
                    timestamp, energy_dict = sample
                    energy_samples.append({
                        'timestamp_ns': int(timestamp * 1_000_000_000),  # Convert seconds to ns
                        'pkg_energy_uj': energy_dict.get('package-0', 0),
                        'core_energy_uj': energy_dict.get('core', 0),
                        'uncore_energy_uj': energy_dict.get('uncore', 0),
                        'dram_energy_uj': 0  # DRAM not in samples
                    })
                elif len(sample) == 2 and isinstance(sample[1], (int, float)):
                    # This could be an interrupt sample: (timestamp, value)
                    interrupt_samples.append({
                        'timestamp_ns': sample[0],
                        'interrupts_per_sec': sample[1]
                    })
                else:
                    # Unknown sample type
                    dprint(f"⚠️ Unknown sample format: {sample}")
            
            dprint(f"📊 Processed {len(energy_samples)} energy samples, {len(cpu_samples)} CPU samples, {len(interrupt_samples)} interrupt samples")
        # ====================================================================
        # Load canonical metrics from override file
        # ====================================================================
        import yaml
        from pathlib import Path
        
        canonical_metrics = {}
        store_extra = True
        
        override_path = Path("config/turbostat_override.yaml")
        if override_path.exists():
            try:
                with open(override_path, 'r') as f:
                    override_config = yaml.safe_load(f)
                canonical_metrics = override_config.get('canonical_metrics', {})
                store_extra = override_config.get('store_extra_in_json', True)
                dprint(f"📋 Loaded {len(canonical_metrics)} canonical metrics from override file")
            except Exception as e:
                dprint(f"⚠️ Failed to load override file: {e}")

        # ====================================================================
        # Extract CPU samples from turbostat continuous data
        # ====================================================================
        cpu_samples = []
        if hasattr(raw_energy, 'turbostat') and raw_energy.turbostat.get('dataframe') is not None:
            df = raw_energy.turbostat['dataframe']
            
            # Get timing info from metadata
            start_ns = None
            interval_ns = 100_000_000  # Default 100ms
            if hasattr(raw_energy, 'metadata'):
                start_ns = raw_energy.metadata.get('turbostat_start_ns')
                interval_ns = raw_energy.metadata.get('turbostat_interval_ns', 100_000_000)
            
            if not df.empty:
                for idx, row in df.iterrows():
                    # Calculate timestamp using monotonic clock
                    if start_ns is not None:
                        timestamp_ns = start_ns + (idx + 1) * interval_ns
                    else:
                        # Fallback to old method
                        timestamp_ns = int((raw_energy.start_time + idx * 0.1) * 1e9)
                    
                    # Start with timestamp
                    sample = {'timestamp_ns': timestamp_ns}
                    
                    # Extract canonical metrics
                    for our_name, turbostat_col in canonical_metrics.items():
                        try:
                            val = float(row.get(turbostat_col, 0.0))
                            
                            # Scale percentages (C-states, GPU RC6)
                            if our_name in ['c1_residency', 'c2_residency', 'c3_residency',
                                           'c6_residency', 'c7_residency',
                                           'pkg_c8_residency', 'pkg_c9_residency', 'pkg_c10_residency',
                                           'gpu_rc6']:
                                val = val / 100.0
                            
                            # IPC might need scaling if >10
                            if our_name == 'ipc' and val > 10:
                                val = val / 10.0
                            
                            sample[our_name] = val
                        except (TypeError, ValueError):
                            sample[our_name] = 0.0
                    
                    # Store all other columns in JSON
                    if store_extra:
                        extra = {}
                        for col in df.columns:
                            if col not in canonical_metrics.values():
                                val = row.get(col)
                                if val is not None:
                                    try:
                                        extra[col] = float(val)
                                    except (TypeError, ValueError):
                                        extra[col] = str(val)
                        sample['extra_metrics_json'] = json.dumps(extra) if extra else '{}'
                    
                    cpu_samples.append(sample)
                
                print(f"📊 Extracted {len(cpu_samples)} CPU samples with {len(canonical_metrics)} canonical metrics")
                
                if cpu_samples:
                    print(f"🔍 First 3 CPU samples:")
                    for i, sample in enumerate(cpu_samples[:3]):
                        print(f"   Sample {i}: {sample}")
        # ====================================================================
        # Calculate thermal metrics from CPU samples
        # ====================================================================
        if cpu_samples and len(cpu_samples) > 0:
            temps = [s.get('package_temp') for s in cpu_samples if s.get('package_temp', 0) > 10]
            if temps:
                start_temp_c = temps[0]          # First sample
                max_temp_c = max(temps)           # Maximum during run
                min_temp_c = min(temps)           # Minimum during run
                thermal_delta_c = max_temp_c - start_temp_c
            else:
                start_temp_c = 0
                max_temp_c = 0
                min_temp_c = 0
                thermal_delta_c = 0
        else:
            start_temp_c = 0
            max_temp_c = 0
            min_temp_c = 0
            thermal_delta_c = 0
            
        # ====================================================================
        # Step 2: Compute Derived Energy (Layer 3) using Baseline (Layer 2)
        # ====================================================================
        derived = self.energy_analyzer.compute(raw_energy, self.baseline)  # DerivedEnergyMeasurement (Layer 3)
        
        # ====================================================================
        # Step 3: Calculate sustainability (optional)
        # ====================================================================
        sustainability = self.sustainability.calculate_from_derived(
            derived, 
            country_code=country_code,
            query_count=1
        )
        
        # ====================================================================
        # Step 4: Energy per token (using Layer 3)
        # ====================================================================
        if exec_result.get('tokens', {}).get('total', 0) > 0:
            energy_per_token = derived.workload_energy_j / exec_result['tokens']['total']
        else:
            energy_per_token = 0
        
        # ====================================================================
        # Step 5: CPU metrics from Layer 3's performance_counters
        # ====================================================================
        if hasattr(derived, 'performance_counters') and derived.performance_counters:
            cpu_metrics = {
                'instructions': derived.performance_counters.instructions,
                'cycles': derived.performance_counters.cpu_cycles,
                'ipc': derived.performance_counters.instructions_per_cycle(),
                'cache_misses': derived.performance_counters.cache_misses,
                'context_switches': derived.performance_counters.total_context_switches()
            }
        else:
            # Fallback if performance_counters not available
            cpu_metrics = {
                'instructions': 0,
                'cycles': 0,
                'ipc': 0,
                'cache_misses': 0,
                'context_switches': 0,
                'note': 'Performance counters not available'
            }
        

        # ====================================================================
        # Step 6: Return ALL THREE LAYERS with ML features
        # ====================================================================


        # ====================================================================
        # Capture system state AFTER run
        # ====================================================================
        intr_after = self._read_interrupts()
        temp_max = max(temp_start, self._read_temperature())
        memory_after = self.get_process_memory()
        duration = raw_energy.duration_seconds
        interrupt_rate = self.get_interrupt_rate(intr_before, intr_after, duration)

        result = {
            'experiment_id': exec_result.get('experiment_id'),
            'task_id': task_id,
            'workflow': 'linear',
            'country_code': country_code,
            'execution': exec_result,
            
            # THREE LAYERS – All available for analysis
            'layer1_raw': raw_energy.to_dict(),                    # Raw hardware readings
            'layer2_baseline': self.baseline.to_dict() if self.baseline else None,  # Idle reference
            'layer3_derived': derived.to_dict(),                   # Corrected metrics
            
            # Backward compatibility (keep old keys)
            'raw_energy': raw_energy.to_dict(),
            'derived_energy': derived.to_dict(),
            
            # Other metrics
            'sustainability': sustainability.to_dict() if sustainability else None,
            'network_metrics': network_metrics,
            'energy_per_token': energy_per_token,
            'cpu_metrics': cpu_metrics,
            
            # ====================================================================
            # NEW: ML Features Dictionary (ALL features for training)
            # ====================================================================
            'ml_features': {
                # Hardware metrics (from layer3_derived)
                'start_time_ns': int(run_start_dt.timestamp() * 1_000_000_000),
                'end_time_ns': int(run_end_dt.timestamp() * 1_000_000_000),
                'start_time_iso': run_start_dt.isoformat(),
                'end_time_iso': run_end_dt.isoformat(),
                'duration_sec': run_duration_sec,
                'duration_ms': run_duration_sec * 1000,                
                'instructions': derived.instructions,
                'cycles': derived.cycles,
                'ipc': derived.ipc,
                'cache_misses': derived.cache_misses,
                'cache_references': derived.cache_references,
                'cache_miss_rate': derived.cache_misses / derived.cache_references if derived.cache_references > 0 else 0,
                'context_switches_voluntary': derived.context_switches_voluntary,
                'context_switches_involuntary': derived.context_switches_involuntary,
                'total_context_switches': derived.total_context_switches,
                'thread_migrations': derived.thread_migrations,
                'run_queue_length': derived.run_queue_length,
                'kernel_time_ms': derived.kernel_time_ms,
                'user_time_ms': derived.user_time_ms,
                'frequency_mhz': derived.frequency_mhz,
                'package_temp_celsius': derived.package_temp_celsius,
                'baseline_temp_celsius': self.baseline.cpu_temperature_c if self.baseline else None,
                
                # C-state metrics
                'c2_time_seconds': derived.c2_time_seconds,
                'c3_time_seconds': derived.c3_time_seconds,
                'c6_time_seconds': derived.c6_time_seconds,
                'c7_time_seconds': derived.c7_time_seconds,
                
                # Ring bus
                'ring_bus_freq_mhz': derived.ring_bus_current_mhz,
                'wakeup_latency_us': derived.wakeup_latency_us,
                
                # Thermal validity
                'thermal_during_experiment': derived.thermal_during_experiment,
                'thermal_now_active': derived.thermal_now_active,
                'thermal_since_boot': derived.thermal_since_boot,
                'experiment_valid': (derived.thermal_during_experiment == 0 and derived.thermal_now_active == 0),
                
                # Token metrics (from execution)
                'total_tokens': exec_result.get('tokens', {}).get('total', 0),
                'prompt_tokens': exec_result.get('tokens', {}).get('prompt', 0),
                'completion_tokens': exec_result.get('tokens', {}).get('completion', 0),
                
                # Network metrics (for cloud models)
                'dns_latency_ms': network_metrics.get('dns_latency_ms', 0),
                'api_latency_ms': exec_result.get('api_latency_ms', 0),
                'compute_time_ms': exec_result.get('compute_time_ms', exec_result.get('execution_time_ms', 0)),
                # =============================================================
                # NEW: System State Metrics (ADD THESE)
                # =============================================================
                'governor': governor,
                'baseline_id': self.baseline.baseline_id if self.baseline else None,
                'turbo_enabled': 1 if turbo == 'enabled' else 0,
                'interrupt_rate': interrupt_rate,
                'start_temp_c': start_temp_c,
                'max_temp_c': max_temp_c,
                'min_temp_c': min_temp_c,
                'thermal_delta_c': thermal_delta_c,
                'is_cold_start': 1 if is_cold else 0,
                'background_cpu_percent': background_cpu,
                'process_count': process_count,
                'rss_memory_mb': memory_after.get('rss_mb', 0),
                'vms_memory_mb': memory_after.get('vms_mb', 0),                
                # Metadata
                'model_name': executor.config.get('model_id', 'unknown'),
                'provider': executor.provider,
                'task_id': task_id,
                'country_code': country_code,
                'workflow_type': 'linear',
                
                # ====================================================================
                # TARGETS (what we want to predict)
                # ====================================================================
                'energy_j': derived.workload_energy_j,
                'carbon_g': sustainability.carbon.grams if sustainability else 0,
                'duration_ms': derived.duration_seconds * 1000,
            },
            'energy_samples': energy_samples,
            'cpu_samples': cpu_samples,
            'interrupt_samples': interrupt_samples,            
            'harness_timestamp': datetime.now().isoformat(),
            'scientific_notes': {
                'measurement_scope': 'client_side_orchestration_only',
                'layers': {
                    'layer1_raw': 'RawEnergyMeasurement (archived)',
                    'layer2_baseline': self.baseline is not None,
                    'layer3_derived': 'DerivedEnergyMeasurement (used for analysis)'
                },
                'includes': ['cpu_energy', 'memory_energy', 'local_computation', 'performance_counters'],
                'excludes': ['model_inference_on_cloud'] if is_cloud else [],
                'baseline_corrected': self.baseline is not None
            }
        }
        

        # ====================================================================
        # Add high-frequency samples to result
        # ====================================================================
        if hasattr(self.energy_engine, 'last_samples'):
            result['energy_samples'] = self.energy_engine.last_samples
            dprint(f"📊 Added {len(self.energy_engine.last_samples)} energy samples to result")
        else:
            dprint("⚠️ No last_samples attribute found in energy_engine")

        if hasattr(self.energy_engine, 'last_interrupt_samples'):
            result['interrupt_samples'] = self.energy_engine.last_interrupt_samples
            dprint(f"📊 Added {len(self.energy_engine.last_interrupt_samples)} energy samples to result")
        else:
            dprint("⚠️ No last_samples attribute found in energy_engine")

                

        dprint(f"✅ Harness complete: {derived.workload_energy_j:.4f}J workload energy")
        return result

    def run_agentic(self, executor, task: str, task_id: str = None, is_cloud: bool = True, country_code: str = "US", run_number: int = 1) -> Dict[str, Any]:
        """
        Run agentic executor with synchronized energy measurement.
        
        Same critical alignment as linear:
        Energy measurement wraps ENTIRE agentic pipeline.
        
        This captures ALL orchestration tax in one synchronized window:
        - Planning
        - Tool execution  
        - Synthesis
        - Inter-step delays
        
        Uses the same 3‑layer architecture and returns ALL THREE LAYERS.
        """
        dprint(f"\n{'='*70}")
        dprint(f"🔬 HARNESS: Starting AGENTIC measurement")
        dprint(f"{'='*70}")
        self.energy_engine.scheduler.reset_interrupt_samples()
        # Measure network latency for cloud models
        network_metrics = {}
        if is_cloud:
            network_metrics = self._measure_network_latency()
            dprint(f"📡 Network DNS: {network_metrics.get('dns_latency_ms', 0):.1f}ms")
        # ====================================================================
        # Capture system state BEFORE run (M3-1 through M3-6)
        # ====================================================================
        intr_before = self._read_interrupts()
        temp_start = self._read_temperature()
        governor = self.get_governor()
        turbo = self.get_turbo_status()
        background_cpu = self.get_background_cpu()
        process_count = self.get_process_count()
        memory_before = self.get_process_memory()
        is_cold = self._is_cold_start(run_number)        
        # ====================================================================
        # Step 1: Get Raw Energy Measurement (Layer 1)
        # ====================================================================
        run_start_dt = datetime.now()  # Human-readable start time
        run_start_perf = time.perf_counter()  # High-precision for duration        
        self.energy_engine.start_measurement()
        exec_result = executor.execute_comparison(task)
        raw_energy = self.energy_engine.stop_measurement()  # RawEnergyMeasurement (Layer 1)
        run_end_dt = datetime.now()  # Human-readable end time
        run_end_perf = time.perf_counter()
        run_duration_sec = run_end_perf - run_start_perf        
        # ====================================================================
        # DEBUG: Check what's available in energy_engine
        # ====================================================================
        dprint(f"🔍 DEBUG - energy_engine attributes: {dir(self.energy_engine)}")
        if hasattr(self.energy_engine, 'last_samples'):
            dprint(f"🔍 DEBUG - samples keys: {self.energy_engine.last_samples}")
            if self.energy_engine.last_samples:
                dprint(f"🔍 DEBUG - first sample type: {type(self.energy_engine.last_samples[0])}")
                dprint(f"🔍 DEBUG - first sample type: {type(self.energy_engine.last_samples[0])}")
        else:
            dprint("🔍 DEBUG - energy_engine has NO 'samples' attribute")

        # ====================================================================
        # Load canonical metrics from override file
        # ====================================================================
        import yaml
        from pathlib import Path
        
        canonical_metrics = {}
        store_extra = True
        
        override_path = Path("config/turbostat_override.yaml")
        if override_path.exists():
            try:
                with open(override_path, 'r') as f:
                    override_config = yaml.safe_load(f)
                canonical_metrics = override_config.get('canonical_metrics', {})
                store_extra = override_config.get('store_extra_in_json', True)
                dprint(f"📋 Loaded {len(canonical_metrics)} canonical metrics from override file")
            except Exception as e:
                dprint(f"⚠️ Failed to load override file: {e}")

        # ====================================================================
        # Get high-frequency samples from energy engine
        # ====================================================================
        energy_samples = []
        cpu_samples = []
        interrupt_samples = []
        
        if hasattr(self.energy_engine, 'last_samples'):
            samples = self.energy_engine.last_samples
            dprint(f"🔍 DEBUG - Found {len(samples)} samples in energy_engine.last_samples")

            # DEBUG: Print first few samples to see structure
            for i, sample in enumerate(samples[:3]):
                dprint(f"🔍 DEBUG - Sample {i}: length={len(sample)}, content={sample}") 

            # Samples are tuples - we need to identify them by structure
            for sample in samples:
                if len(sample) == 2 and isinstance(sample[1], dict):
                    # This is an energy sample: (timestamp, {'core':..., 'package-0':..., 'uncore':...})
                    timestamp, energy_dict = sample
                    energy_samples.append({
                        'timestamp_ns': int(timestamp * 1_000_000_000),  # Convert seconds to ns
                        'pkg_energy_uj': energy_dict.get('package-0', 0),
                        'core_energy_uj': energy_dict.get('core', 0),
                        'uncore_energy_uj': energy_dict.get('uncore', 0),
                        'dram_energy_uj': 0  # DRAM not in samples
                    })
                elif len(sample) == 2 and isinstance(sample[1], (int, float)):
                    # This could be an interrupt sample: (timestamp, value)
                    interrupt_samples.append({
                        'timestamp_ns': sample[0],
                        'interrupts_per_sec': sample[1]
                    })
                else:
                    # Unknown sample type
                    dprint(f"⚠️ Unknown sample format: {sample}")
            
            dprint(f"📊 Processed {len(energy_samples)} energy samples, {len(cpu_samples)} CPU samples, {len(interrupt_samples)} interrupt samples") 


        # ====================================================================
        # Extract CPU samples from turbostat continuous data
        # ====================================================================
        cpu_samples = []
        if hasattr(raw_energy, 'turbostat') and raw_energy.turbostat.get('dataframe') is not None:
            df = raw_energy.turbostat['dataframe']
            
            # Get timing info from metadata
            start_ns = None
            interval_ns = 100_000_000  # Default 100ms
            if hasattr(raw_energy, 'metadata'):
                start_ns = raw_energy.metadata.get('turbostat_start_ns')
                interval_ns = raw_energy.metadata.get('turbostat_interval_ns', 100_000_000)
            
            if not df.empty:
                for idx, row in df.iterrows():
                    # Calculate timestamp using monotonic clock
                    if start_ns is not None:
                        timestamp_ns = start_ns + (idx + 1) * interval_ns
                    else:
                        # Fallback to old method
                        timestamp_ns = int((raw_energy.start_time + idx * 0.1) * 1e9)
                    
                    # Start with timestamp
                    sample = {'timestamp_ns': timestamp_ns}
                    
                    # Extract canonical metrics
                    for our_name, turbostat_col in canonical_metrics.items():
                        try:
                            val = float(row.get(turbostat_col, 0.0))
                            
                            # Scale percentages (C-states, GPU RC6)
                            if our_name in ['c1_residency', 'c2_residency', 'c3_residency',
                                           'c6_residency', 'c7_residency',
                                           'pkg_c8_residency', 'pkg_c9_residency', 'pkg_c10_residency',
                                           'gpu_rc6']:
                                val = val / 100.0
                            
                            # IPC might need scaling if >10
                            if our_name == 'ipc' and val > 10:
                                val = val / 10.0
                            
                            sample[our_name] = val
                        except (TypeError, ValueError):
                            sample[our_name] = 0.0
                    
                    # Store all other columns in JSON
                    if store_extra:
                        extra = {}
                        for col in df.columns:
                            if col not in canonical_metrics.values():
                                val = row.get(col)
                                if val is not None:
                                    try:
                                        extra[col] = float(val)
                                    except (TypeError, ValueError):
                                        extra[col] = str(val)
                        sample['extra_metrics_json'] = json.dumps(extra) if extra else '{}'
                    
                    cpu_samples.append(sample)
                
                print(f"📊 Extracted {len(cpu_samples)} CPU samples with {len(canonical_metrics)} canonical metrics")
                if cpu_samples:
                    print(f"🔍 First 3 CPU samples:")
                    for i, sample in enumerate(cpu_samples[:3]):
                        print(f"   Sample {i}: {sample}")                

        # ====================================================================
        # Calculate thermal metrics from CPU samples
        # ====================================================================
        if cpu_samples and len(cpu_samples) > 0:
            temps = [s.get('package_temp') for s in cpu_samples if s.get('package_temp', 0) > 10]
            if temps:
                start_temp_c = temps[0]          # First sample
                max_temp_c = max(temps)           # Maximum during run
                min_temp_c = min(temps)           # Minimum during run
                thermal_delta_c = max_temp_c - start_temp_c
            else:
                start_temp_c = 0
                max_temp_c = 0
                min_temp_c = 0
                thermal_delta_c = 0
        else:
            start_temp_c = 0
            max_temp_c = 0
            min_temp_c = 0
            thermal_delta_c = 0

        # ====================================================================
        # Step 2: Compute Derived Energy (Layer 3) using Baseline (Layer 2)
        # ====================================================================
        derived = self.energy_analyzer.compute(raw_energy, self.baseline)  # DerivedEnergyMeasurement (Layer 3)
        
        # ====================================================================
        # Step 3: Calculate sustainability
        # ====================================================================
        sustainability = self.sustainability.calculate_from_derived(
            derived,
            country_code=country_code,
            query_count=1
        )
        
        # ====================================================================
        # Step 4: Energy per token
        # ====================================================================
        if exec_result.get('tokens', {}).get('total', 0) > 0:
            energy_per_token = derived.workload_energy_j / exec_result['tokens']['total']
        else:
            energy_per_token = 0
        
        # ====================================================================
        # Step 5: CPU metrics from Layer 3
        # ====================================================================
        if hasattr(derived, 'performance_counters') and derived.performance_counters:
            cpu_metrics = {
                'instructions': derived.performance_counters.instructions,
                'cycles': derived.performance_counters.cpu_cycles,
                'ipc': derived.performance_counters.instructions_per_cycle(),
                'cache_misses': derived.performance_counters.cache_misses,
                'context_switches': derived.performance_counters.total_context_switches()
            }
        else:
            cpu_metrics = {
                'instructions': 0,
                'cycles': 0,
                'ipc': 0,
                'cache_misses': 0,
                'context_switches': 0,
                'note': 'Performance counters not available'
            }
        # ====================================================================
        # Capture system state AFTER run
        # ====================================================================
        intr_after = self._read_interrupts()
        temp_max = max(temp_start, self._read_temperature())
        memory_after = self.get_process_memory()
        duration = raw_energy.duration_seconds
        interrupt_rate = self.get_interrupt_rate(intr_before, intr_after, duration) 

        # ====================================================================
        # DEBUG: Check token data before building ml_features
        # ====================================================================
        dprint(f"🔍 DEBUG*** - agentic exec_result keys: {exec_result.keys()}")
        dprint(f"🔍 DEBUG ***- agentic tokens in exec_result: {exec_result.get('tokens', {})}")
        dprint(f"🔍 DEBUG*** - agentic token total: {exec_result.get('tokens', {}).get('total')}")
        dprint(f"🔍 DEBUG*** - agentic token prompt: {exec_result.get('tokens', {}).get('prompt')}")
        dprint(f"🔍 DEBUG*** - agentic token completion: {exec_result.get('tokens', {}).get('completion')}")        
        # ====================================================================  
        # NEW: Capture orchestration events from executor
        # ====================================================================
        orchestration_events = []
        if hasattr(executor, '_events') and executor._events:
            orchestration_events = executor._events.copy()  # Copy to prevent modification
            print(f"🔍 DEBUG - Captured {len(orchestration_events)} orchestration events from executor")
            # Clear events to prevent mixing between runs
            executor._events = []
        else:
            print("🔍 DEBUG - No orchestration events found in executor")        
        
        # ====================================================================
        # Step 6: Return ALL THREE LAYERS with ML features
        # ====================================================================
        result = {
            'experiment_id': exec_result.get('experiment_id'),
            'task_id': task_id,
            'workflow': 'agentic',
            'country_code': country_code,
            'execution': exec_result,
            
            # THREE LAYERS – All available for analysis
            'layer1_raw': raw_energy.to_dict(),                    # Raw hardware readings
            'layer2_baseline': self.baseline.to_dict() if self.baseline else None,  # Idle reference
            'layer3_derived': derived.to_dict(),                   # Corrected metrics
            
            # Backward compatibility
            'raw_energy': raw_energy.to_dict(),
            'derived_energy': derived.to_dict(),
            
            # Other metrics
            'sustainability': sustainability.to_dict() if sustainability else None,
            'network_metrics': network_metrics,
            'energy_per_token': energy_per_token,
            'cpu_metrics': cpu_metrics,
            'orchestration_events': orchestration_events,
            # ====================================================================
            # NEW: ML Features Dictionary (ALL features for training)
            # ====================================================================
            'ml_features': {
                # Same hardware metrics as linear
                'start_time_ns': int(run_start_dt.timestamp() * 1_000_000_000),
                'end_time_ns': int(run_end_dt.timestamp() * 1_000_000_000),
                'start_time_iso': run_start_dt.isoformat(),
                'end_time_iso': run_end_dt.isoformat(),
                'duration_sec': run_duration_sec,
                'duration_ms': run_duration_sec * 1000,                
                'instructions': derived.instructions,
                'cycles': derived.cycles,
                'ipc': derived.ipc,
                'cache_misses': derived.cache_misses,
                'cache_references': derived.cache_references,
                'cache_miss_rate': derived.cache_misses / derived.cache_references if derived.cache_references > 0 else 0,
                'context_switches_voluntary': derived.context_switches_voluntary,
                'context_switches_involuntary': derived.context_switches_involuntary,
                'total_context_switches': derived.total_context_switches,
                'thread_migrations': derived.thread_migrations,
                'run_queue_length': derived.run_queue_length,
                'kernel_time_ms': derived.kernel_time_ms,
                'user_time_ms': derived.user_time_ms,
                'frequency_mhz': derived.frequency_mhz,
                'package_temp_celsius': derived.package_temp_celsius,
                'baseline_temp_celsius': self.baseline.cpu_temperature_c if self.baseline else None,
                
                # C-state metrics
                'c2_time_seconds': derived.c2_time_seconds,
                'c3_time_seconds': derived.c3_time_seconds,
                'c6_time_seconds': derived.c6_time_seconds,
                'c7_time_seconds': derived.c7_time_seconds,
                
                # Ring bus
                'ring_bus_freq_mhz': derived.ring_bus_current_mhz,
                'wakeup_latency_us': derived.wakeup_latency_us,
                
                # Thermal validity
                'thermal_during_experiment': derived.thermal_during_experiment,
                'thermal_now_active': derived.thermal_now_active,
                'thermal_since_boot': derived.thermal_since_boot,
                'experiment_valid': (derived.thermal_during_experiment == 0 and derived.thermal_now_active == 0),
                
                # Token metrics
                'total_tokens': exec_result.get('tokens', {}).get('total', 0),
                'prompt_tokens': exec_result.get('tokens', {}).get('prompt', 0),
                'completion_tokens': exec_result.get('tokens', {}).get('completion', 0),                
                
                # ====================================================================
                # AGENTIC-SPECIFIC FEATURES
                # ====================================================================
                'planning_time_ms': exec_result.get('phase_times', {}).get('planning_ms', 0),
                'execution_time_ms': exec_result.get('phase_times', {}).get('execution_ms', 0),
                'synthesis_time_ms': exec_result.get('phase_times', {}).get('synthesis_ms', 0),
                'phase_planning_ratio': exec_result.get('phase_ratios', {}).get('planning_ratio', 0) if exec_result.get('phase_ratios') else 0,
                'phase_execution_ratio': exec_result.get('phase_ratios', {}).get('execution_ratio', 0) if exec_result.get('phase_ratios') else 0,
                'phase_synthesis_ratio': exec_result.get('phase_ratios', {}).get('synthesis_ratio', 0) if exec_result.get('phase_ratios') else 0,
                'llm_calls': exec_result.get('llm_calls', 1),
                'tool_calls': exec_result.get('tool_count', 0),
                'tools_used': len(exec_result.get('tools_used', [])),
                'steps': exec_result.get('steps', 1),
                'avg_step_time_ms': exec_result.get('avg_step_time_ms', 0),
                'complexity_level': exec_result.get('complexity_level', 1),
                'complexity_score': exec_result.get('complexity_score', {}).get('raw_score', 0),
                
                # Network
                'dns_latency_ms': network_metrics.get('dns_latency_ms', 0),
                'api_latency_ms': exec_result.get('api_latency_ms', 0),
                'compute_time_ms': exec_result.get('compute_time_ms', exec_result.get('total_time_ms', 0)),
                # =============================================================
                # NEW: System State Metrics (ADD THESE)
                # =============================================================
                'governor': governor,
                'baseline_id': self.baseline.baseline_id if self.baseline else None,
                'turbo_enabled': 1 if turbo == 'enabled' else 0,
                'interrupt_rate': interrupt_rate,
                'start_temp_c': start_temp_c,
                'max_temp_c': max_temp_c,
                'min_temp_c': min_temp_c,
                'thermal_delta_c': thermal_delta_c,
                'is_cold_start': 1 if is_cold else 0,
                'background_cpu_percent': background_cpu,
                'process_count': process_count,
                'rss_memory_mb': memory_after.get('rss_mb', 0),
                'vms_memory_mb': memory_after.get('vms_mb', 0),                
                # Metadata
                'model_name': executor.config.get('model_id', 'unknown'),
                'provider': executor.provider,
                'task_id': task_id,
                'country_code': country_code,
                'workflow_type': 'agentic',
                
                # ====================================================================
                # TARGETS
                # ====================================================================
                'energy_j': derived.workload_energy_j,
                'orchestration_tax_j': derived.orchestration_tax_j,
                'carbon_g': sustainability.carbon.grams if sustainability else 0,
                'duration_ms': derived.duration_seconds * 1000,
            },
            'energy_samples': energy_samples,
            'cpu_samples': cpu_samples,
            'interrupt_samples': interrupt_samples,            
            'harness_timestamp': datetime.now().isoformat(),
            'scientific_notes': {
                'measurement_scope': 'client_side_orchestration_only',
                'layers': {
                    'layer1_raw': 'RawEnergyMeasurement (archived)',
                    'layer2_baseline': self.baseline is not None,
                    'layer3_derived': 'DerivedEnergyMeasurement (used for analysis)'
                },
                'includes': ['planning', 'tool_execution', 'synthesis', 'cpu_energy', 'performance_counters'],
                'excludes': ['model_inference_on_cloud'] if is_cloud else [],
                'baseline_corrected': self.baseline is not None
            }
        }
        
        # ====================================================================
        # Add high-frequency samples to result
        # ====================================================================
        if hasattr(self.energy_engine, 'last_samples'):
            result['energy_samples'] = self.energy_engine.last_samples
            dprint(f"📊 Added {len(self.energy_engine.last_samples)} energy samples to result")
        else:
            dprint("⚠️ No last_samples attribute found in energy_engine")

        if hasattr(self.energy_engine, 'last_interrupt_samples'):
            result['interrupt_samples'] = self.energy_engine.last_interrupt_samples
            dprint(f"📊 Added {len(self.energy_engine.last_interrupt_samples)} energy samples to result")
        else:
            dprint("⚠️ No last_samples attribute found in energy_engine")

        # ====================================================================
        # DEBUG - Check orchestration events before returning
        # ====================================================================
        print(f"🔍 DEBUG HARNESS - orchestration_events in result: {'orchestration_events' in result}")
        if 'orchestration_events' in result:
            print(f"🔍 DEBUG HARNESS - Number of events: {len(result['orchestration_events'])}")
            if len(result['orchestration_events']) > 0:
                print(f"🔍 DEBUG HARNESS - First event keys: {result['orchestration_events'][0].keys()}")                

        dprint(f"✅ Harness complete: {derived.workload_energy_j:.4f}J workload energy")
        return result

    def run_comparison(self, linear_executor, agentic_executor, task: str, task_id: str = None, 
                      cool_down: Optional[int] = None, n_repetitions: int = 30, 
                      include_warmup: bool = True, 
                      country_code: str = "US", hardware_info: Optional[Dict] = None, 
                      save_to_db: bool = False) -> Dict[str, Any]:
        """
        Run multiple comparisons with warmup and statistical analysis.
        
        This is the publishable experimental protocol:
        1. Warmup run (discarded) to stabilize system
        2. N repetitions of (linear + cool‑down + agentic)
        3. Statistical analysis of all runs
        
        Returns results with ALL THREE LAYERS for each run.
        """
        dprint(f"\n{'#'*70}")
        dprint(f"📊 COMPARISON EXPERIMENT: Linear vs Agentic")
        dprint(f"   Task: {task[:100]}")
        dprint(f"   Repetitions: {n_repetitions}")
        dprint(f"{'#'*70}")

        # ====================================================================
        # Initialize results collection
        # ====================================================================
        self._collected_samples = {
            'energy_samples': [],           # Flat list (backward compatibility)
            'cpu_samples': [],               # Flat list
            'interrupt_samples': [],          # Flat list
            'energy_samples_by_run': [],      # NEW: list of lists
            'cpu_samples_by_run': [],         # NEW: list of lists
            'interrupt_samples_by_run': []    # NEW: list of lists
        }      
               
        # Determine if cloud model (check provider)
        is_cloud = linear_executor.provider != 'ollama'
        
        # Set cool‑down period
        if cool_down is None:
            cool_down = self.config.get_settings().experiment.cool_down_seconds
        
        # Warmup run (optional, recommended)
        if include_warmup:
            dprint(f"\n🔥 Warmup phase (results discarded)")
            self._warmup_run(linear_executor, task, is_agentic=False)
            time.sleep(cool_down)
            self._warmup_run(agentic_executor, task, is_agentic=True)
            time.sleep(cool_down)
        
        # Storage for all runs
        all_linear = []
        all_agentic = []
        all_taxes = []
        
        for i in range(n_repetitions):
            dprint(f"\n{'─'*50}")
            dprint(f"📋 Repetition {i+1}/{n_repetitions}")
            dprint(f"{'─'*50}")
            
            # Run linear
            linear_result = self.run_linear(linear_executor, task, task_id, is_cloud,country_code=country_code, run_number=i+1)
            all_linear.append(linear_result)
            # Collect samples from linear run
            if 'energy_samples' in linear_result:
                self._collected_samples['energy_samples'].extend(linear_result['energy_samples'])

            if 'cpu_samples' in linear_result:
                self._collected_samples['cpu_samples'].extend(linear_result['cpu_samples'])

            if 'interrupt_samples' in linear_result:
                self._collected_samples['interrupt_samples'].extend(linear_result['interrupt_samples'])
            # ====================================================================
            # Collect orchestration events from agentic run (with protection)
            # ====================================================================
            try:
                if agentic_result and 'orchestration_events' in agentic_result:
                    if 'orchestration_events_by_run' not in self._collected_samples:
                        self._collected_samples['orchestration_events_by_run'] = []
                    self._collected_samples['orchestration_events_by_run'].append(agentic_result['orchestration_events'])
                    dprint(f"   📝 Collected {len(agentic_result['orchestration_events'])} orchestration events")
                else:
                    dprint("   ⚠️ No orchestration events in agentic_result")
            except Exception as e:
                dprint(f"   ⚠️ Error collecting events: {e}")
                # Still add an empty list to maintain alignment
                if 'orchestration_events_by_run' not in self._collected_samples:
                    self._collected_samples['orchestration_events_by_run'] = []
                self._collected_samples['orchestration_events_by_run'].append([])
            
            
            # Cool‑down
            dprint(f"⏳ Cool‑down: {cool_down}s")
            time.sleep(cool_down)
            
            # Run agentic
            agentic_result = self.run_agentic(agentic_executor, task, task_id, is_cloud, country_code=country_code, run_number=i+1 )
            all_agentic.append(agentic_result)
            if 'energy_samples' in agentic_result:
                self._collected_samples['energy_samples'].extend(agentic_result['energy_samples'])

            if 'cpu_samples' in agentic_result:
                self._collected_samples['cpu_samples'].extend(agentic_result['cpu_samples'])

            if 'interrupt_samples' in agentic_result:
                self._collected_samples['interrupt_samples'].extend(agentic_result['interrupt_samples'])


            ##temporary debug
            print(f"🔍 DEBUG: linear_result['layer3_derived'] keys = {linear_result['layer3_derived'].keys()}")
            
            # Compute tax for this pair (using Layer 3 workload energy)
            # DerivedEnergyMeasurement stores values in microjoules with '_uj' suffix
            # Compute tax for this pair (using Layer 3 workload energy)
            linear_layer3 = linear_result['layer3_derived']
            agentic_layer3 = agentic_result['layer3_derived']
            
            # Debug to see actual structure
            print(f"🔍 DEBUG: linear_layer3 keys = {list(linear_layer3.keys())}")
            if 'energy_uj' in linear_layer3:
                print(f"🔍 DEBUG: energy_uj keys = {list(linear_layer3['energy_uj'].keys())}")
            
            # Extract linear workload energy from the nested structure
            if 'energy_uj' in linear_layer3 and 'workload' in linear_layer3['energy_uj']:
                linear_energy = linear_layer3['energy_uj']['workload'] / 1_000_000
            else:
                logger.warning(f"Could not find workload energy. Keys: {list(linear_layer3.keys())}")
                linear_energy = 0
            
            # Extract agentic workload energy
            if 'energy_uj' in agentic_layer3 and 'workload' in agentic_layer3['energy_uj']:
                agentic_energy = agentic_layer3['energy_uj']['workload'] / 1_000_000
            else:
                logger.warning(f"Could not find workload energy. Keys: {list(agentic_layer3.keys())}")
                agentic_energy = 0
            
            tax = agentic_energy / linear_energy if linear_energy > 0 else 0
            all_taxes.append(tax)
            
            # Cool‑down between repetitions (except last)
            if i < n_repetitions - 1:
                time.sleep(cool_down)
        # ====================================================================
        # Calculate heat flux for all runs (using baseline temperature)
        # ====================================================================
        # Get baseline temperature from the baseline measurement
        baseline_temp = None
        if self.baseline and hasattr(self.baseline, 'cpu_temperature_c'):
            baseline_temp = self.baseline.cpu_temperature_c
        
        # Calculate heat flux for each run
        for run in all_linear + all_agentic:
            if 'ml_features' in run:
                current_temp = run['ml_features'].get('package_temp_celsius')
                
                if current_temp and baseline_temp:
                    # Temperature rise from baseline
                    temp_rise = current_temp - baseline_temp
                    duration = run['ml_features'].get('duration_ms', 0) / 1000  # seconds
                    
                    if duration > 0:
                        heat_flux = temp_rise / duration  # °C per second
                        run['ml_features']['heat_flux'] = heat_flux
                        dprint(f"🔥 Heat flux: {heat_flux:.2f}°C/s for run {run['ml_features'].get('run_number', '?')}")   

        # ====================================================================
        # Build grouped samples in correct order (all linear first, then all agentic)
        # ====================================================================
        # Energy samples
        self._collected_samples['energy_samples_by_run'] = []
        for run in all_linear:
            if 'energy_samples' in run:
                self._collected_samples['energy_samples_by_run'].append(run['energy_samples'])
        for run in all_agentic:
            if 'energy_samples' in run:
                self._collected_samples['energy_samples_by_run'].append(run['energy_samples'])
        
        # CPU samples
        self._collected_samples['cpu_samples_by_run'] = []
        for run in all_linear:
            if 'cpu_samples' in run:
                self._collected_samples['cpu_samples_by_run'].append(run['cpu_samples'])
        for run in all_agentic:
            if 'cpu_samples' in run:
                self._collected_samples['cpu_samples_by_run'].append(run['cpu_samples'])
        
        # Interrupt samples
        self._collected_samples['interrupt_samples_by_run'] = []
        for run in all_linear:
            if 'interrupt_samples' in run:
                self._collected_samples['interrupt_samples_by_run'].append(run['interrupt_samples'])
        for run in all_agentic:
            if 'interrupt_samples' in run:
                self._collected_samples['interrupt_samples_by_run'].append(run['interrupt_samples'])
        
        dprint(f"📊 Grouped samples: energy={len(self._collected_samples['energy_samples_by_run'])}, "
               f"cpu={len(self._collected_samples['cpu_samples_by_run'])}, "
               f"interrupt={len(self._collected_samples['interrupt_samples_by_run'])}")

        # ====================================================================
        # Statistical analysis
        # ====================================================================
        def calc_stats(data):
            arr = np.array(data)
            n = len(arr)
            
            if n >= 2:
                mean = np.mean(arr)
                std = np.std(arr, ddof=1)
                # 95% confidence interval (requires n-1 degrees of freedom)
                ci = scipy_stats.t.interval(0.95, n-1, loc=mean, scale=std/np.sqrt(n))
                ci_lower, ci_upper = ci[0], ci[1]
            elif n == 1:
                mean = arr[0]
                std = float('nan')
                ci_lower = float('nan')
                ci_upper = float('nan')
            else:  # n == 0
                mean = 0
                std = float('nan')
                ci_lower = float('nan')
                ci_upper = float('nan')
            
            return {
                'mean': mean,
                'std': std,
                'ci_lower': ci_lower,
                'ci_upper': ci_upper,
                'min': np.min(arr) if n > 0 else 0,
                'max': np.max(arr) if n > 0 else 0,
                'n': n
            }
        print(f"🔍 Agentic interrupt samples count: {len(agentic_result.get('interrupt_samples', []))}")

        # Extract energy values from Layer 3 (DerivedEnergyMeasurement)
        
        linear_energies = [r['layer3_derived']['energy_uj']['workload'] / 1_000_000 for r in all_linear]
        
        agentic_energies = [r['layer3_derived']['energy_uj']['workload'] / 1_000_000 for r in all_agentic]

        linear_times = [r['execution']['execution_time_ms'] for r in all_linear]
        agentic_times = [r['execution']['total_time_ms'] for r in all_agentic]
        
        # Energy per token from Layer 3
        linear_ept = [r['energy_per_token'] for r in all_linear]
        agentic_ept = [r['energy_per_token'] for r in all_agentic]

        # ====================================================================
        # Collect raw events from agentic runs (store temporarily)
        # ====================================================================
        raw_agentic_events = []
        for i, result in enumerate(all_agentic):
            if result and 'orchestration_events' in result:
                raw_agentic_events.append(result['orchestration_events'])
                dprint(f"🔍 DEBUG - Collected {len(result['orchestration_events'])} orchestration events from agentic run {i+1}")
            else:
                raw_agentic_events.append([])
                dprint(f"🔍 DEBUG - No orchestration events in agentic run {i+1}")

        # ====================================================================
        # Build grouped samples after the loop
        # ====================================================================
        # Build energy_samples_by_run in order: [L1, L2, L3, A1, A2, A3]
        self._collected_samples['energy_samples_by_run'] = [
            run['energy_samples'] for run in all_linear if 'energy_samples' in run
        ] + [
            run['energy_samples'] for run in all_agentic if 'energy_samples' in run
        ]
        
        self._collected_samples['cpu_samples_by_run'] = [
            run['cpu_samples'] for run in all_linear if 'cpu_samples' in run
        ] + [
            run['cpu_samples'] for run in all_agentic if 'cpu_samples' in run
        ]
        
        self._collected_samples['interrupt_samples_by_run'] = [
            run['interrupt_samples'] for run in all_linear if 'interrupt_samples' in run
        ] + [
            run['interrupt_samples'] for run in all_agentic if 'interrupt_samples' in run
        ]
        
        # ====================================================================
        # Build orchestration events by run (AGENTIC ONLY)
        # ====================================================================
        # For orchestration events, we only have them for agentic runs
        # But we need to place them in the same order as runs:
        # [L1, L2, L3, A1, A2, A3] → So first n_repetitions entries are empty lists
        orchestration_events_by_run = []
        
        # Add empty lists for linear runs
        for _ in range(n_repetitions):
            orchestration_events_by_run.append([])
        
        # Add events for agentic runs (using raw_agentic_events collected above)
        orchestration_events_by_run.extend(raw_agentic_events)
        dprint(f"📊 Added {len(raw_agentic_events)} agentic event groups")


        results = {
            'task': task,
            'task_id': task_id,
            'n_repetitions': n_repetitions,
            'cool_down_seconds': cool_down,
            'is_cloud': is_cloud,
            'statistics': {
                'linear_energy_j': calc_stats(linear_energies),
                'agentic_energy_j': calc_stats(agentic_energies),
                'linear_time_ms': calc_stats(linear_times),
                'agentic_time_ms': calc_stats(agentic_times),
                'orchestration_tax': calc_stats(all_taxes),
                'linear_energy_per_token': calc_stats(linear_ept),
                'agentic_energy_per_token': calc_stats(agentic_ept)
            },
            'all_runs': {
                'linear': [r['experiment_id'] for r in all_linear],
                'agentic': [r['experiment_id'] for r in all_agentic],
                'taxes': all_taxes
            },
            'scientific_notes': {
                'measurement_scope': 'client_side_orchestration_only',
                'layers': {
                    'layer1_raw': 'RawEnergyMeasurement (archived per run)',
                    'layer2_baseline': self.baseline is not None,
                    'layer3_derived': 'DerivedEnergyMeasurement (used for analysis)'
                },
                'includes': ['cpu_energy', 'memory_energy', 'local_computation', 'orchestration_overhead'],
                'excludes': ['model_inference_on_cloud'] if is_cloud else [],
                'baseline_corrected': self.baseline is not None,
                'warmup_performed': include_warmup,
                'statistical_method': "Student's t-test, 95% CI"
            }
        }

        # ====================================================================
        # Add collected samples to results
        # ====================================================================
        if hasattr(self, '_collected_samples'):
            results['energy_samples'] = self._collected_samples.get('energy_samples', [])
            results['cpu_samples'] = self._collected_samples.get('cpu_samples', [])
            results['interrupt_samples'] = self._collected_samples.get('interrupt_samples', [])
            # NEW: Add per-run samples
            results['energy_samples_by_run'] = self._collected_samples.get('energy_samples_by_run', [])
            results['cpu_samples_by_run'] = self._collected_samples.get('cpu_samples_by_run', [])
            results['interrupt_samples_by_run'] = self._collected_samples.get('interrupt_samples_by_run', [])
            results['orchestration_events_by_run'] = orchestration_events_by_run

            dprint(f"📊 Added samples to final results: energy={len(results['energy_samples'])}, "
                   f"cpu={len(results['cpu_samples'])}, interrupt={len(results['interrupt_samples'])}")         
        
        # Print summary
        dprint(f"\n{'#'*70}")
        dprint(f"📊 EXPERIMENT SUMMARY")
        dprint(f"{'#'*70}")
        dprint(f"   Linear energy:   {results['statistics']['linear_energy_j']['mean']:.4f} ± {results['statistics']['linear_energy_j']['std']:.4f} J")
        dprint(f"   Agentic energy:  {results['statistics']['agentic_energy_j']['mean']:.4f} ± {results['statistics']['agentic_energy_j']['std']:.4f} J")
        dprint(f"   Orchestration tax: {results['statistics']['orchestration_tax']['mean']:.2f}x "
              f"[95% CI: {results['statistics']['orchestration_tax']['ci_lower']:.2f}, {results['statistics']['orchestration_tax']['ci_upper']:.2f}]")
        dprint(f"   Energy per token: Linear={results['statistics']['linear_energy_per_token']['mean']:.6f} J/tok, Agentic={results['statistics']['agentic_energy_per_token']['mean']:.6f} J/tok")
        dprint(f"{'#'*70}")
            # ====================================================================
        # Display ALL hardware parameters from Layer 1 (25 requirements)
        # ====================================================================
        print("\n" + "="*70)
        print("🔧 HARDWARE PARAMETERS DEEP DIVE (Layer 1 - All 25 Requirements)")
        print("="*70)

        
        def display_hardware(runs, label):
            """Display ALL hardware parameters from DerivedEnergyMeasurement (Layer 3)."""
            for idx, run in enumerate(runs):
                print(f"\n📊 {label} Run {idx+1}:")
                
                # Use layer3_derived – this contains all computed metrics
                derived = run['layer3_derived']
                
                # --------------------------------------------------------------------
                # Req 1.1, 1.3: RAPL Energy Domains & Uncore Waste
                # --------------------------------------------------------------------
                energy_uj = derived.get('energy_uj', {})
                print("   ⚡ RAPL Energy (Req 1.1):")
                print(f"      Package: {energy_uj.get('package', 0)/1e6:.3f} J")
                print(f"      Core:    {energy_uj.get('core', 0)/1e6:.3f} J")
                
                uncore = energy_uj.get('uncore', 0)
                if uncore > 0:
                    print(f"      Uncore:  {uncore/1e6:.3f} J (includes GPU if no separate GPU domain)")
                
                dram = energy_uj.get('dram')
                if dram:
                    print(f"      DRAM:    {dram/1e6:.3f} J")
                
                # --------------------------------------------------------------------
                # Req 1.5, 1.6, 1.10, 1.12, 1.43: Performance Counters & Scheduler
                # --------------------------------------------------------------------
                perf = derived.get('performance', {})
                print("   📈 Performance Counters:")
                print(f"      Instructions:      {perf.get('instructions', 0):,}")
                print(f"      Cycles:            {perf.get('cycles', 0):,}")
                print(f"      IPC:               {perf.get('ipc', 0):.2f}")
                print(f"      Cache References:  {perf.get('cache_references', 0):,}")
                print(f"      Cache Misses:      {perf.get('cache_misses', 0):,}")
                cache_refs = perf.get('cache_references', 1)
                miss_rate = (perf.get('cache_misses', 0) / cache_refs) if cache_refs > 0 else 0
                print(f"      Cache Miss Rate:   {miss_rate:.2%}")
                print(f"      Page Faults:       {perf.get('page_faults', 0):,} "
                      f"(major: {perf.get('major_page_faults', 0)}, "
                      f"minor: {perf.get('minor_page_faults', 0)})")
                
                scheduler = derived.get('scheduler', {})
                #print(f"      Voluntary Ctx Sw:  {scheduler.get('context_switches_voluntary', 0):,}")
                #print(f"      Involuntary Ctx Sw:{scheduler.get('context_switches_involuntary', 0):,}")
                #print(f"      Thread Migrations: {scheduler.get('thread_migrations', 0):,}")
                
                sched = derived.get('scheduler', {})
                print(f"🔍 DEBUG scheduler keys: {list(sched.keys())}")
                power = derived.get('power_states', {})
                print(f"🔍 DEBUG power_states keys: {list(power.keys())}")
                print(f"🔍 DEBUG frequency_mhz: {power.get('frequency_mhz', 'MISSING')}")

                # --------------------------------------------------------------------
                # Req 1.9: Thermal + Derived Thermal Metrics
                # --------------------------------------------------------------------
                thermal = derived.get('thermal', {})
                print("   🌡️ Thermal (Req 1.9):")
                
                
                pkg_temp = thermal.get('package_temp_celsius')
                

                if pkg_temp and pkg_temp > -100:
                    print(f"      Package Temp: {pkg_temp:.1f}°C")
                else:
                    print(f"      Package Temp: N/A")
                
                core_temps = thermal.get('core_temps_celsius', [])
                valid_temps = [t for t in core_temps if t > 10]
                if valid_temps:
                    temps = ', '.join([f"{t:.1f}°C" for t in valid_temps])
                    print(f"      Core Temps:   [{temps}]")
                # ====================================================================
                # NEW: Display Heat Flux if available
                # ====================================================================
                if 'ml_features' in run:
                    heat_flux = run['ml_features'].get('heat_flux')
                    if heat_flux is not None:
                        # Color code based on severity
                        if heat_flux > 3.0:
                            flux_indicator = "🔴 HIGH"
                        elif heat_flux > 1.5:
                            flux_indicator = "🟡 MODERATE"
                        else:
                            flux_indicator = "🟢 LOW"
                        
                        print(f"      Heat Flux: {heat_flux:.2f}°C/s {flux_indicator}")
                        
                        # Add interpretation
                        if heat_flux > 3.0:
                            print(f"         ⚠️  Rapid heating - thermal event risk")
                        elif heat_flux < 0:
                            print(f"         ❄️  System cooling down")        
                # ====================================================================
                # NEW: Display derived thermal metrics from the 'thermal' section
                # ====================================================================
                # Check if we have the new thermal section in derived
                exp_start = derived.get('exp_start_time', 'N/A')
                exp_end = derived.get('exp_end_time', 'N/A')                
                during = derived.get('thermal_during_experiment', 0)
                now_active = derived.get('thermal_now_active', 0)
                since_boot = derived.get('thermal_since_boot', 0)
                # Experiment validity: No throttling during experiment AND not throttling now
                experiment_valid = (during == 0 and now_active == 0)
                    
                print(f"\n   📋 Experiment Timeline:")
                print(f"      Start: {exp_start}")
                print(f"      End:   {exp_end}")
                print(f"\n   ✅ Thermal Summary:")
                print(f"      Since Boot: {'YES' if since_boot else 'NO'}")
                print(f"      During Experiment: {'YES' if during else 'NO'}")
                print(f"      Active at End: {'YES' if now_active else 'NO'}")
                print(f"\n   🔬 Experiment Valid: {'YES' if experiment_valid else 'NO'}")
                # ====================================================================
                # NEW: System State Metrics (M3-1 through M3-6)
                # ====================================================================
                if 'ml_features' in run:
                    ml = run['ml_features']
                    print("\n   ⚙️ System State:")
                    
                    # M3-1: Governor/Turbo
                    governor = ml.get('governor', 'unknown')
                    turbo = ml.get('turbo_enabled', 0)
                    turbo_status = "ENABLED" if turbo else "DISABLED"
                    print(f"      CPU Governor: {governor}")
                    print(f"      Turbo Boost: {turbo_status}")
                    
                    # M3-2: Interrupt Rate
                    intr_rate = ml.get('interrupt_rate', 0)
                    if intr_rate > 0:
                        # Color code based on interrupt rate
                        if intr_rate > 2000:
                            intr_indicator = "🔴 HIGH"
                        elif intr_rate > 1000:
                            intr_indicator = "🟡 MODERATE"
                        else:
                            intr_indicator = "🟢 LOW"
                        print(f"      Interrupt Rate: {intr_rate:.0f}/sec {intr_indicator}")
                    
                    # M3-3: Temperature Tracking
                    start_temp = ml.get('start_temp_c', 0)
                    max_temp = ml.get('max_temp_c', 0)
                    if start_temp > 0 and max_temp > 0:
                        temp_rise = max_temp - start_temp
                        print(f"      Temperature: {start_temp:.1f}°C → {max_temp:.1f}°C (Δ{temp_rise:+.1f}°C)")
                    
                    # M3-4: Cold Start Flag
                    cold_start = ml.get('is_cold_start', 0)
                    if cold_start:
                        print(f"      Cold Start: YES (first run)")
                    
                    # M3-5: Background Noise
                    bg_cpu = ml.get('background_cpu_percent', 0)
                    proc_count = ml.get('process_count', 0)
                    if bg_cpu > 0 or proc_count > 0:
                        print(f"      Background CPU: {bg_cpu:.1f}%")
                        print(f"      Running Processes: {proc_count}")
                    
                    # M3-6: Memory Metrics
                    rss = ml.get('rss_memory_mb', 0)
                    vms = ml.get('vms_memory_mb', 0)
                    if rss > 0 or vms > 0:
                        print(f"      Process Memory:")
                        print(f"         RSS: {rss:.1f} MB")
                        print(f"         VMS: {vms:.1f} MB")
                    
                    # Heat Flux (already in your code)
                    heat_flux = ml.get('heat_flux')
                    if heat_flux is not None:
                        if heat_flux > 3.0:
                            flux_indicator = "🔴 HIGH"
                        elif heat_flux > 1.5:
                            flux_indicator = "🟡 MODERATE"
                        else:
                            flux_indicator = "🟢 LOW"
                        print(f"      Heat Flux: {heat_flux:.2f}°C/s {flux_indicator}")                    
              
                # --------------------------------------------------------------------
                # Req 1.7, 1.41, 1.8, 1.4: Power States (C-states, frequencies, GPU)
                # --------------------------------------------------------------------
                power = derived.get('power_states', {})
                print("   💤 Power States (Req 1.7, 1.41, 1.8, 1.4):")
                cstates = power.get('c_state_residencies', {})
                if cstates:
                    # Show only C‑states with positive residency
                    states = [f"{k}: {v:.1f}%" for k, v in cstates.items() if v > 0]
                    if states:
                        print(f"      C-state residency (per-core avg): {', '.join(states)}")
                        print(f"      (Note: Values can sum to >100% as each core reports independently)")
                print(f"      CPU Frequency:  {power.get('frequency_mhz', 0):.0f} MHz")
                gpu_freq = power.get('gpu_frequency_mhz', 0)
                if gpu_freq > 0:
                    print(f"      GPU Frequency:  {gpu_freq:.0f} MHz")
                gpu_rc6 = power.get('gpu_rc6_percent', 0)
                if gpu_rc6 > 0:
                    print(f"      GPU RC6:        {gpu_rc6:.1f}%")
                
                # --------------------------------------------------------------------
                # Req 1.23, 1.36: Scheduler Metrics (additional)
                # --------------------------------------------------------------------
                scheduler = derived.get('scheduler', {})
                print("   🔄 Scheduler Metrics:")
                print(f"      Voluntary Ctx Sw:  {scheduler.get('context_switches_voluntary', 0):,}")
                print(f"      Involuntary Ctx Sw: {scheduler.get('context_switches_involuntary', 0):,}")
                print(f"      Thread Migrations: {scheduler.get('thread_migrations', 0):,}")
                print(f"      Run Queue Length: {scheduler.get('run_queue_length', 0):.2f}")
                print(f"      Kernel Time:      {scheduler.get('kernel_time_ms', 0):.2f} ms")
                print(f"      User Time:        {scheduler.get('user_time_ms', 0):.2f} ms")
                # ====================================================================
                # DEBUG: See what's in derived
                # ====================================================================
               # print("\n   🔍 DEBUG: derived keys =", list(derived.keys()))
                if 'msr' in derived:
                    #print("   🔍 DEBUG: msr keys =", list(derived['msr'].keys()))
                    if derived['msr'] and isinstance(derived['msr'], dict):
                        msr_preview = str(derived['msr'])[:200]
                        print(f"   🔍 DEBUG: msr content = {msr_preview}...")


                # ====================================================================
                # DEBUG: Print raw msr_data structure
                # ====================================================================
                msr_data = derived.get('msr', {})
                #if msr_data:
                    #dprint("\n 🔍 DEBUG: msr_data top-level keys =", list(msr_data.keys()))
                    #dprint("   🔍 DEBUG: wakeup_latency_us =", msr_data.get('wakeup_latency_us'))
                    #dprint("   🔍 DEBUG: thermal_throttle =", msr_data.get('thermal_throttle'))
                    #dprint("   🔍 DEBUG: baseline keys =", list(msr_data.get('baseline', {}).keys()))
                    #dprint("   🔍 DEBUG: dynamic keys =", list(msr_data.get('dynamic', {}).keys()))
                # ====================================================================
                # MSR Metrics Display (Fixed for actual structure)
                # ====================================================================
                msr_data = derived.get('msr', {})
                if msr_data:
                    print("\n   🔧 MSR Metrics:")
                    
                    # Ring bus frequency
                    ring_bus = msr_data.get('ring_bus', {})
                    if ring_bus and ring_bus.get('current_mhz'):
                        print(f"      Ring Bus Frequency: {ring_bus['current_mhz']:.1f} MHz")
                    
                    # Wake-up latency - directly from top level
                    wake_lat = msr_data.get('wakeup_latency_us')
                    if wake_lat:
                        print(f"      Wake-up Latency: {wake_lat:.2f} µs")
                    
                    # Thermal throttle - directly from top level
                    throttle = msr_data.get('thermal_throttle')
                    if throttle is not None:
                        status = "DETECTED" if throttle else "NOT DETECTED"
                        print(f"      Thermal Throttle Flag: {throttle} ({status})")

                    
                    # C-state times - from c_states
                    c_states = msr_data.get('c_states', {})
                    if c_states:
                        print("      C-State Times (since boot):")
                        for state, state_data in c_states.items():
                            seconds = state_data.get('seconds', 0)
                            if seconds > 0:
                                if seconds < 60:
                                    time_str = f"{seconds:.2f} seconds"
                                elif seconds < 3600:
                                    time_str = f"{seconds/60:.2f} minutes"
                                else:
                                    time_str = f"{seconds/3600:.2f} hours"
                                print(f"         {state.upper()}: {time_str}")
                    
                    # TSC frequency for reference
                    tsc_freq = msr_data.get('tsc_frequency_hz', 0)
                    if tsc_freq:
                        print(f"      TSC Frequency: {tsc_freq/1e6:.0f} MHz")

       
        #display_hardware(all_linear, "LINEAR")
        #display_hardware(all_agentic, "AGENTIC") 
        for i in range(len(all_linear)):
            display_hardware([all_linear[i]], f"LINEAR Run {i+1}")
            if i < len(all_agentic):
                display_hardware([all_agentic[i]], f"AGENTIC Run {i+1}")


        # ====================================================================
        # IPC Efficiency Analysis
        # ====================================================================
        if len(all_linear) > 0 and len(all_agentic) > 0:
            # Get average IPC from each run
            linear_ipcs = []
            agentic_ipcs = []
            
            for run in all_linear:
                perf = run['layer3_derived'].get('performance', {})
                ipc = perf.get('ipc', 0)
                if ipc > 0:
                    linear_ipcs.append(ipc)
            
            for run in all_agentic:
                perf = run['layer3_derived'].get('performance', {})
                ipc = perf.get('ipc', 0)
                if ipc > 0:
                    agentic_ipcs.append(ipc)
            
            if linear_ipcs and agentic_ipcs:
                avg_linear_ipc = sum(linear_ipcs) / len(linear_ipcs)
                avg_agentic_ipc = sum(agentic_ipcs) / len(agentic_ipcs)
                ipc_ratio = avg_agentic_ipc / avg_linear_ipc if avg_linear_ipc > 0 else 0
                
                print(f"\n{'='*70}")
                print("📊 IPC EFFICIENCY ANALYSIS")
                print("="*70)
                print(f"   Linear IPC:  {avg_linear_ipc:.2f}")
                print(f"   Agentic IPC: {avg_agentic_ipc:.2f}")
                print(f"   Efficiency Ratio: {ipc_ratio:.2f}x")
                
                if ipc_ratio > 1:
                    print(f"   → Agentic workflow keeps CPU {((ipc_ratio-1)*100):.1f}% busier")
                    print(f"   → Higher instruction density during orchestration")
                elif ipc_ratio < 1:
                    print(f"   → Agentic workflow is {((1-ipc_ratio)*100):.1f}% less CPU efficient")
                    print(f"   → More pipeline stalls or cache misses")
                else:
                    print(f"   → No IPC difference between workflows")
                
                print("="*70)                
 

        # ====================================================================
        # Helper function to calculate sustainability stats (ENHANCED with energy)
        # ====================================================================
        def calc_sustainability_stats(runs, label):
            """Calculate and display sustainability stats with energy for consistency check."""
            if not runs or not runs[0].get('sustainability'):
                return None
            
            carbon_total = 0
            water_total = 0
            methane_total = 0
            energy_total = 0  # ← NEW
            count = 0
            
            for run in runs:
                sus = run.get('sustainability', {})
                if sus:
                    if 'carbon' in sus:
                        carbon_total += sus['carbon'].get('grams', 0)
                    if 'water' in sus:
                        water_total += sus['water'].get('milliliters', 0)
                    if 'methane' in sus:
                        methane_total += sus['methane'].get('grams', 0)
                    
                    # ← NEW: Get energy from layer3_derived
                    derived = run.get('layer3_derived', {})
                    energy_uj = derived.get('energy_uj', {})
                    energy_total += energy_uj.get('workload', 0) / 1_000_000
                    
                    count += 1
            
            if count > 0:
                carbon_mean = carbon_total / count
                energy_mean = energy_total / count
                
                print(f"\n   📊 {label} Workflow ({count} runs):")
                print(f"      Grid region: {runs[0].get('country_code', 'US')}")
                print(f"      Energy:  {energy_mean:.6f} J")  # ← NEW
                print(f"      Carbon:  {carbon_mean:.6f} g CO₂e")
                print(f"      Water:   {water_total/count:.6f} ml")
                print(f"      Methane: {methane_total/count:.6f} g CH₄")
            
            return {
                'energy': energy_total / count if count > 0 else 0,  # ← NEW
                'carbon': carbon_total / count if count > 0 else 0,
                'water': water_total / count if count > 0 else 0,
                'methane': methane_total / count if count > 0 else 0,
                'count': count
            }                             
        # ====================================================================
        # SECTION 1: GRID FACTORS & SOURCES (Shown once)
        # ====================================================================
        print("\n" + "="*70)
        print("🌍 SUSTAINABILITY IMPACT")
        print("="*70)
        
        if all_linear and all_linear[0].get('sustainability'):
            sus = all_linear[0]['sustainability']
            country = all_linear[0].get('country_code', 'US')
            print(f"\n   📍 Grid Region: {country}")
            print(f"   " + "-"*50)
            
            if sus and 'carbon' in sus:
                c = sus['carbon']
                print(f"   Carbon Intensity: {c.get('source', 'Unknown')}")
                print(f"      Factor: {c.get('grams_per_kwh', 0):.1f} g/kWh")
                print(f"      Uncertainty: ±{c.get('uncertainty_percent', 0)}% [Req 2.16]")
            
            if sus and 'water' in sus:
                w = sus['water']
                print(f"\n   Water Intensity: {w.get('source', 'Unknown')}")
            
            if sus and 'methane' in sus:
                m = sus['methane']
                print(f"\n   Methane Leakage: {m.get('source', 'Unknown')}")

        # ====================================================================
        # SECTION 2: PER-WORKFLOW METRICS (Clean table format)
        # ====================================================================
        linear_stats = calc_sustainability_stats(all_linear, "LINEAR")
        agentic_stats = calc_sustainability_stats(all_agentic, "AGENTIC")
        
        if linear_stats and agentic_stats:
            print(f"\n   📊 WORKFLOW COMPARISON")
            print(f"   " + "="*50)
            print(f"   {'Metric':<20} {'LINEAR':>15} {'AGENTIC':>15} {'RATIO':>10}")
            print(f"   " + "-"*60)
            
            # Energy
            energy_ratio = (agentic_stats['energy'] / linear_stats['energy']) if linear_stats['energy'] else float('nan')   # <-- FIX
            print(f"   {'Energy (J)':<20} {linear_stats['energy']:>15.6f} {agentic_stats['energy']:>15.6f} "
                  f"{energy_ratio:>10.2f}x")
            
            # Carbon
            carbon_ratio = (agentic_stats['carbon'] / linear_stats['carbon']) if linear_stats['carbon'] else float('nan')   # <-- FIX
            print(f"   {'Carbon (mg)':<20} {linear_stats['carbon']*1000:>15.6f} {agentic_stats['carbon']*1000:>15.6f} "
                  f"{carbon_ratio:>10.2f}x")
            
            # Water
            water_ratio = (agentic_stats['water'] / linear_stats['water']) if linear_stats['water'] else float('nan')       # <-- FIX
            print(f"   {'Water (µl)':<20} {linear_stats['water']*1000:>15.6f} {agentic_stats['water']*1000:>15.6f} "
                  f"{water_ratio:>10.2f}x")
            
            # Methane
            methane_ratio = (agentic_stats['methane'] / linear_stats['methane']) if linear_stats['methane'] else float('nan') # <-- FIX
            print(f"   {'Methane (mg)':<20} {linear_stats['methane']*1000:>15.6f} {agentic_stats['methane']*1000:>15.6f} "
                  f"{methane_ratio:>10.2f}x")
            dprint(f"DEBUG methane values: linear={linear_stats['methane']}, agentic={agentic_stats['methane']}")
            
            print(f"   " + "-"*60)

        # ====================================================================
        # SECTION 3: DERIVED METRICS (Tax, Reasoning, Scarcity)
        # ====================================================================
        if linear_stats and agentic_stats:
            wait_tax_energy = agentic_stats['energy'] - linear_stats['energy']
            reasoning_ratio = (linear_stats['energy'] / agentic_stats['energy']) * 100 if agentic_stats['energy'] > 0 else 0
            
            print(f"\n   📈 DERIVED METRICS")
            print(f"   " + "-"*50)
            print(f"   [2.8] Wait-Tax Per Query:")
            print(f"         Energy: {wait_tax_energy:.4f} J")
            print(f"         Carbon: {(agentic_stats['carbon'] - linear_stats['carbon'])*1000:.3f} mg")
            
            print(f"\n   [2.11] Reasoning-to-Waste:")
            print(f"         Reasoning: {reasoning_ratio:.1f}%")
            print(f"         Waste:     {100-reasoning_ratio:.1f}%")
            
            # Energy Scarcity Index (Req 2.13)
            print(f"\n   [2.13] Energy Scarcity Index:")
            print(f"         Linear:  {linear_stats['energy']/3.6e6/10:.8f}")
            print(f"         Agentic: {agentic_stats['energy']/3.6e6/10:.8f}")

        # ====================================================================
        # SECTION 4: MODULE 3 EXECUTION METRICS (Keep this)
        # ====================================================================
        if all_agentic:
            print("\n   🤖 EXECUTION METRICS [Module 3]")
            print("   " + "-"*50)
            
            total_llm = 0
            total_tools = 0
            total_steps = 0
            total_plan = 0
            total_exec = 0
            total_syn = 0
            complexity_sum = 0
            
            for run in all_agentic:
                exec_data = run.get('execution', {})
                total_llm += exec_data.get('llm_calls', 0)
                total_tools += exec_data.get('tool_count', 0)
                total_steps += exec_data.get('steps', 0)
                
                phase_times = exec_data.get('phase_times', {})
                total_plan += phase_times.get('planning_ms', 0)
                total_exec += phase_times.get('execution_ms', 0)
                total_syn += phase_times.get('synthesis_ms', 0)
                
                complexity = exec_data.get('complexity_score', {})
                if isinstance(complexity, dict):
                    complexity_sum += complexity.get('raw_score', 0)
            
            count = len(all_agentic)
            if count > 0:
                print(f"\n      [3.2] Complexity Level: {agentic_stats.get('complexity_level', 1)}")
                print(f"      [3.2] Complexity Score: {complexity_sum/count:.3f}")
                print(f"\n      [3.6] Phase Breakdown:")
                print(f"         Planning:  {total_plan/count:6.1f} ms")
                print(f"         Execution: {total_exec/count:6.1f} ms")
                print(f"         Synthesis: {total_syn/count:6.1f} ms")
                print(f"         {'─'*30}")
                print(f"         TOTAL:     {(total_plan+total_exec+total_syn)/count:6.1f} ms")
                
                print(f"\n      Workload Characteristics:")
                print(f"         LLM Calls:  {total_llm/count:.1f}")
                print(f"         Tool Calls: {total_tools/count:.1f}")
                print(f"         Steps:      {total_steps/count:.1f}")
        
        print("="*70)

            


        # ====================================================================
        # Create ML-ready dataset from all runs
        # ====================================================================
        ml_dataset = {
            'linear_runs': [],
            'agentic_runs': [],
            'all_runs': []
        }
        
        for i, run in enumerate(all_linear):
            if 'ml_features' in run:
                run['ml_features']['run_number'] = i + 1
                run['ml_features']['experiment_id'] = run.get('experiment_id')
                ml_dataset['linear_runs'].append(run['ml_features'])
                ml_dataset['all_runs'].append(run['ml_features'])
        
        for i, run in enumerate(all_agentic):
            if 'ml_features' in run:
                run['ml_features']['run_number'] = i + 1
                run['ml_features']['experiment_id'] = run.get('experiment_id')
                ml_dataset['agentic_runs'].append(run['ml_features'])
                ml_dataset['all_runs'].append(run['ml_features'])



        # Add to results
        results['ml_dataset'] = ml_dataset
        
        # Optional: Save to CSV for immediate use
        try:
            import pandas as pd
            df = pd.DataFrame(ml_dataset['all_runs'])
            csv_path = f"data/ml_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(csv_path, index=False)
            print(f"\n💾 ML dataset saved to: {csv_path} ({len(df)} runs, {len(df.columns)} features)")
        except ImportError:
            print("\n⚠️ pandas not installed. Run: pip install pandas")
        except Exception as e:
            print(f"\n⚠️ Could not save CSV: {e}")        

        if save_to_db:
            experiment_meta = {
                'name': f"{task_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                'description': f"Task: {task[:100]}",
                'workflow_type': 'comparison',
                'model_name': linear_executor.config.get('model_id', 'unknown'),
                'provider': linear_executor.provider,
                'task_name': task_id or 'custom',
                'country_code': country_code
            }
            self.save_to_database(results, experiment_meta, hardware_info)


        return results
        
    
    