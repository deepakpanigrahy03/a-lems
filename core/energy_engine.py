#!/usr/bin/env python3
"""
================================================================================
ENERGY ENGINE – Main Orchestrator for Module 1
================================================================================

This module ties together all hardware readers and provides a clean interface
for measuring energy consumption during AI workloads. It is designed to be used
as a context manager or directly via start/stop methods.

The EnergyEngine handles:
- Initializing all readers (RAPL, perf, turbostat, sensors, MSR)
- Core pinning for consistent measurements (Req 1.15)
- Idle baseline measurement using research‑grade utility (Req 1.45)
- High‑frequency sampling (Req 1.46)
- Coordinated start/stop of all readers
- Assembling a complete RawEnergyMeasurement object
- Validation of measurement quality

IMPORTANT: This engine returns RAW measurements only.
Baseline correction happens in the analysis layer (EnergyAnalyzer).

All paths and settings come from Module 0’s configuration (hw_config.json,
app_settings.yaml). No hardcoding.

Author: Deepak Panigrahy
================================================================================
"""

import json
import logging
import os
import platform
import queue
import statistics
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests

# ====================================================================
# ADD THIS IMPORT
# ====================================================================
from core.models.baseline_measurement import BaselineMeasurement
# Import the new raw measurement model (Layer 1)
from core.models.raw_energy_measurement import RawEnergyMeasurement
from core.utils.core_pinner import CorePinner
# Import utilities
from core.utils.debug import dprint, init_debug_from_env
from core.utils.validators import MeasurementValidator

# ====================================================================


# ============================================================================
# Fix Python path – ensure core modules are importable
# ============================================================================
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import the new raw measurement model (Layer 1)
from core.models.raw_energy_measurement import RawEnergyMeasurement
from core.readers.msr_reader import MSRReader
from core.readers.perf_reader import PerfReader
# ============================================================================
# Import all readers and models
# ============================================================================
from core.readers.rapl_reader import RAPLReader
from core.readers.scheduler_monitor import SchedulerMonitor
from core.readers.sensor_reader import SensorReader
from core.readers.turbostat_reader import TurbostatReader
from core.utils.core_pinner import CorePinner
# Import utilities
from core.utils.debug import dprint, init_debug_from_env
# Import baseline utilities (will be used by analysis layer, not here)
from core.utils.idle_baseline import measure_idle_baseline as measure_baseline
from core.utils.validators import MeasurementValidator

# Initialize debug system
init_debug_from_env()

logger = logging.getLogger(__name__)


class EnergyEngine:
    """
    Main energy measurement engine orchestrating all readers.

    This is the primary interface for Modules 2 and 3. It provides:
    1. Context manager for easy measurement (`with engine as m:`)
    2. High‑frequency sampling (Req 1.46)
    3. Idle baseline measurement (Req 1.45) – stored separately, NOT applied
    4. Core pinning (Req 1.15)
    5. Measurement validation
    6. Multiple‑run statistics

    All readers are initialized from the configuration passed in.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the energy engine with configuration from Module 0.

        Args:
            config: The complete configuration dictionary, usually loaded from
                    `hw_config.json` and merged with `app_settings.yaml`.
                    Expected to contain sections for each reader and settings.

        The constructor instantiates all readers and sets up logging.
        """
        dprint("Initializing EnergyEngine")
        dprint("\n" + "=" * 60)
        dprint("🔍 ENERGY ENGINE CONFIG DEBUG")
        dprint("=" * 60)
        dprint(f"Config type: {type(config)}")
        dprint(f"Config keys: {list(config.keys())}")
        dprint(f"'rapl' in config: {'rapl' in config}")
        if "rapl" in config:
            dprint(f"rapl paths: {config['rapl'].get('paths', {})}")
        dprint(f"'settings' in config: {'settings' in config}")
        if "settings" in config:
            dprint(f"settings keys: {list(config['settings'].keys())}")
        dprint("=" * 60 + "\n")

        # interrupt initializations
        self.collect_interrupt_samples = True
        self._interrupt_sample_counter = 0
        self.last_interrupt_samples = []

        # Store configuration
        self.config = config
        self.settings = config.get("settings", {})  # app_settings.yaml content

        # --------------------------------------------------------------------
        # Initialize all hardware readers
        # --------------------------------------------------------------------
        self.rapl = RAPLReader(config)
        self.perf = PerfReader(config)
        self.turbostat = TurbostatReader(config)
        self.sensor = SensorReader(config)
        self.msr = MSRReader(config)
        self.scheduler = SchedulerMonitor(config)

        self.sensor.initialize()
        # --------------------------------------------------------------------
        # Load settings from config – handle both dict and object formats
        # --------------------------------------------------------------------
        experiment_settings = self.settings.get("experiment", {})
        measurement_settings = self.settings.get("measurement", {})

        # Convert to dictionary if they are objects
        if hasattr(experiment_settings, "__dict__"):
            experiment_settings = experiment_settings.__dict__
        if hasattr(measurement_settings, "__dict__"):
            measurement_settings = measurement_settings.__dict__

        # Core pinning (Req 1.15)
        self.pinned_cores = experiment_settings.get("pinned_cores", [0, 1])

        # Sampling rate (Req 1.46)
        self.sampling_rate_hz = measurement_settings.get("sampling_rate_hz", 100)

        # Idle baseline duration (Req 1.45) – used as default for the utility
        self.idle_baseline_seconds = measurement_settings.get("baseline_seconds", 5)

        # Cool‑down between runs (Req 3.5)
        self.cool_down_seconds = experiment_settings.get("cool_down_seconds", 30)

        # Create core pinner instance
        self.core_pinner = CorePinner(default_cores=self.pinned_cores)

        # Idle baseline storage (joules per second per domain)
        # This is stored as metadata only – NEVER applied to raw data
        self.idle_baseline: Optional[Dict[str, float]] = None

        # Measurement state
        self.measurement_id: Optional[str] = None
        self.start_time: Optional[float] = None
        self.start_readings: Dict[str, Any] = {}
        self.end_readings: Dict[str, Any] = {}
        self.measurement: Optional[RawEnergyMeasurement] = None

        # Sampling thread for high‑frequency data
        self._sampling_thread: Optional[threading.Thread] = None
        self._sampling_queue: queue.Queue = queue.Queue()
        self._sampling_active = False
        msr_available = (
            hasattr(self.msr, "helper_available") and self.msr.helper_available
        ) or self.msr.rdmsr_available
        # ====================================================================
        # Thermal sampling (1Hz)
        # ====================================================================
        self.thermal_queue = queue.Queue()
        self.thermal_sampling_active = False
        self.thermal_sampling_thread = None
        self.thermal_config = config.get("thermal", {})
        self.thermal_rate_hz = self.thermal_config.get("sampling_rate_hz", 1)

        # Web UI settings - multiple servers support
        webui_config = self.settings.get("webui", {})
        self.webui_enabled = webui_config.get("enabled", False)
        self.webui_servers = []

        if self.webui_enabled:
            for server in webui_config.get("servers", []):
                if server.get("active", False):
                    self.webui_servers.append(
                        {
                            "url": server["url"]
                            + server.get("api_endpoint", "/api/update"),
                            "name": server.get("name", "unknown"),
                            "timeout": webui_config.get("timeout_ms", 1) / 1000.0,
                        }
                    )

        self.current_run_id = None

        dprint(
            "EnergyEngine initialized",
            readers={
                "rapl": self.rapl is not None,
                "perf": self.perf.perf_available,
                "turbostat": self.turbostat.available,
                "sensor": len(self.sensor.available_sensors) > 0,
                "msr": msr_available,
            },
        )

    def _start_thermal_sampling(self):
        """Start thermal sampling thread"""
        self.thermal_sampling_active = True
        self.thermal_sampling_thread = threading.Thread(
            target=self._thermal_sampling_loop
        )
        self.thermal_sampling_thread.daemon = True
        self.thermal_sampling_thread.start()

    def _thermal_sampling_loop(self):
        """Thermal sampling thread (1Hz default)"""
        interval = 1.0 / self.thermal_rate_hz
        next_sample = time.time()

        while self.thermal_sampling_active:
            now = time.time()
            if now >= next_sample:
                # Read all thermal sensors
                readings = self.sensor.read_all_thermal()
                throttle_detected = False

                # Check for throttling conditions
                for role, temp in readings.items():
                    if temp and hasattr(self.sensor, "throttle_thresholds"):
                        threshold = self.sensor.throttle_thresholds.get(role)
                        if threshold and temp > threshold:
                            throttle_detected = True

                self.thermal_queue.put((now, readings, throttle_detected))
                print(f"🔍 THERMAL DEBUG - Sample at {now}: {readings}")
                next_sample = now + interval  # Prevent drift
            else:
                time.sleep(min(0.1, next_sample - now))

    def _stop_thermal_sampling(self):
        """Stop thermal sampling and collect remaining samples"""
        self.thermal_sampling_active = False
        if self.thermal_sampling_thread:
            self.thermal_sampling_thread.join(timeout=2)

        # Collect any remaining samples
        thermal_samples = []
        while not self.thermal_queue.empty():
            thermal_samples.append(self.thermal_queue.get())
        return thermal_samples

    # ------------------------------------------------------------------------
    # Core pinning (Req 1.15)
    # ------------------------------------------------------------------------
    def pin_cores(self, cores: Optional[List[int]] = None) -> None:
        """
        Pin the current process to specific CPU cores.

        Args:
            cores: List of core indices. If None, uses the configured default.

        Req 1.15: Physical Core Affinity – eliminates noise from thread migration.
        """
        if cores is None:
            cores = self.pinned_cores
        self.core_pinner.pin_to_cores(cores)
        dprint(f"Pinned to cores {cores}")

    # ------------------------------------------------------------------------
    # Idle baseline measurement (Req 1.45) – uses research‑grade utility
    # ------------------------------------------------------------------------
    def measure_idle_baseline(
        self,
        duration_seconds: int = 10,
        num_samples: int = 10,
        pre_wait_seconds: int = 10,
        force_remeasure: bool = False,
    ) -> BaselineMeasurement:
        """
        Measure system idle energy baseline.
        """
        # Measure baseline using utility (returns BaselineMeasurement object)
        dprint(f"🔍 DEBUG - force_remeasure value: {force_remeasure}")
        baseline = measure_baseline(
            rapl_reader=self.rapl,
            core_pinner=self.core_pinner,
            duration_seconds=duration_seconds,
            num_samples=num_samples,
            pre_wait_seconds=pre_wait_seconds,
            pin_cores=self.pinned_cores,
            force_remeasure=force_remeasure,
        )

        print(f">>> INPUT duration_seconds: {duration_seconds}")
        print(f">>> INPUT num_samples: {num_samples}")
        print(f">>> INPUT pre_wait_seconds: {pre_wait_seconds}")

        # DEBUG: Object ID after measure_baseline
        print(f"🔍 DEBUG1 - baseline object ID after measure_baseline: {id(baseline)}")
        print(
            f"🔍 DEBUG1 - baseline metadata after measure_baseline: {baseline.metadata}"
        )

        # Update baseline_id with PID to ensure uniqueness
        baseline.baseline_id = f"baseline_{int(time.time())}_{os.getpid()}"
        baseline.timestamp = time.time()

        # DEBUG: Object ID after updates
        print(f"🔍 DEBUG1 - baseline object ID after updates: {id(baseline)}")
        print(f"🔍 DEBUG1 - baseline metadata after updates: {baseline.metadata}")
        print(f"🔍 DEBUG1 - baseline.__dict__: {baseline.__dict__}")

        self.idle_baseline = baseline
        return baseline

    # ------------------------------------------------------------------------
    # High‑frequency sampling (Req 1.46)
    # ------------------------------------------------------------------------
    def _sampling_loop(self) -> None:
        """
        Background thread that samples RAPL at the configured rate.
        Samples are put into a queue for later retrieval.
        """
        interval = 1.0 / self.sampling_rate_hz
        next_sample = time.time()
        sample_counter = 0

        while self._sampling_active:
            try:
                # Take a sample
                timestamp = time.time()
                energy = self.rapl.read_energy_safe()

                # Put in queue (non-blocking)
                try:
                    self._sampling_queue.put_nowait((timestamp, energy))
                except queue.Full:
                    # Queue full – remove oldest and add new
                    try:
                        self._sampling_queue.get_nowait()
                        self._sampling_queue.put_nowait((timestamp, energy))
                    except queue.Empty:
                        pass

                # 🔴 NEW DEBUG LINE 1: Print every time counter is a multiple of 10
                if sample_counter % 10 == 0:
                    print(
                        f"🔍 DEBUG: counter={sample_counter}, will check interrupt condition"
                    )

                # Sample interrupts every 10th iteration (10 Hz)
                if self.collect_interrupt_samples and sample_counter % 10 == 0:
                    print(
                        f"🔍 DEBUG: ACTUALLY triggering interrupt at counter={sample_counter}"
                    )
                    self.scheduler.sample_interrupts()
                sample_counter += 1

                # Sample interrupts every 10th iteration (10 Hz)
                if self.collect_interrupt_samples and sample_counter % 10 == 0:
                    print(
                        f"🔍 DEBUG: ACTUALLY triggering interrupt at counter={sample_counter}"
                    )
                    self.scheduler.sample_interrupts()
                sample_counter += 1

                # ========== ADD WEB UI CODE HERE ==========
                # Send to all active Web UI servers (non-blocking)
                if self.webui_enabled and self.current_run_id and self.webui_servers:
                    print(f"🔍 WEBUI: Sending data for run {self.current_run_id}")
                    for server in self.webui_servers:
                        print(f"   To server: {server['url']}")
                        try:
                            # Get latest CPU data if available
                            cpu_data = {}
                            if hasattr(self, "turbostat") and self.turbostat:
                                if hasattr(self.turbostat, "get_latest_sample"):
                                    cpu_data = self.turbostat.get_latest_sample() or {}

                            # Get interrupt rate
                            irq_rate = 0
                            if hasattr(self, "scheduler") and self.scheduler:
                                irq_rate = getattr(
                                    self.scheduler,
                                    "get_current_interrupt_rate",
                                    lambda: 0,
                                )()

                            # Push raw sample to this server
                            requests.post(
                                server["url"],
                                json={
                                    "run_id": self.current_run_id,
                                    "server": server["name"],
                                    "timestamp": time.time(),
                                    "cpu_busy_mhz": cpu_data.get("Bzy_MHz", 0),
                                    "cpu_avg_mhz": cpu_data.get("Avg_MHz", 0),
                                    "package_temp": cpu_data.get("PkgTmp", 0),
                                    "pkg_power": energy.get("package-0", 0) / 1_000_000,
                                    "interrupt_rate": irq_rate,
                                },
                                timeout=server["timeout"],
                                headers={"Connection": "close"},
                            )
                        except Exception:
                            pass  # Silent fail per server
                # ========== END WEB UI CODE ==========

                # Schedule next sample
                next_sample += interval
                sleep_time = max(0, next_sample - time.time())

                if sleep_time > 0:
                    time.sleep(sleep_time)
                else:
                    # Running behind – warn and continue
                    logger.warning(
                        "Sampling loop falling behind; increase interval or reduce load"
                    )
                    next_sample = time.time()

            except Exception as e:
                logger.error(f"Sampling error: {e}")
                time.sleep(0.01)

    def _start_sampling(self) -> None:
        """Start the high‑frequency sampling thread."""
        self._start_thermal_sampling()
        if self._sampling_thread and self._sampling_thread.is_alive():
            logger.warning("Sampling already active")
            return

        self._sampling_active = True
        self._sampling_thread = threading.Thread(
            target=self._sampling_loop, name="EnergyEngine-Sampler", daemon=True
        )
        self._sampling_thread.start()
        dprint("Started high‑frequency sampling")

    def _stop_sampling(self) -> List[tuple]:
        """Stop sampling and retrieve all collected samples."""
        self._sampling_active = False

        if self._sampling_thread:
            self._sampling_thread.join(timeout=2.0)

        samples = []
        while not self._sampling_queue.empty():
            try:
                samples.append(self._sampling_queue.get_nowait())
            except queue.Empty:
                break

        dprint(f"Stopped sampling, collected {len(samples)} samples")
        return samples

    # ------------------------------------------------------------------------
    # Measurement session management
    # ------------------------------------------------------------------------

    def start_measurement(self) -> str:
        """
        Start a new measurement session.

        This method:
        1. Captures snapshot values (RAPL, scheduler)
        2. Starts continuous readers (perf, turbostat)
        3. Starts high-frequency sampling
        """
        self.measurement_id = f"meas_{int(time.time())}_{os.getpid()}"
        self.start_time = time.time()

        # Pin to dedicated cores first
        self.pin_cores()

        # ====================================================================
        # STEP 1: Snapshot readers - capture START values
        # ====================================================================
        rapl_start = self.rapl.read_energy()
        self.scheduler_start = self.scheduler.read_all()

        self._start_thermal_sampling()

        # ====================================================================
        # Capture MSR thermal snapshot at START
        # ====================================================================
        msr_thermal_start = None
        msr_available = (
            hasattr(self.msr, "helper_available") and self.msr.helper_available
        ) or self.msr.rdmsr_available
        if hasattr(self, "msr") and msr_available:
            try:
                msr_thermal_start = self.msr.snapshot_thermal_state()
                print(f"\n🔥 THERMAL SNAPSHOT - START:")
                print(f"   Package: {msr_thermal_start['package']}")
                print(f"   Core: {msr_thermal_start['core']}")
                logger.debug(f"MSR thermal start: {msr_thermal_start}")
            except Exception as e:
                print(f"❌ Failed to capture start MSR thermal: {e}")
                logger.warning(f"Could not capture start MSR thermal: {e}")

        self.start_readings = {
            "rapl": rapl_start,
            "scheduler": self.scheduler_start,
            "sensor": self.sensor.read_temperatures(),
            "msr_thermal": msr_thermal_start,  # Store start thermal state
        }
        # ====================================================================
        # Capture C-state counters at START
        # ====================================================================
        if hasattr(self, "msr") and self.msr:
            self.msr._cstate_start = self.msr.snapshot_cstate_counters()
            logger.debug(f"C-state start: {self.msr._cstate_start}")
        # ====================================================================
        # STEP 2: Start continuous readers
        # ====================================================================
        # perf: continuous counter (attaches to process)
        self.perf.start_process_measurement()

        # turbostat: continuous sampling at the same rate as RAPL
        # <-- NEW: Compute interval in milliseconds from sampling_rate_hz
        interval_ms = (
            int(1000 / self.sampling_rate_hz) if self.sampling_rate_hz > 0 else 100
        )
        # <-- MODIFIED: Pass interval_ms to start_monitoring (new signature)
        self.turbostat.start_monitoring(interval_ms=interval_ms)

        # Start high-frequency sampling (RAPL samples)
        self._start_sampling()

        # NEW: Start interrupt sampling
        if self.collect_interrupt_samples:
            self.scheduler.start_interrupt_sampling()

        dprint(f"Started measurement {self.measurement_id}")
        return self.measurement_id

    def stop_measurement(self) -> RawEnergyMeasurement:
        """
        Stop measurement and return raw results.

        This method:
        1. Stops continuous readers and collects their data
        2. Captures snapshot end values
        3. Computes deltas
        4. Assembles RawEnergyMeasurement
        """
        if self.measurement_id is None:
            raise RuntimeError("No measurement in progress")

        # ====================================================================
        # STEP 1: Stop continuous readers FIRST
        # ====================================================================
        # Stop high-frequency sampling
        samples = self._stop_sampling()
        if samples:
            print(f"🔍 DEBUG - Number of samples: {len(samples)}")
            print(f"🔍 DEBUG - First sample type: {type(samples[0])}")
            print(f"🔍 DEBUG - First sample length: {len(samples[0])}")
            print(f"🔍 DEBUG - First sample content: {samples[0]}")
            if len(samples) > 1:
                print(f"🔍 DEBUG - Second sample content: {samples[1]}")

        interrupt_samples = []
        if (
            hasattr(self, "collect_interrupt_samples")
            and self.collect_interrupt_samples
        ):
            interrupt_samples = self.scheduler.stop_interrupt_sampling()
            logger.debug(f"Collected {len(interrupt_samples)} interrupt samples")

        # ====================================================================
        # Store samples in instance for later access by harness
        # ====================================================================
        self.last_interrupt_samples = interrupt_samples
        self.last_samples = samples
        # Optional: separate by type if samples have type field
        self.last_energy_samples = []
        self.last_cpu_samples = []

        # Try to categorize samples if they have a 'type' field
        # If we need to categorize, we need to know the tuple structure
        # For example, if samples are (timestamp, type, value) tuples
        for sample in samples:
            if len(sample) >= 2:
                # This is a guess - adjust based on actual tuple structure
                if isinstance(sample[1], (int, float)):  # If second element is a number
                    self.last_energy_samples.append(sample)
                # Add more logic based on actual formatif hasattr(self.energy_engine, 'samples'):
        # Stop perf and get accumulated counters

        perf_data = self.perf.stop_process_measurement()

        # Stop turbostat and get continuous data
        turbostat_data = self.turbostat.stop_monitoring()
        thermal_samples = self._stop_thermal_sampling()
        # ====================================================================
        # STEP 2: Capture snapshot readers END values
        # ====================================================================
        rapl_end = self.rapl.read_energy()
        scheduler_end = self.scheduler.read_all()

        # ====================================================================
        # STEP 3: Capture MSR thermal snapshot at END (BEFORE timestamp)
        # ====================================================================
        msr_thermal_end = None
        msr_available = (
            hasattr(self.msr, "helper_available") and self.msr.helper_available
        ) or self.msr.rdmsr_available
        if hasattr(self, "msr") and msr_available:
            try:
                msr_thermal_end = self.msr.snapshot_thermal_state()
                print(f"\n🔥 THERMAL SNAPSHOT - END:")
                print(f"   Package: {msr_thermal_end['package']}")
                print(f"   Core: {msr_thermal_end['core']}")
                logger.debug(f"MSR thermal end: {msr_thermal_end}")
            except Exception as e:
                print(f"❌ Failed to capture end MSR thermal: {e}")
                logger.warning(f"Could not capture end MSR thermal: {e}")

        # ====================================================================
        # Calculate C-state deltas (per-run, not cumulative)
        # ====================================================================
        cstate_deltas = {}
        msr_metrics = {}
        if (
            hasattr(self, "msr")
            and hasattr(self.msr, "_cstate_start")
            and self.msr._cstate_start
        ):
            try:
                self.msr._cstate_end = self.msr.snapshot_cstate_counters()

                for state in ["c2", "c3", "c6", "c7"]:
                    start_val = self.msr._cstate_start.get(state, 0)
                    end_val = self.msr._cstate_end.get(state, 0)
                    delta = max(0, end_val - start_val)
                    cstate_deltas[f"{state}_time_seconds"] = delta

                logger.debug(f"C-state deltas: {cstate_deltas}")

                # Add to msr_metrics
                msr_metrics["cstate_deltas"] = cstate_deltas

            except Exception as e:
                logger.warning(f"Failed to calculate C-state deltas: {e}")

        # Set end time IMMEDIATELY after thermal snapshot
        self.end_time = time.time()
        duration_seconds = self.end_time - self.start_time

        # Capture sensor temps AFTER timestamp (less timing-critical)
        sensor_end = self.sensor.read_temperatures()

        # ====================================================================
        # STEP 3: Compute deltas for snapshot readers
        # ====================================================================
        start_rapl = self.start_readings.get("rapl", {})

        # Scheduler delta
        scheduler_delta = {}
        start_sched = self.start_readings.get("scheduler", {})
        for key in scheduler_end:
            if key in start_sched:
                if isinstance(scheduler_end[key], (int, float)) and isinstance(
                    start_sched[key], (int, float)
                ):
                    scheduler_delta[key] = scheduler_end[key] - start_sched[key]
                else:
                    scheduler_delta[key] = scheduler_end[key]
            else:
                scheduler_delta[key] = scheduler_end[key]
                # ADD THIS DEBUG
        print(f"🔍 DEBUG scheduler_delta keys: {list(scheduler_delta.keys())}")
        print(
            f"🔍 DEBUG run queue value: {scheduler_delta.get('runnable', 'NOT FOUND')}"
        )

        # ====================================================================
        # STEP 4: Calculate actual sampling rate
        # ====================================================================
        actual_sampling_rate = (
            len(samples) / duration_seconds if duration_seconds > 0 else 0
        )

        # ====================================================================
        # STEP 5: Validate perf data
        # ====================================================================
        if hasattr(perf_data, "duration_seconds") and perf_data.duration_seconds <= 0:
            logger.warning(f"perf duration invalid: {perf_data.duration_seconds}s")

        # ====================================================================
        # Capture MSR metrics and calculate C-state deltas
        # ====================================================================
        msr_metrics = {}
        msr_available = (
            hasattr(self.msr, "helper_available") and self.msr.helper_available
        ) or self.msr.rdmsr_available

        if hasattr(self, "msr") and msr_available:
            try:
                # Get base MSR metrics (ring bus, thermal, etc.)
                msr_metrics = self.msr.get_all_metrics()

                # ========== NEW: Calculate C-state deltas if start exists ==========
                if hasattr(self.msr, "_cstate_start") and self.msr._cstate_start:
                    # Capture end snapshot
                    self.msr._cstate_end = self.msr.snapshot_cstate_counters()

                    start_counters = self.msr._cstate_start.get("counters", {})
                    end_counters = self.msr._cstate_end.get("counters", {})

                    cstate_deltas = {}
                    for state in ["c2", "c3", "c6", "c7"]:
                        start_val = start_counters.get(state, 0)
                        end_val = end_counters.get(state, 0)
                        delta_raw = max(0, end_val - start_val)

                        # Convert to seconds using TSC frequency
                        if (
                            hasattr(self.msr, "tsc_frequency_hz")
                            and self.msr.tsc_frequency_hz
                        ):
                            delta_sec = delta_raw / self.msr.tsc_frequency_hz
                        else:
                            delta_sec = delta_raw / 2.8e9  # Fallback

                        # Add directly to msr_metrics top level (for DB)
                        msr_metrics[f"{state}_time_seconds"] = delta_sec
                        cstate_deltas[f"{state}_delta_raw"] = delta_raw

                    msr_metrics["cstate_deltas"] = cstate_deltas
                    logger.debug(
                        f"C-state deltas (sec): c2={msr_metrics.get('c2_time_seconds', 0):.3f}"
                    )

                # Debug prints
                print(f"🟢 ENGINE_ID: {id(msr_metrics)}")
                print(
                    f"🟢 ENGINE_VALUE: c2={msr_metrics.get('c2_time_seconds', 0):.3f}s"
                )
                print(f"🟢 ENGINE_KEYS: {list(msr_metrics.keys())}")

            except Exception as e:
                logger.warning(f"MSR measurement failed: {e}")

        # Set end time and duration AFTER all readings are done
        self.end_time = time.time()
        duration_seconds = self.end_time - self.start_time

        # ====================================================================
        # STEP 7: Calculate derived thermal metrics (BEFORE building measurement)
        # ====================================================================
        thermal_start = self.start_readings.get("msr_thermal")
        thermal_end = msr_thermal_end

        thermal_during_experiment = 0
        thermal_now_active = 0
        thermal_since_boot = 0

        if thermal_start and thermal_end:
            # Get package and core log bits
            start_pkg_log = thermal_start["package"].get("thermal_log", 0)
            start_core_log = thermal_start["core"].get("thermal_log", 0)
            end_pkg_log = thermal_end["package"].get("thermal_log", 0)
            end_core_log = thermal_end["core"].get("thermal_log", 0)

            # Get current throttling state at end
            end_pkg_now = thermal_end["package"].get("thermal_now", 0)
            end_core_now = thermal_end["core"].get("thermal_now", 0)

            # Combined OR logic
            start_combined_log = 1 if (start_pkg_log == 1 or start_core_log == 1) else 0
            end_combined_log = 1 if (end_pkg_log == 1 or end_core_log == 1) else 0

            # Did throttling occur DURING experiment?
            thermal_during_experiment = (
                1 if end_combined_log > start_combined_log else 0
            )

            # Is system throttling NOW at end?
            thermal_now_active = 1 if (end_pkg_now == 1 or end_core_now == 1) else 0

            # Has system ever throttled since boot?
            thermal_since_boot = end_combined_log

        # Print derived metrics for verification (temporary)
        print(f"\n🔥 THERMAL DERIVED:")
        print(f"   during_experiment: {thermal_during_experiment}")
        print(f"   now_active: {thermal_now_active}")
        print(f"   since_boot: {thermal_since_boot}")

        # ====================================================================
        # STEP 8: Build raw measurement object
        # ====================================================================
        measurement = RawEnergyMeasurement(
            measurement_id=self.measurement_id,
            start_time=self.start_time,
            end_time=self.end_time,
            duration_seconds=duration_seconds,
            # RAPL (snapshot)
            rapl_start_uj=start_rapl,
            rapl_end_uj=rapl_end,
            # perf (continuous) - store object directly
            perf=perf_data,
            # turbostat (continuous) - store the full data dictionary
            turbostat=turbostat_data,
            # thermal (sensor)
            thermal=(
                sensor_end.to_dict() if hasattr(sensor_end, "to_dict") else sensor_end
            ),
            thermal_samples=thermal_samples,
            # Swap Start and end fileds
            scheduler_start=self.scheduler_start,
            scheduler_end=scheduler_end,
            # scheduler (snapshot with delta)
            scheduler_metrics=scheduler_delta,
            # High-frequency samples
            samples=samples,
            # Actual sampling rate
            sampling_rate_hz=actual_sampling_rate,
            # MSR metrics
            msr_metrics=msr_metrics,
            # NEW: Pass derived thermal values
            thermal_during_experiment=thermal_during_experiment,
            thermal_now_active=thermal_now_active,
            thermal_since_boot=thermal_since_boot,
            # Metadata with thermal calculations
            metadata={
                "hostname": platform.node(),
                "python_version": sys.version.split()[0],
                "cpu_affinity": list(os.sched_getaffinity(0)),
                "pinned_cores": self.pinned_cores,
                "turbostat_version": self.turbostat.turbostat_version,
                "cpu_topology": self.turbostat.cpu_topology,
                # Raw thermal snapshots
                "thermal_start": thermal_start,
                "thermal_end": thermal_end,
                # Derived thermal metrics
                "thermal_during_experiment": thermal_during_experiment,
                "thermal_now_active": thermal_now_active,
                "thermal_since_boot": thermal_since_boot,
            },
        )

        # Add baseline info if available
        if self.idle_baseline:
            measurement.metadata["baseline_available"] = True
            measurement.metadata["baseline_power_watts"] = self.idle_baseline

        self.measurement = measurement
        self.measurement_id = None
        self.print_summary(measurement)
        return measurement

    def print_summary(self, measurement: RawEnergyMeasurement):
        """
        Print a comprehensive summary of all collected data.
        """
        print("\n" + "=" * 70)
        print("📊 ENERGY ENGINE COMPLETE SUMMARY")
        print("=" * 70)

        # 1. Timing
        print(f"\n⏱️  Duration: {measurement.duration_seconds:.3f}s")
        print(
            f"   Start: {measurement.start_time:.3f}, End: {measurement.end_time:.3f}"
        )

        # 2. RAPL Energy (snapshot deltas)
        rapl_start = measurement.rapl_start_uj
        rapl_end = measurement.rapl_end_uj
        print("\n⚡ RAPL Energy (µJ):")
        for domain in set(rapl_start.keys()) | set(rapl_end.keys()):
            start_val = rapl_start.get(domain, 0)
            end_val = rapl_end.get(domain, 0)
            delta = end_val - start_val
            print(f"   {domain}: {delta:>12} µJ ({delta/1e6:.6f} J)")

        # 3. perf counters – using correct attribute names
        if measurement.perf:
            perf = measurement.perf
            print("\n📈 Performance Counters:")
            # Instructions
            if hasattr(perf, "instructions_retired"):
                print(f"   Instructions: {perf.instructions_retired:,}")
            # Cycles
            if hasattr(perf, "cpu_cycles"):
                print(f"   Cycles:       {perf.cpu_cycles:,}")
            # IPC (method)
            if hasattr(perf, "instructions_per_cycle"):
                print(f"   IPC:          {perf.instructions_per_cycle():.2f}")
            # Cache
            if hasattr(perf, "cache_references"):
                print(f"   Cache Refs:   {perf.cache_references:,}")
            if hasattr(perf, "cache_misses"):
                print(f"   Cache Misses: {perf.cache_misses:,}")
                if perf.cache_references:
                    miss_rate = perf.cache_misses / perf.cache_references
                    print(f"   Miss Rate:    {miss_rate:.2%}")
            # Page faults
            major = getattr(perf, "major_page_faults", 0)
            minor = getattr(perf, "minor_page_faults", 0)
            if major or minor:
                print(f"   Page Faults:  {major+minor} (major={major}, minor={minor})")
            # Context switches
            vol = getattr(perf, "context_switches_voluntary", 0)
            invol = getattr(perf, "context_switches_involuntary", 0)
            if vol or invol:
                print(f"   Ctx Switches: vol={vol}, invol={invol}")
            # Thread migrations
            if hasattr(perf, "thread_migrations"):
                print(f"   Migrations:   {perf.thread_migrations:,}")

        # 4. Turbostat continuous data
        if measurement.turbostat and measurement.turbostat.get("dataframe") is not None:
            df = measurement.turbostat["dataframe"]
            summary = measurement.turbostat.get("summary", {})
            print(
                "\n🌡️ Turbostat Summary (over {:.2f}s, {} samples):".format(
                    measurement.turbostat.get("duration_seconds", 0),
                    measurement.turbostat.get("num_samples", 0),
                )
            )
            # C‑states
            cstates = [k for k in summary if k.endswith("_mean") and k.startswith("C")]
            if cstates:
                print("   C‑state residencies (%):")
                for c in sorted(cstates):
                    print(f"      {c}: {summary[c]:.2f}")
            # Frequency
            if "frequency_mean" in summary:
                print(
                    f"   Frequency (MHz): mean={summary['frequency_mean']:.0f}, "
                    f"min={summary.get('frequency_min', 0):.0f}, "
                    f"max={summary.get('frequency_max', 0):.0f}, "
                    f"std={summary.get('frequency_stddev', 0):.2f}"
                )
            # Temperature
            if "package_temp_mean" in summary:
                print(f"   Package Temp (°C): mean={summary['package_temp_mean']:.1f}")
            # Print first few turbostat samples
            if len(df) > 0:
                print("\n   First 3 turbostat samples:")
                for i in range(min(3, len(df))):
                    row = df.iloc[i].to_dict()
                    print(f"      Sample {i+1}: {row}")

        # 5. MSR metrics
        if measurement.msr_metrics:
            msr = measurement.msr_metrics
            print("\n🔧 MSR Metrics:")
            ring_bus = msr.get("ring_bus", {})
            if ring_bus:
                print(
                    f"   Ring Bus Frequency: {ring_bus.get('current_mhz', 0):.1f} MHz"
                )
            if "wakeup_latency_us" in msr:
                print(f"   Wake‑up Latency: {msr['wakeup_latency_us']:.2f} µs")
            if "thermal_throttle" in msr:
                print(f"   Thermal Throttle Flag: {msr['thermal_throttle']}")

        # 6. Scheduler metrics
        if measurement.scheduler_metrics:
            sched = measurement.scheduler_metrics
            print("\n🔄 Scheduler Metrics:")
            print(f"   Voluntary Ctx Sw:   {sched.get('voluntary_switches', 0)}")
            print(f"   Involuntary Ctx Sw: {sched.get('involuntary_switches', 0)}")
            print(f"   Thread Migrations:  {sched.get('thread_migrations', 0)}")
            print(f"   Run Queue Length:    {sched.get('runnable', 0):.2f}")
            print(f"   Kernel Time:         {sched.get('system_time', 0):.2f} ms")
            print(f"   User Time:           {sched.get('user_time', 0):.2f} ms")

        # 7. High‑frequency samples
        if measurement.samples:
            print(f"\n📊 High‑Frequency Samples: {len(measurement.samples)} collected")
            if len(measurement.samples) > 0:
                print("   First sample:", measurement.samples[0])

        print("\n" + "=" * 70)

    # ------------------------------------------------------------------------
    # Context manager interface
    # ------------------------------------------------------------------------
    def __enter__(self):
        """Enter context: start measurement."""
        self.start_measurement()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context: stop measurement and store result in self.measurement."""
        self.measurement = self.stop_measurement()

    # ------------------------------------------------------------------------
    # Multiple‑run convenience
    # ------------------------------------------------------------------------
    def run_multiple(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: dict = None,
        iterations: int = 10,
        cool_down: Optional[int] = None,
        output_file: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Run a function multiple times, measuring each run and collecting statistics.

        Args:
            func: The function to run (should contain the AI workload).
            args: Positional arguments for the function.
            kwargs: Keyword arguments for the function.
            iterations: Number of runs.
            cool_down: Seconds to wait between runs (default from config).
            output_file: Optional JSON file to save results.

        Returns:
            A dictionary containing:
                - 'all_runs': list of RawEnergyMeasurement objects
                - 'summary': statistical summary of key metrics
                - 'metadata': run parameters

        Req 3.4: Automated 10‑run iteration.
        """
        if kwargs is None:
            kwargs = {}
        if cool_down is None:
            cool_down = self.cool_down_seconds

        dprint(f"Running {iterations} iterations with {cool_down}s cool‑down")

        results = []
        for i in range(iterations):
            dprint(f"Run {i+1}/{iterations}")
            with self as engine:
                result = func(*args, **kwargs)
            results.append(self.measurement)

            if i < iterations - 1:
                time.sleep(cool_down)

        # Compute summary statistics
        summaries = {}
        metrics = [
            ("package_energy_uj", "µJ"),
            ("core_energy_uj", "µJ"),
            ("duration_seconds", "s"),
        ]

        for metric, unit in metrics:
            values = []
            for m in results:
                if m is not None:
                    if metric == "package_energy_uj":
                        values.append(m.package_energy_uj)
                    elif metric == "core_energy_uj":
                        values.append(m.core_energy_uj)
                    elif metric == "duration_seconds":
                        values.append(m.duration_seconds)

            if values:
                summaries[metric] = {
                    "mean": statistics.mean(values),
                    "median": statistics.median(values),
                    "stdev": statistics.stdev(values) if len(values) > 1 else 0,
                    "min": min(values),
                    "max": max(values),
                    "unit": unit,
                }

        output = {
            "metadata": {
                "iterations": iterations,
                "cool_down_seconds": cool_down,
                "function": func.__name__,
                "baseline_available": self.idle_baseline is not None,
            },
            "all_runs": [m.to_dict() if m else {} for m in results if m is not None],
            "summary": summaries,
        }

        if output_file:
            with open(output_file, "w") as f:
                json.dump(output, f, indent=2, default=str)
            dprint(f"Saved results to {output_file}")

        return output

    def __str__(self) -> str:
        """String representation of the energy engine."""
        readers = []
        if self.rapl:
            readers.append("rapl")
        if self.perf.perf_available:
            readers.append("perf")
        if self.turbostat.available:
            readers.append("turbostat")
        if self.sensor.available_sensors:
            readers.append("sensor")
        if self.msr.rdmsr_available:
            readers.append("msr")

        return f"EnergyEngine(readers={', '.join(readers)})"


# ============================================================================
# Example usage (standalone test)
# ============================================================================
if __name__ == "__main__":
    import json

    from core.config_loader import ConfigLoader

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("\n" + "=" * 70)
    print("ENERGY ENGINE TEST – RAW MEASUREMENTS ONLY")
    print("=" * 70)

    # Load configuration from Module 0
    config_loader = ConfigLoader()
    config = config_loader.get_hardware_config()  # hw_config.json

    # Merge with app settings
    settings = config_loader.get_settings()

    # Handle settings (could be dict or object from config_loader)
    if hasattr(settings, "__dict__"):
        config["settings"] = settings.__dict__
    else:
        config["settings"] = settings

    # Create engine
    engine = EnergyEngine(config)
    print(f"📊 {engine}")

    # Measure idle baseline (optional – stored as metadata)
    print("\n📝 Measuring idle baseline (2 samples of 2s each for quick test)...")
    baseline = engine.measure_idle_baseline(
        duration_seconds=2, num_samples=2, pre_wait_seconds=2
    )
    print(f"Idle baseline: {baseline} W")
    print(f"   (This baseline is stored as metadata, NOT applied to raw data)")

    # Simple workload for testing
    def dummy_workload():
        """A dummy workload that just sleeps."""
        time.sleep(1)
        return "done"

    # Single measurement – returns RAW data only
    print("\n📝 Running single measurement (RAW data)...")
    with engine as m:
        result = dummy_workload()

    print(f"Result: {result}")
    print(f"Raw package energy: {engine.measurement.package_energy_uj / 1e6:.4f} J")
    print(f"Raw core energy: {engine.measurement.core_energy_uj / 1e6:.4f} J")

    if engine.measurement.metadata.get("baseline_available"):
        print(
            f"Baseline available: {engine.measurement.metadata['baseline_power_watts']} W"
        )
        print(f"   (Use EnergyAnalyzer to compute corrected values)")

    # Multiple runs
    print("\n📝 Running 3 iterations with 2 second cool-down...")
    stats = engine.run_multiple(dummy_workload, iterations=3, cool_down=2)

    print("\n📊 Summary Statistics (RAW values):")
    for metric, values in stats["summary"].items():
        print(
            f"   {metric}: mean={values['mean']:.2f} {values['unit']}, "
            f"stdev={values['stdev']:.2f}"
        )

    print("\n" + "=" * 70)
    print("✅ Test complete – Raw measurements only, baseline stored separately!")
    print("=" * 70)
