#!/usr/bin/env python3
"""
================================================================================
TURBOSTAT READER - Clean Implementation Using pandas
================================================================================

Author: Deepak Panigrahy

This module provides a clean, robust reader for Intel turbostat output.
turbostat reads CPU MSRs to provide detailed performance data including:
- C-state residency percentages (time spent in idle states)
- CPU frequency during execution
- iGPU RC6 residency and frequency
- Package temperature
- Power estimates for package, cores, GPU, RAM

Requirements Covered:
Req 1.4 – DVFS Attribution (frequencies)
Req 1.7 – C-State Residency (via turbostat)
Req 1.8 – iGPU RC6 & GT Freq
Req 1.9 – Package Thermal Jitter (temperature)
Req 1.41 – Deepest Core C-State (via residency columns)
================================================================================
"""

import io
import logging
import os
import platform
import re
import signal
import subprocess
import sys
import threading
import time
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# ============================================================================
# Fix Python path – ensures we can import sibling modules
# ============================================================================
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.models.energy_measurement import PowerState

# Set up logging for this module
logger = logging.getLogger(__name__)


class TurbostatReader:
    """
    Reads Intel turbostat metrics using pandas for clean parsing.

    This class handles:
    - Starting continuous monitoring of turbostat
    - Draining output in background thread to prevent pipe buffer full
    - Stopping monitoring and collecting all samples
    - Parsing tab-separated output into pandas DataFrame
    - Computing summary statistics (mean, std, min, max, median)
    - Mapping columns to our internal PowerState object
    - Capturing CPU topology for hybrid processors (P-cores/E-cores)
    - Recording turbostat version for reproducibility

    The reader uses column mappings from hw_config.json, which are
    detected automatically by Module 0's hardware detection.
    """

    def __init__(self, config: Dict):
        """
        Initialize the TurbostatReader with configuration from Module 0.

        Args:
            config: The full configuration dictionary loaded from
                   hw_config.json (contains 'turbostat' section)

        What this does:
        1. Extracts turbostat-specific configuration
        2. Sets up column mappings (forward and reverse)
        3. Locates the real turbostat binary
        4. Records turbostat version for reproducibility
        5. Captures CPU topology for hybrid processors
        """
        # Get turbostat section from config
        self.turbostat_config = config.get("turbostat", {})
        self.available = self.turbostat_config.get("available", False)

        # Column mappings: our metric name -> actual turbostat column name
        # Example: {'c6_residency': 'CPU%c6', 'package_temp': 'PkgTmp'}
        self.column_map = self.turbostat_config.get("columns", {})

        # Reverse mapping: column name -> metric name (useful during parsing)
        self.reverse_map = {v: k for k, v in self.column_map.items()}

        # Find the real turbostat binary (not the wrapper script)
        self.real_binary = self.turbostat_config.get("real_binary")
        if self.real_binary and os.path.exists(self.real_binary):
            self.turbostat_path = self.real_binary
            logger.info(f"Using real turbostat binary: {self.real_binary}")
        else:
            self.turbostat_path = self._find_turbostat()
            logger.info(f"Using turbostat from PATH: {self.turbostat_path}")

        # Mark as unavailable if binary not found
        if not self.turbostat_path:
            logger.warning("turbostat not found")
            self.available = False

        # Record turbostat version for reproducibility
        self.turbostat_version = self._get_turbostat_version()
        if self.turbostat_version:
            logger.info(f"Turbostat version: {self.turbostat_version}")

        # Capture CPU topology for hybrid processors (P-cores/E-cores)
        self.cpu_topology = self._get_cpu_topology()
        if self.cpu_topology.get("has_hybrid"):
            logger.info(
                f"Hybrid CPU detected: {len(self.cpu_topology.get('p_cores', []))} P-cores, "
                f"{len(self.cpu_topology.get('e_cores', []))} E-cores"
            )

        # Continuous monitoring attributes
        self.turbostat_process: Optional[subprocess.Popen] = None
        self._monitoring_active = False
        self._start_time: Optional[float] = None

        # FIX: Background thread and buffer for draining output
        self._output_buffer: List[str] = []
        self._reader_thread: Optional[threading.Thread] = None
        self._stop_reader = False

    def _find_turbostat(self) -> Optional[str]:
        """
        Locate the real turbostat binary.

        On Ubuntu, turbostat is often a wrapper that execs the real binary
        from a kernel-versioned directory like:
        /usr/lib/linux-tools/5.15.0/turbostat

        Returns:
            Path to turbostat binary or None if not found
        """
        # Try kernel-specific path first (most reliable)
        kernel_release = platform.release()
        kernel_path = f"/usr/lib/linux-tools/{kernel_release}/turbostat"
        if os.path.exists(kernel_path):
            return kernel_path

        # Fallback: try to find via 'which' command
        try:
            result = subprocess.run(
                ["which", "turbostat"], capture_output=True, text=True
            )
            if result.returncode == 0:
                wrapper_path = result.stdout.strip()
                return wrapper_path
        except:
            pass

        return None

    def _get_turbostat_version(self) -> Optional[str]:
        """
        Get turbostat version for reproducibility.

        This is critical for publications – reviewers may ask
        which version of tools were used.

        Returns:
            Version string or None if unavailable
        """
        if not self.turbostat_path:
            return None
        try:
            result = subprocess.run(
                [self.turbostat_path, "--version"],
                capture_output=True,
                text=True,
                timeout=2,
            )
            return result.stdout.strip() or result.stderr.strip()
        except:
            return None

    def _get_cpu_topology(self) -> Dict[str, Any]:
        """
        Capture CPU topology information for hybrid CPUs (P-cores/E-cores).

        Modern Intel CPUs (Alder Lake, Raptor Lake) have:
        - Performance cores (P-cores) - higher frequency, more power
        - Efficiency cores (E-cores) - lower frequency, less power

        This information is critical for accurate interpretation of turbostat data
        because P-cores and E-cores have different C-state behaviors and frequencies.

        Returns:
            Dictionary with CPU topology information:
            - has_hybrid: True if hybrid architecture detected
            - p_cores: List of P-core IDs (if detectable)
            - e_cores: List of E-core IDs (if detectable)
            - raw_lscpu: Complete lscpu output for reference
        """
        topology = {"has_hybrid": False, "p_cores": [], "e_cores": [], "raw_lscpu": ""}

        try:
            # Run lscpu to get CPU information
            result = subprocess.run(
                ["lscpu"], capture_output=True, text=True, timeout=2
            )
            if result.returncode == 0:
                topology["raw_lscpu"] = result.stdout

                # Check for hybrid flag
                for line in result.stdout.split("\n"):
                    if "Hybrid" in line:
                        topology["has_hybrid"] = True

                    # Try to parse core type mappings if available
                    # Format example: "Core P-core:0-3 E-core:4-7"
                    if "Core" in line and ":" in line:
                        parts = line.split(":")
                        if len(parts) > 1:
                            core_info = parts[1].strip()
                            if "P-core" in core_info:
                                # Parse P-core ranges
                                import re

                                p_ranges = re.findall(r"P-core:([\d,-]+)", core_info)
                                for r in p_ranges:
                                    # Expand ranges like "0-3" to [0,1,2,3]
                                    if "-" in r:
                                        start, end = map(int, r.split("-"))
                                        topology["p_cores"].extend(
                                            list(range(start, end + 1))
                                        )
                                    else:
                                        topology["p_cores"].append(int(r))

                            if "E-core" in core_info:
                                # Parse E-core ranges
                                import re

                                e_ranges = re.findall(r"E-core:([\d,-]+)", core_info)
                                for r in e_ranges:
                                    if "-" in r:
                                        start, end = map(int, r.split("-"))
                                        topology["e_cores"].extend(
                                            list(range(start, end + 1))
                                        )
                                    else:
                                        topology["e_cores"].append(int(r))

        except Exception as e:
            logger.debug(f"Failed to get CPU topology: {e}")

        return topology

    def _drain_output(self):
        """
        Background thread that continuously reads turbostat output.
        Uses select() with timeout to prevent blocking.
        Starts immediately to capture all output.
        """
        import select

        logger.debug("Turbostat output drainer thread started")

        if not self.turbostat_process or not self.turbostat_process.stdout:
            logger.error("No turbostat process or stdout pipe")
            return

        # Check if pipe is readable
        import os

        fd = self.turbostat_process.stdout.fileno()
        logger.debug(f"Turbostat stdout pipe FD: {fd}")

        lines_read = 0
        while not self._stop_reader:
            try:
                # Check if data is available to read (with 100ms timeout)
                reads, _, _ = select.select(
                    [self.turbostat_process.stdout], [], [], 0.1
                )

                if reads:
                    # Data available, read it
                    line = self.turbostat_process.stdout.readline()
                    if line:
                        self._output_buffer.append(line)
                        lines_read += 1

                        # Log first few lines to verify data is flowing
                        if lines_read <= 5:
                            logger.debug(
                                f"✅ Turbostat line {lines_read}: {line.strip()[:100]}"
                            )
                        elif lines_read == 6:
                            logger.debug(f"Turbostat drainer continuing to collect...")

                        # Log progress occasionally
                        if lines_read % 50 == 0:
                            buffer_size = len("".join(self._output_buffer)) // 1024
                            logger.debug(
                                f"Turbostat drainer collected {lines_read} lines, buffer: {buffer_size}KB"
                            )
                    else:
                        # EOF - process probably died
                        logger.warning("EOF reached on turbostat stdout")
                        break
                else:
                    # No data available - log occasionally to show thread is alive
                    if lines_read == 0 and time.time() % 2 < 0.1:  # Every ~2 seconds
                        logger.debug("Turbostat drainer waiting for data...")

            except Exception as e:
                logger.error(f"Error in turbostat drainer thread: {e}")
                time.sleep(0.1)

        buffer_size = (
            len("".join(self._output_buffer)) // 1024 if self._output_buffer else 0
        )
        logger.info(
            f"Turbostat output drainer stopped, collected {lines_read} lines, buffer: {buffer_size}KB"
        )

    def start_monitoring(self, interval_ms: int = 100):
        """
        Start turbostat in continuous mode, collecting data every `interval_ms`.
        The output is captured by the background drainer thread.
        """
        if not self.available:
            logger.warning(
                "turbostat not available, cannot start continuous monitoring"
            )
            return

        self._monitoring_active = True
        self._output_buffer = []
        self._stop_reader = False
        self._start_time = time.time()

        interval = max(0.1, interval_ms / 1000.0)  # turbostat minimum is 0.1s

        cmd = [
            self.turbostat_path,
            "--quiet",
            "--Summary",
            "--show",
            ",".join(self.column_map.values()),
            "--interval",
            str(interval),
        ]

        logger.debug(f"Starting turbostat: {' '.join(cmd)}")
        self.turbostat_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )

        # Start the drainer thread to collect output as it arrives
        self._reader_thread = threading.Thread(target=self._drain_output, daemon=True)
        self._reader_thread.start()
        logger.info(
            f"turbostat continuous monitoring started (interval={interval_ms}ms)"
        )

    def stop_monitoring(self) -> Dict[str, Any]:
        """
        Stop turbostat, collect all buffered output, parse into a DataFrame,
        compute summary statistics, and return everything.
        """
        if not self._monitoring_active:
            logger.warning("stop_monitoring called but monitoring was not active")
            return {
                "dataframe": None,
                "num_samples": 0,
                "duration_seconds": 0,
                "summary": {},
            }

        self._stop_reader = True
        self._monitoring_active = False

        # Gracefully terminate turbostat
        if self.turbostat_process:
            self.turbostat_process.terminate()
            try:
                self.turbostat_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.turbostat_process.kill()
                self.turbostat_process.wait()

        # Wait for the drainer thread to finish
        if self._reader_thread and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=2)

        # Collect all output from the buffer
        output = "".join(self._output_buffer)
        duration = time.time() - self._start_time if self._start_time else 0

        # Parse the continuous output into a DataFrame
        df = self._parse_continuous_output(output)
        num_samples = len(df) if df is not None else 0

        # Compute summary statistics (mean, std, min, max, etc.)
        summary = self.compute_summary(df) if df is not None else {}

        logger.info(f"turbostat stopped: {num_samples} samples in {duration:.2f}s")

        return {
            "dataframe": df,
            "num_samples": num_samples,
            "duration_seconds": duration,
            "summary": summary,
        }

    def _parse_continuous_output(self, output: str) -> Optional[pd.DataFrame]:
        """
        Parse continuous turbostat output.

        Rules:
        - Header: first non-numeric line (not ending with "sec")
        - Data rows: lines starting with a digit
        - Column order: preserved exactly as turbostat outputs
        - NO timestamp added here - Energy Engine handles timestamps
        """
        if not output or len(output.strip()) < 10:
            print("⚠️ Turbostat output too short or empty")
            return None

        try:
            lines = output.strip().split("\n")

            # Debug: Show first few raw lines
            print(f"\n🔍 RAW TURBOSTAT LINES ({len(lines)} total):")
            for i, line in enumerate(lines[:10]):
                print(f"   Line {i:2d}: '{line}'")

            header_columns = None
            data_lines = []

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Skip interval line (ends with "sec")
                if line.endswith("sec"):
                    continue

                # Safer header detection using regex
                # Header is first line that doesn't start with a digit
                if header_columns is None and not re.match(r"^\d", line):
                    header_columns = re.split(r"\s+", line)
                    print(f"📋 Header detected: {header_columns}")
                    continue

                # Data rows: lines starting with a digit
                if header_columns and re.match(r"^\d", line):
                    parts = re.split(r"\s+", line)

                    # Only take up to number of header columns
                    if len(parts) >= len(header_columns):
                        data_lines.append(" ".join(parts[: len(header_columns)]))

            if not header_columns or not data_lines:
                print("❌ No turbostat data detected")
                return None

            print(f"✅ Found {len(data_lines)} data rows")

            # ====================================================================
            # Parse with pandas using actual header columns
            # ====================================================================
            from io import StringIO

            data_str = "\n".join(data_lines)

            df = pd.read_csv(
                StringIO(data_str), sep=r"\s+", names=header_columns, engine="python"
            )

            # Convert all columns to numeric
            for col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            print(f"✅ Parsed {len(df)} rows with columns: {list(df.columns)[:5]}...")
            return df

        except Exception as e:
            print(f"❌ Turbostat parse error: {e}")
            import traceback

            traceback.print_exc()
            return None

    def get_column_mapping(self) -> Dict[str, str]:
        """
        Get mapping from turbostat column names to internal metric names.

        This uses the 'columns' section from hw_config.json which maps
        our internal names (like 'cpu_util_percent') to turbostat column names.

        Returns:
            Dictionary: turbostat_column_name -> internal_metric_name
        """
        # The column_map is already stored as self.column_map
        # But it's in the direction: internal_name -> turbostat_column
        # We need the reverse for lookup

        reverse_map = {}
        for internal_name, turbostat_col in self.column_map.items():
            reverse_map[turbostat_col] = internal_name

        return reverse_map

    def compute_summary(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Compute summary statistics using reverse_map for canonical names.
        """
        summary = {}

        if df is None or df.empty:
            return summary

        # Use all rows (turbostat already gives package summary)
        for col in df.columns:
            # Map to internal canonical name if available
            internal_name = self.reverse_map.get(col, col)

            try:
                numeric_values = pd.to_numeric(df[col], errors="coerce").dropna()

                if len(numeric_values) > 0:
                    # Store statistics with canonical names
                    summary[f"{internal_name}_mean"] = numeric_values.mean()
                    summary[f"{internal_name}_std"] = numeric_values.std()
                    summary[f"{internal_name}_min"] = numeric_values.min()
                    summary[f"{internal_name}_max"] = numeric_values.max()
                    summary[f"{internal_name}_median"] = numeric_values.median()

                    # Special handling for frequencies (keep for backward compatibility)
                    if col == "Bzy_MHz":
                        summary["frequency_busy_mhz"] = numeric_values.mean()
                        summary["frequency_busy_min"] = numeric_values.min()
                        summary["frequency_busy_max"] = numeric_values.max()

                    if col == "Avg_MHz":
                        summary["frequency_avg_mhz"] = numeric_values.mean()
                        summary["frequency_avg_min"] = numeric_values.min()
                        summary["frequency_avg_max"] = numeric_values.max()

            except Exception as e:
                logger.debug(f"Could not process column {col}: {e}")

        return summary

    def save_raw_data(self, df: pd.DataFrame, experiment_id: str) -> Optional[str]:
        """
        Save raw turbostat data to CSV for reviewer requests.

        In top-tier publications, reviewers may ask to see raw data.
        This method allows saving the full dataframe for later analysis.

        Args:
            df: DataFrame to save
            experiment_id: Unique identifier for this experiment

        Returns:
            Path to saved file or None if save failed
        """
        if df is None or df.empty:
            logger.warning("No data to save")
            return None

        # Create data directory if it doesn't exist
        data_dir = Path("data/turbostat_raw")
        data_dir.mkdir(parents=True, exist_ok=True)

        # Save with timestamp and experiment ID
        filename = data_dir / f"turbostat_{experiment_id}_{int(time.time())}.csv"

        try:
            df.to_csv(filename, index=False)
            logger.info(f"Raw turbostat data saved to {filename}")
            return str(filename)
        except Exception as e:
            logger.error(f"Failed to save raw data: {e}")
            return None

    def read_metrics(self, duration_ms: int = 100) -> PowerState:
        """
        Run turbostat for a specified duration and return parsed metrics.

        This is the original snapshot method kept for backward compatibility.
        For new code, use continuous monitoring instead.

        Args:
            duration_ms: Measurement duration in milliseconds

        Returns:
            PowerState object with values from the first data row
        """
        power_state = PowerState()

        if not self.available or not self.turbostat_path:
            return power_state

        # Get columns to read
        columns_to_read = list(self.column_map.values())
        if not columns_to_read:
            return power_state

        # turbostat needs at least 100ms
        interval = max(0.1, duration_ms / 1000)

        # Build command for snapshot measurement
        cmd = [
            self.turbostat_path,
            "--quiet",
            "--Summary",
            "--show",
            ",".join(columns_to_read),
            "--interval",
            str(interval),
            "sleep",
            str(interval + 0.1),
        ]

        try:
            # Run turbostat and capture output
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=interval + 2
            )

            if result.returncode != 0:
                return power_state

            # Try stdout first, then stderr
            df = self._parse_turbostat_output(result.stdout)
            if df is None or df.empty:
                df = self._parse_turbostat_output(result.stderr)

            if df is not None and not df.empty:
                # Convert to numeric
                for col in df.columns:
                    try:
                        df[col] = pd.to_numeric(df[col])
                    except:
                        pass

                # Get package-level row for snapshot
                if "CPU" in df.columns:
                    package_df = df[df["CPU"] == "-"]
                    if not package_df.empty and len(package_df) > 0:
                        row = package_df.iloc[0]
                    else:
                        row = df.iloc[0]
                else:
                    row = df.iloc[0]

                # Map columns to PowerState fields
                for col in df.columns:
                    if col in self.reverse_map:
                        metric = self.reverse_map[col]
                        value = row[col]
                        self._set_metric(power_state, metric, value)

        except Exception as e:
            logger.error(f"turbostat snapshot error: {e}")

        return power_state

    def _parse_turbostat_output(self, output: str) -> Optional[pd.DataFrame]:
        """
        Parse snapshot turbostat output (for backward compatibility).

        Args:
            output: Raw turbostat output

        Returns:
            DataFrame with parsed data or None
        """
        if not output or len(output.strip()) < 10:
            return None

        try:
            lines = output.strip().split("\n")

            # Find header line
            header_idx = -1
            for i, line in enumerate(lines):
                if any(col in line for col in ["Avg_MHz", "Busy%", "Bzy_MHz", "Core"]):
                    header_idx = i
                    break

            if header_idx == -1:
                return None

            # Parse data lines
            data_lines = "\n".join(lines[header_idx:])
            df = pd.read_csv(
                io.StringIO(data_lines),
                sep="\t",
                skipinitialspace=True,
                engine="python",
            )
            return df

        except Exception as e:
            logger.debug(f"Failed to parse turbostat output: {e}")
            return None

    def _set_metric(self, power_state: PowerState, metric: str, value: float) -> None:
        """
        Map a parsed metric value to the appropriate field in PowerState.

        Args:
            power_state: The object to update
            metric: Our internal metric name (e.g., 'c1_residency')
            value: The numeric value
        """
        try:
            # Skip NaN values
            if pd.isna(value):
                return

            # C-state residencies (e.g., c1_residency, c6_residency)
            if metric.startswith("c") and "residency" in metric:
                c_state = metric.split("_")[0].upper()
                power_state.c_state_residencies[c_state] = float(value)

            # iGPU metrics
            elif metric == "gfx_rc6":
                power_state.igpu_rc6_percent = float(value)
            elif metric == "gfx_freq":
                power_state.igpu_frequency_mhz = float(value)

            # Temperature
            elif metric == "package_temp":
                power_state.package_temperature_celsius = float(value)

            # Capture both frequency metrics
            elif metric == "bzy_mhz":
                # Frequency when CPU is busy
                power_state.frequencies_mhz[0] = float(value)
            elif metric == "avg_mhz":
                # Average frequency including idle time
                power_state.avg_frequency_mhz = float(value)
            elif metric == "tsc_mhz":
                # Time Stamp Counter frequency (constant)
                power_state.tsc_frequency_mhz = float(value)

            # Power estimates
            elif metric == "pkg_watts":
                power_state.package_power_watts = float(value)
            elif metric == "cor_watts":
                power_state.core_power_watts = float(value)
            elif metric == "gfx_watts":
                power_state.gpu_power_watts = float(value)
            elif metric == "ram_watts":
                power_state.ram_power_watts = float(value)

        except Exception as e:
            logger.debug(f"Failed to set metric {metric}={value}: {e}")

    def __str__(self) -> str:
        """String representation for debugging."""
        status = "available" if self.available else "unavailable"
        hybrid = " (Hybrid)" if self.cpu_topology.get("has_hybrid") else ""
        return f"TurbostatReader({status}{hybrid}, {len(self.column_map)} columns)"


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    """
    Example: Test turbostat reader with both snapshot and continuous modes.
    """
    import json

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("\n" + "=" * 70)
    print("TURBOSTAT READER TEST")
    print("=" * 70)

    # Load hardware configuration
    config_path = Path("config/hw_config.json")
    if not config_path.exists():
        print("❌ Config file not found!")
        sys.exit(1)

    with open(config_path) as f:
        config = json.load(f)
    print(f"✅ Loaded config from {config_path}")

    # Create reader instance
    reader = TurbostatReader(config)
    print(f"📊 {reader}")

    # Print turbostat version
    if reader.turbostat_version:
        print(f"📌 Turbostat version: {reader.turbostat_version}")

    # Print CPU topology
    if reader.cpu_topology.get("has_hybrid"):
        print(
            f"🔧 Hybrid CPU detected: {len(reader.cpu_topology.get('p_cores', []))} P-cores, "
            f"{len(reader.cpu_topology.get('e_cores', []))} E-cores"
        )

    # Test 1: Snapshot mode
    print("\n" + "=" * 70)
    print("TEST 1: Snapshot Mode (500ms)")
    print("=" * 70)
    power_state = reader.read_metrics(duration_ms=500)

    print("\nC-STATE RESIDENCIES:")
    if power_state.c_state_residencies:
        for c, v in sorted(power_state.c_state_residencies.items()):
            print(f"   {c}: {v:.2f}%")

    print(f"\nFREQUENCY (Busy): {power_state.frequencies_mhz.get(0, 0):.0f} MHz")
    if hasattr(power_state, "avg_frequency_mhz") and power_state.avg_frequency_mhz:
        print(f"FREQUENCY (Average): {power_state.avg_frequency_mhz:.0f} MHz")
    print(f"TEMPERATURE: {power_state.package_temperature_celsius:.1f}°C")

    # Test 2: Continuous monitoring
    print("\n" + "=" * 70)
    print("TEST 2: Continuous Monitoring (3 seconds)")
    print("=" * 70)

    print("\n📝 Starting continuous monitoring...")
    reader.start_monitoring()

    # Simulate workload
    for i in range(3):
        time.sleep(1)
        print(f"   Workload running... {i+1}/3")

    # Stop and get data
    print("\n📝 Stopping monitoring...")
    data = reader.stop_monitoring()

    print(
        f"\n📊 Collected {data['num_samples']} samples over {data['duration_seconds']:.2f}s"
    )

    # Print summary statistics
    print("\n📊 Summary Statistics (Package-Level Only):")
    summary = data["summary"]

    # Group by metric name for cleaner display
    metrics = {}
    for key, value in summary.items():
        base = key.rsplit("_", 1)[0]
        stat = key.rsplit("_", 1)[1] if "_" in key else "value"
        if base not in metrics:
            metrics[base] = {}
        metrics[base][stat] = value

    for metric, stats in sorted(metrics.items()):
        if metric and not metric.startswith("Time"):
            print(f"\n   {metric}:")
            if "mean" in stats:
                print(f"      Mean:   {stats['mean']:.2f}")
            if "median" in stats:
                print(f"      Median: {stats['median']:.2f}")
            if "std" in stats:
                print(f"      Std:    {stats['std']:.2f}")
            if "min" in stats:
                print(f"      Min:    {stats['min']:.2f}")
            if "max" in stats:
                print(f"      Max:    {stats['max']:.2f}")

    # Optional: Save raw data
    if data["dataframe"] is not None:
        filename = reader.save_raw_data(data["dataframe"], "test_run")
        if filename:
            print(f"\n💾 Raw data saved to: {filename}")

    print("\n" + "=" * 70)
    print("✅ Test complete!")
    print("=" * 70)
