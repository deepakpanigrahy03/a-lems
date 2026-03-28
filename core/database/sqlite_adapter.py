#!/usr/bin/env python3
"""
================================================================================
SQLITE ADAPTER – SQLite implementation of DatabaseInterface
================================================================================

PURPOSE:
    Implements the DatabaseInterface for SQLite. This adapter handles all
    SQLite-specific connection details, query execution, and data insertion.

WHY THIS EXISTS:
    - Separates SQLite-specific code from the rest of the application
    - Makes it easy to switch to PostgreSQL later
    - Centralizes all SQLite pragmas and settings
    - Implements the adapter pattern for database flexibility

UNITS AND ASSUMPTIONS:
    - All time values are in nanoseconds
    - All energy values are in microjoules
    - SQLite connection uses Row factory for dict-like access
    - Foreign keys must be enabled for data integrity
    - WAL journal mode for better concurrency

AUTHOR: Deepak Panigrahy
================================================================================
"""

import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .base import DatabaseError, DatabaseInterface
from .schema import (CREATE_CPU_SAMPLES, CREATE_ENERGY_SAMPLES,
                     CREATE_ENVIRONMENT_CONFIG, CREATE_EVENTS_INDEXES,
                     CREATE_EXPERIMENTS, CREATE_HARDWARE_CONFIG,
                     CREATE_IDLE_BASELINES, CREATE_INTERRUPT_SAMPLES,
                     CREATE_LLM_INTERACTIONS, CREATE_ML_VIEW,
                     CREATE_ORCHESTRATION_ANALYSIS,
                     CREATE_ORCHESTRATION_EVENTS, CREATE_RUNS,
                     CREATE_RUNS_INDEXES, CREATE_TAX_INDEXES,
                     CREATE_TAX_SUMMARY, TASK_CATEGORIES_SCHEMA,
                     THERMAL_SAMPLES_SCHEMA, CREATE_RESEARCH_METRICS_VIEW, ENERGY_SAMPLES_WITH_POWER_VIEW)


class SQLiteAdapter(DatabaseInterface):
    """
    SQLite implementation of the database interface.

    This adapter handles all SQLite-specific operations including:
    - Connection management with appropriate pragmas
    - Transaction handling
    - Data insertion with SQLite-specific methods (lastrowid)
    - Batch inserts for performance
    - Schema creation with IF NOT EXISTS
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize SQLite adapter with configuration.

        Args:
            config: Dictionary with SQLite settings:
                - path: Database file path
                - journal_mode: 'WAL', 'DELETE', etc. (default: 'WAL')
                - timeout: Connection timeout in seconds (default: 30)
                - detect_types: Enable type detection (default: 1)
        """
        self.config = config
        self.conn = None
        self._in_transaction = False

        # Set defaults
        self.config.setdefault("path", "data/experiments.db")
        self.config.setdefault("journal_mode", "WAL")
        self.config.setdefault("timeout", 30)
        self.config.setdefault("detect_types", 1)

    # ========================================================================
    # 1. CONNECTION MANAGEMENT
    # ========================================================================

    def connect(self) -> None:
        """Establish SQLite connection with proper settings."""
        try:
            db_path = self.config.get("path")
            timeout = self.config.get("timeout")

            # Ensure directory exists
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

            # Connect with timeout
            self.conn = sqlite3.connect(
                db_path,
                timeout=timeout,
                detect_types=self.config.get("detect_types"),
                isolation_level=None,
            )

            # Use Row factory for dictionary-like access
            self.conn.row_factory = sqlite3.Row

            # Enable foreign key support (critical for data integrity)
            self.conn.execute("PRAGMA foreign_keys = ON")

            # Set journal mode for better concurrency
            journal_mode = self.config.get("journal_mode")
            self.conn.execute(f"PRAGMA journal_mode = {journal_mode}")

            # Set other performance pragmas
            self.conn.execute("PRAGMA synchronous = NORMAL")
            self.conn.execute("PRAGMA cache_size = -2000")  # 2MB cache

        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to connect to SQLite: {e}")

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute(
        self, query: str, params: Union[tuple, dict, None] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a query and return results as dictionaries.

        Args:
            query: SQL query string with placeholders
            params: Parameters for the query

        Returns:
            List of dictionaries, each representing a row
        """
        if not self.conn:
            self.connect()

        try:
            if params is None:
                cursor = self.conn.execute(query)
            else:
                cursor = self.conn.execute(query, params)

            # Convert rows to dictionaries
            return [dict(row) for row in cursor.fetchall()]

        except sqlite3.Error as e:
            raise DatabaseError(f"Query execution failed: {e}\nQuery: {query}")

    def execute_many(self, query: str, params_list: List[Union[tuple, dict]]) -> int:
        """
        Execute a query multiple times with different parameters.

        Args:
            query: SQL query string
            params_list: List of parameter sets

        Returns:
            Number of rows affected
        """
        if not self.conn:
            raise DatabaseError("Database not connected. Call connect() first.")

        try:
            cursor = self.conn.executemany(query, params_list)
            return cursor.rowcount

        except sqlite3.Error as e:
            raise DatabaseError(f"Batch execution failed: {e}")

    # ========================================================================
    # 2. TRANSACTION HANDLING
    # ========================================================================

    def transaction(self) -> "SQLiteAdapter":
        """Begin a transaction. Legacy method - use context manager instead."""
        if not self.conn:
            self.connect()

        self.conn.execute("BEGIN TRANSACTION")
        self._in_transaction = True
        return self

    def commit(self) -> None:
        """Commit the current transaction."""
        if self._in_transaction and self.conn:
            self.conn.commit()
            self._in_transaction = False

    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self._in_transaction and self.conn:
            self.conn.rollback()
            self._in_transaction = False

    # ========================================================================
    # 3. CONTEXT MANAGER SUPPORT
    # ========================================================================

    def __enter__(self):
        """Enter context manager - begin transaction."""
        if not self.conn:
            self.connect()
        self.conn.execute("BEGIN")
        self._in_transaction = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - commit or rollback."""
        try:
            if exc_type is None:
                # No exception - commit
                if self._in_transaction:
                    self.conn.commit()
            else:
                # Exception occurred - rollback
                if self._in_transaction:
                    self.conn.rollback()
        finally:
            self._in_transaction = False

    # ========================================================================
    # 4. TABLE CREATION
    # ========================================================================

    def create_tables(self) -> None:
        """Create all tables if they don't exist."""
        if not self.conn:
            self.connect()

        # Execute each CREATE statement in order - NO transaction wrapper
        self.conn.execute(CREATE_EXPERIMENTS)
        self.conn.execute(CREATE_HARDWARE_CONFIG)
        self.conn.execute(CREATE_IDLE_BASELINES)
        self.conn.execute(CREATE_RUNS)
        self.conn.executescript(CREATE_RUNS_INDEXES)
        self.conn.execute(CREATE_ORCHESTRATION_EVENTS)
        self.conn.executescript(CREATE_EVENTS_INDEXES)
        self.conn.execute(CREATE_TAX_SUMMARY)
        self.conn.executescript(CREATE_TAX_INDEXES)
        self.conn.executescript(CREATE_ENERGY_SAMPLES)
        self.conn.executescript(CREATE_CPU_SAMPLES)
        self.conn.executescript(CREATE_INTERRUPT_SAMPLES)
        self.conn.executescript(TASK_CATEGORIES_SCHEMA)
        self.conn.executescript(THERMAL_SAMPLES_SCHEMA)
        self.conn.execute(CREATE_ML_VIEW)
        self.conn.execute(CREATE_ORCHESTRATION_ANALYSIS)
        self.conn.execute(CREATE_ENVIRONMENT_CONFIG)
        self.conn.executescript(CREATE_LLM_INTERACTIONS)
        self.conn.execute(CREATE_RESEARCH_METRICS_VIEW)
        self.conn.executescript(ENERGY_SAMPLES_WITH_POWER_VIEW)

        # Commit explicitly (DDL should be committed)
        self.conn.commit()

    # ========================================================================
    # 5. CORE DATA INSERTION
    # ========================================================================

    def insert_experiment(self, experiment_data: Dict[str, Any]) -> int:
        """
        Insert a new experiment record with all fields including session tracking.
        """
        if not self.conn:
            self.connect()
        cursor = self.conn.execute(
            """
            INSERT INTO experiments (
                name, description, workflow_type, model_name, provider,
                task_name, country_code, group_id, status, started_at, runs_total,optimization_enabled,
                hw_id, env_id                 
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                experiment_data.get("name", "unnamed"),
                experiment_data.get("description", ""),
                experiment_data.get("workflow_type"),
                experiment_data.get("model_name"),
                experiment_data.get("provider"),
                experiment_data.get("task_name"),
                experiment_data.get("country_code", "US"),
                experiment_data.get("group_id"),
                experiment_data.get("status", "pending"),
                experiment_data.get("started_at"),
                experiment_data.get("runs_total"),
                experiment_data.get("optimization_enabled", 0),
                experiment_data.get("hw_id"),
                experiment_data.get("env_id"),
            ),
        )
        return cursor.lastrowid

    def insert_hardware(self, hardware_data: Dict[str, Any]) -> int:
        """
        Insert hardware configuration or return existing ID.
        Now supports hardware fingerprint fields and updates old records.
        """

        # Get hardware hash (may be None for old data)
        hw_hash = hardware_data.get("hardware_hash")

        # ============================================================
        # STEP 1: Try to find by hash (new method)
        # ============================================================
        if hw_hash:
            results = self.execute(
                "SELECT hw_id FROM hardware_config WHERE hardware_hash = ?", (hw_hash,)
            )
            if results:
                old_hw_id = results[0]["hw_id"]

        # ============================================================
        # STEP 2: Try to find by CPU model + cores (old record without hash)
        # ============================================================
        cpu_model = hardware_data.get("cpu_model")
        cpu_cores = hardware_data.get("cpu_cores")

        if cpu_model and cpu_cores:
            results = self.execute(
                "SELECT hw_id FROM hardware_config WHERE cpu_model = ? AND cpu_cores = ?",
                (cpu_model, cpu_cores),
            )
            if results:
                old_hw_id = results[0]["hw_id"]

                # UPDATE the old record with new columns
                self.execute(
                    """
                    UPDATE hardware_config SET
                        hardware_hash = ?,
                        hostname = ?,
                        cpu_model = ?,
                        cpu_cores = ?,
                        cpu_threads = ?,
                        cpu_architecture = ?,
                        cpu_vendor = ?,
                        cpu_family = ?,
                        cpu_model_id = ?,
                        cpu_stepping = ?,
                        has_avx2 = ?,
                        has_avx512 = ?,
                        has_vmx = ?,
                        gpu_model = ?,
                        gpu_driver = ?,
                        gpu_count = ?,
                        gpu_power_available = ?,
                        ram_gb = ?,
                        kernel_version = ?,
                        microcode_version = ?,
                        rapl_domains = ?,
                        rapl_has_dram = ?,
                        rapl_has_uncore = ?,
                        system_manufacturer = ?,
                        system_product = ?,
                        system_type = ?,
                        virtualization_type = ?,
                        detected_at = ?
                    WHERE hw_id = ?
                """,
                    (
                        hardware_data.get("hardware_hash"),
                        hardware_data.get("hostname"),
                        hardware_data.get("cpu_model"),
                        hardware_data.get("cpu_cores"),
                        hardware_data.get("cpu_threads"),
                        hardware_data.get("cpu_architecture"),
                        hardware_data.get("cpu_vendor"),
                        hardware_data.get("cpu_family"),
                        hardware_data.get("cpu_model_id"),
                        hardware_data.get("cpu_stepping"),
                        hardware_data.get("has_avx2"),
                        hardware_data.get("has_avx512"),
                        hardware_data.get("has_vmx"),
                        hardware_data.get("gpu_model"),
                        hardware_data.get("gpu_driver"),
                        hardware_data.get("gpu_count"),
                        hardware_data.get("gpu_power_available"),
                        hardware_data.get("ram_gb"),
                        hardware_data.get("kernel_version"),
                        hardware_data.get("microcode_version"),
                        hardware_data.get("rapl_domains"),
                        hardware_data.get("rapl_has_dram"),
                        hardware_data.get("rapl_has_uncore"),
                        hardware_data.get("system_manufacturer"),
                        hardware_data.get("system_product"),
                        hardware_data.get("system_type"),
                        hardware_data.get("virtualization_type"),
                        hardware_data.get("detected_at"),
                        old_hw_id,
                    ),
                )

                return old_hw_id

        # ============================================================
        # STEP 3: No match found - insert new record
        # ============================================================
        cursor = self.conn.execute(
            """
            INSERT INTO hardware_config (
                hardware_hash, hostname, cpu_model, cpu_cores, cpu_threads,
                cpu_architecture, cpu_vendor, cpu_family, cpu_model_id, cpu_stepping,
                has_avx2, has_avx512, has_vmx,
                gpu_model, gpu_driver, gpu_count, gpu_power_available,
                ram_gb, kernel_version, microcode_version,
                rapl_domains, rapl_has_dram, rapl_has_uncore,
                system_manufacturer, system_product, system_type, virtualization_type,
                detected_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                hardware_data.get("hardware_hash"),
                hardware_data.get("hostname"),
                hardware_data.get("cpu_model"),
                hardware_data.get("cpu_cores"),
                hardware_data.get("cpu_threads"),
                hardware_data.get("cpu_architecture"),
                hardware_data.get("cpu_vendor"),
                hardware_data.get("cpu_family"),
                hardware_data.get("cpu_model_id"),
                hardware_data.get("cpu_stepping"),
                hardware_data.get("has_avx2"),
                hardware_data.get("has_avx512"),
                hardware_data.get("has_vmx"),
                hardware_data.get("gpu_model"),
                hardware_data.get("gpu_driver"),
                hardware_data.get("gpu_count"),
                hardware_data.get("gpu_power_available"),
                hardware_data.get("ram_gb"),
                hardware_data.get("kernel_version"),
                hardware_data.get("microcode_version"),
                hardware_data.get("rapl_domains"),
                hardware_data.get("rapl_has_dram"),
                hardware_data.get("rapl_has_uncore"),
                hardware_data.get("system_manufacturer"),
                hardware_data.get("system_product"),
                hardware_data.get("system_type"),
                hardware_data.get("virtualization_type"),
                hardware_data.get("detected_at"),
            ),
        )

        return cursor.lastrowid

    # ============================================================
    # NEW METHOD - ENVIRONMENT CONFIG (4 spaces indentation)
    # ============================================================
    def insert_environment_config(self, env_data: dict) -> int:
        """
        Insert environment configuration or return existing ID.

        Args:
            env_data: Dictionary with environment fields:
                - env_hash: Unique environment fingerprint
                - python_version, python_implementation
                - os_name, os_version, kernel_version
                - git_commit, git_branch, git_dirty
                - numpy_version, torch_version, transformers_version
                - container_runtime, container_image

        Returns:
            env_id: Primary key of existing or new record
        """
        env_hash = env_data.get("env_hash")

        # Check if exists
        if env_hash:
            results = self.execute(
                "SELECT env_id FROM environment_config WHERE env_hash = ?", (env_hash,)
            )
            if results:
                return results[0]["env_id"]

        # Insert new
        cursor = self.conn.execute(
            """
            INSERT INTO environment_config (
                env_hash, python_version, python_implementation,
                os_name, os_version, kernel_version,
                git_commit, git_branch, git_dirty,
                numpy_version, torch_version, transformers_version,
                container_runtime, container_image
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                env_data.get("env_hash"),
                env_data.get("python_version"),
                env_data.get("python_implementation"),
                env_data.get("os_name"),
                env_data.get("os_version"),
                env_data.get("kernel_version"),
                env_data.get("git_commit"),
                env_data.get("git_branch"),
                env_data.get("git_dirty"),
                env_data.get("numpy_version"),
                env_data.get("torch_version"),
                env_data.get("transformers_version"),
                env_data.get("container_runtime"),
                env_data.get("container_image"),
            ),
        )

        return cursor.lastrowid

    def insert_baseline(self, baseline_data: Dict[str, Any]) -> str:
        """
        Insert an idle baseline measurement.
        """
        print(f"🔍 DEBUG - insert_baseline received: {baseline_data.keys()}")
        print(f"🔍 DEBUG - power_watts: {baseline_data.get('power_watts')}")

        if not self.conn:
            self.connect()

        baseline_id = (
            baseline_data.get("baseline_id")
            or f"baseline_{int(datetime.now().timestamp())}"
        )

        # Extract nested dictionaries
        power = baseline_data.get("power_watts", {})
        std_dev = baseline_data.get("std_dev_watts", {})
        metadata = baseline_data.get("metadata", {})
        # ========== ADD THESE DEBUG LINES ==========
        print(f"🔍 DEBUG sqladapter- metadata in insert_baseline: {metadata}")
        print(
            f"🔍 DEBUG sqladapter- governor from metadata: {metadata.get('governor')}"
        )
        print(f"🔍 DEBUG sqladapter- turbo from metadata: {metadata.get('turbo')}")
        # ===========================================

        self.conn.execute(
            """
            INSERT INTO idle_baselines
            (baseline_id, timestamp, package_power_watts, core_power_watts,
             uncore_power_watts, dram_power_watts, duration_seconds, sample_count,
             package_std, core_std, uncore_std, dram_std,
             governor, turbo, background_cpu, process_count, method)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                baseline_id,
                baseline_data.get("timestamp"),
                power.get("package-0"),
                power.get("core"),
                power.get("uncore"),
                power.get("dram", 0),
                baseline_data.get("duration_seconds"),
                baseline_data.get("sample_count"),
                std_dev.get("package-0"),
                std_dev.get("core"),
                std_dev.get("uncore"),
                std_dev.get("dram", 0),
                metadata.get("governor"),
                metadata.get("turbo"),
                metadata.get("background_cpu"),
                metadata.get("processes"),
                baseline_data.get("method", "idle_measurement"),
            ),
        )

        return baseline_id

    def insert_run(self, run_data: Dict[str, Any]) -> int:
        """
        Insert a single run with all its metrics.

        This is a simplified version - the full 80-column insert
        will be implemented in the repositories split.
        """
        # This is a placeholder - full implementation will be in repositories/runs.py
        raise NotImplementedError("Full run insertion will be in repositories/runs.py")

    def insert_orchestration_events(
        self, run_id: int, events: List[Dict[str, Any]]
    ) -> None:
        """Insert orchestration events for a run."""
        if not events:
            return

        # No transaction wrapper - caller manages transactions
        for ev in events:
            self.conn.execute(
                """
                INSERT INTO orchestration_events
                (run_id, step_index, phase, event_type, start_time_ns, end_time_ns,
                 duration_ns, power_watts, cpu_util_percent, interrupt_rate,
                 event_energy_uj, tax_contribution_uj, tax_percent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    run_id,
                    ev.get("step_index"),
                    ev.get("phase"),
                    ev.get("event_type"),
                    ev.get("start_time_ns"),
                    ev.get("end_time_ns"),
                    ev.get("duration_ns"),
                    ev.get("power_watts"),
                    ev.get("cpu_util_percent"),
                    ev.get("interrupt_rate"),
                    ev.get("event_energy_uj"),
                    ev.get("tax_contribution_uj"),
                    ev.get("tax_percent"),
                ),
            )

    def insert_energy_samples(self, run_id: int, samples: List[Dict[str, Any]]) -> None:
        """Insert high-frequency RAPL samples."""
        if not samples:
            return

        # No transaction wrapper - caller manages transactions
        for s in samples:
            self.conn.execute(
                """
                INSERT INTO energy_samples
                (run_id, timestamp_ns, pkg_energy_uj, core_energy_uj, uncore_energy_uj, dram_energy_uj)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    run_id,
                    s.get("timestamp_ns"),
                    s.get("pkg_energy_uj"),
                    s.get("core_energy_uj"),
                    s.get("uncore_energy_uj"),
                    s.get("dram_energy_uj"),
                ),
            )

    def insert_cpu_samples(self, run_id: int, samples: List[Dict[str, Any]]) -> None:
        """Insert CPU samples from turbostat."""
        if not samples:
            return

        # No transaction wrapper - caller manages transactions
        for s in samples:
            self.conn.execute(
                """
                INSERT INTO cpu_samples
                (run_id, timestamp_ns, 
                 cpu_util_percent, cpu_busy_mhz, cpu_avg_mhz,
                 c1_residency, c2_residency, c3_residency, c6_residency, c7_residency,
                 pkg_c8_residency, pkg_c9_residency, pkg_c10_residency,
                 package_power, dram_power,
                 gpu_rc6,
                 package_temp, ipc,
                 extra_metrics_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    run_id,
                    s.get("timestamp_ns"),
                    s.get("cpu_util_percent"),
                    s.get("cpu_busy_mhz"),
                    s.get("cpu_avg_mhz"),
                    s.get("c1_residency"),
                    s.get("c2_residency"),
                    s.get("c3_residency"),
                    s.get("c6_residency"),
                    s.get("c7_residency"),
                    s.get("pkg_c8_residency"),
                    s.get("pkg_c9_residency"),
                    s.get("pkg_c10_residency"),
                    s.get("package_power"),
                    s.get("dram_power"),
                    s.get("gpu_rc6"),
                    s.get("package_temp"),
                    s.get("ipc"),
                    s.get("extra_metrics_json"),
                ),
            )

    def insert_interrupt_samples(
        self, run_id: int, samples: List[Dict[str, Any]]
    ) -> None:
        """Insert interrupt samples."""
        if not samples:
            return

        # No transaction wrapper - caller manages transactions
        for s in samples:
            self.conn.execute(
                """
                INSERT INTO interrupt_samples
                (run_id, timestamp_ns, interrupts_per_sec)
                VALUES (?, ?, ?)
            """,
                (run_id, s.get("timestamp_ns"), s.get("interrupts_per_sec")),
            )

    def create_tax_summaries(self, exp_id: int) -> None:
        """
        Create tax summary entries for all agentic runs in an experiment.
        """
        # Get all runs with their run_number and workflow_type
        runs_info = self.execute(
            """
            SELECT r.run_id, r.run_number, e.workflow_type
            FROM runs r
            JOIN experiments e ON r.exp_id = e.exp_id
            WHERE r.exp_id = ?
        """,
            (exp_id,),
        )

        # Build mapping: run_number -> {linear: run_id, agentic: run_id}
        pairs = {}
        for r in runs_info:
            num = r["run_number"]
            if num not in pairs:
                pairs[num] = {}
            pairs[num][r["workflow_type"]] = r["run_id"]

        # For each pair that has both linear and agentic, compute tax
        # No transaction wrapper - caller manages transactions
        for num, pair in pairs.items():
            linear_id = pair.get("linear")
            agentic_id = pair.get("agentic")
            if linear_id and agentic_id:
                # Get dynamic energies
                energies = self.execute(
                    "SELECT run_id, dynamic_energy_uj FROM runs WHERE run_id IN (?, ?)",
                    (linear_id, agentic_id),
                )
                energy_dict = {e["run_id"]: e["dynamic_energy_uj"] for e in energies}

                linear_uj = energy_dict.get(linear_id, 0)
                agentic_uj = energy_dict.get(agentic_id, 0)
                tax_uj = agentic_uj - linear_uj
                tax_percent = (tax_uj / agentic_uj * 100) if agentic_uj > 0 else 0

                self.conn.execute(
                    """
                    INSERT INTO orchestration_tax_summary
                    (linear_run_id, agentic_run_id, linear_dynamic_uj, agentic_dynamic_uj,
                     orchestration_tax_uj, tax_percent)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (linear_id, agentic_id, linear_uj, agentic_uj, tax_uj, tax_percent),
                )

    def insert_llm_interaction(self, interaction_data: dict) -> int:
        """Insert an LLM interaction record with all research metrics."""
        cursor = self.conn.execute(
            """
            INSERT INTO llm_interactions (
                run_id, step_index, workflow_type,
                prompt, response,
                model_name, provider,
                prompt_tokens, completion_tokens, total_tokens,
                api_latency_ms, compute_time_ms,
                app_throughput_kbps,
                total_time_ms, preprocess_ms, non_local_ms, local_compute_ms, postprocess_ms,
                cpu_percent_during_wait,
                bytes_sent_approx, bytes_recv_approx, tcp_retransmits,
                error_message, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                interaction_data.get("run_id"),
                interaction_data.get("step_index"),
                interaction_data.get("workflow_type"),
                interaction_data.get("prompt"),
                interaction_data.get("response"),
                interaction_data.get("model_name"),
                interaction_data.get("provider"),
                interaction_data.get("prompt_tokens", 0),
                interaction_data.get("completion_tokens", 0),
                interaction_data.get("total_tokens", 0),
                interaction_data.get("api_latency_ms", 0),
                interaction_data.get("compute_time_ms", 0),
                interaction_data.get("app_throughput_kbps", 0),
                interaction_data.get("total_time_ms", 0),
                interaction_data.get("preprocess_ms", 0),
                interaction_data.get("non_local_ms", 0),
                interaction_data.get("local_compute_ms", 0),
                interaction_data.get("postprocess_ms", 0),
                interaction_data.get("cpu_percent_during_wait", 0),
                interaction_data.get("bytes_sent_approx", 0),
                interaction_data.get("bytes_recv_approx", 0),
                interaction_data.get("tcp_retransmits", 0),
                interaction_data.get("error_message", ""),
                interaction_data.get("status", "success")

            ),
        )
        return cursor.lastrowid

    # ========================================================================
    # 6. QUERY METHODS
    # ========================================================================

    def get_run(self, run_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve a single run by ID."""
        result = self.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        return result[0] if result else None

    def get_runs_by_experiment(self, exp_id: int) -> List[Dict[str, Any]]:
        """Retrieve all runs for an experiment."""
        if not self.conn:
            self.connect()  # Auto-reconnect if needed
        return self.execute(
            "SELECT * FROM runs WHERE exp_id = ? ORDER BY run_number", (exp_id,)
        )

    def get_tax_summaries(self, exp_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Retrieve tax summaries, optionally filtered by experiment."""
        if exp_id is None:
            return self.execute("SELECT * FROM orchestration_tax_summary")

        return self.execute(
            """
            SELECT ots.* FROM orchestration_tax_summary ots
            JOIN runs r ON ots.linear_run_id = r.run_id
            WHERE r.exp_id = ?
        """,
            (exp_id,),
        )

    def get_ml_data(self, workflow: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieve flattened ML data from the ml_features view."""
        if workflow:
            return self.execute(
                "SELECT * FROM ml_features WHERE workflow_type = ?", (workflow,)
            )
        return self.execute("SELECT * FROM ml_features")
