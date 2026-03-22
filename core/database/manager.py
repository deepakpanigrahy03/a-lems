#!/usr/bin/env python3
"""
================================================================================
DATABASE MANAGER – Lightweight orchestrator for database operations
================================================================================

PURPOSE:
    Provides a unified interface to the database layer by coordinating:
    - Database adapter (SQLite/PostgreSQL)
    - Repositories for different data domains
    - Transaction management
    - Connection lifecycle

WHY THIS EXISTS:
    - Single entry point for all database operations
    - Hides complexity of repositories from callers
    - Manages adapter lifecycle
    - Follows facade pattern

AUTHOR: Deepak Panigrahy
================================================================================
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import DatabaseInterface
from .factory import DatabaseFactory
from .repositories import (EventsRepository, RunsRepository, SamplesRepository,
                           TaxRepository, ThermalRepository)


class DatabaseManager:
    """
    Main interface to the A‑LEMS database.

    This class orchestrates all database operations by delegating to:
    - A database adapter (SQLite/PostgreSQL)
    - Specialized repositories for different data domains

    Usage:
        config = config_loader.get_db_config()
        db = DatabaseManager(config)

        # Insert data
        exp_id = db.insert_experiment(experiment_data)
        run_id = db.insert_run(exp_id, hw_id, run_data)

        # Query data
        runs = db.get_runs_by_experiment(exp_id)
        ml_data = db.get_ml_data()

        # Transaction management
        with db.transaction():
            db.insert_hardware(hw_data)
            db.insert_baseline(baseline_data)
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize database manager with configuration.

        Args:
            config: Database configuration dictionary from ConfigLoader
        """
        self.config = config
        self.db: Optional[DatabaseInterface] = None
        self._connect()

        # Initialize repositories
        self.runs = RunsRepository(self.db)
        self.events = EventsRepository(self.db)
        self.samples = SamplesRepository(self.db)
        self.tax = TaxRepository(self.db)
        self.thermal = ThermalRepository(self.db)

    def _connect(self) -> None:
        """Create database adapter and establish connection."""
        self.db = DatabaseFactory.create(self.config)
        self.db.connect()

    def close(self) -> None:
        """Close database connection."""
        if self.db:
            self.db.close()

    # ========================================================================
    # Transaction Management
    # ========================================================================

    def transaction(self):
        """
        Begin a transaction context.

        Usage:
            with db.transaction():
                db.insert_experiment(data1)
                db.insert_experiment(data2)
        """
        return self.db

    def commit(self) -> None:
        """Commit the current transaction."""
        self.db.commit()

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.db.rollback()

    # ========================================================================
    # Table Management
    # ========================================================================

    def create_tables(self) -> None:
        """Create all tables if they don't exist."""
        self.db.create_tables()

    # ========================================================================
    # Core Insert Methods (Delegated to Repositories)
    # ========================================================================

    def insert_experiment(self, experiment_data: Dict[str, Any]) -> int:
        """Insert a new experiment record."""
        return self.db.insert_experiment(experiment_data)

    def insert_hardware(self, hardware_data: Dict[str, Any]) -> int:
        """Insert or retrieve hardware configuration."""
        return self.db.insert_hardware(hardware_data)

    def insert_environment_config(self, env_data: dict) -> int:
        """Insert environment config or return existing ID"""
        return self.db.insert_environment_config(env_data)

    def insert_baseline(self, baseline_data: Dict[str, Any]) -> str:
        """Insert an idle baseline measurement."""
        return self.db.insert_baseline(baseline_data)

    def insert_run(
        self, exp_id: int, hw_id: Optional[int], run_data: Dict[str, Any]
    ) -> int:
        """
        Insert a complete run with all metrics.

        Delegates to RunsRepository for the complex 80+ column insert.
        """
        return self.runs.insert_run(exp_id, hw_id, run_data)

    def insert_orchestration_events(
        self, run_id: int, events: List[Dict[str, Any]]
    ) -> None:
        """Insert orchestration events for a run."""
        self.events.insert_events(run_id, events)

    def insert_energy_samples(self, run_id: int, samples: List[Dict[str, Any]]) -> None:
        """Insert high-frequency RAPL samples."""
        self.samples.insert_energy_samples(run_id, samples)

    def insert_cpu_samples(self, run_id: int, samples: List[Dict[str, Any]]) -> None:
        """Insert CPU samples."""
        self.samples.insert_cpu_samples(run_id, samples)

    def insert_interrupt_samples(
        self, run_id: int, samples: List[Dict[str, Any]]
    ) -> None:
        """Insert interrupt samples."""
        self.samples.insert_interrupt_samples(run_id, samples)

    def create_tax_summaries(self, exp_id: int) -> None:
        """Create tax summaries for an experiment."""
        self.tax.create_tax_summaries(exp_id)

    def insert_thermal_samples(
        self, run_id: int, thermal_samples: List[Dict[str, Any]]
    ) -> None:
        """Insert thermal samples for a run."""
        self.thermal.insert_thermal_samples(run_id, thermal_samples)
    def insert_llm_interaction(self, interaction_data: dict) -> int:
        """Insert an LLM interaction record."""
        return self.db.insert_llm_interaction(interaction_data)

    # ========================================================================
    # Query Methods (Delegated to Adapter)
    # ========================================================================

    def get_run(self, run_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve a single run by ID."""
        return self.db.get_run(run_id)

    def get_runs_by_experiment(self, exp_id: int) -> List[Dict[str, Any]]:
        """Retrieve all runs for an experiment."""
        return self.db.get_runs_by_experiment(exp_id)

    def get_tax_summaries(self, exp_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Retrieve tax summaries."""
        return self.db.get_tax_summaries(exp_id)

    def get_ml_data(self, workflow: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieve flattened ML data."""
        return self.db.get_ml_data(workflow)

    def update_run_stats(self, run_id: int, stats: Dict) -> None:
        """Update run with aggregated statistics from samples."""
        self.runs.update_run_stats(run_id, stats)

    # ========================================================================
    # Context Manager Support
    # ========================================================================

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - close connection."""
        self.close()

    def create_tax_summary_for_pair(
        self,
        linear_id: int,
        agentic_id: int,
        linear_uj: int,
        agentic_uj: int,
        linear_orchestration_uj: int = 0,
        agentic_orchestration_uj: int = 0,
    ) -> None:
        """
        Create tax summary for one pair.

        Args:
            exp_id: Experiment ID
            linear_id: Linear run ID
            agentic_id: Agentic run ID
            linear_uj: Linear dynamic energy in microjoules
            agentic_uj: Agentic dynamic energy in microjoules
        """
        self.tax.create_tax_summary_for_pair(
            linear_id,
            agentic_id,
            linear_uj,
            agentic_uj,
            linear_orchestration_uj,
            agentic_orchestration_uj,
        )
