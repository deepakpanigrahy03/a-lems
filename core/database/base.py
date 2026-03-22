#!/usr/bin/env python3
"""
================================================================================
DATABASE INTERFACE – Abstract Base Class for All Database Adapters
================================================================================

PURPOSE:
    Defines a common interface that all database adapters (SQLite, PostgreSQL)
    must implement. This allows switching databases by simply changing the
    config file, without modifying any application code.

WHY THIS EXISTS:
    - To support future migration from SQLite to PostgreSQL
    - To keep database code modular and testable
    - To enforce consistent method signatures across adapters
    - To implement the adapter pattern for database flexibility

UNITS AND ASSUMPTIONS:
    - All time values are in nanoseconds
    - All energy values are in microjoules
    - All methods raise DatabaseError on failures
    - Context manager support for transactions

AUTHOR: Deepak Panigrahy
================================================================================
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from datetime import datetime


class DatabaseError(Exception):
    """Base exception for all database operations."""
    pass


class DatabaseInterface(ABC):
    """
    Abstract base class for all database adapters.
    
    Any new database adapter (SQLite, PostgreSQL, MySQL) must implement
    all methods defined here. This ensures the rest of the application
    can interact with any database seamlessly.
    
    The interface is divided into sections:
    1. Connection management
    2. Transaction handling
    3. Table creation
    4. Core data insertion
    5. Query methods
    6. Context manager support
    """
    
    # ========================================================================
    # 1. CONNECTION MANAGEMENT
    # ========================================================================
    
    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the database.
        
        This method should:
        - Create the connection using config parameters
        - Set any necessary pragmas (for SQLite)
        - Configure connection pool (for PostgreSQL)
        - Raise DatabaseError if connection fails
        
        Raises:
            DatabaseError: If connection fails
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Close database connection and release resources.
        
        This method should:
        - Close the connection gracefully
        - Release any connection pool resources
        - Be safe to call multiple times
        """
        pass
    
    @abstractmethod
    def execute(self, query: str, params: Union[tuple, dict, None] = None) -> List[Dict[str, Any]]:
        """
        Execute a raw SQL query and return results.
        
        Args:
            query: SQL query string with placeholders
            params: Parameters for the query (tuple for positional, dict for named)
            
        Returns:
            List of dictionaries, each representing a row with column names as keys
            
        Raises:
            DatabaseError: If query execution fails
            
        Example:
            results = db.execute(
                "SELECT * FROM runs WHERE exp_id = ?", 
                (exp_id,)
            )
        """
        pass
    
    @abstractmethod
    def execute_many(self, query: str, params_list: List[Union[tuple, dict]]) -> int:
        """
        Execute the same query with multiple parameter sets.
        
        Used for batch inserts where performance matters.
        
        Args:
            query: SQL query string
            params_list: List of parameter sets, each a tuple or dict
            
        Returns:
            Number of rows affected
            
        Raises:
            DatabaseError: If execution fails
        """
        pass
    
    # ========================================================================
    # 2. TRANSACTION HANDLING
    # ========================================================================
    
    @abstractmethod
    def transaction(self) -> 'DatabaseInterface':
        """
        Begin a transaction. Returns self for context manager usage.
        
        Usage:
            with db.transaction():
                db.insert_run(data1)
                db.insert_run(data2)
                
        This ensures atomicity: both inserts succeed or both fail.
        """
        pass
    
    @abstractmethod
    def commit(self) -> None:
        """Commit the current transaction."""
        pass
    
    @abstractmethod
    def rollback(self) -> None:
        """Rollback the current transaction."""
        pass
    
    # ========================================================================
    # 3. TABLE CREATION
    # ========================================================================
    
    @abstractmethod
    def create_tables(self) -> None:
        """
        Create all necessary tables if they don't exist.
        
        For SQLite, this runs CREATE TABLE IF NOT EXISTS statements.
        For PostgreSQL, this may use CREATE TABLE IF NOT EXISTS or migrations.
        
        Tables to create:
        - experiments
        - hardware_config
        - idle_baselines
        - runs
        - orchestration_events
        - orchestration_tax_summary
        - energy_samples
        - cpu_samples
        - interrupt_samples
        
        Also creates indexes and views (ml_features).
        """
        pass
    
    # ========================================================================
    # 4. CORE DATA INSERTION
    # ========================================================================
    
    @abstractmethod
    def insert_experiment(self, experiment_data: Dict[str, Any]) -> int:
        """
        Insert a new experiment record.
        
        Args:
            experiment_data: Dictionary with keys:
                - name: str - Experiment name
                - description: str (optional) - Description
                - workflow_type: str - 'linear' or 'agentic'
                - model_name: str - LLM model identifier
                - provider: str - 'cloud' or 'local'
                - task_name: str - Task identifier
                - country_code: str - ISO country code
                
        Returns:
            exp_id of the newly created experiment
        """
        pass
    
    @abstractmethod
    def insert_hardware(self, hardware_data: Dict[str, Any]) -> int:
        """
        Insert or retrieve hardware configuration.
        
        Should check for existing record (by hostname + kernel_version)
        and return existing hw_id if found.
        
        Args:
            hardware_data: Dictionary with keys:
                - hostname: str
                - cpu_model: str
                - cpu_cores: int
                - cpu_threads: int
                - ram_gb: int
                - kernel_version: str
                - microcode_version: str (optional)
                - rapl_domains: str (optional)
                
        Returns:
            hw_id of the hardware record (existing or new)
        """
        pass
    
    @abstractmethod
    def insert_baseline(self, baseline_data: Dict[str, Any]) -> str:
        """
        Insert an idle baseline measurement.
        
        Args:
            baseline_data: Dictionary with keys:
                - baseline_id: str (optional, generated if not provided)
                - timestamp: float
                - package_power_watts: float
                - core_power_watts: float
                - uncore_power_watts: float (optional)
                - dram_power_watts: float (optional)
                - duration_seconds: int
                - sample_count: int
                - package_std: float (optional)
                - core_std: float (optional)
                - governor: str
                - turbo: str ('enabled'/'disabled')
                - background_cpu: float
                - process_count: int
                - method: str
                
        Returns:
            baseline_id of the new baseline
        """
        pass
    
    @abstractmethod
    def insert_run(self, run_data: Dict[str, Any]) -> int:
        """
        Insert a single run with all its metrics.
        
        This is the main method for inserting experiment results.
        It should extract all fields from the harness output and
        compute derived metrics.
        
        Args:
            run_data: Complete run dictionary from harness containing:
                - ml_features: dict with 77+ fields
                - layer3_derived: dict (optional)
                - execution: dict (optional)
                - sustainability: dict (optional)
                - orchestration_events: list (optional)
                - energy_samples: list (optional)
                - cpu_samples: list (optional)
                - interrupt_samples: list (optional)
                
        Returns:
            run_id of the newly inserted run
        """
        pass
    
    @abstractmethod
    def insert_orchestration_events(self, run_id: int, events: List[Dict[str, Any]]) -> None:
        """
        Insert orchestration events for a run.
        
        Each event represents a phase (planning, execution, waiting, synthesis)
        or a specific action (LLM call, tool call).
        
        Args:
            run_id: Foreign key to runs table
            events: List of event dictionaries with keys:
                - step_index: int (optional)
                - phase: str ('planning'/'execution'/'waiting'/'synthesis')
                - event_type: str
                - start_time_ns: int
                - end_time_ns: int
                - duration_ns: int
                - power_watts: float (optional)
                - cpu_util_percent: float (optional)
                - interrupt_rate: float (optional)
                - event_energy_uj: int
                - tax_contribution_uj: int
                - tax_percent: float
        """
        pass
    
    @abstractmethod
    def insert_energy_samples(self, run_id: int, samples: List[Dict[str, Any]]) -> None:
        """
        Insert high-frequency RAPL samples.
        
        Args:
            run_id: Foreign key to runs table
            samples: List of sample dictionaries with keys:
                - timestamp_ns: int
                - pkg_energy_uj: int (optional)
                - core_energy_uj: int (optional)
                - uncore_energy_uj: int (optional)
                - dram_energy_uj: int (optional)
        """
        pass
    
    @abstractmethod
    def insert_cpu_samples(self, run_id: int, samples: List[Dict[str, Any]]) -> None:
        """
        Insert CPU samples.
        
        Args:
            run_id: Foreign key to runs table
            samples: List of sample dictionaries with keys:
                - timestamp_ns: int
                - cpu_util_percent: float
                - cpu_busy_mhz: float
                - cpu_avg_mhz: float
                - c1_residency: float
                - c2_residency: float
                - c3_residency: float
                - c6_residency: float
                - c7_residency: float
                - pkg_c8_residency: float
                - pkg_c9_residency: float
                - pkg_c10_residency: float
                - package_power: float
                - dram_power: float
                - gpu_rc6: float
                - package_temp: float
                - ipc: float
                - extra_metrics_json: str (optional)
        """
        pass
    
    @abstractmethod
    def insert_interrupt_samples(self, run_id: int, samples: List[Dict[str, Any]]) -> None:
        """
        Insert interrupt rate samples.
        
        Args:
            run_id: Foreign key to runs table
            samples: List of sample dictionaries with keys:
                - timestamp_ns: int
                - interrupts_per_sec: float
        """
        pass

    @abstractmethod
    def insert_llm_interaction(self, interaction_data: dict) -> int:
        """Insert an LLM interaction record."""
        pass    
    
    @abstractmethod
    def create_tax_summaries(self, exp_id: int) -> None:
        """
        Create tax summary entries for all agentic runs in an experiment.
        
        This method:
        1. Finds all runs in the experiment
        2. Pairs linear and agentic runs by run_number
        3. Computes tax = agentic_dynamic_uj - linear_dynamic_uj
        4. Stores results in orchestration_tax_summary table
        
        Args:
            exp_id: Experiment ID
        """
        pass
    
    # ========================================================================
    # 5. QUERY METHODS
    # ========================================================================
    
    @abstractmethod
    def get_run(self, run_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve a single run by its ID."""
        pass
    
    @abstractmethod
    def get_runs_by_experiment(self, exp_id: int) -> List[Dict[str, Any]]:
        """Retrieve all runs for an experiment, ordered by run_number."""
        pass
    
    @abstractmethod
    def get_tax_summaries(self, exp_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Retrieve tax summaries, optionally filtered by experiment.
        
        Args:
            exp_id: If provided, only summaries for runs in that experiment
            
        Returns:
            List of tax summary dictionaries
        """
        pass
    
    @abstractmethod
    def get_ml_data(self, workflow: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieve flattened ML data from the ml_features view.
        
        This is the primary method for loading data into ML pipelines.
        The view contains all features and targets in a single row per run.
        
        Args:
            workflow: If provided, filter by 'linear' or 'agentic'
            
        Returns:
            List of dictionaries, each representing one run with all ML columns
        """
        pass
    
    # ========================================================================
    # 6. CONTEXT MANAGER SUPPORT
    # ========================================================================
    
    def __enter__(self) -> 'DatabaseInterface':
        """Enter context manager - returns self."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager - closes connection."""
        self.close()
      