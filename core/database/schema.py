#!/usr/bin/env python3
"""
================================================================================
DATABASE SCHEMA – SQL definitions for all A‑LEMS tables
================================================================================

PURPOSE:
    Contains all CREATE TABLE, CREATE INDEX, and CREATE VIEW statements
    for the A‑LEMS database. This file is pure SQL – no Python logic.

WHY THIS EXISTS:
    - Separates schema definition from database logic
    - Makes schema changes easier to track
    - Can be reused by migration scripts
    - Keeps SQL in one place for review

TABLES:
    1. experiments
    2. hardware_config
    3. idle_baselines
    4. runs (main table, 70+ columns)
    5. orchestration_events
    6. orchestration_tax_summary
    7. energy_samples
    8. cpu_samples
    9. interrupt_samples
    10. ml_features (view)

AUTHOR: Deepak Panigrahy
================================================================================
"""

# ========================================================================
# Table 1: experiments
# ========================================================================
CREATE_EXPERIMENTS = """
CREATE TABLE IF NOT EXISTS experiments (
    exp_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    workflow_type TEXT CHECK(workflow_type IN ('linear','agentic','comparison')),
    model_name TEXT,
    provider TEXT,
    task_name TEXT,
    country_code TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- ========== NEW COLUMNS START ==========
    group_id TEXT,                          -- Session ID (TD7)
    status TEXT DEFAULT 'pending',           -- pending/running/completed/partial/failed (TD8)
    started_at TIMESTAMP,                    -- When experiment started (TD8)
    completed_at TIMESTAMP,                  -- When experiment ended (TD8)
    error_message TEXT,                      -- Error if failed (TD8)
    runs_completed INTEGER DEFAULT 0,        -- Number of successful runs (TD8)
    runs_total INTEGER                       -- Total runs planned (TD8)
    -- ========== NEW COLUMNS END ==========
);
"""

# ========================================================================
# Table 2: hardware_config
# ========================================================================
CREATE_HARDWARE_CONFIG = """
CREATE TABLE IF NOT EXISTS hardware_config (
    hw_id INTEGER PRIMARY KEY AUTOINCREMENT,
    hostname TEXT,
    cpu_model TEXT,
    cpu_cores INTEGER,
    cpu_threads INTEGER,
    ram_gb INTEGER,
    kernel_version TEXT,
    microcode_version TEXT,
    rapl_domains TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# ========================================================================
# Table 3: idle_baselines
# ========================================================================
CREATE_IDLE_BASELINES = """
CREATE TABLE IF NOT EXISTS idle_baselines (
    baseline_id TEXT PRIMARY KEY,
    timestamp REAL NOT NULL,
    package_power_watts REAL,
    core_power_watts REAL,
    uncore_power_watts REAL,
    dram_power_watts REAL,
    duration_seconds INTEGER,
    sample_count INTEGER,
    package_std REAL,
    core_std REAL,
    uncore_std REAL,
    dram_std REAL,
    governor TEXT,
    turbo TEXT,
    background_cpu REAL,
    process_count INTEGER,
    method TEXT
);
"""

# ========================================================================
# Table 4: runs (core table with 70+ columns)
# ========================================================================
CREATE_RUNS = """
CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    exp_id INTEGER NOT NULL,
    hw_id INTEGER,
    baseline_id TEXT,
    run_number INTEGER,
    workflow_type TEXT NOT NULL,

    -- Timing (all in nanoseconds)
    start_time_ns INTEGER,
    end_time_ns INTEGER,
    duration_ns INTEGER,

    -- Energy (all in microjoules)
    total_energy_uj INTEGER,
    dynamic_energy_uj INTEGER,
    baseline_energy_uj INTEGER,
    avg_power_watts REAL,

    -- Performance counters
    instructions BIGINT,
    cycles BIGINT,
    ipc REAL,
    cache_misses BIGINT,
    cache_references BIGINT,
    cache_miss_rate REAL,
    page_faults INTEGER,
    major_page_faults INTEGER,
    minor_page_faults INTEGER,

    -- Scheduler metrics
    context_switches_voluntary INTEGER,
    context_switches_involuntary INTEGER,
    total_context_switches INTEGER,
    thread_migrations INTEGER,
    run_queue_length REAL,
    kernel_time_ms REAL,
    user_time_ms REAL,

    -- Frequency & ring bus
    frequency_mhz REAL,
    ring_bus_freq_mhz REAL,

    -- Thermal metrics
    package_temp_celsius REAL,
    baseline_temp_celsius REAL,
    start_temp_c REAL,
    max_temp_c REAL,
    min_temp_c REAL,
    thermal_delta_c REAL,
    thermal_during_experiment BOOLEAN,
    thermal_now_active BOOLEAN,
    thermal_since_boot BOOLEAN,
    experiment_valid BOOLEAN,

    -- C‑state residencies
    c2_time_seconds REAL,
    c3_time_seconds REAL,
    c6_time_seconds REAL,
    c7_time_seconds REAL,

    -- MSR / wakeup
    wakeup_latency_us REAL,
    interrupt_rate REAL,
    thermal_throttle_flag INTEGER,

    -- Memory usage
    rss_memory_mb REAL,
    vms_memory_mb REAL,

    -- Token counts
    total_tokens INTEGER,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,

    -- Network latencies
    dns_latency_ms REAL,
    api_latency_ms REAL,
    compute_time_ms REAL,

    -- System state
    governor TEXT,
    turbo_enabled BOOLEAN,
    is_cold_start BOOLEAN,
    background_cpu_percent REAL,
    process_count INTEGER,

    -- Agentic‑specific metrics (NULL for linear runs)
    planning_time_ms REAL,
    execution_time_ms REAL,
    synthesis_time_ms REAL,
    phase_planning_ratio REAL,
    phase_execution_ratio REAL,
    phase_synthesis_ratio REAL,
    llm_calls INTEGER,
    tool_calls INTEGER,
    tools_used INTEGER,
    steps INTEGER,
    avg_step_time_ms REAL,
    complexity_level INTEGER,
    complexity_score REAL,

    -- Sustainability metrics
    carbon_g REAL,
    water_ml REAL,
    methane_mg REAL,

    -- Derived efficiency metrics
    energy_per_instruction REAL,
    energy_per_cycle REAL,
    energy_per_token REAL,
    instructions_per_token REAL,
    interrupts_per_second REAL,

    -- Cryptographic run state hash
    run_state_hash TEXT,

    FOREIGN KEY(exp_id) REFERENCES experiments(exp_id),
    FOREIGN KEY(hw_id) REFERENCES hardware_config(hw_id),
    FOREIGN KEY(baseline_id) REFERENCES idle_baselines(baseline_id)
);
"""

# ========================================================================
# Indexes for runs table
# ========================================================================
CREATE_RUNS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_runs_exp_id ON runs(exp_id);
CREATE INDEX IF NOT EXISTS idx_runs_hw_id ON runs(hw_id);
CREATE INDEX IF NOT EXISTS idx_runs_energy ON runs(total_energy_uj);
CREATE INDEX IF NOT EXISTS idx_runs_ipc ON runs(ipc);
CREATE INDEX IF NOT EXISTS idx_runs_interrupt ON runs(interrupt_rate);
CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_unique ON runs(exp_id, run_number, workflow_type);
"""

# ========================================================================
# Table 5: orchestration_events
# ========================================================================
CREATE_ORCHESTRATION_EVENTS = """
CREATE TABLE IF NOT EXISTS orchestration_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    step_index INTEGER,
    phase TEXT CHECK(phase IN ('planning','execution','waiting','synthesis')),
    event_type TEXT NOT NULL,
    start_time_ns INTEGER NOT NULL,
    end_time_ns INTEGER NOT NULL,
    duration_ns INTEGER NOT NULL,
    power_watts REAL,
    cpu_util_percent REAL,
    interrupt_rate REAL,
    event_energy_uj INTEGER,
    tax_contribution_uj INTEGER,
    tax_percent REAL,
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);
"""

CREATE_EVENTS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_events_run ON orchestration_events(run_id);
CREATE INDEX IF NOT EXISTS idx_events_phase ON orchestration_events(phase);
"""

# ========================================================================
# Table 6: orchestration_tax_summary
# ========================================================================
CREATE_TAX_SUMMARY = """
CREATE TABLE IF NOT EXISTS orchestration_tax_summary (
    comparison_id INTEGER PRIMARY KEY AUTOINCREMENT,
    linear_run_id INTEGER NOT NULL,
    agentic_run_id INTEGER NOT NULL,
    linear_dynamic_uj INTEGER,
    agentic_dynamic_uj INTEGER,
    orchestration_tax_uj INTEGER,
    tax_percent REAL,
    FOREIGN KEY(linear_run_id) REFERENCES runs(run_id),
    FOREIGN KEY(agentic_run_id) REFERENCES runs(run_id)
);
"""

CREATE_TAX_INDEXES = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_tax_pair ON orchestration_tax_summary(linear_run_id, agentic_run_id);
"""

# ========================================================================
# Table 7: energy_samples
# ========================================================================
CREATE_ENERGY_SAMPLES = """
CREATE TABLE IF NOT EXISTS energy_samples (
    sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    pkg_energy_uj INTEGER,
    core_energy_uj INTEGER,
    uncore_energy_uj INTEGER,
    dram_energy_uj INTEGER,
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_energy_run_time ON energy_samples(run_id, timestamp_ns);
"""

# ========================================================================
# Table 8: cpu_samples
# ========================================================================
CREATE_CPU_SAMPLES = """
-- ========================================================================
-- ========================================================================
-- Table: cpu_samples
-- Purpose: High-frequency CPU telemetry from turbostat
-- 
-- This table stores ALL canonical metrics from turbostat_override.yaml
-- ========================================================================
CREATE TABLE IF NOT EXISTS cpu_samples (
    sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    
    -- --------------------------------------------------------------------
    -- CPU Activity (Core metrics)
    -- --------------------------------------------------------------------
    cpu_util_percent REAL,
    cpu_busy_mhz REAL,
    cpu_avg_mhz REAL,
    
    -- --------------------------------------------------------------------
    -- Core C-States
    -- --------------------------------------------------------------------
    c1_residency REAL,
    c2_residency REAL,
    c3_residency REAL,
    c6_residency REAL,
    c7_residency REAL,
    
    -- --------------------------------------------------------------------
    -- Package C-States (Deep sleep)
    -- --------------------------------------------------------------------
    pkg_c8_residency REAL,
    pkg_c9_residency REAL,
    pkg_c10_residency REAL,
    
    -- --------------------------------------------------------------------
    -- Power Metrics
    -- --------------------------------------------------------------------
    package_power REAL,
    dram_power REAL,
    
    -- --------------------------------------------------------------------
    -- GPU Metrics
    -- --------------------------------------------------------------------
    gpu_rc6 REAL,
    
    -- --------------------------------------------------------------------
    -- Temperature & Efficiency
    -- --------------------------------------------------------------------
    package_temp REAL,
    ipc REAL,
    
    -- --------------------------------------------------------------------
    -- JSON for any additional columns
    -- --------------------------------------------------------------------
    extra_metrics_json TEXT,
    
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_cpu_samples_run_id ON cpu_samples(run_id);
CREATE INDEX IF NOT EXISTS idx_cpu_samples_timestamp ON cpu_samples(run_id, timestamp_ns);

-- ========================================================================
-- Indexes for performance
-- ========================================================================

-- Index for filtering by run_id (essential for JOINs)
CREATE INDEX IF NOT EXISTS idx_cpu_samples_run_id ON cpu_samples(run_id);

-- Composite index for time-series queries (most common access pattern)
-- This speeds up queries like: SELECT * FROM cpu_samples WHERE run_id = ? ORDER BY timestamp_ns
CREATE INDEX IF NOT EXISTS idx_cpu_samples_timestamp ON cpu_samples(run_id, timestamp_ns);

-- Optional: If you frequently query by timestamp range without run_id
-- CREATE INDEX IF NOT EXISTS idx_cpu_samples_time ON cpu_samples(timestamp_ns);
"""

# ========================================================================
# Table 9: interrupt_samples
# ========================================================================
CREATE_INTERRUPT_SAMPLES = """
CREATE TABLE IF NOT EXISTS interrupt_samples (
    sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    interrupts_per_sec REAL,
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_interrupt_run_time ON interrupt_samples(run_id, timestamp_ns);
"""

# ========================================================================
# View 10: ml_features (flattened for ML)
# ========================================================================
CREATE_ML_VIEW = """
CREATE VIEW IF NOT EXISTS ml_features AS
SELECT
    r.run_id,
    e.workflow_type,
    e.country_code,
    e.model_name,
    e.provider,
    r.run_number,
    r.duration_ns / 1e6 AS duration_ms,
    r.total_energy_uj / 1e6 AS energy_j,
    r.dynamic_energy_uj / 1e6 AS dynamic_energy_j,
    r.avg_power_watts,
    r.instructions,
    r.cycles,
    r.ipc,
    r.cache_misses,
    r.cache_references,
    r.cache_miss_rate,
    r.page_faults,
    r.major_page_faults,
    r.minor_page_faults,
    r.context_switches_voluntary,
    r.context_switches_involuntary,
    r.total_context_switches,
    r.thread_migrations,
    r.run_queue_length,
    r.kernel_time_ms,
    r.user_time_ms,
    r.frequency_mhz,
    r.ring_bus_freq_mhz,
    r.package_temp_celsius,
    r.start_temp_c,
    r.max_temp_c,
    r.thermal_delta_c,
    r.c2_time_seconds,
    r.c3_time_seconds,
    r.c6_time_seconds,
    r.c7_time_seconds,
    r.wakeup_latency_us,
    r.interrupt_rate,
    r.thermal_throttle_flag,
    r.rss_memory_mb,
    r.vms_memory_mb,
    r.total_tokens,
    r.prompt_tokens,
    r.completion_tokens,
    r.dns_latency_ms,
    r.api_latency_ms,
    r.compute_time_ms,
    r.governor,
    r.turbo_enabled,
    r.is_cold_start,
    r.background_cpu_percent,
    r.process_count,
    r.planning_time_ms,
    r.execution_time_ms,
    r.synthesis_time_ms,
    r.phase_planning_ratio,
    r.phase_execution_ratio,
    r.phase_synthesis_ratio,
    r.llm_calls,
    r.tool_calls,
    r.tools_used,
    r.steps,
    r.avg_step_time_ms,
    r.complexity_level,
    r.complexity_score,
    r.carbon_g,
    r.water_ml,
    r.methane_mg,
    r.run_state_hash,
    r.energy_per_instruction,
    r.energy_per_cycle,
    r.energy_per_token,
    r.instructions_per_token,
    r.interrupts_per_second,
    -- Targets
    r.total_energy_uj / 1e6 AS energy_j,
    CASE 
        WHEN e.workflow_type = 'agentic' 
        THEN ots.orchestration_tax_uj / 1e6
        ELSE 0
    END AS orchestration_tax_j,
    CASE 
        WHEN r.total_tokens > 0 
        THEN (r.total_energy_uj / 1e6) / r.total_tokens 
        ELSE 0 
    END AS energy_per_token
FROM runs r
JOIN experiments e ON r.exp_id = e.exp_id
LEFT JOIN orchestration_tax_summary ots ON r.run_id = ots.agentic_run_id;
"""