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
    runs_total INTEGER,
    optimization_enabled INTEGER DEFAULT 0,                                            -- Total runs planned (TD8)
    -- ========== NEW COLUMNS END ==========
    hw_id INTEGER REFERENCES hardware_config(hw_id),
    env_id INTEGER REFERENCES environment_config(env_id)    
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
    pkg_energy_uj INTEGER,      -- Raw package energy
    core_energy_uj INTEGER,      -- Raw core energy
    uncore_energy_uj INTEGER,    -- Raw uncore energy
    dram_energy_uj INTEGER,      -- Raw DRAM energy
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

    -- CPU metrics (aggregated from samples)
    cpu_busy_mhz REAL,
    cpu_avg_mhz REAL,

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

    --swap metrics
    swap_total_mb REAL,
    swap_end_free_mb REAL,
    swap_start_used_mb REAL,
    swap_end_used_mb REAL,
    swap_start_cached_mb REAL,
    swap_end_cached_mb REAL,
    swap_end_percent REAL,

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
    -- Network metrics (NEW)
    bytes_sent INTEGER,
    bytes_recv INTEGER,
    tcp_retransmits INTEGER,

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
    orchestration_cpu_ms REAL,
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
    linear_orchestration_uj INTEGER,    -- ← ADD THIS
    agentic_orchestration_uj INTEGER,   -- ← ADD THIS    
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
# Table:10 thermal_samples - 1Hz thermal telemetry
# ========================================================================
THERMAL_SAMPLES_SCHEMA = """
CREATE TABLE IF NOT EXISTS thermal_samples (
    sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    sample_time_s REAL,
    cpu_temp REAL,
    system_temp REAL,
    wifi_temp REAL,
    throttle_event INTEGER DEFAULT 0,
    all_zones_json TEXT,
    sensor_count INTEGER,
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_thermal_run_time ON thermal_samples(run_id, timestamp_ns);
"""
# ========================================================================
# View 11: ml_features (flattened for ML)
# ========================================================================
CREATE_ML_VIEW = """
CREATE VIEW IF NOT EXISTS ml_features AS
SELECT
    -- ========================================================================
    -- 1. Core identifiers (for joins)
    -- ========================================================================
    r.run_id,
    r.exp_id,
    e.group_id,
    r.run_number,
    r.workflow_type,
    
    -- ========================================================================
    -- 2. Experiment metadata (from experiments table)
    -- ========================================================================
    e.task_name,
    e.provider,
    e.model_name,
    e.country_code,
    e.optimization_enabled,
    
    -- ========================================================================
    -- 3. Hardware specs (from hardware_config)
    -- ========================================================================
    h.cpu_model,
    h.cpu_cores,
    h.cpu_threads,
    h.cpu_architecture,
    h.cpu_vendor,
    h.cpu_family,
    h.cpu_model_id,
    h.cpu_stepping,
    h.has_avx2,
    h.has_avx512,
    h.has_vmx,
    h.gpu_model,
    h.gpu_driver,
    h.gpu_count,
    h.ram_gb,
    h.system_type,
    h.system_manufacturer,
    h.system_product,
    h.virtualization_type,
    h.microcode_version,
    
    -- ========================================================================
    -- 4. Environment (from environment_config)
    -- ========================================================================
    env.python_version,
    env.git_commit,
    env.git_branch,
    env.git_dirty,
    env.numpy_version,
    env.torch_version,
    env.transformers_version,
    
    -- ========================================================================
    -- 5. Timing (from runs)
    -- ========================================================================
    r.start_time_ns,
    r.end_time_ns,
    r.duration_ns,
    r.duration_ns / 1e6 AS duration_ms,
    
    -- ========================================================================
    -- 6. Energy metrics (from runs)
    -- ========================================================================
    r.total_energy_uj,
    r.total_energy_uj / 1e6 AS total_energy_j,
    r.dynamic_energy_uj,
    r.dynamic_energy_uj / 1e6 AS dynamic_energy_j,
    r.baseline_energy_uj,
    r.baseline_energy_uj / 1e6 AS baseline_energy_j,
    r.avg_power_watts,
    r.pkg_energy_uj / 1e6 AS pkg_energy_j,
    r.core_energy_uj / 1e6 AS core_energy_j,
    r.uncore_energy_uj / 1e6 AS uncore_energy_j,
    r.dram_energy_uj / 1e6 AS dram_energy_j,
    
    -- ========================================================================
    -- 7. Performance counters (from runs)
    -- ========================================================================
    r.instructions,
    r.cycles,
    r.ipc,
    r.cache_misses,
    r.cache_references,
    r.cache_miss_rate,
    r.page_faults,
    r.major_page_faults,
    r.minor_page_faults,
    
    -- ========================================================================
    -- 8. Scheduler metrics (from runs)
    -- ========================================================================
    r.context_switches_voluntary,
    r.context_switches_involuntary,
    r.total_context_switches,
    r.thread_migrations,
    r.run_queue_length,
    r.kernel_time_ms,
    r.user_time_ms,
    
    -- ========================================================================
    -- 9. Frequency & bus (from runs)
    -- ========================================================================
    r.frequency_mhz,
    r.ring_bus_freq_mhz,
    r.cpu_busy_mhz,
    r.cpu_avg_mhz,
    
    -- ========================================================================
    -- 10. Thermal metrics (from runs)
    -- ========================================================================
    r.package_temp_celsius,
    r.baseline_temp_celsius,
    r.start_temp_c,
    r.max_temp_c,
    r.min_temp_c,
    r.thermal_delta_c,
    r.thermal_during_experiment,
    r.thermal_now_active,
    r.thermal_since_boot,
    r.experiment_valid,
    
    -- ========================================================================
    -- 11. C-state residencies (from runs)
    -- ========================================================================
    r.c2_time_seconds,
    r.c3_time_seconds,
    r.c6_time_seconds,
    r.c7_time_seconds,
    
    -- ========================================================================
    -- 12. Memory & swap (from runs)
    -- ========================================================================
    r.swap_total_mb,
    r.swap_end_free_mb,
    r.swap_start_used_mb,
    r.swap_end_used_mb,
    r.swap_start_cached_mb,
    r.swap_end_cached_mb,
    r.swap_end_percent,
    r.rss_memory_mb,
    r.vms_memory_mb,
    
    -- ========================================================================
    -- 13. MSR & wakeup (from runs)
    -- ========================================================================
    r.wakeup_latency_us,
    r.interrupt_rate,
    r.thermal_throttle_flag,
    
    -- ========================================================================
    -- 14. Token counts (from runs)
    -- ========================================================================
    r.total_tokens,
    r.prompt_tokens,
    r.completion_tokens,
    
    -- ========================================================================
    -- 15. Network latencies (from runs)
    -- ========================================================================
    r.dns_latency_ms,
    r.api_latency_ms,
    r.compute_time_ms,
    
    -- ========================================================================
    -- 16. Network metrics (NEW)
    -- ========================================================================
    r.bytes_sent,
    r.bytes_recv,
    r.tcp_retransmits,
    
    -- ========================================================================
    -- 17. System state (from runs)
    -- ========================================================================
    r.governor,
    r.turbo_enabled,
    r.is_cold_start,
    r.background_cpu_percent,
    r.process_count,
    
    -- ========================================================================
    -- 18. Agentic-specific metrics (from runs)
    -- ========================================================================
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
    
    -- ========================================================================
    -- 19. Sustainability metrics (from runs)
    -- ========================================================================
    r.carbon_g,
    r.water_ml,
    r.methane_mg,
    
    -- ========================================================================
    -- 20. Derived efficiency metrics (from runs)
    -- ========================================================================
    r.energy_per_instruction,
    r.energy_per_cycle,
    r.energy_per_token,
    r.instructions_per_token,
    r.interrupts_per_second,
    
    -- ========================================================================
    -- 21. Baseline data (from idle_baselines)
    -- ========================================================================
    ib.package_power_watts AS baseline_package_power,
    ib.core_power_watts AS baseline_core_power,
    ib.uncore_power_watts AS baseline_uncore_power,
    ib.dram_power_watts AS baseline_dram_power,
    ib.governor AS baseline_governor,
    ib.turbo AS baseline_turbo,
    ib.background_cpu AS baseline_background_cpu,
    ib.process_count AS baseline_process_count,
    
    -- ========================================================================
    -- 22. Tax summary (target variable)
    -- ========================================================================
    ots.orchestration_tax_uj / 1e6 AS orchestration_tax_j,
    ots.tax_percent,
    
    -- ========================================================================
    -- 23. Cryptographic run state hash
    -- ========================================================================
    r.run_state_hash
    
FROM runs r
JOIN experiments e ON r.exp_id = e.exp_id
LEFT JOIN hardware_config h ON r.hw_id = h.hw_id
LEFT JOIN environment_config env ON e.env_id = env.env_id
LEFT JOIN idle_baselines ib ON r.baseline_id = ib.baseline_id
LEFT JOIN orchestration_tax_summary ots ON r.run_id = ots.agentic_run_id;
"""
# ========================================================================
# View: orchestration_analysis - Derived metrics for tax calculation
# ========================================================================
CREATE_ORCHESTRATION_ANALYSIS = """
CREATE VIEW IF NOT EXISTS orchestration_analysis AS
SELECT 
    r.run_id,
    r.exp_id,
    r.workflow_type,
    r.run_number,
    
    -- Raw energies (Joules)
    r.pkg_energy_uj/1e6 as pkg_energy_j,
    r.core_energy_uj/1e6 as core_energy_j,
    r.uncore_energy_uj/1e6 as uncore_energy_j,
    r.dram_energy_uj/1e6 as dram_energy_j,
    
    -- Timing
    r.duration_ns/1e9 as duration_sec,
    
    -- Baseline values (idle energy during run)
    ib.package_power_watts * (r.duration_ns/1e9) as baseline_pkg_j,
    ib.core_power_watts * (r.duration_ns/1e9) as baseline_core_j,
    ib.uncore_power_watts * (r.duration_ns/1e9) as baseline_uncore_j,
    
    -- Derived metrics (YOUR FORMULAS!)
    (r.pkg_energy_uj/1e6) - (ib.package_power_watts * (r.duration_ns/1e9)) as workload_energy_j,
    (r.core_energy_uj/1e6) - (ib.core_power_watts * (r.duration_ns/1e9)) as reasoning_energy_j,
    (r.uncore_energy_uj/1e6) - (ib.uncore_power_watts * (r.duration_ns/1e9)) as orchestration_tax_j,
    
    -- Efficiency metrics
    ((r.core_energy_uj/1e6) - (ib.core_power_watts * (r.duration_ns/1e9))) / 
        (r.instructions/1e9) as joules_per_billion_instructions,
    r.instructions * 1.0 / r.cycles as ipc,
    r.cache_misses * 1.0 / r.cache_references as cache_miss_rate,
    
    -- Ratios (what proportion of energy went where?)
    CASE 
        WHEN (r.pkg_energy_uj/1e6) > 0 
        THEN (r.core_energy_uj/1e6) / (r.pkg_energy_uj/1e6) 
        ELSE 0 
    END as core_share,
    
    CASE 
        WHEN (r.pkg_energy_uj/1e6) > 0 
        THEN (r.uncore_energy_uj/1e6) / (r.pkg_energy_uj/1e6) 
        ELSE 0 
    END as uncore_share,
    
    -- Metadata
    e.provider,
    e.task_name,
    e.country_code

FROM runs r
JOIN experiments e ON r.exp_id = e.exp_id
JOIN idle_baselines ib ON r.baseline_id = ib.baseline_id
WHERE r.baseline_id IS NOT NULL;
"""

# ========================================================================
# Table: task_categories - Task to category mapping for analysis
# ========================================================================
TASK_CATEGORIES_SCHEMA = """
CREATE TABLE IF NOT EXISTS task_categories (
    task_id TEXT PRIMARY KEY,
    category TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_task_categories_id ON task_categories(task_id);
"""
# Add new columns to hardware_config table
CREATE_HARDWARE_CONFIG = """
CREATE TABLE IF NOT EXISTS hardware_config (
    hw_id INTEGER PRIMARY KEY AUTOINCREMENT,
    hardware_hash TEXT UNIQUE,              
    hostname TEXT,
    cpu_model TEXT,
    cpu_cores INTEGER,
    cpu_threads INTEGER,
    cpu_architecture TEXT,                  
    cpu_vendor TEXT,                         
    cpu_family INTEGER,                      
    cpu_model_id INTEGER,                    
    cpu_stepping INTEGER,                    
    has_avx2 BOOLEAN,                        
    has_avx512 BOOLEAN,                      
    has_vmx BOOLEAN,                         
    gpu_model TEXT,                          
    gpu_driver TEXT,                          
    gpu_count INTEGER,                        
    gpu_power_available BOOLEAN,              
    ram_gb REAL,
    kernel_version TEXT,
    microcode_version TEXT,
    rapl_domains TEXT,
    rapl_has_dram BOOLEAN,                   
    rapl_has_uncore BOOLEAN,                  
    system_manufacturer TEXT,                 
    system_product TEXT,                      
    system_type TEXT,                         
    virtualization_type TEXT,                  
    detected_at TIMESTAMP,                    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Add environment_config table (NEW)
CREATE_ENVIRONMENT_CONFIG = """
CREATE TABLE IF NOT EXISTS environment_config (
    env_id INTEGER PRIMARY KEY AUTOINCREMENT,
    env_hash TEXT UNIQUE,
    python_version TEXT,
    python_implementation TEXT,
    os_name TEXT,
    os_version TEXT,
    kernel_version TEXT,
    llm_framework TEXT,
    framework_version TEXT,
    git_commit TEXT,
    git_branch TEXT,
    git_dirty BOOLEAN,
    numpy_version TEXT,
    torch_version TEXT,
    transformers_version TEXT,
    container_runtime TEXT,
    container_image TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
# ========================================================================
# Table: llm_interactions - Store prompts, responses, and per-call metrics
# ========================================================================
CREATE_LLM_INTERACTIONS = """
CREATE TABLE IF NOT EXISTS llm_interactions (
    interaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    step_index INTEGER,
    workflow_type TEXT,
    prompt TEXT,
    response TEXT,
    model_name TEXT,
    provider TEXT,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    total_tokens INTEGER,
    
    -- Existing (keep)
    api_latency_ms REAL,
    compute_time_ms REAL,
    
    -- Renamed
    app_throughput_kbps REAL,
    
    -- NEW columns
    total_time_ms REAL,
    preprocess_ms REAL,
    non_local_ms REAL,
    local_compute_ms REAL,
    postprocess_ms REAL,
    cpu_percent_during_wait REAL,
    bytes_sent_approx INTEGER,
    bytes_recv_approx INTEGER,
    tcp_retransmits INTEGER,
    error_message TEXT,
    status TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_llm_run ON llm_interactions(run_id);
CREATE INDEX IF NOT EXISTS idx_llm_workflow ON llm_interactions(workflow_type);
"""
# ========================================================================
# View : ml_view - analytical view that flattens runs and llm_interactions for ML modeling
# ========================================================================
CREATE_RESEARCH_METRICS_VIEW = """
CREATE VIEW IF NOT EXISTS research_metrics_view AS
SELECT 
    r.run_id,
    r.exp_id,
    e.provider,
    r.workflow_type,
    r.duration_ns / 1e6 AS total_time_ms,
    r.compute_time_ms,
    r.orchestration_cpu_ms,
    r.bytes_sent AS total_bytes_sent,
    r.bytes_recv AS total_bytes_recv,
    
    COALESCE(i.total_wait_ms, 0) AS total_wait_ms,
    COALESCE(i.total_llm_compute_ms, 0) AS total_llm_compute_ms,
    
    CASE WHEN r.duration_ns > 0 THEN r.orchestration_cpu_ms * 1.0 / (r.duration_ns / 1e6) ELSE 0 END AS ooi_time,
    CASE WHEN r.compute_time_ms > 0 THEN r.orchestration_cpu_ms * 1.0 / r.compute_time_ms ELSE 0 END AS ooi_cpu,
    CASE WHEN r.duration_ns > 0 THEN COALESCE(i.total_llm_compute_ms, 0) * 1.0 / (r.duration_ns / 1e6) ELSE 0 END AS ucr,
    CASE WHEN r.duration_ns > 0 THEN COALESCE(i.total_wait_ms, 0) * 1.0 / (r.duration_ns / 1e6) ELSE 0 END AS network_ratio

FROM runs r
JOIN experiments e ON r.exp_id = e.exp_id
LEFT JOIN (
    SELECT 
        run_id, 
        SUM(non_local_ms) AS total_wait_ms,
        SUM(local_compute_ms) AS total_llm_compute_ms
    FROM llm_interactions
    GROUP BY run_id
) i ON r.run_id = i.run_id
WHERE (i.total_llm_compute_ms > 0 OR i.total_wait_ms > 0)
  AND (r.orchestration_cpu_ms <= r.compute_time_ms * 5);

"""

ENERGY_SAMPLES_WITH_POWER_VIEW = """
CREATE VIEW IF NOT EXISTS energy_samples_with_power AS
SELECT 
    e1.sample_id,
    e1.run_id,
    e1.timestamp_ns / 1e9 AS time_s,
    e1.pkg_energy_uj,
    e1.core_energy_uj,
    e1.uncore_energy_uj,
    e1.dram_energy_uj,
    -- Power (watts) = Δenergy (µJ) / Δtime (seconds) / 1e6
    (e1.pkg_energy_uj - e2.pkg_energy_uj) / ((e1.timestamp_ns - e2.timestamp_ns) / 1e9) / 1e6 AS pkg_power_watts,
    (e1.core_energy_uj - e2.core_energy_uj) / ((e1.timestamp_ns - e2.timestamp_ns) / 1e9) / 1e6 AS core_power_watts,
    (e1.uncore_energy_uj - e2.uncore_energy_uj) / ((e1.timestamp_ns - e2.timestamp_ns) / 1e9) / 1e6 AS uncore_power_watts,
    (e1.dram_energy_uj - e2.dram_energy_uj) / ((e1.timestamp_ns - e2.timestamp_ns) / 1e9) / 1e6 AS dram_power_watts
FROM energy_samples e1
LEFT JOIN energy_samples e2 ON e1.run_id = e2.run_id 
    AND e2.timestamp_ns = (
        SELECT MAX(timestamp_ns) 
        FROM energy_samples e3 
        WHERE e3.run_id = e1.run_id AND e3.timestamp_ns < e1.timestamp_ns
    )
WHERE e2.timestamp_ns IS NOT NULL
ORDER BY e1.run_id, e1.timestamp_ns;
"""
