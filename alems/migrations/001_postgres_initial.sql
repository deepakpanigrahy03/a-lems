-- ============================================================
-- A-LEMS PostgreSQL Schema — Clean Design (No UUIDs)
-- Oracle VM: psql -U alems -d alems_central -f 001_postgres_initial.sql
--
-- Design principles:
--   - PostgreSQL assigns BIGSERIAL sequential PKs (meaningful, space-efficient)
--   - Local SQLite IDs (run_id, exp_id) stored as reference columns
--   - Collision safety: UNIQUE(hw_id, run_id) per table
--   - Analytical queries use hw_id + exp_id (indexed)
--   - No UUIDs anywhere
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ── hardware_config ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hardware_config (
    hw_id               BIGSERIAL   PRIMARY KEY,
    hardware_hash       TEXT        UNIQUE NOT NULL,
    hostname            TEXT,
    cpu_model           TEXT,
    cpu_cores           INTEGER,
    cpu_threads         INTEGER,
    ram_gb              INTEGER,
    kernel_version      TEXT,
    microcode_version   TEXT,
    rapl_domains        TEXT,
    cpu_architecture    TEXT,
    cpu_vendor          TEXT,
    cpu_family          INTEGER,
    cpu_model_id        INTEGER,
    cpu_stepping        INTEGER,
    has_avx2            BOOLEAN,
    has_avx512          BOOLEAN,
    has_vmx             BOOLEAN,
    gpu_model           TEXT,
    gpu_driver          TEXT,
    gpu_count           INTEGER,
    gpu_power_available BOOLEAN,
    rapl_has_dram       BOOLEAN,
    rapl_has_uncore     BOOLEAN,
    system_manufacturer TEXT,
    system_product      TEXT,
    system_type         TEXT,
    virtualization_type TEXT,
    created_at          TIMESTAMP   DEFAULT NOW(),
    detected_at         TIMESTAMP,
    last_seen           TIMESTAMP,
    agent_status        TEXT        DEFAULT 'offline',
    agent_version       TEXT,
    api_key             TEXT,
    server_hw_id        INTEGER
);

-- ── environment_config ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS environment_config (
    env_id                  BIGSERIAL   PRIMARY KEY,
    env_hash                TEXT        UNIQUE NOT NULL,
    python_version          TEXT,
    python_implementation   TEXT,
    os_name                 TEXT,
    os_version              TEXT,
    kernel_version          TEXT,
    llm_framework           TEXT,
    framework_version       TEXT,
    git_commit              TEXT,
    git_branch              TEXT,
    git_dirty               BOOLEAN,
    numpy_version           TEXT,
    torch_version           TEXT,
    transformers_version    TEXT,
    container_runtime       TEXT,
    container_image         TEXT,
    created_at              TIMESTAMP   DEFAULT NOW()
);

-- ── idle_baselines ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS idle_baselines (
    baseline_id         TEXT        PRIMARY KEY,
    timestamp           DOUBLE PRECISION NOT NULL,
    package_power_watts DOUBLE PRECISION,
    core_power_watts    DOUBLE PRECISION,
    uncore_power_watts  DOUBLE PRECISION,
    dram_power_watts    DOUBLE PRECISION,
    duration_seconds    INTEGER,
    sample_count        INTEGER,
    package_std         DOUBLE PRECISION,
    core_std            DOUBLE PRECISION,
    uncore_std          DOUBLE PRECISION,
    dram_std            DOUBLE PRECISION,
    governor            TEXT,
    turbo               TEXT,
    background_cpu      DOUBLE PRECISION,
    process_count       INTEGER,
    method              TEXT
);

-- ── task_categories ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS task_categories (
    task_id     TEXT PRIMARY KEY,
    category    TEXT NOT NULL
);

-- ── experiments ───────────────────────────────────────────────────────────────
-- global_exp_id: server-assigned BIGSERIAL (meaningful sequential ID)
-- exp_id:        local SQLite autoincrement (reference only, NOT PK)
-- UNIQUE(hw_id, exp_id): collision-safe sync
CREATE TABLE IF NOT EXISTS experiments (
    global_exp_id       BIGSERIAL   PRIMARY KEY,
    exp_id              BIGINT      NOT NULL,
    hw_id               BIGINT      NOT NULL REFERENCES hardware_config(hw_id),
    env_id              BIGINT,
    name                TEXT        NOT NULL,
    description         TEXT,
    workflow_type       TEXT,
    model_name          TEXT,
    provider            TEXT,
    task_name           TEXT,
    country_code        TEXT,
    group_id            TEXT,
    status              TEXT        DEFAULT 'pending',
    started_at          TIMESTAMP,
    completed_at        TIMESTAMP,
    error_message       TEXT,
    runs_completed      INTEGER     DEFAULT 0,
    runs_total          INTEGER,
    optimization_enabled INTEGER    DEFAULT 0,
    created_at          TIMESTAMP   DEFAULT NOW(),
    dispatch_source     TEXT        DEFAULT 'local',
    UNIQUE (hw_id, exp_id)
);

-- ── runs ──────────────────────────────────────────────────────────────────────
-- global_run_id: server-assigned BIGSERIAL (meaningful sequential ID)
-- run_id:        local SQLite autoincrement (reference only, NOT PK)
-- exp_id:        local SQLite exp_id (reference, indexed)
-- UNIQUE(hw_id, run_id): collision-safe sync
CREATE TABLE IF NOT EXISTS runs (
    global_run_id           BIGSERIAL   PRIMARY KEY,
    run_id                  BIGINT      NOT NULL,
    exp_id                  BIGINT      NOT NULL,
    global_exp_id           BIGINT      NOT NULL REFERENCES experiments(global_exp_id),
    hw_id                   BIGINT      NOT NULL REFERENCES hardware_config(hw_id),
    baseline_id             TEXT        REFERENCES idle_baselines(baseline_id),
    run_number              INTEGER,
    workflow_type           TEXT        NOT NULL,
    synced_at               TIMESTAMP   DEFAULT NOW(),
    start_time_ns           BIGINT,
    end_time_ns             BIGINT,
    duration_ns             BIGINT,
    total_energy_uj         BIGINT,
    dynamic_energy_uj       BIGINT,
    baseline_energy_uj      BIGINT,
    avg_power_watts         DOUBLE PRECISION,
    pkg_energy_uj           BIGINT,
    core_energy_uj          BIGINT,
    uncore_energy_uj        BIGINT,
    dram_energy_uj          BIGINT,
    instructions            BIGINT,
    cycles                  BIGINT,
    ipc                     DOUBLE PRECISION,
    cache_misses            BIGINT,
    cache_references        BIGINT,
    cache_miss_rate         DOUBLE PRECISION,
    page_faults             INTEGER,
    major_page_faults       INTEGER,
    minor_page_faults       INTEGER,
    context_switches_voluntary   INTEGER,
    context_switches_involuntary INTEGER,
    total_context_switches       INTEGER,
    thread_migrations            INTEGER,
    run_queue_length             DOUBLE PRECISION,
    kernel_time_ms               DOUBLE PRECISION,
    user_time_ms                 DOUBLE PRECISION,
    frequency_mhz           DOUBLE PRECISION,
    ring_bus_freq_mhz       DOUBLE PRECISION,
    cpu_busy_mhz            DOUBLE PRECISION,
    cpu_avg_mhz             DOUBLE PRECISION,
    package_temp_celsius    DOUBLE PRECISION,
    baseline_temp_celsius   DOUBLE PRECISION,
    start_temp_c            DOUBLE PRECISION,
    max_temp_c              DOUBLE PRECISION,
    min_temp_c              DOUBLE PRECISION,
    thermal_delta_c         DOUBLE PRECISION,
    thermal_during_experiment BOOLEAN,
    thermal_now_active      BOOLEAN,
    thermal_since_boot      BOOLEAN,
    experiment_valid        BOOLEAN,
    c2_time_seconds         DOUBLE PRECISION,
    c3_time_seconds         DOUBLE PRECISION,
    c6_time_seconds         DOUBLE PRECISION,
    c7_time_seconds         DOUBLE PRECISION,
    swap_total_mb           DOUBLE PRECISION,
    swap_end_free_mb        DOUBLE PRECISION,
    swap_start_used_mb      DOUBLE PRECISION,
    swap_end_used_mb        DOUBLE PRECISION,
    swap_start_cached_mb    DOUBLE PRECISION,
    swap_end_cached_mb      DOUBLE PRECISION,
    swap_end_percent        DOUBLE PRECISION,
    wakeup_latency_us       DOUBLE PRECISION,
    interrupt_rate          DOUBLE PRECISION,
    thermal_throttle_flag   INTEGER,
    rss_memory_mb           DOUBLE PRECISION,
    vms_memory_mb           DOUBLE PRECISION,
    total_tokens            INTEGER,
    prompt_tokens           INTEGER,
    completion_tokens       INTEGER,
    dns_latency_ms          DOUBLE PRECISION,
    api_latency_ms          DOUBLE PRECISION,
    compute_time_ms         DOUBLE PRECISION,
    governor                TEXT,
    turbo_enabled           BOOLEAN,
    is_cold_start           BOOLEAN,
    background_cpu_percent  DOUBLE PRECISION,
    process_count           INTEGER,
    planning_time_ms        DOUBLE PRECISION,
    execution_time_ms       DOUBLE PRECISION,
    synthesis_time_ms       DOUBLE PRECISION,
    phase_planning_ratio    DOUBLE PRECISION,
    phase_execution_ratio   DOUBLE PRECISION,
    phase_synthesis_ratio   DOUBLE PRECISION,
    llm_calls               INTEGER,
    tool_calls              INTEGER,
    tools_used              INTEGER,
    steps                   INTEGER,
    avg_step_time_ms        DOUBLE PRECISION,
    complexity_level        INTEGER,
    complexity_score        DOUBLE PRECISION,
    carbon_g                DOUBLE PRECISION,
    water_ml                DOUBLE PRECISION,
    methane_mg              DOUBLE PRECISION,
    energy_per_instruction  DOUBLE PRECISION,
    energy_per_cycle        DOUBLE PRECISION,
    energy_per_token        DOUBLE PRECISION,
    instructions_per_token  DOUBLE PRECISION,
    interrupts_per_second   DOUBLE PRECISION,
    run_state_hash          TEXT,
    bytes_sent              BIGINT,
    bytes_recv              BIGINT,
    tcp_retransmits         INTEGER,
    orchestration_cpu_ms    DOUBLE PRECISION,
    UNIQUE (hw_id, run_id)
);

-- ── child sample tables ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS energy_samples (
    sample_id       BIGSERIAL   PRIMARY KEY,
    run_id    BIGINT      NOT NULL,
    hw_id           BIGINT      NOT NULL REFERENCES hardware_config(hw_id),
    timestamp_ns    BIGINT      NOT NULL,
    pkg_energy_uj   BIGINT,
    core_energy_uj  BIGINT,
    uncore_energy_uj BIGINT,
    dram_energy_uj  BIGINT,
    UNIQUE (hw_id, run_id, timestamp_ns)
);

CREATE TABLE IF NOT EXISTS cpu_samples (
    sample_id           BIGSERIAL   PRIMARY KEY,
    run_id        BIGINT      NOT NULL,
    hw_id               BIGINT      NOT NULL REFERENCES hardware_config(hw_id),
    timestamp_ns        BIGINT      NOT NULL,
    cpu_util_percent    DOUBLE PRECISION,
    cpu_busy_mhz        DOUBLE PRECISION,
    cpu_avg_mhz         DOUBLE PRECISION,
    c1_residency        DOUBLE PRECISION,
    c2_residency        DOUBLE PRECISION,
    c3_residency        DOUBLE PRECISION,
    c6_residency        DOUBLE PRECISION,
    c7_residency        DOUBLE PRECISION,
    pkg_c8_residency    DOUBLE PRECISION,
    pkg_c9_residency    DOUBLE PRECISION,
    pkg_c10_residency   DOUBLE PRECISION,
    package_power       DOUBLE PRECISION,
    dram_power          DOUBLE PRECISION,
    gpu_rc6             DOUBLE PRECISION,
    package_temp        DOUBLE PRECISION,
    ipc                 DOUBLE PRECISION,
    extra_metrics_json  TEXT,
    UNIQUE (hw_id, run_id, timestamp_ns)
);

CREATE TABLE IF NOT EXISTS thermal_samples (
    sample_id       BIGSERIAL   PRIMARY KEY,
    run_id    BIGINT      NOT NULL,
    hw_id           BIGINT      NOT NULL REFERENCES hardware_config(hw_id),
    timestamp_ns    BIGINT      NOT NULL,
    sample_time_s   DOUBLE PRECISION,
    cpu_temp        DOUBLE PRECISION,
    system_temp     DOUBLE PRECISION,
    wifi_temp       DOUBLE PRECISION,
    throttle_event  INTEGER     DEFAULT 0,
    all_zones_json  TEXT,
    sensor_count    INTEGER,
    UNIQUE (hw_id, run_id, timestamp_ns)
);

CREATE TABLE IF NOT EXISTS interrupt_samples (
    sample_id           BIGSERIAL   PRIMARY KEY,
    run_id        BIGINT      NOT NULL,
    hw_id               BIGINT      NOT NULL REFERENCES hardware_config(hw_id),
    timestamp_ns        BIGINT      NOT NULL,
    interrupts_per_sec  DOUBLE PRECISION,
    UNIQUE (hw_id, run_id, timestamp_ns)
);

CREATE TABLE IF NOT EXISTS orchestration_events (
    event_id            BIGSERIAL   PRIMARY KEY,
    run_id        BIGINT      NOT NULL,
    hw_id               BIGINT      NOT NULL REFERENCES hardware_config(hw_id),
    step_index          INTEGER,
    phase               TEXT,
    event_type          TEXT        NOT NULL,
    start_time_ns       BIGINT      NOT NULL,
    end_time_ns         BIGINT      NOT NULL,
    duration_ns         BIGINT      NOT NULL,
    power_watts         DOUBLE PRECISION,
    cpu_util_percent    DOUBLE PRECISION,
    interrupt_rate      DOUBLE PRECISION,
    event_energy_uj     BIGINT,
    tax_contribution_uj BIGINT,
    tax_percent         DOUBLE PRECISION,
    UNIQUE (hw_id, run_id, start_time_ns, event_type)
);

CREATE TABLE IF NOT EXISTS llm_interactions (
    interaction_id          BIGSERIAL   PRIMARY KEY,
    run_id            BIGINT      NOT NULL,
    hw_id                   BIGINT      NOT NULL REFERENCES hardware_config(hw_id),
    step_index              INTEGER,
    workflow_type           TEXT,
    prompt                  TEXT,
    response                TEXT,
    model_name              TEXT,
    provider                TEXT,
    prompt_tokens           INTEGER,
    completion_tokens       INTEGER,
    total_tokens            INTEGER,
    api_latency_ms          DOUBLE PRECISION,
    compute_time_ms         DOUBLE PRECISION,
    created_at              TIMESTAMP,
    app_throughput_kbps     DOUBLE PRECISION,
    total_time_ms           DOUBLE PRECISION,
    preprocess_ms           DOUBLE PRECISION,
    non_local_ms            DOUBLE PRECISION,
    local_compute_ms        DOUBLE PRECISION,
    postprocess_ms          DOUBLE PRECISION,
    cpu_percent_during_wait DOUBLE PRECISION,
    error_message           TEXT,
    status                  TEXT,
    bytes_sent_approx       BIGINT,
    bytes_recv_approx       BIGINT,
    tcp_retransmits         INTEGER
);

CREATE TABLE IF NOT EXISTS orchestration_tax_summary (
    comparison_id           BIGSERIAL   PRIMARY KEY,
    run_id            BIGINT,
    hw_id                   BIGINT      REFERENCES hardware_config(hw_id),
    linear_run_id           BIGINT      NOT NULL,
    agentic_run_id          BIGINT      NOT NULL,
    linear_dynamic_uj       BIGINT,
    agentic_dynamic_uj      BIGINT,
    orchestration_tax_uj    BIGINT,
    tax_percent             DOUBLE PRECISION,
    linear_orchestration_uj BIGINT,
    agentic_orchestration_uj BIGINT,
    UNIQUE (hw_id, linear_run_id, agentic_run_id)
);

CREATE TABLE IF NOT EXISTS outliers (
    outlier_id    BIGSERIAL   PRIMARY KEY,
    run_id  BIGINT      NOT NULL,
    hw_id         BIGINT      NOT NULL REFERENCES hardware_config(hw_id),
    column_name   TEXT        NOT NULL,
    value         DOUBLE PRECISION,
    mean          DOUBLE PRECISION,
    std_dev       DOUBLE PRECISION,
    sigma         DOUBLE PRECISION,
    severity      TEXT        NOT NULL DEFAULT 'mild',
    excluded      INTEGER     NOT NULL DEFAULT 0,
    reason        TEXT,
    detected_at   TIMESTAMP   DEFAULT NOW()
);

-- ── Server-only tables ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS job_queue (
    job_id                  TEXT        PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
    global_exp_id           BIGINT      REFERENCES experiments(global_exp_id),
    experiment_config_json  TEXT        NOT NULL,
    status                  TEXT        DEFAULT 'pending',
    priority                INTEGER     DEFAULT 5,
    on_disconnect           TEXT        DEFAULT 'fail',
    target_hw_id            BIGINT      REFERENCES hardware_config(hw_id),
    dispatched_to_hw_id     BIGINT      REFERENCES hardware_config(hw_id),
    dispatched_at           TIMESTAMP,
    started_at              TIMESTAMP,
    completed_at            TIMESTAMP,
    created_at              TIMESTAMP   DEFAULT NOW(),
    error_message           TEXT,
    retry_count             INTEGER     DEFAULT 0,
    max_retries             INTEGER     DEFAULT 0,
    created_by_hw_id        BIGINT      REFERENCES hardware_config(hw_id)
);

CREATE TABLE IF NOT EXISTS run_status_cache (
    hw_id               BIGINT      PRIMARY KEY REFERENCES hardware_config(hw_id),
    job_id              TEXT,
    run_id              INTEGER,
    exp_id              INTEGER,
    global_run_id       BIGINT,
    workflow_type       TEXT,
    status              TEXT        DEFAULT 'idle',
    elapsed_s           INTEGER,
    energy_uj           BIGINT,
    avg_power_watts     DOUBLE PRECISION,
    total_tokens        INTEGER,
    steps               INTEGER,
    task_name           TEXT,
    model_name          TEXT,
    last_updated        TIMESTAMP   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sync_log (
    log_id              BIGSERIAL   PRIMARY KEY,
    hw_id               BIGINT      NOT NULL REFERENCES hardware_config(hw_id),
    sync_started_at     TIMESTAMP   DEFAULT NOW(),
    sync_completed_at   TIMESTAMP,
    runs_synced         INTEGER     DEFAULT 0,
    rows_total          INTEGER     DEFAULT 0,
    status              TEXT        DEFAULT 'in_progress',
    error_details       TEXT
);

CREATE TABLE IF NOT EXISTS experiment_submissions (
    submission_id       TEXT        PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
    submitted_by_hw_id  BIGINT      REFERENCES hardware_config(hw_id),
    config_json         TEXT        NOT NULL,
    name                TEXT        NOT NULL,
    description         TEXT,
    review_status       TEXT        DEFAULT 'pending_review',
    reviewed_by         TEXT,
    reviewed_at         TIMESTAMP,
    review_notes        TEXT,
    submitted_at        TIMESTAMP   DEFAULT NOW(),
    promoted_to_job_id  TEXT        REFERENCES job_queue(job_id)
);

-- ── Indexes ───────────────────────────────────────────────────────────────────

-- Core analytical queries (hw_id + exp_id)
CREATE INDEX IF NOT EXISTS idx_runs_hw_exp           ON runs(hw_id, exp_id);
CREATE INDEX IF NOT EXISTS idx_runs_hw_exp_workflow   ON runs(hw_id, exp_id, workflow_type);
CREATE INDEX IF NOT EXISTS idx_runs_exp_id            ON runs(exp_id);
CREATE INDEX IF NOT EXISTS idx_runs_hw_id             ON runs(hw_id);
CREATE INDEX IF NOT EXISTS idx_runs_workflow          ON runs(workflow_type);
CREATE INDEX IF NOT EXISTS idx_runs_global_exp        ON runs(global_exp_id);

-- Experiments
CREATE INDEX IF NOT EXISTS idx_exp_hw_id             ON experiments(hw_id);
CREATE INDEX IF NOT EXISTS idx_exp_hw_exp            ON experiments(hw_id, exp_id);

-- Child tables
CREATE INDEX IF NOT EXISTS idx_energy_hw_run         ON energy_samples(hw_id, run_id);
CREATE INDEX IF NOT EXISTS idx_cpu_hw_run            ON cpu_samples(hw_id, run_id);
CREATE INDEX IF NOT EXISTS idx_thermal_hw_run        ON thermal_samples(hw_id, run_id);
CREATE INDEX IF NOT EXISTS idx_interrupt_hw_run      ON interrupt_samples(hw_id, run_id);
CREATE INDEX IF NOT EXISTS idx_orch_hw_run           ON orchestration_events(hw_id, run_id);
CREATE INDEX IF NOT EXISTS idx_llm_hw_run            ON llm_interactions(hw_id, run_id);

-- Server operational
CREATE INDEX IF NOT EXISTS idx_job_status            ON job_queue(status);
CREATE INDEX IF NOT EXISTS idx_job_target_hw         ON job_queue(target_hw_id);
CREATE INDEX IF NOT EXISTS idx_sync_log_hw           ON sync_log(hw_id);
