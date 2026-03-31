-- ============================================================
-- A-LEMS Migration 002 — PostgreSQL Views
-- Mirrors SQLite views from core/database/schema.py
-- Safe to run multiple times (CREATE OR REPLACE)
-- Run: psql -U alems -d alems_central -h localhost -f 002_postgres_views.sql
-- ============================================================

-- ── ml_features ───────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW ml_features AS
SELECT
    r.run_id, r.exp_id, e.group_id, r.run_number, r.workflow_type,
    e.task_name, e.provider, e.model_name, e.country_code, e.optimization_enabled,
    h.cpu_model, h.cpu_cores, h.cpu_threads, h.cpu_architecture, h.cpu_vendor,
    h.cpu_family, h.cpu_model_id, h.cpu_stepping,
    h.has_avx2, h.has_avx512, h.has_vmx,
    h.gpu_model, h.gpu_driver, h.gpu_count, h.ram_gb,
    h.system_type, h.system_manufacturer, h.system_product,
    h.virtualization_type, h.microcode_version,
    env.python_version, env.git_commit, env.git_branch, env.git_dirty,
    r.total_energy_uj/1e6                          AS total_energy_j,
    r.dynamic_energy_uj/1e6                        AS dynamic_energy_j,
    r.baseline_energy_uj/1e6                       AS baseline_energy_j,
    r.pkg_energy_uj/1e6                            AS pkg_energy_j,
    r.core_energy_uj/1e6                           AS core_energy_j,
    r.uncore_energy_uj/1e6                         AS uncore_energy_j,
    r.dram_energy_uj/1e6                           AS dram_energy_j,
    r.avg_power_watts,
    r.duration_ns/1e9                              AS duration_sec,
    r.duration_ns/1e6                              AS duration_ms,
    r.instructions, r.cycles, r.ipc,
    r.cache_misses, r.cache_references, r.cache_miss_rate,
    r.page_faults, r.major_page_faults, r.minor_page_faults,
    r.context_switches_voluntary, r.context_switches_involuntary, r.total_context_switches,
    r.thread_migrations, r.run_queue_length, r.kernel_time_ms, r.user_time_ms,
    r.frequency_mhz, r.ring_bus_freq_mhz, r.cpu_busy_mhz, r.cpu_avg_mhz,
    r.package_temp_celsius, r.baseline_temp_celsius,
    r.start_temp_c, r.max_temp_c, r.min_temp_c, r.thermal_delta_c,
    r.thermal_during_experiment, r.thermal_now_active, r.thermal_since_boot,
    r.experiment_valid,
    r.c2_time_seconds, r.c3_time_seconds, r.c6_time_seconds, r.c7_time_seconds,
    r.swap_total_mb, r.swap_end_free_mb, r.swap_start_used_mb, r.swap_end_used_mb,
    r.swap_start_cached_mb, r.swap_end_cached_mb, r.swap_end_percent,
    r.wakeup_latency_us, r.interrupt_rate, r.thermal_throttle_flag,
    r.rss_memory_mb, r.vms_memory_mb,
    r.total_tokens, r.prompt_tokens, r.completion_tokens,
    r.dns_latency_ms, r.api_latency_ms, r.compute_time_ms,
    r.bytes_sent, r.bytes_recv, r.tcp_retransmits,
    r.governor, r.turbo_enabled, r.is_cold_start, r.background_cpu_percent, r.process_count,
    r.planning_time_ms, r.execution_time_ms, r.synthesis_time_ms,
    r.phase_planning_ratio, r.phase_execution_ratio, r.phase_synthesis_ratio,
    r.llm_calls, r.tool_calls, r.tools_used, r.steps, r.avg_step_time_ms,
    r.complexity_level, r.complexity_score,
    r.carbon_g, r.water_ml, r.methane_mg,
    r.energy_per_instruction, r.energy_per_cycle, r.energy_per_token,
    r.instructions_per_token, r.interrupts_per_second,
    ib.package_power_watts  AS baseline_package_power,
    ib.core_power_watts     AS baseline_core_power,
    ib.uncore_power_watts   AS baseline_uncore_power,
    ib.dram_power_watts     AS baseline_dram_power,
    ib.governor             AS baseline_governor,
    ib.turbo                AS baseline_turbo,
    ib.background_cpu       AS baseline_background_cpu,
    ib.process_count        AS baseline_process_count,
    ots.orchestration_tax_uj/1e6  AS orchestration_tax_j,
    ots.tax_percent,
    r.run_state_hash
FROM runs r
JOIN experiments e              ON r.exp_id    = e.exp_id AND r.hw_id = e.hw_id
LEFT JOIN hardware_config h     ON r.hw_id     = h.hw_id
LEFT JOIN environment_config env ON e.env_id   = env.env_id
LEFT JOIN idle_baselines ib     ON r.baseline_id = ib.baseline_id
LEFT JOIN orchestration_tax_summary ots ON r.global_run_id = ots.agentic_run_id;


-- ── orchestration_analysis ────────────────────────────────────────────────────
CREATE OR REPLACE VIEW orchestration_analysis AS
SELECT
    r.run_id, r.exp_id, r.workflow_type, r.run_number,
    r.pkg_energy_uj/1e6                                            AS pkg_energy_j,
    r.core_energy_uj/1e6                                           AS core_energy_j,
    r.uncore_energy_uj/1e6                                         AS uncore_energy_j,
    r.dram_energy_uj/1e6                                           AS dram_energy_j,
    r.duration_ns/1e9                                              AS duration_sec,
    ib.package_power_watts * (r.duration_ns/1e9)                   AS baseline_pkg_j,
    ib.core_power_watts    * (r.duration_ns/1e9)                   AS baseline_core_j,
    ib.uncore_power_watts  * (r.duration_ns/1e9)                   AS baseline_uncore_j,
    (r.pkg_energy_uj/1e6)   - (ib.package_power_watts * (r.duration_ns/1e9)) AS workload_energy_j,
    (r.core_energy_uj/1e6)  - (ib.core_power_watts    * (r.duration_ns/1e9)) AS reasoning_energy_j,
    (r.uncore_energy_uj/1e6)- (ib.uncore_power_watts  * (r.duration_ns/1e9)) AS orchestration_tax_j,
    CASE WHEN r.instructions > 0
         THEN ((r.core_energy_uj/1e6) - (ib.core_power_watts * (r.duration_ns/1e9)))
              / (r.instructions/1e9::DOUBLE PRECISION)
         ELSE NULL END                                             AS joules_per_billion_instructions,
    CASE WHEN r.cycles > 0
         THEN r.instructions::DOUBLE PRECISION / r.cycles
         ELSE NULL END                                             AS ipc,
    CASE WHEN r.cache_references > 0
         THEN r.cache_misses::DOUBLE PRECISION / r.cache_references
         ELSE NULL END                                             AS cache_miss_rate,
    CASE WHEN r.pkg_energy_uj > 0
         THEN (r.core_energy_uj::DOUBLE PRECISION)   / r.pkg_energy_uj ELSE 0 END AS core_share,
    CASE WHEN r.pkg_energy_uj > 0
         THEN (r.uncore_energy_uj::DOUBLE PRECISION) / r.pkg_energy_uj ELSE 0 END AS uncore_share,
    e.provider, e.task_name, e.country_code
FROM runs r
JOIN experiments e          ON r.exp_id    = e.exp_id AND r.hw_id = e.hw_id
JOIN idle_baselines ib      ON r.baseline_id = ib.baseline_id
WHERE r.baseline_id IS NOT NULL;


-- ── research_metrics_view ─────────────────────────────────────────────────────
CREATE OR REPLACE VIEW research_metrics_view AS
SELECT
    r.run_id, r.exp_id, e.provider, r.workflow_type,
    r.duration_ns / 1e6                                    AS total_time_ms,
    r.compute_time_ms,
    r.orchestration_cpu_ms,
    r.bytes_sent                                            AS total_bytes_sent,
    r.bytes_recv                                            AS total_bytes_recv,
    COALESCE(i.total_wait_ms, 0)                           AS total_wait_ms,
    COALESCE(i.total_llm_compute_ms, 0)                    AS total_llm_compute_ms,
    CASE WHEN r.duration_ns > 0
         THEN r.orchestration_cpu_ms / (r.duration_ns / 1e6)         ELSE 0 END AS ooi_time,
    CASE WHEN r.compute_time_ms > 0
         THEN r.orchestration_cpu_ms / r.compute_time_ms              ELSE 0 END AS ooi_cpu,
    CASE WHEN r.duration_ns > 0
         THEN COALESCE(i.total_llm_compute_ms,0) / (r.duration_ns/1e6) ELSE 0 END AS ucr,
    CASE WHEN r.duration_ns > 0
         THEN COALESCE(i.total_wait_ms,0) / (r.duration_ns/1e6)       ELSE 0 END AS network_ratio
FROM runs r
JOIN experiments e ON r.exp_id = e.exp_id AND r.hw_id = e.hw_id
LEFT JOIN (
    SELECT run_id,
           SUM(non_local_ms)      AS total_wait_ms,
           SUM(local_compute_ms)  AS total_llm_compute_ms
    FROM llm_interactions
    GROUP BY run_id
) i ON r.run_id = i.run_id AND r.hw_id = (
    SELECT hw_id FROM runs WHERE run_id = r.run_id LIMIT 1
)
WHERE (i.total_llm_compute_ms > 0 OR i.total_wait_ms > 0)
  AND (r.orchestration_cpu_ms <= r.compute_time_ms * 5);


-- ── energy_samples_with_power ─────────────────────────────────────────────────
-- Uses LAG() window function instead of correlated subquery (PG-idiomatic)
CREATE OR REPLACE VIEW energy_samples_with_power AS
SELECT
    e1.sample_id, e1.run_id, e1.hw_id,
    e1.timestamp_ns / 1e9                                          AS time_s,
    e1.pkg_energy_uj, e1.core_energy_uj, e1.uncore_energy_uj, e1.dram_energy_uj,
    CASE WHEN (e1.timestamp_ns - LAG(e1.timestamp_ns) OVER w) > 0
         THEN (e1.pkg_energy_uj    - LAG(e1.pkg_energy_uj)    OVER w)::DOUBLE PRECISION
              / ((e1.timestamp_ns  - LAG(e1.timestamp_ns)     OVER w) / 1e9) / 1e6
         ELSE NULL END AS pkg_power_watts,
    CASE WHEN (e1.timestamp_ns - LAG(e1.timestamp_ns) OVER w) > 0
         THEN (e1.core_energy_uj   - LAG(e1.core_energy_uj)   OVER w)::DOUBLE PRECISION
              / ((e1.timestamp_ns  - LAG(e1.timestamp_ns)     OVER w) / 1e9) / 1e6
         ELSE NULL END AS core_power_watts,
    CASE WHEN (e1.timestamp_ns - LAG(e1.timestamp_ns) OVER w) > 0
         THEN (e1.uncore_energy_uj - LAG(e1.uncore_energy_uj) OVER w)::DOUBLE PRECISION
              / ((e1.timestamp_ns  - LAG(e1.timestamp_ns)     OVER w) / 1e9) / 1e6
         ELSE NULL END AS uncore_power_watts,
    CASE WHEN (e1.timestamp_ns - LAG(e1.timestamp_ns) OVER w) > 0
         THEN (e1.dram_energy_uj   - LAG(e1.dram_energy_uj)   OVER w)::DOUBLE PRECISION
              / ((e1.timestamp_ns  - LAG(e1.timestamp_ns)     OVER w) / 1e9) / 1e6
         ELSE NULL END AS dram_power_watts
FROM energy_samples e1
WINDOW w AS (PARTITION BY e1.run_id, e1.hw_id ORDER BY e1.timestamp_ns);
