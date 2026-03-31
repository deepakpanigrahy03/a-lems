# A-LEMS Database Tables — Complete Reference

## Overview

A-LEMS uses two databases:
- **SQLite** (`data/experiments.db`) — local source of truth on every machine
- **PostgreSQL** (`alems_central`) — central aggregation on Oracle VM

---

## SQLite Tables (local, every machine)

### Core measurement tables (created by test_harness)

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `experiments` | One row per experiment run | `exp_id`, `task_name`, `provider`, `model_name`, `workflow_type`, `group_id`, `status` |
| `runs` | One row per individual run | `run_id`, `exp_id`, `workflow_type`, `total_energy_uj`, `duration_ns`, `ipc`, `sync_status`, `sync_samples_status` |
| `energy_samples` | Per-second energy readings during run | `run_id`, `timestamp_ns`, `pkg_energy_uj`, `core_energy_uj` |
| `cpu_samples` | Per-second CPU metrics | `run_id`, `timestamp_ns`, `cpu_util_percent`, `ipc`, `c1_residency`–`c7_residency` |
| `thermal_samples` | Temperature readings | `run_id`, `timestamp_ns`, `package_temp_c` |
| `interrupt_samples` | Interrupt rate samples | `run_id`, `timestamp_ns` |
| `orchestration_events` | Agentic workflow steps | `run_id`, `event_type`, `start_time_ns`, `end_time_ns` |
| `llm_interactions` | Per-LLM-call metrics | `run_id`, `prompt_tokens`, `completion_tokens`, `api_latency_ms`, `non_local_ms` |
| `orchestration_tax_summary` | Linear vs agentic energy comparison | `linear_run_id`, `agentic_run_id`, `tax_percent` |
| `outliers` | Statistical outlier runs | `run_id` |
| `idle_baselines` | Idle power measurements | `baseline_id`, `package_power_watts`, `core_power_watts` |
| `hardware_config` | This machine's hardware profile | `hw_id`, `hostname`, `cpu_model`, `hardware_hash` |
| `environment_config` | Python/OS environment snapshot | `env_id`, `os_name`, `python_version`, `git_commit` |
| `task_categories` | Task → category mapping | `task_id`, `category` |

### Sync tracking columns (added by migration 007)

| Column | Table | Meaning |
|--------|-------|---------|
| `sync_status` | `runs` | 0=pending, 1=synced, 2=failed |
| `sync_samples_status` | `runs` | 0=pending, 1=samples synced |
| `last_seen` | `hardware_config` | Last heartbeat timestamp |
| `agent_status` | `hardware_config` | idle/running/syncing/offline |
| `api_key` | `hardware_config` | Auth key for server API |
| `server_hw_id` | `hardware_config` | hw_id on PostgreSQL server |

### GUI tables (created by db_migrations.py on startup)

| Table | Purpose |
|-------|---------|
| `schema_version` | Migration tracking |
| `coverage_matrix` | Cached experiment coverage per hw×task×model |
| `hypotheses` | Researcher hypothesis notes |
| `gui_tags` | Run tagging |

### SQLite Views (created by schema.py)

| View | Purpose | Key columns |
|------|---------|-------------|
| `ml_features` | Flattened runs+experiments+hardware+environment for ML | All run metrics + hardware specs + env info |
| `orchestration_analysis` | Energy decomposition per run | `workload_energy_j`, `reasoning_energy_j`, `orchestration_tax_j`, `cache_miss_rate` |
| `research_metrics_view` | OOI/UCR ratios for research | `ooi_time`, `ooi_cpu`, `ucr`, `network_ratio` |
| `energy_samples_with_power` | Power (W) derived from energy deltas | `pkg_power_watts`, `core_power_watts`, `time_s` |

---

## PostgreSQL Tables (Oracle VM, alems_central)

### Mirrored from SQLite (populated via bulk-sync)

Same structure as SQLite but with additional server-assigned columns:

| Extra column | Table | Purpose |
|-------------|-------|---------|
| `global_run_id` | `runs` | Server-assigned BIGSERIAL PK |
| `global_exp_id` | `experiments` | Server-assigned BIGSERIAL PK |
| `hw_id` | all tables | References `hardware_config.hw_id` |
| `synced_at` | `runs` | When this run arrived at server |

### Server-only tables

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `job_queue` | Experiment dispatch queue | `job_id`, `status`, `target_hw_id`, `experiment_config_json`, `created_at`, `started_at`, `completed_at` |
| `run_status_cache` | Live status of each machine (1 row/machine) | `hw_id`, `run_id`, `status`, `task_name`, `energy_uj`, `elapsed_s`, `last_updated` |
| `sync_log` | History of sync batches from each agent | `hw_id`, `runs_synced`, `rows_total`, `status`, `sync_completed_at` |
| `experiment_submissions` | Researcher experiment proposals | `submission_id`, `submitted_by_hw_id`, `config_json`, `review_status` |

### PostgreSQL Views (created by 002_postgres_views.sql)

Same 4 views as SQLite — `ml_features`, `orchestration_analysis`, `research_metrics_view`, `energy_samples_with_power` — adapted for PostgreSQL syntax.

---

## sync_status flow

```
New run created by test_harness
    → sync_status = 0 (pending)
    
Agent phase 1 sync: run metadata → PostgreSQL
    → sync_status = 1 (synced)
    
Agent phase 2 sync: energy/cpu/thermal samples → PostgreSQL
    → sync_samples_status = 1 (samples synced)
    
On failure (HTTP 500 from server):
    → sync_status = 2 (failed, retried automatically)
```

---

## job_queue status flow

```
GUI dispatches job → status = 'pending'
Agent polls GET /get-job → status = 'dispatched'
Agent starts execution → status = 'running' (via POST /job-status)
test_harness completes → status = 'completed'
On error → status = 'failed'
```

---

## run_status_cache

One row per machine. Updated by:
1. Agent heartbeat every 5s during run (status=running, live energy/task data)
2. Agent post-completion heartbeat (status=idle, final energy data)
3. Server clears to idle when agent sends status=idle

Used by: Fleet Tab 1 server — live job monitoring.

---

## Key relationships

```
hardware_config (hw_id)
    ↓ FK
experiments (exp_id, hw_id)
    ↓ FK  
runs (run_id, exp_id, hw_id)
    ↓ FK
energy_samples, cpu_samples, thermal_samples,
interrupt_samples, orchestration_events,
llm_interactions, outliers

runs ←→ orchestration_tax_summary (linear_run_id, agentic_run_id)
runs → idle_baselines (baseline_id FK, nullable)
experiments → environment_config (env_id FK)
job_queue → hardware_config (target_hw_id FK)
run_status_cache → hardware_config (hw_id PK)
sync_log → hardware_config (hw_id FK)
```

---

## Quick queries

```bash
# SQLite: sync status breakdown
sqlite3 data/experiments.db \
  "SELECT sync_status, COUNT(*) FROM runs GROUP BY sync_status;"

# PG: runs per machine
PGPASSWORD=Ganesh123 psql -U alems -d alems_central -h localhost -c \
  "SELECT h.hostname, COUNT(*) FROM runs r 
   JOIN hardware_config h ON r.hw_id=h.hw_id GROUP BY h.hostname;"

# PG: last job per machine
PGPASSWORD=Ganesh123 psql -U alems -d alems_central -h localhost -c \
  "SELECT h.hostname, rsc.task_name, rsc.energy_uj, rsc.status, rsc.last_updated
   FROM run_status_cache rsc JOIN hardware_config h ON h.hw_id=rsc.hw_id;"

# PG: job history
PGPASSWORD=Ganesh123 psql -U alems -d alems_central -h localhost -c \
  "SELECT job_id, status, target_hw_id, 
   EXTRACT(EPOCH FROM (completed_at-started_at)) AS duration_s
   FROM job_queue ORDER BY created_at DESC LIMIT 10;"

# PG: sync history  
PGPASSWORD=Ganesh123 psql -U alems -d alems_central -h localhost -c \
  "SELECT h.hostname, s.runs_synced, s.status, s.sync_completed_at
   FROM sync_log s JOIN hardware_config h ON h.hw_id=s.hw_id
   ORDER BY s.log_id DESC LIMIT 10;"
```
