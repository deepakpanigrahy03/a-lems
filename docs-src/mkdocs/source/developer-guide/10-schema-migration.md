# Schema Design

## SQLite → PostgreSQL mapping

### What changes

Only **additive** changes to existing SQLite schema. No column renames, no type changes, no table drops.

| Table | Column added | Type | Purpose |
|-------|-------------|------|---------|
| `runs` | `global_run_id` | TEXT | UUID, PK in PostgreSQL |
| `runs` | `sync_status` | INTEGER DEFAULT 0 | 0=unsynced, 1=synced, 2=failed, 3=skipped |
| `experiments` | `global_exp_id` | TEXT | UUID, PK in PostgreSQL |
| `energy_samples` | `global_run_id` | TEXT | Denormalised FK for fast sync |
| `cpu_samples` | `global_run_id` | TEXT | " |
| `thermal_samples` | `global_run_id` | TEXT | " |
| `interrupt_samples` | `global_run_id` | TEXT | " |
| `orchestration_events` | `global_run_id` | TEXT | " |
| `llm_interactions` | `global_run_id` | TEXT | " |
| `orchestration_tax_summary` | `global_run_id` | TEXT | " |
| `hardware_config` | `last_seen` | TIMESTAMP | Updated by every heartbeat |
| `hardware_config` | `agent_status` | TEXT | offline/idle/busy/syncing |
| `hardware_config` | `agent_version` | TEXT | Semver string |
| `hardware_config` | `server_hw_id` | INTEGER | Server-assigned hw_id |
| `hardware_config` | `api_key` | TEXT | Auth token (lazy migration) |

### sync_status values

```
0 = unsynced     (default — all rows start here)
1 = synced       (successfully sent to PostgreSQL)
2 = sync_failed  (will retry on next sync cycle)
3 = skipped      (local mode, user opted out)
```

### Migration 007

Applied by `python -m alems.migrations.run_migrations`. Idempotent — checks each `ALTER TABLE` before running. Safe to run multiple times.

After migration, backfill runs automatically:
- 1554 existing runs → deterministic UUIDs via `uuid5(namespace, "run:{hw_id}:{run_id}")`
- 436 existing experiments → `uuid5(namespace, "exp:{hw_id}:{exp_id}")`
- All 7 child tables → `global_run_id` propagated from parent `runs` row

## PostgreSQL key design

### Primary keys

| Table | PK | Rationale |
|-------|-----|-----------|
| `experiments` | `global_exp_id TEXT` | UUID from local machine |
| `runs` | `global_run_id TEXT` | UUID from local machine |
| `hardware_config` | `hw_id BIGSERIAL` | Server assigns, machines map via `hardware_hash` |
| `energy_samples` | `sample_id BIGSERIAL` | Server assigns |
| All other child tables | `BIGSERIAL` | Server assigns |

### Collision prevention

```sql
-- experiments: two machines can have exp_id=1 independently
UNIQUE (hw_id, exp_id)

-- runs: same
UNIQUE (hw_id, run_id)

-- child tables: sync is idempotent
UNIQUE (global_run_id, timestamp_ns)      -- energy/cpu/thermal/interrupt
UNIQUE (global_run_id, start_time_ns, event_type)  -- orchestration_events
UNIQUE (linear_run_id, agentic_run_id)    -- orchestration_tax_summary
```

### Server-only tables (no SQLite equivalent)

```sql
job_queue            -- experiment dispatch queue
run_status_cache     -- live run metrics per machine (heartbeat updates)
sync_log             -- per-machine sync history
experiment_submissions -- researcher → admin review queue
```

## Type mapping (SQLite → PostgreSQL)

| SQLite | PostgreSQL |
|--------|-----------|
| `INTEGER PRIMARY KEY AUTOINCREMENT` | `BIGSERIAL PRIMARY KEY` |
| `INTEGER` | `BIGINT` |
| `REAL` | `DOUBLE PRECISION` |
| `TEXT` | `TEXT` |
| `BOOLEAN` | `BOOLEAN` |
| `BIGINT` | `BIGINT` |
| `TIMESTAMP` | `TIMESTAMP` |
| `datetime('now')` | `NOW()` |

All column names preserved exactly. No renames.
