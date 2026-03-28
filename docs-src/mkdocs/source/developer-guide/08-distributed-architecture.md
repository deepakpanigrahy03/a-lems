# A-LEMS Distributed Architecture

## Overview

A-LEMS runs as a **hub-and-spoke** system. The Oracle VM is the brain — it holds the job queue, the PostgreSQL database, and the Streamlit dashboard visible to everyone. Local researcher machines are worker nodes — each runs the full A-LEMS stack locally (SQLite + Streamlit) and optionally connects to the server as an agent.

![Architecture diagram](../diagrams/architecture.svg)

## Three operating modes

Every machine in the system operates in exactly one of three modes at any time:

| Mode | Who | Database | Server connection |
|------|-----|----------|-------------------|
| `server` | Oracle VM | PostgreSQL | IS the server |
| `connected` | Local machine, agent running | SQLite (local) + syncs to PG | Outbound HTTP to :8000 |
| `local` | Local machine, agent stopped | SQLite only | None |

Mode is detected automatically from the environment:

```python
# server  → ALEMS_DB_URL starts with "postgresql://"
# local   → no ALEMS_DB_URL, ~/.alems/agent.conf mode = "local"
# connected → ~/.alems/agent.conf mode = "connected"
```

Every Streamlit page reads this at render time and adjusts its display accordingly. The same page file runs correctly in all three modes.

## Write path (critical — never changes)

```
test_harness subprocess
    ↓ writes directly
SQLite on local machine
    ↓ async, after run completes
Agent sync thread
    ↓ POST /bulk-sync
FastAPI on Oracle VM
    ↓ INSERT ON CONFLICT DO NOTHING
PostgreSQL alems_central
```

**The server is never in the write path.** If the server is down, experiments keep running and data accumulates in SQLite with `sync_status=0`. When the server comes back, the sync thread picks up all pending rows automatically.

## Real-time status path

```
Agent (during active run)
    ↓ POST /heartbeat every 5s carrying live metrics
FastAPI
    ↓ UPSERT
run_status_cache table (PostgreSQL)
    ↓ SELECT
Streamlit server dashboard
```

The server never connects to any agent. All connections are outbound from agent to server.

## Key design decisions

### 1. UUID bridge for collision-free sync

Local SQLite uses `AUTOINCREMENT` integers for all primary keys. Two machines both have `run_id=1`, `run_id=2`, etc. To prevent collision in PostgreSQL:

- `global_run_id` (UUID) added to `runs` — generated locally at row creation, used as PK in PostgreSQL
- `global_exp_id` (UUID) added to `experiments` — same pattern
- All 7 child tables carry `global_run_id` as a denormalised FK — avoids joins during sync

Local integer IDs are preserved as reference columns in PostgreSQL. Existing queries using `run_id` continue to work unchanged.

### 2. hardware_hash as stable machine identity

`hardware_config.hardware_hash` (already existed, already UNIQUE) is the fingerprint of a machine. When a machine registers with the server:

```
POST /register {hardware_hash, ...}
→ server upserts hardware_config by hardware_hash
→ returns server-side hw_id + api_key
→ agent saves both to ~/.alems/agent.conf
```

The server's `hw_id` may differ from the local `hw_id` — `hardware_hash` is the stable bridge.

### 3. No parallel jobs per machine

Energy measurements are invalidated when two workloads run simultaneously on the same machine. Enforcement is double-checked:

- **Server**: before dispatching a job, queries `job_queue WHERE dispatched_to_hw_id = ? AND status = 'running'` — if any active job found, returns `{job: null}`
- **Agent**: checks `_active_run` threading Event before accepting any job from the poll response

### 4. Same SQLAlchemy queries on both databases

`alems/shared/db_layer.py` provides a dialect-aware engine factory. All queries use SQLAlchemy `text()` with named parameters. The two dialect differences handled explicitly:

- `NOW()` vs `datetime('now')`
- `ON CONFLICT (col) DO UPDATE` (same syntax in both — SQLite 3.24+)
- PostgreSQL `BIGSERIAL` vs SQLite `INTEGER AUTOINCREMENT`

### 5. Configurable offline behaviour

When an agent goes offline mid-run, behaviour is controlled by `on_disconnect` field set per job:

| Value | Behaviour |
|-------|-----------|
| `fail` | Mark job failed, do not requeue (default — preserves measurement integrity) |
| `requeue` | Mark pending again, dispatch to next available machine |
| `wait` | Leave as running, agent resumes on reconnect |

## Component inventory

| Component | File | Purpose |
|-----------|------|---------|
| FastAPI server | `alems/server/main.py` | All HTTP endpoints |
| Agent main loop | `alems/agent/agent.py` | 3 threads: heartbeat, poll, sync |
| Heartbeat client | `alems/agent/heartbeat.py` | All outbound HTTP calls |
| Sync client | `alems/agent/sync_client.py` | SQLite → bulk-sync with retry |
| Job executor | `alems/agent/job_executor.py` | Wraps test_harness subprocess |
| Mode manager | `alems/agent/mode_manager.py` | Reads/writes ~/.alems/agent.conf |
| UUID generator | `alems/shared/uuid_gen.py` | Deterministic backfill + random new |
| DB layer | `alems/shared/db_layer.py` | SQLAlchemy dialect abstraction |
| Pydantic models | `alems/shared/models.py` | HTTP API contract |
| Migration 007 | `alems/migrations/007_distributed_identity.sql` | SQLite schema additions |
| PG schema | `alems/migrations/001_postgres_initial.sql` | Full PostgreSQL schema |
| Backfill | `alems/agent/backfill.py` | UUID assignment for existing 1554 runs |

## Ports and services (Oracle VM)

| Port | Service | Managed by |
|------|---------|-----------|
| 8501 | Streamlit UI | existing (manual or systemd) |
| 8000 | FastAPI orchestration API | `alems-api.service` (systemd) |
| 5432 | PostgreSQL | `postgresql.service` (systemd) |
