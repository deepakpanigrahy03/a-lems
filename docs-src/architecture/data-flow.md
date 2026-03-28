# Data Flow & Migration Guide

## Core principle — every machine is symmetric

Every machine in A-LEMS (including the Oracle VM itself) follows
the identical data flow:

```
test_harness subprocess
    ↓ writes directly (no network)
local SQLite  ~/mydrive/a-lems/data/experiments.db
    ↓ async, after run completes, agent sync thread
POST /bulk-sync → FastAPI on Oracle VM :8000
    ↓ INSERT ON CONFLICT DO NOTHING
PostgreSQL alems_central (Oracle VM only)
```

PostgreSQL is never in the write path. Server being down = zero impact
on running experiments. Data accumulates in SQLite with sync_status=0
and syncs automatically when server is reachable.

---

## Oracle VM is just another machine

The Oracle VM is special in only one way: it hosts PostgreSQL and
the FastAPI server. For everything else it behaves identically to
any researcher laptop:

- Runs test_harness locally → writes to its own SQLite
- Runs its own agent → syncs to localhost:8000
- Its hw_id in PostgreSQL = 1 (first registered machine)

```toml
# ~/.alems/agent.conf on Oracle VM
[agent]
mode = "connected"
server_url = "http://localhost:8000"   ← localhost, not public IP
```

---

## Migration process — local only, no network

Migration runs independently on each machine. No remote connections.

### What migration 007 does (SQLite)

Adds these columns to the existing database:

| Table | Column | Purpose |
|-------|--------|---------|
| runs | global_run_id TEXT | UUID, collision-free PK in PostgreSQL |
| runs | sync_status INTEGER DEFAULT 0 | 0=unsynced, 1=synced, 2=failed |
| experiments | global_exp_id TEXT | UUID for experiments |
| energy_samples + 6 others | global_run_id TEXT | Denormalised for fast sync |
| hardware_config | last_seen, agent_status, agent_version, server_hw_id | Agent tracking |

Then backfills all existing rows with deterministic UUIDs:
- `uuid5(namespace, "run:{hw_id}:{run_id}")` for each existing run
- Same inputs always produce same UUID — safe to rerun

### Run order

```bash
# Step 1 — on every machine (SQLite migration + backfill)
python -m alems.migrations.run_migrations

# Step 2 — on Oracle VM only (PostgreSQL schema creation)
ALEMS_DB_URL=postgresql://alems:password@localhost/alems_central \
    python -m alems.migrations.run_migrations --postgres

# Step 3 — verify
python -m alems.tests.test_e2e --sqlite-only
```

---

## Three services on Oracle VM

All three run as separate processes:

| Service | Command | Port | systemd unit |
|---------|---------|------|-------------|
| Streamlit UI | streamlit run streamlit_app.py | 8501 | existing |
| FastAPI orchestration | uvicorn alems.server.main:app | 8000 | alems-api.service |
| Local agent | python -m alems.agent start | — | alems-agent.service |

The agent on the VM connects to its own FastAPI via localhost:8000.

---

## UI mode detection

Every distributed page detects its mode at render time:

```python
ALEMS_DB_URL starts with "postgresql://"  →  mode = "server"
~/.alems/agent.conf mode = "connected"    →  mode = "connected"
otherwise                                 →  mode = "local"
```

| Mode | What the page shows |
|------|---------------------|
| server | All machines, full admin controls, PostgreSQL data |
| connected | Own machine + server data via API, sync status |
| local | Own machine only from SQLite, prompt to connect |

For `alems` package to be importable from Streamlit, ensure
`sys.path` includes the project root. Add to top of `streamlit_app.py`:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
```

---

## Sync status values

```
sync_status = 0  →  unsynced (default for all new rows)
sync_status = 1  →  synced successfully to PostgreSQL
sync_status = 2  →  sync failed, will retry next cycle (60s)
sync_status = 3  →  skipped (local mode, user opted out)
```

Sync is always idempotent — sending the same run twice to PostgreSQL
uses ON CONFLICT DO NOTHING, so duplicates are impossible.