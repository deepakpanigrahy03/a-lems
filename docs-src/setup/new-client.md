# Adding a New Client Machine

## Prerequisites

- A-LEMS repo cloned to `~/mydrive/a-lems`
- Existing venv at `~/mydrive/a-lems/venv` with A-LEMS dependencies
- Network access to Oracle VM port 8000
- Git access to `https://github.com/deepakpanigrahy03/a-lems.git`

---

## Step 1 — Get latest code

```bash
cd ~/mydrive/a-lems
git pull
```

---

## Step 2 — Install agent dependencies

```bash
source venv/bin/activate
pip install httpx pydantic tomli sqlalchemy
```

---

## Step 3 — Run migration

This is the single most important step. Run it once on every new machine.
It is fully idempotent — safe to run multiple times, self-healing if
schema_version was set but columns are missing.

```bash
cd ~/mydrive/a-lems
source venv/bin/activate
python -m alems.migrations.run_migrations
```

Expected output:
```
[migration] SQLite target: /home/<user>/mydrive/a-lems/data/experiments.db
[migration] Applying migration 007 (distributed identity)...
  + experiments.global_exp_id
  + runs.global_run_id
  + runs.sync_status
  + energy_samples.global_run_id
  ... (all child tables)
  + hardware_config.last_seen
  + hardware_config.agent_status
[migration] Migration 007 applied successfully
[migration] Running UUID backfill for existing rows...
[backfill] hw_id=X, db=...
  experiments: N rows backfilled
  runs: N rows backfilled
  ...
[backfill] Complete — N total rows updated
```

### Known issue — schema_version mismatch

If the machine has schema_version >= 7 already set (from a previous
partial run) but columns are missing, the migration self-heals:

```
[migration] Migration 007 version set but columns missing — reapplying...
```

This is handled automatically. No manual intervention needed.

---

## Step 4 — Verify SQLite migration

```bash
python -m alems.tests.test_e2e --sqlite-only
```

Expected output:
```
A-LEMS End-to-End Test Suite
==================================================
[SQLite tests]
  ✓  T01 SQLite migration 007 applied
  ✓  T02 UUID backfill — no NULL global_run_id in runs
  ✓  T02b UUID backfill — no NULL global_exp_id in experiments
  ✓  T02c UUID format valid (uuid4/uuid5)
  ✓  T02d global_run_id unique across all runs
  ✓  T02e child table global_run_id propagated
  ✓  T10 Sync monitor counts consistent
==================================================
Results: 7 passed · 0 failed · 0 skipped
All tests passed ✓
```

**Do not proceed to Step 5 until all 7 pass.**

---

## Step 5 — Create agent config

```bash
mkdir -p ~/.alems
cat > ~/.alems/agent.conf << 'EOF'
[agent]
mode = "connected"
server_url = "http://129.153.71.47:8000"
api_key = ""
hw_id_local = 1
hw_id_server = 0

[sync]
interval_seconds = 60
batch_size = 100
retry_max = 3
retry_backoff_s = 30

[execution]
poll_interval_s = 10
heartbeat_s = 30
heartbeat_run_s = 5
EOF
```

Note: `api_key` is intentionally empty — filled automatically on first
registration with the server.

---

## Step 6 — Verify server reachable

```bash
curl http://129.153.71.47:8000/health
```

Expected:
```json
{"status":"ok","mode":"server","version":"1.0.0","connected_agents":0}
```

If this fails, check network access to port 8000. The agent will retry
automatically but confirm connectivity before starting.

---

## Step 7 — Start the agent

```bash
cd ~/mydrive/a-lems
source venv/bin/activate

# Foreground (development / first run — see registration happen)
python -m alems.agent start

# Background (daily use)
nohup python -m alems.agent start > ~/.alems/agent.log 2>&1 &
echo $! > ~/.alems/agent.pid
```

On first start the agent will:
1. Run UUID backfill check (instant if already done)
2. POST /register → server assigns api_key and server_hw_id
3. Save both to ~/.alems/agent.conf automatically
4. Start heartbeat (30s), poll (10s), sync (60s) threads

---

## Step 8 — Verify full stack

```bash
python -m alems.tests.test_e2e --server http://129.153.71.47:8000
```

Expected: 14 passed · 0 failed · 0 skipped

---

## Step 9 — Check agent status

```bash
python -m alems.agent status
```

Expected output:
```
A-LEMS Agent Status
  mode:          connected
  server_url:    http://129.153.71.47:8000
  registered:    True
  server alive:  True
  local hw_id:   3
  server hw_id:  1
  unsynced runs: 0
  db:            /home/<user>/mydrive/a-lems/data/experiments.db
```

---

## Switching modes

```bash
# Go offline (local experiments only, no sync)
python -m alems.agent set-mode local

# Reconnect (triggers catch-up sync of all unsynced runs)
python -m alems.agent set-mode connected
```

---

## Useful commands

```bash
# Stop background agent
kill $(cat ~/.alems/agent.pid)

# Watch agent logs
tail -f ~/.alems/agent.log

# Check unsynced runs
sqlite3 data/experiments.db \
  "SELECT sync_status, COUNT(*) FROM runs GROUP BY sync_status"
# 0 = unsynced, 1 = synced, 2 = failed

# Force immediate sync
python -m alems.agent set-mode connected  # triggers sync on reconnect
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ModuleNotFoundError: alems` | sys.path missing project root | Add `sys.path.insert(0, ...)` to streamlit_app.py — see install.md |
| `no such column: global_exp_id` | Migration ran but columns missing | Re-run `python -m alems.migrations.run_migrations` — self-heals |
| `Invalid api_key` from server | api_key empty or wrong | Delete api_key from agent.conf, restart agent to re-register |
| Agent shows `local` mode | agent.conf mode = local | `python -m alems.agent set-mode connected` |
| Sync fails repeatedly | Server unreachable | Check `curl http://129.153.71.47:8000/health` |
| UUID format error in uuid_gen.py | Namespace UUID had non-hex chars | Use `a1e05000-0000-4000-8000-000000000001` as namespace |
