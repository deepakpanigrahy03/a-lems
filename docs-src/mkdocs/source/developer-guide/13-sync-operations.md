# Sync & Backload Operations

## Architecture

```
Local SQLite  ──(outbound HTTP)──▶  FastAPI :8000  ──▶  PostgreSQL
(source of truth)                   Oracle VM           alems_central
```

Server never writes to SQLite. All sync is outbound from agent.

---

## Sync status reference

**`runs.sync_status`**

| Value | Meaning | Action |
|-------|---------|--------|
| 0 | Unsynced | Will sync on next agent cycle |
| 1 | Synced | Run metadata in PostgreSQL |
| 2 | Failed | Will retry; reset with button or SQL |

**`runs.sync_samples_status`**

| Value | Meaning |
|-------|---------|
| 0 | Samples pending phase-2 sync |
| 1 | Samples synced |

**Two-phase sync:**

- **Phase 1** — run metadata (experiments + runs only, no sample rows) — batch size 50
- **Phase 2** — child tables (energy_samples, cpu_samples, thermal_samples, interrupt_samples, etc.) — batch size 3 (large rows)

Phase 2 only runs after all phase-1 runs are synced.

---

## Sync via GUI (Fleet Control)

Open `streamlit_app.py` → Command Centre → **Fleet Control** → **⟳ Sync & Connect** tab.

### Connect to server

1. Enter server URL: `http://129.153.71.47:8000`
2. Click **🔗 Connect** — saves to `~/.alems/agent.conf`
3. Run `python -m alems.agent start` — sync begins automatically

### Sync status counters

Five counters shown: Total / Synced / Pending / Failed / Samples pending

### Manual sync controls

| Button | What it does |
|--------|-------------|
| ⬆ Sync pending now | Calls `sync_unsynced_runs()` immediately in-process |
| 🔄 Reset failed → retry | Sets `sync_status=2` → `0` for all failed rows |
| ⬆ Sync samples now | Triggers phase-2 for runs with `sync_samples_status=0` |

!!! warning "Connect first"
    Sync buttons are disabled in local mode. Connect to server before using them.

---

## Sync via CLI

```bash
cd ~/mydrive/a-lems && source venv/bin/activate

# Check connection and sync status
python -m alems.agent status

# Start agent (auto-syncs every 60s in background)
python -m alems.agent start

# Monitor sync progress (separate terminal)
watch -n 5 'sqlite3 data/experiments.db \
  "SELECT sync_status, COUNT(*) FROM runs GROUP BY sync_status;"'
```

---

## Backload historical runs

Backload = syncing runs that existed before agent was configured.

**Automatic** — agent picks up all `sync_status=0` runs automatically on start. No special command needed.

```bash
# Start agent — it will work through the backlog in 50-run batches
python -m alems.agent start

# Expected log output during backload:
# [sync] Phase 1: 50 runs metadata synced
# [sync] Phase 1: 50 runs metadata synced
# ... (repeats until sync_status=0 count reaches 0)
# [sync] Phase 2: 3 runs samples synced
# ... (repeats until sync_samples_status=0 count reaches 0)
```

**How long does it take?**

- Phase 1: ~1-2 seconds per batch of 50 runs → 1500 runs ≈ 1 minute
- Phase 2: ~5-30 seconds per batch of 3 runs (depends on sample count) → varies

**Reset and retry failed runs:**

```bash
sqlite3 data/experiments.db \
  "UPDATE runs SET sync_status=0 WHERE sync_status=2;"
# Agent retries automatically on next cycle
```

---

## Verify sync worked

```bash
# Local: all should be sync_status=1
sqlite3 data/experiments.db \
  "SELECT sync_status, COUNT(*) FROM runs GROUP BY sync_status;"
# Healthy output:
# 1|1554

# Oracle VM: PG count should match
psql -U alems -d alems_central -h localhost -c "
  SELECT 'runs' AS tbl, COUNT(*) FROM runs
  UNION ALL SELECT 'experiments', COUNT(*) FROM experiments
  UNION ALL SELECT 'energy_samples', COUNT(*) FROM energy_samples;"

# Samples check
sqlite3 data/experiments.db \
  "SELECT sync_samples_status, COUNT(*) FROM runs GROUP BY sync_samples_status;"
# Healthy: 1|1554
```

---

## Debug

### Agent won't connect

```bash
# Check server alive
curl http://129.153.71.47:8000/health
# Expected: {"status":"ok","mode":"server",...}

# Check agent config
cat ~/.alems/agent.conf
# Must have: mode=connected, server_url=..., api_key=<non-empty>

# Re-register if api_key is empty
sed -i 's/api_key = .*/api_key = ""/' ~/.alems/agent.conf
python -m alems.agent start  # will re-register on startup
```

### HTTP 500 during bulk-sync

```bash
# On Oracle VM — check exact error
sudo journalctl -u alems-api -n 30 --no-pager

# Common errors and fixes:
```

| Error | Cause | Fix |
|-------|-------|-----|
| `runs_baseline_id_fkey` FK violation | Baseline not in PG | Fixed: all baselines sent every batch |
| `global_exp_id NOT NULL` | Experiment not flushed before run insert | Fixed: session.flush() + map |
| `column does not exist` | Schema mismatch PG vs SQLite | Run `001_postgres_initial.sql` on fresh DB |
| `UNIQUE constraint` | Duplicate sync attempt | Safe — ON CONFLICT DO UPDATE handles it |

### Runs stuck at sync_status=2

```bash
# Reset to 0 and let agent retry
sqlite3 data/experiments.db \
  "UPDATE runs SET sync_status=0 WHERE sync_status=2;"
```

### Check what's in PG vs SQLite

```bash
# On Oracle VM
psql -U alems -d alems_central -h localhost -c "
  SELECT h.hostname, COUNT(*) AS pg_runs
  FROM runs r JOIN hardware_config h ON r.hw_id = h.hw_id
  GROUP BY h.hostname;"

# On local machine
sqlite3 data/experiments.db \
  "SELECT sync_status, COUNT(*) FROM runs GROUP BY sync_status;"
```

---

## Key files

| File | Role |
|------|------|
| `alems/agent/sync_client.py` | Phase 1+2 sync, payload builder, mark functions |
| `alems/agent/agent.py` | Sync thread (60s loop) |
| `alems/agent/mode_manager.py` | Reads `~/.alems/agent.conf` |
| `alems/server/main.py` | `/bulk-sync` endpoint |
| `alems/migrations/007_distributed_identity.sql` | `sync_status`, `sync_samples_status` columns |
| `gui/pages/fleet.py` | GUI sync controls |

---

## Healthy state

```
sqlite sync_status:  1 | 1554   (all synced)
sqlite samples:      1 | 1554   (all samples synced)
PG runs count:       1554       (matches)
Agent:               mode=connected, registered=True, server alive=True
```
