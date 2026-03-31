# A-LEMS Sync & Backload — Operations Guide

## Architecture

```
Local SQLite (source of truth)
    ↓ agent sync_client.py — outbound HTTP only
Oracle VM FastAPI :8000
    ↓ bulk-sync endpoint
PostgreSQL alems_central
    ↑ read-only
streamlit_server.py :8502
```

**Sync is always outbound from agent → server. Server never touches SQLite.**

---

## Sync status values

| Column | Value | Meaning |
|--------|-------|---------|
| sync_status | 0 | Unsynced — not yet sent to PG |
| sync_status | 1 | Synced — run metadata in PG |
| sync_status | 2 | Failed — will retry next cycle |
| sync_samples_status | 0 | Samples not yet synced (phase 2) |
| sync_samples_status | 1 | Samples synced to PG |

**Two-phase sync:**
- Phase 1: run metadata (experiments + runs, no sample tables) — fast, ~100 runs/batch
- Phase 2: child samples (energy_samples, cpu_samples, thermal_samples, etc.) — 3 runs/batch (large rows)

---

## How to sync — GUI

Open `streamlit_app.py` → **Fleet Control** → **⟳ Sync & Connect** tab.

| Button | What it does |
|--------|-------------|
| 🔗 Connect | Sets mode=connected, saves server URL to `~/.alems/agent.conf` |
| ⏹ Disconnect | Sets mode=local |
| ⟳ Check server | Pings `/health` endpoint |
| ⬆ Sync pending now | Calls `sync_unsynced_runs()` immediately in-process |
| 🔄 Reset failed → retry | Sets sync_status=0 for all sync_status=2 rows |
| ⬆ Sync samples now | Calls `_sync_pending_samples()` for phase-2 rows |

**Sync runs automatically** when agent is running — every 60s in background thread.

---

## How to sync — CLI (recommended for backload)

```bash
cd ~/mydrive/a-lems && source venv/bin/activate

# Check status
python -m alems.agent status

# Start agent (runs sync in background automatically)
python -m alems.agent start

# Check sync counts
sqlite3 data/experiments.db \
  "SELECT sync_status, COUNT(*) FROM runs GROUP BY sync_status;"

# Reset failed runs and let agent retry
sqlite3 data/experiments.db \
  "UPDATE runs SET sync_status=0 WHERE sync_status=2;"
```

---

## Backloading historical runs

Backload = syncing runs that existed before the agent was set up.

**Normal flow (automatic):** Agent picks up `sync_status=0` runs in batches of 50, retries `sync_status=2` runs automatically.

**Force immediate backload:**
```bash
# On local machine — start agent, it will sync everything
python -m alems.agent start

# Monitor progress in another terminal
watch -n 5 'sqlite3 data/experiments.db \
  "SELECT sync_status, COUNT(*) FROM runs GROUP BY sync_status;"'
```

**If sync keeps failing (HTTP 500):**
```bash
# On Oracle VM — check server logs
sudo journalctl -u alems-api -n 30 --no-pager

# Common fixes:
# 1. baseline_id FK violation — already fixed (all baselines sent every batch)
# 2. experiment not found — flush fix already deployed
# 3. Reset failed and retry
sqlite3 data/experiments.db "UPDATE runs SET sync_status=0 WHERE sync_status=2;"
```

**Sample backload (phase 2):**
```bash
# Check how many runs need samples synced
sqlite3 data/experiments.db \
  "SELECT COUNT(*) FROM runs WHERE sync_status=1 AND sync_samples_status=0;"

# Agent handles phase 2 automatically (3 runs/batch to avoid timeout)
# Or trigger via Fleet Control → Sync tab → ⬆ Sync samples now
```

---

## Verify sync worked

```bash
# On local machine
sqlite3 data/experiments.db \
  "SELECT sync_status, COUNT(*) FROM runs GROUP BY sync_status;"
# Expected after full sync: only 1|N rows (all synced)

# On Oracle VM — check PG row counts
psql -U alems -d alems_central -h localhost -c "
  SELECT 'runs' AS tbl, COUNT(*) FROM runs
  UNION ALL SELECT 'experiments', COUNT(*) FROM experiments
  UNION ALL SELECT 'energy_samples', COUNT(*) FROM energy_samples;"

# Compare: SQLite synced count should equal PG runs count
```

---

## Debug checklist

**Agent won't connect:**
```bash
curl http://129.153.71.47:8000/health   # check server alive
cat ~/.alems/agent.conf                  # check server_url and api_key
python -m alems.agent status             # check registered: True
```

**Sync fails with HTTP 500:**
```bash
# On Oracle VM
sudo journalctl -u alems-api -n 50 --no-pager
# Error will say exactly which table/constraint failed
```

**Runs stuck at sync_status=2:**
```bash
sqlite3 data/experiments.db "UPDATE runs SET sync_status=0 WHERE sync_status=2;"
# Then restart agent or click "Reset failed → retry" in Fleet Control
```

**Baseline FK violation (resolved — doc for reference):**
Was caused by sync client only sending baselines referenced by current batch.
Fixed: all idle_baselines sent every batch (small table, idempotent upsert).

**global_exp_id NOT NULL violation (resolved):**
Was caused by SELECT for global_exp_id running before session.flush().
Fixed: flush after experiments insert + pre-built exp_id map.

---

## Key files

| File | Role |
|------|------|
| `alems/agent/sync_client.py` | Phase 1+2 sync, payload builder, mark_synced |
| `alems/agent/agent.py` | Sync thread (60s loop), heartbeat thread |
| `alems/agent/mode_manager.py` | Reads ~/.alems/agent.conf |
| `alems/server/main.py` | bulk-sync endpoint, flush+map fix |
| `alems/migrations/007_distributed_identity.sql` | sync_status, sync_samples_status columns |
| `gui/pages/fleet.py` | GUI sync controls (Sync & Connect tab) |

---

## Normal healthy state

```
sqlite sync_status:
  0 (unsynced):  0        ← all synced
  1 (synced):    1554     ← all runs
  2 (failed):    0        ← nothing stuck

PG runs count = SQLite sync_status=1 count ✓
Agent status: mode=connected, registered=True, server alive=True
```
