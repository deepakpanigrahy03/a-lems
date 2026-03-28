# Client Registration & Onboarding

## How registration works

Registration is automatic — no admin approval required for lab machines.

```
Step 1: git clone or git pull
        git clone https://github.com/deepakpanigrahy03/a-lems.git
        cd a-lems

Step 2: Install agent dependencies
        source venv/bin/activate
        pip install httpx pydantic tomli sqlalchemy

Step 3: Run SQLite migration
        python -m alems.migrations.run_migrations

Step 4: Start agent (triggers auto-registration)
        python -m alems.agent start --mode connected
```

On first start the agent:
1. Reads `hardware_config` from local SQLite (CPU, RAM, architecture)
2. `POST /register {hardware_hash, hostname, cpu_model, ...}` — no auth required
3. Server checks `hardware_hash` in `hardware_config` table
   - New machine → INSERT row, generate `api_key`, return `{api_key, server_hw_id}`
   - Known machine → UPDATE `last_seen`, return existing credentials
4. Agent saves `api_key` and `server_hw_id` to `~/.alems/agent.conf`
5. All subsequent calls use `Authorization: Bearer {api_key}`

**The `hardware_hash`** is a fingerprint of CPU model, cores, architecture.
Same physical machine always gets the same `hardware_hash` → same server `hw_id`.

---

## Re-registration (after server wipe or api_key lost)

```bash
# Clear stored credentials
sed -i 's/api_key = .*/api_key = ""/' ~/.alems/agent.conf

# Restart agent — will re-register automatically
python -m alems.agent start --mode connected
```

---

## What the server sees when a client connects

```sql
-- PostgreSQL hardware_config after registration
SELECT hw_id, hostname, cpu_model, agent_status, last_seen
FROM hardware_config;

 hw_id |   hostname   |        cpu_model         | agent_status |       last_seen
-------+--------------+--------------------------+--------------+------------------------
     1 | UBUNTU2505   | 11th Gen Intel i7-1165G7 | idle         | 2026-03-28 20:30:00
     2 | lab-server-1 | AMD Ryzen 9 5950X        | running      | 2026-03-28 20:30:05
     3 | macbook-pro  | Apple M2                 | offline      | 2026-03-28 19:45:00
```

---

## GUI mode behaviour

### Oracle VM Streamlit (mode=server)

Reads PostgreSQL directly. Shows:
- All registered machines (connected + offline)
- Global counts: total runs, experiments, active jobs
- Live metrics per machine from `run_status_cache` (updated every 5s by heartbeat)
- Auto-refreshes every 5s

### Local Streamlit (mode=connected)

Reads local SQLite + fetches from server API. Shows:
- Own machine card highlighted
- All other machines from server
- Own sync status (how many runs pending)
- Auto-refreshes every 5s

### Local Streamlit (mode=local)

Reads local SQLite only. Shows:
- Own machine only
- Local run counts
- Pending sync count
- Prompt to connect to server

---

## Live status vs historical sync

These are TWO SEPARATE paths — do not confuse them:

```
LIVE STATUS (real-time, during run):
  test_harness running
      ↓
  Agent reads SQLite every 5s
      ↓
  POST /heartbeat {live_metrics: energy_uj, power_w, tokens, steps}
      ↓
  Server updates run_status_cache
      ↓
  Streamlit reads run_status_cache → shows live speedometer

HISTORICAL SYNC (after run completes):
  test_harness finishes → SQLite has complete run data
      ↓
  Agent sync thread: SELECT WHERE sync_status=0
      ↓
  POST /bulk-sync {runs[], energy_samples[], cpu_samples[], ...}
      ↓
  PostgreSQL runs/experiments/samples tables
      ↓
  Available for reports and analysis
```

Live status never goes through bulk sync.
Bulk sync never blocks live status.

---

## Agent configuration reference

`~/.alems/agent.conf`:

```toml
[agent]
mode = "connected"              # "local" | "connected"
server_url = "http://129.153.71.47:8000"
api_key = ""                    # filled automatically on registration
hw_id_local = 1                 # local SQLite hw_id
hw_id_server = 0                # server PostgreSQL hw_id (filled on registration)

[sync]
interval_seconds = 60           # background sync interval
batch_size = 5                  # runs per bulk-sync call
retry_max = 3                   # retries on failure
retry_backoff_s = 30            # wait between retries
timeout_seconds = 300           # bulk-sync HTTP timeout (large payloads)
http_timeout_seconds = 10       # heartbeat/poll HTTP timeout

[execution]
poll_interval_s = 10            # job poll frequency
heartbeat_s = 30                # heartbeat when idle
heartbeat_run_s = 5             # heartbeat when run is active (live metrics)
```

**Oracle VM agent.conf** — points to localhost:
```toml
[agent]
mode = "connected"
server_url = "http://localhost:8000"   ← localhost, not public IP
```

**Local machine agent.conf** — points to Oracle VM:
```toml
[agent]
mode = "connected"
server_url = "http://129.153.71.47:8000"
```

---

## Agent systemd service (Oracle VM)

The Oracle VM runs the agent as a systemd service so it starts automatically:

```bash
sudo systemctl status alems-agent    # check status
sudo systemctl start alems-agent     # start
sudo systemctl stop alems-agent      # stop
sudo journalctl -u alems-agent -f    # live logs
```

Local machines run agent manually or add their own systemd service using the
same pattern as `alems-agent.service` on the Oracle VM.

---

## Troubleshooting registration

| Symptom | Cause | Fix |
|---------|-------|-----|
| `Illegal header value b'Bearer '` | api_key empty, used in auth header | Clear api_key in agent.conf, restart |
| `HTTP 401 Unauthorized` | Stale api_key after server wipe | Clear api_key in agent.conf, restart |
| `Registration error: name 'TIMEOUT' is not defined` | Old TIMEOUT constant removed | Update heartbeat.py to use `_get_http_timeout()` |
| `Registration failed — running without server` | Server unreachable | Check `curl http://129.153.71.47:8000/health` |
| Agent shows `local` mode | mode_manager reads agent.conf | Run `python -m alems.agent set-mode connected` |
