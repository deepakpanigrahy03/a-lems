# Setup Guide

## Oracle VM (server)

### Prerequisites
- Oracle VM running Ubuntu 24.04 aarch64
- A-LEMS repo cloned to `~/mydrive/a-lems`
- Existing venv at `~/mydrive/a-lems/venv`
- Port 8000 opened in Oracle Cloud Console security list

### One-command install

```bash
# Set a strong password first
export ALEMS_DB_PASSWORD="your_strong_password_here"

cd ~/mydrive/a-lems
chmod +x setup_oracle_vm.sh
./setup_oracle_vm.sh
```

### What it installs
1. PostgreSQL 16 — creates `alems` user and `alems_central` database
2. Python dependencies — `fastapi uvicorn psycopg2-binary sqlalchemy httpx pydantic`
3. PostgreSQL schema — applies `001_postgres_initial.sql`
4. systemd service — `alems-api.service` on port 8000, auto-starts on boot
5. Firewall — opens port 8000 via iptables

### Oracle Cloud Console — open port 8000

1. Oracle Cloud Console → Networking → Virtual Cloud Networks
2. Click your VCN → Subnets → your subnet → Security List
3. Add Ingress Rule:
   - Source CIDR: `0.0.0.0/0`
   - IP Protocol: TCP
   - Destination Port: `8000`

### Verify

```bash
# Check service
sudo systemctl status alems-api

# Check health endpoint
curl http://129.153.71.47:8000/health
# Expected: {"status":"ok","mode":"server","version":"1.0.0","connected_agents":0}

# Check logs
sudo journalctl -u alems-api -f
```

### Environment variables

```bash
# Required for FastAPI (set in systemd service)
ALEMS_DB_URL=postgresql://alems:password@localhost/alems_central

# Streamlit server mode (add to ~/.bashrc or systemd service)
ALEMS_DB_URL=postgresql://alems:password@localhost/alems_central
```

---

## Local machine (researcher)

### Prerequisites
- A-LEMS repo cloned (same version as server)
- Existing venv with A-LEMS dependencies
- Network access to Oracle VM port 8000

### Install

```bash
cd ~/mydrive/a-lems
chmod +x setup_local_agent.sh
./setup_local_agent.sh
```

### Manual steps (if script fails)

```bash
# 1. Install agent deps
source venv/bin/activate
pip install httpx pydantic tomli sqlalchemy

# 2. Apply SQLite migration
python -m alems.migrations.run_migrations

# 3. Create config
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

### Start the agent

```bash
cd ~/mydrive/a-lems
source venv/bin/activate

# Foreground (development)
python -m alems.agent start

# Background (daily use)
nohup python -m alems.agent start > ~/.alems/agent.log 2>&1 &
echo $! > ~/.alems/agent.pid

# Check status
python -m alems.agent status

# Stop background agent
kill $(cat ~/.alems/agent.pid)
```

### Switch modes

```bash
# Go offline (local only)
python -m alems.agent set-mode local

# Reconnect to server (triggers catch-up sync)
python -m alems.agent set-mode connected
```

### First run sequence

On first start, the agent will:
1. Check UUID backfill (assigns `global_run_id` to all existing runs — fast, idempotent)
2. Register with the server (`POST /register`) — saves `api_key` to `~/.alems/agent.conf`
3. Start heartbeat, poll, and sync threads

If the server is unreachable, the agent starts in degraded mode (local behaviour) and retries registration automatically.

---

## End-to-end verification

```bash
# SQLite tests only (no server needed)
python -m alems.tests.test_e2e --sqlite-only

# Full stack (server must be running)
python -m alems.tests.test_e2e --server http://129.153.71.47:8000

# With PostgreSQL verification
ALEMS_DB_URL=postgresql://alems:pass@localhost/alems_central \
    python -m alems.tests.test_e2e --server http://129.153.71.47:8000
```

Expected output (all passing):
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

[Server tests — http://129.153.71.47:8000]
  ✓  T03 Server /health reachable
  ✓  T04 Agent registration returns api_key
  ✓  T05 Heartbeat accepted
  ✓  T06 Job enqueue and fetch
  ✓  T07 Bulk sync inserts rows
  ✓  T08 Re-sync is idempotent (no duplicates)
  ✓  T09 Streamlit mode detection

==================================================
Results: 14 passed · 0 failed · 0 skipped

All tests passed ✓
```
