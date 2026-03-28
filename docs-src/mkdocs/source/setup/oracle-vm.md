# Oracle VM Setup Guide

## Overview

The Oracle VM hosts three services:
- **Streamlit UI** — port 8501 (existing)
- **FastAPI orchestration API** — port 8000 (new, managed by systemd)
- **PostgreSQL** — port 5432 (local only, not exposed externally)

---

## Prerequisites

- Oracle VM: Ubuntu 24.04 aarch64
- A-LEMS repo cloned to `~/mydrive/a-lems`
- Existing venv at `~/mydrive/a-lems/venv`
- SSH access to VM: `ssh dpani@129.153.71.47`

---

## Step 1 — Pull latest code

```bash
ssh dpani@129.153.71.47
cd ~/mydrive/a-lems
git pull
source venv/bin/activate
```

---

## Step 2 — Run setup script

```bash
export ALEMS_DB_PASSWORD="your_password_no_special_chars"
chmod +x setup_oracle_vm.sh
./setup_oracle_vm.sh
```

**Password rule:** Do NOT use `@ # $ % & +` in password — these break
the PostgreSQL connection URL. Use alphanumeric + underscore only.
Example: `Alems_Lab_2026`

Expected output (all 7 steps):
```
[1/7] Installing system packages... PostgreSQL 16 installed
[2/7] Setting up PostgreSQL database... alems_central ready
[3/7] Installing Python dependencies... done
[4/7] Applying PostgreSQL schema... 15 tables created
[5/7] Creating systemd service... alems-api: RUNNING
[6/7] Opening firewall port 8000...
[7/7] Verification... {"status":"ok","mode":"server"...}
```

---

## Step 3 — Switch to .env file (recommended)

The setup script uses inline `Environment=` in the systemd service.
Replace with `EnvironmentFile=` for better security:

```bash
# Create .env file (never commit to git)
cat > ~/mydrive/a-lems/.env << 'EOF'
ALEMS_DB_URL=postgresql://alems:your_password@localhost/alems_central
PYTHONPATH=/home/dpani/mydrive/a-lems
EOF
chmod 600 ~/mydrive/a-lems/.env

# Add to .gitignore
echo ".env" >> ~/mydrive/a-lems/.gitignore
echo ".env.*" >> ~/mydrive/a-lems/.gitignore
```

Update the systemd service to use EnvironmentFile:

```bash
sudo tee /etc/systemd/system/alems-api.service > /dev/null << 'EOF'
[Unit]
Description=A-LEMS Orchestration API
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=dpani
WorkingDirectory=/home/dpani/mydrive/a-lems
EnvironmentFile=/home/dpani/mydrive/a-lems/.env
ExecStart=/home/dpani/mydrive/a-lems/venv/bin/uvicorn alems.server.main:app --host 0.0.0.0 --port 8000 --workers 2
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=alems-api

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl restart alems-api
sleep 3
curl http://localhost:8000/health
```

---

## Step 4 — Open port 8000 in Oracle Cloud Console

The VM firewall (iptables) and the systemd service are ready, but
Oracle Cloud has its own network security layer that must also be opened.

### Oracle Cloud Console steps:

1. Go to [cloud.oracle.com](https://cloud.oracle.com)
2. Navigate: **Networking → Virtual Cloud Networks → your VCN**
3. Click: **Subnets → your subnet → Security Lists → Default Security List**
4. Click: **Add Ingress Rules**
5. Fill in:
   - Source CIDR: `0.0.0.0/0`
   - IP Protocol: `TCP`
   - Destination Port Range: `8000`
6. Click: **Add Ingress Rules**

### Also ensure iptables allows it on the VM:

```bash
sudo iptables -I INPUT -p tcp --dport 8000 -j ACCEPT

# Make persistent across reboots
sudo apt install -y iptables-persistent
sudo netfilter-persistent save
```

### Verify from outside the VM:

```bash
# From your local machine
curl http://129.153.71.47:8000/health
# Expected: {"status":"ok","mode":"server","version":"1.0.0","connected_agents":0}
```

---

## Step 5 — Systemd service management

```bash
# Check status
sudo systemctl status alems-api

# View live logs
sudo journalctl -u alems-api -f

# View last 30 lines
sudo journalctl -u alems-api --no-pager -n 30

# Restart after code change
cd ~/mydrive/a-lems && git pull
sudo systemctl restart alems-api

# Stop / start
sudo systemctl stop alems-api
sudo systemctl start alems-api

# Disable autostart
sudo systemctl disable alems-api

# Re-enable autostart
sudo systemctl enable alems-api
```

---

## Step 6 — Verify full stack from local machine

```bash
# On local machine
cd ~/mydrive/a-lems
source venv/bin/activate
python -m alems.tests.test_e2e --server http://129.153.71.47:8000
```

Expected: 14 passed · 0 failed · 0 skipped

---

## Updating FastAPI after code changes

```bash
# On Oracle VM
cd ~/mydrive/a-lems
git pull
sudo systemctl restart alems-api
sleep 3
curl http://localhost:8000/health
```

No need to re-run setup script. Just pull + restart.

---

## PostgreSQL management

```bash
# Connect to database
psql -U alems -d alems_central -h localhost

# List tables
\dt

# Check row counts
SELECT schemaname, tablename,
       n_live_tup as rows
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;

# Check connected machines
SELECT hw_id, hostname, agent_status, last_seen FROM hardware_config;

# Check synced runs
SELECT hw_id, COUNT(*) as runs FROM runs GROUP BY hw_id;
```

---

## Services overview on Oracle VM

| Service | Port | Managed by | Auto-starts |
|---------|------|-----------|-------------|
| PostgreSQL | 5432 (local) | systemd postgresql.service | Yes |
| FastAPI API | 8000 | systemd alems-api.service | Yes |
| Streamlit UI | 8501 | manual / existing setup | Check existing |

All three must be running for full functionality.

Check all at once:
```bash
sudo systemctl is-active postgresql alems-api
curl http://localhost:8000/health
curl http://localhost:8501  # Streamlit
```
