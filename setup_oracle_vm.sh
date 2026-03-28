#!/bin/bash
# =============================================================================
# A-LEMS Oracle VM Setup Script
# Run on: Oracle VM (129.153.71.47) as ubuntu user
# OS: Ubuntu 24.04 aarch64
# =============================================================================
# Usage:
#   chmod +x setup_oracle_vm.sh
#   ./setup_oracle_vm.sh
#
# What this does:
#   1. Installs PostgreSQL 16
#   2. Creates alems database + user
#   3. Installs Python dependencies into existing venv
#   4. Creates systemd service for FastAPI (:8000)
#   5. Opens port 8000 via iptables (Oracle Cloud security list done separately)
#   6. Applies PostgreSQL schema
#   7. Verifies everything is running
# =============================================================================

set -euo pipefail

ALEMS_DIR="$HOME/mydrive/a-lems"
VENV="$ALEMS_DIR/venv"
DB_NAME="alems_central"
DB_USER="alems"
DB_PASS="${ALEMS_DB_PASSWORD:-changeme_set_env_first}"
API_PORT=8000

echo "============================================================"
echo " A-LEMS Oracle VM Setup"
echo " Dir:  $ALEMS_DIR"
echo " Port: $API_PORT"
echo "============================================================"

# ── Guard: must be in correct directory ──────────────────────────────────────
if [ ! -f "$ALEMS_DIR/streamlit_app.py" ]; then
    echo "ERROR: $ALEMS_DIR/streamlit_app.py not found"
    echo "Clone the repo first or check ALEMS_DIR path"
    exit 1
fi

if [ -z "$ALEMS_DB_PASSWORD" ]; then
    echo "WARNING: ALEMS_DB_PASSWORD not set — using 'changeme_set_env_first'"
    echo "Set it: export ALEMS_DB_PASSWORD=your_strong_password"
    echo "Press Ctrl+C to abort, or Enter to continue with default..."
    read -r
fi

# ── 1. System packages ────────────────────────────────────────────────────────
echo ""
echo "[1/7] Installing system packages..."
sudo apt-get update -qq
sudo apt-get install -y -qq \
    postgresql postgresql-contrib \
    python3-dev libpq-dev \
    graphviz \
    curl

echo "  PostgreSQL version: $(psql --version)"

# ── 2. PostgreSQL: create user + database ─────────────────────────────────────
echo ""
echo "[2/7] Setting up PostgreSQL database..."

# Start PostgreSQL if not running
sudo systemctl enable postgresql
sudo systemctl start postgresql

# Create user and database (idempotent)
sudo -u postgres psql -c "
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$DB_USER') THEN
            CREATE USER $DB_USER WITH PASSWORD '$DB_PASS';
        ELSE
            ALTER USER $DB_USER WITH PASSWORD '$DB_PASS';
        END IF;
    END \$\$;
" 2>/dev/null || true

sudo -u postgres psql -c "
    SELECT 'CREATE DATABASE $DB_NAME OWNER $DB_USER'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$DB_NAME')
" --tuples-only | sudo -u postgres psql 2>/dev/null || true

sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;" 2>/dev/null || true

# Allow local connections
PG_HBA=$(sudo -u postgres psql -t -c "SHOW hba_file;" | tr -d ' ')
if ! sudo grep -q "^local.*$DB_USER" "$PG_HBA" 2>/dev/null; then
    echo "  Updating pg_hba.conf for local connections..."
    sudo -u postgres psql -c "ALTER USER $DB_USER WITH SUPERUSER;" 2>/dev/null || true
fi

echo "  Database '$DB_NAME' ready"

# ── 3. Python dependencies ────────────────────────────────────────────────────
echo ""
echo "[3/7] Installing Python dependencies..."
source "$VENV/bin/activate"

pip install -q --upgrade pip
pip install -q \
    fastapi \
    "uvicorn[standard]" \
    psycopg2-binary \
    sqlalchemy \
    httpx \
    pydantic \
    tomli

echo "  Python deps installed"

# ── 4. Apply PostgreSQL schema ────────────────────────────────────────────────
echo ""
echo "[4/7] Applying PostgreSQL schema..."

export ALEMS_DB_URL="postgresql://$DB_USER:$DB_PASS@localhost/$DB_NAME"

# Apply SQL directly
PGPASSWORD="$DB_PASS" psql \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -h localhost \
    -f "$ALEMS_DIR/alems/migrations/001_postgres_initial.sql" \
    --quiet

echo "  Schema applied"

# Verify tables
TABLE_COUNT=$(PGPASSWORD="$DB_PASS" psql \
    -U "$DB_USER" -d "$DB_NAME" -h localhost \
    -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';" | tr -d ' ')
echo "  Tables created: $TABLE_COUNT"

# ── 5. systemd service for FastAPI ────────────────────────────────────────────
echo ""
echo "[5/7] Creating systemd service for FastAPI..."

sudo tee /etc/systemd/system/alems-api.service > /dev/null << EOF
[Unit]
Description=A-LEMS Orchestration API
Documentation=https://github.com/your-org/a-lems
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=$USER
WorkingDirectory=$ALEMS_DIR
Environment="ALEMS_DB_URL=postgresql://$DB_USER:$DB_PASS@localhost/$DB_NAME"
Environment="PYTHONPATH=$ALEMS_DIR"
ExecStart=$VENV/bin/uvicorn alems.server.main:app --host 0.0.0.0 --port $API_PORT --workers 2
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=alems-api

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable alems-api
sudo systemctl start alems-api

sleep 3

if sudo systemctl is-active --quiet alems-api; then
    echo "  alems-api service: RUNNING"
else
    echo "  ERROR: alems-api failed to start"
    sudo journalctl -u alems-api --no-pager -n 20
    exit 1
fi

# ── 6. Open port 8000 ─────────────────────────────────────────────────────────
echo ""
echo "[6/7] Opening firewall port $API_PORT..."

# iptables (immediate)
sudo iptables -I INPUT -p tcp --dport $API_PORT -j ACCEPT 2>/dev/null || true

# Persist across reboots
if command -v netfilter-persistent &>/dev/null; then
    sudo netfilter-persistent save 2>/dev/null || true
elif command -v ufw &>/dev/null; then
    sudo ufw allow $API_PORT/tcp 2>/dev/null || true
fi

echo "  NOTE: Also open port $API_PORT in Oracle Cloud Console:"
echo "  VCN → Subnets → Security List → Add Ingress Rule"
echo "  Source CIDR: 0.0.0.0/0  Protocol: TCP  Port: $API_PORT"

# ── 7. Verify everything ──────────────────────────────────────────────────────
echo ""
echo "[7/7] Verification..."

# Health check
sleep 2
HEALTH=$(curl -s --max-time 5 "http://localhost:$API_PORT/health" || echo "FAILED")
echo "  Health endpoint: $HEALTH"

if echo "$HEALTH" | grep -q '"status":"ok"'; then
    echo ""
    echo "============================================================"
    echo " ✓ Setup complete!"
    echo ""
    echo "  API:        http://129.153.71.47:$API_PORT"
    echo "  Health:     http://129.153.71.47:$API_PORT/health"
    echo "  DB URL:     postgresql://$DB_USER:****@localhost/$DB_NAME"
    echo ""
    echo "  Next steps:"
    echo "  1. Open port $API_PORT in Oracle Cloud Console security list"
    echo "  2. On local machines: python -m alems.agent start --mode connected"
    echo "  3. Run SQLite migration: python -m alems.migrations.run_migrations"
    echo "============================================================"
else
    echo ""
    echo "ERROR: Health check failed. Check logs:"
    echo "  sudo journalctl -u alems-api -f"
    exit 1
fi
