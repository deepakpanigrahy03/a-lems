#!/bin/bash
# =============================================================================
# A-LEMS Local Agent Setup
# Run on: any researcher's local machine (Linux or macOS)
# =============================================================================
# Usage:
#   chmod +x setup_local_agent.sh
#   ./setup_local_agent.sh
#   ./setup_local_agent.sh --server http://129.153.71.47:8000
#   ./setup_local_agent.sh --local-only    # no server connection
# =============================================================================

set -euo pipefail

ALEMS_DIR="${ALEMS_DIR:-$HOME/mydrive/a-lems}"
VENV="$ALEMS_DIR/venv"
SERVER_URL="http://129.153.71.47:8000"
MODE="connected"

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --server) SERVER_URL="$2"; shift 2 ;;
        --local-only) MODE="local"; shift ;;
        --dir) ALEMS_DIR="$2"; VENV="$ALEMS_DIR/venv"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

echo "============================================================"
echo " A-LEMS Local Agent Setup"
echo " Dir:    $ALEMS_DIR"
echo " Mode:   $MODE"
echo " Server: $SERVER_URL"
echo "============================================================"

# ── Guard ─────────────────────────────────────────────────────────────────────
if [ ! -f "$ALEMS_DIR/streamlit_app.py" ]; then
    echo "ERROR: Not an A-LEMS directory: $ALEMS_DIR"
    echo "Set ALEMS_DIR or use --dir /path/to/a-lems"
    exit 1
fi

if [ ! -d "$VENV" ]; then
    echo "ERROR: venv not found at $VENV"
    echo "Create it first with the existing A-LEMS setup"
    exit 1
fi

source "$VENV/bin/activate"

# ── 1. Install agent dependencies ─────────────────────────────────────────────
echo ""
echo "[1/4] Installing agent dependencies..."
pip install -q httpx pydantic tomli sqlalchemy

# Check graphviz for diagram rendering
if ! command -v dot &>/dev/null; then
    echo "  NOTE: graphviz not found (optional for diagram rendering)"
    echo "  Install: brew install graphviz (macOS) or apt install graphviz"
fi

# ── 2. Run SQLite migration ───────────────────────────────────────────────────
echo ""
echo "[2/4] Applying SQLite migration 007..."
cd "$ALEMS_DIR"

DB_PATH="$ALEMS_DIR/data/experiments.db"
if [ ! -f "$DB_PATH" ]; then
    echo "  WARNING: $DB_PATH not found — migration will run when DB is created"
else
    python -m alems.migrations.run_migrations --db "$DB_PATH"
fi

# ── 3. Create agent config ────────────────────────────────────────────────────
echo ""
echo "[3/4] Creating agent config..."

CONF_DIR="$HOME/.alems"
CONF_FILE="$CONF_DIR/agent.conf"
mkdir -p "$CONF_DIR"

if [ -f "$CONF_FILE" ]; then
    echo "  Config already exists at $CONF_FILE — skipping"
else
    cat > "$CONF_FILE" << EOF
[agent]
mode = "$MODE"
server_url = "$SERVER_URL"
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
    echo "  Config created at $CONF_FILE"
fi

# ── 4. Test server connectivity ───────────────────────────────────────────────
echo ""
echo "[4/4] Testing server connectivity..."

if [ "$MODE" = "local" ]; then
    echo "  Local-only mode — skipping server test"
else
    if curl -s --max-time 5 "$SERVER_URL/health" | grep -q '"status":"ok"'; then
        echo "  ✓ Server reachable at $SERVER_URL"
    else
        echo "  ✗ Server not reachable at $SERVER_URL"
        echo "  Agent will retry automatically — continuing anyway"
    fi
fi

echo ""
echo "============================================================"
echo " ✓ Local agent setup complete!"
echo ""
echo "  Start agent (foreground):"
echo "    cd $ALEMS_DIR"
echo "    source venv/bin/activate"
echo "    python -m alems.agent start"
echo ""
echo "  Start agent (background):"
echo "    nohup python -m alems.agent start > ~/.alems/agent.log 2>&1 &"
echo "    echo \$! > ~/.alems/agent.pid"
echo ""
echo "  Check status:"
echo "    python -m alems.agent status"
echo ""
echo "  Switch modes:"
echo "    python -m alems.agent set-mode local"
echo "    python -m alems.agent set-mode connected"
echo "============================================================"
