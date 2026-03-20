"""
gui/config.py
─────────────────────────────────────────────────────────────────────────────
Central configuration — DB path, API URL, Plotly theme, colours.
All thresholds and insight constants live in config/insights_rules.yaml.
All UI behaviour lives in config/dashboard.yaml.
Never hardcode thresholds or sustainability factors here.
─────────────────────────────────────────────────────────────────────────────
"""
from pathlib import Path
import yaml

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH      = PROJECT_ROOT / "data" / "experiments.db"
LIVE_API     = "http://localhost:8765"
CONFIG_DIR   = PROJECT_ROOT / "config"

# ── Human-readable energy comparisons (for sustainability pages) ─────────────
# Energy in Joules - used by helpers.py to make energy numbers relatable
# These are reference values for contextualizing energy consumption
_PHONE_CHARGE_J = 20000      # 20,000 J ≈ charging a typical smartphone
_WHATSAPP_MSG_J = 0.014      # 0.014 J ≈ energy to send a WhatsApp message
_GOOGLE_SEARCH_J = 0.3       # 0.3 J ≈ one Google search
_BABY_FEED_ML = 200          # 200 ml ≈ water to feed a baby (for water metrics)

# Additional sustainability reference values (optional)
_CO2_TREE_SEQ_KG_PER_YEAR = 22  # kg CO2 sequestered by one tree per year
_WATER_BOTTLE_ML = 500          # Standard water bottle size in ml

# ── YAML config loader ────────────────────────────────────────────────────────
def _load_yaml(filename: str) -> dict:
    """Load a YAML config file. Returns {} on missing/parse error."""
    path = CONFIG_DIR / filename
    try:
        with open(path) as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"[config] Warning: could not load {filename}: {e}")
        return {}

# Loaded once at import — all pages import from here
INSIGHTS_RULES = _load_yaml("insights_rules.yaml")
DASHBOARD_CFG  = _load_yaml("dashboard.yaml")
DESIGNER_CFG   = _load_yaml("experiment_designer.yaml")
TEMPLATES_CFG  = _load_yaml("experiment_templates.yaml")
GAP_RULES      = _load_yaml("gap_detection.yaml")

# ── Plotly dark theme ─────────────────────────────────────────────────────────
PL = dict(
    paper_bgcolor="#0f1520", plot_bgcolor="#090d13",
    font=dict(family="IBM Plex Mono, monospace", size=10, color="#7090b0"),
    margin=dict(l=40, r=20, t=30, b=30),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=9)),
    colorway=["#22c55e", "#ef4444", "#3b82f6", "#f59e0b", "#38bdf8", "#a78bfa"],
    xaxis=dict(gridcolor="#1e2d45", linecolor="#1e2d45", tickfont=dict(size=9)),
    yaxis=dict(gridcolor="#1e2d45", linecolor="#1e2d45", tickfont=dict(size=9)),
)

# Workflow colour map (used across all pages)
WF_COLORS = {"linear": "#22c55e", "agentic": "#ef4444"}

# Status colours for session tree (live execution view)
STATUS_COLORS = {
    "completed":   "#3b82f6",   # blue  — done
    "running":     "#22c55e",   # green — active
    "pending":     "#f59e0b",   # yellow — queued
    "not_started": "#4b5563",   # gray  — waiting
    "failed":      "#ef4444",   # red   — error
}

STATUS_ICONS = {
    "completed":   "●",
    "running":     "🟢",
    "pending":     "🟡",
    "not_started": "○",
    "failed":      "🔴",
}

# ── Sidebar Navigation ────────────────────────────────────────────────────────
# (label, page_id) — page_id=None → section header (not clickable)
# Section headers: plain text, no emoji (clean professional look)
# Menu items: emoji prefix for visual scanning
NAV_GROUPS = [
    ("◈  Overview",                  "overview"),

    ("EXPERIMENT CONTROL",           None),
    ("▶  Execute Run",               "execute"),
    ("🧪  Experiment Designer",       "designer"),
    ("≡  Experiments",               "experiments"),
    ("⚙  Settings",                  "settings"),

    ("EXPLORATION",                  None),
    ("⊞  Run Explorer",              "explorer"),
    ("⬡  Sessions",                  "sessions"),

    ("ENERGY & COMPUTE",             None),
    ("⚡  Energy",                   "energy"),
    ("◉  Domains",                   "domains"),
    ("♻  Sustainability",            "sustainability"),

    ("ORCHESTRATION",                None),
    ("▲  Tax Attribution",           "tax"),
    ("⇌  Agentic vs Linear",         "agentic_linear"),
    ("◑  Query Analysis",            "query_analysis"),

    ("SYSTEM BEHAVIOR",              None),
    ("▣  CPU & C-States",            "cpu"),
    ("〜  Scheduler",                 "scheduler"),
    ("⚠  Anomalies",                 "anomalies"),

    ("RESEARCH",                     None),
    ("🔬  Research Insights",         "research_insights"),

    ("ADVANCED",                     None),
    ("📼  Run Replay",                "live"),
    ("📋  Schema & Docs",             "schema_docs"),
    ("🔍  SQL Query",                 "sql_query"),
]
