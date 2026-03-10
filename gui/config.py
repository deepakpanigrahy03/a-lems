"""
gui/config.py
Central configuration — DB path, API URL, Plotly theme, colours, human-insight constants.
Import from here in every other module.
"""
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
# PROJECT_ROOT is the a-lems/ directory (parent of gui/)
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH      = PROJECT_ROOT / "data" / "experiments.db"
LIVE_API     = "http://localhost:8765"

# ── Human-insight scaling factors ─────────────────────────────────────────────
_PHONE_CHARGE_J  = 36_000   # ~10 Wh to fully charge a phone
_WHATSAPP_MSG_J  = 0.003    # ~3 mJ per WhatsApp message
_GOOGLE_SEARCH_J = 1.0      # ~1 J per Google search
_BABY_FEED_ML    = 150.0    # ml per baby feed

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

# ── Sidebar navigation groups ──────────────────────────────────────────────────
# (None pid = section header, rendered as a label, not a button)
NAV_GROUPS = [
    # ── Overview first ──────────────────────────────────────────────────────
    ("◈  Overview",           "overview"),

    # ── Experiment Control ──────────────────────────────────────────────────
    ("EXPERIMENT CONTROL",    None),
    ("▶  Execute Run",        "execute"),
    ("≡  Experiments",        "experiments"),
    ("⚙  Settings",           "settings"),

    # ── Exploration ─────────────────────────────────────────────────────────
    ("EXPLORATION",           None),
    ("⊞  Run Explorer",       "explorer"),

    # ── Energy & Compute ────────────────────────────────────────────────────
    ("ENERGY & COMPUTE",      None),
    ("⚡  Energy",             "energy"),
    ("◉  Domains",            "domains"),
    ("♻  Sustainability",     "sustainability"),

    # ── Orchestration ───────────────────────────────────────────────────────
    ("ORCHESTRATION",         None),
    ("▲  Tax Attribution",    "tax"),
    ("⇌  Agentic vs Linear",  "agentic_linear"),
    ("◑  Query Analysis",     "query_analysis"),

    # ── System Behavior ─────────────────────────────────────────────────────
    ("SYSTEM BEHAVIOR",       None),
    ("▣  CPU & C-States",     "cpu"),
    ("〜  Scheduler",          "scheduler"),
    ("⚠  Anomalies",          "anomalies"),

    # ── Research Insights ───────────────────────────────────────────────────
    ("RESEARCH",              None),
    ("🔬  Research Insights",  "research_insights"),

    # ── Advanced ────────────────────────────────────────────────────────────
    ("ADVANCED",              None),
    ("📼  Run Replay",         "live"),
    ("📋  Schema & Docs",      "schema_docs"),
]
