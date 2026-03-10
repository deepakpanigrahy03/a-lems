"""
A-LEMS Energy Measurement Dashboard
=====================================
Entry point — intentionally thin (~80 lines).

All page logic lives in gui/pages/<name>.py
Shared config     → gui/config.py
DB helpers        → gui/db.py
Visual helpers    → gui/helpers.py
Sidebar           → gui/sidebar.py

Run:
    pip install streamlit plotly pandas requests pyyaml
    streamlit run streamlit_app.py
"""

import sys
from pathlib import Path

import streamlit as st

# ── Ensure gui/ is importable ─────────────────────────────────────────────────
_ROOT = Path(__file__).parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# ── Page config (must be first Streamlit call) ────────────────────────────────
st.set_page_config(
    page_title="A-LEMS · Energy Measurement",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background: #090d13; }
[data-testid="stSidebar"]          { background: #0f1520; border-right: 1px solid #1e2d45; }
[data-testid="stHeader"]           { background: transparent; }
.block-container { padding-top:1.2rem; padding-bottom:2rem; max-width:1600px; }
h1 { font-size:1.15rem !important; color:#e8f0f8 !important; }
h2 { font-size:1rem   !important; color:#b8c8d8 !important; }
h3 { font-size:0.9rem !important; color:#7090b0 !important; }
p, li { font-size:0.82rem; color:#b8c8d8; }
.stMetric label {
    font-size:0.7rem !important; color:#3d5570 !important;
    text-transform:uppercase; letter-spacing:.07em; }
.stMetric [data-testid="stMetricValue"] {
    font-size:1.4rem !important;
    font-family:'IBM Plex Mono',monospace !important; }
.stDataFrame { font-size:0.78rem; }
code { font-size:0.75rem; }
</style>
""", unsafe_allow_html=True)

# ── Core imports (sidebar + data) ─────────────────────────────────────────────
from gui.sidebar import render_sidebar
from gui.db      import (
    load_overview, load_runs, load_tax
)
import pandas as pd

# ── Load shared data ───────────────────────────────────────────────────────────
ov   = load_overview()
runs = load_runs()
tax  = load_tax()

lin = runs[runs.workflow_type == "linear"]  if not runs.empty else pd.DataFrame()
age = runs[runs.workflow_type == "agentic"] if not runs.empty else pd.DataFrame()

avg_lin_j = lin.energy_j.mean() if not lin.empty and "energy_j" in lin.columns else 0.0
avg_age_j = age.energy_j.mean() if not age.empty and "energy_j" in age.columns else 0.0
tax_mult  = avg_age_j / avg_lin_j if avg_lin_j > 0 else 0.0

plan_ms     = float(ov.get("avg_planning_ms",  0) or 0)
exec_ms     = float(ov.get("avg_execution_ms", 0) or 0)
synth_ms    = float(ov.get("avg_synthesis_ms", 0) or 0)
phase_total = plan_ms + exec_ms + synth_ms or 1
plan_pct    = plan_ms  / phase_total * 100
exec_pct    = exec_ms  / phase_total * 100
synth_pct   = synth_ms / phase_total * 100

# Shared context dict — passed into every page render() call
CTX = dict(
    ov=ov, runs=runs, tax=tax, lin=lin, age=age,
    avg_lin_j=avg_lin_j, avg_age_j=avg_age_j, tax_mult=tax_mult,
    plan_ms=plan_ms, exec_ms=exec_ms, synth_ms=synth_ms,
    plan_pct=plan_pct, exec_pct=exec_pct, synth_pct=synth_pct,
)

# ── Sidebar → active page_id ───────────────────────────────────────────────────
page_id = render_sidebar()

# ── Page dispatcher ────────────────────────────────────────────────────────────
_PAGES = {
    "overview":          "gui.pages.overview",
    "execute":           "gui.pages.execute",
    "experiments":       "gui.pages.experiments",
    "settings":          "gui.pages.settings",
    "explorer":          "gui.pages.explorer",
    "energy":            "gui.pages.energy",
    "domains":           "gui.pages.domains",
    "sustainability":    "gui.pages.sustainability",
    "tax":               "gui.pages.tax",
    "agentic_linear":    "gui.pages.agentic_linear",
    "query_analysis":    "gui.pages.query_analysis",
    "cpu":               "gui.pages.cpu",
    "scheduler":         "gui.pages.scheduler",
    "anomalies":         "gui.pages.anomalies",
    "research_insights": "gui.pages.research_insights",
    "live":              "gui.pages.live",
    "schema_docs":       "gui.pages.schema_docs",
    # Legacy — preserved but not in nav
    "sql_query":         "gui.pages.sql_query",
}

if page_id in _PAGES:
    import importlib
    _mod = importlib.import_module(_PAGES[page_id])
    _mod.render(CTX)
else:
    st.error(f"Unknown page: `{page_id}`")
    st.info("Available: " + ", ".join(_PAGES.keys()))
