"""
streamlit_server.py
────────────────────────────────────────────────────────────────────────────
A-LEMS Server Dashboard — PostgreSQL / global view.

Run ONLY on Oracle VM (or any machine with ALEMS_DB_URL set):
    streamlit run streamlit_server.py --server.port 8502

On local boxes this exits immediately with a clear message.
────────────────────────────────────────────────────────────────────────────
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# ── Guard: refuse to run if no PostgreSQL URL configured ─────────────────────
_DB_URL = os.environ.get("ALEMS_DB_URL", "")
if not _DB_URL.startswith("postgresql"):
    import streamlit as st
    st.set_page_config(page_title="A-LEMS Server", page_icon="⚡", layout="wide")
    st.error(
        "## ⊘  Server dashboard requires PostgreSQL\n\n"
        "This is the **server-mode** Streamlit app. It only runs on the Oracle VM "
        "where `ALEMS_DB_URL` is set.\n\n"
        "**To view local data**, run:\n"
        "```\nstreamlit run streamlit_app.py\n```\n\n"
        "**To run the server dashboard on Oracle VM:**\n"
        "```\nstreamlit run streamlit_server.py --server.port 8502\n```"
    )
    st.stop()

import importlib
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="A-LEMS · Server Dashboard",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
.stMetric label { font-size:0.7rem !important; color:#3d5570 !important;
    text-transform:uppercase; letter-spacing:.07em; }
.stMetric [data-testid="stMetricValue"] { font-size:1.4rem !important;
    font-family:'IBM Plex Mono',monospace !important; }
</style>
""", unsafe_allow_html=True)

from gui.db_pg import load_overview, load_runs, load_machines

# ── Shared data ───────────────────────────────────────────────────────────────
ov   = load_overview()
runs = load_runs()

lin = runs[runs.workflow_type == "linear"]  if not runs.empty else pd.DataFrame()
age = runs[runs.workflow_type == "agentic"] if not runs.empty else pd.DataFrame()
avg_lin_j = lin.energy_j.mean() if not lin.empty and "energy_j" in lin.columns else 0.0
avg_age_j = age.energy_j.mean() if not age.empty and "energy_j" in age.columns else 0.0
tax_mult  = avg_age_j / avg_lin_j if avg_lin_j > 0 else 0.0

plan_ms  = float(ov.get("avg_planning_ms",  0) or 0)
exec_ms  = float(ov.get("avg_execution_ms", 0) or 0)
synth_ms = float(ov.get("avg_synthesis_ms", 0) or 0)
phase_total = plan_ms + exec_ms + synth_ms or 1

CTX = dict(
    ov=ov, runs=runs, lin=lin, age=age,
    avg_lin_j=avg_lin_j, avg_age_j=avg_age_j, tax_mult=tax_mult,
    plan_ms=plan_ms, exec_ms=exec_ms, synth_ms=synth_ms,
    plan_pct=plan_ms / phase_total * 100,
    exec_ms_pct=exec_ms / phase_total * 100,
    synth_pct=synth_ms / phase_total * 100,
    db_mode="server",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
from gui.sidebar import render_sidebar
from gui.config import PAGE_META, PAGE_TO_SECTION, PAGES_BLOCKED, SECTION_ACCENTS, SECTION_PAGES

render_sidebar()

nav_section = st.session_state.get("nav_section")
nav_page    = st.session_state.get("nav_page")
nav_last    = st.session_state.get("nav_last", {})

# Server badge
st.sidebar.markdown(
    "<div style='margin:8px 12px;padding:6px 10px;background:#0d2b0d;"
    "border:1px solid #22c55e44;border-radius:6px;font-size:10px;"
    "color:#22c55e;font-family:IBM Plex Mono,monospace;'>🌐 SERVER MODE · PostgreSQL</div>",
    unsafe_allow_html=True,
)

# ── Page modules ──────────────────────────────────────────────────────────────
# Server pages use db_pg directly — native PG SQL, no _adapt_sql needed.
# Pages that reference gui.db will get SQLite version — list only server-safe pages.
_PAGE_MODULES = {
    "overview":              "gui.pages.overview",
    "multi_host_status":     "gui.pages.multi_host_status",
    "dispatch_queue":        "gui.pages.dispatch_queue",
    "sync_monitor":          "gui.pages.sync_monitor",
    "experiment_submissions":"gui.pages.experiment_submissions",
    "experiments":           "gui.pages.experiments",
    "explorer":              "gui.pages.explorer",
    "energy":                "gui.pages.energy",
    "sustainability":        "gui.pages.sustainability",
    "tax":                   "gui.pages.tax",
    "sql_query":             "gui.pages.sql_query",
    "sessions":              "gui.pages.sessions",
    "schema_docs":           "gui.pages.schema_docs",
}

# ── Dispatcher ────────────────────────────────────────────────────────────────
if nav_section is None:
    importlib.import_module("gui.pages.overview").render(CTX)
elif nav_page is None:
    from gui.components.section_landing import render as _landing
    _landing(nav_section, last_page=nav_last.get(nav_section))
else:
    if nav_section:
        st.session_state.setdefault("nav_last", {})[nav_section] = nav_page
    if nav_page in PAGES_BLOCKED:
        st.warning(PAGES_BLOCKED[nav_page])
    elif nav_page not in _PAGE_MODULES:
        st.info(f"Page `{nav_page}` not available in server mode yet.")
    else:
        importlib.import_module(_PAGE_MODULES[nav_page]).render(CTX)
