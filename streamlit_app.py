"""
A-LEMS Energy Measurement Dashboard
=====================================
Entry point — 3-layer navigation dispatcher.

  nav_section=None              → Overview (default)
  nav_section set, nav_page=None → Section landing card grid
  nav_section set, nav_page set  → Actual page
"""
import sys
import importlib
from pathlib import Path

import streamlit as st
import pandas as pd

_ROOT = Path(__file__).parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

st.set_page_config(
    page_title="A-LEMS · Energy Measurement",
    page_icon="⚡",
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

from gui.sidebar import render_sidebar
from gui.db      import load_overview, load_runs, load_tax
from gui.config  import PAGES_BLOCKED, PAGE_TO_SECTION, SECTION_PAGES, PAGE_META, SECTION_ACCENTS

# ── Shared data ───────────────────────────────────────────────────────────────
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

CTX = dict(
    ov=ov, runs=runs, tax=tax, lin=lin, age=age,
    avg_lin_j=avg_lin_j, avg_age_j=avg_age_j, tax_mult=tax_mult,
    plan_ms=plan_ms, exec_ms=exec_ms, synth_ms=synth_ms,
    plan_pct=plan_ms / phase_total * 100,
    exec_pct=exec_ms / phase_total * 100,
    synth_pct=synth_ms / phase_total * 100,
)

# ── Page modules ──────────────────────────────────────────────────────────────
_PAGE_MODULES = {
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
    "sql_query":         "gui.pages.sql_query",
    "designer":          "gui.pages.designer",
    "sessions":          "gui.pages.sessions",
    "session_analysis":  "gui.pages.session_analysis",
    "models":            "gui.pages.models",
    # Data Quality
    # Energy & Silicon
    "thermal":           "gui.pages.thermal",
    "baseline":          "gui.pages.baseline",
    # Agentic Intelligence
    "phase_drilldown":   "gui.pages.phase_drilldown",
    # Sessions & Runs
    "run_drilldown":     "gui.pages.run_drilldown",
    # Research & Insights
    "efficiency":        "gui.pages.efficiency",
    "ml_features":       "gui.pages.ml_features_page",
    "hypotheses":        "gui.pages.hypotheses",
    # Environment
    "carbon_country":    "gui.pages.carbon_country",
    "water_methane":     "gui.pages.water_methane",
    # Developer Tools
    "env_config":        "gui.pages.env_config",
    "llm_log":           "gui.pages.llm_log",
    "ml_export":         "gui.pages.ml_export",
    # Silicon Lab
    "hw_registry":       "gui.pages.hw_registry",
    "silicon_compare":   "gui.pages.silicon_compare",
    "silicon_journey":   "gui.pages.silicon_journey",
    # Data Movement
    "data_cache":        "gui.pages.data_cache",
    "data_tokens":       "gui.pages.data_tokens",
    "data_network":      "gui.pages.data_network",
    "data_swap":         "gui.pages.data_swap",
    "data_interrupts":   "gui.pages.data_interrupts",
    # Data Quality
    "dq_validity":       "gui.pages.dq_validity",
    "dq_coverage":       "gui.pages.dq_coverage",
    "dq_sufficiency":    "gui.pages.dq_sufficiency",
    "dq_integrity":      "gui.pages.dq_integrity",
    "dq_schema":         "gui.pages.dq_schema",
}

# ── Sidebar ───────────────────────────────────────────────────────────────────
render_sidebar()

nav_section = st.session_state.get("nav_section")
nav_page    = st.session_state.get("nav_page")
nav_last    = st.session_state.get("nav_last", {})


# ── Helpers ───────────────────────────────────────────────────────────────────
def _render_stub(page_id: str, section: str) -> None:
    meta   = PAGE_META.get(page_id, {})
    label  = meta.get("label", page_id)
    icon   = meta.get("icon",  "◈")
    desc   = meta.get("desc",  "")
    accent = SECTION_ACCENTS.get(section, "#3b82f6")
    st.markdown(
        f"<div style='padding:48px 32px;text-align:center;"
        f"border:1px solid {accent}33;border-radius:14px;"
        f"background:linear-gradient(135deg,{accent}08,transparent);margin-top:8px;'>"
        f"<div style='font-size:40px;margin-bottom:12px;'>{icon}</div>"
        f"<div style='font-size:20px;font-weight:700;color:#e8f0f8;"
        f"font-family:IBM Plex Mono,monospace;margin-bottom:6px;'>{label}</div>"
        f"<div style='font-size:12px;color:#64748b;"
        f"font-family:IBM Plex Mono,monospace;margin-bottom:16px;'>{desc}</div>"
        f"<div style='display:inline-block;font-size:10px;padding:4px 14px;"
        f"border-radius:4px;background:{accent}22;color:{accent};"
        f"font-family:IBM Plex Mono,monospace;font-weight:700;"
        f"letter-spacing:.1em;'>COMING SOON</div>"
        f"</div>",
        unsafe_allow_html=True)


# ── 3-layer dispatcher ────────────────────────────────────────────────────────

# Layer 1 — No section → Overview
if nav_section is None:
    importlib.import_module("gui.pages.overview").render(CTX)

# Layer 2 — Section, no page → Section landing
elif nav_page is None:
    from gui.components.section_landing import render as _landing
    _landing(nav_section, last_page=nav_last.get(nav_section))

# Layer 3 — Section + page → Page
else:
    # Breadcrumb (not on overview)
    if nav_page != "overview":
        from gui.components.breadcrumb import render as _bc
        _bc(nav_page)

    # Update last-visited memory
    if nav_section:
        st.session_state.setdefault("nav_last", {})[nav_section] = nav_page

    # Blocked
    if nav_page in PAGES_BLOCKED:
        reason = PAGES_BLOCKED[nav_page]
        st.markdown(
            f"<div style='padding:32px 28px;border:1px solid #2a0c0c;"
            f"border-left:4px solid #ef4444;border-radius:12px;"
            f"background:#0d0505;margin-top:8px;'>"
            f"<div style='font-size:14px;font-weight:700;color:#f87171;"
            f"font-family:IBM Plex Mono,monospace;margin-bottom:8px;'>"
            f"⊘  Data gap — page blocked</div>"
            f"<div style='font-size:12px;color:#fca5a5;"
            f"font-family:IBM Plex Mono,monospace;line-height:1.7;'>{reason}</div>"
            f"</div>",
            unsafe_allow_html=True)

    # Stub
    elif nav_page not in _PAGE_MODULES:
        _render_stub(nav_page, nav_section or "")

    # Real page
    else:
        importlib.import_module(_PAGE_MODULES[nav_page]).render(CTX)
