"""
A-LEMS Streamlit Dashboard  
All 10 pages. Reads directly from SQLite — no server.py dependency.

Run:
    pip install streamlit plotly pandas
    streamlit run streamlit_app.py

Config: set DB_PATH and PROJECT_ROOT below.
"""

import sqlite3
import subprocess
import time
import shlex
from pathlib import Path
from contextlib import contextmanager

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import json as _json
try:
    import requests as _req
    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False
    class _req:
        @staticmethod
        def get(*a, **kw): raise RuntimeError("requests not installed")
try:
    import yaml as _yaml
    _YAML_OK = True
except ImportError:
    _YAML_OK = False

# ── CONFIG ─────────────────────────────────────────────────────────────────────
DB_PATH      = Path(__file__).parent / "data" / "experiments.db"
PROJECT_ROOT = Path(__file__).parent          # where manage.py / core/ lives

# ── LIVE API + HUMAN INSIGHT HELPERS ─────────────────────────────────────────
LIVE_API = "http://localhost:8765"

_PHONE_CHARGE_J  = 36_000   # ~10Wh to fully charge a phone
_WHATSAPP_MSG_J  = 0.003    # ~3mJ per message
_GOOGLE_SEARCH_J = 1.0      # ~1J per Google search
_BABY_FEED_ML    = 150.0    # ml per feed

def _human_energy(joules: float):
    if joules <= 0:
        return []
    ins = []
    phone_pct = joules / _PHONE_CHARGE_J * 100
    ins.append(("📱", f"{phone_pct:.5f}% of a full phone charge"))
    led_ms = joules / 10 * 1000
    ins.append(("💡", f"{led_ms:.1f}ms of a 10W LED bulb" if led_ms < 1000 else f"{led_ms/1000:.2f}s of a 10W LED"))
    msgs = joules / _WHATSAPP_MSG_J
    ins.append(("💬", f"≈{msgs:.0f} WhatsApp messages"))
    searches = joules / _GOOGLE_SEARCH_J
    ins.append(("🔍", f"≈{searches:.3f} Google searches"))
    return ins

def _human_water(ml: float) -> str:
    if not ml or ml <= 0: return "—"
    if ml < 1:   return f"{ml*1000:.1f}µl (raindrop≈50µl)"
    if ml < 150: return f"{ml:.2f}ml ({ml/_BABY_FEED_ML*100:.1f}% of one baby feed)"
    return f"{ml:.1f}ml ({ml/_BABY_FEED_ML:.1f}× baby feeds)"

def _human_carbon(mg: float) -> str:
    if not mg or mg <= 0: return "—"
    car_mm = mg / 1000 / 120 * 1e6
    return f"{mg:.3f}mg CO₂e ≈ {car_mm:.2f}mm of car driving"

# ── PAGE CONFIG ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="A-LEMS · Energy Measurement",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ────────────────────────────────────────────────────────────────────────
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
.stMetric [data-testid="stMetricValue"] {
    font-size:1.4rem !important; font-family:'IBM Plex Mono',monospace !important; }
.stDataFrame { font-size:0.78rem; }
code { font-size: 0.75rem; }
</style>
""", unsafe_allow_html=True)

# ── DB ─────────────────────────────────────────────────────────────────────────
@contextmanager
def db():
    con = sqlite3.connect(str(DB_PATH), timeout=15)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    try:
        yield con
    finally:
        con.close()

@st.cache_data(ttl=30, show_spinner=False)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    with db() as con:
        try:
            return pd.read_sql_query(sql, con, params=params)
        except Exception as _e:
            # Bubble up so caller can show a meaningful error
            raise _e

def q_safe(sql: str, params: tuple = ()) -> tuple:
    """Returns (DataFrame, error_string). Use in UI pages so errors are shown."""
    with db() as con:
        try:
            return pd.read_sql_query(sql, con, params=params), None
        except Exception as _e:
            return pd.DataFrame(), str(_e)

@st.cache_data(ttl=30, show_spinner=False)
def q1(sql: str, params: tuple = ()) -> dict:
    with db() as con:
        try:
            row = con.execute(sql, params).fetchone()
            return dict(row) if row else {}
        except Exception:
            return {}

# ── PLOTLY THEME ───────────────────────────────────────────────────────────────
PL = dict(
    paper_bgcolor="#0f1520", plot_bgcolor="#090d13",
    font=dict(family="IBM Plex Mono, monospace", size=10, color="#7090b0"),
    margin=dict(l=40, r=20, t=30, b=30),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=9)),
    colorway=["#22c55e","#ef4444","#3b82f6","#f59e0b","#38bdf8","#a78bfa"],
    xaxis=dict(gridcolor="#1e2d45", linecolor="#1e2d45", tickfont=dict(size=9)),
    yaxis=dict(gridcolor="#1e2d45", linecolor="#1e2d45", tickfont=dict(size=9)),
)

def fl(fig, **kw):
    fig.update_layout(**PL, **kw)
    return fig

WF_COLORS = {"linear": "#22c55e", "agentic": "#ef4444"}

# ── SIDEBAR ────────────────────────────────────────────────────────────────────
PAGES = [
    ("◈  Overview",        "overview"),
    ("⚡  Energy",          "energy"),
    ("▣  CPU & C-States",  "cpu"),
    ("⇌  Scheduler",       "scheduler"),
    ("◉  Domains",         "domains"),
    ("▲  Tax Attribution", "tax"),
    ("⚠  Anomalies",       "anomalies"),
    ("▶  Execute Run",     "execute"),
    ("⊞  Sample Explorer", "explorer"),
    ("≡  Experiments",     "experiments"),
    ("◑  Query Analysis",  "query_analysis"),
    ("🔴  Live Monitor",   "live"),
    ("💬  SQL Query",      "sql_query"),
    ("⚙  Settings",        "settings"),
]

with st.sidebar:
    st.markdown("### ⚡ A-LEMS")
    st.markdown(
        "<div style='font-size:9px;color:#3d5570;text-transform:uppercase;"
        "letter-spacing:.08em;margin-bottom:4px;'>Energy Measurement</div>",
        unsafe_allow_html=True,
    )
    selected = st.radio("Navigation", [p[0] for p in PAGES], label_visibility="collapsed")
    page_id  = dict(PAGES)[selected]
    st.divider()
    try:
        _n = q1("SELECT COUNT(*) AS n FROM runs").get("n", "—")
        st.caption(f"**Runs:** {_n}")
        st.caption(f"**DB:** {DB_PATH.name}")
        _db_ok = True
    except Exception:
        st.caption("⚠ DB not connected")
        _db_ok = False
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()

# ── CORE DATA ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=30, show_spinner=False)
def load_overview():
    return q1("""
        SELECT
            COUNT(DISTINCT e.exp_id) AS total_experiments,
            COUNT(r.run_id)          AS total_runs,
            SUM(CASE WHEN r.workflow_type='linear'  THEN 1 ELSE 0 END) AS linear_runs,
            SUM(CASE WHEN r.workflow_type='agentic' THEN 1 ELSE 0 END) AS agentic_runs,
            AVG(CASE WHEN r.workflow_type='linear'  THEN r.total_energy_uj END)/1e6 AS avg_linear_j,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.total_energy_uj END)/1e6 AS avg_agentic_j,
            MAX(r.total_energy_uj)/1e6 AS max_energy_j,
            MIN(r.total_energy_uj)/1e6 AS min_energy_j,
            SUM(r.total_energy_uj)/1e6 AS total_energy_j,
            AVG(r.ipc) AS avg_ipc, MAX(r.ipc) AS max_ipc,
            AVG(r.cache_miss_rate)*100 AS avg_cache_miss_pct,
            SUM(r.carbon_g)*1000 AS total_carbon_mg,
            AVG(r.carbon_g)*1000 AS avg_carbon_mg,
            AVG(r.water_ml) AS avg_water_ml,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.planning_time_ms  END) AS avg_planning_ms,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.execution_time_ms END) AS avg_execution_ms,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.synthesis_time_ms END) AS avg_synthesis_ms
        FROM experiments e LEFT JOIN runs r ON e.exp_id = r.exp_id
    """)

@st.cache_data(ttl=30, show_spinner=False)
def load_runs():
    # Explicit column list avoids workflow_type_x/y collision from SELECT *
    return q("""
        SELECT
            r.run_id, r.exp_id, r.workflow_type, r.run_number,
            r.duration_ns/1e6               AS duration_ms,
            r.total_energy_uj/1e6           AS energy_j,
            r.dynamic_energy_uj/1e6         AS dynamic_energy_j,
            r.ipc, r.cache_miss_rate, r.thread_migrations,
            r.context_switches_voluntary, r.context_switches_involuntary,
            r.total_context_switches, r.frequency_mhz,
            r.package_temp_celsius, r.thermal_delta_c, r.thermal_throttle_flag,
            r.interrupt_rate, r.api_latency_ms,
            r.planning_time_ms, r.execution_time_ms, r.synthesis_time_ms,
            r.llm_calls, r.tool_calls, r.total_tokens,
            r.complexity_level, r.complexity_score,
            r.carbon_g, r.water_ml,
            r.energy_per_token, r.energy_per_instruction,
            e.provider, e.country_code, e.model_name, e.task_name,
            r.governor, r.turbo_enabled
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        ORDER BY r.run_id DESC
    """)

@st.cache_data(ttl=30, show_spinner=False)
def load_tax():
    return q("""
        SELECT
            ots.comparison_id, ots.linear_run_id, ots.agentic_run_id,
            ots.tax_percent,
            ots.orchestration_tax_uj/1e6 AS tax_j,
            ots.linear_dynamic_uj/1e6    AS linear_dynamic_j,
            ots.agentic_dynamic_uj/1e6   AS agentic_dynamic_j,
            ra.planning_time_ms, ra.execution_time_ms, ra.synthesis_time_ms,
            ra.llm_calls, ra.tool_calls, ra.total_tokens,
            el.task_name, el.country_code, el.provider
        FROM orchestration_tax_summary ots
        JOIN runs rl ON ots.linear_run_id  = rl.run_id
        JOIN runs ra ON ots.agentic_run_id = ra.run_id
        JOIN experiments el ON rl.exp_id = el.exp_id
        ORDER BY ots.tax_percent DESC
    """)

ov   = load_overview()
runs = load_runs()
tax  = load_tax()

lin  = runs[runs.workflow_type=="linear"]  if not runs.empty and "workflow_type" in runs.columns else pd.DataFrame()
age  = runs[runs.workflow_type=="agentic"] if not runs.empty and "workflow_type" in runs.columns else pd.DataFrame()

avg_lin_j = lin.energy_j.mean()  if not lin.empty  and "energy_j" in lin.columns  else 0.0
avg_age_j = age.energy_j.mean()  if not age.empty  and "energy_j" in age.columns  else 0.0
tax_mult  = avg_age_j / avg_lin_j if avg_lin_j > 0 else 0.0

plan_ms  = float(ov.get("avg_planning_ms",  0) or 0)
exec_ms  = float(ov.get("avg_execution_ms", 0) or 0)
synth_ms = float(ov.get("avg_synthesis_ms", 0) or 0)
phase_total = plan_ms + exec_ms + synth_ms or 1
plan_pct  = plan_ms  / phase_total * 100
exec_pct  = exec_ms  / phase_total * 100
synth_pct = synth_ms / phase_total * 100


# ══════════════════════════════════════════════════════════════════════════════
# OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if page_id == "overview":
    st.title("Overview — Agentic vs Linear Energy")

    # Hero bar
    bar_pct = f"{100/max(tax_mult,1):.0f}%"
    st.markdown(f"""
    <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                padding:20px 24px;margin-bottom:16px;border-top:2px solid #ef4444;">
      <div style="font-size:18px;font-weight:600;color:#e8f0f8;margin-bottom:4px;">
        Agentic costs <span style="color:#ef4444;font-family:'IBM Plex Mono',monospace;">
        {tax_mult:.1f}×</span> more energy than linear for the same task
      </div>
      <div style="font-size:11px;color:#3d5570;margin-bottom:16px;">
        Measured across {ov.get("total_runs","—")} runs · {ov.get("total_experiments","—")} experiments
      </div>
      <div style="display:flex;align-items:center;gap:16px;margin-bottom:10px;">
        <div style="width:70px;font-size:11px;color:#7090b0;">Linear</div>
        <div style="flex:1;background:#1a2438;border-radius:4px;overflow:hidden;height:28px;">
          <div style="width:{bar_pct};background:#22c55e;height:100%;display:flex;
               align-items:center;padding-left:10px;font-size:10px;color:#fff;
               font-family:'IBM Plex Mono',monospace;">{avg_lin_j:.3f}J</div>
        </div>
        <div style="width:50px;font-size:10px;color:#7090b0;font-family:monospace;">1×</div>
      </div>
      <div style="display:flex;align-items:center;gap:16px;">
        <div style="width:70px;font-size:11px;color:#7090b0;">Agentic</div>
        <div style="flex:1;background:#1a2438;border-radius:4px;overflow:hidden;height:28px;">
          <div style="width:100%;background:#ef4444;height:100%;display:flex;
               align-items:center;padding-left:10px;font-size:10px;color:#fff;
               font-family:'IBM Plex Mono',monospace;">{avg_age_j:.3f}J</div>
        </div>
        <div style="width:50px;font-size:10px;color:#ef4444;font-family:monospace;
             font-weight:600;">{tax_mult:.1f}×</div>
      </div>
    </div>""", unsafe_allow_html=True)

    # Phase bar (only if agentic runs have phase data)
    if plan_ms > 0:
        st.markdown(f"""
        <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                    padding:16px 20px;margin-bottom:16px;">
          <div style="font-size:9px;color:#3d5570;text-transform:uppercase;
               letter-spacing:.08em;margin-bottom:8px;">
            Where the overhead goes — agentic time breakdown</div>
          <div style="display:flex;height:22px;border-radius:4px;overflow:hidden;gap:1px;">
            <div style="width:{plan_pct:.0f}%;background:#f59e0b;display:flex;align-items:center;
                 justify-content:center;font-size:9px;color:rgba(255,255,255,.85);
                 font-family:monospace;">{plan_pct:.0f}% plan</div>
            <div style="width:{exec_pct:.0f}%;background:#3b82f6;display:flex;align-items:center;
                 justify-content:center;font-size:9px;color:rgba(255,255,255,.85);
                 font-family:monospace;">{exec_pct:.0f}% exec</div>
            <div style="width:{synth_pct:.0f}%;background:#a78bfa;display:flex;align-items:center;
                 justify-content:center;font-size:9px;color:rgba(255,255,255,.85);
                 font-family:monospace;">{synth_pct:.0f}% synth</div>
          </div>
          <div style="display:flex;gap:20px;margin-top:8px;font-size:9px;color:#3d5570;">
            <span><span style="color:#f59e0b">■</span> Planning {plan_ms:.0f}ms — pure overhead</span>
            <span><span style="color:#3b82f6">■</span> Execution {exec_ms:.0f}ms — tool latency</span>
            <span><span style="color:#a78bfa">■</span> Synthesis {synth_ms:.0f}ms — context merge</span>
          </div>
        </div>""", unsafe_allow_html=True)

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("Total Runs",   ov.get("total_runs","—"))
    c2.metric("Tax Multiple", f"{tax_mult:.1f}×",
              delta=f"{(tax_mult-1)*100:.0f}% overhead", delta_color="inverse")
    c3.metric("Avg Planning", f"{plan_ms:.0f}ms",
              delta=f"{plan_pct:.0f}% of agentic time",  delta_color="inverse")
    c4.metric("Peak IPC",     f"{ov.get('max_ipc', 0) or 0:.3f}")
    c5.metric("Avg Carbon",   f"{ov.get('avg_carbon_mg', 0) or 0:.3f}mg")
    c6.metric("Total Energy", f"{ov.get('total_energy_j', 0) or 0:.1f}J")

    st.divider()

    if not runs.empty and "energy_j" in runs.columns:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Duration vs Energy**")
            _df = runs.dropna(subset=["energy_j","duration_ms"]).copy()
            _df["duration_s"] = _df["duration_ms"] / 1000
            fig = px.scatter(_df, x="duration_s", y="energy_j",
                             color="workflow_type", color_discrete_map=WF_COLORS,
                             hover_data=["run_id","provider","task_name"],
                             labels={"duration_s":"Duration (s)","energy_j":"Energy (J)"})
            st.plotly_chart(fl(fig), use_container_width=True)

        with col2:
            st.markdown("**IPC vs Cache Miss**")
            _df2 = runs.dropna(subset=["ipc","cache_miss_rate"]).copy()
            _df2["cache_miss_pct"] = _df2["cache_miss_rate"] * 100
            fig2 = px.scatter(_df2, x="cache_miss_pct", y="ipc",
                              color="workflow_type", color_discrete_map=WF_COLORS,
                              hover_data=["run_id","provider"],
                              labels={"cache_miss_pct":"Cache Miss %","ipc":"IPC"})
            st.plotly_chart(fl(fig2), use_container_width=True)
    else:
        st.info("No run data — run experiments first.")


# ══════════════════════════════════════════════════════════════════════════════
# ENERGY
# ══════════════════════════════════════════════════════════════════════════════
elif page_id == "energy":
    st.title("Energy Analysis")

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Min Energy",      f"{ov.get('min_energy_j',0) or 0:.3f}J")
    c2.metric("Max Energy",      f"{ov.get('max_energy_j',0) or 0:.3f}J")
    c3.metric("Total Measured",  f"{ov.get('total_energy_j',0) or 0:.1f}J")
    c4.metric("Avg Carbon",      f"{ov.get('avg_carbon_mg',0) or 0:.3f}mg")
    c5.metric("Avg Water",       f"{ov.get('avg_water_ml',0) or 0:.3f}ml")
    st.divider()

    if not runs.empty and "energy_j" in runs.columns:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Energy per run — sorted, log scale**")
            sr = runs.dropna(subset=["energy_j"]).sort_values("energy_j").reset_index(drop=True)
            sr["run_idx"] = sr.index
            fig = px.bar(sr, x="run_idx", y="energy_j",
                         color="workflow_type", color_discrete_map=WF_COLORS,
                         log_y=True, hover_data=["run_id","provider","task_name"],
                         labels={"energy_j":"Energy (J)","run_idx":"Run (sorted)"})
            fig.update_xaxes(showticklabels=False)
            st.plotly_chart(fl(fig), use_container_width=True)
            st.caption("Log scale — agentic runs cluster at the top.")

        with col2:
            st.markdown("**Carbon by provider · region**")
            if "carbon_g" in runs.columns:
                _cd = runs.dropna(subset=["carbon_g"]).copy()
                _cd["group"]     = _cd["provider"].fillna("?") + "·" + _cd["country_code"].fillna("?")
                _cd["carbon_mg"] = _cd["carbon_g"] * 1000
                _ca = _cd.groupby("group")["carbon_mg"].mean().reset_index()
                fig3 = px.bar(_ca, x="group", y="carbon_mg", log_y=True, color="group",
                              labels={"carbon_mg":"avg mg CO₂e","group":""})
                st.plotly_chart(fl(fig3), use_container_width=True)
                st.caption("IN grid (0.82 kg/kWh) = 2× US factor — same energy, double carbon.")

        st.divider()

        if "api_latency_ms" in runs.columns:
            _cl = runs[(runs.provider != "local") & runs.api_latency_ms.notna() & runs.energy_j.notna()].copy()
            _cl["api_latency_s"] = _cl["api_latency_ms"] / 1000
            if not _cl.empty:
                st.markdown("**Energy vs API latency** — longer wait = more idle RAPL drain")
                fig4 = px.scatter(_cl, x="api_latency_s", y="energy_j",
                                  color="country_code", log_y=True,
                                  hover_data=["run_id","workflow_type"],
                                  labels={"api_latency_s":"API Latency (s)","energy_j":"Energy (J)"})
                st.plotly_chart(fl(fig4), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# CPU & C-STATES
# ══════════════════════════════════════════════════════════════════════════════
elif page_id == "cpu":
    st.title("CPU & C-State Analysis")

    cstate_df = q("""
        SELECT e.provider, r.workflow_type,
               AVG(cs.c1_residency) AS c1, AVG(cs.c2_residency) AS c2,
               AVG(cs.c3_residency) AS c3, AVG(cs.c6_residency) AS c6,
               AVG(cs.c7_residency) AS c7,
               AVG(cs.cpu_util_percent) AS util,
               AVG(cs.package_power) AS pkg_w,
               COUNT(cs.sample_id) AS samples
        FROM cpu_samples cs
        JOIN runs r ON cs.run_id = r.run_id
        JOIN experiments e ON r.exp_id = e.exp_id
        GROUP BY e.provider, r.workflow_type
    """)

    if not cstate_df.empty:
        st.markdown("**C-State Residency** — higher C6/C7 = deeper sleep = more efficient idle")
        CSTATE_COLORS = {"C0":"#ef4444","C1":"#38bdf8","C2":"#3b82f6",
                         "C3":"#a78bfa","C6":"#22c55e","C7":"#f59e0b"}
        for _, row in cstate_df.iterrows():
            c0 = max(0.0, 100 - float(row.c1 or 0) - float(row.c2 or 0)
                              - float(row.c3 or 0) - float(row.c6 or 0) - float(row.c7 or 0))
            cs_data = pd.DataFrame([
                {"State":"C0","Residency%": c0},
                {"State":"C1","Residency%": float(row.c1 or 0)},
                {"State":"C2","Residency%": float(row.c2 or 0)},
                {"State":"C3","Residency%": float(row.c3 or 0)},
                {"State":"C6","Residency%": float(row.c6 or 0)},
                {"State":"C7","Residency%": float(row.c7 or 0)},
            ])
            st.markdown(f"**{row.provider} · {row.workflow_type}** — "
                        f"{float(row.pkg_w or 0):.2f}W · {int(row.samples):,} samples")
            fig = px.bar(cs_data, x="Residency%", y="State", orientation="h",
                         color="State", color_discrete_map=CSTATE_COLORS)
            fig.update_layout(**PL, height=160, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        st.info("Cloud: mostly C6/C7 (deep sleep between API calls). "
                "Local: forced C0 throughout inference loop.")
    else:
        st.info("No cpu_samples yet — run experiments to populate.")

    st.divider()

    if not runs.empty and "ipc" in runs.columns:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**IPC Distribution**")
            _ri = runs.dropna(subset=["ipc"])
            fig = px.histogram(_ri, x="ipc", color="workflow_type",
                               color_discrete_map=WF_COLORS,
                               nbins=20, barmode="overlay", opacity=.75,
                               labels={"ipc":"IPC"})
            st.plotly_chart(fl(fig), use_container_width=True)

        with col2:
            st.markdown("**Cache Miss vs Energy**")
            if "cache_miss_rate" in runs.columns and "energy_j" in runs.columns:
                _rm = runs.dropna(subset=["cache_miss_rate","energy_j"]).copy()
                _rm["cache_miss_pct"] = _rm["cache_miss_rate"] * 100
                fig2 = px.scatter(_rm, x="cache_miss_pct", y="energy_j",
                                  color="workflow_type", color_discrete_map=WF_COLORS,
                                  log_y=True, hover_data=["run_id","provider"],
                                  labels={"cache_miss_pct":"Cache Miss %","energy_j":"Energy J"})
                st.plotly_chart(fl(fig2), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULER
# ══════════════════════════════════════════════════════════════════════════════
elif page_id == "scheduler":
    st.title("OS Scheduler Analysis")

    if not runs.empty and "thread_migrations" in runs.columns:
        sc = runs.dropna(subset=["thread_migrations"])
        lsc = sc[sc.workflow_type=="linear"]
        asc = sc[sc.workflow_type=="agentic"]
        avg_l = lsc.thread_migrations.mean() if not lsc.empty else 0
        avg_a = asc.thread_migrations.mean() if not asc.empty else 0

        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Max Migrations", f"{int(sc.thread_migrations.max()):,}")
        c2.metric("Linear avg",     f"{avg_l:.0f}")
        c3.metric("Agentic avg",    f"{avg_a:.0f}",
                  delta=f"{avg_a/max(avg_l,1):.1f}× vs linear", delta_color="inverse")
        c4.metric("Max IRQ/s",
                  f"{sc.interrupt_rate.max():,.0f}" if "interrupt_rate" in sc.columns else "—")
        c5.metric("Avg Cache Miss", f"{ov.get('avg_cache_miss_pct',0) or 0:.1f}%")

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Thread Migrations vs Duration**")
            _sm = sc.dropna(subset=["duration_ms"]).copy()
            _sm["duration_s"] = _sm["duration_ms"] / 1000
            fig = px.scatter(_sm, x="duration_s", y="thread_migrations",
                             color="workflow_type", color_discrete_map=WF_COLORS,
                             hover_data=["run_id","provider"],
                             labels={"duration_s":"Duration (s)","thread_migrations":"Migrations"})
            st.plotly_chart(fl(fig), use_container_width=True)
            st.caption("r²≈0.89 — phase transitions in agentic runs cause migration bursts.")

        with col2:
            st.markdown("**Migrations → Cache Miss (causal chain)**")
            if "cache_miss_rate" in sc.columns:
                _sm2 = sc.dropna(subset=["cache_miss_rate"]).copy()
                _sm2["cache_miss_pct"] = _sm2["cache_miss_rate"] * 100
                fig2 = px.scatter(_sm2, x="thread_migrations", y="cache_miss_pct",
                                  color="workflow_type", color_discrete_map=WF_COLORS,
                                  hover_data=["run_id"],
                                  labels={"thread_migrations":"Migrations","cache_miss_pct":"Cache Miss %"})
                st.plotly_chart(fl(fig2), use_container_width=True)
                st.caption("Migrations → cache eviction → IPC drop → energy waste.")
    else:
        st.info("No scheduler data available.")


# ══════════════════════════════════════════════════════════════════════════════
# DOMAINS
# ══════════════════════════════════════════════════════════════════════════════
elif page_id == "domains":
    st.title("Domain Energy Breakdown")
    domains = q("SELECT * FROM orchestration_analysis ORDER BY run_id")

    if not domains.empty:
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Avg Core Share",
                  f"{domains.core_share.mean()*100:.1f}%"   if "core_share"   in domains.columns else "—")
        c2.metric("Avg Uncore Share",
                  f"{domains.uncore_share.mean()*100:.1f}%" if "uncore_share" in domains.columns else "—")
        c3.metric("Avg Workload J",
                  f"{domains.workload_energy_j.mean():.3f}J"    if "workload_energy_j"    in domains.columns else "—")
        c4.metric("Avg Tax J",
                  f"{domains.orchestration_tax_j.mean():.3f}J"  if "orchestration_tax_j"  in domains.columns else "—")
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Domain shares — stacked**")
            tp = domains.head(30)
            fig = go.Figure()
            for col, color, name in [("core_energy_j","#3b82f6","Core"),
                                      ("uncore_energy_j","#38bdf8","Uncore"),
                                      ("dram_energy_j","#a78bfa","DRAM")]:
                if col in tp.columns:
                    fig.add_trace(go.Bar(name=name, x=tp.run_id.astype(str),
                                         y=tp[col], marker_color=color))
            fig.update_layout(barmode="stack", **PL)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.markdown("**Workload vs Tax**")
            fig2 = go.Figure()
            for col, color, name in [("workload_energy_j","#22c55e","Workload"),
                                      ("orchestration_tax_j","#ef4444","Tax")]:
                if col in domains.columns:
                    fig2.add_trace(go.Bar(name=name, x=domains.run_id.astype(str),
                                          y=domains[col], marker_color=color))
            fig2.update_layout(barmode="stack", **PL)
            st.plotly_chart(fig2, use_container_width=True)
        st.markdown("**Per-run breakdown**")
        sc = [c for c in ["run_id","workflow_type","task_name","pkg_energy_j","core_energy_j",
                           "uncore_energy_j","dram_energy_j","workload_energy_j","orchestration_tax_j"]
              if c in domains.columns]
        st.dataframe(domains[sc], use_container_width=True, hide_index=True)
    else:
        st.info("No domain data — idle_baselines must be linked to runs.")


# ══════════════════════════════════════════════════════════════════════════════
# TAX ATTRIBUTION
# ══════════════════════════════════════════════════════════════════════════════
elif page_id == "tax":
    st.title("Tax Attribution")

    if not tax.empty:
        avg_tax = float(tax.tax_percent.mean()) if "tax_percent" in tax.columns else 0
        max_tax = float(tax.tax_percent.max())  if "tax_percent" in tax.columns else 0

        col1,col2,col3 = st.columns(3)
        with col1:
            st.markdown(f"""
            <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                        padding:14px 16px;border-left:3px solid #f59e0b;">
              <div style="font-size:11px;font-weight:600;color:#f59e0b;margin-bottom:8px;">
                ① Planning Phase Tax</div>
              <div style="font-size:10px;color:#7090b0;line-height:1.65;">
                Avg <strong style="color:#e8f0f8">{plan_ms:.0f}ms</strong> before any useful work.
                Memoizing plans for repeated tasks could recover &gt;40% of queries.</div>
            </div>""", unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                        padding:14px 16px;border-left:3px solid #3b82f6;">
              <div style="font-size:11px;font-weight:600;color:#3b82f6;margin-bottom:8px;">
                ② Tool API Latency Tax</div>
              <div style="font-size:10px;color:#7090b0;line-height:1.65;">
                Execution phase: <strong style="color:#e8f0f8">{exec_ms:.0f}ms</strong>.
                CPU idles during API wait but RAPL keeps charging. Async dispatch = 40–60% cut.</div>
            </div>""", unsafe_allow_html=True)
        with col3:
            st.markdown(f"""
            <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                        padding:14px 16px;border-left:3px solid #ef4444;">
              <div style="font-size:11px;font-weight:600;color:#ef4444;margin-bottom:8px;">
                ③ Measured Tax: avg {avg_tax:.1f}% · peak {max_tax:.1f}%</div>
              <div style="font-size:10px;color:#7090b0;line-height:1.65;">
                Route simple tasks linearly — removes planning + synthesis entirely.
                Classifier overhead &lt;1ms.</div>
            </div>""", unsafe_allow_html=True)

        st.divider()
        sc = [c for c in ["comparison_id","task_name","provider","country_code",
                           "linear_dynamic_j","agentic_dynamic_j","tax_j","tax_percent",
                           "planning_time_ms","execution_time_ms","synthesis_time_ms",
                           "llm_calls","tool_calls"] if c in tax.columns]
        st.dataframe(tax[sc], use_container_width=True, hide_index=True)

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Tax % distribution**")
            fig = px.histogram(tax, x="tax_percent", nbins=10,
                               color_discrete_sequence=["#3b82f6"],
                               labels={"tax_percent":"Tax %"})
            st.plotly_chart(fl(fig), use_container_width=True)
        with col2:
            if "llm_calls" in tax.columns:
                st.markdown("**Tax vs LLM calls**")
                _tx = tax.dropna(subset=["llm_calls","tax_percent"])
                fig2 = px.scatter(_tx, x="llm_calls", y="tax_percent",
                                  color_discrete_sequence=["#f59e0b"],
                                  hover_data=["task_name","provider"],
                                  labels={"llm_calls":"LLM Calls","tax_percent":"Tax %"})
                st.plotly_chart(fl(fig2), use_container_width=True)
    else:
        st.info("No tax data yet — run comparison experiments.")


# ══════════════════════════════════════════════════════════════════════════════
# ANOMALIES
# ══════════════════════════════════════════════════════════════════════════════
elif page_id == "anomalies":
    st.title("Anomaly Detection")
    anom = q("""
        WITH stats AS (
            SELECT AVG(total_energy_uj/1e6) AS me, AVG(ipc) AS mi,
                   AVG(cache_miss_rate) AS mm FROM runs WHERE total_energy_uj IS NOT NULL
        ),
        stdev AS (
            SELECT SQRT(AVG((total_energy_uj/1e6-me)*(total_energy_uj/1e6-me))) AS se
            FROM runs, stats WHERE total_energy_uj IS NOT NULL
        )
        SELECT r.run_id, r.exp_id, r.workflow_type, e.task_name, e.provider,
               r.total_energy_uj/1e6 AS energy_j, r.ipc,
               r.cache_miss_rate*100 AS cache_miss_pct,
               r.thermal_delta_c, r.interrupt_rate,
               CASE WHEN r.total_energy_uj/1e6 > me+2*se THEN 1 ELSE 0 END AS flag_high_energy,
               CASE WHEN r.ipc < mi*0.5                  THEN 1 ELSE 0 END AS flag_low_ipc,
               CASE WHEN r.cache_miss_rate > mm*1.5      THEN 1 ELSE 0 END AS flag_high_miss,
               CASE WHEN r.thermal_throttle_flag=1        THEN 1 ELSE 0 END AS flag_thermal
        FROM runs r JOIN experiments e ON r.exp_id=e.exp_id, stats, stdev
        WHERE r.total_energy_uj IS NOT NULL
          AND (r.total_energy_uj/1e6>me+2*se OR r.ipc<mi*0.5
               OR r.cache_miss_rate>mm*1.5   OR r.thermal_throttle_flag=1)
        ORDER BY energy_j DESC
    """)
    if not anom.empty:
        c1,c2,c3 = st.columns(3)
        c1.metric("High-Energy", int(anom.flag_high_energy.sum()) if "flag_high_energy" in anom.columns else "—")
        c2.metric("Low-IPC",     int(anom.flag_low_ipc.sum())     if "flag_low_ipc"     in anom.columns else "—")
        c3.metric("Thermal",     int(anom.flag_thermal.sum())     if "flag_thermal"     in anom.columns else "—")
        st.divider()
        st.dataframe(anom, use_container_width=True, hide_index=True)
    else:
        st.success("No anomalies — all runs within normal range.")


# ══════════════════════════════════════════════════════════════════════════════
# EXECUTE RUN
# ══════════════════════════════════════════════════════════════════════════════
elif page_id == "execute":
    st.title("Execute Run")
    st.caption(f"Project root: `{PROJECT_ROOT}`  ·  venv must be activated before starting Streamlit")

    # ── Available tasks (from DB + tasks.yaml + presets) ─────────────────────
    _tl = q("SELECT DISTINCT task_name FROM experiments WHERE task_name IS NOT NULL ORDER BY task_name")
    _known_db = _tl.task_name.tolist() if not _tl.empty else []

    # Also load from tasks.yaml for tasks not yet run
    _yaml_tasks = []
    try:
        if _YAML_OK:
            import yaml as _yaml_exec
            _ty = _yaml_exec.safe_load(open(PROJECT_ROOT / "config" / "tasks.yaml"))
            _yaml_tasks = [t.get("id","") for t in (_ty or {}).get("tasks",[]) if t.get("id")]
    except Exception:
        pass

    PRESET_TASKS = ["simple","capital","research_summary","code_generation",
                    "stock_lookup","comparative_research","deep_research"]
    all_tasks = list(dict.fromkeys(PRESET_TASKS + _yaml_tasks + _known_db))
    # Category map for display
    _cat_map = {}
    try:
        if _YAML_OK:
            _ty2 = _yaml_exec.safe_load(open(PROJECT_ROOT / "config" / "tasks.yaml"))
            _cat_map = {t.get("id",""):t.get("category","") for t in (_ty2 or {}).get("tasks",[])}
    except Exception:
        pass

    # ── Two modes: batch (run_experiment) vs single (test_harness) ───────────
    tab_batch, tab_single = st.tabs([
        "⚡ Batch — run_experiment (multi-task, multi-provider)",
        "🔬 Single — test_harness (one task, fine-grained)",
    ])

    # ── Gauge helpers (pure HTML/CSS — no JS needed) ──────────────────────────
    def _gauge_html(value, vmin, vmax, label, unit, color, warn=None, danger=None):
        """Render an SVG arc speedometer gauge."""
        pct   = max(0, min(1, (value - vmin) / max(vmax - vmin, 1e-9)))
        angle = -140 + pct * 280          # arc from -140° to +140°
        rad   = 3.14159265 / 180
        r     = 52
        cx, cy = 60, 62
        # arc end point
        ex = cx + r * __import__('math').sin(angle * rad)
        ey = cy - r * __import__('math').cos(angle * rad)
        large = 1 if pct > 0.5 else 0
        # Determine needle color
        if danger and value >= danger:
            nclr = "#ef4444"
        elif warn and value >= warn:
            nclr = "#f59e0b"
        else:
            nclr = color
        # Background arc
        bx = cx + r * __import__('math').sin(140 * rad)
        by = cy - r * __import__('math').cos(140 * rad)
        ex0 = cx - r * __import__('math').sin(140 * rad)
        ey0 = cy - r * __import__('math').cos(140 * rad)
        return f"""
        <div style="text-align:center;padding:4px 0;">
          <svg width="120" height="90" viewBox="0 0 120 90">
            <path d="M {bx:.1f} {by:.1f} A {r} {r} 0 1 1 {ex0:.1f} {ey0:.1f}"
                  fill="none" stroke="#1e2d45" stroke-width="8" stroke-linecap="round"/>
            <path d="M {bx:.1f} {by:.1f} A {r} {r} 0 {large} 1 {ex:.1f} {ey:.1f}"
                  fill="none" stroke="{nclr}" stroke-width="8" stroke-linecap="round"/>
            <circle cx="{cx}" cy="{cy}" r="4" fill="{nclr}"/>
            <text x="{cx}" y="{cy+4}" text-anchor="middle"
                  font-size="14" font-weight="700" fill="#e8f0f8"
                  font-family="monospace">{value:.1f}</text>
            <text x="{cx}" y="{cy+18}" text-anchor="middle"
                  font-size="7" fill="#7090b0">{unit}</text>
            <text x="{cx}" y="82" text-anchor="middle"
                  font-size="8" font-weight="600" fill="{nclr}">{label}</text>
            <text x="6"  y="72" text-anchor="middle" font-size="6" fill="#3d5570">{vmin}</text>
            <text x="114" y="72" text-anchor="middle" font-size="6" fill="#3d5570">{vmax}</text>
          </svg>
        </div>"""

    def _bar_gauge_html(value, vmax, label, unit, color):
        """Horizontal bar gauge for CPU util / IRQ."""
        pct = max(0, min(100, value / max(vmax, 1) * 100))
        return f"""
        <div style="margin:6px 0 10px;">
          <div style="display:flex;justify-content:space-between;
                      font-size:9px;color:#7090b0;margin-bottom:3px;">
            <span style="font-weight:600;color:#e8f0f8">{label}</span>
            <span style="font-family:monospace;color:{color}">{value:.0f} {unit}</span>
          </div>
          <div style="background:#1e2d45;border-radius:3px;height:8px;overflow:hidden;">
            <div style="background:{color};width:{pct:.1f}%;height:100%;
                        border-radius:3px;transition:width 0.3s;"></div>
          </div>
        </div>"""

    def _stream_and_gauge(cmd_parts, cwd, run_label=""):
        """
        Split-screen execution: terminal log (left 55%) + live gauges (right 45%).
        Polls server.py every 2s for live samples while process runs.
        Falls back gracefully if server is offline.
        """
        import math, time as _tm

        out_col, gauge_col = st.columns([11, 9])

        # ── Left: terminal ────────────────────────────────────────────────────
        with out_col:
            st.markdown(
                "<div style='font-size:10px;font-weight:600;color:#7090b0;"
                "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                "⬛ Terminal output</div>",
                unsafe_allow_html=True)
            prog_ph   = st.progress(0)
            status_ph = st.empty()
            out_ph    = st.empty()

        # ── Right: live gauges ────────────────────────────────────────────────
        with gauge_col:
            st.markdown(
                "<div style='font-size:10px;font-weight:600;color:#7090b0;"
                "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                "⚡ Live telemetry</div>",
                unsafe_allow_html=True)
            phase_ph   = st.empty()
            gauge_ph   = st.empty()
            bar_ph     = st.empty()
            insight_ph = st.empty()
            mini_ph    = st.empty()

        # ── Check server once ─────────────────────────────────────────────────
        _srv_live = False
        if _REQUESTS_OK:
            for _ep in ["/health", "/api/system/status", "/"]:
                try:
                    if _req.get(f"{LIVE_API}{_ep}", timeout=1).status_code < 500:
                        _srv_live = True
                        break
                except Exception:
                    pass

        # Initial gauge state
        _last_pw = _last_tp = _last_util = _last_irq = 0.0
        _last_core_w = _last_dram_w = 0.0
        _last_ipc = 0.0
        _phase_str = "starting"
        _last_rid = int(q1("SELECT COALESCE(MAX(run_id),0) AS n FROM runs").get("n",0))
        _energy_acc = []   # rolling pkg_w samples for human insight

        def _refresh_gauges(rid):
            nonlocal _last_pw, _last_tp, _last_util, _last_irq
            nonlocal _last_core_w, _last_dram_w, _last_ipc, _phase_str
            if not _REQUESTS_OK:
                return
            try:
                _er = _req.get(f"{LIVE_API}/api/runs/{rid}/samples/energy", timeout=2).json()
                _pw_rows = _er.get("power",[]) if isinstance(_er,dict) else []
                if _pw_rows:
                    _lp = _pw_rows[-1]
                    _last_pw     = float(_lp.get("pkg_w",    _last_pw))
                    _last_core_w = float(_lp.get("core_w",   _last_core_w))
                    _last_dram_w = float(_lp.get("dram_w",   _last_dram_w))
                    _energy_acc.append(_last_pw)
                    if len(_energy_acc) > 60: _energy_acc.pop(0)
            except Exception:
                pass
            try:
                _cr = _req.get(f"{LIVE_API}/api/runs/{rid}/samples/cpu", timeout=2).json()
                if isinstance(_cr,list) and _cr:
                    _lc = _cr[-1]
                    _last_tp   = float(_lc.get("package_temp", _last_tp))
                    _last_util = float(_lc.get("cpu_util_percent", _last_util))
                    _last_ipc  = float(_lc.get("ipc", _last_ipc))
            except Exception:
                pass
            try:
                _ir = _req.get(f"{LIVE_API}/api/runs/{rid}/samples/interrupts", timeout=2).json()
                if isinstance(_ir,list) and _ir:
                    _last_irq = float(_ir[-1].get("interrupts_per_sec", _last_irq))
            except Exception:
                pass

        def _draw_gauges():
            # Speedometer row: Pkg W · Core W · Temp °C
            _g1 = _gauge_html(_last_pw,    0, 80,  "Pkg Power",  "W",   "#3b82f6",
                               warn=50, danger=70)
            _g2 = _gauge_html(_last_core_w,0, 60,  "Core Power", "W",   "#22c55e",
                               warn=40, danger=55)
            _g3 = _gauge_html(_last_tp,    30, 105,"Package",    "°C",  "#f59e0b",
                               warn=80, danger=95)
            gauge_ph.markdown(
                f"<div style='display:flex;justify-content:space-around;'>"
                f"{_g1}{_g2}{_g3}</div>",
                unsafe_allow_html=True)

            # Bar gauges: CPU util, IRQ, IPC
            _b1 = _bar_gauge_html(_last_util, 100,  "CPU Util",  "%",    "#38bdf8")
            _b2 = _bar_gauge_html(min(_last_irq,50000), 50000,
                                              "IRQ Rate",  "/s",   "#f59e0b")
            _b3 = _bar_gauge_html(_last_ipc,  3.0,  "IPC",       "inst/cycle","#a78bfa")
            bar_ph.markdown(
                f"<div style='padding:0 8px'>{_b1}{_b2}{_b3}</div>",
                unsafe_allow_html=True)

            # Phase badge
            _pc = {"starting":"#7090b0","planning":"#f59e0b","execution":"#3b82f6",
                   "synthesis":"#a78bfa","llm_wait":"#38bdf8",
                   "complete":"#22c55e","running":"#22c55e"}.get(_phase_str,"#7090b0")
            phase_ph.markdown(
                f"<div style='font-size:10px;padding:4px 10px;background:{_pc}22;"
                f"border:1px solid {_pc};border-radius:4px;display:inline-block;"
                f"color:{_pc};margin-bottom:4px;'>"
                f"● Phase: <b>{_phase_str}</b></div>",
                unsafe_allow_html=True)

            # Human insight
            _avg_pw  = sum(_energy_acc)/len(_energy_acc) if _energy_acc else 0
            _est_j   = _avg_pw * len(_energy_acc) * 2  # ~2s per poll tick
            if _est_j > 0:
                _hi = _human_energy(_est_j)
                insight_ph.markdown(
                    "<div style='font-size:8px;color:#3d5570;margin-top:4px;'>"
                    "So far: "
                    + " · ".join(f"{ic} {d}" for ic, d in _hi[:2])
                    + "</div>", unsafe_allow_html=True)

        # ── Launch process ────────────────────────────────────────────────────
        lines = []
        try:
            proc = subprocess.Popen(
                cmd_parts, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, cwd=str(cwd), bufsize=1,
            )

            _poll_count = 0
            _gauge_every = 4   # update gauges every 4 lines (~2s at typical output rate)

            for raw in iter(proc.stdout.readline, ""):
                line = raw.rstrip()
                if not line:
                    continue
                lines.append(line)

                # Colour-code log lines
                lo = line.lower()
                _line_color = "#e8f0f8"
                if any(k in lo for k in ["error","fail","exception","traceback"]):
                    _line_color = "#ef4444"
                elif any(k in lo for k in ["complete","saved","done","✅"]):
                    _line_color = "#22c55e"
                elif any(k in lo for k in ["planning","plan"]):
                    _line_color = "#f59e0b"
                    _phase_str  = "planning"
                elif any(k in lo for k in ["execut","tool_call","tool call"]):
                    _line_color = "#3b82f6"
                    _phase_str  = "execution"
                elif any(k in lo for k in ["synth","finaliz"]):
                    _line_color = "#a78bfa"
                    _phase_str  = "synthesis"
                elif "run" in lo or "rep" in lo:
                    _phase_str  = "running"

                # Colour-code terminal output
                _colored = "".join(
                    f"<span style='color:{_line_color}'>{l}</span>\n"
                    for l in lines[-60:]
                )
                out_ph.markdown(
                    f"<div style='background:#050810;border:1px solid #1e2d45;"
                    f"border-radius:4px;padding:8px 12px;font-family:monospace;"
                    f"font-size:9px;line-height:1.5;height:340px;overflow-y:auto;'>"
                    f"{_colored}</div>",
                    unsafe_allow_html=True)

                # Progress heuristic
                for pat in ["rep ", "repetition ", "run "]:
                    if pat in lo and "/" in lo:
                        try:
                            seg = lo.split(pat)[-1].split("/")
                            d, t = int(seg[0].strip()), int(seg[1].split()[0])
                            prog_ph.progress(min(d/t, 1.0))
                            status_ph.caption(f"Rep {d}/{t}")
                        except Exception:
                            pass
                        break
                if any(k in lo for k in ["complete","saved","finished","done"]):
                    prog_ph.progress(1.0)
                    _phase_str = "complete"

                # Poll gauges periodically
                _poll_count += 1
                if _srv_live and _poll_count % _gauge_every == 0:
                    # Detect if a new run was created since we started
                    _new_rid = int(q1("SELECT COALESCE(MAX(run_id),0) AS n FROM runs").get("n",0))
                    if _new_rid > _last_rid:
                        _last_rid = _new_rid
                    _refresh_gauges(_last_rid)
                _draw_gauges()

            proc.wait()
            _phase_str = "complete" if proc.returncode == 0 else "error"
            _draw_gauges()

            # ── Final human-insight summary ───────────────────────────────────
            if _energy_acc:
                _total_j = sum(_energy_acc) * 2
                _hi_final = _human_energy(_total_j)
                mini_ph.markdown(
                    "<div style='background:#0f1520;border:1px solid #22c55e33;"
                    "border-radius:6px;padding:8px 12px;margin-top:6px;'>"
                    "<div style='font-size:9px;font-weight:600;color:#22c55e;"
                    "margin-bottom:4px;'>⚡ Run energy summary</div>"
                    + "".join(
                        f"<div style='font-size:9px;color:#b8c8d8;margin:2px 0;'>{ic} {d}</div>"
                        for ic, d in _hi_final
                    ) + "</div>", unsafe_allow_html=True)

            # ── Parse & render MASTER SUMMARY from terminal output ────────────
            # Looks for lines like:
            #   cloud   GSM8K Arithmetic   1.4600   4.1553   3.58x   [-0.82, 7.99]
            # that appear between the === MASTER SUMMARY === header and the next ===
            import re as _re
            _summary_rows = []
            _in_summary   = False
            _saved_file   = None
            for _line in lines:
                _ll = _line.strip()
                if "MASTER SUMMARY" in _ll:
                    _in_summary = True
                    continue
                if _in_summary and _ll.startswith("==="):
                    _in_summary = False
                    continue
                if _in_summary and _ll.startswith("---"):
                    continue
                if _in_summary and _ll and not _ll.startswith("Provider"):
                    # Try to parse a data row:
                    # provider  task_name  linear_j  agentic_j  tax  [ci_lo, ci_hi]
                    _m = _re.match(
                        r'^(\S+)\s+(.*?)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)x?\s*(\[.*?\])?',
                        _ll
                    )
                    if _m:
                        _prov, _task, _lin, _age, _tax, _ci = _m.groups()
                        _lin_j = float(_lin)
                        _age_j = float(_age)
                        _tax_x = float(_tax)
                        _summary_rows.append({
                            "provider":   _prov,
                            "task":       _task.strip(),
                            "linear_j":   _lin_j,
                            "agentic_j":  _age_j,
                            "tax_x":      _tax_x,
                            "ci":         _ci or "",
                        })
                # Detect saved file path
                _fm = _re.search(r'Results saved to[:\s]+(\S+\.json)', _line)
                if _fm:
                    _saved_file = _fm.group(1)

            if _summary_rows:
                # Render the master summary card below the split-screen panel
                st.markdown("---")
                st.markdown(
                    "<div style='font-size:13px;font-weight:700;color:#e8f0f8;"
                    "letter-spacing:.05em;margin-bottom:12px;'>"
                    "📊 Master Summary</div>",
                    unsafe_allow_html=True)

                # Colour scale for tax multiplier
                def _tax_color(tx):
                    if tx >= 10: return "#ef4444"
                    if tx >= 5:  return "#f59e0b"
                    if tx >= 3:  return "#38bdf8"
                    return "#22c55e"

                # Build styled HTML table
                _rows_html = ""
                for _r in _summary_rows:
                    _tc   = _tax_color(_r["tax_x"])
                    _diff = _r["agentic_j"] - _r["linear_j"]
                    _diff_str = f"+{_diff:.2f}J" if _diff > 0 else f"{_diff:.2f}J"
                    _diff_c   = "#ef4444" if _diff > 0 else "#22c55e"
                    # bar widths proportional within row
                    _max_j  = max(_r["linear_j"], _r["agentic_j"], 0.001)
                    _lw     = _r["linear_j"]  / _max_j * 100
                    _aw     = _r["agentic_j"] / _max_j * 100
                    # Human insight for agentic energy
                    _hi_row = _human_energy(_r["agentic_j"])
                    _hi_str = _hi_row[0][1] if _hi_row else ""
                    _rows_html += f"""
                    <tr style="border-bottom:1px solid #1e2d45;">
                      <td style="padding:10px 8px;font-size:10px;color:#7090b0;
                                 white-space:nowrap;">{_r['provider']}</td>
                      <td style="padding:10px 8px;font-size:10px;color:#e8f0f8;
                                 min-width:180px;">{_r['task']}</td>
                      <td style="padding:10px 8px;">
                        <div style="font-size:10px;color:#22c55e;font-family:monospace;
                                    margin-bottom:2px;">{_r['linear_j']:.4f}J</div>
                        <div style="background:#1e2d45;border-radius:2px;height:5px;width:120px;">
                          <div style="background:#22c55e;width:{_lw:.0f}%;height:100%;border-radius:2px;"></div>
                        </div>
                      </td>
                      <td style="padding:10px 8px;">
                        <div style="font-size:10px;color:#ef4444;font-family:monospace;
                                    margin-bottom:2px;">{_r['agentic_j']:.4f}J</div>
                        <div style="background:#1e2d45;border-radius:2px;height:5px;width:120px;">
                          <div style="background:#ef4444;width:{_aw:.0f}%;height:100%;border-radius:2px;"></div>
                        </div>
                      </td>
                      <td style="padding:10px 8px;text-align:center;">
                        <span style="font-size:13px;font-weight:700;color:{_tc};
                                     font-family:monospace;">{_r['tax_x']:.2f}×</span>
                      </td>
                      <td style="padding:10px 8px;font-size:9px;color:#3d5570;
                                 font-family:monospace;">{_r['ci']}</td>
                      <td style="padding:10px 8px;">
                        <span style="font-size:{_diff_c};color:{_diff_c};
                                     font-family:monospace;font-size:9px;">{_diff_str}</span>
                        <div style="font-size:8px;color:#3d5570;margin-top:2px;">{_hi_str}</div>
                      </td>
                    </tr>"""

                st.markdown(f"""
                <div style="background:#0a0e1a;border:1px solid #1e2d45;
                            border-radius:8px;overflow:hidden;margin-bottom:16px;">
                  <table style="width:100%;border-collapse:collapse;">
                    <thead>
                      <tr style="background:#0f1520;border-bottom:2px solid #1e2d45;">
                        <th style="padding:8px;font-size:9px;color:#3d5570;
                                   text-align:left;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">Provider</th>
                        <th style="padding:8px;font-size:9px;color:#3d5570;
                                   text-align:left;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">Task</th>
                        <th style="padding:8px;font-size:9px;color:#22c55e;
                                   text-align:left;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">Linear</th>
                        <th style="padding:8px;font-size:9px;color:#ef4444;
                                   text-align:left;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">Agentic</th>
                        <th style="padding:8px;font-size:9px;color:#f59e0b;
                                   text-align:center;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">Tax</th>
                        <th style="padding:8px;font-size:9px;color:#3d5570;
                                   text-align:left;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">95% CI</th>
                        <th style="padding:8px;font-size:9px;color:#3d5570;
                                   text-align:left;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">Δ / Insight</th>
                      </tr>
                    </thead>
                    <tbody>{_rows_html}</tbody>
                  </table>
                </div>""", unsafe_allow_html=True)

                # Winner/loser highlights
                if len(_summary_rows) > 1:
                    _best  = min(_summary_rows, key=lambda r: r["tax_x"])
                    _worst = max(_summary_rows, key=lambda r: r["tax_x"])
                    _hcols = st.columns(3)
                    _hcols[0].success(
                        f"**✅ Lowest overhead**\n\n"
                        f"{_best['provider']} · {_best['task'][:28]}\n\n"
                        f"**{_best['tax_x']:.2f}×** tax"
                    )
                    _hcols[1].error(
                        f"**⚠ Highest overhead**\n\n"
                        f"{_worst['provider']} · {_worst['task'][:28]}\n\n"
                        f"**{_worst['tax_x']:.2f}×** tax"
                    )
                    _avg_tax = sum(r["tax_x"] for r in _summary_rows) / len(_summary_rows)
                    _hcols[2].info(
                        f"**📈 Session average**\n\n"
                        f"{len(_summary_rows)} comparisons\n\n"
                        f"**{_avg_tax:.2f}×** mean tax"
                    )

                # Visualise summary inline
                import pandas as _pd_sum
                _sdf = _pd_sum.DataFrame(_summary_rows)
                _sdf["label"] = _sdf["provider"] + " · " + _sdf["task"].str[:20]
                _sfig = go.Figure()
                _sfig.add_trace(go.Bar(
                    name="Linear", x=_sdf["label"], y=_sdf["linear_j"],
                    marker_color="#22c55e", text=_sdf["linear_j"].round(3),
                    textposition="outside", textfont=dict(size=8)))
                _sfig.add_trace(go.Bar(
                    name="Agentic", x=_sdf["label"], y=_sdf["agentic_j"],
                    marker_color="#ef4444", text=_sdf["agentic_j"].round(3),
                    textposition="outside", textfont=dict(size=8)))
                _sfig.update_layout(**PL, barmode="group", height=280,
                    title="Linear vs Agentic energy — this session",
                    xaxis_tickangle=20)
                st.plotly_chart(_sfig, use_container_width=True)

                if _saved_file:
                    st.caption(f"💾 Results saved to `{_saved_file}`")

            return proc.returncode

        except FileNotFoundError:
            out_ph.error(
                f"Cannot find `python`. Activate the venv:\n\n"
                f"```bash\ncd {cwd}\nsource venv/bin/activate\nstreamlit run streamlit_app.py\n```"
            )
            return -1
        except Exception as ex:
            out_ph.error(f"Unexpected error: {ex}")
            return -1

    # ══ TAB 1: run_experiment ═════════════════════════════════════════════════
    with tab_batch:
        col_cfg, col_out = st.columns([1, 2])

        with col_cfg:
            st.markdown("**Tasks**")

            # ── Task selector: multiselect from DB/yaml + custom entry ────────
            _fmt_task = lambda t: f"{t}  [{_cat_map.get(t,'?')}]" if _cat_map.get(t) else t

            _b_all = st.checkbox("Run ALL tasks", value=False, key="b_all_tasks")

            if _b_all:
                _selected_tasks = all_tasks
                st.caption(f"All {len(all_tasks)} tasks selected")
                tasks_input = "all"
            else:
                _selected_tasks = st.multiselect(
                    "Select tasks",
                    options=all_tasks,
                    default=all_tasks[:2] if len(all_tasks) >= 2 else all_tasks,
                    format_func=_fmt_task,
                    key="b_task_multi",
                    help="Tasks from DB + tasks.yaml. Add custom below.",
                )

                # ── Custom task writer ─────────────────────────────────────────
                with st.expander("➕ Add a custom task", expanded=False):
                    st.caption(
                        "Define a new task inline. It will be saved to `config/tasks.yaml` "
                        "with `category: custom` and added to the run."
                    )
                    _ct_id    = st.text_input("Task ID (no spaces)", key="ct_id",
                                              placeholder="my_custom_task")
                    _ct_name  = st.text_input("Display name", key="ct_name",
                                              placeholder="My Custom Task")
                    _ct_prompt= st.text_area("Prompt", key="ct_prompt", height=80,
                                             placeholder="Explain quantum entanglement in simple terms.")
                    _ct_level = st.selectbox("Complexity level",
                                             ["easy","medium","hard"], index=1, key="ct_level")
                    _ct_tools = st.number_input("Expected tool calls (0 = no tools)",
                                                0, 20, 0, key="ct_tools")
                    _ct_save  = st.button("💾 Save task to tasks.yaml", key="ct_save")

                    if _ct_save:
                        if not _ct_id.strip() or not _ct_prompt.strip():
                            st.error("Task ID and Prompt are required.")
                        elif " " in _ct_id.strip():
                            st.error("Task ID must have no spaces.")
                        else:
                            _new_task = {
                                "id":          _ct_id.strip(),
                                "name":        _ct_name.strip() or _ct_id.strip(),
                                "category":    "custom",
                                "level":       _ct_level,
                                "tool_calls":  int(_ct_tools),
                                "prompt":      _ct_prompt.strip(),
                            }
                            _yaml_path = PROJECT_ROOT / "config" / "tasks.yaml"
                            try:
                                if _YAML_OK:
                                    import yaml as _yaml_w
                                    _existing = {}
                                    if _yaml_path.exists():
                                        _existing = _yaml_w.safe_load(_yaml_path.read_text()) or {}
                                    _tlist = _existing.get("tasks", [])
                                    # Update if exists, append if new
                                    _ids = [t.get("id") for t in _tlist]
                                    if _ct_id.strip() in _ids:
                                        _tlist[_ids.index(_ct_id.strip())] = _new_task
                                        st.success(f"Updated existing task `{_ct_id.strip()}`")
                                    else:
                                        _tlist.append(_new_task)
                                        st.success(f"Added `{_ct_id.strip()}` with category=custom")
                                    _existing["tasks"] = _tlist
                                    _yaml_path.write_text(
                                        _yaml_w.dump(_existing, allow_unicode=True, sort_keys=False)
                                    )
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.error("PyYAML not installed — run: pip install pyyaml")
                            except Exception as _ye:
                                st.error(f"Could not write tasks.yaml: {_ye}")

                tasks_input = ",".join(_selected_tasks) if _selected_tasks else "simple"

            if not _b_all and not _selected_tasks:
                st.warning("Select at least one task.")

            # Show selected task cards
            if not _b_all and _selected_tasks:
                _card_cols = st.columns(min(len(_selected_tasks), 3))
                for _ci, _tn in enumerate(_selected_tasks[:9]):
                    _cat = _cat_map.get(_tn, "?")
                    _cc  = {"reasoning":"#f59e0b","coding":"#3b82f6","qa":"#22c55e",
                             "summarization":"#38bdf8","classification":"#a78bfa",
                             "extraction":"#e879f9","custom":"#ef4444"}.get(_cat, "#7090b0")
                    _card_cols[_ci % 3].markdown(
                        f"<div style='background:#0f1520;border:1px solid #1e2d45;"
                        f"border-left:2px solid {_cc};border-radius:4px;"
                        f"padding:4px 8px;margin:2px 0;font-size:9px;'>"
                        f"<span style='color:#e8f0f8'>{_tn}</span> "
                        f"<span style='color:{_cc}'>{_cat}</span></div>",
                        unsafe_allow_html=True
                    )
                if len(_selected_tasks) > 9:
                    st.caption(f"… and {len(_selected_tasks)-9} more")
            st.markdown("**Providers**")
            b_providers = st.multiselect("Providers", ["cloud","local"], default=["cloud"],
                                         key="b_prov")
            st.markdown("**Run options**")
            b_reps     = st.number_input("Repetitions", 1, 100, 3, key="b_reps")
            b_country  = st.selectbox("Grid region",
                                      ["US","DE","FR","NO","IN","AU","GB","CN","BR"],
                                      format_func=lambda x: {
                                          "US":"🇺🇸 US","DE":"🇩🇪 DE","FR":"🇫🇷 FR",
                                          "NO":"🇳🇴 NO","IN":"🇮🇳 IN","AU":"🇦🇺 AU",
                                          "GB":"🇬🇧 GB","CN":"🇨🇳 CN","BR":"🇧🇷 BR",
                                      }.get(x,x), key="b_country")
            b_cooldown = st.number_input("Cool-down (s)", 0, 120, 5, step=5, key="b_cd")
            b_save_db  = st.checkbox("--save-db",   value=True,  key="b_savedb")
            b_opt      = st.checkbox("--optimizer", value=False, key="b_opt")
            b_warmup   = st.checkbox("--no-warmup", value=False, key="b_warmup")
            b_out      = st.text_input("--output (JSON file, optional)", value="",
                                       key="b_outfile")

            prov_arg = ",".join(b_providers) if b_providers else "cloud"
            b_cmd = [
                "python", "-m", "core.execution.tests.run_experiment",
                "--tasks",       tasks_input.strip(),
                "--providers",   prov_arg,
                "--repetitions", str(int(b_reps)),
                "--country",     b_country,
                "--cool-down",   str(int(b_cooldown)),
            ]
            if b_save_db: b_cmd.append("--save-db")
            if b_opt:     b_cmd.append("--optimizer")
            if b_warmup:  b_cmd.append("--no-warmup")
            if b_out.strip():
                b_cmd += ["--output", b_out.strip()]

            st.divider()
            st.markdown("**Command**")
            st.code(" \\\n  ".join(b_cmd), language="bash")

            b_run  = st.button("▶ Run batch", type="primary", use_container_width=True, key="b_run")
            b_list = st.button("📋 List tasks", use_container_width=True, key="b_list")

        with col_out:
            if b_list:
                with st.spinner("Querying harness…"):
                    r = subprocess.run(
                        ["python","-m","core.execution.tests.run_experiment","--list-tasks"],
                        capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=30,
                    )
                    st.code(r.stdout or r.stderr or "(no output)")
            elif b_run:
                if not b_providers:
                    st.warning("Select at least one provider.")
                else:
                    rc = _stream_and_gauge(b_cmd, PROJECT_ROOT)
                    if rc == 0:
                        st.success("✅ Batch complete — click 🔄 Refresh to see results.")
                        st.cache_data.clear()
                    elif rc != -1:
                        st.error(f"Process exited with code {rc}")
            else:
                st.markdown("**Recent runs**")
                if not runs.empty:
                    _sc = [c for c in ["run_id","workflow_type","task_name","provider",
                                       "country_code","energy_j","ipc","carbon_g"]
                           if c in runs.columns]
                    st.dataframe(runs.head(20)[_sc], use_container_width=True, hide_index=True)

    # ══ TAB 2: test_harness ═══════════════════════════════════════════════════
    with tab_single:
        col_cfg2, col_out2 = st.columns([1, 2])

        with col_cfg2:
            st.markdown("**Single-task harness**")
            h_task    = st.selectbox("Task ID", all_tasks, key="h_task")
            h_prov    = st.selectbox("Provider", ["cloud","local"], key="h_prov")
            h_reps    = st.number_input("Repetitions", 1, 100, 3, key="h_reps")
            h_country = st.selectbox("Grid region",
                                     ["US","DE","FR","NO","IN","AU","GB","CN","BR"],
                                     format_func=lambda x: {
                                         "US":"🇺🇸 US","DE":"🇩🇪 DE","FR":"🇫🇷 FR",
                                         "NO":"🇳🇴 NO","IN":"🇮🇳 IN","AU":"🇦🇺 AU",
                                         "GB":"🇬🇧 GB","CN":"🇨🇳 CN","BR":"🇧🇷 BR",
                                     }.get(x,x), key="h_country")
            h_cd      = st.number_input("Cool-down (s)", 0, 120, 5, step=5, key="h_cd")
            h_save_db = st.checkbox("--save-db",   value=True,  key="h_savedb")
            h_opt     = st.checkbox("--optimizer", value=False, key="h_opt")
            h_warmup  = st.checkbox("--no-warmup", value=False, key="h_warmup")
            h_debug   = st.checkbox("--debug",     value=False, key="h_debug")

            h_cmd = [
                "python", "-m", "core.execution.tests.test_harness",
                "--task-id",     h_task,
                "--provider",    h_prov,
                "--repetitions", str(int(h_reps)),
                "--country",     h_country,
                "--cool-down",   str(int(h_cd)),
            ]
            if h_save_db: h_cmd.append("--save-db")
            if h_opt:     h_cmd.append("--optimizer")
            if h_warmup:  h_cmd.append("--no-warmup")
            if h_debug:   h_cmd.append("--debug")

            st.divider()
            st.markdown("**Command**")
            st.code(" \\\n  ".join(h_cmd), language="bash")

            h_run  = st.button("▶ Run single", type="primary", use_container_width=True, key="h_run")
            h_list = st.button("📋 List tasks", use_container_width=True, key="h_list")

        with col_out2:
            if h_list:
                with st.spinner("Querying harness…"):
                    r = subprocess.run(
                        ["python","-m","core.execution.tests.test_harness","--list-tasks"],
                        capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=30,
                    )
                    st.code(r.stdout or r.stderr or "(no output)")

            elif h_run:
                rc = _stream_and_gauge(h_cmd, PROJECT_ROOT)
                if rc == 0:
                    st.success("✅ Run complete — click 🔄 Refresh to see results.")
                    st.cache_data.clear()
                elif rc != -1:
                    st.error(f"Process exited with code {rc}")
            else:
                st.info("Configure options on the left and click ▶ Run single.")
                st.markdown("**Quick reference**")
                st.code(
                    "# Single task, 5 reps, save to DB\n"
                    "python -m core.execution.tests.test_harness \\\n"
                    "  --task-id research_summary \\\n"
                    "  --repetitions 5 --save-db\n\n"
                    "# Batch: multiple tasks & providers\n"
                    "python -m core.execution.tests.run_experiment \\\n"
                    "  --tasks research_summary,capital \\\n"
                    "  --providers cloud,local \\\n"
                    "  --repetitions 10 --save-db --country IN",
                    language="bash",
                )


# ══════════════════════════════════════════════════════════════════════════════
# SAMPLE EXPLORER  — 100Hz RAPL · cpu_samples · interrupt_samples
# ══════════════════════════════════════════════════════════════════════════════
elif page_id == "explorer":
    st.title("Sample Explorer")
    st.caption("100Hz RAPL energy · CPU c-states · interrupt timeseries per run")

    if runs.empty:
        st.info("No runs available — run experiments first.")
    else:
        # ── Run picker ────────────────────────────────────────────────────────
        _sel = runs[runs.energy_j.notna()].copy()

        def _lbl(r):
            task = str(r.task_name or "?")[:22]
            return (f"Run {int(r.run_id):>4}  {r.workflow_type:<8}  "
                    f"{r.provider:<6}  {r.energy_j:.3f}J  {task}")

        _labels = [_lbl(r) for _, r in _sel.iterrows()]
        _ids    = _sel.run_id.tolist()

        col_pick, col_stats = st.columns([2, 1])
        with col_pick:
            chosen = st.selectbox("Select run", _labels,
                                  help="Sorted newest-first. Shows run_id, workflow, provider, energy, task.")
        rid = int(_ids[_labels.index(chosen)])

        # ── Load all sample tables for this run ───────────────────────────────
        _err_ph = st.empty()

        def load_samples(run_id: int):
            errors = []

            # energy_samples — matches exact DDL
            es_df, e1 = q_safe(f"""
                SELECT
                    (timestamp_ns - MIN(timestamp_ns) OVER (PARTITION BY run_id)) / 1e6
                        AS elapsed_ms,
                    pkg_energy_uj  / 1e6                  AS pkg_j,
                    core_energy_uj / 1e6                  AS core_j,
                    COALESCE(uncore_energy_uj, 0) / 1e6   AS uncore_j,
                    COALESCE(dram_energy_uj,   0) / 1e6   AS dram_j
                FROM energy_samples
                WHERE run_id = {run_id}
                ORDER BY timestamp_ns
            """)
            if e1: errors.append(f"energy_samples: {e1}")

            # cpu_samples — matches exact DDL (no 'ipc' column in cpu_samples — it IS there)
            cs_df, e2 = q_safe(f"""
                SELECT
                    (timestamp_ns - MIN(timestamp_ns) OVER (PARTITION BY run_id)) / 1e6
                        AS elapsed_ms,
                    COALESCE(cpu_util_percent, 0)   AS cpu_util_percent,
                    COALESCE(ipc,              0)   AS ipc,
                    COALESCE(package_power,    0)   AS pkg_w,
                    COALESCE(dram_power,       0)   AS dram_w,
                    COALESCE(c1_residency,     0)   AS c1,
                    COALESCE(c2_residency,     0)   AS c2,
                    COALESCE(c3_residency,     0)   AS c3,
                    COALESCE(c6_residency,     0)   AS c6,
                    COALESCE(c7_residency,     0)   AS c7,
                    COALESCE(pkg_c8_residency, 0)   AS c8,
                    COALESCE(package_temp,     0)   AS pkg_temp
                FROM cpu_samples
                WHERE run_id = {run_id}
                ORDER BY timestamp_ns
            """)
            if e2: errors.append(f"cpu_samples: {e2}")

            # interrupt_samples — matches exact DDL
            irq_df, e3 = q_safe(f"""
                SELECT
                    (timestamp_ns - MIN(timestamp_ns) OVER (PARTITION BY run_id)) / 1e6
                        AS elapsed_ms,
                    interrupts_per_sec
                FROM interrupt_samples
                WHERE run_id = {run_id}
                ORDER BY timestamp_ns
            """)
            if e3: errors.append(f"interrupt_samples: {e3}")

            # orchestration_events (nullable table — may be empty for linear runs)
            ev_df, e4 = q_safe(f"""
                SELECT
                    (start_time_ns - MIN(start_time_ns) OVER (PARTITION BY run_id)) / 1e6
                        AS start_ms,
                    duration_ns / 1e6  AS duration_ms,
                    phase, event_type
                FROM orchestration_events
                WHERE run_id = {run_id}
                ORDER BY start_time_ns
            """)
            if e4: errors.append(f"orchestration_events: {e4}")

            return es_df, cs_df, irq_df, ev_df, errors

        with st.spinner(f"Loading samples for run {rid}…"):
            es, cs, irq, ev, errs = load_samples(rid)

        if errs:
            for err in errs:
                st.error(f"⚠ SQL error — {err}")

        # ── KPI row ───────────────────────────────────────────────────────────
        _r = runs[runs.run_id == rid].iloc[0]
        k1,k2,k3,k4,k5 = st.columns(5)
        k1.metric("Run",              f"{rid} — {_r.workflow_type}")
        k2.metric("Total energy",     f"{_r.energy_j:.3f}J")
        k3.metric("Energy samples",   f"{len(es):,}")
        k4.metric("CPU samples",      f"{len(cs):,}")
        k5.metric("Interrupt samples",f"{len(irq):,}")

        st.divider()

        # ── Power timeseries ──────────────────────────────────────────────────
        if not es.empty and len(es) > 2:
            _es = es.copy()
            dt  = (_es.elapsed_ms.diff() / 1000).replace(0, float("nan"))  # sec, avoid /0
            _es["pkg_w"]   = (_es.pkg_j.diff()   / dt).clip(lower=0)
            _es["core_w"]  = (_es.core_j.diff()  / dt).clip(lower=0)
            _es["dram_w"]  = (_es.dram_j.diff()  / dt).clip(lower=0)
            _es = _es.iloc[1:].copy()  # drop first NaN row

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**⚡ Power over time (Watts)**")
                st.caption("Instantaneous power derived from RAPL Δenergy / Δtime at 100Hz")
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=_es.elapsed_ms, y=_es.pkg_w, name="PKG",
                    line=dict(color="#3b82f6", width=1.5),
                    fill="tozeroy", fillcolor="rgba(59,130,246,.08)"))
                fig.add_trace(go.Scatter(
                    x=_es.elapsed_ms, y=_es.core_w, name="Core",
                    line=dict(color="#22c55e", width=1)))
                fig.add_trace(go.Scatter(
                    x=_es.elapsed_ms, y=_es.dram_w, name="DRAM",
                    line=dict(color="#a78bfa", width=1)))
                st.plotly_chart(fl(fig, xaxis_title="elapsed ms", yaxis_title="Watts"),
                                use_container_width=True)

            with col2:
                st.markdown("**∫ Cumulative energy (Joules)**")
                st.caption("Raw RAPL counter values — monotonically increasing throughout run")
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(
                    x=es.elapsed_ms, y=es.pkg_j, name="PKG",
                    line=dict(color="#3b82f6", width=1.5)))
                fig2.add_trace(go.Scatter(
                    x=es.elapsed_ms, y=es.core_j, name="Core",
                    line=dict(color="#22c55e", width=1)))
                fig2.add_trace(go.Scatter(
                    x=es.elapsed_ms, y=es.dram_j, name="DRAM",
                    line=dict(color="#a78bfa", width=1)))
                st.plotly_chart(fl(fig2, xaxis_title="elapsed ms", yaxis_title="Joules"),
                                use_container_width=True)
        else:
            st.info(f"No energy_samples found for run {rid}. "
                    f"Check that the RAPL collector ran during this experiment.")

        # ── CPU + C-States ────────────────────────────────────────────────────
        if not cs.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**IPC + CPU utilisation**")
                st.caption("IPC = instructions/cycle (left axis) · util% (right axis)")
                fig3 = make_subplots(specs=[[{"secondary_y": True}]])
                fig3.add_trace(go.Scatter(
                    x=cs.elapsed_ms, y=cs.ipc, name="IPC",
                    line=dict(color="#22c55e", width=1.5)), secondary_y=False)
                fig3.add_trace(go.Scatter(
                    x=cs.elapsed_ms, y=cs.cpu_util_percent, name="CPU Util%",
                    line=dict(color="#f59e0b", width=1)), secondary_y=True)
                fig3.update_layout(**PL)
                fig3.update_yaxes(title_text="IPC",       secondary_y=False,
                                  gridcolor="#1e2d45", tickfont=dict(size=9))
                fig3.update_yaxes(title_text="CPU Util%", secondary_y=True,
                                  gridcolor="rgba(0,0,0,0)", tickfont=dict(size=9))
                st.plotly_chart(fig3, use_container_width=True)

            with col2:
                st.markdown("**C-State residency over time**")
                st.caption("Stacked: C6/C7 = deep sleep, C0 = active execution")
                fig4 = go.Figure()
                for _col, _color, _name in [
                    ("c7","#f59e0b","C7 (deepest)"),
                    ("c6","#22c55e","C6"),
                    ("c3","#38bdf8","C3"),
                    ("c2","#3b82f6","C2"),
                    ("c1","#a78bfa","C1"),
                ]:
                    fig4.add_trace(go.Scatter(
                        x=cs.elapsed_ms, y=cs[_col], name=_name,
                        line=dict(color=_color, width=1),
                        stackgroup="cstate", fill="tonexty"))
                st.plotly_chart(fl(fig4, xaxis_title="elapsed ms",
                                   yaxis_title="Residency %"),
                                use_container_width=True)

            # Package power + temp side by side
            col3, col4 = st.columns(2)
            with col3:
                st.markdown("**Package power (W) from turbostat**")
                st.caption("Direct turbostat reading — cross-check vs RAPL power above")
                fig5 = go.Figure()
                fig5.add_trace(go.Scatter(
                    x=cs.elapsed_ms, y=cs.pkg_w, name="PKG W",
                    line=dict(color="#3b82f6", width=1.5),
                    fill="tozeroy", fillcolor="rgba(59,130,246,.06)"))
                if cs.dram_w.any():
                    fig5.add_trace(go.Scatter(
                        x=cs.elapsed_ms, y=cs.dram_w, name="DRAM W",
                        line=dict(color="#a78bfa", width=1)))
                st.plotly_chart(fl(fig5, xaxis_title="elapsed ms", yaxis_title="Watts"),
                                use_container_width=True)

            with col4:
                if cs.pkg_temp.any():
                    st.markdown("**Package temperature (°C)**")
                    st.caption("Thermal headroom — sustained load shows gradual rise")
                    fig6 = go.Figure()
                    fig6.add_trace(go.Scatter(
                        x=cs.elapsed_ms, y=cs.pkg_temp, name="Temp °C",
                        line=dict(color="#ef4444", width=1.5),
                        fill="tozeroy", fillcolor="rgba(239,68,68,.06)"))
                    st.plotly_chart(fl(fig6, xaxis_title="elapsed ms",
                                       yaxis_title="°C"), use_container_width=True)
                else:
                    st.info("No temperature data for this run.")

        # ── IRQ timeseries ────────────────────────────────────────────────────
        if not irq.empty:
            st.markdown("**IRQ rate (interrupts/sec)**")
            st.caption("Spikes = API response arrivals or timer interrupts during phase transitions")
            fig7 = go.Figure()
            fig7.add_trace(go.Scatter(
                x=irq.elapsed_ms, y=irq.interrupts_per_sec, name="IRQ/s",
                line=dict(color="#ef4444", width=1.5),
                fill="tozeroy", fillcolor="rgba(239,68,68,.06)"))
            st.plotly_chart(fl(fig7, xaxis_title="elapsed ms", yaxis_title="IRQ/s"),
                            use_container_width=True)

        # ── Orchestration events Gantt ────────────────────────────────────────
        if not ev.empty:
            st.markdown("**Orchestration Event Timeline**")
            st.caption("Each bar = one agent phase. Width = duration. Hover for detail.")
            PHASE_C = {"planning":"#f59e0b","execution":"#3b82f6","synthesis":"#a78bfa"}
            fig8 = go.Figure()
            for _, row in ev.iterrows():
                fig8.add_trace(go.Bar(
                    x=[row.duration_ms],
                    y=[f"{row.phase or '?'} / {row.event_type or '?'}"],
                    base=row.start_ms, orientation="h",
                    marker_color=PHASE_C.get(str(row.phase), "#3b82f6"),
                    hovertemplate=(f"<b>{row.event_type}</b><br>"
                                   f"Phase: {row.phase}<br>"
                                   f"Duration: {row.duration_ms:.0f}ms<extra></extra>"),
                ))
            fig8.update_layout(**PL, xaxis_title="elapsed ms",
                               showlegend=False, height=max(200, len(ev) * 32))
            st.plotly_chart(fig8, use_container_width=True)
        elif _r.workflow_type == "agentic":
            st.info("No orchestration events recorded for this agentic run.")


# ══════════════════════════════════════════════════════════════════════════════
# EXPERIMENTS
# ══════════════════════════════════════════════════════════════════════════════
elif page_id == "experiments":
    st.title("Saved Experiments")

    exps = q("""
        SELECT e.exp_id, e.name, e.task_name, e.provider, e.country_code,
               e.status, e.workflow_type AS exp_workflow,
               COUNT(r.run_id) AS run_count,
               AVG(CASE WHEN r.workflow_type='linear'  THEN r.total_energy_uj END)/1e6 AS avg_linear_j,
               AVG(CASE WHEN r.workflow_type='agentic' THEN r.total_energy_uj END)/1e6 AS avg_agentic_j
        FROM experiments e LEFT JOIN runs r ON e.exp_id = r.exp_id
        GROUP BY e.exp_id ORDER BY e.exp_id DESC
    """)

    if not exps.empty:
        sc = [c for c in ["exp_id","name","task_name","provider","country_code",
                           "status","run_count","avg_linear_j","avg_agentic_j"]
              if c in exps.columns]
        st.dataframe(exps[sc], use_container_width=True, hide_index=True)
        st.divider()

        selected_exp = st.selectbox(
            "Inspect experiment",
            exps.exp_id.tolist(),
            format_func=lambda eid: f"Exp {eid} — {exps[exps.exp_id==eid]['name'].values[0]}",
        )

        exp_runs = q(f"SELECT * FROM runs WHERE exp_id={selected_exp} ORDER BY run_number")
        exp_tax  = q(f"""
            SELECT ots.comparison_id, ots.tax_percent,
                   ots.orchestration_tax_uj/1e6 AS tax_j,
                   ots.linear_dynamic_uj/1e6    AS linear_dynamic_j,
                   ots.agentic_dynamic_uj/1e6   AS agentic_dynamic_j
            FROM orchestration_tax_summary ots
            JOIN runs r ON ots.linear_run_id = r.run_id
            WHERE r.exp_id = {selected_exp}
        """)

        if not exp_runs.empty:
            lin_avg = exp_runs[exp_runs.workflow_type=="linear"].total_energy_uj.mean() / 1e6                       if not exp_runs[exp_runs.workflow_type=="linear"].empty else 0
            age_avg = exp_runs[exp_runs.workflow_type=="agentic"].total_energy_uj.mean() / 1e6                       if not exp_runs[exp_runs.workflow_type=="agentic"].empty else 0
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Total runs",   len(exp_runs))
            c2.metric("Avg Linear J", f"{lin_avg:.3f}")
            c3.metric("Avg Agentic J",f"{age_avg:.3f}")
            c4.metric("Tax multiple", f"{age_avg/lin_avg:.1f}×" if lin_avg > 0 else "—")

            sc2 = [c for c in ["run_id","workflow_type","run_number","total_energy_uj",
                                "ipc","cache_miss_rate","thread_migrations","carbon_g"]
                   if c in exp_runs.columns]
            st.dataframe(exp_runs[sc2], use_container_width=True, hide_index=True)

        if not exp_tax.empty:
            st.markdown("**Tax pairs**")
            sc3 = [c for c in ["comparison_id","linear_dynamic_j","agentic_dynamic_j",
                                "tax_j","tax_percent"] if c in exp_tax.columns]
            st.dataframe(exp_tax[sc3], use_container_width=True, hide_index=True)
    else:
        st.info("No experiments found.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: QUERY ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
elif page_id == "query_analysis":
    st.title("Query Type Analysis")
    st.caption("Energy · latency · tokens · sustainability — grouped by category and workflow")

    # ── Level 1: category × workflow summary ──────────────────────────────────
    cat_df, _e1 = q_safe("""
        SELECT
            COALESCE(tc.category, 'uncategorised')           AS category,
            r.workflow_type,
            COUNT(*)                                          AS runs,
            ROUND(AVG(r.total_energy_uj)   / 1e6, 4)        AS avg_energy_j,
            ROUND(AVG(r.dynamic_energy_uj) / 1e6, 4)        AS avg_dynamic_j,
            ROUND(AVG(r.duration_ns)       / 1e9, 3)        AS avg_duration_s,
            ROUND(AVG(r.total_tokens),            1)        AS avg_tokens,
            ROUND(AVG(CASE WHEN r.total_tokens > 0
                THEN r.total_energy_uj / r.total_tokens END) / 1e3, 4) AS avg_mj_per_token,
            ROUND(AVG(r.total_energy_uj / 1e6 /
                NULLIF(r.duration_ns / 1e9, 0)),             4) AS avg_j_per_sec,
            ROUND(AVG(r.planning_time_ms),        1)        AS avg_plan_ms,
            ROUND(AVG(r.execution_time_ms),       1)        AS avg_exec_ms,
            ROUND(AVG(r.synthesis_time_ms),       1)        AS avg_synth_ms,
            ROUND(AVG(r.carbon_g) * 1000,         4)        AS avg_carbon_mg,
            ROUND(AVG(r.water_ml),                4)        AS avg_water_ml,
            ROUND(AVG(es_agg.core_j),             4)        AS avg_core_j,
            ROUND(AVG(es_agg.uncore_j),           4)        AS avg_uncore_j,
            ROUND(AVG(es_agg.dram_j),             4)        AS avg_dram_j
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        LEFT JOIN task_categories tc ON e.task_name = tc.task_id
        LEFT JOIN (
            SELECT run_id,
                   (MAX(core_energy_uj)   - MIN(core_energy_uj))   / 1e6 AS core_j,
                   (MAX(uncore_energy_uj) - MIN(uncore_energy_uj)) / 1e6 AS uncore_j,
                   (MAX(dram_energy_uj)   - MIN(dram_energy_uj))   / 1e6 AS dram_j
            FROM energy_samples GROUP BY run_id
        ) es_agg ON r.run_id = es_agg.run_id
        GROUP BY COALESCE(tc.category,'uncategorised'), r.workflow_type
        ORDER BY category, r.workflow_type
    """)

    if _e1:
        st.error(f"Query error: {_e1}")
    elif cat_df.empty:
        st.info("No data — run experiments and ensure task_categories table is populated.")
    else:
        # KPI row
        _lin_cat = cat_df[cat_df.workflow_type == "linear"]
        _age_cat = cat_df[cat_df.workflow_type == "agentic"]
        k1,k2,k3,k4,k5 = st.columns(5)
        k1.metric("Categories",   cat_df.category.nunique())
        k2.metric("Total runs",   int(cat_df.runs.sum()))
        k3.metric("Best mJ/token",
                  f"{cat_df.avg_mj_per_token.min():.4f}" if cat_df.avg_mj_per_token.notna().any() else "—")
        k4.metric("Avg linear J", f"{_lin_cat.avg_energy_j.mean():.3f}J" if not _lin_cat.empty else "—")
        k5.metric("Avg agentic J",f"{_age_cat.avg_energy_j.mean():.3f}J" if not _age_cat.empty else "—")

        st.divider()

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Energy per query by category**")
            fig = px.bar(cat_df.dropna(subset=["avg_energy_j"]),
                         x="category", y="avg_energy_j", color="workflow_type",
                         barmode="group", color_discrete_map=WF_COLORS,
                         labels={"avg_energy_j":"Avg Energy (J)","category":"Category"})
            st.plotly_chart(fl(fig), use_container_width=True)
        with c2:
            st.markdown("**Energy per token (mJ)**")
            fig2 = px.bar(cat_df.dropna(subset=["avg_mj_per_token"]),
                          x="category", y="avg_mj_per_token", color="workflow_type",
                          barmode="group", color_discrete_map=WF_COLORS,
                          labels={"avg_mj_per_token":"mJ / token","category":"Category"})
            st.plotly_chart(fl(fig2), use_container_width=True)

        c3, c4 = st.columns(2)
        with c3:
            st.markdown("**Phase time breakdown (agentic)**")
            _ap = cat_df[cat_df.workflow_type == "agentic"].copy()
            if not _ap.empty:
                _ph = _ap[["category","avg_plan_ms","avg_exec_ms","avg_synth_ms"]].melt(
                    id_vars="category", var_name="phase", value_name="ms")
                _ph["phase"] = _ph["phase"].map({
                    "avg_plan_ms":"Planning","avg_exec_ms":"Execution","avg_synth_ms":"Synthesis"})
                fig3 = px.bar(_ph.dropna(), x="category", y="ms", color="phase",
                              barmode="stack",
                              color_discrete_map={"Planning":"#f59e0b","Execution":"#3b82f6","Synthesis":"#a78bfa"},
                              labels={"ms":"ms","category":"Category"})
                st.plotly_chart(fl(fig3), use_container_width=True)
            else:
                st.info("No agentic data.")
        with c4:
            st.markdown("**Hardware domain breakdown (linear)**")
            _hl = cat_df[cat_df.workflow_type == "linear"].copy()
            if not _hl.empty:
                _hm = _hl[["category","avg_core_j","avg_uncore_j","avg_dram_j"]].melt(
                    id_vars="category", var_name="domain", value_name="j")
                _hm["domain"] = _hm["domain"].map(
                    {"avg_core_j":"Core","avg_uncore_j":"Uncore","avg_dram_j":"DRAM"})
                fig4 = px.bar(_hm.dropna(), x="category", y="j", color="domain",
                              barmode="stack",
                              color_discrete_map={"Core":"#3b82f6","Uncore":"#38bdf8","DRAM":"#a78bfa"},
                              labels={"j":"Joules","category":"Category"})
                st.plotly_chart(fl(fig4), use_container_width=True)
            else:
                st.info("No linear data.")

        st.divider()

        # ── Level 2: per-task ─────────────────────────────────────────────────
        st.markdown("### Level 2 — Per-task detail")
        _sel_cat = st.selectbox("Filter category",
                                ["all"] + sorted(cat_df.category.dropna().unique().tolist()),
                                key="qa_cat")
        _cat_where = f"WHERE tc.category = '{_sel_cat}'" if _sel_cat != "all" else ""

        task_df, _e2 = q_safe(f"""
            SELECT e.task_name,
                   COALESCE(tc.category,'uncategorised') AS category,
                   r.workflow_type,
                   COUNT(*) AS runs,
                   ROUND(AVG(r.total_energy_uj)/1e6, 4)  AS avg_energy_j,
                   ROUND(AVG(r.duration_ns)/1e9,    3)   AS avg_duration_s,
                   ROUND(AVG(r.total_tokens),        1)  AS avg_tokens,
                   ROUND(AVG(CASE WHEN r.total_tokens > 0
                       THEN r.total_energy_uj/r.total_tokens END)/1e3, 4) AS avg_mj_per_token,
                   ROUND(AVG(r.carbon_g)*1000,       4)  AS avg_carbon_mg,
                   ROUND(AVG(r.water_ml),            4)  AS avg_water_ml,
                   ROUND(AVG(r.llm_calls),           1)  AS avg_llm_calls,
                   ROUND(AVG(r.tool_calls),          1)  AS avg_tool_calls
            FROM runs r
            JOIN experiments e ON r.exp_id = e.exp_id
            LEFT JOIN task_categories tc ON e.task_name = tc.task_id
            {_cat_where}
            GROUP BY e.task_name, COALESCE(tc.category,'uncategorised'), r.workflow_type
            ORDER BY avg_energy_j DESC
        """)
        if _e2:
            st.error(_e2)
        elif not task_df.empty:
            fig5 = px.bar(task_df.dropna(subset=["avg_energy_j"]),
                          x="task_name", y="avg_energy_j", color="workflow_type",
                          barmode="group", color_discrete_map=WF_COLORS,
                          hover_data=["category","avg_tokens","avg_mj_per_token"],
                          labels={"avg_energy_j":"Avg Energy (J)","task_name":"Task"})
            fig5.update_xaxes(tickangle=30)
            st.plotly_chart(fl(fig5), use_container_width=True)
            _sc = [c for c in ["task_name","category","workflow_type","runs","avg_energy_j",
                                "avg_duration_s","avg_tokens","avg_mj_per_token",
                                "avg_carbon_mg","avg_water_ml","avg_llm_calls","avg_tool_calls"]
                   if c in task_df.columns]
            st.dataframe(task_df[_sc], use_container_width=True, hide_index=True)

        st.divider()

        # ── Level 3: human-scale insights ────────────────────────────────────
        st.markdown("### Level 3 — Human-scale energy insights")
        st.caption("Translating joules into things you can feel")

        if not runs.empty and "task_name" in runs.columns:
            _t_opts = sorted(runs.task_name.dropna().unique().tolist())
            _sel_t  = st.selectbox("Select task to interpret", _t_opts, key="qa_human_task")
            _tr     = runs[runs.task_name == _sel_t]

            for _wf, _border in [("linear","#22c55e"),("agentic","#ef4444")]:
                _wr = _tr[_tr.workflow_type == _wf]
                if _wr.empty: continue
                _ej  = float(_wr.energy_j.mean())
                _wml = float(_wr.water_ml.mean())    if "water_ml"   in _wr.columns and _wr.water_ml.notna().any()   else 0
                _cmg = float(_wr.carbon_g.mean()*1000) if "carbon_g" in _wr.columns and _wr.carbon_g.notna().any()  else 0
                _tok = float(_wr.total_tokens.mean())  if "total_tokens" in _wr.columns and _wr.total_tokens.notna().any() else 0
                _dur = float(_wr.duration_ms.mean()/1000) if "duration_ms" in _wr.columns and _wr.duration_ms.notna().any() else 0

                _ins = _human_energy(_ej)
                _ins_html = "".join(
                    f"<div style='margin:2px 0;font-size:10px;color:#b8c8d8;'>{ic} {desc}</div>"
                    for ic, desc in _ins)
                st.markdown(f"""
                <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                            padding:14px 18px;margin-bottom:8px;border-left:3px solid {_border};">
                  <div style="font-size:12px;font-weight:600;color:#e8f0f8;margin-bottom:6px;">
                    {_wf.upper()} · {_sel_t}
                    <span style="font-family:monospace;color:{_border};margin-left:12px;">
                      {_ej:.4f}J</span>
                    {f'<span style="font-size:9px;color:#3d5570;margin-left:8px;">{_tok:.0f} tokens · {_dur:.1f}s</span>' if _tok > 0 else ''}
                  </div>
                  {_ins_html}
                  <div style="margin-top:8px;font-size:10px;color:#7090b0;border-top:1px solid #1e2d45;padding-top:6px;">
                    💧 {_human_water(_wml)} &nbsp;·&nbsp; 🌱 {_human_carbon(_cmg)}
                  </div>
                </div>""", unsafe_allow_html=True)



# ══════════════════════════════════════════════════════════════════════════════
# PAGE: RUN REPLAY (formerly Live Monitor)
# Renamed — "live" only makes sense during execution.
# This page lets you inspect any completed run as an interactive timeline.
# ══════════════════════════════════════════════════════════════════════════════
elif page_id == "live":
    st.title("📼 Run Replay")
    st.caption(
        "Inspect any completed run as a full timeline · "
        "Live gauges appear automatically in Execute Run during recording"
    )

    # ── Server status ─────────────────────────────────────────────────────────
    _srv_ok2 = False
    if _REQUESTS_OK:
        for _ep in ["/health", "/api/system/status", "/"]:
            try:
                if _req.get(f"{LIVE_API}{_ep}", timeout=1.5).status_code < 500:
                    _srv_ok2 = True
                    break
            except Exception:
                pass

    if not _srv_ok2:
        st.warning(
            "server.py is offline — start it for richer data: "
            "uvicorn server:app --host 0.0.0.0 --port 8765 --reload  "
            "(you can still browse completed runs from the local DB below)"
        )

    # ── Run picker ────────────────────────────────────────────────────────────
    _rp_runs, _rp_err = q_safe("""
        SELECT r.run_id, r.workflow_type, r.run_number,
               e.task_name, e.provider,
               ROUND(r.total_energy_uj/1e6,4) AS energy_j,
               ROUND(r.duration_ns/1e9,2)     AS duration_s,
               r.ipc, r.total_tokens
        FROM runs r JOIN experiments e ON r.exp_id=e.exp_id
        ORDER BY r.run_id DESC LIMIT 100
    """)

    if _rp_err or _rp_runs.empty:
        st.info("No runs in DB yet — run an experiment first.")
    else:
        def _rp_lbl(row):
            return (f"#{int(row.run_id):>4}  {str(row.workflow_type):<8}  "
                    f"{str(row.task_name or '?'):<22}  "
                    f"{row.energy_j:.3f}J  {row.duration_s:.1f}s")

        _rp_opts  = {_rp_lbl(r): int(r.run_id) for _, r in _rp_runs.iterrows()}
        _rp_sel   = st.selectbox("Select run to inspect", list(_rp_opts.keys()), key="rp_sel")
        _rp_rid   = _rp_opts[_rp_sel]
        _rp_row   = _rp_runs[_rp_runs.run_id == _rp_rid].iloc[0]

        # KPI banner
        _wf  = str(_rp_row.workflow_type)
        _clr = "#22c55e" if _wf == "linear" else "#ef4444"
        st.markdown(f"""
        <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:6px;
                    padding:10px 16px;display:flex;gap:24px;flex-wrap:wrap;
                    margin-bottom:8px;border-left:3px solid {_clr};">
          <span style="font-size:11px;color:#7090b0;">Run <b style="color:#e8f0f8">#{_rp_rid}</b></span>
          <span style="font-size:11px;font-weight:600;color:{_clr}">{_wf}</span>
          <span style="font-size:11px;color:#7090b0;">task: <b style="color:#e8f0f8">{_rp_row.task_name}</b></span>
          <span style="font-size:11px;color:#7090b0;">provider: <b style="color:#e8f0f8">{_rp_row.provider}</b></span>
          <span style="font-size:11px;color:#7090b0;">energy: <b style="color:#f59e0b">{_rp_row.energy_j:.4f}J</b></span>
          <span style="font-size:11px;color:#7090b0;">duration: <b style="color:#e8f0f8">{_rp_row.duration_s:.2f}s</b></span>
        </div>""", unsafe_allow_html=True)

        # Human insight for this run
        _rp_hi = _human_energy(float(_rp_row.energy_j))
        if _rp_hi:
            st.markdown(
                "<div style='font-size:9px;color:#3d5570;margin-bottom:12px;'>"
                + " &nbsp;·&nbsp; ".join(f"{ic} {d}" for ic, d in _rp_hi)
                + "</div>", unsafe_allow_html=True)

        st.divider()

        # ── Load sample data (server if available, else direct DB) ────────────
        _load_src = "server" if _srv_ok2 else "db"
        _e_rows, _c_rows, _i_rows = [], [], []

        if _srv_ok2:
            try:
                _er = _req.get(f"{LIVE_API}/api/runs/{_rp_rid}/samples/energy", timeout=5).json()
                _e_rows = _er.get("power", []) if isinstance(_er, dict) else []
                _c_rows = _req.get(f"{LIVE_API}/api/runs/{_rp_rid}/samples/cpu", timeout=5).json()
                _i_rows = _req.get(f"{LIVE_API}/api/runs/{_rp_rid}/samples/interrupts", timeout=5).json()
                if not isinstance(_c_rows, list): _c_rows = []
                if not isinstance(_i_rows, list): _i_rows = []
            except Exception as _ex:
                st.warning(f"Server fetch failed ({_ex}) — falling back to direct DB")
                _load_src = "db"

        if _load_src == "db":
            _e_df, _ = q_safe(f"""
                SELECT ROUND((timestamp_ns-MIN(timestamp_ns) OVER (PARTITION BY run_id))/1e6,1) AS elapsed_ms,
                       ROUND(pkg_energy_uj/1e6,6)    AS pkg_j,
                       ROUND(core_energy_uj/1e6,6)   AS core_j,
                       ROUND(dram_energy_uj/1e6,6)   AS dram_j
                FROM energy_samples WHERE run_id={_rp_rid}
                ORDER BY timestamp_ns
            """)
            # Compute instantaneous watts from cumulative J (MAX-MIN approach)
            if not _e_df.empty:
                _e_df["pkg_w"]  = _e_df["pkg_j"].diff()  / (_e_df["elapsed_ms"].diff()/1000).replace(0, float("nan"))
                _e_df["core_w"] = _e_df["core_j"].diff() / (_e_df["elapsed_ms"].diff()/1000).replace(0, float("nan"))
                _e_df["dram_w"] = _e_df["dram_j"].diff() / (_e_df["elapsed_ms"].diff()/1000).replace(0, float("nan"))
                _e_rows = _e_df.dropna().to_dict("records")

            _c_df, _ = q_safe(f"""
                SELECT ROUND((timestamp_ns-MIN(timestamp_ns) OVER (PARTITION BY run_id))/1e6,1) AS elapsed_ms,
                       cpu_util_percent, package_temp, ipc, c6_residency, c1_residency
                FROM cpu_samples WHERE run_id={_rp_rid} ORDER BY timestamp_ns
            """)
            _c_rows = _c_df.to_dict("records") if not _c_df.empty else []

            _i_df, _ = q_safe(f"""
                SELECT ROUND((timestamp_ns-MIN(timestamp_ns) OVER (PARTITION BY run_id))/1e6,1) AS elapsed_ms,
                       interrupts_per_sec
                FROM interrupt_samples WHERE run_id={_rp_rid} ORDER BY timestamp_ns
            """)
            _i_rows = _i_df.to_dict("records") if not _i_df.empty else []

        # ── Timeline scrubber ─────────────────────────────────────────────────
        _max_t = 0
        if _e_rows: _max_t = max(_max_t, _e_rows[-1].get("elapsed_ms",0))
        if _c_rows: _max_t = max(_max_t, _c_rows[-1].get("elapsed_ms",0))

        if _max_t > 0:
            _t_range = st.slider(
                "Timeline window (ms)",
                min_value=0, max_value=int(_max_t),
                value=(0, int(_max_t)),
                step=max(1, int(_max_t//200)),
                key="rp_trange",
                help="Drag to zoom into a time window",
            )
            _t0, _t1 = _t_range

            def _trim(rows, t0, t1):
                return [r for r in rows
                        if t0 <= r.get("elapsed_ms",0) <= t1]

            _e_trim = _trim(_e_rows, _t0, _t1)
            _c_trim = _trim(_c_rows, _t0, _t1)
            _i_trim = _trim(_i_rows, _t0, _t1)
        else:
            _e_trim, _c_trim, _i_trim = _e_rows, _c_rows, _i_rows

        # ── Charts ────────────────────────────────────────────────────────────
        if not _e_trim and not _c_trim and not _i_trim:
            st.info(
                f"No sample data found for run #{_rp_rid}. "
                "Samples are only stored when `--save-db` is used with high-frequency logging enabled."
            )
        else:
            st.markdown(f"**Sample source: `{_load_src}` · "
                        f"{len(_e_trim)} energy · {len(_c_trim)} CPU · {len(_i_trim)} IRQ samples "
                        f"in window**")

            def _replay_chart(rows, xcol, ycols, names, colors, ytitle, height=200):
                if not rows: return None
                _df = pd.DataFrame(rows)
                fig = go.Figure()
                for yc, nm, clr in zip(ycols, names, colors):
                    if yc not in _df.columns: continue
                    _sub = _df[[xcol, yc]].dropna()
                    if _sub.empty: continue
                    _r, _g, _b = int(clr[1:3],16), int(clr[3:5],16), int(clr[5:7],16)
                    fig.add_trace(go.Scatter(
                        x=_sub[xcol], y=_sub[yc], name=nm,
                        line=dict(color=clr, width=1.5),
                        fill="tozeroy" if nm == names[0] else None,
                        fillcolor=f"rgba({_r},{_g},{_b},0.07)" if nm == names[0] else None,
                    ))
                fig.update_layout(**PL, height=height,
                                  xaxis_title="elapsed ms", yaxis_title=ytitle)
                return fig

            r1c1, r1c2 = st.columns(2)
            with r1c1:
                st.markdown("**Power draw (W)**")
                _f = _replay_chart(_e_trim, "elapsed_ms",
                    ["pkg_w","core_w","dram_w"], ["Pkg","Core","DRAM"],
                    ["#3b82f6","#22c55e","#a78bfa"], "Watts")
                if _f: st.plotly_chart(_f, use_container_width=True)

            with r1c2:
                st.markdown("**Temperature (°C)**")
                _f2 = _replay_chart(_c_trim, "elapsed_ms",
                    ["package_temp"], ["Pkg Temp"],
                    ["#ef4444"], "°C")
                if _f2: st.plotly_chart(_f2, use_container_width=True)

            r2c1, r2c2 = st.columns(2)
            with r2c1:
                st.markdown("**CPU utilisation (%)**")
                _f3 = _replay_chart(_c_trim, "elapsed_ms",
                    ["cpu_util_percent"], ["CPU Util"],
                    ["#38bdf8"], "% util")
                if _f3: st.plotly_chart(_f3, use_container_width=True)

            with r2c2:
                st.markdown("**IRQ rate**")
                _f4 = _replay_chart(_i_trim, "elapsed_ms",
                    ["interrupts_per_sec"], ["IRQ/s"],
                    ["#f59e0b"], "IRQ/s")
                if _f4: st.plotly_chart(_f4, use_container_width=True)

            # C-state breakdown
            if _c_trim and "c6_residency" in (_c_trim[0] if _c_trim else {}):
                st.markdown("**C-state residency over time**")
                _f5 = _replay_chart(_c_trim, "elapsed_ms",
                    ["c6_residency","c1_residency"],
                    ["C6 (deep sleep)","C1 (light idle)"],
                    ["#22c55e","#f59e0b"], "Residency %", height=160)
                if _f5: st.plotly_chart(_f5, use_container_width=True)

            # Orchestration events timeline
            _ev, _ev_e = q_safe(f"""
                SELECT step_index, phase, event_type,
                       ROUND((start_time_ns - MIN(start_time_ns) OVER ())/1e6,1) AS start_ms,
                       ROUND(duration_ns/1e6,1)        AS duration_ms,
                       ROUND(event_energy_uj/1e6,6)    AS event_j,
                       ROUND(power_watts,2)             AS power_w
                FROM orchestration_events
                WHERE run_id={_rp_rid}
                ORDER BY start_time_ns
            """)
            if not _ev.empty:
                st.divider()
                st.markdown("**Orchestration events timeline**")
                PHASE_C = {"planning":"#f59e0b","execution":"#3b82f6",
                           "synthesis":"#a78bfa","llm_wait":"#38bdf8"}
                _ev_fig = go.Figure()
                for _ph_name, _ph_clr in PHASE_C.items():
                    _ph_rows = _ev[_ev.phase == _ph_name]
                    if _ph_rows.empty: continue
                    _r,_g,_b = int(_ph_clr[1:3],16),int(_ph_clr[3:5],16),int(_ph_clr[5:7],16)
                    _ev_fig.add_trace(go.Bar(
                        name=_ph_name.capitalize(),
                        x=_ph_rows.start_ms,
                        y=_ph_rows.duration_ms,
                        marker_color=_ph_clr,
                        marker_line_width=0,
                        width=max(20, float(_ph_rows.duration_ms.mean()) * 0.8),
                        hovertemplate=(
                            "<b>%{customdata[0]}</b><br>"
                            "start: %{x}ms<br>duration: %{y}ms<br>"
                            "energy: %{customdata[1]:.6f}J<br>"
                            "power: %{customdata[2]:.2f}W<extra></extra>"
                        ),
                        customdata=_ph_rows[["event_type","event_j","power_w"]].values,
                    ))
                _ev_fig.update_layout(**PL, height=220, barmode="overlay",
                    xaxis_title="elapsed ms", yaxis_title="event duration ms")
                st.plotly_chart(_ev_fig, use_container_width=True)
                st.dataframe(_ev[["step_index","phase","event_type","start_ms",
                                   "duration_ms","event_j","power_w"]].round(4),
                             use_container_width=True, hide_index=True)

elif page_id == "sql_query":
    st.title("💬 SQL Query")
    st.caption(f"Ad-hoc SELECT queries against `{DB_PATH.name}` · results exportable as CSV")

    QUERY_LIBRARY = {
        "— pick a preset —": "",
        "Energy by category": (
            "SELECT tc.category, r.workflow_type, COUNT(*) AS runs,\n"
            "  ROUND(AVG(r.total_energy_uj)/1e6,4) AS avg_energy_j,\n"
            "  ROUND(AVG(r.dynamic_energy_uj)/1e6,4) AS avg_dynamic_j\n"
            "FROM runs r\n"
            "JOIN experiments e ON r.exp_id=e.exp_id\n"
            "LEFT JOIN task_categories tc ON e.task_name=tc.task_id\n"
            "GROUP BY tc.category, r.workflow_type ORDER BY tc.category"
        ),
        "Tax breakdown by task": (
            "SELECT tc.category, e.task_name,\n"
            "  ROUND(AVG(ots.linear_dynamic_uj/1e6),4) AS linear_j,\n"
            "  ROUND(AVG(ots.agentic_dynamic_uj/1e6),4) AS agentic_j,\n"
            "  ROUND(AVG(ots.orchestration_tax_uj/1e6),4) AS tax_j,\n"
            "  ROUND(AVG(ots.tax_percent),2) AS tax_pct\n"
            "FROM orchestration_tax_summary ots\n"
            "JOIN runs rl ON ots.linear_run_id=rl.run_id\n"
            "JOIN experiments e ON rl.exp_id=e.exp_id\n"
            "LEFT JOIN task_categories tc ON e.task_name=tc.task_id\n"
            "GROUP BY tc.category, e.task_name"
        ),
        "Energy per token by model": (
            "SELECT e.model_name, e.provider,\n"
            "  ROUND(AVG(r.energy_per_token*1000),4) AS avg_mj_per_token,\n"
            "  COUNT(*) AS runs\n"
            "FROM runs r JOIN experiments e ON r.exp_id=e.exp_id\n"
            "WHERE r.total_tokens>0\n"
            "GROUP BY e.model_name, e.provider ORDER BY avg_mj_per_token"
        ),
        "Carbon by provider · region": (
            "SELECT e.provider, e.country_code,\n"
            "  ROUND(SUM(r.carbon_g)*1000,3) AS total_carbon_mg,\n"
            "  ROUND(SUM(r.water_ml),2) AS total_water_ml,\n"
            "  COUNT(*) AS runs\n"
            "FROM runs r JOIN experiments e ON r.exp_id=e.exp_id\n"
            "GROUP BY e.provider, e.country_code ORDER BY total_carbon_mg DESC"
        ),
        "Sample counts per run": (
            "SELECT r.run_id, r.workflow_type, e.task_name,\n"
            "  COUNT(DISTINCT es.sample_id) AS energy_samples,\n"
            "  COUNT(DISTINCT cs.sample_id) AS cpu_samples\n"
            "FROM runs r\n"
            "JOIN experiments e ON r.exp_id=e.exp_id\n"
            "LEFT JOIN energy_samples es ON r.run_id=es.run_id\n"
            "LEFT JOIN cpu_samples cs ON r.run_id=cs.run_id\n"
            "GROUP BY r.run_id ORDER BY r.run_id DESC LIMIT 20"
        ),
        "Recent runs": (
            "SELECT r.run_id, r.workflow_type, e.task_name, e.provider,\n"
            "  ROUND(r.total_energy_uj/1e6,4) AS energy_j,\n"
            "  ROUND(r.duration_ns/1e9,2) AS duration_s,\n"
            "  r.total_tokens, r.ipc\n"
            "FROM runs r JOIN experiments e ON r.exp_id=e.exp_id\n"
            "ORDER BY r.run_id DESC LIMIT 30"
        ),
        "Sustainability report": (
            "SELECT e.provider, tc.category,\n"
            "  ROUND(SUM(r.carbon_g),4) AS total_carbon_g,\n"
            "  ROUND(SUM(r.water_ml),2) AS total_water_ml,\n"
            "  COUNT(*) AS runs\n"
            "FROM runs r JOIN experiments e ON r.exp_id=e.exp_id\n"
            "LEFT JOIN task_categories tc ON e.task_name=tc.task_id\n"
            "GROUP BY e.provider, tc.category"
        ),
    }

    _preset = st.selectbox("Preset queries", list(QUERY_LIBRARY.keys()), key="sql_preset")
    _default_sql = QUERY_LIBRARY.get(_preset, "")

    _sql_input = st.text_area("SQL (SELECT only)", value=_default_sql, height=150,
                               key="sql_input",
                               placeholder="SELECT * FROM runs LIMIT 10")

    _col_r, _col_l = st.columns([2, 1])
    with _col_r:
        _sql_run = st.button("▶ Run query", type="primary", key="sql_run")
    with _col_l:
        _row_limit = st.number_input("Row limit", 10, 10000, 500, step=100, key="sql_limit")

    if _sql_run:
        _cleaned = _sql_input.strip()
        _upper   = _cleaned.upper()
        _bad     = [kw for kw in ["DROP","DELETE","UPDATE","INSERT","ALTER","CREATE",
                                   "REPLACE","ATTACH"] if kw in _upper]
        if _bad:
            st.error(f"Blocked keywords: {', '.join(_bad)}. SELECT only.")
        elif not _cleaned:
            st.warning("Enter a SQL query first.")
        else:
            if "LIMIT" not in _upper:
                _cleaned = f"SELECT * FROM ({_cleaned}) _q LIMIT {int(_row_limit)}"
            _result, _sql_err = q_safe(_cleaned)
            if _sql_err:
                st.error(f"SQL Error: {_sql_err}")
            elif _result.empty:
                st.info("Query returned 0 rows.")
            else:
                st.success(f"✓ {len(_result):,} rows")
                st.dataframe(_result, use_container_width=True, hide_index=True)
                st.download_button("⬇ Download CSV", data=_result.to_csv(index=False),
                                   file_name="alems_query.csv", mime="text/csv",
                                   key="sql_dl")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: SETTINGS
# ══════════════════════════════════════════════════════════════════════════════
elif page_id == "settings":
    st.title("⚙ Settings")
    st.caption("Read-only view of all A-LEMS configuration files + live DB statistics")

    _cfg = PROJECT_ROOT / "config"

    def _load_yaml(p):
        if not _YAML_OK: return None, "pip install pyyaml"
        try:
            with open(p) as f: return _yaml.safe_load(f), None
        except Exception as e: return None, str(e)

    def _load_json(p):
        try:
            with open(p) as f: return _json.load(f), None
        except Exception as e: return None, str(e)

    # app_settings.yaml
    _app, _app_e = _load_yaml(_cfg / "app_settings.yaml")
    if _app_e:
        st.error(f"app_settings.yaml: {_app_e}")
    elif _app:
        _db_eng = (_app.get("database") or {}).get("engine","?")
        _sr     = ((_app.get("webui")      or {}).get("sampling_rate_hz"))          or "?"
        _cd     = ((_app.get("experiment") or {}).get("cool_down_seconds"))         or "?"
        _ta     = ((_app.get("alerts")     or {}).get("temperature_threshold_celsius")) or "?"
        _di     = ((_app.get("experiment") or {}).get("default_iterations"))        or "?"
        k1,k2,k3,k4,k5 = st.columns(5)
        k1.metric("DB engine",     str(_db_eng))
        k2.metric("Sampling",      f"{_sr}Hz")
        k3.metric("Cool-down",     f"{_cd}s")
        k4.metric("Temp alert",    f"{_ta}°C")
        k5.metric("Default iters", str(_di))
        with st.expander("app_settings.yaml", expanded=False):
            st.json(_app)

    st.divider()

    # hw_config.json
    _hw, _hw_e = _load_json(_cfg / "hw_config.json")
    if _hw_e:
        st.error(f"hw_config.json: {_hw_e}")
    elif _hw:
        _cpu  = _hw.get("cpu")       or {}
        _rapl = _hw.get("rapl")      or {}
        _ts   = _hw.get("turbostat") or {}
        _meta = _hw.get("metadata")  or {}
        h1,h2,h3,h4 = st.columns(4)
        h1.metric("CPU", str(_cpu.get("model_name", _meta.get("cpu_model","?")))[:30])
        h2.metric("Cores", str(_cpu.get("physical_cores", _cpu.get("cores","?"))))
        _domains = _rapl.get("domains", _rapl.get("available_domains",[]))
        h3.metric("RAPL domains", str(len(_domains or [])))
        _ts_ok = _ts.get("available", _ts.get("found", False))
        h4.metric("turbostat", "✅" if _ts_ok else "❌")
        with st.expander("hw_config.json", expanded=False):
            st.json(_hw)

    st.divider()

    # tasks.yaml
    _tc, _tc_e = _load_yaml(_cfg / "tasks.yaml")
    if _tc_e:
        st.error(f"tasks.yaml: {_tc_e}")
    elif _tc:
        _tlist = _tc.get("tasks",[])
        t1,t2 = st.columns(2)
        t1.metric("Tasks defined", len(_tlist))
        t2.metric("Categories",    len({t.get("category","") for t in _tlist}))
        _tdf = pd.DataFrame([{
            "id":t.get("id",""),"category":t.get("category",""),
            "name":t.get("name",""),"level":t.get("level",""),
            "tool_calls":t.get("tool_calls",0)} for t in _tlist])
        st.dataframe(_tdf, use_container_width=True, hide_index=True)
        with st.expander("tasks.yaml", expanded=False):
            st.json(_tc)

    st.divider()

    # models.json
    _mo, _mo_e = _load_json(_cfg / "models.json")
    if _mo_e:
        st.error(f"models.json: {_mo_e}")
    elif _mo:
        with st.expander("models.json", expanded=False):
            st.json(_mo)

    # DB stats
    st.divider()
    st.markdown("**Database row counts**")
    _dbs, _dbs_e = q_safe("""
        SELECT 'experiments'        AS tbl, COUNT(*) AS rows FROM experiments UNION ALL
        SELECT 'runs',              COUNT(*) FROM runs                         UNION ALL
        SELECT 'energy_samples',    COUNT(*) FROM energy_samples               UNION ALL
        SELECT 'cpu_samples',       COUNT(*) FROM cpu_samples                  UNION ALL
        SELECT 'interrupt_samples', COUNT(*) FROM interrupt_samples            UNION ALL
        SELECT 'orchestration_events', COUNT(*) FROM orchestration_events      UNION ALL
        SELECT 'task_categories',   COUNT(*) FROM task_categories
    """)
    if not _dbs_e and not _dbs.empty:
        _dbs.columns = ["Table","Rows"]
        st.dataframe(_dbs, use_container_width=True, hide_index=True)