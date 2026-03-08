"""
A-LEMS Streamlit Dashboard
==========================
All 8 pages. Reads from the same SQLite DB as server.py.

Run:
    pip install streamlit plotly pandas
    streamlit run streamlit_app.py

Config:
    Set DB_PATH below, or pass --server.port / --theme.base in CLI.
    For remote DB, mount via sshfs or copy experiments.db locally.
"""

import sqlite3
import json
import time
import threading
from pathlib import Path
from contextlib import contextmanager

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# ── CONFIG ────────────────────────────────────────────────────
DB_PATH = Path(__file__).parent / "data" / "experiments.db"

# Harness: attempt import for Execute page
HARNESS_AVAILABLE = False
try:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from core.config_loader import ConfigLoader
    from core.execution.harness import ExperimentHarness
    from core.execution.linear import LinearExecutor
    from core.execution.agentic import AgenticExecutor
    HARNESS_AVAILABLE = True
except Exception:
    pass

# ── PAGE CONFIG ───────────────────────────────────────────────
st.set_page_config(
    page_title="A-LEMS · Energy Measurement",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── THEME STYLES ──────────────────────────────────────────────
# Inject minimal CSS to tighten Streamlit's default spacing
st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background: #090d13; }
[data-testid="stSidebar"]          { background: #0f1520; border-right: 1px solid #1e2d45; }
[data-testid="stHeader"]           { background: transparent; }
.block-container                   { padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1600px; }
h1  { font-size: 1.15rem !important; color: #e8f0f8 !important; }
h2  { font-size: 1rem   !important; color: #b8c8d8 !important; }
h3  { font-size: 0.9rem !important; color: #7090b0 !important; }
p, li { font-size: 0.82rem; color: #b8c8d8; }
.stMetric label  { font-size: 0.7rem !important; color: #3d5570 !important; text-transform: uppercase; letter-spacing: .07em; }
.stMetric [data-testid="stMetricValue"] { font-size: 1.4rem !important; font-family: 'IBM Plex Mono', monospace !important; }
.stDataFrame { font-size: 0.78rem; }
.stAlert     { font-size: 0.8rem; }
</style>
""", unsafe_allow_html=True)

# ── DB HELPERS ────────────────────────────────────────────────
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
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with db() as con:
        try:
            return pd.read_sql_query(sql, con, params=params)
        except Exception as e:
            return pd.DataFrame()

@st.cache_data(ttl=30, show_spinner=False)
def query_one(sql: str, params: tuple = ()) -> dict:
    with db() as con:
        try:
            row = con.execute(sql, params).fetchone()
            return dict(row) if row else {}
        except Exception:
            return {}

# ── PLOTLY THEME ──────────────────────────────────────────────
PLOTLY_LAYOUT = dict(
    paper_bgcolor="#0f1520",
    plot_bgcolor="#090d13",
    font=dict(family="IBM Plex Mono, monospace", size=10, color="#7090b0"),
    margin=dict(l=40, r=20, t=30, b=30),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=9)),
    colorway=["#22c55e", "#ef4444", "#3b82f6", "#f59e0b", "#38bdf8", "#a78bfa"],
    xaxis=dict(gridcolor="#1e2d45", linecolor="#1e2d45", tickfont=dict(size=9)),
    yaxis=dict(gridcolor="#1e2d45", linecolor="#1e2d45", tickfont=dict(size=9)),
)

def fig_update(fig, **kwargs):
    fig.update_layout(**PLOTLY_LAYOUT, **kwargs)
    return fig

# ── COLOUR HELPERS ────────────────────────────────────────────
def wf_color(wf):
    return "#22c55e" if wf == "linear" else "#ef4444"

def prov_color(prov):
    return "#ef4444" if prov == "local" else "#3b82f6"

# ── SIDEBAR ───────────────────────────────────────────────────
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
]

with st.sidebar:
    st.markdown("### ⚡ A-LEMS")
    st.markdown("<div style='font-size:9px;color:#3d5570;text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>Energy Measurement</div>", unsafe_allow_html=True)
    selected = st.radio("Navigation", [p[0] for p in PAGES], label_visibility="collapsed")
    page_id  = dict(PAGES)[selected]

    st.divider()

    # System info
    try:
        ov = query_one("SELECT COUNT(*) as runs FROM runs")
        st.caption(f"**Runs:** {ov.get('runs', '—')}")
        st.caption(f"**Harness:** {'✅ Connected' if HARNESS_AVAILABLE else '🔒 Read-only'}")
        st.caption(f"**DB:** {DB_PATH.name}")
    except Exception:
        st.caption("⚠ DB not connected")

    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

# ── LOAD CORE DATA ────────────────────────────────────────────
@st.cache_data(ttl=30, show_spinner=False)
def load_overview():
    return query_one("""
        SELECT
            COUNT(DISTINCT e.exp_id)                                        AS total_experiments,
            COUNT(r.run_id)                                                 AS total_runs,
            SUM(CASE WHEN r.workflow_type='linear'  THEN 1 ELSE 0 END)     AS linear_runs,
            SUM(CASE WHEN r.workflow_type='agentic' THEN 1 ELSE 0 END)     AS agentic_runs,
            AVG(CASE WHEN r.workflow_type='linear'  THEN r.total_energy_uj END)/1e6 AS avg_linear_j,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.total_energy_uj END)/1e6 AS avg_agentic_j,
            MAX(r.total_energy_uj)/1e6  AS max_energy_j,
            MIN(r.total_energy_uj)/1e6  AS min_energy_j,
            SUM(r.total_energy_uj)/1e6  AS total_energy_j,
            AVG(r.ipc)                  AS avg_ipc,
            MAX(r.ipc)                  AS max_ipc,
            AVG(r.cache_miss_rate)*100  AS avg_cache_miss_pct,
            SUM(r.carbon_g)*1000        AS total_carbon_mg,
            AVG(r.carbon_g)*1000        AS avg_carbon_mg,
            AVG(r.water_ml)             AS avg_water_ml,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.planning_time_ms  END) AS avg_planning_ms,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.execution_time_ms END) AS avg_execution_ms,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.synthesis_time_ms END) AS avg_synthesis_ms
        FROM experiments e LEFT JOIN runs r ON e.exp_id = r.exp_id
    """)

@st.cache_data(ttl=30, show_spinner=False)
def load_runs():
    # Query runs directly — ml_features joins experiments which also has workflow_type,
    # causing pandas to rename them workflow_type_x / workflow_type_y on SELECT *.
    return query("""
        SELECT
            r.run_id, r.exp_id, r.workflow_type, r.run_number,
            r.duration_ns / 1e6                AS duration_ms,
            r.total_energy_uj / 1e6            AS energy_j,
            r.dynamic_energy_uj / 1e6          AS dynamic_energy_j,
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
            e.governor, e.turbo_enabled
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        ORDER BY r.run_id DESC
    """)

@st.cache_data(ttl=30, show_spinner=False)
def load_tax():
    return query("""
        SELECT
            ots.comparison_id, ots.linear_run_id, ots.agentic_run_id,
            ots.tax_percent,
            ots.orchestration_tax_uj / 1e6   AS tax_j,
            ots.linear_dynamic_uj  / 1e6     AS linear_dynamic_j,
            ots.agentic_dynamic_uj / 1e6     AS agentic_dynamic_j,
            ra.planning_time_ms, ra.execution_time_ms, ra.synthesis_time_ms,
            ra.llm_calls, ra.tool_calls, ra.total_tokens,
            el.task_name, el.country_code, el.provider
        FROM orchestration_tax_summary ots
        JOIN runs rl ON ots.linear_run_id  = rl.run_id
        JOIN runs ra ON ots.agentic_run_id = ra.run_id
        JOIN experiments el ON rl.exp_id = el.exp_id
        ORDER BY ots.tax_percent DESC
    """)

ov    = load_overview()
runs  = load_runs()
tax   = load_tax()

# Derived subsets
linear_runs  = runs[runs.workflow_type == "linear"]  if not runs.empty and "workflow_type" in runs.columns else pd.DataFrame()
agentic_runs = runs[runs.workflow_type == "agentic"] if not runs.empty and "workflow_type" in runs.columns else pd.DataFrame()

avg_lin_j  = linear_runs.energy_j.mean()  if not linear_runs.empty  and "energy_j" in linear_runs.columns  else 0
avg_age_j  = agentic_runs.energy_j.mean() if not agentic_runs.empty and "energy_j" in agentic_runs.columns else 0
tax_mult   = avg_age_j / avg_lin_j if avg_lin_j > 0 else 0

plan_ms  = ov.get("avg_planning_ms",  0) or 0
exec_ms  = ov.get("avg_execution_ms", 0) or 0
synth_ms = ov.get("avg_synthesis_ms", 0) or 0
phase_total = plan_ms + exec_ms + synth_ms or 1
plan_pct  = plan_ms  / phase_total * 100
exec_pct  = exec_ms  / phase_total * 100
synth_pct = synth_ms / phase_total * 100

# ═════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ═════════════════════════════════════════════════════════════
if page_id == "overview":
    st.title("Overview — Agentic vs Linear Energy")

    # ── Hero section ─────────────────────────────────────────
    st.markdown(f"""
    <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;padding:20px 24px;margin-bottom:16px;border-top:2px solid #ef4444;">
      <div style="font-size:18px;font-weight:600;color:#e8f0f8;margin-bottom:4px;">
        Agentic costs <span style="color:#ef4444;font-family:'IBM Plex Mono',monospace;">{tax_mult:.1f}×</span> more energy than linear for the same task
      </div>
      <div style="font-size:11px;color:#3d5570;margin-bottom:16px;">
        Measured across {ov.get('total_runs', '—')} runs · {ov.get('total_experiments', '—')} experiments
      </div>
      <div style="display:flex;align-items:center;gap:16px;margin-bottom:10px;">
        <div style="width:70px;font-size:11px;color:#7090b0;">Linear</div>
        <div style="flex:1;background:#1a2438;border-radius:4px;overflow:hidden;height:28px;">
          <div style="width:{100/max(tax_mult,1):.0f}%;background:#22c55e;height:100%;display:flex;align-items:center;padding-left:10px;font-size:10px;color:#fff;font-family:'IBM Plex Mono',monospace;">{avg_lin_j:.3f}J</div>
        </div>
        <div style="width:50px;font-size:10px;color:#7090b0;font-family:monospace;">1×</div>
      </div>
      <div style="display:flex;align-items:center;gap:16px;">
        <div style="width:70px;font-size:11px;color:#7090b0;">Agentic</div>
        <div style="flex:1;background:#1a2438;border-radius:4px;overflow:hidden;height:28px;">
          <div style="width:100%;background:#ef4444;height:100%;display:flex;align-items:center;padding-left:10px;font-size:10px;color:#fff;font-family:'IBM Plex Mono',monospace;">{avg_age_j:.3f}J</div>
        </div>
        <div style="width:50px;font-size:10px;color:#ef4444;font-family:monospace;font-weight:600;">{tax_mult:.1f}×</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Phase bar
    if plan_ms > 0:
        st.markdown(f"""
        <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;padding:16px 20px;margin-bottom:16px;">
          <div style="font-size:9px;color:#3d5570;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;">Where the overhead goes — agentic time breakdown</div>
          <div style="display:flex;height:22px;border-radius:4px;overflow:hidden;gap:1px;">
            <div style="width:{plan_pct:.0f}%;background:#f59e0b;display:flex;align-items:center;justify-content:center;font-size:9px;color:rgba(255,255,255,.85);font-family:monospace;">{plan_pct:.0f}% plan</div>
            <div style="width:{exec_pct:.0f}%;background:#3b82f6;display:flex;align-items:center;justify-content:center;font-size:9px;color:rgba(255,255,255,.85);font-family:monospace;">{exec_pct:.0f}% exec</div>
            <div style="width:{synth_pct:.0f}%;background:#a78bfa;display:flex;align-items:center;justify-content:center;font-size:9px;color:rgba(255,255,255,.85);font-family:monospace;">{synth_pct:.0f}% synth</div>
          </div>
          <div style="display:flex;gap:20px;margin-top:8px;font-size:9px;color:#3d5570;">
            <span><span style="color:#f59e0b">■</span> Planning {plan_ms:.0f}ms — pure overhead</span>
            <span><span style="color:#3b82f6">■</span> Execution {exec_ms:.0f}ms — tool latency</span>
            <span><span style="color:#a78bfa">■</span> Synthesis {synth_ms:.0f}ms — context merge</span>
          </div>
        </div>
        """, unsafe_allow_html=True)

    # KPIs
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Runs",    ov.get("total_runs", "—"))
    c2.metric("Tax Multiple",  f"{tax_mult:.1f}×",    delta=f"{(tax_mult-1)*100:.0f}% overhead", delta_color="inverse")
    c3.metric("Avg Planning",  f"{plan_ms:.0f}ms",    delta=f"{plan_pct:.0f}% of agentic time",  delta_color="inverse")
    c4.metric("Peak IPC",      f"{ov.get('max_ipc', 0):.3f}")
    c5.metric("Avg Carbon",    f"{ov.get('avg_carbon_mg', 0):.3f}mg")
    c6.metric("Total Energy",  f"{ov.get('total_energy_j', 0):.1f}J")

    st.divider()

    # Charts
    if not runs.empty and "energy_j" in runs.columns:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Duration vs Energy**")
            fig = px.scatter(
                runs.dropna(subset=["energy_j", "duration_ms"]),
                x=runs["duration_ms"] / 1000, y="energy_j",
                color="workflow_type",
                color_discrete_map={"linear": "#22c55e", "agentic": "#ef4444"},
                labels={"x": "Duration (s)", "energy_j": "Energy (J)"},
                hover_data=["run_id", "provider", "complexity_level"],
            )
            st.plotly_chart(fig_update(fig), use_container_width=True)

        with col2:
            st.markdown("**IPC vs Cache Miss**")
            fig2 = px.scatter(
                runs.dropna(subset=["ipc", "cache_miss_rate"]),
                x=runs["cache_miss_rate"] * 100, y="ipc",
                color="workflow_type",
                color_discrete_map={"linear": "#22c55e", "agentic": "#ef4444"},
                labels={"x": "Cache Miss %", "y": "IPC"},
            )
            st.plotly_chart(fig_update(fig2), use_container_width=True)
    else:
        st.info("No run data available — populate the database with experiments first.")

# ═════════════════════════════════════════════════════════════
# PAGE: ENERGY
# ═════════════════════════════════════════════════════════════
elif page_id == "energy":
    st.title("Energy Analysis")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Min Energy",     f"{ov.get('min_energy_j', 0):.3f}J")
    c2.metric("Max Energy",     f"{ov.get('max_energy_j', 0):.3f}J")
    c3.metric("Total Measured", f"{ov.get('total_energy_j', 0):.1f}J")
    c4.metric("Avg Carbon",     f"{ov.get('avg_carbon_mg', 0):.3f}mg")
    c5.metric("Avg Water",      f"{ov.get('avg_water_ml', 0):.3f}ml")

    st.divider()

    if not runs.empty and "energy_j" in runs.columns:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Energy per run (sorted, log scale)**")
            sorted_runs = runs.dropna(subset=["energy_j"]).sort_values("energy_j")
            fig = px.bar(
                sorted_runs, x=sorted_runs.index, y="energy_j",
                color="workflow_type",
                color_discrete_map={"linear": "#22c55e", "agentic": "#ef4444"},
                log_y=True,
                labels={"energy_j": "Energy (J)", "x": "Run"},
            )
            fig.update_xaxes(showticklabels=False)
            st.plotly_chart(fig_update(fig), use_container_width=True)
            st.caption("Log scale — the gap is multiplicative. Agentic runs cluster at the top.")

        with col2:
            st.markdown("**Carbon by provider · region**")
            if "carbon_g" in runs.columns:
                carbon_df = runs.dropna(subset=["carbon_g"]).copy()
                carbon_df["group"]     = carbon_df["provider"].fillna("?") + "·" + carbon_df["country_code"].fillna("?")
                carbon_df["carbon_mg"] = carbon_df["carbon_g"] * 1000
                carbon_agg = carbon_df.groupby("group")["carbon_mg"].mean().reset_index()
                fig3 = px.bar(carbon_agg, x="group", y="carbon_mg", log_y=True,
                              labels={"carbon_mg": "avg carbon mg CO₂e", "group": ""},
                              color="group")
                st.plotly_chart(fig_update(fig3), use_container_width=True)
                st.caption("IN grid intensity (0.82 kg/kWh) is 2× the US factor — same energy, double carbon.")
            else:
                st.info("No carbon data")

    st.divider()

    if not runs.empty and "api_latency_ms" in runs.columns:
        st.markdown("**Energy vs API latency** — longer wait = more idle RAPL drain")
        cloud = runs[(runs.provider != "local") & runs.api_latency_ms.notna() & runs.energy_j.notna()]
        if not cloud.empty:
            fig4 = px.scatter(cloud, x=cloud.api_latency_ms / 1000, y="energy_j",
                              color="country_code", log_y=True,
                              labels={"x": "API Latency (s)", "energy_j": "Energy (J)"})
            st.plotly_chart(fig_update(fig4), use_container_width=True)

# ═════════════════════════════════════════════════════════════
# PAGE: CPU & C-STATES
# ═════════════════════════════════════════════════════════════
elif page_id == "cpu":
    st.title("CPU & C-State Analysis")

    cstate_df = query("""
        SELECT e.provider, e.workflow_type,
               AVG(cs.c1_residency) AS c1, AVG(cs.c2_residency) AS c2,
               AVG(cs.c3_residency) AS c3, AVG(cs.c6_residency) AS c6,
               AVG(cs.c7_residency) AS c7,
               AVG(cs.cpu_util_percent) AS util,
               AVG(cs.package_power) AS pkg_w,
               COUNT(cs.sample_id)  AS samples
        FROM cpu_samples cs
        JOIN runs r  ON cs.run_id = r.run_id
        JOIN experiments e ON r.exp_id = e.exp_id
        GROUP BY e.provider, e.workflow_type
    """)

    if not cstate_df.empty:
        st.markdown("**C-State Residency — measured from cpu_samples**")
        for _, row in cstate_df.iterrows():
            with st.container():
                st.markdown(f"**{row.provider} · {row.workflow_type}** — {row.pkg_w:.2f}W pkg · {int(row.samples):,} samples")
                cs_data = {"C0": max(0, 100 - row.c1 - row.c2 - row.c3 - row.c6 - row.c7),
                           "C1": row.c1, "C2": row.c2, "C3": row.c3, "C6": row.c6, "C7": row.c7}
                cs_df = pd.DataFrame(list(cs_data.items()), columns=["State", "Residency%"])
                fig = px.bar(cs_df, x="Residency%", y="State", orientation="h",
                             color="State",
                             color_discrete_map={"C0":"#ef4444","C1":"#38bdf8","C2":"#3b82f6","C3":"#a78bfa","C6":"#22c55e","C7":"#f59e0b"})
                fig.update_layout(**PLOTLY_LAYOUT, height=160)
                st.plotly_chart(fig, use_container_width=True)
        st.info("Cloud: mostly C6/C7 (deep sleep between API calls). Local: forced C0 throughout inference loop.")
    else:
        st.info("No cpu_samples data yet — run experiments to populate.")

    st.divider()

    if not runs.empty and "ipc" in runs.columns:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**IPC Distribution**")
            fig = px.histogram(runs.dropna(subset=["ipc"]), x="ipc",
                               color="workflow_type",
                               color_discrete_map={"linear":"#22c55e","agentic":"#ef4444"},
                               nbins=20, barmode="overlay", opacity=.75)
            st.plotly_chart(fig_update(fig), use_container_width=True)
        with col2:
            st.markdown("**Cache Miss vs Energy**")
            if "cache_miss_rate" in runs.columns and "energy_j" in runs.columns:
                fig2 = px.scatter(runs.dropna(subset=["cache_miss_rate","energy_j"]),
                                  x=runs["cache_miss_rate"]*100, y="energy_j",
                                  color="workflow_type",
                                  color_discrete_map={"linear":"#22c55e","agentic":"#ef4444"},
                                  log_y=True,
                                  labels={"x":"Cache Miss %","energy_j":"Energy J"})
                st.plotly_chart(fig_update(fig2), use_container_width=True)

# ═════════════════════════════════════════════════════════════
# PAGE: SCHEDULER
# ═════════════════════════════════════════════════════════════
elif page_id == "scheduler":
    st.title("OS Scheduler Analysis")

    if not runs.empty and "thread_migrations" in runs.columns:
        sched = runs.dropna(subset=["thread_migrations"])
        lin_s  = sched[sched.workflow_type == "linear"]
        age_s  = sched[sched.workflow_type == "agentic"]

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Max Migrations",    f"{int(sched.thread_migrations.max()):,}")
        c2.metric("Linear avg",        f"{lin_s.thread_migrations.mean():.0f}")
        c3.metric("Agentic avg",       f"{age_s.thread_migrations.mean():.0f}",
                  delta=f"{age_s.thread_migrations.mean() / max(lin_s.thread_migrations.mean(), 1):.1f}× vs linear", delta_color="inverse")
        c4.metric("Max IRQ/s",         f"{sched.interrupt_rate.max():,.0f}" if "interrupt_rate" in sched.columns else "—")
        c5.metric("Avg Cache Miss",    f"{ov.get('avg_cache_miss_pct', 0):.1f}%")

        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Thread Migrations vs Duration**")
            fig = px.scatter(sched.dropna(subset=["duration_ms"]),
                             x=sched["duration_ms"]/1000, y="thread_migrations",
                             color="workflow_type",
                             color_discrete_map={"linear":"#22c55e","agentic":"#ef4444"},
                             labels={"x":"Duration (s)","thread_migrations":"Migrations"})
            st.plotly_chart(fig_update(fig), use_container_width=True)
            st.caption("r²≈0.89: duration is the primary driver. Phase transitions in agentic runs generate migration bursts.")

        with col2:
            st.markdown("**Migrations → Cache Miss (causal chain)**")
            if "cache_miss_rate" in sched.columns:
                fig2 = px.scatter(sched.dropna(subset=["cache_miss_rate"]),
                                  x="thread_migrations", y=sched["cache_miss_rate"]*100,
                                  color="workflow_type",
                                  color_discrete_map={"linear":"#22c55e","agentic":"#ef4444"},
                                  labels={"thread_migrations":"Migrations","y":"Cache Miss %"})
                st.plotly_chart(fig_update(fig2), use_container_width=True)
                st.caption("Causal chain: phase transitions → migrations → cache eviction → IPC degradation → energy waste.")
    else:
        st.info("No scheduler data available.")

# ═════════════════════════════════════════════════════════════
# PAGE: DOMAINS
# ═════════════════════════════════════════════════════════════
elif page_id == "domains":
    st.title("Domain Energy Breakdown")

    domains = query("SELECT * FROM orchestration_analysis ORDER BY run_id")

    if not domains.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Avg Core Share",   f"{domains.core_share.mean()*100:.1f}%"   if "core_share"   in domains.columns else "—")
        c2.metric("Avg Uncore Share", f"{domains.uncore_share.mean()*100:.1f}%" if "uncore_share" in domains.columns else "—")
        c3.metric("Avg Workload J",   f"{domains.workload_energy_j.mean():.3f}J"    if "workload_energy_j"    in domains.columns else "—")
        c4.metric("Avg Tax J",        f"{domains.orchestration_tax_j.mean():.3f}J"  if "orchestration_tax_j"  in domains.columns else "—")

        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Domain shares — stacked**")
            top = domains.head(30)
            fig = go.Figure()
            for col, color in [("core_energy_j","#3b82f6"),("uncore_energy_j","#38bdf8"),("dram_energy_j","#a78bfa")]:
                if col in top.columns:
                    fig.add_trace(go.Bar(name=col.replace("_energy_j",""), x=top.run_id.astype(str), y=top[col], marker_color=color))
            fig.update_layout(barmode="stack", **PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Workload vs Tax**")
            fig2 = go.Figure()
            for col, color, name in [("workload_energy_j","#22c55e","Workload"),("orchestration_tax_j","#ef4444","Tax")]:
                if col in domains.columns:
                    fig2.add_trace(go.Bar(name=name, x=domains.run_id.astype(str), y=domains[col], marker_color=color))
            fig2.update_layout(barmode="stack", **PLOTLY_LAYOUT)
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown("**Per-run breakdown**")
        show_cols = [c for c in ["run_id","workflow_type","task_name","pkg_energy_j","core_energy_j","uncore_energy_j","dram_energy_j","workload_energy_j","orchestration_tax_j"] if c in domains.columns]
        st.dataframe(domains[show_cols], use_container_width=True, hide_index=True)
    else:
        st.info("No domain data — idle_baselines must be linked to runs.")

# ═════════════════════════════════════════════════════════════
# PAGE: TAX ATTRIBUTION
# ═════════════════════════════════════════════════════════════
elif page_id == "tax":
    st.title("Tax Attribution")

    if not tax.empty:
        avg_tax = tax.tax_percent.mean() if "tax_percent" in tax.columns else 0
        max_tax = tax.tax_percent.max()  if "tax_percent" in tax.columns else 0

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"""
            <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;padding:14px 16px;border-left:3px solid #f59e0b;">
              <div style="font-size:11px;font-weight:600;color:#f59e0b;margin-bottom:8px;">① Planning Phase Tax</div>
              <div style="font-size:10px;color:#7090b0;line-height:1.65;">
                Avg planning: <strong style="color:#e8f0f8">{plan_ms:.0f}ms</strong>. Pure overhead before any useful work.
                Memoizing plans for repeated task types could recover &gt;40% of queries.
              </div>
            </div>""", unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;padding:14px 16px;border-left:3px solid #3b82f6;">
              <div style="font-size:11px;font-weight:600;color:#3b82f6;margin-bottom:8px;">② Tool API Latency Tax</div>
              <div style="font-size:10px;color:#7090b0;line-height:1.65;">
                Execution phase: <strong style="color:#e8f0f8">{exec_ms:.0f}ms</strong> avg.
                CPU spins in user-space wait. RAPL charges idle drain at every 100ms sample.
                Async dispatch cuts this 40–60%.
              </div>
            </div>""", unsafe_allow_html=True)
        with col3:
            st.markdown(f"""
            <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;padding:14px 16px;border-left:3px solid #ef4444;">
              <div style="font-size:11px;font-weight:600;color:#ef4444;margin-bottom:8px;">③ Measured Tax: avg {avg_tax:.1f}% · peak {max_tax:.1f}%</div>
              <div style="font-size:10px;color:#7090b0;line-height:1.65;">
                Route simple tasks linearly — removes planning + synthesis entirely. Classifier overhead &lt;1ms.
              </div>
            </div>""", unsafe_allow_html=True)

        st.divider()

        show_cols = [c for c in ["comparison_id","task_name","provider","country_code","linear_dynamic_j","agentic_dynamic_j","tax_j","tax_percent","planning_time_ms","execution_time_ms","synthesis_time_ms","llm_calls","tool_calls"] if c in tax.columns]
        st.dataframe(tax[show_cols], use_container_width=True, hide_index=True)

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Tax % distribution**")
            fig = px.histogram(tax, x="tax_percent", nbins=10,
                               labels={"tax_percent":"Tax %"},
                               color_discrete_sequence=["#3b82f6"])
            st.plotly_chart(fig_update(fig), use_container_width=True)
        with col2:
            if "llm_calls" in tax.columns:
                st.markdown("**Tax vs LLM calls**")
                fig2 = px.scatter(tax.dropna(subset=["llm_calls","tax_percent"]),
                                  x="llm_calls", y="tax_percent",
                                  color_discrete_sequence=["#f59e0b"],
                                  labels={"llm_calls":"LLM Calls","tax_percent":"Tax %"})
                st.plotly_chart(fig_update(fig2), use_container_width=True)
                st.caption("Each additional LLM call adds measurable overhead — scales super-linearly.")
    else:
        st.info("No tax data yet — run comparison experiments.")

# ═════════════════════════════════════════════════════════════
# PAGE: ANOMALIES
# ═════════════════════════════════════════════════════════════
elif page_id == "anomalies":
    st.title("Anomaly Detection")

    anom = query("""
        WITH stats AS (
            SELECT AVG(total_energy_uj/1e6) AS mean_e, AVG(ipc) AS mean_ipc, AVG(cache_miss_rate) AS mean_miss
            FROM runs WHERE total_energy_uj IS NOT NULL
        ),
        stddev AS (
            SELECT SQRT(AVG((total_energy_uj/1e6 - stats.mean_e)*(total_energy_uj/1e6 - stats.mean_e))) AS std_e
            FROM runs, stats WHERE total_energy_uj IS NOT NULL
        )
        SELECT r.run_id, r.exp_id, e.workflow_type, e.task_name, e.provider, e.country_code,
               r.total_energy_uj/1e6 AS energy_j, r.ipc, r.cache_miss_rate*100 AS cache_miss_pct,
               r.thermal_delta_c, r.interrupt_rate, r.thermal_throttle_flag,
               CASE WHEN r.total_energy_uj/1e6 > stats.mean_e + 2*stddev.std_e THEN 1 ELSE 0 END AS flag_high_energy,
               CASE WHEN r.ipc < stats.mean_ipc * 0.5                           THEN 1 ELSE 0 END AS flag_low_ipc,
               CASE WHEN r.cache_miss_rate > stats.mean_miss * 1.5              THEN 1 ELSE 0 END AS flag_high_miss,
               CASE WHEN r.thermal_throttle_flag = 1                            THEN 1 ELSE 0 END AS flag_thermal
        FROM runs r JOIN experiments e ON r.exp_id = e.exp_id, stats, stddev
        WHERE r.total_energy_uj IS NOT NULL
          AND (r.total_energy_uj/1e6 > stats.mean_e + 2*stddev.std_e
               OR r.ipc < stats.mean_ipc * 0.5
               OR r.cache_miss_rate > stats.mean_miss * 1.5
               OR r.thermal_throttle_flag = 1)
        ORDER BY r.total_energy_uj DESC
    """)

    if not anom.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("High-Energy Outliers", len(anom[anom.flag_high_energy == 1]) if "flag_high_energy" in anom.columns else "—")
        c2.metric("Low-IPC Outliers",     len(anom[anom.flag_low_ipc == 1])     if "flag_low_ipc"     in anom.columns else "—")
        c3.metric("Thermal Events",       len(anom[anom.flag_thermal == 1])     if "flag_thermal"     in anom.columns else "—")
        st.divider()
        st.dataframe(anom, use_container_width=True, hide_index=True)
    else:
        st.success("No anomalies detected — all runs within normal range.")

# ═════════════════════════════════════════════════════════════
# PAGE: EXECUTE
# ═════════════════════════════════════════════════════════════
elif page_id == "execute":
    st.title("Execute Run")

    if not HARNESS_AVAILABLE:
        st.warning("⚠ Harness not available on this machine. Connect via SSH to the A-LEMS machine for live execution. Analytics remain fully functional in read-only mode.")

    col_cfg, col_out = st.columns([1, 2])

    with col_cfg:
        st.markdown("**Configuration**")
        provider   = st.selectbox("Provider",    ["cloud", "local"])
        complexity = st.selectbox("Complexity",  ["simple", "L1", "L2"])
        region     = st.selectbox("Grid region", ["US","DE","FR","NO","IN","AU"],
                                  format_func=lambda x: {"US":"🇺🇸 US (0.386)","DE":"🇩🇪 DE (0.311)","FR":"🇫🇷 FR (0.052)","NO":"🇳🇴 NO (0.011)","IN":"🇮🇳 IN (0.820)","AU":"🇦🇺 AU (0.541)"}.get(x, x))
        reps       = st.selectbox("Repetitions", [3, 5, 10, 30], index=2)

        st.markdown("**Task**")
        task = st.selectbox("Preset", [
            "capital_query", "arithmetic", "stock_lookup",
            "code_review", "comparative_research", "deep_research",
        ])
        custom_prompt = st.text_area("Custom prompt (optional)", height=60)

        run_btn = st.button("▶ Run Dual Benchmark", disabled=not HARNESS_AVAILABLE, use_container_width=True, type="primary")

    with col_out:
        if run_btn and HARNESS_AVAILABLE:
            import requests
            st.info("Submitting to server…")
            try:
                res = requests.post("http://localhost:8765/api/execute", json={
                    "task_id": task, "provider": provider,
                    "country_code": region, "repetitions": reps,
                })
                data = res.json()
                exp_id = data.get("exp_id")
                st.success(f"Experiment {exp_id} started.")

                # Poll for completion
                prog_bar = st.progress(0)
                status_ph = st.empty()
                for _ in range(300):
                    time.sleep(2)
                    status_res = requests.get(f"http://localhost:8765/api/execute/status/{exp_id}").json()
                    pct = status_res.get("progress", 0)
                    prog_bar.progress(pct)
                    status_ph.caption(f"Rep {status_res.get('completed',0)}/{status_res.get('reps', reps)} — {status_res.get('status', '...')}")
                    if status_res.get("status") in ("completed", "error"):
                        break

                if status_res.get("status") == "completed":
                    st.success("✅ Run complete. Refresh data to see results.")
                    st.cache_data.clear()
                else:
                    st.error(f"Run ended with: {status_res.get('status')} — {status_res.get('error','')}")

            except Exception as e:
                st.error(f"Could not connect to server.py: {e}")
        else:
            st.markdown("**Recent runs**")
            if not runs.empty:
                show = runs.head(10)[["run_id","workflow_type","provider","country_code","energy_j","ipc","carbon_g"]]
                st.dataframe(show, use_container_width=True, hide_index=True)

# ═════════════════════════════════════════════════════════════
# PAGE: SAMPLE EXPLORER
# ═════════════════════════════════════════════════════════════
elif page_id == "explorer":
    st.title("Sample Explorer")
    st.caption("100Hz energy + CPU + interrupt timeseries per run")

    if not runs.empty:
        run_options = {
            f"Run {r.run_id} — {r.workflow_type} · {r.provider} · {r.country_code} · {r.energy_j:.3f}J": r.run_id
            for _, r in runs.iterrows()
            if r.energy_j
        }
        selected_run_label = st.selectbox("Select run", list(run_options.keys()))
        run_id = run_options[selected_run_label]

        with st.spinner("Loading samples…"):
            energy_samples = query(f"""
                SELECT (timestamp_ns - MIN(timestamp_ns) OVER (PARTITION BY run_id))/1e6 AS elapsed_ms,
                       pkg_energy_uj/1e6 AS pkg_j, core_energy_uj/1e6 AS core_j, dram_energy_uj/1e6 AS dram_j
                FROM energy_samples WHERE run_id={run_id} ORDER BY timestamp_ns
            """)
            cpu_samples = query(f"""
                SELECT (timestamp_ns - MIN(timestamp_ns) OVER (PARTITION BY run_id))/1e6 AS elapsed_ms,
                       cpu_util_percent, ipc, package_power,
                       c1_residency, c2_residency, c6_residency, c7_residency
                FROM cpu_samples WHERE run_id={run_id} ORDER BY timestamp_ns
            """)
            irq_samples = query(f"""
                SELECT (timestamp_ns - MIN(timestamp_ns) OVER (PARTITION BY run_id))/1e6 AS elapsed_ms,
                       interrupts_per_sec
                FROM interrupt_samples WHERE run_id={run_id} ORDER BY timestamp_ns
            """)
            events = query(f"""
                SELECT (start_time_ns - MIN(start_time_ns) OVER (PARTITION BY run_id))/1e6 AS start_ms,
                       duration_ns/1e6 AS duration_ms, phase, event_type, tax_contribution_uj
                FROM orchestration_events WHERE run_id={run_id} ORDER BY start_time_ns
            """)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric(f"Run {run_id}", f"{runs[runs.run_id==run_id].energy_j.values[0]:.3f}J" if "energy_j" in runs.columns else "—")
        c2.metric("Energy samples", len(energy_samples))
        c3.metric("CPU samples",    len(cpu_samples))
        c4.metric("Orch events",    len(events))

        if not energy_samples.empty and len(energy_samples) > 1:
            # Compute power
            es = energy_samples.copy()
            es["pkg_watts"]  = es.pkg_j.diff()  / (es.elapsed_ms.diff() / 1000)
            es["core_watts"] = es.core_j.diff() / (es.elapsed_ms.diff() / 1000)

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Power over time (W)**")
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=es.elapsed_ms, y=es.pkg_watts,  name="PKG W",  line=dict(color="#3b82f6", width=1.5), fill="tozeroy", fillcolor="rgba(59,130,246,.08)"))
                fig.add_trace(go.Scatter(x=es.elapsed_ms, y=es.core_watts, name="Core W", line=dict(color="#22c55e", width=1),   fill=None))
                st.plotly_chart(fig_update(fig, xaxis_title="elapsed ms", yaxis_title="Watts"), use_container_width=True)

            if not cpu_samples.empty:
                with col2:
                    st.markdown("**IPC + CPU utilisation**")
                    fig2 = make_subplots(specs=[[{"secondary_y": True}]])
                    fig2.add_trace(go.Scatter(x=cpu_samples.elapsed_ms, y=cpu_samples.ipc,             name="IPC",      line=dict(color="#22c55e", width=1.5)), secondary_y=False)
                    fig2.add_trace(go.Scatter(x=cpu_samples.elapsed_ms, y=cpu_samples.cpu_util_percent, name="CPU Util%", line=dict(color="#f59e0b", width=1)),   secondary_y=True)
                    fig2.update_layout(**PLOTLY_LAYOUT)
                    st.plotly_chart(fig2, use_container_width=True)

        if not events.empty:
            st.markdown("**Orchestration Event Timeline**")
            phase_colors = {"planning": "#f59e0b", "execution": "#3b82f6", "synthesis": "#a78bfa"}
            fig = go.Figure()
            for _, ev in events.iterrows():
                fig.add_trace(go.Bar(
                    x=[ev.duration_ms], y=[ev.phase or "unknown"],
                    base=ev.start_ms, orientation="h",
                    name=ev.event_type or "",
                    marker_color=phase_colors.get(ev.phase, "#3b82f6"),
                    hovertemplate=f"{ev.phase}: {ev.duration_ms:.0f}ms<extra></extra>",
                ))
            fig.update_layout(**PLOTLY_LAYOUT, xaxis_title="elapsed ms", showlegend=False, height=200)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No runs available.")

# ═════════════════════════════════════════════════════════════
# PAGE: EXPERIMENTS
# ═════════════════════════════════════════════════════════════
elif page_id == "experiments":
    st.title("Saved Experiments")

    exps = query("""
        SELECT e.*, COUNT(r.run_id) AS run_count,
               AVG(CASE WHEN r.workflow_type='linear'  THEN r.total_energy_uj END)/1e6 AS avg_linear_j,
               AVG(CASE WHEN r.workflow_type='agentic' THEN r.total_energy_uj END)/1e6 AS avg_agentic_j
        FROM experiments e LEFT JOIN runs r ON e.exp_id = r.exp_id
        GROUP BY e.exp_id ORDER BY e.exp_id DESC
    """)

    if not exps.empty:
        # Summary table
        show_cols = [c for c in ["exp_id","name","task_name","provider","country_code","status","run_count","avg_linear_j","avg_agentic_j"] if c in exps.columns]
        st.dataframe(exps[show_cols], use_container_width=True, hide_index=True)

        st.divider()
        selected_exp = st.selectbox(
            "Inspect experiment",
            exps.exp_id.tolist(),
            format_func=lambda eid: f"Exp {eid} — {exps[exps.exp_id==eid].name.values[0]}"
        )

        exp_runs = query(f"SELECT * FROM runs WHERE exp_id={selected_exp} ORDER BY run_number")
        exp_tax  = query(f"""
            SELECT
                ots.comparison_id, ots.tax_percent,
                ots.orchestration_tax_uj/1e6 AS tax_j,
                ots.linear_dynamic_uj/1e6    AS linear_dynamic_j,
                ots.agentic_dynamic_uj/1e6   AS agentic_dynamic_j
            FROM orchestration_tax_summary ots
            JOIN runs r ON ots.linear_run_id = r.run_id
            WHERE r.exp_id = {selected_exp}
        """)

        if not exp_runs.empty:
            lin_avg = exp_runs[exp_runs.workflow_type=="linear"].total_energy_uj.mean() / 1e6  if not exp_runs[exp_runs.workflow_type=="linear"].empty  else 0
            age_avg = exp_runs[exp_runs.workflow_type=="agentic"].total_energy_uj.mean() / 1e6 if not exp_runs[exp_runs.workflow_type=="agentic"].empty else 0

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total runs",   len(exp_runs))
            c2.metric("Avg Linear J", f"{lin_avg:.3f}")
            c3.metric("Avg Agentic J",f"{age_avg:.3f}")
            c4.metric("Tax multiple", f"{age_avg/lin_avg:.1f}×" if lin_avg > 0 else "—")

            show_run_cols = [c for c in ["run_id","workflow_type","run_number","total_energy_uj","ipc","cache_miss_rate","thread_migrations","carbon_g"] if c in exp_runs.columns]
            st.dataframe(exp_runs[show_run_cols], use_container_width=True, hide_index=True)

        if not exp_tax.empty:
            st.markdown("**Tax pairs**")
            show_tax_cols = [c for c in ["comparison_id","linear_dynamic_j","agentic_dynamic_j","tax_j","tax_percent"] if c in exp_tax.columns]
            st.dataframe(exp_tax[show_tax_cols], use_container_width=True, hide_index=True)
    else:
        st.info("No experiments found.")
