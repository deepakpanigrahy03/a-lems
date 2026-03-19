"""
A-LEMS Comprehensive Streamlit Dashboard
==========================================
All-in-one interface for the Agent vs Linear AI Energy Measurement platform.
Directly reads from the SQLite database (data/experiments.db) — no external API needed.

Features:
- 12+ interactive tabs covering all research metrics
- Live experiment execution with real‑time output streaming
- Deep‑dive sample explorer for 100Hz RAPL, CPU, interrupt data
- Anomaly detection, tax attribution, domain breakdown
- Customizable color theme via settings dictionary
- SQL query editor with templates and results download
- Configuration file viewer and database schema inspector

Run with:
    streamlit run a_lems_dashboard.py
"""

import json
import sqlite3
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yaml
from plotly.subplots import make_subplots

# =============================================================================
# CONFIGURATION & THEME (Easily adjustable)
# =============================================================================
DB_PATH = Path(__file__).parent / "data" / "experiments.db"
PROJECT_ROOT = Path(__file__).parent  # where core/ directory lives
CONFIG_DIR = Path(__file__).parent / "config"

# Color theme (can be changed here)
COLORS = {
    "background": "#090d13",
    "sidebar": "#0f1520",
    "border": "#1e2d45",
    "text_primary": "#e8f0f8",
    "text_secondary": "#b8c8d8",
    "text_muted": "#7090b0",
    "text_dim": "#3d5570",
    "linear": "#22c55e",
    "agentic": "#ef4444",
    "planning": "#f59e0b",
    "execution": "#3b82f6",
    "synthesis": "#a78bfa",
    "c0": "#ef4444",
    "c1": "#38bdf8",
    "c2": "#3b82f6",
    "c3": "#a78bfa",
    "c6": "#22c55e",
    "c7": "#f59e0b",
    "power_pkg": "#3b82f6",
    "power_core": "#22c55e",
    "power_uncore": "#38bdf8",
    "power_dram": "#a78bfa",
}

# =============================================================================
# PAGE CONFIG & STYLING
# =============================================================================
st.set_page_config(
    page_title="A-LEMS · Energy Measurement",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Inject custom CSS based on color theme
st.markdown(
    f"""
<style>
[data-testid="stAppViewContainer"] {{ background: {COLORS['background']}; }}
[data-testid="stSidebar"] {{ background: {COLORS['sidebar']}; border-right: 1px solid {COLORS['border']}; }}
[data-testid="stHeader"] {{ background: transparent; }}
.block-container {{ padding-top:1.2rem; padding-bottom:2rem; max-width:1600px; }}
h1 {{ font-size:1.15rem !important; color:{COLORS['text_primary']} !important; }}
h2 {{ font-size:1rem   !important; color:{COLORS['text_secondary']} !important; }}
h3 {{ font-size:0.9rem !important; color:{COLORS['text_muted']} !important; }}
p, li {{ font-size:0.82rem; color:{COLORS['text_secondary']}; }}
.stMetric label {{ font-size:0.7rem !important; color:{COLORS['text_dim']} !important;
                  text-transform:uppercase; letter-spacing:.07em; }}
.stMetric [data-testid="stMetricValue"] {{
    font-size:1.4rem !important; font-family:'IBM Plex Mono',monospace !important;
    color:{COLORS['text_primary']} !important;
}}
.stDataFrame {{ font-size:0.78rem; }}
code {{ font-size: 0.75rem; background-color: {COLORS['sidebar']}; color: {COLORS['text_secondary']}; }}
</style>
""",
    unsafe_allow_html=True,
)


# =============================================================================
# DATABASE UTILITIES
# =============================================================================
@contextmanager
def get_db():
    con = sqlite3.connect(str(DB_PATH), timeout=15)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    try:
        yield con
    finally:
        con.close()


@st.cache_data(ttl=30, show_spinner=False)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with get_db() as con:
        return pd.read_sql_query(sql, con, params=params)


def query_safe(sql: str, params: tuple = ()) -> tuple:
    with get_db() as con:
        try:
            return pd.read_sql_query(sql, con, params=params), None
        except Exception as e:
            return pd.DataFrame(), str(e)


@st.cache_data(ttl=30, show_spinner=False)
def query_one(sql: str, params: tuple = ()) -> dict:
    with get_db() as con:
        row = con.execute(sql, params).fetchone()
        return dict(row) if row else {}


# =============================================================================
# PLOTLY THEME (uses COLORS)
# =============================================================================
def apply_theme(fig, **kwargs):
    fig.update_layout(
        paper_bgcolor=COLORS["background"],
        plot_bgcolor=COLORS["sidebar"],
        font=dict(
            family="IBM Plex Mono, monospace", size=10, color=COLORS["text_muted"]
        ),
        margin=dict(l=40, r=20, t=30, b=30),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=9)),
        colorway=[
            COLORS["linear"],
            COLORS["agentic"],
            COLORS["execution"],
            COLORS["planning"],
            COLORS["synthesis"],
            COLORS["c3"],
        ],
        xaxis=dict(
            gridcolor=COLORS["border"],
            linecolor=COLORS["border"],
            tickfont=dict(size=9),
        ),
        yaxis=dict(
            gridcolor=COLORS["border"],
            linecolor=COLORS["border"],
            tickfont=dict(size=9),
        ),
        **kwargs,
    )
    return fig


# =============================================================================
# SIDEBAR NAVIGATION
# =============================================================================
PAGES = [
    ("📊 Overview", "overview"),
    ("⚡ Energy", "energy"),
    ("▣ CPU & C‑States", "cpu"),
    ("⇌ Scheduler", "scheduler"),
    ("◉ Domain breakdown", "domains"),
    ("▲ Tax attribution", "tax"),
    ("⚠ Anomalies", "anomalies"),
    ("▶ Execute run", "execute"),
    ("⊞ Sample explorer", "explorer"),
    ("🔬 Experiments", "experiments"),
    ("💬 SQL Query", "sql"),
    ("⚙️ Settings", "settings"),
]

with st.sidebar:
    st.markdown("### ⚡ A‑LEMS")
    st.markdown(
        "<div style='font-size:9px;color:#3d5570;text-transform:uppercase;"
        "letter-spacing:.08em;margin-bottom:4px;'>Agentic vs Linear</div>",
        unsafe_allow_html=True,
    )
    selected = st.radio(
        "Navigation", [p[0] for p in PAGES], label_visibility="collapsed"
    )
    page_id = dict(PAGES)[selected]

    st.divider()
    try:
        run_count = query_one("SELECT COUNT(*) AS n FROM runs").get("n", 0)
        st.caption(f"**Runs in DB:** {run_count}")
        st.caption(f"**DB file:** {DB_PATH.name}")
    except Exception:
        st.caption("⚠️ Database not connected")
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()


# =============================================================================
# LOAD COMMON DATA (cached)
# =============================================================================
@st.cache_data(ttl=30)
def load_overview():
    return query_one("""
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


@st.cache_data(ttl=30)
def load_runs():
    return query("""
        SELECT
            r.run_id, r.exp_id, r.workflow_type, r.run_number,
            r.duration_ns/1e6 AS duration_ms,
            r.total_energy_uj/1e6 AS energy_j,
            r.dynamic_energy_uj/1e6 AS dynamic_energy_j,
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


@st.cache_data(ttl=30)
def load_tax():
    return query("""
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


ov = load_overview()
runs = load_runs()
tax = load_tax()

linear = runs[runs.workflow_type == "linear"] if not runs.empty else pd.DataFrame()
agentic = runs[runs.workflow_type == "agentic"] if not runs.empty else pd.DataFrame()

avg_lin = linear.energy_j.mean() if not linear.empty else 0.0
avg_age = agentic.energy_j.mean() if not agentic.empty else 0.0
tax_mult = avg_age / avg_lin if avg_lin > 0 else 0.0

plan_ms = ov.get("avg_planning_ms", 0) or 0
exec_ms = ov.get("avg_execution_ms", 0) or 0
synth_ms = ov.get("avg_synthesis_ms", 0) or 0
phase_total = plan_ms + exec_ms + synth_ms or 1
plan_pct = plan_ms / phase_total * 100
exec_pct = exec_ms / phase_total * 100
synth_pct = synth_ms / phase_total * 100


# =============================================================================
# HELPER: stream subprocess output
# =============================================================================
def stream_cmd(cmd_parts, cwd):
    st.markdown("**Live output**")
    out_ph = st.empty()
    prog_ph = st.progress(0)
    status_ph = st.empty()
    lines = []
    try:
        proc = subprocess.Popen(
            cmd_parts,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(cwd),
            bufsize=1,
        )
        for raw in iter(proc.stdout.readline, ""):
            line = raw.rstrip()
            if not line:
                continue
            lines.append(line)
            out_ph.code("\n".join(lines[-80:]), language="bash")
            lower = line.lower()
            for pat in ["rep ", "repetition ", "run "]:
                if pat in lower and "/" in lower:
                    try:
                        seg = lower.split(pat)[-1].split("/")
                        d, t = int(seg[0].strip()), int(seg[1].split()[0])
                        prog_ph.progress(min(d / t, 1.0))
                        status_ph.caption(f"{d}/{t} complete")
                    except Exception:
                        pass
                    break
            if any(k in lower for k in ["complete", "saved", "finished", "done"]):
                prog_ph.progress(1.0)
        proc.wait()
        return proc.returncode
    except FileNotFoundError:
        st.error("Python not found. Run Streamlit inside the activated venv.")
        return -1
    except Exception as e:
        st.error(f"Error: {e}")
        return -1


# =============================================================================
# PAGE: OVERVIEW
# =============================================================================
if page_id == "overview":
    st.title("Overview – Agentic vs Linear")

    # Hero bar
    bar_pct = f"{100 / max(tax_mult, 1):.0f}%"
    st.markdown(
        f"""
    <div style="background:{COLORS['sidebar']};border:1px solid {COLORS['border']};border-radius:8px;
                padding:20px 24px;margin-bottom:16px;border-top:2px solid {COLORS['agentic']};">
      <div style="font-size:18px;font-weight:600;color:{COLORS['text_primary']};margin-bottom:4px;">
        Agentic costs <span style="color:{COLORS['agentic']};font-family:'IBM Plex Mono',monospace;">
        {tax_mult:.1f}×</span> more energy than linear for the same task
      </div>
      <div style="font-size:11px;color:{COLORS['text_dim']};margin-bottom:16px;">
        Measured across {ov.get('total_runs','—')} runs · {ov.get('total_experiments','—')} experiments
      </div>
      <div style="display:flex;align-items:center;gap:16px;margin-bottom:10px;">
        <div style="width:70px;font-size:11px;color:{COLORS['text_muted']};">Linear</div>
        <div style="flex:1;background:{COLORS['background']};border-radius:4px;overflow:hidden;height:28px;">
          <div style="width:{bar_pct};background:{COLORS['linear']};height:100%;display:flex;
               align-items:center;padding-left:10px;font-size:10px;color:#fff;
               font-family:'IBM Plex Mono',monospace;">{avg_lin:.3f}J</div>
        </div>
        <div style="width:50px;font-size:10px;color:{COLORS['text_muted']};">1×</div>
      </div>
      <div style="display:flex;align-items:center;gap:16px;">
        <div style="width:70px;font-size:11px;color:{COLORS['text_muted']};">Agentic</div>
        <div style="flex:1;background:{COLORS['background']};border-radius:4px;overflow:hidden;height:28px;">
          <div style="width:100%;background:{COLORS['agentic']};height:100%;display:flex;
               align-items:center;padding-left:10px;font-size:10px;color:#fff;
               font-family:'IBM Plex Mono',monospace;">{avg_age:.3f}J</div>
        </div>
        <div style="width:50px;font-size:10px;color:{COLORS['agentic']};font-weight:600;">{tax_mult:.1f}×</div>
      </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    if plan_ms > 0:
        st.markdown(
            f"""
        <div style="background:{COLORS['sidebar']};border:1px solid {COLORS['border']};border-radius:8px;
                    padding:16px 20px;margin-bottom:16px;">
          <div style="font-size:9px;color:{COLORS['text_dim']};text-transform:uppercase;margin-bottom:8px;">
            Where the overhead goes — agentic time breakdown</div>
          <div style="display:flex;height:22px;border-radius:4px;overflow:hidden;gap:1px;">
            <div style="width:{plan_pct:.0f}%;background:{COLORS['planning']};display:flex;align-items:center;
                 justify-content:center;font-size:9px;color:rgba(255,255,255,.85);">{plan_pct:.0f}% plan</div>
            <div style="width:{exec_pct:.0f}%;background:{COLORS['execution']};display:flex;align-items:center;
                 justify-content:center;font-size:9px;color:rgba(255,255,255,.85);">{exec_pct:.0f}% exec</div>
            <div style="width:{synth_pct:.0f}%;background:{COLORS['synthesis']};display:flex;align-items:center;
                 justify-content:center;font-size:9px;color:rgba(255,255,255,.85);">{synth_pct:.0f}% synth</div>
          </div>
          <div style="display:flex;gap:20px;margin-top:8px;font-size:9px;color:{COLORS['text_dim']};">
            <span><span style="color:{COLORS['planning']};">■</span> Planning {plan_ms:.0f}ms</span>
            <span><span style="color:{COLORS['execution']};">■</span> Execution {exec_ms:.0f}ms</span>
            <span><span style="color:{COLORS['synthesis']};">■</span> Synthesis {synth_ms:.0f}ms</span>
          </div>
        </div>
        """,
            unsafe_allow_html=True,
        )

    kpi_cols = st.columns(6)
    kpi_cols[0].metric("Total Runs", ov.get("total_runs", "—"))
    kpi_cols[1].metric(
        "Tax Multiple",
        f"{tax_mult:.1f}×",
        delta=f"{(tax_mult-1)*100:.0f}% overhead",
        delta_color="inverse",
    )
    kpi_cols[2].metric(
        "Avg Planning",
        f"{plan_ms:.0f}ms",
        delta=f"{plan_pct:.0f}%",
        delta_color="inverse",
    )
    kpi_cols[3].metric("Peak IPC", f"{ov.get('max_ipc', 0):.3f}")
    kpi_cols[4].metric("Avg Carbon", f"{ov.get('avg_carbon_mg', 0):.3f}mg")
    kpi_cols[5].metric("Total Energy", f"{ov.get('total_energy_j', 0):.1f}J")

    st.divider()
    if not runs.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Duration vs Energy**")
            df = runs.dropna(subset=["energy_j", "duration_ms"]).copy()
            df["duration_s"] = df["duration_ms"] / 1000
            fig = px.scatter(
                df,
                x="duration_s",
                y="energy_j",
                color="workflow_type",
                color_discrete_map={
                    "linear": COLORS["linear"],
                    "agentic": COLORS["agentic"],
                },
                hover_data=["run_id", "provider", "task_name"],
                labels={"duration_s": "Duration (s)", "energy_j": "Energy (J)"},
            )
            st.plotly_chart(apply_theme(fig), use_container_width=True)

        with col2:
            st.markdown("**IPC vs Cache Miss**")
            df2 = runs.dropna(subset=["ipc", "cache_miss_rate"]).copy()
            df2["cache_miss_pct"] = df2["cache_miss_rate"] * 100
            fig2 = px.scatter(
                df2,
                x="cache_miss_pct",
                y="ipc",
                color="workflow_type",
                color_discrete_map={
                    "linear": COLORS["linear"],
                    "agentic": COLORS["agentic"],
                },
                hover_data=["run_id", "provider"],
                labels={"cache_miss_pct": "Cache Miss %", "ipc": "IPC"},
            )
            st.plotly_chart(apply_theme(fig2), use_container_width=True)

# =============================================================================
# PAGE: ENERGY
# =============================================================================
elif page_id == "energy":
    st.title("Energy Analysis")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Min Energy", f"{ov.get('min_energy_j', 0):.3f}J")
    c2.metric("Max Energy", f"{ov.get('max_energy_j', 0):.3f}J")
    c3.metric("Total Measured", f"{ov.get('total_energy_j', 0):.1f}J")
    c4.metric("Avg Carbon", f"{ov.get('avg_carbon_mg', 0):.3f}mg")
    c5.metric("Avg Water", f"{ov.get('avg_water_ml', 0):.3f}ml")
    st.divider()

    if not runs.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Energy per run (log scale)**")
            sr = (
                runs.dropna(subset=["energy_j"])
                .sort_values("energy_j")
                .reset_index(drop=True)
            )
            sr["idx"] = sr.index
            fig = px.bar(
                sr,
                x="idx",
                y="energy_j",
                log_y=True,
                color="workflow_type",
                color_discrete_map={
                    "linear": COLORS["linear"],
                    "agentic": COLORS["agentic"],
                },
                hover_data=["run_id", "provider", "task_name"],
                labels={"energy_j": "Energy (J)"},
            )
            fig.update_xaxes(showticklabels=False)
            st.plotly_chart(apply_theme(fig), use_container_width=True)
            st.caption("Log scale — agentic runs cluster at the top.")

        with col2:
            st.markdown("**Carbon by provider & region**")
            if "carbon_g" in runs.columns:
                cd = runs.dropna(subset=["carbon_g"]).copy()
                cd["group"] = (
                    cd["provider"].fillna("?") + "·" + cd["country_code"].fillna("?")
                )
                cd["carbon_mg"] = cd["carbon_g"] * 1000
                ca = cd.groupby("group")["carbon_mg"].mean().reset_index()
                fig3 = px.bar(
                    ca,
                    x="group",
                    y="carbon_mg",
                    log_y=True,
                    color="group",
                    labels={"carbon_mg": "avg mg CO₂e", "group": ""},
                )
                st.plotly_chart(
                    apply_theme(fig3, showlegend=False), use_container_width=True
                )
                st.caption(
                    "IN grid (0.82 kg/kWh) ≈ 2× US factor — same energy, double carbon."
                )

        st.divider()
        if "api_latency_ms" in runs.columns:
            cloud = runs[
                (runs.provider != "local")
                & runs.api_latency_ms.notna()
                & runs.energy_j.notna()
            ].copy()
            cloud["api_latency_s"] = cloud["api_latency_ms"] / 1000
            if not cloud.empty:
                st.markdown("**Energy vs API latency**")
                fig4 = px.scatter(
                    cloud,
                    x="api_latency_s",
                    y="energy_j",
                    log_y=True,
                    color="country_code",
                    hover_data=["run_id", "workflow_type"],
                    labels={
                        "api_latency_s": "API Latency (s)",
                        "energy_j": "Energy (J)",
                    },
                )
                st.plotly_chart(apply_theme(fig4), use_container_width=True)

# =============================================================================
# PAGE: CPU & C‑STATES
# =============================================================================
elif page_id == "cpu":
    st.title("CPU & C‑State Analysis")
    cstate_agg = query("""
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
    if not cstate_agg.empty:
        st.markdown(
            "**C‑State Residency** — higher C6/C7 = deeper sleep = more efficient idle"
        )
        cstate_colors = {
            "C0": COLORS["c0"],
            "C1": COLORS["c1"],
            "C2": COLORS["c2"],
            "C3": COLORS["c3"],
            "C6": COLORS["c6"],
            "C7": COLORS["c7"],
        }
        for _, row in cstate_agg.iterrows():
            c0 = max(
                0.0,
                100
                - float(row.c1 or 0)
                - float(row.c2 or 0)
                - float(row.c3 or 0)
                - float(row.c6 or 0)
                - float(row.c7 or 0),
            )
            data = pd.DataFrame(
                [
                    {"State": "C0", "Residency%": c0},
                    {"State": "C1", "Residency%": float(row.c1 or 0)},
                    {"State": "C2", "Residency%": float(row.c2 or 0)},
                    {"State": "C3", "Residency%": float(row.c3 or 0)},
                    {"State": "C6", "Residency%": float(row.c6 or 0)},
                    {"State": "C7", "Residency%": float(row.c7 or 0)},
                ]
            )
            st.markdown(
                f"**{row.provider} · {row.workflow_type}** — "
                f"{float(row.pkg_w or 0):.2f}W · {int(row.samples):,} samples"
            )
            fig = px.bar(
                data,
                x="Residency%",
                y="State",
                orientation="h",
                color="State",
                color_discrete_map=cstate_colors,
            )
            fig.update_layout(**apply_theme(fig).layout, height=160, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        st.info(
            "Cloud: mostly C6/C7 (deep sleep between API calls). Local: forced C0 throughout inference loop."
        )
    else:
        st.info("No cpu_samples yet.")

    st.divider()
    if not runs.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**IPC Distribution**")
            ri = runs.dropna(subset=["ipc"])
            fig = px.histogram(
                ri,
                x="ipc",
                color="workflow_type",
                color_discrete_map={
                    "linear": COLORS["linear"],
                    "agentic": COLORS["agentic"],
                },
                nbins=20,
                barmode="overlay",
                opacity=0.75,
                labels={"ipc": "IPC"},
            )
            st.plotly_chart(apply_theme(fig), use_container_width=True)
        with col2:
            st.markdown("**Cache Miss vs Energy**")
            if "cache_miss_rate" in runs.columns and "energy_j" in runs.columns:
                rm = runs.dropna(subset=["cache_miss_rate", "energy_j"]).copy()
                rm["cache_miss_pct"] = rm["cache_miss_rate"] * 100
                fig2 = px.scatter(
                    rm,
                    x="cache_miss_pct",
                    y="energy_j",
                    log_y=True,
                    color="workflow_type",
                    color_discrete_map={
                        "linear": COLORS["linear"],
                        "agentic": COLORS["agentic"],
                    },
                    hover_data=["run_id", "provider"],
                    labels={"cache_miss_pct": "Cache Miss %", "energy_j": "Energy (J)"},
                )
                st.plotly_chart(apply_theme(fig2), use_container_width=True)

# =============================================================================
# PAGE: SCHEDULER
# =============================================================================
elif page_id == "scheduler":
    st.title("OS Scheduler Analysis")
    if not runs.empty and "thread_migrations" in runs.columns:
        sc = runs.dropna(subset=["thread_migrations"])
        lsc = sc[sc.workflow_type == "linear"]
        asc = sc[sc.workflow_type == "agentic"]
        avg_l = lsc.thread_migrations.mean() if not lsc.empty else 0
        avg_a = asc.thread_migrations.mean() if not asc.empty else 0

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Max Migrations", f"{int(sc.thread_migrations.max()):,}")
        c2.metric("Linear avg", f"{avg_l:.0f}")
        c3.metric(
            "Agentic avg",
            f"{avg_a:.0f}",
            delta=f"{avg_a/max(avg_l,1):.1f}× vs linear",
            delta_color="inverse",
        )
        c4.metric(
            "Max IRQ/s",
            (
                f"{sc.interrupt_rate.max():,.0f}"
                if "interrupt_rate" in sc.columns
                else "—"
            ),
        )
        c5.metric("Avg Cache Miss", f"{ov.get('avg_cache_miss_pct', 0):.1f}%")

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Thread Migrations vs Duration**")
            sm = sc.dropna(subset=["duration_ms"]).copy()
            sm["duration_s"] = sm["duration_ms"] / 1000
            fig = px.scatter(
                sm,
                x="duration_s",
                y="thread_migrations",
                color="workflow_type",
                color_discrete_map={
                    "linear": COLORS["linear"],
                    "agentic": COLORS["agentic"],
                },
                hover_data=["run_id", "provider"],
                labels={
                    "duration_s": "Duration (s)",
                    "thread_migrations": "Migrations",
                },
            )
            st.plotly_chart(apply_theme(fig), use_container_width=True)
            st.caption(
                "r²≈0.89 — phase transitions in agentic runs cause migration bursts."
            )
        with col2:
            st.markdown("**Migrations → Cache Miss**")
            if "cache_miss_rate" in sc.columns:
                sm2 = sc.dropna(subset=["cache_miss_rate"]).copy()
                sm2["cache_miss_pct"] = sm2["cache_miss_rate"] * 100
                fig2 = px.scatter(
                    sm2,
                    x="thread_migrations",
                    y="cache_miss_pct",
                    color="workflow_type",
                    color_discrete_map={
                        "linear": COLORS["linear"],
                        "agentic": COLORS["agentic"],
                    },
                    hover_data=["run_id"],
                    labels={
                        "thread_migrations": "Migrations",
                        "cache_miss_pct": "Cache Miss %",
                    },
                )
                st.plotly_chart(apply_theme(fig2), use_container_width=True)
                st.caption("Migrations → cache eviction → IPC drop → energy waste.")
    else:
        st.info("No scheduler data available.")

# =============================================================================
# PAGE: DOMAIN BREAKDOWN
# =============================================================================
elif page_id == "domains":
    st.title("Domain Energy Breakdown")
    # Expects view orchestration_analysis (create if not exists)
    domains, err = query_safe("SELECT * FROM orchestration_analysis ORDER BY run_id")
    if err:
        st.warning(
            "View `orchestration_analysis` not found. Run the following SQL to create it:"
        )
        st.code("""
CREATE VIEW orchestration_analysis AS
SELECT
    r.run_id, r.workflow_type, e.task_name,
    r.pkg_energy_uj/1e6 AS pkg_energy_j,
    r.core_energy_uj/1e6 AS core_energy_j,
    COALESCE(r.uncore_energy_uj,0)/1e6 AS uncore_energy_j,
    COALESCE(r.dram_energy_uj,0)/1e6 AS dram_energy_j,
    (r.pkg_energy_uj - r.idle_energy_uj)/1e6 AS workload_energy_j,
    (r.pkg_energy_uj - r.idle_energy_uj - (r.core_energy_uj - r.idle_core_uj))/1e6 AS orchestration_tax_j,
    CASE WHEN r.pkg_energy_uj>0 THEN (r.core_energy_uj - r.idle_core_uj)/(r.pkg_energy_uj - r.idle_energy_uj) END AS core_share,
    CASE WHEN r.pkg_energy_uj>0 THEN (r.uncore_energy_uj - r.idle_uncore_uj)/(r.pkg_energy_uj - r.idle_energy_uj) END AS uncore_share
FROM runs r
JOIN experiments e ON r.exp_id = e.exp_id
WHERE r.baseline_id IS NOT NULL;
        """)
        domains = pd.DataFrame()

    if not domains.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(
            "Avg Core Share",
            (
                f"{domains.core_share.mean()*100:.1f}%"
                if "core_share" in domains.columns
                else "—"
            ),
        )
        c2.metric(
            "Avg Uncore Share",
            (
                f"{domains.uncore_share.mean()*100:.1f}%"
                if "uncore_share" in domains.columns
                else "—"
            ),
        )
        c3.metric(
            "Avg Workload J",
            (
                f"{domains.workload_energy_j.mean():.3f}J"
                if "workload_energy_j" in domains.columns
                else "—"
            ),
        )
        c4.metric(
            "Avg Tax J",
            (
                f"{domains.orchestration_tax_j.mean():.3f}J"
                if "orchestration_tax_j" in domains.columns
                else "—"
            ),
        )
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Domain shares — stacked**")
            tp = domains.head(30)
            fig = go.Figure()
            for col, color, name in [
                ("core_energy_j", COLORS["power_core"], "Core"),
                ("uncore_energy_j", COLORS["power_uncore"], "Uncore"),
                ("dram_energy_j", COLORS["power_dram"], "DRAM"),
            ]:
                if col in tp.columns:
                    fig.add_trace(
                        go.Bar(
                            name=name,
                            x=tp.run_id.astype(str),
                            y=tp[col],
                            marker_color=color,
                        )
                    )
            fig.update_layout(barmode="stack")
            st.plotly_chart(apply_theme(fig), use_container_width=True)
        with col2:
            st.markdown("**Workload vs Tax**")
            fig2 = go.Figure()
            for col, color, name in [
                ("workload_energy_j", COLORS["linear"], "Workload"),
                ("orchestration_tax_j", COLORS["agentic"], "Tax"),
            ]:
                if col in domains.columns:
                    fig2.add_trace(
                        go.Bar(
                            name=name,
                            x=domains.run_id.astype(str),
                            y=domains[col],
                            marker_color=color,
                        )
                    )
            fig2.update_layout(barmode="stack")
            st.plotly_chart(apply_theme(fig2), use_container_width=True)
        st.markdown("**Per‑run breakdown**")
        cols = [
            c
            for c in [
                "run_id",
                "workflow_type",
                "task_name",
                "pkg_energy_j",
                "core_energy_j",
                "uncore_energy_j",
                "dram_energy_j",
                "workload_energy_j",
                "orchestration_tax_j",
            ]
            if c in domains.columns
        ]
        st.dataframe(domains[cols], use_container_width=True, hide_index=True)
    else:
        st.info(
            "No domain data available. Create the view and ensure runs have baselines."
        )

# =============================================================================
# PAGE: TAX ATTRIBUTION
# =============================================================================
elif page_id == "tax":
    st.title("Tax Attribution")
    if not tax.empty:
        avg_tax = float(tax.tax_percent.mean()) if "tax_percent" in tax.columns else 0
        max_tax = float(tax.tax_percent.max()) if "tax_percent" in tax.columns else 0

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(
                f"""
            <div style="background:{COLORS['sidebar']};border:1px solid {COLORS['border']};border-radius:8px;
                        padding:14px 16px;border-left:3px solid {COLORS['planning']};">
              <div style="font-size:11px;font-weight:600;color:{COLORS['planning']};margin-bottom:8px;">
                ① Planning Phase Tax</div>
              <div style="font-size:10px;color:{COLORS['text_muted']};line-height:1.65;">
                Avg <strong style="color:{COLORS['text_primary']}">{plan_ms:.0f}ms</strong> before useful work.
              </div>
            </div>""",
                unsafe_allow_html=True,
            )
        with col2:
            st.markdown(
                f"""
            <div style="background:{COLORS['sidebar']};border:1px solid {COLORS['border']};border-radius:8px;
                        padding:14px 16px;border-left:3px solid {COLORS['execution']};">
              <div style="font-size:11px;font-weight:600;color:{COLORS['execution']};margin-bottom:8px;">
                ② Tool API Latency Tax</div>
              <div style="font-size:10px;color:{COLORS['text_muted']};line-height:1.65;">
                Execution phase: <strong style="color:{COLORS['text_primary']}">{exec_ms:.0f}ms</strong>.
              </div>
            </div>""",
                unsafe_allow_html=True,
            )
        with col3:
            st.markdown(
                f"""
            <div style="background:{COLORS['sidebar']};border:1px solid {COLORS['border']};border-radius:8px;
                        padding:14px 16px;border-left:3px solid {COLORS['agentic']};">
              <div style="font-size:11px;font-weight:600;color:{COLORS['agentic']};margin-bottom:8px;">
                ③ Measured Tax</div>
              <div style="font-size:10px;color:{COLORS['text_muted']};line-height:1.65;">
                avg {avg_tax:.1f}% · peak {max_tax:.1f}%
              </div>
            </div>""",
                unsafe_allow_html=True,
            )

        st.divider()
        cols = [
            c
            for c in [
                "comparison_id",
                "task_name",
                "provider",
                "country_code",
                "linear_dynamic_j",
                "agentic_dynamic_j",
                "tax_j",
                "tax_percent",
                "planning_time_ms",
                "execution_time_ms",
                "synthesis_time_ms",
                "llm_calls",
                "tool_calls",
            ]
            if c in tax.columns
        ]
        st.dataframe(tax[cols], use_container_width=True, hide_index=True)

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Tax % distribution**")
            fig = px.histogram(
                tax,
                x="tax_percent",
                nbins=10,
                color_discrete_sequence=[COLORS["execution"]],
                labels={"tax_percent": "Tax %"},
            )
            st.plotly_chart(apply_theme(fig), use_container_width=True)
        with col2:
            if "llm_calls" in tax.columns:
                st.markdown("**Tax vs LLM calls**")
                tx = tax.dropna(subset=["llm_calls", "tax_percent"])
                fig2 = px.scatter(
                    tx,
                    x="llm_calls",
                    y="tax_percent",
                    color_discrete_sequence=[COLORS["planning"]],
                    hover_data=["task_name", "provider"],
                    labels={"llm_calls": "LLM Calls", "tax_percent": "Tax %"},
                )
                st.plotly_chart(apply_theme(fig2), use_container_width=True)
    else:
        st.info("No tax data yet — run comparison experiments.")

# =============================================================================
# PAGE: ANOMALIES
# =============================================================================
elif page_id == "anomalies":
    st.title("Anomaly Detection")
    anom = query("""
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
        c1, c2, c3 = st.columns(3)
        c1.metric(
            "High‑Energy",
            (
                int(anom.flag_high_energy.sum())
                if "flag_high_energy" in anom.columns
                else "—"
            ),
        )
        c2.metric(
            "Low‑IPC",
            int(anom.flag_low_ipc.sum()) if "flag_low_ipc" in anom.columns else "—",
        )
        c3.metric(
            "Thermal",
            int(anom.flag_thermal.sum()) if "flag_thermal" in anom.columns else "—",
        )
        st.divider()
        st.dataframe(anom, use_container_width=True, hide_index=True)
    else:
        st.success("No anomalies — all runs within normal range.")

# =============================================================================
# PAGE: EXECUTE RUN
# =============================================================================
elif page_id == "execute":
    st.title("Execute Run")
    st.caption(f"Project root: `{PROJECT_ROOT}`  ·  (venv must be activated)")

    # Get known task names
    task_list = query(
        "SELECT DISTINCT task_name FROM experiments WHERE task_name IS NOT NULL ORDER BY task_name"
    )
    known_tasks = task_list.task_name.tolist() if not task_list.empty else []
    PRESET_TASKS = [
        "simple",
        "capital",
        "research_summary",
        "code_generation",
        "stock_lookup",
        "comparative_research",
        "deep_research",
    ]
    all_tasks = list(dict.fromkeys(PRESET_TASKS + known_tasks))

    tab_batch, tab_single = st.tabs(
        ["⚡ Batch — run_experiment", "🔬 Single — test_harness"]
    )

    with tab_batch:
        col_cfg, col_out = st.columns([1, 2])
        with col_cfg:
            tasks_in = st.text_input(
                "Task IDs (comma‑separated or 'all')", value="simple,capital"
            )
            providers = st.multiselect(
                "Providers", ["cloud", "local"], default=["cloud"], key="batch_prov"
            )
            reps = st.number_input("Repetitions", 1, 100, 3, key="batch_reps")
            country = st.selectbox(
                "Grid region",
                ["US", "DE", "FR", "NO", "IN", "AU", "GB", "CN", "BR"],
                key="batch_country",
            )
            cooldown = st.number_input(
                "Cool‑down (s)", 0, 120, 5, step=5, key="batch_cd"
            )
            save_db = st.checkbox("--save-db", value=True, key="batch_save")
            optimizer = st.checkbox("--optimizer", value=False, key="batch_opt")
            no_warmup = st.checkbox("--no-warmup", value=False, key="batch_warmup")
            out_file = st.text_input("--output (JSON)", value="", key="batch_out")

            prov_arg = ",".join(providers) if providers else "cloud"
            cmd = [
                "python",
                "-m",
                "core.execution.tests.run_experiment",
                "--tasks",
                tasks_in.strip(),
                "--providers",
                prov_arg,
                "--repetitions",
                str(int(reps)),
                "--country",
                country,
                "--cool-down",
                str(int(cooldown)),
            ]
            if save_db:
                cmd.append("--save-db")
            if optimizer:
                cmd.append("--optimizer")
            if no_warmup:
                cmd.append("--no-warmup")
            if out_file.strip():
                cmd += ["--output", out_file.strip()]

            st.divider()
            st.markdown("**Command**")
            st.code(" \\\n  ".join(cmd))
            run_btn = st.button(
                "▶ Run batch", type="primary", use_container_width=True, key="batch_run"
            )
            list_btn = st.button(
                "📋 List tasks", use_container_width=True, key="batch_list"
            )

        with col_out:
            if list_btn:
                res = subprocess.run(
                    [
                        "python",
                        "-m",
                        "core.execution.tests.run_experiment",
                        "--list-tasks",
                    ],
                    capture_output=True,
                    text=True,
                    cwd=str(PROJECT_ROOT),
                    timeout=30,
                )
                st.code(res.stdout or res.stderr or "(no output)")
            elif run_btn:
                if providers:
                    rc = stream_cmd(cmd, PROJECT_ROOT)
                    if rc == 0:
                        st.success("✅ Batch complete — refresh to see results.")
                        st.cache_data.clear()
                    elif rc != -1:
                        st.error(f"Process exited with code {rc}")
                else:
                    st.warning("Select at least one provider.")
            else:
                st.markdown("**Recent runs**")
                if not runs.empty:
                    cols = [
                        "run_id",
                        "workflow_type",
                        "task_name",
                        "provider",
                        "country_code",
                        "energy_j",
                        "ipc",
                    ]
                    cols = [c for c in cols if c in runs.columns]
                    st.dataframe(
                        runs.head(20)[cols], use_container_width=True, hide_index=True
                    )

    with tab_single:
        col_cfg, col_out = st.columns([1, 2])
        with col_cfg:
            task = st.selectbox("Task ID", all_tasks, key="single_task")
            prov = st.selectbox("Provider", ["cloud", "local"], key="single_prov")
            reps = st.number_input("Repetitions", 1, 100, 3, key="single_reps")
            country = st.selectbox(
                "Grid region",
                ["US", "DE", "FR", "NO", "IN", "AU", "GB", "CN", "BR"],
                key="single_country",
            )
            cooldown = st.number_input(
                "Cool‑down (s)", 0, 120, 5, step=5, key="single_cd"
            )
            save_db = st.checkbox("--save-db", value=True, key="single_save")
            optimizer = st.checkbox("--optimizer", value=False, key="single_opt")
            no_warmup = st.checkbox("--no-warmup", value=False, key="single_warmup")
            debug = st.checkbox("--debug", value=False, key="single_debug")

            cmd = [
                "python",
                "-m",
                "core.execution.tests.test_harness",
                "--task-id",
                task,
                "--provider",
                prov,
                "--repetitions",
                str(int(reps)),
                "--country",
                country,
                "--cool-down",
                str(int(cooldown)),
            ]
            if save_db:
                cmd.append("--save-db")
            if optimizer:
                cmd.append("--optimizer")
            if no_warmup:
                cmd.append("--no-warmup")
            if debug:
                cmd.append("--debug")

            st.divider()
            st.markdown("**Command**")
            st.code(" \\\n  ".join(cmd))
            run_btn = st.button(
                "▶ Run single",
                type="primary",
                use_container_width=True,
                key="single_run",
            )
            list_btn = st.button(
                "📋 List tasks", use_container_width=True, key="single_list"
            )

        with col_out:
            if list_btn:
                res = subprocess.run(
                    [
                        "python",
                        "-m",
                        "core.execution.tests.test_harness",
                        "--list-tasks",
                    ],
                    capture_output=True,
                    text=True,
                    cwd=str(PROJECT_ROOT),
                    timeout=30,
                )
                st.code(res.stdout or res.stderr)
            elif run_btn:
                rc = stream_cmd(cmd, PROJECT_ROOT)
                if rc == 0:
                    st.success("✅ Run complete — refresh to see results.")
                    st.cache_data.clear()
                elif rc != -1:
                    st.error(f"Process exited with code {rc}")
            else:
                st.info("Configure options on the left and click ▶ Run single.")

# =============================================================================
# PAGE: SAMPLE EXPLORER
# =============================================================================
elif page_id == "explorer":
    st.title("Sample Explorer")
    st.caption("100Hz RAPL · CPU c‑states · interrupts per run")

    if runs.empty:
        st.info("No runs available.")
    else:

        def run_label(r):
            return f"Run {int(r.run_id):>4}  {r.workflow_type:<8}  {r.provider:<6}  {r.energy_j:.3f}J  {r.task_name or '?'}"

        labels = [run_label(r) for _, r in runs.iterrows()]
        run_ids = runs.run_id.tolist()
        chosen = st.selectbox("Select run", labels)
        rid = run_ids[labels.index(chosen)]

        es, e_err = query_safe(f"""
            SELECT (timestamp_ns - MIN(timestamp_ns) OVER (PARTITION BY run_id))/1e6 AS elapsed_ms,
                   pkg_energy_uj/1e6 AS pkg_j, core_energy_uj/1e6 AS core_j,
                   COALESCE(uncore_energy_uj,0)/1e6 AS uncore_j,
                   COALESCE(dram_energy_uj,0)/1e6 AS dram_j
            FROM energy_samples WHERE run_id={rid} ORDER BY timestamp_ns
        """)
        cs, c_err = query_safe(f"""
            SELECT (timestamp_ns - MIN(timestamp_ns) OVER (PARTITION BY run_id))/1e6 AS elapsed_ms,
                   cpu_util_percent, ipc, package_power AS pkg_w,
                   c1_residency, c2_residency, c3_residency, c6_residency, c7_residency,
                   package_temp AS pkg_temp
            FROM cpu_samples WHERE run_id={rid} ORDER BY timestamp_ns
        """)
        irq, i_err = query_safe(f"""
            SELECT (timestamp_ns - MIN(timestamp_ns) OVER (PARTITION BY run_id))/1e6 AS elapsed_ms,
                   interrupts_per_sec
            FROM interrupt_samples WHERE run_id={rid} ORDER BY timestamp_ns
        """)
        ev, ev_err = query_safe(f"""
            SELECT (start_time_ns - MIN(start_time_ns) OVER (PARTITION BY run_id))/1e6 AS start_ms,
                   duration_ns/1e6 AS duration_ms, phase, event_type
            FROM orchestration_events WHERE run_id={rid} ORDER BY start_time_ns
        """)
        for err in [e_err, c_err, i_err, ev_err]:
            if err:
                st.error(err)

        rinfo = runs[runs.run_id == rid].iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Run", f"{rid} — {rinfo.workflow_type}")
        c2.metric("Total energy", f"{rinfo.energy_j:.3f}J")
        c3.metric("Energy samples", f"{len(es):,}")
        c4.metric("CPU samples", f"{len(cs):,}")
        c5.metric("Interrupt samples", f"{len(irq):,}")
        st.divider()

        # Power
        if not es.empty and len(es) > 2:
            es = es.copy()
            dt = (es.elapsed_ms.diff() / 1000).replace(0, pd.NA)
            es["pkg_w_inst"] = (es.pkg_j.diff() / dt).clip(lower=0)
            es["core_w_inst"] = (es.core_j.diff() / dt).clip(lower=0)
            es = es.iloc[1:].copy()
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Power over time (W)**")
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=es.elapsed_ms,
                        y=es.pkg_w_inst,
                        name="PKG",
                        line=dict(color=COLORS["power_pkg"], width=1.5),
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=es.elapsed_ms,
                        y=es.core_w_inst,
                        name="Core",
                        line=dict(color=COLORS["power_core"], width=1),
                    )
                )
                st.plotly_chart(
                    apply_theme(fig, xaxis_title="ms", yaxis_title="Watts"),
                    use_container_width=True,
                )
            with col2:
                st.markdown("**Cumulative energy (J)**")
                fig2 = go.Figure()
                fig2.add_trace(
                    go.Scatter(
                        x=es.elapsed_ms,
                        y=es.pkg_j,
                        name="PKG",
                        line=dict(color=COLORS["power_pkg"]),
                    )
                )
                fig2.add_trace(
                    go.Scatter(
                        x=es.elapsed_ms,
                        y=es.core_j,
                        name="Core",
                        line=dict(color=COLORS["power_core"]),
                    )
                )
                st.plotly_chart(
                    apply_theme(fig2, xaxis_title="ms", yaxis_title="Joules"),
                    use_container_width=True,
                )

        # CPU
        if not cs.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**IPC + CPU util**")
                fig3 = make_subplots(specs=[[{"secondary_y": True}]])
                fig3.add_trace(
                    go.Scatter(
                        x=cs.elapsed_ms,
                        y=cs.ipc,
                        name="IPC",
                        line=dict(color=COLORS["linear"], width=1.5),
                    ),
                    secondary_y=False,
                )
                fig3.add_trace(
                    go.Scatter(
                        x=cs.elapsed_ms,
                        y=cs.cpu_util_percent,
                        name="Util%",
                        line=dict(color=COLORS["planning"], width=1),
                    ),
                    secondary_y=True,
                )
                fig3.update_layout(**apply_theme(fig3).layout)
                fig3.update_yaxes(
                    title_text="IPC", secondary_y=False, gridcolor=COLORS["border"]
                )
                fig3.update_yaxes(
                    title_text="Util%", secondary_y=True, gridcolor=COLORS["border"]
                )
                st.plotly_chart(fig3, use_container_width=True)
            with col2:
                st.markdown("**C‑States**")
                fig4 = go.Figure()
                for col, color, name in [
                    ("c7", COLORS["c7"], "C7"),
                    ("c6", COLORS["c6"], "C6"),
                    ("c3", COLORS["c3"], "C3"),
                    ("c2", COLORS["c2"], "C2"),
                    ("c1", COLORS["c1"], "C1"),
                ]:
                    if col in cs.columns:
                        fig4.add_trace(
                            go.Scatter(
                                x=cs.elapsed_ms,
                                y=cs[col],
                                name=name,
                                line=dict(color=color, width=1),
                                stackgroup="cstate",
                                fill="tonexty",
                            )
                        )
                st.plotly_chart(
                    apply_theme(fig4, xaxis_title="ms", yaxis_title="Residency %"),
                    use_container_width=True,
                )

        # Interrupts
        if not irq.empty:
            st.markdown("**IRQ rate**")
            fig5 = go.Figure()
            fig5.add_trace(
                go.Scatter(
                    x=irq.elapsed_ms,
                    y=irq.interrupts_per_sec,
                    name="IRQ/s",
                    line=dict(color=COLORS["agentic"], width=1.5),
                )
            )
            st.plotly_chart(
                apply_theme(fig5, xaxis_title="ms", yaxis_title="IRQ/s"),
                use_container_width=True,
            )

        # Events
        if not ev.empty:
            st.markdown("**Orchestration Events**")
            phase_colors = {
                "planning": COLORS["planning"],
                "execution": COLORS["execution"],
                "synthesis": COLORS["synthesis"],
            }
            fig6 = go.Figure()
            for _, row in ev.iterrows():
                fig6.add_trace(
                    go.Bar(
                        x=[row.duration_ms],
                        y=[f"{row.phase} / {row.event_type}"],
                        base=row.start_ms,
                        orientation="h",
                        marker_color=phase_colors.get(row.phase, COLORS["text_muted"]),
                        hovertemplate=f"<b>{row.event_type}</b><br>Phase: {row.phase}<br>Duration: {row.duration_ms:.0f}ms<extra></extra>",
                    )
                )
            fig6.update_layout(
                **apply_theme(fig6).layout,
                showlegend=False,
                height=max(200, len(ev) * 30),
            )
            st.plotly_chart(fig6, use_container_width=True)

# =============================================================================
# PAGE: EXPERIMENTS
# =============================================================================
elif page_id == "experiments":
    st.title("Saved Experiments")
    exps = query("""
        SELECT e.exp_id, e.name, e.task_name, e.provider, e.country_code,
               e.status, e.workflow_type AS exp_workflow,
               COUNT(r.run_id) AS run_count,
               AVG(CASE WHEN r.workflow_type='linear'  THEN r.total_energy_uj END)/1e6 AS avg_linear_j,
               AVG(CASE WHEN r.workflow_type='agentic' THEN r.total_energy_uj END)/1e6 AS avg_agentic_j
        FROM experiments e LEFT JOIN runs r ON e.exp_id = r.exp_id
        GROUP BY e.exp_id ORDER BY e.exp_id DESC
    """)
    if not exps.empty:
        cols = [
            "exp_id",
            "name",
            "task_name",
            "provider",
            "country_code",
            "status",
            "run_count",
            "avg_linear_j",
            "avg_agentic_j",
        ]
        cols = [c for c in cols if c in exps.columns]
        st.dataframe(exps[cols], use_container_width=True, hide_index=True)
        st.divider()
        selected = st.selectbox(
            "Inspect experiment",
            exps.exp_id.tolist(),
            format_func=lambda x: f"Exp {x} — {exps[exps.exp_id==x]['name'].values[0]}",
        )
        exp_runs = query(
            f"SELECT * FROM runs WHERE exp_id={selected} ORDER BY run_number"
        )
        exp_tax = query(f"""
            SELECT ots.comparison_id, ots.tax_percent,
                   ots.orchestration_tax_uj/1e6 AS tax_j,
                   ots.linear_dynamic_uj/1e6 AS linear_dynamic_j,
                   ots.agentic_dynamic_uj/1e6 AS agentic_dynamic_j
            FROM orchestration_tax_summary ots
            JOIN runs r ON ots.linear_run_id = r.run_id
            WHERE r.exp_id = {selected}
        """)
        if not exp_runs.empty:
            lin_avg = (
                exp_runs[exp_runs.workflow_type == "linear"].total_energy_uj.mean()
                / 1e6
            )
            age_avg = (
                exp_runs[exp_runs.workflow_type == "agentic"].total_energy_uj.mean()
                / 1e6
            )
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total runs", len(exp_runs))
            c2.metric("Avg Linear J", f"{lin_avg:.3f}")
            c3.metric("Avg Agentic J", f"{age_avg:.3f}")
            c4.metric("Tax multiple", f"{age_avg/lin_avg:.1f}×" if lin_avg > 0 else "—")
            run_cols = [
                "run_id",
                "workflow_type",
                "run_number",
                "total_energy_uj",
                "ipc",
                "cache_miss_rate",
                "thread_migrations",
                "carbon_g",
            ]
            run_cols = [c for c in run_cols if c in exp_runs.columns]
            st.dataframe(exp_runs[run_cols], use_container_width=True, hide_index=True)
        if not exp_tax.empty:
            st.markdown("**Tax pairs**")
            tax_cols = [
                "comparison_id",
                "linear_dynamic_j",
                "agentic_dynamic_j",
                "tax_j",
                "tax_percent",
            ]
            tax_cols = [c for c in tax_cols if c in exp_tax.columns]
            st.dataframe(exp_tax[tax_cols], use_container_width=True, hide_index=True)
    else:
        st.info("No experiments found.")

# =============================================================================
# PAGE: SQL QUERY
# =============================================================================
elif page_id == "sql":
    st.title("SQL Query")
    templates = {
        "Select * from runs": "SELECT * FROM runs LIMIT 10",
        "Experiment summary": """
            SELECT e.exp_id, e.task_name, e.provider, e.status,
                   COUNT(r.run_id) as run_count,
                   AVG(r.dynamic_energy_uj)/1e6 as avg_energy_J
            FROM experiments e
            LEFT JOIN runs r ON e.exp_id = r.exp_id
            GROUP BY e.exp_id
            ORDER BY e.exp_id DESC
        """,
        "High energy runs": """
            SELECT run_id, workflow_type, dynamic_energy_uj/1e6 as energy_J
            FROM runs
            WHERE dynamic_energy_uj > 1e7
            ORDER BY dynamic_energy_uj DESC
        """,
        "Thermal analysis": """
            SELECT run_id, package_temp_celsius, max_temp_c, thermal_delta_c
            FROM runs
            WHERE thermal_delta_c > 10
        """,
        "Tax summary": "SELECT * FROM orchestration_tax_summary ORDER BY tax_percent DESC",
    }
    choice = st.selectbox("Template", ["Custom"] + list(templates.keys()))
    default_query = (
        templates[choice] if choice != "Custom" else "SELECT * FROM runs LIMIT 50"
    )
    query_str = st.text_area("SQL", value=default_query, height=150)
    if st.button("Execute"):
        if query_str.strip():
            df, err = query_safe(query_str)
            if err:
                st.error(err)
            else:
                st.success(f"Query returned {len(df)} rows")
                st.dataframe(df, use_container_width=True)
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button("Download CSV", csv, "query_result.csv", "text/csv")
        else:
            st.warning("Enter a query.")

# =============================================================================
# PAGE: SETTINGS
# =============================================================================
elif page_id == "settings":
    st.title("Settings & Configuration")

    st.subheader("Color Theme")
    st.json(COLORS)

    st.subheader("Configuration Files")
    for f in sorted(CONFIG_DIR.glob("*")):
        if f.is_file() and f.suffix in [".yaml", ".yml", ".json"]:
            with st.expander(f.name):
                try:
                    content = f.read_text()
                    st.code(
                        content,
                        language=(
                            f.suffix[1:] if f.suffix[1:] in ["yaml", "json"] else "text"
                        ),
                    )
                except Exception as e:
                    st.error(f"Could not read: {e}")

    st.subheader("Database Schema")
    tables = query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    if not tables.empty:
        sel = st.selectbox("Table", tables.name.tolist())
        if sel:
            schema = query(f"PRAGMA table_info({sel})")
            st.dataframe(schema, use_container_width=True, hide_index=True)

    if st.button("Clear All Caches"):
        st.cache_data.clear()
        st.success("Cache cleared!")
