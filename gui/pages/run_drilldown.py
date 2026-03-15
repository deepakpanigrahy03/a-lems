"""
gui/pages/run_drilldown.py  —  🔍  Run Drilldown
─────────────────────────────────────────────────────────────────────────────
All sensor streams for one run in one view.
energy_samples: 537,228 rows
cpu_samples:    113,981 rows
thermal_samples: 24,729 rows
interrupt_samples: available
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from gui.db     import q, q1
from gui.config import PL, WF_COLORS, STATUS_COLORS

ACCENT = "#3b82f6"


def render(ctx: dict) -> None:
    runs = ctx.get("runs", pd.DataFrame())

    # ── Run selector ──────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:16px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px;'>"
        f"Run Drilldown — all sensor streams</div>"
        f"<div style='font-size:11px;color:#94a3b8;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"energy · cpu · thermal · interrupts — unified time-series view</div>"
        f"</div>",
        unsafe_allow_html=True)

    # Build run list with metadata
    run_meta = q("""
        SELECT r.run_id, e.workflow_type, e.model_name, e.task_name,
               r.total_energy_uj/1e6 AS energy_j,
               r.duration_ns/1e6     AS duration_ms,
               r.package_temp_celsius AS temp_c
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        WHERE r.total_energy_uj IS NOT NULL
        ORDER BY r.run_id DESC
        LIMIT 200
    """)

    if run_meta.empty:
        st.info("No runs available.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        wf_opts = ["All"] + sorted(run_meta["workflow_type"].dropna().unique().tolist())
        sel_wf  = st.selectbox("Workflow", wf_opts, key="rd_wf")
    with col2:
        model_opts = ["All"] + sorted(run_meta["model_name"].dropna().unique().tolist())
        sel_model  = st.selectbox("Model", model_opts, key="rd_model")
    with col3:
        task_opts = ["All"] + sorted(run_meta["task_name"].dropna().unique().tolist())
        sel_task  = st.selectbox("Task", task_opts, key="rd_task")

    filtered = run_meta.copy()
    if sel_wf    != "All": filtered = filtered[filtered["workflow_type"] == sel_wf]
    if sel_model != "All": filtered = filtered[filtered["model_name"]    == sel_model]
    if sel_task  != "All": filtered = filtered[filtered["task_name"]     == sel_task]

    if filtered.empty:
        st.info("No runs match filters.")
        return

    sel_run = st.selectbox(
        "Select run",
        filtered["run_id"].tolist(),
        key="rd_run_sel",
        format_func=lambda x: (
            f"Run {x} · "
            + str(filtered[filtered["run_id"]==x]["workflow_type"].values[0] or "")
            + " · "
            + str(filtered[filtered["run_id"]==x]["model_name"].values[0] or "")[:20]
            + f" · {filtered[filtered['run_id']==x]['energy_j'].values[0]:.2f}J"
        ))

    run_row = filtered[filtered["run_id"] == sel_run].iloc[0]

    # ── Run summary card ──────────────────────────────────────────────────────
    wf_clr = WF_COLORS.get(run_row.get("workflow_type",""), ACCENT)
    st.markdown(
        f"<div style='display:flex;gap:16px;padding:10px 14px;"
        f"background:#111827;border:1px solid {wf_clr}33;"
        f"border-left:3px solid {wf_clr};border-radius:0 8px 8px 0;"
        f"margin-bottom:16px;font-family:IBM Plex Mono,monospace;'>"
        f"<div><span style='font-size:9px;color:#475569;'>Run</span>"
        f"<div style='font-size:16px;font-weight:700;color:{wf_clr};'>#{sel_run}</div></div>"
        f"<div><span style='font-size:9px;color:#475569;'>Workflow</span>"
        f"<div style='font-size:12px;color:#f1f5f9;'>{run_row.get('workflow_type','?')}</div></div>"
        f"<div><span style='font-size:9px;color:#475569;'>Model</span>"
        f"<div style='font-size:12px;color:#f1f5f9;'>{run_row.get('model_name','?')}</div></div>"
        f"<div><span style='font-size:9px;color:#475569;'>Task</span>"
        f"<div style='font-size:12px;color:#f1f5f9;'>{run_row.get('task_name','?')}</div></div>"
        f"<div><span style='font-size:9px;color:#475569;'>Energy</span>"
        f"<div style='font-size:12px;color:#f59e0b;'>{run_row.get('energy_j',0):.3f}J</div></div>"
        f"<div><span style='font-size:9px;color:#475569;'>Duration</span>"
        f"<div style='font-size:12px;color:#94a3b8;'>{run_row.get('duration_ms',0):.0f}ms</div></div>"
        f"</div>",
        unsafe_allow_html=True)

    run_id = int(sel_run)

    # ── Load all sensor streams ───────────────────────────────────────────────
    energy_s = q(f"""
        SELECT timestamp_ns/1e9 AS t,
               pkg_energy_uj/1e6  AS pkg_j,
               core_energy_uj/1e6 AS core_j,
               uncore_energy_uj/1e6 AS uncore_j,
               dram_energy_uj/1e6   AS dram_j
        FROM energy_samples WHERE run_id={run_id}
        ORDER BY timestamp_ns
    """)

    cpu_s = q(f"""
        SELECT timestamp_ns/1e9 AS t,
               cpu_util_percent, cpu_busy_mhz, ipc,
               package_power, dram_power,
               c6_residency, c7_residency
        FROM cpu_samples WHERE run_id={run_id}
        ORDER BY timestamp_ns
    """)

    thermal_s = q(f"""
        SELECT timestamp_ns/1e9 AS t,
               cpu_temp, system_temp, throttle_event
        FROM thermal_samples WHERE run_id={run_id}
        ORDER BY timestamp_ns
    """)

    interrupt_s = q(f"""
        SELECT timestamp_ns/1e9 AS t, interrupts_per_sec
        FROM interrupt_samples WHERE run_id={run_id}
        ORDER BY timestamp_ns
    """)

    # Normalise time to 0
    def norm_t(df):
        if df.empty or "t" not in df.columns: return df
        df = df.copy()
        df["t"] -= df["t"].min()
        return df

    energy_s   = norm_t(energy_s)
    cpu_s      = norm_t(cpu_s)
    thermal_s  = norm_t(thermal_s)
    interrupt_s = norm_t(interrupt_s)

    streams_available = sum([
        not energy_s.empty, not cpu_s.empty,
        not thermal_s.empty, not interrupt_s.empty
    ])

    if streams_available == 0:
        st.info("No time-series samples found for this run.")
        return

    st.markdown(
        f"<div style='font-size:10px;color:#475569;"
        f"font-family:IBM Plex Mono,monospace;margin-bottom:12px;'>"
        f"energy: {len(energy_s):,} samples · "
        f"cpu: {len(cpu_s):,} samples · "
        f"thermal: {len(thermal_s):,} samples · "
        f"interrupts: {len(interrupt_s):,} samples</div>",
        unsafe_allow_html=True)

    # ── Energy time series ────────────────────────────────────────────────────
    if not energy_s.empty:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:#f59e0b;"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Energy — RAPL domains</div>", unsafe_allow_html=True)
        fig = go.Figure()
        for col_n, label, clr in [
            ("pkg_j",    "Package", "#f59e0b"),
            ("core_j",   "Core",    "#22c55e"),
            ("uncore_j", "Uncore",  "#3b82f6"),
            ("dram_j",   "DRAM",    "#a78bfa"),
        ]:
            sub = energy_s[energy_s[col_n].notna()]
            if sub.empty: continue
            fig.add_trace(go.Scatter(
                x=sub["t"], y=sub[col_n],
                mode="lines", name=label,
                line=dict(width=1.5, color=clr)))
        fig.update_layout(**PL, height=240,
                          xaxis_title="Time (s)",
                          yaxis_title="Cumulative energy (J)")
        st.plotly_chart(fig, use_container_width=True, key=f"rd_energy_{run_id}")

    # ── CPU ───────────────────────────────────────────────────────────────────
    if not cpu_s.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:#22c55e;"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"CPU utilisation & frequency</div>", unsafe_allow_html=True)
            fig2 = go.Figure()
            if "cpu_util_percent" in cpu_s.columns:
                fig2.add_trace(go.Scatter(
                    x=cpu_s["t"], y=cpu_s["cpu_util_percent"].fillna(0),
                    mode="lines", name="CPU util %",
                    line=dict(color="#22c55e", width=1.5)))
            if "cpu_busy_mhz" in cpu_s.columns:
                fig2.add_trace(go.Scatter(
                    x=cpu_s["t"], y=cpu_s["cpu_busy_mhz"].fillna(0),
                    mode="lines", name="Busy MHz",
                    yaxis="y2",
                    line=dict(color="#60a5fa", width=1, dash="dot")))
            fig2.update_layout(
                **{**PL, "margin": dict(l=40,r=60,t=20,b=30)},
                height=220,
                xaxis_title="Time (s)",
                yaxis_title="CPU util %",
                yaxis2=dict(title="MHz", overlaying="y", side="right",
                            gridcolor="rgba(0,0,0,0)",
                            tickfont=dict(size=9)))
            st.plotly_chart(fig2, use_container_width=True,
                            key=f"rd_cpu_{run_id}")

        with col2:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:#a78bfa;"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"C-state residency</div>", unsafe_allow_html=True)
            fig3 = go.Figure()
            for col_n, label, clr in [
                ("c6_residency", "C6", "#a78bfa"),
                ("c7_residency", "C7", "#ec4899"),
            ]:
                if col_n not in cpu_s.columns: continue
                sub = cpu_s[cpu_s[col_n].notna()]
                if sub.empty: continue
                fig3.add_trace(go.Scatter(
                    x=sub["t"], y=sub[col_n],
                    mode="lines", name=label,
                    line=dict(width=1.5, color=clr)))
            fig3.update_layout(**PL, height=220,
                               xaxis_title="Time (s)",
                               yaxis_title="C-state residency %")
            st.plotly_chart(fig3, use_container_width=True,
                            key=f"rd_cstate_{run_id}")

    # ── Thermal ───────────────────────────────────────────────────────────────
    if not thermal_s.empty:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:#ef4444;"
            f"text-transform:uppercase;letter-spacing:.1em;margin:8px 0 8px;'>"
            f"Thermal</div>", unsafe_allow_html=True)
        fig4 = go.Figure()
        for col_n, label, clr in [
            ("cpu_temp",    "CPU",    "#ef4444"),
            ("system_temp", "System", "#3b82f6"),
        ]:
            sub = thermal_s[thermal_s[col_n].notna() & (thermal_s[col_n] > -100)]
            if sub.empty: continue
            fig4.add_trace(go.Scatter(
                x=sub["t"], y=sub[col_n],
                mode="lines", name=label,
                line=dict(width=1.5, color=clr)))
        fig4.update_layout(**PL, height=200,
                           xaxis_title="Time (s)",
                           yaxis_title="Temperature (°C)")
        st.plotly_chart(fig4, use_container_width=True, key=f"rd_thermal_{run_id}")

    # ── Interrupts ────────────────────────────────────────────────────────────
    if not interrupt_s.empty:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:#f59e0b;"
            f"text-transform:uppercase;letter-spacing:.1em;margin:8px 0 8px;'>"
            f"Interrupt rate</div>", unsafe_allow_html=True)
        fig5 = go.Figure(go.Scatter(
            x=interrupt_s["t"],
            y=interrupt_s["interrupts_per_sec"].fillna(0),
            mode="lines",
            line=dict(color="#f59e0b", width=1.5),
            fill="tozeroy",
            fillcolor="rgba(245,158,11,0.13)"))
        fig5.update_layout(**PL, height=180,
                           xaxis_title="Time (s)",
                           yaxis_title="Interrupts/sec",
                           showlegend=False)
        st.plotly_chart(fig5, use_container_width=True, key=f"rd_irq_{run_id}")
