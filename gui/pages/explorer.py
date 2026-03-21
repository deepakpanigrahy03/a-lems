"""
gui/pages/explorer.py  —  ⊞  Run Explorer
Render function: render(ctx)
ctx keys: ov, runs, tax, lin, age, avg_lin_j, avg_age_j, tax_mult,
          plan_ms, exec_ms, synth_ms, plan_pct, exec_pct, synth_pct
"""

import subprocess

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from gui.config import DB_PATH, LIVE_API, PL, PROJECT_ROOT, WF_COLORS
from gui.db import q, q1, q_safe
from gui.helpers import (_bar_gauge_html, _gauge_html, _human_carbon,
                         _human_energy, _human_water, fl)

try:
    import requests as _req

    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False

    class _req:
        @staticmethod
        def get(*a, **kw):
            raise RuntimeError("requests not installed")


try:
    import yaml as _yaml

    _YAML_OK = True
except ImportError:
    _YAML_OK = False


def render(ctx: dict):
    ov = ctx["ov"]
    runs = ctx["runs"]
    tax = ctx["tax"]
    avg_lin_j = ctx["avg_lin_j"]
    avg_age_j = ctx["avg_age_j"]
    tax_mult = ctx["tax_mult"]
    plan_ms = ctx["plan_ms"]
    exec_ms = ctx["exec_ms"]
    synth_ms = ctx["synth_ms"]
    plan_pct = ctx["plan_pct"]
    exec_pct = ctx["exec_pct"]
    synth_pct = ctx["synth_pct"]
    lin = ctx["lin"]
    age = ctx["age"]

    st.title("Sample Explorer")
    st.caption("100Hz RAPL energy · CPU c-states · interrupt timeseries per run")

    if runs.empty:
        st.info("No runs available — run experiments first.")
    else:
        # ── Run picker ────────────────────────────────────────────────────────
        _sel = runs[runs.energy_j.notna()].copy()

        def _lbl(r):
            task = str(r.task_name or "?")[:22]
            return (
                f"Run {int(r.run_id):>4}  {r.workflow_type:<8}  "
                f"{r.provider:<6}  {r.energy_j:.3f}J  {task}"
            )

        _labels = [_lbl(r) for _, r in _sel.iterrows()]
        _ids = _sel.run_id.tolist()

        col_pick, col_stats = st.columns([2, 1])
        with col_pick:
            chosen = st.selectbox(
                "Select run",
                _labels,
                help="Sorted newest-first. Shows run_id, workflow, provider, energy, task.",
            )
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
            if e1:
                errors.append(f"energy_samples: {e1}")

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
            if e2:
                errors.append(f"cpu_samples: {e2}")

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
            if e3:
                errors.append(f"interrupt_samples: {e3}")

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
            if e4:
                errors.append(f"orchestration_events: {e4}")

            return es_df, cs_df, irq_df, ev_df, errors

        with st.spinner(f"Loading samples for run {rid}…"):
            es, cs, irq, ev, errs = load_samples(rid)

        if errs:
            for err in errs:
                st.error(f"⚠ SQL error — {err}")

        # ── KPI row ───────────────────────────────────────────────────────────
        _r = runs[runs.run_id == rid].iloc[0]
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Run", f"{rid} — {_r.workflow_type}")
        k2.metric("Total energy", f"{_r.energy_j:.3f}J")
        k3.metric("Energy samples", f"{len(es):,}")
        k4.metric("CPU samples", f"{len(cs):,}")
        k5.metric("Interrupt samples", f"{len(irq):,}")

        st.divider()

        # ── Power timeseries ──────────────────────────────────────────────────
        if not es.empty and len(es) > 2:
            _es = es.copy()
            dt = (_es.elapsed_ms.diff() / 1000).replace(
                0, float("nan")
            )  # sec, avoid /0
            _es["pkg_w"] = (_es.pkg_j.diff() / dt).clip(lower=0)
            _es["core_w"] = (_es.core_j.diff() / dt).clip(lower=0)
            _es["dram_w"] = (_es.dram_j.diff() / dt).clip(lower=0)
            _es = _es.iloc[1:].copy()  # drop first NaN row

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**⚡ Power over time (Watts)**")
                st.caption(
                    "Instantaneous power derived from RAPL Δenergy / Δtime at 100Hz"
                )
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=_es.elapsed_ms,
                        y=_es.pkg_w,
                        name="PKG",
                        line=dict(color="#3b82f6", width=1.5),
                        fill="tozeroy",
                        fillcolor="rgba(59,130,246,.08)",
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=_es.elapsed_ms,
                        y=_es.core_w,
                        name="Core",
                        line=dict(color="#22c55e", width=1),
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=_es.elapsed_ms,
                        y=_es.dram_w,
                        name="DRAM",
                        line=dict(color="#a78bfa", width=1),
                    )
                )
                st.plotly_chart(
                    fl(fig, xaxis_title="elapsed ms", yaxis_title="Watts"),
                    use_container_width=True,
                )

            with col2:
                st.markdown("**∫ Cumulative energy (Joules)**")
                st.caption(
                    "Raw RAPL counter values — monotonically increasing throughout run"
                )
                fig2 = go.Figure()
                fig2.add_trace(
                    go.Scatter(
                        x=es.elapsed_ms,
                        y=es.pkg_j,
                        name="PKG",
                        line=dict(color="#3b82f6", width=1.5),
                    )
                )
                fig2.add_trace(
                    go.Scatter(
                        x=es.elapsed_ms,
                        y=es.core_j,
                        name="Core",
                        line=dict(color="#22c55e", width=1),
                    )
                )
                fig2.add_trace(
                    go.Scatter(
                        x=es.elapsed_ms,
                        y=es.dram_j,
                        name="DRAM",
                        line=dict(color="#a78bfa", width=1),
                    )
                )
                st.plotly_chart(
                    fl(fig2, xaxis_title="elapsed ms", yaxis_title="Joules"),
                    use_container_width=True,
                )
        else:
            st.info(
                f"No energy_samples found for run {rid}. "
                f"Check that the RAPL collector ran during this experiment."
            )

        # ── CPU + C-States ────────────────────────────────────────────────────
        if not cs.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**IPC + CPU utilisation**")
                st.caption("IPC = instructions/cycle (left axis) · util% (right axis)")
                fig3 = make_subplots(specs=[[{"secondary_y": True}]])
                fig3.add_trace(
                    go.Scatter(
                        x=cs.elapsed_ms,
                        y=cs.ipc,
                        name="IPC",
                        line=dict(color="#22c55e", width=1.5),
                    ),
                    secondary_y=False,
                )
                fig3.add_trace(
                    go.Scatter(
                        x=cs.elapsed_ms,
                        y=cs.cpu_util_percent,
                        name="CPU Util%",
                        line=dict(color="#f59e0b", width=1),
                    ),
                    secondary_y=True,
                )
                fig3.update_layout(**PL)
                fig3.update_yaxes(
                    title_text="IPC",
                    secondary_y=False,
                    gridcolor="#1e2d45",
                    tickfont=dict(size=9),
                )
                fig3.update_yaxes(
                    title_text="CPU Util%",
                    secondary_y=True,
                    gridcolor="rgba(0,0,0,0)",
                    tickfont=dict(size=9),
                )
                st.plotly_chart(fig3, use_container_width=True)

            with col2:
                st.markdown("**C-State residency over time**")
                st.caption("Stacked: C6/C7 = deep sleep, C0 = active execution")
                fig4 = go.Figure()
                for _col, _color, _name in [
                    ("c7", "#f59e0b", "C7 (deepest)"),
                    ("c6", "#22c55e", "C6"),
                    ("c3", "#38bdf8", "C3"),
                    ("c2", "#3b82f6", "C2"),
                    ("c1", "#a78bfa", "C1"),
                ]:
                    fig4.add_trace(
                        go.Scatter(
                            x=cs.elapsed_ms,
                            y=cs[_col],
                            name=_name,
                            line=dict(color=_color, width=1),
                            stackgroup="cstate",
                            fill="tonexty",
                        )
                    )
                st.plotly_chart(
                    fl(fig4, xaxis_title="elapsed ms", yaxis_title="Residency %"),
                    use_container_width=True,
                )

            # Package power + temp side by side
            col3, col4 = st.columns(2)
            with col3:
                st.markdown("**Package power (W) from turbostat**")
                st.caption("Direct turbostat reading — cross-check vs RAPL power above")
                fig5 = go.Figure()
                fig5.add_trace(
                    go.Scatter(
                        x=cs.elapsed_ms,
                        y=cs.pkg_w,
                        name="PKG W",
                        line=dict(color="#3b82f6", width=1.5),
                        fill="tozeroy",
                        fillcolor="rgba(59,130,246,.06)",
                    )
                )
                if cs.dram_w.any():
                    fig5.add_trace(
                        go.Scatter(
                            x=cs.elapsed_ms,
                            y=cs.dram_w,
                            name="DRAM W",
                            line=dict(color="#a78bfa", width=1),
                        )
                    )
                st.plotly_chart(
                    fl(fig5, xaxis_title="elapsed ms", yaxis_title="Watts"),
                    use_container_width=True,
                )

            with col4:
                if cs.pkg_temp.any():
                    st.markdown("**Package temperature (°C)**")
                    st.caption("Thermal headroom — sustained load shows gradual rise")
                    fig6 = go.Figure()
                    fig6.add_trace(
                        go.Scatter(
                            x=cs.elapsed_ms,
                            y=cs.pkg_temp,
                            name="Temp °C",
                            line=dict(color="#ef4444", width=1.5),
                            fill="tozeroy",
                            fillcolor="rgba(239,68,68,.06)",
                        )
                    )
                    st.plotly_chart(
                        fl(fig6, xaxis_title="elapsed ms", yaxis_title="°C"),
                        use_container_width=True,
                    )
                else:
                    st.info("No temperature data for this run.")

        # ── IRQ timeseries ────────────────────────────────────────────────────
        if not irq.empty:
            st.markdown("**IRQ rate (interrupts/sec)**")
            st.caption(
                "Spikes = API response arrivals or timer interrupts during phase transitions"
            )
            fig7 = go.Figure()
            fig7.add_trace(
                go.Scatter(
                    x=irq.elapsed_ms,
                    y=irq.interrupts_per_sec,
                    name="IRQ/s",
                    line=dict(color="#ef4444", width=1.5),
                    fill="tozeroy",
                    fillcolor="rgba(239,68,68,.06)",
                )
            )
            st.plotly_chart(
                fl(fig7, xaxis_title="elapsed ms", yaxis_title="IRQ/s"),
                use_container_width=True,
            )

        # ── Orchestration events Gantt ────────────────────────────────────────
        if not ev.empty:
            st.markdown("**Orchestration Event Timeline**")
            st.caption(
                "Each bar = one agent phase. Width = duration. Hover for detail."
            )
            PHASE_C = {
                "planning": "#f59e0b",
                "execution": "#3b82f6",
                "synthesis": "#a78bfa",
            }
            fig8 = go.Figure()
            for _, row in ev.iterrows():
                fig8.add_trace(
                    go.Bar(
                        x=[row.duration_ms],
                        y=[f"{row.phase or '?'} / {row.event_type or '?'}"],
                        base=row.start_ms,
                        orientation="h",
                        marker_color=PHASE_C.get(str(row.phase), "#3b82f6"),
                        hovertemplate=(
                            f"<b>{row.event_type}</b><br>"
                            f"Phase: {row.phase}<br>"
                            f"Duration: {row.duration_ms:.0f}ms<extra></extra>"
                        ),
                    )
                )
            fig8.update_layout(
                **PL,
                xaxis_title="elapsed ms",
                showlegend=False,
                height=max(200, len(ev) * 32),
            )
            st.plotly_chart(fig8, use_container_width=True)
        elif _r.workflow_type == "agentic":
            st.info("No orchestration events recorded for this agentic run.")


# ══════════════════════════════════════════════════════════════════════════════
# EXPERIMENTS
# ══════════════════════════════════════════════════════════════════════════════
