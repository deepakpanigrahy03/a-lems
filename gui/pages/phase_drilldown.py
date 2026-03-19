"""
gui/pages/phase_drilldown.py  —  ⬡  Phase Drilldown
─────────────────────────────────────────────────────────────────────────────
Per-step planning/execution/synthesis energy from orchestration_events.
2,773 events available. Richest unused table in the schema.
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1

ACCENT = "#ef4444"
PH_COLORS = {
    "planning": "#3b82f6",
    "execution": "#22c55e",
    "synthesis": "#a78bfa",
    "waiting": "#f59e0b",
}


def render(ctx: dict) -> None:
    total_events = q1("SELECT COUNT(*) AS n FROM orchestration_events").get("n", 0) or 0

    if total_events == 0:
        st.info("No orchestration events yet. Run agentic experiments to populate.")
        return

    # ── Load events ───────────────────────────────────────────────────────────
    events = q("""
        SELECT
            oe.event_id, oe.run_id, oe.step_index, oe.phase,
            oe.event_type,
            oe.duration_ns / 1e6         AS duration_ms,
            oe.power_watts,
            oe.cpu_util_percent,
            oe.interrupt_rate,
            oe.event_energy_uj / 1e6     AS event_energy_j,
            oe.tax_contribution_uj / 1e6 AS tax_j,
            oe.tax_percent,
            e.model_name, e.task_name, e.workflow_type
        FROM orchestration_events oe
        JOIN runs r ON oe.run_id = r.run_id
        JOIN experiments e ON r.exp_id = e.exp_id
        ORDER BY oe.event_id DESC
    """)

    if events.empty:
        st.info("No orchestration event data.")
        return

    # ── KPIs ──────────────────────────────────────────────────────────────────
    phase_counts = events["phase"].value_counts()
    total_tax_j = events["tax_j"].sum()
    avg_step_ms = events.groupby("run_id")["duration_ms"].sum().mean()

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Phase Drilldown — {total_events:,} orchestration events</div>"
        f"<div style='display:grid;grid-template-columns:repeat(5,1fr);gap:12px;'>"
        + "".join(
            [
                f"<div><div style='font-size:16px;font-weight:700;color:{c};"
                f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
                f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
                f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
                for v, l, c in [
                    (
                        f"{phase_counts.get('planning',0):,}",
                        "Planning events",
                        PH_COLORS["planning"],
                    ),
                    (
                        f"{phase_counts.get('execution',0):,}",
                        "Execution events",
                        PH_COLORS["execution"],
                    ),
                    (
                        f"{phase_counts.get('synthesis',0):,}",
                        "Synthesis events",
                        PH_COLORS["synthesis"],
                    ),
                    (
                        f"{phase_counts.get('waiting',0):,}",
                        "Waiting events",
                        PH_COLORS["waiting"],
                    ),
                    (f"{total_tax_j:.2f}J", "Total tax energy", ACCENT),
                ]
            ]
        )
        + "</div></div>",
        unsafe_allow_html=True,
    )

    # ── Phase energy breakdown ────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Energy by phase</div>",
            unsafe_allow_html=True,
        )
        phase_e = (
            events.groupby("phase")["event_energy_j"]
            .agg(["sum", "mean", "count"])
            .reset_index()
        )
        fig = go.Figure(
            go.Bar(
                x=phase_e["phase"],
                y=phase_e["sum"],
                marker_color=[PH_COLORS.get(p, "#94a3b8") for p in phase_e["phase"]],
                marker_line_width=0,
                text=[f"{v:.3f}J" for v in phase_e["sum"]],
                textposition="outside",
                textfont=dict(size=9),
            )
        )
        fig.update_layout(
            **PL, height=260, yaxis_title="Total energy (J)", showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True, key="pd_phase_energy")

    with col2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Avg duration by phase</div>",
            unsafe_allow_html=True,
        )
        phase_d = events.groupby("phase")["duration_ms"].mean().reset_index()
        fig2 = go.Figure(
            go.Bar(
                x=phase_d["phase"],
                y=phase_d["duration_ms"],
                marker_color=[PH_COLORS.get(p, "#94a3b8") for p in phase_d["phase"]],
                marker_line_width=0,
            )
        )
        fig2.update_layout(
            **PL, height=260, yaxis_title="Avg duration (ms)", showlegend=False
        )
        st.plotly_chart(fig2, use_container_width=True, key="pd_phase_dur")

    # ── Tax contribution by phase ─────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Orchestration tax contribution by phase</div>",
        unsafe_allow_html=True,
    )

    tax_df = events[events["tax_j"].notna() & (events["tax_j"] > 0)]
    if not tax_df.empty:
        tax_phase = tax_df.groupby("phase")["tax_j"].sum().reset_index()
        fig3 = go.Figure(
            go.Pie(
                labels=tax_phase["phase"],
                values=tax_phase["tax_j"],
                marker_colors=[PH_COLORS.get(p, "#94a3b8") for p in tax_phase["phase"]],
                hole=0.5,
                textinfo="label+percent",
            )
        )
        fig3.update_layout(
            **{**PL, "margin": dict(l=20, r=20, t=20, b=20)},
            height=240,
            showlegend=True,
        )
        st.plotly_chart(fig3, use_container_width=True, key="pd_tax_pie")

    # ── Per-run step timeline ─────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Step timeline — select run</div>",
        unsafe_allow_html=True,
    )

    run_ids = events["run_id"].dropna().unique().tolist()
    sel_run = st.selectbox(
        "Run",
        sorted(run_ids, reverse=True)[:50],
        key="pd_run_sel",
        format_func=lambda x: f"Run {x}",
    )

    run_events = events[events["run_id"] == sel_run].sort_values("step_index")

    if not run_events.empty:
        fig4 = go.Figure()
        for phase, clr in PH_COLORS.items():
            sub = run_events[run_events["phase"] == phase]
            if sub.empty:
                continue
            fig4.add_trace(
                go.Bar(
                    x=sub["step_index"],
                    y=sub["duration_ms"],
                    name=phase,
                    marker_color=clr,
                    marker_line_width=0,
                )
            )
        fig4.update_layout(
            **PL,
            height=260,
            barmode="stack",
            xaxis_title="Step index",
            yaxis_title="Duration (ms)",
        )
        st.plotly_chart(
            fig4, use_container_width=True, key=f"pd_step_timeline_{sel_run}"
        )

        # Power per step
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:12px 0 8px;'>"
            f"Power per step — Run {sel_run}</div>",
            unsafe_allow_html=True,
        )
        fig5 = go.Figure()
        for phase, clr in PH_COLORS.items():
            sub = run_events[run_events["phase"] == phase]
            sub = sub[sub["power_watts"].notna()]
            if sub.empty:
                continue
            fig5.add_trace(
                go.Scatter(
                    x=sub["step_index"],
                    y=sub["power_watts"],
                    mode="markers+lines",
                    name=phase,
                    marker=dict(color=clr, size=6),
                    line=dict(color=clr, width=1),
                )
            )
        fig5.update_layout(
            **PL, height=220, xaxis_title="Step index", yaxis_title="Power (W)"
        )
        st.plotly_chart(fig5, use_container_width=True, key=f"pd_power_{sel_run}")

    # ── Phase stats by task ───────────────────────────────────────────────────
    if "task_name" in events.columns:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            f"Planning ratio by task</div>",
            unsafe_allow_html=True,
        )
        task_phase = (
            events.groupby(["task_name", "phase"])["duration_ms"].sum().reset_index()
        )
        fig6 = go.Figure()
        for phase, clr in PH_COLORS.items():
            sub = task_phase[task_phase["phase"] == phase]
            if sub.empty:
                continue
            fig6.add_trace(
                go.Bar(
                    x=sub["task_name"],
                    y=sub["duration_ms"],
                    name=phase,
                    marker_color=clr,
                    marker_line_width=0,
                )
            )
        fig6.update_layout(
            **PL,
            height=240,
            barmode="stack",
            xaxis_title="Task",
            yaxis_title="Total duration (ms)",
            xaxis_tickangle=-30,
        )
        st.plotly_chart(fig6, use_container_width=True, key="pd_task_phase")
