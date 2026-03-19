"""
gui/pages/data_interrupts.py  —  ∿  Interrupts
Interrupt rate, wakeup latency, and their energy cost.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1
from gui.pages._dm_helpers import get_runs, no_data_banner, rgba

ACCENT = "#a78bfa"


def render(ctx: dict) -> None:
    df = get_runs(ctx)

    if df.empty or "interrupt_rate" not in df.columns:
        no_data_banner("No interrupt rate data available yet.", ACCENT)
        return

    df = df[df["interrupt_rate"].notna() & (df["interrupt_rate"] > 0)].copy()
    if df.empty:
        no_data_banner("No runs with interrupt data yet.", ACCENT)
        return

    has_energy = "energy_j" in df.columns
    has_wakeup = "wakeup_latency_us" in df.columns
    has_irq_ps = "interrupts_per_second" in df.columns

    lin = df[df["workflow_type"] == "linear"]
    age = df[df["workflow_type"] == "agentic"]

    avg_irq_lin = lin["interrupt_rate"].mean() if not lin.empty else 0
    avg_irq_age = age["interrupt_rate"].mean() if not age.empty else 0
    avg_wakeup = df["wakeup_latency_us"].mean() if has_wakeup else 0
    avg_irq_ps = df["interrupts_per_second"].mean() if has_irq_ps else 0

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Interrupts — {len(df)} runs</div>"
        f"<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:12px;'>"
        + "".join(
            [
                f"<div><div style='font-size:18px;font-weight:700;color:{c};"
                f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
                f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
                f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
                for v, l, c in [
                    (
                        f"{avg_irq_lin:.0f}",
                        "Interrupt rate linear",
                        WF_COLORS["linear"],
                    ),
                    (
                        f"{avg_irq_age:.0f}",
                        "Interrupt rate agentic",
                        WF_COLORS["agentic"],
                    ),
                    (f"{avg_wakeup:.1f}µs", "Avg wakeup latency", "#f59e0b"),
                    (f"{avg_irq_ps:.0f}/s", "Interrupts/sec", ACCENT),
                ]
            ]
        )
        + "</div></div>",
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Interrupt rate vs Energy</div>",
            unsafe_allow_html=True,
        )
        fig = go.Figure()
        if has_energy:
            for wf, clr in WF_COLORS.items():
                sub = df[df["workflow_type"] == wf]
                sub = sub[sub["energy_j"].notna() & (sub["energy_j"] > 0)]
                if sub.empty:
                    continue
                fig.add_trace(
                    go.Scatter(
                        x=sub["interrupt_rate"],
                        y=sub["energy_j"],
                        mode="markers",
                        name=wf,
                        marker=dict(color=clr, size=5, opacity=0.6),
                    )
                )
        fig.update_layout(
            **PL, height=260, xaxis_title="Interrupt rate", yaxis_title="Energy (J)"
        )
        st.plotly_chart(fig, use_container_width=True, key="dm_irq_scatter")

    with col2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Interrupt rate distribution</div>",
            unsafe_allow_html=True,
        )
        fig2 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = df[df["workflow_type"] == wf]["interrupt_rate"].dropna()
            if sub.empty:
                continue
            fig2.add_trace(
                go.Box(y=sub, name=wf, marker_color=clr, line_color=clr, boxmean=True)
            )
        fig2.update_layout(
            **PL, height=260, yaxis_title="Interrupt rate", showlegend=False
        )
        st.plotly_chart(fig2, use_container_width=True, key="dm_irq_box")

    # ── Trend over runs ───────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Interrupt rate trend across runs</div>",
        unsafe_allow_html=True,
    )
    df_sorted = df.sort_values("run_id") if "run_id" in df.columns else df
    fig3 = go.Figure()
    for wf, clr in WF_COLORS.items():
        sub = df_sorted[df_sorted["workflow_type"] == wf]
        if sub.empty:
            continue
        x_vals = sub["run_id"] if "run_id" in sub.columns else sub.index
        fig3.add_trace(
            go.Scatter(
                x=x_vals,
                y=sub["interrupt_rate"],
                mode="markers+lines",
                name=wf,
                marker=dict(color=clr, size=3),
                line=dict(color=clr, width=1),
                opacity=0.7,
            )
        )
    fig3.update_layout(
        **PL, height=220, xaxis_title="Run ID", yaxis_title="Interrupt rate"
    )
    st.plotly_chart(fig3, use_container_width=True, key="dm_irq_trend")

    # ── interrupt_samples time series ─────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Time-series drilldown — interrupt samples</div>",
        unsafe_allow_html=True,
    )

    sample_count = q1("SELECT COUNT(*) AS n FROM interrupt_samples").get("n", 0) or 0

    if sample_count == 0:
        st.info("No interrupt_samples time-series data yet.")
    else:
        run_ids = (
            df["run_id"]
            .dropna()
            .astype(int)
            .sort_values(ascending=False)
            .head(50)
            .tolist()
            if "run_id" in df.columns
            else []
        )

        if run_ids:
            sel_run = st.selectbox(
                "Select run",
                run_ids,
                key="dm_irq_run_sel",
                format_func=lambda x: f"Run {x}",
            )
            samples = q(f"""
                SELECT timestamp_ns / 1e9 AS time_s, interrupts_per_sec
                FROM interrupt_samples
                WHERE run_id = {int(sel_run)}
                ORDER BY timestamp_ns
            """)
            if not samples.empty:
                samples["time_s"] -= samples["time_s"].min()
                fig4 = go.Figure(
                    go.Scatter(
                        x=samples["time_s"],
                        y=samples["interrupts_per_sec"],
                        mode="lines",
                        line=dict(color=ACCENT, width=1.5),
                        fill="tozeroy",
                        fillcolor=rgba(ACCENT, 0.13),  # rgba() not hex8
                    )
                )
                fig4.update_layout(
                    **PL,
                    height=220,
                    xaxis_title="Time (s)",
                    yaxis_title="Interrupts/sec",
                    showlegend=False,
                )
                st.plotly_chart(
                    fig4, use_container_width=True, key=f"dm_irq_samples_{sel_run}"
                )

    # ── Insight ───────────────────────────────────────────────────────────────
    irq_delta = avg_irq_age - avg_irq_lin
    st.markdown(
        f"<div style='margin-top:12px;padding:10px 14px;"
        f"background:#1a0e40;border-left:3px solid {ACCENT};"
        f"border-radius:0 8px 8px 0;font-size:11px;"
        f"color:#c4b5fd;font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
        f"Agentic runs generate <b>{irq_delta:+.0f}</b> more interrupts than linear. "
        f"Each interrupt forces a C-state exit, preventing deep sleep recovery "
        f"and directly increasing package energy.</div>",
        unsafe_allow_html=True,
    )
