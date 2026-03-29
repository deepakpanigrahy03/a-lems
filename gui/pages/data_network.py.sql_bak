"""
gui/pages/data_network.py  —  ⌁  Network & API
Energy cost of network latency — DNS, API, compute time.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.pages._dm_helpers import get_runs, no_data_banner

ACCENT = "#a78bfa"


def render(ctx: dict) -> None:
    df = get_runs(ctx)

    if df.empty or "api_latency_ms" not in df.columns:
        no_data_banner("No network latency data available yet.", ACCENT)
        return

    df = df[df["api_latency_ms"].notna() & (df["api_latency_ms"] > 0)].copy()
    if df.empty:
        no_data_banner("No runs with API latency data yet.", ACCENT)
        return

    has_energy = "energy_j" in df.columns
    has_dns = "dns_latency_ms" in df.columns
    has_compute = "compute_time_ms" in df.columns

    if "duration_ns" in df.columns:
        df["duration_ms"] = df["duration_ns"] / 1e6
    else:
        df["duration_ms"] = 0

    lin = df[df["workflow_type"] == "linear"]
    age = df[df["workflow_type"] == "agentic"]

    avg_api_lin = lin["api_latency_ms"].mean() if not lin.empty else 0
    avg_api_age = age["api_latency_ms"].mean() if not age.empty else 0
    avg_dns = df["dns_latency_ms"].mean() if has_dns else 0
    avg_compute = df["compute_time_ms"].mean() if has_compute else 0

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Network & API — {len(df)} runs</div>"
        f"<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:12px;'>"
        + "".join(
            [
                f"<div><div style='font-size:18px;font-weight:700;color:{c};"
                f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
                f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
                f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
                for v, l, c in [
                    (f"{avg_api_lin:.0f}ms", "API latency linear", WF_COLORS["linear"]),
                    (
                        f"{avg_api_age:.0f}ms",
                        "API latency agentic",
                        WF_COLORS["agentic"],
                    ),
                    (f"{avg_dns:.1f}ms", "Avg DNS latency", "#60a5fa"),
                    (f"{avg_compute:.0f}ms", "Avg compute time", "#f59e0b"),
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
            f"API latency distribution</div>",
            unsafe_allow_html=True,
        )
        fig = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = df[df["workflow_type"] == wf]["api_latency_ms"].dropna()
            if sub.empty:
                continue
            fig.add_trace(
                go.Histogram(x=sub, name=wf, marker_color=clr, opacity=0.7, nbinsx=40)
            )
        fig.update_layout(
            **PL,
            height=260,
            barmode="overlay",
            xaxis_title="API latency (ms)",
            yaxis_title="Run count",
        )
        st.plotly_chart(fig, use_container_width=True, key="dm_net_hist")

    with col2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"API latency vs Energy</div>",
            unsafe_allow_html=True,
        )
        fig2 = go.Figure()
        if has_energy:
            for wf, clr in WF_COLORS.items():
                sub = df[df["workflow_type"] == wf]
                sub = sub[sub["energy_j"].notna() & (sub["energy_j"] > 0)]
                if sub.empty:
                    continue
                fig2.add_trace(
                    go.Scatter(
                        x=sub["api_latency_ms"],
                        y=sub["energy_j"],
                        mode="markers",
                        name=wf,
                        marker=dict(color=clr, size=5, opacity=0.6),
                    )
                )
        fig2.update_layout(
            **PL, height=260, xaxis_title="API latency (ms)", yaxis_title="Energy (J)"
        )
        st.plotly_chart(fig2, use_container_width=True, key="dm_net_scatter")

    # ── Latency breakdown ─────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Latency breakdown — DNS vs API vs Compute</div>",
        unsafe_allow_html=True,
    )

    agg_dict = {"api": ("api_latency_ms", "mean")}
    if has_dns:
        agg_dict["dns"] = ("dns_latency_ms", "mean")
    if has_compute:
        agg_dict["cmp"] = ("compute_time_ms", "mean")

    breakdown = df.groupby("workflow_type").agg(**agg_dict).reset_index()

    fig3 = go.Figure()
    for col_name, label, clr in [
        ("dns", "DNS", "#60a5fa"),
        ("api", "API wait", "#f59e0b"),
        ("cmp", "Compute", "#22c55e"),
    ]:
        if col_name not in breakdown.columns:
            continue
        fig3.add_trace(
            go.Bar(
                x=breakdown["workflow_type"],
                y=breakdown[col_name],
                name=label,
                marker_color=clr,
                marker_line_width=0,
            )
        )
    fig3.update_layout(**PL, height=240, barmode="stack", yaxis_title="Avg ms")
    st.plotly_chart(fig3, use_container_width=True, key="dm_net_breakdown")

    # ── Percentiles ───────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"API latency percentiles</div>",
        unsafe_allow_html=True,
    )
    rows = []
    for wf in df["workflow_type"].unique():
        sub = df[df["workflow_type"] == wf]["api_latency_ms"].dropna()
        if sub.empty:
            continue
        rows.append(
            {
                "Workflow": wf,
                "P50": round(sub.quantile(0.50), 1),
                "P75": round(sub.quantile(0.75), 1),
                "P90": round(sub.quantile(0.90), 1),
                "P95": round(sub.quantile(0.95), 1),
                "P99": round(sub.quantile(0.99), 1),
                "Max": round(sub.max(), 1),
            }
        )
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

    # ── Insight ───────────────────────────────────────────────────────────────
    latency_overhead = avg_api_age - avg_api_lin
    st.markdown(
        f"<div style='margin-top:12px;padding:10px 14px;"
        f"background:#1a0e40;border-left:3px solid {ACCENT};"
        f"border-radius:0 8px 8px 0;font-size:11px;"
        f"color:#c4b5fd;font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
        f"Agentic runs have <b>{latency_overhead:+.0f}ms</b> API latency vs linear. "
        f"During API wait the CPU idles but RAPL still records package power — "
        f"reducing API latency is one of the highest-leverage ways to cut agentic energy."
        f"</div>",
        unsafe_allow_html=True,
    )
