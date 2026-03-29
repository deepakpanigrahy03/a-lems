"""
gui/pages/research_metrics_view_page.py  —  🔬  Orchestration Metrics
─────────────────────────────────────────────────────────────────────────────
Publication-ready analysis from research_metrics_view.

Metrics computed:
  OOI_time  = orchestration_cpu_ms / total_time_ms
              (fraction of total time spent on orchestration CPU)

  OOI_cpu   = orchestration_cpu_ms / compute_time_ms
              (fraction of compute time that is orchestration overhead)

  UCR       = total_llm_compute_ms / total_time_ms
              (useful compute ratio — how much time is actual model inference)

  network_ratio = total_wait_ms / total_time_ms
                  (fraction of time waiting for network/API)

These are the paper-ready metrics from the handoff doc.
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1

ACCENT = "#38bdf8"


def render(ctx: dict) -> None:

    # ── Load from research_metrics_view ───────────────────────────────────────
    df = q("""
        SELECT
            run_id,
            provider,
            workflow_type,
            total_time_ms,
            compute_time_ms,
            orchestration_cpu_ms,
            total_bytes_sent,
            total_bytes_recv,
            total_wait_ms,
            total_llm_compute_ms,
            ooi_time,
            ooi_cpu,
            ucr,
            network_ratio
        FROM research_metrics_view
        ORDER BY run_id DESC
    """)

    if df.empty:
        st.markdown(
            f"<div style='padding:40px;text-align:center;"
            f"border:1px solid {ACCENT}33;border-radius:12px;"
            f"background:{ACCENT}08;margin-top:8px;'>"
            f"<div style='font-size:28px;margin-bottom:8px;'>🔬</div>"
            f"<div style='font-size:14px;color:{ACCENT};"
            f"font-family:IBM Plex Mono,monospace;margin-bottom:6px;'>"
            f"research_metrics_view has no data</div>"
            f"<div style='font-size:11px;color:#475569;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"The view requires llm_interactions data with "
            f"local_compute_ms and non_local_ms populated. "
            f"Check llm_interactions table — run experiments with "
            f"the interaction logger enabled.</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
        return

    n_runs = len(df)

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px;'>"
        f"Orchestration Intelligence Metrics — {n_runs} runs</div>"
        f"<div style='font-size:11px;color:#94a3b8;'>"
        f"OOI · UCR · Network Ratio — publication-ready analysis from "
        f"research_metrics_view</div></div>",
        unsafe_allow_html=True,
    )

    # ── Paper-ready results table ─────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
        f"Paper-ready results — avg by workflow × provider</div>",
        unsafe_allow_html=True,
    )

    summary = (
        df.groupby(["workflow_type", "provider"])[
            ["ooi_time", "ooi_cpu", "ucr", "network_ratio"]
        ]
        .agg(["mean", "count"])
        .round(4)
    )

    # Flat summary for display
    flat = (
        df.groupby(["workflow_type", "provider"])
        .agg(
            runs=("run_id", "count"),
            ooi_time=("ooi_time", "mean"),
            ooi_cpu=("ooi_cpu", "mean"),
            ucr=("ucr", "mean"),
            network_ratio=("network_ratio", "mean"),
        )
        .reset_index()
        .round(4)
    )
    flat.columns = [
        "Workflow", "Provider", "Runs",
        "OOI_time", "OOI_cpu", "UCR", "Network Ratio",
    ]

    # Render as styled HTML table — highlights key numbers
    rows_html = ""
    for _, r in flat.iterrows():
        wf_clr = WF_COLORS.get(r["Workflow"], "#94a3b8")
        ooi_cpu_clr = (
            "#ef4444" if float(r["OOI_cpu"]) > 0.8 else
            "#f59e0b" if float(r["OOI_cpu"]) > 0.5 else
            "#22c55e"
        )
        rows_html += (
            f"<tr style='border-bottom:1px solid #1f2937;'>"
            f"<td style='padding:9px 10px;'>"
            f"<span style='color:{wf_clr};font-weight:700;font-size:11px;'>"
            f"{r['Workflow']}</span></td>"
            f"<td style='padding:9px 10px;font-size:11px;color:#94a3b8;'>"
            f"{r['Provider']}</td>"
            f"<td style='padding:9px 10px;font-size:11px;color:#475569;'>"
            f"{r['Runs']}</td>"
            f"<td style='padding:9px 10px;font-family:monospace;font-size:12px;"
            f"color:#38bdf8;'>{r['OOI_time']:.4f}</td>"
            f"<td style='padding:9px 10px;font-family:monospace;font-size:12px;"
            f"color:{ooi_cpu_clr};font-weight:700;'>{r['OOI_cpu']:.4f}</td>"
            f"<td style='padding:9px 10px;font-family:monospace;font-size:12px;"
            f"color:#a78bfa;'>{r['UCR']:.4f}</td>"
            f"<td style='padding:9px 10px;font-family:monospace;font-size:12px;"
            f"color:#f59e0b;'>{r['Network Ratio']:.4f}</td>"
            f"</tr>"
        )

    st.markdown(
        f"<div style='background:#07090f;border:1px solid #1e2d45;"
        f"border-radius:8px;overflow:hidden;margin-bottom:16px;'>"
        f"<table style='width:100%;border-collapse:collapse;'>"
        f"<thead><tr style='background:#0a0e1a;border-bottom:2px solid #1e2d45;'>"
        f"<th style='padding:8px 10px;font-size:9px;color:#475569;text-align:left;"
        f"text-transform:uppercase;'>Workflow</th>"
        f"<th style='padding:8px 10px;font-size:9px;color:#475569;text-align:left;"
        f"text-transform:uppercase;'>Provider</th>"
        f"<th style='padding:8px 10px;font-size:9px;color:#475569;text-align:left;"
        f"text-transform:uppercase;'>Runs</th>"
        f"<th style='padding:8px 10px;font-size:9px;color:#38bdf8;text-align:left;"
        f"text-transform:uppercase;'>OOI_time</th>"
        f"<th style='padding:8px 10px;font-size:9px;color:#ef4444;text-align:left;"
        f"text-transform:uppercase;'>OOI_cpu</th>"
        f"<th style='padding:8px 10px;font-size:9px;color:#a78bfa;text-align:left;"
        f"text-transform:uppercase;'>UCR</th>"
        f"<th style='padding:8px 10px;font-size:9px;color:#f59e0b;text-align:left;"
        f"text-transform:uppercase;'>Network Ratio</th>"
        f"</tr></thead><tbody>{rows_html}</tbody></table></div>",
        unsafe_allow_html=True,
    )

    # ── Core claim banner ─────────────────────────────────────────────────────
    # Reproduce the paper-ready insight from the handoff doc
    agentic = df[df["workflow_type"] == "agentic"]
    linear  = df[df["workflow_type"] == "linear"]

    if not agentic.empty and not linear.empty:
        avg_ooi_cpu_age  = agentic["ooi_cpu"].mean()
        avg_ooi_time_age = agentic["ooi_time"].mean()
        avg_net_cloud    = df[df["provider"] == "cloud"]["network_ratio"].mean()

        st.markdown(
            f"<div style='padding:14px 18px;background:#07090f;"
            f"border:1px solid #22c55e33;border-left:4px solid #22c55e;"
            f"border-radius:8px;margin-bottom:16px;'>"
            f"<div style='font-size:10px;color:#22c55e;font-weight:700;"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Core claim — publication ready</div>"
            f"<div style='font-size:12px;color:#f1f5f9;line-height:1.8;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"While agentic workflows increase orchestration CPU usage to near 100% "
            f"(OOI_cpu ≈ <b style='color:#ef4444'>{avg_ooi_cpu_age:.2f}</b>), "
            f"this overhead accounts for only "
            f"<b style='color:#38bdf8'>{avg_ooi_time_age*100:.1f}%</b> of total execution time "
            f"(OOI_time), revealing that orchestration is CPU-intensive but time-efficient. "
            + (
                f"Cloud deployments shift the bottleneck to network latency "
                f"(<b style='color:#f59e0b'>{avg_net_cloud*100:.0f}%</b> of total time)."
                if pd.notna(avg_net_cloud) else ""
            )
            + f"</div></div>",
            unsafe_allow_html=True,
        )

    # ── Metric charts ─────────────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"OOI_cpu by workflow + provider</div>",
            unsafe_allow_html=True,
        )
        fig1 = go.Figure()
        for prov in df["provider"].dropna().unique():
            sub = df[df["provider"] == prov]
            fig1.add_trace(go.Box(
                y=sub["ooi_cpu"],
                x=sub["workflow_type"],
                name=prov,
                boxmean=True,
            ))
        fig1.update_layout(
            **PL, height=260,
            yaxis_title="OOI_cpu (orchestration / compute time)",
            boxmode="group",
        )
        st.plotly_chart(fig1, use_container_width=True, key="rm_ooi_cpu")

    with col2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"UCR vs network_ratio by provider</div>",
            unsafe_allow_html=True,
        )
        fig2 = go.Figure()
        for prov in df["provider"].dropna().unique():
            sub = df[df["provider"] == prov]
            clr = "#22c55e" if prov == "local" else "#3b82f6"
            fig2.add_trace(go.Scatter(
                x=sub["ucr"],
                y=sub["network_ratio"],
                mode="markers",
                name=prov,
                marker=dict(color=clr, size=5, opacity=0.6),
            ))
        fig2.update_layout(
            **PL, height=260,
            xaxis_title="UCR (useful compute ratio)",
            yaxis_title="Network ratio",
        )
        st.plotly_chart(fig2, use_container_width=True, key="rm_ucr_net")

    # ── OOI time breakdown stacked bar ────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Time breakdown — where does execution time go?</div>",
        unsafe_allow_html=True,
    )

    time_agg = (
        df.groupby(["workflow_type", "provider"])
        .agg(
            orchestration=("orchestration_cpu_ms", "mean"),
            llm_compute=("total_llm_compute_ms",   "mean"),
            network_wait=("total_wait_ms",          "mean"),
        )
        .reset_index()
    )
    time_agg["other"] = (
        df.groupby(["workflow_type", "provider"])["total_time_ms"].mean().values
        - time_agg["orchestration"]
        - time_agg["llm_compute"]
        - time_agg["network_wait"]
    ).clip(lower=0)

    time_agg["label"] = time_agg["workflow_type"] + " · " + time_agg["provider"]

    fig3 = go.Figure()
    for col_n, label, clr in [
        ("orchestration", "Orchestration CPU", "#ef4444"),
        ("llm_compute",   "LLM compute",       "#3b82f6"),
        ("network_wait",  "Network wait",      "#f59e0b"),
        ("other",         "Other",             "#475569"),
    ]:
        fig3.add_trace(go.Bar(
            x=time_agg["label"],
            y=time_agg[col_n],
            name=label,
            marker_color=clr,
            marker_line_width=0,
        ))
    fig3.update_layout(
        **PL, height=280, barmode="stack",
        yaxis_title="Avg milliseconds",
        showlegend=True,
    )
    st.plotly_chart(fig3, use_container_width=True, key="rm_time_breakdown")

    # ── Raw data table ────────────────────────────────────────────────────────
    with st.expander(f"Raw data — {n_runs} runs"):
        show_cols = [
            "run_id", "workflow_type", "provider",
            "ooi_time", "ooi_cpu", "ucr", "network_ratio",
            "total_time_ms", "orchestration_cpu_ms",
            "total_llm_compute_ms", "total_wait_ms",
        ]
        st.dataframe(
            df[show_cols].round(4),
            use_container_width=True, hide_index=True,
        )
