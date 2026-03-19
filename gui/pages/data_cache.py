"""
gui/pages/data_cache.py  —  ◧  Cache & Memory
Energy cost of cache misses and memory pressure.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.pages._dm_helpers import col_exists, get_runs, no_data_banner

ACCENT = "#a78bfa"


def render(ctx: dict) -> None:
    df = get_runs(ctx)

    if df.empty:
        no_data_banner("No runs in database yet.", ACCENT)
        return

    # Only keep rows with valid cache data
    if "cache_miss_rate" not in df.columns:
        no_data_banner("cache_miss_rate not collected yet.", ACCENT)
        return

    df = df[df["cache_miss_rate"].notna() & (df["cache_miss_rate"] > 0)].copy()
    if df.empty:
        no_data_banner("No runs with cache miss rate data yet.", ACCENT)
        return

    lin = df[df["workflow_type"] == "linear"]
    age = df[df["workflow_type"] == "agentic"]

    avg_cmr_lin = lin["cache_miss_rate"].mean() if not lin.empty else 0
    avg_cmr_age = age["cache_miss_rate"].mean() if not age.empty else 0
    avg_ipc_lin = lin["ipc"].mean() if not lin.empty and "ipc" in lin.columns else 0
    avg_ipc_age = age["ipc"].mean() if not age.empty and "ipc" in age.columns else 0
    avg_rss = df["rss_memory_mb"].mean() if "rss_memory_mb" in df.columns else 0

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Cache & Memory — {len(df)} runs</div>"
        f"<div style='display:grid;grid-template-columns:repeat(5,1fr);gap:12px;'>"
        + "".join(
            [
                f"<div><div style='font-size:18px;font-weight:700;color:{c};"
                f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
                f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
                f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
                for v, l, c in [
                    (f"{avg_cmr_lin:.3f}", "Cache miss linear", WF_COLORS["linear"]),
                    (f"{avg_cmr_age:.3f}", "Cache miss agentic", WF_COLORS["agentic"]),
                    (f"{avg_ipc_lin:.2f}", "IPC linear", WF_COLORS["linear"]),
                    (f"{avg_ipc_age:.2f}", "IPC agentic", WF_COLORS["agentic"]),
                    (f"{avg_rss:.0f} MB", "Avg RSS memory", ACCENT),
                ]
            ]
        )
        + "</div></div>",
        unsafe_allow_html=True,
    )

    # ── Cache miss rate vs Energy ─────────────────────────────────────────────
    col1, col2 = st.columns(2)
    has_energy = "energy_j" in df.columns

    with col1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Cache miss rate vs Energy</div>",
            unsafe_allow_html=True,
        )
        fig = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = df[df["workflow_type"] == wf]
            if sub.empty or not has_energy:
                continue
            sub = sub[sub["energy_j"].notna() & (sub["energy_j"] > 0)]
            fig.add_trace(
                go.Scatter(
                    x=sub["cache_miss_rate"],
                    y=sub["energy_j"],
                    mode="markers",
                    name=wf,
                    marker=dict(color=clr, size=5, opacity=0.6),
                )
            )
        fig.update_layout(
            **PL, height=280, xaxis_title="Cache miss rate", yaxis_title="Energy (J)"
        )
        st.plotly_chart(fig, use_container_width=True, key="dm_cache_scatter")

    with col2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"IPC vs Energy</div>",
            unsafe_allow_html=True,
        )
        fig2 = go.Figure()
        if col_exists(df, "ipc", "energy_j"):
            for wf, clr in WF_COLORS.items():
                sub = df[df["workflow_type"] == wf]
                sub = sub[
                    sub["ipc"].notna()
                    & (sub["ipc"] > 0)
                    & sub["energy_j"].notna()
                    & (sub["energy_j"] > 0)
                ]
                if sub.empty:
                    continue
                fig2.add_trace(
                    go.Scatter(
                        x=sub["ipc"],
                        y=sub["energy_j"],
                        mode="markers",
                        name=wf,
                        marker=dict(color=clr, size=5, opacity=0.6),
                    )
                )
        fig2.update_layout(
            **PL, height=280, xaxis_title="IPC", yaxis_title="Energy (J)"
        )
        st.plotly_chart(fig2, use_container_width=True, key="dm_ipc_scatter")

    # ── Distribution ──────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Cache miss rate distribution</div>",
        unsafe_allow_html=True,
    )
    fig3 = go.Figure()
    for wf, clr in WF_COLORS.items():
        sub = df[df["workflow_type"] == wf]["cache_miss_rate"].dropna()
        if sub.empty:
            continue
        fig3.add_trace(
            go.Histogram(x=sub, name=wf, marker_color=clr, opacity=0.7, nbinsx=40)
        )
    fig3.update_layout(
        **PL,
        height=220,
        barmode="overlay",
        xaxis_title="Cache miss rate",
        yaxis_title="Run count",
    )
    st.plotly_chart(fig3, use_container_width=True, key="dm_cache_hist")

    # ── RSS Memory ────────────────────────────────────────────────────────────
    col3, col4 = st.columns(2)
    with col3:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"RSS memory by workflow</div>",
            unsafe_allow_html=True,
        )
        if "rss_memory_mb" in df.columns:
            mem_stats = (
                df.groupby("workflow_type")["rss_memory_mb"]
                .agg(["mean", "median", "max"])
                .reset_index()
            )
            fig4 = go.Figure()
            for stat, clr in [
                ("mean", "#3b82f6"),
                ("median", "#22c55e"),
                ("max", "#ef4444"),
            ]:
                fig4.add_trace(
                    go.Bar(
                        x=mem_stats["workflow_type"],
                        y=mem_stats[stat],
                        name=stat,
                        marker_color=clr,
                        marker_line_width=0,
                    )
                )
            fig4.update_layout(
                **PL, height=220, barmode="group", yaxis_title="RSS (MB)"
            )
            st.plotly_chart(fig4, use_container_width=True, key="dm_rss_bar")
        else:
            st.info("rss_memory_mb not in dataset.")

    with col4:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Energy by cache efficiency tier</div>",
            unsafe_allow_html=True,
        )
        if has_energy:
            df["cache_tier"] = pd.cut(
                df["cache_miss_rate"],
                bins=[0, 0.3, 0.5, 1.0],
                labels=["Low <30%", "Med 30-50%", "High >50%"],
            )
            tier_e = (
                df.groupby(["cache_tier", "workflow_type"], observed=True)["energy_j"]
                .mean()
                .reset_index()
            )
            fig5 = go.Figure()
            for wf, clr in WF_COLORS.items():
                sub = tier_e[tier_e["workflow_type"] == wf]
                if sub.empty:
                    continue
                fig5.add_trace(
                    go.Bar(
                        x=sub["cache_tier"],
                        y=sub["energy_j"],
                        name=wf,
                        marker_color=clr,
                        marker_line_width=0,
                    )
                )
            fig5.update_layout(
                **PL, height=220, barmode="group", yaxis_title="Avg energy (J)"
            )
            st.plotly_chart(fig5, use_container_width=True, key="dm_tier_energy")

    # ── Insight ───────────────────────────────────────────────────────────────
    cmr_delta = avg_cmr_age - avg_cmr_lin
    st.markdown(
        f"<div style='margin-top:8px;padding:10px 14px;"
        f"background:#1a0e40;border-left:3px solid {ACCENT};"
        f"border-radius:0 8px 8px 0;font-size:11px;"
        f"color:#c4b5fd;font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
        f"Agentic runs have <b>{abs(cmr_delta):.4f} "
        f"{'higher' if cmr_delta > 0 else 'lower'}</b> cache miss rate than linear. "
        f"Higher cache miss rates force expensive DRAM accesses, increasing energy.</div>",
        unsafe_allow_html=True,
    )
