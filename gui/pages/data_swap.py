"""
gui/pages/data_swap.py  —  ⇅  Swap & Paging
Memory pressure and swap activity impact on energy.
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.config import PL, WF_COLORS
from gui.pages._dm_helpers import get_runs, no_data_banner

ACCENT = "#a78bfa"


def render(ctx: dict) -> None:
    df = get_runs(ctx)

    if df.empty:
        no_data_banner("No runs in database yet.", ACCENT)
        return

    if "swap_end_used_mb" not in df.columns:
        no_data_banner("swap_end_used_mb not collected yet — check scheduler_monitor.py.", ACCENT)
        return

    df = df[df["swap_end_used_mb"].notna()].copy()
    if df.empty:
        no_data_banner("No swap data in runs yet.", ACCENT)
        return

    has_energy       = "energy_j" in df.columns
    has_swap_start   = "swap_start_used_mb" in df.columns
    has_swap_pct     = "swap_end_percent" in df.columns

    df_swapped   = df[df["swap_end_used_mb"] > 0]
    swap_pct_val = round(len(df_swapped) / len(df) * 100, 1) if len(df) > 0 else 0

    lin = df[df["workflow_type"] == "linear"]
    age = df[df["workflow_type"] == "agentic"]
    avg_swap_lin = lin["swap_end_used_mb"].mean() if not lin.empty else 0
    avg_swap_age = age["swap_end_used_mb"].mean() if not age.empty else 0

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Swap & Paging — {len(df)} runs</div>"
        f"<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:12px;'>"
        + "".join([
            f"<div><div style='font-size:18px;font-weight:700;color:{c};"
            f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
            f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
            for v, l, c in [
                (f"{len(df_swapped)}",     "Runs with swap",     "#f59e0b"),
                (f"{swap_pct_val}%",       "Swap usage rate",    "#f59e0b"),
                (f"{avg_swap_lin:.1f} MB", "Avg swap linear",    WF_COLORS["linear"]),
                (f"{avg_swap_age:.1f} MB", "Avg swap agentic",   WF_COLORS["agentic"]),
            ]
        ])
        + "</div></div>",
        unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Swap used vs Energy</div>", unsafe_allow_html=True)
        fig = go.Figure()
        if has_energy:
            for wf, clr in WF_COLORS.items():
                sub = df[df["workflow_type"] == wf]
                sub = sub[sub["energy_j"].notna() & (sub["energy_j"] > 0)]
                if sub.empty: continue
                fig.add_trace(go.Scatter(
                    x=sub["swap_end_used_mb"], y=sub["energy_j"],
                    mode="markers", name=wf,
                    marker=dict(color=clr, size=5, opacity=0.6)))
        fig.update_layout(**PL, height=260,
                          xaxis_title="Swap used at end (MB)",
                          yaxis_title="Energy (J)")
        st.plotly_chart(fig, use_container_width=True, key="dm_swap_scatter")

    with col2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Swap usage distribution</div>", unsafe_allow_html=True)
        fig2 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = df[df["workflow_type"] == wf]["swap_end_used_mb"].dropna()
            if sub.empty: continue
            fig2.add_trace(go.Histogram(
                x=sub, name=wf, marker_color=clr, opacity=0.7, nbinsx=30))
        fig2.update_layout(**PL, height=260, barmode="overlay",
                           xaxis_title="Swap used (MB)",
                           yaxis_title="Run count")
        st.plotly_chart(fig2, use_container_width=True, key="dm_swap_hist")

    # ── Swap delta ────────────────────────────────────────────────────────────
    if has_swap_start:
        df["swap_delta"] = df["swap_end_used_mb"] - df["swap_start_used_mb"]
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            f"Swap delta (end − start) — memory pressure during run</div>",
            unsafe_allow_html=True)
        fig3 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = df[df["workflow_type"] == wf]["swap_delta"].dropna()
            if sub.empty: continue
            fig3.add_trace(go.Box(
                y=sub, name=wf,
                marker_color=clr, line_color=clr, boxmean=True))
        fig3.add_hline(y=0, line_dash="dot", line_color="#475569", line_width=1)
        fig3.update_layout(**PL, height=240,
                           yaxis_title="Swap delta (MB)", showlegend=False)
        st.plotly_chart(fig3, use_container_width=True, key="dm_swap_delta")

    # ── Energy by swap tier ───────────────────────────────────────────────────
    if has_energy:
        df["swap_tier"] = pd.cut(
            df["swap_end_used_mb"],
            bins=[-1, 0, 100, 500, float("inf")],
            labels=["None", "Light <100MB", "Med <500MB", "Heavy >500MB"])
        tier_e = df.groupby(
            ["swap_tier", "workflow_type"], observed=True)["energy_j"] \
            .mean().reset_index()
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            f"Avg energy by swap pressure tier</div>", unsafe_allow_html=True)
        fig4 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = tier_e[tier_e["workflow_type"] == wf]
            if sub.empty: continue
            fig4.add_trace(go.Bar(
                x=sub["swap_tier"], y=sub["energy_j"],
                name=wf, marker_color=clr, marker_line_width=0))
        fig4.update_layout(**PL, height=240, barmode="group",
                           yaxis_title="Avg energy (J)")
        st.plotly_chart(fig4, use_container_width=True, key="dm_swap_tier")
