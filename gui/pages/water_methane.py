"""
gui/pages/water_methane.py  —  💧  Water & Methane
─────────────────────────────────────────────────────────────────────────────
Water and methane footprint of AI workloads.
1,194/1,204 runs have water_ml and methane_mg data.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.db     import q, q1
from gui.config import PL, WF_COLORS

ACCENT = "#34d399"

# Reference values
_WATER_BOTTLE_ML  = 500
_BABY_FEED_ML     = 200
_METHANE_FRIDGE_MG = 900  # approximate mg methane equivalent per hour of fridge operation


def render(ctx: dict) -> None:
    runs = ctx.get("runs", pd.DataFrame())

    if runs.empty:
        st.info("No run data available.")
        return

    df = runs.copy()
    has_water   = "water_ml" in df.columns and df["water_ml"].notna().any()
    has_methane = "methane_mg" in df.columns and df["methane_mg"].notna().any()

    if not has_water and not has_methane:
        st.info("No water or methane data in runs yet.")
        return

    df_w = df[df["water_ml"].notna() & (df["water_ml"] > 0)]   if has_water   else pd.DataFrame()
    df_m = df[df["methane_mg"].notna() & (df["methane_mg"] > 0)] if has_methane else pd.DataFrame()

    total_water   = df_w["water_ml"].sum()   if not df_w.empty else 0
    total_methane = df_m["methane_mg"].sum() if not df_m.empty else 0
    avg_water     = df_w["water_ml"].mean()  if not df_w.empty else 0
    avg_methane   = df_m["methane_mg"].mean() if not df_m.empty else 0

    # Human-readable equivalents
    bottles = total_water / _WATER_BOTTLE_ML
    baby_feeds = total_water / _BABY_FEED_ML

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Water & Methane Footprint</div>"
        f"<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:12px;'>"
        + "".join([
            f"<div><div style='font-size:18px;font-weight:700;color:{c};"
            f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
            f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
            for v, l, c in [
                (f"{total_water:.0f}ml",   "Total water",        "#38bdf8"),
                (f"{avg_water:.2f}ml",     "Avg per run",        "#38bdf8"),
                (f"{total_methane:.1f}mg", "Total methane",      "#f59e0b"),
                (f"{avg_methane:.3f}mg",   "Avg methane/run",    "#f59e0b"),
            ]
        ])
        + "</div>"
        + f"<div style='margin-top:8px;font-size:10px;color:#475569;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"Equivalent to {bottles:.1f} water bottles · {baby_feeds:.0f} baby feeds"
        f"</div></div>",
        unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["💧 Water", "🔥 Methane"])

    with tab1:
        if df_w.empty:
            st.info("No water data yet.")
        else:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:#38bdf8;"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"Water consumption by workflow</div>", unsafe_allow_html=True)
                fig = go.Figure()
                for wf, clr in WF_COLORS.items():
                    sub = df_w[df_w["workflow_type"] == wf]["water_ml"].dropna()
                    if sub.empty: continue
                    fig.add_trace(go.Box(
                        y=sub, name=wf,
                        marker_color=clr, line_color=clr, boxmean=True))
                fig.update_layout(**PL, height=260,
                                  yaxis_title="Water (ml)",
                                  showlegend=False)
                st.plotly_chart(fig, use_container_width=True, key="wm_water_box")

            with col2:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:#38bdf8;"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"Water vs Energy correlation</div>", unsafe_allow_html=True)
                fig2 = go.Figure()
                if "energy_j" in df_w.columns:
                    for wf, clr in WF_COLORS.items():
                        sub = df_w[df_w["workflow_type"] == wf]
                        sub = sub[sub["energy_j"].notna() & (sub["energy_j"] > 0)]
                        if sub.empty: continue
                        fig2.add_trace(go.Scatter(
                            x=sub["energy_j"], y=sub["water_ml"],
                            mode="markers", name=wf,
                            marker=dict(color=clr, size=5, opacity=0.6)))
                fig2.update_layout(**PL, height=260,
                                   xaxis_title="Energy (J)",
                                   yaxis_title="Water (ml)")
                st.plotly_chart(fig2, use_container_width=True, key="wm_water_scatter")

            # Water by task
            if "task_name" in df_w.columns:
                task_water = df_w.groupby(["task_name","workflow_type"])["water_ml"] \
                                 .mean().reset_index()
                fig3 = go.Figure()
                for wf, clr in WF_COLORS.items():
                    sub = task_water[task_water["workflow_type"] == wf]
                    if sub.empty: continue
                    fig3.add_trace(go.Bar(
                        x=sub["task_name"], y=sub["water_ml"],
                        name=wf, marker_color=clr, marker_line_width=0))
                fig3.update_layout(**PL, height=240, barmode="group",
                                   xaxis_title="Task",
                                   yaxis_title="Avg water (ml)",
                                   xaxis_tickangle=-30)
                st.plotly_chart(fig3, use_container_width=True, key="wm_water_task")

    with tab2:
        if df_m.empty:
            st.info("No methane data yet.")
        else:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:#f59e0b;"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"Methane by workflow</div>", unsafe_allow_html=True)
                fig4 = go.Figure()
                for wf, clr in WF_COLORS.items():
                    sub = df_m[df_m["workflow_type"] == wf]["methane_mg"].dropna()
                    if sub.empty: continue
                    fig4.add_trace(go.Box(
                        y=sub, name=wf,
                        marker_color=clr, line_color=clr, boxmean=True))
                fig4.update_layout(**PL, height=260,
                                   yaxis_title="Methane (mg)",
                                   showlegend=False)
                st.plotly_chart(fig4, use_container_width=True, key="wm_methane_box")

            with col2:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:#f59e0b;"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"Methane vs Energy</div>", unsafe_allow_html=True)
                fig5 = go.Figure()
                if "energy_j" in df_m.columns:
                    for wf, clr in WF_COLORS.items():
                        sub = df_m[df_m["workflow_type"] == wf]
                        sub = sub[sub["energy_j"].notna() & (sub["energy_j"] > 0)]
                        if sub.empty: continue
                        fig5.add_trace(go.Scatter(
                            x=sub["energy_j"], y=sub["methane_mg"],
                            mode="markers", name=wf,
                            marker=dict(color=clr, size=5, opacity=0.6)))
                fig5.update_layout(**PL, height=260,
                                   xaxis_title="Energy (J)",
                                   yaxis_title="Methane (mg)")
                st.plotly_chart(fig5, use_container_width=True, key="wm_methane_scatter")

            # Methane by task
            if "task_name" in df_m.columns:
                task_methane = df_m.groupby(["task_name","workflow_type"])["methane_mg"] \
                                   .mean().reset_index()
                fig6 = go.Figure()
                for wf, clr in WF_COLORS.items():
                    sub = task_methane[task_methane["workflow_type"] == wf]
                    if sub.empty: continue
                    fig6.add_trace(go.Bar(
                        x=sub["task_name"], y=sub["methane_mg"],
                        name=wf, marker_color=clr, marker_line_width=0))
                fig6.update_layout(**PL, height=240, barmode="group",
                                   xaxis_title="Task",
                                   yaxis_title="Avg methane (mg)",
                                   xaxis_tickangle=-30)
                st.plotly_chart(fig6, use_container_width=True, key="wm_methane_task")
