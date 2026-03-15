"""
gui/pages/baseline.py  —  ⊟  Baseline & Idle
─────────────────────────────────────────────────────────────────────────────
Idle baseline drift, governor state, turbo on/off energy impact.
5 idle_baselines records. Foundation for all dynamic energy calculations.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

from gui.db     import q, q1
from gui.config import PL, WF_COLORS

ACCENT = "#f59e0b"


def render(ctx: dict) -> None:
    baselines = q("""
        SELECT baseline_id, timestamp, package_power_watts,
               core_power_watts, uncore_power_watts, dram_power_watts,
               duration_seconds, sample_count,
               package_std, core_std, uncore_std, dram_std,
               governor, turbo, background_cpu, process_count, method
        FROM idle_baselines
        ORDER BY timestamp DESC
    """)

    if baselines.empty:
        st.info("No idle baselines recorded yet.")
        return

    # ── Header ────────────────────────────────────────────────────────────────
    latest = baselines.iloc[0]
    pkg_w  = latest.get("package_power_watts") or 0
    core_w = latest.get("core_power_watts") or 0

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Idle Baselines — {len(baselines)} recorded</div>"
        f"<div style='display:grid;grid-template-columns:repeat(5,1fr);gap:12px;'>"
        + "".join([
            f"<div><div style='font-size:18px;font-weight:700;color:{c};"
            f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
            f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
            for v, l, c in [
                (f"{pkg_w:.2f}W",    "Latest pkg idle",    ACCENT),
                (f"{core_w:.2f}W",   "Latest core idle",   "#22c55e"),
                (str(latest.get("governor","?")), "Governor", "#60a5fa"),
                (str(latest.get("turbo","?")),    "Turbo",    "#a78bfa"),
                (f"{latest.get('background_cpu',0):.1f}%", "BG CPU", "#94a3b8"),
            ]
        ])
        + "</div></div>",
        unsafe_allow_html=True)

    # ── Baseline drift chart ──────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
        f"Baseline power drift over time</div>",
        unsafe_allow_html=True)

    bl = baselines.sort_values("timestamp").copy()
    bl["ts_fmt"] = bl["timestamp"].apply(
        lambda x: datetime.fromtimestamp(float(x)).strftime("%m/%d %H:%M")
        if x else "?")

    fig = go.Figure()
    for col_n, label, clr in [
        ("package_power_watts", "Package", ACCENT),
        ("core_power_watts",    "Core",    "#22c55e"),
        ("uncore_power_watts",  "Uncore",  "#3b82f6"),
        ("dram_power_watts",    "DRAM",    "#a78bfa"),
    ]:
        sub = bl[bl[col_n].notna()]
        if sub.empty: continue
        fig.add_trace(go.Scatter(
            x=sub["ts_fmt"], y=sub[col_n],
            mode="lines+markers", name=label,
            line=dict(width=2, color=clr),
            marker=dict(size=8, color=clr)))
    fig.update_layout(**PL, height=280,
                      xaxis_title="Baseline time",
                      yaxis_title="Idle power (W)")
    st.plotly_chart(fig, use_container_width=True, key="bl_drift_chart")

    # ── Stability check — std dev ─────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Measurement stability — std deviation</div>",
        unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        fig2 = go.Figure()
        for col_n, label, clr in [
            ("package_std", "Package σ", ACCENT),
            ("core_std",    "Core σ",    "#22c55e"),
            ("uncore_std",  "Uncore σ",  "#3b82f6"),
        ]:
            sub = bl[bl[col_n].notna()]
            if sub.empty: continue
            fig2.add_trace(go.Bar(
                x=sub["ts_fmt"], y=sub[col_n],
                name=label, marker_color=clr, marker_line_width=0))
        fig2.update_layout(**PL, height=220, barmode="group",
                           yaxis_title="Std dev (W)")
        st.plotly_chart(fig2, use_container_width=True, key="bl_std_bar")

    with col2:
        # Background CPU at baseline time
        fig3 = go.Figure(go.Bar(
            x=bl["ts_fmt"],
            y=bl["background_cpu"].fillna(0),
            marker_color=[
                "#ef4444" if v > 5 else "#22c55e"
                for v in bl["background_cpu"].fillna(0)
            ],
            marker_line_width=0,
        ))
        fig3.add_hline(y=5, line_dash="dot", line_color="#f59e0b",
                       line_width=1)
        fig3.update_layout(**PL, height=220,
                           yaxis_title="Background CPU %",
                           showlegend=False)
        st.plotly_chart(fig3, use_container_width=True, key="bl_bg_cpu")

    # ── Baseline detail table ─────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Full baseline records</div>",
        unsafe_allow_html=True)

    display = baselines[[
        "baseline_id", "package_power_watts", "core_power_watts",
        "uncore_power_watts", "dram_power_watts",
        "governor", "turbo", "background_cpu", "duration_seconds",
        "sample_count", "method"
    ]].copy()
    display.columns = [
        "ID", "Pkg (W)", "Core (W)", "Uncore (W)", "DRAM (W)",
        "Governor", "Turbo", "BG CPU%", "Duration(s)", "Samples", "Method"
    ]
    st.dataframe(display.round(4), use_container_width=True)

    # ── How baselines affect energy calculations ───────────────────────────────
    st.markdown(
        f"<div style='margin-top:16px;padding:10px 14px;"
        f"background:#1a1000;border-left:3px solid {ACCENT};"
        f"border-radius:0 8px 8px 0;font-size:11px;"
        f"color:#fcd34d;font-family:IBM Plex Mono,monospace;line-height:1.8;'>"
        f"<b>How baselines work:</b> dynamic_energy = total_energy − (idle_power × duration). "
        f"A higher idle baseline reduces the apparent dynamic energy. "
        f"If your baseline drifts significantly between sessions, "
        f"cross-session energy comparisons become unreliable. "
        f"Recalibrate baselines at the start of each measurement session."
        f"</div>",
        unsafe_allow_html=True)

    # ── Runs per baseline ──────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Runs using each baseline</div>",
        unsafe_allow_html=True)

    bl_usage = q("""
        SELECT baseline_id, COUNT(*) AS run_count,
               AVG(total_energy_uj/1e6) AS avg_energy_j
        FROM runs
        WHERE baseline_id IS NOT NULL
        GROUP BY baseline_id
        ORDER BY run_count DESC
    """)
    if not bl_usage.empty:
        fig4 = go.Figure(go.Bar(
            x=bl_usage["baseline_id"],
            y=bl_usage["run_count"],
            marker_color=ACCENT, marker_line_width=0))
        fig4.update_layout(**PL, height=200,
                           xaxis_title="Baseline ID",
                           yaxis_title="Runs using this baseline",
                           showlegend=False)
        st.plotly_chart(fig4, use_container_width=True, key="bl_usage_bar")
