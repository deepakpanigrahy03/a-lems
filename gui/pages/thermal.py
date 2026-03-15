"""
gui/pages/thermal.py  —  🌡  Thermal Analysis
─────────────────────────────────────────────────────────────────────────────
Per-run temperature time series, throttle events, thermal delta.
Uses thermal_samples table (24,729 rows) with all_zones_json.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json

from gui.db     import q, q1
from gui.config import PL, WF_COLORS, STATUS_COLORS

ACCENT = "#f59e0b"


def render(ctx: dict) -> None:
    runs = ctx.get("runs", pd.DataFrame())

    total_samples  = q1("SELECT COUNT(*) AS n FROM thermal_samples").get("n", 0) or 0
    throttle_count = q1("SELECT COUNT(*) AS n FROM thermal_samples WHERE throttle_event=1").get("n", 0) or 0
    affected_runs  = q1("SELECT COUNT(DISTINCT run_id) AS n FROM thermal_samples WHERE throttle_event=1").get("n", 0) or 0

    # ── Header ────────────────────────────────────────────────────────────────
    throttle_pct = round(throttle_count / total_samples * 100, 1) if total_samples else 0
    health_clr = "#22c55e" if throttle_pct < 10 else "#f59e0b" if throttle_pct < 50 else "#ef4444"

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Thermal Analysis — {total_samples:,} samples</div>"
        f"<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:12px;'>"
        + "".join([
            f"<div><div style='font-size:18px;font-weight:700;color:{c};"
            f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
            f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
            for v, l, c in [
                (f"{total_samples:,}",    "Total samples",      ACCENT),
                (f"{throttle_count:,}",   "Throttle events",    "#ef4444"),
                (f"{affected_runs}",      "Affected runs",      "#f97316"),
                (f"{throttle_pct}%",      "Throttle rate",      health_clr),
            ]
        ])
        + "</div></div>",
        unsafe_allow_html=True)

    # Alert if ALL samples are throttled
    if throttle_pct > 90:
        st.markdown(
            f"<div style='padding:10px 14px;background:#2a0c0c;"
            f"border-left:3px solid #ef4444;border-radius:0 8px 8px 0;"
            f"font-size:11px;color:#fca5a5;"
            f"font-family:IBM Plex Mono,monospace;margin-bottom:12px;line-height:1.7;'>"
            f"⚠ <b>{throttle_pct}% of thermal samples have throttle_event=1.</b> "
            f"This likely means the throttle_event field records a sensor state "
            f"(e.g. always-on flag) rather than actual throttling. "
            f"Check your thermal sensor collection logic. "
            f"Cross-reference with thermal_throttle_flag in runs table.</div>",
            unsafe_allow_html=True)

    # ── Run-level thermal stats ───────────────────────────────────────────────
    if not runs.empty and "package_temp_celsius" in runs.columns:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Package temp distribution by workflow</div>",
                unsafe_allow_html=True)
            df_t = runs[runs["package_temp_celsius"].notna() &
                        (runs["package_temp_celsius"] > 0)]
            fig = go.Figure()
            for wf, clr in WF_COLORS.items():
                sub = df_t[df_t["workflow_type"] == wf]["package_temp_celsius"].dropna()
                if sub.empty: continue
                fig.add_trace(go.Box(
                    y=sub, name=wf,
                    marker_color=clr, line_color=clr, boxmean=True))
            fig.update_layout(**PL, height=260,
                              yaxis_title="Package temp (°C)",
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True, key="th_temp_box")

        with col2:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Thermal delta vs Energy</div>",
                unsafe_allow_html=True)
            fig2 = go.Figure()
            if "thermal_delta_c" in runs.columns and "energy_j" in runs.columns:
                for wf, clr in WF_COLORS.items():
                    sub = runs[runs["workflow_type"] == wf]
                    sub = sub[sub["thermal_delta_c"].notna() &
                              sub["energy_j"].notna() &
                              (sub["energy_j"] > 0)]
                    if sub.empty: continue
                    fig2.add_trace(go.Scatter(
                        x=sub["thermal_delta_c"], y=sub["energy_j"],
                        mode="markers", name=wf,
                        marker=dict(color=clr, size=5, opacity=0.6)))
            fig2.update_layout(**PL, height=260,
                               xaxis_title="Thermal delta (°C)",
                               yaxis_title="Energy (J)")
            st.plotly_chart(fig2, use_container_width=True, key="th_delta_scatter")

    # ── Time-series drilldown ─────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Time-series drilldown — thermal zones per run</div>",
        unsafe_allow_html=True)

    run_ids = q("""
        SELECT DISTINCT run_id FROM thermal_samples
        ORDER BY run_id DESC LIMIT 100
    """).get("run_id", pd.Series()).tolist()

    if not run_ids:
        st.info("No thermal sample data yet.")
        return

    sel_run = st.selectbox("Select run", run_ids, key="th_run_sel",
                           format_func=lambda x: f"Run {x}")

    samples = q(f"""
        SELECT timestamp_ns/1e9 AS time_s,
               cpu_temp, system_temp, wifi_temp,
               throttle_event, all_zones_json, sensor_count
        FROM thermal_samples
        WHERE run_id = {int(sel_run)}
        ORDER BY timestamp_ns
    """)

    if samples.empty:
        st.info("No samples for this run.")
        return

    samples["time_s"] -= samples["time_s"].min()

    # Parse JSON zones for first row to show available sensors
    first_json = samples["all_zones_json"].dropna().iloc[0] if samples["all_zones_json"].notna().any() else "{}"
    try:
        zones = json.loads(first_json)
        zone_keys = list(zones.keys())
    except Exception:
        zone_keys = []

    fig3 = go.Figure()
    # Core temp traces
    for col_n, label, clr in [
        ("cpu_temp",    "CPU Package",  "#ef4444"),
        ("system_temp", "System",       "#3b82f6"),
        ("wifi_temp",   "WiFi",         "#22c55e"),
    ]:
        sub = samples[samples[col_n].notna() & (samples[col_n] > -100)]
        if sub.empty: continue
        fig3.add_trace(go.Scatter(
            x=sub["time_s"], y=sub[col_n],
            mode="lines", name=label,
            line=dict(width=1.5, color=clr)))

    # Throttle event markers
    throttled = samples[samples["throttle_event"] == 1]
    if not throttled.empty:
        fig3.add_trace(go.Scatter(
            x=throttled["time_s"],
            y=throttled["cpu_temp"].fillna(50),
            mode="markers", name="Throttle event",
            marker=dict(color="#ef4444", size=8, symbol="x")))

    fig3.update_layout(**PL, height=300,
                       xaxis_title="Time (s)",
                       yaxis_title="Temperature (°C)")
    st.plotly_chart(fig3, use_container_width=True, key=f"th_ts_{sel_run}")

    # Show JSON zones table
    if zone_keys:
        st.markdown(
            f"<div style='font-size:10px;color:#475569;"
            f"font-family:IBM Plex Mono,monospace;margin-top:4px;'>"
            f"Sensors available: {', '.join(zone_keys)}</div>",
            unsafe_allow_html=True)

    # Stats for this run
    stat_cols = st.columns(4)
    for col, (val, label) in zip(stat_cols, [
        (f"{samples['cpu_temp'].max():.1f}°C", "Max CPU temp"),
        (f"{samples['cpu_temp'].min():.1f}°C", "Min CPU temp"),
        (f"{samples['cpu_temp'].mean():.1f}°C","Avg CPU temp"),
        (f"{len(throttled)}",                  "Throttle events"),
    ]):
        with col:
            st.metric(label, val)
