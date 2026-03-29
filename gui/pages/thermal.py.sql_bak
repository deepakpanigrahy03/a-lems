"""
gui/pages/thermal.py  —  🌡  Thermal Analysis
─────────────────────────────────────────────────────────────────────────────
Per-run temperature time series, throttle events, thermal delta.
Uses thermal_samples table (24,729 rows) with all_zones_json.

Phase 3 addition:
  • Parse all_zones_json across ALL runs — not just selected run
  • SEN1-4 zone trend analysis across workflow types
  • wifi_temp vs api_latency correlation (interesting pattern from handoff)
  • Thermal zone heatmap — which zones run hot on which tasks
  • Cross-run zone statistics
─────────────────────────────────────────────────────────────────────────────
"""

import json

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, STATUS_COLORS, WF_COLORS
from gui.db import q, q1

ACCENT = "#f59e0b"

# Zone names we expect in all_zones_json — expand as needed
_KNOWN_ZONES = ["SEN1", "SEN2", "SEN3", "SEN4", "TCPU", "wifi_temp",
                "cpu_temp", "system_temp", "acpitz"]


def _parse_zones_bulk(limit: int = 5000) -> pd.DataFrame:
    """
    Load a sample of thermal_samples rows and parse all_zones_json.
    Returns a flat DataFrame with one row per (run_id, timestamp, zone, temp).
    Limited to avoid loading all 24k+ rows at once.
    """
    raw = q(f"""
        SELECT
            ts.run_id,
            ts.timestamp_ns / 1e9  AS time_s,
            ts.cpu_temp,
            ts.wifi_temp,
            ts.all_zones_json,
            ts.throttle_event,
            r.workflow_type,
            e.task_name,
            e.provider
        FROM thermal_samples ts
        JOIN runs r        ON ts.run_id  = r.run_id
        JOIN experiments e ON r.exp_id   = e.exp_id
        WHERE ts.all_zones_json IS NOT NULL
          AND ts.all_zones_json != '{{}}'
          AND ts.all_zones_json != 'null'
        ORDER BY ts.run_id DESC, ts.timestamp_ns
        LIMIT {limit}
    """)

    if raw.empty:
        return pd.DataFrame()

    rows = []
    for _, row in raw.iterrows():
        try:
            zones = json.loads(row["all_zones_json"] or "{}")
        except Exception:
            continue
        for zone_name, temp_val in zones.items():
            if temp_val is None:
                continue
            try:
                temp = float(temp_val)
            except (TypeError, ValueError):
                continue
            if temp < -50 or temp > 150:   # sanity filter
                continue
            rows.append({
                "run_id":        row["run_id"],
                "time_s":        row["time_s"],
                "zone":          zone_name,
                "temp":          temp,
                "workflow_type": row["workflow_type"],
                "task_name":     row["task_name"],
                "provider":      row["provider"],
                "throttle_event": row.get("throttle_event", 0),
            })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


def render(ctx: dict) -> None:
    runs = ctx.get("runs", pd.DataFrame())

    total_samples  = q1("SELECT COUNT(*) AS n FROM thermal_samples").get("n", 0) or 0
    throttle_count = q1(
        "SELECT COUNT(*) AS n FROM thermal_samples WHERE throttle_event=1"
    ).get("n", 0) or 0
    affected_runs  = q1(
        "SELECT COUNT(DISTINCT run_id) AS n FROM thermal_samples WHERE throttle_event=1"
    ).get("n", 0) or 0

    # ── Header ────────────────────────────────────────────────────────────────
    throttle_pct = round(throttle_count / total_samples * 100, 1) if total_samples else 0
    health_clr   = (
        "#22c55e" if throttle_pct < 10 else
        "#f59e0b" if throttle_pct < 50 else
        "#ef4444"
    )

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
                (f"{total_samples:,}", "Total samples",  ACCENT),
                (f"{throttle_count:,}", "Throttle events", "#ef4444"),
                (f"{affected_runs}",   "Affected runs",   "#f97316"),
                (f"{throttle_pct}%",   "Throttle rate",   health_clr),
            ]
        ])
        + "</div></div>",
        unsafe_allow_html=True,
    )

    # Alert if ALL samples are throttled — known sensor state issue
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
            unsafe_allow_html=True,
        )

    # ── Run-level thermal stats (original — unchanged) ────────────────────────
    if not runs.empty and "package_temp_celsius" in runs.columns:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Package temp distribution by workflow</div>",
                unsafe_allow_html=True,
            )
            df_t = runs[
                runs["package_temp_celsius"].notna()
                & (runs["package_temp_celsius"] > 0)
            ]
            fig = go.Figure()
            for wf, clr in WF_COLORS.items():
                sub = df_t[df_t["workflow_type"] == wf]["package_temp_celsius"].dropna()
                if sub.empty:
                    continue
                fig.add_trace(go.Box(
                    y=sub, name=wf, marker_color=clr, line_color=clr, boxmean=True
                ))
            fig.update_layout(
                **PL, height=260, yaxis_title="Package temp (°C)", showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True, key="th_temp_box")

        with col2:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Thermal delta vs Energy</div>",
                unsafe_allow_html=True,
            )
            fig2 = go.Figure()
            if "thermal_delta_c" in runs.columns and "energy_j" in runs.columns:
                for wf, clr in WF_COLORS.items():
                    sub = runs[runs["workflow_type"] == wf]
                    sub = sub[
                        sub["thermal_delta_c"].notna()
                        & sub["energy_j"].notna()
                        & (sub["energy_j"] > 0)
                    ]
                    if sub.empty:
                        continue
                    fig2.add_trace(go.Scatter(
                        x=sub["thermal_delta_c"],
                        y=sub["energy_j"],
                        mode="markers",
                        name=wf,
                        marker=dict(color=clr, size=5, opacity=0.6),
                    ))
            fig2.update_layout(
                **PL, height=260,
                xaxis_title="Thermal delta (°C)",
                yaxis_title="Energy (J)",
            )
            st.plotly_chart(fig2, use_container_width=True, key="th_delta_scatter")

    # ── Time-series drilldown (original — unchanged) ──────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Time-series drilldown — thermal zones per run</div>",
        unsafe_allow_html=True,
    )

    run_ids = q("""
        SELECT DISTINCT run_id FROM thermal_samples
        ORDER BY run_id DESC LIMIT 100
    """).get("run_id", pd.Series()).tolist()

    if not run_ids:
        st.info("No thermal sample data yet.")
        return

    sel_run = st.selectbox(
        "Select run", run_ids, key="th_run_sel",
        format_func=lambda x: f"Run {x}"
    )

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
    first_json = (
        samples["all_zones_json"].dropna().iloc[0]
        if samples["all_zones_json"].notna().any()
        else "{}"
    )
    try:
        zones     = json.loads(first_json)
        zone_keys = list(zones.keys())
    except Exception:
        zone_keys = []

    fig3 = go.Figure()
    for col_n, label, clr in [
        ("cpu_temp",    "CPU Package", "#ef4444"),
        ("system_temp", "System",      "#3b82f6"),
        ("wifi_temp",   "WiFi",        "#22c55e"),
    ]:
        sub = samples[samples[col_n].notna() & (samples[col_n] > -100)]
        if sub.empty:
            continue
        fig3.add_trace(go.Scatter(
            x=sub["time_s"], y=sub[col_n],
            mode="lines", name=label,
            line=dict(width=1.5, color=clr),
        ))

    throttled = samples[samples["throttle_event"] == 1]
    if not throttled.empty:
        fig3.add_trace(go.Scatter(
            x=throttled["time_s"],
            y=throttled["cpu_temp"].fillna(50),
            mode="markers", name="Throttle event",
            marker=dict(color="#ef4444", size=8, symbol="x"),
        ))

    fig3.update_layout(
        **PL, height=300, xaxis_title="Time (s)", yaxis_title="Temperature (°C)"
    )
    st.plotly_chart(fig3, use_container_width=True, key=f"th_ts_{sel_run}")

    if zone_keys:
        st.markdown(
            f"<div style='font-size:10px;color:#475569;"
            f"font-family:IBM Plex Mono,monospace;margin-top:4px;'>"
            f"Sensors available: {', '.join(zone_keys)}</div>",
            unsafe_allow_html=True,
        )

    stat_cols = st.columns(4)
    for col, (val, label) in zip(stat_cols, [
        (f"{samples['cpu_temp'].max():.1f}°C", "Max CPU temp"),
        (f"{samples['cpu_temp'].min():.1f}°C", "Min CPU temp"),
        (f"{samples['cpu_temp'].mean():.1f}°C", "Avg CPU temp"),
        (f"{len(throttled)}",                   "Throttle events"),
    ]):
        with col:
            st.metric(label, val)

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 3 — THERMAL ZONE DEEP DIVE
    # Parses all_zones_json across all runs for cross-run zone analysis.
    # ══════════════════════════════════════════════════════════════════════════

    st.markdown("---")
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;'>"
        f"🔬 Thermal Zone Deep Dive — cross-run analysis</div>",
        unsafe_allow_html=True,
    )

    # Sample limit control — parsing JSON is expensive
    z_limit = st.slider(
        "Samples to parse (higher = more accurate, slower)",
        500, 10000, 3000, 500,
        key="th_zone_limit",
        help="Parses this many thermal_samples rows and extracts all JSON zones",
    )

    with st.spinner(f"Parsing {z_limit:,} thermal samples for zone data..."):
        zone_df = _parse_zones_bulk(limit=z_limit)

    if zone_df.empty:
        st.info(
            "No parseable all_zones_json data found. "
            "Zones may all be NULL or empty — check thermal sensor collection."
        )
        return

    # Which zones are available across the dataset?
    available_zones = sorted(zone_df["zone"].unique().tolist())
    n_runs_with_zones = zone_df["run_id"].nunique()

    st.markdown(
        f"<div style='font-size:11px;color:#475569;"
        f"font-family:IBM Plex Mono,monospace;margin-bottom:12px;'>"
        f"Found <b style='color:#f1f5f9'>{len(available_zones)}</b> unique zones "
        f"across <b style='color:#f1f5f9'>{n_runs_with_zones}</b> runs: "
        f"{', '.join(available_zones)}</div>",
        unsafe_allow_html=True,
    )

    # ── Zone mean temperature by workflow type ─────────────────────────────────
    # Core question: do agentic runs run hotter on specific zones vs linear?
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
        f"Zone avg temperature — linear vs agentic</div>",
        unsafe_allow_html=True,
    )

    zone_wf = (
        zone_df.groupby(["zone", "workflow_type"])["temp"]
        .mean().reset_index()
    )
    zone_wf.columns = ["zone", "workflow_type", "avg_temp"]

    fig_zone_wf = go.Figure()
    for wf, clr in WF_COLORS.items():
        sub = zone_wf[zone_wf["workflow_type"] == wf]
        if sub.empty:
            continue
        fig_zone_wf.add_trace(go.Bar(
            x=sub["zone"],
            y=sub["avg_temp"],
            name=wf,
            marker_color=clr,
            marker_line_width=0,
        ))
    fig_zone_wf.update_layout(
        **PL, height=260, barmode="group",
        yaxis_title="Avg temperature (°C)",
        xaxis_title="Thermal zone",
        showlegend=True,
    )
    st.plotly_chart(fig_zone_wf, use_container_width=True, key="th_zone_wf_bar")

    # ── Thermal zone heatmap — zone × task ────────────────────────────────────
    # Which tasks drive which zones hot? Reveals workload thermal signature.
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Zone × task thermal heatmap</div>",
        unsafe_allow_html=True,
    )

    zone_task = (
        zone_df.groupby(["zone", "task_name"])["temp"]
        .mean().reset_index()
    )
    if not zone_task.empty:
        pivot_zt = zone_task.pivot_table(
            index="zone", columns="task_name",
            values="temp", aggfunc="mean", fill_value=None,
        )

        fig_zt = go.Figure(go.Heatmap(
            z=pivot_zt.values.tolist(),
            x=list(pivot_zt.columns),
            y=list(pivot_zt.index),
            colorscale=[
                [0.0, "#0d1117"],
                [0.3, "#1e3a5f"],
                [0.6, "#854d0e"],
                [1.0, "#ef4444"],
            ],
            showscale=True,
            colorbar=dict(title="°C", tickfont=dict(size=9)),
            texttemplate="%{z:.1f}",
            textfont=dict(size=9),
        ))
        fig_zt.update_layout(
            **{**PL, "margin": dict(l=100, r=60, t=20, b=80)},
            height=max(260, len(pivot_zt) * 36 + 80),
            xaxis_tickangle=-30,
        )
        st.plotly_chart(fig_zt, use_container_width=True, key="th_zone_task_heat")

    # ── WiFi temp vs API latency correlation ──────────────────────────────────
    # Handoff doc noted: "wifi_temp correlates with api_latency (interesting pattern)"
    # This section quantifies and visualises that relationship.
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"wifi_temp × api_latency — the interesting correlation</div>",
        unsafe_allow_html=True,
    )

    # Get per-run avg wifi_temp from zone_df
    wifi_df = zone_df[zone_df["zone"].str.lower().isin(
        ["wifi_temp", "wifi", "wlan", "iwlwifi_1"]
    )].groupby("run_id")["temp"].mean().reset_index()
    wifi_df.columns = ["run_id", "avg_wifi_temp"]

    # Join with runs to get api_latency_ms
    if not runs.empty and "api_latency_ms" in runs.columns:
        runs_api = runs[["run_id", "api_latency_ms", "workflow_type"]].dropna(
            subset=["api_latency_ms"]
        )
        wifi_api = wifi_df.merge(runs_api, on="run_id", how="inner")

        if len(wifi_api) >= 5:
            corr = wifi_api["avg_wifi_temp"].corr(wifi_api["api_latency_ms"])

            col_w1, col_w2 = st.columns([2, 1])
            with col_w1:
                fig_wifi = go.Figure()
                for wf, clr in WF_COLORS.items():
                    sub = wifi_api[wifi_api["workflow_type"] == wf]
                    if sub.empty:
                        continue
                    fig_wifi.add_trace(go.Scatter(
                        x=sub["avg_wifi_temp"],
                        y=sub["api_latency_ms"],
                        mode="markers",
                        name=wf,
                        marker=dict(color=clr, size=6, opacity=0.65),
                    ))
                fig_wifi.update_layout(
                    **PL, height=260,
                    xaxis_title="Avg WiFi temperature (°C)",
                    yaxis_title="API latency (ms)",
                    showlegend=True,
                )
                st.plotly_chart(fig_wifi, use_container_width=True,
                                key="th_wifi_api_scatter")

            with col_w2:
                corr_strength = (
                    "strong"   if abs(corr) >= 0.5 else
                    "moderate" if abs(corr) >= 0.3 else
                    "weak"
                )
                corr_clr = (
                    "#ef4444" if abs(corr) >= 0.5 else
                    "#f59e0b" if abs(corr) >= 0.3 else
                    "#22c55e"
                )
                st.markdown(
                    f"<div style='padding:14px;background:#111827;"
                    f"border:1px solid {corr_clr}33;border-left:3px solid {corr_clr};"
                    f"border-radius:8px;margin-top:8px;'>"
                    f"<div style='font-size:28px;font-weight:800;color:{corr_clr};"
                    f"font-family:IBM Plex Mono,monospace;'>{corr:.3f}</div>"
                    f"<div style='font-size:10px;color:#94a3b8;margin-top:2px;'>"
                    f"Pearson r</div>"
                    f"<div style='font-size:11px;color:{corr_clr};margin-top:8px;"
                    f"font-weight:600;'>{corr_strength} correlation</div>"
                    f"<div style='font-size:10px;color:#475569;margin-top:6px;"
                    f"line-height:1.5;'>"
                    f"n={len(wifi_api)} runs</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

                # Research insight based on correlation
                if abs(corr) >= 0.3:
                    direction = "higher" if corr > 0 else "lower"
                    st.markdown(
                        f"<div style='padding:10px 12px;background:#1a1000;"
                        f"border-left:3px solid #f59e0b;border-radius:0 8px 8px 0;"
                        f"font-size:10px;color:#fcd34d;"
                        f"font-family:IBM Plex Mono,monospace;margin-top:8px;"
                        f"line-height:1.6;'>"
                        f"WiFi chip running {direction} temperature correlates "
                        f"with {direction} API latency. "
                        f"Possible cause: thermal throttling of the WiFi chip "
                        f"reducing network throughput. "
                        f"Consider wifi_temp as a feature for cloud run "
                        f"energy/latency prediction."
                        f"</div>",
                        unsafe_allow_html=True,
                    )
        else:
            st.info(
                "Not enough runs with both wifi_temp zone data and api_latency_ms "
                f"to compute correlation (found {len(wifi_api)} matching runs, need ≥5)."
            )
    else:
        st.info("api_latency_ms not available in runs — skipping wifi correlation.")

    # ── Zone stability — std_dev per zone across runs ─────────────────────────
    # A zone with high std_dev across runs is thermally unstable —
    # its readings are less reliable as a sensor signal.
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Zone stability — temperature variance across runs</div>",
        unsafe_allow_html=True,
    )

    zone_stats = (
        zone_df.groupby("zone")["temp"]
        .agg(["mean", "std", "min", "max", "count"])
        .reset_index()
    )
    zone_stats.columns = ["Zone", "Mean °C", "Std Dev", "Min °C", "Max °C", "Samples"]
    zone_stats = zone_stats.sort_values("Std Dev", ascending=False)
    zone_stats = zone_stats.round(2)

    # Visualise std dev as a bar chart — high variance zones are unreliable
    fig_stab = go.Figure(go.Bar(
        x=zone_stats["Zone"],
        y=zone_stats["Std Dev"],
        marker_color=[
            "#ef4444" if v > 10 else "#f59e0b" if v > 5 else "#22c55e"
            for v in zone_stats["Std Dev"]
        ],
        marker_line_width=0,
        text=zone_stats["Mean °C"].apply(lambda v: f"μ={v:.1f}°C"),
        textposition="outside",
        textfont=dict(size=9),
    ))
    fig_stab.update_layout(
        **PL, height=240,
        yaxis_title="Std deviation (°C)",
        xaxis_title="Zone",
        showlegend=False,
    )
    st.plotly_chart(fig_stab, use_container_width=True, key="th_zone_stability")

    st.dataframe(zone_stats, use_container_width=True, hide_index=True)

    # ── Research note ─────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='margin-top:14px;padding:10px 14px;"
        f"background:#0c1f3a;border-left:3px solid #3b82f6;"
        f"border-radius:0 8px 8px 0;font-size:11px;"
        f"color:#93c5fd;font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
        f"<b>Research note:</b> High-variance zones (std > 10°C) are poor ML features "
        f"as their readings are noisy across runs. Low-variance zones that still differ "
        f"between linear and agentic workflows are the strongest thermal signal for "
        f"energy prediction. wifi_temp is worth including if the wifi×api_latency "
        f"correlation is moderate or strong — it captures network chip thermal state "
        f"which RAPL does not measure."
        f"</div>",
        unsafe_allow_html=True,
    )
