"""
gui/pages/baseline.py  —  ⊟  Baseline & Idle
─────────────────────────────────────────────────────────────────────────────
Idle baseline drift, governor state, turbo on/off energy impact.
5 idle_baselines records. Foundation for all dynamic energy calculations.

Phase 3 addition:
  • Baseline-adjusted energy analysis
    dynamic_energy = total_energy − (idle_power × duration)
    Shows true compute cost vs raw RAPL reading
  • Per-baseline energy comparison — are runs on different baselines comparable?
  • Turbo on/off energy impact analysis
  • Governor impact on energy efficiency
  • Baseline quality score per run
─────────────────────────────────────────────────────────────────────────────
"""

from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1

ACCENT = "#f59e0b"

# ── Configurable thresholds ───────────────────────────────────────────────────
# Change these to tune what counts as "acceptable" for your hardware.

# If idle package power drifts more than this between baselines, flag it.
BASELINE_DRIFT_WARN_W    = 0.5   # watts — warn if drift exceeds this
BASELINE_DRIFT_SEVERE_W  = 1.5   # watts — severe if drift exceeds this

# If background CPU at baseline time exceeded this, the baseline is noisy.
BG_CPU_WARN_PCT          = 5.0   # percent

# Minimum samples for a baseline to be considered reliable.
MIN_BASELINE_SAMPLES     = 10


def render(ctx: dict) -> None:
    runs = ctx.get("runs", pd.DataFrame())

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
    latest  = baselines.iloc[0]
    pkg_w   = latest.get("package_power_watts") or 0
    core_w  = latest.get("core_power_watts") or 0

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
                (f"{pkg_w:.2f}W",                        "Latest pkg idle",  ACCENT),
                (f"{core_w:.2f}W",                       "Latest core idle", "#22c55e"),
                (str(latest.get("governor", "?")),        "Governor",         "#60a5fa"),
                (str(latest.get("turbo", "?")),           "Turbo",            "#a78bfa"),
                (f"{latest.get('background_cpu',0):.1f}%","BG CPU",           "#94a3b8"),
            ]
        ])
        + "</div></div>",
        unsafe_allow_html=True,
    )

    # ── Baseline drift chart (original — unchanged) ───────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
        f"Baseline power drift over time</div>",
        unsafe_allow_html=True,
    )

    bl = baselines.sort_values("timestamp").copy()
    bl["ts_fmt"] = bl["timestamp"].apply(
        lambda x: datetime.fromtimestamp(float(x)).strftime("%m/%d %H:%M")
        if x else "?"
    )

    fig = go.Figure()
    for col_n, label, clr in [
        ("package_power_watts", "Package",  ACCENT),
        ("core_power_watts",    "Core",     "#22c55e"),
        ("uncore_power_watts",  "Uncore",   "#3b82f6"),
        ("dram_power_watts",    "DRAM",     "#a78bfa"),
    ]:
        sub = bl[bl[col_n].notna()]
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub["ts_fmt"], y=sub[col_n],
            mode="lines+markers", name=label,
            line=dict(width=2, color=clr),
            marker=dict(size=8, color=clr),
        ))
    fig.update_layout(
        **PL, height=280, xaxis_title="Baseline time", yaxis_title="Idle power (W)"
    )
    st.plotly_chart(fig, use_container_width=True, key="bl_drift_chart")

    # Flag significant drift between baselines
    if len(bl) >= 2:
        pkg_vals = bl["package_power_watts"].dropna()
        if len(pkg_vals) >= 2:
            drift = float(pkg_vals.max() - pkg_vals.min())
            drift_clr = (
                "#ef4444" if drift >= BASELINE_DRIFT_SEVERE_W else
                "#f59e0b" if drift >= BASELINE_DRIFT_WARN_W   else
                "#22c55e"
            )
            drift_msg = (
                f"⚠ Severe drift ({drift:.3f}W) — cross-session energy comparisons unreliable."
                if drift >= BASELINE_DRIFT_SEVERE_W else
                f"⚠ Moderate drift ({drift:.3f}W) — recalibrate before next session."
                if drift >= BASELINE_DRIFT_WARN_W else
                f"✓ Stable baselines — drift {drift:.3f}W within acceptable range."
            )
            st.markdown(
                f"<div style='padding:8px 14px;background:#111827;"
                f"border-left:3px solid {drift_clr};border-radius:0 8px 8px 0;"
                f"font-size:11px;color:{drift_clr};"
                f"font-family:IBM Plex Mono,monospace;margin-bottom:12px;'>"
                f"{drift_msg}</div>",
                unsafe_allow_html=True,
            )

    # ── Stability check (original — unchanged) ────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Measurement stability — std deviation</div>",
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)
    with col1:
        fig2 = go.Figure()
        for col_n, label, clr in [
            ("package_std", "Package σ", ACCENT),
            ("core_std",    "Core σ",    "#22c55e"),
            ("uncore_std",  "Uncore σ",  "#3b82f6"),
        ]:
            sub = bl[bl[col_n].notna()]
            if sub.empty:
                continue
            fig2.add_trace(go.Bar(
                x=sub["ts_fmt"], y=sub[col_n],
                name=label, marker_color=clr, marker_line_width=0,
            ))
        fig2.update_layout(**PL, height=220, barmode="group", yaxis_title="Std dev (W)")
        st.plotly_chart(fig2, use_container_width=True, key="bl_std_bar")

    with col2:
        fig3 = go.Figure(go.Bar(
            x=bl["ts_fmt"],
            y=bl["background_cpu"].fillna(0),
            marker_color=[
                "#ef4444" if v > BG_CPU_WARN_PCT else "#22c55e"
                for v in bl["background_cpu"].fillna(0)
            ],
            marker_line_width=0,
        ))
        fig3.add_hline(
            y=BG_CPU_WARN_PCT, line_dash="dot",
            line_color="#f59e0b", line_width=1,
            annotation_text=f"warn threshold ({BG_CPU_WARN_PCT}%)",
            annotation_font_size=9,
        )
        fig3.update_layout(
            **PL, height=220, yaxis_title="Background CPU %", showlegend=False
        )
        st.plotly_chart(fig3, use_container_width=True, key="bl_bg_cpu")

    # ── Baseline detail table (original — unchanged) ──────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Full baseline records</div>",
        unsafe_allow_html=True,
    )

    display = baselines[[
        "baseline_id", "package_power_watts", "core_power_watts",
        "uncore_power_watts", "dram_power_watts", "governor", "turbo",
        "background_cpu", "duration_seconds", "sample_count", "method",
    ]].copy()
    display.columns = [
        "ID", "Pkg (W)", "Core (W)", "Uncore (W)", "DRAM (W)",
        "Governor", "Turbo", "BG CPU%", "Duration(s)", "Samples", "Method",
    ]
    st.dataframe(display.round(4), use_container_width=True)

    # ── How baselines affect energy (original — unchanged) ────────────────────
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
        unsafe_allow_html=True,
    )

    # ── Runs per baseline (original — unchanged) ──────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Runs using each baseline</div>",
        unsafe_allow_html=True,
    )

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
            marker_color=ACCENT,
            marker_line_width=0,
        ))
        fig4.update_layout(
            **PL, height=200,
            xaxis_title="Baseline ID",
            yaxis_title="Runs using this baseline",
            showlegend=False,
        )
        st.plotly_chart(fig4, use_container_width=True, key="bl_usage_bar")

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 3 — BASELINE-ADJUSTED ENERGY ANALYSIS
    # Shows true dynamic energy vs raw RAPL total.
    # dynamic_energy_j = total_energy_j − (idle_pkg_watts × duration_s)
    # ══════════════════════════════════════════════════════════════════════════

    st.markdown("---")
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;'>"
        f"🔬 Baseline-Adjusted Energy Analysis</div>",
        unsafe_allow_html=True,
    )

    # Load runs joined with their baseline power reading
    adj_df = q("""
        SELECT
            r.run_id,
            r.workflow_type,
            r.total_energy_uj   / 1e6          AS total_energy_j,
            r.dynamic_energy_uj / 1e6          AS stored_dynamic_j,
            r.duration_ns       / 1e9          AS duration_s,
            ib.package_power_watts              AS baseline_pkg_w,
            ib.core_power_watts                 AS baseline_core_w,
            ib.governor,
            ib.turbo,
            ib.background_cpu                   AS baseline_bg_cpu,
            e.task_name,
            e.provider,
            e.model_name
        FROM runs r
        JOIN experiments e   ON r.exp_id      = e.exp_id
        JOIN idle_baselines ib ON r.baseline_id = ib.baseline_id
        WHERE r.baseline_id IS NOT NULL
          AND r.total_energy_uj > 0
          AND r.duration_ns     > 0
        ORDER BY r.run_id DESC
    """)

    if adj_df.empty:
        st.info(
            "No runs with a linked baseline yet — baseline_id is NULL for all runs. "
            "Runs need to be collected with --save-db and a calibrated baseline."
        )
        return

    # ── Recompute adjusted dynamic energy ─────────────────────────────────────
    # Formula: true_dynamic_j = total_energy_j − (idle_pkg_watts × duration_s)
    # This removes the idle power contribution from the total RAPL reading.
    adj_df["computed_dynamic_j"] = (
        adj_df["total_energy_j"]
        - adj_df["baseline_pkg_w"] * adj_df["duration_s"]
    ).clip(lower=0)   # can't be negative — floor at 0

    # How much of total energy is idle vs dynamic?
    adj_df["idle_fraction_pct"] = (
        (adj_df["baseline_pkg_w"] * adj_df["duration_s"])
        / adj_df["total_energy_j"] * 100
    ).clip(0, 100).round(1)

    # Difference between stored dynamic_energy_uj and our recomputed value
    # Non-zero means the harness used a different baseline value at collection time.
    adj_df["dynamic_delta_j"] = (
        adj_df["computed_dynamic_j"] - adj_df["stored_dynamic_j"]
    ).round(6)

    n_adj = len(adj_df)
    avg_idle_pct   = adj_df["idle_fraction_pct"].mean()
    avg_total_j    = adj_df["total_energy_j"].mean()
    avg_dynamic_j  = adj_df["computed_dynamic_j"].mean()
    avg_overhead_j = avg_total_j - avg_dynamic_j

    # ── Adjusted energy KPIs ──────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    for col, val, label, clr in [
        (k1, f"{avg_total_j:.4f} J",    "Avg total energy",    ACCENT),
        (k2, f"{avg_dynamic_j:.4f} J",  "Avg dynamic energy",  "#22c55e"),
        (k3, f"{avg_overhead_j:.4f} J", "Avg idle overhead",   "#ef4444"),
        (k4, f"{avg_idle_pct:.1f}%",    "Idle fraction",       "#a78bfa"),
    ]:
        with col:
            st.markdown(
                f"<div style='padding:10px 14px;background:#111827;"
                f"border:1px solid {clr}33;border-left:3px solid {clr};"
                f"border-radius:8px;margin-bottom:12px;'>"
                f"<div style='font-size:18px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;line-height:1;'>{val}</div>"
                f"<div style='font-size:9px;color:#94a3b8;margin-top:3px;"
                f"text-transform:uppercase;letter-spacing:.08em;'>{label}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # ── Total vs dynamic energy by workflow ───────────────────────────────────
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Total vs adjusted dynamic — by workflow</div>",
            unsafe_allow_html=True,
        )
        wf_adj = (
            adj_df.groupby("workflow_type")[
                ["total_energy_j", "computed_dynamic_j"]
            ].mean().reset_index()
        )
        fig_adj = go.Figure()
        fig_adj.add_trace(go.Bar(
            x=wf_adj["workflow_type"],
            y=wf_adj["total_energy_j"],
            name="Total (RAPL)",
            marker_color="#3b82f6",
            marker_line_width=0,
        ))
        fig_adj.add_trace(go.Bar(
            x=wf_adj["workflow_type"],
            y=wf_adj["computed_dynamic_j"],
            name="Dynamic (adjusted)",
            marker_color="#22c55e",
            marker_line_width=0,
        ))
        fig_adj.update_layout(
            **PL, height=260, barmode="group",
            yaxis_title="Avg energy (J)",
            showlegend=True,
        )
        st.plotly_chart(fig_adj, use_container_width=True, key="bl_adj_wf_bar")

    with col_b:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Idle fraction % by workflow</div>",
            unsafe_allow_html=True,
        )
        # Box plot of idle_fraction_pct — shows how much baseline-idle
        # varies across runs within the same workflow type.
        fig_idle = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = adj_df[adj_df["workflow_type"] == wf]["idle_fraction_pct"].dropna()
            if sub.empty:
                continue
            fig_idle.add_trace(go.Box(
                y=sub, name=wf, marker_color=clr,
                line_color=clr, boxmean=True,
            ))
        fig_idle.update_layout(
            **PL, height=260,
            yaxis_title="Idle fraction (%)",
            showlegend=False,
        )
        st.plotly_chart(fig_idle, use_container_width=True, key="bl_idle_frac_box")

    # ── Turbo on/off energy impact ─────────────────────────────────────────────
    # Key question: do runs collected while turbo was enabled show higher energy?
    # This helps decide whether to control for turbo state in ML features.
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Turbo state impact on adjusted dynamic energy</div>",
        unsafe_allow_html=True,
    )

    turbo_groups = adj_df.groupby("turbo")[
        ["total_energy_j", "computed_dynamic_j", "idle_fraction_pct"]
    ].agg(["mean", "std", "count"]).round(4)

    if len(adj_df["turbo"].dropna().unique()) >= 2:
        turbo_agg = (
            adj_df.groupby("turbo")[["total_energy_j", "computed_dynamic_j"]]
            .mean().reset_index()
        )
        fig_turbo = go.Figure()
        fig_turbo.add_trace(go.Bar(
            x=turbo_agg["turbo"].astype(str),
            y=turbo_agg["total_energy_j"],
            name="Total energy",
            marker_color="#3b82f6", marker_line_width=0,
        ))
        fig_turbo.add_trace(go.Bar(
            x=turbo_agg["turbo"].astype(str),
            y=turbo_agg["computed_dynamic_j"],
            name="Dynamic energy",
            marker_color="#22c55e", marker_line_width=0,
        ))
        fig_turbo.update_layout(
            **PL, height=240, barmode="group",
            xaxis_title="Turbo state",
            yaxis_title="Avg energy (J)",
        )
        st.plotly_chart(fig_turbo, use_container_width=True, key="bl_turbo_bar")
    else:
        turbo_vals = adj_df["turbo"].dropna().unique()
        turbo_str  = str(turbo_vals[0]) if len(turbo_vals) > 0 else "unknown"
        st.markdown(
            f"<div style='padding:8px 14px;background:#0c1f3a;"
            f"border-left:3px solid #3b82f6;border-radius:0 8px 8px 0;"
            f"font-size:11px;color:#93c5fd;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"All baselines used turbo={turbo_str} — "
            f"no contrast available to measure turbo impact. "
            f"Collect baselines with both turbo on and off to quantify the effect."
            f"</div>",
            unsafe_allow_html=True,
        )

    # ── Governor impact ───────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Governor impact on energy efficiency</div>",
        unsafe_allow_html=True,
    )

    if adj_df["governor"].nunique() >= 2:
        gov_agg = (
            adj_df.groupby("governor")[
                ["total_energy_j", "computed_dynamic_j", "idle_fraction_pct"]
            ].mean().reset_index()
        )
        fig_gov = go.Figure()
        for col_n, label, clr in [
            ("total_energy_j",    "Total",   "#3b82f6"),
            ("computed_dynamic_j","Dynamic", "#22c55e"),
        ]:
            fig_gov.add_trace(go.Bar(
                x=gov_agg["governor"],
                y=gov_agg[col_n],
                name=label,
                marker_color=clr,
                marker_line_width=0,
            ))
        fig_gov.update_layout(
            **PL, height=240, barmode="group",
            xaxis_title="CPU governor",
            yaxis_title="Avg energy (J)",
        )
        st.plotly_chart(fig_gov, use_container_width=True, key="bl_gov_bar")
    else:
        gov_val = adj_df["governor"].dropna().unique()
        gov_str = str(gov_val[0]) if len(gov_val) > 0 else "unknown"
        st.markdown(
            f"<div style='padding:8px 14px;background:#0c1f3a;"
            f"border-left:3px solid #3b82f6;border-radius:0 8px 8px 0;"
            f"font-size:11px;color:#93c5fd;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"All runs used governor=<b>{gov_str}</b> — "
            f"no contrast available. Collect runs under different governors "
            f"(powersave vs performance) to quantify the efficiency difference."
            f"</div>",
            unsafe_allow_html=True,
        )

    # ── Per-baseline energy comparison ────────────────────────────────────────
    # If runs from different baseline IDs show different avg dynamic energy,
    # it may be a baseline calibration issue rather than a real energy difference.
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Per-baseline energy comparison — are baselines comparable?</div>",
        unsafe_allow_html=True,
    )

    bl_compare = (
        adj_df.groupby("turbo")[  # group by baseline_id not turbo — kept for compat
            ["total_energy_j", "computed_dynamic_j"]
        ].agg(["mean", "count"]).round(4)
    )

    bl_energy = q("""
        SELECT
            r.baseline_id,
            r.workflow_type,
            COUNT(*)                            AS n_runs,
            AVG(r.total_energy_uj   / 1e6)      AS avg_total_j,
            AVG(r.dynamic_energy_uj / 1e6)      AS avg_stored_dynamic_j,
            AVG(r.duration_ns / 1e9)            AS avg_duration_s,
            ib.package_power_watts              AS idle_pkg_w
        FROM runs r
        JOIN idle_baselines ib ON r.baseline_id = ib.baseline_id
        WHERE r.baseline_id IS NOT NULL
          AND r.total_energy_uj > 0
        GROUP BY r.baseline_id, r.workflow_type
        ORDER BY r.baseline_id, r.workflow_type
    """)

    if not bl_energy.empty:
        # Recompute adjusted dynamic for this summary
        bl_energy["avg_computed_dynamic_j"] = (
            bl_energy["avg_total_j"]
            - bl_energy["idle_pkg_w"] * bl_energy["avg_duration_s"]
        ).clip(lower=0).round(4)

        display_bl = bl_energy[[
            "baseline_id", "workflow_type", "n_runs",
            "avg_total_j", "avg_computed_dynamic_j",
            "idle_pkg_w", "avg_duration_s",
        ]].copy()
        display_bl.columns = [
            "Baseline ID", "Workflow", "Runs",
            "Avg Total (J)", "Avg Dynamic (J)",
            "Idle Pkg (W)", "Avg Duration (s)",
        ]
        st.dataframe(display_bl.round(4), use_container_width=True, hide_index=True)

        # Flag if dynamic energy differs > 20% between baselines for same workflow
        for wf in bl_energy["workflow_type"].unique():
            sub = bl_energy[bl_energy["workflow_type"] == wf]
            if len(sub) < 2:
                continue
            dyn_range = sub["avg_computed_dynamic_j"].max() - sub["avg_computed_dynamic_j"].min()
            dyn_mean  = sub["avg_computed_dynamic_j"].mean()
            if dyn_mean > 0:
                variance_pct = dyn_range / dyn_mean * 100
                if variance_pct > 20:
                    st.markdown(
                        f"<div style='padding:8px 14px;background:#2a0c0c;"
                        f"border-left:3px solid #ef4444;border-radius:0 8px 8px 0;"
                        f"font-size:11px;color:#fca5a5;"
                        f"font-family:IBM Plex Mono,monospace;margin-top:6px;'>"
                        f"⚠ {wf.capitalize()} runs show {variance_pct:.1f}% variance in dynamic energy "
                        f"across baselines — baseline calibration may be inconsistent. "
                        f"Filter analysis to single baseline_id for reliable comparisons."
                        f"</div>",
                        unsafe_allow_html=True,
                    )

    # ── ML feature recommendations ────────────────────────────────────────────
    st.markdown(
        f"<div style='margin-top:16px;padding:12px 16px;"
        f"background:#0c1f3a;border-left:3px solid #3b82f6;"
        f"border-radius:0 8px 8px 0;font-size:11px;"
        f"color:#93c5fd;font-family:IBM Plex Mono,monospace;line-height:1.8;'>"
        f"<b>ML training features from this page:</b><br>"
        f"• <code>computed_dynamic_j</code> — use this as energy target, not total_energy_j<br>"
        f"• <code>idle_fraction_pct</code> — how much of run energy was baseline idle overhead<br>"
        f"• <code>baseline_pkg_w</code> — idle power at time of run (normalisation factor)<br>"
        f"• <code>governor</code> — CPU frequency policy (categorical feature)<br>"
        f"• <code>turbo</code> — turbo boost state (binary feature)<br>"
        f"• <code>baseline_bg_cpu</code> — system noise at baseline time (quality weight)"
        f"</div>",
        unsafe_allow_html=True,
    )
