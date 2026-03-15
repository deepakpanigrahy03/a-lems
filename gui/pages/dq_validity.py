"""
gui/pages/dq_validity.py  —  ✓  Run Validity
─────────────────────────────────────────────────────────────────────────────
Shows researchers which runs are flagged as invalid and why.
Flags checked:
  • experiment_valid = 0
  • thermal_throttle_flag = 1
  • baseline_id IS NULL (no idle baseline — energy readings unreliable)
  • thermal_during_experiment = 1 (thermal event during run)
  • background_cpu_percent > 20 (noisy environment)
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.db      import q, q1
from gui.config  import PL, STATUS_COLORS
from gui.helpers import fl


def render(ctx: dict) -> None:
    dark = st.session_state.get("theme", "dark") == "dark"
    accent = "#f472b6"

    # ── KPI query ─────────────────────────────────────────────────────────────
    stats = q1("""
        SELECT
            COUNT(*)                                                    AS total,
            SUM(CASE WHEN COALESCE(experiment_valid,1) = 0 THEN 1 ELSE 0 END) AS invalid,
            SUM(CASE WHEN COALESCE(thermal_throttle_flag,0) = 1 THEN 1 ELSE 0 END) AS throttled,
            SUM(CASE WHEN baseline_id IS NULL           THEN 1 ELSE 0 END) AS no_baseline,
            SUM(CASE WHEN COALESCE(thermal_during_experiment,0) = 1 THEN 1 ELSE 0 END) AS thermal_event,
            SUM(CASE WHEN COALESCE(background_cpu_percent,0) > 20 THEN 1 ELSE 0 END) AS noisy_env,
            SUM(CASE WHEN COALESCE(experiment_valid,1) = 1
                      AND COALESCE(thermal_throttle_flag,0) != 1
                      AND baseline_id IS NOT NULL       THEN 1 ELSE 0 END) AS clean
        FROM runs
    """) or {}

    total      = int(stats.get("total",        0))
    invalid    = int(stats.get("invalid",       0))
    throttled  = int(stats.get("throttled",     0))
    no_base    = int(stats.get("no_baseline",   0))
    thermal_ev = int(stats.get("thermal_event", 0))
    noisy      = int(stats.get("noisy_env",     0))
    clean      = int(stats.get("clean",         0))
    clean_pct  = round(clean / total * 100, 1) if total else 0

    # ── Header ────────────────────────────────────────────────────────────────
    health_clr = "#22c55e" if clean_pct >= 90 else "#f59e0b" if clean_pct >= 70 else "#ef4444"
    st.markdown(
        f"<div style='padding:16px 20px;background:linear-gradient(135deg,{accent}12,{accent}06);"
        f"border:1px solid {accent}33;border-radius:12px;margin-bottom:20px;"
        f"display:flex;align-items:center;gap:16px;'>"
        f"<div><div style='font-size:32px;font-weight:800;color:{health_clr};"
        f"font-family:IBM Plex Mono,monospace;line-height:1;'>{clean_pct}%</div>"
        f"<div style='font-size:10px;color:#94a3b8;text-transform:uppercase;"
        f"letter-spacing:.1em;margin-top:2px;'>Clean runs</div></div>"
        f"<div style='flex:1;'>"
        f"<div style='font-size:13px;color:#f1f5f9;font-family:IBM Plex Mono,monospace;"
        f"margin-bottom:4px;'>{clean} of {total} runs pass all validity checks</div>"
        f"<div style='background:#1f2937;border-radius:3px;height:6px;'>"
        f"<div style='background:linear-gradient(90deg,{health_clr}99,{health_clr});"
        f"width:{clean_pct}%;height:100%;border-radius:3px;'></div></div></div>"
        f"</div>",
        unsafe_allow_html=True)

    # ── KPI cards ─────────────────────────────────────────────────────────────
    cols = st.columns(5)
    flags = [
        ("Invalid runs",       invalid,    "#ef4444", "experiment_valid = 0"),
        ("Thermal throttle",   throttled,  "#f59e0b", "thermal_throttle_flag = 1"),
        ("No baseline",        no_base,    "#f59e0b", "baseline_id IS NULL"),
        ("Thermal event",      thermal_ev, "#f97316", "thermal_during_experiment = 1"),
        ("Noisy environment",  noisy,      "#a78bfa", "background_cpu_percent > 20"),
    ]
    for col, (label, val, clr, condition) in zip(cols, flags):
        with col:
            pct = round(val / total * 100, 1) if total else 0
            bg  = "#1a0505" if val > 0 else "#0c1a0c"
            st.markdown(
                f"<div style='padding:12px 14px;background:{bg};"
                f"border:1px solid {clr}33;border-left:3px solid {clr};"
                f"border-radius:8px;margin-bottom:8px;'>"
                f"<div style='font-size:22px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;line-height:1;'>{val}</div>"
                f"<div style='font-size:9px;color:#94a3b8;margin-top:3px;"
                f"text-transform:uppercase;letter-spacing:.08em;'>{label}</div>"
                f"<div style='font-size:8px;color:#475569;margin-top:2px;'>{pct}% of runs</div>"
                f"</div>",
                unsafe_allow_html=True)

    st.markdown("---")

    # ── Flagged runs table ────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Flagged Runs</div>",
        unsafe_allow_html=True)

    flagged_df = q("""
        SELECT
            r.run_id,
            e.name            AS experiment,
            e.model_name      AS model,
            e.workflow_type   AS workflow,
            e.task_name       AS task,
            r.run_number,
            r.experiment_valid,
            r.thermal_throttle_flag,
            r.baseline_id,
            r.thermal_during_experiment,
            r.background_cpu_percent,
            r.total_energy_uj / 1e6 AS energy_j,
            r.package_temp_celsius  AS temp_c
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        WHERE COALESCE(r.experiment_valid,1) = 0
           OR COALESCE(r.thermal_throttle_flag,0) = 1
           OR r.baseline_id IS NULL
           OR COALESCE(r.thermal_during_experiment,0) = 1
           OR COALESCE(r.background_cpu_percent,0) > 20
        ORDER BY r.run_id DESC
        LIMIT 200
    """)

    if flagged_df.empty:
        st.markdown(
            f"<div style='padding:40px;text-align:center;"
            f"border:1px solid #22c55e33;border-radius:12px;"
            f"background:#052e1a22;'>"
            f"<div style='font-size:32px;margin-bottom:8px;'>✓</div>"
            f"<div style='font-size:16px;color:#22c55e;"
            f"font-family:IBM Plex Mono,monospace;'>All runs are valid</div>"
            f"<div style='font-size:12px;color:#475569;margin-top:4px;'>"
            f"No flags detected across {total} runs</div>"
            f"</div>",
            unsafe_allow_html=True)
    else:
        # Build flag summary column
        def _flags(row):
            f = []
            if row.get("experiment_valid") == 0:         f.append("INVALID")
            if row.get("thermal_throttle_flag") == 1:    f.append("THROTTLE")
            if pd.isna(row.get("baseline_id")):          f.append("NO_BASELINE")
            if row.get("thermal_during_experiment") == 1:f.append("THERMAL_EVENT")
            if (row.get("background_cpu_percent") or 0) > 20: f.append("NOISY")
            return " · ".join(f)

        flagged_df["flags"] = flagged_df.apply(_flags, axis=1)
        display = flagged_df[[
            "run_id", "experiment", "model", "workflow", "task",
            "run_number", "energy_j", "temp_c", "flags"
        ]].copy()
        display.columns = [
            "Run ID", "Experiment", "Model", "Workflow", "Task",
            "Run #", "Energy (J)", "Temp (°C)", "Flags"
        ]
        st.dataframe(display, use_container_width=True, height=400)

        # ── Flag trend chart ──────────────────────────────────────────────────
        if len(flagged_df) > 1:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{accent};"
                f"text-transform:uppercase;letter-spacing:.1em;"
                f"margin:20px 0 10px;'>Flag distribution</div>",
                unsafe_allow_html=True)

            flag_counts = {
                "Invalid":        invalid,
                "Throttled":      throttled,
                "No baseline":    no_base,
                "Thermal event":  thermal_ev,
                "Noisy env":      noisy,
            }
            flag_counts = {k: v for k, v in flag_counts.items() if v > 0}

            if flag_counts:
                fig = go.Figure(go.Bar(
                    x=list(flag_counts.keys()),
                    y=list(flag_counts.values()),
                    marker_color=["#ef4444", "#f59e0b", "#f59e0b", "#f97316", "#a78bfa"][
                        :len(flag_counts)],
                    marker_line_width=0,
                ))
                fig.update_layout(
                    **PL,
                    height=220,
                    showlegend=False,
                    yaxis_title="Run count",
                )
                st.plotly_chart(fig, use_container_width=True,
                                key="dq_validity_bar")

    # ── Researcher note ───────────────────────────────────────────────────────
    st.markdown(
        f"<div style='margin-top:16px;padding:10px 14px;"
        f"background:#0c1f3a;border-left:3px solid #3b82f6;"
        f"border-radius:0 8px 8px 0;font-size:11px;"
        f"color:#93c5fd;font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
        f"<b>Researcher note:</b> Flagged runs are included in all analysis pages by default. "
        f"Use the <b>Sensor Coverage</b> page to see which columns are affected, "
        f"and <b>Sufficiency Advisor</b> to check if removing flagged runs "
        f"drops your cell counts below the 30-run threshold."
        f"</div>",
        unsafe_allow_html=True)
