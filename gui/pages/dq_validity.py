"""
gui/pages/dq_validity.py  —  ✓  Run Validity
─────────────────────────────────────────────────────────────────────────────
Shows researchers which runs are flagged as invalid and why.

Flags checked (original):
  • experiment_valid = 0
  • thermal_throttle_flag = 1
  • baseline_id IS NULL (no idle baseline — energy readings unreliable)
  • thermal_during_experiment = 1 (thermal event during run)
  • background_cpu_percent > 20 (noisy environment)

Added in Phase 2:
  • Statistical outlier detection — runs > 2σ or > 3σ from population mean
    across 8 key energy/performance columns. Results stored in outliers table
    so they persist across sessions and can be excluded from analysis.
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, STATUS_COLORS
from gui.db import q, q1
from gui.db_migrations import detect_and_store_outliers
from gui.helpers import fl


# Columns we run outlier detection on — chosen because they are the most
# meaningful for energy research and the most likely to reveal bad data.
_OUTLIER_COLUMNS = [
    "energy_j",
    "duration_ms",
    "ipc",
    "cache_miss_rate",
    "api_latency_ms",
    "package_temp_celsius",
    "avg_power_watts",
    "total_tokens",
]


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

    total      = int(stats.get("total", 0))
    invalid    = int(stats.get("invalid", 0))
    throttled  = int(stats.get("throttled", 0))
    no_base    = int(stats.get("no_baseline", 0))
    thermal_ev = int(stats.get("thermal_event", 0))
    noisy      = int(stats.get("noisy_env", 0))
    clean      = int(stats.get("clean", 0))
    clean_pct  = round(clean / total * 100, 1) if total else 0

    # ── Header ────────────────────────────────────────────────────────────────
    health_clr = (
        "#22c55e" if clean_pct >= 90 else
        "#f59e0b" if clean_pct >= 70 else
        "#ef4444"
    )
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
        unsafe_allow_html=True,
    )

    # ── KPI cards ─────────────────────────────────────────────────────────────
    cols = st.columns(5)
    flags = [
        ("Invalid runs",      invalid,    "#ef4444", "experiment_valid = 0"),
        ("Thermal throttle",  throttled,  "#f59e0b", "thermal_throttle_flag = 1"),
        ("No baseline",       no_base,    "#f59e0b", "baseline_id IS NULL"),
        ("Thermal event",     thermal_ev, "#f97316", "thermal_during_experiment = 1"),
        ("Noisy environment", noisy,      "#a78bfa", "background_cpu_percent > 20"),
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
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # ── Flagged runs table (original — unchanged) ─────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Flagged Runs</div>",
        unsafe_allow_html=True,
    )

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
            unsafe_allow_html=True,
        )
    else:
        # Build a human-readable flag summary column
        def _flags(row):
            f = []
            if row.get("experiment_valid") == 0:           f.append("INVALID")
            if row.get("thermal_throttle_flag") == 1:      f.append("THROTTLE")
            if pd.isna(row.get("baseline_id")):            f.append("NO_BASELINE")
            if row.get("thermal_during_experiment") == 1:  f.append("THERMAL_EVENT")
            if (row.get("background_cpu_percent") or 0) > 20: f.append("NOISY")
            return " · ".join(f)

        flagged_df["flags"] = flagged_df.apply(_flags, axis=1)
        display = flagged_df[[
            "run_id", "experiment", "model", "workflow",
            "task", "run_number", "energy_j", "temp_c", "flags",
        ]].copy()
        display.columns = [
            "Run ID", "Experiment", "Model", "Workflow",
            "Task", "Run #", "Energy (J)", "Temp (°C)", "Flags",
        ]
        st.dataframe(display, use_container_width=True, height=400)

        # ── Flag distribution chart ───────────────────────────────────────────
        if len(flagged_df) > 1:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{accent};"
                f"text-transform:uppercase;letter-spacing:.1em;"
                f"margin:20px 0 10px;'>Flag distribution</div>",
                unsafe_allow_html=True,
            )
            flag_counts = {k: v for k, v in {
                "Invalid": invalid, "Throttled": throttled,
                "No baseline": no_base, "Thermal event": thermal_ev,
                "Noisy env": noisy,
            }.items() if v > 0}

            if flag_counts:
                fig = go.Figure(go.Bar(
                    x=list(flag_counts.keys()),
                    y=list(flag_counts.values()),
                    marker_color=["#ef4444","#f59e0b","#f59e0b","#f97316","#a78bfa"]
                        [:len(flag_counts)],
                    marker_line_width=0,
                ))
                fig.update_layout(
                    **PL, height=220, showlegend=False, yaxis_title="Run count",
                )
                st.plotly_chart(fig, use_container_width=True, key="dq_validity_bar")

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # OUTLIER DETECTION — Phase 2 addition
    # Detects runs that are statistically anomalous (> 2σ or > 3σ from mean)
    # across 8 key energy/performance columns.
    # Results are stored in the outliers table so they persist across sessions.
    # ══════════════════════════════════════════════════════════════════════════

    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:#f97316;"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Statistical Outlier Detection</div>",
        unsafe_allow_html=True,
    )

    # ── Controls row ──────────────────────────────────────────────────────────
    oc1, oc2, oc3 = st.columns([2, 2, 2])
    with oc1:
        mild_sigma   = st.slider("Mild threshold (σ)",   1.5, 3.0, 2.0, 0.1,
                                  help="Runs beyond this σ are flagged as mild outliers")
    with oc2:
        severe_sigma = st.slider("Severe threshold (σ)", 2.5, 5.0, 3.0, 0.1,
                                  help="Runs beyond this σ are flagged as severe outliers")
    with oc3:
        st.markdown("<div style='margin-top:26px;'></div>", unsafe_allow_html=True)
        run_detection = st.button(
            "🔍 Run Outlier Detection",
            use_container_width=True,
            help="Scans all runs and writes results to the outliers table",
        )

    # ── Run detection when button clicked ─────────────────────────────────────
    if run_detection:
        with st.spinner("Scanning runs for outliers..."):
            n_found = detect_and_store_outliers(
                columns=_OUTLIER_COLUMNS,
                mild_sigma=mild_sigma,
                severe_sigma=severe_sigma,
            )
        if n_found > 0:
            st.success(f"✓ Found and stored {n_found} new outlier(s) in the outliers table.")
        else:
            st.info("No new outliers found — all existing records updated.")

    # ── Load outliers from DB ─────────────────────────────────────────────────
    outliers_df = q("""
        SELECT
            o.outlier_id,
            o.run_id,
            o.column_name,
            ROUND(o.value,    4)   AS value,
            ROUND(o.mean,     4)   AS mean,
            ROUND(o.std_dev,  4)   AS std_dev,
            ROUND(o.sigma,    2)   AS sigma,
            o.severity,
            o.excluded,
            o.reason,
            o.detected_at,
            e.task_name,
            e.model_name,
            r.workflow_type
        FROM outliers o
        JOIN runs r        ON o.run_id  = r.run_id
        JOIN experiments e ON r.exp_id  = e.exp_id
        ORDER BY o.sigma DESC
    """)

    if outliers_df.empty:
        # No outliers yet — show prompt to run detection
        st.markdown(
            "<div style='padding:28px;text-align:center;"
            "border:1px solid #f97316aa;border-radius:10px;"
            "background:#1a0a0022;margin-top:8px;'>"
            "<div style='font-size:13px;color:#f97316;"
            "font-family:IBM Plex Mono,monospace;'>No outliers detected yet</div>"
            "<div style='font-size:11px;color:#475569;margin-top:6px;'>"
            "Click <b>Run Outlier Detection</b> above to scan all runs.</div>"
            "</div>",
            unsafe_allow_html=True,
        )
    else:
        # ── Summary KPIs ──────────────────────────────────────────────────────
        n_total   = len(outliers_df)
        n_severe  = int((outliers_df["severity"] == "severe").sum())
        n_mild    = int((outliers_df["severity"] == "mild").sum())
        n_excluded = int((outliers_df["excluded"] == 1).sum())
        n_runs_affected = outliers_df["run_id"].nunique()

        sk1, sk2, sk3, sk4 = st.columns(4)
        for col, val, label, clr in [
            (sk1, n_total,         "Total outliers",    "#f97316"),
            (sk2, n_severe,        "Severe (>3σ)",      "#ef4444"),
            (sk3, n_mild,          "Mild (2–3σ)",       "#f59e0b"),
            (sk4, n_runs_affected, "Runs affected",     "#a78bfa"),
        ]:
            with col:
                st.markdown(
                    f"<div style='padding:10px 14px;background:#111827;"
                    f"border:1px solid {clr}33;border-left:3px solid {clr};"
                    f"border-radius:8px;margin-bottom:12px;'>"
                    f"<div style='font-size:22px;font-weight:700;color:{clr};"
                    f"font-family:IBM Plex Mono,monospace;line-height:1;'>{val}</div>"
                    f"<div style='font-size:9px;color:#94a3b8;margin-top:3px;"
                    f"text-transform:uppercase;letter-spacing:.08em;'>{label}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

        # ── Outlier breakdown by column ────────────────────────────────────────
        # Shows which metrics have the most outliers — useful for spotting
        # systematic sensor issues vs random noise.
        st.markdown(
            "<div style='font-size:11px;font-weight:600;color:#f97316;"
            "text-transform:uppercase;letter-spacing:.1em;margin:8px 0 8px;'>"
            "Outliers by metric</div>",
            unsafe_allow_html=True,
        )

        col_counts = (
            outliers_df.groupby(["column_name", "severity"])
            .size().reset_index(name="count")
        )

        fig_cols = go.Figure()
        for sev, clr in [("severe", "#ef4444"), ("mild", "#f59e0b")]:
            sub = col_counts[col_counts["severity"] == sev]
            if sub.empty:
                continue
            fig_cols.add_trace(go.Bar(
                x=sub["column_name"],
                y=sub["count"],
                name=sev,
                marker_color=clr,
                marker_line_width=0,
            ))
        fig_cols.update_layout(
            **PL, height=220, barmode="stack",
            yaxis_title="Outlier count",
            xaxis_title="Metric",
            showlegend=True,
        )
        st.plotly_chart(fig_cols, use_container_width=True, key="dq_outlier_col_chart")

        # ── σ distribution scatter — shows spread of outlier severity ──────────
        st.markdown(
            "<div style='font-size:11px;font-weight:600;color:#f97316;"
            "text-transform:uppercase;letter-spacing:.1em;margin:8px 0 8px;'>"
            "Sigma distribution — how far outliers deviate</div>",
            unsafe_allow_html=True,
        )
        fig_sigma = go.Figure()
        for sev, clr in [("severe", "#ef4444"), ("mild", "#f59e0b")]:
            sub = outliers_df[outliers_df["severity"] == sev]
            if sub.empty:
                continue
            fig_sigma.add_trace(go.Scatter(
                x=sub["run_id"],
                y=sub["sigma"],
                mode="markers",
                name=sev,
                marker=dict(color=clr, size=6, opacity=0.7),
                text=sub["column_name"] + " · " + sub["run_id"].astype(str),
                hovertemplate="%{text}<br>σ = %{y:.2f}<extra></extra>",
            ))
        # Reference lines for mild and severe thresholds
        fig_sigma.add_hline(y=mild_sigma,   line_dash="dot",
                             line_color="#f59e0b", annotation_text=f"mild ({mild_sigma}σ)")
        fig_sigma.add_hline(y=severe_sigma, line_dash="dot",
                             line_color="#ef4444", annotation_text=f"severe ({severe_sigma}σ)")
        fig_sigma.update_layout(
            **PL, height=240,
            xaxis_title="Run ID",
            yaxis_title="Sigma (σ)",
        )
        st.plotly_chart(fig_sigma, use_container_width=True, key="dq_outlier_sigma")

        # ── Full outlier table with exclude toggle ─────────────────────────────
        st.markdown(
            "<div style='font-size:11px;font-weight:600;color:#f97316;"
            "text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            "All detected outliers</div>",
            unsafe_allow_html=True,
        )

        # Filter controls
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            sev_filter = st.selectbox(
                "Severity", ["all", "severe", "mild"], key="dq_sev_filter"
            )
        with fc2:
            col_filter = st.selectbox(
                "Metric", ["all"] + sorted(outliers_df["column_name"].unique().tolist()),
                key="dq_col_filter",
            )
        with fc3:
            excl_filter = st.selectbox(
                "Show", ["all", "not excluded", "excluded only"],
                key="dq_excl_filter",
            )

        # Apply filters
        view_df = outliers_df.copy()
        if sev_filter != "all":
            view_df = view_df[view_df["severity"] == sev_filter]
        if col_filter != "all":
            view_df = view_df[view_df["column_name"] == col_filter]
        if excl_filter == "not excluded":
            view_df = view_df[view_df["excluded"] == 0]
        elif excl_filter == "excluded only":
            view_df = view_df[view_df["excluded"] == 1]

        # Display table
        display_out = view_df[[
            "run_id", "column_name", "value", "mean", "sigma",
            "severity", "excluded", "task_name", "workflow_type", "reason",
        ]].copy()
        display_out.columns = [
            "Run ID", "Metric", "Value", "Pop. Mean", "Sigma",
            "Severity", "Excluded", "Task", "Workflow", "Reason",
        ]
        st.dataframe(display_out, use_container_width=True, height=360,
                     hide_index=True)

        # ── Exclude / include controls ─────────────────────────────────────────
        # Allows researcher to soft-exclude outlier runs from analysis.
        # The run data is never deleted — only the excluded flag is toggled.
        st.markdown(
            "<div style='font-size:11px;font-weight:600;color:#f97316;"
            "text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            "Exclude / Include runs</div>",
            unsafe_allow_html=True,
        )

        exc1, exc2, exc3 = st.columns(3)
        target_run_id = exc1.number_input(
            "Run ID to toggle", min_value=1, step=1, key="dq_excl_run_id"
        )
        action = exc2.selectbox(
            "Action", ["exclude", "include"], key="dq_excl_action"
        )
        exc3.markdown("<div style='margin-top:26px;'></div>", unsafe_allow_html=True)
        if exc3.button("Apply", use_container_width=True, key="dq_excl_apply"):
            _apply_exclusion(int(target_run_id), action)
            st.success(
                f"Run {target_run_id} {'excluded from' if action == 'exclude' else 'included in'} analysis."
            )
            st.rerun()

        # ── Excluded runs summary ─────────────────────────────────────────────
        if n_excluded > 0:
            st.markdown(
                f"<div style='padding:10px 14px;background:#1a0a00;"
                f"border-left:3px solid #ef4444;border-radius:0 8px 8px 0;"
                f"font-size:11px;color:#fca5a5;"
                f"font-family:IBM Plex Mono,monospace;margin-top:8px;'>"
                f"⊘ {n_excluded} outlier record(s) are currently excluded from analysis. "
                f"Analysis pages that use the <code>outliers</code> table will "
                f"automatically filter these runs out when exclusion is respected.</div>",
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # ── Researcher note (original — unchanged) ────────────────────────────────
    st.markdown(
        f"<div style='margin-top:8px;padding:10px 14px;"
        f"background:#0c1f3a;border-left:3px solid #3b82f6;"
        f"border-radius:0 8px 8px 0;font-size:11px;"
        f"color:#93c5fd;font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
        f"<b>Researcher note:</b> Flagged runs are included in all analysis pages by default. "
        f"Use the <b>Sensor Coverage</b> page to see which columns are affected, "
        f"and <b>Sufficiency Advisor</b> to check if removing flagged runs "
        f"drops your cell counts below the 30-run threshold. "
        f"Outliers marked as <b>excluded</b> above are stored in the outliers table "
        f"and can be filtered out in any analysis query using "
        f"<code>LEFT JOIN outliers o ON r.run_id = o.run_id WHERE o.excluded IS NULL OR o.excluded = 0</code>."
        f"</div>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: toggle excluded flag in the outliers table
# ══════════════════════════════════════════════════════════════════════════════

def _apply_exclusion(run_id: int, action: str) -> None:
    """
    Set excluded = 1 (exclude) or excluded = 0 (include) for all
    outlier records belonging to a given run_id.
    Uses a direct sqlite3 connection — not the cached read connection.
    """
    import sqlite3
    from gui.config import DB_PATH

    excluded_val = 1 if action == "exclude" else 0
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=5)
        conn.execute(
            "UPDATE outliers SET excluded = ? WHERE run_id = ?",
            (excluded_val, run_id),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        st.error(f"Could not update outlier exclusion: {e}")
