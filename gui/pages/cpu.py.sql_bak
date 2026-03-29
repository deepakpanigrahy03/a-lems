"""
gui/pages/cpu.py  —  ▣  CPU & C-States
Render function: render(ctx)
ctx keys: ov, runs, tax, lin, age, avg_lin_j, avg_age_j, tax_mult,
          plan_ms, exec_ms, synth_ms, plan_pct, exec_pct, synth_pct

Phase 3 addition:
  • C-state efficiency score per run
    score = (c6_time + c7_time) / total_duration × 100
    Higher = more deep sleep = more power-efficient idle behaviour
  • Score trend over time — is the system getting more/less efficient?
  • Score by workflow, task, model — where are the efficiency wins?
  • ML feature recommendation
─────────────────────────────────────────────────────────────────────────────
"""

import subprocess

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from gui.config import DB_PATH, LIVE_API, PL, PROJECT_ROOT, WF_COLORS
from gui.db import q, q1, q_safe
from gui.helpers import (_bar_gauge_html, _gauge_html, _human_carbon,
                         _human_energy, _human_water, fl)

try:
    import requests as _req
    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False
    class _req:
        @staticmethod
        def get(*a, **kw):
            raise RuntimeError("requests not installed")

try:
    import yaml as _yaml
    _YAML_OK = True
except ImportError:
    _YAML_OK = False

# ── Configurable thresholds ───────────────────────────────────────────────────
# C-state efficiency score = (c6 + c7 residency%) out of 100.
# These thresholds define what "good", "ok", and "poor" efficiency means.
CSTATE_SCORE_GOOD  = 60.0   # % deep sleep — efficient
CSTATE_SCORE_OK    = 30.0   # % deep sleep — acceptable
# Below OK = poor — CPU is mostly active or in shallow C-states


def render(ctx: dict):
    ov        = ctx["ov"]
    runs      = ctx["runs"]
    tax       = ctx["tax"]
    avg_lin_j = ctx["avg_lin_j"]
    avg_age_j = ctx["avg_age_j"]
    tax_mult  = ctx["tax_mult"]
    plan_ms   = ctx["plan_ms"]
    exec_ms   = ctx["exec_ms"]
    synth_ms  = ctx["synth_ms"]
    plan_pct  = ctx["plan_pct"]
    exec_pct  = ctx["exec_pct"]
    synth_pct = ctx["synth_pct"]
    lin       = ctx["lin"]
    age       = ctx["age"]

    st.title("CPU & C-State Analysis")

    # ── C-state residency (original — unchanged) ──────────────────────────────
    cstate_df = q("""
        SELECT e.provider, r.workflow_type,
               AVG(cs.c1_residency) AS c1, AVG(cs.c2_residency) AS c2,
               AVG(cs.c3_residency) AS c3, AVG(cs.c6_residency) AS c6,
               AVG(cs.c7_residency) AS c7,
               AVG(cs.cpu_util_percent) AS util,
               AVG(cs.package_power) AS pkg_w,
               COUNT(cs.sample_id) AS samples
        FROM cpu_samples cs
        JOIN runs r ON cs.run_id = r.run_id
        JOIN experiments e ON r.exp_id = e.exp_id
        GROUP BY e.provider, r.workflow_type
    """)

    if not cstate_df.empty:
        st.markdown(
            "**C-State Residency** — higher C6/C7 = deeper sleep = more efficient idle"
        )
        CSTATE_COLORS = {
            "C0": "#ef4444", "C1": "#38bdf8", "C2": "#3b82f6",
            "C3": "#a78bfa", "C6": "#22c55e", "C7": "#f59e0b",
        }
        for _, row in cstate_df.iterrows():
            c0 = max(
                0.0,
                100 - float(row.c1 or 0) - float(row.c2 or 0)
                    - float(row.c3 or 0) - float(row.c6 or 0) - float(row.c7 or 0),
            )
            cs_data = pd.DataFrame([
                {"State": "C0", "Residency%": c0},
                {"State": "C1", "Residency%": float(row.c1 or 0)},
                {"State": "C2", "Residency%": float(row.c2 or 0)},
                {"State": "C3", "Residency%": float(row.c3 or 0)},
                {"State": "C6", "Residency%": float(row.c6 or 0)},
                {"State": "C7", "Residency%": float(row.c7 or 0)},
            ])
            st.markdown(
                f"**{row.provider} · {row.workflow_type}** — "
                f"{float(row.pkg_w or 0):.2f}W · {int(row.samples):,} samples"
            )
            fig = px.bar(
                cs_data, x="Residency%", y="State",
                orientation="h", color="State",
                color_discrete_map=CSTATE_COLORS,
            )
            fig.update_layout(**PL, height=160, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        st.info(
            "Cloud: mostly C6/C7 (deep sleep between API calls). "
            "Local: forced C0 throughout inference loop."
        )
    else:
        st.info("No cpu_samples yet — run experiments to populate.")

    st.divider()

    # ── IPC + Cache Miss (original — unchanged) ───────────────────────────────
    if not runs.empty and "ipc" in runs.columns:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**IPC Distribution**")
            _ri = runs.dropna(subset=["ipc"])
            fig = px.histogram(
                _ri, x="ipc", color="workflow_type",
                color_discrete_map=WF_COLORS, nbins=20,
                barmode="overlay", opacity=0.75, labels={"ipc": "IPC"},
            )
            st.plotly_chart(fl(fig), use_container_width=True)

        with col2:
            st.markdown("**Cache Miss vs Energy**")
            if "cache_miss_rate" in runs.columns and "energy_j" in runs.columns:
                _rm = runs.dropna(subset=["cache_miss_rate", "energy_j"]).copy()
                _rm["cache_miss_pct"] = _rm["cache_miss_rate"] * 100
                fig2 = px.scatter(
                    _rm, x="cache_miss_pct", y="energy_j",
                    color="workflow_type", color_discrete_map=WF_COLORS,
                    log_y=True, hover_data=["run_id", "provider"],
                    labels={"cache_miss_pct": "Cache Miss %", "energy_j": "Energy J"},
                )
                st.plotly_chart(fl(fig2), use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 3 — C-STATE EFFICIENCY SCORE
    #
    # Score = (c6_time_s + c7_time_s) / duration_s × 100
    #
    # Interpretation:
    #   High score (>60%) → CPU spends most idle time in deep C-states
    #                        → hardware is power-efficient when waiting
    #   Low score  (<30%) → CPU stays in shallow C-states even when idle
    #                        → indicates busy polling, thermal issues, or
    #                           short sleep intervals preventing deep C-states
    #
    # Why this matters for energy ML:
    #   C-state efficiency is a proxy for "how well did the hardware utilise
    #   idle periods?" — a key factor in dynamic energy variance that is
    #   independent of the actual workload.
    # ══════════════════════════════════════════════════════════════════════════

    st.divider()
    st.markdown(
        "<div style='font-size:11px;font-weight:600;color:#f59e0b;"
        "text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;'>"
        "🔬 C-State Efficiency Score</div>",
        unsafe_allow_html=True,
    )

    # Load per-run C-state time data from runs table
    # c2/c3/c6/c7_time_seconds columns are available in runs per the schema
    cs_runs = q("""
        SELECT
            r.run_id,
            r.workflow_type,
            r.duration_ns / 1e9                         AS duration_s,
            COALESCE(r.c2_time_seconds, 0)               AS c2_s,
            COALESCE(r.c3_time_seconds, 0)               AS c3_s,
            COALESCE(r.c6_time_seconds, 0)               AS c6_s,
            COALESCE(r.c7_time_seconds, 0)               AS c7_s,
            r.total_energy_uj / 1e6                      AS energy_j,
            r.ipc,
            e.task_name,
            e.provider,
            e.model_name
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        WHERE r.duration_ns > 0
          AND (r.c6_time_seconds IS NOT NULL OR r.c7_time_seconds IS NOT NULL)
        ORDER BY r.run_id DESC
    """)

    if cs_runs.empty:
        st.info(
            "No per-run C-state time data found "
            "(c6_time_seconds / c7_time_seconds columns are NULL). "
            "These are populated from perf counters during experiment runs."
        )
        return

    # ── Compute efficiency score ───────────────────────────────────────────────
    # Deep sleep time = c6 + c7 (deepest power-saving states)
    # Shallow sleep   = c2 + c3 (lighter states, less power saving)
    # Score           = deep_sleep / total_duration × 100
    cs_runs["deep_sleep_s"]    = cs_runs["c6_s"] + cs_runs["c7_s"]
    cs_runs["shallow_sleep_s"] = cs_runs["c2_s"] + cs_runs["c3_s"]
    cs_runs["active_s"]        = (
        cs_runs["duration_s"]
        - cs_runs["deep_sleep_s"]
        - cs_runs["shallow_sleep_s"]
    ).clip(lower=0)

    cs_runs["efficiency_score"] = (
        cs_runs["deep_sleep_s"] / cs_runs["duration_s"] * 100
    ).clip(0, 100).round(2)

    # Classify runs
    def _classify_score(s):
        if s >= CSTATE_SCORE_GOOD: return "efficient"
        if s >= CSTATE_SCORE_OK:   return "acceptable"
        return "poor"

    cs_runs["efficiency_class"] = cs_runs["efficiency_score"].apply(_classify_score)

    SCORE_COLORS = {
        "efficient":  "#22c55e",
        "acceptable": "#f59e0b",
        "poor":       "#ef4444",
    }

    # ── Summary KPIs ──────────────────────────────────────────────────────────
    n_runs       = len(cs_runs)
    avg_score    = cs_runs["efficiency_score"].mean()
    n_efficient  = int((cs_runs["efficiency_class"] == "efficient").sum())
    n_acceptable = int((cs_runs["efficiency_class"] == "acceptable").sum())
    n_poor       = int((cs_runs["efficiency_class"] == "poor").sum())

    score_clr = (
        "#22c55e" if avg_score >= CSTATE_SCORE_GOOD else
        "#f59e0b" if avg_score >= CSTATE_SCORE_OK   else
        "#ef4444"
    )

    k1, k2, k3, k4, k5 = st.columns(5)
    for col, val, label, clr in [
        (k1, f"{avg_score:.1f}%", "Avg efficiency score",    score_clr),
        (k2, n_efficient,         f"Efficient (≥{CSTATE_SCORE_GOOD}%)",  "#22c55e"),
        (k3, n_acceptable,        f"Acceptable (≥{CSTATE_SCORE_OK}%)", "#f59e0b"),
        (k4, n_poor,              f"Poor (<{CSTATE_SCORE_OK}%)",        "#ef4444"),
        (k5, n_runs,              "Runs scored",              "#94a3b8"),
    ]:
        with col:
            st.markdown(
                f"<div style='padding:10px 14px;background:#111827;"
                f"border:1px solid {clr}33;border-left:3px solid {clr};"
                f"border-radius:8px;margin-bottom:12px;'>"
                f"<div style='font-size:20px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;line-height:1;'>{val}</div>"
                f"<div style='font-size:9px;color:#94a3b8;margin-top:3px;"
                f"text-transform:uppercase;letter-spacing:.08em;'>{label}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # ── Score distribution + score by workflow ────────────────────────────────
    col_s1, col_s2 = st.columns(2)

    with col_s1:
        st.markdown(
            "<div style='font-size:11px;font-weight:600;color:#f59e0b;"
            "text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            "Score distribution</div>",
            unsafe_allow_html=True,
        )
        fig_dist = go.Figure()
        for cls, clr in SCORE_COLORS.items():
            sub = cs_runs[cs_runs["efficiency_class"] == cls]["efficiency_score"]
            if sub.empty:
                continue
            fig_dist.add_trace(go.Histogram(
                x=sub, name=cls, marker_color=clr,
                opacity=0.75, nbinsx=30,
            ))
        # Reference lines
        fig_dist.add_vline(
            x=CSTATE_SCORE_GOOD, line_dash="dot", line_color="#22c55e",
            annotation_text=f"good ({CSTATE_SCORE_GOOD}%)",
            annotation_font_size=9,
        )
        fig_dist.add_vline(
            x=CSTATE_SCORE_OK, line_dash="dot", line_color="#f59e0b",
            annotation_text=f"ok ({CSTATE_SCORE_OK}%)",
            annotation_font_size=9,
        )
        fig_dist.update_layout(
            **PL, height=260, barmode="overlay",
            xaxis_title="Efficiency score (%)",
            yaxis_title="Run count",
        )
        st.plotly_chart(fig_dist, use_container_width=True, key="cs_score_dist")

    with col_s2:
        st.markdown(
            "<div style='font-size:11px;font-weight:600;color:#f59e0b;"
            "text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            "Score by workflow type</div>",
            unsafe_allow_html=True,
        )
        fig_wf = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = cs_runs[cs_runs["workflow_type"] == wf]["efficiency_score"].dropna()
            if sub.empty:
                continue
            fig_wf.add_trace(go.Box(
                y=sub, name=wf, marker_color=clr,
                line_color=clr, boxmean=True,
            ))
        fig_wf.add_hline(
            y=CSTATE_SCORE_GOOD, line_dash="dot", line_color="#22c55e",
            annotation_text="good", annotation_font_size=9,
        )
        fig_wf.add_hline(
            y=CSTATE_SCORE_OK, line_dash="dot", line_color="#f59e0b",
            annotation_text="ok", annotation_font_size=9,
        )
        fig_wf.update_layout(
            **PL, height=260,
            yaxis_title="Efficiency score (%)",
            showlegend=False,
        )
        st.plotly_chart(fig_wf, use_container_width=True, key="cs_score_wf")

    # ── Score vs energy — does efficiency correlate with lower energy? ─────────
    # Key research question: do high-scoring runs actually use less energy?
    # If yes, C-state efficiency is a strong ML feature for energy prediction.
    st.markdown(
        "<div style='font-size:11px;font-weight:600;color:#f59e0b;"
        "text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        "Efficiency score vs energy — does more deep sleep = less energy?</div>",
        unsafe_allow_html=True,
    )

    col_s3, col_s4 = st.columns(2)

    with col_s3:
        fig_corr = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = cs_runs[cs_runs["workflow_type"] == wf].dropna(
                subset=["efficiency_score", "energy_j"]
            )
            if sub.empty:
                continue
            fig_corr.add_trace(go.Scatter(
                x=sub["efficiency_score"],
                y=sub["energy_j"],
                mode="markers",
                name=wf,
                marker=dict(color=clr, size=5, opacity=0.6),
            ))
        fig_corr.update_layout(
            **PL, height=260,
            xaxis_title="C-state efficiency score (%)",
            yaxis_title="Energy (J)",
        )
        st.plotly_chart(fig_corr, use_container_width=True, key="cs_score_energy")

    with col_s4:
        # Mean energy by score class — quantifies the efficiency payoff
        energy_by_class = (
            cs_runs.groupby("efficiency_class")["energy_j"]
            .agg(["mean", "std", "count"])
            .reset_index()
        )
        energy_by_class.columns = ["class", "mean_j", "std_j", "count"]
        energy_by_class["clr"] = energy_by_class["class"].map(SCORE_COLORS)

        fig_ebc = go.Figure(go.Bar(
            x=energy_by_class["class"],
            y=energy_by_class["mean_j"],
            error_y=dict(type="data", array=energy_by_class["std_j"].tolist()),
            marker_color=energy_by_class["clr"].tolist(),
            marker_line_width=0,
            text=energy_by_class["count"].apply(lambda n: f"n={n}"),
            textposition="outside",
            textfont=dict(size=9),
        ))
        fig_ebc.update_layout(
            **PL, height=260,
            yaxis_title="Mean energy (J)",
            xaxis_title="Efficiency class",
            showlegend=False,
        )
        st.plotly_chart(fig_ebc, use_container_width=True, key="cs_energy_class")

    # Compute and display the correlation coefficient
    sub_corr = cs_runs[["efficiency_score", "energy_j"]].dropna()
    if len(sub_corr) >= 5:
        corr = sub_corr["efficiency_score"].corr(sub_corr["energy_j"])
        corr_clr = (
            "#ef4444" if abs(corr) >= 0.5 else
            "#f59e0b" if abs(corr) >= 0.3 else
            "#22c55e"
        )
        direction = "negative" if corr < 0 else "positive"
        interpretation = (
            f"{'Strong' if abs(corr)>=0.5 else 'Moderate' if abs(corr)>=0.3 else 'Weak'} "
            f"{direction} correlation — "
            + (
                "higher C-state efficiency reliably predicts lower energy. "
                "Strong ML feature."
                if corr < -0.3 else
                "higher efficiency associated with lower energy but weakly. "
                "Include as secondary feature."
                if corr < 0 else
                "unexpected positive correlation — may indicate confounding "
                "with run duration or workflow type. Investigate further."
            )
        )
        st.markdown(
            f"<div style='padding:10px 14px;background:#111827;"
            f"border:1px solid {corr_clr}33;border-left:3px solid {corr_clr};"
            f"border-radius:8px;margin-bottom:12px;display:flex;gap:14px;"
            f"align-items:center;'>"
            f"<div style='font-size:24px;font-weight:800;color:{corr_clr};"
            f"font-family:IBM Plex Mono,monospace;flex-shrink:0;'>{corr:.3f}</div>"
            f"<div style='font-size:11px;color:#94a3b8;line-height:1.6;'>"
            f"Pearson r (efficiency vs energy) · n={len(sub_corr)} runs<br>"
            f"<span style='color:{corr_clr};'>{interpretation}</span>"
            f"</div></div>",
            unsafe_allow_html=True,
        )

    # ── Score by task — which tasks are most efficient? ───────────────────────
    st.markdown(
        "<div style='font-size:11px;font-weight:600;color:#f59e0b;"
        "text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        "Efficiency score by task</div>",
        unsafe_allow_html=True,
    )

    task_score = (
        cs_runs.groupby("task_name")["efficiency_score"]
        .agg(["mean", "std", "count"])
        .reset_index()
        .sort_values("mean", ascending=False)
    )
    task_score.columns = ["task", "mean_score", "std_score", "count"]

    fig_task = go.Figure(go.Bar(
        x=task_score["task"],
        y=task_score["mean_score"],
        error_y=dict(type="data", array=task_score["std_score"].fillna(0).tolist()),
        marker_color=[
            "#22c55e" if v >= CSTATE_SCORE_GOOD else
            "#f59e0b" if v >= CSTATE_SCORE_OK   else
            "#ef4444"
            for v in task_score["mean_score"]
        ],
        marker_line_width=0,
        text=task_score["count"].apply(lambda n: f"n={n}"),
        textposition="outside",
        textfont=dict(size=9),
    ))
    fig_task.add_hline(
        y=CSTATE_SCORE_GOOD, line_dash="dot", line_color="#22c55e",
        annotation_text=f"good ({CSTATE_SCORE_GOOD}%)", annotation_font_size=9,
    )
    fig_task.add_hline(
        y=CSTATE_SCORE_OK, line_dash="dot", line_color="#f59e0b",
        annotation_text=f"ok ({CSTATE_SCORE_OK}%)", annotation_font_size=9,
    )
    fig_task.update_layout(
        **PL, height=260,
        yaxis_title="Avg efficiency score (%)",
        xaxis_tickangle=-30,
        showlegend=False,
    )
    st.plotly_chart(fig_task, use_container_width=True, key="cs_task_score")

    # ── Score trend over time ─────────────────────────────────────────────────
    # Is the system getting more or less C-state efficient over time?
    # Degrading trend could mean thermal buildup, governor change, or code change.
    st.markdown(
        "<div style='font-size:11px;font-weight:600;color:#f59e0b;"
        "text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        "Efficiency score trend over time</div>",
        unsafe_allow_html=True,
    )

    trend_df = cs_runs[["run_id", "efficiency_score", "workflow_type"]].sort_values("run_id")
    trend_df["rolling_mean"] = (
        trend_df["efficiency_score"].rolling(20, min_periods=5).mean()
    )

    fig_trend = go.Figure()
    # Raw points per workflow
    for wf, clr in WF_COLORS.items():
        sub = trend_df[trend_df["workflow_type"] == wf]
        if sub.empty:
            continue
        fig_trend.add_trace(go.Scatter(
            x=sub["run_id"],
            y=sub["efficiency_score"],
            mode="markers",
            name=f"{wf} (raw)",
            marker=dict(color=clr, size=3, opacity=0.4),
            showlegend=True,
        ))
    # Rolling mean — all workflows combined
    fig_trend.add_trace(go.Scatter(
        x=trend_df["run_id"],
        y=trend_df["rolling_mean"],
        mode="lines",
        name="20-run rolling mean",
        line=dict(color="#f59e0b", width=2),
    ))
    fig_trend.add_hline(
        y=CSTATE_SCORE_GOOD, line_dash="dot", line_color="#22c55e",
        annotation_text="good", annotation_font_size=9,
    )
    fig_trend.update_layout(
        **PL, height=260,
        xaxis_title="Run ID (chronological)",
        yaxis_title="Efficiency score (%)",
    )
    st.plotly_chart(fig_trend, use_container_width=True, key="cs_trend")

    # ── C-state time breakdown — where is time spent? ─────────────────────────
    st.markdown(
        "<div style='font-size:11px;font-weight:600;color:#f59e0b;"
        "text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        "Average time breakdown by workflow</div>",
        unsafe_allow_html=True,
    )

    time_agg = (
        cs_runs.groupby("workflow_type")[
            ["active_s", "shallow_sleep_s", "deep_sleep_s"]
        ].mean().reset_index()
    )

    fig_time = go.Figure()
    for col_n, label, clr in [
        ("active_s",        "Active (C0)",       "#ef4444"),
        ("shallow_sleep_s", "Shallow (C2+C3)",   "#f59e0b"),
        ("deep_sleep_s",    "Deep sleep (C6+C7)","#22c55e"),
    ]:
        fig_time.add_trace(go.Bar(
            x=time_agg["workflow_type"],
            y=time_agg[col_n],
            name=label,
            marker_color=clr,
            marker_line_width=0,
        ))
    fig_time.update_layout(
        **PL, height=260, barmode="stack",
        yaxis_title="Avg seconds",
        showlegend=True,
    )
    st.plotly_chart(fig_time, use_container_width=True, key="cs_time_breakdown")

    # ── ML feature note ───────────────────────────────────────────────────────
    st.markdown(
        "<div style='margin-top:14px;padding:12px 16px;"
        "background:#0c1f3a;border-left:3px solid #3b82f6;"
        "border-radius:0 8px 8px 0;font-size:11px;"
        "color:#93c5fd;font-family:IBM Plex Mono,monospace;line-height:1.8;'>"
        "<b>ML training features from C-state efficiency:</b><br>"
        "• <code>efficiency_score</code> — % time in C6+C7 (primary feature)<br>"
        "• <code>deep_sleep_s</code> — absolute time in deep C-states<br>"
        "• <code>shallow_sleep_s</code> — time in C2+C3 (secondary)<br>"
        "• <code>active_s / duration_s</code> — active fraction (complement of score)<br>"
        "• <code>efficiency_class</code> — categorical: efficient / acceptable / poor<br>"
        "These features capture <i>how well the hardware utilised idle periods</i> — "
        "a key energy variance factor independent of workload complexity."
        "</div>",
        unsafe_allow_html=True,
    )
