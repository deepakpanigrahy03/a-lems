"""
gui/pages/experiment_planner.py  —  ◈  Experiment Planner
─────────────────────────────────────────────────────────────────────────────
Two tools in one page:

TOOL 1 — Auto Experiment Suggester
  Reads coverage_matrix to find the biggest gaps.
  Outputs exact run commands to close those gaps.
  Prioritises by: (1) zero-run cells first, (2) furthest from threshold.

TOOL 2 — Energy Budget Estimator
  Given task + model + provider + N runs, estimates:
  • Total energy in joules
  • Estimated duration
  • Carbon footprint
  • Cost equivalent (WhatsApp messages, phone charges)
  Uses historical avg from runs table as the basis.
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1
from gui.db_migrations import refresh_coverage_matrix

ACCENT      = "#22c55e"
MIN_RUNS    = 30   # sufficiency threshold — must match dq_sufficiency.py

# Human-readable energy comparisons
_WHATSAPP_J  = 0.014
_PHONE_J     = 20000
_SEARCH_J    = 0.3


def render(ctx: dict) -> None:

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px;'>"
        f"Experiment Planner</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>"
        f"Auto-suggest which experiments to run next · "
        f"Estimate energy cost before running."
        f"</div></div>",
        unsafe_allow_html=True,
    )

    tab1, tab2 = st.tabs(["◈  Auto Suggester", "⚡  Energy Budget"])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — AUTO EXPERIMENT SUGGESTER
    # ══════════════════════════════════════════════════════════════════════════
    with tab1:
        _render_suggester()

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — ENERGY BUDGET ESTIMATOR
    # ══════════════════════════════════════════════════════════════════════════
    with tab2:
        _render_budget_estimator(ctx)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — AUTO SUGGESTER
# ─────────────────────────────────────────────────────────────────────────────

def _render_suggester():
    accent = ACCENT

    # ── Controls ──────────────────────────────────────────────────────────────
    sc1, sc2, sc3 = st.columns(3)
    with sc1:
        threshold = st.number_input(
            "Target runs per cell", min_value=5, max_value=200,
            value=MIN_RUNS, step=5, key="planner_threshold",
            help="How many runs per hw×model×task×workflow combination",
        )
    with sc2:
        top_n = st.slider(
            "Show top N gaps", 3, 20, 10, 1, key="planner_topn",
        )
    with sc3:
        st.markdown("<div style='margin-top:26px;'></div>", unsafe_allow_html=True)
        if st.button("⟳ Refresh coverage table",
                     use_container_width=True, key="planner_refresh"):
            with st.spinner("Recomputing coverage..."):
                n = refresh_coverage_matrix()
            st.success(f"Updated — {n} cells written.")
            st.rerun()

    # ── Load coverage data ────────────────────────────────────────────────────
    from gui.db import is_server_mode
    if is_server_mode():
        # PG: live query with correct GROUP BY (no coverage_matrix table in PG)
        from gui.db_pg import load_coverage
        coverage = load_coverage()
    else:
        # SQLite: try cached coverage_matrix, fall back to live query
        coverage = q("""
            SELECT cm.hw_id, cm.model_name, cm.task_name,
                   cm.workflow_type, cm.run_count, cm.last_updated, h.hostname
            FROM coverage_matrix cm
            LEFT JOIN hardware_config h ON cm.hw_id = h.hw_id
            ORDER BY cm.run_count ASC
        """)
        if coverage.empty:
            coverage = q("""
                SELECT r.hw_id, e.model_name, e.task_name, e.workflow_type,
                       COUNT(*) AS run_count, h.hostname
                FROM runs r
                JOIN experiments e ON r.exp_id = e.exp_id
                LEFT JOIN hardware_config h ON r.hw_id = h.hw_id
                WHERE e.model_name IS NOT NULL
                  AND e.task_name  IS NOT NULL
                  AND e.workflow_type IS NOT NULL
                GROUP BY r.hw_id, e.model_name, e.task_name, e.workflow_type, h.hostname
                ORDER BY run_count ASC
            """)
            if not coverage.empty:
                st.info("Coverage table empty — showing live results. Click ⟳ Refresh.")

    if coverage.empty:
        st.warning(
            "No coverage data available. "
            "Run some experiments first, then click Refresh."
        )
        return

    # ── Compute gaps ──────────────────────────────────────────────────────────
    coverage["runs_needed"] = (threshold - coverage["run_count"]).clip(lower=0)
    coverage["sufficient"]  = coverage["run_count"] >= threshold
    coverage["pct"]         = (coverage["run_count"] / threshold * 100).clip(upper=100)
    coverage["hostname"]    = coverage["hostname"].fillna(
        "hw_" + coverage["hw_id"].astype(str)
    )

    gaps = coverage[~coverage["sufficient"]].sort_values(
        ["run_count", "task_name"]
    ).head(top_n)

    # ── Summary header ────────────────────────────────────────────────────────
    total_cells     = len(coverage)
    sufficient      = int(coverage["sufficient"].sum())
    total_needed    = int(coverage["runs_needed"].sum())
    zero_cells      = int((coverage["run_count"] == 0).sum())
    readiness       = round(sufficient / total_cells * 100, 1) if total_cells else 0

    rclr = (
        "#22c55e" if readiness >= 80 else
        "#f59e0b" if readiness >= 40 else
        "#ef4444"
    )

    k1, k2, k3, k4 = st.columns(4)
    for col, val, label, clr in [
        (k1, f"{readiness}%",   "Dataset readiness",   rclr),
        (k2, sufficient,         "Sufficient cells",    "#22c55e"),
        (k3, total_cells - sufficient, "Gaps remaining", "#f59e0b"),
        (k4, zero_cells,         "Zero-run cells",      "#ef4444"),
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

    if gaps.empty:
        st.markdown(
            f"<div style='padding:28px;text-align:center;"
            f"border:1px solid #22c55e33;border-radius:10px;background:#052e1a22;'>"
            f"<div style='font-size:20px;color:#22c55e;font-weight:700;'>"
            f"✓ All {total_cells} cells are sufficient</div>"
            f"<div style='font-size:11px;color:#475569;margin-top:6px;'>"
            f"Every hw×model×task×workflow combination has ≥{threshold} runs.</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
        return

    # ── Gap cards with commands ───────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:8px 0 12px;'>"
        f"Top {len(gaps)} gaps — run these next</div>",
        unsafe_allow_html=True,
    )

    for rank, (_, row) in enumerate(gaps.iterrows(), 1):
        needed   = int(row["runs_needed"])
        current  = int(row["run_count"])
        model    = str(row.get("model_name", "?"))
        task     = str(row.get("task_name",  "?"))
        workflow = str(row.get("workflow_type", "?"))
        host     = str(row.get("hostname", "?"))
        pct      = float(row.get("pct", 0))

        # Determine provider from workflow hint
        # local workflows → local provider, cloud workflows → cloud
        provider = "local" if workflow == "linear" and "local" in host.lower() \
                   else "cloud"

        urgency_clr = "#ef4444" if current == 0 else "#f59e0b" if pct < 30 else "#38bdf8"
        urgency_lbl = "ZERO RUNS" if current == 0 else f"{pct:.0f}% complete"

        # Generate the exact terminal command to close this gap
        cmd_single = (
            f"python -m core.execution.tests.test_harness "
            f"--task-id {task} --provider {provider} "
            f"--repetitions {needed} --save-db"
        )
        cmd_batch = (
            f"python -m core.execution.tests.run_experiment "
            f"--tasks {task} --providers {provider} "
            f"--repetitions {needed} --save-db"
        )

        st.markdown(
            f"<div style='padding:12px 16px;background:#0d1117;"
            f"border:1px solid {urgency_clr}33;border-left:3px solid {urgency_clr};"
            f"border-radius:8px;margin-bottom:10px;'>"
            f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:8px;'>"
            f"<div style='font-size:13px;font-weight:700;color:#94a3b8;"
            f"font-family:IBM Plex Mono,monospace;'>#{rank}</div>"
            f"<div style='font-size:12px;font-weight:600;color:#f1f5f9;flex:1;'>"
            f"{model} · {task} · {workflow}</div>"
            f"<div style='font-size:9px;color:{host};"
            f"color:#94a3b8;'>{host}</div>"
            f"<div style='font-size:9px;padding:2px 8px;border-radius:4px;"
            f"background:{urgency_clr}22;color:{urgency_clr};font-weight:700;'>"
            f"{urgency_lbl}</div>"
            f"</div>"
            # Progress bar
            f"<div style='background:#1f2937;border-radius:3px;height:5px;"
            f"margin-bottom:8px;overflow:hidden;'>"
            f"<div style='background:{urgency_clr};width:{pct:.0f}%;"
            f"height:100%;border-radius:3px;'></div></div>"
            f"<div style='font-size:11px;color:#f59e0b;margin-bottom:8px;'>"
            f"Need <b style='color:#f1f5f9'>{needed}</b> more runs "
            f"({current}/{threshold} so far)</div>"
            # Commands
            f"<div style='font-size:9px;color:#475569;margin-bottom:3px;'>"
            f"Single harness:</div>"
            f"<code style='font-size:9px;color:#38bdf8;background:#050c18;"
            f"padding:4px 8px;border-radius:4px;display:block;"
            f"overflow-x:auto;white-space:nowrap;margin-bottom:6px;'>"
            f"{cmd_single}</code>"
            f"<div style='font-size:9px;color:#475569;margin-bottom:3px;'>"
            f"Batch runner:</div>"
            f"<code style='font-size:9px;color:#a78bfa;background:#050c18;"
            f"padding:4px 8px;border-radius:4px;display:block;"
            f"overflow-x:auto;white-space:nowrap;'>"
            f"{cmd_batch}</code>"
            f"</div>",
            unsafe_allow_html=True,
        )

    # ── Coverage heatmap ──────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Full coverage heatmap</div>",
        unsafe_allow_html=True,
    )

    if not coverage.empty:
        coverage["cell_label"] = (
            coverage["model_name"] + " · " + coverage["workflow_type"]
        )
        pivot = coverage.pivot_table(
            index="cell_label", columns="task_name",
            values="run_count", aggfunc="sum", fill_value=0,
        )
        z_norm = [[min(v / threshold, 1.0) for v in row]
                  for row in pivot.values.tolist()]

        fig = go.Figure(go.Heatmap(
            z=z_norm,
            x=list(pivot.columns),
            y=list(pivot.index),
            text=pivot.values.tolist(),
            texttemplate="%{text}",
            textfont=dict(size=10),
            colorscale=[
                [0.0, "#2a0c0c"], [0.01, "#7f1d1d"],
                [0.5,  "#854d0e"], [1.0,  "#14532d"],
            ],
            showscale=True,
            colorbar=dict(
                title=f"/{threshold}",
                tickvals=[0, 0.5, 1.0],
                ticktext=["0", f"{threshold//2}", f"≥{threshold}"],
                tickfont=dict(size=9),
            ),
        ))
        fig.update_layout(
            **{**PL, "margin": dict(l=200, r=80, t=20, b=80)},
            height=max(300, len(pivot) * 40 + 80),
            xaxis_tickangle=-30,
        )
        st.plotly_chart(fig, use_container_width=True, key="planner_heatmap")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — ENERGY BUDGET ESTIMATOR
# ─────────────────────────────────────────────────────────────────────────────

def _render_budget_estimator(ctx: dict):
    accent = "#f59e0b"

    st.markdown(
        f"<div style='font-size:11px;color:#94a3b8;margin-bottom:16px;'>"
        f"Estimate energy cost, duration, and carbon footprint "
        f"before running experiments — based on historical averages.</div>",
        unsafe_allow_html=True,
    )

    # ── Load historical averages per (task × model × provider × workflow) ─────
    hist = q("""
        SELECT
            e.task_name,
            e.model_name,
            e.provider,
            r.workflow_type,
            COUNT(*)                            AS n_runs,
            AVG(r.total_energy_uj / 1e6)        AS avg_energy_j,
            AVG(r.duration_ns / 1e9)            AS avg_duration_s,
            AVG(r.carbon_g)                     AS avg_carbon_g,
            AVG(r.total_tokens)                 AS avg_tokens,
            sqrt(
        AVG((r.total_energy_uj / 1e6) * (r.total_energy_uj / 1e6)) 
        - 
        AVG(r.total_energy_uj / 1e6) * AVG(r.total_energy_uj / 1e6)
    ) AS std_energy_j
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        WHERE e.task_name    IS NOT NULL
          AND e.model_name   IS NOT NULL
          AND r.total_energy_uj > 0
          AND r.duration_ns > 0
        GROUP BY e.task_name, e.model_name, e.provider, r.workflow_type
        HAVING COUNT(*) >= 3
        ORDER BY e.task_name, e.model_name, r.workflow_type
    """)

    # SQLite doesn't have STDDEV_POP — handle gracefully
    if "std_energy_j" not in hist.columns:
        hist["std_energy_j"] = 0.0

    if hist.empty:
        st.info(
            "Not enough historical data yet. "
            "Need at least 3 completed runs per combination. "
            "Run some experiments first."
        )
        return

    # ── Selectors ─────────────────────────────────────────────────────────────
    tasks     = sorted(hist["task_name"].dropna().unique().tolist())
    models    = sorted(hist["model_name"].dropna().unique().tolist())
    providers = sorted(hist["provider"].dropna().unique().tolist())
    workflows = sorted(hist["workflow_type"].dropna().unique().tolist())

    fc1, fc2 = st.columns(2)
    with fc1:
        sel_task     = st.selectbox("Task",     tasks,     key="budget_task")
        sel_model    = st.selectbox("Model",    models,    key="budget_model")
    with fc2:
        sel_provider = st.selectbox("Provider", providers, key="budget_provider")
        sel_workflow = st.selectbox("Workflow", workflows, key="budget_workflow")

    n_runs_planned = st.number_input(
        "Number of runs to estimate",
        min_value=1, max_value=1000, value=30, step=1,
        key="budget_n_runs",
    )

    # ── Look up matching historical record ────────────────────────────────────
    match = hist[
        (hist["task_name"]    == sel_task) &
        (hist["model_name"]   == sel_model) &
        (hist["provider"]     == sel_provider) &
        (hist["workflow_type"] == sel_workflow)
    ]

    if match.empty:
        # Try relaxed match — same task + workflow, any model
        match = hist[
            (hist["task_name"]     == sel_task) &
            (hist["workflow_type"] == sel_workflow)
        ]
        if not match.empty:
            st.markdown(
                f"<div style='padding:8px 14px;background:#1a1000;"
                f"border-left:3px solid #f59e0b;border-radius:0 8px 8px 0;"
                f"font-size:11px;color:#fcd34d;"
                f"font-family:IBM Plex Mono,monospace;margin-bottom:12px;'>"
                f"⚠ No exact match for {sel_model} + {sel_provider}. "
                f"Showing estimate based on {len(match)} similar runs "
                f"(same task + workflow, different model).</div>",
                unsafe_allow_html=True,
            )

    if match.empty:
        st.warning(
            f"No historical data for task='{sel_task}', "
            f"workflow='{sel_workflow}'. "
            f"Run at least 3 experiments with this combination first."
        )
        return

    # Use the best matching row
    row = match.iloc[0]
    avg_e_j    = float(row["avg_energy_j"]   or 0)
    avg_dur_s  = float(row["avg_duration_s"] or 0)
    avg_c_g    = float(row["avg_carbon_g"]   or 0)
    avg_tokens = float(row["avg_tokens"]     or 0)
    std_e_j    = float(row["std_energy_j"]   or 0)
    hist_n     = int(row["n_runs"])

    # ── Compute estimates ──────────────────────────────────────────────────────
    total_energy_j   = avg_e_j   * n_runs_planned
    total_dur_s      = avg_dur_s * n_runs_planned
    total_carbon_g   = avg_c_g   * n_runs_planned
    total_tokens     = avg_tokens * n_runs_planned

    # Confidence range using ±1 std dev (68% interval)
    energy_low  = max(0, (avg_e_j - std_e_j) * n_runs_planned)
    energy_high = (avg_e_j + std_e_j) * n_runs_planned

    # Human-readable equivalents
    whatsapp_msgs = total_energy_j / _WHATSAPP_J
    phone_pct     = total_energy_j / _PHONE_J * 100
    google_search = total_energy_j / _SEARCH_J

    # Duration formatting
    def _fmt_duration(s):
        if s >= 3600: return f"{s/3600:.1f} hours"
        if s >= 60:   return f"{s/60:.1f} minutes"
        return f"{s:.0f} seconds"

    # ── Estimate display ──────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:16px 20px;background:#0d1117;"
        f"border:1px solid {accent}33;border-radius:12px;margin-bottom:16px;'>"
        f"<div style='font-size:11px;color:#475569;margin-bottom:12px;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"Estimate for <b style='color:#f1f5f9'>{n_runs_planned} runs</b> of "
        f"{sel_task} · {sel_model} · {sel_provider} · {sel_workflow} "
        f"(based on {hist_n} historical runs)</div>"
        f"<div style='display:grid;grid-template-columns:repeat(2,1fr);gap:12px;'>"
        # Energy
        f"<div style='background:#111827;border-radius:8px;padding:12px;'>"
        f"<div style='font-size:9px;color:#475569;text-transform:uppercase;"
        f"letter-spacing:.08em;margin-bottom:4px;'>Total energy</div>"
        f"<div style='font-size:24px;font-weight:700;color:{accent};"
        f"font-family:IBM Plex Mono,monospace;'>{total_energy_j:.2f} J</div>"
        f"<div style='font-size:10px;color:#475569;margin-top:3px;'>"
        f"Range: {energy_low:.2f} – {energy_high:.2f} J (±1σ)</div>"
        f"<div style='font-size:10px;color:#94a3b8;margin-top:3px;'>"
        f"Avg per run: {avg_e_j:.4f} J</div>"
        f"</div>"
        # Duration
        f"<div style='background:#111827;border-radius:8px;padding:12px;'>"
        f"<div style='font-size:9px;color:#475569;text-transform:uppercase;"
        f"letter-spacing:.08em;margin-bottom:4px;'>Estimated duration</div>"
        f"<div style='font-size:24px;font-weight:700;color:#3b82f6;"
        f"font-family:IBM Plex Mono,monospace;'>{_fmt_duration(total_dur_s)}</div>"
        f"<div style='font-size:10px;color:#475569;margin-top:3px;'>"
        f"Avg per run: {_fmt_duration(avg_dur_s)}</div>"
        f"</div>"
        # Carbon
        f"<div style='background:#111827;border-radius:8px;padding:12px;'>"
        f"<div style='font-size:9px;color:#475569;text-transform:uppercase;"
        f"letter-spacing:.08em;margin-bottom:4px;'>Carbon footprint</div>"
        f"<div style='font-size:24px;font-weight:700;color:#34d399;"
        f"font-family:IBM Plex Mono,monospace;'>{total_carbon_g*1000:.2f} mg</div>"
        f"<div style='font-size:10px;color:#475569;margin-top:3px;'>"
        f"Avg per run: {avg_c_g*1000:.3f} mg CO₂</div>"
        f"</div>"
        # Tokens
        f"<div style='background:#111827;border-radius:8px;padding:12px;'>"
        f"<div style='font-size:9px;color:#475569;text-transform:uppercase;"
        f"letter-spacing:.08em;margin-bottom:4px;'>Total tokens</div>"
        f"<div style='font-size:24px;font-weight:700;color:#a78bfa;"
        f"font-family:IBM Plex Mono,monospace;'>{int(total_tokens):,}</div>"
        f"<div style='font-size:10px;color:#475569;margin-top:3px;'>"
        f"Avg per run: {int(avg_tokens):,} tokens</div>"
        f"</div>"
        f"</div></div>",
        unsafe_allow_html=True,
    )

    # ── Human-readable equivalents ────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:12px 16px;background:#0c1f3a;"
        f"border:1px solid #3b82f633;border-radius:8px;margin-bottom:16px;'>"
        f"<div style='font-size:10px;color:#475569;text-transform:uppercase;"
        f"letter-spacing:.08em;margin-bottom:8px;'>Energy equivalents</div>"
        f"<div style='display:flex;gap:20px;flex-wrap:wrap;'>"
        f"<div style='font-size:11px;color:#93c5fd;font-family:IBM Plex Mono,monospace;'>"
        f"= <b style='color:#f1f5f9'>{whatsapp_msgs:,.0f}</b> WhatsApp messages</div>"
        f"<div style='font-size:11px;color:#93c5fd;font-family:IBM Plex Mono,monospace;'>"
        f"= <b style='color:#f1f5f9'>{phone_pct:.4f}%</b> of a phone charge</div>"
        f"<div style='font-size:11px;color:#93c5fd;font-family:IBM Plex Mono,monospace;'>"
        f"= <b style='color:#f1f5f9'>{google_search:,.0f}</b> Google searches</div>"
        f"</div></div>",
        unsafe_allow_html=True,
    )

    # ── Compare workflows — same task, both workflows ─────────────────────────
    # Shows the energy cost of running linear vs agentic for this task
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
        f"Workflow comparison — {n_runs_planned} runs each</div>",
        unsafe_allow_html=True,
    )

    task_hist = hist[
        (hist["task_name"]  == sel_task) &
        (hist["model_name"] == sel_model) &
        (hist["provider"]   == sel_provider)
    ]

    if len(task_hist) >= 2:
        fig_cmp = go.Figure()
        for _, wrow in task_hist.iterrows():
            wf   = wrow["workflow_type"]
            e_j  = float(wrow["avg_energy_j"] or 0) * n_runs_planned
            dur  = float(wrow["avg_duration_s"] or 0) * n_runs_planned
            clr  = WF_COLORS.get(wf, "#94a3b8")
            fig_cmp.add_trace(go.Bar(
                x=[f"{wf}\n({n_runs_planned} runs)"],
                y=[e_j],
                name=wf,
                marker_color=clr,
                marker_line_width=0,
                text=[f"{e_j:.2f}J"],
                textposition="outside",
                textfont=dict(size=10),
            ))
        fig_cmp.update_layout(
            **PL, height=240, barmode="group",
            yaxis_title=f"Estimated energy (J) for {n_runs_planned} runs",
            showlegend=True,
        )
        st.plotly_chart(fig_cmp, use_container_width=True, key="budget_cmp_bar")

        # Tax multiplier for this budget
        lin_row = task_hist[task_hist["workflow_type"] == "linear"]
        age_row = task_hist[task_hist["workflow_type"] == "agentic"]
        if not lin_row.empty and not age_row.empty:
            lin_e = float(lin_row.iloc[0]["avg_energy_j"] or 0)
            age_e = float(age_row.iloc[0]["avg_energy_j"] or 0)
            if lin_e > 0:
                mult = age_e / lin_e
                extra_j = (age_e - lin_e) * n_runs_planned
                st.markdown(
                    f"<div style='padding:10px 14px;background:#1a0505;"
                    f"border-left:3px solid #ef4444;border-radius:0 8px 8px 0;"
                    f"font-size:11px;color:#fca5a5;"
                    f"font-family:IBM Plex Mono,monospace;'>"
                    f"Agentic costs <b style='color:#ef4444'>{mult:.2f}×</b> more than linear "
                    f"for {sel_task}. "
                    f"Running {n_runs_planned} agentic runs costs "
                    f"<b style='color:#f1f5f9'>{extra_j:.2f}J extra</b> "
                    f"vs {n_runs_planned} linear runs."
                    f"</div>",
                    unsafe_allow_html=True,
                )
    else:
        st.info(
            f"Only one workflow available for {sel_task} + {sel_model}. "
            f"Run both linear and agentic to see the comparison."
        )

    # ── Historical distribution for context ───────────────────────────────────
    with st.expander(
        f"Historical distribution — {hist_n} past runs of this combination"
    ):
        hist_runs = q(f"""
            SELECT r.total_energy_uj / 1e6 AS energy_j,
                   r.duration_ns / 1e9     AS duration_s
            FROM runs r
            JOIN experiments e ON r.exp_id = e.exp_id
            WHERE e.task_name    = '{sel_task}'
              AND e.model_name   = '{sel_model}'
              AND e.provider     = '{sel_provider}'
              AND r.workflow_type = '{sel_workflow}'
              AND r.total_energy_uj > 0
            ORDER BY r.run_id DESC
            LIMIT 200
        """)
        if not hist_runs.empty:
            fig_h = go.Figure(go.Histogram(
                x=hist_runs["energy_j"],
                nbinsx=30,
                marker_color=accent,
                marker_line_width=0,
                opacity=0.8,
            ))
            fig_h.add_vline(
                x=avg_e_j, line_dash="dot", line_color="#f1f5f9",
                annotation_text=f"mean {avg_e_j:.4f}J",
                annotation_font_size=9,
            )
            fig_h.update_layout(
                **PL, height=220,
                xaxis_title="Energy per run (J)",
                yaxis_title="Count",
                showlegend=False,
            )
            st.plotly_chart(fig_h, use_container_width=True, key="budget_hist")
