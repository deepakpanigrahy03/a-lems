"""
gui/pages/dq_sufficiency.py  —  ◈  Sufficiency Advisor
─────────────────────────────────────────────────────────────────────────────
FLAGSHIP DATA QUALITY PAGE.

Tells researchers exactly how many more experiments they need to run
before their data is statistically sufficient for each cell:
  cell = hardware (hw_id) × model × task × workflow_type

Threshold: MIN_RUNS_PER_CELL = 30 (configurable)

For each cell shows:
  • Current run count
  • Runs needed to reach threshold
  • Progress bar
  • Which combinations have ZERO data (biggest gaps)
  • Overall dataset readiness score

Phase 2 addition:
  • coverage_matrix table integration — fast persistent lookup
  • Refresh button writes live counts to coverage_matrix
  • Last-updated timestamp so researcher knows if data is stale
  • Auto-suggest experiment commands for top 5 gaps
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL
from gui.db import q, q1
from gui.db_migrations import refresh_coverage_matrix

MIN_RUNS = 30  # Statistical significance threshold per cell


def _load_from_coverage_table() -> pd.DataFrame:
    """
    Load from the persistent coverage_matrix table.
    Returns empty df if table is empty — caller falls back to live query.
    """
    return q("""
        SELECT
            cm.hw_id,
            cm.model_name,
            cm.task_name,
            cm.workflow_type,
            cm.run_count,
            cm.last_updated,
            h.hostname
        FROM coverage_matrix cm
        LEFT JOIN hardware_config h ON cm.hw_id = h.hw_id
        ORDER BY cm.run_count DESC
    """)


def _load_from_live_query() -> pd.DataFrame:
    """
    Compute coverage directly from runs table — source of truth.
    Slower than coverage_matrix table but always accurate.
    """
    return q("""
        SELECT
            h.hostname,
            r.hw_id,
            e.model_name,
            e.provider,
            e.task_name,
            e.workflow_type,
            COUNT(*) AS run_count,
            MIN(r.start_time_ns) AS first_run_ns,
            MAX(r.start_time_ns) AS last_run_ns
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        LEFT JOIN hardware_config h ON r.hw_id = h.hw_id
        WHERE e.model_name IS NOT NULL
          AND e.task_name  IS NOT NULL
          AND e.workflow_type IS NOT NULL
        GROUP BY r.hw_id, e.model_name, e.task_name, e.workflow_type
        ORDER BY run_count DESC
    """)


def render(ctx: dict) -> None:
    accent = "#f472b6"

    # ── Coverage matrix status bar ────────────────────────────────────────────
    # Check if coverage_matrix table has data and when it was last refreshed.
    # This tells the researcher whether the cached table is stale.
    cm_meta = q1("""
        SELECT
            COUNT(*)        AS cell_count,
            MAX(last_updated) AS last_updated,
            SUM(run_count)  AS total_runs_cached
        FROM coverage_matrix
    """) or {}

    cm_cell_count   = int(cm_meta.get("cell_count", 0) or 0)
    cm_last_updated = cm_meta.get("last_updated") or None
    cm_has_data     = cm_cell_count > 0

    # ── Controls row — refresh button + data source indicator ─────────────────
    ctrl1, ctrl2, ctrl3 = st.columns([3, 2, 2])

    with ctrl1:
        # Show when coverage_matrix was last refreshed
        if cm_has_data and cm_last_updated:
            st.markdown(
                f"<div style='padding:8px 12px;background:#0c1f0c;"
                f"border:1px solid #22c55e33;border-radius:8px;"
                f"font-size:10px;font-family:IBM Plex Mono,monospace;'>"
                f"<span style='color:#22c55e;'>✓ Coverage table:</span> "
                f"<span style='color:#94a3b8;'>{cm_cell_count} cells cached · "
                f"last updated {str(cm_last_updated)[:16]}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"<div style='padding:8px 12px;background:#1a0e00;"
                f"border:1px solid #f59e0b33;border-radius:8px;"
                f"font-size:10px;font-family:IBM Plex Mono,monospace;'>"
                f"<span style='color:#f59e0b;'>⚠ Coverage table empty</span> "
                f"<span style='color:#94a3b8;'>— click Refresh to populate</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

    with ctrl2:
        # Refresh button — recomputes from live runs and writes to coverage_matrix
        if st.button("⟳ Refresh Coverage Table",
                     use_container_width=True, key="suf_refresh"):
            with st.spinner("Computing coverage from live runs..."):
                rows_updated = refresh_coverage_matrix()
            st.success(f"✓ Coverage table updated — {rows_updated} cells written.")
            st.rerun()

    with ctrl3:
        # Data source toggle — use cached table or always query live
        use_cache = st.checkbox(
            "Use cached table (faster)",
            value=cm_has_data,
            key="suf_use_cache",
            help="Uncheck to always query live from runs table — slower but always current",
        )

    st.markdown("<div style='margin-bottom:8px;'></div>", unsafe_allow_html=True)

    # ── Load data — from cache or live ───────────────────────────────────────
    if use_cache and cm_has_data:
        df = _load_from_coverage_table()
        data_source = "coverage_matrix table"
    else:
        df = _load_from_live_query()
        data_source = "live query"

    if df.empty:
        st.info("No runs with complete metadata yet. Run some experiments first.")
        return

    # ── Derived columns ───────────────────────────────────────────────────────
    df["runs_needed"] = (MIN_RUNS - df["run_count"]).clip(lower=0)
    df["sufficient"]  = df["run_count"] >= MIN_RUNS
    df["pct"]         = (df["run_count"] / MIN_RUNS * 100).clip(upper=100).round(1)
    df["hostname"]    = df["hostname"].fillna("hw_" + df["hw_id"].astype(str))

    total_cells        = len(df)
    sufficient_cells   = int(df["sufficient"].sum())
    total_runs_needed  = int(df["runs_needed"].sum())
    readiness          = round(sufficient_cells / total_cells * 100, 1) if total_cells else 0

    # ── Header — readiness score ──────────────────────────────────────────────
    health_clr = (
        "#22c55e" if readiness >= 80 else
        "#f59e0b" if readiness >= 40 else
        "#ef4444"
    )

    st.markdown(
        f"<div style='padding:20px 24px;"
        f"background:linear-gradient(135deg,{accent}12,{accent}06);"
        f"border:1px solid {accent}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='display:flex;align-items:center;gap:24px;margin-bottom:12px;'>"
        f"<div>"
        f"<div style='font-size:40px;font-weight:800;color:{health_clr};"
        f"font-family:IBM Plex Mono,monospace;line-height:1;'>{readiness}%</div>"
        f"<div style='font-size:10px;color:#94a3b8;text-transform:uppercase;"
        f"letter-spacing:.1em;margin-top:2px;'>Dataset readiness</div>"
        f"</div>"
        f"<div style='flex:1;display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;'>"
        + "".join([
            f"<div style='text-align:center;'>"
            f"<div style='font-size:20px;font-weight:700;color:{c};"
            f"font-family:IBM Plex Mono,monospace;'>{v}</div>"
            f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-top:2px;'>{l}</div></div>"
            for v, l, c in [
                (sufficient_cells,              "Sufficient cells", "#22c55e"),
                (total_cells - sufficient_cells, "Need more runs",  "#f59e0b"),
                (total_runs_needed,              "Runs needed",     "#ef4444"),
            ]
        ])
        + f"</div></div>"
        f"<div style='background:#1f2937;border-radius:3px;height:6px;'>"
        f"<div style='background:linear-gradient(90deg,{health_clr}99,{health_clr});"
        f"width:{readiness}%;height:100%;border-radius:3px;'></div></div>"
        f"<div style='font-size:10px;color:#475569;margin-top:6px;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"Threshold: {MIN_RUNS} runs per cell (hw × model × task × workflow) · "
        f"Source: {data_source}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── Filters ───────────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        show_filter = st.selectbox(
            "Show",
            ["All cells", "Insufficient only", "Sufficient only"],
            key="dq_suf_filter",
        )
    with col2:
        workflow_opts = ["All"] + sorted(df["workflow_type"].dropna().unique().tolist())
        wf_filter = st.selectbox("Workflow", workflow_opts, key="dq_suf_wf")
    with col3:
        model_opts = ["All"] + sorted(df["model_name"].dropna().unique().tolist())
        m_filter = st.selectbox("Model", model_opts, key="dq_suf_model")

    view = df.copy()
    if show_filter == "Insufficient only":
        view = view[~view["sufficient"]]
    if show_filter == "Sufficient only":
        view = view[view["sufficient"]]
    if wf_filter != "All":
        view = view[view["workflow_type"] == wf_filter]
    if m_filter != "All":
        view = view[view["model_name"] == m_filter]

    # ── Coverage matrix heatmap (original — unchanged) ────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 10px;'>"
        f"Coverage matrix — runs per cell</div>",
        unsafe_allow_html=True,
    )

    if not view.empty:
        view["cell_label"] = view["model_name"] + " · " + view["workflow_type"]
        pivot = view.pivot_table(
            index="cell_label",
            columns="task_name",
            values="run_count",
            aggfunc="sum",
            fill_value=0,
        )
        z_text = pivot.values.tolist()
        z_vals = [[min(v / MIN_RUNS, 1.0) for v in row] for row in z_text]

        fig = go.Figure(go.Heatmap(
            z=z_vals,
            x=list(pivot.columns),
            y=list(pivot.index),
            text=z_text,
            texttemplate="%{text}",
            textfont=dict(size=10),
            colorscale=[
                [0.0,  "#2a0c0c"],
                [0.01, "#7f1d1d"],
                [0.5,  "#854d0e"],
                [1.0,  "#14532d"],
            ],
            showscale=True,
            colorbar=dict(
                title=f"/{MIN_RUNS}",
                tickvals=[0, 0.5, 1.0],
                ticktext=["0", f"{MIN_RUNS//2}", f"≥{MIN_RUNS}"],
                tickfont=dict(size=9),
            ),
        ))
        fig.update_layout(
            **{**PL, "margin": dict(l=180, r=80, t=20, b=80)},
            height=max(300, len(pivot) * 40 + 80),
            xaxis_tickangle=-30,
        )
        st.plotly_chart(fig, use_container_width=True, key="dq_suf_heatmap")

    # ── Detailed table (original — unchanged) ─────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 10px;'>"
        f"Cell detail — {len(view)} cells</div>",
        unsafe_allow_html=True,
    )

    if view.empty:
        st.info("No cells match the current filters.")
        return

    display = view[[
        "hostname", "model_name", "task_name",
        "workflow_type", "run_count", "runs_needed", "pct",
    ]].copy()
    display.columns = [
        "Host", "Model", "Task", "Workflow",
        "Runs", "Still needed", "Progress %",
    ]
    display = display.sort_values("Progress %", ascending=True)
    st.dataframe(display, use_container_width=True, height=350)

    # ── Top 5 gaps with auto-generated run commands ───────────────────────────
    # Phase 2 addition: generate the exact command the researcher needs to run
    # so they can copy-paste it directly into the Execute tab or terminal.
    gaps = view[~view["sufficient"]].sort_values("run_count").head(5)
    if not gaps.empty:
        st.markdown(
            f"<div style='margin-top:16px;font-size:11px;font-weight:600;"
            f"color:{accent};text-transform:uppercase;letter-spacing:.1em;"
            f"margin-bottom:10px;'>Top 5 gaps — run these next</div>",
            unsafe_allow_html=True,
        )
        for _, row in gaps.iterrows():
            needed   = int(row["runs_needed"])
            model    = str(row.get("model_name", "?"))
            task     = str(row.get("task_name",  "?"))
            workflow = str(row.get("workflow_type", "?"))
            host     = str(row.get("hostname", "?"))
            provider = str(row.get("provider", "cloud")) if "provider" in row.index else "cloud"

            # Auto-generate the command to close this gap
            suggested_cmd = (
                f"python -m core.execution.tests.test_harness "
                f"--task-id {task} --provider {provider} "
                f"--repetitions {needed} --save-db"
            )

            st.markdown(
                f"<div style='padding:10px 14px;background:#1a1a2e;"
                f"border:1px solid #f59e0b33;border-left:3px solid #f59e0b;"
                f"border-radius:0 8px 8px 0;margin-bottom:8px;'>"
                f"<div style='font-size:12px;color:#f1f5f9;margin-bottom:4px;'>"
                f"<b>{model}</b> · {task} · {workflow}"
                f"  <span style='color:#94a3b8;font-size:10px;'>on {host}</span>"
                f"</div>"
                f"<div style='font-size:11px;color:#f59e0b;margin-bottom:6px;'>"
                f"Run {needed} more to reach {MIN_RUNS}-run threshold "
                f"({int(row['run_count'])} / {MIN_RUNS} so far · "
                f"{int(row['pct'])}% complete)</div>"
                f"<div style='font-size:9px;color:#475569;margin-bottom:4px;'>"
                f"Suggested command:</div>"
                f"<code style='font-size:9px;color:#38bdf8;"
                f"background:#050c18;padding:4px 8px;border-radius:4px;"
                f"display:block;overflow-x:auto;white-space:nowrap;'>"
                f"{suggested_cmd}</code>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # ── Coverage table health note ─────────────────────────────────────────────
    # Reminds researcher to refresh after running new experiments
    if cm_has_data:
        st.markdown(
            f"<div style='margin-top:16px;padding:10px 14px;"
            f"background:#0c1f3a;border-left:3px solid #3b82f6;"
            f"border-radius:0 8px 8px 0;font-size:11px;"
            f"color:#93c5fd;font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
            f"<b>Tip:</b> The coverage table was last updated {str(cm_last_updated)[:16]}. "
            f"After running new experiments, click <b>⟳ Refresh Coverage Table</b> "
            f"to update the counts. The live query option always reflects current data "
            f"but is slower on large databases."
            f"</div>",
            unsafe_allow_html=True,
        )
