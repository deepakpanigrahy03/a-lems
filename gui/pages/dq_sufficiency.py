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
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.db     import q, q1
from gui.config import PL

MIN_RUNS = 30   # Statistical significance threshold per cell


def render(ctx: dict) -> None:
    accent = "#f472b6"

    # ── Load coverage matrix ──────────────────────────────────────────────────
    df = q("""
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

    if df.empty:
        st.info("No runs with complete metadata yet. Run some experiments first.")
        return

    df["runs_needed"] = (MIN_RUNS - df["run_count"]).clip(lower=0)
    df["sufficient"]  = df["run_count"] >= MIN_RUNS
    df["pct"]         = (df["run_count"] / MIN_RUNS * 100).clip(upper=100).round(1)
    df["hostname"]    = df["hostname"].fillna(f"hw_{df['hw_id']}")

    total_cells     = len(df)
    sufficient_cells = df["sufficient"].sum()
    total_runs_needed = df["runs_needed"].sum()
    readiness = round(sufficient_cells / total_cells * 100, 1) if total_cells else 0

    # ── Header — readiness score ──────────────────────────────────────────────
    health_clr = "#22c55e" if readiness >= 80 else \
                 "#f59e0b" if readiness >= 40 else "#ef4444"

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
                (sufficient_cells,    "Sufficient cells",   "#22c55e"),
                (total_cells - sufficient_cells, "Need more runs", "#f59e0b"),
                (int(total_runs_needed), "Runs needed",      "#ef4444"),
            ]
        ])
        + f"</div></div>"
        f"<div style='background:#1f2937;border-radius:3px;height:6px;'>"
        f"<div style='background:linear-gradient(90deg,{health_clr}99,{health_clr});"
        f"width:{readiness}%;height:100%;border-radius:3px;'></div></div>"
        f"<div style='font-size:10px;color:#475569;margin-top:6px;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"Threshold: {MIN_RUNS} runs per cell (hw × model × task × workflow)</div>"
        f"</div>",
        unsafe_allow_html=True)

    # ── Filters ───────────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        show_filter = st.selectbox(
            "Show", ["All cells", "Insufficient only", "Sufficient only"],
            key="dq_suf_filter")
    with col2:
        workflow_opts = ["All"] + sorted(df["workflow_type"].dropna().unique().tolist())
        wf_filter = st.selectbox("Workflow", workflow_opts, key="dq_suf_wf")
    with col3:
        model_opts = ["All"] + sorted(df["model_name"].dropna().unique().tolist())
        m_filter = st.selectbox("Model", model_opts, key="dq_suf_model")

    view = df.copy()
    if show_filter == "Insufficient only": view = view[~view["sufficient"]]
    if show_filter == "Sufficient only":   view = view[view["sufficient"]]
    if wf_filter != "All":                 view = view[view["workflow_type"] == wf_filter]
    if m_filter  != "All":                 view = view[view["model_name"]    == m_filter]

    # ── Coverage matrix heatmap ───────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 10px;'>"
        f"Coverage matrix — runs per cell</div>",
        unsafe_allow_html=True)

    # Pivot: rows = model+workflow, cols = task
    if not view.empty:
        view["cell_label"] = view["model_name"] + " · " + view["workflow_type"]
        pivot = view.pivot_table(
            index="cell_label", columns="task_name",
            values="run_count", aggfunc="sum", fill_value=0
        )
        # Colour: 0=red, 1-29=amber, 30+=green
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

    # ── Detailed table ────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 10px;'>"
        f"Cell detail — {len(view)} cells</div>",
        unsafe_allow_html=True)

    if view.empty:
        st.info("No cells match the current filters.")
        return

    display = view[[
        "hostname", "model_name", "task_name", "workflow_type",
        "run_count", "runs_needed", "pct"
    ]].copy()
    display.columns = [
        "Host", "Model", "Task", "Workflow",
        "Runs", "Still needed", "Progress %"
    ]
    display = display.sort_values("Progress %", ascending=True)
    st.dataframe(display, use_container_width=True, height=350)

    # ── Next experiment recommendations ───────────────────────────────────────
    gaps = view[~view["sufficient"]].sort_values("run_count").head(5)
    if not gaps.empty:
        st.markdown(
            f"<div style='margin-top:16px;font-size:11px;font-weight:600;"
            f"color:{accent};text-transform:uppercase;letter-spacing:.1em;"
            f"margin-bottom:10px;'>Top 5 gaps — run these next</div>",
            unsafe_allow_html=True)
        for _, row in gaps.iterrows():
            needed = int(row["runs_needed"])
            st.markdown(
                f"<div style='padding:10px 14px;background:#1a1a2e;"
                f"border:1px solid #f59e0b33;border-left:3px solid #f59e0b;"
                f"border-radius:0 8px 8px 0;margin-bottom:6px;"
                f"font-family:IBM Plex Mono,monospace;'>"
                f"<div style='font-size:12px;color:#f1f5f9;margin-bottom:3px;'>"
                f"<b>{row['model_name']}</b> · {row['task_name']} · {row['workflow_type']}"
                f"  <span style='color:#94a3b8;font-size:10px;'>on {row['hostname']}</span>"
                f"</div>"
                f"<div style='font-size:11px;color:#f59e0b;'>"
                f"Run {needed} more experiments to reach {MIN_RUNS}-run threshold "
                f"({int(row['run_count'])} / {MIN_RUNS} so far)</div>"
                f"</div>",
                unsafe_allow_html=True)
