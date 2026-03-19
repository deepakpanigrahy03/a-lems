"""
gui/pages/ml_export.py  —  ⬡  ML Export
─────────────────────────────────────────────────────────────────────────────
Export quality-scored, filtered datasets for model training pipelines.
Shows the ml_features view (70+ columns), quality filters, and export.
─────────────────────────────────────────────────────────────────────────────
"""

import json
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1

ACCENT = "#94a3b8"
MIN_RUNS = 30


def render(ctx: dict) -> None:
    # ── Check ml_features view ────────────────────────────────────────────────
    total = q1("SELECT COUNT(*) AS n FROM ml_features").get("n", 0) or 0

    if total == 0:
        st.info("No data in ml_features view yet. Run some experiments first.")
        return

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:6px;'>"
        f"ML Export Pipeline</div>"
        f"<div style='font-size:12px;color:#94a3b8;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"{total:,} rows in ml_features view · 70+ columns · "
        f"Filter → Quality-score → Export</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── Export filters ────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Export filters</div>",
        unsafe_allow_html=True,
    )

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        wf_opts = ["All"] + list(
            q(
                "SELECT DISTINCT workflow_type FROM ml_features "
                "WHERE workflow_type IS NOT NULL ORDER BY workflow_type"
            )
            .get("workflow_type", pd.Series())
            .tolist()
        )
        sel_wf = st.selectbox("Workflow", wf_opts, key="ml_wf")
    with col2:
        model_opts = ["All"] + list(
            q(
                "SELECT DISTINCT model_name FROM ml_features "
                "WHERE model_name IS NOT NULL ORDER BY model_name"
            )
            .get("model_name", pd.Series())
            .tolist()
        )
        sel_model = st.selectbox("Model", model_opts, key="ml_model")
    with col3:
        task_opts = ["All"] + list(
            q(
                "SELECT DISTINCT task_name FROM ml_features "
                "WHERE task_name IS NOT NULL ORDER BY task_name"
            )
            .get("task_name", pd.Series())
            .tolist()
        )
        sel_task = st.selectbox("Task", task_opts, key="ml_task")
    with col4:
        quality_filter = st.selectbox(
            "Quality",
            ["All runs", "Valid only", "With baseline", "Full quality"],
            key="ml_quality",
        )

    # Build WHERE
    where_parts = []
    if sel_wf != "All":
        where_parts.append(f"workflow_type = '{sel_wf}'")
    if sel_model != "All":
        where_parts.append(f"model_name = '{sel_model}'")
    if sel_task != "All":
        where_parts.append(f"task_name = '{sel_task}'")

    if quality_filter == "Valid only":
        where_parts.append(
            "run_id IN (SELECT run_id FROM runs WHERE COALESCE(experiment_valid,1)=1)"
        )
    elif quality_filter == "With baseline":
        where_parts.append(
            "run_id IN (SELECT run_id FROM runs WHERE baseline_id IS NOT NULL)"
        )
    elif quality_filter == "Full quality":
        where_parts.append("""run_id IN (
            SELECT run_id FROM runs
            WHERE COALESCE(experiment_valid,1)=1
              AND baseline_id IS NOT NULL
              AND COALESCE(thermal_throttle_flag,0)=0
        )""")

    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    # ── Feature preview ───────────────────────────────────────────────────────
    preview = q(f"""
        SELECT * FROM ml_features {where}
        ORDER BY run_id DESC LIMIT 500
    """)

    filtered_count = (
        q1(f"SELECT COUNT(*) AS n FROM ml_features {where}").get("n", 0) or 0
    )

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        st.metric("Filtered rows", f"{filtered_count:,}")
    with col_b:
        st.metric("Columns", f"{len(preview.columns) if not preview.empty else 0}")
    with col_c:
        pct = round(filtered_count / total * 100, 1) if total else 0
        st.metric("% of total", f"{pct}%")

    if preview.empty:
        st.info("No data matches the current filters.")
        return

    # ── Feature correlation heatmap — top energy correlates ──────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Top energy correlates — feature importance preview</div>",
        unsafe_allow_html=True,
    )

    numeric_cols = [
        "ipc",
        "cache_miss_rate",
        "total_tokens",
        "api_latency_ms",
        "interrupt_rate",
        "avg_power_watts",
        "duration_ms",
        "planning_time_ms",
        "llm_calls",
        "tool_calls",
        "complexity_score",
    ]
    avail = [c for c in numeric_cols if c in preview.columns]

    if "energy_j" in preview.columns and avail:
        corrs = []
        for col in avail:
            try:
                c = preview[["energy_j", col]].dropna()
                if len(c) > 10:
                    r = c.corr().iloc[0, 1]
                    corrs.append((col, round(r, 3)))
            except Exception:
                pass

        if corrs:
            corrs.sort(key=lambda x: abs(x[1]), reverse=True)
            cols_n = [c[0] for c in corrs]
            vals = [c[1] for c in corrs]
            colors = ["#22c55e" if v > 0 else "#ef4444" for v in vals]

            fig = go.Figure(
                go.Bar(
                    x=vals,
                    y=cols_n,
                    orientation="h",
                    marker_color=colors,
                    marker_line_width=0,
                    text=[f"{v:+.3f}" for v in vals],
                    textposition="outside",
                    textfont=dict(size=9),
                )
            )
            fig.add_vline(x=0, line_color="#475569", line_width=1)
            fig.update_layout(
                **{**PL, "margin": dict(l=140, r=60, t=10, b=30)},
                height=max(200, len(corrs) * 28),
                xaxis_title="Pearson correlation with energy_j",
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, key="ml_corr_bar")

    # ── Data preview ──────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Data preview — first 500 rows</div>",
        unsafe_allow_html=True,
    )

    # Select key columns for preview
    key_cols = [
        c
        for c in [
            "run_id",
            "workflow_type",
            "model_name",
            "task_name",
            "country_code",
            "energy_j",
            "dynamic_energy_j",
            "avg_power_watts",
            "duration_ms",
            "ipc",
            "cache_miss_rate",
            "total_tokens",
            "api_latency_ms",
            "interrupt_rate",
            "carbon_g",
            "energy_per_token",
        ]
        if c in preview.columns
    ]

    st.dataframe(preview[key_cols].round(4), use_container_width=True, height=300)

    # ── Export ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Export</div>",
        unsafe_allow_html=True,
    )

    col_e1, col_e2 = st.columns(2)

    with col_e1:
        # CSV export via Streamlit download
        if filtered_count <= 50000:
            export_df = q(f"SELECT * FROM ml_features {where} ORDER BY run_id DESC")
            if not export_df.empty:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv = export_df.to_csv(index=False)
                st.download_button(
                    label=f"⬇  Download CSV — {filtered_count:,} rows",
                    data=csv,
                    file_name=f"alems_ml_features_{ts}.csv",
                    mime="text/csv",
                    key="ml_csv_download",
                )
        else:
            st.warning(
                f"Dataset too large for browser download ({filtered_count:,} rows). "
                "Use SQL Query page to export directly."
            )

    with col_e2:
        # Export metadata card
        meta = {
            "exported_at": datetime.now().isoformat(),
            "filters": {
                "workflow": sel_wf,
                "model": sel_model,
                "task": sel_task,
                "quality": quality_filter,
            },
            "row_count": filtered_count,
            "column_count": len(preview.columns),
        }
        st.download_button(
            label="⬇  Download export metadata (JSON)",
            data=json.dumps(meta, indent=2),
            file_name=f"alems_export_meta_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            key="ml_meta_download",
        )

    st.markdown(
        f"<div style='margin-top:10px;padding:10px 14px;"
        f"background:#0c1f3a;border-left:3px solid #3b82f6;"
        f"border-radius:0 8px 8px 0;font-size:11px;"
        f"color:#93c5fd;font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
        f"<b>Training pipeline note:</b> Use <b>Full quality</b> filter for training data "
        f"(removes invalid runs, runs without baselines, and throttled runs). "
        f"The <code>energy_j</code> column is the primary regression target. "
        f"<code>orchestration_tax_j</code> is the agentic overhead target.</div>",
        unsafe_allow_html=True,
    )
