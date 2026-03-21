"""
gui/pages/query_analysis.py  —  ◑  Query Analysis
Render function: render(ctx)
ctx keys: ov, runs, tax, lin, age, avg_lin_j, avg_age_j, tax_mult,
          plan_ms, exec_ms, synth_ms, plan_pct, exec_pct, synth_pct
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


def render(ctx: dict):
    ov = ctx["ov"]
    runs = ctx["runs"]
    tax = ctx["tax"]
    avg_lin_j = ctx["avg_lin_j"]
    avg_age_j = ctx["avg_age_j"]
    tax_mult = ctx["tax_mult"]
    plan_ms = ctx["plan_ms"]
    exec_ms = ctx["exec_ms"]
    synth_ms = ctx["synth_ms"]
    plan_pct = ctx["plan_pct"]
    exec_pct = ctx["exec_pct"]
    synth_pct = ctx["synth_pct"]
    lin = ctx["lin"]
    age = ctx["age"]

    st.title("Query Type Analysis")
    st.caption(
        "Energy · latency · tokens · sustainability — grouped by category and workflow"
    )

    # ── Level 1: category × workflow summary ──────────────────────────────────
    cat_df, _e1 = q_safe("""
        SELECT
            COALESCE(tc.category, 'uncategorised')           AS category,
            r.workflow_type,
            COUNT(*)                                          AS runs,
            ROUND(AVG(r.total_energy_uj)   / 1e6, 4)        AS avg_energy_j,
            ROUND(AVG(r.dynamic_energy_uj) / 1e6, 4)        AS avg_dynamic_j,
            ROUND(AVG(r.duration_ns)       / 1e9, 3)        AS avg_duration_s,
            ROUND(AVG(r.total_tokens),            1)        AS avg_tokens,
            ROUND(AVG(CASE WHEN r.total_tokens > 0
                THEN r.total_energy_uj / r.total_tokens END) / 1e3, 4) AS avg_mj_per_token,
            ROUND(AVG(r.total_energy_uj / 1e6 /
                NULLIF(r.duration_ns / 1e9, 0)),             4) AS avg_j_per_sec,
            ROUND(AVG(r.planning_time_ms),        1)        AS avg_plan_ms,
            ROUND(AVG(r.execution_time_ms),       1)        AS avg_exec_ms,
            ROUND(AVG(r.synthesis_time_ms),       1)        AS avg_synth_ms,
            ROUND(AVG(r.carbon_g) * 1000,         4)        AS avg_carbon_mg,
            ROUND(AVG(r.water_ml),                4)        AS avg_water_ml,
            ROUND(AVG(es_agg.core_j),             4)        AS avg_core_j,
            ROUND(AVG(es_agg.uncore_j),           4)        AS avg_uncore_j,
            ROUND(AVG(es_agg.dram_j),             4)        AS avg_dram_j
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        LEFT JOIN task_categories tc ON e.task_name = tc.task_id
        LEFT JOIN (
            SELECT run_id,
                   (MAX(core_energy_uj)   - MIN(core_energy_uj))   / 1e6 AS core_j,
                   (MAX(uncore_energy_uj) - MIN(uncore_energy_uj)) / 1e6 AS uncore_j,
                   (MAX(dram_energy_uj)   - MIN(dram_energy_uj))   / 1e6 AS dram_j
            FROM energy_samples GROUP BY run_id
        ) es_agg ON r.run_id = es_agg.run_id
        GROUP BY COALESCE(tc.category,'uncategorised'), r.workflow_type
        ORDER BY category, r.workflow_type
    """)

    if _e1:
        st.error(f"Query error: {_e1}")
    elif cat_df.empty:
        st.info(
            "No data — run experiments and ensure task_categories table is populated."
        )
    else:
        # KPI row
        _lin_cat = cat_df[cat_df.workflow_type == "linear"]
        _age_cat = cat_df[cat_df.workflow_type == "agentic"]
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Categories", cat_df.category.nunique())
        k2.metric("Total runs", int(cat_df.runs.sum()))
        k3.metric(
            "Best mJ/token",
            (
                f"{cat_df.avg_mj_per_token.min():.4f}"
                if cat_df.avg_mj_per_token.notna().any()
                else "—"
            ),
        )
        k4.metric(
            "Avg linear J",
            f"{_lin_cat.avg_energy_j.mean():.3f}J" if not _lin_cat.empty else "—",
        )
        k5.metric(
            "Avg agentic J",
            f"{_age_cat.avg_energy_j.mean():.3f}J" if not _age_cat.empty else "—",
        )

        st.divider()

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Energy per query by category**")
            fig = px.bar(
                cat_df.dropna(subset=["avg_energy_j"]),
                x="category",
                y="avg_energy_j",
                color="workflow_type",
                barmode="group",
                color_discrete_map=WF_COLORS,
                labels={"avg_energy_j": "Avg Energy (J)", "category": "Category"},
            )
            st.plotly_chart(fl(fig), use_container_width=True)
        with c2:
            st.markdown("**Energy per token (mJ)**")
            fig2 = px.bar(
                cat_df.dropna(subset=["avg_mj_per_token"]),
                x="category",
                y="avg_mj_per_token",
                color="workflow_type",
                barmode="group",
                color_discrete_map=WF_COLORS,
                labels={"avg_mj_per_token": "mJ / token", "category": "Category"},
            )
            st.plotly_chart(fl(fig2), use_container_width=True)

        c3, c4 = st.columns(2)
        with c3:
            st.markdown("**Phase time breakdown (agentic)**")
            _ap = cat_df[cat_df.workflow_type == "agentic"].copy()
            if not _ap.empty:
                _ph = _ap[
                    ["category", "avg_plan_ms", "avg_exec_ms", "avg_synth_ms"]
                ].melt(id_vars="category", var_name="phase", value_name="ms")
                _ph["phase"] = _ph["phase"].map(
                    {
                        "avg_plan_ms": "Planning",
                        "avg_exec_ms": "Execution",
                        "avg_synth_ms": "Synthesis",
                    }
                )
                fig3 = px.bar(
                    _ph.dropna(),
                    x="category",
                    y="ms",
                    color="phase",
                    barmode="stack",
                    color_discrete_map={
                        "Planning": "#f59e0b",
                        "Execution": "#3b82f6",
                        "Synthesis": "#a78bfa",
                    },
                    labels={"ms": "ms", "category": "Category"},
                )
                st.plotly_chart(fl(fig3), use_container_width=True)
            else:
                st.info("No agentic data.")
        with c4:
            st.markdown("**Hardware domain breakdown (linear)**")
            _hl = cat_df[cat_df.workflow_type == "linear"].copy()
            if not _hl.empty:
                _hm = _hl[
                    ["category", "avg_core_j", "avg_uncore_j", "avg_dram_j"]
                ].melt(id_vars="category", var_name="domain", value_name="j")
                _hm["domain"] = _hm["domain"].map(
                    {
                        "avg_core_j": "Core",
                        "avg_uncore_j": "Uncore",
                        "avg_dram_j": "DRAM",
                    }
                )
                fig4 = px.bar(
                    _hm.dropna(),
                    x="category",
                    y="j",
                    color="domain",
                    barmode="stack",
                    color_discrete_map={
                        "Core": "#3b82f6",
                        "Uncore": "#38bdf8",
                        "DRAM": "#a78bfa",
                    },
                    labels={"j": "Joules", "category": "Category"},
                )
                st.plotly_chart(fl(fig4), use_container_width=True)
            else:
                st.info("No linear data.")

        st.divider()

        # ── Level 2: per-task ─────────────────────────────────────────────────
        st.markdown("### Level 2 — Per-task detail")
        _sel_cat = st.selectbox(
            "Filter category",
            ["all"] + sorted(cat_df.category.dropna().unique().tolist()),
            key="qa_cat",
        )
        _cat_where = f"WHERE tc.category = '{_sel_cat}'" if _sel_cat != "all" else ""

        task_df, _e2 = q_safe(f"""
            SELECT e.task_name,
                   COALESCE(tc.category,'uncategorised') AS category,
                   r.workflow_type,
                   COUNT(*) AS runs,
                   ROUND(AVG(r.total_energy_uj)/1e6, 4)  AS avg_energy_j,
                   ROUND(AVG(r.duration_ns)/1e9,    3)   AS avg_duration_s,
                   ROUND(AVG(r.total_tokens),        1)  AS avg_tokens,
                   ROUND(AVG(CASE WHEN r.total_tokens > 0
                       THEN r.total_energy_uj/r.total_tokens END)/1e3, 4) AS avg_mj_per_token,
                   ROUND(AVG(r.carbon_g)*1000,       4)  AS avg_carbon_mg,
                   ROUND(AVG(r.water_ml),            4)  AS avg_water_ml,
                   ROUND(AVG(r.llm_calls),           1)  AS avg_llm_calls,
                   ROUND(AVG(r.tool_calls),          1)  AS avg_tool_calls
            FROM runs r
            JOIN experiments e ON r.exp_id = e.exp_id
            LEFT JOIN task_categories tc ON e.task_name = tc.task_id
            {_cat_where}
            GROUP BY e.task_name, COALESCE(tc.category,'uncategorised'), r.workflow_type
            ORDER BY avg_energy_j DESC
        """)
        if _e2:
            st.error(_e2)
        elif not task_df.empty:
            fig5 = px.bar(
                task_df.dropna(subset=["avg_energy_j"]),
                x="task_name",
                y="avg_energy_j",
                color="workflow_type",
                barmode="group",
                color_discrete_map=WF_COLORS,
                hover_data=["category", "avg_tokens", "avg_mj_per_token"],
                labels={"avg_energy_j": "Avg Energy (J)", "task_name": "Task"},
            )
            fig5.update_xaxes(tickangle=30)
            st.plotly_chart(fl(fig5), use_container_width=True)
            _sc = [
                c
                for c in [
                    "task_name",
                    "category",
                    "workflow_type",
                    "runs",
                    "avg_energy_j",
                    "avg_duration_s",
                    "avg_tokens",
                    "avg_mj_per_token",
                    "avg_carbon_mg",
                    "avg_water_ml",
                    "avg_llm_calls",
                    "avg_tool_calls",
                ]
                if c in task_df.columns
            ]
            st.dataframe(task_df[_sc], use_container_width=True, hide_index=True)

        st.divider()

        # ── Level 3: human-scale insights ────────────────────────────────────
        st.markdown("### Level 3 — Human-scale energy insights")
        st.caption("Translating joules into things you can feel")

        if not runs.empty and "task_name" in runs.columns:
            _t_opts = sorted(runs.task_name.dropna().unique().tolist())
            _sel_t = st.selectbox(
                "Select task to interpret", _t_opts, key="qa_human_task"
            )
            _tr = runs[runs.task_name == _sel_t]

            for _wf, _border in [("linear", "#22c55e"), ("agentic", "#ef4444")]:
                _wr = _tr[_tr.workflow_type == _wf]
                if _wr.empty:
                    continue
                _ej = float(_wr.energy_j.mean())
                _wml = (
                    float(_wr.water_ml.mean())
                    if "water_ml" in _wr.columns and _wr.water_ml.notna().any()
                    else 0
                )
                _cmg = (
                    float(_wr.carbon_g.mean() * 1000)
                    if "carbon_g" in _wr.columns and _wr.carbon_g.notna().any()
                    else 0
                )
                _tok = (
                    float(_wr.total_tokens.mean())
                    if "total_tokens" in _wr.columns and _wr.total_tokens.notna().any()
                    else 0
                )
                _dur = (
                    float(_wr.duration_ms.mean() / 1000)
                    if "duration_ms" in _wr.columns and _wr.duration_ms.notna().any()
                    else 0
                )

                _ins = _human_energy(_ej)
                _ins_html = "".join(
                    f"<div style='margin:2px 0;font-size:10px;color:#b8c8d8;'>{ic} {desc}</div>"
                    for ic, desc in _ins
                )
                st.markdown(
                    f"""
                <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                            padding:14px 18px;margin-bottom:8px;border-left:3px solid {_border};">
                  <div style="font-size:12px;font-weight:600;color:#e8f0f8;margin-bottom:6px;">
                    {_wf.upper()} · {_sel_t}
                    <span style="font-family:monospace;color:{_border};margin-left:12px;">
                      {_ej:.4f}J</span>
                    {f'<span style="font-size:9px;color:#3d5570;margin-left:8px;">{_tok:.0f} tokens · {_dur:.1f}s</span>' if _tok > 0 else ''}
                  </div>
                  {_ins_html}
                  <div style="margin-top:8px;font-size:10px;color:#7090b0;border-top:1px solid #1e2d45;padding-top:6px;">
                    💧 {_human_water(_wml)} &nbsp;·&nbsp; 🌱 {_human_carbon(_cmg)}
                  </div>
                </div>""",
                    unsafe_allow_html=True,
                )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: RUN REPLAY (formerly Live Monitor)
# Renamed — "live" only makes sense during execution.
# This page lets you inspect any completed run as an interactive timeline.
# ══════════════════════════════════════════════════════════════════════════════
