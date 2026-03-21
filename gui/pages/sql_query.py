"""
gui/pages/sql_query.py  —  💬  SQL Query (legacy)
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

    st.title("💬 SQL Query")
    st.caption(
        f"Ad-hoc SELECT queries against `{DB_PATH.name}` · results exportable as CSV"
    )

    QUERY_LIBRARY = {
        "— pick a preset —": "",
        "Energy by category": (
            "SELECT tc.category, r.workflow_type, COUNT(*) AS runs,\n"
            "  ROUND(AVG(r.total_energy_uj)/1e6,4) AS avg_energy_j,\n"
            "  ROUND(AVG(r.dynamic_energy_uj)/1e6,4) AS avg_dynamic_j\n"
            "FROM runs r\n"
            "JOIN experiments e ON r.exp_id=e.exp_id\n"
            "LEFT JOIN task_categories tc ON e.task_name=tc.task_id\n"
            "GROUP BY tc.category, r.workflow_type ORDER BY tc.category"
        ),
        "Tax breakdown by task": (
            "SELECT tc.category, e.task_name,\n"
            "  ROUND(AVG(ots.linear_dynamic_uj/1e6),4) AS linear_j,\n"
            "  ROUND(AVG(ots.agentic_dynamic_uj/1e6),4) AS agentic_j,\n"
            "  ROUND(AVG(ots.orchestration_tax_uj/1e6),4) AS tax_j,\n"
            "  ROUND(AVG(ots.tax_percent),2) AS tax_pct\n"
            "FROM orchestration_tax_summary ots\n"
            "JOIN runs rl ON ots.linear_run_id=rl.run_id\n"
            "JOIN experiments e ON rl.exp_id=e.exp_id\n"
            "LEFT JOIN task_categories tc ON e.task_name=tc.task_id\n"
            "GROUP BY tc.category, e.task_name"
        ),
        "Energy per token by model": (
            "SELECT e.model_name, e.provider,\n"
            "  ROUND(AVG(r.energy_per_token*1000),4) AS avg_mj_per_token,\n"
            "  COUNT(*) AS runs\n"
            "FROM runs r JOIN experiments e ON r.exp_id=e.exp_id\n"
            "WHERE r.total_tokens>0\n"
            "GROUP BY e.model_name, e.provider ORDER BY avg_mj_per_token"
        ),
        "Carbon by provider · region": (
            "SELECT e.provider, e.country_code,\n"
            "  ROUND(SUM(r.carbon_g)*1000,3) AS total_carbon_mg,\n"
            "  ROUND(SUM(r.water_ml),2) AS total_water_ml,\n"
            "  COUNT(*) AS runs\n"
            "FROM runs r JOIN experiments e ON r.exp_id=e.exp_id\n"
            "GROUP BY e.provider, e.country_code ORDER BY total_carbon_mg DESC"
        ),
        "Sample counts per run": (
            "SELECT r.run_id, r.workflow_type, e.task_name,\n"
            "  COUNT(DISTINCT es.sample_id) AS energy_samples,\n"
            "  COUNT(DISTINCT cs.sample_id) AS cpu_samples\n"
            "FROM runs r\n"
            "JOIN experiments e ON r.exp_id=e.exp_id\n"
            "LEFT JOIN energy_samples es ON r.run_id=es.run_id\n"
            "LEFT JOIN cpu_samples cs ON r.run_id=cs.run_id\n"
            "GROUP BY r.run_id ORDER BY r.run_id DESC LIMIT 20"
        ),
        "Recent runs": (
            "SELECT r.run_id, r.workflow_type, e.task_name, e.provider,\n"
            "  ROUND(r.total_energy_uj/1e6,4) AS energy_j,\n"
            "  ROUND(r.duration_ns/1e9,2) AS duration_s,\n"
            "  r.total_tokens, r.ipc\n"
            "FROM runs r JOIN experiments e ON r.exp_id=e.exp_id\n"
            "ORDER BY r.run_id DESC LIMIT 30"
        ),
        "Sustainability report": (
            "SELECT e.provider, tc.category,\n"
            "  ROUND(SUM(r.carbon_g),4) AS total_carbon_g,\n"
            "  ROUND(SUM(r.water_ml),2) AS total_water_ml,\n"
            "  COUNT(*) AS runs\n"
            "FROM runs r JOIN experiments e ON r.exp_id=e.exp_id\n"
            "LEFT JOIN task_categories tc ON e.task_name=tc.task_id\n"
            "GROUP BY e.provider, tc.category"
        ),
    }

    _preset = st.selectbox(
        "Preset queries", list(QUERY_LIBRARY.keys()), key="sql_preset"
    )
    _default_sql = QUERY_LIBRARY.get(_preset, "")

    _sql_input = st.text_area(
        "SQL (SELECT only)",
        value=_default_sql,
        height=150,
        key="sql_input",
        placeholder="SELECT * FROM runs LIMIT 10",
    )

    _col_r, _col_l = st.columns([2, 1])
    with _col_r:
        _sql_run = st.button("▶ Run query", type="primary", key="sql_run")
    with _col_l:
        _row_limit = st.number_input(
            "Row limit", 10, 10000, 500, step=100, key="sql_limit"
        )

    if _sql_run:
        _cleaned = _sql_input.strip()
        _upper = _cleaned.upper()
        _bad = [
            kw
            for kw in [
                "DROP",
                "DELETE",
                "UPDATE",
                "INSERT",
                "ALTER",
                "CREATE",
                "REPLACE",
                "ATTACH",
            ]
            if kw in _upper
        ]
        if _bad:
            st.error(f"Blocked keywords: {', '.join(_bad)}. SELECT only.")
        elif not _cleaned:
            st.warning("Enter a SQL query first.")
        else:
            if "LIMIT" not in _upper:
                _cleaned = f"SELECT * FROM ({_cleaned}) _q LIMIT {int(_row_limit)}"
            _result, _sql_err = q_safe(_cleaned)
            if _sql_err:
                st.error(f"SQL Error: {_sql_err}")
            elif _result.empty:
                st.info("Query returned 0 rows.")
            else:
                st.success(f"✓ {len(_result):,} rows")
                st.dataframe(_result, use_container_width=True, hide_index=True)
                st.download_button(
                    "⬇ Download CSV",
                    data=_result.to_csv(index=False),
                    file_name="alems_query.csv",
                    mime="text/csv",
                    key="sql_dl",
                )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: SETTINGS
# ══════════════════════════════════════════════════════════════════════════════
