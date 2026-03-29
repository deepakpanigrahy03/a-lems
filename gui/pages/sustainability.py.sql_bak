"""
gui/pages/sustainability.py  —  ♻  Sustainability
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

    st.title("♻ Sustainability Metrics")
    st.caption("Carbon · Water · Methane — per run, per token, per provider")

    _sus, _sus_e = q_safe("""
        SELECT e.provider, e.model_name, r.workflow_type, e.task_name,
               COUNT(*) AS runs,
               ROUND(AVG(r.carbon_g)*1000, 4) AS avg_carbon_mg,
               ROUND(SUM(r.carbon_g)*1000, 3)  AS total_carbon_mg,
               ROUND(AVG(r.water_ml), 4)         AS avg_water_ml,
               ROUND(SUM(r.water_ml), 2)          AS total_water_ml,
               ROUND(AVG(r.methane_mg), 4)         AS avg_methane_mg,
               ROUND(AVG(r.total_energy_uj)/1e6, 4) AS avg_energy_j,
               ROUND(AVG(CASE WHEN r.total_tokens>0
                   THEN r.carbon_g/r.total_tokens*1e6 END), 4) AS ug_carbon_per_token
        FROM runs r JOIN experiments e ON r.exp_id=e.exp_id
        WHERE r.carbon_g IS NOT NULL
        GROUP BY e.provider, e.model_name, r.workflow_type, e.task_name
        ORDER BY total_carbon_mg DESC
    """)

    if _sus_e or _sus.empty:
        st.info(
            "No sustainability data yet — carbon_g / water_ml columns must be populated."
        )
    else:
        # KPI row
        _tc = _sus.total_carbon_mg.sum()
        _tw = _sus.total_water_ml.sum()
        _avg_ept = (
            _sus.ug_carbon_per_token.mean()
            if _sus.ug_carbon_per_token.notna().any()
            else 0
        )
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total carbon", f"{_tc:.2f}mg CO₂e")
        k2.metric("Total water", f"{_tw:.2f}ml")
        k3.metric("µg CO₂/token", f"{_avg_ept:.4f}")
        k4.metric("Human comparison", _human_water(_sus.avg_water_ml.mean()))

        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Carbon (mg) by provider × workflow**")
            _fig_c = px.bar(
                _sus.groupby(["provider", "workflow_type"])["avg_carbon_mg"]
                .mean()
                .reset_index(),
                x="provider",
                y="avg_carbon_mg",
                color="workflow_type",
                barmode="group",
                color_discrete_map=WF_COLORS,
                labels={"avg_carbon_mg": "Avg CO₂ (mg)"},
            )
            st.plotly_chart(fl(_fig_c), use_container_width=True)
        with c2:
            st.markdown("**Water (ml) by provider × workflow**")
            _fig_w = px.bar(
                _sus.groupby(["provider", "workflow_type"])["avg_water_ml"]
                .mean()
                .reset_index(),
                x="provider",
                y="avg_water_ml",
                color="workflow_type",
                barmode="group",
                color_discrete_map=WF_COLORS,
                labels={"avg_water_ml": "Avg Water (ml)"},
            )
            st.plotly_chart(fl(_fig_w), use_container_width=True)

        st.markdown("**Full sustainability table**")
        _sc = [
            c
            for c in [
                "provider",
                "model_name",
                "workflow_type",
                "task_name",
                "runs",
                "avg_carbon_mg",
                "total_carbon_mg",
                "avg_water_ml",
                "total_water_ml",
                "avg_methane_mg",
                "ug_carbon_per_token",
            ]
            if c in _sus.columns
        ]
        st.dataframe(_sus[_sc].round(4), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: AGENTIC VS LINEAR  (new dedicated comparison page)
# ══════════════════════════════════════════════════════════════════════════════
