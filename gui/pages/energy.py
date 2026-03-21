"""
gui/pages/energy.py  —  ⚡  Energy
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

    st.title("Energy Analysis")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Min Energy", f"{ov.get('min_energy_j',0) or 0:.3f}J")
    c2.metric("Max Energy", f"{ov.get('max_energy_j',0) or 0:.3f}J")
    c3.metric("Total Measured", f"{ov.get('total_energy_j',0) or 0:.1f}J")
    c4.metric("Avg Carbon", f"{ov.get('avg_carbon_mg',0) or 0:.3f}mg")
    c5.metric("Avg Water", f"{ov.get('avg_water_ml',0) or 0:.3f}ml")
    st.divider()

    if not runs.empty and "energy_j" in runs.columns:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Energy per run — sorted, log scale**")
            sr = (
                runs.dropna(subset=["energy_j"])
                .sort_values("energy_j")
                .reset_index(drop=True)
            )
            sr["run_idx"] = sr.index
            fig = px.bar(
                sr,
                x="run_idx",
                y="energy_j",
                color="workflow_type",
                color_discrete_map=WF_COLORS,
                log_y=True,
                hover_data=["run_id", "provider", "task_name"],
                labels={"energy_j": "Energy (J)", "run_idx": "Run (sorted)"},
            )
            fig.update_xaxes(showticklabels=False)
            st.plotly_chart(fl(fig), use_container_width=True)
            st.caption("Log scale — agentic runs cluster at the top.")

        with col2:
            st.markdown("**Carbon by provider · region**")
            if "carbon_g" in runs.columns:
                _cd = runs.dropna(subset=["carbon_g"]).copy()
                _cd["group"] = (
                    _cd["provider"].fillna("?") + "·" + _cd["country_code"].fillna("?")
                )
                _cd["carbon_mg"] = _cd["carbon_g"] * 1000
                _ca = _cd.groupby("group")["carbon_mg"].mean().reset_index()
                fig3 = px.bar(
                    _ca,
                    x="group",
                    y="carbon_mg",
                    log_y=True,
                    color="group",
                    labels={"carbon_mg": "avg mg CO₂e", "group": ""},
                )
                st.plotly_chart(fl(fig3), use_container_width=True)
                st.caption(
                    "IN grid (0.82 kg/kWh) = 2× US factor — same energy, double carbon."
                )

        st.divider()

        if "api_latency_ms" in runs.columns:
            _cl = runs[
                (runs.provider != "local")
                & runs.api_latency_ms.notna()
                & runs.energy_j.notna()
            ].copy()
            _cl["api_latency_s"] = _cl["api_latency_ms"] / 1000
            if not _cl.empty:
                st.markdown(
                    "**Energy vs API latency** — longer wait = more idle RAPL drain"
                )
                fig4 = px.scatter(
                    _cl,
                    x="api_latency_s",
                    y="energy_j",
                    color="country_code",
                    log_y=True,
                    hover_data=["run_id", "workflow_type"],
                    labels={
                        "api_latency_s": "API Latency (s)",
                        "energy_j": "Energy (J)",
                    },
                )
                st.plotly_chart(fl(fig4), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# CPU & C-STATES
# ══════════════════════════════════════════════════════════════════════════════
