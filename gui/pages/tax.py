"""
gui/pages/tax.py  —  ▲  Tax Attribution
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

    st.title("Tax Attribution")

    if not tax.empty:
        avg_tax = float(tax.tax_percent.mean()) if "tax_percent" in tax.columns else 0
        max_tax = float(tax.tax_percent.max()) if "tax_percent" in tax.columns else 0

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(
                f"""
            <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                        padding:14px 16px;border-left:3px solid #f59e0b;">
              <div style="font-size:11px;font-weight:600;color:#f59e0b;margin-bottom:8px;">
                ① Planning Phase Tax</div>
              <div style="font-size:10px;color:#7090b0;line-height:1.65;">
                Avg <strong style="color:#e8f0f8">{plan_ms:.0f}ms</strong> before any useful work.
                Memoizing plans for repeated tasks could recover &gt;40% of queries.</div>
            </div>""",
                unsafe_allow_html=True,
            )
        with col2:
            st.markdown(
                f"""
            <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                        padding:14px 16px;border-left:3px solid #3b82f6;">
              <div style="font-size:11px;font-weight:600;color:#3b82f6;margin-bottom:8px;">
                ② Tool API Latency Tax</div>
              <div style="font-size:10px;color:#7090b0;line-height:1.65;">
                Execution phase: <strong style="color:#e8f0f8">{exec_ms:.0f}ms</strong>.
                CPU idles during API wait but RAPL keeps charging. Async dispatch = 40–60% cut.</div>
            </div>""",
                unsafe_allow_html=True,
            )
        with col3:
            st.markdown(
                f"""
            <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                        padding:14px 16px;border-left:3px solid #ef4444;">
              <div style="font-size:11px;font-weight:600;color:#ef4444;margin-bottom:8px;">
                ③ Measured Tax: avg {avg_tax:.1f}% · peak {max_tax:.1f}%</div>
              <div style="font-size:10px;color:#7090b0;line-height:1.65;">
                Route simple tasks linearly — removes planning + synthesis entirely.
                Classifier overhead &lt;1ms.</div>
            </div>""",
                unsafe_allow_html=True,
            )

        st.divider()
        sc = [
            c
            for c in [
                "comparison_id",
                "task_name",
                "provider",
                "country_code",
                "linear_dynamic_j",
                "agentic_dynamic_j",
                "tax_j",
                "tax_percent",
                "planning_time_ms",
                "execution_time_ms",
                "synthesis_time_ms",
                "llm_calls",
                "tool_calls",
            ]
            if c in tax.columns
        ]
        st.dataframe(tax[sc], use_container_width=True, hide_index=True)

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Tax % distribution**")
            fig = px.histogram(
                tax,
                x="tax_percent",
                nbins=10,
                color_discrete_sequence=["#3b82f6"],
                labels={"tax_percent": "Tax %"},
            )
            st.plotly_chart(fl(fig), use_container_width=True)
        with col2:
            if "llm_calls" in tax.columns:
                st.markdown("**Tax vs LLM calls**")
                _tx = tax.dropna(subset=["llm_calls", "tax_percent"])
                fig2 = px.scatter(
                    _tx,
                    x="llm_calls",
                    y="tax_percent",
                    color_discrete_sequence=["#f59e0b"],
                    hover_data=["task_name", "provider"],
                    labels={"llm_calls": "LLM Calls", "tax_percent": "Tax %"},
                )
                st.plotly_chart(fl(fig2), use_container_width=True)
    else:
        st.info("No tax data yet — run comparison experiments.")


# ══════════════════════════════════════════════════════════════════════════════
# ANOMALIES
# ══════════════════════════════════════════════════════════════════════════════
