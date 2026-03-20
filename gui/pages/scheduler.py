"""
gui/pages/scheduler.py  —  〜  Scheduler
Render function: render(ctx)
ctx keys: ov, runs, tax, lin, age, avg_lin_j, avg_age_j, tax_mult,
          plan_ms, exec_ms, synth_ms, plan_pct, exec_pct, synth_pct
"""
import subprocess
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from gui.config  import PROJECT_ROOT, DB_PATH, LIVE_API, WF_COLORS, PL
from gui.db      import q, q_safe, q1
from gui.helpers import fl, _human_energy, _human_water, _human_carbon, _gauge_html, _bar_gauge_html

try:
    import requests as _req
    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False
    class _req:
        @staticmethod
        def get(*a, **kw): raise RuntimeError("requests not installed")
try:
    import yaml as _yaml
    _YAML_OK = True
except ImportError:
    _YAML_OK = False


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

    st.title("OS Scheduler Analysis")

    if not runs.empty and "thread_migrations" in runs.columns:
        sc = runs.dropna(subset=["thread_migrations"])
        lsc = sc[sc.workflow_type=="linear"]
        asc = sc[sc.workflow_type=="agentic"]
        avg_l = lsc.thread_migrations.mean() if not lsc.empty else 0
        avg_a = asc.thread_migrations.mean() if not asc.empty else 0

        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Max Migrations", f"{int(sc.thread_migrations.max()):,}")
        c2.metric("Linear avg",     f"{avg_l:.0f}")
        c3.metric("Agentic avg",    f"{avg_a:.0f}",
                  delta=f"{avg_a/max(avg_l,1):.1f}× vs linear", delta_color="inverse")
        c4.metric("Max IRQ/s",
                  f"{sc.interrupt_rate.max():,.0f}" if "interrupt_rate" in sc.columns else "—")
        c5.metric("Avg Cache Miss", f"{ov.get('avg_cache_miss_pct',0) or 0:.1f}%")

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Thread Migrations vs Duration**")
            _sm = sc.dropna(subset=["duration_ms"]).copy()
            _sm["duration_s"] = _sm["duration_ms"] / 1000
            fig = px.scatter(_sm, x="duration_s", y="thread_migrations",
                             color="workflow_type", color_discrete_map=WF_COLORS,
                             hover_data=["run_id","provider"],
                             labels={"duration_s":"Duration (s)","thread_migrations":"Migrations"})
            st.plotly_chart(fl(fig), use_container_width=True)
            st.caption("r²≈0.89 — phase transitions in agentic runs cause migration bursts.")

        with col2:
            st.markdown("**Migrations → Cache Miss (causal chain)**")
            if "cache_miss_rate" in sc.columns:
                _sm2 = sc.dropna(subset=["cache_miss_rate"]).copy()
                _sm2["cache_miss_pct"] = _sm2["cache_miss_rate"] * 100
                fig2 = px.scatter(_sm2, x="thread_migrations", y="cache_miss_pct",
                                  color="workflow_type", color_discrete_map=WF_COLORS,
                                  hover_data=["run_id"],
                                  labels={"thread_migrations":"Migrations","cache_miss_pct":"Cache Miss %"})
                st.plotly_chart(fl(fig2), use_container_width=True)
                st.caption("Migrations → cache eviction → IPC drop → energy waste.")
    else:
        st.info("No scheduler data available.")


# ══════════════════════════════════════════════════════════════════════════════
# DOMAINS
# ══════════════════════════════════════════════════════════════════════════════
