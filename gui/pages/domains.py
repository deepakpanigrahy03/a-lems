"""
gui/pages/domains.py  —  ◉  Domains
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

    st.title("Domain Energy Breakdown")
    domains = q("SELECT * FROM orchestration_analysis ORDER BY run_id")

    if not domains.empty:
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Avg Core Share",
                  f"{domains.core_share.mean()*100:.1f}%"   if "core_share"   in domains.columns else "—")
        c2.metric("Avg Uncore Share",
                  f"{domains.uncore_share.mean()*100:.1f}%" if "uncore_share" in domains.columns else "—")
        c3.metric("Avg Workload J",
                  f"{domains.workload_energy_j.mean():.3f}J"    if "workload_energy_j"    in domains.columns else "—")
        c4.metric("Avg Tax J",
                  f"{domains.orchestration_tax_j.mean():.3f}J"  if "orchestration_tax_j"  in domains.columns else "—")
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Domain shares — stacked**")
            tp = domains.head(30)
            fig = go.Figure()
            for col, color, name in [("core_energy_j","#3b82f6","Core"),
                                      ("uncore_energy_j","#38bdf8","Uncore"),
                                      ("dram_energy_j","#a78bfa","DRAM")]:
                if col in tp.columns:
                    fig.add_trace(go.Bar(name=name, x=tp.run_id.astype(str),
                                         y=tp[col], marker_color=color))
            fig.update_layout(barmode="stack", **PL)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.markdown("**Workload vs Tax**")
            fig2 = go.Figure()
            for col, color, name in [("workload_energy_j","#22c55e","Workload"),
                                      ("orchestration_tax_j","#ef4444","Tax")]:
                if col in domains.columns:
                    fig2.add_trace(go.Bar(name=name, x=domains.run_id.astype(str),
                                          y=domains[col], marker_color=color))
            fig2.update_layout(barmode="stack", **PL)
            st.plotly_chart(fig2, use_container_width=True)
        st.markdown("**Per-run breakdown**")
        sc = [c for c in ["run_id","workflow_type","task_name","pkg_energy_j","core_energy_j",
                           "uncore_energy_j","dram_energy_j","workload_energy_j","orchestration_tax_j"]
              if c in domains.columns]
        st.dataframe(domains[sc], use_container_width=True, hide_index=True)
    else:
        st.info("No domain data — idle_baselines must be linked to runs.")


# ══════════════════════════════════════════════════════════════════════════════
# TAX ATTRIBUTION
# ══════════════════════════════════════════════════════════════════════════════
