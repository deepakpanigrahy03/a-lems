"""
gui/pages/experiments.py  —  ≡  Experiments
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

    st.title("Saved Experiments")

    exps = q("""
        SELECT e.exp_id, e.name, e.task_name, e.provider, e.country_code,
               e.status, e.workflow_type AS exp_workflow,
               COUNT(r.run_id) AS run_count,
               AVG(CASE WHEN r.workflow_type='linear'  THEN r.total_energy_uj END)/1e6 AS avg_linear_j,
               AVG(CASE WHEN r.workflow_type='agentic' THEN r.total_energy_uj END)/1e6 AS avg_agentic_j
        FROM experiments e LEFT JOIN runs r ON e.exp_id = r.exp_id
        GROUP BY e.exp_id ORDER BY e.exp_id DESC
    """)

    if not exps.empty:
        sc = [c for c in ["exp_id","name","task_name","provider","country_code",
                           "status","run_count","avg_linear_j","avg_agentic_j"]
              if c in exps.columns]
        st.dataframe(exps[sc], use_container_width=True, hide_index=True)
        st.divider()

        selected_exp = st.selectbox(
            "Inspect experiment",
            exps.exp_id.tolist(),
            format_func=lambda eid: f"Exp {eid} — {exps[exps.exp_id==eid]['name'].values[0]}",
        )

        exp_runs = q(f"SELECT * FROM runs WHERE exp_id={selected_exp} ORDER BY run_number")
        exp_tax  = q(f"""
            SELECT ots.comparison_id, ots.tax_percent,
                   ots.orchestration_tax_uj/1e6 AS tax_j,
                   ots.linear_dynamic_uj/1e6    AS linear_dynamic_j,
                   ots.agentic_dynamic_uj/1e6   AS agentic_dynamic_j
            FROM orchestration_tax_summary ots
            JOIN runs r ON ots.linear_run_id = r.run_id
            WHERE r.exp_id = {selected_exp}
        """)

        if not exp_runs.empty:
            lin_avg = exp_runs[exp_runs.workflow_type=="linear"].total_energy_uj.mean() / 1e6                       if not exp_runs[exp_runs.workflow_type=="linear"].empty else 0
            age_avg = exp_runs[exp_runs.workflow_type=="agentic"].total_energy_uj.mean() / 1e6                       if not exp_runs[exp_runs.workflow_type=="agentic"].empty else 0
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Total runs",   len(exp_runs))
            c2.metric("Avg Linear J", f"{lin_avg:.3f}")
            c3.metric("Avg Agentic J",f"{age_avg:.3f}")
            c4.metric("Tax multiple", f"{age_avg/lin_avg:.1f}×" if lin_avg > 0 else "—")

            sc2 = [c for c in ["run_id","workflow_type","run_number","total_energy_uj",
                                "ipc","cache_miss_rate","thread_migrations","carbon_g"]
                   if c in exp_runs.columns]
            st.dataframe(exp_runs[sc2], use_container_width=True, hide_index=True)

        if not exp_tax.empty:
            st.markdown("**Tax pairs**")
            sc3 = [c for c in ["comparison_id","linear_dynamic_j","agentic_dynamic_j",
                                "tax_j","tax_percent"] if c in exp_tax.columns]
            st.dataframe(exp_tax[sc3], use_container_width=True, hide_index=True)
    else:
        st.info("No experiments found.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: QUERY ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
