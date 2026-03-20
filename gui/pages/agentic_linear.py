"""
gui/pages/agentic_linear.py  —  ⇌  Agentic vs Linear
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

    st.title("⇌ Agentic vs Linear — Deep Comparison")
    st.caption("Head-to-head on every metric that matters for your PhD")

    _cmp, _cmp_e = q_safe("""
        SELECT e.task_name, e.provider, e.model_name,
               r.workflow_type,
               COUNT(*) AS runs,
               ROUND(AVG(r.total_energy_uj)/1e6, 4)   AS avg_energy_j,
               ROUND(AVG(r.dynamic_energy_uj)/1e6, 4)  AS avg_dynamic_j,
               ROUND(AVG(r.duration_ns)/1e9, 3)         AS avg_duration_s,
               ROUND(AVG(r.total_tokens), 1)            AS avg_tokens,
               ROUND(AVG(CASE WHEN r.total_tokens>0
                   THEN r.total_energy_uj/r.total_tokens END)/1e3, 4) AS avg_mj_token,
               ROUND(AVG(r.ipc), 3)                     AS avg_ipc,
               ROUND(AVG(r.cache_miss_rate)*100, 2)      AS avg_cache_miss,
               ROUND(AVG(r.carbon_g)*1000, 4)            AS avg_carbon_mg,
               ROUND(AVG(r.planning_time_ms), 1)          AS avg_plan_ms,
               ROUND(AVG(r.execution_time_ms), 1)         AS avg_exec_ms,
               ROUND(AVG(r.synthesis_time_ms), 1)         AS avg_synth_ms,
               ROUND(AVG(r.llm_calls), 2)                 AS avg_llm_calls,
               ROUND(AVG(r.tool_calls), 2)                AS avg_tool_calls,
               ROUND(AVG(r.thermal_delta_c), 2)            AS avg_temp_rise
        FROM runs r JOIN experiments e ON r.exp_id=e.exp_id
        GROUP BY e.task_name, e.provider, e.model_name, r.workflow_type
        ORDER BY e.task_name, r.workflow_type
    """)

    if _cmp_e or _cmp.empty:
        st.info("No data yet.")
    else:
        # Pivot to wide format for direct comparison
        _lin = _cmp[_cmp.workflow_type=="linear"].set_index(["task_name","provider"])
        _age = _cmp[_cmp.workflow_type=="agentic"].set_index(["task_name","provider"])
        _metrics = ["avg_energy_j","avg_dynamic_j","avg_duration_s","avg_tokens",
                    "avg_mj_token","avg_ipc","avg_cache_miss","avg_carbon_mg",
                    "avg_plan_ms","avg_temp_rise","avg_llm_calls","avg_tool_calls"]

        _tab_heat, _tab_bar, _tab_table = st.tabs(
            ["🌡 Heatmap", "📊 Bar Charts", "📋 Table"])

        with _tab_heat:
            st.markdown("**Energy overhead ratio (agentic÷linear) per task × provider**")
            _ratio = _age["avg_energy_j"] / _lin["avg_energy_j"].replace(0, float("nan"))
            _ratio_df = _ratio.reset_index()
            _ratio_df.columns = ["task_name","provider","ratio"]
            if not _ratio_df.empty:
                _fig_h = px.density_heatmap(
                    _ratio_df, x="provider", y="task_name", z="ratio",
                    color_continuous_scale="RdYlGn_r",
                    labels={"ratio":"Agentic/Linear energy ratio"})
                st.plotly_chart(fl(_fig_h), use_container_width=True)
                st.caption("Values > 1.0 = agentic costs more. Red = high overhead.")

        with _tab_bar:
            _sel_m = st.selectbox("Metric", _metrics, key="al_metric")
            _fig_al = px.bar(_cmp.dropna(subset=[_sel_m]),
                             x="task_name", y=_sel_m, color="workflow_type",
                             barmode="group", facet_col="provider",
                             color_discrete_map=WF_COLORS,
                             labels={"task_name":"Task"},
                             height=400)
            _fig_al.update_xaxes(tickangle=35)
            st.plotly_chart(fl(_fig_al), use_container_width=True)

        with _tab_table:
            st.dataframe(_cmp.round(4), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: RESEARCH INSIGHTS  (merged sql_query + guided questions + schema)
# ══════════════════════════════════════════════════════════════════════════════
