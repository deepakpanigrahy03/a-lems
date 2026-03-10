"""
gui/pages/cpu.py  —  ▣  CPU & C-States
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

    st.title("CPU & C-State Analysis")

    cstate_df = q("""
        SELECT e.provider, r.workflow_type,
               AVG(cs.c1_residency) AS c1, AVG(cs.c2_residency) AS c2,
               AVG(cs.c3_residency) AS c3, AVG(cs.c6_residency) AS c6,
               AVG(cs.c7_residency) AS c7,
               AVG(cs.cpu_util_percent) AS util,
               AVG(cs.package_power) AS pkg_w,
               COUNT(cs.sample_id) AS samples
        FROM cpu_samples cs
        JOIN runs r ON cs.run_id = r.run_id
        JOIN experiments e ON r.exp_id = e.exp_id
        GROUP BY e.provider, r.workflow_type
    """)

    if not cstate_df.empty:
        st.markdown("**C-State Residency** — higher C6/C7 = deeper sleep = more efficient idle")
        CSTATE_COLORS = {"C0":"#ef4444","C1":"#38bdf8","C2":"#3b82f6",
                         "C3":"#a78bfa","C6":"#22c55e","C7":"#f59e0b"}
        for _, row in cstate_df.iterrows():
            c0 = max(0.0, 100 - float(row.c1 or 0) - float(row.c2 or 0)
                              - float(row.c3 or 0) - float(row.c6 or 0) - float(row.c7 or 0))
            cs_data = pd.DataFrame([
                {"State":"C0","Residency%": c0},
                {"State":"C1","Residency%": float(row.c1 or 0)},
                {"State":"C2","Residency%": float(row.c2 or 0)},
                {"State":"C3","Residency%": float(row.c3 or 0)},
                {"State":"C6","Residency%": float(row.c6 or 0)},
                {"State":"C7","Residency%": float(row.c7 or 0)},
            ])
            st.markdown(f"**{row.provider} · {row.workflow_type}** — "
                        f"{float(row.pkg_w or 0):.2f}W · {int(row.samples):,} samples")
            fig = px.bar(cs_data, x="Residency%", y="State", orientation="h",
                         color="State", color_discrete_map=CSTATE_COLORS)
            fig.update_layout(**PL, height=160, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        st.info("Cloud: mostly C6/C7 (deep sleep between API calls). "
                "Local: forced C0 throughout inference loop.")
    else:
        st.info("No cpu_samples yet — run experiments to populate.")

    st.divider()

    if not runs.empty and "ipc" in runs.columns:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**IPC Distribution**")
            _ri = runs.dropna(subset=["ipc"])
            fig = px.histogram(_ri, x="ipc", color="workflow_type",
                               color_discrete_map=WF_COLORS,
                               nbins=20, barmode="overlay", opacity=.75,
                               labels={"ipc":"IPC"})
            st.plotly_chart(fl(fig), use_container_width=True)

        with col2:
            st.markdown("**Cache Miss vs Energy**")
            if "cache_miss_rate" in runs.columns and "energy_j" in runs.columns:
                _rm = runs.dropna(subset=["cache_miss_rate","energy_j"]).copy()
                _rm["cache_miss_pct"] = _rm["cache_miss_rate"] * 100
                fig2 = px.scatter(_rm, x="cache_miss_pct", y="energy_j",
                                  color="workflow_type", color_discrete_map=WF_COLORS,
                                  log_y=True, hover_data=["run_id","provider"],
                                  labels={"cache_miss_pct":"Cache Miss %","energy_j":"Energy J"})
                st.plotly_chart(fl(fig2), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULER
# ══════════════════════════════════════════════════════════════════════════════
