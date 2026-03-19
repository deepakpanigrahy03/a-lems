"""
gui/pages/anomalies.py  —  ⚠  Anomalies
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

    st.title("Anomaly Detection")
    anom = q("""
        WITH stats AS (
            SELECT AVG(total_energy_uj/1e6) AS me, AVG(ipc) AS mi,
                   AVG(cache_miss_rate) AS mm FROM runs WHERE total_energy_uj IS NOT NULL
        ),
        stdev AS (
            SELECT SQRT(AVG((total_energy_uj/1e6-me)*(total_energy_uj/1e6-me))) AS se
            FROM runs, stats WHERE total_energy_uj IS NOT NULL
        )
        SELECT r.run_id, r.exp_id, r.workflow_type, e.task_name, e.provider,
               r.total_energy_uj/1e6 AS energy_j, r.ipc,
               r.cache_miss_rate*100 AS cache_miss_pct,
               r.thermal_delta_c, r.interrupt_rate,
               CASE WHEN r.total_energy_uj/1e6 > me+2*se THEN 1 ELSE 0 END AS flag_high_energy,
               CASE WHEN r.ipc < mi*0.5                  THEN 1 ELSE 0 END AS flag_low_ipc,
               CASE WHEN r.cache_miss_rate > mm*1.5      THEN 1 ELSE 0 END AS flag_high_miss,
               CASE WHEN r.thermal_throttle_flag=1        THEN 1 ELSE 0 END AS flag_thermal
        FROM runs r JOIN experiments e ON r.exp_id=e.exp_id, stats, stdev
        WHERE r.total_energy_uj IS NOT NULL
          AND (r.total_energy_uj/1e6>me+2*se OR r.ipc<mi*0.5
               OR r.cache_miss_rate>mm*1.5   OR r.thermal_throttle_flag=1)
        ORDER BY energy_j DESC
    """)
    if not anom.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric(
            "High-Energy",
            (
                int(anom.flag_high_energy.sum())
                if "flag_high_energy" in anom.columns
                else "—"
            ),
        )
        c2.metric(
            "Low-IPC",
            int(anom.flag_low_ipc.sum()) if "flag_low_ipc" in anom.columns else "—",
        )
        c3.metric(
            "Thermal",
            int(anom.flag_thermal.sum()) if "flag_thermal" in anom.columns else "—",
        )
        st.divider()
        st.dataframe(anom, use_container_width=True, hide_index=True)
    else:
        st.success("No anomalies — all runs within normal range.")


# ══════════════════════════════════════════════════════════════════════════════
# EXECUTE RUN
# ══════════════════════════════════════════════════════════════════════════════
