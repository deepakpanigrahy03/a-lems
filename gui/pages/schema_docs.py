"""
gui/pages/schema_docs.py  —  📋  Schema & Docs
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

    st.title("📋 Schema & Docs")
    st.caption(
        "Quick reference — redirect to Research Insights → Schema tab for full detail"
    )
    st.info(
        "Full interactive schema is in **🔬 Research Insights → Schema & Data Model** tab."
    )
    st.markdown("### DB tables at a glance")
    _db_tbls, _ = q_safe("""
        SELECT name FROM sqlite_master WHERE type='table' ORDER BY name
    """)
    if not _db_tbls.empty:
        for _tbl in _db_tbls.name.tolist():
            _cnt = q1(f"SELECT COUNT(*) AS n FROM {_tbl}").get("n", 0)
            _cols_df, _ = q_safe(f"PRAGMA table_info({_tbl})")
            if not _cols_df.empty:
                _col_names = " · ".join(_cols_df["name"].tolist()[:10])
                with st.expander(f"**{_tbl}** — {_cnt:,} rows"):
                    st.caption(_col_names + (" …" if len(_cols_df) > 10 else ""))
                    st.dataframe(
                        _cols_df[
                            ["cid", "name", "type", "notnull", "dflt_value", "pk"]
                        ],
                        use_container_width=True,
                        hide_index=True,
                    )
