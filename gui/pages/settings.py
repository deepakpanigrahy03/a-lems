"""
gui/pages/settings.py  —  ⚙  Settings
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
    import json as _json
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

    st.title("⚙ Settings")
    st.caption("Read-only view of all A-LEMS configuration files + live DB statistics")

    _cfg = PROJECT_ROOT / "config"

    def _load_yaml(p):
        if not _YAML_OK: return None, "pip install pyyaml"
        try:
            with open(p) as f: return _yaml.safe_load(f), None
        except Exception as e: return None, str(e)

    def _load_json(p):
        try:
            with open(p) as f: return _json.load(f), None
        except Exception as e: return None, str(e)

    # app_settings.yaml
    _app, _app_e = _load_yaml(_cfg / "app_settings.yaml")
    if _app_e:
        st.error(f"app_settings.yaml: {_app_e}")
    elif _app:
        _db_eng = (_app.get("database") or {}).get("engine","?")
        _sr     = ((_app.get("webui")      or {}).get("sampling_rate_hz"))          or "?"
        _cd     = ((_app.get("experiment") or {}).get("cool_down_seconds"))         or "?"
        _ta     = ((_app.get("alerts")     or {}).get("temperature_threshold_celsius")) or "?"
        _di     = ((_app.get("experiment") or {}).get("default_iterations"))        or "?"
        k1,k2,k3,k4,k5 = st.columns(5)
        k1.metric("DB engine",     str(_db_eng))
        k2.metric("Sampling",      f"{_sr}Hz")
        k3.metric("Cool-down",     f"{_cd}s")
        k4.metric("Temp alert",    f"{_ta}°C")
        k5.metric("Default iters", str(_di))
        with st.expander("app_settings.yaml", expanded=False):
            st.json(_app)

    st.divider()

    # hw_config.json
    _hw, _hw_e = _load_json(_cfg / "hw_config.json")
    if _hw_e:
        st.error(f"hw_config.json: {_hw_e}")
    elif _hw:
        _cpu  = _hw.get("cpu")       or {}
        _rapl = _hw.get("rapl")      or {}
        _ts   = _hw.get("turbostat") or {}
        _meta = _hw.get("metadata")  or {}
        h1,h2,h3,h4 = st.columns(4)
        h1.metric("CPU", str(_cpu.get("model_name", _meta.get("cpu_model","?")))[:30])
        h2.metric("Cores", str(_cpu.get("physical_cores", _cpu.get("cores","?"))))
        _domains = _rapl.get("domains", _rapl.get("available_domains",[]))
        h3.metric("RAPL domains", str(len(_domains or [])))
        _ts_ok = _ts.get("available", _ts.get("found", False))
        h4.metric("turbostat", "✅" if _ts_ok else "❌")
        with st.expander("hw_config.json", expanded=False):
            st.json(_hw)

    st.divider()

    # tasks.yaml
    _tc, _tc_e = _load_yaml(_cfg / "tasks.yaml")
    if _tc_e:
        st.error(f"tasks.yaml: {_tc_e}")
    elif _tc:
        _tlist = _tc.get("tasks",[])
        t1,t2 = st.columns(2)
        t1.metric("Tasks defined", len(_tlist))
        t2.metric("Categories",    len({t.get("category","") for t in _tlist}))
        _tdf = pd.DataFrame([{
            "id":t.get("id",""),"category":t.get("category",""),
            "name":t.get("name",""),"level":t.get("level",""),
            "tool_calls":t.get("tool_calls",0)} for t in _tlist])
        st.dataframe(_tdf, use_container_width=True, hide_index=True)
        with st.expander("tasks.yaml", expanded=False):
            st.json(_tc)

    st.divider()

    # models.json
    _mo, _mo_e = _load_json(_cfg / "models.json")
    if _mo_e:
        st.error(f"models.json: {_mo_e}")
    elif _mo:
        with st.expander("models.json", expanded=False):
            st.json(_mo)

    # DB stats
    st.divider()
    st.markdown("**Database row counts**")
    _dbs, _dbs_e = q_safe("""
        SELECT 'experiments'        AS tbl, COUNT(*) AS rows FROM experiments UNION ALL
        SELECT 'runs',              COUNT(*) FROM runs                         UNION ALL
        SELECT 'energy_samples',    COUNT(*) FROM energy_samples               UNION ALL
        SELECT 'cpu_samples',       COUNT(*) FROM cpu_samples                  UNION ALL
        SELECT 'interrupt_samples', COUNT(*) FROM interrupt_samples            UNION ALL
        SELECT 'orchestration_events', COUNT(*) FROM orchestration_events      UNION ALL
        SELECT 'task_categories',   COUNT(*) FROM task_categories
    """)
    if not _dbs_e and not _dbs.empty:
        _dbs.columns = ["Table","Rows"]
        st.dataframe(_dbs, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: SUSTAINABILITY  (new — carved out of energy page)
# ══════════════════════════════════════════════════════════════════════════════
