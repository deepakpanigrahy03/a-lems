"""
gui/pages/overview.py  —  ◈  Overview
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

    st.title("Overview — A-LEMS Energy Dashboard")

    # ── Live job status banner ─────────────────────────────────────────────────
    # Bypass ALL caching — direct SQLite read every render so status is fresh.
    # COUNT(r.run_id) gives the real completed count even if experiments.runs_completed lags.
    _live_jobs = pd.DataFrame()
    try:
        from gui.db import db as _db_ctx
        with _db_ctx() as _lcon:
            _live_jobs = pd.read_sql_query("""
                SELECT
                    e.exp_id,
                    e.name,
                    e.status,
                    e.model_name,
                    e.provider,
                    e.task_name,
                    e.runs_total,
                    e.started_at,
                    e.completed_at,
                    e.error_message,
                    COUNT(r.run_id)                                 AS runs_done_actual,
                    COALESCE(e.runs_completed, COUNT(r.run_id))     AS runs_done,
                    ROUND(
                        100.0 * COALESCE(e.runs_completed, COUNT(r.run_id))
                        / NULLIF(e.runs_total, 0), 0)               AS pct_done
                FROM experiments e
                LEFT JOIN runs r ON r.exp_id = e.exp_id
                WHERE e.status IN ('running','pending','started','error')
                   OR (e.status = 'completed'
                       AND datetime(e.completed_at) > datetime('now','-10 minutes'))
                GROUP BY e.exp_id
                ORDER BY e.started_at DESC
                LIMIT 12
            """, _lcon)
    except Exception as _live_err:
        st.caption(f"⚠ Live status unavailable: {_live_err}")

    if not _live_jobs.empty:
        _has_running = any(s in ("running","started")
                           for s in _live_jobs.status.values)

        # Auto-clear cache every ~4 s while jobs are running so page re-renders fresh
        if _has_running:
            import time as _t
            if "live_last_refresh" not in st.session_state:
                st.session_state.live_last_refresh = 0.0
            if _t.time() - st.session_state.live_last_refresh > 4:
                st.session_state.live_last_refresh = _t.time()
                st.cache_data.clear()

        _hdr_col, _hdr_btn = st.columns([7, 1])
        with _hdr_col:
            _pulse = "🟢" if _has_running else "⚫"
            st.markdown(
                f"<div style='font-size:10px;font-weight:700;color:#22c55e;"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:6px;'>"
                f"{_pulse} Live / Recent Jobs</div>",
                unsafe_allow_html=True)
        with _hdr_btn:
            if st.button("↺", key="ov_live_refresh", help="Force refresh"):
                st.cache_data.clear()
                st.rerun()

        for _, _jrow in _live_jobs.iterrows():
            _jstat  = str(_jrow.get("status", "?"))
            _jdone  = int(_jrow.get("runs_done") or 0)
            _jtot   = int(_jrow.get("runs_total") or 0)
            _jpct   = float(_jrow.get("pct_done") or 0)
            _jerr   = str(_jrow.get("error_message") or "")
            _jclr   = {"running":"#22c55e","completed":"#3b82f6",
                       "pending":"#f59e0b","started":"#22c55e",
                       "error":"#ef4444"}.get(_jstat,"#7090b0")
            _jpulse = "●" if _jstat in ("running","started") else "○"

            # Elapsed duration
            _jdur = ""
            try:
                import datetime as _dt
                _ts = _dt.datetime.fromisoformat(
                    str(_jrow.get("started_at","")).replace("Z",""))
                _jend_s = str(_jrow.get("completed_at") or "")
                _te = _dt.datetime.fromisoformat(_jend_s.replace("Z",""))                       if _jend_s and _jend_s not in ("None","")                       else _dt.datetime.now()
                _sec = int((_te - _ts).total_seconds())
                _jdur = f"{_sec//60}m{_sec%60:02d}s"
            except Exception:
                pass

            _err_html = (
                f"<div style='font-size:8px;color:#ef4444;margin-top:3px;'>"
                f"{_jerr[:80]}</div>"
                if _jerr and _jerr not in ("None","") else ""
            )
            st.markdown(f"""
            <div style="background:#0a1018;border:1px solid #1e2d45;
                        border-left:3px solid {_jclr};border-radius:5px;
                        padding:7px 12px;margin-bottom:5px;">
              <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
                <span style="font-size:9px;color:{_jclr};font-weight:700;
                      min-width:68px;">{_jpulse} {_jstat.upper()}</span>
                <span style="font-size:9px;color:#e8f0f8;font-weight:600;
                      flex:2;">{str(_jrow.get("task_name","?"))}</span>
                <span style="font-size:9px;color:#5a7090;flex:2;
                      overflow:hidden;white-space:nowrap;">{str(_jrow.get("model_name","?"))[:22]}</span>
                <span style="font-size:9px;color:#5a7090;min-width:48px;">{str(_jrow.get("provider","?"))}</span>
                <span style="font-size:9px;color:#3d5570;min-width:38px;">{_jdur}</span>
                <span style="font-size:9px;color:{_jclr};min-width:40px;
                      font-family:monospace;text-align:right;">{_jdone}/{_jtot}</span>
              </div>
              <div style="background:#1e2d45;border-radius:3px;height:5px;overflow:hidden;">
                <div style="background:{_jclr};width:{min(_jpct,100):.0f}%;
                            height:100%;border-radius:3px;"></div>
              </div>
              {_err_html}
            </div>""", unsafe_allow_html=True)
        st.markdown("")

    # ── Hero comparison bar ────────────────────────────────────────────────────
    bar_pct = f"{100/max(tax_mult,1):.0f}%"
    st.markdown(f"""
    <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                padding:20px 24px;margin-bottom:16px;border-top:2px solid #ef4444;">
      <div style="font-size:18px;font-weight:600;color:#e8f0f8;margin-bottom:4px;">
        Agentic costs <span style="color:#ef4444;font-family:'IBM Plex Mono',monospace;">
        {tax_mult:.1f}×</span> more energy than linear for the same task
      </div>
      <div style="font-size:11px;color:#3d5570;margin-bottom:16px;">
        Measured across {ov.get("total_runs","—")} runs · {ov.get("total_experiments","—")} experiments
      </div>
      <div style="display:flex;align-items:center;gap:16px;margin-bottom:10px;">
        <div style="width:70px;font-size:11px;color:#7090b0;">Linear</div>
        <div style="flex:1;background:#1a2438;border-radius:4px;overflow:hidden;height:28px;">
          <div style="width:{bar_pct};background:#22c55e;height:100%;display:flex;
               align-items:center;padding-left:10px;font-size:10px;color:#fff;
               font-family:'IBM Plex Mono',monospace;">{avg_lin_j:.3f}J</div>
        </div>
        <div style="width:50px;font-size:10px;color:#7090b0;font-family:monospace;">1×</div>
      </div>
      <div style="display:flex;align-items:center;gap:16px;">
        <div style="width:70px;font-size:11px;color:#7090b0;">Agentic</div>
        <div style="flex:1;background:#1a2438;border-radius:4px;overflow:hidden;height:28px;">
          <div style="width:100%;background:#ef4444;height:100%;display:flex;
               align-items:center;padding-left:10px;font-size:10px;color:#fff;
               font-family:'IBM Plex Mono',monospace;">{avg_age_j:.3f}J</div>
        </div>
        <div style="width:50px;font-size:10px;color:#ef4444;font-family:monospace;
             font-weight:600;">{tax_mult:.1f}×</div>
      </div>
    </div>""", unsafe_allow_html=True)

    # ── Phase breakdown (unchanged) ───────────────────────────────────────────
    if plan_ms > 0:
        st.markdown(f"""
        <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                    padding:16px 20px;margin-bottom:16px;">
          <div style="font-size:9px;color:#3d5570;text-transform:uppercase;
               letter-spacing:.08em;margin-bottom:8px;">
            Where the overhead goes — agentic time breakdown</div>
          <div style="display:flex;height:22px;border-radius:4px;overflow:hidden;gap:1px;">
            <div style="width:{plan_pct:.0f}%;background:#f59e0b;display:flex;align-items:center;
                 justify-content:center;font-size:9px;color:rgba(255,255,255,.85);
                 font-family:monospace;">{plan_pct:.0f}% plan</div>
            <div style="width:{exec_pct:.0f}%;background:#3b82f6;display:flex;align-items:center;
                 justify-content:center;font-size:9px;color:rgba(255,255,255,.85);
                 font-family:monospace;">{exec_pct:.0f}% exec</div>
            <div style="width:{synth_pct:.0f}%;background:#a78bfa;display:flex;align-items:center;
                 justify-content:center;font-size:9px;color:rgba(255,255,255,.85);
                 font-family:monospace;">{synth_pct:.0f}% synth</div>
          </div>
          <div style="display:flex;gap:20px;margin-top:8px;font-size:9px;color:#3d5570;">
            <span><span style="color:#f59e0b">■</span> Planning {plan_ms:.0f}ms — pure overhead</span>
            <span><span style="color:#3b82f6">■</span> Execution {exec_ms:.0f}ms — tool latency</span>
            <span><span style="color:#a78bfa">■</span> Synthesis {synth_ms:.0f}ms — context merge</span>
          </div>
        </div>""", unsafe_allow_html=True)

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("Total Runs",   ov.get("total_runs","—"))
    c2.metric("Tax Multiple", f"{tax_mult:.1f}×",
              delta=f"{(tax_mult-1)*100:.0f}% overhead", delta_color="inverse")
    c3.metric("Avg Planning", f"{plan_ms:.0f}ms",
              delta=f"{plan_pct:.0f}% of agentic time",  delta_color="inverse")
    c4.metric("Peak IPC",     f"{ov.get('max_ipc', 0) or 0:.3f}")
    c5.metric("Avg Carbon",   f"{ov.get('avg_carbon_mg', 0) or 0:.3f}mg")
    c6.metric("Total Energy", f"{ov.get('total_energy_j', 0) or 0:.1f}J")

    st.divider()

    # ── Apple-to-apple: per model×workflow comparison ─────────────────────────
    st.markdown("### 🍎 Apple-to-Apple: Model Comparison")
    st.caption("Same tasks, same conditions — cloud vs local, agentic vs linear, side by side")

    _model_cmp, _ = q_safe("""
        SELECT e.model_name, e.provider, r.workflow_type,
               e.task_name,
               COUNT(*)                                    AS runs,
               ROUND(AVG(r.total_energy_uj)/1e6,4)        AS avg_energy_j,
               ROUND(AVG(r.dynamic_energy_uj)/1e6,4)      AS avg_dynamic_j,
               ROUND(AVG(r.duration_ns)/1e9,3)            AS avg_duration_s,
               ROUND(AVG(r.total_tokens),1)               AS avg_tokens,
               ROUND(AVG(CASE WHEN r.total_tokens>0
                   THEN r.total_energy_uj/r.total_tokens END)/1e3,4) AS avg_mj_per_token,
               ROUND(AVG(r.ipc),3)                        AS avg_ipc,
               ROUND(AVG(r.carbon_g)*1000,4)              AS avg_carbon_mg,
               ROUND(AVG(r.water_ml),4)                   AS avg_water_ml
        FROM runs r JOIN experiments e ON r.exp_id=e.exp_id
        WHERE e.model_name IS NOT NULL
        GROUP BY e.model_name, e.provider, r.workflow_type, e.task_name
        ORDER BY e.provider, e.model_name, r.workflow_type
    """)

    if not _model_cmp.empty:
        # Summary cards per model
        _models_list = _model_cmp.model_name.dropna().unique().tolist()
        _model_cols  = st.columns(min(len(_models_list), 4))
        for _mi, _mname in enumerate(_models_list[:4]):
            _mdf = _model_cmp[_model_cmp.model_name == _mname]
            _mlin = _mdf[_mdf.workflow_type=="linear"].avg_energy_j.mean()
            _mage = _mdf[_mdf.workflow_type=="agentic"].avg_energy_j.mean()
            _mprov = str(_mdf.provider.iloc[0]) if not _mdf.empty else "?"
            _mmult = _mage / _mlin if _mlin and _mlin > 0 else 1
            _pclr = "#38bdf8" if _mprov == "cloud" else "#22c55e"
            with _model_cols[_mi]:
                st.markdown(f"""
                <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:8px;
                            padding:12px 14px;border-top:2px solid {_pclr};">
                  <div style="font-size:10px;font-weight:600;color:#e8f0f8;
                       margin-bottom:2px;">{_mname[:28]}</div>
                  <div style="font-size:9px;color:{_pclr};margin-bottom:8px;">{_mprov}</div>
                  <div style="font-size:9px;color:#7090b0;">
                    Linear: <b style="color:#22c55e;font-family:monospace">
                    {(f'{_mlin:.4f}' if _mlin and _mlin == _mlin else '—')}J</b></div>
                  <div style="font-size:9px;color:#7090b0;">
                    Agentic: <b style="color:#ef4444;font-family:monospace">
                    {(f'{_mage:.4f}' if _mage and _mage == _mage else '—')}J</b></div>
                  <div style="font-size:9px;color:#f59e0b;margin-top:4px;">
                    Overhead: <b>{_mmult:.2f}×</b></div>
                </div>""", unsafe_allow_html=True)

        st.markdown("")
        # Grouped bar: model × workflow × task
        _task_list = _model_cmp.task_name.dropna().unique().tolist()
        _sel_task_ov = st.selectbox(
            "Filter task for model comparison",
            ["all"] + sorted(_task_list), key="ov_task_filter")
        _cmp_filtered = _model_cmp if _sel_task_ov == "all" else \
                        _model_cmp[_model_cmp.task_name == _sel_task_ov]

        _cmp_pivot = _cmp_filtered.copy()
        _cmp_pivot["model_wf"] = _cmp_pivot["model_name"].astype(str) + \
                                  " · " + _cmp_pivot["workflow_type"].astype(str)

        col_cmp1, col_cmp2 = st.columns(2)
        with col_cmp1:
            st.markdown("**Energy (J) — model × workflow**")
            _fig_cmp = px.bar(
                _cmp_pivot.groupby(["model_wf","workflow_type"])["avg_energy_j"].mean().reset_index(),
                x="model_wf", y="avg_energy_j", color="workflow_type",
                barmode="group", color_discrete_map=WF_COLORS,
                labels={"avg_energy_j":"Avg Energy (J)","model_wf":"Model · Workflow"})
            _fig_cmp.update_xaxes(tickangle=30)
            st.plotly_chart(fl(_fig_cmp), use_container_width=True)

        with col_cmp2:
            st.markdown("**mJ / token — model × workflow**")
            _fig_cmp2 = px.bar(
                _cmp_pivot.groupby(["model_wf","workflow_type"])["avg_mj_per_token"].mean().reset_index(),
                x="model_wf", y="avg_mj_per_token", color="workflow_type",
                barmode="group", color_discrete_map=WF_COLORS,
                labels={"avg_mj_per_token":"mJ/token","model_wf":"Model · Workflow"})
            _fig_cmp2.update_xaxes(tickangle=30)
            st.plotly_chart(fl(_fig_cmp2), use_container_width=True)

    st.divider()

    # ── Original duration vs energy + IPC vs cache miss (preserved) ───────────
    if not runs.empty and "energy_j" in runs.columns:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Duration vs Energy — all runs**")
            _df = runs.dropna(subset=["energy_j","duration_ms"]).copy()
            _df["duration_s"] = _df["duration_ms"] / 1000
            fig = px.scatter(_df, x="duration_s", y="energy_j",
                             color="workflow_type", color_discrete_map=WF_COLORS,
                             hover_data=["run_id","provider","task_name"],
                             labels={"duration_s":"Duration (s)","energy_j":"Energy (J)"})
            st.plotly_chart(fl(fig), use_container_width=True)

        with col2:
            st.markdown("**IPC vs Cache Miss**")
            _df2 = runs.dropna(subset=["ipc","cache_miss_rate"]).copy()
            _df2["cache_miss_pct"] = _df2["cache_miss_rate"] * 100
            fig2 = px.scatter(_df2, x="cache_miss_pct", y="ipc",
                              color="workflow_type", color_discrete_map=WF_COLORS,
                              hover_data=["run_id","provider"],
                              labels={"cache_miss_pct":"Cache Miss %","ipc":"IPC"})
            st.plotly_chart(fl(fig2), use_container_width=True)

    # ── Provider × model × task summary table ────────────────────────────────
    st.divider()
    st.markdown("### Full comparison matrix")
    if not _model_cmp.empty:
        _show_cols = [c for c in ["model_name","provider","workflow_type","task_name",
                                   "runs","avg_energy_j","avg_dynamic_j","avg_duration_s",
                                   "avg_tokens","avg_mj_per_token","avg_ipc",
                                   "avg_carbon_mg","avg_water_ml"]
                      if c in _model_cmp.columns]
        st.dataframe(_model_cmp[_show_cols].round(4),
                     use_container_width=True, hide_index=True)
    else:
        st.info("No run data — run experiments first.")


# ══════════════════════════════════════════════════════════════════════════════
# ENERGY
# ══════════════════════════════════════════════════════════════════════════════
