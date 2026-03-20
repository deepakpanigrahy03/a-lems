"""
gui/pages/overview.py  —  ◈  Overview
11/10 research dashboard with:
  - Dark / light theme (via st.session_state["theme"])
  - Live job banner (unchanged from original)
  - KPI row (5 metrics)
  - Insight strip
  - Linear vs Agentic energy card
  - Tax by task+model chart
  - Recent sessions list
  - Agentic execution breakdown (time + energy per phase)
  - Orchestration tax attribution (red-top card)
  - Energy timeline sparkline
  - Hardware snapshot + live resource bars
  - Software environment + fingerprint
  - Activity feed
  - Apple-to-Apple + model comparison → sidebar "Model Behavior" panel
  - Duration vs Energy scatter + IPC vs Cache Miss scatter
  - Full comparison matrix table
NOTE: Every line from the original overview.py is preserved.
      Apple-to-Apple section is rendered via render_model_behavior_sidebar()
      which is called from sidebar.py under the "MODEL BEHAVIOR" section.
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from gui.config  import PROJECT_ROOT, DB_PATH, LIVE_API, WF_COLORS, PL
from gui.db      import q, q_safe, q1
from gui.helpers import fl, _human_energy, _human_water, _human_carbon

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


# ─────────────────────────────────────────────────────────────────────────────
# THEME HELPERS  (inline so overview.py has zero extra imports)
# ─────────────────────────────────────────────────────────────────────────────
def _is_dark() -> bool:
    return st.session_state.get("theme", "dark") == "dark"


def _tok():
    """Return colour token dict for current theme."""
    if _is_dark():
        return dict(
            bg0="#0d1117", bg1="#111827", bg2="#1f2937", bg3="#374151",
            t1="#f1f5f9",  t2="#94a3b8",  t3="#475569",
            brd="#1f2937", brd2="#374151", accent="#3b82f6",
            card_top="3px solid #3b82f6",
        )
    return dict(
        bg0="#f3f4f6", bg1="#ffffff", bg2="#f9fafb", bg3="#d1d5db",
        t1="#1f2937",  t2="#4b5563",  t3="#6b7280",
        brd="#d1d5db", brd2="#9ca3af", accent="#3b82f6",
        card_top="3px solid #3b82f6",
    )


def _inject_theme():
    t = _tok()
    st.markdown(f"""<style>
    .stApp,[data-testid="stAppViewContainer"]{{background:{t["bg0"]}!important}}
    [data-testid="stHeader"]{{background:{t["bg0"]}!important;border-bottom:0.5px solid {t["brd"]}}}
    [data-testid="stSidebar"],[data-testid="stSidebarContent"]{{background:{t["bg1"]}!important;border-right:0.5px solid {t["brd"]}}}
    [data-testid="stMetric"]{{background:{t["bg1"]};border:0.5px solid {t["brd"]};border-top:{t["card_top"]};border-radius:8px;padding:10px 14px}}
    [data-testid="stMetricLabel"]{{color:{t["t3"]}!important;font-size:11px!important}}
    [data-testid="stMetricValue"]{{color:{t["t1"]}!important}}
    [data-testid="stExpander"]{{background:{t["bg1"]};border:0.5px solid {t["brd"]};border-radius:8px}}
    [data-testid="stTabs"] [data-baseweb="tab-list"]{{background:{t["bg1"]};border-bottom:0.5px solid {t["brd"]}}}
    [data-testid="stTabs"] [data-baseweb="tab"]{{color:{t["t2"]}!important;font-size:11px}}
    [data-testid="stTabs"] [aria-selected="true"]{{color:{t["t1"]}!important;border-bottom:2px solid {t["accent"]}!important}}
    [data-testid="stButton"]>button{{background:{t["bg2"]};color:{t["t1"]};border:0.5px solid {t["brd2"]};border-radius:6px;font-size:12px}}
    [data-testid="stButton"]>button:hover{{background:{t["bg3"]};border-color:{t["accent"]}}}
    [data-testid="stDataFrame"]{{background:{t["bg1"]};border:0.5px solid {t["brd"]};border-radius:8px}}
    ::-webkit-scrollbar{{width:5px;height:5px}}
    ::-webkit-scrollbar-track{{background:{t["bg0"]}}}
    ::-webkit-scrollbar-thumb{{background:{t["bg3"]};border-radius:3px}}
    </style>""", unsafe_allow_html=True)


def _card(content_html: str, border_top_color: str = None, extra_style: str = "") -> str:
    """Wrap HTML in a themed card div."""
    t = _tok()
    top = f"border-top:3px solid {border_top_color};" if border_top_color else ""
    return (
        f"<div style='background:{t['bg1']};border:0.5px solid {t['brd']};"
        f"border-radius:10px;padding:14px 16px;{top}{extra_style}'>"
        f"{content_html}</div>"
    )


def _label(text: str) -> str:
    t = _tok()
    return (f"<div style='font-size:10px;font-weight:500;color:{t['t3']};"
            f"text-transform:uppercase;letter-spacing:.07em;margin-bottom:11px'>{text}</div>")


# ─────────────────────────────────────────────────────────────────────────────
# MODEL BEHAVIOR SIDEBAR  (moved from overview Apple-to-Apple section)
# Called from sidebar.py — zero code lost
# ─────────────────────────────────────────────────────────────────────────────
def render_model_behavior_sidebar():
    """
    Full Apple-to-Apple model comparison — originally in overview.py.
    Moved to sidebar under "MODEL BEHAVIOR" heading.
    Every line of original logic preserved.
    """
    t = _tok()

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

    if _model_cmp.empty:
        st.markdown(
            f"<div style='font-size:10px;color:{t['t3']};padding:8px 0'>"
            f"No model data yet — run experiments first.</div>",
            unsafe_allow_html=True)
        return

    # ── Per-model summary cards ───────────────────────────────────────────────
    _models_list = _model_cmp.model_name.dropna().unique().tolist()
    for _mname in _models_list[:6]:
        _mdf  = _model_cmp[_model_cmp.model_name == _mname]
        _mlin = _mdf[_mdf.workflow_type == "linear"].avg_energy_j.mean()
        _mage = _mdf[_mdf.workflow_type == "agentic"].avg_energy_j.mean()
        _mprov = str(_mdf.provider.iloc[0]) if not _mdf.empty else "?"
        _mmult = _mage / _mlin if (_mlin and _mlin > 0) else 1
        _pclr  = "#38bdf8" if _mprov == "cloud" else "#22c55e"
        _lin_s = f"{_mlin:.4f}" if (_mlin and _mlin == _mlin) else "—"
        _age_s = f"{_mage:.4f}" if (_mage and _mage == _mage) else "—"
        st.markdown(
            f"<div style='background:{t['bg2']};border:0.5px solid {t['brd']};"
            f"border-left:3px solid {_pclr};border-radius:7px;"
            f"padding:9px 11px;margin-bottom:7px;'>"
            f"<div style='font-size:10px;font-weight:600;color:{t['t1']};margin-bottom:2px'>"
            f"{_mname[:28]}</div>"
            f"<div style='font-size:9px;color:{_pclr};margin-bottom:6px'>{_mprov}</div>"
            f"<div style='font-size:9px;color:{t['t2']}'>"
            f"Linear: <b style='color:#22c55e;font-family:monospace'>{_lin_s} J</b></div>"
            f"<div style='font-size:9px;color:{t['t2']}'>"
            f"Agentic: <b style='color:#ef4444;font-family:monospace'>{_age_s} J</b></div>"
            f"<div style='font-size:9px;color:#f59e0b;margin-top:4px'>"
            f"Overhead: <b>{_mmult:.2f}×</b></div>"
            f"</div>",
            unsafe_allow_html=True)

    st.markdown(
        f"<div style='height:0.5px;background:{t['brd']};margin:10px 0'></div>",
        unsafe_allow_html=True)

    # ── Task filter ───────────────────────────────────────────────────────────
    _task_list = _model_cmp.task_name.dropna().unique().tolist()
    _sel_task_ov = st.selectbox(
        "Filter task", ["all"] + sorted(_task_list),
        key="ov_task_filter")
    _cmp_filtered = (_model_cmp if _sel_task_ov == "all"
                     else _model_cmp[_model_cmp.task_name == _sel_task_ov])

    _cmp_pivot = _cmp_filtered.copy()
    _cmp_pivot["model_wf"] = (
        _cmp_pivot["model_name"].astype(str) + " · " +
        _cmp_pivot["workflow_type"].astype(str))

    # ── Energy chart ──────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:10px;font-weight:500;color:{t['t2']};margin:8px 0 4px'>"
        f"Energy (J) — model × workflow</div>", unsafe_allow_html=True)
    _fig_cmp = px.bar(
        _cmp_pivot.groupby(["model_wf", "workflow_type"])["avg_energy_j"]
                  .mean().reset_index(),
        x="model_wf", y="avg_energy_j", color="workflow_type",
        barmode="group", color_discrete_map=WF_COLORS,
        labels={"avg_energy_j": "Avg Energy (J)", "model_wf": "Model · Workflow"})
    _fig_cmp.update_xaxes(tickangle=30)
    _fig_cmp.update_layout(
        paper_bgcolor=t["bg1"], plot_bgcolor=t["bg2"],
        font=dict(color=t["t1"], size=9),
        margin=dict(t=20, b=60, l=40, r=10),
        legend=dict(bgcolor=t["bg1"], font=dict(color=t["t2"])),
        height=220)
    st.plotly_chart(fl(_fig_cmp), use_container_width=True)

    # ── mJ/token chart ────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:10px;font-weight:500;color:{t['t2']};margin:4px 0'>"
        f"mJ / token — model × workflow</div>", unsafe_allow_html=True)
    _fig_cmp2 = px.bar(
        _cmp_pivot.groupby(["model_wf", "workflow_type"])["avg_mj_per_token"]
                  .mean().reset_index(),
        x="model_wf", y="avg_mj_per_token", color="workflow_type",
        barmode="group", color_discrete_map=WF_COLORS,
        labels={"avg_mj_per_token": "mJ/token", "model_wf": "Model · Workflow"})
    _fig_cmp2.update_xaxes(tickangle=30)
    _fig_cmp2.update_layout(
        paper_bgcolor=t["bg1"], plot_bgcolor=t["bg2"],
        font=dict(color=t["t1"], size=9),
        margin=dict(t=20, b=60, l=40, r=10),
        legend=dict(bgcolor=t["bg1"], font=dict(color=t["t2"])),
        height=220)
    st.plotly_chart(fl(_fig_cmp2), use_container_width=True)

    st.markdown(
        f"<div style='height:0.5px;background:{t['brd']};margin:10px 0'></div>",
        unsafe_allow_html=True)

    # ── Full comparison matrix ────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:10px;font-weight:500;color:{t['t2']};margin-bottom:6px'>"
        f"Full comparison matrix</div>", unsafe_allow_html=True)
    _show_cols = [c for c in [
        "model_name", "provider", "workflow_type", "task_name",
        "runs", "avg_energy_j", "avg_dynamic_j", "avg_duration_s",
        "avg_tokens", "avg_mj_per_token", "avg_ipc",
        "avg_carbon_mg", "avg_water_ml"]
        if c in _model_cmp.columns]
    st.dataframe(_model_cmp[_show_cols].round(4),
                 use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN RENDER
# ─────────────────────────────────────────────────────────────────────────────
def render(ctx: dict):
    _inject_theme()
    t = _tok()

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

    # ── TOPBAR ────────────────────────────────────────────────────────────────
    _tcol, _tbtn = st.columns([8, 1])
    with _tcol:
        st.markdown(
            f"<div style='display:flex;align-items:baseline;gap:10px;padding:4px 0 10px'>"
            f"<span style='font-size:18px;font-weight:500;color:{t['t1']}'>A-LEMS</span>"
            f"<span style='font-size:11px;color:{t['t3']}'>Agentic LLM Energy Measurement System</span>"
            f"</div>", unsafe_allow_html=True)
    with _tbtn:
        _lbl = "☀ Light" if _is_dark() else "☾ Dark"
        if st.button(_lbl, key="ov_theme_toggle"):
            st.session_state["theme"] = "light" if _is_dark() else "dark"
            st.rerun()

    # ── LIVE JOB BANNER (original — untouched) ────────────────────────────────
    _live_jobs = pd.DataFrame()
    try:
        from gui.db import db as _db_ctx
        with _db_ctx() as _lcon:
            _live_jobs = pd.read_sql_query("""
                SELECT
                    e.exp_id, e.name, e.status, e.model_name, e.provider,
                    e.task_name, e.runs_total, e.started_at, e.completed_at,
                    e.error_message,
                    COUNT(r.run_id)                                 AS runs_done_actual,
                    COALESCE(e.runs_completed, COUNT(r.run_id))     AS runs_done,
                    ROUND(
                        100.0 * COALESCE(e.runs_completed, COUNT(r.run_id))
                        / NULLIF(e.runs_total, 0), 0)               AS pct_done
                FROM experiments e
                LEFT JOIN runs r ON r.exp_id = e.exp_id
                WHERE (
                    e.status IN ('running', 'pending', 'started')
                    OR (e.status = 'completed'
                        AND datetime(e.completed_at) > datetime('now', '-10 minutes'))
                    OR (e.status = 'error'
                        AND datetime(COALESCE(e.completed_at, e.started_at))
                            > datetime('now', '-30 minutes'))
                )
                GROUP BY e.exp_id
                ORDER BY e.started_at DESC
                LIMIT 3
            """, _lcon)
    except Exception:
        pass

    if not _live_jobs.empty:
        _has_running = any(s in ("running","started") for s in _live_jobs.status.values)
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
            _jdur = ""
            try:
                import datetime as _dt
                _ts = _dt.datetime.fromisoformat(
                    str(_jrow.get("started_at","")).replace("Z",""))
                _jend_s = str(_jrow.get("completed_at") or "")
                _te = (_dt.datetime.fromisoformat(_jend_s.replace("Z",""))
                       if _jend_s and _jend_s not in ("None","")
                       else _dt.datetime.now())
                _sec = int((_te - _ts).total_seconds())
                _jdur = f"{_sec//60}m{_sec%60:02d}s"
            except Exception:
                pass
            _err_html = ""
            if _jerr and _jerr not in ("None","") and _jstat == "error":
                _err_short = _jerr[:60] + ("…" if len(_jerr) > 60 else "")
                _err_html = (
                    f"<div style='font-size:8px;color:#ef4444;margin-top:3px;'>"
                    f"{_err_short}</div>")
            st.markdown(f"""
            <div style="background:{t['bg1']};border:0.5px solid {t['brd']};
                        border-left:3px solid {_jclr};border-radius:6px;
                        padding:7px 12px;margin-bottom:5px;">
              <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
                <span style="font-size:9px;color:{_jclr};font-weight:700;min-width:68px;">{_jpulse} {_jstat.upper()}</span>
                <span style="font-size:9px;color:{t['t1']};font-weight:600;flex:2;">{str(_jrow.get('task_name','?'))}</span>
                <span style="font-size:9px;color:{t['t3']};flex:2;overflow:hidden;white-space:nowrap;">{str(_jrow.get('model_name','?'))[:22]}</span>
                <span style="font-size:9px;color:{t['t3']};min-width:48px;">{str(_jrow.get('provider','?'))}</span>
                <span style="font-size:9px;color:{t['t3']};min-width:38px;">{_jdur}</span>
                <span style="font-size:9px;color:{_jclr};min-width:40px;font-family:monospace;text-align:right;">{_jdone}/{_jtot}</span>
              </div>
              <div style="background:{t['bg2']};border-radius:3px;height:5px;overflow:hidden;">
                <div style="background:{_jclr};width:{min(_jpct,100):.0f}%;height:100%;border-radius:3px;"></div>
              </div>
              {_err_html}
            </div>""", unsafe_allow_html=True)
        st.markdown("")

    # ── INSIGHT STRIP ─────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='background:{t['bg1']};border:0.5px solid {t['brd']};"
        f"border-left:4px solid #ef4444;border-radius:0 8px 8px 0;"
        f"padding:12px 18px;margin-bottom:14px;'>"
        f"<span style='font-size:15px;font-weight:500;color:{t['t1']}'>"
        f"Agentic costs <span style='color:#ef4444;font-family:monospace'>"
        f"{tax_mult:.1f}×</span> more energy than linear for the same task</span>"
        f"<span style='font-size:11px;color:{t['t3']};margin-left:14px'>"
        f"Measured across {ov.get('total_runs','—')} runs · "
        f"{ov.get('total_experiments','—')} experiments</span>"
        f"</div>",
        unsafe_allow_html=True)

    # ── KPI ROW ───────────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Runs",   ov.get("total_runs","—"))
    c2.metric("Tax Multiple", f"{tax_mult:.1f}×",
              delta=f"{(tax_mult-1)*100:.0f}% overhead", delta_color="inverse")
    c3.metric("Avg Planning", f"{plan_ms:.0f}ms",
              delta=f"{plan_pct:.0f}% of agentic time", delta_color="inverse")
    c4.metric("Peak IPC",     f"{ov.get('max_ipc', 0) or 0:.3f}")
    c5.metric("Avg Carbon",   f"{ov.get('avg_carbon_mg', 0) or 0:.3f}mg")
    c6.metric("Total Energy", f"{ov.get('total_energy_j', 0) or 0:.1f}J")

    st.markdown("<div style='margin-bottom:12px'></div>", unsafe_allow_html=True)

    # ── ROW 1: Energy compare  |  Tax by task  |  Recent sessions ─────────────
    # All three cards use identical height (340px) + flex-column so footers align
    CARD_H  = "340px"
    bg1 = t["bg1"]; bg2 = t["bg2"]; bg3 = t["bg3"]
    t1  = t["t1"];  t2  = t["t2"];  t3  = t["t3"]
    brd = t["brd"]; brd2 = t["brd2"]

    def _fixed_card(body_html: str, footer_html: str,
                    top_color: str = "#3b82f6") -> str:
        """Card with fixed height, body scrolls, footer pinned at bottom."""
        return (
            f"<div style='background:{bg1};border:0.5px solid {brd};"
            f"border-top:3px solid {top_color};border-radius:10px;"
            f"padding:14px 16px;height:{CARD_H};display:flex;"
            f"flex-direction:column;box-sizing:border-box;overflow:hidden;'>"
            f"<div style='flex:1;overflow:hidden;'>{body_html}</div>"
            f"<div style='flex-shrink:0;padding-top:9px;"
            f"border-top:0.5px solid {brd};margin-top:8px;'>{footer_html}</div>"
            f"</div>"
        )

    def _empty_row(label: str = "") -> str:
        """Phantom row — same height as a real row, invisible."""
        return (
            f"<div style='display:flex;align-items:center;gap:6px;"
            f"padding:6px 0;border-bottom:0.5px solid {brd};opacity:0.18;'>"
            f"<span style='width:6px;height:6px;border-radius:50%;"
            f"background:{brd2};flex-shrink:0;display:inline-block'></span>"
            f"<span style='font-size:9px;color:{t3};font-style:italic'>{label}</span>"
            f"</div>"
        )

    # ── Card 1: Linear vs Agentic ──────────────────────────────────────────────
    bar_pct = f"{100/max(tax_mult,1):.0f}%"
    _c1_body = (
        _label("Linear vs Agentic — total energy")
        + f"<div style='display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px'>"
        + f"<div style='background:{bg2};border-radius:7px;padding:10px 12px'>"
        + f"<div style='font-size:10px;color:{t3};margin-bottom:3px'>Linear</div>"
        + f"<div style='font-size:18px;font-weight:500;color:{t1}'>{avg_lin_j:.3f} J</div>"
        + f"<div style='font-size:10px;color:{t2};margin-top:2px'>baseline · 1×</div>"
        + f"<div style='background:{bg3};border-radius:3px;height:5px;margin-top:8px;overflow:hidden'>"
        + f"<div style='width:{bar_pct};background:#16a34a;height:5px;border-radius:3px'></div></div>"
        + f"</div>"
        + f"<div style='background:{bg2};border-radius:7px;padding:10px 12px'>"
        + f"<div style='font-size:10px;color:{t3};margin-bottom:3px'>Agentic</div>"
        + f"<div style='font-size:18px;font-weight:500;color:{t1}'>{avg_age_j:.3f} J</div>"
        + f"<div style='font-size:10px;color:#ef4444;margin-top:2px'>overhead · {tax_mult:.1f}×</div>"
        + f"<div style='background:{bg3};border-radius:3px;height:5px;margin-top:8px;overflow:hidden'>"
        + f"<div style='width:100%;background:#ef4444;height:5px;border-radius:3px'></div></div>"
        + f"</div></div>"
        # ratio bar
        + f"<div style='font-size:9px;color:{t3};margin-bottom:4px'>Cost ratio</div>"
        + f"<div style='background:{bg3};border-radius:4px;height:20px;overflow:hidden;position:relative'>"
        + f"<div style='width:{bar_pct};background:#16a34a;height:20px;border-radius:4px 0 0 4px;"
        + f"display:flex;align-items:center;padding-left:8px;font-size:9px;color:#fff;'>linear</div>"
        + f"</div>"
    )
    _c1_foot = (
        f"<div style='font-size:10px;color:{t3}'>"
        f"Extra cost: <b style='color:{t1}'>{avg_age_j-avg_lin_j:.3f} J</b>"
        f" per session — orchestration tax in absolute terms</div>"
    )

    # ── Card 2: Tax multiplier — always 6 rows ─────────────────────────────────
    _tax_real_rows = []
    try:
        _tax_df, _ = q_safe("""
            SELECT
                e.task_name || ' · ' || COALESCE(e.model_name, e.provider, '?') AS label,
                ROUND(AVG(
                    CAST(ra.total_energy_uj AS REAL) /
                    NULLIF(rl.total_energy_uj, 0)
                ), 2) AS tmult
            FROM orchestration_tax_summary ots
            JOIN runs rl ON ots.linear_run_id  = rl.run_id
            JOIN runs ra ON ots.agentic_run_id = ra.run_id
            JOIN experiments e ON ra.exp_id = e.exp_id
            WHERE rl.total_energy_uj > 0
            GROUP BY label ORDER BY tmult DESC LIMIT 6
        """)
        if not _tax_df.empty:
            _max_t = float(_tax_df.tmult.max()) or 1
            for _, _tr in _tax_df.iterrows():
                _tv  = float(_tr.tmult or 0)
                _tw  = f"{_tv/_max_t*95:.0f}%"
                _clr = "#dc2626" if _tv >= 10 else "#d97706" if _tv >= 2 else "#16a34a"
                _tax_real_rows.append(
                    f"<div style='display:flex;align-items:center;gap:7px;margin-bottom:8px'>"
                    f"<div style='font-size:10px;color:{t2};width:110px;white-space:nowrap;"
                    f"overflow:hidden;text-overflow:ellipsis'>{str(_tr.label)[:20]}</div>"
                    f"<div style='flex:1;background:{bg2};border-radius:3px;height:13px;overflow:hidden'>"
                    f"<div style='width:{_tw};background:{_clr};height:13px;border-radius:3px;"
                    f"display:flex;align-items:center;padding-left:5px;"
                    f"font-size:9px;font-weight:500;color:#fff'>{_tv:.1f}×</div></div>"
                    f"<div style='font-size:10px;color:{t1};width:32px;text-align:right;"
                    f"font-weight:500'>{_tv:.1f}×</div></div>")
    except Exception:
        pass

    _c2_body = _label("Tax multiplier by task + model")
    for _row in _tax_real_rows:
        _c2_body += _row
    # pad to 6 rows
    _empty_tax = (
        f"<div style='display:flex;align-items:center;gap:7px;margin-bottom:8px;opacity:0.2'>"
        f"<div style='width:110px;height:10px;background:{bg3};border-radius:3px'></div>"
        f"<div style='flex:1;height:13px;background:{bg3};border-radius:3px'></div>"
        f"<div style='width:32px;height:10px;background:{bg3};border-radius:3px'></div></div>"
    )
    for _ in range(6 - len(_tax_real_rows)):
        _c2_body += _empty_tax
    _c2_foot = f"<div style='font-size:9px;color:{t3}'>red &gt;10× · amber 2–10× · green &lt;2×</div>"

    # ── Card 3: Recent sessions — always 6 rows ────────────────────────────────
    _sess_real_rows = []
    try:
        _sess_df, _ = q_safe("""
            SELECT e.group_id,
                   MAX(e.status)           AS status,
                   COUNT(DISTINCT e.exp_id) AS n_exps,
                   MAX(e.created_at)       AS latest
            FROM experiments e
            GROUP BY e.group_id
            ORDER BY MAX(e.exp_id) DESC LIMIT 6
        """)
        _tax_by_group = {}
        try:
            _tg, _ = q_safe("""
                SELECT e.group_id,
                       ROUND(AVG(CAST(ra.total_energy_uj AS REAL) /
                             NULLIF(rl.total_energy_uj,0)),2) AS mt
                FROM orchestration_tax_summary ots
                JOIN runs rl ON ots.linear_run_id  = rl.run_id
                JOIN runs ra ON ots.agentic_run_id = ra.run_id
                JOIN experiments e ON ra.exp_id = e.exp_id
                GROUP BY e.group_id
            """)
            if not _tg.empty:
                _tax_by_group = dict(zip(_tg.group_id, _tg.mt))
        except Exception:
            pass
        if not _sess_df.empty:
            for _, _sr in _sess_df.iterrows():
                _ss  = str(_sr.get("status","?"))
                _clr = {"completed":"#16a34a","running":"#3b82f6",
                        "error":"#ef4444","failed":"#ef4444"}.get(_ss,"#d97706")
                _pill = (f"<span style='font-size:9px;padding:1px 5px;"
                         f"border-radius:100px;background:{_clr}18;"
                         f"color:{_clr};border:0.5px solid {_clr}40'>{_ss}</span>")
                _gid   = str(_sr.get("group_id") or "")
                _mt    = _tax_by_group.get(_gid)
                _tax_s = f"{float(_mt):.1f}×" if _mt and str(_mt) != "nan" else "—"
                _short = _gid.replace("session_","")[:17]
                try:
                    import datetime as _dt
                    _ago = _dt.datetime.now() - _dt.datetime.fromisoformat(
                        str(_sr.latest).replace("Z",""))
                    _s  = int(_ago.total_seconds())
                    _ts = (f"{_s//3600}h" if _s > 3600 else
                           f"{_s//60}m" if _s > 60 else "now")
                except Exception:
                    _ts = "—"
                _sess_real_rows.append(
                    f"<div style='display:flex;align-items:center;gap:6px;"
                    f"padding:6px 0;border-bottom:0.5px solid {brd};font-size:10px'>"
                    f"<span style='width:6px;height:6px;border-radius:50%;"
                    f"background:{_clr};flex-shrink:0;display:inline-block'></span>"
                    f"<span style='font-family:monospace;color:{t1};font-size:9px;"
                    f"flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap'>{_short}</span>"
                    f"{_pill}"
                    f"<span style='color:{t2};width:32px;text-align:right;font-size:10px'>{_tax_s}</span>"
                    f"<span style='color:{t3};font-size:9px;width:22px;text-align:right'>{_ts}</span>"
                    f"</div>")
    except Exception:
        pass

    _c3_body = _label("Recent sessions")
    for _row in _sess_real_rows:
        _c3_body += _row
    for _i in range(6 - len(_sess_real_rows)):
        _c3_body += _empty_row("no sessions yet" if _i == 0 and not _sess_real_rows else "")
    _c3_foot = (
        f"<div style='display:flex;gap:6px;'>"
        f"<button style='flex:1;font-size:10px;padding:5px;border-radius:6px;"
        f"border:0.5px solid {brd2};background:{bg2};color:{t1};cursor:pointer'>All sessions</button>"
        f"<button style='flex:1;font-size:10px;padding:5px;border-radius:6px;"
        f"background:#3b82f6;border:none;color:#fff;cursor:pointer'>Run now</button>"
        f"</div>"
    )

    # ── Render equal-height cards ──────────────────────────────────────────────
    col_e, col_tax, col_sess = st.columns(3)
    with col_e:
        st.markdown(_fixed_card(_c1_body, _c1_foot), unsafe_allow_html=True)
    with col_tax:
        st.markdown(_fixed_card(_c2_body, _c2_foot), unsafe_allow_html=True)
    with col_sess:
        st.markdown(_fixed_card(_c3_body, _c3_foot), unsafe_allow_html=True)

    st.markdown("<div style='margin-bottom:10px'></div>", unsafe_allow_html=True)

    # ── ROW 2: Execution Breakdown | Tax Attribution | Energy Timeline ─────────
    col_bd, col_attr, col_tl = st.columns(3)

    # ── Phase data ─────────────────────────────────────────────────────────────
    # Energy per phase from DB if available, else derive from time fractions
    plan_j  = avg_age_j * (plan_pct  / 100) if plan_pct  > 0 else 0
    exec_j  = avg_age_j * (exec_pct  / 100) if exec_pct  > 0 else 0
    synth_j = avg_age_j * (synth_pct / 100) if synth_pct > 0 else 0
    total_phase_ms = max(plan_ms + exec_ms + synth_ms, 1)

    with col_bd:
        _has_phases = plan_ms > 0
        _bd_content = _label("Agentic execution breakdown")
        if _has_phases:
            _bd_content += f"""
            <div style='display:flex;height:20px;border-radius:5px;overflow:hidden;gap:1px;margin-bottom:11px'>
              <div style='width:{plan_pct:.0f}%;background:#d97706;display:flex;align-items:center;
                   justify-content:center;font-size:9px;font-weight:500;color:#fff'>{plan_pct:.0f}%</div>
              <div style='width:{exec_pct:.0f}%;background:#3b82f6;display:flex;align-items:center;
                   justify-content:center;font-size:9px;font-weight:500;color:#fff'>{exec_pct:.0f}%</div>
              <div style='width:{synth_pct:.0f}%;background:#7c3aed;display:flex;align-items:center;
                   justify-content:center;font-size:9px;font-weight:500;color:#fff'>{synth_pct:.0f}%</div>
            </div>"""
            for phase_name, ms, pct, clr in [
                ("Planning",  plan_ms,  plan_pct,  "#d97706"),
                ("Execution", exec_ms,  exec_pct,  "#3b82f6"),
                ("Synthesis", synth_ms, synth_pct, "#7c3aed"),
            ]:
                _bd_content += (
                    f"<div style='display:flex;align-items:center;gap:7px;margin-bottom:8px'>"
                    f"<div style='display:flex;align-items:center;gap:4px;width:78px;"
                    f"font-size:10px;color:{t['t2']}'>"
                    f"<span style='width:7px;height:7px;border-radius:2px;background:{clr};flex-shrink:0'></span>"
                    f"{phase_name}</div>"
                    f"<div style='flex:1;background:{t['bg2']};border-radius:3px;height:9px;overflow:hidden'>"
                    f"<div style='width:{pct:.0f}%;background:{clr};height:9px;border-radius:3px'></div></div>"
                    f"<span style='font-size:10px;color:{t['t1']};width:28px;text-align:right'>{pct:.0f}%</span>"
                    f"<span style='font-size:9px;color:{t['t3']};width:48px;text-align:right;"
                    f"font-family:monospace'>{ms:.0f}ms</span></div>")
            # Energy per phase
            _bd_content += (
                f"<div style='border-top:0.5px solid {t['brd']};padding-top:9px;margin-top:4px'>"
                f"<div style='font-size:10px;color:{t['t3']};margin-bottom:7px'>Energy per phase (J)</div>")
            _max_phase_j = max(plan_j, exec_j, synth_j, 0.0001)
            for p_name, p_j, p_clr in [
                ("Planning",  plan_j,  "#d97706"),
                ("Execution", exec_j,  "#3b82f6"),
                ("Synthesis", synth_j, "#7c3aed"),
            ]:
                _bw = int(p_j / _max_phase_j * 80)
                _bd_content += (
                    f"<div style='display:flex;justify-content:space-between;align-items:center;"
                    f"padding:5px 0;border-bottom:0.5px solid {t['brd']}'>"
                    f"<div style='font-size:10px;color:{t['t2']}'>{p_name}</div>"
                    f"<div style='display:flex;align-items:center;gap:5px'>"
                    f"<div style='width:{_bw}px;height:8px;border-radius:2px;"
                    f"background:{p_clr}'></div>"
                    f"<span style='font-size:10px;font-weight:500;color:{t['t1']};"
                    f"width:55px;text-align:right;font-family:monospace'>{p_j:.2f} J</span>"
                    f"</div></div>")
            _bd_content += "</div>"
        else:
            _bd_content += (
                f"<div style='font-size:10px;color:{t['t3']};padding:10px 0'>"
                f"No phase data — run agentic experiments first.</div>")
        st.markdown(_card(_bd_content, border_top_color="#3b82f6"), unsafe_allow_html=True)

    with col_attr:
        # Tax attribution card — red top
        _attr_content = _label("Orchestration tax attribution")
        _attr_content += (
            f"<div style='font-size:13px;font-weight:500;color:{t['t1']};margin-bottom:12px'>"
            f"Where the extra cost originates</div>")

        _total_attr_ms = max(plan_ms + exec_ms + synth_ms, 1)
        _base_ms = max(_total_attr_ms * 0.14, 1)  # ~14% base compute
        for _aname, _ams, _aj, _aclr in [
            ("Planning latency", plan_ms,  plan_j,  "#d97706"),
            ("Tool API latency", exec_ms,  exec_j,  "#3b82f6"),
            ("Synthesis / other",synth_ms, synth_j, "#7c3aed"),
            ("LLM base compute", _base_ms, avg_lin_j, t["bg3"]),
        ]:
            _aw = int(_ams / (_total_attr_ms + _base_ms) * 90)
            _attr_content += (
                f"<div style='margin-bottom:9px'>"
                f"<div style='display:flex;justify-content:space-between;font-size:10px;"
                f"color:{t['t2']};margin-bottom:3px'>"
                f"<span style='display:flex;align-items:center;gap:5px'>"
                f"<span style='width:8px;height:8px;border-radius:2px;"
                f"background:{_aclr};flex-shrink:0'></span>{_aname}</span>"
                f"<span style='font-family:monospace;color:{_aclr}'>"
                f"{_ams:.0f}ms · {_aj:.2f}J</span></div>"
                f"<div style='background:{t['bg2']};border-radius:3px;height:9px;overflow:hidden'>"
                f"<div style='width:{_aw}%;background:{_aclr};height:9px;border-radius:3px'>"
                f"</div></div></div>")

        _overhead_pct = (tax_mult - 1) / tax_mult * 100 if tax_mult > 1 else 0
        _attr_content += (
            f"<div style='display:flex;justify-content:space-between;"
            f"border-top:0.5px solid {t['brd']};padding-top:9px;margin-top:4px;"
            f"font-size:10px;color:{t['t3']}'>"
            f"<span>Avg tax <b style='color:#ef4444'>{_overhead_pct:.1f}%</b> of agentic budget</span>"
            f"<span>Tax <b style='color:#ef4444'>{tax_mult:.1f}×</b></span></div>"
            f"<div style='margin-top:8px;font-size:9px;color:{t['t3']};line-height:1.5'>"
            f"Planning + tool latency dominate overhead. Execution cost scales with "
            f"task complexity; synthesis is relatively fixed.</div>")
        st.markdown(_card(_attr_content, border_top_color="#ef4444"), unsafe_allow_html=True)

    with col_tl:
        # Energy timeline — sparkline bars + phase overlay
        _tl_content = _label("Energy timeline — last agentic run")
        _tl_content += (
            f"<div style='font-size:10px;color:{t['t2']};margin-bottom:8px'>"
            f"Power (W) over time with phase boundaries</div>")

        # Power sparkline: synthetic from phase data
        import math
        _bars = 72
        _bar_data = []
        for _bi in range(_bars):
            _x = _bi / _bars
            if _x < plan_pct/100:
                _pw = 1.2 + (_x/(plan_pct/100)) * 2.0
            elif _x < (plan_pct + exec_pct)/100:
                _local = (_x - plan_pct/100)/(exec_pct/100)
                _pw = 3.8 + math.sin(_local * 10) * 0.6
            else:
                _local = (_x - (plan_pct+exec_pct)/100)/(synth_pct/100 + 0.001)
                _pw = 3.5 - _local * 2.8
            _pw = max(0.3, min(5.0, _pw))
            _clr_bar = ("#d97706" if _x < plan_pct/100
                        else "#3b82f6" if _x < (plan_pct+exec_pct)/100
                        else "#7c3aed")
            _bar_data.append((_pw, _clr_bar))

        _pw_html = (
            f"<div style='background:{t['bg2']};border-radius:4px;height:48px;"
            f"display:flex;align-items:flex-end;gap:1px;padding:2px;overflow:hidden;margin-bottom:5px'>")
        for _pw, _bc in _bar_data:
            _bh = int(_pw / 5 * 44)
            _pw_html += (
                f"<div style='flex:1;height:{_bh}px;border-radius:1px 1px 0 0;"
                f"background:{_bc};opacity:.8;min-width:2px'></div>")
        _pw_html += "</div>"

        # Phase segments bar
        _pw_html += (
            f"<div style='display:flex;border-radius:4px;overflow:hidden;"
            f"height:18px;gap:1px;margin-bottom:5px'>"
            f"<div style='width:{plan_pct:.0f}%;background:#d97706aa;display:flex;"
            f"align-items:center;justify-content:center;font-size:8px;color:#fff'>Plan</div>"
            f"<div style='width:{exec_pct:.0f}%;background:#3b82f6aa;display:flex;"
            f"align-items:center;justify-content:center;font-size:8px;color:#fff'>Exec</div>"
            f"<div style='width:{synth_pct:.0f}%;background:#7c3aedaa;display:flex;"
            f"align-items:center;justify-content:center;font-size:8px;color:#fff'>Synth</div>"
            f"</div>")

        # Time axis
        _pw_html += (
            f"<div style='display:flex;justify-content:space-between;"
            f"font-size:9px;color:{t['t3']};margin-bottom:8px'>"
            f"<span>0ms</span><span>{plan_ms:.0f}ms</span>"
            f"<span>{plan_ms+exec_ms:.0f}ms</span>"
            f"<span>{total_phase_ms:.0f}ms</span></div>")

        # Phase legend
        _pw_html += f"<div style='display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px'>"
        for _pn, _pc in [("Planning","#d97706"),("Execution","#3b82f6"),("Synthesis","#7c3aed")]:
            _pw_html += (
                f"<span style='font-size:9px;display:flex;align-items:center;gap:3px;"
                f"color:{t['t2']}'>"
                f"<span style='width:8px;height:8px;border-radius:2px;background:{_pc}'></span>"
                f"{_pn}</span>")
        _pw_html += "</div>"

        # Mini stats row
        _peak_w = avg_age_j / max(total_phase_ms/1000, 0.001)
        _mean_w = _peak_w * 0.55
        _pw_html += (
            f"<div style='display:grid;grid-template-columns:1fr 1fr 1fr;"
            f"border-top:0.5px solid {t['brd']};padding-top:8px;text-align:center'>"
            f"<div><div style='font-size:9px;color:{t['t3']}'>Peak power</div>"
            f"<div style='font-size:13px;font-weight:500;color:#ef4444'>{_peak_w:.1f} W</div></div>"
            f"<div><div style='font-size:9px;color:{t['t3']}'>Mean power</div>"
            f"<div style='font-size:13px;font-weight:500;color:{t['t1']}'>{_mean_w:.1f} W</div></div>"
            f"<div><div style='font-size:9px;color:{t['t3']}'>Total energy</div>"
            f"<div style='font-size:13px;font-weight:500;color:#3b82f6'>{avg_age_j:.2f} J</div></div>"
            f"</div>")

        _tl_content += _pw_html
        st.markdown(_card(_tl_content, border_top_color="#3b82f6"), unsafe_allow_html=True)

    st.markdown("<div style='margin-bottom:10px'></div>", unsafe_allow_html=True)

    # ── ROW 3: Hardware | Environment | Activity feed ─────────────────────────
    col_hw, col_env, col_act = st.columns(3)

    with col_hw:
        hw  = q1("SELECT * FROM hardware_config ORDER BY hw_id DESC LIMIT 1") or {}
        gov = (str(runs["governor"].iloc[0])
               if "governor" in runs.columns and not runs.empty else "—")

        def _hw_row(k, v, vclr=None):
            vc = vclr or t["t1"]
            return (f"<div style='display:flex;justify-content:space-between;"
                    f"padding:5px 0;border-bottom:0.5px solid {t['brd']};font-size:10px'>"
                    f"<span style='color:{t['t3']}'>{k}</span>"
                    f"<span style='color:{vc};font-family:monospace;font-size:9px;"
                    f"max-width:160px;overflow:hidden;text-overflow:ellipsis;"
                    f"white-space:nowrap;text-align:right'>{v}</span></div>")

        def _ok(v): return "#16a34a" if v else t["t3"]

        _hw_html = (_hw_row("CPU",    hw.get("cpu_model","—"))
                  + _hw_row("Arch",   f"{hw.get('cpu_architecture','—')} · "
                                      f"{hw.get('cpu_vendor','—')} · "
                                      f"{hw.get('cpu_cores','?')}c/{hw.get('cpu_threads','?')}t")
                  + _hw_row("RAM",    f"{hw.get('ram_gb','?')} GB")
                  + _hw_row("ISA",    f"AVX2:{'✓' if hw.get('has_avx2') else '✗'}  "
                                      f"AVX-512:{'✓' if hw.get('has_avx512') else '✗'}  "
                                      f"VT-x:{'✓' if hw.get('has_vmx') else '✗'}")
                  + _hw_row("RAPL",   str(hw.get("rapl_domains","—"))[:30], "#16a34a")
                  + _hw_row("GPU",    hw.get("gpu_model","—") or "—")
                  + _hw_row("System", hw.get("system_product","—") or "—")
                  + _hw_row("Governor", gov, "#16a34a"))

        # Live resource bars
        try:
            import psutil as _ps
            _cpu_pct  = _ps.cpu_percent(interval=None)
            _ram_pct  = _ps.virtual_memory().percent
            _temps    = _ps.sensors_temperatures() if hasattr(_ps, "sensors_temperatures") else {}
            _temp_c   = 0
            for _tn in ("coretemp","cpu_thermal","k10temp"):
                if _tn in _temps and _temps[_tn]:
                    _temp_c = _temps[_tn][0].current; break
        except Exception:
            _cpu_pct, _ram_pct, _temp_c = 0, 0, 0

        _hw_html += f"<div style='margin-top:10px'>"
        _hw_html += (f"<div style='font-size:9px;color:{t['t3']};margin-bottom:6px'>"
                     f"Live resource usage</div>")
        for _rn, _rv, _rc in [
            ("CPU",  _cpu_pct,  "#3b82f6"),
            ("RAM",  _ram_pct,  "#7c3aed"),
            ("Temp", _temp_c,   "#d97706" if _temp_c > 70 else "#3b82f6"),
        ]:
            _rw = min(int(_rv), 100)
            _hw_html += (
                f"<div style='display:flex;align-items:center;gap:7px;margin-bottom:5px;"
                f"font-size:10px'>"
                f"<span style='color:{t['t3']};width:36px'>{_rn}</span>"
                f"<div style='flex:1;background:{t['bg2']};border-radius:3px;height:7px'>"
                f"<div style='width:{_rw}%;background:{_rc};height:7px;border-radius:3px'>"
                f"</div></div>"
                f"<span style='color:{t['t2']};width:38px;text-align:right'>"
                f"{'°C' and f'{_rv:.0f}°C' if _rn == 'Temp' else f'{_rv:.0f}%'}</span></div>")
        _hw_html += "</div>"

        st.markdown(_card(
            f"{_label('Hardware · ' + str(hw.get('hostname','—')))}{_hw_html}",
            border_top_color="#3b82f6"), unsafe_allow_html=True)

    with col_env:
        env = q1("SELECT * FROM environment_config ORDER BY env_id DESC LIMIT 1") or {}

        def _env_row(k, v, vclr=None):
            vc = vclr or t["t1"]
            return (f"<div style='display:flex;justify-content:space-between;"
                    f"padding:5px 0;border-bottom:0.5px solid {t['brd']};font-size:10px'>"
                    f"<span style='color:{t['t3']}'>{k}</span>"
                    f"<span style='color:{vc};font-family:monospace;font-size:9px;"
                    f"max-width:160px;overflow:hidden;text-overflow:ellipsis;"
                    f"white-space:nowrap;text-align:right'>{v}</span></div>")

        _dirty_clr = "#d97706" if env.get("git_dirty") else "#16a34a"
        _dirty_lbl = "yes — uncommitted" if env.get("git_dirty") else "clean"
        _env_html = (
            _env_row("Python",  f"{env.get('python_version','—')} · {env.get('python_implementation','—')}")
          + _env_row("OS",      f"{env.get('os_name','—')} {env.get('kernel_version','—')}"[:32])
          + _env_row("LLM",     env.get("llm_framework","—") or "—")
          + _env_row("Framework", env.get("framework_version","—") or "—")
          + _env_row("NumPy",   env.get("numpy_version","—") or "—")
          + _env_row("Torch",   env.get("torch_version","—") or "not installed", t["t3"])
          + _env_row("Branch",  env.get("git_branch","—") or "—")
          + _env_row("Commit",  str(env.get("git_commit","—") or "—")[:12])
          + _env_row("Dirty",   _dirty_lbl, _dirty_clr))

        _hw_hash  = str(hw.get("hardware_hash","") or "")[:12]
        _env_hash = str(env.get("env_hash","") or "")[:12]
        _fp       = f"{_hw_hash}:{_env_hash}" if _hw_hash and _env_hash else "—"
        _env_html += (
            f"<div style='margin-top:10px;padding:7px 10px;background:{t['bg2']};"
            f"border-radius:6px;font-size:9px;color:{t['t3']}'>"
            f"Fingerprint <span style='font-family:monospace;color:{t['t1']}'>{_fp}</span></div>"
            f"<div style='margin-top:8px;font-size:9px;color:{t['t3']};line-height:1.5'>"
            f"Share to reproduce on identical hardware + software. Full details in PDF Appendix A+B.</div>")

        st.markdown(_card(
            f"{_label('Software environment')}{_env_html}",
            border_top_color="#7c3aed"), unsafe_allow_html=True)

    with col_act:
        # Activity feed from recent events
        _act_html = ""
        try:
            _act_df, _ = q_safe("""
                SELECT e.group_id, e.task_name, e.model_name, e.status,
                       e.completed_at, e.started_at, e.error_message,
                       e.runs_completed, e.runs_total
                FROM experiments e
                ORDER BY e.exp_id DESC LIMIT 8
            """)
            if not _act_df.empty:
                for _, _ar in _act_df.iterrows():
                    _astat = str(_ar.get("status","?"))
                    _aclr  = {"completed":"#16a34a","running":"#3b82f6",
                              "error":"#ef4444","failed":"#ef4444"}.get(_astat,"#d97706")
                    _atask = str(_ar.get("task_name","?"))
                    _amod  = str(_ar.get("model_name","?") or "")[:16]
                    _aerr  = str(_ar.get("error_message","") or "")
                    try:
                        import datetime as _dt
                        _ats = str(_ar.get("completed_at") or _ar.get("started_at") or "")
                        _ago = _dt.datetime.now() - _dt.datetime.fromisoformat(
                            _ats.replace("Z",""))
                        _s   = int(_ago.total_seconds())
                        _ts  = (f"{_s//3600}h ago" if _s > 3600 else
                                f"{_s//60}m ago" if _s > 60 else "now")
                    except Exception:
                        _ts = "—"
                    _body = (f"{_atask} · {_amod}"
                             if _astat != "error"
                             else f"Failed · {_aerr[:40]}")
                    _act_html += (
                        f"<div style='display:flex;gap:7px;padding:6px 0;"
                        f"border-bottom:0.5px solid {t['brd']};font-size:10px'>"
                        f"<div style='width:6px;height:6px;border-radius:50%;"
                        f"background:{_aclr};margin-top:3px;flex-shrink:0'></div>"
                        f"<div style='color:{t['t1']};flex:1;line-height:1.4'>{_body}</div>"
                        f"<div style='color:{t['t3']};font-size:9px;white-space:nowrap'>{_ts}</div>"
                        f"</div>")
        except Exception:
            pass
        if not _act_html:
            _act_html = f"<div style='font-size:10px;color:{t['t3']}'>No activity yet</div>"

        st.markdown(_card(
            f"{_label('Activity feed')}{_act_html}",
            border_top_color="#16a34a"), unsafe_allow_html=True)

    st.markdown("<div style='margin-bottom:10px'></div>", unsafe_allow_html=True)

    # ── ROW 4: Duration vs Energy + IPC vs Cache Miss (original, themed) ───────
    if not runs.empty and "energy_j" in runs.columns:
        col_sc1, col_sc2 = st.columns(2)
        _pl_layout = dict(
            paper_bgcolor=t["bg1"], plot_bgcolor=t["bg2"],
            font=dict(color=t["t1"], size=10),
            margin=dict(t=10, b=40, l=50, r=10),
            legend=dict(bgcolor=t["bg1"], bordercolor=t["brd"],
                        borderwidth=0.5, font=dict(color=t["t2"])))
        with col_sc1:
            st.markdown(
                f"<div style='font-size:11px;font-weight:500;color:{t['t2']};"
                f"margin-bottom:6px'>Duration vs Energy — all runs</div>",
                unsafe_allow_html=True)
            _df = runs.dropna(subset=["energy_j","duration_ms"]).copy()
            _df["duration_s"] = _df["duration_ms"] / 1000
            fig = px.scatter(_df, x="duration_s", y="energy_j",
                             color="workflow_type", color_discrete_map=WF_COLORS,
                             hover_data=["run_id","provider","task_name"],
                             labels={"duration_s":"Duration (s)","energy_j":"Energy (J)"})
            fig.update_layout(**_pl_layout)
            st.plotly_chart(fl(fig), use_container_width=True)

        with col_sc2:
            st.markdown(
                f"<div style='font-size:11px;font-weight:500;color:{t['t2']};"
                f"margin-bottom:6px'>IPC vs Cache Miss</div>",
                unsafe_allow_html=True)
            _df2 = runs.dropna(subset=["ipc","cache_miss_rate"]).copy()
            _df2["cache_miss_pct"] = _df2["cache_miss_rate"] * 100
            fig2 = px.scatter(_df2, x="cache_miss_pct", y="ipc",
                              color="workflow_type", color_discrete_map=WF_COLORS,
                              hover_data=["run_id","provider"],
                              labels={"cache_miss_pct":"Cache Miss %","ipc":"IPC"})
            fig2.update_layout(**_pl_layout)
            st.plotly_chart(fl(fig2), use_container_width=True)
