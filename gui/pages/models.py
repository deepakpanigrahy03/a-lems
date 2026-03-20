"""
gui/pages/models.py  —  🍎 Models
Reads config/models.json live on every render — no hardcoding.
Schema:  { "cloud": { "linear": {...}, "agentic": {...} },
           "local":  { "linear": {...}, "agentic": {...} } }
"""
import json
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

from gui.config  import PROJECT_ROOT, WF_COLORS
from gui.db      import q_safe
from gui.helpers import fl

# ── Theme helpers ─────────────────────────────────────────────────────────────
def _is_dark() -> bool:
    return st.session_state.get("theme", "dark") == "dark"

def _tok() -> dict:
    if _is_dark():
        return dict(
            bg0="#0d1117", bg1="#111827", bg2="#1f2937", bg3="#374151",
            t1="#f1f5f9",  t2="#94a3b8",  t3="#475569",
            brd="#1f2937", brd2="#374151", accent="#3b82f6",
        )
    return dict(
        bg0="#f3f4f6", bg1="#ffffff", bg2="#f9fafb", bg3="#d1d5db",
        t1="#1f2937",  t2="#4b5563",  t3="#6b7280",
        brd="#d1d5db", brd2="#9ca3af", accent="#3b82f6",
    )


# ── Load models.json — no caching so changes show immediately ─────────────────
def _load_models() -> dict:
    for candidate in [
        PROJECT_ROOT / "config" / "models.json",
        PROJECT_ROOT / "models.json",
    ]:
        if candidate.exists():
            try:
                return json.loads(candidate.read_text())
            except Exception as e:
                return {"_error": str(e), "_path": str(candidate)}
    return {"_missing": True}


# ── DB stats keyed by (model_name, workflow_type) ─────────────────────────────
def _load_stats() -> dict:
    stats = {}
    try:
        df, _ = q_safe("""
            SELECT e.model_name, r.workflow_type,
                   COUNT(*)                                  AS runs,
                   ROUND(AVG(r.total_energy_uj)/1e6, 4)     AS avg_j,
                   ROUND(AVG(r.dynamic_energy_uj)/1e6, 4)   AS avg_dyn_j,
                   ROUND(AVG(r.duration_ns)/1e9, 3)         AS avg_dur_s,
                   ROUND(AVG(r.total_tokens), 1)            AS avg_tokens,
                   ROUND(AVG(CASE WHEN r.total_tokens > 0
                       THEN r.total_energy_uj / r.total_tokens
                       END) / 1e3, 4)                       AS avg_mj_tok,
                   ROUND(AVG(r.ipc), 3)                     AS avg_ipc,
                   ROUND(AVG(r.carbon_g)*1000, 4)           AS avg_carbon_mg
            FROM runs r
            JOIN experiments e ON r.exp_id = e.exp_id
            WHERE e.model_name IS NOT NULL
            GROUP BY e.model_name, r.workflow_type
        """)
        if not df.empty:
            for _, row in df.iterrows():
                stats[(str(row["model_name"]), str(row["workflow_type"]))] = row.to_dict()
    except Exception:
        pass
    return stats


def _find_stat(stats: dict, spec: dict, wf: str) -> dict | None:
    """Match a spec to a stats row. Tries model_id then name."""
    for field in ("model_id", "name"):
        v = spec.get(field)
        if v:
            hit = stats.get((str(v), wf))
            if hit:
                return hit
    return None


# ── Spec block (linear or agentic half of a card) ─────────────────────────────
def _spec_block(spec: dict, wf: str, wf_stats: dict | None, t: dict) -> str:
    bg2, bg3   = t["bg2"], t["bg3"]
    t1, t2, t3 = t["t1"], t["t2"], t["t3"]
    brd        = t["brd"]
    wf_clr     = "#16a34a" if wf == "linear" else "#ef4444"

    name   = spec.get("name", "—")
    mid    = spec.get("model_id", "")
    ep     = spec.get("api_endpoint", "")
    mpath  = spec.get("model_path", "")
    mtok   = spec.get("max_tokens", "—")
    temp   = spec.get("temperature", "—")
    tools_s = spec.get("tools_supported", False)
    tools  = spec.get("tools", [])

    def _row(k, v, vc=None):
        vc = vc or t2
        return (
            f"<div style='display:flex;justify-content:space-between;"
            f"padding:4px 0;border-bottom:0.5px solid {brd};font-size:10px'>"
            f"<span style='color:{t3}'>{k}</span>"
            f"<span style='color:{vc};font-family:monospace;max-width:150px;"
            f"overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"
            f"text-align:right'>{str(v) if v not in (None,'') else '—'}</span></div>"
        )

    html = (
        f"<div style='background:{bg2};border-radius:7px;padding:10px 12px;"
        f"border-left:3px solid {wf_clr};margin-bottom:8px'>"
        f"<div style='font-size:9px;font-weight:700;color:{wf_clr};"
        f"text-transform:uppercase;letter-spacing:.07em;margin-bottom:5px'>{wf}</div>"
        f"<div style='font-size:11px;font-weight:500;color:{t1};margin-bottom:7px'>"
        f"{name}</div>"
    )
    if mid:    html += _row("Model ID",    mid)
    if ep:     html += _row("Endpoint",    ep.replace("https://","")[:38])
    if mpath:  html += _row("Model file",  Path(mpath).name)
    html += _row("Max tokens",  mtok)
    html += _row("Temperature", temp)
    if tools_s and tools:
        html += _row("Tools", "✓ " + ", ".join(tools), "#16a34a")
    else:
        html += _row("Tools", "✓ enabled" if tools_s else "✗ disabled",
                     "#16a34a" if tools_s else t3)

    # Measured energy section
    if wf_stats:
        avg_j    = wf_stats.get("avg_j", 0) or 0
        avg_mj   = wf_stats.get("avg_mj_tok", 0) or 0
        avg_dur  = wf_stats.get("avg_dur_s", 0) or 0
        avg_tok  = wf_stats.get("avg_tokens", 0) or 0
        runs     = int(wf_stats.get("runs", 0) or 0)
        html += (
            f"<div style='margin-top:8px;padding-top:7px;border-top:0.5px solid {brd}'>"
            f"<div style='font-size:9px;color:{t3};margin-bottom:6px'>"
            f"Measured · {runs} runs · {avg_tok:.0f} avg tokens</div>"
            f"<div style='display:grid;grid-template-columns:1fr 1fr 1fr;gap:5px'>"
        )
        for lbl, val, unit in [
            ("Energy",   avg_j,   "J"),
            ("mJ/token", avg_mj,  "mJ"),
            ("Duration", avg_dur, "s"),
        ]:
            html += (
                f"<div style='background:{bg3};border-radius:5px;padding:5px 4px;"
                f"text-align:center'>"
                f"<div style='font-size:9px;color:{t3}'>{lbl}</div>"
                f"<div style='font-size:11px;font-weight:500;color:{t1};"
                f"font-family:monospace'>{val:.3f}</div>"
                f"<div style='font-size:9px;color:{t3}'>{unit}</div>"
                f"</div>"
            )
        html += "</div></div>"
    else:
        html += (
            f"<div style='margin-top:8px;font-size:9px;color:{t3};"
            f"font-style:italic;padding-top:5px;border-top:0.5px solid {brd}'>"
            f"Not yet measured</div>"
        )

    html += "</div>"
    return html


# ── Full group card (cloud / local) ───────────────────────────────────────────
def _group_card(gkey: str, gcfg: dict, stats: dict, t: dict) -> str:
    bg1, bg2, bg3 = t["bg1"], t["bg2"], t["bg3"]
    t1, t2, t3    = t["t1"], t["t2"], t["t3"]
    brd           = t["brd"]
    clr  = {"cloud": "#38bdf8", "local": "#22c55e"}.get(gkey, "#7c3aed")
    icon = {"cloud": "☁",       "local": "💻"     }.get(gkey, "⚙")

    lin_spec  = gcfg.get("linear",  {})
    age_spec  = gcfg.get("agentic", {})
    lin_stats = _find_stat(stats, lin_spec,  "linear")
    age_stats = _find_stat(stats, age_spec, "agentic")

    html = (
        f"<div style='background:{bg1};border:0.5px solid {brd};"
        f"border-top:3px solid {clr};border-radius:10px;"
        f"padding:14px 15px;box-sizing:border-box'>"
        # header
        f"<div style='display:flex;align-items:center;gap:8px;margin-bottom:12px'>"
        f"<span style='font-size:18px'>{icon}</span>"
        f"<span style='font-size:13px;font-weight:600;color:{t1}'>"
        f"{gkey.title()} provider</span>"
        f"<span style='font-size:9px;padding:2px 8px;border-radius:100px;"
        f"background:{clr}18;color:{clr};border:0.5px solid {clr}40;"
        f"margin-left:auto'>{gkey}</span>"
        f"</div>"
    )
    html += _spec_block(lin_spec,  "linear",  lin_stats,  t)
    html += _spec_block(age_spec, "agentic", age_stats, t)

    # Tax multiple if both measured
    if lin_stats and age_stats:
        lj = lin_stats.get("avg_j") or 0
        aj = age_stats.get("avg_j") or 0
        if lj > 0:
            mult = aj / lj
            mc   = "#ef4444" if mult >= 2 else "#d97706" if mult >= 1.2 else "#16a34a"
            html += (
                f"<div style='background:{bg2};border-radius:7px;padding:10px 12px;"
                f"display:flex;justify-content:space-between;align-items:center;"
                f"margin-top:4px'>"
                f"<div>"
                f"<div style='font-size:9px;color:{t3};margin-bottom:2px'>Orchestration tax</div>"
                f"<div style='font-size:10px;color:{t2}'>agentic ÷ linear</div>"
                f"</div>"
                f"<span style='font-size:20px;font-weight:600;color:{mc};"
                f"font-family:monospace'>{mult:.2f}×</span>"
                f"</div>"
            )

    html += "</div>"
    return html


# ── Main render ───────────────────────────────────────────────────────────────
def render(ctx: dict = None):
    t    = _tok()
    bg1  = t["bg1"]; bg2 = t["bg2"]; bg3 = t["bg3"]
    t1   = t["t1"];  t2  = t["t2"];  t3  = t["t3"]
    brd  = t["brd"]; brd2 = t["brd2"]; accent = t["accent"]

    # ── Page header ───────────────────────────────────────────────────────────
    h_col, btn_col = st.columns([7, 1])
    with h_col:
        st.markdown(
            f"<div style='display:flex;align-items:baseline;gap:10px;margin-bottom:2px'>"
            f"<span style='font-size:20px;font-weight:600;color:{t1}'>🍎 Models</span>"
            f"<span style='font-size:12px;color:{t3}'>Apple-to-Apple energy comparison</span>"
            f"</div>",
            unsafe_allow_html=True)
    with btn_col:
        if st.button("↺ Reload", key="models_reload"):
            st.rerun()  # No cache — file is always re-read

    # ── Load config live ──────────────────────────────────────────────────────
    cfg = _load_models()

    if cfg.get("_missing"):
        st.markdown(
            f"<div style='background:{bg1};border:0.5px solid {brd};"
            f"border-left:3px solid #ef4444;border-radius:8px;"
            f"padding:14px 16px;margin:12px 0;font-size:11px;color:{t2}'>"
            f"<b style='color:#ef4444'>config/models.json not found.</b><br/>"
            f"<span style='color:{t3}'>Expected at: "
            f"<code style='background:{bg2};padding:1px 5px;border-radius:3px'>"
            f"{PROJECT_ROOT}/config/models.json</code></span>"
            f"</div>",
            unsafe_allow_html=True)
        return

    if cfg.get("_error"):
        st.error(f"Error reading models.json: {cfg['_error']}")
        return

    # Show config path + mod time
    cfg_path = PROJECT_ROOT / "config" / "models.json"
    _mtime = "—"
    try:
        import datetime as _dt
        _mtime = _dt.datetime.fromtimestamp(
            cfg_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass

    groups = [k for k in cfg
              if not k.startswith("_") and isinstance(cfg[k], dict)]

    st.markdown(
        f"<div style='font-size:10px;color:{t3};margin-bottom:14px'>"
        f"Live from "
        f"<code style='background:{bg2};padding:1px 5px;border-radius:3px;font-size:9px'>"
        f"config/models.json</code>"
        f" · modified {_mtime}"
        f" · {len(groups)} provider group{'s' if len(groups) != 1 else ''}: "
        f"<b style='color:{t2}'>{', '.join(groups)}</b></div>",
        unsafe_allow_html=True)

    # ── Model config cards — one per provider group ───────────────────────────
    stats = _load_stats()
    cols  = st.columns(max(len(groups), 1))
    for i, gkey in enumerate(groups):
        with cols[i]:
            st.markdown(_group_card(gkey, cfg[gkey], stats, t),
                        unsafe_allow_html=True)

    st.markdown(
        f"<div style='height:0.5px;background:{brd};margin:18px 0 14px'></div>",
        unsafe_allow_html=True)

    # ── Apple-to-Apple charts ─────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:500;color:{t2};"
        f"text-transform:uppercase;letter-spacing:.07em;margin-bottom:12px'>"
        f"Measured comparison — all runs</div>",
        unsafe_allow_html=True)

    _full_df = pd.DataFrame()
    try:
        _full_df, _ = q_safe("""
            SELECT e.model_name, e.provider, r.workflow_type, e.task_name,
                   COUNT(*)                                        AS runs,
                   ROUND(AVG(r.total_energy_uj)/1e6, 4)           AS avg_energy_j,
                   ROUND(AVG(r.dynamic_energy_uj)/1e6, 4)         AS avg_dynamic_j,
                   ROUND(AVG(r.duration_ns)/1e9, 3)               AS avg_duration_s,
                   ROUND(AVG(r.total_tokens), 1)                  AS avg_tokens,
                   ROUND(AVG(CASE WHEN r.total_tokens > 0
                       THEN r.total_energy_uj / r.total_tokens
                       END) / 1e3, 4)                             AS avg_mj_per_token,
                   ROUND(AVG(r.ipc), 3)                           AS avg_ipc,
                   ROUND(AVG(r.carbon_g)*1000, 4)                 AS avg_carbon_mg,
                   ROUND(AVG(r.water_ml), 4)                      AS avg_water_ml
            FROM runs r
            JOIN experiments e ON r.exp_id = e.exp_id
            WHERE e.model_name IS NOT NULL
            GROUP BY e.model_name, e.provider, r.workflow_type, e.task_name
            ORDER BY e.provider, e.model_name, r.workflow_type
        """)
    except Exception:
        pass

    if _full_df.empty:
        st.markdown(
            f"<div style='background:{bg1};border:0.5px solid {brd};"
            f"border-radius:8px;padding:20px;text-align:center;"
            f"color:{t3};font-size:12px'>"
            f"No run data yet — run experiments to see charts here.</div>",
            unsafe_allow_html=True)
        return

    # Task filter
    _, fc = st.columns([4, 1])
    with fc:
        _tasks = sorted(_full_df.task_name.dropna().unique().tolist())
        _sel   = st.selectbox("Task", ["all"] + _tasks, key="models_task")
    _filt = (_full_df if _sel == "all"
             else _full_df[_full_df.task_name == _sel]).copy()
    _filt["model_wf"] = (
        _filt["model_name"].astype(str) + " · " +
        _filt["workflow_type"].astype(str))

    _pl = dict(
        paper_bgcolor=bg1, plot_bgcolor=bg2,
        font=dict(color=t1, size=10),
        margin=dict(t=20, b=70, l=50, r=10),
        legend=dict(bgcolor=bg1, bordercolor=brd,
                    borderwidth=0.5, font=dict(color=t2)),
        height=260)

    for (ch_l, ch_r), (col_l, col_r), (lbl_l, lbl_r) in [
        (("avg_energy_j", "avg_mj_per_token"),
         st.columns(2),
         ("Energy (J) — model × workflow", "mJ / token — model × workflow")),
        (("avg_duration_s", "avg_carbon_mg"),
         st.columns(2),
         ("Avg duration (s)", "Carbon footprint (mg CO₂)")),
    ]:
        for col, metric, lbl in [(ch_l, col_l, lbl_l), (ch_r, col_r, lbl_r)]:
            pass  # loop structure — see below

    # Draw charts explicitly (avoid loop variable confusion)
    ch1, ch2 = st.columns(2)
    for col_ctx, metric, lbl in [
        (ch1, "avg_energy_j",    "Energy (J) — model × workflow"),
        (ch2, "avg_mj_per_token","mJ / token — model × workflow"),
    ]:
        with col_ctx:
            st.markdown(
                f"<div style='font-size:11px;font-weight:500;color:{t2};"
                f"margin-bottom:4px'>{lbl}</div>", unsafe_allow_html=True)
            fig = px.bar(
                _filt.groupby(["model_wf","workflow_type"])[metric]
                      .mean().reset_index(),
                x="model_wf", y=metric, color="workflow_type",
                barmode="group", color_discrete_map=WF_COLORS,
                labels={metric: lbl, "model_wf": ""})
            fig.update_xaxes(tickangle=30)
            fig.update_layout(**_pl)
            st.plotly_chart(fl(fig), use_container_width=True)

    ch3, ch4 = st.columns(2)
    for col_ctx, metric, lbl in [
        (ch3, "avg_duration_s", "Avg duration (s)"),
        (ch4, "avg_carbon_mg",  "Carbon footprint (mg CO₂)"),
    ]:
        with col_ctx:
            st.markdown(
                f"<div style='font-size:11px;font-weight:500;color:{t2};"
                f"margin-bottom:4px'>{lbl}</div>", unsafe_allow_html=True)
            fig = px.bar(
                _filt.groupby(["model_wf","workflow_type"])[metric]
                      .mean().reset_index(),
                x="model_wf", y=metric, color="workflow_type",
                barmode="group", color_discrete_map=WF_COLORS,
                labels={metric: lbl, "model_wf": ""})
            fig.update_xaxes(tickangle=30)
            fig.update_layout(**_pl)
            st.plotly_chart(fl(fig), use_container_width=True)

    # ── Full matrix ───────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='height:0.5px;background:{brd};margin:8px 0 12px'></div>",
        unsafe_allow_html=True)
    st.markdown(
        f"<div style='font-size:11px;font-weight:500;color:{t2};"
        f"margin-bottom:8px'>Full comparison matrix</div>",
        unsafe_allow_html=True)
    _show = [c for c in [
        "model_name","provider","workflow_type","task_name","runs",
        "avg_energy_j","avg_dynamic_j","avg_duration_s","avg_tokens",
        "avg_mj_per_token","avg_ipc","avg_carbon_mg","avg_water_ml"]
        if c in _filt.columns]
    st.dataframe(_filt[_show].round(4),
                 use_container_width=True, hide_index=True)
