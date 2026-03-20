"""
gui/pages/research_insights.py  —  🔬  Research Insights
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

    st.title("🔬 Research Insights")
    st.caption("Guided analysis · Custom SQL Lab · Database schema — all in one place")

    # ── Load research_insights.yaml ────────────────────────────────────────────
    _riq_path = PROJECT_ROOT / "config" / "research_insights.yaml"
    _riq_data = []
    _riq_err  = None
    if _YAML_OK:
        try:
            import yaml as _yaml_ri
            if _riq_path.exists():
                _riq_data = _yaml_ri.safe_load(_riq_path.read_text()) or []
            else:
                _riq_err = f"File not found: {_riq_path}\nCreate it or save the template from Settings."
        except Exception as _ye:
            _riq_err = str(_ye)
    else:
        _riq_err = "PyYAML not installed — run: pip install pyyaml"

    tab_guided, tab_lab, tab_schema = st.tabs([
        "💡 Guided Insights",
        "🧪 Custom Query Lab",
        "📐 Schema & Data Model",
    ])

    # ══ TAB 1: GUIDED INSIGHTS ═════════════════════════════════════════════════
    with tab_guided:
        if _riq_err:
            st.warning(f"Cannot load research questions: {_riq_err}")
            st.info("Add `config/research_insights.yaml` to your project root. "
                    "A template is available in the Schema tab.")
        else:
            # Build tab → questions map
            _tabs_map = {}
            for _entry in _riq_data:
                _tname = _entry.get("tab","General")
                _tabs_map.setdefault(_tname, []).extend(_entry.get("questions",[]))

            if not _tabs_map:
                st.info("No questions defined yet in research_insights.yaml")
            else:
                _riq_tab_names = list(_tabs_map.keys())
                _riq_tabs = st.tabs(_riq_tab_names)

                for _ti, (_tname, _qtab) in enumerate(
                        zip(_riq_tab_names, _riq_tabs)):
                    with _qtab:
                        _q_opts = [q.get("question","?") for q in _qtab if isinstance(_qtab,list)] \
                                  if isinstance(_qtab, list) else \
                                  [q.get("question","?") for q in _tabs_map[_tname]]
                        _questions = _tabs_map[_tname]
                        _q_labels  = [q.get("question","?") for q in _questions]
                        _sel_q     = st.selectbox(
                            "Research question",
                            _q_labels,
                            key=f"riq_q_{_ti}",
                        )
                        _q_obj = next((q for q in _questions
                                       if q.get("question") == _sel_q), None)

                        if _q_obj and st.button("▶ Run Analysis",
                                                key=f"riq_run_{_ti}",
                                                type="primary"):
                            _sql = _q_obj.get("sql","").strip()
                            _disp = _q_obj.get("display",{})
                            _dtype = _disp.get("type","table")

                            _res, _rerr = q_safe(_sql)

                            if _rerr:
                                st.error(f"SQL error: {_rerr}")
                                with st.expander("SQL used"):
                                    st.code(_sql, language="sql")
                            elif _res.empty:
                                st.info("Query returned 0 rows.")
                            else:
                                # ── Insight Summary card ──────────────────────
                                _nrows = len(_res)
                                _ncols = len(_res.columns)
                                _num_cols = _res.select_dtypes("number").columns.tolist()
                                _insight_lines = [
                                    f"Query returned **{_nrows} rows** × {_ncols} columns.",
                                ]
                                if _num_cols:
                                    _best_col = _num_cols[0]
                                    _mn  = _res[_best_col].mean()
                                    _mx  = _res[_best_col].max()
                                    _mi  = _res[_best_col].min()
                                    _insight_lines.append(
                                        f"`{_best_col}`: mean={_mn:.4f}, "
                                        f"max={_mx:.4f}, min={_mi:.4f}")
                                st.markdown(
                                    "<div style='background:#0f1520;border:1px solid #1e2d45;"
                                    "border-radius:6px;padding:12px 16px;margin-bottom:12px;"
                                    "border-left:3px solid #22c55e;'>"
                                    "<div style='font-size:10px;font-weight:600;"
                                    "color:#22c55e;margin-bottom:6px;'>📊 Insight Summary</div>"
                                    + "".join(
                                        f"<div style='font-size:10px;color:#b8c8d8;'>{l}</div>"
                                        for l in _insight_lines
                                    ) + "</div>", unsafe_allow_html=True)

                                # ── Chart ─────────────────────────────────────
                                _xcol = _disp.get("x")
                                _ycol = _disp.get("y")
                                _ccol = _disp.get("color","workflow_type")

                                if _dtype in ("bar","grouped_bar") and _xcol and _ycol:
                                    if _xcol in _res.columns and _ycol in _res.columns:
                                        _kw = dict(barmode="group") if _ccol in _res.columns else {}
                                        _fg = px.bar(_res.dropna(subset=[_ycol]),
                                                     x=_xcol, y=_ycol,
                                                     color=_ccol if _ccol in _res.columns else None,
                                                     color_discrete_map=WF_COLORS,
                                                     **_kw)
                                        _fg.update_xaxes(tickangle=30)
                                        st.plotly_chart(fl(_fg), use_container_width=True)

                                elif _dtype == "scatter" and _xcol and _ycol:
                                    if _xcol in _res.columns and _ycol in _res.columns:
                                        _fg = px.scatter(_res.dropna(subset=[_xcol,_ycol]),
                                                         x=_xcol, y=_ycol,
                                                         color=_ccol if _ccol in _res.columns else None,
                                                         color_discrete_map=WF_COLORS,
                                                         trendline="lowess" if len(_res)>3 else None,
                                         trendline_options={"frac":0.5} if len(_res)>3 else {})
                                        st.plotly_chart(fl(_fg), use_container_width=True)

                                elif _dtype == "pie":
                                    _vn = _disp.get("value","total_j")
                                    _nn = _disp.get("names","phase")
                                    if _vn in _res.columns and _nn in _res.columns:
                                        _fg = px.pie(_res, values=_vn, names=_nn,
                                                     color_discrete_sequence=px.colors.sequential.Plasma_r)
                                        st.plotly_chart(fl(_fg), use_container_width=True)

                                elif _dtype == "stacked_bar":
                                    _segs = _disp.get("segments",[])
                                    _xc   = _disp.get("x","task_name")
                                    _segs_present = [s for s in _segs if s in _res.columns]
                                    if _segs_present and _xc in _res.columns:
                                        _fg = go.Figure()
                                        _seg_colors = ["#3b82f6","#38bdf8","#a78bfa",
                                                       "#22c55e","#f59e0b"]
                                        for _si, _sc in enumerate(_segs_present):
                                            _r,_g,_b = (
                                                int(_seg_colors[_si%5][1:3],16),
                                                int(_seg_colors[_si%5][3:5],16),
                                                int(_seg_colors[_si%5][5:7],16),
                                            )
                                            _fg.add_trace(go.Bar(
                                                name=_sc,
                                                x=_res[_xc],
                                                y=_res[_sc],
                                                marker_color=_seg_colors[_si%5],
                                            ))
                                        _fg.update_layout(barmode="stack", **PL)
                                        _fg.update_xaxes(tickangle=30)
                                        st.plotly_chart(_fg, use_container_width=True)

                                elif _dtype == "heatmap":
                                    _xc = _disp.get("x","provider")
                                    _yc = _disp.get("y","model_name")
                                    _vc = _disp.get("value","avg_mj_per_token")
                                    if all(c in _res.columns for c in [_xc,_yc,_vc]):
                                        _fg = px.density_heatmap(
                                            _res, x=_xc, y=_yc, z=_vc,
                                            color_continuous_scale="Blues")
                                        st.plotly_chart(fl(_fg), use_container_width=True)

                                # Note for ml_features
                                if _disp.get("note"):
                                    st.info(_disp["note"])

                                # ── Supporting data table ─────────────────────
                                st.markdown("**Supporting data**")
                                _hl_col = _disp.get("highlight_column")
                                st.dataframe(_res.round(4),
                                             use_container_width=True, hide_index=True)

                                # ── SQL expander ──────────────────────────────
                                with st.expander("SQL used", expanded=False):
                                    st.code(_sql, language="sql")

                                # ── Export ────────────────────────────────────
                                st.download_button(
                                    "⬇ Download CSV", data=_res.to_csv(index=False),
                                    file_name=f"insight_{_tname.replace(' ','_')}.csv",
                                    mime="text/csv", key=f"riq_dl_{_ti}")

    # ══ TAB 2: CUSTOM QUERY LAB ════════════════════════════════════════════════
    with tab_lab:
        # ── Pre-built presets (from old sql_query page — ALL PRESERVED) ────────
        QUERY_LIBRARY = {
            "— pick a preset —": "",
            "Energy by category": (
                "SELECT tc.category, r.workflow_type, COUNT(*) AS runs,\n"
                "  ROUND(AVG(r.total_energy_uj)/1e6,4) AS avg_energy_j,\n"
                "  ROUND(AVG(r.dynamic_energy_uj)/1e6,4) AS avg_dynamic_j\n"
                "FROM runs r\n"
                "JOIN experiments e ON r.exp_id=e.exp_id\n"
                "LEFT JOIN task_categories tc ON e.task_name=tc.task_id\n"
                "GROUP BY tc.category, r.workflow_type ORDER BY tc.category"
            ),
            "Tax breakdown by task": (
                "SELECT tc.category, e.task_name,\n"
                "  ROUND(AVG(ots.linear_dynamic_uj/1e6),4) AS linear_j,\n"
                "  ROUND(AVG(ots.agentic_dynamic_uj/1e6),4) AS agentic_j,\n"
                "  ROUND(AVG(ots.orchestration_tax_uj/1e6),4) AS tax_j,\n"
                "  ROUND(AVG(ots.tax_percent),2) AS tax_pct\n"
                "FROM orchestration_tax_summary ots\n"
                "JOIN runs rl ON ots.linear_run_id=rl.run_id\n"
                "JOIN experiments e ON rl.exp_id=e.exp_id\n"
                "LEFT JOIN task_categories tc ON e.task_name=tc.task_id\n"
                "GROUP BY tc.category, e.task_name"
            ),
            "Energy per token by model": (
                "SELECT e.model_name, e.provider,\n"
                "  ROUND(AVG(r.energy_per_token*1000),4) AS avg_mj_per_token,\n"
                "  COUNT(*) AS runs\n"
                "FROM runs r JOIN experiments e ON r.exp_id=e.exp_id\n"
                "WHERE r.total_tokens>0\n"
                "GROUP BY e.model_name, e.provider ORDER BY avg_mj_per_token"
            ),
            "Carbon by provider · region": (
                "SELECT e.provider, e.country_code,\n"
                "  ROUND(SUM(r.carbon_g)*1000,3) AS total_carbon_mg,\n"
                "  ROUND(SUM(r.water_ml),2) AS total_water_ml,\n"
                "  COUNT(*) AS runs\n"
                "FROM runs r JOIN experiments e ON r.exp_id=e.exp_id\n"
                "GROUP BY e.provider, e.country_code ORDER BY total_carbon_mg DESC"
            ),
            "Sample counts per run": (
                "SELECT r.run_id, r.workflow_type, e.task_name,\n"
                "  COUNT(DISTINCT es.sample_id) AS energy_samples,\n"
                "  COUNT(DISTINCT cs.sample_id) AS cpu_samples\n"
                "FROM runs r\n"
                "JOIN experiments e ON r.exp_id=e.exp_id\n"
                "LEFT JOIN energy_samples es ON r.run_id=es.run_id\n"
                "LEFT JOIN cpu_samples cs ON r.run_id=cs.run_id\n"
                "GROUP BY r.run_id ORDER BY r.run_id DESC LIMIT 20"
            ),
            "Recent runs": (
                "SELECT r.run_id, r.workflow_type, e.task_name, e.provider,\n"
                "  ROUND(r.total_energy_uj/1e6,4) AS energy_j,\n"
                "  ROUND(r.duration_ns/1e9,2) AS duration_s,\n"
                "  r.total_tokens, r.ipc\n"
                "FROM runs r JOIN experiments e ON r.exp_id=e.exp_id\n"
                "ORDER BY r.run_id DESC LIMIT 30"
            ),
            "Sustainability report": (
                "SELECT e.provider, tc.category,\n"
                "  ROUND(SUM(r.carbon_g),4) AS total_carbon_g,\n"
                "  ROUND(SUM(r.water_ml),2) AS total_water_ml,\n"
                "  COUNT(*) AS runs\n"
                "FROM runs r JOIN experiments e ON r.exp_id=e.exp_id\n"
                "LEFT JOIN task_categories tc ON e.task_name=tc.task_id\n"
                "GROUP BY e.provider, tc.category"
            ),
            "ML features (for prediction)": "SELECT * FROM ml_features LIMIT 200",
            "Orchestration analysis view":   "SELECT * FROM orchestration_analysis LIMIT 100",
            "Live experiment status": (
                "SELECT exp_id, name, status, model_name, provider, task_name,\n"
                "  runs_completed, runs_total, started_at, completed_at\n"
                "FROM experiments\n"
                "ORDER BY started_at DESC LIMIT 20"
            ),
        }

        _lab_preset = st.selectbox("Preset queries",
                                   list(QUERY_LIBRARY.keys()), key="lab_preset")
        _lab_default = QUERY_LIBRARY.get(_lab_preset, "")

        # Sync preset → text area via session_state so widget updates when preset changes
        _prev_preset = st.session_state.get("_lab_prev_preset", "")
        if _lab_preset != _prev_preset:
            st.session_state["_lab_prev_preset"] = _lab_preset
            if _lab_default:
                st.session_state["lab_sql"] = _lab_default

        _lab_sql = st.text_area(
            "SQL (SELECT only)",
            height=160, key="lab_sql",
            placeholder="SELECT * FROM runs LIMIT 10",
        )

        _lab_col1, _lab_col2 = st.columns([2, 1])
        with _lab_col1:
            _lab_run = st.button("▶ Run query", type="primary", key="lab_run")
        with _lab_col2:
            _lab_limit = st.number_input("Row limit", 10, 10000, 500,
                                         step=100, key="lab_limit")

        if _lab_run:
            _lsql = _lab_sql.strip()
            _upper = _lsql.upper()
            _bad = [kw for kw in ["DROP","DELETE","UPDATE","INSERT","ALTER",
                                   "CREATE","REPLACE","ATTACH"] if kw in _upper]
            if _bad:
                st.error(f"Blocked: {', '.join(_bad)}. SELECT only.")
            elif not _lsql:
                st.warning("Enter a SQL query.")
            else:
                if "LIMIT" not in _upper:
                    _lsql = f"SELECT * FROM ({_lsql}) _q LIMIT {int(_lab_limit)}"
                _lres, _lerr = q_safe(_lsql)
                if _lerr:
                    st.error(f"SQL Error: {_lerr}")
                elif _lres.empty:
                    st.info("Query returned 0 rows.")
                else:
                    st.success(f"✓ {len(_lres):,} rows · {len(_lres.columns)} columns")
                    st.dataframe(_lres, use_container_width=True, hide_index=True)

                    # ── Dynamic chart builder ─────────────────────────────────
                    st.divider()
                    st.markdown("**📈 Chart builder**")
                    _num_c = _lres.select_dtypes("number").columns.tolist()
                    _all_c = _lres.columns.tolist()
                    if _num_c:
                        _cb1, _cb2, _cb3, _cb4 = st.columns(4)
                        _ch_type = _cb1.selectbox("Chart type",
                            ["Bar","Scatter","Line","Histogram","Box"],
                            key="cb_type")
                        _ch_x = _cb2.selectbox("X axis", _all_c,  key="cb_x")
                        _ch_y = _cb3.selectbox("Y axis", _num_c,  key="cb_y")
                        _ch_c = _cb4.selectbox("Colour by",
                            ["(none)"] + [c for c in _all_c if c not in _num_c],
                            key="cb_c")
                        _clr_arg = _ch_c if _ch_c != "(none)" else None
                        _clr_map = WF_COLORS if _clr_arg == "workflow_type" else None

                        _chart_kwargs = dict(
                            x=_ch_x, y=_ch_y, color=_clr_arg,
                            color_discrete_map=_clr_map,
                        )
                        _chart_kwargs = {k:v for k,v in _chart_kwargs.items() if v is not None}

                        if _ch_type == "Bar":
                            _cfig = px.bar(_lres.dropna(subset=[_ch_y]), **_chart_kwargs)
                            _cfig.update_xaxes(tickangle=30)
                        elif _ch_type == "Scatter":
                            _cfig = px.scatter(_lres.dropna(subset=[_ch_x,_ch_y]), **_chart_kwargs)
                        elif _ch_type == "Line":
                            _cfig = px.line(_lres.dropna(subset=[_ch_y]), **_chart_kwargs)
                        elif _ch_type == "Histogram":
                            _cfig = px.histogram(_lres, x=_ch_x,
                                                  color=_clr_arg,
                                                  color_discrete_map=_clr_map)
                        else:  # Box
                            _cfig = px.box(_lres, x=_ch_x, y=_ch_y,
                                           color=_clr_arg,
                                           color_discrete_map=_clr_map)
                        st.plotly_chart(fl(_cfig), use_container_width=True)

                    # ── Export ────────────────────────────────────────────────
                    st.download_button(
                        "⬇ Download CSV",
                        data=_lres.to_csv(index=False),
                        file_name="alems_query.csv",
                        mime="text/csv", key="lab_dl")

    # ══ TAB 3: SCHEMA & DATA MODEL ═════════════════════════════════════════════
    with tab_schema:
        st.markdown("### Database Schema — A-LEMS")
        st.caption("Reference for writing SQL queries in the Custom Lab")

        _schema_html = """
<style>
.schema-table { width:100%; border-collapse:collapse; font-size:9px;
                font-family:'IBM Plex Mono',monospace; margin-bottom:16px; }
.schema-table th { background:#0f1520; color:#3b82f6; text-align:left;
                   padding:4px 8px; border-bottom:1px solid #1e2d45; }
.schema-table td { padding:3px 8px; color:#b8c8d8; border-bottom:1px solid #0d1825; }
.schema-table tr:hover td { background:#0f1520; }
.tname { font-size:11px; font-weight:700; color:#e8f0f8;
         margin:12px 0 4px; padding:4px 8px;
         background:#1e2d45; border-radius:4px; display:inline-block; }
.pk { color:#f59e0b; } .fk { color:#38bdf8; } .idx { color:#a78bfa; }
</style>
"""
        SCHEMA_TABLES = {
            "experiments": [
                ("exp_id","INTEGER PK","Experiment identifier"),
                ("name","TEXT","Auto-generated name"),
                ("workflow_type","TEXT","comparison | linear | agentic"),
                ("model_name","TEXT","e.g. Groq Llama 3.3 70B"),
                ("provider","TEXT","cloud | local"),
                ("task_name","TEXT","→ task_categories.task_id"),
                ("country_code","TEXT","Grid region for carbon calc"),
                ("status","TEXT","pending|running|completed|error"),
                ("runs_completed","INTEGER","Progress counter"),
                ("runs_total","INTEGER","Total planned runs"),
            ],
            "runs": [
                ("run_id","INTEGER PK","Run identifier"),
                ("exp_id","INTEGER FK","→ experiments.exp_id"),
                ("workflow_type","TEXT","linear | agentic"),
                ("run_number","INTEGER","Repetition index"),
                ("total_energy_uj","REAL","Cumulative µJ (total)"),
                ("dynamic_energy_uj","REAL","µJ above idle baseline"),
                ("duration_ns","INTEGER","Wall-clock nanoseconds"),
                ("total_tokens","INTEGER","prompt + completion"),
                ("ipc","REAL","Instructions per cycle"),
                ("cache_miss_rate","REAL","LLC miss rate 0–1"),
                ("carbon_g","REAL","CO₂ equivalent grams"),
                ("water_ml","REAL","Water consumed ml"),
                ("planning_time_ms","REAL","Planning phase ms"),
                ("execution_time_ms","REAL","Execution phase ms"),
                ("synthesis_time_ms","REAL","Synthesis phase ms"),
                ("llm_calls","INTEGER","LLM API calls"),
                ("tool_calls","INTEGER","Tool invocations"),
                ("governor","TEXT","CPU frequency governor"),
                ("thermal_delta_c","REAL","Temp rise during run"),
            ],
            "energy_samples": [
                ("sample_id","INTEGER PK","Sample identifier"),
                ("run_id","INTEGER FK","→ runs.run_id"),
                ("timestamp_ns","INTEGER","UNIX nanoseconds"),
                ("pkg_energy_uj","REAL","Package cumulative µJ"),
                ("core_energy_uj","REAL","Core cumulative µJ"),
                ("uncore_energy_uj","REAL","Uncore cumulative µJ"),
                ("dram_energy_uj","REAL","DRAM cumulative µJ"),
            ],
            "cpu_samples": [
                ("sample_id","INTEGER PK",""),
                ("run_id","INTEGER FK","→ runs.run_id"),
                ("timestamp_ns","INTEGER",""),
                ("cpu_util_percent","REAL","0–100%"),
                ("cpu_busy_mhz","REAL","Busy core MHz"),
                ("ipc","REAL","Instructions/cycle"),
                ("package_temp","REAL","°C"),
                ("c1_residency","REAL","%"),
                ("c6_residency","REAL","Deep sleep %"),
            ],
            "orchestration_events": [
                ("event_id","INTEGER PK",""),
                ("run_id","INTEGER FK","→ runs.run_id"),
                ("step_index","INTEGER","Step order"),
                ("phase","TEXT","planning|execution|synthesis|llm_wait"),
                ("event_type","TEXT","tool_call|llm_invoke|plan|etc"),
                ("start_time_ns","INTEGER",""),
                ("duration_ns","INTEGER",""),
                ("event_energy_uj","REAL","µJ for this event"),
                ("power_watts","REAL","Mean W during event"),
                ("tax_contribution_uj","REAL","Overhead µJ"),
            ],
            "orchestration_tax_summary": [
                ("comparison_id","INTEGER PK",""),
                ("linear_run_id","INTEGER FK","→ runs.run_id"),
                ("agentic_run_id","INTEGER FK","→ runs.run_id"),
                ("linear_dynamic_uj","REAL",""),
                ("agentic_dynamic_uj","REAL",""),
                ("orchestration_tax_uj","REAL","agentic−linear µJ"),
                ("tax_percent","REAL","% overhead"),
            ],
            "task_categories": [
                ("task_id","TEXT PK","Matches experiments.task_name"),
                ("category","TEXT","reasoning|coding|qa|etc"),
            ],
            "ml_features": [
                ("run_id","INTEGER FK","→ runs.run_id"),
                ("(many columns)","REAL","Pre-computed features for ML models"),
            ],
        }

        for _tname, _cols in SCHEMA_TABLES.items():
            st.markdown(f"<div class='tname'>📋 {_tname}</div>", unsafe_allow_html=True)
            _rows_html = "".join(
                f"<tr><td class='{'pk' if 'PK' in t else 'fk' if 'FK' in t else ''}'>{c}</td>"
                f"<td style='color:#3d5570'>{t}</td>"
                f"<td>{d}</td></tr>"
                for c, t, d in _cols
            )
            st.markdown(
                _schema_html +
                f"<table class='schema-table'>"
                f"<tr><th>Column</th><th>Type</th><th>Notes</th></tr>"
                f"{_rows_html}</table>",
                unsafe_allow_html=True)

        # ── YAML template download ─────────────────────────────────────────────
        st.divider()
        st.markdown("**research_insights.yaml — template**")
        st.caption(f"Save this to `{_riq_path}` to enable Guided Insights")
        _yaml_template = (PROJECT_ROOT / "config" / "research_insights.yaml").read_text() \
                         if _riq_path.exists() else \
                         "# Template not found — run the app once with the file in place"
        st.code(_yaml_template[:3000], language="yaml")
        st.download_button(
            "⬇ Download research_insights.yaml template",
            data=(_riq_path.read_text() if _riq_path.exists() else "# research_insights.yaml not found"),
            file_name="research_insights.yaml",
            mime="text/yaml",
            key="dl_riq_yaml",
        )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: SCHEMA DOCS  (lightweight reference page)
# ══════════════════════════════════════════════════════════════════════════════
