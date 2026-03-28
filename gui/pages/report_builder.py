"""
gui/pages/report_builder.py
─────────────────────────────────────────────────────────────────────────────
REPORTS → Report Builder

Three tabs:
  ◈  Configure   — goal, filters, sections
  ◎  Preview     — what will be generated
  ⚡  Generate    — live progress + download outputs
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import uuid, json, traceback
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from gui.config import PL, DB_PATH, PROJECT_ROOT
from gui.report_engine import (
    GoalRegistry, ReportRunner, ReportConfig, ReportFilter,
    SectionConfig, ReportType, OutputFormat,
    get_available_filters, get_run_count,
)

# ── Configurable defaults ──────────────────────────────────────────────────────
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "reports" / "output"

SECTION_OPTIONS = [
    "title_page", "executive_summary", "goal_and_hypothesis",
    "system_profile", "experiment_setup", "results_table",
    "visualizations", "diagrams", "metrics_explanation",
    "hypothesis_verdict", "goal_analysis", "interpretation",
    "conclusion", "appendix",
]
SECTION_LABELS = {
    "title_page":          "Title Page",
    "executive_summary":   "Executive Summary",
    "goal_and_hypothesis": "Goal & Hypothesis",
    "system_profile":      "System Profile",
    "experiment_setup":    "Experiment Setup & Methodology",
    "results_table":       "Statistical Results Table",
    "visualizations":      "Visualisations (Charts)",
    "diagrams":            "Architecture Diagrams",
    "metrics_explanation": "Metrics Explanation (from docs)",
    "hypothesis_verdict":  "Hypothesis Verdict",
    "goal_analysis":       "Goal-Based Analysis",
    "interpretation":      "Interpretation",
    "conclusion":          "Conclusion",
    "appendix":            "Appendix",
}


def _header() -> None:
    st.markdown("""
    <div style="background:linear-gradient(90deg,#0f1520,#1a1f35);
                padding:1.5rem 1.8rem;border-radius:10px;
                border-left:4px solid #a78bfa;margin-bottom:1.5rem;">
      <h2 style="margin:0;color:#e8f0f8;font-family:'IBM Plex Mono',monospace;
                 font-size:1.25rem;">◎  Report Builder</h2>
      <p style="margin:.4rem 0 0;color:#7090b0;font-size:.82rem;
                font-family:'IBM Plex Mono',monospace;">
        Configure a research goal · select filters · generate publication-quality PDF + HTML
      </p>
    </div>
    """, unsafe_allow_html=True)


def _kpi_strip(run_count: int, goal_count: int, report_count: int) -> None:
    c1, c2, c3, c4 = st.columns(4)
    for col, val, label, color in [
        (c1, run_count,    "runs available",    "#22c55e"),
        (c2, goal_count,   "research goals",    "#a78bfa"),
        (c3, report_count, "reports generated", "#38bdf8"),
        (c4, "PDF + HTML", "output formats",    "#f59e0b"),
    ]:
        col.markdown(f"""
        <div style="background:#0d1828;border:1px solid #1e2d45;border-radius:8px;
                    padding:.9rem 1rem;text-align:center;">
          <div style="font-size:1.4rem;font-weight:600;color:{color};
                      font-family:'IBM Plex Mono',monospace;">{val}</div>
          <div style="font-size:.72rem;color:#7090b0;margin-top:.2rem;
                      font-family:'IBM Plex Mono',monospace;">{label}</div>
        </div>""", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom:1rem'></div>", unsafe_allow_html=True)


def _tab_configure(registry: GoalRegistry, avail_filters: dict) -> dict | None:
    st.markdown("#### ◈  Research Goal")

    goals = registry.list_goals()
    if not goals:
        st.error("No research goals loaded. Check `gui/report_engine/goals/` directory.")
        return None

    goal_options = {g.goal_id: f"{g.name}  [{g.category.value}]" for g in goals}
    selected_goal_id = st.selectbox(
        "Select goal",
        options=list(goal_options.keys()),
        format_func=lambda x: goal_options[x],
        key="rb_goal_id",
    )
    goal = registry.get_goal(selected_goal_id)

    if goal:
        st.markdown(f"""
        <div style="background:#0d1828;border-left:3px solid #a78bfa;
                    padding:.7rem 1rem;border-radius:0 6px 6px 0;
                    font-family:'IBM Plex Mono',monospace;font-size:.78rem;
                    color:#7090b0;margin:.5rem 0 .5rem;">
          {goal.description[:300]}{'…' if len(goal.description) > 300 else ''}
        </div>""", unsafe_allow_html=True)
        if goal.hypothesis:
            st.markdown(f"""
            <div style="background:#0d1828;border-left:3px solid #38bdf8;
                        padding:.6rem 1rem;border-radius:0 6px 6px 0;
                        font-family:'IBM Plex Mono',monospace;font-size:.78rem;
                        color:#c8d8e8;margin-bottom:1rem;">
              <span style="color:#38bdf8;font-size:.7rem;">HYPOTHESIS</span><br>
              {goal.hypothesis[:200]}{'…' if len(goal.hypothesis) > 200 else ''}
            </div>""", unsafe_allow_html=True)

    st.divider()
    st.markdown("#### ◈  Report Setup")
    col1, col2 = st.columns(2)
    with col1:
        report_title = st.text_input(
            "Report title",
            value=f"{goal.name if goal else 'Research'} Report — {datetime.now().strftime('%Y-%m-%d')}",
            key="rb_title",
        )
        report_type = st.selectbox(
            "Report type",
            options=["goal", "hypothesis", "problem", "exploratory"],
            format_func=lambda x: {
                "goal":        "Goal-Based — What are we optimising?",
                "hypothesis":  "Hypothesis — Is this assumption valid?",
                "problem":     "Problem — What is wrong?",
                "exploratory": "Exploratory — What insights emerge?",
            }[x],
            key="rb_report_type",
        )
    with col2:
        output_formats = st.multiselect(
            "Output formats",
            options=["pdf", "html", "json"],
            default=["pdf", "html"],
            key="rb_formats",
        )
        pdf_watermark = st.selectbox(
            "PDF watermark",
            options=["None", "DRAFT", "PRELIMINARY", "CONFIDENTIAL"],
            key="rb_watermark",
        )

    st.divider()
    st.markdown("#### ◈  Run Filters")
    col1, col2 = st.columns(2)
    with col1:
        wf_types = st.multiselect(
            "Workflow types",
            options=avail_filters.get("workflow_types", ["linear", "agentic"]),
            default=avail_filters.get("workflow_types", ["linear", "agentic"]),
            key="rb_wf_types",
        )
        providers = st.multiselect(
            "Providers (blank = all)",
            options=avail_filters.get("providers", []),
            key="rb_providers",
        )
        models = st.multiselect(
            "Models (blank = all)",
            options=avail_filters.get("models", []),
            key="rb_models",
        )
    with col2:
        task_names = st.multiselect(
            "Tasks (blank = all)",
            options=avail_filters.get("task_names", []),
            key="rb_tasks",
        )
        min_runs = st.number_input(
            "Minimum runs required",
            min_value=1, max_value=500, value=5,
            key="rb_min_runs",
        )
        exclude_tags = st.multiselect(
            "Exclude tagged runs",
            options=["exclude", "anomaly", "thermal-issue", "bad-baseline", "rerun-needed"],
            default=["exclude"],
            key="rb_exclude_tags",
        )

    # Live run count
    try:
        f = ReportFilter(
            workflow_types=wf_types or [],
            providers=providers or [],
            models=models or [],
            task_names=task_names or [],
            min_runs=int(min_runs),
            exclude_tags=exclude_tags or [],
        )
        count = get_run_count(str(DB_PATH), f)
        color = "#22c55e" if count >= min_runs else "#ef4444"
        st.markdown(
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:.8rem;"
            f"color:{color};margin:.5rem 0;'>● {count} runs match current filters</div>",
            unsafe_allow_html=True,
        )
    except Exception:
        pass

    st.divider()
    st.markdown("#### ◈  Report Sections")
    default_sections = (
        goal.report_sections if goal and goal.report_sections else SECTION_OPTIONS
    )
    selected_sections = []
    cols = st.columns(2)
    for i, sec_id in enumerate(SECTION_OPTIONS):
        with cols[i % 2]:
            checked = st.checkbox(
                SECTION_LABELS.get(sec_id, sec_id),
                value=(sec_id in default_sections),
                key=f"rb_sec_{sec_id}",
                disabled=(sec_id in ("title_page", "appendix")),
            )
            if checked:
                selected_sections.append(sec_id)

    for mandatory in ("title_page", "appendix"):
        if mandatory not in selected_sections:
            selected_sections.append(mandatory)

    return {
        "goal_id":        selected_goal_id,
        "report_title":   report_title,
        "report_type":    report_type,
        "output_formats": output_formats,
        "pdf_watermark":  None if pdf_watermark == "None" else pdf_watermark,
        "filters": {
            "workflow_types": wf_types or [],
            "providers":      providers or [],
            "models":         models or [],
            "task_names":     task_names or [],
            "min_runs":       int(min_runs),
            "exclude_tags":   exclude_tags or [],
        },
        "sections": selected_sections,
    }


def _tab_preview(cfg: dict | None, registry: GoalRegistry) -> None:
    if not cfg:
        st.info("Complete the Configure tab first.")
        return

    goal = registry.get_goal(cfg["goal_id"])
    if not goal:
        st.error("Goal not found.")
        return

    st.markdown("#### ◎  Report Preview")
    st.markdown(f"""
    <div style="background:#0d1828;border:1px solid #1e2d45;border-radius:10px;
                padding:1.2rem 1.4rem;font-family:'IBM Plex Mono',monospace;">
      <div style="font-size:1rem;color:#e8f0f8;margin-bottom:.6rem;">
        {cfg['report_title']}
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:.4rem;">
        <div style="font-size:.75rem;color:#7090b0;">Goal</div>
        <div style="font-size:.75rem;color:#c8d8e8;">{goal.name}</div>
        <div style="font-size:.75rem;color:#7090b0;">Type</div>
        <div style="font-size:.75rem;color:#c8d8e8;">{cfg['report_type'].capitalize()}</div>
        <div style="font-size:.75rem;color:#7090b0;">Formats</div>
        <div style="font-size:.75rem;color:#c8d8e8;">{', '.join(cfg['output_formats']).upper()}</div>
        <div style="font-size:.75rem;color:#7090b0;">Workflows</div>
        <div style="font-size:.75rem;color:#c8d8e8;">{', '.join(cfg['filters']['workflow_types']) or 'all'}</div>
        <div style="font-size:.75rem;color:#7090b0;">Stat test</div>
        <div style="font-size:.75rem;color:#c8d8e8;">{goal.eval_criteria.stat_test.value.replace('_','-').upper()}</div>
        <div style="font-size:.75rem;color:#7090b0;">α / CI</div>
        <div style="font-size:.75rem;color:#c8d8e8;">{goal.eval_criteria.alpha} / {goal.eval_criteria.ci_level*100:.0f}%</div>
      </div>
    </div>""", unsafe_allow_html=True)

    st.markdown("<br>**Sections included:**", unsafe_allow_html=True)
    cols = st.columns(2)
    for i, sec_id in enumerate(cfg["sections"]):
        cols[i % 2].markdown(
            f"<span style='color:#22c55e;font-size:.8rem;"
            f"font-family:IBM Plex Mono,monospace;'>✓  {SECTION_LABELS.get(sec_id, sec_id)}</span>",
            unsafe_allow_html=True,
        )

    if goal.metrics:
        st.markdown("<br>**Metrics to be evaluated:**", unsafe_allow_html=True)
        rows = [{"Metric": m.name, "Column": m.column, "Unit": m.unit,
                 "Direction": "↓ lower" if m.direction == "lower_is_better" else "↑ higher",
                 "Formula": m.formula or "—"} for m in goal.metrics]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _tab_generate(cfg: dict | None, registry: GoalRegistry) -> None:
    if not cfg:
        st.info("Complete the Configure tab first.")
        return

    st.markdown("#### ⚡  Generate Report")

    try:
        f = ReportFilter(**{k: v for k, v in cfg["filters"].items()})
        count = get_run_count(str(DB_PATH), f)
        if count < cfg["filters"]["min_runs"]:
            st.warning(
                f"⚠  Only {count} runs match filters "
                f"(minimum: {cfg['filters']['min_runs']}). "
                "Report will be generated with LOW confidence."
            )
        else:
            st.success(f"✓  {count} runs ready for analysis.")
    except Exception as e:
        st.warning(f"Could not pre-check run count: {e}")

    output_dir = st.text_input(
        "Output directory", value=str(DEFAULT_OUTPUT_DIR), key="rb_output_dir"
    )
    use_llm = st.toggle(
        "LLM-enhanced narrative", value=False, key="rb_use_llm",
        help="Uses Anthropic API to enrich narrative. Falls back to deterministic if unavailable.",
    )

    st.divider()

    if not st.button("◎  Generate Report", type="primary",
                     use_container_width=True, key="rb_generate_btn"):
        return

    # Build config
    report_id = str(uuid.uuid4())[:8]
    filters = ReportFilter(**{k: v for k, v in cfg["filters"].items()})
    sections = [SectionConfig(section_id=s, enabled=True) for s in cfg["sections"]]
    output_formats = [OutputFormat(f) for f in cfg["output_formats"]]

    config = ReportConfig(
        report_id=report_id,
        title=cfg["report_title"],
        goal_id=cfg["goal_id"],
        report_type=ReportType(cfg["report_type"]),
        filters=filters,
        sections=sections,
        output_formats=output_formats,
        pdf_watermark=cfg.get("pdf_watermark"),
        include_toc=True,
        interactive_charts=True,
    )

    # Live progress
    progress_bar = st.progress(0.0, text="Initialising…")
    status_box   = st.empty()
    log_lines: list[str] = []
    log_exp = st.expander("Generation log", expanded=False)

    def progress_cb(msg: str, pct: float) -> None:
        progress_bar.progress(min(pct, 1.0), text=msg)
        status_box.markdown(
            f"<span style='font-family:IBM Plex Mono,monospace;"
            f"font-size:.8rem;color:#7090b0;'>{msg}</span>",
            unsafe_allow_html=True,
        )
        log_lines.append(f"[{pct*100:4.0f}%]  {msg}")
        log_exp.text("\n".join(log_lines))

    try:
        runner = ReportRunner(
            db_path=str(DB_PATH),
            project_root=str(PROJECT_ROOT),
            output_dir=output_dir,
            use_llm=use_llm,
        )
        report_run = runner.generate(config, progress_cb=progress_cb)
        progress_bar.progress(1.0, text="Complete ✓")

        conf_color = {"HIGH": "#22c55e", "MEDIUM": "#f59e0b", "LOW": "#ef4444"}.get(
            report_run.confidence_level, "#7090b0"
        )
        verdict_color = {
            "SUPPORTED": "#22c55e", "REJECTED": "#ef4444",
            "INCONCLUSIVE": "#f59e0b", "INSUFFICIENT_DATA": "#7090b0",
        }.get(report_run.hypothesis_verdict, "#7090b0")

        st.markdown(f"""
        <div style="background:#0d1828;border:1px solid #22c55e;border-radius:10px;
                    padding:1.2rem 1.4rem;margin:1rem 0;
                    font-family:'IBM Plex Mono',monospace;">
          <div style="color:#22c55e;font-size:.9rem;margin-bottom:.6rem;">
            ✓  Report generated successfully
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:.4rem;">
            <div style="font-size:.75rem;color:#7090b0;">Report ID</div>
            <div style="font-size:.75rem;color:#c8d8e8;">{report_run.report_id}</div>
            <div style="font-size:.75rem;color:#7090b0;">Runs analysed</div>
            <div style="font-size:.75rem;color:#c8d8e8;">{report_run.run_count}</div>
            <div style="font-size:.75rem;color:#7090b0;">Confidence</div>
            <div style="font-size:.75rem;color:{conf_color};">{report_run.confidence_level}</div>
            <div style="font-size:.75rem;color:#7090b0;">Verdict</div>
            <div style="font-size:.75rem;color:{verdict_color};">{report_run.hypothesis_verdict}</div>
            <div style="font-size:.75rem;color:#7090b0;">Repro hash</div>
            <div style="font-size:.75rem;color:#c8d8e8;">{report_run.reproducibility_hash}</div>
          </div>
        </div>""", unsafe_allow_html=True)

        # Warn about any formats that failed silently
        requested_fmts = set(cfg.get("output_formats", []))
        failed_fmts = requested_fmts - set(report_run.output_paths.keys())
        for failed_fmt in sorted(failed_fmts):
            st.warning(
                f"⚠  {failed_fmt.upper()} generation failed — "
                "expand the Generation log above to see the error."
            )

        # Download buttons
        dl_cols = st.columns(max(1, len(report_run.output_paths)))
        for col, (fmt, path) in zip(dl_cols, report_run.output_paths.items()):
            p = Path(path)
            if p.exists():
                mime = ("application/pdf" if fmt == "pdf"
                        else "text/html" if fmt == "html" else "application/json")
                with open(p, "rb") as fh:
                    col.download_button(
                        label=f"⬇  Download {fmt.upper()}",
                        data=fh.read(),
                        file_name=p.name,
                        mime=mime,
                        use_container_width=True,
                    )

        st.session_state.setdefault("recent_report_ids", [])
        st.session_state["recent_report_ids"].insert(0, report_run.report_id)

    except Exception as e:
        progress_bar.progress(0.0, text="Failed")
        st.error(f"Report generation failed: {e}")
        st.code(traceback.format_exc(), language="python")


def render(ctx: dict) -> None:
    _header()

    try:
        registry = GoalRegistry.get()
        goals    = registry.list_goals()
    except Exception as e:
        st.error(f"Could not load Goal Registry: {e}")
        return

    try:
        avail_filters = get_available_filters(str(DB_PATH))
    except Exception:
        avail_filters = {"workflow_types": ["linear", "agentic"],
                         "providers": [], "models": [], "task_names": []}

    try:
        import sqlite3
        conn = sqlite3.connect(str(DB_PATH))
        report_count = conn.execute("SELECT COUNT(*) FROM report_runs").fetchone()[0]
        conn.close()
    except Exception:
        report_count = 0

    _kpi_strip(
        run_count=len(ctx.get("runs", pd.DataFrame())),
        goal_count=len(goals),
        report_count=report_count,
    )

    tab1, tab2, tab3 = st.tabs(["◈  Configure", "◎  Preview", "⚡  Generate"])

    with tab1:
        cfg = _tab_configure(registry, avail_filters)
        if cfg:
            st.session_state["rb_current_cfg"] = cfg

    with tab2:
        _tab_preview(st.session_state.get("rb_current_cfg"), registry)

    with tab3:
        _tab_generate(st.session_state.get("rb_current_cfg"), registry)

    st.markdown("---")
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:.72rem;"
        "color:#4a5568;text-align:center;'>"
        "A-LEMS Report Engine · same config + same data = identical reproducibility hash · "
        "Citations and references reserved for a future version"
        "</div>",
        unsafe_allow_html=True,
    )
