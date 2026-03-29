"""
gui/pages/goal_registry_page.py
─────────────────────────────────────────────────────────────────────────────
REPORTS → Goal Registry

Browse and manage all research goals. Three tabs:
  ⊟  All Goals    — card grid with category, metrics, stat config
  🔬  Inspect      — deep dive into a single goal
  ◈  New Goal     — YAML editor with validation + save to DB
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import json, sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import DB_PATH
from gui.report_engine.theme import make_layout, Colours, Margins, Sizes
from gui.report_engine import GoalRegistry, GoalCategory
from gui.report_engine.models import ResearchGoal

# ── Category colours ──────────────────────────────────────────────────────────
_CAT_COL = {
    "efficiency":   "#22c55e",
    "latency":      "#3b82f6",
    "cost":         "#f59e0b",
    "thermal":      "#ef4444",
    "quality":      "#a78bfa",
    "network":      "#38bdf8",
    "memory":       "#f472b6",
    "tokens":       "#fb923c",
    "carbon":       "#34d399",
    "comparative":  "#94a3b8",
    "custom":       "#7090b0",
}

_GOAL_TEMPLATE = """\
goal:
  goal_id: my_custom_goal
  name: My Custom Research Goal
  category: efficiency        # efficiency|latency|cost|thermal|quality|network|memory|tokens|carbon|comparative|custom
  version: "1.0.0"
  tags: [custom]
  description: >
    Describe what this goal measures and why it matters.
  hypothesis: >
    State the research hypothesis this goal will test. (optional)

  metrics:
    - name: Total Energy
      column: total_energy_uj
      unit: µJ
      direction: lower_is_better
      display_precision: 0
      description: Total package energy from RAPL sensors

    - name: Duration
      column: duration_ns
      unit: ms
      direction: lower_is_better
      formula: "duration_ns / 1e6"
      display_precision: 1

  thresholds:
    total_energy_uj:
      warn: 10000000
      severe: 50000000
      unit: µJ

  eval_criteria:
    stat_test: mann_whitney     # mann_whitney|t_test|bootstrap|wilcoxon
    alpha: 0.05
    effect_size: cohens_d
    min_runs_per_group: 5
    report_ci: true
    ci_level: 0.95
    comparison_mode: relative   # relative|absolute|baseline

  narrative_persona: research_engineer  # energy_researcher|systems_engineer|hardware_engineer|research_engineer
  doc_sections:
    - research/measurement-methodology.md
  diagram_ids:
    - data-flow

  report_sections:
    - title_page
    - executive_summary
    - goal_and_hypothesis
    - system_profile
    - experiment_setup
    - results_table
    - visualizations
    - hypothesis_verdict
    - goal_analysis
    - interpretation
    - conclusion
    - appendix
"""


# ── Header ─────────────────────────────────────────────────────────────────────

def _header() -> None:
    st.markdown("""
    <div style="background:linear-gradient(90deg,#0f1520,#1a1f35);
                padding:1.5rem 1.8rem;border-radius:10px;
                border-left:4px solid #a78bfa;margin-bottom:1.5rem;">
      <h2 style="margin:0;color:#e8f0f8;font-family:'IBM Plex Mono',monospace;
                 font-size:1.25rem;">⊟  Goal Registry</h2>
      <p style="margin:.4rem 0 0;color:#7090b0;font-size:.82rem;
                font-family:'IBM Plex Mono',monospace;">
        Define and manage research goals · each goal drives a full report pipeline
      </p>
    </div>
    """, unsafe_allow_html=True)


# ── KPI strip ──────────────────────────────────────────────────────────────────

def _kpi_strip(goals: list[ResearchGoal]) -> None:
    cats = set(g.category.value for g in goals)
    yaml_goals = sum(1 for g in goals)
    c1, c2, c3, c4 = st.columns(4)
    for col, val, label, color in [
        (c1, len(goals),   "total goals",       "#a78bfa"),
        (c2, len(cats),    "categories",         "#38bdf8"),
        (c3, yaml_goals,   "YAML-defined",       "#22c55e"),
        (c4, 10,           "built-in goals",     "#f59e0b"),
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


# ── Goal card ──────────────────────────────────────────────────────────────────

def _goal_card(goal: ResearchGoal, selected: bool = False) -> bool:
    """Render one goal card. Returns True if user clicked it."""
    cat_color = _CAT_COL.get(goal.category.value, "#7090b0")
    border = "#a78bfa" if selected else "#1e2d45"
    metric_names = [m.name for m in goal.metrics[:4]]
    metric_str   = " · ".join(metric_names)
    if len(goal.metrics) > 4:
        metric_str += f" +{len(goal.metrics)-4} more"

    st.markdown(f"""
    <div style="background:#0d1828;border:1px solid {border};border-radius:10px;
                padding:1rem 1.1rem;margin-bottom:.6rem;">
      <div style="display:flex;align-items:center;gap:.6rem;margin-bottom:.5rem;">
        <span style="background:{cat_color}22;color:{cat_color};
                     border:1px solid {cat_color};border-radius:10px;
                     padding:1px 8px;font-size:.68rem;
                     font-family:'IBM Plex Mono',monospace;">
          {goal.category.value}
        </span>
        <span style="font-size:.88rem;font-weight:600;color:#e8f0f8;
                     font-family:'IBM Plex Mono',monospace;">{goal.name}</span>
      </div>
      <div style="font-size:.74rem;color:#7090b0;
                  font-family:'IBM Plex Mono',monospace;margin-bottom:.4rem;
                  line-height:1.5;">
        {goal.description[:130]}{'…' if len(goal.description) > 130 else ''}
      </div>
      <div style="font-size:.68rem;color:#4a6080;
                  font-family:'IBM Plex Mono',monospace;">
        {len(goal.metrics)} metrics · {metric_str[:60]}
      </div>
    </div>""", unsafe_allow_html=True)

    return st.button(
        f"Inspect {goal.goal_id}",
        key=f"inspect_{goal.goal_id}",
        use_container_width=True,
    )


# ── Tab 1: All Goals ────────────────────────────────────────────────────────────

def _tab_all_goals(goals: list[ResearchGoal]) -> None:
    if not goals:
        st.info("No goals loaded. Check `gui/report_engine/goals/` directory.")
        return

    # Category filter
    all_cats = sorted(set(g.category.value for g in goals))
    sel_cat = st.selectbox(
        "Filter by category",
        ["(all)"] + all_cats,
        key="gr_cat_filter",
    )
    filtered = goals if sel_cat == "(all)" else [
        g for g in goals if g.category.value == sel_cat
    ]

    # Category distribution chart
    cat_counts: dict[str, int] = {}
    for g in goals:
        cat_counts[g.category.value] = cat_counts.get(g.category.value, 0) + 1

    fig = go.Figure(go.Bar(
        x=list(cat_counts.keys()),
        y=list(cat_counts.values()),
        marker_color=[_CAT_COL.get(c, "#7090b0") for c in cat_counts.keys()],
        text=list(cat_counts.values()),
        textposition="outside",
        textfont=dict(size=9, color="#7090b0"),
    ))
    fig.update_layout(
        **make_layout(),
        height=200,
        margin=dict(l=40, r=20, t=30, b=40),
    )

    st.plotly_chart(fig, use_container_width=True)

    st.markdown(f"**{len(filtered)} goals** {'(filtered)' if sel_cat != '(all)' else ''}")
    st.markdown("")

    # Two-column card grid
    col1, col2 = st.columns(2)
    for i, goal in enumerate(filtered):
        with (col1 if i % 2 == 0 else col2):
            clicked = _goal_card(goal)
            if clicked:
                st.session_state["gr_inspect_id"] = goal.goal_id
                st.rerun()


# ── Tab 2: Inspect ─────────────────────────────────────────────────────────────

def _tab_inspect(goals: list[ResearchGoal], registry: GoalRegistry) -> None:
    goal_ids = [g.goal_id for g in goals]
    if not goal_ids:
        st.info("No goals available.")
        return

    # Pre-select if navigated from All Goals tab
    default_idx = 0
    if "gr_inspect_id" in st.session_state:
        try:
            default_idx = goal_ids.index(st.session_state["gr_inspect_id"])
        except ValueError:
            pass

    sel_id = st.selectbox(
        "Select goal to inspect",
        options=goal_ids,
        index=default_idx,
        key="gr_inspect_select",
    )
    goal = registry.get_goal(sel_id)
    if not goal:
        st.error("Goal not found.")
        return

    cat_color = _CAT_COL.get(goal.category.value, "#7090b0")

    # Header card
    st.markdown(f"""
    <div style="background:#0d1828;border-left:4px solid {cat_color};
                border-radius:0 10px 10px 0;padding:1.1rem 1.3rem;
                margin-bottom:1rem;">
      <div style="font-size:1rem;color:#e8f0f8;
                  font-family:'IBM Plex Mono',monospace;">{goal.name}</div>
      <div style="font-size:.72rem;color:#7090b0;margin-top:.3rem;
                  font-family:'IBM Plex Mono',monospace;">
        ID: {goal.goal_id} · v{goal.version} ·
        Category: {goal.category.value} ·
        Persona: {goal.narrative_persona}
      </div>
    </div>""", unsafe_allow_html=True)

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Description**")
        st.markdown(
            f"<div style='font-size:.78rem;color:#c8d8e8;"
            f"font-family:IBM Plex Mono,monospace;line-height:1.6;"
            f"background:#0d1828;padding:.8rem;border-radius:6px;'>"
            f"{goal.description}</div>",
            unsafe_allow_html=True,
        )
        if goal.hypothesis:
            st.markdown("**Hypothesis**")
            st.markdown(
                f"<div style='font-size:.78rem;color:#38bdf8;"
                f"font-family:IBM Plex Mono,monospace;line-height:1.6;"
                f"background:#0d1828;padding:.8rem;border-radius:6px;"
                f"border-left:3px solid #38bdf8;'>"
                f"{goal.hypothesis}</div>",
                unsafe_allow_html=True,
            )

    with c2:
        st.markdown("**Statistical configuration**")
        ec = goal.eval_criteria
        config_rows = [
            ("Test",         ec.stat_test.value.replace("_", "-").upper()),
            ("Alpha (α)",    str(ec.alpha)),
            ("Effect size",  ec.effect_size_measure),
            ("Min runs/group", str(ec.min_runs_per_group)),
            ("CI level",     f"{ec.ci_level*100:.0f}%"),
            ("Comparison",   ec.comparison_mode),
        ]
        for k, v in config_rows:
            st.markdown(
                f"<div style='display:flex;justify-content:space-between;"
                f"font-family:IBM Plex Mono,monospace;font-size:.76rem;"
                f"padding:3px 0;border-bottom:1px solid #1e2d45;'>"
                f"<span style='color:#7090b0;'>{k}</span>"
                f"<span style='color:#c8d8e8;'>{v}</span></div>",
                unsafe_allow_html=True,
            )

    st.markdown("**Metrics**")
    if goal.metrics:
        rows = []
        for m in goal.metrics:
            rows.append({
                "Name":       m.name,
                "Column":     m.column,
                "Unit":       m.unit,
                "Direction":  "↓ lower" if m.direction == "lower_is_better" else "↑ higher",
                "Precision":  m.display_precision,
                "Formula":    m.formula or "—",
                "Description": m.description[:50] if m.description else "—",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # Thresholds
    if goal.thresholds:
        st.markdown("**Alert thresholds**")
        th_rows = [
            {"Metric": k, "Warn": v.warn, "Severe": v.severe, "Unit": v.unit}
            for k, v in goal.thresholds.items()
        ]
        st.dataframe(pd.DataFrame(th_rows), use_container_width=True, hide_index=True)

    # Assets
    st.markdown("**Documentation sections**")
    for ds in goal.doc_sections:
        st.markdown(
            f"<span style='font-size:.75rem;font-family:IBM Plex Mono,monospace;"
            f"color:#7090b0;'>📄 {ds}</span>",
            unsafe_allow_html=True,
        )
    st.markdown("**Diagram assets**")
    for d in goal.diagram_ids:
        st.markdown(
            f"<span style='font-size:.75rem;font-family:IBM Plex Mono,monospace;"
            f"color:#7090b0;'>🖼 {d}</span>",
            unsafe_allow_html=True,
        )

    # Quick-launch report
    st.markdown("")
    if st.button(
        f"◎ Generate Report for '{goal.name}'",
        type="primary",
        key=f"launch_{goal.goal_id}",
    ):
        st.session_state["rb_goal_id"] = goal.goal_id
        st.session_state["rb_current_cfg"] = {
            "goal_id":        goal.goal_id,
            "report_title":   f"{goal.name} — {datetime.now().strftime('%Y-%m-%d')}",
            "report_type":    "hypothesis" if goal.hypothesis else "goal",
            "output_formats": ["pdf", "html"],
            "pdf_watermark":  None,
            "filters":        {
                "workflow_types": ["linear", "agentic"],
                "providers": [], "models": [], "task_names": [],
                "min_runs": goal.eval_criteria.min_runs_per_group,
                "exclude_tags": ["exclude"],
            },
            "sections": goal.report_sections or [
                "title_page", "executive_summary", "goal_and_hypothesis",
                "system_profile", "experiment_setup", "results_table",
                "visualizations", "hypothesis_verdict", "goal_analysis",
                "interpretation", "conclusion", "appendix",
            ],
        }
        st.session_state["nav_page"] = "report_builder"
        st.rerun()


# ── Tab 3: New Goal ─────────────────────────────────────────────────────────────

def _tab_new_goal(registry: GoalRegistry) -> None:
    st.markdown("#### ◈  Define a New Research Goal")
    st.caption(
        "Write a YAML goal definition. Click Validate to check it, "
        "then Save to register it permanently in the DB."
    )

    yaml_input = st.text_area(
        "Goal YAML",
        value=st.session_state.get("gr_yaml_draft", _GOAL_TEMPLATE),
        height=500,
        key="gr_yaml_input",
        help="Follow the template exactly. All fields shown are required.",
    )
    st.session_state["gr_yaml_draft"] = yaml_input

    col1, col2, col3 = st.columns(3)

    if col1.button("✓ Validate", use_container_width=True, key="gr_validate"):
        result = registry.validate_yaml(yaml_input)
        if result["valid"]:
            st.success(f"✓ Valid — goal_id: `{result['goal_id']}`")
        else:
            for err in result["errors"]:
                st.error(f"✗ {err}")

    if col2.button("💾 Save to DB", use_container_width=True, key="gr_save"):
        result = registry.validate_yaml(yaml_input)
        if not result["valid"]:
            for err in result["errors"]:
                st.error(f"✗ {err}")
        else:
            try:
                goal = registry.get_goal(result["goal_id"])
                conn = sqlite3.connect(str(DB_PATH))
                conn.execute("""
                    INSERT OR REPLACE INTO research_goals (
                        goal_id, name, category, description, hypothesis,
                        metrics_json, thresholds_json, eval_criteria_json,
                        narrative_persona, doc_sections_json, diagram_ids_json,
                        report_sections_json, version, tags_json, source,
                        created_at, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    goal.goal_id, goal.name, goal.category.value,
                    goal.description, goal.hypothesis,
                    json.dumps([vars(m) for m in goal.metrics]),
                    json.dumps({k: vars(v) for k, v in goal.thresholds.items()}),
                    json.dumps(vars(goal.eval_criteria)),
                    goal.narrative_persona,
                    json.dumps(goal.doc_sections),
                    json.dumps(goal.diagram_ids),
                    json.dumps(goal.report_sections),
                    goal.version,
                    json.dumps(goal.tags),
                    "user_defined",
                    datetime.utcnow().isoformat(),
                    datetime.utcnow().isoformat(),
                ))
                conn.commit()
                conn.close()
                st.success(f"✓ Goal `{goal.goal_id}` saved to database.")
                st.session_state["gr_yaml_draft"] = _GOAL_TEMPLATE
                st.rerun()
            except Exception as e:
                st.error(f"Save failed: {e}")

    if col3.button("↺ Reset", use_container_width=True, key="gr_reset"):
        st.session_state["gr_yaml_draft"] = _GOAL_TEMPLATE
        st.rerun()


# ── Main render ────────────────────────────────────────────────────────────────

def render(ctx: dict) -> None:
    _header()

    try:
        registry = GoalRegistry.get()
        goals    = registry.list_goals()
    except Exception as e:
        st.error(f"Could not load Goal Registry: {e}")
        return

    _kpi_strip(goals)

    tab1, tab2, tab3 = st.tabs([
        "⊟  All Goals",
        "🔬  Inspect",
        "◈  New Goal",
    ])

    with tab1:
        _tab_all_goals(goals)

    with tab2:
        _tab_inspect(goals, registry)

    with tab3:
        _tab_new_goal(registry)

    st.markdown("---")
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:.72rem;"
        "color:#4a5568;text-align:center;'>"
        "Goal Registry · YAML goals loaded from gui/report_engine/goals/ · "
        "Custom goals saved to research_goals table · "
        "GoalRegistry.get() is a singleton — available everywhere in the GUI"
        "</div>",
        unsafe_allow_html=True,
    )
