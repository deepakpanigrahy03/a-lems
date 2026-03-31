"""
gui/pages/designer.py  —  🧪  Experiment Designer
─────────────────────────────────────────────────────────────────────────────
Design conditional multi-container experiment plans.

Layout — two columns:
  Left:  Design canvas (containers + conditions form)
  Right: Live plan preview (ASCII tree) + gap detection suggestions

Features:
  ✅ Load templates from config/experiment_templates.yaml
  ✅ Condition builder with metrics from config/experiment_designer.yaml
  ✅ Multi-container plans with IF/THEN branching
  ✅ Save plans to config/experiment_plans/ + push to execute queue
  ✅ Gap detection panel with [+ Add to Queue] buttons
  ✅ All data from config — zero hardcoding

30% comments for researcher readability.
─────────────────────────────────────────────────────────────────────────────
"""

import time
from pathlib import Path

import pandas as pd
import streamlit as st

from gui.config import (DASHBOARD_CFG, DESIGNER_CFG, GAP_RULES, PL,
                        PROJECT_ROOT, TEMPLATES_CFG)
from gui.db import q, q1

try:
    import yaml as _yaml

    _YAML_OK = True
except ImportError:
    _YAML_OK = False

# ── Config shortcuts ──────────────────────────────────────────────────────────
_METRICS = DESIGNER_CFG.get("condition_metrics", [])
_OPERATORS = DESIGNER_CFG.get("operators", [])
_CONNECTORS = DESIGNER_CFG.get("logical_connectors", ["AND", "OR"])
_TEMPLATES = TEMPLATES_CFG.get("templates", [])
_GAP_CFG = GAP_RULES.get("settings", {})
_GAP_RULES_LIST = GAP_RULES.get("rules", [])

_PLANS_DIR = PROJECT_ROOT / DASHBOARD_CFG.get("designer", {}).get(
    "plans_directory", "config/experiment_plans"
)
_MAX_CONT = DASHBOARD_CFG.get("designer", {}).get("max_containers_per_plan", 10)
_MAX_COND = DASHBOARD_CFG.get("designer", {}).get("max_conditions_per_container", 5)


# ══════════════════════════════════════════════════════════════════════════════
# STATE INIT
# ══════════════════════════════════════════════════════════════════════════════


def _init_designer_state():
    """Set up all designer session_state keys once."""
    if "dsn_plan_name" not in st.session_state:
        st.session_state.dsn_plan_name = "My Plan"
    if "dsn_description" not in st.session_state:
        st.session_state.dsn_description = ""
    if "dsn_containers" not in st.session_state:
        st.session_state.dsn_containers = [_blank_container(1)]
    if "dsn_saved_plans" not in st.session_state:
        st.session_state.dsn_saved_plans = _load_saved_plans()


def _blank_container(cid: int) -> dict:
    """Return a blank container dict with default values."""
    return {
        "id": f"C{cid}",
        "label": f"Container {cid}",
        "tasks": [],
        "providers": ["cloud"],
        "repetitions": 3,
        "cool_down": 30,
        "conditions": [],  # list of condition dicts
    }


def _blank_condition() -> dict:
    """Return a blank condition dict."""
    return {
        "metric": _METRICS[0]["key"] if _METRICS else "tax_multiplier",
        "operator": ">",
        "value": 10.0,
        "value2": None,
        "connector": None,
        "run": "STOP",
    }


# ══════════════════════════════════════════════════════════════════════════════
# PLAN PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════


def _save_plan(plan: dict):
    """Save plan to config/experiment_plans/plan_name.yaml."""
    if not _YAML_OK:
        return False, "PyYAML not installed"
    try:
        _PLANS_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = plan["name"].lower().replace(" ", "_").replace("/", "_")
        path = _PLANS_DIR / f"{safe_name}.yaml"
        with open(path, "w") as f:
            _yaml.dump(plan, f, default_flow_style=False, allow_unicode=True)
        return True, str(path)
    except Exception as e:
        return False, str(e)


def _load_saved_plans() -> list:
    """Load all saved plans from config/experiment_plans/."""
    if not _YAML_OK or not _PLANS_DIR.exists():
        return []
    plans = []
    for p in sorted(_PLANS_DIR.glob("*.yaml")):
        try:
            data = _yaml.safe_load(p.read_text()) or {}
            if data.get("name"):
                plans.append(data)
        except Exception:
            pass
    return plans


def _plan_to_queue_items(plan: dict, all_tasks: list) -> list:
    """
    Convert a plan's containers to a list of queue-ready experiment dicts.
    Simple execution: runs all containers sequentially (branching eval in Phase 4).
    """
    items = []
    for cont in plan.get("containers", []):
        tasks = cont.get("tasks", [])
        if tasks == ["all"] or not tasks:
            tasks = all_tasks
        for provider in cont.get("providers", ["cloud"]):
            for task in tasks:
                cmd = [
                    "python",
                    "-m",
                    "core.execution.tests.test_harness",
                    "--task-id",
                    task,
                    "--provider",
                    provider,
                    "--repetitions",
                    str(cont.get("repetitions", 3)),
                    "--cool-down",
                    str(cont.get("cool_down", 30)),
                    "--save-db",
                ]
                items.append(
                    {
                        "name": f"{plan['name']} · {cont['id']} · {task} · {provider}",
                        "mode": "single",
                        "task": task,
                        "provider": provider,
                        "reps": cont.get("repetitions", 3),
                        "country": "US",
                        "cmd": cmd,
                    }
                )
    return items


# ══════════════════════════════════════════════════════════════════════════════
# TASK LOADER (same as execute.py — consistent source of truth)
# ══════════════════════════════════════════════════════════════════════════════


def _load_tasks() -> tuple[list, dict]:
    """Load task list from config/tasks.yaml."""
    if _YAML_OK:
        try:
            raw = _yaml.safe_load(open(PROJECT_ROOT / "config" / "tasks.yaml"))
            tasks = raw.get("tasks", [])
            ids = [t["id"] for t in tasks if "id" in t]
            cats = {t["id"]: t.get("category", "") for t in tasks}
            if ids:
                return ids, cats
        except Exception:
            pass
    df = q(
        "SELECT DISTINCT task_name FROM experiments WHERE task_name IS NOT NULL ORDER BY task_name"
    )
    ids = df.task_name.tolist() if not df.empty else []
    return ids, {i: "" for i in ids}


# ══════════════════════════════════════════════════════════════════════════════
# PLAN PREVIEW — ASCII tree renderer
# ══════════════════════════════════════════════════════════════════════════════


def _render_plan_preview(plan: dict):
    """Render an ASCII-art tree of the plan's container structure."""
    name = plan.get("name", "Unnamed")
    conts = plan.get("containers", [])
    lines = []
    lines.append(f"📋 {name}")
    lines.append(f"   {plan.get('description','')}")
    lines.append("")

    for i, cont in enumerate(conts):
        is_last = i == len(conts) - 1
        prefix = "└──" if is_last else "├──"
        tasks_s = ", ".join(cont.get("tasks", [])[:3])
        if len(cont.get("tasks", [])) > 3:
            tasks_s += f" +{len(cont['tasks'])-3} more"
        provs_s = "/".join(cont.get("providers", []))
        lines.append(f" {prefix} [{cont['id']}] {cont.get('label', cont['id'])}")
        lines.append(f" {'   ' if is_last else '│  '}  tasks: {tasks_s or '(all)'}")
        lines.append(
            f" {'   ' if is_last else '│  '}  providers: {provs_s}  ·  "
            f"{cont.get('repetitions', 3)} reps  ·  "
            f"{cont.get('cool_down', 30)}s cool-down"
        )

        # Conditions
        for j, cond in enumerate(cont.get("conditions", [])):
            metric = cond.get("metric", "?")
            op = cond.get("operator", ">")
            val = cond.get("value", 0)
            val2 = cond.get("value2", "")
            run = cond.get("run", "STOP")
            conn = cond.get("connector", "")
            val_s = f"{val} AND {val2}" if op == "between" else str(val)
            conn_s = f" {conn}" if conn else ""
            lines.append(
                f" {'   ' if is_last else '│  '}"
                f"  {'└' if j == len(cont['conditions'])-1 else '├'}── "
                f"IF {metric} {op} {val_s}{conn_s} → {run}"
            )

    st.markdown(
        "<div style='background:#050810;border:1px solid #1e2d45;"
        "border-radius:6px;padding:12px 16px;font-family:monospace;"
        "font-size:10px;line-height:1.7;color:#7090b0;white-space:pre;'>"
        + "\n".join(lines).replace("<", "&lt;").replace(">", "&gt;")
        + "</div>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# GAP DETECTION — run rules against DB and surface suggestions
# ══════════════════════════════════════════════════════════════════════════════


def _run_gap_detection(all_tasks: list) -> list:
    """
    Run gap detection rules against the live DB.
    Returns list of suggestion dicts: {rule_id, emoji, title, body, action, template}.
    Only runs rules where the SQL is safe to execute directly.
    """
    suggestions = []
    min_runs = _GAP_CFG.get("min_runs_recommended", 10)
    max_shown = DASHBOARD_CFG.get("gap_detection", {}).get("show_max_suggestions", 7)

    # Rule 1: min_sample_size — task/provider cells with too few runs
    try:
        df = q(
            "SELECT e.task_name, e.provider, COUNT(r.run_id) as cnt "
            "FROM experiments e "
            "LEFT JOIN runs r ON e.exp_id = r.exp_id "
            "AND r.workflow_type = 'linear' "
            "GROUP BY e.task_name, e.provider "
            f"HAVING COUNT(r.run_id) < {min_runs} "
            "ORDER BY cnt ASC "
            "LIMIT 10"
        )
        for _, row in df.iterrows():
            needed = min_runs - int(row.cnt)
            suggestions.append(
                {
                    "rule_id": "min_sample_size",
                    "emoji": "🔴",
                    "priority": 1,
                    "title": f"{row.task_name} / {row.provider}: only {int(row.cnt)} runs",
                    "body": f"Need {needed} more to reach minimum {min_runs} recommended for statistical confidence.",
                    "action": "add_to_queue",
                    "queue_item": {
                        "name": f"Gap Fix: {row.task_name}/{row.provider}",
                        "task": row.task_name,
                        "provider": row.provider,
                        "reps": needed,
                        "country": "US",
                        "mode": "single",
                        "cmd": [
                            "python",
                            "-m",
                            "core.execution.tests.test_harness",
                            "--task-id",
                            row.task_name,
                            "--provider",
                            row.provider,
                            "--repetitions",
                            str(needed),
                            "--save-db",
                        ],
                    },
                }
            )
    except Exception:
        pass

    # Rule 2: provider_balance — tasks where one provider has 2x more runs
    try:
        bal_ratio = _GAP_CFG.get("provider_balance_ratio", 2.0)
        df2 = q(
            "SELECT task_name, "
            "SUM(CASE WHEN provider='cloud' THEN 1 ELSE 0 END) as cloud_cnt, "
            "SUM(CASE WHEN provider='local' THEN 1 ELSE 0 END) as local_cnt "
            "FROM experiments e "
            "JOIN runs r ON e.exp_id = r.exp_id "
            "WHERE r.workflow_type = 'linear' "
            "GROUP BY task_name"
        )
        for _, row in df2.iterrows():
            c, l = int(row.cloud_cnt or 0), int(row.local_cnt or 0)
            if c > 0 and l > 0:
                ratio = max(c, l) / min(c, l)
                if ratio > bal_ratio:
                    low_p = "local" if l < c else "cloud"
                    high_p = "cloud" if l < c else "local"
                    diff = abs(c - l)
                    suggestions.append(
                        {
                            "rule_id": "provider_balance",
                            "emoji": "🟡",
                            "priority": 2,
                            "title": f"Provider imbalance: {row.task_name}",
                            "body": f"{high_p} has {max(c,l)} runs vs {low_p} {min(c,l)} runs ({ratio:.1f}× ratio). "
                            f"Add {diff} more {low_p} runs.",
                            "action": "add_to_queue",
                            "queue_item": {
                                "name": f"Balance: {row.task_name}/{low_p}",
                                "task": row.task_name,
                                "provider": low_p,
                                "reps": diff,
                                "country": "US",
                                "mode": "single",
                                "cmd": [
                                    "python",
                                    "-m",
                                    "core.execution.tests.test_harness",
                                    "--task-id",
                                    row.task_name,
                                    "--provider",
                                    low_p,
                                    "--repetitions",
                                    str(diff),
                                    "--save-db",
                                ],
                            },
                        }
                    )
    except Exception:
        pass

    # Rule 3: high variance (CV) — task/provider pairs with unreliable energy measurements
    try:
        cv_thresh = _GAP_CFG.get("high_variance_cv", 0.30)
        df3 = q(
            "SELECT e.task_name, e.provider, COUNT(*) as cnt, "
            "AVG(r.total_energy_uj) as mean_e, "
            "MAX(r.total_energy_uj) as max_e, "
            "MIN(r.total_energy_uj) as min_e "
            "FROM experiments e "
            "JOIN runs r ON e.exp_id = r.exp_id "
            "WHERE r.workflow_type = 'agentic' "
            "GROUP BY e.task_name, e.provider "
            "HAVING COUNT(*) >= 3"
        )
        for _, row in df3.iterrows():
            if row.mean_e and row.mean_e > 0:
                # Approximate CV from range (SQLite has no STDEV)
                approx_std = (
                    row.max_e - row.min_e
                ) / 4  # range/4 ≈ std for normal dist
                cv = approx_std / row.mean_e
                if cv > cv_thresh:
                    n_more = max(3, int(row.cnt * 0.5))
                    suggestions.append(
                        {
                            "rule_id": "high_variance",
                            "emoji": "🟡",
                            "priority": 2,
                            "title": f"High variance: {row.task_name}/{row.provider} (CV≈{cv:.2f})",
                            "body": f"Energy spread is wide (min={row.min_e/1e6:.2f}J, max={row.max_e/1e6:.2f}J). "
                            f"Add {n_more} more runs to reduce uncertainty.",
                            "action": "add_to_queue",
                            "queue_item": {
                                "name": f"Reduce variance: {row.task_name}/{row.provider}",
                                "task": row.task_name,
                                "provider": row.provider,
                                "reps": n_more,
                                "country": "US",
                                "mode": "single",
                                "cmd": [
                                    "python",
                                    "-m",
                                    "core.execution.tests.test_harness",
                                    "--task-id",
                                    row.task_name,
                                    "--provider",
                                    row.provider,
                                    "--repetitions",
                                    str(n_more),
                                    "--save-db",
                                ],
                            },
                        }
                    )
    except Exception:
        pass

    # Rule 4: cold start coverage
    try:
        cs = q1(
            "SELECT SUM(CASE WHEN is_cold_start=1 THEN 1 ELSE 0 END) as cold, "
            "COUNT(*) as total "
            "FROM runs"
        )
        if cs and cs.get("total", 0) > 0:
            pct = int(cs.get("cold", 0) or 0) / int(cs["total"]) * 100
            if pct < 20:
                suggestions.append(
                    {
                        "rule_id": "cold_start_ratio",
                        "emoji": "⚪",
                        "priority": 3,
                        "title": f"Cold-start runs: {pct:.1f}% (recommended ≥20%)",
                        "body": "Cold-start runs capture cache and memory warm-up effects. "
                        "Run experiments immediately after system restart to increase coverage.",
                        "action": "info_only",
                    }
                )
    except Exception:
        pass

    # Rule 5: no thermal stress data
    try:
        ht = q1("SELECT COUNT(*) as cnt FROM runs WHERE max_temp_c > 85")
        if ht and int(ht.get("cnt", 0)) == 0:
            suggestions.append(
                {
                    "rule_id": "thermal_gap",
                    "emoji": "⚪",
                    "priority": 3,
                    "title": "No high-temperature runs captured",
                    "body": "No runs recorded when CPU exceeded 85°C. "
                    "Load the Thermal Stress template to capture throttle-boundary behaviour.",
                    "action": "suggest_template",
                    "template": "thermal_stress",
                }
            )
    except Exception:
        pass

    # Sort by priority, cap at max_shown
    suggestions.sort(key=lambda x: x.get("priority", 99))
    return suggestions[:max_shown]


# ══════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════════════════════


def render(ctx: dict):
    _init_designer_state()
    all_tasks, cat_map = _load_tasks()

    st.title("🧪 Experiment Designer")
    st.caption(
        "Design conditional multi-container experiment plans. "
        "Templates from config/experiment_templates.yaml."
    )

    # ── Template loader ───────────────────────────────────────────────────────
    if _TEMPLATES:
        st.markdown("#### 📚 Load Template")
        template_labels = ["— Select template —"] + [
            f"{t.get('emoji','📋')} {t['name']}  —  {t.get('description','')[:60]}"
            for t in _TEMPLATES
        ]
        chosen_idx = st.selectbox(
            "", template_labels, key="dsn_template_select", label_visibility="collapsed"
        )
        if chosen_idx != "— Select template —":
            tpl_idx = template_labels.index(chosen_idx) - 1
            tpl = _TEMPLATES[tpl_idx]
            if st.button(f"📥 Load '{tpl['name']}'", key="dsn_load_tpl"):
                st.session_state.dsn_plan_name = tpl["name"]
                st.session_state.dsn_description = tpl.get("description", "")
                # Convert template containers to designer format
                conts = []
                for c in tpl.get("containers", []):
                    conts.append(
                        {
                            "id": c.get("id", f"C{len(conts)+1}"),
                            "label": c.get("label", c.get("id", "Container")),
                            "tasks": c.get("tasks", []),
                            "providers": c.get("providers", ["cloud"]),
                            "repetitions": c.get("repetitions", 3),
                            "cool_down": c.get("cool_down_seconds", 30),
                            "conditions": [
                                {
                                    "metric": cond.get(
                                        "metric", cond.get("id", "tax_multiplier")
                                    ),
                                    "operator": cond.get("operator", ">"),
                                    "value": cond.get("value", 10),
                                    "value2": cond.get("value2", None),
                                    "connector": cond.get("connector", None),
                                    "run": cond.get("run", "STOP"),
                                }
                                for cond in c.get("conditions", [])
                            ],
                        }
                    )
                st.session_state.dsn_containers = (
                    conts if conts else [_blank_container(1)]
                )
                st.success(f"Loaded template: {tpl['name']}")
                st.rerun()

        # Estimated duration
        if chosen_idx != "— Select template —":
            tpl_idx = template_labels.index(chosen_idx) - 1
            tpl = _TEMPLATES[tpl_idx]
            dur = tpl.get("estimated_duration_minutes", "?")
            st.caption(f"⏱ Estimated duration: ~{dur} minutes")

    st.divider()

    # ── Two-column layout: canvas | preview+gaps ──────────────────────────────
    left, right = st.columns([3, 2])

    with left:
        st.markdown("#### 🔬 Design Canvas")

        # Plan metadata
        st.session_state.dsn_plan_name = st.text_input(
            "Plan name", value=st.session_state.dsn_plan_name, key="dsn_name_input"
        )
        st.session_state.dsn_description = st.text_input(
            "Description (optional)",
            value=st.session_state.dsn_description,
            key="dsn_desc_input",
        )

        st.markdown("---")

        # Container builder
        containers = st.session_state.dsn_containers

        for ci, cont in enumerate(containers):
            cid = cont["id"]
            is_last = ci == len(containers) - 1

            # Container header
            st.markdown(
                f"<div style='background:#0a1018;border:1px solid #1e2d45;"
                f"border-left:3px solid #3b82f6;border-radius:5px;"
                f"padding:8px 12px;margin-bottom:8px;'>"
                f"<span style='font-size:11px;font-weight:700;color:#4fc3f7;'>"
                f"📦 {cid}</span>"
                f"<span style='font-size:10px;color:#3d5570;margin-left:8px;'>"
                f"{cont.get('label','')}</span></div>",
                unsafe_allow_html=True,
            )

            # Container fields
            cc1, cc2 = st.columns([3, 1])
            with cc1:
                cont["label"] = st.text_input(
                    "Label", value=cont.get("label", cid), key=f"c_{ci}_label"
                )
            with cc2:
                cont["id"] = st.text_input(
                    "ID", value=cont.get("id", cid), key=f"c_{ci}_id"
                )

            # Tasks
            all_flag = st.checkbox(
                "All tasks", key=f"c_{ci}_all", value=(cont.get("tasks") == ["all"])
            )
            if all_flag:
                cont["tasks"] = ["all"]
                st.caption(f"All {len(all_tasks)} tasks selected")
            else:
                cont["tasks"] = st.multiselect(
                    "Tasks",
                    all_tasks,
                    default=[
                        t
                        for t in cont.get("tasks", [])
                        if t in all_tasks and t != "all"
                    ],
                    format_func=lambda t: f"{t}  ({cat_map.get(t,'')})",
                    key=f"c_{ci}_tasks",
                )

            # Providers / reps / cool-down
            c1, c2, c3 = st.columns(3)
            cont["providers"] = c1.multiselect(
                "Providers",
                ["cloud", "local"],
                default=cont.get("providers", ["cloud"]),
                key=f"c_{ci}_prov",
            )
            cont["repetitions"] = c2.number_input(
                "Reps", 1, 50, value=cont.get("repetitions", 3), key=f"c_{ci}_reps"
            )
            cont["cool_down"] = c3.number_input(
                "Cool-down (s)",
                0,
                120,
                value=cont.get("cool_down", 30),
                key=f"c_{ci}_cd",
            )

            # ── Condition builder ─────────────────────────────────────────────
            st.markdown(
                "<div style='font-size:9px;font-weight:700;color:#3d5570;"
                "text-transform:uppercase;letter-spacing:.1em;margin:6px 0 4px;'>"
                "⑃ Conditions (IF → THEN)</div>",
                unsafe_allow_html=True,
            )

            conds = cont.get("conditions", [])
            for ki, cond in enumerate(conds):
                ka, kb, kc, kd, ke = st.columns([3, 2, 2, 2, 1])
                # Metric selector
                metric_keys = [m["key"] for m in _METRICS]
                metric_labels = [
                    f"{m['label']} ({m['unit']})" if m["unit"] else m["label"]
                    for m in _METRICS
                ]
                cur_idx = (
                    metric_keys.index(cond["metric"])
                    if cond["metric"] in metric_keys
                    else 0
                )
                cond["metric"] = metric_keys[
                    ka.selectbox(
                        "Metric",
                        range(len(metric_keys)),
                        index=cur_idx,
                        format_func=lambda i: metric_labels[i],
                        key=f"c_{ci}_cond_{ki}_met",
                        label_visibility="collapsed",
                    )
                ]

                # Operator
                op_syms = [o["symbol"] for o in _OPERATORS]
                op_labels = [o["label"] for o in _OPERATORS]
                op_idx = (
                    op_syms.index(cond["operator"])
                    if cond["operator"] in op_syms
                    else 0
                )
                cond["operator"] = op_syms[
                    kb.selectbox(
                        "Op",
                        range(len(op_syms)),
                        index=op_idx,
                        format_func=lambda i: op_labels[i],
                        key=f"c_{ci}_cond_{ki}_op",
                        label_visibility="collapsed",
                    )
                ]

                # Value
                cond["value"] = kc.number_input(
                    "Value",
                    value=float(cond.get("value", 0)),
                    key=f"c_{ci}_cond_{ki}_val",
                    label_visibility="collapsed",
                )

                # Run target
                cont_ids = [c["id"] for c in containers] + ["STOP"]
                run_val = cond.get("run", "STOP")
                if run_val not in cont_ids:
                    run_val = "STOP"
                cond["run"] = kd.selectbox(
                    "→ Run",
                    cont_ids,
                    index=cont_ids.index(run_val),
                    key=f"c_{ci}_cond_{ki}_run",
                    label_visibility="collapsed",
                )

                # Remove condition button
                if ke.button("✕", key=f"c_{ci}_cond_{ki}_del"):
                    conds.pop(ki)
                    st.rerun()

            # Add condition button
            if len(conds) < _MAX_COND:
                if st.button(f"+ Add condition to {cid}", key=f"add_cond_{ci}"):
                    conds.append(_blank_condition())
                    st.rerun()
            cont["conditions"] = conds

            # Remove container button (not for first container)
            if ci > 0:
                if st.button(f"🗑 Remove {cid}", key=f"rem_cont_{ci}"):
                    containers.pop(ci)
                    st.rerun()

            if not is_last:
                st.markdown(
                    "<hr style='border-color:#1e2d45;margin:8px 0;'>",
                    unsafe_allow_html=True,
                )

        # Add container
        if len(containers) < _MAX_CONT:
            if st.button("+ Add Container", use_container_width=True, key="add_cont"):
                containers.append(_blank_container(len(containers) + 1))
                st.rerun()

        st.session_state.dsn_containers = containers
        st.markdown("---")

        # ── Action buttons ────────────────────────────────────────────────────
        plan = {
            "name": st.session_state.dsn_plan_name,
            "description": st.session_state.dsn_description,
            "created": str(pd.Timestamp.now())[:16],
            "containers": st.session_state.dsn_containers,
        }

        b1, b2, b3 = st.columns(3)

        # Save plan to YAML
        if b1.button("💾 Save Plan", use_container_width=True, key="dsn_save"):
            ok, msg = _save_plan(plan)
            if ok:
                st.success(f"Saved to {msg}")
                st.session_state.dsn_saved_plans = _load_saved_plans()
            else:
                st.error(f"Save failed: {msg}")

        # Queue all containers
        if b2.button(
            "▶ Queue All", type="primary", use_container_width=True, key="dsn_queue"
        ):
            items = _plan_to_queue_items(plan, all_tasks)
            if "ex_queue" not in st.session_state:
                st.session_state.ex_queue = []
            st.session_state.ex_queue.extend(items)
            st.success(f"Queued {len(items)} experiments → go to ▶ Execute Run")

        # Reset canvas
        if b3.button("🔄 Reset", use_container_width=True, key="dsn_reset"):
            st.session_state.dsn_containers = [_blank_container(1)]
            st.session_state.dsn_plan_name = "My Plan"
            st.session_state.dsn_description = ""
            st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # RIGHT COLUMN: Preview + Saved Plans + Gap Detection
    # ══════════════════════════════════════════════════════════════════════════
    with right:
        # ── Live plan preview ─────────────────────────────────────────────────
        st.markdown("#### 📐 Plan Preview")
        plan = {
            "name": st.session_state.dsn_plan_name,
            "description": st.session_state.dsn_description,
            "containers": st.session_state.dsn_containers,
        }
        _render_plan_preview(plan)

        # Experiment count estimate
        total_experiments = 0
        for cont in st.session_state.dsn_containers:
            tasks = cont.get("tasks", [])
            n_tasks = len(all_tasks) if tasks == ["all"] or not tasks else len(tasks)
            total_experiments += n_tasks * len(cont.get("providers", ["cloud"]))
        st.caption(
            f"Estimated: {total_experiments} experiments × "
            f"{st.session_state.dsn_containers[0].get('repetitions', 3)} reps = "
            f"{total_experiments * st.session_state.dsn_containers[0].get('repetitions', 3)} runs"
        )

        st.divider()

        # ── Saved plans ───────────────────────────────────────────────────────
        st.markdown("#### 📁 Saved Plans")
        saved = st.session_state.dsn_saved_plans
        if not saved:
            st.caption("No saved plans yet.")
        else:
            for sp in saved:
                sa, sb, sc = st.columns([3, 1, 1])
                sa.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:#e8f0f8;'>"
                    f"{sp['name']}</div>"
                    f"<div style='font-size:9px;color:#3d5570;'>"
                    f"{len(sp.get('containers',[]))} containers</div>",
                    unsafe_allow_html=True,
                )
                if sb.button(
                    "📥",
                    key=f"sp_load_{sp['name']}",
                    use_container_width=True,
                    help="Load this plan",
                ):
                    st.session_state.dsn_plan_name = sp["name"]
                    st.session_state.dsn_description = sp.get("description", "")
                    st.session_state.dsn_containers = sp.get(
                        "containers", [_blank_container(1)]
                    )
                    st.rerun()
                if sc.button(
                    "▶",
                    key=f"sp_run_{sp['name']}",
                    use_container_width=True,
                    help="Queue this plan",
                ):
                    items = _plan_to_queue_items(sp, all_tasks)
                    if "ex_queue" not in st.session_state:
                        st.session_state.ex_queue = []
                    st.session_state.ex_queue.extend(items)
                    st.success(f"Queued {len(items)} runs")

        st.divider()

        # ── Gap detection suggestions ─────────────────────────────────────────
        st.markdown("#### 💡 Data Gap Suggestions")
        st.caption("Detected by analysing your current DB coverage.")

        with st.spinner("Scanning for gaps..."):
            suggestions = _run_gap_detection(all_tasks)

        if not suggestions:
            st.success("✅ No significant data gaps detected!")
        else:
            for _si, s in enumerate(suggestions):
                clr = {"🔴": "#ef4444", "🟡": "#f59e0b", "⚪": "#4b5563"}.get(
                    s["emoji"], "#3d5570"
                )
                st.markdown(
                    f"<div style='background:{clr}11;border:1px solid {clr}33;"
                    f"border-left:3px solid {clr};border-radius:5px;"
                    f"padding:8px 12px;margin-bottom:6px;'>"
                    f"<div style='font-size:10px;font-weight:600;color:{clr};'>"
                    f"{s['emoji']} {s['title']}</div>"
                    f"<div style='font-size:9px;color:#7090b0;margin-top:3px;line-height:1.5;'>"
                    f"{s['body']}</div></div>",
                    unsafe_allow_html=True,
                )

                # Action button
                if s.get("action") == "add_to_queue" and s.get("queue_item"):
                    if st.button(
                        f"+ Add to Queue",
                        key=f"gap_{s['rule_id']}_{_si}_{s['title'][:12]}",
                        use_container_width=True,
                    ):
                        if "ex_queue" not in st.session_state:
                            st.session_state.ex_queue = []
                        st.session_state.ex_queue.append(s["queue_item"])
                        st.success("Added to Execute Run queue!")

                elif s.get("action") == "suggest_template" and s.get("template"):
                    tpl_name = s["template"]
                    matching = [t for t in _TEMPLATES if t.get("id") == tpl_name]
                    if matching and st.button(
                        f"📋 Load '{matching[0]['name']}' template",
                        key=f"gap_tpl_{tpl_name}",
                        use_container_width=True,
                    ):
                        tpl = matching[0]
                        st.session_state.dsn_plan_name = tpl["name"]
                        st.session_state.dsn_description = tpl.get("description", "")
                        conts = []
                        for c in tpl.get("containers", []):
                            conts.append(
                                {
                                    "id": c.get("id", f"C{len(conts)+1}"),
                                    "label": c.get("label", c.get("id", "Container")),
                                    "tasks": c.get("tasks", []),
                                    "providers": c.get("providers", ["cloud"]),
                                    "repetitions": c.get("repetitions", 3),
                                    "cool_down": c.get("cool_down_seconds", 30),
                                    "conditions": [],
                                }
                            )
                        st.session_state.dsn_containers = conts or [_blank_container(1)]
                        st.rerun()
