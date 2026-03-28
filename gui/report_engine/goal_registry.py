"""
A-LEMS Report Engine — Goal Registry
Loads, validates, and serves ResearchGoal definitions from YAML files.
Singleton — call GoalRegistry.get() everywhere.
"""

from __future__ import annotations
import os, yaml, logging
from pathlib import Path
from typing import Optional
from .models import (
    ResearchGoal, MetricSpec, EvalCriteria, Threshold,
    GoalCategory, StatTest
)

log = logging.getLogger(__name__)

# Default goals directory — sits next to this file
_GOALS_DIR = Path(__file__).parent / "goals"


def _parse_metric(d: dict) -> MetricSpec:
    return MetricSpec(
        name=d["name"],
        column=d["column"],
        unit=d.get("unit", ""),
        direction=d.get("direction", "lower_is_better"),
        display_precision=d.get("display_precision", 2),
        baseline_col=d.get("baseline_col"),
        alert_threshold=d.get("alert_threshold"),
        formula=d.get("formula"),
        description=d.get("description", ""),
    )


def _parse_eval(d: dict) -> EvalCriteria:
    return EvalCriteria(
        stat_test=StatTest(d.get("stat_test", "mann_whitney")),
        alpha=d.get("alpha", 0.05),
        effect_size_measure=d.get("effect_size", "cohens_d"),
        min_runs_per_group=d.get("min_runs_per_group", 5),
        bootstrap_n=d.get("bootstrap_n", 5000),
        report_ci=d.get("report_ci", True),
        ci_level=d.get("ci_level", 0.95),
        comparison_mode=d.get("comparison_mode", "relative"),
    )


def _parse_thresholds(d: dict) -> dict[str, Threshold]:
    out = {}
    for k, v in d.items():
        out[k] = Threshold(warn=v["warn"], severe=v["severe"], unit=v.get("unit", ""))
    return out


def load_goal_from_yaml(path: Path) -> ResearchGoal:
    with open(path) as f:
        data = yaml.safe_load(f)
    g = data["goal"]
    return ResearchGoal(
        goal_id=g["goal_id"],
        name=g["name"],
        category=GoalCategory(g.get("category", "custom")),
        description=g.get("description", ""),
        hypothesis=g.get("hypothesis"),
        metrics=[_parse_metric(m) for m in g.get("metrics", [])],
        thresholds=_parse_thresholds(g.get("thresholds", {})),
        eval_criteria=_parse_eval(g.get("eval_criteria", {})),
        narrative_persona=g.get("narrative_persona", "research_engineer"),
        doc_sections=g.get("doc_sections", []),
        diagram_ids=g.get("diagram_ids", []),
        report_sections=g.get("report_sections", []),
        version=g.get("version", "1.0.0"),
        tags=g.get("tags", []),
    )


class GoalRegistry:
    """Singleton goal store. Thread-safe for Streamlit (single-process)."""

    _instance: Optional[GoalRegistry] = None
    _goals: dict[str, ResearchGoal] = {}
    _loaded: bool = False

    @classmethod
    def get(cls) -> GoalRegistry:
        if cls._instance is None:
            cls._instance = cls()
        if not cls._loaded:
            cls._instance.load_all()
        return cls._instance

    def load_all(self, extra_dirs: list[Path] | None = None) -> dict[str, list[str]]:
        """Load all YAML files from goals dir. Returns {loaded: [...], errors: [...]}."""
        loaded, errors = [], []
        dirs = [_GOALS_DIR] + (extra_dirs or [])
        for d in dirs:
            if not d.exists():
                log.warning(f"Goals dir not found: {d}")
                continue
            for f in sorted(d.glob("*.yaml")):
                try:
                    goal = load_goal_from_yaml(f)
                    self._goals[goal.goal_id] = goal
                    loaded.append(goal.goal_id)
                    log.info(f"Loaded goal: {goal.goal_id}")
                except Exception as e:
                    errors.append(f"{f.name}: {e}")
                    log.error(f"Failed to load goal {f}: {e}")
        self._loaded = True
        return {"loaded": loaded, "errors": errors}

    def list_goals(self) -> list[ResearchGoal]:
        return list(self._goals.values())

    def get_goal(self, goal_id: str) -> ResearchGoal | None:
        return self._goals.get(goal_id)

    def get_goal_ids(self) -> list[str]:
        return list(self._goals.keys())

    def register(self, goal: ResearchGoal) -> None:
        self._goals[goal.goal_id] = goal

    def register_from_yaml_string(self, yaml_str: str) -> ResearchGoal:
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            f.write(yaml_str)
            tmp = Path(f.name)
        try:
            goal = load_goal_from_yaml(tmp)
            self.register(goal)
            return goal
        finally:
            tmp.unlink(missing_ok=True)

    def validate_yaml(self, yaml_str: str) -> dict[str, str | list]:
        """Returns {valid: bool, goal_id: str | None, errors: list[str]}."""
        errors = []
        try:
            data = yaml.safe_load(yaml_str)
            if "goal" not in data:
                errors.append("Missing top-level 'goal:' key")
                return {"valid": False, "goal_id": None, "errors": errors}
            g = data["goal"]
            for req in ["goal_id", "name", "metrics"]:
                if req not in g:
                    errors.append(f"Missing required field: {req}")
            if not errors:
                goal = self.register_from_yaml_string(yaml_str)
                return {"valid": True, "goal_id": goal.goal_id, "errors": []}
        except Exception as e:
            errors.append(str(e))
        return {"valid": False, "goal_id": None, "errors": errors}
