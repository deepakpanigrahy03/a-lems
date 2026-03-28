"""
gui/report_engine/content_registry.py
─────────────────────────────────────────────────────────────────────────────
Content Registry — all report text lives in Markdown files, not Python.

Principles:
  - Researchers write Markdown, engine renders it
  - Every {{variable}} slot is filled from live data at render time
  - Goal-specific content overrides generic section content
  - Shared blocks can be imported with {% include 'shared/rapl.md' %}
  - Citations use [REF-N] placeholders, filled in Phase 4

File layout:
    gui/report_engine/content/
    ├── sections/
    │   ├── executive_summary.md     ← generic template
    │   ├── methodology.md
    │   ├── goal_analysis.md
    │   ├── interpretation.md
    │   └── conclusion.md
    ├── goals/
    │   ├── energy_efficiency/
    │   │   ├── intro.md             ← goal overrides generic section
    │   │   ├── hypothesis_framing.md
    │   │   └── interpretation.md
    │   └── orchestration_overhead/
    │       └── intro.md
    └── shared/
        ├── statistical_methods.md
        ├── rapl_methodology.md
        └── citation_slots.md
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import re, logging
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)

_CONTENT_DIR = Path(__file__).parent / "content"


# ══════════════════════════════════════════════════════════════════════════════
# TEMPLATE RENDERER  (Jinja2-lite, no dependency)
# ══════════════════════════════════════════════════════════════════════════════

class _SimpleRenderer:
    """
    Minimal template renderer.
    Handles: {{variable}}, {{obj.attr}}, {{list[0]}}, {{val|format('.2f')}}
    Does NOT handle loops or conditionals — those belong in Python, not templates.
    """

    _VAR_RE = re.compile(r"\{\{(.+?)\}\}")

    def render(self, template: str, context: dict[str, Any]) -> str:
        def replace(m: re.Match) -> str:
            expr = m.group(1).strip()
            try:
                return self._eval(expr, context)
            except Exception as e:
                log.debug(f"Template var '{expr}' failed: {e}")
                return f"[{expr}]"

        return self._VAR_RE.sub(replace, template)

    def _eval(self, expr: str, ctx: dict) -> str:
        # Handle format filter: val|format('.2f')
        if "|format(" in expr:
            parts = expr.split("|format(", 1)
            val = self._resolve(parts[0].strip(), ctx)
            fmt = parts[1].rstrip(")").strip().strip("'\"")
            if isinstance(val, (int, float)):
                return format(val, fmt)
            return str(val)

        # Handle join filter: list|join(', ')
        if "|join(" in expr:
            parts = expr.split("|join(", 1)
            val = self._resolve(parts[0].strip(), ctx)
            sep = parts[1].rstrip(")").strip().strip("'\"")
            if isinstance(val, (list, tuple)):
                return sep.join(str(v) for v in val)
            return str(val)

        val = self._resolve(expr, ctx)
        return str(val) if val is not None else f"[{expr}]"

    def _resolve(self, expr: str, ctx: dict) -> Any:
        """Resolve dot-notation and list index access."""
        parts = re.split(r"[\.\[]", expr.rstrip("]"))
        val: Any = ctx
        for part in parts:
            part = part.strip().strip("'\"")
            if isinstance(val, dict):
                val = val.get(part)
            elif hasattr(val, part):
                val = getattr(val, part)
            elif part.isdigit() and isinstance(val, (list, tuple)):
                idx = int(part)
                val = val[idx] if idx < len(val) else None
            else:
                return None
        return val


_renderer = _SimpleRenderer()


# ══════════════════════════════════════════════════════════════════════════════
# CONTENT LOADER
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_includes(text: str, content_dir: Path) -> str:
    """
    Process {% include 'shared/rapl_methodology.md' %} directives.
    """
    include_re = re.compile(r"\{%\s*include\s+['\"](.+?)['\"]\s*%\}")

    def do_include(m: re.Match) -> str:
        rel = m.group(1)
        path = content_dir / rel
        if path.exists():
            return path.read_text(encoding="utf-8")
        log.warning(f"Include not found: {path}")
        return f"[include: {rel} not found]"

    return include_re.sub(do_include, text)


def _load_md(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""
    except Exception as e:
        log.warning(f"Cannot read {path}: {e}")
        return ""


# ══════════════════════════════════════════════════════════════════════════════
# RESOLUTION PRIORITY
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_content(
    section_id: str,
    goal_id: str | None,
    content_dir: Path,
) -> str:
    """
    Resolution order (first found wins):
    1. goals/{goal_id}/{section_id}.md       ← goal-specific
    2. goals/{goal_id}/intro.md              ← goal intro (for goal_and_hypothesis)
    3. sections/{section_id}.md              ← generic section template
    4. ""                                    ← empty (engine uses fallback)
    """
    candidates = []
    if goal_id:
        candidates.append(content_dir / "goals" / goal_id / f"{section_id}.md")
        if section_id in ("goal_and_hypothesis", "goal_analysis"):
            candidates.append(content_dir / "goals" / goal_id / "intro.md")
    candidates.append(content_dir / "sections" / f"{section_id}.md")

    for path in candidates:
        if path.exists():
            text = _load_md(path)
            if text.strip():
                return _resolve_includes(text, content_dir)

    return ""


# ══════════════════════════════════════════════════════════════════════════════
# SINGLETON REGISTRY
# ══════════════════════════════════════════════════════════════════════════════

class ContentRegistry:
    """
    Singleton. Call ContentRegistry.get() everywhere.
    Loads Markdown content from the content/ directory.
    Renders {{variable}} slots using a live data context.
    """

    _instance: Optional[ContentRegistry] = None

    @classmethod
    def get(cls) -> ContentRegistry:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self, content_dir: Path | None = None):
        self.content_dir = content_dir or _CONTENT_DIR

    def get_section(
        self,
        section_id: str,
        goal_id: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> str:
        """
        Load and render a section's Markdown content.

        Args:
            section_id: e.g. 'executive_summary', 'methodology'
            goal_id:    e.g. 'energy_efficiency' (selects goal-specific content)
            context:    dict of variables for {{variable}} substitution

        Returns:
            Rendered Markdown string (empty string if no content found)
        """
        raw = _resolve_content(section_id, goal_id, self.content_dir)
        if not raw:
            return ""
        if context:
            return _renderer.render(raw, context)
        return raw

    def get_shared(self, block_id: str, context: dict[str, Any] | None = None) -> str:
        """Load a shared content block from content/shared/{block_id}.md"""
        path = self.content_dir / "shared" / f"{block_id}.md"
        raw = _load_md(path)
        if raw and context:
            return _renderer.render(raw, context)
        return raw

    def list_sections(self) -> list[str]:
        sections_dir = self.content_dir / "sections"
        if not sections_dir.exists():
            return []
        return [f.stem for f in sections_dir.glob("*.md")]

    def list_goal_content(self, goal_id: str) -> list[str]:
        goal_dir = self.content_dir / "goals" / goal_id
        if not goal_dir.exists():
            return []
        return [f.stem for f in goal_dir.glob("*.md")]

    def has_content(self, section_id: str, goal_id: str | None = None) -> bool:
        return bool(_resolve_content(section_id, goal_id, self.content_dir))

    def render_string(self, template: str, context: dict[str, Any]) -> str:
        """Render any Markdown string with variable substitution."""
        resolved = _resolve_includes(template, self.content_dir)
        return _renderer.render(resolved, context)

    def build_context(
        self,
        goal,              # ResearchGoal
        narrative,         # ReportNarrative
        stat_results: list,
        system_profile,    # SystemProfile | None
        run_count: int,
        workflow_types: list[str],
    ) -> dict[str, Any]:
        """
        Build the standard template context dict that all section
        templates can reference via {{variable}} syntax.
        """
        # Stat results as a dict by metric name for easy lookup
        stats_by_metric: dict[str, Any] = {}
        for r in stat_results:
            stats_by_metric[r.metric_name] = {
                "p_value":       r.p_value,
                "effect_size":   r.effect_size,
                "effect_label":  r.effect_label.value,
                "pct_difference":r.pct_difference(),
                "significant":   r.significant,
                "group_a_mean":  r.group_a_mean,
                "group_b_mean":  r.group_b_mean,
                "unit":          r.unit,
            }

        ctx: dict[str, Any] = {
            # Goal
            "goal": {
                "goal_id":     goal.goal_id,
                "name":        goal.name,
                "category":    goal.category.value,
                "description": goal.description,
                "hypothesis":  goal.hypothesis or "",
                "version":     goal.version,
                "persona":     goal.narrative_persona,
            },
            # Narrative
            "executive_summary":  narrative.executive_summary,
            "key_findings":       narrative.key_findings,
            "verdict":            narrative.hypothesis_verdict.value,
            "verdict_explanation":narrative.verdict_explanation,
            "confidence":         narrative.confidence_level.value,
            "confidence_rationale":narrative.confidence_rationale,
            "recommendations":    narrative.recommendations,
            "limitations":        narrative.limitations,
            "anomaly_flags":      narrative.anomaly_flags,
            # Stats
            "stats":          stats_by_metric,
            "stat_results":   stat_results,
            "run_count":      run_count,
            "workflow_types": workflow_types,
            # System
            "system_profile": {
                "summary":    system_profile.summary_line() if system_profile else "Hardware profile not available",
                "cpu":        system_profile.cpu_model if system_profile else "Unknown",
                "cores":      system_profile.cpu_cores_logical if system_profile else "?",
                "ram_gb":     system_profile.ram_gb if system_profile else "?",
                "env":        system_profile.env_type.value if system_profile else "Unknown",
                "rapl_zones": system_profile.rapl_zones if system_profile else [],
            },
        }
        return ctx
