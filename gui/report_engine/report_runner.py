"""
A-LEMS Report Engine — Report Runner
The single entry point. Wires together:
  GoalRegistry → DataFetcher → StatEngine → NarrativeEngine →
  ChartBuilder → MkDocsIngester → PDFSynthesizer → HTMLSynthesizer

Usage:
    runner = ReportRunner(db_path, project_root, output_dir)
    result = runner.generate(config)
"""

from __future__ import annotations
import hashlib, json, logging, sqlite3, uuid
from datetime import datetime
from pathlib import Path
from typing import Callable

from .models import (
    ReportConfig, ReportRun, OutputFormat,
    ConfidenceLevel, HypothesisVerdict,
)
from .goal_registry import GoalRegistry
from .data_fetcher import fetch_runs, apply_metrics, get_run_count
from .stat_engine import run_all_tests, compute_confidence
from .narrative_engine import generate_narrative
from .chart_builder import build_goal_charts
from .mkdocs_ingester import MkDocsIngester
from .system_profiler import get_or_collect_profile, load_latest_profile
from .pdf_synthesizer import build_pdf
from .html_synthesizer import build_html

log = logging.getLogger(__name__)


def _hash(config: ReportConfig, run_ids: list[str]) -> str:
    payload = json.dumps({
        "goal_id": config.goal_id,
        "version": config.version,
        "filters": str(config.filters),
        "run_ids": sorted(run_ids),
    }, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _rasterise_svg(svg_path: Path) -> bytes:
    """Convert SVG → PNG bytes for PDF embedding. Falls back to empty on failure."""
    try:
        import cairosvg
        return cairosvg.svg2png(url=str(svg_path), dpi=300, scale=2)
    except ImportError:
        pass
    try:
        import subprocess, tempfile, os
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp_path = tmp.name
        subprocess.run(
            ["inkscape", "--export-type=png", "--export-dpi=300",
             f"--export-filename={tmp_path}", str(svg_path)],
            capture_output=True, timeout=15
        )
        if os.path.exists(tmp_path):
            data = open(tmp_path, "rb").read()
            os.unlink(tmp_path)
            return data
    except Exception:
        pass
    log.warning(f"SVG rasterisation unavailable for {svg_path.name} — skipping diagram in PDF")
    return b""


class ReportRunner:
    """Orchestrates end-to-end report generation."""

    def __init__(
        self,
        db_path: str | Path,
        project_root: str | Path,
        output_dir: str | Path,
        use_llm: bool = False,
        llm_fn: Callable | None = None,
    ):
        self.db_path = Path(db_path)
        self.project_root = Path(project_root)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.use_llm = use_llm
        self.llm_fn = llm_fn

        self.registry = GoalRegistry.get()
        self.ingester = MkDocsIngester(project_root)

    # ── Progress callback helper ──────────────────────────────────────────

    def generate(
        self,
        config: ReportConfig,
        progress_cb: Callable[[str, float], None] | None = None,
    ) -> ReportRun:
        """
        Generate a complete report from a ReportConfig.
        progress_cb(message, fraction) is called at each phase.
        Returns a ReportRun dataclass (also persisted to DB).
        """
        def progress(msg: str, pct: float):
            log.info(f"[{pct*100:.0f}%] {msg}")
            if progress_cb:
                progress_cb(msg, pct)

        # ── Phase 1: Validate ─────────────────────────────────────────────
        progress("Validating configuration…", 0.02)
        goal = self.registry.get_goal(config.goal_id)
        if goal is None:
            raise ValueError(f"Unknown goal_id: '{config.goal_id}'. "
                             f"Available: {self.registry.get_goal_ids()}")

        run_count = get_run_count(self.db_path, config.filters)
        if run_count < config.filters.min_runs:
            log.warning(
                f"Only {run_count} runs match filters "
                f"(minimum {config.filters.min_runs}). "
                "Report will be generated with LOW confidence."
            )

        # ── Phase 2: Fetch data ───────────────────────────────────────────
        progress("Fetching experiment runs from database…", 0.10)
        df = fetch_runs(self.db_path, config.filters)
        df = apply_metrics(df, goal.metrics)

        if df.empty:
            raise ValueError("No runs found matching the specified filters.")

        # ── Phase 3: Statistical analysis ─────────────────────────────────
        progress("Running statistical tests…", 0.25)
        group_col = "workflow_type"
        groups = df[group_col].dropna().unique().tolist()
        group_a = "linear"
        group_b = "agentic"

        # Fall back gracefully if expected groups not present
        if group_a not in groups:
            group_a = groups[0] if groups else "group_a"
        if group_b not in groups or group_b == group_a:
            group_b = groups[1] if len(groups) > 1 else groups[0]

        stat_results = run_all_tests(
            df, goal.metrics, group_col,
            group_a, group_b, goal.eval_criteria
        )
        runs_per_group = {
            g: int((df[group_col] == g).sum()) for g in groups
        }

        # ── Phase 4: System profile ────────────────────────────────────────
        progress("Loading system profile…", 0.35)
        try:
            conn = sqlite3.connect(str(self.db_path))
            sys_profile = load_latest_profile(conn)
            conn.close()
            if sys_profile is None:
                sys_profile = get_or_collect_profile(self.db_path)
        except Exception as e:
            log.warning(f"System profile unavailable: {e}")
            sys_profile = None

        # ── Phase 5: Narrative generation ─────────────────────────────────
        progress("Generating narrative…", 0.45)
        narrative = generate_narrative(
            goal=goal,
            results=stat_results,
            system_profile=sys_profile,
            runs_per_group=runs_per_group,
            use_llm=self.use_llm,
            llm_fn=self.llm_fn,
        )

        # ── Phase 6: Charts ────────────────────────────────────────────────
        progress("Building charts…", 0.55)
        # PDF needs PNG; HTML needs JSON
        need_pdf  = OutputFormat.PDF  in config.output_formats
        need_html = OutputFormat.HTML in config.output_formats

        chart_pngs:  list[tuple[str, bytes]] = []
        chart_jsons: list[tuple[str, str]]   = []

        if need_pdf:
            chart_pngs = build_goal_charts(config.goal_id, df, stat_results, as_png=True)
        if need_html:
            chart_jsons = build_goal_charts(config.goal_id, df, stat_results, as_png=False)

        # ── Phase 7: Diagrams ──────────────────────────────────────────────
        progress("Resolving architecture diagrams…", 0.65)
        diagram_pngs: list[tuple[str, bytes]] = []
        for diag_id in goal.diagram_ids:
            svg_path = self.ingester.get_diagram_path(diag_id)
            if svg_path and svg_path.exists():
                png = _rasterise_svg(svg_path)
                if png:
                    diagram_pngs.append((diag_id, png))

        # ── Phase 8: Documentation sections ───────────────────────────────
        progress("Ingesting documentation sections…", 0.72)
        doc_sections = self.ingester.resolve_doc_sections(goal.doc_sections)

        # ── Phase 9: Reproducibility hash ─────────────────────────────────
        run_ids = df["run_id"].tolist() if "run_id" in df.columns else []
        repro_hash = _hash(config, run_ids)

        # ── Phase 10: Render outputs ───────────────────────────────────────
        output_paths: dict[str, str] = {}
        base_name = f"{config.report_id}_{goal.goal_id}"

        if need_pdf:
            progress("Rendering PDF…", 0.80)
            pdf_path = self.output_dir / f"{base_name}.pdf"
            try:
                build_pdf(
                    config=config, goal=goal, narrative=narrative,
                    stat_results=stat_results, system_profile=sys_profile,
                    chart_images=chart_pngs, diagram_images=diagram_pngs,
                    doc_sections=doc_sections, output_path=pdf_path,
                    reproducibility_hash=repro_hash,
                )
                output_paths["pdf"] = str(pdf_path)
            except Exception as e:
                import traceback as _tb
                log.error(f"PDF render failed: {e}\n{_tb.format_exc()}")
                progress(f"⚠  PDF failed — {type(e).__name__}: {e}", 0.85)

        if need_html:
            progress("Rendering HTML…", 0.90)
            html_path = self.output_dir / f"{base_name}.html"
            try:
                build_html(
                    config=config, goal=goal, narrative=narrative,
                    stat_results=stat_results, system_profile=sys_profile,
                    chart_jsons=chart_jsons, diagram_images=diagram_pngs,
                    doc_sections=doc_sections, output_path=html_path,
                    reproducibility_hash=repro_hash,
                )
                output_paths["html"] = str(html_path)
            except Exception as e:
                log.error(f"HTML render failed: {e}")

        if OutputFormat.JSON in config.output_formats:
            json_path = self.output_dir / f"{base_name}.json"
            _write_json_report(
                config, goal, narrative, stat_results,
                sys_profile, repro_hash, run_count, json_path
            )
            output_paths["json"] = str(json_path)

        # ── Phase 11: Persist to DB ────────────────────────────────────────
        progress("Saving report record to database…", 0.97)
        report_run = ReportRun(
            report_id=config.report_id,
            goal_id=config.goal_id,
            report_type=config.report_type.value,
            title=config.title,
            run_filter_json=json.dumps(vars(config.filters), default=str),
            config_yaml=config.version,
            narrative_json=json.dumps({
                "executive_summary": narrative.executive_summary,
                "key_findings": narrative.key_findings,
                "hypothesis_verdict": narrative.hypothesis_verdict.value,
                "confidence_level": narrative.confidence_level.value,
            }),
            stat_results_json=json.dumps([
                {
                    "metric": r.metric_name, "p": r.p_value,
                    "d": r.effect_size, "sig": r.significant,
                    "pct_diff": r.pct_difference(),
                }
                for r in stat_results
            ]),
            confidence_level=narrative.confidence_level.value,
            confidence_rationale=narrative.confidence_rationale,
            hypothesis_verdict=narrative.hypothesis_verdict.value,
            output_paths=output_paths,
            run_count=run_count,
            generated_at=datetime.utcnow(),
            generator_version="1.0.0",
            reproducibility_hash=repro_hash,
        )
        _persist_report_run(report_run, self.db_path)

        progress("Complete.", 1.0)
        return report_run


def _write_json_report(
    config, goal, narrative, stat_results,
    sys_profile, repro_hash, run_count, path: Path
) -> None:
    data = {
        "report_id": config.report_id,
        "goal_id": config.goal_id,
        "title": config.title,
        "generated_at": datetime.utcnow().isoformat(),
        "reproducibility_hash": repro_hash,
        "run_count": run_count,
        "confidence": narrative.confidence_level.value,
        "verdict": narrative.hypothesis_verdict.value,
        "executive_summary": narrative.executive_summary,
        "key_findings": narrative.key_findings,
        "statistical_results": [
            {
                "metric": r.metric_name,
                "group_a": r.group_a_label,
                "group_b": r.group_b_label,
                "group_a_mean": r.group_a_mean,
                "group_b_mean": r.group_b_mean,
                "pct_difference": r.pct_difference(),
                "p_value": r.p_value,
                "effect_size": r.effect_size,
                "effect_label": r.effect_label.value,
                "significant": r.significant,
                "unit": r.unit,
            }
            for r in stat_results
        ],
        "system_profile": sys_profile.summary_line() if sys_profile else None,
        "limitations": narrative.limitations,
        "recommendations": narrative.recommendations,
    }
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _persist_report_run(run: ReportRun, db_path: Path) -> None:
    try:
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            INSERT OR REPLACE INTO report_runs (
                report_id, goal_id, report_type, title,
                run_filter_json, config_yaml, narrative_json,
                stat_results_json, confidence_level, confidence_rationale,
                hypothesis_verdict, output_paths_json, run_count,
                generated_at, generator_version, reproducibility_hash
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run.report_id, run.goal_id, run.report_type, run.title,
            run.run_filter_json, run.config_yaml, run.narrative_json,
            run.stat_results_json, run.confidence_level, run.confidence_rationale,
            run.hypothesis_verdict, json.dumps(run.output_paths), run.run_count,
            run.generated_at.isoformat(), run.generator_version,
            run.reproducibility_hash,
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"Failed to persist report run: {e}")
