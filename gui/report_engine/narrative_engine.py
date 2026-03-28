"""
A-LEMS Report Engine — Narrative Engine
Converts metric snapshots + stat results into goal-aware, publication-quality
narrative text.

Two modes:
  1. DETERMINISTIC (default, zero latency): template-driven, always works
  2. LLM-ENHANCED (opt-in): sends structured prompt to local/API LLM for
     richer prose — falls back to deterministic on failure

Persona templates define tone and domain vocabulary per goal category.
"""

from __future__ import annotations
import json, logging
from datetime import datetime
from .models import (
    ResearchGoal, MetricSnapshot, StatTestResult,
    ReportNarrative, ConfidenceLevel, HypothesisVerdict,
    EffectSizeLabel, SystemProfile,
)
from .stat_engine import compute_confidence

log = logging.getLogger(__name__)


# ── Persona vocabulary ────────────────────────────────────────────────────────

_PERSONA_VOICE = {
    "energy_researcher": {
        "domain": "energy efficiency and power measurement",
        "metric_verb": "consumed",
        "improvement": "more energy-efficient",
        "degradation": "higher energy overhead",
        "unit_context": "µJ measured via RAPL package domain",
    },
    "systems_engineer": {
        "domain": "system performance and orchestration overhead",
        "metric_verb": "exhibited",
        "improvement": "lower overhead",
        "degradation": "increased orchestration tax",
        "unit_context": "CPU time and wall-clock latency",
    },
    "hardware_engineer": {
        "domain": "hardware behaviour under workload",
        "metric_verb": "recorded",
        "improvement": "better thermal stability",
        "degradation": "increased thermal pressure",
        "unit_context": "sensor measurements from on-die temperature probes",
    },
    "research_engineer": {
        "domain": "AI system measurement and evaluation",
        "metric_verb": "demonstrated",
        "improvement": "superior efficiency",
        "degradation": "measurable overhead",
        "unit_context": "cross-metric instrumentation",
    },
}


# ── Statistical significance phrasing ────────────────────────────────────────

def _sig_phrase(r: StatTestResult) -> str:
    if r.test_name == "insufficient_data":
        return "could not be statistically evaluated (insufficient data)"
    if not r.significant:
        return (
            f"did not reach statistical significance "
            f"(p={r.p_value:.3f}, {r.effect_label.value} effect, d={r.effect_size:.2f})"
        )
    direction = "higher" if r.group_b_mean > r.group_a_mean else "lower"
    return (
        f"was significantly {direction} in {r.group_b_label} workflows "
        f"(p={r.p_value:.4f}, {r.effect_label.value} effect, d={r.effect_size:.2f}, "
        f"Δ={r.pct_difference():+.1f}%)"
    )


def _effect_sentence(r: StatTestResult) -> str:
    pct = r.pct_difference()
    direction = "increase" if pct > 0 else "decrease"
    abs_pct = abs(pct)
    size_word = {
        EffectSizeLabel.NEGLIGIBLE: "negligible practical",
        EffectSizeLabel.SMALL: "small practical",
        EffectSizeLabel.MEDIUM: "moderate practical",
        EffectSizeLabel.LARGE: "large practical",
    }[r.effect_label]
    return (
        f"The {abs_pct:.1f}% {direction} in {r.metric_name} represents a "
        f"{size_word} effect (Cohen's d = {r.effect_size:.2f})."
    )


# ── Verdict determination ─────────────────────────────────────────────────────

def _determine_verdict(
    goal: ResearchGoal,
    results: list[StatTestResult],
    confidence: ConfidenceLevel,
) -> tuple[HypothesisVerdict, str]:
    if confidence == ConfidenceLevel.LOW or not results:
        return (
            HypothesisVerdict.INSUFFICIENT,
            "Insufficient data to evaluate the hypothesis with confidence."
        )
    if not goal.hypothesis:
        return (
            HypothesisVerdict.INCONCLUSIVE,
            "No formal hypothesis was defined for this goal."
        )

    sig_results = [r for r in results if r.significant]
    large_effects = [r for r in results if r.effect_label == EffectSizeLabel.LARGE]
    n_total = len(results)

    if len(sig_results) >= max(1, n_total * 0.5) and large_effects:
        return (
            HypothesisVerdict.SUPPORTED,
            f"{len(sig_results)}/{n_total} metrics reached significance with "
            f"{len(large_effects)} showing large effect sizes, consistent with "
            f"the stated hypothesis."
        )
    if len(sig_results) == 0:
        return (
            HypothesisVerdict.REJECTED,
            f"None of the {n_total} metrics reached statistical significance "
            f"(α=0.05), failing to support the hypothesis."
        )
    return (
        HypothesisVerdict.INCONCLUSIVE,
        f"{len(sig_results)}/{n_total} metrics reached significance but "
        f"effect sizes were mixed, providing only partial evidence."
    )


# ── Section narrative builders ────────────────────────────────────────────────

def _build_executive_summary(
    goal: ResearchGoal,
    results: list[StatTestResult],
    confidence: ConfidenceLevel,
    verdict: HypothesisVerdict,
    profile: SystemProfile | None,
) -> str:
    persona = _PERSONA_VOICE.get(goal.narrative_persona, _PERSONA_VOICE["research_engineer"])
    hw_line = profile.summary_line() if profile else "hardware profile unavailable"
    sig_count = sum(1 for r in results if r.significant)
    n = len(results)

    sig_sentence = (
        f"{sig_count} of {n} metrics reached statistical significance"
        if n > 0 else "no statistical tests were run"
    )

    verdict_line = {
        HypothesisVerdict.SUPPORTED: "The hypothesis is supported by the evidence.",
        HypothesisVerdict.REJECTED: "The hypothesis is not supported by the evidence.",
        HypothesisVerdict.INCONCLUSIVE: "The evidence is inconclusive.",
        HypothesisVerdict.INSUFFICIENT: "Data is insufficient for a definitive verdict.",
    }[verdict]

    return (
        f"This report presents a {goal.category.value} analysis of LLM workflow "
        f"measurements conducted on {hw_line}. "
        f"The analysis focuses on {persona['domain']} across linear and agentic "
        f"workflow types. "
        f"A total of {n} metrics were evaluated; {sig_sentence}. "
        f"Overall report confidence is {confidence.value}. "
        f"{verdict_line}"
    )


def _build_key_findings(
    results: list[StatTestResult],
    goal: ResearchGoal,
) -> list[str]:
    findings = []
    # Sort: significant first, then by effect size descending
    sorted_r = sorted(
        results,
        key=lambda r: (r.significant, abs(r.effect_size)),
        reverse=True
    )
    for r in sorted_r[:5]:
        findings.append(
            f"{r.metric_name} {_sig_phrase(r)}. "
            f"{_effect_sentence(r)}"
        )
    if not findings:
        findings.append(
            "No metrics with sufficient data were available for this analysis."
        )
    return findings


def _build_methodology(goal: ResearchGoal, profile: SystemProfile | None) -> str:
    hw = profile.summary_line() if profile else "unspecified hardware"
    rapl = ", ".join(profile.rapl_zones) if profile else "unavailable"
    criteria = goal.eval_criteria
    return (
        f"Experiments were executed on {hw}. "
        f"Energy measurements were collected via Linux RAPL sensors "
        f"(zones: {rapl}) using the A-LEMS harness. "
        f"Statistical comparisons used the {criteria.stat_test.value.replace('_', '-')} "
        f"test at α={criteria.alpha}, with effect sizes reported as Cohen's d. "
        f"Confidence intervals are at the {criteria.ci_level*100:.0f}% level."
    )


def _build_limitations(
    results: list[StatTestResult],
    confidence: ConfidenceLevel,
    goal: ResearchGoal,
) -> list[str]:
    lims = []
    insuf = [r for r in results if r.test_name == "insufficient_data"]
    if insuf:
        lims.append(
            f"{len(insuf)} metric(s) had insufficient runs for reliable "
            f"statistical testing (minimum: {goal.eval_criteria.min_runs_per_group} per group)."
        )
    if confidence == ConfidenceLevel.LOW:
        lims.append(
            "Overall confidence is LOW — conclusions should be treated as "
            "preliminary pending additional data collection."
        )
    lims.append(
        "Carbon footprint estimates use a fixed grid intensity factor and "
        "do not account for time-of-day or regional grid variation."
    )
    lims.append(
        "RAPL measurements capture package and DRAM domains; GPU energy "
        "is not included unless a compatible GPU power sensor is present."
    )
    lims.append(
        "Citations and external references are reserved for a future version "
        "of this report and are not included in the current output."
    )
    return lims


def _build_recommendations(
    results: list[StatTestResult],
    goal: ResearchGoal,
    verdict: HypothesisVerdict,
) -> list[str]:
    recs = []
    large_agentic = [
        r for r in results
        if r.significant and r.effect_label == EffectSizeLabel.LARGE
        and r.group_b_mean > r.group_a_mean
    ]
    if large_agentic and goal.category.value in ("efficiency", "cost", "carbon"):
        recs.append(
            f"Consider profiling orchestration logic for the metrics "
            f"{', '.join(r.metric_name for r in large_agentic[:2])} "
            f"where agentic overhead is largest."
        )
    if verdict == HypothesisVerdict.INSUFFICIENT:
        recs.append(
            f"Collect at least {goal.eval_criteria.min_runs_per_group * 2} runs "
            f"per workflow type to achieve sufficient statistical power."
        )
    recs.append(
        "Run the Sufficiency Advisor (Data Quality → Sufficiency) to identify "
        "under-covered model × task × workflow combinations."
    )
    recs.append(
        "Consider enabling baseline-adjusted energy analysis using idle_baselines "
        "table to normalise for background system load."
    )
    return recs


# ── Main entry point ──────────────────────────────────────────────────────────

def generate_narrative(
    goal: ResearchGoal,
    results: list[StatTestResult],
    system_profile: SystemProfile | None,
    runs_per_group: dict[str, int],
    use_llm: bool = False,
    llm_fn=None,           # callable(prompt: str) -> str, optional
) -> ReportNarrative:
    """
    Generate a complete ReportNarrative for a goal.

    Args:
        goal: The ResearchGoal driving this report
        results: List of StatTestResult from stat_engine
        system_profile: Hardware profile (or None)
        runs_per_group: {workflow_type: run_count}
        use_llm: If True and llm_fn provided, use LLM for prose
        llm_fn: callable(prompt) -> str

    Returns:
        ReportNarrative dataclass
    """
    confidence, confidence_rationale = compute_confidence(
        results, goal.eval_criteria.min_runs_per_group, runs_per_group
    )
    verdict, verdict_explanation = _determine_verdict(goal, results, confidence)

    exec_summary = _build_executive_summary(
        goal, results, confidence, verdict, system_profile
    )
    key_findings = _build_key_findings(results, goal)
    limitations = _build_limitations(results, confidence, goal)
    recommendations = _build_recommendations(results, goal, verdict)

    # Section narratives — one paragraph per major section
    section_narratives: dict[str, str] = {}

    section_narratives["experiment_setup"] = _build_methodology(goal, system_profile)

    section_narratives["goal_analysis"] = (
        f"The primary goal of this analysis was to assess {goal.description.strip()} "
        f"The evaluation focused on {len(goal.metrics)} metrics across workflow types, "
        f"with particular attention to {goal.metrics[0].name if goal.metrics else 'core metrics'}. "
        f"The {goal.eval_criteria.stat_test.value.replace('_', '-')} test was selected "
        f"for its robustness to non-normal distributions, which are common in "
        f"system measurement data."
    )

    section_narratives["interpretation"] = (
        f"The results {verdict_explanation} "
        f"{'The magnitude of differences across workflow types suggests that ' if results else ''}"
        f"{'further investigation is warranted ' if verdict == HypothesisVerdict.INCONCLUSIVE else ''}"
        f"{'practical significance aligns with statistical findings ' if verdict == HypothesisVerdict.SUPPORTED else ''}"
        f"in the context of {goal.category.value} analysis."
    )

    section_narratives["conclusion"] = (
        f"This {goal.category.value} analysis of {sum(runs_per_group.values()) if runs_per_group else '?'} "
        f"experiment runs provides "
        f"{'strong' if confidence == ConfidenceLevel.HIGH else 'preliminary' if confidence == ConfidenceLevel.LOW else 'moderate'} "
        f"evidence regarding {goal.name.lower()}. "
        f"{verdict_explanation} "
        f"The A-LEMS measurement platform captured high-fidelity sensor data "
        f"enabling rigorous statistical evaluation of the stated research goal."
    )

    # Optional LLM enhancement — enriches exec_summary and key_findings
    if use_llm and llm_fn is not None:
        try:
            prompt = _build_llm_prompt(
                goal, results, confidence, verdict,
                exec_summary, key_findings, system_profile
            )
            llm_text = llm_fn(prompt)
            enhanced = _parse_llm_response(llm_text)
            if enhanced.get("executive_summary"):
                exec_summary = enhanced["executive_summary"]
            if enhanced.get("key_findings"):
                key_findings = enhanced["key_findings"]
        except Exception as e:
            log.warning(f"LLM narrative enhancement failed: {e}. Using deterministic fallback.")

    return ReportNarrative(
        executive_summary=exec_summary,
        key_findings=key_findings,
        anomaly_flags=_detect_anomalies(results),
        hypothesis_verdict=verdict,
        verdict_explanation=verdict_explanation,
        recommendations=recommendations,
        limitations=limitations,
        confidence_level=confidence,
        confidence_rationale=confidence_rationale,
        section_narratives=section_narratives,
    )


def _detect_anomalies(results: list[StatTestResult]) -> list[str]:
    anomalies = []
    for r in results:
        if r.significant and r.effect_label == EffectSizeLabel.NEGLIGIBLE:
            anomalies.append(
                f"{r.metric_name}: statistically significant (p={r.p_value:.4f}) "
                f"but negligible effect size — likely a large-sample artefact."
            )
        if abs(r.pct_difference()) > 500:
            anomalies.append(
                f"{r.metric_name}: extreme difference ({r.pct_difference():+.0f}%) — "
                f"verify data quality for this metric."
            )
    return anomalies


def _build_llm_prompt(
    goal: ResearchGoal,
    results: list[StatTestResult],
    confidence: ConfidenceLevel,
    verdict: HypothesisVerdict,
    exec_summary: str,
    key_findings: list[str],
    profile: SystemProfile | None,
) -> str:
    stats_json = json.dumps(
        [{"metric": r.metric_name, "p": round(r.p_value, 4),
          "d": round(r.effect_size, 3), "sig": r.significant,
          "pct_diff": round(r.pct_difference(), 1)}
         for r in results],
        indent=2
    )
    hw = profile.summary_line() if profile else "unknown hardware"
    return f"""You are writing a section of a scientific research report on AI system energy measurement.

RESEARCH GOAL: {goal.name}
DESCRIPTION: {goal.description}
HYPOTHESIS: {goal.hypothesis or 'None stated'}
HARDWARE: {hw}
CONFIDENCE: {confidence.value}
VERDICT: {verdict.value}

STATISTICAL RESULTS:
{stats_json}

DRAFT EXECUTIVE SUMMARY (improve this, keep it factual, 3-4 sentences, IEEE style):
{exec_summary}

DRAFT KEY FINDINGS (improve these, keep 3-5 bullets, start each with the metric name):
{chr(10).join('- ' + f for f in key_findings)}

Respond ONLY with valid JSON in this exact format:
{{
  "executive_summary": "...",
  "key_findings": ["...", "...", "..."]
}}"""


def _parse_llm_response(text: str) -> dict:
    import re
    text = text.strip()
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except Exception:
            pass
    return {}
