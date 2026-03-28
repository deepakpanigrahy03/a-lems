"""
A-LEMS Report Engine — Statistical Engine
Computes all statistical tests, effect sizes, and confidence intervals.
Pure Python + scipy/numpy — no Streamlit dependency.
"""

from __future__ import annotations
import math, logging
import numpy as np
import pandas as pd
from typing import Optional
from .models import (
    StatTestResult, StatTest, EvalCriteria,
    EffectSizeLabel, ConfidenceLevel
)

log = logging.getLogger(__name__)


# ── Effect size helpers ───────────────────────────────────────────────────────

def _cohens_d(a: np.ndarray, b: np.ndarray) -> float:
    """Pooled Cohen's d."""
    na, nb = len(a), len(b)
    if na < 2 or nb < 2:
        return 0.0
    pooled_std = math.sqrt(
        ((na - 1) * np.var(a, ddof=1) + (nb - 1) * np.var(b, ddof=1))
        / (na + nb - 2)
    )
    if pooled_std == 0:
        return 0.0
    return (np.mean(a) - np.mean(b)) / pooled_std


def _label_effect(d: float) -> EffectSizeLabel:
    ad = abs(d)
    if ad < 0.2:
        return EffectSizeLabel.NEGLIGIBLE
    if ad < 0.5:
        return EffectSizeLabel.SMALL
    if ad < 0.8:
        return EffectSizeLabel.MEDIUM
    return EffectSizeLabel.LARGE


def _bootstrap_ci(
    a: np.ndarray,
    b: np.ndarray,
    n_boot: int = 5000,
    ci_level: float = 0.95,
    rng: np.random.Generator | None = None,
) -> tuple[float, float]:
    """Bootstrap CI for difference of means (b - a)."""
    rng = rng or np.random.default_rng(42)
    diffs = []
    for _ in range(n_boot):
        sa = rng.choice(a, size=len(a), replace=True)
        sb = rng.choice(b, size=len(b), replace=True)
        diffs.append(np.mean(sb) - np.mean(sa))
    lo = (1 - ci_level) / 2
    hi = 1 - lo
    return float(np.quantile(diffs, lo)), float(np.quantile(diffs, hi))


# ── Core test dispatcher ──────────────────────────────────────────────────────

def run_stat_test(
    group_a: pd.Series,
    group_b: pd.Series,
    metric_name: str,
    group_a_label: str,
    group_b_label: str,
    criteria: EvalCriteria,
    unit: str = "",
) -> StatTestResult:
    """
    Run the configured stat test and return a fully populated StatTestResult.
    Gracefully handles small samples and zero-variance cases.
    """
    a = group_a.dropna().to_numpy(dtype=float)
    b = group_b.dropna().to_numpy(dtype=float)

    # Guard: need at least 2 values per group
    if len(a) < 2 or len(b) < 2:
        return _insufficient_result(
            metric_name, group_a_label, group_b_label, a, b, unit
        )

    # Core test
    try:
        from scipy import stats as sp
        if criteria.stat_test == StatTest.T_TEST:
            stat, p = sp.ttest_ind(a, b, equal_var=False)
        elif criteria.stat_test == StatTest.WILCOXON and len(a) == len(b):
            stat, p = sp.wilcoxon(a, b)
        else:  # default: Mann-Whitney
            stat, p = sp.mannwhitneyu(a, b, alternative="two-sided")
    except Exception as e:
        log.warning(f"Stat test failed for {metric_name}: {e}")
        stat, p = 0.0, 1.0

    d = _cohens_d(a, b)
    effect_label = _label_effect(d)
    significant = bool(p < criteria.alpha)

    # Confidence interval
    if criteria.stat_test == StatTest.BOOTSTRAP:
        ci_lo, ci_hi = _bootstrap_ci(a, b, criteria.bootstrap_n, criteria.ci_level)
    else:
        try:
            from scipy import stats as sp
            diff_mean = np.mean(b) - np.mean(a)
            se = math.sqrt(np.var(a, ddof=1) / len(a) + np.var(b, ddof=1) / len(b))
            t_crit = sp.t.ppf(0.5 + criteria.ci_level / 2, df=len(a) + len(b) - 2)
            ci_lo = diff_mean - t_crit * se
            ci_hi = diff_mean + t_crit * se
        except Exception:
            ci_lo, ci_hi = float(np.mean(b) - np.mean(a)), float(np.mean(b) - np.mean(a))

    return StatTestResult(
        test_name=criteria.stat_test.value,
        metric_name=metric_name,
        group_a_label=group_a_label,
        group_b_label=group_b_label,
        group_a_n=len(a),
        group_b_n=len(b),
        group_a_mean=float(np.mean(a)),
        group_b_mean=float(np.mean(b)),
        group_a_median=float(np.median(a)),
        group_b_median=float(np.median(b)),
        group_a_std=float(np.std(a, ddof=1)),
        group_b_std=float(np.std(b, ddof=1)),
        statistic=float(stat),
        p_value=float(p),
        effect_size=float(d),
        effect_label=effect_label,
        ci_low=float(ci_lo),
        ci_high=float(ci_hi),
        ci_level=criteria.ci_level,
        significant=significant,
        unit=unit,
    )


def _insufficient_result(
    metric_name: str,
    group_a_label: str,
    group_b_label: str,
    a: np.ndarray,
    b: np.ndarray,
    unit: str,
) -> StatTestResult:
    return StatTestResult(
        test_name="insufficient_data",
        metric_name=metric_name,
        group_a_label=group_a_label,
        group_b_label=group_b_label,
        group_a_n=len(a),
        group_b_n=len(b),
        group_a_mean=float(np.mean(a)) if len(a) else 0.0,
        group_b_mean=float(np.mean(b)) if len(b) else 0.0,
        group_a_median=float(np.median(a)) if len(a) else 0.0,
        group_b_median=float(np.median(b)) if len(b) else 0.0,
        group_a_std=0.0, group_b_std=0.0,
        statistic=0.0, p_value=1.0,
        effect_size=0.0, effect_label=EffectSizeLabel.NEGLIGIBLE,
        ci_low=0.0, ci_high=0.0, ci_level=0.95,
        significant=False, unit=unit,
        notes="Insufficient data for statistical test",
    )


# ── Confidence gate ───────────────────────────────────────────────────────────

def compute_confidence(
    results: list[StatTestResult],
    min_runs: int,
    actual_runs_per_group: dict[str, int],
) -> tuple[ConfidenceLevel, str]:
    """
    Determine overall report confidence level.
    Returns (level, rationale).
    """
    issues = []

    for grp, n in actual_runs_per_group.items():
        if n < min_runs:
            issues.append(f"Group '{grp}' has only {n} runs (minimum: {min_runs})")

    if not results:
        return ConfidenceLevel.LOW, "No statistical results computed"

    sig_count = sum(1 for r in results if r.significant)
    insuf_count = sum(1 for r in results if r.test_name == "insufficient_data")
    large_effect = sum(1 for r in results if r.effect_label == EffectSizeLabel.LARGE)

    if insuf_count > 0:
        issues.append(f"{insuf_count} metric(s) had insufficient data for testing")

    # Contradictions: significant p but negligible effect
    contradictions = sum(
        1 for r in results
        if r.significant and r.effect_label == EffectSizeLabel.NEGLIGIBLE
    )
    if contradictions:
        issues.append(f"{contradictions} metric(s) show significant p but negligible effect size")

    if issues:
        rationale = " | ".join(issues)
        if len(issues) >= 2 or insuf_count > 0:
            return ConfidenceLevel.LOW, rationale
        return ConfidenceLevel.MEDIUM, rationale

    if sig_count > 0 and large_effect > 0:
        return ConfidenceLevel.HIGH, (
            f"{sig_count}/{len(results)} metrics significant, "
            f"{large_effect} with large effect size"
        )

    return ConfidenceLevel.MEDIUM, (
        f"{sig_count}/{len(results)} metrics reach significance"
    )


# ── Batch runner ──────────────────────────────────────────────────────────────

def run_all_tests(
    df: pd.DataFrame,
    metrics: list,        # list[MetricSpec]
    group_col: str,
    group_a: str,
    group_b: str,
    criteria: EvalCriteria,
) -> list[StatTestResult]:
    """
    Run stat tests for all metrics in a goal against the given groups.
    Skips metrics whose column is not present in df.
    """
    results = []
    df_a = df[df[group_col] == group_a]
    df_b = df[df[group_col] == group_b]

    for m in metrics:
        col = m.column
        if col not in df.columns:
            log.debug(f"Skipping metric {m.name}: column '{col}' not in dataframe")
            continue
        try:
            result = run_stat_test(
                df_a[col], df_b[col],
                m.name, group_a, group_b,
                criteria, m.unit,
            )
            results.append(result)
        except Exception as e:
            log.warning(f"Test failed for metric {m.name}: {e}")

    return results
