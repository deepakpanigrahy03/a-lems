"""
================================================================================
BASE MODULE – Shared utility functions for experiment execution
================================================================================

Purpose:
    Contains base classes and utility functions used across execution modules.
    This is the first module extracted from harness.py during refactoring.

Functions:
    calc_stats(data) – Calculate mean, std, and 95% CI for experimental data

Author: Deepak Panigrahy (refactored from harness.py)
================================================================================
"""

from typing import Any, Dict, List

import numpy as np
from scipy import stats as scipy_stats


def calc_stats(data: List[float]) -> Dict[str, float]:
    """
    Calculate statistics for experimental data.

    Args:
        data: List of numeric values (energies, times, taxes, etc.)

    Returns:
        Dictionary with:
            - mean: Arithmetic mean
            - std: Sample standard deviation (n-1)
            - ci_lower: Lower bound of 95% confidence interval
            - ci_upper: Upper bound of 95% confidence interval
            - min: Minimum value
            - max: Maximum value
            - n: Number of samples

    Scientific note:
        Uses Student's t-distribution for CI when n>=2.
        For n=1, returns mean only (CI = nan).
        For n=0, returns zeros and nan.
    """
    arr = np.array(data)
    n = len(arr)

    if n >= 2:
        mean = np.mean(arr)
        std = np.std(arr, ddof=1)
        # 95% confidence interval using Student's t
        ci = scipy_stats.t.interval(0.95, n - 1, loc=mean, scale=std / np.sqrt(n))
        ci_lower, ci_upper = ci[0], ci[1]
    elif n == 1:
        mean = arr[0]
        std = float("nan")
        ci_lower = float("nan")
        ci_upper = float("nan")
    else:  # n == 0
        mean = 0
        std = float("nan")
        ci_lower = float("nan")
        ci_upper = float("nan")

    return {
        "mean": mean,
        "std": std,
        "ci_lower": ci_lower,
        "ci_upper": ci_upper,
        "min": np.min(arr) if n > 0 else 0,
        "max": np.max(arr) if n > 0 else 0,
        "n": n,
    }
