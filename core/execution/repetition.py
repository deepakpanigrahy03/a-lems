#!/usr/bin/env python3
"""
================================================================================
REPETITION EXPERIMENTS – Run multiple iterations with statistical analysis
================================================================================

Purpose:
    Single runs are meaningless for research. This module runs 50-100 repetitions
    and computes proper statistics: mean, std dev, confidence intervals.

Author: Deepak Panigrahy
================================================================================
"""

import time
from typing import Any, Callable, Dict, List

import numpy as np
from scipy import stats


class RepetitionExperiment:
    """
    Runs multiple iterations with statistical analysis.

    Required for publication:
    - 50-100 repetitions minimum
    - Mean and standard deviation
    - 95% confidence intervals
    - Outlier detection
    """

    def __init__(self, harness, n_iterations: int = 50):
        self.harness = harness
        self.n = n_iterations

    def run_comparison(self, linear_exec, agentic_exec, task: str) -> Dict[str, Any]:
        """Run N comparisons and compute statistics."""

        linear_energies = []
        agentic_energies = []
        linear_times = []
        agentic_times = []
        taxes = []

        for i in range(self.n):
            print(f"🔄 Iteration {i+1}/{self.n}")

            # Run single comparison
            result = self.harness.run_comparison(linear_exec, agentic_exec, task)

            # Extract metrics
            linear_energies.append(
                result["linear"]["derived_energy"]["energy"]["workload_energy_j"]
            )
            agentic_energies.append(
                result["agentic"]["derived_energy"]["energy"]["workload_energy_j"]
            )
            linear_times.append(result["linear"]["execution"]["execution_time_ms"])
            agentic_times.append(result["agentic"]["execution"]["total_time_ms"])
            taxes.append(result["orchestration_tax"]["energy_multiplier"])

            # Cool-down between runs
            if i < self.n - 1:
                time.sleep(10)

        # Calculate statistics
        def calc_stats(data):
            arr = np.array(data)
            mean = np.mean(arr)
            std = np.std(arr, ddof=1)
            ci = stats.t.interval(
                0.95, len(arr) - 1, loc=mean, scale=std / np.sqrt(len(arr))
            )
            return {
                "mean": mean,
                "std": std,
                "ci_lower": ci[0],
                "ci_upper": ci[1],
                "min": np.min(arr),
                "max": np.max(arr),
                "n": len(arr),
            }

        return {
            "linear_energy": calc_stats(linear_energies),
            "agentic_energy": calc_stats(agentic_energies),
            "linear_time_ms": calc_stats(linear_times),
            "agentic_time_ms": calc_stats(agentic_times),
            "orchestration_tax": calc_stats(taxes),
            "n_iterations": self.n,
            "task": task,
        }
