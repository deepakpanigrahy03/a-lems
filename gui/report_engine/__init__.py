"""
A-LEMS Report Engine
Public API surface — import from here everywhere in the GUI.

Usage:
    from gui.report_engine import ReportRunner, GoalRegistry, ReportConfig
"""

from .models import (
    ResearchGoal, MetricSpec, EvalCriteria,
    ReportConfig, ReportFilter, SectionConfig,
    ReportRun, ReportNarrative, StatTestResult,
    SystemProfile, MetricSnapshot,
    GoalCategory, ReportType, StatTest,
    ConfidenceLevel, HypothesisVerdict, OutputFormat,
)
from .goal_registry import GoalRegistry, load_goal_from_yaml
from .report_runner import ReportRunner
from .system_profiler import collect_profile, get_or_collect_profile
from .stat_engine import run_all_tests, compute_confidence
from .data_fetcher import fetch_runs, get_available_filters, get_run_count

__all__ = [
    # Models
    "ResearchGoal", "MetricSpec", "EvalCriteria",
    "ReportConfig", "ReportFilter", "SectionConfig",
    "ReportRun", "ReportNarrative", "StatTestResult",
    "SystemProfile", "MetricSnapshot",
    "GoalCategory", "ReportType", "StatTest",
    "ConfidenceLevel", "HypothesisVerdict", "OutputFormat",
    # Services
    "GoalRegistry", "load_goal_from_yaml",
    "ReportRunner", "collect_profile", "get_or_collect_profile",
    "run_all_tests", "compute_confidence",
    "fetch_runs", "get_available_filters", "get_run_count",
]

VERSION = "1.0.0"
