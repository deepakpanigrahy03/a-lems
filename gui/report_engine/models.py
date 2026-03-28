"""
A-LEMS Report Engine — Core Data Models
All dataclasses / enums used across the engine. No external deps.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
import datetime


# ── Enumerations ─────────────────────────────────────────────────────────────

class GoalCategory(str, Enum):
    EFFICIENCY   = "efficiency"
    LATENCY      = "latency"
    COST         = "cost"
    THERMAL      = "thermal"
    QUALITY      = "quality"
    NETWORK      = "network"
    MEMORY       = "memory"
    TOKENS       = "tokens"
    CARBON       = "carbon"
    COMPARATIVE  = "comparative"
    CUSTOM       = "custom"


class ReportType(str, Enum):
    GOAL         = "goal"          # What are we optimising?
    PROBLEM      = "problem"       # What is wrong?
    HYPOTHESIS   = "hypothesis"    # Is this assumption valid?
    EXPLORATORY  = "exploratory"   # What insights emerge?


class StatTest(str, Enum):
    T_TEST       = "t_test"
    MANN_WHITNEY = "mann_whitney"
    BOOTSTRAP    = "bootstrap"
    WILCOXON     = "wilcoxon"


class ConfidenceLevel(str, Enum):
    HIGH    = "HIGH"
    MEDIUM  = "MEDIUM"
    LOW     = "LOW"


class HypothesisVerdict(str, Enum):
    SUPPORTED     = "SUPPORTED"
    REJECTED      = "REJECTED"
    INCONCLUSIVE  = "INCONCLUSIVE"
    INSUFFICIENT  = "INSUFFICIENT_DATA"


class EffectSizeLabel(str, Enum):
    NEGLIGIBLE = "negligible"
    SMALL      = "small"
    MEDIUM     = "medium"
    LARGE      = "large"


class EnvType(str, Enum):
    LOCAL   = "LOCAL"
    DOCKER  = "DOCKER"
    VM      = "VM"
    CLOUD   = "CLOUD"


class OutputFormat(str, Enum):
    PDF  = "pdf"
    HTML = "html"
    JSON = "json"


# ── Metric Specification ──────────────────────────────────────────────────────

@dataclass
class MetricSpec:
    name: str                          # display name: "Total Energy"
    column: str                        # DB column or derived key
    unit: str                          # "µJ", "ms", "bytes", "%", "tokens"
    direction: str                     # "lower_is_better" | "higher_is_better"
    display_precision: int = 2
    baseline_col: str | None = None    # column for baseline-adjusted view
    alert_threshold: float | None = None
    formula: str | None = None         # e.g. "orchestration_cpu_ms / compute_time_ms"
    description: str = ""


@dataclass
class Threshold:
    warn: float
    severe: float
    unit: str = ""


@dataclass
class EvalCriteria:
    stat_test: StatTest = StatTest.MANN_WHITNEY
    alpha: float = 0.05
    effect_size_measure: str = "cohens_d"   # "cohens_d" | "eta_squared"
    min_runs_per_group: int = 5
    bootstrap_n: int = 5000
    report_ci: bool = True
    ci_level: float = 0.95
    comparison_mode: str = "relative"       # "absolute" | "relative" | "baseline"


# ── Research Goal ─────────────────────────────────────────────────────────────

@dataclass
class ResearchGoal:
    goal_id: str
    name: str
    category: GoalCategory
    description: str
    hypothesis: str | None
    metrics: list[MetricSpec]
    thresholds: dict[str, Threshold] = field(default_factory=dict)
    eval_criteria: EvalCriteria = field(default_factory=EvalCriteria)
    narrative_persona: str = "research_engineer"
    doc_sections: list[str] = field(default_factory=list)
    diagram_ids: list[str] = field(default_factory=list)
    report_sections: list[str] = field(default_factory=list)
    version: str = "1.0.0"
    tags: list[str] = field(default_factory=list)


# ── System Profile ────────────────────────────────────────────────────────────

@dataclass
class SystemProfile:
    profile_id: str
    cpu_model: str
    cpu_cores_physical: int
    cpu_cores_logical: int
    cpu_freq_max_mhz: float
    ram_gb: float
    env_type: EnvType
    os_name: str
    kernel: str
    rapl_zones: list[str]
    gpu_model: str | None
    thermal_tdp_w: float | None
    disk_gb: float | None
    collected_at: datetime.datetime = field(default_factory=datetime.datetime.utcnow)

    def summary_line(self) -> str:
        return (
            f"{self.cpu_model} · {self.cpu_cores_logical} cores · "
            f"{self.ram_gb:.0f} GB RAM · {self.env_type.value}"
        )


# ── Statistical Results ───────────────────────────────────────────────────────

@dataclass
class StatTestResult:
    test_name: str
    metric_name: str
    group_a_label: str
    group_b_label: str
    group_a_n: int
    group_b_n: int
    group_a_mean: float
    group_b_mean: float
    group_a_median: float
    group_b_median: float
    group_a_std: float
    group_b_std: float
    statistic: float
    p_value: float
    effect_size: float
    effect_label: EffectSizeLabel
    ci_low: float
    ci_high: float
    ci_level: float
    significant: bool
    unit: str = ""
    notes: str = ""

    def pct_difference(self) -> float:
        if self.group_a_mean == 0:
            return 0.0
        return ((self.group_b_mean - self.group_a_mean) / abs(self.group_a_mean)) * 100


@dataclass
class MetricSnapshot:
    """Computed metric values ready for report consumption."""
    goal_id: str
    run_count: int
    workflow_types: list[str]
    metric_rows: list[dict[str, Any]]      # one dict per metric
    stat_results: list[StatTestResult]
    raw_df_json: str                        # serialised for storage
    computed_at: datetime.datetime = field(default_factory=datetime.datetime.utcnow)


# ── Narrative ─────────────────────────────────────────────────────────────────

@dataclass
class ReportNarrative:
    executive_summary: str
    key_findings: list[str]                # 3–5 bullet points
    anomaly_flags: list[str]
    hypothesis_verdict: HypothesisVerdict
    verdict_explanation: str
    recommendations: list[str]
    limitations: list[str]
    confidence_level: ConfidenceLevel
    confidence_rationale: str
    section_narratives: dict[str, str]     # section_id → paragraph


# ── Report Config (parsed from YAML) ─────────────────────────────────────────

@dataclass
class ReportFilter:
    workflow_types: list[str] = field(default_factory=lambda: ["linear", "agentic"])
    providers: list[str] = field(default_factory=list)
    models: list[str] = field(default_factory=list)
    task_names: list[str] = field(default_factory=list)
    min_runs: int = 5
    exclude_tags: list[str] = field(default_factory=lambda: ["exclude"])
    date_from: str | None = None
    date_to: str | None = None
    min_energy_uj: float | None = None
    max_energy_uj: float | None = None


@dataclass
class SectionConfig:
    section_id: str
    enabled: bool = True
    docs: list[str] = field(default_factory=list)        # mkdocs keys
    diagrams: list[str] = field(default_factory=list)    # diagram IDs
    metrics: list[str] = field(default_factory=list)     # metric names
    charts: list[dict] = field(default_factory=list)     # chart specs
    custom_text: str | None = None


@dataclass
class ReportConfig:
    report_id: str
    title: str
    goal_id: str
    report_type: ReportType
    filters: ReportFilter
    sections: list[SectionConfig]
    secondary_goal_ids: list[str] = field(default_factory=list)
    stat_test: StatTest = StatTest.MANN_WHITNEY
    alpha: float = 0.05
    narrative_persona: str | None = None   # override goal default
    output_formats: list[OutputFormat] = field(default_factory=lambda: [OutputFormat.PDF, OutputFormat.HTML])
    pdf_paper: str = "A4"
    pdf_watermark: str | None = None
    include_toc: bool = True
    interactive_charts: bool = True
    version: str = "1.0.0"


# ── Report Run (persisted) ────────────────────────────────────────────────────

@dataclass
class ReportRun:
    report_id: str
    goal_id: str
    report_type: str
    title: str
    run_filter_json: str
    config_yaml: str
    narrative_json: str
    stat_results_json: str
    confidence_level: str
    output_paths: dict[str, str]           # format → file path
    generated_at: datetime.datetime
    confidence_rationale: str = ""
    hypothesis_verdict: str = ""
    generator_version: str = "1.0.0"
    reproducibility_hash: str = ""         # SHA256(config + data_version)
    run_count: int = 0
    notes: str = ""
