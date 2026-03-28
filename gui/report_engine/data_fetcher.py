"""
gui/report_engine/data_fetcher.py
─────────────────────────────────────────────────────────────────────────────
Data fetcher — queries A-LEMS SQLite database.

Fix log:
  v1 → v2: e.status not r.status
  v2 → v3: _build_select/_build_base_query received SchemaDiscovery instance
            but called .has_table()/.has_column() which live on SchemaMap.
            Fixed by calling sd.ensure_discovered() which returns SchemaMap.
  v3 → v4: workflow_type lives in RUNS table not experiments.
            experiments.workflow_type = 'comparison' (experiment type).
            runs.workflow_type = 'linear' | 'agentic' (the field we filter on).
            Moved workflow_type out of _EXP_WANTED, all WHERE filters now
            correctly use r.workflow_type.
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import sqlite3, logging
import numpy as np
import pandas as pd
from pathlib import Path

from .models import ReportFilter, MetricSpec
from .schema_discovery import SchemaDiscovery, SchemaMap

log = logging.getLogger(__name__)

_DB_PATH = Path(__file__).parent.parent.parent / "data" / "experiments.db"

# ── Columns we want from each table (if they exist) ──────────────────────────
# Edit these lists as your schema evolves — never hardcode inside queries.

_RUNS_WANTED = [
    # Identity
    "run_id", "exp_id", "hw_id", "run_number",
    # ── THE key filter field — linear | agentic (lives in runs, not experiments)
    "workflow_type",
    # Energy
    "total_energy_uj", "dynamic_energy_uj", "baseline_energy_uj",
    "avg_power_watts", "pkg_energy_uj", "core_energy_uj",
    "uncore_energy_uj", "dram_energy_uj",
    # Performance
    "duration_ns", "instructions", "cycles", "ipc",
    "cache_misses", "cache_references", "cache_miss_rate",
    # Memory
    "page_faults", "major_page_faults", "minor_page_faults",
    "rss_memory_mb", "vms_memory_mb",
    "swap_start_used_mb", "swap_end_used_mb", "swap_end_percent",
    # CPU
    "compute_time_ms", "orchestration_cpu_ms",
    "kernel_time_ms", "user_time_ms",
    "frequency_mhz", "cpu_avg_mhz",
    "context_switches_voluntary", "context_switches_involuntary",
    "c2_time_seconds", "c6_time_seconds", "c7_time_seconds",
    # Thermal
    "package_temp_celsius", "max_temp_c", "min_temp_c",
    "thermal_delta_c", "thermal_throttle_flag",
    "thermal_during_experiment",
    # Tokens & LLM
    "total_tokens", "prompt_tokens", "completion_tokens",
    "energy_per_token", "llm_calls", "steps", "avg_step_time_ms",
    # Network
    "dns_latency_ms", "api_latency_ms",
    "bytes_sent", "bytes_recv", "tcp_retransmits",
    # Agentic phases
    "planning_time_ms", "execution_time_ms", "synthesis_time_ms",
    "phase_planning_ratio", "phase_execution_ratio", "phase_synthesis_ratio",
    # Environmental
    "carbon_g", "water_ml", "methane_mg",
    # Derived
    "energy_per_instruction", "energy_per_token", "instructions_per_token",
    "complexity_score", "experiment_valid",
    # Timing
    "start_time_ns", "end_time_ns",
    # ── Full schema — every remaining runs column
    "baseline_id",
    "baseline_temp_celsius",
    "start_temp_c",
    "c3_time_seconds",
    "complexity_level",
    "cpu_busy_mhz",
    "ring_bus_freq_mhz",
    "energy_per_cycle",
    "governor",
    "turbo_enabled",
    "is_cold_start",
    "interrupt_rate",
    "interrupts_per_second",
    "process_count",
    "background_cpu_percent",
    "run_queue_length",
    "run_state_hash",
    "swap_total_mb",
    "swap_start_cached_mb",
    "swap_end_free_mb",
    "swap_end_cached_mb",
    "thermal_now_active",
    "thermal_since_boot",
    "thread_migrations",
    "tool_calls",
    "tools_used",
    "total_context_switches",
    "wakeup_latency_us",
]

# workflow_type intentionally NOT here — lives in runs, not experiments
# experiments.workflow_type = 'comparison' (experiment type)
# runs.workflow_type         = 'linear' | 'agentic' (what we filter on)
# status IS in experiments (completed/error/failed/running) — used for filtering
_EXP_WANTED = [
    "provider", "model_name",
    "task_name", "group_id", "status",
]

_HW_WANTED = ["cpu_model", "cpu_cores", "ram_gb"]


# ── Internal builders — all take SchemaMap (not SchemaDiscovery) ──────────────

def _build_select(sm: SchemaMap) -> str:
    """
    Build SELECT clause from columns that actually exist in the live schema.
    sm must be a SchemaMap — call sd.get_map() before passing here.
    """
    runs_cols = sm.safe_columns("runs",            _RUNS_WANTED)
    exp_cols  = sm.safe_columns("experiments",     _EXP_WANTED)
    hw_cols   = sm.safe_columns("hardware_config", _HW_WANTED)

    parts = []
    for col in runs_cols:
        parts.append(f"    r.{col}")
    for col in exp_cols:
        parts.append(f"    e.{col}")
    for col in hw_cols:
        parts.append(f"    hw.{col}")

    # Always include these aggregated / joined columns
    parts += [
        "    COALESCE(li.total_wait_ms, 0)        AS total_wait_ms",
        "    COALESCE(li.total_llm_compute_ms, 0) AS total_llm_compute_ms",
        "    COALESCE(li.avg_api_latency_ms, 0)   AS avg_api_latency_ms",
        "    COALESCE(li.step_count, 0)            AS step_count",
        "    ot.tax_percent",
    ]

    # OOI/UCR only if the view exists
    if sm.has_table("research_metrics_view"):
        parts += [
            "    rmv.ooi_time",
            "    rmv.ooi_cpu",
            "    rmv.ucr",
            "    rmv.network_ratio",
        ]

    return ",\n".join(parts)


def _build_base_query(sm: SchemaMap) -> str:
    """
    Build the full base FROM/JOIN query.
    sm must be a SchemaMap — call sd.get_map() before passing here.
    """
    select = _build_select(sm)

    # Use e.status filter only if the column exists
    status_filter = ""
    if sm.has_column("experiments", "status"):
        status_filter = "AND e.status = 'completed'"
    else:
        log.debug("experiments.status not found — no status filter applied")

    # research_metrics_view LEFT JOIN only if view exists
    rmv_join = ""
    if sm.has_table("research_metrics_view"):
        rmv_join = "LEFT JOIN research_metrics_view rmv ON r.run_id = rmv.run_id"

    return f"""
SELECT
{select}
FROM runs r
JOIN experiments e ON r.exp_id = e.exp_id
LEFT JOIN hardware_config hw ON r.hw_id = hw.hw_id
LEFT JOIN (
    SELECT
        run_id,
        SUM(non_local_ms)     AS total_wait_ms,
        SUM(local_compute_ms) AS total_llm_compute_ms,
        AVG(api_latency_ms)   AS avg_api_latency_ms,
        COUNT(*)              AS step_count
    FROM llm_interactions
    GROUP BY run_id
) li ON r.run_id = li.run_id
LEFT JOIN orchestration_tax_summary ot
    ON (r.run_id = ot.linear_run_id OR r.run_id = ot.agentic_run_id)
{rmv_join}
WHERE 1=1
{status_filter}
"""


def _build_where(f: ReportFilter) -> tuple[str, list]:
    """Build WHERE clause additions from filter object."""
    clauses, params = [], []

    if f.workflow_types:
        ph = ",".join("?" * len(f.workflow_types))
        clauses.append(f"AND r.workflow_type IN ({ph})")
        params.extend(f.workflow_types)

    if f.providers:
        ph = ",".join("?" * len(f.providers))
        clauses.append(f"AND e.provider IN ({ph})")
        params.extend(f.providers)

    if f.models:
        ph = ",".join("?" * len(f.models))
        clauses.append(f"AND e.model_name IN ({ph})")
        params.extend(f.models)

    if f.task_names:
        ph = ",".join("?" * len(f.task_names))
        clauses.append(f"AND e.task_name IN ({ph})")
        params.extend(f.task_names)

    if f.date_from:
        clauses.append("AND r.started_at >= ?")
        params.append(f.date_from)

    if f.date_to:
        clauses.append("AND r.started_at <= ?")
        params.append(f.date_to)

    if f.min_energy_uj is not None:
        clauses.append("AND r.total_energy_uj >= ?")
        params.append(f.min_energy_uj)

    if f.max_energy_uj is not None:
        clauses.append("AND r.total_energy_uj <= ?")
        params.append(f.max_energy_uj)

    return " ".join(clauses), params


def _tag_exclusion(
    filters: ReportFilter,
    conn: sqlite3.Connection,
) -> tuple[str, list]:
    """Add tag exclusion if the tags table exists."""
    try:
        conn.execute("SELECT 1 FROM tags LIMIT 1")
        if filters.exclude_tags:
            ph = ",".join("?" * len(filters.exclude_tags))
            return (
                f"AND r.run_id NOT IN "
                f"(SELECT run_id FROM tags WHERE label IN ({ph}))",
                list(filters.exclude_tags),
            )
    except sqlite3.OperationalError:
        pass
    return "", []


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def fetch_runs(
    db_path: str | Path,
    filters: ReportFilter,
) -> pd.DataFrame:
    """
    Fetch filtered runs from the A-LEMS database.
    Schema-safe: only requests columns that exist in the live DB.
    workflow_types filter is only applied if the list is non-empty.
    """
    db_path = Path(db_path)

    # Get SchemaMap (not SchemaDiscovery) — this is what builders need
    sd = SchemaDiscovery.get()
    sm: SchemaMap = sd.ensure_discovered(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        base_sql               = _build_base_query(sm)
        where_sql, where_params = _build_where(filters)
        tag_sql,   tag_params   = _tag_exclusion(filters, conn)

        full_sql = base_sql + "\n" + where_sql + "\n" + tag_sql
        params   = where_params + tag_params

        log.debug(f"fetch_runs SQL:\n{full_sql}\nparams: {params}")

        df = pd.read_sql_query(full_sql, conn, params=params)

        # Baseline-adjusted energy column
        try:
            row = conn.execute(
                "SELECT AVG(package_power_watts) FROM idle_baselines LIMIT 1"
            ).fetchone()
            baseline_w = row[0] if row and row[0] else 0.0
        except Exception:
            baseline_w = 0.0

        if (baseline_w > 0
                and "duration_ns" in df.columns
                and "total_energy_uj" in df.columns):
            df["idle_baseline_uj"] = (
                baseline_w * (df["duration_ns"] / 1e9) * 1e6
            )
        else:
            df["idle_baseline_uj"] = 0.0

        log.info(f"fetch_runs: {len(df)} rows × {df.shape[1]} cols")
        return df

    finally:
        conn.close()


def get_run_count(
    db_path: str | Path,
    filters: ReportFilter,
) -> int:
    """
    Fast run count without fetching all columns.
    workflow_types filter only applied when list is non-empty.
    """
    db_path = Path(db_path)
    sd = SchemaDiscovery.get()
    sm: SchemaMap = sd.ensure_discovered(db_path)

    where_parts = ["WHERE 1=1"]
    params: list = []

    # Status filter — only if column exists
    if sm.has_column("experiments", "status"):
        where_parts.append("AND e.status = 'completed'")

    # workflow_type — only filter if caller passed actual values
    if filters.workflow_types:
        ph = ",".join("?" * len(filters.workflow_types))
        where_parts.append(f"AND r.workflow_type IN ({ph})")
        params.extend(filters.workflow_types)

    if filters.providers:
        ph = ",".join("?" * len(filters.providers))
        where_parts.append(f"AND e.provider IN ({ph})")
        params.extend(filters.providers)

    if filters.models:
        ph = ",".join("?" * len(filters.models))
        where_parts.append(f"AND e.model_name IN ({ph})")
        params.extend(filters.models)

    if filters.task_names:
        ph = ",".join("?" * len(filters.task_names))
        where_parts.append(f"AND e.task_name IN ({ph})")
        params.extend(filters.task_names)

    sql = f"""
        SELECT COUNT(*)
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        {' '.join(where_parts)}
    """
    try:
        conn = sqlite3.connect(str(db_path))
        result = conn.execute(sql, params).fetchone()[0]
        conn.close()
        log.debug(f"get_run_count: {result} runs (filters={filters})")
        return result
    except Exception as e:
        log.error(f"get_run_count error: {e}\nSQL: {sql}")
        return 0


def get_available_filters(db_path: str | Path) -> dict[str, list]:
    """
    Return distinct values for UI filter dropdowns.
    Reads actual values from your DB — no hardcoding.
    """
    db_path = Path(db_path)
    sd = SchemaDiscovery.get()
    sm: SchemaMap = sd.ensure_discovered(db_path)

    result: dict[str, list] = {
        "workflow_types": [],
        "providers":      [],
        "models":         [],
        "task_names":     [],
    }

    try:
        conn = sqlite3.connect(str(db_path))
        # workflow_type comes from runs; others from experiments
        if sm.has_column("runs", "workflow_type"):
            rows = conn.execute(
                "SELECT DISTINCT workflow_type FROM runs "
                "WHERE workflow_type IS NOT NULL ORDER BY workflow_type"
            ).fetchall()
            result["workflow_types"] = [r[0] for r in rows if r[0]]

        for col, key in [
            ("provider",   "providers"),
            ("model_name", "models"),
            ("task_name",  "task_names"),
        ]:
            if sm.has_column("experiments", col):
                rows = conn.execute(
                    f"SELECT DISTINCT {col} FROM experiments "
                    f"WHERE {col} IS NOT NULL ORDER BY {col}"
                ).fetchall()
                result[key] = [r[0] for r in rows if r[0]]
        conn.close()
    except Exception as e:
        log.error(f"get_available_filters error: {e}")

    return result


def evaluate_formula(df: pd.DataFrame, formula: str) -> pd.Series:
    """Safely evaluate a metric formula string against the dataframe."""
    import re
    expr = formula
    expr = re.sub(
        r"NULLIF\((\w+),\s*0\)",
        r"df['\1'].replace(0, float('nan'))",
        expr,
    )
    for col in sorted(df.columns, key=len, reverse=True):
        expr = re.sub(
            rf"(?<!\[')(?<!\w){re.escape(col)}(?!\w)(?!'\])",
            f"df['{col}']",
            expr,
        )
    try:
        result = eval(expr, {"df": df, "np": np, "float": float})
        if isinstance(result, (int, float)):
            return pd.Series([result] * len(df), index=df.index)
        return result.astype(float)
    except Exception as e:
        log.warning(f"Formula eval failed '{formula}': {e}")
        return pd.Series([np.nan] * len(df), index=df.index)


def apply_metrics(df: pd.DataFrame, metrics: list[MetricSpec]) -> pd.DataFrame:
    """Add computed metric columns to the dataframe."""
    for m in metrics:
        out_col = f"_metric_{m.column}"
        if m.formula:
            df[out_col] = evaluate_formula(df, m.formula)
        elif m.column in df.columns:
            df[out_col] = df[m.column]
        else:
            log.debug(f"Metric column not in df: {m.column}")
            df[out_col] = np.nan
    return df
