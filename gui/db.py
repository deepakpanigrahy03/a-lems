"""
gui/db.py
Database access layer — connection, query helpers, cached data loaders.
All other modules import from here; nothing else touches SQLite directly.
"""
import sqlite3
from contextlib import contextmanager

import pandas as pd
import streamlit as st

from gui.config import DB_PATH


# ── Low-level connection ───────────────────────────────────────────────────────
@contextmanager
def db():
    con = sqlite3.connect(str(DB_PATH), timeout=15)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    try:
        yield con
    finally:
        con.close()


# ── Query helpers ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=30, show_spinner=False)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Cached query — raises on error so callers see it."""
    with db() as con:
        return pd.read_sql_query(sql, con, params=params)


def q_safe(sql: str, params: tuple = ()) -> tuple:
    """Uncached query — returns (DataFrame, error_str). Use in UI pages."""
    with db() as con:
        try:
            return pd.read_sql_query(sql, con, params=params), None
        except Exception as _e:
            return pd.DataFrame(), str(_e)


@st.cache_data(ttl=30, show_spinner=False)
def q1(sql: str, params: tuple = ()) -> dict:
    """Cached single-row query — returns dict (empty on error)."""
    with db() as con:
        try:
            row = con.execute(sql, params).fetchone()
            return dict(row) if row else {}
        except Exception:
            return {}


# ── Cached bulk loaders ────────────────────────────────────────────────────────
@st.cache_data(ttl=30, show_spinner=False)
def load_overview() -> dict:
    return q1("""
        SELECT
            COUNT(DISTINCT e.exp_id) AS total_experiments,
            COUNT(r.run_id)          AS total_runs,
            SUM(CASE WHEN r.workflow_type='linear'  THEN 1 ELSE 0 END) AS linear_runs,
            SUM(CASE WHEN r.workflow_type='agentic' THEN 1 ELSE 0 END) AS agentic_runs,
            AVG(CASE WHEN r.workflow_type='linear'  THEN r.total_energy_uj END)/1e6 AS avg_linear_j,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.total_energy_uj END)/1e6 AS avg_agentic_j,
            MAX(r.total_energy_uj)/1e6 AS max_energy_j,
            MIN(r.total_energy_uj)/1e6 AS min_energy_j,
            SUM(r.total_energy_uj)/1e6 AS total_energy_j,
            AVG(r.ipc) AS avg_ipc, MAX(r.ipc) AS max_ipc,
            AVG(r.cache_miss_rate)*100 AS avg_cache_miss_pct,
            SUM(r.carbon_g)*1000 AS total_carbon_mg,
            AVG(r.carbon_g)*1000 AS avg_carbon_mg,
            AVG(r.water_ml) AS avg_water_ml,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.planning_time_ms  END) AS avg_planning_ms,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.execution_time_ms END) AS avg_execution_ms,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.synthesis_time_ms END) AS avg_synthesis_ms
        FROM experiments e LEFT JOIN runs r ON e.exp_id = r.exp_id
    """)


@st.cache_data(ttl=30, show_spinner=False)
def load_runs() -> pd.DataFrame:
    return q("""
        SELECT
            r.run_id, r.exp_id, r.workflow_type, r.run_number,
            r.duration_ns/1e6               AS duration_ms,
            r.total_energy_uj/1e6           AS energy_j,
            r.dynamic_energy_uj/1e6         AS dynamic_energy_j,
            r.ipc, r.cache_miss_rate, r.thread_migrations,
            r.context_switches_voluntary, r.context_switches_involuntary,
            r.total_context_switches, r.frequency_mhz,
            r.package_temp_celsius, r.thermal_delta_c, r.thermal_throttle_flag,
            r.interrupt_rate, r.api_latency_ms,
            r.planning_time_ms, r.execution_time_ms, r.synthesis_time_ms,
            r.llm_calls, r.tool_calls, r.total_tokens,
            r.complexity_level, r.complexity_score,
            r.carbon_g, r.water_ml,
            r.energy_per_token, r.energy_per_instruction,
            e.provider, e.country_code, e.model_name, e.task_name,
            r.governor, r.turbo_enabled,
            r.rss_memory_mb, r.vms_memory_mb,
            r.prompt_tokens, r.completion_tokens,
            r.dns_latency_ms, r.compute_time_ms,
            r.swap_total_mb, r.swap_start_used_mb,
            r.swap_end_used_mb, r.swap_end_percent,
            r.wakeup_latency_us, r.interrupts_per_second,
            r.instructions, r.cycles,
            r.start_time_ns, r.avg_power_watts,
            r.experiment_valid, r.background_cpu_percent
             
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        ORDER BY r.run_id DESC
    """)


@st.cache_data(ttl=30, show_spinner=False)
def load_tax() -> pd.DataFrame:
    return q("""
        SELECT
            ots.comparison_id, ots.linear_run_id, ots.agentic_run_id,
            ots.tax_percent,
            ots.orchestration_tax_uj/1e6 AS tax_j,
            ots.linear_dynamic_uj/1e6    AS linear_dynamic_j,
            ots.agentic_dynamic_uj/1e6   AS agentic_dynamic_j,
            ra.planning_time_ms, ra.execution_time_ms, ra.synthesis_time_ms,
            ra.llm_calls, ra.tool_calls, ra.total_tokens,
            el.task_name, el.country_code, el.provider
        FROM orchestration_tax_summary ots
        JOIN runs rl ON ots.linear_run_id  = rl.run_id
        JOIN runs ra ON ots.agentic_run_id = ra.run_id
        JOIN experiments el ON rl.exp_id = el.exp_id
        ORDER BY ots.tax_percent DESC
    """)
