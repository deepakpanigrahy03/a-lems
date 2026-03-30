"""
gui/db_pg.py
────────────────────────────────────────────────────────────────────────────
PostgreSQL-only database layer for streamlit_server.py.
Native PG SQL — no adaptation layer, no SQLite fallback.
Import this ONLY from streamlit_server.py and server-mode pages.
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations
import os
import psycopg2
import psycopg2.extras
import pandas as pd
import streamlit as st


def _url() -> str:
    return os.environ["ALEMS_DB_URL"]


def _conn():
    return psycopg2.connect(_url())


def _conn_dict():
    return psycopg2.connect(_url(), cursor_factory=psycopg2.extras.RealDictCursor)


@st.cache_data(ttl=30, show_spinner=False)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    con = _conn()
    try:
        return pd.read_sql_query(sql, con, params=params or None)
    finally:
        con.close()


def q_safe(sql: str, params: tuple = ()) -> tuple[pd.DataFrame, str | None]:
    try:
        con = _conn()
        df = pd.read_sql_query(sql, con, params=params or None)
        con.close()
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)


@st.cache_data(ttl=30, show_spinner=False)
def q1(sql: str, params: tuple = ()) -> dict:
    try:
        con = _conn_dict()
        cur = con.cursor()
        cur.execute(sql, params or None)
        row = cur.fetchone()
        con.close()
        return dict(row) if row else {}
    except Exception:
        return {}


def execute(sql: str, params: tuple = ()) -> None:
    """Write query — no cache."""
    con = _conn()
    cur = con.cursor()
    cur.execute(sql, params or None)
    con.commit()
    con.close()


# ── Cached bulk loaders (native PG SQL) ──────────────────────────────────────

@st.cache_data(ttl=30, show_spinner=False)
def load_overview() -> dict:
    return q1("""
        SELECT
            COUNT(DISTINCT e.exp_id)  AS total_experiments,
            COUNT(r.run_id)           AS total_runs,
            SUM(CASE WHEN r.workflow_type='linear'  THEN 1 ELSE 0 END) AS linear_runs,
            SUM(CASE WHEN r.workflow_type='agentic' THEN 1 ELSE 0 END) AS agentic_runs,
            AVG(CASE WHEN r.workflow_type='linear'  THEN r.total_energy_uj END)/1e6 AS avg_linear_j,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.total_energy_uj END)/1e6 AS avg_agentic_j,
            MAX(r.total_energy_uj)/1e6  AS max_energy_j,
            MIN(r.total_energy_uj)/1e6  AS min_energy_j,
            SUM(r.total_energy_uj)/1e6  AS total_energy_j,
            AVG(r.ipc) AS avg_ipc, MAX(r.ipc) AS max_ipc,
            AVG(r.cache_miss_rate)*100  AS avg_cache_miss_pct,
            SUM(r.carbon_g)*1000        AS total_carbon_mg,
            AVG(r.carbon_g)*1000        AS avg_carbon_mg,
            AVG(r.water_ml)             AS avg_water_ml,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.planning_time_ms  END) AS avg_planning_ms,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.execution_time_ms END) AS avg_execution_ms,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.synthesis_time_ms END) AS avg_synthesis_ms
        FROM experiments e
        LEFT JOIN runs r ON e.exp_id = r.exp_id AND e.hw_id = r.hw_id
    """)


@st.cache_data(ttl=30, show_spinner=False)
def load_runs() -> pd.DataFrame:
    return q("""
        SELECT
            r.global_run_id, r.run_id, r.exp_id, r.hw_id,
            h.hostname,
            r.workflow_type, r.run_number,
            r.duration_ns / 1e6             AS duration_ms,
            r.total_energy_uj / 1e6         AS energy_j,
            r.dynamic_energy_uj / 1e6       AS dynamic_energy_j,
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
            r.experiment_valid, r.background_cpu_percent,
            r.bytes_sent, r.bytes_recv, r.tcp_retransmits,
            r.major_page_faults, r.minor_page_faults, r.page_faults
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id AND r.hw_id = e.hw_id
        LEFT JOIN hardware_config h ON r.hw_id = h.hw_id
        ORDER BY r.global_run_id DESC
    """)


@st.cache_data(ttl=30, show_spinner=False)
def load_machines() -> pd.DataFrame:
    return q("""
        SELECT hw_id, hostname, cpu_model, os_name, agent_status,
               last_seen, server_hw_id
        FROM hardware_config
        ORDER BY last_seen DESC NULLS LAST
    """)


@st.cache_data(ttl=30, show_spinner=False)
def load_coverage() -> pd.DataFrame:
    """Coverage matrix — live from PG (no cached table needed in PG)."""
    return q("""
        SELECT
            r.hw_id,
            h.hostname,
            e.model_name,
            e.task_name,
            e.workflow_type,
            COUNT(*) AS run_count,
            MAX(r.synced_at) AS last_updated
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id AND r.hw_id = e.hw_id
        LEFT JOIN hardware_config h ON r.hw_id = h.hw_id
        WHERE e.model_name IS NOT NULL
          AND e.task_name  IS NOT NULL
          AND e.workflow_type IS NOT NULL
        GROUP BY r.hw_id, h.hostname, e.model_name, e.task_name, e.workflow_type
        ORDER BY run_count ASC
    """)


@st.cache_data(ttl=30, show_spinner=False)
def load_stuck_experiments(stuck_mins: int = 30) -> pd.DataFrame:
    """Replaces the julianday() query from execute.py — native PG."""
    return q(f"""
        SELECT exp_id, task_name, provider, group_id,
               started_at, runs_completed, runs_total
        FROM experiments
        WHERE status = 'running'
          AND started_at IS NOT NULL
          AND EXTRACT(EPOCH FROM (NOW() - started_at)) / 60 > {stuck_mins}
        ORDER BY exp_id
    """)
