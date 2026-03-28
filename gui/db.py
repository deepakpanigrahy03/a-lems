"""
gui/db.py
────────────────────────────────────────────────────────────────────────────
Database access layer — dialect-aware connection, query helpers, data loaders.

Mode detection (automatic, no config needed):
  ALEMS_DB_URL starts with "postgresql://"  →  server mode  →  PostgreSQL
  No ALEMS_DB_URL                           →  local mode   →  SQLite

All existing queries work unchanged on both dialects because column names
are identical between SQLite and PostgreSQL.

Note on SQLite-specific functions:
  strftime(), datetime() etc. will fail on PostgreSQL.
  The Research query engine page should show a warning in server mode.
  Standard pages (energy, runs, experiments) use only standard SQL.
────────────────────────────────────────────────────────────────────────────
"""

import os
import sqlite3
from contextlib import contextmanager

import pandas as pd
import streamlit as st

from gui.config import DB_PATH

# ── Mode detection ────────────────────────────────────────────────────────────

def _db_url() -> str:
    return os.environ.get("ALEMS_DB_URL", "")

def is_server_mode() -> bool:
    return _db_url().startswith("postgresql")

def _adapt_sql(sql: str) -> str:
    """
    Adapt SQLite-specific SQL to PostgreSQL.
    Called automatically when is_server_mode() is True.
    """
    if not is_server_mode():
        return sql
    import re
    # ROUND(expr, n) → ROUND(expr::numeric, n)
    sql = re.sub(
        r'ROUND\(([^,]+),\s*(\d+)\)',
        lambda m: f'ROUND(({m.group(1)})::numeric, {m.group(2)})',
        sql
    )
    # strftime('%Y-%m', datetime(col/1e9,'unixepoch')) → to_char(to_timestamp(col/1e9),'YYYY-MM')
    sql = re.sub(
        r"strftime\('([^']+)',\s*datetime\(([^,]+),\s*'unixepoch'\)\)",
        lambda m: f"to_char(to_timestamp({m.group(2)}), '{m.group(1).replace('%Y','YYYY').replace('%m','MM').replace('%d','DD')}')",
        sql
    )
    # datetime(col/1e9,'unixepoch') → to_timestamp(col/1e9)
    sql = re.sub(
        r"datetime\(([^,]+),\s*'unixepoch'\)",
        lambda m: f"to_timestamp({m.group(1)})",
        sql
    )
    # CAST(x AS REAL) → CAST(x AS DOUBLE PRECISION)
    sql = sql.replace("CAST(", "CAST(").replace("AS REAL)", "AS DOUBLE PRECISION)")
    return sql

def get_db_label() -> str:
    return "PostgreSQL · server" if is_server_mode() else "SQLite · local"


# ── Low-level connection ───────────────────────────────────────────────────────

@contextmanager
def db():
    """
    Yields an active database connection.
    SQLite on local, PostgreSQL on server.
    Both support .execute() and pd.read_sql_query().
    """
    if is_server_mode():
        yield from _pg_connection()
    else:
        yield from _sqlite_connection()


@contextmanager
def _sqlite_connection():
    con = sqlite3.connect(str(DB_PATH), timeout=15)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    try:
        yield con
    finally:
        con.close()


@contextmanager
def _pg_connection():
    """PostgreSQL connection via psycopg2."""
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        raise RuntimeError(
            "psycopg2 not installed. Run: pip install psycopg2-binary"
        )

    url = _db_url()
    # Parse postgresql://user:pass@host/dbname
    con = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
    con.autocommit = True
    try:
        yield _PgWrapper(con)
    finally:
        con.close()


class _PgWrapper:
    """
    Wraps psycopg2 connection to match sqlite3 interface used by callers.
    Supports: .execute(sql, params) → cursor with .fetchone()/.fetchall()
    Supports: pd.read_sql_query(sql, wrapper)  via __enter__/__exit__
    """
    def __init__(self, con):
        self._con = con

    def execute(self, sql: str, params: tuple = ()):
        # Convert SQLite ? placeholders to PostgreSQL %s
        sql_pg = _adapt_sql(sql).replace("?", "%s")
        cur = self._con.cursor()
        cur.execute(sql_pg, params)
        return _PgCursor(cur)

    def cursor(self):
        return self._con.cursor()

    # Allow pd.read_sql_query(sql, pg_wrapper) to work
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class _PgCursor:
    """Wraps psycopg2 cursor to match sqlite3.Row interface."""
    def __init__(self, cur):
        self._cur = cur

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        return dict(row)

    def fetchall(self):
        return [dict(r) for r in self._cur.fetchall()]

    def __iter__(self):
        return iter(self.fetchall())


# ── Query helpers ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=30, show_spinner=False)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Cached query — returns DataFrame. Works on both SQLite and PostgreSQL."""
    if is_server_mode():
        try:
            import psycopg2
            url = _db_url()
            con = psycopg2.connect(url)
            sql_pg = _adapt_sql(sql).replace("?", "%s")
            df = pd.read_sql_query(sql_pg, con, params=params or None)
            con.close()
            return df
        except Exception as e:
            st.error(f"PostgreSQL query error: {e}")
            return pd.DataFrame()
    else:
        con = sqlite3.connect(str(DB_PATH), timeout=15)
        try:
            return pd.read_sql_query(sql, con, params=params)
        finally:
            con.close()


def q_safe(sql: str, params: tuple = ()) -> tuple:
    """Uncached query — returns (DataFrame, error_str). Use in UI pages."""
    try:
        return q(sql, params), None
    except Exception as e:
        return pd.DataFrame(), str(e)


@st.cache_data(ttl=30, show_spinner=False)
def q1(sql: str, params: tuple = ()) -> dict:
    """Cached single-row query — returns dict (empty on error)."""
    if is_server_mode():
        try:
            import psycopg2
            import psycopg2.extras
            url = _db_url()
            con = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
            cur = con.cursor()
            sql_pg = _adapt_sql(sql).replace("?", "%s")
            cur.execute(sql_pg, params or None)
            row = cur.fetchone()
            con.close()
            return dict(row) if row else {}
        except Exception as e:
            return {}
    else:
        con = sqlite3.connect(str(DB_PATH), timeout=15)
        con.row_factory = sqlite3.Row
        try:
            row = con.execute(sql, params).fetchone()
            return dict(row) if row else {}
        except Exception:
            return {}
        finally:
            con.close()


# ── Cached bulk loaders ───────────────────────────────────────────────────────
# These use only standard SQL — work on both SQLite and PostgreSQL unchanged.

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
        WHERE r.workflow_type IN ('linear', 'agentic')
    """)


@st.cache_data(ttl=30, show_spinner=False)
def load_runs() -> pd.DataFrame:
    return q("""
        SELECT
            r.run_id, r.exp_id, r.hw_id, r.workflow_type, r.run_number,
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
            r.experiment_valid, r.background_cpu_percent,
            r.bytes_sent, r.bytes_recv, r.tcp_retransmits,
            r.major_page_faults, r.minor_page_faults, r.page_faults
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        WHERE r.workflow_type IN ('linear', 'agentic')
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
