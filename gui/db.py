"""
gui/db.py
────────────────────────────────────────────────────────────────────────────
Database access layer — dialect-aware, works on SQLite and PostgreSQL.

Mode detection (automatic):
  ALEMS_DB_URL=postgresql://...  →  server mode  →  PostgreSQL
  no ALEMS_DB_URL                →  local mode   →  SQLite

SQL compatibility strategy:
  _adapt_sql() transforms SQLite SQL to PostgreSQL before execution.
  Transformations applied:
    ROUND(expr, n)                    → ROUND(CAST(expr AS NUMERIC), n)
    CAST(x AS REAL)                   → CAST(x AS DOUBLE PRECISION)
    AS REAL                           → AS DOUBLE PRECISION
    datetime('now',...)               → NOW() - INTERVAL ...
    datetime(x,'unixepoch')           → to_timestamp(x)
    datetime(col)                     → col  (bare cast, PG col already typed)
    strftime(fmt, ...)                → to_char(to_timestamp(...), fmt)
    julianday('now') - julianday(col) → EXTRACT(EPOCH FROM (NOW()-col::timestamp))/86400
    ? params                          → %s params
────────────────────────────────────────────────────────────────────────────
"""

import os
import re
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


def get_db_label() -> str:
    return "PostgreSQL · server" if is_server_mode() else "SQLite · local"


# ── SQL dialect adapter ───────────────────────────────────────────────────────

def _fix_round(sql: str) -> str:
    """
    Replace ROUND(expr, n) with ROUND(CAST(expr AS NUMERIC), n).
    Uses parenthesis counting to handle any level of nesting correctly.
    Works on both SQLite and PostgreSQL.
    """
    result = []
    i = 0
    upper = sql.upper()
    while i < len(sql):
        if upper[i:i+6] == 'ROUND(' :
            result.append('ROUND(CAST(')
            i += 6
            depth  = 1
            inner  = []
            while i < len(sql) and depth > 0:
                c = sql[i]
                if c == '(':
                    depth += 1
                elif c == ')':
                    depth -= 1
                    if depth == 0:
                        break
                inner.append(c)
                i += 1
            i += 1  # skip closing )

            inner_str = ''.join(inner)

            # Find last comma at depth 0 — separates expr from precision
            d, last_comma = 0, -1
            for j, c in enumerate(inner_str):
                if c == '(':
                    d += 1
                elif c == ')':
                    d -= 1
                elif c == ',' and d == 0:
                    last_comma = j

            if last_comma >= 0:
                expr      = inner_str[:last_comma].strip()
                precision = inner_str[last_comma + 1:].strip()
                result.append(f'{expr} AS NUMERIC), {precision})')
            else:
                # ROUND with single arg — no precision
                result.append(f'{inner_str} AS NUMERIC))')
        else:
            result.append(sql[i])
            i += 1

    return ''.join(result)


def _adapt_sql(sql: str) -> str:
    """
    Transform SQLite SQL to PostgreSQL-compatible SQL.
    Only called when is_server_mode() is True.
    Safe to call — returns sql unchanged if not server mode.
    """
    if not is_server_mode():
        return sql

    # 1. CAST(x AS REAL) → CAST(x AS DOUBLE PRECISION)
    sql = re.sub(r'\bAS\s+REAL\b', 'AS DOUBLE PRECISION', sql, flags=re.IGNORECASE)

    # 2. ROUND(expr, n) → ROUND(CAST(expr AS NUMERIC), n)
    sql = _fix_round(sql)

    # 3. datetime('now', '-N minutes/hours/days')
    #    → NOW() - INTERVAL 'N minutes'
    def _now_interval(m: re.Match) -> str:
        sign  = '-' if '-' in m.group(1) else '+'
        parts = m.group(1).strip().strip("'+-").split()
        n, unit = parts[0], parts[1] if len(parts) > 1 else 'seconds'
        return f"(NOW() {sign} INTERVAL '{n} {unit}')"

    sql = re.sub(
        r"datetime\(\s*'now'\s*,\s*'([^']+)'\s*\)",
        _now_interval,
        sql, flags=re.IGNORECASE,
    )

    # 4. datetime(expr, 'unixepoch') → to_timestamp(expr)
    sql = re.sub(
        r"datetime\(([^,)]+),\s*'unixepoch'\)",
        lambda m: f"to_timestamp({m.group(1).strip()})",
        sql, flags=re.IGNORECASE,
    )

    # 5. strftime('%Y-%m', expr) → to_char(expr, 'YYYY-MM')
    _fmt_map = {
        '%Y': 'YYYY', '%m': 'MM', '%d': 'DD',
        '%H': 'HH24', '%M': 'MI', '%S': 'SS',
    }
    def _strftime(m: re.Match) -> str:
        fmt = m.group(1)
        for k, v in _fmt_map.items():
            fmt = fmt.replace(k, v)
        return f"to_char({m.group(2).strip()}, '{fmt}')"

    sql = re.sub(
        r"strftime\(\s*'([^']+)'\s*,\s*([^)]+)\)",
        _strftime,
        sql, flags=re.IGNORECASE,
    )

    # 6. julianday('now') - julianday(col)  →  EXTRACT(EPOCH FROM (NOW() - col::timestamp)) / 86400
    #    Pattern: (julianday('now') - julianday(expr)) * N
    #    Semantics: difference in days * N  (execute.py uses * 1440 for minutes)
    #    PG equivalent: EXTRACT(EPOCH FROM (NOW() - expr::timestamp)) / 86400 * N
    def _julianday_diff(m: re.Match) -> str:
        expr = m.group(1).strip()
        return f"(EXTRACT(EPOCH FROM (NOW() - ({expr})::timestamp)) / 86400.0)"

    sql = re.sub(
        r"julianday\(\s*'now'\s*\)\s*-\s*julianday\(\s*([^)]+)\s*\)",
        _julianday_diff,
        sql, flags=re.IGNORECASE,
    )

    # 7. bare datetime(expr) cast — SQLite uses datetime(x) to normalise strings/timestamps.
    #    In PG the column is already a proper timestamp type, so strip the wrapper.
    #    Must run AFTER rules 3 & 4 so only bare (no second arg) calls remain.
    #    Use paren-counting so COALESCE(...) and other nested calls are handled correctly.
    def _strip_bare_datetime(sql: str) -> str:
        result = []
        i = 0
        upper = sql.upper()
        tag = 'DATETIME('
        while i < len(sql):
            if upper[i:i+len(tag)] == tag:
                # Check there is no 'unixepoch' / interval second arg (those were handled above)
                # Scan inner content
                inner_start = i + len(tag)
                depth = 1
                j = inner_start
                while j < len(sql) and depth > 0:
                    if sql[j] == '(':
                        depth += 1
                    elif sql[j] == ')':
                        depth -= 1
                    j += 1
                inner = sql[inner_start:j - 1]
                # Only strip if no second argument at depth-0 (no comma at depth 0)
                d, has_arg = 0, False
                for c in inner:
                    if c == '(':
                        d += 1
                    elif c == ')':
                        d -= 1
                    elif c == ',' and d == 0:
                        has_arg = True
                        break
                if has_arg:
                    # Has a second arg — already handled (unixepoch/interval) or unknown; keep
                    result.append(sql[i:j])
                else:
                    # Bare datetime(expr) → just expr
                    result.append(inner)
                i = j
            else:
                result.append(sql[i])
                i += 1
        return ''.join(result)

    sql = _strip_bare_datetime(sql)

    # 8. SQLite string concat with || when mixing types — keep as-is (PG supports ||)

    # 9. ? → %s  (parameter placeholder)
    sql = sql.replace('?', '%s')

    return sql


# ── Low-level connections ─────────────────────────────────────────────────────

@contextmanager
def _sqlite_connection():
    con = sqlite3.connect(str(DB_PATH), timeout=15)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    try:
        yield con
    finally:
        con.close()


def _pg_connect():
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        raise RuntimeError("psycopg2 not installed: pip install psycopg2-binary")
    return psycopg2.connect(_db_url())


def _pg_connect_dict():
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        raise RuntimeError("psycopg2 not installed: pip install psycopg2-binary")
    return psycopg2.connect(_db_url(), cursor_factory=psycopg2.extras.RealDictCursor)


@contextmanager
def db():
    """Yields connection — SQLite locally, PostgreSQL on server."""
    if is_server_mode():
        con = _pg_connect_dict()
        con.autocommit = True
        try:
            yield _PgWrapper(con)
        finally:
            con.close()
    else:
        with _sqlite_connection() as con:
            yield con


class _PgWrapper:
    """Wraps psycopg2 connection to match sqlite3 interface."""
    def __init__(self, con):
        self._con = con

    def execute(self, sql: str, params=()):
        sql_pg = _adapt_sql(sql)
        cur = self._con.cursor()
        cur.execute(sql_pg, params or None)
        return _PgCursor(cur)

    def cursor(self):
        return self._con.cursor()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


class _PgCursor:
    """Wraps psycopg2 cursor to match sqlite3.Row interface."""
    def __init__(self, cur):
        self._cur = cur

    def fetchone(self):
        row = self._cur.fetchone()
        return dict(row) if row else None

    def fetchall(self):
        return [dict(r) for r in self._cur.fetchall()]

    def __iter__(self):
        return iter(self.fetchall())


# ── Query helpers ─────────────────────────────────────────────────────────────

def _pg_engine():
    """SQLAlchemy engine for PostgreSQL — avoids pandas psycopg2 warning."""
    try:
        from sqlalchemy import create_engine
        return create_engine(_db_url(), pool_pre_ping=True)
    except Exception as e:
        raise RuntimeError(f"SQLAlchemy engine failed: {e}")


@st.cache_data(ttl=30, show_spinner=False)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Cached query → DataFrame. Dialect-aware."""
    if is_server_mode():
        try:
            from sqlalchemy import text as _text
            engine = _pg_engine()
            sql_pg = _adapt_sql(sql)
            with engine.connect() as con:
                df = pd.read_sql_query(_text(sql_pg), con, params=params or None)
            return df
        except Exception as e:
            st.error(f"Query error: {e}")
            return pd.DataFrame()
    else:
        con = sqlite3.connect(str(DB_PATH), timeout=15)
        try:
            return pd.read_sql_query(sql, con, params=params)
        finally:
            con.close()


def q_safe(sql: str, params: tuple = ()) -> tuple[pd.DataFrame, str | None]:
    """Uncached query → (DataFrame, error). Use in UI pages."""
    if is_server_mode():
        try:
            from sqlalchemy import text as _text
            engine = _pg_engine()
            sql_pg = _adapt_sql(sql)
            with engine.connect() as con:
                df = pd.read_sql_query(_text(sql_pg), con, params=params or None)
            return df, None
        except Exception as e:
            return pd.DataFrame(), str(e)
    else:
        con = sqlite3.connect(str(DB_PATH), timeout=15)
        try:
            return pd.read_sql_query(sql, con, params=params), None
        except Exception as e:
            return pd.DataFrame(), str(e)
        finally:
            con.close()


@st.cache_data(ttl=30, show_spinner=False)
def q1(sql: str, params: tuple = ()) -> dict:
    """Cached single-row query → dict. Dialect-aware."""
    if is_server_mode():
        try:
            con    = _pg_connect_dict()
            cur    = con.cursor()
            sql_pg = _adapt_sql(sql)
            cur.execute(sql_pg, params or None)
            row = cur.fetchone()
            con.close()
            return dict(row) if row else {}
        except Exception:
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
# Standard SQL only — no dialect-specific functions needed here.

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
            AVG(r.ipc)                  AS avg_ipc,
            MAX(r.ipc)                  AS max_ipc,
            AVG(r.cache_miss_rate)*100  AS avg_cache_miss_pct,
            SUM(r.carbon_g)*1000        AS total_carbon_mg,
            AVG(r.carbon_g)*1000        AS avg_carbon_mg,
            AVG(r.water_ml)             AS avg_water_ml,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.planning_time_ms  END) AS avg_planning_ms,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.execution_time_ms END) AS avg_execution_ms,
            AVG(CASE WHEN r.workflow_type='agentic' THEN r.synthesis_time_ms END) AS avg_synthesis_ms
        FROM experiments e
        LEFT JOIN runs r ON e.exp_id = r.exp_id
        WHERE r.workflow_type IN ('linear', 'agentic')
    """)


@st.cache_data(ttl=30, show_spinner=False)
def load_runs() -> pd.DataFrame:
    return q("""
        SELECT
            r.run_id, r.exp_id, r.hw_id, r.workflow_type, r.run_number,
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
        JOIN experiments e ON r.exp_id = e.exp_id
        WHERE r.workflow_type IN ('linear', 'agentic')
        ORDER BY r.run_id DESC
    """)


@st.cache_data(ttl=30, show_spinner=False)
def load_tax() -> pd.DataFrame:
    return q("""
        SELECT
            ots.comparison_id,
            ots.linear_run_id,
            ots.agentic_run_id,
            ots.tax_percent,
            ots.orchestration_tax_uj / 1e6  AS tax_j,
            ots.linear_dynamic_uj / 1e6     AS linear_dynamic_j,
            ots.agentic_dynamic_uj / 1e6    AS agentic_dynamic_j,
            ra.planning_time_ms, ra.execution_time_ms, ra.synthesis_time_ms,
            ra.llm_calls, ra.tool_calls, ra.total_tokens,
            el.task_name, el.country_code, el.provider
        FROM orchestration_tax_summary ots
        JOIN runs rl  ON ots.linear_run_id  = rl.run_id
        JOIN runs ra  ON ots.agentic_run_id = ra.run_id
        JOIN experiments el ON rl.exp_id    = el.exp_id
        ORDER BY ots.tax_percent DESC
    """)
