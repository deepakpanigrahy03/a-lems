"""
A-LEMS Dashboard Server
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Run locally:    uvicorn server:app --host 0.0.0.0 --port 8765 --reload
Access local:   http://localhost:8765
Access remote:  http://<machine-ip>:8765
SSH tunnel:     ssh -L 8765:localhost:8765 user@host  then  http://localhost:8765
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import json
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import (BackgroundTasks, FastAPI, HTTPException, WebSocket,
                     WebSocketDisconnect)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

# ── Paths ────────────────────────────────────────────────────────────────────
BASE = Path(__file__).parent
DB = BASE / "data" / "experiments.db"
HTML = BASE / "dashboard.html"

# ── Try importing A-LEMS harness (only works on measurement machine) ─────────
HARNESS_OK = False
try:
    sys.path.insert(0, str(BASE))
    from core.config_loader import ConfigLoader
    from core.execution.agentic import AgenticExecutor
    from core.execution.harness import ExperimentHarness
    from core.execution.linear import LinearExecutor

    HARNESS_OK = True
    print("✅  A-LEMS harness loaded — live execution enabled")
except Exception as e:
    print(f"⚠️   Harness unavailable ({e}) — read-only / remote mode")

app = FastAPI(title="A-LEMS API", version="2.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# ── Token auth — named per-researcher tokens ──────────────────────────────────
# ALEMS_TOKENS_JSON = {"supervisor": "alems-xxx", "colleague": "alems-yyy", ...}
_TOKENS_RAW = os.environ.get("ALEMS_TOKENS_JSON", "")
_TOKEN_MAP: dict = {}  # token_value → researcher_name

try:
    if _TOKENS_RAW:
        _parsed = json.loads(_TOKENS_RAW)
        # Support both {name: token} and flat string formats
        if isinstance(_parsed, dict):
            _TOKEN_MAP = {v: k for k, v in _parsed.items()}  # invert: token→name
        elif isinstance(_parsed, str):
            _TOKEN_MAP = {_parsed: "researcher"}
except Exception:
    pass

# Fallback: single token from ALEMS_TOKEN env var
_SINGLE_TOKEN = os.environ.get("ALEMS_TOKEN", "")
if _SINGLE_TOKEN and not _TOKEN_MAP:
    try:
        _t = json.loads(_SINGLE_TOKEN)
        if isinstance(_t, dict):
            _TOKEN_MAP = {v: k for k, v in _t.items()}
    except Exception:
        _TOKEN_MAP = {_SINGLE_TOKEN: "researcher"}


def _check_token(token: str = "") -> str:
    """
    Validates token. Returns researcher name if valid.
    Raises 403 if invalid.
    Returns "" if no tokens configured (local/open mode).
    """
    if not _TOKEN_MAP:
        return "local"  # No tokens configured → open access
    if token in _TOKEN_MAP:
        return _TOKEN_MAP[token]  # Return researcher name
    raise HTTPException(403, "Invalid token. Contact the lab owner for your token.")


# Active run sessions: session_id -> dict
_runs: dict = {}  # sid → {status, log, progress, done, result}

# ── Session persistence — survives server restarts ────────────────────────────
import json as _json

_SESSIONS_DIR = BASE / "logs" / "sessions"
_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)


def _save_session(sid: str):
    try:
        s = _runs[sid].copy()
        s.pop("result", None)
        (_SESSIONS_DIR / f"{sid}.json").write_text(_json.dumps(s))
    except Exception:
        pass


def _load_session(sid: str) -> dict:
    p = _SESSIONS_DIR / f"{sid}.json"
    if p.exists():
        try:
            return _json.loads(p.read_text())
        except Exception:
            pass
    return {}


# ── DB helpers ───────────────────────────────────────────────────────────────
@contextmanager
def db():
    con = sqlite3.connect(str(DB), timeout=15)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    try:
        yield con
    finally:
        con.close()


def q(sql, p=[]):
    with db() as c:
        return [dict(r) for r in c.execute(sql, p).fetchall()]


def q1(sql, p=[]):
    with db() as c:
        r = c.execute(sql, p).fetchone()
        return dict(r) if r else None


def _ts():
    return time.strftime("%H:%M:%S")


# ══════════════════════════════════════════════════════════════════════════════
# EXPERIMENTS
# ══════════════════════════════════════════════════════════════════════════════


@app.get("/api/experiments")
def get_experiments():
    return q("""
        SELECT e.*,
            COUNT(DISTINCT r.run_id)                                          AS run_count,
            ROUND(AVG(CASE WHEN r.workflow_type='linear'  THEN r.total_energy_uj END)/1e6,4) AS avg_linear_j,
            ROUND(AVG(CASE WHEN r.workflow_type='agentic' THEN r.total_energy_uj END)/1e6,4) AS avg_agentic_j,
            ROUND(AVG(r.ipc),3)                                               AS avg_ipc,
            ROUND(AVG(r.cache_miss_rate)*100,1)                               AS avg_cache_miss_pct,
            ROUND(AVG(r.thread_migrations),0)                                 AS avg_migrations,
            ROUND(AVG(r.carbon_g)*1000,4)                                     AS avg_carbon_mg,
            ROUND(AVG(ots.tax_percent),2)                                     AS avg_tax_pct
        FROM experiments e
        LEFT JOIN runs r   ON e.exp_id = r.exp_id
        LEFT JOIN orchestration_tax_summary ots ON r.run_id = ots.agentic_run_id
        GROUP BY e.exp_id
        ORDER BY e.exp_id DESC
    """)


@app.get("/api/experiments/{exp_id}")
def get_experiment(exp_id: int):
    exp = q1("SELECT * FROM experiments WHERE exp_id=?", [exp_id])
    if not exp:
        raise HTTPException(404, "Experiment not found")

    exp["runs"] = q(
        """
        SELECT r.run_id, r.workflow_type, r.run_number,
            ROUND(r.total_energy_uj/1e6,4)   AS energy_j,
            ROUND(r.pkg_energy_uj/1e6,4)     AS pkg_j,
            ROUND(r.core_energy_uj/1e6,4)    AS core_j,
            ROUND(r.uncore_energy_uj/1e6,4)  AS uncore_j,
            ROUND(r.dram_energy_uj/1e6,4)    AS dram_j,
            ROUND(r.dynamic_energy_uj/1e6,4) AS dynamic_j,
            ROUND(r.duration_ns/1e6,1)       AS duration_ms,
            ROUND(r.ipc,3)                   AS ipc,
            ROUND(r.cache_miss_rate*100,1)   AS cache_miss_pct,
            r.thread_migrations,
            r.total_context_switches,
            r.context_switches_voluntary,
            r.context_switches_involuntary,
            ROUND(r.carbon_g*1000,4)         AS carbon_mg,
            ROUND(r.water_ml,4)              AS water_ml,
            ROUND(r.methane_mg,4)            AS methane_mg,
            r.planning_time_ms, r.execution_time_ms, r.synthesis_time_ms,
            r.phase_planning_ratio, r.phase_execution_ratio, r.phase_synthesis_ratio,
            r.llm_calls, r.tool_calls, r.total_tokens, r.prompt_tokens, r.completion_tokens,
            ROUND(r.frequency_mhz,0)         AS freq_mhz,
            ROUND(r.package_temp_celsius,1)  AS temp_c,
            ROUND(r.thermal_delta_c,1)       AS thermal_delta_c,
            r.thermal_throttle_flag,
            ROUND(r.interrupt_rate,0)        AS irq_rate,
            ROUND(r.rss_memory_mb,1)         AS rss_mb,
            ROUND(r.dns_latency_ms,2)        AS dns_ms,
            ROUND(r.api_latency_ms,0)        AS api_ms,
            r.c2_time_seconds, r.c3_time_seconds, r.c6_time_seconds, r.c7_time_seconds,
            r.wakeup_latency_us, r.governor, r.turbo_enabled, r.is_cold_start,
            oa.workload_energy_j, oa.reasoning_energy_j,
            oa.orchestration_tax_j          AS oa_tax_j,
            oa.core_share, oa.uncore_share,
            oa.joules_per_billion_instructions
        FROM runs r
        LEFT JOIN orchestration_analysis oa ON r.run_id = oa.run_id
        WHERE r.exp_id=?
        ORDER BY r.run_number, r.workflow_type
    """,
        [exp_id],
    )

    exp["tax"] = q(
        """
        SELECT ots.comparison_id,
            ROUND(ots.linear_dynamic_uj/1e6,4)    AS linear_j,
            ROUND(ots.agentic_dynamic_uj/1e6,4)   AS agentic_j,
            ROUND(ots.orchestration_tax_uj/1e6,4) AS tax_j,
            ROUND(ots.tax_percent,2)               AS tax_pct,
            ots.linear_run_id, ots.agentic_run_id
        FROM orchestration_tax_summary ots
        JOIN runs r ON ots.agentic_run_id = r.run_id
        WHERE r.exp_id=?
    """,
        [exp_id],
    )

    return exp


# ══════════════════════════════════════════════════════════════════════════════
# RUNS
# ══════════════════════════════════════════════════════════════════════════════


@app.get("/api/runs")
def get_runs(limit: int = 500):
    return q(
        """
        SELECT mf.*,
            e.name AS exp_name, e.task_name, e.status,
            oa.pkg_energy_j, oa.core_energy_j, oa.uncore_energy_j, oa.dram_energy_j,
            oa.workload_energy_j, oa.reasoning_energy_j,
            oa.orchestration_tax_j  AS oa_tax_j,
            oa.core_share, oa.uncore_share,
            oa.joules_per_billion_instructions
        FROM ml_features mf
        JOIN experiments e ON mf.run_id IN (SELECT run_id FROM runs WHERE exp_id = e.exp_id)
        LEFT JOIN orchestration_analysis oa ON mf.run_id = oa.run_id
        LIMIT ?
    """,
        [limit],
    )


@app.get("/api/runs/{run_id}")
def get_run(run_id: int):
    row = q1(
        """
        SELECT r.*, e.name AS exp_name, e.task_name, e.provider, e.country_code,
            oa.workload_energy_j, oa.reasoning_energy_j,
            oa.orchestration_tax_j AS oa_tax_j,
            oa.core_share, oa.uncore_share, oa.baseline_pkg_j,
            oa.joules_per_billion_instructions
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        LEFT JOIN orchestration_analysis oa ON r.run_id = oa.run_id
        WHERE r.run_id=?
    """,
        [run_id],
    )
    if not row:
        raise HTTPException(404, "Run not found")
    return row


@app.get("/api/runs/{run_id}/samples/energy")
def get_energy_samples(run_id: int, downsample: int = 1):
    rows = q(
        """
        SELECT sample_id,
            ROUND((timestamp_ns -
                (SELECT MIN(timestamp_ns) FROM energy_samples WHERE run_id=?)
            )/1e6, 1)           AS elapsed_ms,
            ROUND(pkg_energy_uj/1e6,6)    AS pkg_j,
            ROUND(core_energy_uj/1e6,6)   AS core_j,
            ROUND(uncore_energy_uj/1e6,6) AS uncore_j,
            ROUND(dram_energy_uj/1e6,6)   AS dram_j
        FROM energy_samples
        WHERE run_id=?
        ORDER BY timestamp_ns
    """,
        [run_id, run_id],
    )

    if downsample > 1:
        rows = [r for i, r in enumerate(rows) if i % downsample == 0]

    # Compute instantaneous power (W = delta_J / delta_s)
    power = []
    for i in range(1, len(rows)):
        dt = (rows[i]["elapsed_ms"] - rows[i - 1]["elapsed_ms"]) / 1000.0
        if dt > 0:
            power.append(
                {
                    "elapsed_ms": rows[i]["elapsed_ms"],
                    "pkg_w": round((rows[i]["pkg_j"] - rows[i - 1]["pkg_j"]) / dt, 3),
                    "core_w": round(
                        (rows[i]["core_j"] - rows[i - 1]["core_j"]) / dt, 3
                    ),
                    "uncore_w": round(
                        (rows[i]["uncore_j"] - rows[i - 1]["uncore_j"]) / dt, 3
                    ),
                    "dram_w": round(
                        (rows[i]["dram_j"] - rows[i - 1]["dram_j"]) / dt, 3
                    ),
                }
            )
    return {"raw": rows, "power": power, "count": len(rows)}


@app.get("/api/runs/{run_id}/samples/cpu")
def get_cpu_samples(run_id: int):
    return q(
        """
        SELECT sample_id,
            ROUND((timestamp_ns -
                (SELECT MIN(timestamp_ns) FROM cpu_samples WHERE run_id=?)
            )/1e6, 1) AS elapsed_ms,
            cpu_util_percent, cpu_busy_mhz, cpu_avg_mhz,
            c1_residency, c2_residency, c3_residency,
            c6_residency, c7_residency,
            pkg_c8_residency, pkg_c9_residency, pkg_c10_residency,
            package_power, dram_power, package_temp, ipc,
            extra_metrics_json
        FROM cpu_samples
        WHERE run_id=?
        ORDER BY timestamp_ns
    """,
        [run_id, run_id],
    )


@app.get("/api/runs/{run_id}/samples/interrupts")
def get_interrupt_samples(run_id: int):
    return q(
        """
        SELECT ROUND((timestamp_ns -
            (SELECT MIN(timestamp_ns) FROM interrupt_samples WHERE run_id=?)
        )/1e6, 1) AS elapsed_ms,
        interrupts_per_sec
        FROM interrupt_samples
        WHERE run_id=?
        ORDER BY timestamp_ns
    """,
        [run_id, run_id],
    )


@app.get("/api/runs/{run_id}/events")
def get_events(run_id: int):
    return q(
        """
        SELECT event_id, step_index, phase, event_type,
            ROUND((start_time_ns -
                (SELECT MIN(start_time_ns) FROM orchestration_events WHERE run_id=?)
            )/1e6, 1) AS start_ms,
            ROUND(duration_ns/1e6,1)          AS duration_ms,
            ROUND(power_watts,2)              AS power_w,
            cpu_util_percent,
            ROUND(event_energy_uj/1e6,6)      AS event_energy_j,
            ROUND(tax_contribution_uj/1e6,6)  AS tax_j,
            ROUND(tax_percent,2)              AS tax_pct
        FROM orchestration_events
        WHERE run_id=?
        ORDER BY start_time_ns
    """,
        [run_id, run_id],
    )


# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════


@app.get("/api/analytics/stats")
def get_stats():
    """Top-level KPI numbers for the dashboard header."""
    return q1("""
        SELECT
            COUNT(DISTINCT e.exp_id)                              AS total_experiments,
            COUNT(r.run_id)                                       AS total_runs,
            COUNT(CASE WHEN r.workflow_type='linear'  THEN 1 END) AS linear_runs,
            COUNT(CASE WHEN r.workflow_type='agentic' THEN 1 END) AS agentic_runs,
            ROUND(AVG(CASE WHEN r.workflow_type='linear'  THEN r.total_energy_uj END)/1e6,3) AS avg_linear_j,
            ROUND(AVG(CASE WHEN r.workflow_type='agentic' THEN r.total_energy_uj END)/1e6,3) AS avg_agentic_j,
            ROUND(MIN(r.total_energy_uj)/1e6,4)                   AS min_energy_j,
            ROUND(MAX(r.total_energy_uj)/1e6,3)                   AS max_energy_j,
            ROUND(SUM(r.total_energy_uj)/1e6,1)                   AS total_energy_j,
            ROUND(AVG(r.ipc),3)                                   AS avg_ipc,
            ROUND(MAX(r.ipc),3)                                   AS max_ipc,
            ROUND(AVG(r.thread_migrations),0)                     AS avg_migrations,
            ROUND(MAX(r.thread_migrations),0)                     AS max_migrations,
            ROUND(AVG(r.carbon_g)*1000,4)                         AS avg_carbon_mg,
            ROUND(SUM(r.carbon_g)*1000,3)                         AS total_carbon_mg,
            ROUND(AVG(r.water_ml),4)                              AS avg_water_ml,
            ROUND(AVG(ots.tax_percent),2)                         AS avg_tax_pct,
            COUNT(DISTINCT e.provider)                            AS providers,
            COUNT(DISTINCT e.country_code)                        AS countries,
            COUNT(DISTINCT e.task_name)                           AS task_types
        FROM experiments e
        LEFT JOIN runs r ON e.exp_id = r.exp_id
        LEFT JOIN orchestration_tax_summary ots ON r.run_id = ots.agentic_run_id
        WHERE r.total_energy_uj IS NOT NULL
    """)


@app.get("/api/analytics/tax")
def get_tax_overview():
    """Full orchestration tax table with phase attribution."""
    return q("""
        SELECT e.exp_id, e.name, e.task_name, e.provider, e.country_code, e.model_name,
            ROUND(ots.linear_dynamic_uj/1e6,4)    AS linear_j,
            ROUND(ots.agentic_dynamic_uj/1e6,4)   AS agentic_j,
            ROUND(ots.orchestration_tax_uj/1e6,4) AS tax_j,
            ROUND(ots.tax_percent,2)               AS tax_pct,
            r_a.planning_time_ms,
            r_a.execution_time_ms,
            r_a.synthesis_time_ms,
            r_a.llm_calls, r_a.tool_calls,
            ROUND(r_a.carbon_g*1000,4)             AS agentic_carbon_mg,
            ROUND(r_l.carbon_g*1000,4)             AS linear_carbon_mg,
            ROUND((r_a.carbon_g - r_l.carbon_g)*1000,4) AS marginal_carbon_mg,
            ROUND(r_a.water_ml - r_l.water_ml, 4) AS marginal_water_ml
        FROM orchestration_tax_summary ots
        JOIN runs r_a ON ots.agentic_run_id = r_a.run_id
        JOIN runs r_l ON ots.linear_run_id  = r_l.run_id
        JOIN experiments e ON r_a.exp_id    = e.exp_id
        ORDER BY ots.tax_percent DESC
    """)


@app.get("/api/analytics/domains")
def get_domain_breakdown():
    """Per-domain energy from orchestration_analysis view (requires baseline)."""
    return q("""
        SELECT run_id, exp_id, workflow_type, run_number,
            provider, task_name, country_code,
            ROUND(pkg_energy_j,4)               AS pkg_j,
            ROUND(core_energy_j,4)              AS core_j,
            ROUND(uncore_energy_j,4)            AS uncore_j,
            ROUND(dram_energy_j,4)              AS dram_j,
            ROUND(workload_energy_j,4)          AS workload_j,
            ROUND(reasoning_energy_j,4)         AS reasoning_j,
            ROUND(orchestration_tax_j,4)        AS tax_j,
            ROUND(core_share*100,1)             AS core_pct,
            ROUND(uncore_share*100,1)           AS uncore_pct,
            ROUND(duration_sec,2)               AS duration_s,
            ROUND(ipc,3)                        AS ipc,
            ROUND(cache_miss_rate*100,1)        AS cache_miss_pct,
            ROUND(joules_per_billion_instructions,4) AS j_per_bn_inst
        FROM orchestration_analysis
        ORDER BY pkg_j DESC
    """)


@app.get("/api/analytics/cstates")
def get_cstates():
    """C-state residency averaged per workflow+provider+country group."""
    return q("""
        SELECT e.workflow_type, e.provider, e.country_code,
            COUNT(*) AS run_count,
            ROUND(AVG(r.c2_time_seconds),3)   AS avg_c2_s,
            ROUND(AVG(r.c3_time_seconds),3)   AS avg_c3_s,
            ROUND(AVG(r.c6_time_seconds),3)   AS avg_c6_s,
            ROUND(AVG(r.c7_time_seconds),3)   AS avg_c7_s,
            ROUND(AVG(r.wakeup_latency_us),2) AS avg_wakeup_us,
            ROUND(AVG(r.thermal_throttle_flag),3) AS throttle_rate,
            ROUND(AVG(cs.avg_c6),2)           AS avg_c6_residency_pct,
            ROUND(AVG(cs.avg_c1),2)           AS avg_c1_residency_pct
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        LEFT JOIN (
            SELECT run_id,
                AVG(c6_residency) AS avg_c6,
                AVG(c1_residency) AS avg_c1
            FROM cpu_samples GROUP BY run_id
        ) cs ON r.run_id = cs.run_id
        GROUP BY e.workflow_type, e.provider, e.country_code
        ORDER BY e.provider, e.workflow_type
    """)


@app.get("/api/analytics/baselines")
def get_baselines():
    return q("""
        SELECT baseline_id,
            datetime(timestamp,'unixepoch','localtime') AS measured_at,
            ROUND(package_power_watts,3) AS pkg_w,
            ROUND(core_power_watts,3)    AS core_w,
            ROUND(uncore_power_watts,3)  AS uncore_w,
            ROUND(dram_power_watts,3)    AS dram_w,
            ROUND(package_std,3)         AS pkg_std,
            duration_seconds, sample_count,
            governor, turbo, background_cpu, method
        FROM idle_baselines
        ORDER BY timestamp DESC
    """)


@app.get("/api/analytics/hardware")
def get_hardware():
    return q("SELECT * FROM hardware_config ORDER BY hw_id DESC")


@app.get("/api/analytics/anomalies")
def get_anomalies():
    """Statistical anomaly detection across all runs."""
    return q("""
        WITH stats AS (
            SELECT AVG(total_energy_uj/1e6) AS mean_e,
                   AVG(ipc)                 AS mean_ipc,
                   AVG(interrupt_rate)      AS mean_irq
            FROM runs WHERE total_energy_uj IS NOT NULL
        )
        SELECT r.run_id, e.exp_id, e.task_name, e.provider,
            r.workflow_type, r.run_number,
            ROUND(r.total_energy_uj/1e6,3)  AS energy_j,
            ROUND(r.ipc,3)                  AS ipc,
            ROUND(r.cache_miss_rate*100,1)  AS cache_miss_pct,
            r.thread_migrations,
            ROUND(r.interrupt_rate,0)       AS irq_rate,
            ROUND(r.thermal_delta_c,1)      AS thermal_delta,
            r.thermal_throttle_flag,
            CASE WHEN r.total_energy_uj/1e6 > s.mean_e*3  THEN 1 ELSE 0 END AS flag_energy,
            CASE WHEN r.ipc < 0.5                          THEN 1 ELSE 0 END AS flag_ipc_low,
            CASE WHEN r.ipc > 2.5                          THEN 1 ELSE 0 END AS flag_ipc_high,
            CASE WHEN r.cache_miss_rate*100 > 50           THEN 1 ELSE 0 END AS flag_cache,
            CASE WHEN r.interrupt_rate > 50000             THEN 1 ELSE 0 END AS flag_irq,
            CASE WHEN r.thermal_delta_c > 40               THEN 1 ELSE 0 END AS flag_thermal,
            CASE WHEN r.thermal_throttle_flag = 1          THEN 1 ELSE 0 END AS flag_throttle
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        CROSS JOIN stats s
        WHERE r.total_energy_uj IS NOT NULL
          AND (
              r.total_energy_uj/1e6 > s.mean_e*3
           OR r.ipc < 0.5 OR r.ipc > 2.5
           OR r.cache_miss_rate*100 > 50
           OR r.interrupt_rate > 50000
           OR r.thermal_delta_c > 40
           OR r.thermal_throttle_flag = 1
          )
        ORDER BY energy_j DESC
    """)


# ══════════════════════════════════════════════════════════════════════════════
# SYSTEM STATUS
# ══════════════════════════════════════════════════════════════════════════════


@app.get("/api/system/status")
def system_status():
    counts = q1("""
        SELECT
            (SELECT COUNT(*) FROM experiments)          AS experiments,
            (SELECT COUNT(*) FROM runs)                 AS runs,
            (SELECT COUNT(*) FROM energy_samples)       AS energy_samples,
            (SELECT COUNT(*) FROM cpu_samples)          AS cpu_samples,
            (SELECT COUNT(*) FROM orchestration_events) AS events,
            (SELECT COUNT(*) FROM idle_baselines)       AS baselines
    """)
    return {
        "harness_available": HARNESS_OK,
        "db_path": str(DB),
        "db_exists": DB.exists(),
        "mode": "live-execution" if HARNESS_OK else "read-only",
        "counts": counts,
    }


# ══════════════════════════════════════════════════════════════════════════════
# EXECUTE — trigger real benchmark (same machine or SSH tunnel)
# ══════════════════════════════════════════════════════════════════════════════


@app.post("/api/run/start")
async def start_run(payload: dict, background_tasks: BackgroundTasks):
    _researcher = _check_token(payload.get("token", ""))
    """
    payload: {task_id, model, country_code, repetitions, cool_down}
    Returns session_id immediately. Poll /api/run/status/{id} or
    subscribe to WebSocket /ws/run/{id} for live log lines.
    Works via SSH tunnel from any remote machine.
    """
    if not HARNESS_OK:
        raise HTTPException(503, "Harness not available on this machine.")

    sid = f"ses_{int(time.time()*1000)}"
    _runs[sid] = {
        "status": "starting",
        "log": [],
        "progress": 0.0,
        "done": False,
        "result": None,
        "researcher": _researcher,
    }

    def _run():
        import subprocess as _sp
        import sys as _sys

        try:
            task_id = payload.get("task_id", "simple")
            country = payload.get("country_code", "US")
            reps = int(payload.get("repetitions", 3))
            cool = int(payload.get("cool_down", 5))
            provider = payload.get("provider", "cloud")
            if provider not in ("cloud", "local"):
                provider = "cloud"

            _log(sid, f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            _log(sid, f"Session  : {sid}")
            _log(sid, f"Triggered: {_researcher}")
            _log(sid, f"Task     : {task_id}  Provider: {provider}")
            _log(sid, f"Region   : {country}   Reps: {reps}")
            _log(sid, f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

            # Choose module based on payload
            # batch mode → run_experiment, single → test_harness
            tasks_list = payload.get("tasks", [])
            is_batch = len(tasks_list) > 1 or task_id == "batch"

            if is_batch:
                prov_arg = ",".join(payload.get("providers", [provider]))
                tasks_arg = ",".join(tasks_list) if tasks_list else task_id
                cmd = [
                    _sys.executable,
                    "-m",
                    "core.execution.tests.run_experiment",
                    "--tasks",
                    tasks_arg,
                    "--providers",
                    prov_arg,
                    "--repetitions",
                    str(reps),
                    "--country",
                    country,
                    "--cool-down",
                    str(cool),
                    "--save-db",
                ]
            else:
                cmd = [
                    _sys.executable,
                    "-m",
                    "core.execution.tests.test_harness",
                    "--task-id",
                    task_id,
                    "--provider",
                    provider,
                    "--repetitions",
                    str(reps),
                    "--country",
                    country,
                    "--cool-down",
                    str(cool),
                    "--save-db",
                ]
            _log(sid, f"CMD: {' '.join(cmd)}")
            _runs[sid]["status"] = "running"

            proc = _sp.Popen(
                cmd,
                cwd=str(BASE),
                stdout=_sp.PIPE,
                stderr=_sp.STDOUT,
                text=True,
                bufsize=1,
            )

            rep_count = 0
            for line in iter(proc.stdout.readline, ""):
                line = line.rstrip()
                if not line:
                    continue
                _log(sid, line)
                # Track progress from output
                if "Pair" in line and "saved" in line:
                    rep_count += 1
                    _runs[sid]["progress"] = rep_count / reps

            proc.wait()
            if proc.returncode == 0:
                _runs[sid]["status"] = "complete"
                _runs[sid]["done"] = True
                _log(sid, f"✅ Complete — {reps} pairs saved to DB")
            else:
                _runs[sid]["status"] = "error"
                _runs[sid]["done"] = True
                _log(sid, f"❌ Process exited with code {proc.returncode}")

        except Exception as ex:
            import traceback

            _runs[sid]["status"] = "error"
            _runs[sid]["done"] = True
            _log(sid, f"❌ Error: {ex}")
            _log(sid, traceback.format_exc().splitlines()[-1])

    background_tasks.add_task(_run)
    return {"session_id": sid, "status": "started"}


@app.get("/api/run/status/{sid}")
def run_status(sid: str):
    if sid not in _runs:
        # Try loading from disk (server may have restarted)
        s = _load_session(sid)
        if not s:
            raise HTTPException(404, "Session not found")
    else:
        s = _runs[sid]
    return {
        "session_id": sid,
        "status": s["status"],
        "done": s["done"],
        "progress": s["progress"],
        "log": s["log"],
        "summary": _summarise(s["result"]) if s["done"] else None,
    }


def _log(sid, msg):
    _runs[sid]["log"].append(f"[{_ts()}] {msg}")
    _save_session(sid)


def _summarise(r):
    if not r:
        return None
    try:
        return {
            "avg_linear_j": round(r["linear"].get("avg_workload_energy_j", 0) or 0, 4),
            "avg_agentic_j": round(
                r["agentic"].get("avg_workload_energy_j", 0) or 0, 4
            ),
            "avg_tax_j": round(r["agentic"].get("avg_orchestration_tax_j", 0) or 0, 4),
            "avg_tax_pct": round(r["agentic"].get("avg_tax_percent", 0) or 0, 2),
            "repetitions": r.get("repetitions", 0),
        }
    except:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# WEBSOCKET — stream new DB samples during an active run (~10Hz poll)
# ══════════════════════════════════════════════════════════════════════════════


@app.websocket("/ws/samples")
async def ws_samples(websocket: WebSocket):
    """
    Client sends: {"run_id": 123}  OR  {"latest": true}
    Server streams: {"type":"energy","data":[...]}  {"type":"cpu","data":[...]}
    Stops when run is finished (no new samples for 3s).
    Works identically over SSH tunnel.
    """
    await websocket.accept()
    try:
        msg = await asyncio.wait_for(websocket.receive_json(), timeout=10)
        run_id = msg.get("run_id")
        if msg.get("latest") or not run_id:
            row = q1("SELECT MAX(run_id) AS rid FROM runs")
            run_id = row["rid"] if row else None

        if not run_id:
            await websocket.send_json({"type": "error", "msg": "No run_id found"})
            return

        await websocket.send_json({"type": "init", "run_id": run_id})
        last_e = last_c = last_i = 0
        idle = 0

        while True:
            e_rows = q(
                """
                SELECT sample_id,
                    ROUND((timestamp_ns -
                        (SELECT MIN(timestamp_ns) FROM energy_samples WHERE run_id=?)
                    )/1e6,1) AS elapsed_ms,
                    ROUND(pkg_energy_uj/1e6,6)    AS pkg_j,
                    ROUND(core_energy_uj/1e6,6)   AS core_j,
                    ROUND(uncore_energy_uj/1e6,6) AS uncore_j,
                    ROUND(dram_energy_uj/1e6,6)   AS dram_j
                FROM energy_samples
                WHERE run_id=? AND sample_id > ?
                ORDER BY sample_id LIMIT 20
            """,
                [run_id, run_id, last_e],
            )

            c_rows = q(
                """
                SELECT sample_id,
                    ROUND((timestamp_ns -
                        (SELECT MIN(timestamp_ns) FROM cpu_samples WHERE run_id=?)
                    )/1e6,1) AS elapsed_ms,
                    cpu_util_percent, package_power, dram_power,
                    package_temp, ipc, c6_residency, c1_residency
                FROM cpu_samples
                WHERE run_id=? AND sample_id > ?
                ORDER BY sample_id LIMIT 20
            """,
                [run_id, run_id, last_c],
            )

            i_rows = q(
                """
                SELECT sample_id,
                    ROUND((timestamp_ns -
                        (SELECT MIN(timestamp_ns) FROM interrupt_samples WHERE run_id=?)
                    )/1e6,1) AS elapsed_ms,
                    interrupts_per_sec
                FROM interrupt_samples
                WHERE run_id=? AND sample_id > ?
                ORDER BY sample_id LIMIT 20
            """,
                [run_id, run_id, last_i],
            )

            if e_rows:
                last_e = e_rows[-1]["sample_id"]
                await websocket.send_json({"type": "energy", "data": e_rows})
                idle = 0
            if c_rows:
                last_c = c_rows[-1]["sample_id"]
                await websocket.send_json({"type": "cpu", "data": c_rows})
                idle = 0
            if i_rows:
                last_i = i_rows[-1]["sample_id"]
                await websocket.send_json({"type": "interrupts", "data": i_rows})
                idle = 0

            if not e_rows and not c_rows and not i_rows:
                idle += 1
                if idle > 30:  # 3 seconds of silence = run done
                    rr = q1("SELECT end_time_ns FROM runs WHERE run_id=?", [run_id])
                    if rr and rr.get("end_time_ns"):
                        await websocket.send_json({"type": "done", "run_id": run_id})
                        break

            await asyncio.sleep(0.1)

    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    except Exception as ex:
        try:
            await websocket.send_json({"type": "error", "msg": str(ex)})
        except:
            pass


@app.websocket("/ws/run/{sid}")
async def ws_run_log(websocket: WebSocket, sid: str):
    """Stream log lines from an active run session."""
    await websocket.accept()
    last = 0
    try:
        while True:
            if sid not in _runs:
                await websocket.send_json({"type": "error", "msg": "session not found"})
                break
            s = _runs[sid]
            new = s["log"][last:]
            if new:
                for line in new:
                    await websocket.send_json(
                        {"type": "log", "msg": line, "progress": s["progress"]}
                    )
                last += len(new)
            if s["done"]:
                await websocket.send_json(
                    {
                        "type": "complete",
                        "status": s["status"],
                        "summary": _summarise(s["result"]),
                    }
                )
                break
            await asyncio.sleep(0.2)
    except WebSocketDisconnect:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# SERVE DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════


@app.get("/")
def root():
    if HTML.exists():
        return HTMLResponse(HTML.read_text())
    return HTMLResponse("""
        <h2 style='font-family:monospace;padding:40px'>
        A-LEMS server running.<br>
        Place <code>dashboard.html</code> next to <code>server.py</code> then refresh.
        </h2>
    """)


@app.get("/api/online")
def online_status():
    """Public endpoint — no auth. Tells the hosted dashboard if live mode is available."""
    return {
        "online": True,
        "harness": HARNESS_OK,
        "version": "2.0",
        "auth_required": bool(_TOKEN_MAP),
        "researchers": list(_TOKEN_MAP.values()),  # names only, never tokens
    }


@app.get("/health")
def health():
    return {"status": "ok", "db": DB.exists(), "harness": HARNESS_OK}


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="A-LEMS Dashboard Server")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--reload", action="store_true")
    args = p.parse_args()

    print(f"""
┌──────────────────────────────────────────────────┐
│  A-LEMS Dashboard Server v2.0                    │
├──────────────────────────────────────────────────┤
│  DB     : {DB}
│  Mode   : {'LIVE EXECUTION ✅' if HARNESS_OK else 'READ-ONLY ⚠️  (harness not found)'}
│  Local  : http://localhost:{args.port}
│  Network: http://0.0.0.0:{args.port}
│  SSH    : ssh -L {args.port}:localhost:{args.port} user@<host>
└──────────────────────────────────────────────────┘
""")
    uvicorn.run("server:app", host=args.host, port=args.port, reload=args.reload)
