"""
alems/server/main.py
────────────────────────────────────────────────────────────────────────────
FastAPI application for A-LEMS Oracle VM server.

Run with systemd (see docs/setup/oracle-vm.md):
    uvicorn alems.server.main:app --host 0.0.0.0 --port 8000

Environment variables:
    ALEMS_DB_URL  — PostgreSQL connection string (required)
                    e.g. postgresql://alems:password@localhost/alems_central
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from alems.shared.models import (
    BulkSyncPayload, BulkSyncResponse,
    ExperimentSubmitRequest,
    HeartbeatRequest, HeartbeatResponse,
    HealthResponse,
    JobResponse, JobDetail,
    JobStatusRequest,
    RegisterRequest, RegisterResponse,
    SubmissionReviewRequest,
)
from alems.shared.db_layer import (
    get_engine, get_session,
    upsert_hardware, get_or_create_api_key, verify_api_key,
    get_next_job, upsert_run_status_cache,
)

AGENT_VERSION = "1.0.0"
_engine = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _engine
    db_url = os.environ.get("ALEMS_DB_URL")
    if not db_url:
        raise RuntimeError("ALEMS_DB_URL environment variable not set")
    _engine = get_engine(db_url)
    print(f"[server] Connected to: {db_url.split('@')[-1]}")
    yield
    _engine.dispose()


app = FastAPI(
    title="A-LEMS Orchestration API",
    version=AGENT_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Dependency: authenticated session ─────────────────────────────────────────

def get_db():
    with get_session(_engine) as session:
        yield session


def _auth(
    hardware_hash: str,
    api_key: str,
    session: Session,
) -> int:
    """Verify api_key and return hw_id. Raises 401 on failure."""
    hw_id = verify_api_key(session, hardware_hash, api_key)
    if not hw_id:
        raise HTTPException(status_code=401, detail="Invalid api_key")
    return hw_id


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
def health(session: Session = Depends(get_db)):
    row = session.execute(text("""
        SELECT COUNT(*) FROM hardware_config
        WHERE last_seen > NOW() - INTERVAL '2 minutes'
    """)).fetchone()
    connected = int(row[0]) if row else 0
    return HealthResponse(
        status="ok",
        mode="server",
        version=AGENT_VERSION,
        connected_agents=connected,
    )


# ── Registration ──────────────────────────────────────────────────────────────

@app.post("/register", response_model=RegisterResponse)
def register(req: RegisterRequest, session: Session = Depends(get_db)):
    """
    Register or re-register a machine.
    Idempotent — safe to call on every agent start.
    """
    hw_data = req.model_dump(exclude_none=True)
    hw_data["agent_status"] = "idle"

    hw_id  = upsert_hardware(session, hw_data)
    api_key = get_or_create_api_key(session, hw_id)
    session.commit()

    print(f"[server] Registered hw_id={hw_id} hostname={req.hostname}")
    return RegisterResponse(
        api_key=api_key,
        server_hw_id=hw_id,
        message="registered",
    )


# ── Heartbeat ─────────────────────────────────────────────────────────────────

@app.post("/heartbeat", response_model=HeartbeatResponse)
def heartbeat(req: HeartbeatRequest, session: Session = Depends(get_db)):
    hw_id = _auth(req.hardware_hash, req.api_key, session)

    now_expr = "NOW()"
    session.execute(text(f"""
        UPDATE hardware_config
        SET last_seen    = {now_expr},
            agent_status = :status,
            agent_version = :version
        WHERE hw_id = :hw_id
    """), {"status": req.status, "version": req.agent_version, "hw_id": hw_id})

    # Update live run status cache if agent is running something
    if req.status == "running" and req.live:
        live = req.live.model_dump(exclude_none=True)
        live["status"] = "running"
        upsert_run_status_cache(session, hw_id, live)
    elif req.status == "idle":
        session.execute(text("""
            UPDATE run_status_cache SET status='idle', last_updated=NOW()
            WHERE hw_id = :hw_id
        """), {"hw_id": hw_id})

    session.commit()

    # Decide if we need to ask agent to do something
    action = None
    if req.unsynced_runs > 50:
        action = "sync_now"

    return HeartbeatResponse(ok=True, action=action)


# ── Job dispatch ──────────────────────────────────────────────────────────────

@app.get("/get-job", response_model=JobResponse)
def get_job(
    hardware_hash: str,
    authorization: str = Header(default=""),
    session: Session = Depends(get_db),
):
    api_key = authorization.replace("Bearer ", "").strip()
    hw_id   = _auth(hardware_hash, api_key, session)

    job_dict = get_next_job(session, hw_id)
    session.commit()

    if not job_dict:
        return JobResponse(job=None)

    return JobResponse(job=JobDetail(
        job_id=job_dict["job_id"],
        command=_build_command_from_config(job_dict.get("experiment_config_json", "{}")),
        exp_config=_parse_json(job_dict.get("experiment_config_json", "{}")),
        on_disconnect=job_dict.get("on_disconnect", "fail"),
    ))


@app.post("/job-status")
def job_status(req: JobStatusRequest, session: Session = Depends(get_db)):
    hw_id = _auth(req.hardware_hash, req.api_key, session)

    status_map = {
        "started":   "running",
        "completed": "completed",
        "failed":    "failed",
    }
    db_status = status_map.get(req.status, req.status)

    now_expr = "NOW()"
    if db_status == "running":
        session.execute(text(f"""
            UPDATE job_queue SET status=:s, started_at={now_expr}
            WHERE job_id=:id AND dispatched_to_hw_id=:hw
        """), {"s": db_status, "id": req.job_id, "hw": hw_id})
    else:
        session.execute(text(f"""
            UPDATE job_queue
            SET status=:s, completed_at={now_expr}, error_message=:err
            WHERE job_id=:id AND dispatched_to_hw_id=:hw
        """), {"s": db_status, "id": req.job_id,
               "hw": hw_id, "err": req.error_message})

    session.commit()
    return {"ok": True}


# ── Bulk sync ─────────────────────────────────────────────────────────────────

@app.post("/bulk-sync", response_model=BulkSyncResponse)
def bulk_sync(payload: BulkSyncPayload, session: Session = Depends(get_db)):
    hw_id = _auth(payload.hardware_hash, payload.api_key, session)

    rows_inserted  = 0
    synced_run_ids = []

    try:
        # 1. Upsert hardware_config
        if payload.hardware_data:
            hw = dict(payload.hardware_data)
            hw["agent_status"] = "syncing"
            upsert_hardware(session, hw)

        # 2. Upsert experiments
        for exp in payload.experiments:
            exp = _remap_for_pg(exp, hw_id)
            _upsert_pg(session, "experiments", exp, "global_exp_id")
            rows_inserted += 1

        # 3. Upsert runs
        for run in payload.runs:
            run = _remap_for_pg(run, hw_id)
            _upsert_pg(session, "runs", run, "global_run_id")
            synced_run_ids.append(run["global_run_id"])
            rows_inserted += 1

        # 4. Child tables
        child_map = {
            "energy_samples":            ("energy_samples",            "global_run_id, timestamp_ns"),
            "cpu_samples":               ("cpu_samples",               "global_run_id, timestamp_ns"),
            "thermal_samples":           ("thermal_samples",           "global_run_id, timestamp_ns"),
            "interrupt_samples":         ("interrupt_samples",         "global_run_id, timestamp_ns"),
            "orchestration_events":      ("orchestration_events",      "global_run_id, start_time_ns, event_type"),
            "llm_interactions":          ("llm_interactions",          None),
            "orchestration_tax_summary": ("orchestration_tax_summary", "linear_run_id, agentic_run_id"),
        }
        for attr, (tbl, conflict_cols) in child_map.items():
            rows = getattr(payload, attr, [])
            for row in rows:
                row = _remap_child_for_pg(row)
                if conflict_cols:
                    _upsert_pg(session, tbl, row, conflict_cols)
                else:
                    _insert_ignore_pg(session, tbl, row)
                rows_inserted += 1

        # 5. Mark hw as idle after sync
        session.execute(text("""
            UPDATE hardware_config SET agent_status='idle' WHERE hw_id=:id
        """), {"id": hw_id})

        session.commit()

        return BulkSyncResponse(
            ok=True,
            synced_run_ids=synced_run_ids,
            rows_inserted=rows_inserted,
            message=f"synced {len(synced_run_ids)} runs, {rows_inserted} rows",
        )

    except Exception as e:
        session.rollback()
        print(f"[server] bulk-sync error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Experiment submissions ────────────────────────────────────────────────────

@app.post("/experiments/submit")
def submit_experiment(req: ExperimentSubmitRequest, session: Session = Depends(get_db)):
    hw_id = _auth(req.hardware_hash, req.api_key, session)

    session.execute(text("""
        INSERT INTO experiment_submissions
            (submitted_by_hw_id, config_json, name, description, review_status)
        VALUES (:hw_id, :cfg, :name, :desc, 'pending_review')
    """), {"hw_id": hw_id, "cfg": req.config_json,
           "name": req.name, "desc": req.description})
    session.commit()
    return {"ok": True, "message": "Submitted for review"}


@app.get("/experiments/queue")
def get_submission_queue(session: Session = Depends(get_db)):
    rows = session.execute(text("""
        SELECT s.*, h.hostname
        FROM experiment_submissions s
        LEFT JOIN hardware_config h ON h.hw_id = s.submitted_by_hw_id
        ORDER BY s.submitted_at DESC
    """)).fetchall()
    return [dict(r._mapping) for r in rows]


@app.post("/experiments/review/{submission_id}")
def review_submission(
    submission_id: str,
    req: SubmissionReviewRequest,
    session: Session = Depends(get_db),
):
    import json

    sub = session.execute(text("""
        SELECT * FROM experiment_submissions WHERE submission_id=:id
    """), {"id": submission_id}).fetchone()

    if not sub:
        raise HTTPException(status_code=404, detail="Submission not found")

    status = "approved" if req.action == "approve" else "rejected"

    session.execute(text("""
        UPDATE experiment_submissions
        SET review_status=:s, reviewed_by=:by,
            reviewed_at=NOW(), review_notes=:notes
        WHERE submission_id=:id
    """), {"s": status, "by": req.reviewed_by,
           "notes": req.notes, "id": submission_id})

    job_id = None
    if req.action == "approve":
        sub_dict = dict(sub._mapping)
        result = session.execute(text("""
            INSERT INTO job_queue (experiment_config_json, status, priority, created_by_hw_id)
            VALUES (:cfg, 'pending', 5, :hw_id)
            RETURNING job_id
        """), {"cfg": sub_dict["config_json"],
               "hw_id": sub_dict["submitted_by_hw_id"]})
        job_id = result.fetchone()[0]
        session.execute(text("""
            UPDATE experiment_submissions
            SET promoted_to_job_id=:jid WHERE submission_id=:id
        """), {"jid": job_id, "id": submission_id})

    session.commit()
    return {"ok": True, "status": status, "job_id": job_id}


# ── Machine status (for Streamlit dashboard) ──────────────────────────────────

@app.get("/machines")
def get_machines(session: Session = Depends(get_db)):
    rows = session.execute(text("""
        SELECT h.hw_id, h.hostname, h.cpu_model, h.ram_gb,
               h.agent_status, h.last_seen, h.agent_version,
               c.status as run_status, c.task_name, c.model_name,
               c.elapsed_s, c.energy_uj, c.avg_power_watts,
               COUNT(DISTINCT r.run_id) as total_runs
        FROM hardware_config h
        LEFT JOIN run_status_cache c ON c.hw_id = h.hw_id
        LEFT JOIN runs r ON r.hw_id = h.hw_id
        GROUP BY h.hw_id, c.status, c.task_name, c.model_name,
                 c.elapsed_s, c.energy_uj, c.avg_power_watts
        ORDER BY h.last_seen DESC NULLS LAST
    """)).fetchall()
    return [dict(r._mapping) for r in rows]


# ── Internal helpers ──────────────────────────────────────────────────────────

def _remap_for_pg(row: dict, hw_id: int) -> dict:
    """Remove SQLite-only fields, ensure hw_id is server-side."""
    # SQLite boolean columns that PostgreSQL expects as true BOOLEAN
    _BOOL_COLS = {
        "has_avx2", "has_avx512", "has_vmx", "gpu_power_available",
        "rapl_has_dram", "rapl_has_uncore", "git_dirty",
        "thermal_during_experiment", "thermal_now_active",
        "thermal_since_boot", "experiment_valid", "turbo_enabled",
        "is_cold_start",
    }
    result = {}
    for k, v in row.items():
        if v is None:
            continue
        if k in _BOOL_COLS:
            result[k] = bool(v)  # 0/1 → False/True
        else:
            result[k] = v
    result["hw_id"] = hw_id
    return result


def _remap_child_for_pg(row: dict) -> dict:
    """Remove local sample_id (server assigns its own BIGSERIAL)."""
    row = dict(row)
    row.pop("sample_id", None)
    row.pop("event_id", None)
    row.pop("interaction_id", None)
    return {k: v for k, v in row.items() if v is not None}


def _upsert_pg(session: Session, table: str, row: dict, conflict_cols: str) -> None:
    if not row:
        return
    cols = list(row.keys())
    col_str = ", ".join(cols)
    ph_str  = ", ".join(f":{c}" for c in cols)
    update_str = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c not in conflict_cols
    )
    sql = f"""
        INSERT INTO {table} ({col_str})
        VALUES ({ph_str})
        ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_str}
    """
    if not update_str:
        sql = f"""
            INSERT INTO {table} ({col_str})
            VALUES ({ph_str})
            ON CONFLICT ({conflict_cols}) DO NOTHING
        """
    session.execute(text(sql), row)


def _insert_ignore_pg(session: Session, table: str, row: dict) -> None:
    if not row:
        return
    cols   = list(row.keys())
    col_str = ", ".join(cols)
    ph_str  = ", ".join(f":{c}" for c in cols)
    session.execute(text(f"""
        INSERT INTO {table} ({col_str}) VALUES ({ph_str})
        ON CONFLICT DO NOTHING
    """), row)


def _build_command_from_config(config_json: str) -> str:
    import json
    try:
        cfg = json.loads(config_json)
        from alems.agent.job_executor import build_command
        return build_command(cfg)
    except Exception:
        return ""


def _parse_json(s: str) -> dict:
    import json
    try:
        return json.loads(s)
    except Exception:
        return {}
