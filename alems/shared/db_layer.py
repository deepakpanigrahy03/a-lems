"""
alems/shared/db_layer.py
────────────────────────────────────────────────────────────────────────────
SQLAlchemy-based database abstraction layer.

Single rule: every function here works on BOTH SQLite and PostgreSQL.
The dialect is detected from the engine URL — callers never check.

Used by:
  - alems/agent/sync_client.py   (reads SQLite, builds sync payload)
  - alems/server/routers/sync.py (writes to PostgreSQL)
  - gui pages via get_engine()   (read-only queries, both modes)
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import os
import secrets
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).parent.parent.parent


# ── Engine factory ────────────────────────────────────────────────────────────

def get_engine(db_url: str | None = None) -> Engine:
    url = db_url or os.environ.get("ALEMS_DB_URL") or _default_sqlite_url()

    if url.startswith("sqlite"):
        return create_engine(
            url,
            connect_args={"check_same_thread": False},
            echo=False,
        )
    else:
        return create_engine(
            url,
            pool_pre_ping=True,
            pool_size=int(os.environ.get("ALEMS_DB_POOL_SIZE", 5)),
            max_overflow=int(os.environ.get("ALEMS_DB_MAX_OVERFLOW", 10)),
            pool_timeout=int(os.environ.get("ALEMS_DB_POOL_TIMEOUT", 60)),
            pool_recycle=int(os.environ.get("ALEMS_DB_POOL_RECYCLE", 1800)),
            echo=False,
        )


def is_postgres(engine: Engine) -> bool:
    return engine.dialect.name == "postgresql"


def is_sqlite(engine: Engine) -> bool:
    return engine.dialect.name == "sqlite"


@contextmanager
def get_session(engine: Engine) -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session


def _default_sqlite_url() -> str:
    return f"sqlite:///{PROJECT_ROOT / 'data' / 'experiments.db'}"


# ── Mode detection ────────────────────────────────────────────────────────────

def get_ui_mode(engine: Engine) -> str:
    """
    Returns the UI display mode based on the active database.
      'server'    → PostgreSQL, this is the Oracle VM
      'local'     → SQLite, no agent running or local mode
      'connected' → SQLite, but agent is in connected mode
    """
    if is_postgres(engine):
        return "server"
    # Check agent config for connected mode
    try:
        from alems.agent.mode_manager import get_mode
        return get_mode()
    except Exception:
        return "local"


def get_local_hw_id(engine: Engine) -> int | None:
    """Return hw_id of local machine (SQLite only). None on server."""
    if is_postgres(engine):
        return None
    with get_session(engine) as s:
        row = s.execute(text("SELECT hw_id FROM hardware_config LIMIT 1")).fetchone()
        return int(row[0]) if row else None


# ── Hardware upsert ───────────────────────────────────────────────────────────

def upsert_hardware(session: Session, hw_data: dict) -> int:
    """
    Upsert hardware_config by hardware_hash. Works on both dialects.
    Returns hw_id.
    """

    # Cast SQLite integer booleans to Python bool for PostgreSQL compatibility
    _BOOL_COLS = {
        "has_avx2", "has_avx512", "has_vmx", "gpu_power_available",
        "rapl_has_dram", "rapl_has_uncore",
    }
    hw_data = {
        k: (bool(v) if k in _BOOL_COLS and v is not None else v)
        for k, v in hw_data.items()
    }
    engine = session.get_bind()
    hardware_hash = hw_data["hardware_hash"]

    cols = ", ".join(hw_data.keys())
    placeholders = ", ".join(f":{k}" for k in hw_data.keys())

    if is_sqlite(engine):
        session.execute(text(f"""
            INSERT INTO hardware_config ({cols})
            VALUES ({placeholders})
            ON CONFLICT (hardware_hash) DO UPDATE SET
                last_seen    = datetime('now'),
                agent_status = :agent_status,
                agent_version = :agent_version
        """), hw_data)
    else:
        session.execute(text(f"""
            INSERT INTO hardware_config ({cols})
            VALUES ({placeholders})
            ON CONFLICT (hardware_hash) DO UPDATE SET
                last_seen     = NOW(),
                agent_status  = EXCLUDED.agent_status,
                agent_version = EXCLUDED.agent_version
        """), hw_data)

    row = session.execute(text(
        "SELECT hw_id FROM hardware_config WHERE hardware_hash = :h"
    ), {"h": hardware_hash}).fetchone()
    return int(row[0])


# ── API key management ────────────────────────────────────────────────────────

def generate_api_key() -> str:
    return secrets.token_urlsafe(32)


def get_or_create_api_key(session: Session, hw_id: int) -> str:
    """
    Returns existing api_key for hw_id, or generates and stores a new one.
    Stored in hardware_config — add column if not present (lazy migration).
    """
    engine = session.get_bind()

    # Ensure api_key column exists (lazy — avoids migration ordering issues)
    if is_sqlite(engine):
        try:
            session.execute(text(
                "ALTER TABLE hardware_config ADD COLUMN api_key TEXT"
            ))
            session.commit()
        except Exception:
            pass  # column already exists
    
    row = session.execute(text(
        "SELECT api_key FROM hardware_config WHERE hw_id = :id"
    ), {"id": hw_id}).fetchone()

    if row and row[0]:
        return row[0]

    key = generate_api_key()
    session.execute(text(
        "UPDATE hardware_config SET api_key = :key WHERE hw_id = :id"
    ), {"key": key, "id": hw_id})
    return key


def verify_api_key(session: Session, hardware_hash: str, api_key: str) -> int | None:
    """
    Verify api_key for hardware_hash. Returns hw_id if valid, None if not.
    """
    row = session.execute(text("""
        SELECT hw_id FROM hardware_config
        WHERE hardware_hash = :h AND api_key = :k
    """), {"h": hardware_hash, "k": api_key}).fetchone()
    return int(row[0]) if row else None


# ── Sync helpers ──────────────────────────────────────────────────────────────

def get_unsynced_runs(session: Session, limit: int = 100) -> list[dict]:
    """
    Fetch runs with sync_status=0. Works on both dialects.
    Returns list of dicts with all run columns.
    """
    rows = session.execute(text("""
        SELECT r.*
        FROM runs r
        WHERE r.sync_status = 0
          AND r.global_run_id IS NOT NULL
        ORDER BY r.run_id ASC
        LIMIT :limit
    """), {"limit": limit}).fetchall()
    return [dict(row._mapping) for row in rows]


def get_child_rows(session: Session, run_ids: list[int], table: str) -> list[dict]:
    """
    Fetch all rows from a child table for a list of run_ids.
    Used by sync_client to build the bulk-sync payload.
    """
    if not run_ids:
        return []
    placeholders = ",".join(str(i) for i in run_ids)
    rows = session.execute(text(
        f"SELECT * FROM {table} WHERE run_id IN ({placeholders})"
    )).fetchall()
    return [dict(row._mapping) for row in rows]


def get_experiments_for_runs(session: Session, exp_ids: list[int]) -> list[dict]:
    if not exp_ids:
        return []
    placeholders = ",".join(str(i) for i in exp_ids)
    rows = session.execute(text(
        f"SELECT * FROM experiments WHERE exp_id IN ({placeholders})"
    )).fetchall()
    return [dict(row._mapping) for row in rows]


def mark_runs_synced(session: Session, global_run_ids: list[str]) -> None:
    """Mark runs as synced. Works on both dialects."""
    if not global_run_ids:
        return
    placeholders = ",".join(f"'{rid}'" for rid in global_run_ids)
    session.execute(text(f"""
        UPDATE runs SET sync_status = 1
        WHERE global_run_id IN ({placeholders})
    """))


def mark_runs_failed(session: Session, global_run_ids: list[str]) -> None:
    """Mark runs as sync_failed (will retry)."""
    if not global_run_ids:
        return
    placeholders = ",".join(f"'{rid}'" for rid in global_run_ids)
    session.execute(text(f"""
        UPDATE runs SET sync_status = 2
        WHERE global_run_id IN ({placeholders})
    """))


# ── Job queue helpers ─────────────────────────────────────────────────────────

def get_next_job(session: Session, hw_id: int) -> dict | None:
    """
    Atomically fetch and claim next pending job for hw_id.
    Returns None if machine is busy or no jobs available.
    PostgreSQL only — not called from local agent in local mode.
    """
    # Check if machine already has a running job
    busy = session.execute(text("""
        SELECT job_id FROM job_queue
        WHERE dispatched_to_hw_id = :hw_id
          AND status = 'running'
        LIMIT 1
    """), {"hw_id": hw_id}).fetchone()

    if busy:
        return None

    # Find next available job for this machine
    job = session.execute(text("""
        SELECT * FROM job_queue
        WHERE status = 'pending'
          AND (target_hw_id IS NULL OR target_hw_id = :hw_id)
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
    """), {"hw_id": hw_id}).fetchone()

    if not job:
        return None

    job_dict = dict(job._mapping)

    # Atomically claim it
    engine = session.get_bind()
    now_expr = "NOW()" if is_postgres(engine) else "datetime('now')"
    session.execute(text(f"""
        UPDATE job_queue
        SET status = 'dispatched',
            dispatched_to_hw_id = :hw_id,
            dispatched_at = {now_expr}
        WHERE job_id = :job_id AND status = 'pending'
    """), {"hw_id": hw_id, "job_id": job_dict["job_id"]})

    return job_dict


def upsert_run_status_cache(session: Session, hw_id: int, data: dict) -> None:
    """
    Update live run status cache on the server.
    Called by the heartbeat handler when status='running'.
    PostgreSQL only.
    """
    cols = list(data.keys()) + ["hw_id"]
    vals = list(data.values()) + [hw_id]
    col_str = ", ".join(cols)
    ph_str  = ", ".join(f":{c}" for c in cols)
    update_str = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in data.keys()
        if c != "hw_id"
    )
    payload = dict(zip(cols, vals))
    payload["last_updated"] = "NOW()"

    session.execute(text(f"""
        INSERT INTO run_status_cache ({col_str}, last_updated)
        VALUES ({ph_str}, NOW())
        ON CONFLICT (hw_id) DO UPDATE SET
            {update_str},
            last_updated = NOW()
    """), payload)
