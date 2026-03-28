"""
alems/agent/heartbeat.py
────────────────────────────────────────────────────────────────────────────
All outbound HTTP calls from agent to server.
Every function is fire-and-forget with timeout — server being down
must never block or crash the agent.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Optional

import httpx

from alems.agent.mode_manager import (
    get_api_key, get_execution_config, get_local_hw_id,
    get_server_url, save_registration,
)
from alems.shared.models import (
    HeartbeatRequest, HeartbeatResponse,
    JobDetail, JobResponse,
    JobStatusRequest, LiveMetrics,
    RegisterRequest, RegisterResponse,
)

AGENT_VERSION = "1.0.0"
#TIMEOUT = 10  # coming from execution_config now, default 30s to allow for slow connections and server startup


def _headers() -> dict:
    api_key = get_api_key()
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _get_http_timeout() -> int:
    from alems.agent.mode_manager import get_sync_config
    return int(get_sync_config().get("http_timeout_seconds", 10))

def _post(url: str, payload: dict) -> Optional[dict]:
    try:
        r = httpx.post(url, json=payload, headers=_headers(),
                      timeout=_get_http_timeout())
        r.raise_for_status()
        return r.json()
    except httpx.TimeoutException:
        print(f"[heartbeat] Timeout: {url}")
    except httpx.HTTPStatusError as e:
        print(f"[heartbeat] HTTP {e.response.status_code}: {url}")
    except Exception as e:
        print(f"[heartbeat] Error {url}: {e}")
    return None

def _get(url: str, params: dict | None = None) -> Optional[dict]:
    try:
        r = httpx.get(url, params=params, headers=_headers(),
                     timeout=_get_http_timeout())
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[heartbeat] Error GET {url}: {e}")
    return None


# ── Registration ──────────────────────────────────────────────────────────────

def register(db_path: str) -> bool:
    server_url = get_server_url()
    hw = _read_hardware(db_path)
    if not hw:
        print("[heartbeat] No hardware_config row found — cannot register")
        return False

    hw["agent_version"] = AGENT_VERSION
    
    # Registration uses NO auth header — this call gets the api_key
    try:
        r = httpx.post(
            f"{server_url}/register",
            json=hw,
            headers={"Content-Type": "application/json"},  # no Bearer token
            timeout=_get_http_timeout(), 
        )
        r.raise_for_status()
        resp = r.json()
    except Exception as e:
        print(f"[heartbeat] Registration error: {e}")
        return False

    try:
        r = RegisterResponse(**resp)
        save_registration(r.api_key, r.server_hw_id)
        print(f"[heartbeat] Registered — server_hw_id={r.server_hw_id}")
        return True
    except Exception as e:
        print(f"[heartbeat] Registration parse error: {e}")
        return False


def _read_hardware(db_path: str) -> Optional[dict]:
    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM hardware_config LIMIT 1").fetchone()
        con.close()
        return dict(row) if row else None
    except Exception as e:
        print(f"[heartbeat] Could not read hardware_config: {e}")
        return None


# ── Heartbeat ─────────────────────────────────────────────────────────────────

def send_heartbeat(
    status: str,
    db_path: str,
    live: Optional[LiveMetrics] = None,
    unsynced_runs: int = 0,
    last_sync_at: Optional[str] = None,
) -> Optional[str]:
    """
    POST /heartbeat. Returns action string from server or None.
    Frequency: 30s idle, 5s during active run.
    """
    server_url = get_server_url()
    hw = _read_hardware(db_path)
    if not hw:
        return None

    payload = HeartbeatRequest(
        hardware_hash=hw["hardware_hash"],
        api_key=get_api_key(),
        status=status,
        agent_version=AGENT_VERSION,
        last_sync_at=last_sync_at,
        unsynced_runs=unsynced_runs,
        live=live,
    )

    resp = _post(f"{server_url}/heartbeat", payload.model_dump())
    if not resp:
        return None

    try:
        r = HeartbeatResponse(**resp)
        return r.action
    except Exception:
        return None


# ── Job poll ──────────────────────────────────────────────────────────────────

def fetch_job(db_path: str) -> Optional[JobDetail]:
    """
    GET /get-job. Returns JobDetail if a job is available, None otherwise.
    """
    server_url = get_server_url()
    hw = _read_hardware(db_path)
    if not hw:
        return None

    resp = _get(f"{server_url}/get-job", params={
        "hardware_hash": hw["hardware_hash"],
    })
    if not resp:
        return None

    try:
        r = JobResponse(**resp)
        return r.job
    except Exception as e:
        print(f"[heartbeat] Job parse error: {e}")
        return None


def report_job_status(
    job_id: str,
    status: str,
    db_path: str,
    global_run_id: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Report job started/completed/failed to server."""
    server_url = get_server_url()
    hw = _read_hardware(db_path)
    if not hw:
        return

    payload = JobStatusRequest(
        job_id=job_id,
        api_key=get_api_key(),
        hardware_hash=hw["hardware_hash"],
        status=status,
        error_message=error_message,
        global_run_id=global_run_id,
    )
    _post(f"{server_url}/job-status", payload.model_dump())


# ── Health check ──────────────────────────────────────────────────────────────

def check_server_health(server_url: Optional[str] = None) -> bool:
    """
    Returns True if server is reachable and healthy.
    Used by Streamlit UI to show 🟢/🔴 connection status.
    """
    url = server_url or get_server_url()
    try:
        r = httpx.get(f"{url}/health", timeout=5)
        return r.status_code == 200
    except Exception:
        return False
