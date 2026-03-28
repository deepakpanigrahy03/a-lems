"""
alems/shared/models.py
────────────────────────────────────────────────────────────────────────────
Pydantic models shared by the agent (client) and server (FastAPI).
These define the HTTP API contract — change here = change everywhere.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
from typing import Any, Optional
from pydantic import BaseModel, Field


# ── Registration ──────────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    hardware_hash:       str
    hostname:            str
    cpu_model:           Optional[str] = None
    cpu_cores:           Optional[int] = None
    cpu_threads:         Optional[int] = None
    ram_gb:              Optional[int] = None
    kernel_version:      Optional[str] = None
    microcode_version:   Optional[str] = None
    rapl_domains:        Optional[str] = None
    cpu_architecture:    Optional[str] = None
    cpu_vendor:          Optional[str] = None
    cpu_family:          Optional[int] = None
    cpu_model_id:        Optional[int] = None
    cpu_stepping:        Optional[int] = None
    has_avx2:            Optional[bool] = None
    has_avx512:          Optional[bool] = None
    has_vmx:             Optional[bool] = None
    gpu_model:           Optional[str] = None
    gpu_driver:          Optional[str] = None
    gpu_count:           Optional[int] = None
    gpu_power_available: Optional[bool] = None
    rapl_has_dram:       Optional[bool] = None
    rapl_has_uncore:     Optional[bool] = None
    system_manufacturer: Optional[str] = None
    system_product:      Optional[str] = None
    system_type:         Optional[str] = None
    virtualization_type: Optional[str] = None
    agent_version:       Optional[str] = None


class RegisterResponse(BaseModel):
    api_key:        str
    server_hw_id:   int
    message:        str = "registered"


# ── Heartbeat ─────────────────────────────────────────────────────────────────

class LiveMetrics(BaseModel):
    """Carried in heartbeat only during an active run."""
    run_id:         Optional[int]   = None
    exp_id:         Optional[int]   = None
    global_run_id:  Optional[str]   = None
    job_id:         Optional[str]   = None
    task_name:      Optional[str]   = None
    model_name:     Optional[str]   = None
    workflow_type:  Optional[str]   = None
    elapsed_s:      Optional[int]   = None
    energy_uj:      Optional[int]   = None
    avg_power_watts: Optional[float] = None
    total_tokens:   Optional[int]   = None
    steps:          Optional[int]   = None


class HeartbeatRequest(BaseModel):
    hardware_hash:  str
    api_key:        str
    status:         str             # idle | running | syncing | error
    agent_version:  Optional[str]   = None
    last_sync_at:   Optional[str]   = None   # ISO timestamp
    unsynced_runs:  int             = 0
    live:           Optional[LiveMetrics] = None


class HeartbeatResponse(BaseModel):
    ok:             bool = True
    action:         Optional[str] = None
    # action values: None | "sync_now" | "reregister" | "stop"


# ── Job ───────────────────────────────────────────────────────────────────────

class JobResponse(BaseModel):
    job:            Optional[JobDetail] = None


class JobDetail(BaseModel):
    job_id:         str
    command:        str             # exact CLI command to subprocess
    exp_config:     dict[str, Any]
    on_disconnect:  str = "fail"


class JobStatusRequest(BaseModel):
    job_id:         str
    api_key:        str
    hardware_hash:  str
    status:         str             # started | completed | failed
    error_message:  Optional[str]   = None
    global_run_id:  Optional[str]   = None


# ── Bulk sync ─────────────────────────────────────────────────────────────────

class BulkSyncPayload(BaseModel):
    hardware_hash:              str
    api_key:                    str
    hardware_data:              dict[str, Any]
    experiments:                list[dict[str, Any]] = Field(default_factory=list)
    runs:                       list[dict[str, Any]] = Field(default_factory=list)
    energy_samples:             list[dict[str, Any]] = Field(default_factory=list)
    cpu_samples:                list[dict[str, Any]] = Field(default_factory=list)
    thermal_samples:            list[dict[str, Any]] = Field(default_factory=list)
    interrupt_samples:          list[dict[str, Any]] = Field(default_factory=list)
    orchestration_events:       list[dict[str, Any]] = Field(default_factory=list)
    llm_interactions:           list[dict[str, Any]] = Field(default_factory=list)
    orchestration_tax_summary:  list[dict[str, Any]] = Field(default_factory=list)
    environment_config:          list[dict[str, Any]] = Field(default_factory=list)
    idle_baselines:              list[dict[str, Any]] = Field(default_factory=list)
    task_categories:             list[dict[str, Any]] = Field(default_factory=list)
    outliers:                    list[dict[str, Any]] = Field(default_factory=list)


class BulkSyncResponse(BaseModel):
    ok:             bool
    synced_run_ids: list[int] = Field(default_factory=list)  # local run_ids
    rows_inserted:  int = 0
    message:        str = "ok"


# ── Experiment submission ─────────────────────────────────────────────────────

class ExperimentSubmitRequest(BaseModel):
    hardware_hash:  str
    api_key:        str
    name:           str
    description:    Optional[str] = None
    config_json:    str           # JSON string of experiment config


class SubmissionReviewRequest(BaseModel):
    action:         str           # approve | reject
    reviewed_by:    str
    notes:          Optional[str] = None


# ── Mode detection ────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status:         str = "ok"
    mode:           str = "server"
    version:        str = "1.0.0"
    connected_agents: int = 0
