# API Reference

Base URL: `http://129.153.71.47:8000`

All authenticated endpoints require:
```
Authorization: Bearer {api_key}
Content-Type: application/json
```

`api_key` is returned by `POST /register` and saved to `~/.alems/agent.conf`.

---

## GET /health

Mode detection endpoint. No authentication required.

**Response:**
```json
{
  "status": "ok",
  "mode": "server",
  "version": "1.0.0",
  "connected_agents": 3
}
```

Used by agents to verify server is reachable before switching to connected mode.

---

## POST /register

Register or re-register a machine. Safe to call on every agent start (idempotent).

**Request:** all `hardware_config` columns from local SQLite

```json
{
  "hardware_hash": "abc123...",
  "hostname": "researcher-laptop",
  "cpu_model": "Apple M2",
  "cpu_cores": 8,
  "ram_gb": 16,
  "agent_version": "1.0.0"
}
```

**Response:**
```json
{
  "api_key": "generated_secure_token",
  "server_hw_id": 3,
  "message": "registered"
}
```

---

## POST /heartbeat

Report machine status. Called every 30s (idle) or 5s (during active run).

**Request:**
```json
{
  "hardware_hash": "abc123...",
  "api_key": "token",
  "status": "running",
  "unsynced_runs": 2,
  "last_sync_at": "2026-03-27T10:00:00",
  "live": {
    "run_id": 1555,
    "exp_id": 437,
    "global_run_id": "uuid...",
    "task_name": "gsm8k_basic",
    "model_name": "cloud",
    "workflow_type": "linear",
    "elapsed_s": 42,
    "energy_uj": 1234567,
    "avg_power_watts": 8.3,
    "total_tokens": 892,
    "steps": 3
  }
}
```

**Response:**
```json
{
  "ok": true,
  "action": null
}
```

`action` values: `null` | `"sync_now"` | `"reregister"` | `"stop"`

---

## GET /get-job

Poll for next available job. Returns null if machine is busy or queue is empty.

**Query params:** `hardware_hash=abc123...`
**Header:** `Authorization: Bearer {api_key}`

**Response (job available):**
```json
{
  "job": {
    "job_id": "uuid...",
    "command": "python -m core.execution.tests.test_harness --task-id gsm8k_basic ...",
    "exp_config": {"task_id": "gsm8k_basic", "provider": "cloud", "repetitions": 3},
    "on_disconnect": "fail"
  }
}
```

**Response (no job):**
```json
{"job": null}
```

---

## POST /job-status

Report job lifecycle events.

**Request:**
```json
{
  "job_id": "uuid...",
  "api_key": "token",
  "hardware_hash": "abc123...",
  "status": "completed",
  "global_run_id": "uuid...",
  "error_message": null
}
```

`status` values: `started` | `completed` | `failed`

---

## POST /bulk-sync

Push all unsynced data from local SQLite to PostgreSQL.
All inserts use `ON CONFLICT DO NOTHING` — completely idempotent.

**Request:**
```json
{
  "hardware_hash": "abc123...",
  "api_key": "token",
  "hardware_data": {...},
  "experiments": [...],
  "runs": [...],
  "energy_samples": [...],
  "cpu_samples": [...],
  "thermal_samples": [...],
  "interrupt_samples": [...],
  "orchestration_events": [...],
  "llm_interactions": [...],
  "orchestration_tax_summary": [...]
}
```

**Response:**
```json
{
  "ok": true,
  "synced_run_ids": ["uuid1", "uuid2"],
  "rows_inserted": 847,
  "message": "synced 2 runs, 847 rows"
}
```

---

## POST /experiments/submit

Submit a local experiment to the global review queue.

**Request:**
```json
{
  "hardware_hash": "abc123...",
  "api_key": "token",
  "name": "ARM vs x86 energy comparison",
  "description": "Testing orchestration tax on different architectures",
  "config_json": "{\"task_id\": \"gsm8k_basic\", \"repetitions\": 10}"
}
```

---

## GET /experiments/queue

List all experiment submissions (admin view).

---

## POST /experiments/review/{submission_id}

Approve or reject a submission.

**Request:**
```json
{
  "action": "approve",
  "reviewed_by": "prof_smith",
  "notes": "Good experimental design, approved for ARM machine"
}
```

`action`: `approve` | `reject`

---

## GET /machines

Live machine status — used by Streamlit server dashboard.

**Response:**
```json
[
  {
    "hw_id": 1,
    "hostname": "researcher-laptop",
    "cpu_model": "Apple M2",
    "agent_status": "running",
    "last_seen": "2026-03-27T10:05:00",
    "run_status": "running",
    "task_name": "gsm8k_basic",
    "elapsed_s": 42,
    "total_runs": 1554
  }
]
```
