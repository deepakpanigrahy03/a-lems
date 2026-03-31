# Fleet Control

Fleet Control is the central hub for multi-host operations. Access it from the **Command Centre** sidebar section.

It looks different depending on where you run it:

| Context | URL | What you see |
|---------|-----|-------------|
| Local machine | `localhost:8501` | This machine + remote hosts if connected |
| Oracle VM server | `129.153.71.47:8502` | All registered machines, full admin |

---

## Tab 1 — 🖥 Fleet

Shows every registered machine with live status.

**Local mode:** your machine card with sync counters. If connected to server, the connected machines list appears below.

**Server mode:** full grid — one card per machine showing:

- Agent status (idle / running / syncing / offline)
- Last seen timestamp
- Total synced runs
- Live task if currently running (from `run_status_cache`)

---

## Tab 2 — ▶ Dispatch

Dispatch experiments to any host from a single form.

**How it works:**

1. Select target from the dropdown — includes `localhost` and all connected machines
2. Fill in Task ID, Provider, Repetitions, Model, Workflow, Country
3. Click **🚀 Dispatch Job**

**What happens:**

- **localhost selected** → job added to local Execute Run queue (go to Execute Run → ⚡ Live Execution to start)
- **remote host selected** → job inserted into `job_queue` in PostgreSQL → agent on that machine picks it up within 10 seconds → runs test_harness locally → syncs back

!!! note "Execute Run also has host selector"
    The Execute Run page has a host selector at the top. When a remote host is selected, the Start button dispatches via `job_queue` instead of running locally. All 4 tabs (Create & Queue, Live Execution, Session Analysis, Run History) remain available.

---

## Tab 3 — ⬡ Job Queue

Live view of all jobs across all machines.

| Status | Meaning |
|--------|---------|
| pending | Waiting for an agent to pick up |
| dispatched | Claimed by agent, not yet started |
| running | Executing on target machine |
| completed | Done — run synced back |
| failed | Error — see error_message column |

**Actions (server mode):** Cancel pending/dispatched jobs, boost priority.

Jobs are picked up within 10 seconds of creation (agent poll interval).

---

## Tab 4 — ⟳ Sync & Connect (local mode only)

See [Sync & Backload Operations](../developer-guide/13-sync-operations.md) for full detail.

**Quick reference:**

```
Connect    → set mode=connected, enter server URL, restart agent
Disconnect → set mode=local
Sync now   → trigger immediate phase-1 sync (run metadata)
Reset failed → mark sync_status=2 rows as 0 for retry
Sync samples → trigger phase-2 sync (energy/cpu/thermal samples)
```

---

## Execute Run behaviour by host

| Target | What happens on Start |
|--------|----------------------|
| localhost | Runs in local thread, live output shown in ⚡ tab |
| remote machine | Inserts into `job_queue`, agent picks up, result syncs back |

Live output is only available for localhost runs. Remote run status is visible in the Job Queue tab.
