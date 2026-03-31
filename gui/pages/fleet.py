"""
gui/pages/fleet.py  —  ◈  Fleet Control
────────────────────────────────────────────────────────────────────────────
Unified multi-host management page. Replaces:
  multi_host_status, multi_host_dispatch, dispatch_queue,
  sync_monitor, experiment_submissions

Four tabs:
  🖥 Fleet       — all machines, connection status, sync health
  ▶ Dispatch    — choose host(s), build job, dispatch or run locally
  ⬡ Job Queue   — live job table, cancel/boost priority
  ⟳ Sync        — connect/disconnect/backload, per-machine sync health

LOCAL mode  (streamlit_app.py, port 8501):
  - Fleet tab: shows this machine only + server summary if connected
  - Dispatch tab: localhost always available; server machines if connected
  - Sync tab: connect/disconnect controls, backload trigger

SERVER mode (streamlit_server.py, port 8502):
  - Full view across all registered machines
  - No connect/disconnect (server is always connected)
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations
import streamlit as st
from gui.pages._agent_utils import get_ui_mode, is_server_alive, get_server_url

ACCENT = "#22c55e"


def render(ctx: dict) -> None:
    mode = get_ui_mode()
    _header(mode)

    if mode == "server":
        tab1, tab2, tab3, tab4 = st.tabs([
            "🖥 Fleet", "▶ Dispatch", "⬡ Job Queue", "⟳ Sync"
        ])
        with tab1: _fleet_server()
        with tab2: _dispatch_server()
        with tab3: _jobqueue_server()
        with tab4: _sync_server()
    else:
        tab1, tab2, tab3, tab4 = st.tabs([
            "🖥 Fleet", "▶ Dispatch", "⬡ Job Queue", "⟳ Sync & Connect"
        ])
        with tab1: _fleet_local(mode)
        with tab2: _dispatch_local(mode)
        with tab3: _jobqueue_local(mode)
        with tab4: _sync_local(mode)


def _header(mode: str) -> None:
    badge = {
        "server":    ("🌐 SERVER",    "#22c55e"),
        "connected": ("🔗 CONNECTED", "#3b82f6"),
        "local":     ("💻 LOCAL",     "#f59e0b"),
    }.get(mode, ("?", "#475569"))
    st.markdown(
        f"<div style='padding:14px 20px;background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:16px;"
        f"display:flex;align-items:center;justify-content:space-between;'>"
        f"<div><div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:3px;'>◈ Fleet Control</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>Multi-host dispatch, sync health, and job queue.</div></div>"
        f"<div style='font-size:9px;padding:3px 10px;border-radius:4px;"
        f"background:{badge[1]}22;color:{badge[1]};border:1px solid {badge[1]}44;"
        f"font-family:IBM Plex Mono,monospace;font-weight:700;'>{badge[0]}</div></div>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — FLEET
# ══════════════════════════════════════════════════════════════════════════════

def _fleet_server() -> None:
    from gui.db_pg import q
    machines = q("""
        SELECT h.hw_id, h.hostname, h.cpu_model,
               h.cpu_architecture, h.virtualization_type,
               h.agent_status, h.last_seen, h.agent_version,
               h.server_hw_id,
               COUNT(r.global_run_id)  AS total_runs,
               MAX(r.synced_at)        AS last_sync,
               rsc.status              AS live_status,
               rsc.task_name           AS live_task,
               rsc.elapsed_s           AS live_elapsed,
               MAX(ec.os_name)         AS os_name,
               MAX(ec.os_version)      AS os_version
        FROM hardware_config h
        LEFT JOIN runs r              ON r.hw_id = h.hw_id
        LEFT JOIN experiments e       ON e.hw_id = h.hw_id
        LEFT JOIN environment_config ec ON ec.env_id = e.env_id
        LEFT JOIN run_status_cache rsc ON rsc.hw_id = h.hw_id
        GROUP BY h.hw_id, h.hostname, h.cpu_model,
                 h.cpu_architecture, h.virtualization_type,
                 h.agent_status, h.last_seen, h.agent_version, h.server_hw_id,
                 rsc.status, rsc.task_name, rsc.elapsed_s
        ORDER BY h.last_seen DESC NULLS LAST
    """)
    _machine_grid(machines, admin=True)


def _fleet_local(mode: str) -> None:
    from gui.db import q1
    hw = q1("SELECT * FROM hardware_config LIMIT 1")
    sync = q1("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN sync_status=0 THEN 1 ELSE 0 END) as unsynced,
            SUM(CASE WHEN sync_status=1 THEN 1 ELSE 0 END) as synced,
            SUM(CASE WHEN sync_status=2 THEN 1 ELSE 0 END) as failed
        FROM runs
    """)
    _local_machine_card(hw, sync, mode)

    if mode == "connected":
        st.markdown("---")
        st.markdown(
            "<div style='font-size:11px;font-weight:600;color:#3b82f6;"
            "text-transform:uppercase;margin-bottom:8px;'>Connected machines (from server)</div>",
            unsafe_allow_html=True,
        )
        try:
            import httpx
            from alems.agent.mode_manager import get_api_key
            r = httpx.get(f"{get_server_url()}/machines",
                          headers={"Authorization": f"Bearer {get_api_key()}"}, timeout=5)
            if r.status_code == 200:
                import pandas as pd
                df = pd.DataFrame(r.json())
                if not df.empty:
                    st.dataframe(df[["hostname","agent_status","last_seen","total_runs"]
                                    if "total_runs" in df.columns
                                    else [c for c in ["hostname","agent_status","last_seen"] if c in df.columns]],
                                 use_container_width=True, hide_index=True)
        except Exception as e:
            st.warning(f"Could not fetch server machines: {e}")


def _local_machine_card(hw: dict, sync: dict, mode: str) -> None:
    import datetime
    hostname = hw.get("hostname", "this machine")
    cpu      = hw.get("cpu_model", "—")
    os_      = hw.get("os_name", "—")
    synced   = int(sync.get("synced", 0) or 0)
    unsynced = int(sync.get("unsynced", 0) or 0)
    failed   = int(sync.get("failed", 0) or 0)
    total    = int(sync.get("total", 0) or 0)
    status_clr = "#22c55e" if mode == "connected" else "#f59e0b"
    status_txt = "Connected" if mode == "connected" else "Local only"

    st.markdown(
        f"<div style='padding:16px 20px;background:#0d1117;border:1px solid {status_clr}33;"
        f"border-left:4px solid {status_clr};border-radius:10px;margin-bottom:12px;'>"
        f"<div style='display:flex;justify-content:space-between;align-items:flex-start;'>"
        f"<div><div style='font-size:14px;font-weight:700;color:#f1f5f9;margin-bottom:4px;'>"
        f"💻 {hostname}</div>"
        f"<div style='font-size:10px;color:#64748b;font-family:IBM Plex Mono,monospace;'>"
        f"{cpu} · {os_}</div></div>"
        f"<div style='font-size:9px;padding:3px 10px;border-radius:4px;"
        f"background:{status_clr}22;color:{status_clr};border:1px solid {status_clr}44;"
        f"font-weight:700;'>{status_txt}</div></div>"
        f"<div style='display:flex;gap:24px;margin-top:12px;font-size:11px;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"<span style='color:#94a3b8;'>total <b style='color:#f1f5f9;'>{total}</b></span>"
        f"<span style='color:#22c55e;'>synced <b>{synced}</b></span>"
        f"<span style='color:#f59e0b;'>pending <b>{unsynced}</b></span>"
        f"<span style='color:#ef4444;'>failed <b>{failed}</b></span>"
        f"</div></div>",
        unsafe_allow_html=True,
    )
    if mode == "connected":
        from alems.agent.mode_manager import get_server_url as _su, get_server_hw_id
        sv = _su()
        shw = get_server_hw_id()
        st.markdown(
            f"<div style='font-size:10px;color:#64748b;font-family:IBM Plex Mono,monospace;"
            f"padding:8px 12px;background:#090d13;border-radius:6px;margin-top:4px;'>"
            f"server: <b style='color:#3b82f6;'>{sv}</b> · hw_id on server: <b>{shw}</b></div>",
            unsafe_allow_html=True,
        )


def _machine_grid(machines, admin: bool = False) -> None:
    if machines.empty:
        st.info("No machines registered yet.")
        return
    for _, m in machines.iterrows():
        _machine_card(dict(m), admin=admin)


def _machine_card(m: dict, admin: bool = False) -> None:
    import datetime
    status  = m.get("agent_status", "offline")
    clr     = {"idle": "#22c55e", "running": "#f59e0b", "syncing": "#3b82f6",
                "busy": "#f59e0b"}.get(status, "#475569")
    host    = m.get("hostname") or f"hw_{m.get('hw_id')}"
    cpu     = m.get("cpu_model", "—")
    os_name = m.get("os_name", "")
    os_ver  = m.get("os_version", "")
    os_str  = f"{os_name} {os_ver}".strip() or "—"
    runs    = int(m.get("total_runs") or 0)
    seen    = str(m.get("last_seen") or "never")[:16]
    live    = m.get("live_status", "")
    task    = m.get("live_task", "")
    elapsed = m.get("live_elapsed")

    live_html = ""
    if live == "running" and task:
        mins = f"{int(elapsed)//60}m {int(elapsed)%60}s" if elapsed else "—"
        live_html = (f"<div style='margin-top:6px;font-size:10px;color:#f59e0b;"
                     f"font-family:IBM Plex Mono,monospace;'>▶ running: {task} · {mins}</div>")

    with st.expander(f"{'🟢' if status != 'offline' else '⚫'} {host}  ·  {status}  ·  {runs:,} runs", expanded=(status != "offline")):
        st.markdown(
            f"<div style='font-size:10px;color:#94a3b8;font-family:IBM Plex Mono,monospace;"
            f"line-height:1.9;'>"
            f"cpu: <b style='color:#f1f5f9;'>{cpu}</b><br>"
            f"os: <b style='color:#f1f5f9;'>{os_str}</b><br>"
            f"status: <b style='color:{clr};'>{status}</b><br>"
            f"last seen: {seen}<br>"
            f"synced runs: <b style='color:#a78bfa;'>{runs:,}</b>"
            f"{live_html}</div>",
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — DISPATCH
# ══════════════════════════════════════════════════════════════════════════════

def _build_job_form(key_prefix: str) -> dict | None:
    """Shared job config form. Returns dict or None if not submitted."""
    import json
    from gui.config import CONFIG_DIR
    import yaml

    c1, c2, c3 = st.columns(3)
    with c1:
        task_id  = st.text_input("Task ID", value="gsm8k_basic", key=f"{key_prefix}_task")
    with c2:
        provider = st.selectbox("Provider", ["cloud", "local", "groq", "openrouter"],
                                key=f"{key_prefix}_prov")
    with c3:
        reps = st.number_input("Repetitions", 1, 50, 3, key=f"{key_prefix}_reps")

    c4, c5 = st.columns(2)
    with c4:
        model    = st.text_input("Model (optional)", key=f"{key_prefix}_model")
    with c5:
        workflow = st.selectbox("Workflow", ["linear", "agentic", "both"],
                                key=f"{key_prefix}_wf")
    country  = st.selectbox("Country", ["US", "GB", "DE", "FR", "IN", "SG"],
                             key=f"{key_prefix}_country")

    cfg = {"task_id": task_id, "provider": provider, "repetitions": reps,
           "workflow_type": workflow, "country": country}
    if model:
        cfg["model_name"] = model
    return cfg


def _dispatch_server() -> None:
    """Server mode — dispatch to any connected machine."""
    from gui.db_pg import q
    import json

    machines = q("""
        SELECT hw_id, hostname, agent_status, last_seen
        FROM hardware_config
        ORDER BY last_seen DESC NULLS LAST
    """)

    if machines.empty:
        st.warning("No machines registered. Agents must register before dispatch.")
        return

    online = machines[machines.agent_status.isin(["idle", "connected", "syncing"])]

    st.markdown(
        f"<div style='font-size:11px;color:#94a3b8;margin-bottom:12px;'>"
        f"{len(online)} machine(s) online of {len(machines)} registered</div>",
        unsafe_allow_html=True,
    )

    # Host selector
    host_options = {"🌐 All connected machines": None}
    for _, m in online.iterrows():
        host_options[f"{'🟢'} {m['hostname']} (hw_id={m['hw_id']})"] = int(m["hw_id"])

    chosen_label = st.selectbox("Target machine", list(host_options.keys()),
                                key="fleet_dispatch_target")
    target_hw_id = host_options[chosen_label]

    cfg = _build_job_form("srv_dispatch")

    if st.button("🚀 Dispatch Job", type="primary", use_container_width=True,
                 key="fleet_dispatch_btn"):
        _submit_jobs(cfg, target_hw_id, online)


def _dispatch_local(mode: str) -> None:
    """Local/connected mode — localhost always + remote hosts if connected."""
    import json
    from gui.db import q1

    hw = q1("SELECT hw_id, hostname FROM hardware_config LIMIT 1")
    localhost = f"💻 {hw.get('hostname','localhost')} (this machine)"

    host_options = {localhost: "local"}

    if mode == "connected" and is_server_alive():
        try:
            import httpx
            from alems.agent.mode_manager import get_api_key
            r = httpx.get(f"{get_server_url()}/machines",
                          headers={"Authorization": f"Bearer {get_api_key()}"}, timeout=5)
            if r.status_code == 200:
                for m in r.json():
                    if m.get("agent_status") in ("idle", "connected", "syncing"):
                        label = f"🟢 {m['hostname']} (hw_id={m['hw_id']})"
                        host_options[label] = int(m["hw_id"])
        except Exception:
            pass

    chosen_label = st.selectbox("Target machine", list(host_options.keys()),
                                key="fleet_local_target")
    target = host_options[chosen_label]

    cfg = _build_job_form("local_dispatch")

    if st.button("🚀 Dispatch Job", type="primary", use_container_width=True,
                 key="fleet_local_dispatch_btn"):
        if target == "local":
            # Queue to local execute run via session state
            from alems.agent.job_executor import build_command
            item = {
                "name":     f"{cfg['task_id']} / {cfg['provider']}",
                "task":     cfg["task_id"],
                "provider": cfg["provider"],
                "reps":     cfg["repetitions"],
                "country":  cfg.get("country", "US"),
                "mode":     "single",
            }
            if "ex_queue" not in st.session_state:
                st.session_state.ex_queue = []
            st.session_state.ex_queue.append(item)
            st.success("✅ Added to local Execute Run queue — go to Execute Run to start.")
        else:
            if mode != "connected":
                st.error("Not connected to server. Start agent first.")
            else:
                try:
                    import httpx, json
                    from alems.agent.mode_manager import get_api_key
                    from alems.agent.mode_manager import get_server_hw_id
                    payload = {
                        "hardware_hash": "",
                        "api_key": get_api_key(),
                        "experiment_config_json": json.dumps(cfg),
                        "target_hw_id": target,
                        "priority": 5,
                    }
                    r = httpx.post(f"{get_server_url()}/jobs/submit",
                                   json=payload, timeout=10)
                    if r.status_code == 200:
                        st.success(f"✅ Job dispatched to hw_id={target}")
                    else:
                        st.error(f"Server returned {r.status_code}: {r.text}")
                except Exception as e:
                    st.error(f"Dispatch failed: {e}")


def _submit_jobs(cfg: dict, target_hw_id, online) -> None:
    """Insert job(s) into job_queue via PostgreSQL."""
    import json, os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text

    engine  = get_engine(os.environ.get("ALEMS_DB_URL"))
    cfg_json = json.dumps(cfg)
    count = 0
    with get_session(engine) as session:
        if target_hw_id is None:
            # All connected machines
            for _, m in online.iterrows():
                session.execute(text("""
                    INSERT INTO job_queue
                        (experiment_config_json, status, priority, target_hw_id)
                    VALUES (:cfg, 'pending', 5, :hw)
                """), {"cfg": cfg_json, "hw": int(m["hw_id"])})
                count += 1
        else:
            session.execute(text("""
                INSERT INTO job_queue
                    (experiment_config_json, status, priority, target_hw_id)
                VALUES (:cfg, 'pending', 5, :hw)
            """), {"cfg": cfg_json, "hw": target_hw_id})
            count = 1
        session.commit()

    st.success(f"✅ {count} job(s) queued. Agents will pick up within 10 seconds.")
    st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — JOB QUEUE
# ══════════════════════════════════════════════════════════════════════════════

def _jobqueue_server() -> None:
    from gui.db_pg import q
    import pandas as pd

    jobs = q("""
        SELECT j.job_id, j.status, j.priority,
               h_target.hostname  AS target_host,
               h_run.hostname     AS running_on,
               j.created_at, j.dispatched_at, j.started_at, j.completed_at,
               j.retry_count, j.error_message,
               j.experiment_config_json
        FROM job_queue j
        LEFT JOIN hardware_config h_target ON h_target.hw_id = j.target_hw_id
        LEFT JOIN hardware_config h_run    ON h_run.hw_id    = j.dispatched_to_hw_id
        ORDER BY
            CASE j.status WHEN 'running' THEN 0 WHEN 'dispatched' THEN 1
                          WHEN 'pending' THEN 2 ELSE 3 END,
            j.priority DESC, j.created_at DESC
        LIMIT 200
    """)

    _job_summary_cards(jobs)
    st.markdown("---")

    flt = st.selectbox("Filter", ["all","pending","running","dispatched","completed","failed"],
                       key="fleet_jq_filter")
    if flt != "all":
        jobs = jobs[jobs.status == flt]

    for _, job in jobs.iterrows():
        _job_row(dict(job), admin=True)


def _jobqueue_local(mode: str) -> None:
    if mode == "local":
        st.info("Job queue requires server connection. Connect in the Sync & Connect tab.")
        return
    if not is_server_alive():
        st.warning("Server unreachable.")
        return
    try:
        import httpx
        from alems.agent.mode_manager import get_api_key
        r = httpx.get(f"{get_server_url()}/machines",
                      headers={"Authorization": f"Bearer {get_api_key()}"}, timeout=5)
        st.info("Full job queue is visible on server dashboard (port 8502).")
        st.markdown(f"Server: `{get_server_url()}`")
    except Exception as e:
        st.error(str(e))


def _job_summary_cards(jobs) -> None:
    if jobs.empty:
        return
    from collections import Counter
    counts = Counter(jobs.status.tolist())
    STATUS_CLR = {"pending":"#f59e0b","dispatched":"#3b82f6","running":"#22c55e",
                  "completed":"#475569","failed":"#ef4444"}
    cols = st.columns(5)
    for col, s in zip(cols, ["pending","dispatched","running","completed","failed"]):
        clr = STATUS_CLR[s]
        with col:
            st.markdown(
                f"<div style='padding:8px;background:#0d1117;border:1px solid {clr}33;"
                f"border-left:3px solid {clr};border-radius:6px;text-align:center;'>"
                f"<div style='font-size:18px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;'>{counts.get(s,0)}</div>"
                f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;'>{s}</div></div>",
                unsafe_allow_html=True,
            )


def _job_row(job: dict, admin: bool = False) -> None:
    import json, os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text

    STATUS_CLR = {"pending":"#f59e0b","dispatched":"#3b82f6","running":"#22c55e",
                  "completed":"#475569","failed":"#ef4444"}
    status = job.get("status","?")
    clr    = STATUS_CLR.get(status, "#475569")
    jid    = str(job.get("job_id",""))[:12]

    try:
        cfg   = json.loads(job.get("experiment_config_json") or "{}")
        task  = cfg.get("task_id", "—")
        prov  = cfg.get("provider","—")
    except Exception:
        task, prov = "—", "—"

    target = job.get("target_host") or "any"
    runner = job.get("running_on") or "—"

    with st.expander(
        f"[{status.upper()}]  {jid}…  ·  {task}/{prov}  →  {target}",
        expanded=(status == "running"),
    ):
        st.markdown(
            f"<div style='font-size:10px;font-family:IBM Plex Mono,monospace;"
            f"color:#94a3b8;line-height:1.9;'>"
            f"status: <b style='color:{clr};'>{status}</b> · "
            f"target: <b style='color:#f1f5f9;'>{target}</b> · "
            f"running on: <b style='color:#f1f5f9;'>{runner}</b><br>"
            f"created: {str(job.get('created_at',''))[:16]} · "
            f"started: {str(job.get('started_at',''))[:16]}<br>"
            f"retries: {job.get('retry_count',0)}"
            + (f"<br><span style='color:#ef4444;'>error: {job.get('error_message','')}</span>"
               if job.get("error_message") else "")
            + "</div>",
            unsafe_allow_html=True,
        )
        if admin and status in ("pending", "dispatched"):
            c1, c2 = st.columns(2)
            job_id = str(job.get("job_id", ""))
            with c1:
                if st.button("Cancel", key=f"fleet_cancel_{jid}"):
                    engine = get_engine(os.environ.get("ALEMS_DB_URL"))
                    with get_session(engine) as s:
                        s.execute(text(
                            "UPDATE job_queue SET status='cancelled' "
                            "WHERE job_id=:id AND status IN ('pending','dispatched')"
                        ), {"id": job_id})
                        s.commit()
                    st.rerun()
            with c2:
                if st.button("↑ Priority", key=f"fleet_prio_{jid}"):
                    engine = get_engine(os.environ.get("ALEMS_DB_URL"))
                    with get_session(engine) as s:
                        s.execute(text(
                            "UPDATE job_queue SET priority=priority+1 WHERE job_id=:id"
                        ), {"id": job_id})
                        s.commit()
                    st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — SYNC & CONNECT
# ══════════════════════════════════════════════════════════════════════════════

def _sync_server() -> None:
    """Server sync view — all machines, sync log."""
    from gui.db_pg import q
    import pandas as pd

    machines = q("""
        SELECT h.hw_id, h.hostname, h.agent_status, h.last_seen,
               COUNT(r.global_run_id) AS synced_runs,
               MAX(r.synced_at)       AS last_sync
        FROM hardware_config h
        LEFT JOIN runs r ON r.hw_id = h.hw_id
        GROUP BY h.hw_id, h.hostname, h.agent_status, h.last_seen
        ORDER BY h.last_seen DESC NULLS LAST
    """)

    st.markdown(
        "<div style='font-size:11px;font-weight:600;color:#a78bfa;"
        "text-transform:uppercase;margin-bottom:8px;'>Machine sync health</div>",
        unsafe_allow_html=True,
    )
    for _, m in machines.iterrows():
        clr  = "#22c55e" if m.agent_status != "offline" else "#475569"
        seen = str(m.last_seen or "never")[:16]
        sync = str(m.last_sync or "never")[:16]
        st.markdown(
            f"<div style='padding:10px 14px;background:#0d1117;"
            f"border:1px solid {clr}33;border-left:3px solid {clr};"
            f"border-radius:8px;margin-bottom:6px;font-size:10px;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"<b style='color:#f1f5f9;'>{m.hostname}</b> · "
            f"<span style='color:{clr};'>{m.agent_status}</span> · "
            f"synced: <b style='color:#a78bfa;'>{int(m.synced_runs or 0):,}</b> · "
            f"last seen: {seen} · last sync: {sync}</div>",
            unsafe_allow_html=True,
        )

    logs = q("""
        SELECT s.sync_started_at, s.sync_completed_at, s.runs_synced,
               s.rows_total, s.status, h.hostname
        FROM sync_log s
        LEFT JOIN hardware_config h ON h.hw_id = s.hw_id
        ORDER BY s.sync_started_at DESC LIMIT 50
    """)
    if not logs.empty:
        st.markdown(
            "<div style='font-size:11px;font-weight:600;color:#a78bfa;"
            "text-transform:uppercase;margin:12px 0 6px;'>Recent sync log</div>",
            unsafe_allow_html=True,
        )
        st.dataframe(logs, use_container_width=True, hide_index=True)


def _sync_local(mode: str) -> None:
    """Local machine: connect/disconnect + backload controls."""
    from gui.db import q1
    from alems.agent.mode_manager import (
        get_mode, set_mode, get_server_url as _gsu, get_api_key, is_registered
    )

    # ── Connection status card ─────────────────────────────────────────────
    current_mode = get_mode()
    server_url   = _gsu()
    registered   = is_registered()
    server_alive = is_server_alive()

    if current_mode == "connected":
        conn_clr, conn_txt = "#22c55e", "Connected"
    else:
        conn_clr, conn_txt = "#f59e0b", "Local only"

    st.markdown(
        f"<div style='padding:14px 18px;background:#0d1117;"
        f"border:1px solid {conn_clr}33;border-left:4px solid {conn_clr};"
        f"border-radius:10px;margin-bottom:14px;'>"
        f"<div style='display:flex;justify-content:space-between;align-items:center;'>"
        f"<div style='font-size:13px;font-weight:700;color:#f1f5f9;'>Connection Status</div>"
        f"<div style='font-size:9px;padding:3px 10px;border-radius:4px;"
        f"background:{conn_clr}22;color:{conn_clr};border:1px solid {conn_clr}44;"
        f"font-weight:700;font-family:IBM Plex Mono,monospace;'>{conn_txt}</div></div>"
        f"<div style='font-size:10px;color:#94a3b8;font-family:IBM Plex Mono,monospace;"
        f"margin-top:8px;line-height:1.9;'>"
        f"mode: <b style='color:#f1f5f9;'>{current_mode}</b><br>"
        f"server: <b style='color:#3b82f6;'>{server_url}</b><br>"
        f"registered: <b style='color:{'#22c55e' if registered else '#ef4444'};'>"
        f"{'yes' if registered else 'no'}</b><br>"
        f"server reachable: <b style='color:{'#22c55e' if server_alive else '#ef4444'};'>"
        f"{'yes' if server_alive else 'no'}</b></div></div>",
        unsafe_allow_html=True,
    )

    # ── Connect / Disconnect buttons ───────────────────────────────────────
    c1, c2 = st.columns(2)
    with c1:
        if current_mode != "connected":
            new_url = st.text_input("Server URL", value=server_url or "http://129.153.71.47:8000",
                                    key="fleet_server_url")
            if st.button("🔗 Connect", type="primary", use_container_width=True,
                         key="fleet_connect_btn"):
                try:
                    from alems.agent.mode_manager import _write_conf, _read_raw
                    conf = _read_raw()
                    # _read_raw returns nested {section: {key: val}} — agent section
                    if "agent" not in conf or not isinstance(conf.get("agent"), dict):
                        conf["agent"] = {}
                    conf["agent"]["server_url"] = new_url
                    conf["agent"]["mode"] = "connected"
                    _write_conf(conf)
                    st.success("Mode set to connected. Restart agent to activate.")
                    st.code("python -m alems.agent start")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
        else:
            if st.button("⏹ Disconnect", use_container_width=True, key="fleet_disconnect_btn"):
                set_mode("local")
                st.success("Switched to local mode.")
                st.rerun()

    with c2:
        if st.button("⟳ Check server", use_container_width=True, key="fleet_check_btn"):
            alive = is_server_alive()
            if alive:
                st.success("Server reachable ✓")
            else:
                st.error(f"Cannot reach {server_url}")

    st.markdown("---")

    # ── Sync status ────────────────────────────────────────────────────────
    stats = q1("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN sync_status=0 THEN 1 ELSE 0 END) as unsynced,
            SUM(CASE WHEN sync_status=1 THEN 1 ELSE 0 END) as synced,
            SUM(CASE WHEN sync_status=2 THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN sync_samples_status=0 AND sync_status=1 THEN 1 ELSE 0 END) as samples_pending
        FROM runs
    """) or {}

    total          = int(stats.get("total", 0) or 0)
    synced         = int(stats.get("synced", 0) or 0)
    unsynced       = int(stats.get("unsynced", 0) or 0)
    failed         = int(stats.get("failed", 0) or 0)
    samples_pending= int(stats.get("samples_pending", 0) or 0)

    cols = st.columns(5)
    for col, val, label, clr in [
        (cols[0], total,          "Total",          "#94a3b8"),
        (cols[1], synced,         "Synced",          "#22c55e"),
        (cols[2], unsynced,       "Pending",         "#f59e0b"),
        (cols[3], failed,         "Failed",          "#ef4444"),
        (cols[4], samples_pending,"Samples pending", "#a78bfa"),
    ]:
        with col:
            st.markdown(
                f"<div style='padding:10px;background:#0d1117;border:1px solid {clr}33;"
                f"border-left:3px solid {clr};border-radius:8px;text-align:center;'>"
                f"<div style='font-size:18px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;'>{val}</div>"
                f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;'>{label}</div></div>",
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # ── Backload controls ──────────────────────────────────────────────────
    st.markdown(
        "<div style='font-size:12px;font-weight:600;color:#f1f5f9;margin-bottom:8px;'>"
        "⬆ Backload (sync historical runs)</div>"
        "<div style='font-size:10px;color:#64748b;margin-bottom:12px;'>"
        "Force-sync runs that are pending or failed. Runs in batches automatically "
        "when agent is started. Use manual trigger to bypass wait.</div>",
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("⬆ Sync pending now", use_container_width=True,
                     key="fleet_sync_now", disabled=(current_mode != "connected")):
            _trigger_sync_now()
    with c2:
        if st.button("🔄 Reset failed → retry", use_container_width=True,
                     key="fleet_reset_failed", disabled=(failed == 0)):
            _reset_failed_runs()
    with c3:
        if st.button("⬆ Sync samples now", use_container_width=True,
                     key="fleet_sync_samples", disabled=(current_mode != "connected")):
            _trigger_samples_sync()

    if current_mode != "connected":
        st.caption("Connect to server to enable sync controls.")

    # Recent failed runs
    if failed > 0:
        from gui.db import q as _q
        recent_failed = _q("""
            SELECT run_id, exp_id, workflow_type, sync_status
            FROM runs WHERE sync_status=2 ORDER BY run_id DESC LIMIT 10
        """)
        if not recent_failed.empty:
            st.markdown(
                "<div style='font-size:10px;font-weight:600;color:#ef4444;"
                "text-transform:uppercase;margin:10px 0 4px;'>Failed runs (last 10)</div>",
                unsafe_allow_html=True,
            )
            st.dataframe(recent_failed, use_container_width=True, hide_index=True)


def _get_db_path() -> str:
    """Always use DB_PATH from config — never hardcode home path."""
    import os
    from gui.config import DB_PATH
    return os.environ.get("ALEMS_SQLITE_PATH", str(DB_PATH))


def _trigger_sync_now() -> None:
    import sqlite3
    db = _get_db_path()
    try:
        # Counts before
        con = sqlite3.connect(db)
        before = con.execute(
            "SELECT SUM(CASE WHEN sync_status=0 THEN 1 ELSE 0 END),"
            "SUM(CASE WHEN sync_status=2 THEN 1 ELSE 0 END) FROM runs"
        ).fetchone()
        con.close()
        pending_before = int(before[0] or 0)
        failed_before  = int(before[1] or 0)

        from alems.agent.sync_client import sync_unsynced_runs
        result = sync_unsynced_runs(db, immediately=True)

        # Counts after
        con = sqlite3.connect(db)
        after = con.execute(
            "SELECT SUM(CASE WHEN sync_status=0 THEN 1 ELSE 0 END),"
            "SUM(CASE WHEN sync_status=1 THEN 1 ELSE 0 END),"
            "SUM(CASE WHEN sync_status=2 THEN 1 ELSE 0 END) FROM runs"
        ).fetchone()
        con.close()
        pending_after = int(after[0] or 0)
        synced_after  = int(after[1] or 0)
        failed_after  = int(after[2] or 0)

        runs_synced = result.get("runs_synced", 0)
        rows_total  = result.get("rows_total", 0)
        status      = result.get("status", "?")
        error       = result.get("error", "")

        if status == "ok":
            st.success(
                f"✅ Sync complete — **{runs_synced}** run(s) synced, "
                f"**{rows_total}** rows inserted into PostgreSQL"
            )
        else:
            st.error(f"❌ Sync failed: {error}")

        # Audit table
        import pandas as pd
        audit = pd.DataFrame([
            {"metric": "Runs synced this batch",   "value": runs_synced},
            {"metric": "PG rows inserted",         "value": rows_total},
            {"metric": "Pending before",           "value": pending_before},
            {"metric": "Pending after",            "value": pending_after},
            {"metric": "Failed before",            "value": failed_before},
            {"metric": "Failed after",             "value": failed_after},
            {"metric": "Total synced in SQLite",   "value": synced_after},
        ])
        st.dataframe(audit, use_container_width=True, hide_index=True)
        st.cache_data.clear()
        st.rerun()

    except Exception as e:
        st.error(f"Sync error: {e}")
        import traceback
        st.code(traceback.format_exc(), language="text")


def _reset_failed_runs() -> None:
    import sqlite3
    db = _get_db_path()
    try:
        con = sqlite3.connect(db)
        # Fresh count — bypass any cache
        n = con.execute("SELECT COUNT(*) FROM runs WHERE sync_status=2").fetchone()[0]
        if n == 0:
            st.info("No failed runs found in SQLite. Counter may be cached — refreshing.")
            con.close()
            st.cache_data.clear()
            st.rerun()
            return
        # Get sample of failed run_ids for audit
        sample = con.execute(
            "SELECT run_id, exp_id, workflow_type FROM runs "
            "WHERE sync_status=2 ORDER BY run_id DESC LIMIT 5"
        ).fetchall()
        con.execute("UPDATE runs SET sync_status=0 WHERE sync_status=2")
        con.commit()
        con.close()
        st.success(f"✅ Reset **{n}** failed run(s) → pending. Agent will retry on next cycle.")
        import pandas as pd
        st.dataframe(
            pd.DataFrame(sample, columns=["run_id", "exp_id", "workflow_type"])
            .assign(new_status="pending (0)"),
            use_container_width=True, hide_index=True,
        )
        st.cache_data.clear()
        st.rerun()
    except Exception as e:
        st.error(str(e))
        import traceback
        st.code(traceback.format_exc(), language="text")


def _trigger_samples_sync() -> None:
    import sqlite3
    db = _get_db_path()
    try:
        # Count before
        con = sqlite3.connect(db)
        before = con.execute(
            "SELECT COUNT(*) FROM runs "
            "WHERE sync_status=1 AND sync_samples_status=0"
        ).fetchone()[0]
        con.close()

        if before == 0:
            st.info("No runs pending sample sync (sync_samples_status=0 with sync_status=1).")
            return

        from alems.agent.sync_client import _sync_pending_samples
        from alems.agent.mode_manager import get_sync_config
        cfg = get_sync_config()
        _sync_pending_samples(
            db,
            int(cfg.get("retry_max", 3)),
            int(cfg.get("retry_backoff_s", 5)),   # reduced backoff for manual trigger
        )

        # Count after
        con = sqlite3.connect(db)
        after = con.execute(
            "SELECT COUNT(*) FROM runs "
            "WHERE sync_status=1 AND sync_samples_status=0"
        ).fetchone()[0]
        synced_batch = before - after
        con.close()

        st.success(
            f"✅ Samples sync complete — **{synced_batch}** run(s) samples synced "
            f"({after} still pending, agent will continue in background)"
        )
        import pandas as pd
        st.dataframe(pd.DataFrame([
            {"metric": "Pending before", "value": before},
            {"metric": "Synced this batch", "value": synced_batch},
            {"metric": "Still pending", "value": after},
        ]), use_container_width=True, hide_index=True)
        st.cache_data.clear()
        st.rerun()

    except Exception as e:
        st.error(f"Samples sync error: {e}")
        import traceback
        st.code(traceback.format_exc(), language="text")
