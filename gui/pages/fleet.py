"""
gui/pages/fleet.py  —  ◈  Fleet Control
────────────────────────────────────────────────────────────────────────────
Production-grade multi-host management. Four tabs:

  SERVER mode (streamlit_server.py, port 8502):
    Tab 1 — Fleet:     All machines, live job status, KPIs
    Tab 2 — Dispatch:  Choose machine, build job, dispatch
    Tab 3 — Job Queue: Full audit table, cancel/priority
    Tab 4 — Sync:      Per-machine freshness, sync log

  CLIENT mode (streamlit_app.py, port 8501):
    Tab 1 — Fleet:     This machine status card
    Tab 2 — Dispatch:  localhost + remote if connected
    Tab 3 — Job Queue: Own jobs only
    Tab 4 — Sync:      Connect/disconnect, sync controls
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations
import streamlit as st
import pandas as pd
from gui.pages._agent_utils import get_ui_mode, is_server_alive, get_server_url

ACCENT = "#22c55e"
STATUS_CLR = {
    "pending":    "#f59e0b",
    "dispatched": "#3b82f6",
    "running":    "#22c55e",
    "completed":  "#475569",
    "failed":     "#ef4444",
    "cancelled":  "#475569",
    "idle":       "#22c55e",
    "offline":    "#475569",
    "syncing":    "#3b82f6",
}


def _load_tasks() -> list[dict]:
    """Load tasks from config/tasks.yaml."""
    try:
        import yaml
        from gui.config import CONFIG_DIR
        with open(CONFIG_DIR / "tasks.yaml") as f:
            data = yaml.safe_load(f)
        return data.get("tasks", [])
    except Exception:
        return []


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
    badge_map = {
        "server":    ("🌐 SERVER",    "#22c55e"),
        "connected": ("🔗 CONNECTED", "#3b82f6"),
        "local":     ("💻 LOCAL",     "#f59e0b"),
    }
    badge = badge_map.get(mode, ("?", "#475569"))
    st.markdown(
        f"<div style='padding:14px 20px;background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:16px;"
        f"display:flex;align-items:center;justify-content:space-between;'>"
        f"<div><div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:3px;'>◈ Fleet Control</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>Multi-host dispatch · job monitoring · sync health</div></div>"
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

    # ── KPI bar ───────────────────────────────────────────────────────────────
    kpi = q("""
        SELECT
            COUNT(*)                                                    AS total_machines,
            COUNT(*) FILTER (WHERE last_seen > NOW() - INTERVAL '5 minutes') AS alive,
            COUNT(*) FILTER (WHERE agent_status = 'running')           AS running,
            COALESCE((SELECT SUM(avg_power_watts) FROM run_status_cache
                      WHERE status = 'running'), 0)                    AS total_watts
        FROM hardware_config
    """)
    if not kpi.empty:
        row = kpi.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        for col, val, label, clr in [
            (c1, int(row.alive or 0),          "Agents alive",    "#22c55e"),
            (c2, int(row.total_machines or 0), "Registered",      "#94a3b8"),
            (c3, int(row.running or 0),        "Jobs running",    "#f59e0b"),
            (c4, f"{float(row.total_watts or 0):.1f} W", "Total power draw", "#a78bfa"),
        ]:
            with col:
                st.markdown(
                    f"<div style='padding:12px 14px;background:#0d1117;"
                    f"border:1px solid {clr}33;border-left:4px solid {clr};"
                    f"border-radius:8px;'>"
                    f"<div style='font-size:22px;font-weight:700;color:{clr};"
                    f"font-family:IBM Plex Mono,monospace;'>{val}</div>"
                    f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
                    f"letter-spacing:.08em;margin-top:3px;'>{label}</div></div>",
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # ── Active jobs table ─────────────────────────────────────────────────────
    active = q("""
        SELECT
            j.job_id,
            h.hostname          AS machine,
            j.status,
            rsc.task_name,
            rsc.energy_uj,
            rsc.elapsed_s,
            rsc.avg_power_watts AS power_w,
            j.created_at,
            j.started_at,
            j.completed_at,
            j.error_message,
            j.retry_count
        FROM job_queue j
        LEFT JOIN hardware_config h  ON h.hw_id  = j.dispatched_to_hw_id
        LEFT JOIN run_status_cache rsc ON rsc.hw_id = j.dispatched_to_hw_id
        WHERE j.status IN ('pending','dispatched','running')
        ORDER BY
            CASE j.status WHEN 'running' THEN 0 WHEN 'dispatched' THEN 1 ELSE 2 END,
            j.created_at DESC
    """)

    st.markdown(
        "<div style='font-size:11px;font-weight:700;color:#f1f5f9;"
        "text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;'>"
        "⚡ Active Jobs</div>",
        unsafe_allow_html=True,
    )

    if active.empty:
        st.info("No active jobs. Use Dispatch tab to submit experiments.", icon="ℹ️")
    else:
        _active_jobs_table(active)

    st.markdown("---")

    # ── Machine grid ──────────────────────────────────────────────────────────
    machines = q("""
        SELECT
            h.hw_id, h.hostname, h.cpu_model,
            h.cpu_architecture, h.agent_status, h.last_seen,
            h.agent_version,
            COUNT(DISTINCT r.global_run_id) AS total_runs,
            MAX(r.synced_at)                AS last_sync,
            rsc.status                      AS live_status,
            rsc.task_name                   AS live_task,
            rsc.elapsed_s                   AS live_elapsed,
            rsc.energy_uj                   AS live_energy,
            MAX(ec.os_name)                 AS os_name,
            MAX(ec.os_version)              AS os_version
        FROM hardware_config h
        LEFT JOIN runs r               ON r.hw_id   = h.hw_id
        LEFT JOIN experiments e        ON e.hw_id   = h.hw_id
        LEFT JOIN environment_config ec ON ec.env_id = e.env_id
        LEFT JOIN run_status_cache rsc ON rsc.hw_id  = h.hw_id
        GROUP BY h.hw_id, h.hostname, h.cpu_model, h.cpu_architecture,
                 h.agent_status, h.last_seen, h.agent_version,
                 rsc.status, rsc.task_name, rsc.elapsed_s, rsc.energy_uj
        ORDER BY h.last_seen DESC NULLS LAST
    """)

    st.markdown(
        "<div style='font-size:11px;font-weight:700;color:#f1f5f9;"
        "text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;'>"
        "🖥 Registered Machines</div>",
        unsafe_allow_html=True,
    )

    if machines.empty:
        st.info("No machines registered yet.")
    else:
        for _, m in machines.iterrows():
            _machine_card_server(dict(m))


def _active_jobs_table(df: pd.DataFrame) -> None:
    """Production-style active jobs table."""
    header_cols = ["job_id", "machine", "task", "status", "elapsed", "energy", "power"]
    st.markdown(
        "<div style='display:grid;grid-template-columns:180px 120px 140px 100px 80px 100px 80px;"
        "gap:0;padding:6px 12px;background:#0a0f1a;border-radius:6px 6px 0 0;"
        "font-size:9px;font-weight:700;color:#475569;text-transform:uppercase;"
        "letter-spacing:.08em;font-family:IBM Plex Mono,monospace;'>"
        + "".join(f"<div>{h}</div>" for h in header_cols)
        + "</div>",
        unsafe_allow_html=True,
    )
    for _, row in df.iterrows():
        status  = row.get("status", "?")
        clr     = STATUS_CLR.get(status, "#475569")
        jid     = str(row.get("job_id", ""))[:16]
        machine = str(row.get("machine") or "—")
        task    = str(row.get("task_name") or "—")
        elapsed = f"{int(row.elapsed_s or 0)}s" if row.get("elapsed_s") else "—"
        energy  = f"{float(row.energy_uj or 0)/1e6:.2f} J" if row.get("energy_uj") else "—"
        power   = f"{float(row.power_w or 0):.1f} W" if row.get("power_w") else "—"

        st.markdown(
            f"<div style='display:grid;grid-template-columns:180px 120px 140px 100px 80px 100px 80px;"
            f"gap:0;padding:8px 12px;background:#0d1117;border-bottom:1px solid #1e2d45;"
            f"font-size:11px;font-family:IBM Plex Mono,monospace;align-items:center;'>"
            f"<div style='color:#94a3b8;font-size:10px;'>{jid}…</div>"
            f"<div style='color:#f1f5f9;'>{machine}</div>"
            f"<div style='color:#f1f5f9;'>{task}</div>"
            f"<div><span style='padding:2px 8px;border-radius:4px;background:{clr}22;"
            f"color:{clr};font-size:9px;font-weight:700;'>{status.upper()}</span></div>"
            f"<div style='color:#94a3b8;'>{elapsed}</div>"
            f"<div style='color:#a78bfa;'>{energy}</div>"
            f"<div style='color:#f59e0b;'>{power}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )


def _machine_card_server(m: dict) -> None:
    status  = m.get("agent_status", "offline")
    clr     = STATUS_CLR.get(status, "#475569")
    host    = m.get("hostname") or f"hw_{m.get('hw_id')}"
    cpu     = m.get("cpu_model", "—")
    arch    = m.get("cpu_architecture", "")
    os_name = m.get("os_name", "")
    os_ver  = m.get("os_version", "")
    os_str  = f"{os_name} {os_ver}".strip() or "—"
    runs    = int(m.get("total_runs") or 0)
    seen    = str(m.get("last_seen") or "never")[:16]
    sync    = str(m.get("last_sync") or "never")[:16]
    live_task   = m.get("live_task", "")
    live_elapsed= m.get("live_elapsed")
    live_energy = m.get("live_energy")

    live_html = ""
    if live_task and m.get("live_status") == "running":
        e_j = f"{float(live_energy or 0)/1e6:.3f} J" if live_energy else "—"
        live_html = (
            f"<div style='margin-top:8px;padding:6px 10px;background:#0a1a0a;"
            f"border:1px solid #22c55e33;border-radius:6px;font-size:10px;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"▶ <b style='color:#22c55e;'>{live_task}</b> · "
            f"{int(live_elapsed or 0)}s · {e_j}</div>"
        )

    icon = "🟢" if status not in ("offline",) else "⚫"
    with st.expander(f"{icon} {host}  ·  {status}  ·  {runs:,} runs", expanded=(status == "running")):
        st.markdown(
            f"<div style='font-size:10px;color:#94a3b8;font-family:IBM Plex Mono,monospace;"
            f"line-height:2;'>"
            f"cpu: <b style='color:#f1f5f9;'>{cpu}</b> {arch}<br>"
            f"os:  <b style='color:#f1f5f9;'>{os_str}</b><br>"
            f"status: <b style='color:{clr};'>{status}</b> · "
            f"last seen: {seen}<br>"
            f"synced runs: <b style='color:#a78bfa;'>{runs:,}</b> · "
            f"last sync: {sync}"
            f"{live_html}</div>",
            unsafe_allow_html=True,
        )


def _fleet_local(mode: str) -> None:
    from gui.db import q1, q as _q
    hw   = q1("SELECT * FROM hardware_config LIMIT 1")
    sync = q1("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN sync_status=0 THEN 1 ELSE 0 END) as unsynced,
               SUM(CASE WHEN sync_status=1 THEN 1 ELSE 0 END) as synced,
               SUM(CASE WHEN sync_status=2 THEN 1 ELSE 0 END) as failed
        FROM runs
    """) or {}

    host    = hw.get("hostname", "this machine")
    cpu     = hw.get("cpu_model", "—")
    total   = int(sync.get("total", 0) or 0)
    synced  = int(sync.get("synced", 0) or 0)
    unsynced= int(sync.get("unsynced", 0) or 0)
    failed  = int(sync.get("failed", 0) or 0)
    clr     = "#22c55e" if mode == "connected" else "#f59e0b"
    label   = "Connected" if mode == "connected" else "Local only"

    st.markdown(
        f"<div style='padding:16px 20px;background:#0d1117;"
        f"border:1px solid {clr}33;border-left:4px solid {clr};border-radius:10px;'>"
        f"<div style='display:flex;justify-content:space-between;align-items:flex-start;'>"
        f"<div><div style='font-size:15px;font-weight:700;color:#f1f5f9;'>💻 {host}</div>"
        f"<div style='font-size:10px;color:#64748b;font-family:IBM Plex Mono,monospace;"
        f"margin-top:4px;'>{cpu}</div></div>"
        f"<div style='font-size:9px;padding:3px 10px;border-radius:4px;"
        f"background:{clr}22;color:{clr};border:1px solid {clr}44;font-weight:700;'>{label}</div></div>"
        f"<div style='display:flex;gap:24px;margin-top:14px;font-size:11px;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"<span style='color:#94a3b8;'>total <b style='color:#f1f5f9;'>{total:,}</b></span>"
        f"<span style='color:#22c55e;'>synced <b>{synced:,}</b></span>"
        f"<span style='color:#f59e0b;'>pending <b>{unsynced:,}</b></span>"
        f"<span style='color:#ef4444;'>failed <b>{failed:,}</b></span>"
        f"</div></div>",
        unsafe_allow_html=True,
    )

    if mode == "connected":
        from alems.agent.mode_manager import get_server_url as _su, get_server_hw_id
        st.markdown(
            f"<div style='font-size:10px;color:#64748b;font-family:IBM Plex Mono,monospace;"
            f"padding:8px 12px;background:#090d13;border-radius:6px;margin-top:6px;'>"
            f"server: <b style='color:#3b82f6;'>{_su()}</b> · "
            f"server hw_id: <b>{get_server_hw_id()}</b></div>",
            unsafe_allow_html=True,
        )

        # Connected machines from server
        try:
            import httpx
            from alems.agent.mode_manager import get_api_key
            r = httpx.get(f"{get_server_url()}/machines",
                          headers={"Authorization": f"Bearer {get_api_key()}"}, timeout=5)
            if r.status_code == 200:
                machines = r.json()
                if machines:
                    st.markdown(
                        "<div style='font-size:11px;font-weight:600;color:#3b82f6;"
                        "text-transform:uppercase;margin:12px 0 6px;'>Connected machines</div>",
                        unsafe_allow_html=True,
                    )
                    for m in machines:
                        clr2 = STATUS_CLR.get(m.get("agent_status", "offline"), "#475569")
                        st.markdown(
                            f"<div style='padding:8px 12px;background:#0d1117;"
                            f"border:1px solid {clr2}22;border-left:3px solid {clr2};"
                            f"border-radius:6px;margin-bottom:4px;font-size:10px;"
                            f"font-family:IBM Plex Mono,monospace;'>"
                            f"<b style='color:#f1f5f9;'>{m.get('hostname','?')}</b> · "
                            f"<span style='color:{clr2};'>{m.get('agent_status','?')}</span> · "
                            f"runs: {int(m.get('total_runs') or 0):,}</div>",
                            unsafe_allow_html=True,
                        )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — DISPATCH
# ══════════════════════════════════════════════════════════════════════════════

def _build_job_form(key_prefix: str) -> dict:
    """Job config form — multi-task, multi-provider, all from config."""
    tasks      = _load_tasks()
    task_ids   = [t["id"] for t in tasks]
    task_labels= [f"{t['id']}  ({t.get('category','')})" for t in tasks]

    # Multi-task selector
    sel_tasks = st.multiselect(
        "Tasks", task_ids,
        default=[task_ids[0]] if task_ids else [],
        format_func=lambda tid: next(
            (f"{t['id']}  ({t.get('category','')})" for t in tasks if t["id"] == tid), tid
        ),
        key=f"{key_prefix}_tasks",
    )
    if not sel_tasks:
        st.warning("Select at least one task.")

    c1, c2, c3 = st.columns(3)
    with c1:
        sel_providers = st.multiselect(
            "Providers", ["cloud", "local"],
            default=["cloud"], key=f"{key_prefix}_provs"
        )
    with c2:
        reps = st.number_input("Repetitions", 1, 50, 3, key=f"{key_prefix}_reps")
    with c3:
        country = st.selectbox("Country", ["US","GB","DE","FR","IN","SG","NO","AU"],
                               key=f"{key_prefix}_country")

    # Preview command
    if sel_tasks and sel_providers:
        cmd_preview = (
            f"run_experiment --tasks {','.join(sel_tasks)} "
            f"--providers {','.join(sel_providers)} "
            f"--repetitions {reps} --country {country} --save-db"
        )
        st.code(cmd_preview, language="bash")

    return {
        "tasks":     ",".join(sel_tasks),
        "providers": ",".join(sel_providers) if sel_providers else "cloud",
        "repetitions": int(reps),
        "country":   country,
    }


def _dispatch_server() -> None:
    from gui.db_pg import q
    import json, os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text

    machines = q("""
        SELECT hw_id, hostname, agent_status, last_seen
        FROM hardware_config ORDER BY last_seen DESC NULLS LAST
    """)

    if machines.empty:
        st.warning("No machines registered.")
        return

    c1, c2 = st.columns(2)
    c1.metric("Registered", len(machines))
    c2.metric("Online", len(machines[machines.agent_status.isin(["idle","connected","syncing"])]))

    host_options = {"🌐 All registered machines": None}
    for _, m in machines.iterrows():
        icon = "🟢" if m.agent_status in ("idle","connected","syncing") else "⚫"
        host_options[f"{icon} {m['hostname']} · {m['agent_status']} (hw_id={m['hw_id']})"] = int(m["hw_id"])

    chosen = st.selectbox("Target machine", list(host_options.keys()),
                          key="fleet_dispatch_target")
    target_hw_id = host_options[chosen]

    cfg = _build_job_form("srv_dispatch")

    if st.button("🚀 Dispatch Job", type="primary", use_container_width=True,
                 key="fleet_dispatch_btn"):
        cfg_json = json.dumps(cfg)
        engine   = get_engine(os.environ.get("ALEMS_DB_URL"))
        count    = 0
        with get_session(engine) as session:
            if target_hw_id is None:
                for _, m in machines.iterrows():
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
        st.success(f"✅ {count} job(s) queued → target picks up within 10s")
        st.rerun()


def _dispatch_local(mode: str) -> None:
    import json
    from gui.db import q1

    hw = q1("SELECT hw_id, hostname FROM hardware_config LIMIT 1")
    host_options = {f"💻 {hw.get('hostname','localhost')} (this machine)": "local"}

    if mode == "connected" and is_server_alive():
        try:
            import httpx
            from alems.agent.mode_manager import get_api_key
            r = httpx.get(f"{get_server_url()}/machines",
                          headers={"Authorization": f"Bearer {get_api_key()}"}, timeout=3)
            if r.status_code == 200:
                for m in r.json():
                    if m.get("agent_status") in ("idle","connected","syncing"):
                        host_options[f"🟢 {m['hostname']} (hw_id={m['hw_id']})"] = int(m["hw_id"])
        except Exception:
            pass

    chosen = st.selectbox("Target machine", list(host_options.keys()),
                          key="fleet_local_target")
    target = host_options[chosen]

    cfg = _build_job_form("local_dispatch")

    if st.button("🚀 Dispatch Job", type="primary", use_container_width=True,
                 key="fleet_local_dispatch_btn"):
        if target == "local":
            import sys
            cmd = [
                sys.executable, "-m", "core.execution.tests.run_experiment",
                "--tasks",       cfg["tasks"],
                "--providers",   cfg["providers"],
                "--repetitions", str(cfg["repetitions"]),
                "--country",     cfg.get("country", "US"),
                "--save-db",
            ]
            item = {
                "name":     f"{cfg['tasks']} / {cfg['providers']}",
                "tasks":    cfg["tasks"],
                "providers":cfg["providers"],
                "reps":     cfg["repetitions"],
                "country":  cfg.get("country", "US"),
                "mode":     "batch",
                "cmd":      cmd,
            }
            if "ex_queue" not in st.session_state:
                st.session_state.ex_queue = []
            st.session_state.ex_queue.append(item)
            st.success("✅ Added to local Execute Run queue → go to Execute Run → ⚡ Live Execution")
        else:
            if mode != "connected":
                st.error("Not connected. Start agent first.")
            else:
                try:
                    import httpx
                    from alems.agent.mode_manager import get_api_key
                    r = httpx.post(
                        f"{get_server_url()}/jobs/submit",
                        json={"api_key": get_api_key(),
                              "experiment_config_json": json.dumps(cfg),
                              "target_hw_id": target, "priority": 5},
                        timeout=10,
                    )
                    if r.status_code == 200:
                        st.success(f"✅ Dispatched to hw_id={target} · agent picks up within 10s")
                    else:
                        st.error(f"Dispatch failed: {r.status_code}")
                except Exception as e:
                    st.error(f"Error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — JOB QUEUE
# ══════════════════════════════════════════════════════════════════════════════

def _jobqueue_server() -> None:
    from gui.db_pg import q
    import os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text

    jobs = q("""
        SELECT
            j.job_id, j.status, j.priority,
            h_t.hostname  AS target_host,
            h_r.hostname  AS running_on,
            j.created_at, j.dispatched_at, j.started_at, j.completed_at,
            EXTRACT(EPOCH FROM (COALESCE(j.completed_at, NOW()) - j.started_at))
                           AS duration_s,
            j.retry_count, j.error_message,
            j.experiment_config_json,
            rsc.run_id     AS result_run_id,
            rsc.energy_uj  AS result_energy_uj,
            rsc.task_name  AS live_task
        FROM job_queue j
        LEFT JOIN hardware_config h_t ON h_t.hw_id = j.target_hw_id
        LEFT JOIN hardware_config h_r ON h_r.hw_id = j.dispatched_to_hw_id
        LEFT JOIN run_status_cache rsc ON rsc.hw_id = j.dispatched_to_hw_id
        ORDER BY
            CASE j.status WHEN 'running' THEN 0 WHEN 'dispatched' THEN 1
                          WHEN 'pending' THEN 2 ELSE 3 END,
            j.created_at DESC
        LIMIT 200
    """)

    # Summary cards
    if not jobs.empty:
        from collections import Counter
        counts = Counter(jobs.status.tolist())
        cols = st.columns(5)
        for col, s in zip(cols, ["pending","dispatched","running","completed","failed"]):
            clr = STATUS_CLR[s]
            with col:
                st.markdown(
                    f"<div style='padding:10px;background:#0d1117;border:1px solid {clr}33;"
                    f"border-left:3px solid {clr};border-radius:6px;text-align:center;'>"
                    f"<div style='font-size:20px;font-weight:700;color:{clr};"
                    f"font-family:IBM Plex Mono,monospace;'>{counts.get(s,0)}</div>"
                    f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;'>{s}</div></div>",
                    unsafe_allow_html=True,
                )

    pending_count = len(jobs[jobs.status == "pending"]) if not jobs.empty else 0
    if pending_count > 0:
        st.info(
            f"**{pending_count}** job(s) pending — agent polls every 10s and picks up automatically. "
            f"To start immediately: `python -m alems.agent start` on target machine.",
            icon="ℹ️",
        )

    st.markdown("---")

    flt = st.selectbox("Filter", ["all","pending","running","dispatched","completed","failed"],
                       key="fleet_jq_filter")
    filtered = jobs if flt == "all" else jobs[jobs.status == flt]

    for _, job in filtered.iterrows():
        _job_row_server(dict(job))


def _job_row_server(job: dict) -> None:
    import json, os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text

    status  = job.get("status", "?")
    clr     = STATUS_CLR.get(status, "#475569")
    jid     = str(job.get("job_id", ""))[:12]
    target  = job.get("target_host") or "any"
    runner  = job.get("running_on") or "—"
    dur     = f"{float(job.get('duration_s') or 0):.1f}s" if job.get("started_at") else "—"
    energy  = f"{float(job.get('result_energy_uj') or 0)/1e6:.3f} J" if job.get("result_energy_uj") else "—"
    err     = job.get("error_message", "")

    try:
        cfg  = json.loads(job.get("experiment_config_json") or "{}")
        task = cfg.get("task_id", "—")
        prov = cfg.get("provider", "—")
        reps = cfg.get("repetitions", "—")
    except Exception:
        task, prov, reps = "—", "—", "—"

    created   = str(job.get("created_at", ""))[:16]
    started   = str(job.get("started_at", ""))[:16] or "—"
    completed = str(job.get("completed_at", ""))[:16] or "—"

    with st.expander(
        f"[{status.upper()}]  {jid}…  ·  {task}/{prov}  →  {target}  ·  {dur}",
        expanded=(status == "running"),
    ):
        st.markdown(
            f"<div style='font-size:10px;font-family:IBM Plex Mono,monospace;"
            f"color:#94a3b8;line-height:2;'>"
            f"status: <b style='color:{clr};'>{status}</b> · "
            f"target: <b style='color:#f1f5f9;'>{target}</b> · "
            f"ran on: <b style='color:#f1f5f9;'>{runner}</b><br>"
            f"task: <b style='color:#f1f5f9;'>{task}</b> · "
            f"provider: {prov} · reps: {reps}<br>"
            f"created: {created} · started: {started} · completed: {completed}<br>"
            f"duration: <b style='color:#a78bfa;'>{dur}</b> · "
            f"energy: <b style='color:#f59e0b;'>{energy}</b> · "
            f"retries: {job.get('retry_count', 0)}"
            + (f"<br><span style='color:#ef4444;'>error: {err}</span>" if err else "")
            + "</div>",
            unsafe_allow_html=True,
        )
        if status in ("pending", "dispatched"):
            c1, c2 = st.columns(2)
            job_id = str(job.get("job_id", ""))
            with c1:
                if st.button("⊘ Cancel", key=f"fleet_cancel_{jid}"):
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


def _jobqueue_local(mode: str) -> None:
    if mode == "local":
        st.info("Job queue requires server connection. Connect in Sync & Connect tab.")
        return
    if not is_server_alive():
        st.warning("Server unreachable.")
        return
    st.info(f"Full job queue visible at server dashboard · `{get_server_url().replace('8000','8502')}`")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — SYNC
# ══════════════════════════════════════════════════════════════════════════════

def _sync_server() -> None:
    from gui.db_pg import q

    # Server freshness per machine
    st.markdown(
        "<div style='font-size:11px;font-weight:700;color:#a78bfa;"
        "text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;'>"
        "🌐 Server Freshness</div>",
        unsafe_allow_html=True,
    )
    freshness = q("""
        SELECT
            h.hostname, h.agent_status, h.last_seen,
            COUNT(DISTINCT r.global_run_id)                        AS pg_runs,
            MAX(r.synced_at)                                        AS last_sync,
            EXTRACT(EPOCH FROM (NOW() - MAX(r.synced_at)))/3600    AS hours_stale
        FROM hardware_config h
        LEFT JOIN runs r ON r.hw_id = h.hw_id
        GROUP BY h.hw_id, h.hostname, h.agent_status, h.last_seen
        ORDER BY h.last_seen DESC NULLS LAST
    """)

    for _, row in freshness.iterrows():
        hrs  = row.get("hours_stale")
        clr  = "#22c55e" if hrs and hrs < 1 else "#f59e0b" if hrs and hrs < 24 else "#ef4444"
        fresh= f"{hrs:.1f}h ago" if hrs is not None else "never synced"
        seen = str(row.get("last_seen") or "never")[:16]
        st.markdown(
            f"<div style='padding:10px 14px;background:#0d1117;"
            f"border:1px solid {clr}33;border-left:3px solid {clr};"
            f"border-radius:8px;margin-bottom:5px;font-size:10px;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"<b style='color:#f1f5f9;'>{row['hostname']}</b> · "
            f"<span style='color:{clr};'>{row['agent_status']}</span> · "
            f"PG runs: <b style='color:#a78bfa;'>{int(row['pg_runs'] or 0):,}</b> · "
            f"last sync: <b style='color:{clr};'>{fresh}</b> · "
            f"last seen: {seen}</div>",
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # Sync log
    st.markdown(
        "<div style='font-size:11px;font-weight:700;color:#a78bfa;"
        "text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;'>"
        "📋 Sync Log</div>",
        unsafe_allow_html=True,
    )
    logs = q("""
        SELECT h.hostname, s.sync_started_at, s.sync_completed_at,
               s.runs_synced, s.rows_total, s.status
        FROM sync_log s
        LEFT JOIN hardware_config h ON h.hw_id = s.hw_id
        ORDER BY s.log_id DESC LIMIT 50
    """)
    if logs.empty:
        st.info("No sync log entries yet. Start agent to begin syncing.")
    else:
        st.dataframe(logs, use_container_width=True, hide_index=True)


def _sync_local(mode: str) -> None:
    from gui.db import q1
    from alems.agent.mode_manager import (
        get_mode, set_mode, get_server_url as _gsu,
        get_api_key, is_registered
    )
    import sqlite3

    current_mode = get_mode()
    server_url   = _gsu()
    registered   = is_registered()
    server_alive = is_server_alive()
    conn_clr     = "#22c55e" if current_mode == "connected" else "#f59e0b"
    conn_txt     = "Connected" if current_mode == "connected" else "Local only"

    # Connection status card
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
        f"margin-top:8px;line-height:2;'>"
        f"mode: <b style='color:#f1f5f9;'>{current_mode}</b> · "
        f"server: <b style='color:#3b82f6;'>{server_url}</b><br>"
        f"registered: <b style='color:{'#22c55e' if registered else '#ef4444'};'>"
        f"{'yes' if registered else 'no'}</b> · "
        f"server reachable: <b style='color:{'#22c55e' if server_alive else '#ef4444'};'>"
        f"{'yes' if server_alive else 'no'}</b></div></div>",
        unsafe_allow_html=True,
    )

    # Connect / Disconnect
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
                    if "agent" not in conf or not isinstance(conf.get("agent"), dict):
                        conf["agent"] = {}
                    conf["agent"]["server_url"] = new_url
                    conf["agent"]["mode"]       = "connected"
                    _write_conf(conf)
                    st.success("Mode set to connected. Restart agent: `python -m alems.agent start`")
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
            if is_server_alive():
                st.success("Server reachable ✓")
            else:
                st.error(f"Cannot reach {server_url}")

    st.markdown("---")

    # Sync status counters
    stats = q1("""
        SELECT COUNT(*) as total,
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
        (cols[0], total,          "Total",           "#94a3b8"),
        (cols[1], synced,         "Synced",           "#22c55e"),
        (cols[2], unsynced,       "Pending",          "#f59e0b"),
        (cols[3], failed,         "Failed",           "#ef4444"),
        (cols[4], samples_pending,"Samples pending",  "#a78bfa"),
    ]:
        with col:
            st.markdown(
                f"<div style='padding:10px;background:#0d1117;border:1px solid {clr}33;"
                f"border-left:3px solid {clr};border-radius:8px;text-align:center;'>"
                f"<div style='font-size:18px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;'>{val:,}</div>"
                f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;'>{label}</div></div>",
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # Backload controls
    st.markdown(
        "<div style='font-size:12px;font-weight:600;color:#f1f5f9;margin-bottom:4px;'>"
        "⬆ Backload & Sync Controls</div>"
        "<div style='font-size:10px;color:#64748b;margin-bottom:12px;'>"
        "Agent syncs automatically every 60s. Use manual triggers to force immediate sync.</div>",
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


# ── Sync action functions ──────────────────────────────────────────────────────

def _get_db_path() -> str:
    import os
    from gui.config import DB_PATH
    return os.environ.get("ALEMS_SQLITE_PATH", str(DB_PATH))


def _trigger_sync_now() -> None:
    import sqlite3
    db = _get_db_path()
    try:
        con    = sqlite3.connect(db)
        before = con.execute(
            "SELECT SUM(CASE WHEN sync_status=0 THEN 1 ELSE 0 END),"
            "SUM(CASE WHEN sync_status=2 THEN 1 ELSE 0 END) FROM runs"
        ).fetchone()
        con.close()

        from alems.agent.sync_client import sync_unsynced_runs
        result = sync_unsynced_runs(db, immediately=True)

        con   = sqlite3.connect(db)
        after = con.execute(
            "SELECT SUM(CASE WHEN sync_status=0 THEN 1 ELSE 0 END),"
            "SUM(CASE WHEN sync_status=1 THEN 1 ELSE 0 END),"
            "SUM(CASE WHEN sync_status=2 THEN 1 ELSE 0 END) FROM runs"
        ).fetchone()
        con.close()

        runs_synced = result.get("runs_synced", 0)
        rows_total  = result.get("rows_total", 0)

        if result.get("status") == "ok":
            st.success(f"✅ Synced **{runs_synced}** run(s) · **{rows_total}** PG rows inserted")
        else:
            st.error(f"❌ {result.get('error')}")

        import pandas as pd
        st.dataframe(pd.DataFrame([
            {"metric": "Runs synced this batch",  "value": runs_synced},
            {"metric": "PG rows inserted",        "value": rows_total},
            {"metric": "Pending before",          "value": int(before[0] or 0)},
            {"metric": "Pending after",           "value": int(after[0] or 0)},
            {"metric": "Failed before",           "value": int(before[1] or 0)},
            {"metric": "Failed after",            "value": int(after[2] or 0)},
            {"metric": "Total synced",            "value": int(after[1] or 0)},
        ]), use_container_width=True, hide_index=True)
        st.cache_data.clear()
        st.rerun()
    except Exception as e:
        st.error(f"Sync error: {e}")
        import traceback; st.code(traceback.format_exc())


def _reset_failed_runs() -> None:
    import sqlite3
    db = _get_db_path()
    try:
        con = sqlite3.connect(db)
        n   = con.execute("SELECT COUNT(*) FROM runs WHERE sync_status=2").fetchone()[0]
        if n == 0:
            st.info("No failed runs found. Counter may be cached — refreshing.")
            con.close()
            st.cache_data.clear()
            st.rerun()
            return
        sample = con.execute(
            "SELECT run_id, exp_id, workflow_type FROM runs "
            "WHERE sync_status=2 ORDER BY run_id DESC LIMIT 5"
        ).fetchall()
        con.execute("UPDATE runs SET sync_status=0 WHERE sync_status=2")
        con.commit()
        con.close()
        st.success(f"✅ Reset **{n}** failed run(s) → pending")
        import pandas as pd
        st.dataframe(
            pd.DataFrame(sample, columns=["run_id","exp_id","workflow_type"])
            .assign(new_status="pending (0)"),
            use_container_width=True, hide_index=True,
        )
        st.cache_data.clear()
        st.rerun()
    except Exception as e:
        st.error(str(e))


def _trigger_samples_sync() -> None:
    import sqlite3
    db = _get_db_path()
    try:
        con    = sqlite3.connect(db)
        before = con.execute(
            "SELECT COUNT(*) FROM runs WHERE sync_status=1 AND sync_samples_status=0"
        ).fetchone()[0]
        con.close()
        if before == 0:
            st.info("No runs pending sample sync.")
            return

        from alems.agent.sync_client import _sync_pending_samples
        from alems.agent.mode_manager import get_sync_config
        cfg = get_sync_config()
        _sync_pending_samples(db, int(cfg.get("retry_max",3)), 5)

        con   = sqlite3.connect(db)
        after = con.execute(
            "SELECT COUNT(*) FROM runs WHERE sync_status=1 AND sync_samples_status=0"
        ).fetchone()[0]
        con.close()

        synced_batch = before - after
        st.success(f"✅ **{synced_batch}** run(s) samples synced · {after} still pending")
        import pandas as pd
        st.dataframe(pd.DataFrame([
            {"metric": "Pending before",    "value": before},
            {"metric": "Synced this batch", "value": synced_batch},
            {"metric": "Still pending",     "value": after},
        ]), use_container_width=True, hide_index=True)
        st.cache_data.clear()
        st.rerun()
    except Exception as e:
        st.error(f"Samples sync error: {e}")
        import traceback; st.code(traceback.format_exc())
