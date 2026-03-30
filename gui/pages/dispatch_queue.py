"""
gui/pages/dispatch_queue.py  —  ⬡  Dispatch Queue
────────────────────────────────────────────────────────────────────────────
Job queue management across all machines.

SERVER mode:    full queue table, approve/cancel/prioritise
CONNECTED mode: shows own machine's jobs + server queue summary
LOCAL mode:     "no server" notice
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations
import streamlit as st
from gui.pages._agent_utils import get_ui_mode, mode_banner, is_server_alive, get_server_url

ACCENT = "#22c55e"

STATUS_CLR = {
    "pending":    "#f59e0b",
    "dispatched": "#3b82f6",
    "running":    "#22c55e",
    "completed":  "#475569",
    "failed":     "#ef4444",
    "cancelled":  "#475569",
}


def render(ctx: dict) -> None:
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:16px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px;'>"
        f"⬡ Dispatch Queue</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>"
        f"Server job queue — pending, running, and completed jobs.</div></div>",
        unsafe_allow_html=True,
    )

    mode = get_ui_mode()
    mode_banner(mode)

    if mode == "local":
        st.info(
            "Dispatch queue requires a server connection.\n\n"
            "Start the agent: `python -m alems.agent start --mode connected`",
            icon="ℹ️",
        )
        return

    if mode == "server":
        _render_server_queue(ctx)
    else:
        _render_connected_queue(ctx)


def _render_server_queue(ctx: dict):
    import os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text

    engine = get_engine(os.environ.get("ALEMS_DB_URL"))
    with get_session(engine) as session:
        jobs = session.execute(text("""
            SELECT j.*, h.hostname as machine_name
            FROM job_queue j
            LEFT JOIN hardware_config h ON h.hw_id = j.dispatched_to_hw_id
            ORDER BY
                CASE j.status
                    WHEN 'running'    THEN 0
                    WHEN 'dispatched' THEN 1
                    WHEN 'pending'    THEN 2
                    ELSE 3
                END,
                j.priority DESC, j.created_at DESC
            LIMIT 200
        """)).fetchall()
        jobs = [dict(r._mapping) for r in jobs]

    _render_job_summary(jobs)
    st.markdown("---")

    filter_status = st.selectbox(
        "Filter by status",
        ["all", "pending", "running", "dispatched", "completed", "failed"],
        key="dq_filter",
    )
    if filter_status != "all":
        jobs = [j for j in jobs if j.get("status") == filter_status]

    # ── Dispatch new job to all connected machines ────────────────────────────
    with st.expander("⬡ Dispatch job to all connected machines", expanded=False):
        import json
        col1, col2, col3 = st.columns(3)
        with col1:
            task_id = st.text_input("Task ID", value="gsm8k_basic", key="dq_task")
        with col2:
            provider = st.selectbox("Provider", ["cloud", "local"], key="dq_prov")
        with col3:
            reps = st.number_input("Repetitions", min_value=1, max_value=20, value=3, key="dq_reps")
        model = st.text_input("Model (optional)", value="", key="dq_model")
        if st.button("🚀 Dispatch to all connected machines", key="dq_dispatch_all"):
            exp_cfg = {"task_id": task_id, "provider": provider,
                       "repetitions": reps, "workflow_type": "linear"}
            if model:
                exp_cfg["model_name"] = model
            _dispatch_to_all(json.dumps(exp_cfg))

    st.markdown("---")
    for job in jobs:
        _job_card(job, admin=True)


def _render_connected_queue(ctx: dict):
    import httpx
    from alems.agent.mode_manager import get_api_key, get_server_hw_id

    try:
        r = httpx.get(
            f"{get_server_url()}/machines",
            headers={"Authorization": f"Bearer {get_api_key()}"},
            timeout=5,
        )
        machines = r.json() if r.status_code == 200 else []
    except Exception:
        machines = []

    own_hw_id = get_server_hw_id()
    st.caption(f"Your machine server hw_id: {own_hw_id}")

    if not is_server_alive():
        st.warning("Cannot reach server — queue not available")
        return

    # Show own machine's jobs
    try:
        r2 = httpx.get(
            f"{get_server_url()}/get-job",
            params={"hardware_hash": ""},
            headers={"Authorization": f"Bearer {get_api_key()}"},
            timeout=5,
        )
        st.info("Job queue visible on server — visit server Streamlit for full management.")
    except Exception:
        pass


def _render_job_summary(jobs: list[dict]) -> None:
    from collections import Counter
    counts = Counter(j.get("status") for j in jobs)

    cols = st.columns(5)
    for col, status in zip(cols, ["pending", "dispatched", "running", "completed", "failed"]):
        n   = counts.get(status, 0)
        clr = STATUS_CLR.get(status, "#475569")
        with col:
            st.markdown(
                f"<div style='padding:8px 10px;background:#0d1117;"
                f"border:1px solid {clr}33;border-left:3px solid {clr};"
                f"border-radius:6px;text-align:center;'>"
                f"<div style='font-size:18px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;'>{n}</div>"
                f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;'>"
                f"{status}</div></div>",
                unsafe_allow_html=True,
            )


def _job_card(job: dict, admin: bool = False) -> None:
    status  = job.get("status", "unknown")
    clr     = STATUS_CLR.get(status, "#475569")
    job_id  = str(job.get("job_id", ""))[:12]
    machine = job.get("machine_name") or "any"
    prio    = job.get("priority", 5)

    import json
    try:
        cfg = json.loads(job.get("experiment_config_json") or "{}")
        task  = cfg.get("task_id",  "—")
        model = cfg.get("provider", "—")
    except Exception:
        task, model = "—", "—"

    created = str(job.get("created_at", ""))[:16]
    on_disc = job.get("on_disconnect", "fail")

    with st.expander(
        f"[{status.upper()}] {job_id}… · {task} · {machine} · prio={prio}",
        expanded=(status == "running"),
    ):
        st.markdown(
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:11px;"
            f"color:#94a3b8;line-height:1.8;'>"
            f"status: <b style='color:{clr};'>{status}</b><br>"
            f"task: <b style='color:#f1f5f9;'>{task}</b> · provider: {model}<br>"
            f"machine: <b style='color:#f1f5f9;'>{machine}</b> · "
            f"on_disconnect: {on_disc}<br>"
            f"created: {created}</div>",
            unsafe_allow_html=True,
        )
        if admin and status == "pending":
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Cancel", key=f"cancel_{job.get('job_id')}"):
                    _cancel_job(str(job.get("job_id")))
            with col2:
                if st.button("↑ Priority", key=f"prio_{job.get('job_id')}"):
                    _boost_priority(str(job.get("job_id")))


def _dispatch_to_all(config_json: str) -> None:
    """Create one pending job per connected machine (last_seen within 2 min)."""
    import os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text
    engine = get_engine(os.environ.get("ALEMS_DB_URL"))
    with get_session(engine) as session:
        machines = session.execute(text("""
            SELECT hw_id FROM hardware_config
            WHERE agent_status IN ('idle', 'connected', 'syncing')
              AND last_seen > NOW() - INTERVAL '2 minutes'
        """)).fetchall()
        count = 0
        for (hw_id,) in machines:
            session.execute(text("""
                INSERT INTO job_queue
                    (experiment_config_json, status, priority, target_hw_id, created_by_hw_id)
                VALUES (:cfg, 'pending', 5, :hw, :hw)
            """), {"cfg": config_json, "hw": hw_id})
            count += 1
        session.commit()
    if count:
        st.success(f"Dispatched to {count} machine(s)")
    else:
        st.warning("No connected machines found (last_seen within 2 min)")
    st.rerun()


def _cancel_job(job_id: str) -> None:
    import os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text
    engine = get_engine(os.environ.get("ALEMS_DB_URL"))
    with get_session(engine) as session:
        session.execute(text(
            "UPDATE job_queue SET status='cancelled' WHERE job_id=:id AND status='pending'"
        ), {"id": job_id})
        session.commit()
    st.rerun()


def _boost_priority(job_id: str) -> None:
    import os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text
    engine = get_engine(os.environ.get("ALEMS_DB_URL"))
    with get_session(engine) as session:
        session.execute(text(
            "UPDATE job_queue SET priority=priority+1 WHERE job_id=:id"
        ), {"id": job_id})
        session.commit()
    st.rerun()
