"""
gui/pages/experiment_submissions.py  —  ◎  Global Queue
────────────────────────────────────────────────────────────────────────────
Researcher experiment submission and admin review queue.

SERVER mode:    full admin review UI
CONNECTED mode: submit experiments, see own submission status
LOCAL mode:     save locally, shows "connect to submit" prompt
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations
import json
import streamlit as st
from gui.db import q1
from gui.pages._agent_utils import (
    get_ui_mode, mode_banner, is_server_alive, get_server_url,
)

ACCENT = "#38bdf8"

REVIEW_CLR = {
    "pending_review": "#f59e0b",
    "approved":       "#22c55e",
    "auto_approved":  "#22c55e",
    "rejected":       "#ef4444",
}


def render(ctx: dict) -> None:
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:16px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px;'>"
        f"◎ Global Queue</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>"
        f"Submit experiments to the shared server queue for review and execution.</div></div>",
        unsafe_allow_html=True,
    )

    mode = get_ui_mode()
    mode_banner(mode)

    if mode == "server":
        tab1, tab2 = st.tabs(["◎ Pending review", "⊟ Submit experiment"])
        with tab1:
            _render_admin_queue(ctx)
        with tab2:
            _render_submit_form(ctx, admin=True)
    elif mode == "connected":
        tab1, tab2 = st.tabs(["⊟ Submit experiment", "◎ My submissions"])
        with tab1:
            _render_submit_form(ctx, admin=False)
        with tab2:
            _render_my_submissions(ctx)
    else:
        _render_local_mode(ctx)


# ── Admin view (server mode) ──────────────────────────────────────────────────

def _render_admin_queue(ctx: dict) -> None:
    import os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text

    engine = get_engine(os.environ.get("ALEMS_DB_URL"))
    with get_session(engine) as session:
        subs = session.execute(text("""
            SELECT s.*, h.hostname
            FROM experiment_submissions s
            LEFT JOIN hardware_config h ON h.hw_id = s.submitted_by_hw_id
            ORDER BY
                CASE s.review_status WHEN 'pending_review' THEN 0 ELSE 1 END,
                s.submitted_at DESC
        """)).fetchall()
        subs = [dict(r._mapping) for r in subs]

    pending = [s for s in subs if s.get("review_status") == "pending_review"]
    others  = [s for s in subs if s.get("review_status") != "pending_review"]

    st.caption(f"{len(pending)} pending review · {len(others)} resolved")

    for sub in pending + others:
        _submission_card(sub, admin=True)


def _submission_card(sub: dict, admin: bool = False) -> None:
    status  = sub.get("review_status", "pending_review")
    clr     = REVIEW_CLR.get(status, "#475569")
    name    = sub.get("name", "Unnamed")
    host    = sub.get("hostname") or f"hw_{sub.get('submitted_by_hw_id','?')}"
    sub_id  = str(sub.get("submission_id", ""))[:12]
    desc    = sub.get("description") or ""
    submitted = str(sub.get("submitted_at", ""))[:16]

    try:
        cfg = json.loads(sub.get("config_json") or "{}")
        task  = cfg.get("task_id",  "—")
        model = cfg.get("provider", "—")
        reps  = cfg.get("repetitions", "—")
    except Exception:
        task, model, reps = "—", "—", "—"

    with st.expander(
        f"[{status.upper()}] {name} · from {host} · {submitted}",
        expanded=(status == "pending_review"),
    ):
        st.markdown(
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:11px;"
            f"color:#94a3b8;line-height:1.8;'>"
            f"id: {sub_id}…<br>"
            f"task: <b style='color:#f1f5f9;'>{task}</b> · "
            f"provider: {model} · reps: {reps}<br>"
            + (f"desc: {desc}<br>" if desc else "")
            + f"status: <span style='color:{clr};'>{status}</span>"
            + (f" · reviewed by: {sub.get('reviewed_by')}" if sub.get("reviewed_by") else "")
            + (f"<br>notes: {sub.get('review_notes')}" if sub.get("review_notes") else "")
            + "</div>",
            unsafe_allow_html=True,
        )

        if admin and status == "pending_review":
            reviewer = st.text_input("Your name", key=f"reviewer_{sub_id}", value="admin")
            notes    = st.text_area("Notes (optional)", key=f"notes_{sub_id}", height=60)
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✓ Approve", key=f"approve_{sub_id}", type="primary"):
                    _review_submission(str(sub.get("submission_id")), "approve", reviewer, notes)
            with col2:
                if st.button("✗ Reject", key=f"reject_{sub_id}"):
                    _review_submission(str(sub.get("submission_id")), "reject", reviewer, notes)


def _review_submission(sub_id: str, action: str, reviewer: str, notes: str) -> None:
    import httpx
    from alems.agent.mode_manager import get_api_key
    try:
        r = httpx.post(
            f"{get_server_url()}/experiments/review/{sub_id}",
            json={"action": action, "reviewed_by": reviewer, "notes": notes},
            headers={"Authorization": f"Bearer {get_api_key()}"},
            timeout=10,
        )
        if r.status_code == 200:
            st.success(f"Submission {action}d successfully")
            st.rerun()
        else:
            st.error(f"Server error: {r.status_code}")
    except Exception as e:
        st.error(f"Could not reach server: {e}")


# ── Submit form (connected / server mode) ─────────────────────────────────────

def _render_submit_form(ctx: dict, admin: bool = False) -> None:
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;margin-bottom:10px;'>New submission</div>",
        unsafe_allow_html=True,
    )

    name  = st.text_input("Experiment name", key="sub_name")
    desc  = st.text_area("Description", key="sub_desc", height=80)

    col1, col2, col3 = st.columns(3)
    with col1:
        task_id  = st.text_input("Task ID",  value="gsm8k_basic", key="sub_task")
    with col2:
        provider = st.selectbox("Provider", ["cloud", "local"], key="sub_provider")
    with col3:
        reps     = st.number_input("Repetitions", min_value=1, value=3, key="sub_reps")

    col4, col5 = st.columns(2)
    with col4:
        country  = st.text_input("Country", value="US", key="sub_country")
    with col5:
        workflow = st.selectbox("Workflow", ["linear", "agentic", "both"], key="sub_wf")

    cfg = json.dumps({
        "task_id":      task_id,
        "provider":     provider,
        "repetitions":  reps,
        "country":      country,
        "workflow_type": workflow,
    })

    if admin:
        # Server mode: add directly to job queue
        if st.button("Add to job queue", type="primary", key="sub_add_direct"):
            _add_direct_to_queue(cfg, name)
    else:
        if st.button("Submit for review", type="primary", key="sub_submit"):
            _submit_to_server(cfg, name, desc)

        if st.button("Save locally only", key="sub_save_local"):
            _save_locally(cfg, name, desc)


def _submit_to_server(config_json: str, name: str, desc: str) -> None:
    import httpx
    from alems.agent.mode_manager import get_api_key
    try:
        hw = q1("SELECT hardware_hash FROM hardware_config LIMIT 1") or {}
        r  = httpx.post(
            f"{get_server_url()}/experiments/submit",
            json={
                "hardware_hash": hw.get("hardware_hash", ""),
                "api_key":       get_api_key(),
                "name":          name,
                "description":   desc,
                "config_json":   config_json,
            },
            timeout=10,
        )
        if r.status_code == 200:
            st.success("Submitted for review! An admin will approve and queue it.")
        else:
            st.error(f"Server error: {r.status_code}")
    except Exception as e:
        st.error(f"Could not reach server: {e}")


def _add_direct_to_queue(config_json: str, name: str) -> None:
    import os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text
    engine = get_engine(os.environ.get("ALEMS_DB_URL"))
    with get_session(engine) as session:
        session.execute(text("""
            INSERT INTO job_queue (experiment_config_json, status, priority)
            VALUES (:cfg, 'pending', 5)
        """), {"cfg": config_json})
        session.commit()
    st.success(f"'{name}' added to job queue.")
    st.rerun()


def _save_locally(config_json: str, name: str, desc: str) -> None:
    import sqlite3, os
    from pathlib import Path
    db_path = os.environ.get("ALEMS_SQLITE_PATH",
                             str(Path.home() / "mydrive/a-lems/data/experiments.db"))
    try:
        con = sqlite3.connect(db_path)
        con.execute(
            "INSERT INTO saved_experiments (name, config_json, notes) VALUES (?,?,?)",
            (name, config_json, desc)
        )
        con.commit()
        con.close()
        st.success("Saved locally. Will appear in 'My Experiments' and can be submitted when connected.")
    except Exception as e:
        st.error(f"Could not save: {e}")


# ── My submissions (connected mode) ──────────────────────────────────────────

def _render_my_submissions(ctx: dict) -> None:
    import httpx
    from alems.agent.mode_manager import get_api_key
    try:
        r = httpx.get(
            f"{get_server_url()}/experiments/queue",
            headers={"Authorization": f"Bearer {get_api_key()}"},
            timeout=5,
        )
        if r.status_code == 200:
            subs = r.json()
            try:
                from alems.agent.mode_manager import get_server_hw_id
                own_hw_id = get_server_hw_id()
                subs = [s for s in subs if s.get("submitted_by_hw_id") == own_hw_id]
            except Exception:
                pass
            if subs:
                for s in subs:
                    _submission_card(s, admin=False)
            else:
                st.info("No submissions yet.")
        else:
            st.warning("Could not load submissions from server.")
    except Exception as e:
        st.warning(f"Could not reach server: {e}")


# ── Local mode ────────────────────────────────────────────────────────────────

def _render_local_mode(ctx: dict) -> None:
    st.info(
        "You are in local mode. You can save experiments locally and submit them "
        "to the global queue when you connect to the server.\n\n"
        "To connect: `python -m alems.agent start --mode connected`",
        icon="ℹ️",
    )
    _render_submit_form(ctx, admin=False)

    # Show locally saved experiments
    from gui.db import q as db_q
    saved = db_q("SELECT saved_id, name, notes, created_at FROM saved_experiments ORDER BY saved_id DESC")
    if not saved.empty:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;margin:12px 0 6px;'>Saved locally</div>",
            unsafe_allow_html=True,
        )
        st.dataframe(saved, use_container_width=True, hide_index=True)
