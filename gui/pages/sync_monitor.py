"""
gui/pages/sync_monitor.py  —  ⟳  Sync Monitor
────────────────────────────────────────────────────────────────────────────
Per-machine sync health, lag, and retry status.

SERVER mode:    reads sync_log from PostgreSQL, shows all machines
CONNECTED mode: shows own machine's sync status only
LOCAL mode:     shows unsynced run count from local SQLite
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations
import streamlit as st
from gui.db import q, q1
from gui.pages._agent_utils import get_ui_mode, mode_banner, is_server_alive

ACCENT = "#a78bfa"


def render(ctx: dict) -> None:
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:16px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px;'>"
        f"⟳ Sync Monitor</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>"
        f"SQLite → PostgreSQL sync health per machine.</div></div>",
        unsafe_allow_html=True,
    )

    mode = get_ui_mode()
    mode_banner(mode)

    if mode == "server":
        _render_server_sync(ctx)
    elif mode == "connected":
        _render_local_sync(ctx)
        _render_server_summary(ctx)
    else:
        _render_local_sync(ctx)


def _render_local_sync(ctx: dict) -> None:
    """Show unsynced run count from local SQLite."""
    stats = q1("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN sync_status=0 THEN 1 ELSE 0 END) as unsynced,
            SUM(CASE WHEN sync_status=1 THEN 1 ELSE 0 END) as synced,
            SUM(CASE WHEN sync_status=2 THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN sync_status=3 THEN 1 ELSE 0 END) as skipped
        FROM runs
    """) or {}

    total    = int(stats.get("total", 0) or 0)
    unsynced = int(stats.get("unsynced", 0) or 0)
    synced   = int(stats.get("synced", 0) or 0)
    failed   = int(stats.get("failed", 0) or 0)

    c1, c2, c3, c4 = st.columns(4)
    for col, val, label, clr in [
        (c1, total,    "Total runs",    "#94a3b8"),
        (c2, synced,   "Synced",        "#22c55e"),
        (c3, unsynced, "Pending sync",  "#f59e0b"),
        (c4, failed,   "Failed",        "#ef4444"),
    ]:
        with col:
            st.markdown(
                f"<div style='padding:10px 12px;background:#0d1117;"
                f"border:1px solid {clr}33;border-left:3px solid {clr};"
                f"border-radius:8px;text-align:center;'>"
                f"<div style='font-size:20px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;'>{val}</div>"
                f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;'>"
                f"{label}</div></div>",
                unsafe_allow_html=True,
            )

    if unsynced > 0:
        st.warning(
            f"{unsynced} run(s) pending sync. "
            "They will sync automatically when the agent is in connected mode."
        )
    if failed > 0:
        st.error(
            f"{failed} run(s) failed to sync and will be retried. "
            "Check server connectivity."
        )

    # Show recent unsynced runs
    if unsynced > 0:
        recent = q("""
            SELECT run_id, exp_id, workflow_type, global_run_id, sync_status
            FROM runs WHERE sync_status IN (0,2)
            ORDER BY run_id DESC LIMIT 10
        """)
        if not recent.empty:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;margin:12px 0 6px;'>Pending runs</div>",
                unsafe_allow_html=True,
            )
            st.dataframe(recent, use_container_width=True, hide_index=True)


def _render_server_summary(ctx: dict) -> None:
    """Show last sync time from server API."""
    from alems.agent.sync_client import count_unsynced
    from alems.agent.mode_manager import get_local_hw_id
    import os

    db_path = os.environ.get("ALEMS_SQLITE_PATH",
                             str(__import__("pathlib").Path.home() /
                                 "mydrive/a-lems/data/experiments.db"))
    unsynced = count_unsynced(db_path)
    if unsynced == 0:
        st.success("All runs synced to server.")
    else:
        st.info(f"{unsynced} run(s) queued for next sync cycle (≤60s).")


def _render_server_sync(ctx: dict) -> None:
    """Full sync log from PostgreSQL."""
    import os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text

    engine = get_engine(os.environ.get("ALEMS_DB_URL"))
    with get_session(engine) as session:
        logs = session.execute(text("""
            SELECT s.*, h.hostname
            FROM sync_log s
            LEFT JOIN hardware_config h ON h.hw_id = s.hw_id
            ORDER BY s.sync_started_at DESC
            LIMIT 100
        """)).fetchall()
        logs = [dict(r._mapping) for r in logs]

        # Per-machine unsynced counts (not tracked in PG — show from cache)
        machines = session.execute(text("""
            SELECT h.hw_id, h.hostname, h.agent_status, h.last_seen,
                   COUNT(r.global_run_id) FILTER (WHERE r.global_run_id IS NOT NULL) as synced_runs
            FROM hardware_config h
            LEFT JOIN runs r ON r.hw_id = h.hw_id
            GROUP BY h.hw_id, h.hostname, h.agent_status, h.last_seen
            ORDER BY h.last_seen DESC NULLS LAST
        """)).fetchall()

    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;margin-bottom:8px;'>Machine sync health</div>",
        unsafe_allow_html=True,
    )
    for m in machines:
        m = dict(m._mapping)
        clr = "#22c55e" if m.get("agent_status") != "offline" else "#475569"
        st.markdown(
            f"<div style='padding:10px 14px;background:#0d1117;"
            f"border:1px solid {clr}33;border-left:3px solid {clr};"
            f"border-radius:8px;margin-bottom:6px;"
            f"font-size:11px;font-family:IBM Plex Mono,monospace;'>"
            f"<b style='color:#f1f5f9;'>{m.get('hostname','?')}</b> · "
            f"status: <span style='color:{clr};'>{m.get('agent_status','?')}</span> · "
            f"synced runs in PG: <b style='color:#a78bfa;'>{int(m.get('synced_runs') or 0):,}</b> · "
            f"last seen: {str(m.get('last_seen','never'))[:16]}</div>",
            unsafe_allow_html=True,
        )

    if logs:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;margin:12px 0 6px;'>Sync log (last 100)</div>",
            unsafe_allow_html=True,
        )
        import pandas as pd
        df = pd.DataFrame(logs)
        cols = ["hostname", "sync_started_at", "sync_completed_at",
                "runs_synced", "rows_total", "status"]
        st.dataframe(
            df[[c for c in cols if c in df.columns]],
            use_container_width=True,
            hide_index=True,
        )


# ──────────────────────────────────────────────────────────────────────────────
"""
gui/pages/experiment_submissions.py  —  ◎  Global Queue
────────────────────────────────────────────────────────────────────────────
Researcher experiment submission and admin review queue.

SERVER mode:    full admin review UI — approve/reject submissions
CONNECTED mode: submit experiments to global queue
LOCAL mode:     save locally, submit when connected
────────────────────────────────────────────────────────────────────────────
"""
# NOTE: This is a second module in the same file for brevity.
# In production, split into gui/pages/experiment_submissions.py
