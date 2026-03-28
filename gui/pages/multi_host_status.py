"""
gui/pages/multi_host_status.py  —  ⬡  Multi-Host Status
────────────────────────────────────────────────────────────────────────────
Live status dashboard for all connected lab machines.

Mode behaviour:
  SERVER    — reads PostgreSQL run_status_cache + hardware_config
              shows all connected machines, live metrics, global counts
  CONNECTED — fetches from server /machines API
              shows own machine prominently + all others
  LOCAL     — shows single local machine from SQLite
              shows "connect to server" prompt

Live data path (NOT bulk sync):
  Agent heartbeat every 5s during run
  → POST /heartbeat {live_metrics}
  → run_status_cache table in PostgreSQL
  → This page reads run_status_cache
  → Auto-refreshes every 5s
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import time
import streamlit as st
from gui.db import q, q1
from gui.pages._agent_utils import (
    get_ui_mode, mode_banner, is_server_alive,
    fetch_machines_from_server,
)

ACCENT = "#38bdf8"


def render(ctx: dict) -> None:
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:16px;'>"
        f"<div style='display:flex;align-items:center;gap:12px;margin-bottom:4px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;'>⬡ Multi-Host Status</div>"
        f"<div style='font-size:9px;padding:2px 8px;border-radius:4px;"
        f"background:#052e16;color:#22c55e;border:1px solid #16a34a44;'>LIVE</div>"
        f"</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>"
        f"Real-time status of all connected lab machines. "
        f"Live metrics update every 5s via agent heartbeat.</div></div>",
        unsafe_allow_html=True,
    )

    mode      = get_ui_mode()
    server_ok = is_server_alive() if mode == "connected" else None
    mode_banner(mode, server_ok)

    # Controls row
    col1, col2 = st.columns([3, 1])
    with col1:
        auto_refresh = st.checkbox(
            "Auto-refresh every 5s", value=True, key="mhs_refresh"
        )
    with col2:
        if st.button("↺ Refresh now", key="mhs_manual"):
            st.rerun()

    if mode == "server":
        _render_server_view(ctx)
    elif mode == "connected":
        _render_connected_view(ctx, server_ok)
    else:
        _render_local_view(ctx)

    if auto_refresh:
        time.sleep(5)
        st.rerun()


# ── Server mode ───────────────────────────────────────────────────────────────

def _render_server_view(ctx: dict):
    import os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text

    engine = get_engine(os.environ.get("ALEMS_DB_URL"))
    with get_session(engine) as session:

        counts = session.execute(text("""
            SELECT
                (SELECT COUNT(*) FROM hardware_config) as machines,
                (SELECT COUNT(*) FROM hardware_config
                 WHERE last_seen > NOW() - INTERVAL '2 minutes') as online,
                (SELECT COUNT(*) FROM runs) as total_runs,
                (SELECT COUNT(*) FROM experiments) as total_experiments,
                (SELECT COUNT(*) FROM run_status_cache
                 WHERE status = 'running') as active_runs
        """)).fetchone()

        machines = session.execute(text("""
            SELECT
                h.hw_id, h.hostname, h.cpu_model, h.ram_gb,
                h.agent_status, h.last_seen, h.agent_version,
                c.status      as run_status,
                c.task_name, c.model_name, c.workflow_type,
                c.elapsed_s, c.energy_uj, c.avg_power_watts,
                c.total_tokens, c.steps,
                COUNT(DISTINCT r.run_id)  as total_runs,
                COUNT(DISTINCT r.exp_id)  as total_experiments
            FROM hardware_config h
            LEFT JOIN run_status_cache c ON c.hw_id = h.hw_id
            LEFT JOIN runs r             ON r.hw_id = h.hw_id
            GROUP BY
                h.hw_id, h.hostname, h.cpu_model, h.ram_gb,
                h.agent_status, h.last_seen, h.agent_version,
                c.status, c.task_name, c.model_name, c.workflow_type,
                c.elapsed_s, c.energy_uj, c.avg_power_watts,
                c.total_tokens, c.steps
            ORDER BY
                CASE WHEN c.status = 'running' THEN 0
                     WHEN h.agent_status = 'idle' THEN 1
                     ELSE 2 END,
                h.last_seen DESC NULLS LAST
        """)).fetchall()
        machines = [dict(r._mapping) for r in machines]

    # Global KPI strip
    if counts:
        c1, c2, c3, c4, c5 = st.columns(5)
        for col, val, label, clr in [
            (c1, int(counts.machines         or 0), "Machines",     "#38bdf8"),
            (c2, int(counts.online           or 0), "Online",       "#22c55e"),
            (c3, int(counts.active_runs      or 0), "Active runs",  "#f59e0b"),
            (c4, int(counts.total_runs       or 0), "Total runs",   "#a78bfa"),
            (c5, int(counts.total_experiments or 0),"Experiments",  "#94a3b8"),
        ]:
            with col:
                st.markdown(
                    f"<div style='padding:8px 10px;background:#0d1117;"
                    f"border:1px solid {clr}33;border-left:3px solid {clr};"
                    f"border-radius:6px;text-align:center;margin-bottom:12px;'>"
                    f"<div style='font-size:20px;font-weight:700;color:{clr};"
                    f"font-family:IBM Plex Mono,monospace;'>{val:,}</div>"
                    f"<div style='font-size:9px;color:#94a3b8;"
                    f"text-transform:uppercase;'>{label}</div></div>",
                    unsafe_allow_html=True,
                )

    _render_machine_grid(machines, highlight_hw_id=None)


# ── Connected mode ────────────────────────────────────────────────────────────

def _render_connected_view(ctx: dict, server_ok: bool):
    if not server_ok:
        st.warning("Server unreachable — showing local machine only")
        _render_local_view(ctx)
        return

    machines = fetch_machines_from_server()
    if not machines:
        st.info("No machines returned from server yet.")
        _render_local_view(ctx)
        return

    try:
        from alems.agent.mode_manager import get_server_hw_id
        own_hw_id = get_server_hw_id()
    except Exception:
        own_hw_id = None

    _render_machine_grid(machines, highlight_hw_id=own_hw_id)


# ── Local mode ────────────────────────────────────────────────────────────────

def _render_local_view(ctx: dict):
    hw = q1("""
        SELECT h.hw_id, h.hostname, h.cpu_model, h.ram_gb,
               h.agent_status, h.last_seen,
               COUNT(DISTINCT r.run_id)  as total_runs,
               COUNT(DISTINCT r.exp_id)  as total_experiments,
               SUM(CASE WHEN r.sync_status = 0 THEN 1 ELSE 0 END) as unsynced_runs
        FROM hardware_config h
        LEFT JOIN runs r ON r.hw_id = h.hw_id
        GROUP BY h.hw_id LIMIT 1
    """) or {}

    c1, c2, c3 = st.columns(3)
    for col, val, label, clr in [
        (c1, int(hw.get("total_runs",        0) or 0), "Local runs",   "#38bdf8"),
        (c2, int(hw.get("total_experiments", 0) or 0), "Experiments",  "#a78bfa"),
        (c3, int(hw.get("unsynced_runs",     0) or 0), "Pending sync", "#f59e0b"),
    ]:
        with col:
            st.markdown(
                f"<div style='padding:8px 10px;background:#0d1117;"
                f"border:1px solid {clr}33;border-left:3px solid {clr};"
                f"border-radius:6px;text-align:center;margin-bottom:12px;'>"
                f"<div style='font-size:20px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;'>{val:,}</div>"
                f"<div style='font-size:9px;color:#94a3b8;"
                f"text-transform:uppercase;'>{label}</div></div>",
                unsafe_allow_html=True,
            )

    machines = [{
        "hostname":          hw.get("hostname", "local"),
        "cpu_model":         hw.get("cpu_model", "unknown"),
        "ram_gb":            hw.get("ram_gb", 0),
        "agent_status":      "idle",
        "total_runs":        hw.get("total_runs", 0),
        "total_experiments": hw.get("total_experiments", 0),
        "run_status":        None,
        "last_seen":         "local mode",
    }]
    _render_machine_grid(machines, highlight_hw_id=None)

    st.markdown(
        "<div style='padding:10px 14px;background:#0c1f3a;"
        "border-left:3px solid #3b82f6;border-radius:0 8px 8px 0;"
        "font-size:11px;color:#93c5fd;margin-top:12px;'>"
        "To connect to server and see all machines:<br>"
        "<code>python -m alems.agent start --mode connected</code>"
        "</div>",
        unsafe_allow_html=True,
    )


# ── Machine card grid ─────────────────────────────────────────────────────────

def _render_machine_grid(
    machines: list[dict],
    highlight_hw_id: int | None = None,
) -> None:
    if not machines:
        st.info("No machines registered yet.")
        return

    running = [m for m in machines
               if m.get("run_status") == "running"]
    idle    = [m for m in machines
               if m.get("agent_status") == "idle"
               and m.get("run_status") != "running"]
    offline = [m for m in machines
               if m.get("agent_status") not in ("idle", "busy", "syncing")
               and m.get("run_status") != "running"]

    if running:
        st.markdown(
            "<div style='font-size:11px;font-weight:600;color:#f59e0b;"
            "text-transform:uppercase;margin-bottom:8px;'>● Active runs</div>",
            unsafe_allow_html=True,
        )
        for m in running:
            _machine_card(m, highlight=m.get("hw_id") == highlight_hw_id)

    if idle:
        st.markdown(
            "<div style='font-size:11px;font-weight:600;color:#22c55e;"
            "text-transform:uppercase;margin:12px 0 8px;'>● Idle</div>",
            unsafe_allow_html=True,
        )
        for m in idle:
            _machine_card(m, highlight=m.get("hw_id") == highlight_hw_id)

    if offline:
        st.markdown(
            "<div style='font-size:11px;font-weight:600;color:#475569;"
            "text-transform:uppercase;margin:12px 0 8px;'>○ Offline</div>",
            unsafe_allow_html=True,
        )
        for m in offline:
            _machine_card(m, highlight=m.get("hw_id") == highlight_hw_id)


def _machine_card(m: dict, highlight: bool = False) -> None:
    status     = m.get("agent_status", "offline")
    run_status = m.get("run_status") or "idle"
    hostname   = m.get("hostname") or f"hw_{m.get('hw_id', '?')}"

    if run_status == "running":
        clr, dot = "#f59e0b", "●"
    elif status in ("idle", "syncing"):
        clr, dot = "#22c55e", "●"
    else:
        clr, dot = "#475569", "○"

    border_extra = f"box-shadow:0 0 0 2px {clr}66;" if highlight else ""

    # Live run metrics block
    live_html = ""
    if run_status == "running":
        energy_j = (m.get("energy_uj") or 0) / 1e6
        elapsed  = m.get("elapsed_s")    or 0
        tokens   = m.get("total_tokens") or 0
        task     = m.get("task_name")    or "—"
        model    = m.get("model_name")   or "—"
        workflow = m.get("workflow_type") or "—"
        power_w  = m.get("avg_power_watts") or 0
        steps    = m.get("steps")        or 0
        bar_pct  = min(100, energy_j / 100 * 100)

        live_html = (
            f"<div style='margin-top:8px;padding:8px 10px;"
            f"background:#0a0a0a;border-radius:6px;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"<div style='font-size:10px;color:#94a3b8;margin-bottom:6px;'>"
            f"task: <b style='color:#f1f5f9;'>{task}</b>  ·  "
            f"model: <b style='color:#f1f5f9;'>{model}</b>  ·  "
            f"workflow: <b style='color:#f1f5f9;'>{workflow}</b></div>"
            f"<div style='display:flex;gap:16px;font-size:10px;'>"
            f"<span>⏱ <b style='color:{clr};'>{elapsed}s</b></span>"
            f"<span>⚡ <b style='color:#f59e0b;'>{energy_j:.4f}J</b></span>"
            f"<span>🔋 <b style='color:#ef4444;'>{power_w:.1f}W</b></span>"
            f"<span>🪙 <b style='color:#a78bfa;'>{tokens:,} tok</b></span>"
            f"<span>📶 <b style='color:#38bdf8;'>{steps} steps</b></span>"
            f"</div>"
            f"<div style='margin-top:6px;background:#1a1a1a;"
            f"border-radius:3px;height:4px;'>"
            f"<div style='background:{clr};width:{bar_pct:.1f}%;"
            f"height:4px;border-radius:3px;'></div></div>"
            f"</div>"
        )

    total_runs = int(m.get("total_runs")        or 0)
    total_exps = int(m.get("total_experiments") or 0)
    cpu_model  = m.get("cpu_model")  or "unknown"
    ram_gb     = m.get("ram_gb")     or "?"
    last_seen  = str(m.get("last_seen") or "never")[:16]
    own_label  = "  ◀ this machine" if highlight else ""

    st.markdown(
        f"<div style='padding:12px 16px;background:#0d1117;"
        f"border:1px solid {clr}33;border-left:3px solid {clr};"
        f"border-radius:8px;margin-bottom:8px;{border_extra}'>"
        f"<div style='display:flex;align-items:center;gap:10px;"
        f"margin-bottom:6px;'>"
        f"<span style='color:{clr};font-size:12px;'>{dot}</span>"
        f"<span style='font-size:13px;font-weight:600;color:#f1f5f9;'>"
        f"{hostname}</span>"
        f"<span style='font-size:9px;color:{clr};'>{own_label}</span>"
        f"<span style='font-size:9px;color:{clr};margin-left:auto;'>"
        f"{status.upper()}</span>"
        f"</div>"
        f"<div style='font-size:10px;color:#475569;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"CPU: {cpu_model}  ·  RAM: {ram_gb}GB  ·  "
        f"{total_runs:,} runs  ·  {total_exps} experiments  ·  "
        f"last seen: {last_seen}"
        f"</div>"
        f"{live_html}"
        f"</div>",
        unsafe_allow_html=True,
    )
