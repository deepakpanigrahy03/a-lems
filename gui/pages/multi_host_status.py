"""
gui/pages/multi_host_status.py  —  ⬡  Multi-Host Status
────────────────────────────────────────────────────────────────────────────
Live status dashboard for all connected lab machines.

Mode behaviour:
  SERVER    — reads PostgreSQL run_status_cache + hardware_config directly
  CONNECTED — fetches from server /machines API, shows own machine prominently
  LOCAL     — shows single local machine card from SQLite, agent start prompt
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import time
import streamlit as st
from gui.db import q, q1
from gui.pages._agent_utils import (
    get_ui_mode, mode_banner, is_server_alive,
    fetch_machines_from_server, fetch_machines_from_pg,
)

ACCENT = "#38bdf8"


def render(ctx: dict) -> None:
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:16px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px;'>"
        f"⬡ Multi-Host Status</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>"
        f"Live status of all connected lab machines.</div></div>",
        unsafe_allow_html=True,
    )

    mode       = get_ui_mode()
    server_ok  = is_server_alive() if mode == "connected" else None
    mode_banner(mode, server_ok)

    if mode == "server":
        _render_server_view(ctx)
    elif mode == "connected":
        _render_connected_view(ctx, server_ok)
    else:
        _render_local_view(ctx)


# ── Server mode — reads PostgreSQL directly ───────────────────────────────────

def _render_server_view(ctx: dict):
    import os
    from alems.shared.db_layer import get_engine, get_session
    from sqlalchemy import text

    engine = get_engine(os.environ.get("ALEMS_DB_URL"))
    with get_session(engine) as session:
        machines = fetch_machines_from_pg(session)

    _render_machine_grid(machines, show_all=True)

    # Auto-refresh toggle
    if st.checkbox("Auto-refresh every 10s", value=False, key="mhs_autorefresh"):
        time.sleep(10)
        st.rerun()


# ── Connected mode — fetches from /machines API ───────────────────────────────

def _render_connected_view(ctx: dict, server_ok: bool):
    if not server_ok:
        st.warning("Server unreachable — showing local machine data only")
        _render_local_view(ctx)
        return

    machines = fetch_machines_from_server()
    if not machines:
        st.info("No machines returned from server yet.")
        _render_local_view(ctx)
        return

    # Highlight own machine
    try:
        from alems.agent.mode_manager import get_server_hw_id
        own_hw_id = get_server_hw_id()
    except Exception:
        own_hw_id = None

    _render_machine_grid(machines, show_all=True, highlight_hw_id=own_hw_id)

    if st.checkbox("Auto-refresh every 10s", value=False, key="mhs_autorefresh_c"):
        time.sleep(10)
        st.rerun()


# ── Local mode — single machine from SQLite ───────────────────────────────────

def _render_local_view(ctx: dict):
    hw = q1("""
        SELECT h.hw_id, h.hostname, h.cpu_model, h.ram_gb,
               h.agent_status, h.last_seen,
               COUNT(DISTINCT r.run_id) as total_runs,
               AVG(r.total_energy_uj/1e6) as avg_energy_j
        FROM hardware_config h
        LEFT JOIN runs r ON r.hw_id = h.hw_id
        GROUP BY h.hw_id
        LIMIT 1
    """) or {}

    machines = [{
        "hostname":     hw.get("hostname", "local"),
        "cpu_model":    hw.get("cpu_model", "unknown"),
        "ram_gb":       hw.get("ram_gb", 0),
        "agent_status": "idle",
        "total_runs":   hw.get("total_runs", 0),
        "run_status":   None,
        "last_seen":    "local mode",
    }]
    _render_machine_grid(machines, show_all=False)

    st.markdown(
        "<div style='padding:10px 14px;background:#0c1f3a;"
        "border-left:3px solid #3b82f6;border-radius:0 8px 8px 0;"
        "font-size:11px;color:#93c5fd;margin-top:12px;'>"
        "To see all connected machines, start the agent in connected mode:<br>"
        "<code>python -m alems.agent start --mode connected</code>"
        "</div>",
        unsafe_allow_html=True,
    )


# ── Shared machine card renderer ──────────────────────────────────────────────

def _render_machine_grid(
    machines: list[dict],
    show_all: bool = True,
    highlight_hw_id: int | None = None,
) -> None:
    if not machines:
        st.info("No machines registered yet.")
        return

    online  = [m for m in machines if m.get("agent_status") not in ("offline", None)]
    offline = [m for m in machines if m.get("agent_status") in ("offline", None)]

    counts_md = (
        f"**{len(machines)}** machines total · "
        f"**{len(online)}** online · "
        f"**{len(offline)}** offline"
    )
    st.caption(counts_md)

    # Online first, then offline
    for m in online + offline:
        _machine_card(m, highlight=m.get("hw_id") == highlight_hw_id)


def _machine_card(m: dict, highlight: bool = False) -> None:
    status     = m.get("agent_status", "offline")
    run_status = m.get("run_status", "idle")
    hostname   = m.get("hostname") or f"hw_{m.get('hw_id','?')}"

    # Colour logic
    if status == "offline":
        clr, dot = "#475569", "○"
    elif run_status == "running":
        clr, dot = "#f59e0b", "●"
    else:
        clr, dot = "#22c55e", "●"

    border_extra = f"box-shadow:0 0 0 1px {clr}44;" if highlight else ""

    # Live run metrics (if running)
    live_html = ""
    if run_status == "running":
        energy_j  = (m.get("energy_uj") or 0) / 1e6
        elapsed   = m.get("elapsed_s") or 0
        tokens    = m.get("total_tokens") or 0
        task      = m.get("task_name") or "—"
        model     = m.get("model_name") or "—"
        power_w   = m.get("avg_power_watts") or 0
        live_html = (
            f"<div style='margin-top:6px;padding:6px 10px;"
            f"background:#0d1117;border-radius:4px;"
            f"font-size:10px;font-family:IBM Plex Mono,monospace;color:#94a3b8;'>"
            f"task: <b style='color:#f1f5f9;'>{task}</b> · "
            f"model: <b style='color:#f1f5f9;'>{model}</b><br>"
            f"elapsed: <b style='color:{clr};'>{elapsed}s</b> · "
            f"energy: <b style='color:#f59e0b;'>{energy_j:.4f}J</b> · "
            f"power: <b style='color:#ef4444;'>{power_w:.1f}W</b> · "
            f"tokens: <b style='color:#a78bfa;'>{tokens:,}</b>"
            f"</div>"
        )

    last_seen = m.get("last_seen") or "never"
    total_runs = int(m.get("total_runs") or 0)
    cpu_model  = m.get("cpu_model") or "unknown"
    ram_gb     = m.get("ram_gb") or "?"
    own_label  = " ← this machine" if highlight else ""

    st.markdown(
        f"<div style='padding:12px 16px;background:#0d1117;"
        f"border:1px solid {clr}33;border-left:3px solid {clr};"
        f"border-radius:8px;margin-bottom:8px;{border_extra}'>"
        f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:6px;'>"
        f"<span style='color:{clr};font-size:10px;'>{dot}</span>"
        f"<span style='font-size:12px;font-weight:600;color:#f1f5f9;'>"
        f"{hostname}<span style='color:{clr};font-size:9px;margin-left:6px;'>"
        f"{own_label}</span></span>"
        f"<span style='font-size:9px;color:{clr};margin-left:auto;'>"
        f"{status.upper()}</span></div>"
        f"<div style='font-size:10px;color:#475569;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"CPU: {cpu_model} · RAM: {ram_gb}GB · "
        f"{total_runs:,} runs · last seen: {last_seen}</div>"
        f"{live_html}</div>",
        unsafe_allow_html=True,
    )
