"""
gui/pages/_agent_utils.py
────────────────────────────────────────────────────────────────────────────
Shared utilities for all agent/distributed Streamlit pages.

Import in every new page:
    from gui.pages._agent_utils import get_ui_mode, mode_banner, server_url
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import os
import streamlit as st
from typing import Optional


def get_ui_mode() -> str:
    """
    Returns current UI mode:
      'server'    — running on Oracle VM against PostgreSQL
      'connected' — local machine, agent connected to server
      'local'     — local machine, offline / local mode
    """
    db_url = os.environ.get("ALEMS_DB_URL", "")
    if db_url.startswith("postgresql"):
        return "server"
    try:
        from alems.agent.mode_manager import get_mode
        return get_mode()   # 'local' or 'connected'
    except Exception:
        return "local"


def get_server_url() -> str:
    try:
        from alems.agent.mode_manager import get_server_url as _get
        return _get()
    except Exception:
        return os.environ.get("ALEMS_SERVER_URL", "http://129.153.71.47:8000")


def is_server_alive() -> bool:
    try:
        from alems.agent.heartbeat import check_server_health
        return check_server_health()
    except Exception:
        return False


def mode_banner(mode: str, server_alive: Optional[bool] = None) -> None:
    """
    Renders the connection status banner at the top of every distributed page.
    Shows different content per mode so the researcher always knows context.
    """
    if mode == "server":
        st.markdown(
            "<div style='padding:8px 14px;background:#052e16;border:1px solid #16a34a33;"
            "border-left:3px solid #22c55e;border-radius:8px;margin-bottom:14px;"
            "font-size:11px;color:#86efac;font-family:IBM Plex Mono,monospace;'>"
            "🟢 <b>Server mode</b> — PostgreSQL · showing all connected machines"
            "</div>",
            unsafe_allow_html=True,
        )
    elif mode == "connected":
        alive = server_alive if server_alive is not None else is_server_alive()
        if alive:
            st.markdown(
                f"<div style='padding:8px 14px;background:#052e16;border:1px solid #16a34a33;"
                f"border-left:3px solid #22c55e;border-radius:8px;margin-bottom:14px;"
                f"font-size:11px;color:#86efac;font-family:IBM Plex Mono,monospace;'>"
                f"🟢 <b>Connected</b> — agent running · syncing to {get_server_url()}"
                f"</div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"<div style='padding:8px 14px;background:#1c0a00;border:1px solid #f59e0b33;"
                f"border-left:3px solid #f59e0b;border-radius:8px;margin-bottom:14px;"
                f"font-size:11px;color:#fcd34d;font-family:IBM Plex Mono,monospace;'>"
                f"🟡 <b>Connecting</b> — server unreachable · local data shown · "
                f"will sync when {get_server_url()} is back online"
                f"</div>",
                unsafe_allow_html=True,
            )
    else:  # local
        st.markdown(
            "<div style='padding:8px 14px;background:#0f172a;border:1px solid #47556933;"
            "border-left:3px solid #475569;border-radius:8px;margin-bottom:14px;"
            "font-size:11px;color:#94a3b8;font-family:IBM Plex Mono,monospace;'>"
            "🔴 <b>Local mode</b> — SQLite only · start agent with "
            "<code>python -m alems.agent start --mode connected</code> to connect"
            "</div>",
            unsafe_allow_html=True,
        )


def local_only_notice(feature: str = "This feature") -> None:
    """Show when a feature requires server connection but we're in local mode."""
    st.info(
        f"{feature} requires a server connection. "
        f"Switch to connected mode: `python -m alems.agent set-mode connected`",
        icon="ℹ️",
    )


def fetch_machines_from_server() -> list[dict]:
    """Fetch live machine list from server API. Returns [] on any error."""
    try:
        import httpx
        from alems.agent.mode_manager import get_api_key
        r = httpx.get(
            f"{get_server_url()}/machines",
            headers={"Authorization": f"Bearer {get_api_key()}"},
            timeout=5,
        )
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


def fetch_machines_from_pg(session) -> list[dict]:
    """Fetch machine list directly from PostgreSQL (server mode)."""
    from sqlalchemy import text
    rows = session.execute(text("""
        SELECT h.hw_id, h.hostname, h.cpu_model, h.ram_gb,
               h.agent_status, h.last_seen, h.agent_version,
               c.status as run_status, c.task_name, c.model_name,
               c.elapsed_s, c.energy_uj, c.avg_power_watts,
               COUNT(DISTINCT r.run_id) as total_runs
        FROM hardware_config h
        LEFT JOIN run_status_cache c ON c.hw_id = h.hw_id
        LEFT JOIN runs r ON r.hw_id = h.hw_id
        GROUP BY h.hw_id, h.hostname, h.cpu_model, h.ram_gb,
                 h.agent_status, h.last_seen, h.agent_version,
                 c.status, c.task_name, c.model_name,
                 c.elapsed_s, c.energy_uj, c.avg_power_watts
        ORDER BY h.last_seen DESC NULLS LAST
    """)).fetchall()
    return [dict(r._mapping) for r in rows]
