"""
gui/sidebar.py
─────────────────────────────────────────────────────────────────────────────
A-LEMS sidebar: brand, active-session banner, Live Lab panel, grouped nav,
MODEL BEHAVIOR panel (Apple-to-Apple moved from overview),
theme toggle, DB footer.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
from gui.config     import NAV_GROUPS, DB_PATH, STATUS_COLORS, STATUS_ICONS
from gui.db         import q1, q
from gui.connection import get_conn, verify_connection, disconnect

# Section header accent colours
_SECTION_ACCENTS = {
    "EXPERIMENT CONTROL": "#22c55e",
    "EXPLORATION":        "#38bdf8",
    "ENERGY & COMPUTE":   "#f59e0b",
    "ORCHESTRATION":      "#ef4444",
    "SYSTEM BEHAVIOR":    "#a78bfa",
    "RESEARCH":           "#3b82f6",
    "ADVANCED":           "#3d5570",
    "MODEL BEHAVIOR":     "#f472b6",   # pink — new section
    "SETTINGS":           "#475569",
}

_CSS = """
<style>
[data-testid="stSidebar"] .stButton > button {
    background: transparent !important; border: none !important;
    border-radius: 6px !important; padding: 5px 10px !important;
    font-size: 12px !important; font-family: "IBM Plex Mono", monospace !important;
    color: #5a7090 !important; text-align: left !important;
    width: 100% !important; transition: background 0.15s, color 0.15s !important;
    margin: 0 !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: #1a2535 !important; color: #c8d8e8 !important;
}
.nav-active > div > button {
    background: #1e2d45 !important; color: #e8f0f8 !important;
    border-left: 2px solid #3b82f6 !important;
}
</style>
"""


def _read_live_url() -> dict:
    """Read live_url.json written by tunnel_agent.py."""
    import json as _json
    from pathlib import Path as _Path
    for p in [_Path(__file__).parent.parent / "live_url.json", _Path("live_url.json")]:
        if p.exists():
            try:
                return _json.loads(p.read_text())
            except Exception:
                pass
    return {}


def _active_session_banner():
    """
    Show the currently running (or most recent) session at the top of the
    sidebar. Queries the DB for the latest group_id and its experiment status.
    Color-coded: green=running, blue=completed, red=failed.
    """
    try:
        row = q1("""
            SELECT group_id,
                   COUNT(*) as total_exps,
                   SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as done,
                   SUM(CASE WHEN status='running'   THEN 1 ELSE 0 END) as running,
                   SUM(CASE WHEN status='failed'    THEN 1 ELSE 0 END) as failed,
                   SUM(runs_completed) as runs_done,
                   SUM(runs_total)     as runs_total
            FROM experiments
            WHERE group_id = (SELECT group_id FROM experiments ORDER BY exp_id DESC LIMIT 1)
            GROUP BY group_id
        """)
        if not row or not row.get("group_id"):
            return

        gid      = row["group_id"]
        total    = int(row.get("total_exps", 0))
        done     = int(row.get("done", 0))
        running  = int(row.get("running", 0))
        failed   = int(row.get("failed", 0))
        runs_d   = int(row.get("runs_done", 0) or 0)
        runs_t   = int(row.get("runs_total", 1) or 1)

        if running > 0:
            status, clr, icon = "RUNNING",   "#22c55e", "🟢"
        elif failed > 0:
            status, clr, icon = "FAILED",    "#ef4444", "🔴"
        elif done == total:
            status, clr, icon = "COMPLETED", "#3b82f6", "●"
        else:
            status, clr, icon = "PENDING",   "#f59e0b", "🟡"

        short = gid.replace("session_", "").replace("_", " ", 1)[:15]
        pct   = int(runs_d / max(runs_t, 1) * 100)

        st.markdown(
            f"<div style='margin:6px 0 8px;padding:8px 10px;background:#050c18;"
            f"border:1px solid {clr}33;border-left:3px solid {clr};"
            f"border-radius:5px;'>"
            f"<div style='font-size:8px;font-weight:700;color:{clr};"
            f"text-transform:uppercase;letter-spacing:.12em;margin-bottom:4px;'>"
            f"{icon}  Active Session</div>"
            f"<div style='font-size:9px;color:#c8d8e8;font-family:monospace;"
            f"margin-bottom:2px;'>{short}</div>"
            f"<div style='font-size:8px;color:#3d5570;margin-bottom:4px;'>"
            f"{status} · {done}/{total} exps · {runs_d}/{runs_t} runs</div>"
            f"<div style='background:#1e2d45;border-radius:2px;height:4px;'>"
            f"<div style='background:{clr};width:{pct}%;height:100%;"
            f"border-radius:2px;transition:width 0.4s;'></div></div>"
            f"</div>",
            unsafe_allow_html=True,
        )
    except Exception:
        pass


def _live_panel():
    """Live Lab connect / status panel."""
    conn      = get_conn()
    online    = conn.get("verified", False)
    _live     = _read_live_url()
    _lab_live = _live.get("online", False)

    clr  = "#22c55e" if online else "#3b82f6"
    icon = "🟢" if online else "🔌"
    sub  = ("Connected · " + conn["url"].replace("https://", "")[:32]
            if online
            else ("🟢 Lab online — click Connect" if _lab_live
                  else "Offline — analysis mode"))

    st.markdown(
        f"<div style='margin:4px 0;padding:7px 10px;background:#0a1018;"
        f"border:1px solid #1e2d45;border-left:2px solid {clr};"
        f"border-radius:5px;'>"
        f"<div style='font-size:9px;font-weight:700;color:{clr};"
        f"text-transform:uppercase;letter-spacing:.1em;'>{icon}  Live Lab</div>"
        f"<div style='font-size:8px;color:#3d5570;margin-top:2px;"
        f"font-family:monospace;'>{sub}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    if online:
        hclr = "#22c55e" if conn.get("harness") else "#f59e0b"
        htxt = "Harness ready" if conn.get("harness") else "Harness unavailable"
        st.markdown(
            f"<div style='font-size:8px;color:{hclr};padding:2px 4px 6px;'>"
            f"● {htxt}</div>",
            unsafe_allow_html=True,
        )
        if st.button("⏏  Disconnect", key="nav_disconnect", use_container_width=True):
            disconnect()
            st.rerun()
    else:
        with st.expander("⚡ Connect to Live Lab", expanded=False):
            st.markdown(
                "<div style='font-size:8px;color:#5a7090;line-height:1.6;margin-bottom:8px;'>"
                "Run <code>tunnel_agent.py</code> on your lab machine, then paste "
                "the URL and token below to trigger live experiments.<br/>"
                "<b style='color:#7090b0'>URL changes each session — get it from tunnel output.</b>"
                "</div>",
                unsafe_allow_html=True,
            )
            _auto_url = _live.get("url", "") if _lab_live else ""
            _auto_tok = _live.get("token", "") if _lab_live else ""

            if _lab_live and _auto_url:
                st.markdown(
                    "<div style='font-size:8px;color:#22c55e;padding:2px 4px 6px;'>"
                    "🟢 Lab is online — URL auto-detected!</div>",
                    unsafe_allow_html=True)

            _url = st.text_input("Lab URL", value=_auto_url,
                                  placeholder="https://xxxx.trycloudflare.com",
                                  key="conn_url")
            _tok = st.text_input("Access token", value=_auto_tok,
                                  placeholder="alems-xxxxxxxxxxxxxxxx",
                                  type="password", key="conn_tok")

            if st.button("🔗  Connect", key="nav_connect", use_container_width=True):
                if not _url:
                    st.error("Enter the lab URL")
                elif not _tok:
                    st.error("Enter the access token")
                else:
                    with st.spinner("Connecting..."):
                        ok, msg, harness = verify_connection(_url, _tok)
                    if ok:
                        conn.update({"url": _url.rstrip("/"), "token": _tok,
                                     "verified": True, "harness": harness,
                                     "mode": "online", "error": ""})
                        st.session_state["conn"] = conn
                        st.success(f"Connected · harness {'ready' if harness else 'unavailable'}")
                        st.rerun()
                    else:
                        conn["error"] = msg
                        st.session_state["conn"] = conn
                        st.error(msg)

            if conn.get("error"):
                st.caption(f"Last error: {conn['error']}")


def _model_behavior_panel():
    """
    MODEL BEHAVIOR sidebar section.
    Renders Apple-to-Apple model comparison — moved from overview.py.
    All original logic preserved exactly.
    """
    # Import the function from overview — all original code lives there
    try:
        from gui.pages.overview import render_model_behavior_sidebar
        render_model_behavior_sidebar()
    except Exception as _e:
        st.markdown(
            f"<div style='font-size:10px;color:#ef4444;padding:8px 4px'>"
            f"Model Behavior panel error: {str(_e)[:80]}</div>",
            unsafe_allow_html=True)


def _settings_panel():
    """
    SETTINGS sidebar section.
    Theme toggle + any future global settings.
    """
    dark = st.session_state.get("theme", "dark") == "dark"
    _lbl = "☀  Switch to Light mode" if dark else "☾  Switch to Dark mode"
    if st.button(_lbl, key="sidebar_theme_toggle", use_container_width=True):
        st.session_state["theme"] = "light" if dark else "dark"
        st.rerun()

    _theme_name = "Dark" if dark else "Light"
    st.markdown(
        f"<div style='font-size:8px;color:#3d5570;padding:2px 4px 6px;"
        f"font-family:monospace'>Theme: {_theme_name}</div>",
        unsafe_allow_html=True)


def render_sidebar() -> str:
    """Render the full sidebar and return the active page_id."""
    _page_map = {label: pid for label, pid in NAV_GROUPS if pid}
    if "nav_selected" not in st.session_state:
        st.session_state.nav_selected = "◈  Overview"

    with st.sidebar:
        st.markdown(_CSS, unsafe_allow_html=True)

        # ── Brand ─────────────────────────────────────────────────────────
        conn   = get_conn()
        online = conn.get("verified", False)
        dot    = ("<span style='color:#22c55e'>●</span>" if online
                  else "<span style='color:#2d3f55'>○</span>")
        st.markdown(
            f"<div style='padding:14px 4px 2px;display:flex;align-items:baseline;gap:6px;'>"
            f"<span style='font-size:20px;font-weight:800;color:#e8f0f8;"
            f"letter-spacing:-.5px;'>⚡ A-LEMS</span>"
            f"<span style='margin-left:auto;font-size:11px;'>{dot}</span>"
            f"</div>"
            f"<div style='font-size:8px;color:#2d3f55;padding:0 4px 8px;"
            f"text-transform:uppercase;letter-spacing:.14em;'>"
            f"Energy Measurement Lab</div>",
            unsafe_allow_html=True,
        )

        # ── Active session banner ──────────────────────────────────────────
        _active_session_banner()

        # ── Live Lab panel ─────────────────────────────────────────────────
        _live_panel()

        st.markdown(
            "<div style='height:1px;background:#1a2535;margin:8px 0;'></div>",
            unsafe_allow_html=True,
        )

        # ── Navigation ─────────────────────────────────────────────────────
        for label, pid in NAV_GROUPS:
            if pid is None:
                acc = _SECTION_ACCENTS.get(label, "#2d3f55")
                st.markdown(
                    f"<div style='margin:14px 0 3px;border-bottom:1px solid {acc}28;"
                    f"padding-bottom:3px;'>"
                    f"<span style='font-size:8px;font-weight:700;color:{acc};"
                    f"text-transform:uppercase;letter-spacing:.15em;'>{label}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
            else:
                active = st.session_state.nav_selected == label
                if active:
                    st.markdown("<div class='nav-active'>", unsafe_allow_html=True)
                if st.button(label, key=f"nav_{pid}", use_container_width=True):
                    st.session_state.nav_selected = label
                    st.rerun()
                if active:
                    st.markdown("</div>", unsafe_allow_html=True)

        # ── MODEL BEHAVIOR section ─────────────────────────────────────────
        st.markdown(
            "<div style='height:1px;background:#1a2535;margin:10px 0 4px;'></div>",
            unsafe_allow_html=True)
        acc_mb = _SECTION_ACCENTS.get("MODEL BEHAVIOR", "#f472b6")
        st.markdown(
            f"<div style='margin:10px 0 6px;border-bottom:1px solid {acc_mb}28;"
            f"padding-bottom:3px;'>"
            f"<span style='font-size:8px;font-weight:700;color:{acc_mb};"
            f"text-transform:uppercase;letter-spacing:.15em;'>🍎 Models</span>"
            f"</div>",
            unsafe_allow_html=True)
        _is_models = st.session_state.get("nav_selected","") == "_models_page"
        if _is_models:
            st.markdown("<div class='nav-active'>", unsafe_allow_html=True)
        if st.button("Apple-to-Apple comparison", key="nav_models_page",
                     use_container_width=True):
            st.session_state["nav_selected"] = "_models_page"
            st.rerun()
        if _is_models:
            st.markdown("</div>", unsafe_allow_html=True)

        # ── SETTINGS section ───────────────────────────────────────────────
        st.markdown(
            "<div style='height:1px;background:#1a2535;margin:10px 0 4px;'></div>",
            unsafe_allow_html=True)
        acc_set = _SECTION_ACCENTS["SETTINGS"]
        st.markdown(
            f"<div style='margin:10px 0 6px;border-bottom:1px solid {acc_set}28;"
            f"padding-bottom:3px;'>"
            f"<span style='font-size:8px;font-weight:700;color:{acc_set};"
            f"text-transform:uppercase;letter-spacing:.15em;'>⚙ Settings</span>"
            f"</div>",
            unsafe_allow_html=True)
        _settings_panel()

        # ── Footer ─────────────────────────────────────────────────────────
        st.markdown(
            "<div style='height:1px;background:#1a2535;margin:14px 0 8px;'></div>",
            unsafe_allow_html=True,
        )
        try:
            nr = q1("SELECT COUNT(*) AS n FROM runs").get("n", "—")
            ne = q1("SELECT COUNT(*) AS n FROM experiments").get("n", "—")
            st.markdown(
                f"<div style='font-size:9px;color:#2d3f55;padding:0 4px 4px;'>"
                f"<span style='color:#3d5570'>Runs</span> "
                f"<b style='color:#7090b0;font-family:monospace'>{nr}</b>"
                f"&nbsp;&nbsp;"
                f"<span style='color:#3d5570'>Exps</span> "
                f"<b style='color:#7090b0;font-family:monospace'>{ne}</b>"
                f"</div>"
                f"<div style='font-size:8px;color:#1e2d3a;padding:0 4px 6px;'>"
                f"{DB_PATH.name}</div>",
                unsafe_allow_html=True,
            )
        except Exception:
            st.markdown(
                "<div style='font-size:9px;color:#ef4444;padding:0 4px;'>"
                "⚠ DB offline</div>",
                unsafe_allow_html=True,
            )

        if st.button("⟳  Refresh data", key="nav_refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    _sel = st.session_state.nav_selected
    if _sel == "_models_page":
        return "models"
    return _page_map.get(_sel, "overview")
