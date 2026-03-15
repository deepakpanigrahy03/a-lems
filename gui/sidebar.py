"""
gui/sidebar.py
─────────────────────────────────────────────────────────────────────────────
A-LEMS sidebar — Phase 3: 11 clean section buttons.

Sidebar contains:
  • Brand panel (logo, name, version, connection dot)
  • Active session banner
  • Live Lab panel (connect/disconnect)
  • 11 section navigation buttons (coloured dot + name)
  • Theme toggle
  • Footer (RAPL spark bars + stats)

Session state written here:
  nav_section  — clicked section name | None
  nav_page     — None (always cleared on section click → goes to landing)
  nav_last     — preserved (not touched here)

render_sidebar() returns None (dispatcher reads session state directly).
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st

from gui.config     import SECTIONS, SECTION_PAGES, SECTION_ACCENTS, DB_PATH, STATUS_COLORS
from gui.db         import q1
from gui.connection import get_conn, verify_connection, disconnect
from gui.theme      import _tokens


# ── CSS ───────────────────────────────────────────────────────────────────────
def _css(t: dict) -> str:
    return f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600;700&display=swap');

[data-testid="stSidebar"],
[data-testid="stSidebarContent"] {{
    background: {t["bg1"]} !important;
    border-right: 0.5px solid {t["brd"]} !important;
}}
[data-testid="stSidebar"] * {{
    font-family: 'IBM Plex Mono', monospace !important;
    box-sizing: border-box;
}}

/* ── Kill default button styling ────────────────────────────────── */
[data-testid="stSidebar"] .stButton > button {{
    background: transparent !important;
    border: none !important;
    border-left: 2px solid transparent !important;
    border-radius: 0 7px 7px 0 !important;
    padding: 8px 12px 8px 10px !important;
    font-size: 12.5px !important;
    font-weight: 400 !important;
    color: {t["t2"]} !important;
    text-align: left !important;
    width: 100% !important;
    cursor: pointer !important;
    transition: background .13s, border-color .13s, color .13s !important;
    margin: 1px 0 !important;
    position: relative !important;
    line-height: 1.2 !important;
}}
[data-testid="stSidebar"] .stButton > button:hover {{
    background: {t["bg2"]} !important;
    color: {t["t1"]} !important;
    cursor: pointer !important;
}}

/* ── Active section button ──────────────────────────────────────── */
.alems-active [data-testid="stButton"] > button {{
    background: linear-gradient(90deg, {t["bg2"]}ee, {t["bg1"]}) !important;
    color: {t["t1"]} !important;
    font-weight: 600 !important;
    cursor: pointer !important;
}}

/* ── Expander ───────────────────────────────────────────────────── */
[data-testid="stSidebar"] [data-testid="stExpander"] {{
    background: {t["bg2"]} !important;
    border: 0.5px solid {t["brd"]} !important;
    border-radius: 8px !important;
    margin: 3px 4px !important;
}}
[data-testid="stSidebar"] details > summary {{
    font-size: 11px !important;
    color: {t["t2"]} !important;
    cursor: pointer !important;
    padding: 7px 10px !important;
    overflow: visible !important;
}}

/* ── Inputs ─────────────────────────────────────────────────────── */
[data-testid="stSidebar"] input {{
    background: {t["bg2"]} !important;
    color: {t["t1"]} !important;
    border: 0.5px solid {t["brd2"]} !important;
    border-radius: 6px !important;
    font-size: 11px !important;
}}
[data-testid="stSidebar"] input:focus {{
    border-color: {t["accent"]} !important;
    box-shadow: 0 0 0 2px {t["accent"]}22 !important;
    outline: none !important;
}}
[data-testid="stSidebar"] label p {{
    font-size: 9px !important;
    color: {t["t3"]} !important;
    text-transform: uppercase !important;
    letter-spacing: .1em !important;
}}

/* ── Scrollbar ──────────────────────────────────────────────────── */
[data-testid="stSidebarContent"]::-webkit-scrollbar {{ width: 3px; }}
[data-testid="stSidebarContent"]::-webkit-scrollbar-track {{ background: {t["bg1"]}; }}
[data-testid="stSidebarContent"]::-webkit-scrollbar-thumb {{
    background: {t["brd2"]}; border-radius: 2px; }}

/* Force pointer everywhere */
[data-testid="stSidebar"] button {{ cursor: pointer !important; }}
</style>"""


# ── Helpers ───────────────────────────────────────────────────────────────────
def _read_live_url() -> dict:
    import json as _j
    from pathlib import Path as _P
    for p in [_P(__file__).parent.parent / "live_url.json", _P("live_url.json")]:
        if p.exists():
            try:
                return _j.loads(p.read_text())
            except Exception:
                pass
    return {}


def _divider(t: dict):
    st.markdown(
        f"<div style='height:0.5px;background:{t['brd']};margin:6px 0;'></div>",
        unsafe_allow_html=True)


# ── Brand panel ───────────────────────────────────────────────────────────────
def _brand(t: dict, online: bool):
    dot_clr  = "#22c55e" if online else t["t3"]
    dot_glow = "box-shadow:0 0 0 3px #22c55e22;" if online else ""

    # Logo click → go to overview (clear section)
    st.markdown(
        f"<div style='padding:14px 10px 10px;border-bottom:0.5px solid {t['brd']};'>"
        f"<div style='display:flex;align-items:center;gap:8px;margin-bottom:3px;'>"
        f"<div style='width:28px;height:28px;border-radius:7px;flex-shrink:0;"
        f"background:linear-gradient(135deg,#052e16,#064e3b);"
        f"border:0.5px solid #22c55e44;"
        f"display:flex;align-items:center;justify-content:center;"
        f"box-shadow:0 3px 10px #22c55e18;'>"
        f"<svg viewBox='0 0 28 28' width='20' height='20' fill='none'>"
        f"<defs>"
        f"<radialGradient id='rg1' cx='40%' cy='35%' r='60%'>"
        f"<stop offset='0%' stop-color='#6ee7b7'/>"
        f"<stop offset='50%' stop-color='#059669'/>"
        f"<stop offset='100%' stop-color='#022c22'/></radialGradient>"
        f"</defs>"
        f"<circle cx='14' cy='14' r='7' fill='url(#rg1)'/>"
        f"<ellipse cx='14' cy='14' rx='11' ry='4' stroke='#22c55e'"
        f" stroke-width='0.9' fill='none' opacity='0.7' transform='rotate(-15,14,14)'/>"
        f"<line x1='14' y1='2' x2='14' y2='7' stroke='#6ee7b7' stroke-width='1.2' stroke-linecap='round'/>"
        f"<line x1='14' y1='21' x2='14' y2='26' stroke='#6ee7b7' stroke-width='1.2' stroke-linecap='round'/>"
        f"<circle cx='14' cy='2' r='1.4' fill='#6ee7b7'/>"
        f"<circle cx='14' cy='26' r='1.4' fill='#6ee7b7'/>"
        f"</svg></div>"
        f"<span style='font-size:17px;font-weight:800;color:{t['t1']};letter-spacing:-.5px;'>A-LEMS</span>"
        f"<span style='font-size:8px;padding:1px 5px;border-radius:3px;font-weight:600;"
        f"background:{t['bg2']};color:{t['accent']};border:0.5px solid {t['brd2']};'>v2.1</span>"
        f"<div style='margin-left:auto;width:8px;height:8px;border-radius:50%;"
        f"background:{dot_clr};{dot_glow}flex-shrink:0;'></div>"
        f"</div>"
        f"<div style='font-size:8px;color:{t['t3']};text-transform:uppercase;letter-spacing:.13em;'>"
        f"AI Energy Measurement</div>"
        f"</div>",
        unsafe_allow_html=True)

    # Logo → overview
    if st.button("⌂  Home · Overview", key="nav_home", use_container_width=True):
        st.session_state["nav_section"] = None
        st.session_state["nav_page"]    = None
        st.rerun()


# ── Session banner ────────────────────────────────────────────────────────────
def _session_banner(t: dict):
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
            WHERE group_id=(SELECT group_id FROM experiments ORDER BY exp_id DESC LIMIT 1)
            GROUP BY group_id
        """)
        if not row or not row.get("group_id"):
            return
        total  = int(row.get("total_exps", 0))
        done   = int(row.get("done",       0))
        run_   = int(row.get("running",    0))
        fail_  = int(row.get("failed",     0))
        runs_d = int(row.get("runs_done",  0) or 0)
        runs_t = int(row.get("runs_total", 1) or 1)

        if run_:
            status, clr = "RUNNING",   STATUS_COLORS["running"]
        elif fail_:
            status, clr = "FAILED",    STATUS_COLORS["failed"]
        elif done == total:
            status, clr = "COMPLETED", STATUS_COLORS["completed"]
        else:
            status, clr = "PENDING",   STATUS_COLORS["pending"]

        short = row["group_id"].replace("session_", "").replace("_", " ", 1)[:18]
        pct   = int(runs_d / max(runs_t, 1) * 100)

        st.markdown(
            f"<div style='margin:8px 6px 4px;padding:9px 11px;"
            f"background:{t['bg2']};border:0.5px solid {clr}33;"
            f"border-left:2.5px solid {clr};border-radius:8px;"
            f"position:relative;overflow:hidden;'>"
            f"<div style='position:absolute;top:0;right:0;width:50px;height:50px;"
            f"background:radial-gradient(circle,{clr}10 0%,transparent 70%);pointer-events:none;'></div>"
            f"<div style='font-size:8px;font-weight:700;color:{clr};text-transform:uppercase;"
            f"letter-spacing:.12em;margin-bottom:2px;display:flex;align-items:center;gap:5px;'>"
            f"<span style='width:5px;height:5px;border-radius:50%;background:{clr};flex-shrink:0;'></span>"
            f"Active Session</div>"
            f"<div style='font-size:10px;color:{t['t1']};font-weight:500;margin-bottom:1px;'>{short}</div>"
            f"<div style='font-size:8px;color:{t['t3']};margin-bottom:5px;'>"
            f"{status} · {done}/{total} exps · {runs_d}/{runs_t} runs</div>"
            f"<div style='background:{t['bg3']};border-radius:2px;height:3px;'>"
            f"<div style='background:linear-gradient(90deg,{clr}99,{clr});"
            f"width:{pct}%;height:100%;border-radius:2px;'></div></div>"
            f"</div>",
            unsafe_allow_html=True)
    except Exception:
        pass


# ── Live Lab panel ────────────────────────────────────────────────────────────
def _live_lab(t: dict):
    conn     = get_conn()
    online   = conn.get("verified", False)
    _live    = _read_live_url()
    lab_live = _live.get("online", False)
    clr      = STATUS_COLORS["running"] if online else t["accent"]
    sub      = ("Connected · " + conn["url"].replace("https://", "")[:26]
                if online else
                ("🟢 Lab online — click Connect" if lab_live
                 else "Offline · analysis mode"))

    st.markdown(
        f"<div style='margin:4px 6px;padding:7px 11px;"
        f"background:{t['bg2']};border:0.5px solid {t['brd']};"
        f"border-left:2px solid {clr};border-radius:8px;'>"
        f"<div style='font-size:9px;font-weight:700;color:{clr};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:1px;'>"
        f"{'🟢' if online else '🔌'}  Live Lab</div>"
        f"<div style='font-size:8px;color:{t['t3']};'>{sub}</div>"
        f"</div>",
        unsafe_allow_html=True)

    if online:
        hclr = STATUS_COLORS["running"] if conn.get("harness") else STATUS_COLORS["pending"]
        st.markdown(
            f"<div style='font-size:8px;color:{hclr};padding:1px 10px 3px;'>"
            f"● {'Harness ready' if conn.get('harness') else 'Harness unavailable'}</div>",
            unsafe_allow_html=True)
        if st.button("⏏  Disconnect", key="nav_disconnect", use_container_width=True):
            disconnect()
            st.rerun()
    else:
        if "show_connect" not in st.session_state:
            st.session_state["show_connect"] = False
        if st.button("⚡  Connect to Live Lab", key="toggle_connect", use_container_width=True):
            st.session_state["show_connect"] = not st.session_state["show_connect"]
        if st.session_state["show_connect"]:
            st.markdown(
                f"<div style='font-size:8px;color:{t['t3']};line-height:1.6;margin-bottom:6px;'>"
                f"Run <code style='color:{t['accent']};'>tunnel_agent.py</code> on your lab machine.<br>"
                f"<b style='color:{t['t2']};'>URL changes each session.</b></div>",
                unsafe_allow_html=True)
            _au = _live.get("url",   "") if lab_live else ""
            _at = _live.get("token", "") if lab_live else ""
            if lab_live and _au:
                st.markdown(
                    f"<div style='font-size:8px;color:{STATUS_COLORS['running']};padding:0 0 4px;'>"
                    f"🟢 Auto-detected!</div>", unsafe_allow_html=True)
            _url = st.text_input("Lab URL",      value=_au, placeholder="https://xxxx.trycloudflare.com", key="conn_url")
            _tok = st.text_input("Access token", value=_at, placeholder="alems-xxxxxxxxxxxxxxxx", type="password", key="conn_tok")
            if st.button("🔗  Connect",  key="nav_connect", use_container_width=True):
                if not _url:   st.error("Enter the lab URL")
                elif not _tok: st.error("Enter the access token")
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


# ── Section nav — THE MAIN CHANGE: 11 clean buttons ──────────────────────────
def _nav(t: dict):
    """11 section buttons. Coloured dot + name. Active section highlighted."""
    active_section = st.session_state.get("nav_section")

    for section in SECTIONS:
        data   = SECTION_PAGES[section]
        accent = data["accent"]
        active = (section == active_section)

        # Wrap active item for CSS
        if active:
            st.markdown("<div class='alems-active'>", unsafe_allow_html=True)

        # Button label: coloured dot + section name
        # We can't put HTML in button labels — use unicode bullet styled via CSS
        label = f"  {section}"   # spaces reserved for ::before dot
        #_icons = {"COMMAND CENTRE":":material/terminal:","ENERGY & SILICON":":material/bolt:","AGENTIC INTELLIGENCE":":material/psychology:","DATA MOVEMENT":":material/swap_horiz:","SESSIONS & RUNS":":material/history:","RESEARCH & INSIGHTS":":material/biotech:","ENVIRONMENT":":material/eco:","DATA QUALITY":":material/fact_check:","SILICON LAB":":material/memory:","DEVELOPER TOOLS":":material/code:","SETTINGS":":material/settings:"}
        if st.button(label, key=f"sec_{section}", use_container_width=True):
            st.session_state["nav_section"] = section
            st.session_state["nav_page"]    = None   # always show landing first
            st.rerun()

        if active:
            st.markdown("</div>", unsafe_allow_html=True)

    # Inject dot colours via CSS targeted at each button's aria-label
    dot_css = ""
    for section in SECTIONS:
        accent = SECTION_PAGES[section]["accent"]
        safe   = section.replace("'", "\\'").replace("&", "\\&")
        dot_css += f"""
[data-testid="stSidebar"] button[aria-label="  {safe}"] p {{
    color: {accent}cc !important;
    text-align: left !important;
}}
[data-testid="stSidebar"] button[aria-label="{safe}"]::before {{
    display: none !important;
}}
[data-testid="stSidebar"] button[aria-label="  {safe}"]:hover {{
    border-left: 2px solid {accent}66 !important;
}}
[data-testid="stSidebar"] button[aria-label="  {safe}"]:hover p {{
    color: {accent} !important;
}}
"""

    # Active section gets full accent border
    active_sec = st.session_state.get("nav_section", "")
    if active_sec and active_sec in SECTION_PAGES:
        acc = SECTION_PAGES[active_sec]["accent"]
        safe_a = active_sec.replace("'", "\\'").replace("&", "\\&")
        dot_css += f"""
.alems-active [data-testid="stButton"] > button[aria-label="  {safe_a}"] {{
    border-left: 2px solid {acc} !important;
}}"""

        dot_css += """
[data-testid="stSidebar"] .stButton > button {
    -webkit-user-select: none;
}
[data-testid="stSidebar"] .stButton > button::after {
    display: none !important;
}
[data-testid="stSidebar"] .stButton > button p {
    pointer-events: none;
    text-align: left !important;
}
"""
    
    st.markdown(f"<style>{dot_css}</style>", unsafe_allow_html=True)


# ── Settings ──────────────────────────────────────────────────────────────────
def _settings(t: dict):
    dark = st.session_state.get("theme", "dark") == "dark"
    if st.button("☀  Light mode" if dark else "☾  Dark mode",
                 key="sidebar_theme_toggle", use_container_width=True):
        st.session_state["theme"] = "light" if dark else "dark"
        st.rerun()


# ── Footer ────────────────────────────────────────────────────────────────────
def _footer(t: dict):
    _divider(t)
    _h = [6,8,10,7,12,9,11,8,14,10,13,9,11,8,10,12,9,7,11,13,10,8,12,9,11,10,8,13]
    _c = [SECTION_ACCENTS["ENERGY & SILICON"], SECTION_ACCENTS["COMMAND CENTRE"],
          SECTION_ACCENTS["AGENTIC INTELLIGENCE"], SECTION_ACCENTS["DATA MOVEMENT"]]
    bars = "".join(
        f"<div style='flex:1;height:{h}px;background:{_c[i%4]};"
        f"border-radius:1px 1px 0 0;opacity:0.65;min-width:3px;'></div>"
        for i, h in enumerate(_h))
    st.markdown(
        f"<div style='display:flex;align-items:flex-end;gap:1px;"
        f"height:16px;margin:0 6px 6px;'>{bars}</div>",
        unsafe_allow_html=True)
    try:
        nr = q1("SELECT COUNT(*) AS n FROM runs").get("n", "—")
        ne = q1("SELECT COUNT(*) AS n FROM experiments").get("n", "—")
        ns = q1("SELECT COUNT(DISTINCT group_id) AS n FROM experiments").get("n", "—")
        st.markdown(
            f"<div style='display:flex;gap:14px;padding:0 8px 4px;'>"
            + "".join(
                f"<div><div style='font-weight:700;font-size:14px;"
                f"color:{t['accent']};line-height:1;'>{v}</div>"
                f"<div style='font-size:8px;text-transform:uppercase;"
                f"letter-spacing:.09em;color:{t['t3']};margin-top:1px;'>{l}</div></div>"
                for v, l in [(nr, "Runs"), (ne, "Exps"), (ns, "Sessions")]
            ) + "</div>"
            f"<div style='font-size:8px;color:{t['t3']};padding:0 8px 6px;'>"
            f"{DB_PATH.name} · RAPL · perf · psutil</div>",
            unsafe_allow_html=True)
    except Exception:
        pass
    if st.button("⟳  Refresh", key="nav_refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ── Entry point ───────────────────────────────────────────────────────────────
def render_sidebar() -> None:
    """Render sidebar. Sets nav_section + nav_page in session_state."""
    dark = st.session_state.get("theme", "dark") == "dark"
    t    = _tokens(dark)

    # Initialise state on first load
    if "nav_section" not in st.session_state:
        st.session_state["nav_section"] = None
    if "nav_page" not in st.session_state:
        st.session_state["nav_page"] = None
    if "nav_last" not in st.session_state:
        st.session_state["nav_last"] = {}

    with st.sidebar:
        st.markdown(_css(t), unsafe_allow_html=True)

        conn   = get_conn()
        online = conn.get("verified", False)

        _brand(t, online)
        _session_banner(t)
        _live_lab(t)

        st.markdown(
            f"<div style='height:0.5px;background:{t['brd']};margin:8px 0 4px;'></div>",
            unsafe_allow_html=True)

        _nav(t)

        st.markdown(
            f"<div style='height:0.5px;background:{t['brd']};margin:4px 0 6px;'></div>",
            unsafe_allow_html=True)

        _settings(t)
        _footer(t)
