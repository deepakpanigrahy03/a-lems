"""
gui/components/section_landing.py
─────────────────────────────────────────────────────────────────────────────
Generic section landing page — renders the card grid for any section.
Called by streamlit_app.py when nav_page is None.

Features:
  • Section hero: accent colour, icon, title, who, description
  • Resume chip: "↩ Resume: Energy Lab" if last_page exists for this section
  • Card grid: 3 columns, each card shows icon, label, desc, status badge
  • Blocked cards: show data gap reason instead of clickable card
  • Planned cards: shown greyed out, not clickable
  • Clicking a card sets nav_page → st.rerun()
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
from gui.config import SECTION_PAGES, SECTION_ACCENTS, EXISTS, NEW, PLANNED, BLOCKED


# ── Status badge config ───────────────────────────────────────────────────────
_BADGE = {
    EXISTS:  ("LIVE",    "#052e1a", "#4ade80"),
    NEW:     ("BUILD",   "#0c1f3a", "#60a5fa"),
    PLANNED: ("PLANNED", "#1a1028", "#c084fc"),
    BLOCKED: ("BLOCKED", "#2a0c0c", "#f87171"),
}


def render(section: str, last_page: str | None = None) -> None:
    """
    Render the landing page for a section.

    Args:
        section:   section name key from SECTION_PAGES
        last_page: page_id of the last visited page in this section (for resume chip)
    """
    if section not in SECTION_PAGES:
        st.error(f"Unknown section: {section}")
        return

    data   = SECTION_PAGES[section]
    accent = data["accent"]
    pages  = data["pages"]
    dark   = st.session_state.get("theme", "dark") == "dark"
    bg1    = "#111827" if dark else "#ffffff"
    bg2    = "#1f2937" if dark else "#f9fafb"
    bg3    = "#374151" if dark else "#e5e7eb"
    t1     = "#f1f5f9" if dark else "#1f2937"
    t2     = "#94a3b8" if dark else "#4b5563"
    t3     = "#475569" if dark else "#6b7280"
    brd    = "#1f2937" if dark else "#e5e7eb"

    # ── Hero header ───────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:24px 28px 20px;"
        f"background:linear-gradient(135deg,{accent}14,{accent}06);"
        f"border:1px solid {accent}33;border-radius:14px;margin-bottom:20px;'>"

        f"<div style='display:flex;align-items:center;gap:12px;margin-bottom:8px;'>"
        f"<span style='font-size:28px;line-height:1;'>{data['icon']}</span>"
        f"<div>"
        f"<div style='font-size:22px;font-weight:700;color:{t1};"
        f"font-family:IBM Plex Mono,monospace;letter-spacing:-.5px;'>"
        f"{section}</div>"
        f"<div style='font-size:11px;color:{accent};font-family:IBM Plex Mono,monospace;"
        f"text-transform:uppercase;letter-spacing:.12em;margin-top:2px;'>"
        f"{data['who']}</div>"
        f"</div></div>"

        f"<div style='font-size:13px;color:{t2};font-family:IBM Plex Mono,monospace;"
        f"line-height:1.6;max-width:680px;'>{data['description']}</div>"
        f"</div>",
        unsafe_allow_html=True)

    # ── Resume chip ───────────────────────────────────────────────────────────
    if last_page:
        from gui.config import PAGE_META
        lp_meta = PAGE_META.get(last_page, {})
        lp_label = lp_meta.get("label", last_page)
        lp_icon  = lp_meta.get("icon",  "▶")
        col_r, col_spacer = st.columns([3, 7])
        with col_r:
            if st.button(
                f"↩  Resume: {lp_icon} {lp_label}",
                key=f"resume_{section}",
                use_container_width=True,
            ):
                st.session_state["nav_page"] = last_page
                st.rerun()
        st.markdown("<div style='margin-bottom:8px;'></div>", unsafe_allow_html=True)

    # ── Card grid ─────────────────────────────────────────────────────────────
    # Build list of renderable pages
    buildable = [p for p in pages if p["status"] != BLOCKED]
    blocked   = [p for p in pages if p["status"] == BLOCKED]

    cols = st.columns(3)
    for i, page in enumerate(buildable):
        col = cols[i % 3]
        with col:
            _render_card(page, accent, dark, bg2, bg3, t1, t2, t3, brd)

    # Blocked pages in a separate row at the bottom
    if blocked:
        st.markdown(
            f"<div style='margin-top:16px;margin-bottom:6px;"
            f"font-size:9px;font-weight:700;text-transform:uppercase;"
            f"letter-spacing:.1em;color:{t3};'>Data gaps — blocked pages</div>",
            unsafe_allow_html=True)
        bcols = st.columns(3)
        for i, page in enumerate(blocked):
            with bcols[i % 3]:
                _render_blocked_card(page, dark, bg2, t3, brd)


def _render_card(page: dict, accent: str, dark: bool,
                 bg2: str, bg3: str, t1: str, t2: str, t3: str, brd: str) -> None:
    """Render a single clickable page card."""
    status    = page["status"]
    clickable = status in (EXISTS, NEW)
    disabled  = status == PLANNED

    badge_txt, badge_bg, badge_fg = _BADGE.get(status, ("", "#222", "#888"))

    # Card container
    opacity = "1.0" if not disabled else "0.5"
    cursor  = "pointer" if clickable else "default"

    st.markdown(
        f"<div style='border:1px solid {accent}33;border-radius:10px;"
        f"background:{bg2};padding:16px;margin-bottom:4px;"
        f"opacity:{opacity};cursor:{cursor};"
        f"border-top:3px solid {accent};'>"

        f"<div style='display:flex;align-items:center;justify-content:space-between;"
        f"margin-bottom:8px;'>"
        f"<span style='font-size:20px;'>{page['icon']}</span>"
        f"<span style='font-size:8px;padding:2px 6px;border-radius:3px;font-weight:700;"
        f"background:{badge_bg};color:{badge_fg};letter-spacing:.06em;'>{badge_txt}</span>"
        f"</div>"

        f"<div style='font-size:13px;font-weight:600;color:{t1};"
        f"font-family:IBM Plex Mono,monospace;margin-bottom:5px;'>"
        f"{page['label']}</div>"

        f"<div style='font-size:11px;color:{t2};font-family:IBM Plex Mono,monospace;"
        f"line-height:1.5;'>{page['desc']}</div>"
        f"</div>",
        unsafe_allow_html=True)

    if clickable:
        if st.button(
            f"→",
            key=f"card_{page['id']}",
            use_container_width=True,
        ):
            st.session_state["nav_page"] = page["id"]
            # Remember last page for resume chip
            if "nav_last" not in st.session_state:
                st.session_state["nav_last"] = {}
            section = page.get("section", "")
            if section:
                st.session_state["nav_last"][section] = page["id"]
            st.rerun()


def _render_blocked_card(page: dict, dark: bool,
                         bg2: str, t3: str, brd: str) -> None:
    """Render a blocked page card with data gap explanation."""
    badge_txt, badge_bg, badge_fg = _BADGE[BLOCKED]
    reason = page.get("blocked_reason", "Data not yet available.")

    st.markdown(
        f"<div style='border:1px solid #2a0c0c;border-radius:10px;"
        f"background:{bg2};padding:16px;margin-bottom:4px;opacity:0.6;"
        f"border-top:3px solid #ef4444;'>"

        f"<div style='display:flex;align-items:center;justify-content:space-between;"
        f"margin-bottom:8px;'>"
        f"<span style='font-size:20px;opacity:0.5;'>{page['icon']}</span>"
        f"<span style='font-size:8px;padding:2px 6px;border-radius:3px;font-weight:700;"
        f"background:{badge_bg};color:{badge_fg};letter-spacing:.06em;'>{badge_txt}</span>"
        f"</div>"

        f"<div style='font-size:13px;font-weight:600;color:{t3};"
        f"font-family:IBM Plex Mono,monospace;margin-bottom:5px;'>"
        f"{page['label']}</div>"

        f"<div style='font-size:10px;color:#f87171;font-family:IBM Plex Mono,monospace;"
        f"line-height:1.5;'>⚠ {reason}</div>"
        f"</div>",
        unsafe_allow_html=True)
