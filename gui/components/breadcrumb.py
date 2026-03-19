"""
gui/components/breadcrumb.py
─────────────────────────────────────────────────────────────────────────────
Breadcrumb rendered at the top of every page (except Overview).

Shows:  ← ENERGY & SILICON  /  Energy Lab

Clicking the section name → sets nav_page = None → section landing.
─────────────────────────────────────────────────────────────────────────────
"""

import streamlit as st

from gui.config import PAGE_META, PAGE_TO_SECTION, SECTION_ACCENTS


def render(page_id: str) -> None:
    """Render breadcrumb for the given page_id. Call at top of every page."""
    section = PAGE_TO_SECTION.get(page_id)
    if not section:
        return

    page_meta = PAGE_META.get(page_id, {})
    page_label = page_meta.get("label", page_id)
    accent = SECTION_ACCENTS.get(section, "#3b82f6")
    dark = st.session_state.get("theme", "dark") == "dark"
    t3 = "#475569" if dark else "#6b7280"

    col_back, col_spacer = st.columns([2, 8])
    with col_back:
        if st.button(
            f"← {section}",
            key=f"breadcrumb_back_{page_id}",
            use_container_width=True,
        ):
            st.session_state["nav_page"] = None
            st.rerun()

    st.markdown(
        f"<div style='font-size:10px;color:{t3};font-family:IBM Plex Mono,monospace;"
        f"margin-bottom:12px;margin-top:-8px;padding-left:4px;'>"
        f"<span style='color:{accent};'>{section}</span>"
        f"<span style='margin:0 6px;opacity:.4;'>/</span>"
        f"<span>{page_label}</span>"
        f"</div>",
        unsafe_allow_html=True,
    )
