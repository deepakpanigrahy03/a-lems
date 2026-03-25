"""
gui/pages/hypotheses.py  —  💡  Hypothesis Tracker
─────────────────────────────────────────────────────────────────────────────
Research hypothesis tracker with full DB persistence.
Hypotheses are stored in the `hypotheses` table (created by db_migrations.py)
and survive restarts, reboots, and new sessions.

Statuses:
  open         — being investigated
  supported    — evidence confirms it
  rejected     — evidence contradicts it
  inconclusive — evidence is mixed or insufficient
─────────────────────────────────────────────────────────────────────────────
"""

import sqlite3
from datetime import datetime

import pandas as pd
import streamlit as st

from gui.config import DB_PATH, PL
from gui.db import q, q1

ACCENT = "#a78bfa"

STATUS_COLORS = {
    "open":         "#3b82f6",
    "supported":    "#22c55e",
    "rejected":     "#ef4444",
    "inconclusive": "#f59e0b",
}
STATUS_ICONS = {
    "open":         "◎",
    "supported":    "✓",
    "rejected":     "✗",
    "inconclusive": "~",
}


# ── DB helpers — direct sqlite3 so we can write (not cached read conn) ────────

def _load_all() -> pd.DataFrame:
    """Load all hypotheses ordered by updated_at desc."""
    return q("""
        SELECT hypothesis_id, title, description, status,
               evidence_for, evidence_against,
               key_run_id, key_exp_id,
               created_by, created_at, updated_at
        FROM hypotheses
        ORDER BY updated_at DESC
    """)


def _insert(title, description, status, evidence_for,
            evidence_against, key_run_id, key_exp_id, created_by) -> bool:
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=5)
        conn.execute("""
            INSERT INTO hypotheses
                (title, description, status, evidence_for,
                 evidence_against, key_run_id, key_exp_id, created_by,
                 created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            title, description, status, evidence_for,
            evidence_against,
            int(key_run_id) if key_run_id else None,
            int(key_exp_id) if key_exp_id else None,
            created_by,
            datetime.now().isoformat(),
            datetime.now().isoformat(),
        ))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Could not save hypothesis: {e}")
        return False


def _update(hypothesis_id, **kwargs) -> bool:
    """Update any fields on a hypothesis by id."""
    if not kwargs:
        return False
    kwargs["updated_at"] = datetime.now().isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values     = list(kwargs.values()) + [hypothesis_id]
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=5)
        conn.execute(
            f"UPDATE hypotheses SET {set_clause} WHERE hypothesis_id = ?",
            values,
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Could not update hypothesis: {e}")
        return False


def _delete(hypothesis_id: int) -> bool:
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=5)
        conn.execute(
            "DELETE FROM hypotheses WHERE hypothesis_id = ?",
            (hypothesis_id,),
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Could not delete hypothesis: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# MAIN RENDER
# ─────────────────────────────────────────────────────────────────────────────

def render(ctx: dict) -> None:

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px;'>"
        f"Hypothesis Tracker</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>"
        f"State research hypotheses · track supporting and contradicting evidence · "
        f"link to specific runs. Persisted to DB across sessions."
        f"</div></div>",
        unsafe_allow_html=True,
    )

    # ── Load all hypotheses ───────────────────────────────────────────────────
    df = _load_all()

    # ── Summary KPIs ──────────────────────────────────────────────────────────
    if not df.empty:
        total        = len(df)
        n_open       = int((df["status"] == "open").sum())
        n_supported  = int((df["status"] == "supported").sum())
        n_rejected   = int((df["status"] == "rejected").sum())
        n_inconc     = int((df["status"] == "inconclusive").sum())

        k1, k2, k3, k4, k5 = st.columns(5)
        for col, val, label, clr in [
            (k1, total,       "Total",        ACCENT),
            (k2, n_open,      "Open",         "#3b82f6"),
            (k3, n_supported, "Supported",    "#22c55e"),
            (k4, n_rejected,  "Rejected",     "#ef4444"),
            (k5, n_inconc,    "Inconclusive", "#f59e0b"),
        ]:
            with col:
                st.markdown(
                    f"<div style='padding:8px 12px;background:#111827;"
                    f"border:1px solid {clr}33;border-left:3px solid {clr};"
                    f"border-radius:8px;margin-bottom:12px;'>"
                    f"<div style='font-size:20px;font-weight:700;color:{clr};"
                    f"font-family:IBM Plex Mono,monospace;'>{val}</div>"
                    f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
                    f"letter-spacing:.08em;'>{label}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    # ── Tabs ─────────────────────────────────────────────────────────────────
    tab_list, tab_new = st.tabs(["📋  All hypotheses", "➕  Add new"])

    with tab_new:
        _render_add_form()

    with tab_list:
        _render_hypothesis_list(df)


def _render_add_form():
    """Form to create a new hypothesis."""
    accent = ACCENT

    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;'>"
        f"State a new hypothesis</div>",
        unsafe_allow_html=True,
    )

    with st.form("new_hypothesis_form", clear_on_submit=True):
        title = st.text_input(
            "Title *",
            placeholder="e.g. Agentic workflows have higher cache miss rates than linear",
            key="hyp_title",
        )
        description = st.text_area(
            "Description",
            placeholder="Explain the hypothesis in detail — what mechanism drives it?",
            height=80,
            key="hyp_desc",
        )

        col1, col2 = st.columns(2)
        with col1:
            status = st.selectbox(
                "Initial status",
                ["open", "supported", "rejected", "inconclusive"],
                key="hyp_status",
            )
            created_by = st.text_input(
                "Researcher", value="researcher", key="hyp_author"
            )
        with col2:
            key_run_id = st.number_input(
                "Key run_id (optional)", min_value=0, value=0,
                step=1, key="hyp_run_id",
                help="Link to a specific run that is most relevant",
            )
            key_exp_id = st.number_input(
                "Key exp_id (optional)", min_value=0, value=0,
                step=1, key="hyp_exp_id",
            )

        evidence_for = st.text_area(
            "Evidence FOR",
            placeholder="Observations, run_ids, or analysis that support this hypothesis",
            height=80, key="hyp_ev_for",
        )
        evidence_against = st.text_area(
            "Evidence AGAINST",
            placeholder="Observations or data that contradict or complicate this hypothesis",
            height=80, key="hyp_ev_against",
        )

        submitted = st.form_submit_button(
            "💾 Save hypothesis", use_container_width=True
        )

    if submitted:
        if not title.strip():
            st.error("Title is required.")
        else:
            ok = _insert(
                title=title.strip(),
                description=description.strip(),
                status=status,
                evidence_for=evidence_for.strip(),
                evidence_against=evidence_against.strip(),
                key_run_id=key_run_id if key_run_id > 0 else None,
                key_exp_id=key_exp_id if key_exp_id > 0 else None,
                created_by=created_by.strip() or "researcher",
            )
            if ok:
                st.success(f"✓ Hypothesis saved: '{title[:50]}'")
                st.rerun()


def _render_hypothesis_list(df: pd.DataFrame):
    """Render all hypotheses as cards with inline edit/status update."""
    accent = ACCENT

    if df.empty:
        st.markdown(
            f"<div style='padding:40px;text-align:center;"
            f"border:1px solid {accent}33;border-radius:12px;"
            f"background:{accent}08;'>"
            f"<div style='font-size:14px;color:{accent};"
            f"font-family:IBM Plex Mono,monospace;'>No hypotheses yet</div>"
            f"<div style='font-size:11px;color:#475569;margin-top:6px;'>"
            f"Go to ➕ Add new to state your first hypothesis.</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
        return

    # Filter controls
    fc1, fc2 = st.columns(2)
    with fc1:
        status_filter = st.selectbox(
            "Filter by status",
            ["all", "open", "supported", "rejected", "inconclusive"],
            key="hyp_filter_status",
        )
    with fc2:
        sort_by = st.selectbox(
            "Sort by",
            ["newest first", "oldest first", "status"],
            key="hyp_sort",
        )

    view = df.copy()
    if status_filter != "all":
        view = view[view["status"] == status_filter]
    if sort_by == "oldest first":
        view = view.sort_values("created_at")
    elif sort_by == "status":
        order = {"open": 0, "supported": 1, "inconclusive": 2, "rejected": 3}
        view["_sort"] = view["status"].map(order)
        view = view.sort_values("_sort")

    st.markdown(
        f"<div style='font-size:10px;color:#475569;margin-bottom:12px;'>"
        f"Showing {len(view)} of {len(df)} hypotheses</div>",
        unsafe_allow_html=True,
    )

    for _, row in view.iterrows():
        hid     = int(row["hypothesis_id"])
        status  = str(row.get("status", "open"))
        clr     = STATUS_COLORS.get(status, "#94a3b8")
        icon    = STATUS_ICONS.get(status, "◎")
        title   = str(row.get("title", ""))
        desc    = str(row.get("description") or "")
        ev_for  = str(row.get("evidence_for") or "")
        ev_ag   = str(row.get("evidence_against") or "")
        created = str(row.get("created_at") or "")[:16]
        updated = str(row.get("updated_at") or "")[:16]
        run_id  = row.get("key_run_id")
        exp_id  = row.get("key_exp_id")
        author  = str(row.get("created_by") or "researcher")

        with st.expander(f"{icon}  {title[:70]}", expanded=False):
            # Status badge row
            st.markdown(
                f"<div style='display:flex;align-items:center;gap:10px;"
                f"margin-bottom:10px;'>"
                f"<span style='font-size:10px;padding:2px 10px;border-radius:4px;"
                f"background:{clr}22;color:{clr};font-weight:700;border:1px solid {clr}44;'>"
                f"{icon} {status.upper()}</span>"
                f"<span style='font-size:10px;color:#475569;'>"
                f"by {author} · created {created} · updated {updated}</span>"
                + (
                    f"<span style='font-size:10px;color:#60a5fa;margin-left:auto;'>"
                    f"run #{int(run_id)}</span>"
                    if run_id and str(run_id) not in ("nan", "None") else ""
                )
                + (
                    f"<span style='font-size:10px;color:#a78bfa;margin-left:4px;'>"
                    f"exp #{int(exp_id)}</span>"
                    if exp_id and str(exp_id) not in ("nan", "None") else ""
                )
                + "</div>",
                unsafe_allow_html=True,
            )

            if desc:
                st.markdown(
                    f"<div style='font-size:11px;color:#94a3b8;"
                    f"line-height:1.6;margin-bottom:10px;'>{desc}</div>",
                    unsafe_allow_html=True,
                )

            # Evidence columns
            ev_col1, ev_col2 = st.columns(2)
            with ev_col1:
                st.markdown(
                    f"<div style='font-size:10px;color:#22c55e;font-weight:600;"
                    f"text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                    f"Evidence for</div>"
                    f"<div style='font-size:11px;color:#94a3b8;line-height:1.6;"
                    f"padding:8px 10px;background:#052e1a22;border-radius:6px;"
                    f"min-height:40px;'>"
                    f"{ev_for if ev_for else '<span style=\"color:#334155;\">None recorded</span>'}"
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with ev_col2:
                st.markdown(
                    f"<div style='font-size:10px;color:#ef4444;font-weight:600;"
                    f"text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                    f"Evidence against</div>"
                    f"<div style='font-size:11px;color:#94a3b8;line-height:1.6;"
                    f"padding:8px 10px;background:#2a0c0c22;border-radius:6px;"
                    f"min-height:40px;'>"
                    f"{ev_ag if ev_ag else '<span style=\"color:#334155;\">None recorded</span>'}"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            # ── Inline edit controls ──────────────────────────────────────────
            st.markdown(
                "<div style='height:8px;'></div>",
                unsafe_allow_html=True,
            )
            ac1, ac2, ac3, ac4 = st.columns([2, 2, 1, 1])

            with ac1:
                new_status = st.selectbox(
                    "Update status",
                    ["open", "supported", "rejected", "inconclusive"],
                    index=["open","supported","rejected","inconclusive"].index(status),
                    key=f"hyp_new_status_{hid}",
                )
            with ac2:
                new_evidence_for = st.text_input(
                    "Add evidence for",
                    value=ev_for,
                    key=f"hyp_ev_for_{hid}",
                )
            with ac3:
                st.markdown("<div style='margin-top:24px;'></div>", unsafe_allow_html=True)
                if st.button("💾 Save", key=f"hyp_save_{hid}", use_container_width=True):
                    new_ev_ag = st.session_state.get(f"hyp_ev_ag_{hid}", ev_ag)
                    _update(
                        hid,
                        status=new_status,
                        evidence_for=new_evidence_for,
                        evidence_against=new_ev_ag,
                    )
                    st.success("Updated.")
                    st.rerun()

            with ac4:
                st.markdown("<div style='margin-top:24px;'></div>", unsafe_allow_html=True)
                if st.button("🗑 Delete", key=f"hyp_del_{hid}", use_container_width=True):
                    _delete(hid)
                    st.rerun()

            # Evidence against inline edit
            new_ev_ag = st.text_input(
                "Add evidence against",
                value=ev_ag,
                key=f"hyp_ev_ag_{hid}",
            )
