"""
gui/pages/sessions.py  —  ⬡  Sessions
─────────────────────────────────────────────────────────────────────────────
Browse all sessions (group_ids) with experiment counts, run totals,
orchestration tax summary, and a click-through to Session Analysis.

Layout:
  Top: summary metrics bar (total sessions, total runs, avg tax, best session)
  Main: session cards grid — one card per group_id
  Click any card → deep-links to Execute Run Tab 3 with that session selected
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

from gui.config  import PL, STATUS_COLORS
from gui.db      import q, q1
from gui.pages.session_analysis import render_session_analysis


def render(ctx: dict):
    st.title("⬡ Sessions")
    st.caption("All recorded experiment sessions — click any to open its full analysis.")

    # ── Load all sessions from DB ─────────────────────────────────────────────
    try:
        sessions = q(
            "SELECT e.group_id, "
            "COUNT(DISTINCT e.exp_id) as n_exps, "
            "SUM(e.runs_completed) as n_runs, "
            "MIN(e.started_at) as started, "
            "MAX(e.completed_at) as completed, "
            "SUM(CASE WHEN e.status='completed' THEN 1 ELSE 0 END) as done, "
            "SUM(CASE WHEN e.status='running'   THEN 1 ELSE 0 END) as running, "
            "SUM(CASE WHEN e.status='failed'    THEN 1 ELSE 0 END) as failed "
            "FROM experiments e "
            "WHERE e.group_id IS NOT NULL "
            "GROUP BY e.group_id "
            "ORDER BY MAX(e.exp_id) DESC"
        )
    except Exception as e:
        st.error(f"Could not load sessions: {e}")
        return

    if sessions.empty:
        st.info("No sessions recorded yet. Run an experiment from Execute Run.")
        return

    # ── Enrich with tax data ──────────────────────────────────────────────────
    try:
        tax_by_session = q(
            "SELECT e.group_id, "
            "AVG(ots.tax_percent / 100.0) as avg_tax, "
            "MAX(ots.tax_percent / 100.0) as max_tax, "
            "COUNT(ots.comparison_id) as n_pairs "
            "FROM orchestration_tax_summary ots "
            "JOIN runs rl ON ots.linear_run_id = rl.run_id "
            "JOIN experiments e ON rl.exp_id = e.exp_id "
            "GROUP BY e.group_id"
        )
        sessions = sessions.merge(tax_by_session, on="group_id", how="left")
    except Exception:
        sessions["avg_tax"] = None
        sessions["max_tax"] = None
        sessions["n_pairs"]  = 0

    # ── Top summary bar ───────────────────────────────────────────────────────
    total_sessions = len(sessions)
    total_runs     = int(sessions.n_runs.sum() or 0)
    has_tax        = sessions.avg_tax.notna()
    avg_tax_all    = float(sessions[has_tax].avg_tax.mean()) if has_tax.any() else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sessions", total_sessions)
    c2.metric("Total Runs", total_runs)
    c3.metric("Avg Tax", f"{avg_tax_all:.1f}×" if avg_tax_all else "—")
    if has_tax.any():
        best = sessions[has_tax].loc[sessions[has_tax].avg_tax.idxmin()]
        short = str(best.group_id).replace("session_", "")[:13]
        c4.metric("Best Session", short, f"{float(best.avg_tax):.1f}× avg tax")

    st.divider()

    # ── Session selector ──────────────────────────────────────────────────────
    # Use a separate backing key (_gid) so card buttons can set it without
    # conflicting with the selectbox widget key (_sel).
    all_gids = sessions.group_id.tolist()

    # Initialise backing key
    if "sessions_active_gid" not in st.session_state:
        st.session_state["sessions_active_gid"] = all_gids[0] if all_gids else ""

    # If backing key holds a valid gid, use it as the selectbox default index
    _cur = st.session_state["sessions_active_gid"]
    _idx = all_gids.index(_cur) if _cur in all_gids else 0

    def _fmt(gid):
        row = sessions[sessions.group_id == gid].iloc[0]
        short = str(gid).replace("session_", "").replace("_", " ", 1)[:15]
        tax_s = f"  ·  avg {row.avg_tax:.1f}×" if pd.notna(row.get("avg_tax")) else ""
        return f"{short}  ({int(row.n_exps)} exps, {int(row.n_runs or 0)} runs{tax_s})"

    selected = st.selectbox(
        "Select session to inspect",
        all_gids,
        index=_idx,
        format_func=_fmt,
        key="sessions_page_sel",          # ← different key from backing store
    )
    # Keep backing store in sync with selectbox
    st.session_state["sessions_active_gid"] = selected

    # ── Session cards grid ────────────────────────────────────────────────────
    st.markdown("#### All Sessions")
    cols = st.columns(3)
    for i, (_, row) in enumerate(sessions.iterrows()):
        col = cols[i % 3]
        gid   = row.group_id
        short = str(gid).replace("session_", "").replace("_", " ", 1)[:15]

        # Status color
        if int(row.get("running", 0) or 0) > 0:   clr, badge = "#22c55e", "🟢 RUNNING"
        elif int(row.get("failed", 0) or 0) > 0:  clr, badge = "#ef4444", "🔴 PARTIAL FAIL"
        elif int(row.get("done",   0) or 0) == int(row.n_exps or 1): clr, badge = "#3b82f6", "● COMPLETE"
        else:                                       clr, badge = "#f59e0b", "🟡 PARTIAL"

        tax_str = f"{float(row.avg_tax):.1f}× avg tax" if pd.notna(row.get("avg_tax")) else "no tax data"

        with col:
            clicked = st.button(
                f"{short}\n{badge}  ·  {int(row.n_exps)} exps  ·  {tax_str}",
                key=f"sess_card_{i}",
                use_container_width=True,
            )
            if clicked:
                # Write to backing key only — never touch the widget key
                st.session_state["sessions_active_gid"] = gid
                st.rerun()

    st.divider()

    # ── Full analysis for selected session ────────────────────────────────────
    active = st.session_state.get("sessions_active_gid", selected)
    if active:
        render_session_analysis(active)
