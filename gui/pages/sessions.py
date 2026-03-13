"""
gui/pages/sessions.py
─────────────────────────────────────────────────────────────────────────────
FIXES in this version:
  Issue 4: Sessions tab replaced from all-boxes grid → clean collapsible list
           Each session is an expander with metadata visible at a glance.
           Clicking opens full details inline — no navigation away.
           Scrollable because it's a normal vertical list, not a box grid.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
from datetime import datetime

from gui.db      import q, q1
from gui.helpers import fl


def _parse_dt(val):
    if not val or str(val) in ("None",""):
        return None
    try:
        return datetime.fromisoformat(str(val).replace("Z",""))
    except Exception:
        return None


def _fmt_dur(seconds: float) -> str:
    if seconds <= 0 or seconds > 86400: return "—"
    if seconds < 60:   return f"{seconds:.1f}s"
    if seconds < 3600: return f"{seconds/60:.1f}m"
    return f"{seconds/3600:.1f}h"


def _session_row(group_id: str, n_exps: int, n_runs: int,
                 n_done: int, n_fail: int, n_running: int,
                 latest: str, idx: int):
    """Render one session as a collapsible expander row."""

    # Status pill
    if n_running > 0:
        status, clr = "running",   "#22c55e"
    elif n_fail > 0:
        status, clr = "partial",   "#f59e0b"
    elif n_done == n_exps and n_exps > 0:
        status, clr = "completed", "#3b82f6"
    else:
        status, clr = "pending",   "#4b5563"

    # Format timestamp
    try:
        ts = datetime.fromisoformat(str(latest)).strftime("%Y-%m-%d  %H:%M") if latest else "—"
    except Exception:
        ts = str(latest)[:16] if latest else "—"

    header = (
        f"{status.upper()}  ·  {group_id}  ·  "
        f"{n_exps} exp{'s' if n_exps != 1 else ''}  ·  "
        f"{n_runs} run{'s' if n_runs != 1 else ''}  ·  {ts}"
    )

    with st.expander(header, expanded=(idx == 0)):
        # ── Session summary banner ──────────────────────────────────────────
        st.markdown(
            f"<div style='background:#050810;border:1px solid {clr}44;"
            f"border-left:4px solid {clr};border-radius:6px;"
            f"padding:10px 14px;margin-bottom:10px;'>"
            f"<div style='display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:6px;'>"
            f"<span style='font-family:monospace;font-size:11px;color:#e8f0f8;font-weight:700;'>{group_id}</span>"
            f"<span style='background:{clr}22;border:1px solid {clr}55;border-radius:4px;"
            f"padding:1px 8px;font-size:9px;color:{clr};font-weight:700;'>{status}</span>"
            f"</div>"
            f"<div style='font-size:10px;color:#5a7090;display:flex;gap:20px;flex-wrap:wrap;'>"
            f"<span>🔬 <b style='color:#7090b0'>{n_exps}</b> experiments</span>"
            f"<span>▶ <b style='color:#7090b0'>{n_runs}</b> runs</span>"
            f"<span>✅ <b style='color:#3b82f6'>{n_done}</b> done</span>"
            f"{'<span>🟢 <b style=\"color:#22c55e\">'+str(n_running)+'</b> running</span>' if n_running else ''}"
            f"{'<span>🔴 <b style=\"color:#ef4444\">'+str(n_fail)+'</b> failed</span>' if n_fail else ''}"
            f"<span>🕐 {ts}</span>"
            f"</div></div>",
            unsafe_allow_html=True,
        )

        # ── Experiments table ───────────────────────────────────────────────
        try:
            exps = q(f"""
                SELECT exp_id, task_name, provider, status,
                       started_at, completed_at
                FROM experiments
                WHERE group_id = '{group_id}'
                ORDER BY exp_id
            """)
        except Exception:
            exps = None

        # Issue 2: get real run counts from runs table
        try:
            # FIX Issue 2: COUNT(DISTINCT run_number) = rep pairs done
            # Each rep writes one linear + one agentic run with the same run_number
            # So DISTINCT run_number = number of completed pairs (what X/4 should show)
            run_counts = q(f"""
                SELECT exp_id,
                       COUNT(DISTINCT run_number) AS pairs_done
                FROM runs
                WHERE exp_id IN (
                    SELECT exp_id FROM experiments WHERE group_id = '{group_id}'
                )
                GROUP BY exp_id
            """)
            rc_map = {}
            if run_counts is not None and not run_counts.empty:
                rc_map = {int(row.exp_id): int(row.pairs_done or 0)
                          for _, row in run_counts.iterrows()}
        except Exception:
            rc_map = {}

        if exps is not None and not exps.empty:
            rows_html = ""
            for _, exp in exps.iterrows():
                eid    = int(exp.exp_id)
                est    = str(exp.get("status","")).lower()
                s_clr  = {"completed":"#3b82f6","running":"#22c55e",
                           "failed":"#ef4444","error":"#ef4444"}.get(est,"#4b5563")

                # Duration
                s = _parse_dt(exp.get("started_at"))
                e = _parse_dt(exp.get("completed_at"))
                if s and e:
                    dur = _fmt_dur((e - s).total_seconds())
                elif s:
                    dur = _fmt_dur((datetime.now() - s).total_seconds()) + " ⏳"
                else:
                    dur = "—"

                pairs_done = rc_map.get(eid, 0)

                rows_html += (
                    f"<tr style='border-bottom:1px solid #0f1520;'>"
                    f"<td style='padding:6px 8px;font-size:10px;color:#5a7090;"
                    f"font-family:monospace;'>exp_{eid}</td>"
                    f"<td style='padding:6px 8px;font-size:10px;color:#c8d8e8;'>"
                    f"{str(exp.get('task_name','?'))[:28]}</td>"
                    f"<td style='padding:6px 8px;font-size:10px;color:#3b82f6;'>"
                    f"{exp.get('provider','?')}</td>"
                    f"<td style='padding:6px 8px;'>"
                    f"<span style='background:{s_clr}22;border:1px solid {s_clr}55;"
                    f"border-radius:3px;padding:1px 6px;font-size:9px;color:{s_clr};'>"
                    f"{est}</span></td>"
                    f"<td style='padding:6px 8px;font-size:10px;color:#7090b0;"
                    f"font-family:monospace;'>{pairs_done} pairs</td>"
                    f"<td style='padding:6px 8px;font-size:9px;color:#4b6080;"
                    f"font-family:monospace;'>{dur}</td>"
                    f"</tr>"
                )

            st.markdown(
                "<div style='background:#07090f;border:1px solid #1e2d45;"
                "border-radius:6px;overflow:hidden;'>"
                "<table style='width:100%;border-collapse:collapse;'>"
                "<thead><tr style='background:#0a0e1a;border-bottom:1px solid #1e2d45;'>"
                "<th style='padding:6px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>Exp</th>"
                "<th style='padding:6px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>Task</th>"
                "<th style='padding:6px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>Provider</th>"
                "<th style='padding:6px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>Status</th>"
                "<th style='padding:6px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>Runs</th>"
                "<th style='padding:6px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>Duration</th>"
                f"</tr></thead><tbody>{rows_html}</tbody></table></div>",
                unsafe_allow_html=True,
            )
        else:
            st.caption("No experiments found for this session.")

        # ── Quick actions ───────────────────────────────────────────────────
        st.markdown("")
        c1, c2 = st.columns([1, 4])
        if c1.button("📊 Full Analysis", key=f"sess_ana_{group_id}_{idx}",
                     use_container_width=True):
            st.session_state["sessions_open_gid"] = group_id
            st.rerun()


def render(ctx: dict):
    """
    Sessions page — clean collapsible list, no box grid.
    Each row = one expander. Scrollable. Click to open inline.
    """
    st.title("Sessions")

    # ── If a session was clicked for full analysis, show it ─────────────────
    open_gid = st.session_state.get("sessions_open_gid")
    if open_gid:
        if st.button("← Back to sessions list", key="sess_back"):
            del st.session_state["sessions_open_gid"]
            st.rerun()
        else:
            try:
                from gui.pages.session_analysis import render_session_analysis
                render_session_analysis(open_gid)
            except Exception as e:
                st.error(f"Analysis failed: {e}")
            return

    # ── Filters ──────────────────────────────────────────────────────────────
    col_f1, col_f2, col_f3 = st.columns([2, 1, 1])
    with col_f1:
        search = st.text_input("🔍 Filter by session ID or task",
                               placeholder="group_id or task name…",
                               key="sess_search")
    with col_f2:
        status_filter = st.selectbox("Status", ["All","completed","running","partial","pending"],
                                     key="sess_status_filt")
    with col_f3:
        limit = st.number_input("Max sessions", 10, 200, 50, step=10, key="sess_limit")

    # ── Load sessions ─────────────────────────────────────────────────────────
    try:
        sessions = q(f"""
            SELECT
                e.group_id,
                COUNT(DISTINCT e.exp_id)                                    AS n_exps,
                COUNT(DISTINCT r.run_number || '_' || CAST(e.exp_id AS TEXT)) AS n_runs,
                SUM(CASE WHEN e.status='completed' THEN 1 ELSE 0 END)      AS n_done,
                SUM(CASE WHEN e.status IN ('failed','error') THEN 1 ELSE 0 END) AS n_fail,
                SUM(CASE WHEN e.status='running' THEN 1 ELSE 0 END)        AS n_running,
                MAX(e.created_at)                                           AS latest
            FROM experiments e
            LEFT JOIN runs r ON r.exp_id = e.exp_id
            GROUP BY e.group_id
            ORDER BY MAX(e.exp_id) DESC
            LIMIT {int(limit)}
        """)
    except Exception as e:
        st.error(f"Failed to load sessions: {e}")
        return

    if sessions is None or sessions.empty:
        st.markdown(
            "<div style='text-align:center;padding:60px 0;'>"
            "<div style='font-size:48px;margin-bottom:16px;'>📭</div>"
            "<div style='font-size:16px;color:#4b6080;font-weight:600;'>No sessions yet</div>"
            "<div style='font-size:12px;color:#3d5570;margin-top:6px;'>"
            "Run an experiment to see sessions here</div></div>",
            unsafe_allow_html=True)
        return

    # Apply filters
    if search:
        mask = sessions.group_id.str.contains(search, case=False, na=False)
        sessions = sessions[mask]

    if status_filter != "All":
        def _row_status(row):
            if row.n_running > 0:   return "running"
            if row.n_fail > 0:      return "partial"
            if row.n_done == row.n_exps and row.n_exps > 0: return "completed"
            return "pending"
        sessions = sessions[sessions.apply(_row_status, axis=1) == status_filter]

    if sessions.empty:
        st.info("No sessions match the current filter.")
        return

    # ── Summary metrics row ───────────────────────────────────────────────────
    total_sessions = len(sessions)
    total_runs     = int(sessions.n_runs.sum())
    running_now    = int((sessions.n_running > 0).sum())

    m1, m2, m3 = st.columns(3)
    m1.metric("Sessions", total_sessions)
    m2.metric("Total Runs", total_runs)
    m3.metric("Currently Running", running_now,
              delta="active" if running_now > 0 else None)

    st.divider()

    # ── Session list — collapsible rows (Issue 4 fix) ─────────────────────────
    for idx, row in sessions.iterrows():
        _session_row(
            group_id  = str(row.group_id),
            n_exps    = int(row.n_exps or 0),
            n_runs    = int(row.n_runs or 0),
            n_done    = int(row.n_done or 0),
            n_fail    = int(row.n_fail or 0),
            n_running = int(row.n_running or 0),
            latest    = str(row.latest or ""),
            idx       = idx,
        )
