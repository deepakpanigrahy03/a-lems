# gui/components/session_tree.py  — v2
# ─────────────────────────────────────────────────────────────────────────────
# 3-Level tree: SESSION → EXPERIMENT → RUN
# + Run Pulse chart (replaces Gantt)
#
# FIXES vs v1:
#   1. Duration uses datetime.now() not utcnow() — fixes 5h bug
#   2. Live rep count reads from runs table mid-experiment (written per-pair)
#   3. "No runs recorded yet" replaced with actual DB count
#   4. Run Pulse: dot per completed rep pair, color=tax ratio, size=energy
#   5. Infinite-run guard: _run_one resets stop flag per experiment
# ─────────────────────────────────────────────────────────────────────────────

import streamlit as st
from datetime import datetime
import plotly.graph_objects as go

from gui.db      import q, q1
from gui.helpers import _human_energy


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _uj_to_j(uj):
    return (uj or 0) / 1_000_000

def _ns_to_s(ns):
    return (ns or 0) / 1_000_000_000

def _fmt_dur(seconds: float) -> str:
    if seconds <= 0 or seconds > 86400:  return "—"   # guard > 1 day = bad timestamp
    if seconds < 60:   return f"{seconds:.1f}s"
    if seconds < 3600: return f"{seconds/60:.1f}m"
    return f"{seconds/3600:.1f}h"

def _fmt_energy(uj) -> str:
    j = _uj_to_j(uj)
    if j == 0:    return "—"
    if j < 0.001: return f"{j*1e6:.0f} µJ"
    if j < 1:     return f"{j*1000:.1f} mJ"
    return f"{j:.3f} J"

def _fmt_tokens(n) -> str:
    if not n:     return "—"
    if n >= 1000: return f"{n/1000:.1f}k"
    return str(int(n))

def _parse_dt(val) -> datetime | None:
    """Parse ISO datetime string — handles both 'T' and ' ' separators."""
    if not val or str(val) in ("None", ""):
        return None
    try:
        return datetime.fromisoformat(str(val).replace("Z", ""))
    except Exception:
        return None

def _dur_from_now(started_at_str) -> float:
    """Seconds since started_at using LOCAL time (not UTC)."""
    s = _parse_dt(started_at_str)
    if s is None:
        return 0.0
    return max((datetime.now() - s).total_seconds(), 0.0)

def _dur_between(started_at_str, completed_at_str) -> float:
    s = _parse_dt(started_at_str)
    e = _parse_dt(completed_at_str)
    if s is None or e is None:
        return 0.0
    return max((e - s).total_seconds(), 0.0)

_STATUS_CFG = {
    "completed":   ("●",  "#3b82f6"),
    "running":     ("🟢", "#22c55e"),
    "failed":      ("🔴", "#ef4444"),
    "error":       ("🔴", "#ef4444"),
    "pending":     ("🟡", "#f59e0b"),
    "not_started": ("○",  "#4b5563"),
}

def _st_icon_color(status: str):
    return _STATUS_CFG.get(str(status).lower(), ("○", "#4b5563"))

def _wf_badge(wf: str) -> str:
    if wf == "agentic":
        return ("<span style='background:#ef444422;border:1px solid #ef444466;"
                "border-radius:3px;padding:1px 5px;font-size:8px;color:#ef4444;"
                "font-family:monospace;'>A</span>")
    elif wf == "linear":
        return ("<span style='background:#22c55e22;border:1px solid #22c55e66;"
                "border-radius:3px;padding:1px 5px;font-size:8px;color:#22c55e;"
                "font-family:monospace;'>L</span>")
    return (f"<span style='background:#3d557022;border:1px solid #3d557066;"
            f"border-radius:3px;padding:1px 5px;font-size:8px;color:#7090b0;"
            f"font-family:monospace;'>{wf or '?'}</span>")

def _pill(label, color):
    return (f"<span style='background:{color}22;border:1px solid {color}55;"
            f"border-radius:4px;padding:1px 8px;margin-right:4px;"
            f"font-size:9px;color:{color};'>{label}</span>")

def _cell(label, value, color="#c8d8e8", label_color="#4b6080"):
    return (f"<span style='margin-right:14px;'>"
            f"<span style='font-size:8px;color:{label_color};text-transform:uppercase;"
            f"letter-spacing:.04em;'>{label} </span>"
            f"<span style='font-size:10px;color:{color};font-family:monospace;"
            f"font-weight:600;'>{value}</span></span>")


# ══════════════════════════════════════════════════════════════════════════════
# RUN PULSE CHART — replaces Gantt
# One swimlane per experiment. Each dot = one completed rep pair.
# Dot color = tax ratio (green→yellow→red). Dot size = agentic energy.
# Far more meaningful than a time bar for energy research.
# ══════════════════════════════════════════════════════════════════════════════

def _run_pulse_chart(group_id: str, key_suffix: str = ""):
    """
    Render the Run Pulse timeline for a session.
    Reads from runs + orchestration_tax_summary — live mid-experiment.
    """
    if not group_id:
        return

    # Load all run pairs for this session
    try:
        pairs = q(f"""
            SELECT
                e.exp_id,
                e.task_name,
                e.provider,
                e.status                          AS exp_status,
                rl.run_number,
                rl.total_energy_uj / 1e6          AS linear_j,
                ra.total_energy_uj / 1e6          AS agentic_j,
                CASE WHEN rl.total_energy_uj > 0
                     THEN CAST(ra.total_energy_uj AS REAL) / rl.total_energy_uj
                     ELSE 1.0 END                 AS tax_x,
                ra.duration_ns / 1e9              AS agentic_dur_s,
                rl.duration_ns / 1e9              AS linear_dur_s
            FROM orchestration_tax_summary ots
            JOIN runs rl ON ots.linear_run_id  = rl.run_id
            JOIN runs ra ON ots.agentic_run_id = ra.run_id
            JOIN experiments e ON rl.exp_id = e.exp_id
            WHERE e.group_id = '{group_id}'
            ORDER BY e.exp_id, rl.run_number
        """)
    except Exception:
        pairs = None

    # Also get experiments list for swimlane labels
    try:
        exps = q(f"""
            SELECT exp_id, task_name, provider, status,
                   runs_completed, runs_total
            FROM experiments
            WHERE group_id = '{group_id}'
            ORDER BY exp_id
        """)
    except Exception:
        return

    if exps is None or exps.empty:
        return

    fig = go.Figure()

    # Build one trace per experiment
    for _, exp in exps.iterrows():
        eid   = exp.exp_id
        label = f"exp_{eid} · {exp.provider} · {str(exp.task_name)[:14]}"
        exp_status = str(exp.status).lower()
        total_reps = int(exp.runs_total or 0)

        # Completed pairs for this experiment
        exp_pairs = (pairs[pairs.exp_id == eid]
                     if pairs is not None and not pairs.empty else None)

        if exp_pairs is not None and not exp_pairs.empty:
            x_vals  = exp_pairs.run_number.tolist()
            tax_vals = exp_pairs.tax_x.tolist()
            age_j   = exp_pairs.agentic_j.tolist()
            lin_j   = exp_pairs.linear_j.tolist()
            dur_s   = exp_pairs.agentic_dur_s.tolist()

            # Color by tax ratio
            dot_colors = []
            for tx in tax_vals:
                if tx >= 10:   dot_colors.append("#ef4444")
                elif tx >= 5:  dot_colors.append("#f59e0b")
                elif tx >= 2:  dot_colors.append("#38bdf8")
                else:          dot_colors.append("#22c55e")

            # Size by agentic energy (normalized, min 12 max 30)
            max_e = max(age_j) if age_j else 1
            dot_sizes = [max(12, min(30, 12 + (e / max(max_e, 0.001)) * 18))
                         for e in age_j]

            hover = [
                f"<b>Rep {x_vals[i]}</b><br>"
                f"Tax: <b>{tax_vals[i]:.2f}×</b><br>"
                f"Linear: {lin_j[i]:.3f} J<br>"
                f"Agentic: {age_j[i]:.3f} J<br>"
                f"Duration: {dur_s[i]:.1f}s"
                for i in range(len(x_vals))
            ]

            # Completed dots + connecting line
            fig.add_trace(go.Scatter(
                x=x_vals, y=[label] * len(x_vals),
                mode="markers+lines",
                marker=dict(
                    color=dot_colors,
                    size=dot_sizes,
                    line=dict(color="#1e2d45", width=1),
                    symbol="circle",
                ),
                line=dict(color="#1e2d45", width=1),
                hovertemplate="%{customdata}<extra></extra>",
                customdata=hover,
                showlegend=False,
                name=label,
            ))

            # Ghost dots for remaining reps
            done = len(x_vals)
            remaining = [r for r in range(done + 1, total_reps + 1)]
            if remaining:
                fig.add_trace(go.Scatter(
                    x=remaining, y=[label] * len(remaining),
                    mode="markers",
                    marker=dict(
                        color="#1e2d45",
                        size=10,
                        line=dict(color="#2d3f55", width=1),
                        symbol="circle-open",
                    ),
                    showlegend=False,
                    hovertemplate=f"Rep %{{x}} — pending<extra></extra>",
                ))

        else:
            # No pairs yet — show all as pending
            if total_reps > 0:
                fig.add_trace(go.Scatter(
                    x=list(range(1, total_reps + 1)),
                    y=[label] * total_reps,
                    mode="markers",
                    marker=dict(
                        color="#1e2d45", size=10,
                        line=dict(color="#2d3f55", width=1),
                        symbol="circle-open",
                    ),
                    showlegend=False,
                    hovertemplate="Pending<extra></extra>",
                ))

    # Legend annotation
    fig.add_annotation(
        x=1, y=1.08, xref="paper", yref="paper",
        text=(
            "<span style='color:#22c55e'>● &lt;2×</span>  "
            "<span style='color:#38bdf8'>● 2-5×</span>  "
            "<span style='color:#f59e0b'>● 5-10×</span>  "
            "<span style='color:#ef4444'>● &gt;10×</span>  "
            "<span style='color:#4b5563'>○ pending</span>"
            "  <i>dot size = agentic energy</i>"
        ),
        showarrow=False,
        font=dict(size=9, color="#7090b0"),
        align="right",
    )

    n_exps = len(exps)
    try:
        from gui.config import PL
        # Strip keys we set explicitly below to avoid "multiple values" TypeError
        _EXCL = {'margin','xaxis','yaxis','title','plot_bgcolor','paper_bgcolor','height'}
        _pl = {k: v for k, v in PL.items() if k not in _EXCL}
    except Exception:
        _pl = {}
    fig.update_layout(
        **_pl,
        height=max(100 + n_exps * 52, 160),
        margin=dict(l=10, r=10, t=36, b=24),
        title=dict(
            text="⚡ Run Pulse — each dot = one completed rep pair | color = tax ratio | size = energy",
            font=dict(size=9, color="#5a7090"), x=0),
        xaxis=dict(
            title="Repetition",
            tickmode="linear", tick0=1, dtick=1,
            gridcolor="#0f1520", zeroline=False,
        ),
        yaxis=dict(
            gridcolor="#0f1520", tickfont=dict(size=9),
        ),
        plot_bgcolor="#050810",
        paper_bgcolor="#050810",
    )

    _key = f"pulse_{group_id}_{n_exps}"
    if key_suffix:
        _key += f"_{key_suffix}"
    st.plotly_chart(fig, use_container_width=True, key=_key)


# ══════════════════════════════════════════════════════════════════════════════
# LEVEL 3 — RUN ROW
# ══════════════════════════════════════════════════════════════════════════════

def _render_run_row(run: dict, is_last: bool):
    prefix   = "└──" if is_last else "├──"
    dur_s    = _ns_to_s(run.get("duration_ns", 0))
    energy_j = _uj_to_j(run.get("total_energy_uj", 0))
    tokens   = run.get("total_tokens") or 0
    wf       = str(run.get("workflow_type") or "?")
    rnum     = run.get("run_number", run.get("run_id", "?"))
    carbon   = run.get("carbon_g") or 0

    e_clr = ("#ef4444" if energy_j > 500
             else "#f59e0b" if energy_j > 100 else "#22c55e")

    st.markdown(
        f"<div style='font-family:monospace;font-size:9px;line-height:2.0;"
        f"padding:0 0 0 28px;color:#3d5570;display:flex;align-items:center;gap:4px;'>"
        f"<span style='color:#1e2d40;min-width:28px;'>{prefix}</span>"
        f"{_wf_badge(wf)}"
        f"<span style='color:#5a7090;min-width:44px;margin-left:4px;'>rep {rnum}</span>"
        f"<span style='margin-left:8px;'>"
        f"{_cell('energy', _fmt_energy(run.get('total_energy_uj')), e_clr)}"
        f"{_cell('tokens', _fmt_tokens(tokens))}"
        f"{_cell('dur', _fmt_dur(dur_s))}"
        f"{_cell('CO₂', f'{carbon*1000:.2f}mg' if carbon else '—', '#a78bfa')}"
        f"</span>"
        f"</div>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# LEVEL 2 — EXPERIMENT BLOCK
# ══════════════════════════════════════════════════════════════════════════════

def _render_experiment(exp: dict, is_last: bool, expanded: bool, live_log=None):
    exp_id  = exp.get("exp_id", "?")
    status  = str(exp.get("status", "not_started")).lower()
    icon, clr = _st_icon_color(status)
    task    = str(exp.get("task_name", "?"))[:28]
    provider= str(exp.get("provider", "?"))
    model   = str(exp.get("model_name", "") or "").split("/")[-1][:20]
    prefix  = "└──" if is_last else "├──"

    # FIX: query runs table directly for live count mid-experiment
    try:
        run_counts = q(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN workflow_type='linear'  THEN 1 ELSE 0 END) as linear_done,
                SUM(CASE WHEN workflow_type='agentic' THEN 1 ELSE 0 END) as agentic_done
            FROM runs WHERE exp_id = {exp_id}
        """)
        if run_counts is not None and not run_counts.empty:
            rc = int(run_counts.iloc[0].get("agentic_done") or 0)  # pairs = agentic runs
        else:
            rc = int(exp.get("runs_completed") or 0)
    except Exception:
        rc = int(exp.get("runs_completed") or 0)

    rt = int((exp.get("runs_total") or 0))  // 2

    # If still 0 and running, try parsing from live log
    if rc == 0 and status == "running" and live_log:
        import re as _re
        for line in reversed(live_log[-30:]):
            m = _re.search(r'(?:rep|pair)\s+(\d+)\s*/\s*(\d+)', line.lower())
            if m:
                rc = int(m.group(1))
                rt = rt or int(m.group(2))
                break

    # FIX: duration using datetime.now() (local time, matches DB)
    dur_str = "—"
    try:
        s = _parse_dt(exp.get("started_at"))
        if s:
            if exp.get("completed_at"):
                e = _parse_dt(exp.get("completed_at"))
                secs = (e - s).total_seconds() if e else 0
            else:
                secs = (datetime.now() - s).total_seconds()
            dur_str = _fmt_dur(secs)
    except Exception:
        pass

    # Progress bar
    pct = int(rc / max(rt, 1) * 100)
    prog_bar = (
        f"<div style='display:inline-block;width:56px;height:5px;"
        f"background:#1e2d45;border-radius:3px;vertical-align:middle;"
        f"margin:0 4px;overflow:hidden;'>"
        f"<div style='background:{clr};width:{pct}%;height:100%;"
        f"border-radius:3px;'></div></div>"
    )

    header_txt = f"{prefix} {icon} exp_{exp_id}  {provider}  {task}  {status}  {rc}/{rt} pairs"

    with st.expander(header_txt, expanded=expanded):
        st.markdown(
            f"<div style='display:flex;align-items:center;gap:6px;"
            f"font-family:monospace;font-size:10px;flex-wrap:wrap;"
            f"padding:4px 0 6px;border-bottom:1px solid #0f1520;'>"
            f"<span style='font-size:12px;'>{icon}</span>"
            f"<span style='color:#c8d8e8;font-weight:700;'>exp_{exp_id}</span>"
            f"<span style='color:#3b82f6;'>{provider}</span>"
            f"<span style='color:#7090b0;'>{task}</span>"
            f"{'<span style=\"color:#4b6080;font-size:9px;\">'+model+'</span>' if model else ''}"
            f"<span style='background:{clr}18;border:1px solid {clr}44;"
            f"border-radius:3px;padding:1px 6px;font-size:9px;color:{clr};'>{status}</span>"
            f"<span style='color:#2d3f55;font-size:9px;'>{rc}/{rt} pairs</span>"
            f"{prog_bar}"
            f"<span style='color:#2d3f55;font-size:9px;'>{dur_str}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

        # Fetch runs from DB
        try:
            runs_df = q(f"""
                SELECT run_id, run_number, workflow_type, duration_ns,
                       total_energy_uj, total_tokens, carbon_g,
                       experiment_valid, prompt_tokens, completion_tokens,
                       api_latency_ms
                FROM runs
                WHERE exp_id = {exp_id}
                ORDER BY run_number, workflow_type
            """)
        except Exception:
            runs_df = None

        if runs_df is None or runs_df.empty:
            if status == "running":
                st.markdown(
                    "<div style='font-size:9px;color:#3d5570;padding:4px 0 2px;'>"
                    "⏳ Waiting for first rep to complete…</div>",
                    unsafe_allow_html=True)
            else:
                st.caption("No runs recorded yet.")
            return

        # Group by run_number → show linear+agentic pairs
        run_nums = sorted(runs_df["run_number"].unique())
        for rn_idx, rn in enumerate(run_nums):
            pair    = runs_df[runs_df["run_number"] == rn]
            linear  = pair[pair["workflow_type"] == "linear"]
            agentic = pair[pair["workflow_type"] == "agentic"]

            lin_e = _uj_to_j(linear.iloc[0]["total_energy_uj"])  if not linear.empty  else 0
            ag_e  = _uj_to_j(agentic.iloc[0]["total_energy_uj"]) if not agentic.empty else 0

            if lin_e > 0 and ag_e > 0:
                tax     = ag_e / lin_e
                tax_clr = ("#ef4444" if tax > 10
                           else "#f59e0b" if tax > 5
                           else "#38bdf8" if tax > 2
                           else "#22c55e")
                tax_str = f"{tax:.2f}×"
            else:
                tax_clr, tax_str = "#4b6080", "—"

            is_last_pair = (rn_idx == len(run_nums) - 1)
            pair_prefix  = "└──" if is_last_pair else "├──"

            st.markdown(
                f"<div style='padding:2px 0 0 12px;font-size:9px;"
                f"color:#3d5570;font-family:monospace;'>"
                f"<span style='color:#1e2d40;'>{pair_prefix} </span>"
                f"<span style='color:#5a7090;font-weight:600;'>rep {rn}</span>"
                f"<span style='color:#2d3f55;margin-left:8px;'>"
                f"L {_fmt_energy(linear.iloc[0]['total_energy_uj']) if not linear.empty else '—'}"
                f" → A {_fmt_energy(agentic.iloc[0]['total_energy_uj']) if not agentic.empty else '—'}"
                f"</span>"
                f"<span style='color:{tax_clr};font-weight:700;margin-left:6px;'>"
                f"tax {tax_str}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

            all_runs = pair.sort_values("workflow_type").to_dict("records")
            for ri, run in enumerate(all_runs):
                _render_run_row(run, is_last=(ri == len(all_runs) - 1))


# ══════════════════════════════════════════════════════════════════════════════
# LEVEL 1 — SESSION HEADER + FULL TREE
# ══════════════════════════════════════════════════════════════════════════════

def render_session_tree(group_id: str, expanded: bool = False, live_log=None, key_suffix: str = ""):
    """
    Render full 3-level tree + Run Pulse chart for a session.

    Args:
        group_id:  Session group identifier.
        expanded:  True = all nodes open (live view). False = collapsed (analysis).
        live_log:  Pass _store_get('log', []) for mid-run log parsing.
    """
    if not group_id:
        st.caption("No active session yet.")
        return

    try:
        exps = q(f"""
            SELECT exp_id, name, task_name, provider, model_name,
                   workflow_type, status, runs_completed, runs_total,
                   started_at, completed_at, optimization_enabled,
                   error_message
            FROM experiments
            WHERE group_id = '{group_id}'
            ORDER BY exp_id
        """)
    except Exception as e:
        st.error(f"DB error: {e}")
        return

    if exps is None or exps.empty:
        st.caption(f"No experiments found for session: {group_id}")
        return

    # ── Session-level summary ────────────────────────────────────────────────
    n_exps    = len(exps)
    n_done    = (exps["status"] == "completed").sum()
    n_run     = (exps["status"] == "running").sum()
    n_fail    = exps["status"].isin(["failed", "error"]).sum()
    n_pending = n_exps - n_done - n_run - n_fail

    # FIX: live run count from runs table, not experiments.runs_completed
    try:
        rc_row = q(f"""
            SELECT COUNT(DISTINCT r.run_id) as total_runs
            FROM runs r
            JOIN experiments e ON r.exp_id = e.exp_id
            WHERE e.group_id = '{group_id}'
              AND r.workflow_type = 'agentic'
        """)
        total_runs = int(rc_row.iloc[0].total_runs) if rc_row is not None and not rc_row.empty else 0
    except Exception:
        total_runs = int(exps["runs_completed"].fillna(0).sum())

    # FIX: session duration using datetime.now() (local time)
    ses_dur = "—"
    try:
        started_rows = exps[exps["started_at"].notna()]
        if not started_rows.empty:
            s = _parse_dt(started_rows.iloc[0]["started_at"])
            completed_rows = exps[exps["completed_at"].notna()]
            if not completed_rows.empty:
                e = _parse_dt(completed_rows.iloc[-1]["completed_at"])
                secs = (e - s).total_seconds() if e and s else 0
            else:
                secs = (datetime.now() - s).total_seconds() if s else 0
            ses_dur = _fmt_dur(secs)
    except Exception:
        pass

    if   n_run  > 0:       ses_status, ses_clr = "running",   "#22c55e"
    elif n_fail > 0:       ses_status, ses_clr = "partial",   "#f59e0b"
    elif n_done == n_exps: ses_status, ses_clr = "completed", "#3b82f6"
    else:                  ses_status, ses_clr = "pending",   "#4b5563"

    pills = ""
    if n_done:    pills += _pill(f"● {n_done} completed",  "#3b82f6")
    if n_run:     pills += _pill(f"🟢 {n_run} running",    "#22c55e")
    if n_fail:    pills += _pill(f"🔴 {n_fail} failed",    "#ef4444")
    if n_pending: pills += _pill(f"○ {n_pending} pending", "#4b5563")

    st.markdown(
        f"<div style='background:#050810;border:1px solid {ses_clr}44;"
        f"border-left:4px solid {ses_clr};border-radius:6px;"
        f"padding:10px 14px 8px;margin-bottom:6px;'>"
        f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:6px;'>"
        f"<span style='font-size:9px;font-weight:800;color:#3d5570;"
        f"text-transform:uppercase;letter-spacing:.12em;'>SESSION</span>"
        f"<span style='font-family:monospace;font-size:11px;color:#e8f0f8;"
        f"font-weight:700;'>{group_id}</span>"
        f"<span style='background:{ses_clr}22;border:1px solid {ses_clr}55;"
        f"border-radius:4px;padding:1px 8px;font-size:9px;color:{ses_clr};"
        f"font-weight:700;'>{ses_status}</span>"
        f"</div>"
        f"<div style='display:flex;align-items:center;gap:2px;flex-wrap:wrap;margin-bottom:6px;'>"
        f"{_cell('experiments', str(n_exps))}"
        f"{_cell('rep pairs done', str(total_runs))}"
        f"{_cell('duration', ses_dur)}"
        f"</div>"
        f"<div>{pills}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── Run Pulse chart ──────────────────────────────────────────────────────
    _run_pulse_chart(group_id, key_suffix=key_suffix)

    # ── Experiments ──────────────────────────────────────────────────────────
    for idx, (_, row) in enumerate(exps.iterrows()):
        _render_experiment(
            exp=row.to_dict(),
            is_last=(idx == len(exps) - 1),
            expanded=expanded,
            live_log=live_log,
        )
