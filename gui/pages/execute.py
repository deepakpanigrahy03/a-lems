# gui/pages/execute.py  — v5  (Phase 3)
# ─────────────────────────────────────────────────────────────────────────────
# KEY FIXES vs v4:
#   1. Tasks loaded exclusively from config/tasks.yaml (no hardcoded fallbacks
#      beyond a DB query fallback if YAML is genuinely missing).
#   2. Tab-switch persistence: live execution state is stored in session_state
#      OUTSIDE the tab block, so switching to Overview and back never loses it.
#      The live view is rendered at the TOP LEVEL of render(), not inside tab2.
#   3. Session tree with color-coded bucket-wise status is shown during live
#      execution (was missing in v4 — it was defined but not wired up correctly).
# ─────────────────────────────────────────────────────────────────────────────

import math, subprocess, time as _time, re as _re, threading, os, signal
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.config     import PROJECT_ROOT, LIVE_API, WF_COLORS, PL, \
                           INSIGHTS_RULES, DASHBOARD_CFG, STATUS_COLORS, STATUS_ICONS
from gui.connection import get_conn, api_post, api_get
from gui.db         import q, q1
from gui.helpers    import fl, _human_energy, _human_water, _human_carbon

try:
    import requests as _req
    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False

try:
    import yaml as _yaml
    _YAML_OK = True
except ImportError:
    _YAML_OK = False


# ══════════════════════════════════════════════════════════════════════════════
# THREAD-SAFE SHARED STORE
# Background threads CANNOT access st.session_state (missing ScriptRunContext).
# Thread writes to _STORE. render()/_init_state() copies it into session_state.
# ══════════════════════════════════════════════════════════════════════════════
import threading as _threading
_STORE_LOCK = _threading.Lock()
_STORE: dict = {
    "running":     False,
    "done":        False,
    "phase":       "idle",
    "progress":    0.0,
    "log":         [],
    "metrics":     {},
    "result_rows": [],
    "group_id":    "",
    "run_record":  None,
    "sessions":    [],
    "queue":       [],
    "saved":       [],
    "stop":        False,   # set True by UI to kill thread loop
    "current_cmd": "",      # human-readable description of what is running
}

def _store_get(key, default=None):
    with _STORE_LOCK:
        return _STORE.get(key, default)

def _store_set(key, value):
    with _STORE_LOCK:
        _STORE[key] = value

def _store_append(key, value):
    with _STORE_LOCK:
        _STORE.setdefault(key, []).append(value)

def _store_log(line):
    with _STORE_LOCK:
        _STORE["log"].append(line)
        if len(_STORE["log"]) > 200:
            _STORE["log"] = _STORE["log"][-200:]


# ── Config shortcuts ──────────────────────────────────────────────────────────
_STUCK_MINS    = DASHBOARD_CFG.get("stuck_run", {}).get("threshold_minutes", 30)
_KILL_ON_RESET = DASHBOARD_CFG.get("stuck_run", {}).get("kill_process", True)
_QUEUE_FILE    = PROJECT_ROOT / DASHBOARD_CFG.get("queue", {}).get(
                     "persist_file", "config/queue_state.yaml")
_MAX_LOG       = DASHBOARD_CFG.get("live", {}).get("max_log_lines", 200)
_AUTO_SWITCH   = DASHBOARD_CFG.get("live", {}).get("auto_switch_to_analysis", True)


# ══════════════════════════════════════════════════════════════════════════════
# SESSION STATE — initialise all keys once
# ══════════════════════════════════════════════════════════════════════════════

def _init_state():
    """Sync _STORE (thread-written) → session_state (UI-read) on every rerun."""
    with _STORE_LOCK:
        snap = dict(_STORE)
    # Thread-owned keys: always overwrite session_state with latest _STORE value
    for _k in ("running","done","phase","progress","log","metrics",
               "result_rows","group_id","run_record","sessions"):
        st.session_state[f"ex_{_k}"] = snap.get(_k)
    # UI-owned keys: init from _STORE only if not yet in session_state
    for _k in ("queue","saved"):
        if f"ex_{_k}" not in st.session_state:
            st.session_state[f"ex_{_k}"] = snap.get(_k, [])
        # Keep _STORE in sync so thread can drain the queue
        with _STORE_LOCK:
            _STORE[_k] = list(st.session_state[f"ex_{_k}"])
    # Legacy key expected by some render paths
    if "ex_thread" not in st.session_state:
        st.session_state["ex_thread"] = None


# ══════════════════════════════════════════════════════════════════════════════
# QUEUE PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════

def _save_queue():
    if not _YAML_OK:
        return
    try:
        _QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_QUEUE_FILE, "w") as f:
            _yaml.dump({"queue": _store_get("queue", [])}, f)
    except Exception:
        pass


def _load_queue():
    if not _YAML_OK or not _QUEUE_FILE.exists():
        return
    try:
        data = _yaml.safe_load(_QUEUE_FILE.read_text()) or {}
        loaded = data.get("queue", [])
        with _STORE_LOCK:
            if loaded and not _STORE.get("queue"):
                _STORE["queue"] = loaded
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# TASK LOADER — config/tasks.yaml is the SINGLE source of truth
# ══════════════════════════════════════════════════════════════════════════════

def _load_tasks() -> tuple:
    """
    Load task IDs, category map, and display-name map from config/tasks.yaml.
    Falls back to DB distinct task_names only if the YAML file is missing/broken.
    Returns: (task_ids: list, cat_map: {id→category}, name_map: {id→display_name})
    """
    if _YAML_OK:
        yaml_path = PROJECT_ROOT / "config" / "tasks.yaml"
        if yaml_path.exists():
            try:
                raw   = _yaml.safe_load(yaml_path.read_text()) or {}
                tasks = raw.get("tasks", [])
                ids   = [t["id"] for t in tasks if "id" in t]
                cats  = {t["id"]: t.get("category", "") for t in tasks}
                names = {t["id"]: t.get("name", t["id"]) for t in tasks}
                if ids:
                    return ids, cats, names
            except Exception as e:
                st.warning(f"⚠️ Could not parse config/tasks.yaml: {e}")

    # Fallback: read from DB (no hardcoded task names ever)
    try:
        df = q("SELECT DISTINCT task_name FROM experiments "
               "WHERE task_name IS NOT NULL ORDER BY task_name")
        ids = df.task_name.tolist() if not df.empty else []
    except Exception:
        ids = []

    if not ids:
        st.error("❌ No tasks found in config/tasks.yaml and DB is empty. "
                 "Please create config/tasks.yaml.")
        ids = []

    return ids, {i: "" for i in ids}, {i: i for i in ids}


# ══════════════════════════════════════════════════════════════════════════════
# STUCK RUN DETECTOR
# ══════════════════════════════════════════════════════════════════════════════

def _show_stuck_runs():
    try:
        stuck = q(f"""
            SELECT exp_id, task_name, provider, group_id,
                   started_at, runs_completed, runs_total
            FROM experiments
            WHERE status = 'running'
              AND started_at IS NOT NULL
              AND (julianday('now') - julianday(started_at)) * 1440 > {_STUCK_MINS}
            ORDER BY exp_id
        """)
    except Exception:
        return
    if stuck.empty:
        return
    for _, row in stuck.iterrows():
        elapsed = ""
        try:
            started = datetime.fromisoformat(str(row.started_at))
            mins    = int((_time.time() - started.timestamp()) / 60)
            elapsed = f"{mins} min ago"
        except Exception:
            elapsed = str(row.started_at)
        st.markdown(
            f"<div style='background:#1a0508;border:1px solid #ef4444;"
            f"border-left:4px solid #ef4444;border-radius:5px;"
            f"padding:10px 14px;margin-bottom:8px;'>"
            f"<div style='font-size:11px;font-weight:700;color:#ef4444;margin-bottom:4px;'>"
            f"🚨 Stuck Experiment Detected</div>"
            f"<div style='font-size:10px;color:#c8d8e8;'>"
            f"exp_id={row.exp_id} · {row.task_name} · {row.provider} · "
            f"started {elapsed} · {row.runs_completed}/{row.runs_total} runs</div>"
            f"<div style='font-size:9px;color:#7090b0;margin-top:2px;'>"
            f"group: {row.group_id}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
        if st.button(f"⚡ Force Reset exp_{row.exp_id}", key=f"reset_{row.exp_id}"):
            _force_reset_experiment(int(row.exp_id))
            st.success(f"exp_{row.exp_id} marked as error. Refresh to confirm.")
            st.rerun()


def _force_reset_experiment(exp_id: int):
    import sqlite3
    try:
        conn = sqlite3.connect(str(PROJECT_ROOT / "data" / "experiments.db"))
        conn.execute(
            "UPDATE experiments SET status='error', error_message='Force reset by UI' "
            "WHERE exp_id = ?", (exp_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        st.error(f"DB reset failed: {e}")
        return
    if _KILL_ON_RESET:
        try:
            import psutil
            for proc in psutil.process_iter(["pid", "cmdline"]):
                cmdline = " ".join(proc.info.get("cmdline") or [])
                if "run_experiment" in cmdline or "test_harness" in cmdline:
                    os.kill(proc.info["pid"], signal.SIGTERM)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# SVG GAUGES
# ══════════════════════════════════════════════════════════════════════════════

def _gauge_svg(value, vmin, vmax, label, unit, color, warn=None, danger=None):
    pct   = max(0.0, min(1.0, (value - vmin) / max(vmax - vmin, 1e-9)))
    angle = -140 + pct * 280
    r     = 52; cx, cy = 60, 62
    ex    = cx + r * math.sin(math.radians(angle))
    ey    = cy - r * math.cos(math.radians(angle))
    large = 1 if pct > 0.5 else 0
    bx    = cx + r * math.sin(math.radians(-140))
    by    = cy - r * math.cos(math.radians(-140))
    ex0   = cx - r * math.sin(math.radians(-140))
    ey0   = cy - r * math.cos(math.radians(-140))
    nclr  = ("#ef4444" if danger and value >= danger
             else "#f59e0b" if warn and value >= warn else color)
    return (f"<div style='text-align:center;padding:2px 0;'>"
            f"<svg width='120' height='92' viewBox='0 0 120 92'>"
            f"<path d='M {bx:.1f} {by:.1f} A {r} {r} 0 1 1 {ex0:.1f} {ey0:.1f}'"
            f" fill='none' stroke='#1e2d45' stroke-width='8' stroke-linecap='round'/>"
            f"<path d='M {bx:.1f} {by:.1f} A {r} {r} 0 {large} 1 {ex:.1f} {ey:.1f}'"
            f" fill='none' stroke='{nclr}' stroke-width='8' stroke-linecap='round'/>"
            f"<circle cx='{cx}' cy='{cy}' r='4' fill='{nclr}'/>"
            f"<text x='{cx}' y='{cy+5}' text-anchor='middle' font-size='14'"
            f" font-weight='700' fill='#e8f0f8' font-family='monospace'>{value:.1f}</text>"
            f"<text x='{cx}' y='{cy+19}' text-anchor='middle' font-size='7' fill='#7090b0'>{unit}</text>"
            f"<text x='{cx}' y='85' text-anchor='middle' font-size='8'"
            f" font-weight='600' fill='{nclr}'>{label}</text>"
            f"<text x='6'   y='74' text-anchor='middle' font-size='6' fill='#3d5570'>{vmin}</text>"
            f"<text x='114' y='74' text-anchor='middle' font-size='6' fill='#3d5570'>{vmax}</text>"
            f"</svg></div>")


def _bar_gauge(value, vmax, label, unit, color):
    pct = max(0.0, min(100.0, value / max(vmax, 1e-9) * 100))
    return (f"<div style='margin:4px 0 8px;'>"
            f"<div style='display:flex;justify-content:space-between;"
            f"font-size:9px;color:#7090b0;margin-bottom:3px;'>"
            f"<span style='font-weight:600;color:#e8f0f8'>{label}</span>"
            f"<span style='font-family:monospace;color:{color}'>{value:.0f} {unit}</span>"
            f"</div><div style='background:#1e2d45;border-radius:3px;height:7px;overflow:hidden;'>"
            f"<div style='background:{color};width:{pct:.1f}%;height:100%;"
            f"border-radius:3px;transition:width 0.4s;'></div></div></div>")


# ══════════════════════════════════════════════════════════════════════════════
# SESSION TREE — color-coded bucket-wise status view
# FIX: was defined in v4 but never rendered because gid was empty at render time.
# Now gid is stored in st.session_state.ex_group_id, set when thread starts.
# ══════════════════════════════════════════════════════════════════════════════

def _session_tree(group_id: str):
    """
    Render a color-coded tree of all experiments in group_id.
    ● blue=completed  🟢 green=running  ○ gray=not_started
    🟡 yellow=pending  🔴 red=failed
    """
    if not group_id:
        st.caption("No active session group yet.")
        return
    try:
        exps = q(f"""
            SELECT exp_id, task_name, provider, model_name, status,
                   runs_completed, runs_total,
                   started_at, completed_at, optimization_enabled
            FROM experiments
            WHERE group_id = '{group_id}'
            ORDER BY exp_id
        """)
    except Exception:
        return
    if exps.empty:
        st.caption(f"No experiments found for group: {group_id}")
        return

    lines = [
        f"<div style='font-size:9px;font-weight:700;color:#3d5570;"
        f"margin-bottom:6px;font-family:monospace;letter-spacing:.05em;'>"
        f"SESSION  {group_id}</div>"
    ]

    # Count by status for the summary bar
    status_counts = {"completed": 0, "running": 0, "pending": 0, "not_started": 0, "failed": 0}
    for _, row in exps.iterrows():
        s = str(row.get("status", "not_started")).lower()
        if s in status_counts:
            status_counts[s] += 1
        else:
            status_counts["not_started"] += 1

    # Summary pill row
    pills = ""
    pill_cfg = [
        ("completed",   "#3b82f6", "●"),
        ("running",     "#22c55e", "🟢"),
        ("pending",     "#f59e0b", "🟡"),
        ("not_started", "#4b5563", "○"),
        ("failed",      "#ef4444", "🔴"),
    ]
    for key, clr, icon in pill_cfg:
        cnt = status_counts.get(key, 0)
        if cnt > 0:
            pills += (f"<span style='background:{clr}22;border:1px solid {clr}55;"
                      f"border-radius:4px;padding:1px 7px;margin-right:4px;"
                      f"font-size:9px;color:{clr};'>{icon} {cnt} {key}</span>")
    if pills:
        lines.append(f"<div style='margin-bottom:8px;'>{pills}</div>")

    # Tree rows
    for idx, (_, row) in enumerate(exps.iterrows()):
        is_last = (idx == len(exps) - 1)
        prefix  = "└──" if is_last else "├──"
        st_db   = str(row.get("status", "not_started")).lower()

        if   st_db == "completed":  icon, clr = "●",  "#3b82f6"
        elif st_db == "running":    icon, clr = "🟢", "#22c55e"
        elif st_db == "failed":     icon, clr = "🔴", "#ef4444"
        elif st_db == "pending":    icon, clr = "🟡", "#f59e0b"
        else:                       icon, clr = "○",  "#4b5563"

        dur = ""
        try:
            if row.started_at and row.completed_at:
                s = datetime.fromisoformat(str(row.started_at))
                e = datetime.fromisoformat(str(row.completed_at))
                secs = (e - s).total_seconds()
                dur  = f"  {secs:.0f}s" if secs < 60 else f"  {secs/60:.1f}m"
        except Exception:
            pass

        # Try to get a live rep count from the log when DB hasn't updated yet
        _db_done = int(row.runs_completed or 0)
        _db_tot  = int(row.runs_total or 0)
        if _db_done == 0 and str(row.get('status','')) == 'running':
            # parse log for 'rep N/M' or 'pair N/M'
            _log = _store_get('log', [])
            for _ll in reversed(_log[-30:]):
                import re as _re2
                _m = _re2.search(r'(?:rep|pair)\s+(\d+)\s*/\s*(\d+)', _ll.lower())
                if _m:
                    _db_done, _db_tot = int(_m.group(1)), int(_m.group(2))
                    break
        runs_text = f"{_db_done}/{_db_tot}"
        opt_badge = " 🔧" if row.get("optimization_enabled") else ""
        task_str  = str(row.task_name)[:22] if row.task_name else "?"

        lines.append(
            f"<div style='font-family:monospace;font-size:10px;line-height:1.9;"
            f"color:#5a7090;padding:0;'>"
            f"<span style='color:#2d3f55'>{prefix} </span>"
            f"<span style='font-size:11px'>{icon}</span>"
            f"<span style='color:#c8d8e8;margin-left:4px;font-weight:600;'>"
            f"exp_{row.exp_id}</span>"
            f"<span style='color:#3d5570;margin-left:6px;'>{row.provider}</span>"
            f"<span style='color:#7090b0;margin-left:6px;'>{task_str}</span>"
            f"<span style='color:{clr};margin-left:8px;font-size:9px;"
            f"background:{clr}18;padding:1px 5px;border-radius:3px;'>{st_db}</span>"
            f"<span style='color:#2d3f55;margin-left:6px;font-size:9px;'>"
            f"{runs_text}{dur}{opt_badge}</span>"
            f"</div>"
        )

    st.markdown(
        "<div style='background:#050810;border:1px solid #1e2d45;"
        "border-radius:6px;padding:10px 14px;margin-bottom:8px;'>"
        + "".join(lines) + "</div>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# GANTT TIMELINE
# ══════════════════════════════════════════════════════════════════════════════

def _gantt_chart(group_id: str):
    if not group_id:
        return
    try:
        exps = q(f"""
            SELECT exp_id, task_name, provider, status,
                   started_at, completed_at
            FROM experiments
            WHERE group_id = '{group_id}'
            ORDER BY exp_id
        """)
    except Exception:
        return
    if exps.empty:
        return

    bars = []
    now  = datetime.utcnow()
    for _, row in exps.iterrows():
        try:
            s = datetime.fromisoformat(str(row.started_at)) if row.started_at else None
            e = datetime.fromisoformat(str(row.completed_at)) if row.completed_at else None
            if s is None:
                continue
            end   = e if e else now
            dur_s = max((end - s).total_seconds(), 0.5)
            st_db = str(row.get("status", "")).lower()
            clr   = STATUS_COLORS.get(
                "completed" if st_db == "completed"
                else "running"  if st_db == "running"
                else "failed"   if st_db == "failed"
                else "not_started", "#4b5563")
            bars.append({
                "label":  f"exp_{row.exp_id} {row.provider[:3]} {str(row.task_name)[:16]}",
                "start":  0,
                "dur":    dur_s,
                "color":  clr,
                "status": st_db,
            })
        except Exception:
            continue
    if not bars:
        return

    try:
        first_start = datetime.fromisoformat(
            str(exps[exps.started_at.notna()].iloc[0].started_at))
        for i, (_, row) in enumerate(exps.iterrows()):
            if row.started_at:
                s = datetime.fromisoformat(str(row.started_at))
                if i < len(bars):
                    bars[i]["start"] = (s - first_start).total_seconds()
    except Exception:
        pass

    fig = go.Figure()
    for bar in reversed(bars):
        fig.add_trace(go.Bar(
            name=bar["label"],
            x=[bar["dur"]],
            y=[bar["label"]],
            base=bar["start"],
            orientation="h",
            marker_color=bar["color"],
            marker_opacity=0.85,
            showlegend=False,
            hovertemplate=(f"{bar['label']}<br>Duration: {bar['dur']:.1f}s"
                           f"<br>Status: {bar['status']}<extra></extra>"),
        ))
    _pl_g = {k: v for k, v in PL.items() if k != 'margin'}
    fig.update_layout(
        **_pl_g,
        height=max(80 + len(bars) * 28, 140),
        barmode="overlay",
        xaxis_title="Elapsed seconds",
        margin=dict(l=10, r=10, t=24, b=30),
        title=dict(text="⏱ Experiment Timeline", font=dict(size=10), x=0),
    )
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND RUN THREAD
# FIX: stores group_id into st.session_state.ex_group_id as soon as the first
# experiment row appears in the DB, so the session tree always has it.
# ══════════════════════════════════════════════════════════════════════════════

def _thread_run_local(_first_exp: dict, sid: str):
    """
    Runs all queued experiments sequentially.
    Writes ONLY to _STORE — never touches st.session_state (no ScriptRunContext).
    render() copies _STORE → session_state on every rerun so UI stays updated.
    """
    import sqlite3 as _sl3

    def _db1(sql):
        try:
            con = _sl3.connect(str(PROJECT_ROOT / "data" / "experiments.db"), timeout=3)
            row = con.execute(sql).fetchone()
            con.close()
            return row[0] if row else None
        except Exception:
            return None

    def _refresh_gid():
        try:
            con = _sl3.connect(str(PROJECT_ROOT / "data" / "experiments.db"), timeout=2)
            row = con.execute(
                "SELECT group_id FROM experiments ORDER BY exp_id DESC LIMIT 1"
            ).fetchone()
            con.close()
            gid = row[0] if row else ""
            if gid:
                _store_set("group_id", gid)
        except Exception:
            pass

    def _poll_telemetry(rid):
        if not _REQUESTS_OK or not rid:
            return
        m = _store_get("metrics", {}).copy()
        try:
            er = _req.get(f"http://127.0.0.1:8765/api/runs/{rid}/samples/energy", timeout=2).json()
            pw = er.get("power", []) if isinstance(er, dict) else []
            if pw:
                m["pkg_w"]  = float(pw[-1].get("pkg_w", 0))
                m["core_w"] = float(pw[-1].get("core_w", 0))
        except Exception:
            pass
        try:
            cr = _req.get(f"http://127.0.0.1:8765/api/runs/{rid}/samples/cpu", timeout=2).json()
            if isinstance(cr, list) and cr:
                m["temp_c"] = float(cr[-1].get("package_temp", 0))
                m["util"]   = float(cr[-1].get("cpu_util_percent", 0))
                m["ipc"]    = float(cr[-1].get("ipc", 0))
        except Exception:
            pass
        try:
            ir = _req.get(f"http://127.0.0.1:8765/api/runs/{rid}/samples/interrupts", timeout=2).json()
            if isinstance(ir, list) and ir:
                m["irq"] = float(ir[-1].get("interrupts_per_sec", 0))
        except Exception:
            pass
        _store_set("metrics", m)

    def _run_one(exp, exp_sid):
        _store_set("run_record",    {"sid": exp_sid, "name": exp.get("name","?"), "exp": exp})
        _store_set("log",           [])
        _store_set("progress",      0.0)
        _store_set("phase",         "starting")
        _store_set("metrics",       {})
        _store_set("result_rows",   [])
        _store_set("group_id",      "")
        _store_set("stop",          False)
        _store_set("current_cmd",   " ".join(str(x) for x in exp.get("cmd", [])))

        cmd = exp.get("cmd", [])
        if not cmd:
            _store_log(f"[ERROR] No cmd found in experiment config: {exp}")
            return -1

        last_rid = _db1("SELECT COALESCE(MAX(run_id),0) FROM runs") or 0
        line_ctr = 0
        rc       = -1

        try:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, cwd=str(PROJECT_ROOT), bufsize=1)

            for raw in iter(proc.stdout.readline, ""):
                line = raw.rstrip()
                if not line:
                    continue
                _store_log(line)

                # Honour stop request
                if _store_get("stop", False):
                    proc.terminate()
                    _store_log("[STOPPED by user]")
                    proc.wait()
                    return -2

                lo = line.lower()
                if   "planning"  in lo: _store_set("phase", "planning")
                elif "execution" in lo: _store_set("phase", "execution")
                elif "synth"     in lo: _store_set("phase", "synthesis")
                elif "rep " in lo or "pair" in lo: _store_set("phase", "running")
                if any(k in lo for k in ["complete", "saved", "✅"]):
                    _store_set("phase", "complete")

                for pat in ["rep ", "pair ", "progress:"]:
                    if pat in lo and "/" in lo:
                        try:
                            seg = lo.split(pat)[-1].split("/")
                            d = int(seg[0].strip().split()[-1])
                            t = int(seg[1].split()[0])
                            _store_set("progress", min(d / t, 1.0))
                        except Exception:
                            pass
                        break

                _refresh_gid()
                line_ctr += 1
                if line_ctr % 5 == 0:
                    nr = _db1("SELECT COALESCE(MAX(run_id),0) FROM runs") or last_rid
                    if nr > last_rid:
                        last_rid = nr
                    _poll_telemetry(last_rid)

            proc.wait()
            rc = proc.returncode
        except Exception as ex:
            _store_log(f"[ERROR] {ex}")
            rc = -1

        _store_set("phase",    "complete" if rc == 0 else "error")
        _store_set("progress", 1.0)
        _refresh_gid()

        rows = _parse_summary(_store_get("log", []))
        _store_set("result_rows", rows)
        _store_append("sessions", {
            "sid":          exp_sid,
            "name":         exp.get("name", "?"),
            "status":       "complete" if rc == 0 else "error",
            "log":          _store_get("log", []).copy(),
            "summary_rows": rows,
            "ts":           _time.strftime("%H:%M:%S"),
            "rc":           rc,
            "group_id":     _store_get("group_id", ""),
        })
        return rc

    # ── Drain full queue ──────────────────────────────────────────────────────
    _store_set("running", True)
    _store_set("done",    False)

    _run_one(_first_exp, sid)

    while True:
        if _store_get("stop", False):
            break
        with _STORE_LOCK:
            q = _STORE.get("queue", [])
            if not q:
                break
            nxt = q.pop(0)
        _run_one(nxt, f"ses_{int(_time.time()*1000)}")

    _store_set("running", False)
    _store_set("done",    True)


def _parse_summary(lines: list) -> list:
    rows = []; in_sum = False
    for l in lines:
        ll = l.strip()
        if "MASTER SUMMARY" in ll:   in_sum = True;  continue
        if in_sum and ll.startswith("==="): in_sum = False; continue
        if in_sum and ll.startswith("---"): continue
        if in_sum and ll and not ll.startswith("Provider"):
            m = _re.match(r'^(\S+)\s+(.*?)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)x?\s*(\[.*?\])?', ll)
            if m:
                pv, tk, ln, ag, tx, ci = m.groups()
                rows.append({"provider": pv, "task": tk.strip(),
                             "linear_j": float(ln), "agentic_j": float(ag),
                             "tax_x": float(tx), "ci": ci or ""})
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# LIVE VIEW WIDGET
# FIX: extracted into its own function so it can be called OUTSIDE the tab block.
# This means the live view renders whether the user is on the Live tab or any
# other tab — Streamlit will show it above the tabs.
# ══════════════════════════════════════════════════════════════════════════════

def _render_live_view():
    """
    Render the live execution panel. Called at the TOP LEVEL of render() so it
    persists across tab switches. Only visible when ex_running or ex_done is True.
    """
    # Always read directly from _STORE for freshest data
    _running = _store_get('running', False)
    _done    = _store_get('done',    False)
    if not (_running or _done):
        return
    # Update session_state so rest of UI stays in sync
    st.session_state.ex_running = _running
    st.session_state.ex_done    = _done

    rec   = _store_get('run_record') or {}
    phase = _store_get('phase', 'idle')
    prog  = _store_get('progress', 0.0)
    log   = _store_get('log', [])
    m     = _store_get('metrics', {})
    gid   = _store_get('group_id', '')

    # Phase banner + progress bar
    pc = {"starting": "#7090b0", "planning": "#f59e0b",
          "execution": "#3b82f6", "synthesis": "#a78bfa",
          "running": "#22c55e", "complete": "#22c55e",
          "error": "#ef4444", "idle": "#3d5570"}.get(phase, "#7090b0")

    # ── Stop button + phase banner ─────────────────────────────────────────
    hdr_col, stop_col = st.columns([5, 1])
    cur_cmd = _store_get('current_cmd', '')
    hdr_col.markdown(
        f"<div style='background:#080d18;border:1px solid {pc}44;"
        f"border-left:4px solid {pc};border-radius:5px;"
        f"padding:8px 14px;margin:2px 0;display:flex;align-items:center;gap:14px;'>"
        f"<span style='font-size:10px;padding:3px 10px;background:{pc}22;"
        f"border:1px solid {pc};border-radius:4px;color:{pc};font-weight:700;'>"
        f"● {phase.upper()}</span>"
        f"<span style='font-size:10px;color:#e8f0f8;margin-left:6px;font-weight:600;'>{rec.get('name','')}</span>"
        f"<span style='font-size:9px;color:#3d5570;margin-left:8px;'>{int(prog*100)}%</span>"
        f"{'<span style=\"font-size:9px;color:#22c55e;margin-left:8px;\">⚡ RUNNING</span>' if st.session_state.ex_running else ''}"
        f"</div>",
        unsafe_allow_html=True)
    if st.session_state.ex_running:
        if stop_col.button('⏹ Stop', type='secondary', use_container_width=True, key='stop_run_btn'):
            _store_set('stop', True)
            st.warning('Stop signal sent — current experiment will finish its current rep then halt.')
    if cur_cmd:
        st.markdown(
            f"<div style='font-family:monospace;font-size:9px;color:#3d5570;"
            f"background:#05080f;border:1px solid #1e2d45;border-radius:4px;"
            f"padding:4px 10px;margin-bottom:4px;overflow-x:auto;white-space:nowrap;'>"
            f"$ {cur_cmd[:200]}</div>",
            unsafe_allow_html=True)
    st.progress(prog)

    # Two-column layout: LEFT = session tree + Gantt | RIGHT = telemetry + log
    left_col, right_col = st.columns([1, 1])

    with left_col:
        st.markdown(
            "<div style='font-size:10px;font-weight:700;color:#7090b0;"
            "text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px;'>"
            "🌳 Session Tree</div>",
            unsafe_allow_html=True)
        _session_tree(gid)
        st.markdown(
            "<div style='font-size:10px;font-weight:700;color:#7090b0;"
            "text-transform:uppercase;letter-spacing:.08em;margin:8px 0 4px;'>"
            "⏱ Timeline</div>",
            unsafe_allow_html=True)
        _gantt_chart(gid)

    with right_col:
        st.markdown(
            "<div style='font-size:10px;font-weight:700;color:#7090b0;"
            "text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px;'>"
            "⚡ Live Telemetry</div>",
            unsafe_allow_html=True)
        st.markdown(
            f"<div style='display:flex;justify-content:space-around;'>"
            f"{_gauge_svg(m.get('pkg_w',0),0,80,'Pkg Power','W','#3b82f6',warn=50,danger=70)}"
            f"{_gauge_svg(m.get('core_w',0),0,60,'Core Power','W','#22c55e',warn=40,danger=55)}"
            f"{_gauge_svg(m.get('temp_c',0),30,105,'Pkg Temp','°C','#f59e0b',warn=80,danger=95)}"
            f"</div>",
            unsafe_allow_html=True)
        st.markdown(
            _bar_gauge(m.get('util', 0),   100,   "CPU Util",  "%",        "#38bdf8") +
            _bar_gauge(min(m.get('irq', 0), 50000), 50000, "IRQ Rate", "/s", "#f59e0b") +
            _bar_gauge(m.get('ipc', 0),    3.0,   "IPC",       "inst/cyc", "#a78bfa"),
            unsafe_allow_html=True)

        # Live log (last 40 lines)
        if log:
            log_html = "".join(
                f"<div style='color:"
                f"{'#ef4444' if any(k in l.lower() for k in ['error','fail','traceback']) else '#22c55e' if any(k in l.lower() for k in ['complete','saved','✅','pair']) else '#f59e0b' if 'planning' in l.lower() else '#b8c8d8'};"
                f"font-family:monospace;font-size:9px;line-height:1.5;'>"
                f"{l.replace('<','&lt;').replace('>','&gt;')}</div>"
                for l in log[-40:])
            st.markdown(
                "<div style='background:#050810;border:1px solid #1e2d45;"
                "border-radius:4px;padding:8px;height:220px;overflow-y:auto;'>"
                f"{log_html}</div>",
                unsafe_allow_html=True)

    # Auto-refresh while running (no sleep — _STORE is read fresh each rerun)
    if st.session_state.ex_running:
        st.rerun()

    # Results once done
    if st.session_state.ex_done and st.session_state.ex_result_rows:
        st.divider()
        st.markdown("### 📊 Results")
        _analytics_card({
            "sid":          rec.get("sid", ""),
            "summary_rows": st.session_state.ex_result_rows,
            "log":          log,
        })
        st.session_state.ex_done = False

    # Next in queue
    if not st.session_state.ex_running and st.session_state.ex_queue:
        st.info(f"⏳ {len(st.session_state.ex_queue)} more queued — click ▶ Start again.")


# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICS CARD
# ══════════════════════════════════════════════════════════════════════════════

def _analytics_card(session: dict):
    rows  = session.get("summary_rows", [])
    lines = session.get("log", [])
    sid   = session.get("sid", "x")

    if rows:
        _thresholds = INSIGHTS_RULES.get("tax_thresholds", {})
        def _tax_color(tx):
            if tx >= _thresholds.get("extreme", {}).get("min", 15):   return "#ef4444"
            if tx >= _thresholds.get("high",    {}).get("max", 15):   return "#f59e0b"
            if tx >= _thresholds.get("moderate",{}).get("max",  5):   return "#38bdf8"
            return "#22c55e"

        rh = ""
        for r in rows:
            tc  = _tax_color(r["tax_x"])
            mx  = max(r["linear_j"], r["agentic_j"], 0.001)
            lw, aw = r["linear_j"] / mx * 100, r["agentic_j"] / mx * 100
            hi   = _human_energy(r["agentic_j"])
            hi_s = hi[0][1] if hi else ""
            rh += (
                f"<tr style='border-bottom:1px solid #111827;'>"
                f"<td style='padding:9px 8px;font-size:10px;color:#7090b0;'>{r['provider']}</td>"
                f"<td style='padding:9px 8px;font-size:10px;color:#e8f0f8;min-width:140px;'>{r['task']}</td>"
                f"<td style='padding:9px 8px;'>"
                f"<div style='font-size:11px;color:#22c55e;font-family:monospace;'>{r['linear_j']:.4f} J</div>"
                f"<div style='background:#1e2d45;border-radius:2px;height:5px;width:110px;margin-top:3px;'>"
                f"<div style='background:#22c55e;width:{lw:.0f}%;height:100%;border-radius:2px;'></div></div>"
                f"</td><td style='padding:9px 8px;'>"
                f"<div style='font-size:11px;color:#ef4444;font-family:monospace;'>{r['agentic_j']:.4f} J</div>"
                f"<div style='background:#1e2d45;border-radius:2px;height:5px;width:110px;margin-top:3px;'>"
                f"<div style='background:#ef4444;width:{aw:.0f}%;height:100%;border-radius:2px;'></div></div>"
                f"</td><td style='padding:9px 8px;text-align:center;'>"
                f"<span style='font-size:14px;font-weight:700;color:{tc};font-family:monospace;'>"
                f"{r['tax_x']:.2f}×</span>"
                f"</td><td style='padding:9px 8px;font-size:9px;color:#3d5570;font-family:monospace;'>"
                f"{r.get('ci','')}</td>"
                f"<td style='padding:9px 8px;font-size:9px;color:#7090b0;'>{hi_s}</td>"
                f"</tr>"
            )

        st.markdown(
            "<div style='background:#07090f;border:1px solid #1e2d45;border-radius:8px;"
            "overflow:hidden;margin:10px 0;'>"
            "<div style='background:#0a0e1a;padding:8px 14px;border-bottom:1px solid #1e2d45;"
            "font-size:10px;font-weight:700;color:#4fc3f7;letter-spacing:.08em;"
            "text-transform:uppercase;'>⚡ Apple-to-Apple Energy Comparison</div>"
            "<table style='width:100%;border-collapse:collapse;'>"
            "<thead><tr style='background:#0a0e1a;border-bottom:2px solid #1e2d45;'>"
            "<th style='padding:7px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>Provider</th>"
            "<th style='padding:7px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>Task</th>"
            "<th style='padding:7px 8px;font-size:9px;color:#22c55e;text-align:left;text-transform:uppercase;'>Linear</th>"
            "<th style='padding:7px 8px;font-size:9px;color:#ef4444;text-align:left;text-transform:uppercase;'>Agentic</th>"
            "<th style='padding:7px 8px;font-size:9px;color:#f59e0b;text-align:center;text-transform:uppercase;'>Tax</th>"
            "<th style='padding:7px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>95% CI</th>"
            "<th style='padding:7px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>Insight</th>"
            f"</tr></thead><tbody>{rh}</tbody></table></div>",
            unsafe_allow_html=True,
        )

        if len(rows) > 1:
            best  = min(rows, key=lambda r: r["tax_x"])
            worst = max(rows, key=lambda r: r["tax_x"])
            avg_t = sum(r["tax_x"] for r in rows) / len(rows)
            c1, c2, c3 = st.columns(3)
            c1.success(f"**✅ Lowest overhead**\n\n{best['provider']} · {best['task'][:24]}\n\n**{best['tax_x']:.2f}×**")
            c2.error(f"**⚠ Highest overhead**\n\n{worst['provider']} · {worst['task'][:24]}\n\n**{worst['tax_x']:.2f}×**")
            c3.info(f"**📈 Average**\n\n{len(rows)} comparisons · **{avg_t:.2f}×** mean tax")

        df  = pd.DataFrame(rows)
        df["label"] = df["provider"] + " · " + df["task"].str[:22]
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Linear",  x=df["label"], y=df["linear_j"],
                             marker_color="#22c55e", text=df["linear_j"].round(3),
                             textposition="outside", textfont=dict(size=8)))
        fig.add_trace(go.Bar(name="Agentic", x=df["label"], y=df["agentic_j"],
                             marker_color="#ef4444", text=df["agentic_j"].round(3),
                             textposition="outside", textfont=dict(size=8)))
        _pl2 = {k: v for k, v in PL.items() if k != 'margin'}
        fig.update_layout(**_pl2, barmode="group", height=260,
                          title="Linear vs Agentic energy — this run",
                          xaxis_tickangle=20, margin=dict(t=40, b=10))
        st.plotly_chart(fig, use_container_width=True)

        csv = df[["provider", "task", "linear_j", "agentic_j", "tax_x", "ci"]].to_csv(index=False)
        st.download_button("📥 Export CSV", csv,
                           file_name=f"alems_{sid}.csv",
                           mime="text/csv", key=f"csv_{sid}")
    else:
        st.info("No summary rows parsed yet. Check raw log below.")

    with st.expander("📋 Raw log", expanded=False):
        log_html = "".join(
            f"<div style='color:{'#ef4444' if any(k in l.lower() for k in ['error','fail']) else '#22c55e' if any(k in l.lower() for k in ['complete','saved','✅']) else '#b8c8d8'};"
            f"font-family:monospace;font-size:10px;line-height:1.5;'>"
            f"{l.replace('<','&lt;').replace('>','&gt;')}</div>"
            for l in lines)
        st.markdown(
            "<div style='background:#050810;border:1px solid #1e2d45;border-radius:4px;"
            f"padding:10px;max-height:300px;overflow-y:auto;'>{log_html}</div>",
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# REMOTE EXECUTION STREAM
# ══════════════════════════════════════════════════════════════════════════════

def _run_remote(exp: dict, session_id: str, base_url: str):
    lines = []; summary_rows = []
    if not _REQUESTS_OK:
        st.error("pip install requests"); return -1, lines, summary_rows

    prog_ph = st.progress(0); status_ph = st.empty()
    cols = st.columns([11, 9])
    with cols[0]:
        st.markdown("<div style='font-size:10px;font-weight:600;color:#7090b0;"
                    "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                    "⬛ Remote terminal</div>", unsafe_allow_html=True)
        out_ph = st.empty()
    with cols[1]:
        st.markdown("<div style='font-size:10px;font-weight:600;color:#7090b0;"
                    "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                    "⚡ Live telemetry</div>", unsafe_allow_html=True)
        phase_ph = st.empty(); gauge_ph = st.empty(); bar_ph = st.empty()

    _pw = _core_w = _tp = _util = _irq = _ipc = 0.0

    def _draw(phase):
        gauge_ph.markdown(
            f"<div style='display:flex;justify-content:space-around;'>"
            f"{_gauge_svg(_pw,0,80,'Pkg Power','W','#3b82f6',warn=50,danger=70)}"
            f"{_gauge_svg(_core_w,0,60,'Core Power','W','#22c55e',warn=40,danger=55)}"
            f"{_gauge_svg(_tp,30,105,'Pkg Temp','°C','#f59e0b',warn=80,danger=95)}"
            f"</div>", unsafe_allow_html=True)
        bar_ph.markdown(
            _bar_gauge(_util, 100, "CPU Util", "%", "#38bdf8") +
            _bar_gauge(min(_irq, 50000), 50000, "IRQ Rate", "/s", "#f59e0b") +
            _bar_gauge(_ipc, 3.0, "IPC", "inst/cyc", "#a78bfa"),
            unsafe_allow_html=True)
        pc = {"starting": "#7090b0", "running": "#22c55e",
              "complete": "#22c55e", "error": "#ef4444"}.get(phase, "#7090b0")
        phase_ph.markdown(
            f"<div style='font-size:10px;padding:3px 10px;background:{pc}22;"
            f"border:1px solid {pc};border-radius:4px;display:inline-block;color:{pc};'>"
            f"● {phase.upper()}</div>", unsafe_allow_html=True)

    seen = 0
    for _ in range(600):
        _time.sleep(1)
        try:
            r    = _req.get(f"{base_url}/api/run/status/{session_id}", timeout=6)
            data = r.json()
        except Exception as e:
            status_ph.warning(f"Poll error: {e}"); continue

        status = data.get("status", "?")
        log    = data.get("log", [])
        prog   = float(data.get("progress", 0))
        prog_ph.progress(min(prog, 1.0))

        new = log[seen:]; seen = len(log)
        for l in new: lines.append(l)

        if lines:
            html = "".join(
                f"<div style='color:{'#ef4444' if any(k in l.lower() for k in ['error','fail']) else '#22c55e' if any(k in l.lower() for k in ['complete','✅','saved']) else '#b8c8d8'};"
                f"font-family:monospace;font-size:10px;line-height:1.5;'>"
                f"{l.replace('<','&lt;').replace('>','&gt;')}</div>"
                for l in lines[-50:])
            out_ph.markdown(
                "<div style='background:#060a0f;border:1px solid #1e2d45;border-radius:4px;"
                f"padding:8px;max-height:340px;overflow-y:auto;'>{html}</div>",
                unsafe_allow_html=True)

        _draw(status)
        status_ph.markdown(
            f"<div style='font-size:9px;color:#5a7090;'>Session <code>{session_id}</code>"
            f" · <b style='color:#4fc3f7;'>{status}</b></div>", unsafe_allow_html=True)

        if data.get("done") or status in ("complete", "error", "cancelled"):
            if status == "complete":
                prog_ph.progress(1.0)
                st.success("✅ Remote run complete — DB updated on lab machine.")
                summary_rows = _parse_summary(lines)
            else:
                st.error(f"Run ended: {status}")
            return (0 if status == "complete" else 1), lines, summary_rows

    st.warning("Polling timed out."); return -1, lines, []


# ══════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════════════════════

def render(ctx: dict):
    _init_state()
    _load_queue()

    st.title("Execute Run")

    # ── Mode banner ────────────────────────────────────────────────────────────
    _conn = get_conn()
    if _conn.get("verified"):
        _hclr = "#22c55e" if _conn.get("harness") else "#f59e0b"
        _hmsg = ("Harness ready — runs execute on lab machine"
                 if _conn.get("harness") else "Server reachable but harness not loaded")
        st.markdown(
            f"<div style='background:#0a2010;border:1px solid #22c55e33;"
            f"border-left:3px solid #22c55e;border-radius:4px;"
            f"padding:8px 14px;margin-bottom:10px;font-size:11px;'>"
            f"🟢 <b style='color:#22c55e'>LIVE MODE</b>  ·  "
            f"<span style='color:{_hclr}'>{_hmsg}</span><br/>"
            f"<span style='color:#3d5570;font-size:9px;'>Tunnel: {_conn['url']}</span></div>",
            unsafe_allow_html=True)
    else:
        st.markdown(
            "<div style='background:#0a0f1a;border:1px solid #1e2d45;"
            "border-left:3px solid #3b82f6;border-radius:4px;"
            "padding:8px 14px;margin-bottom:10px;font-size:11px;'>"
            "⚫ <b style='color:#3b82f6'>LOCAL MODE</b>  ·  "
            "<span style='color:#5a7090'>Runs execute on this machine.</span></div>",
            unsafe_allow_html=True)

    # ── Queue banner ───────────────────────────────────────────────────────────
    # Keep queue in sync between _STORE and session_state
    if 'ex_queue' not in st.session_state:
        st.session_state.ex_queue = _store_get('queue', [])
    else:
        _store_set('queue', list(st.session_state.ex_queue))
    qlen = len(st.session_state.ex_queue)
    if qlen > 0:
        st.markdown(
            f"<div style='background:#0f1a2e;border:1px solid #3b4fd8;border-radius:4px;"
            f"padding:7px 14px;margin-bottom:10px;font-size:11px;color:#93c5fd;'>"
            f"⏳ <b>{qlen}</b> experiment{'s' if qlen > 1 else ''} queued"
            f"{'  ·  🔴 run in progress' if st.session_state.ex_running else ''}"
            f"</div>",
            unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # LIVE VIEW — rendered OUTSIDE tabs so tab-switching never hides it
    # ══════════════════════════════════════════════════════════════════════════
    # Sync running/done directly from _STORE before checking
    st.session_state.ex_running = _store_get('running', False)
    st.session_state.ex_done    = _store_get('done', False)
    if st.session_state.ex_running or st.session_state.ex_done:
        with st.container():
            st.markdown(
                "<div style='background:#07080f;border:1px solid #22c55e33;"
                "border-left:4px solid #22c55e;border-radius:6px;padding:4px 14px 0;"
                "margin-bottom:6px;'>"
                "<div style='font-size:10px;font-weight:700;color:#22c55e;"
                "letter-spacing:.1em;text-transform:uppercase;padding:6px 0;'>"
                "⚡ Live Execution  —  visible from any tab</div></div>",
                unsafe_allow_html=True)
            _render_live_view()
        st.divider()

    # ── Load tasks from YAML ───────────────────────────────────────────────────
    all_tasks, _cat_map, _name_map = _load_tasks()

    # ── Tab layout ─────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 Create & Queue",
        "⚡ Live Execution",
        "📊 Session Analysis",
        "📈 Run History",
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — CREATE & QUEUE
    # ══════════════════════════════════════════════════════════════════════════
    with tab1:
        _show_stuck_runs()
        left, right = st.columns([1, 1])

        with left:
            st.markdown("#### 🔬 Build Experiment")
            exp_name = st.text_input("Name", value="My Experiment", key="ex_name")
            exp_mode = st.radio("Mode", ["Single (test_harness)", "Batch (run_experiment)"],
                                horizontal=True, key="ex_mode")

            if not all_tasks:
                st.error("No tasks available. Please check config/tasks.yaml.")
                return

            if "Single" in exp_mode:
                task_labels = [f"{tid}  ({_cat_map.get(tid,'')})" for tid in all_tasks]
                h_task_idx  = st.selectbox("Task", range(len(all_tasks)),
                                            format_func=lambda i: task_labels[i], key="h_task_idx")
                h_task    = all_tasks[h_task_idx]
                h_prov    = st.selectbox("Provider", ["cloud", "local"], key="h_prov")
                h_reps    = st.number_input("Repetitions", 1, 100, 3, key="h_reps")
                h_country = st.selectbox("Region",
                    ["US","DE","FR","NO","IN","AU","GB","CN","BR"],
                    format_func=lambda x: {"US":"🇺🇸 US","DE":"🇩🇪 DE","FR":"🇫🇷 FR",
                        "NO":"🇳🇴 NO","IN":"🇮🇳 IN","AU":"🇦🇺 AU",
                        "GB":"🇬🇧 GB","CN":"🇨🇳 CN","BR":"🇧🇷 BR"}.get(x, x), key="h_country")
                h_cd      = st.number_input("Cool-down (s)", 0, 120, 5, step=5, key="h_cd")
                h_save_db = st.checkbox("--save-db",   value=True,  key="h_savedb")
                h_opt     = st.checkbox("--optimizer", value=False, key="h_opt")
                h_warmup  = st.checkbox("--no-warmup", value=False, key="h_warmup")
                h_debug   = st.checkbox("--debug",     value=False, key="h_debug")

                cmd = ["python", "-m", "core.execution.tests.test_harness",
                       "--task-id", h_task, "--provider", h_prov,
                       "--repetitions", str(int(h_reps)), "--country", h_country,
                       "--cool-down", str(int(h_cd))]
                if h_save_db: cmd.append("--save-db")
                if h_opt:     cmd.append("--optimizer")
                if h_warmup:  cmd.append("--no-warmup")
                if h_debug:   cmd.append("--debug")

                meta = {"name": exp_name, "mode": "single", "task": h_task,
                        "provider": h_prov, "reps": int(h_reps),
                        "country": h_country, "cmd": cmd}

            else:
                _b_all = st.checkbox("All tasks", value=False, key="b_all")
                if _b_all:
                    _sel = all_tasks; st.caption(f"All {len(all_tasks)} tasks selected")
                else:
                    _sel = st.multiselect(
                        "Tasks", all_tasks,
                        default=all_tasks[:2] if len(all_tasks) >= 2 else all_tasks,
                        format_func=lambda t: f"{t}  ({_cat_map.get(t,'')})",
                        key="b_task_multi")

                b_prov    = st.multiselect("Providers", ["cloud", "local"],
                                           default=["cloud"], key="b_prov")
                b_reps    = st.number_input("Repetitions", 1, 100, 3, key="b_reps")
                b_country = st.selectbox("Region",
                    ["US","DE","FR","NO","IN","AU","GB","CN","BR"],
                    format_func=lambda x: {"US":"🇺🇸 US","DE":"🇩🇪 DE","FR":"🇫🇷 FR",
                        "NO":"🇳🇴 NO","IN":"🇮🇳 IN","AU":"🇦🇺 AU",
                        "GB":"🇬🇧 GB","CN":"🇨🇳 CN","BR":"🇧🇷 BR"}.get(x, x), key="b_country")
                b_cd      = st.number_input("Cool-down (s)", 0, 120, 5, step=5, key="b_cd")
                b_save_db = st.checkbox("--save-db",   value=True,  key="b_savedb")
                b_opt     = st.checkbox("--optimizer", value=False, key="b_opt")
                b_warmup  = st.checkbox("--no-warmup", value=False, key="b_warmup")

                prov_arg  = ",".join(b_prov)  if b_prov else "cloud"
                tasks_arg = ",".join(_sel)    if _sel   else (all_tasks[0] if all_tasks else "")

                cmd = ["python", "-m", "core.execution.tests.run_experiment",
                       "--tasks", tasks_arg, "--providers", prov_arg,
                       "--repetitions", str(int(b_reps)), "--country", b_country,
                       "--cool-down", str(int(b_cd))]
                if b_save_db: cmd.append("--save-db")
                if b_opt:     cmd.append("--optimizer")
                if b_warmup:  cmd.append("--no-warmup")

                meta = {"name": exp_name, "mode": "batch", "tasks": _sel,
                        "providers": b_prov, "reps": int(b_reps),
                        "country": b_country, "cmd": cmd}

            st.code(" \\\n  ".join(cmd), language="bash")

            c1, c2, c3 = st.columns(3)
            if c1.button("💾 Save", use_container_width=True, key="ex_save"):
                st.session_state.ex_saved.append(dict(meta))
                st.success(f"Saved **{exp_name}**")

            if c2.button("▶ Run Now", type="primary", use_container_width=True, key="ex_run_now"):
                if st.session_state.ex_running:
                    st.warning("A run is already in progress. Queue it instead.")
                else:
                    st.session_state.ex_queue.insert(0, dict(meta))
                    _save_queue()
                    st.success("Queued — go to ⚡ Live Execution"); st.rerun()

            if c3.button("➕ Queue", use_container_width=True, key="ex_queue_btn"):
                st.session_state.ex_queue.append(dict(meta))
                _save_queue()
                st.success(f"Queued at position {len(st.session_state.ex_queue)}")

        with right:
            st.markdown("#### 📁 Saved Experiments")
            if not st.session_state.ex_saved:
                st.caption("No saved experiments yet.")
            else:
                for i, exp in enumerate(st.session_state.ex_saved):
                    ea, eb, ec = st.columns([3, 1, 1])
                    ea.markdown(
                        f"<div style='font-size:12px;font-weight:600;color:#e8f0f8;'>{exp['name']}</div>"
                        f"<div style='font-size:10px;color:#7090b0;'>"
                        f"{exp.get('task', ', '.join(exp.get('tasks', [])))[:30]} · "
                        f"{exp.get('provider', '/'.join(exp.get('providers', [])))} · "
                        f"{exp.get('reps', 3)} reps</div>",
                        unsafe_allow_html=True)
                    if eb.button("▶", key=f"sv_run_{i}", use_container_width=True):
                        st.session_state.ex_queue.insert(0, dict(exp))
                        _save_queue(); st.rerun()
                    if ec.button("🗑", key=f"sv_del_{i}", use_container_width=True):
                        st.session_state.ex_saved.pop(i); st.rerun()

                if st.button("▶▶ Run All Saved", type="primary",
                             use_container_width=True, key="run_all"):
                    for e in st.session_state.ex_saved:
                        st.session_state.ex_queue.append(dict(e))
                    _save_queue()
                    st.success(f"Queued {len(st.session_state.ex_saved)} experiments"); st.rerun()

            st.divider()
            st.markdown("#### ⏳ Queue")
            if not st.session_state.ex_queue:
                st.caption("Queue is empty.")
            else:
                for i, exp in enumerate(st.session_state.ex_queue):
                    qa, qb = st.columns([4, 1])
                    qa.markdown(
                        f"<div style='font-size:11px;color:#93c5fd;'>"
                        f"#{i+1} — <b>{exp['name']}</b></div>",
                        unsafe_allow_html=True)
                    if qb.button("✕", key=f"q_del_{i}", use_container_width=True):
                        st.session_state.ex_queue.pop(i)
                        _save_queue(); st.rerun()

                if st.button("🗑 Clear queue", use_container_width=True, key="clear_q"):
                    st.session_state.ex_queue.clear()
                    _save_queue(); st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — LIVE EXECUTION (start button only; panel is above tabs)
    # ══════════════════════════════════════════════════════════════════════════
    with tab2:
        conn = get_conn()

        if not st.session_state.ex_running and st.session_state.ex_queue:
            next_exp = st.session_state.ex_queue[0]
            rem      = len(st.session_state.ex_queue) - 1

            st.markdown(
                f"<div style='background:#0a1a0a;border:1px solid #22c55e33;"
                f"border-left:3px solid #22c55e;border-radius:4px;"
                f"padding:8px 14px;margin-bottom:10px;font-size:12px;'>"
                f"▶ Ready: <b style='color:#22c55e'>{next_exp['name']}</b>"
                f"{'  ·  '+str(rem)+' more queued' if rem > 0 else ''}"
                f"</div>", unsafe_allow_html=True)

            if st.button(f"▶ Start — {next_exp['name']}", type="primary",
                         use_container_width=True, key="start_next"):

                exp = st.session_state.ex_queue.pop(0)
                _save_queue()
                sid = f"ses_{int(_time.time()*1000)}"

                if conn.get("verified"):
                    payload = {
                        "task_id":      exp.get("task", (exp.get("tasks") or [all_tasks[0] if all_tasks else "gsm8k_basic"])[0]),
                        "provider":     exp.get("provider", (exp.get("providers") or ["cloud"])[0]),
                        "country_code": exp.get("country", "US"),
                        "repetitions":  exp.get("reps", 3),
                        "cool_down":    5,
                        "tasks":        exp.get("tasks", [exp.get("task", all_tasks[0] if all_tasks else "gsm8k_basic")]),
                        "providers":    exp.get("providers", [exp.get("provider", "cloud")]),
                        "token":        conn.get("token", ""),
                    }
                    resp, err = api_post("/api/run/start", payload)
                    if err:
                        st.error(f"Remote start failed: {err}")
                    else:
                        rsid = resp.get("session_id", "")
                        st.success(f"✅ Started — session `{rsid}`")
                        rc, lines, rows = _run_remote(exp, rsid, conn["url"])
                        record = {"sid": sid, "name": exp["name"],
                                  "status": "complete" if rc == 0 else "error",
                                  "log": lines, "summary_rows": rows,
                                  "ts": _time.strftime("%H:%M:%S")}
                        st.session_state.ex_sessions.append(record)
                        if record["status"] == "complete":
                            _analytics_card(record)
                else:
                    st.session_state.ex_run_record = {"sid": sid, "name": exp["name"], "exp": exp}
                    t = threading.Thread(
                        target=_thread_run_local, args=(exp, sid), daemon=True)
                    t.start()
                    st.session_state.ex_thread = t
                    st.rerun()

        elif not st.session_state.ex_running and not st.session_state.ex_queue:
            if not (st.session_state.ex_running or st.session_state.ex_done):
                st.info("Queue is empty. Go to 📋 Create & Queue to add experiments.")

        if st.session_state.ex_running or st.session_state.ex_done:
            st.info("⬆️ Live execution panel is shown above the tabs so it stays visible from any tab.")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — SESSION ANALYSIS
    # ══════════════════════════════════════════════════════════════════════════
    with tab3:
        from gui.pages.session_analysis import render_session_analysis

        st.markdown("### 📊 Session Analysis")

        # Pull recent sessions from DB
        try:
            recent = q("""
                SELECT group_id,
                       COUNT(*) as n_exps,
                       SUM(runs_completed) as n_runs,
                       MAX(created_at) as latest
                FROM experiments
                GROUP BY group_id
                ORDER BY MAX(exp_id) DESC
                LIMIT 10
            """)
        except Exception:
            recent = pd.DataFrame()

        gid_options = recent.group_id.tolist() if not recent.empty else []

        if not gid_options:
            st.info("No sessions in DB yet. Run an experiment first.")
        else:
            sel_gid = st.selectbox(
                "Select session",
                gid_options,
                format_func=lambda g: (
                    f"{g}  ({recent[recent.group_id==g].iloc[0].n_exps:.0f} exps, "
                    f"{recent[recent.group_id==g].iloc[0].n_runs or 0:.0f} runs)"
                    if not recent[recent.group_id==g].empty else g
                ),
                key="t3_gid_sel"
            )
            if sel_gid:
                render_session_analysis(sel_gid)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 — RUN HISTORY
    # ══════════════════════════════════════════════════════════════════════════
    with tab4:
        st.markdown("#### 📈 All Runs This Session")
        if not st.session_state.ex_sessions:
            st.info("No runs yet. Completed runs appear here as expandable cards.")
        else:
            for i, sess in enumerate(reversed(st.session_state.ex_sessions)):
                idx   = len(st.session_state.ex_sessions) - 1 - i
                sclr  = "#22c55e" if sess.get("status") == "complete" else "#ef4444"
                sicon = "✅" if sess.get("status") == "complete" else "❌"
                with st.expander(
                    f"{sicon} {sess['name']}  ·  {sess.get('ts','')}  ·  "
                    f"{len(sess.get('summary_rows',[]))} pairs",
                    expanded=(i == 0)):
                    _analytics_card(sess)
