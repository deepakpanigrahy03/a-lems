# gui/pages/execute.py  — v6  (Phase 3)
# ─────────────────────────────────────────────────────────────────────────────
# FIXES vs v5:
#   1. Tax calc: reads from orchestration_tax_summary DB table — no stdout parsing
#   2. Live view: rendered ONLY when running/done, never bleeds into other pages
#      get_conn() is non-blocking with 2s timeout so startup never hangs
#   3. Timeline: static snapshot, no more full-box rerender flicker
#      Uses a stable key so Plotly doesn't recreate on every rerun
#   4. Run History: always at bottom, newest first, expandable beauty cards
#   5. Tab 2 just shows start button — live panel is ABOVE tabs
#   6. _parse_summary kept as fallback only; primary source is DB
# ─────────────────────────────────────────────────────────────────────────────

import math, subprocess, time as _time, re as _re, threading, os, signal
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from gui.components.session_tree import render_session_tree

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
    "stop":        False,
    "current_cmd": "",
    "timeline_snap": None,   # cached Plotly fig — only rebuilt when group_id changes
    "timeline_gid":  "",     # which group_id the snap belongs to
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
# SESSION STATE
# ══════════════════════════════════════════════════════════════════════════════

def _init_state():
    with _STORE_LOCK:
        snap = dict(_STORE)
    for _k in ("running","done","phase","progress","log","metrics",
               "result_rows","group_id","run_record","sessions"):
        st.session_state[f"ex_{_k}"] = snap.get(_k)
    for _k in ("queue","saved"):
        if f"ex_{_k}" not in st.session_state:
            st.session_state[f"ex_{_k}"] = snap.get(_k, [])
        with _STORE_LOCK:
            _STORE[_k] = list(st.session_state[f"ex_{_k}"])
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
# TASK LOADER
# ══════════════════════════════════════════════════════════════════════════════

def _load_tasks() -> tuple:
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

    try:
        df = q("SELECT DISTINCT task_name FROM experiments "
               "WHERE task_name IS NOT NULL ORDER BY task_name")
        ids = df.task_name.tolist() if not df.empty else []
    except Exception:
        ids = []

    if not ids:
        st.error("❌ No tasks found in config/tasks.yaml and DB is empty.")
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
# FIX #3: STABLE GANTT — built once per group_id, cached in _STORE
# No more full-box rerender every 1s. Only rebuilds when gid changes.
# ══════════════════════════════════════════════════════════════════════════════

def _gantt_chart_stable(group_id: str):
    """Render timeline only when group_id changes — prevents blinking."""
    if not group_id:
        return

    # Return cached fig if same group
    cached_gid = _store_get("timeline_gid", "")
    cached_fig = _store_get("timeline_snap", None)
    if cached_gid == group_id and cached_fig is not None:
        st.plotly_chart(cached_fig, use_container_width=True,
                        key=f"gantt_{group_id}")
        return

    # Build fresh
    try:
        exps = q(f"""
            SELECT exp_id, task_name, provider, status, started_at, completed_at
            FROM experiments WHERE group_id = '{group_id}' ORDER BY exp_id
        """)
    except Exception:
        return
    if exps is None or exps.empty:
        return

    bars = []
    now  = datetime.utcnow()
    first_start = None

    for _, row in exps.iterrows():
        try:
            s = datetime.fromisoformat(str(row.started_at)) if row.started_at else None
            e = datetime.fromisoformat(str(row.completed_at)) if row.completed_at else None
            if s is None:
                continue
            if first_start is None:
                first_start = s
            end   = e if e else now
            dur_s = max((end - s).total_seconds(), 0.5)
            offset= (s - first_start).total_seconds()
            st_db = str(row.get("status", "")).lower()
            clr   = {"completed": "#3b82f6", "running": "#22c55e",
                     "failed": "#ef4444", "error": "#ef4444"}.get(st_db, "#4b5563")
            bars.append({
                "label":  f"exp_{row.exp_id} · {row.provider[:3]} · {str(row.task_name)[:14]}",
                "start":  offset,
                "dur":    dur_s,
                "color":  clr,
                "status": st_db,
            })
        except Exception:
            continue

    if not bars:
        return

    fig = go.Figure()
    for bar in reversed(bars):
        fig.add_trace(go.Bar(
            name=bar["label"],
            x=[bar["dur"]], y=[bar["label"]],
            base=bar["start"],
            orientation="h",
            marker_color=bar["color"],
            marker_opacity=0.85,
            showlegend=False,
            hovertemplate=(f"{bar['label']}<br>{bar['dur']:.1f}s"
                           f"<br>{bar['status']}<extra></extra>"),
        ))

    _pl_g = {k: v for k, v in PL.items() if k != 'margin'}
    fig.update_layout(
        **_pl_g,
        height=max(80 + len(bars) * 26, 120),
        barmode="overlay",
        xaxis_title="Elapsed (s)",
        margin=dict(l=10, r=10, t=20, b=24),
        title=dict(text="⏱ Timeline", font=dict(size=9), x=0),
    )

    _store_set("timeline_snap", fig)
    _store_set("timeline_gid",  group_id)
    st.plotly_chart(fig, use_container_width=True, key=f"gantt_{group_id}")


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND RUN THREAD
# ══════════════════════════════════════════════════════════════════════════════

def _thread_run_local(_first_exp: dict, sid: str):
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
                # Invalidate timeline cache so it rebuilds for new group
                if gid != _store_get("timeline_gid", ""):
                    _store_set("timeline_snap", None)
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

        # FIX #1: Read tax from DB — no more stdout parsing
        gid = _store_get("group_id", "")
        rows = _load_tax_from_db(gid) if gid else []
        _store_set("result_rows", rows)

        _store_append("sessions", {
            "sid":          exp_sid,
            "name":         exp.get("name", "?"),
            "status":       "complete" if rc == 0 else "error",
            "log":          _store_get("log", []).copy(),
            "summary_rows": rows,
            "ts":           _time.strftime("%H:%M:%S"),
            "rc":           rc,
            "group_id":     gid,
        })
        return rc

    _store_set("running", True)
    _store_set("done",    False)

    _run_one(_first_exp, sid)

    # Safety cap: never drain more than 50 queued experiments per thread start
    _max_drain = 50
    _drained   = 0
    while _drained < _max_drain:
        if _store_get("stop", False):
            break
        with _STORE_LOCK:
            q_list = _STORE.get("queue", [])
            if not q_list:
                break
            nxt = q_list.pop(0)
        _drained += 1
        _run_one(nxt, f"ses_{int(_time.time()*1000)}")

    _store_set("running", False)
    _store_set("done",    True)


# ══════════════════════════════════════════════════════════════════════════════
# FIX #1: TAX FROM DB — agentic_energy / linear_energy, always > 1
# ══════════════════════════════════════════════════════════════════════════════

def _load_tax_from_db(group_id: str) -> list:
    """
    Load orchestration tax directly from orchestration_tax_summary.
    tax_multiplier = agentic_dynamic_uj / linear_dynamic_uj  → always >= 1
    Falls back to computing from runs table if summary table is empty.
    """
    if not group_id:
        return []
    try:
        df = q(f"""
            SELECT
                el.provider,
                el.task_name                          AS task,
                rl.total_energy_uj / 1e6              AS linear_j,
                ra.total_energy_uj / 1e6              AS agentic_j,
                -- FIX: agentic / linear, always >= 1
                CASE WHEN rl.total_energy_uj > 0
                     THEN CAST(ra.total_energy_uj AS REAL) / rl.total_energy_uj
                     ELSE 1.0 END                     AS tax_x,
                '' AS ci
            FROM orchestration_tax_summary ots
            JOIN runs rl ON ots.linear_run_id  = rl.run_id
            JOIN runs ra ON ots.agentic_run_id = ra.run_id
            JOIN experiments el ON rl.exp_id = el.exp_id
            WHERE el.group_id = '{group_id}'
            ORDER BY ots.comparison_id
        """)
        if df is not None and not df.empty:
            return df.to_dict("records")
    except Exception:
        pass

    # Fallback: compute from runs directly
    try:
        df2 = q(f"""
            SELECT
                e.provider,
                e.task_name                                      AS task,
                AVG(CASE WHEN r.workflow_type='linear'
                         THEN r.total_energy_uj/1e6 END)         AS linear_j,
                AVG(CASE WHEN r.workflow_type='agentic'
                         THEN r.total_energy_uj/1e6 END)         AS agentic_j
            FROM runs r
            JOIN experiments e ON r.exp_id = e.exp_id
            WHERE e.group_id = '{group_id}'
            GROUP BY e.provider, e.task_name
            HAVING linear_j IS NOT NULL AND agentic_j IS NOT NULL
        """)
        if df2 is not None and not df2.empty:
            df2["tax_x"] = df2["agentic_j"] / df2["linear_j"].clip(lower=1e-9)
            df2["ci"] = ""
            return df2.to_dict("records")
    except Exception:
        pass
    return []


# ══════════════════════════════════════════════════════════════════════════════
# LIVE VIEW WIDGET
# ══════════════════════════════════════════════════════════════════════════════

def _render_live_view():
    _running = _store_get('running', False)
    _done    = _store_get('done',    False)
    if not (_running or _done):
        return
    st.session_state.ex_running = _running
    st.session_state.ex_done    = _done

    rec   = _store_get('run_record') or {}
    phase = _store_get('phase', 'idle')
    prog  = _store_get('progress', 0.0)
    log   = _store_get('log', [])
    m     = _store_get('metrics', {})
    gid   = _store_get('group_id', '')

    pc = {"starting": "#7090b0", "planning": "#f59e0b",
          "execution": "#3b82f6", "synthesis": "#a78bfa",
          "running": "#22c55e", "complete": "#22c55e",
          "error": "#ef4444", "idle": "#3d5570"}.get(phase, "#7090b0")

    hdr_col, stop_col = st.columns([5, 1])
    cur_cmd = _store_get('current_cmd', '')
    hdr_col.markdown(
        f"<div style='background:#080d18;border:1px solid {pc}44;"
        f"border-left:4px solid {pc};border-radius:5px;"
        f"padding:8px 14px;margin:2px 0;display:flex;align-items:center;gap:14px;'>"
        f"<span style='font-size:10px;padding:3px 10px;background:{pc}22;"
        f"border:1px solid {pc};border-radius:4px;color:{pc};font-weight:700;'>"
        f"● {phase.upper()}</span>"
        f"<span style='font-size:10px;color:#e8f0f8;margin-left:6px;font-weight:600;'>"
        f"{rec.get('name','')}</span>"
        f"<span style='font-size:9px;color:#3d5570;margin-left:8px;'>{int(prog*100)}%</span>"
        f"{'<span style=\"font-size:9px;color:#22c55e;margin-left:8px;\">⚡ RUNNING</span>' if _running else ''}"
        f"</div>",
        unsafe_allow_html=True)

    if _running:
        if stop_col.button('⏹ Stop', type='secondary', use_container_width=True,
                           key='stop_run_btn'):
            _store_set('stop', True)
            st.warning('Stop signal sent.')

    if cur_cmd:
        st.markdown(
            f"<div style='font-family:monospace;font-size:9px;color:#3d5570;"
            f"background:#05080f;border:1px solid #1e2d45;border-radius:4px;"
            f"padding:4px 10px;margin-bottom:4px;overflow-x:auto;white-space:nowrap;'>"
            f"$ {cur_cmd[:200]}</div>",
            unsafe_allow_html=True)
    st.progress(prog)

    left_col, right_col = st.columns([1, 1])

    with left_col:
        st.markdown(
            "<div style='font-size:10px;font-weight:700;color:#7090b0;"
            "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
            "🌳 Session Tree</div>", unsafe_allow_html=True)
        render_session_tree(group_id=gid, expanded=True,
                            live_log=_store_get('log', []), key_suffix="live")
        st.markdown(
            "<div style='font-size:10px;font-weight:700;color:#7090b0;"
            "text-transform:uppercase;letter-spacing:.08em;margin:6px 0 2px;'>"
            "⏱ Timeline</div>", unsafe_allow_html=True)
        # FIX #3: stable chart — no blink
        _gantt_chart_stable(gid)

    with right_col:
        st.markdown(
            "<div style='font-size:10px;font-weight:700;color:#7090b0;"
            "text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px;'>"
            "⚡ Live Telemetry</div>", unsafe_allow_html=True)
        st.markdown(
            f"<div style='display:flex;justify-content:space-around;'>"
            f"{_gauge_svg(m.get('pkg_w',0),0,80,'Pkg Power','W','#3b82f6',warn=50,danger=70)}"
            f"{_gauge_svg(m.get('core_w',0),0,60,'Core Power','W','#22c55e',warn=40,danger=55)}"
            f"{_gauge_svg(m.get('temp_c',0),30,105,'Pkg Temp','°C','#f59e0b',warn=80,danger=95)}"
            f"</div>", unsafe_allow_html=True)
        st.markdown(
            _bar_gauge(m.get('util', 0),   100,   "CPU Util",  "%",        "#38bdf8") +
            _bar_gauge(min(m.get('irq', 0), 50000), 50000, "IRQ Rate", "/s", "#f59e0b") +
            _bar_gauge(m.get('ipc', 0),    3.0,   "IPC",       "inst/cyc", "#a78bfa"),
            unsafe_allow_html=True)

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
                f"{log_html}</div>", unsafe_allow_html=True)

    if _running:
        st.rerun()

    if st.session_state.ex_done and st.session_state.ex_result_rows:
        st.divider()
        st.markdown("### 📊 Results")
        _analytics_card({
            "sid":          rec.get("sid", ""),
            "summary_rows": st.session_state.ex_result_rows,
            "log":          log,
        })
        st.session_state.ex_done = False

    if not _running and st.session_state.ex_queue:
        st.info(f"⏳ {len(st.session_state.ex_queue)} more queued — click ▶ Start again.")


# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICS CARD  (uses DB-sourced tax rows — always agentic/linear >= 1)
# ══════════════════════════════════════════════════════════════════════════════

def _analytics_card(session: dict):
    rows  = session.get("summary_rows", [])
    lines = session.get("log", [])
    sid   = session.get("sid", "x")
    gid   = session.get("group_id", "")

    # If no rows in memory, try loading from DB using group_id
    if not rows and gid:
        rows = _load_tax_from_db(gid)

    if rows:
        _thresholds = INSIGHTS_RULES.get("tax_thresholds", {})
        def _tax_color(tx):
            if tx >= _thresholds.get("extreme", {}).get("min", 15):   return "#ef4444"
            if tx >= _thresholds.get("high",    {}).get("max", 15):   return "#f59e0b"
            if tx >= _thresholds.get("moderate",{}).get("max",  5):   return "#38bdf8"
            return "#22c55e"

        rh = ""
        for r in rows:
            lin_j = float(r.get("linear_j", 0) or 0)
            age_j = float(r.get("agentic_j", 0) or 0)
            tax_x = float(r.get("tax_x", 0) or 0)
            # Ensure tax is always agentic/linear
            if lin_j > 0 and age_j > 0:
                tax_x = age_j / lin_j
            tc  = _tax_color(tax_x)
            mx  = max(lin_j, age_j, 0.001)
            lw  = lin_j / mx * 100
            aw  = age_j / mx * 100
            hi   = _human_energy(age_j)
            hi_s = hi[0][1] if hi else ""
            rh += (
                f"<tr style='border-bottom:1px solid #111827;'>"
                f"<td style='padding:9px 8px;font-size:10px;color:#7090b0;'>"
                f"{r.get('provider','')}</td>"
                f"<td style='padding:9px 8px;font-size:10px;color:#e8f0f8;min-width:140px;'>"
                f"{r.get('task','')}</td>"
                f"<td style='padding:9px 8px;'>"
                f"<div style='font-size:11px;color:#22c55e;font-family:monospace;'>"
                f"{lin_j:.4f} J</div>"
                f"<div style='background:#1e2d45;border-radius:2px;height:5px;width:110px;margin-top:3px;'>"
                f"<div style='background:#22c55e;width:{lw:.0f}%;height:100%;border-radius:2px;'></div></div>"
                f"</td><td style='padding:9px 8px;'>"
                f"<div style='font-size:11px;color:#ef4444;font-family:monospace;'>"
                f"{age_j:.4f} J</div>"
                f"<div style='background:#1e2d45;border-radius:2px;height:5px;width:110px;margin-top:3px;'>"
                f"<div style='background:#ef4444;width:{aw:.0f}%;height:100%;border-radius:2px;'></div></div>"
                f"</td><td style='padding:9px 8px;text-align:center;'>"
                f"<span style='font-size:14px;font-weight:700;color:{tc};font-family:monospace;'>"
                f"{tax_x:.2f}×</span>"
                f"</td><td style='padding:9px 8px;font-size:9px;color:#7090b0;'>"
                f"{hi_s}</td>"
                f"</tr>"
            )

        st.markdown(
            "<div style='background:#07090f;border:1px solid #1e2d45;border-radius:8px;"
            "overflow:hidden;margin:10px 0;'>"
            "<div style='background:#0a0e1a;padding:8px 14px;border-bottom:1px solid #1e2d45;"
            "font-size:10px;font-weight:700;color:#4fc3f7;letter-spacing:.08em;"
            "text-transform:uppercase;'>⚡ Agentic vs Linear — Energy Overhead</div>"
            "<table style='width:100%;border-collapse:collapse;'>"
            "<thead><tr style='background:#0a0e1a;border-bottom:2px solid #1e2d45;'>"
            "<th style='padding:7px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>Provider</th>"
            "<th style='padding:7px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>Task</th>"
            "<th style='padding:7px 8px;font-size:9px;color:#22c55e;text-align:left;text-transform:uppercase;'>Linear</th>"
            "<th style='padding:7px 8px;font-size:9px;color:#ef4444;text-align:left;text-transform:uppercase;'>Agentic</th>"
            "<th style='padding:7px 8px;font-size:9px;color:#f59e0b;text-align:center;text-transform:uppercase;'>Tax (A/L)</th>"
            "<th style='padding:7px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;'>Insight</th>"
            f"</tr></thead><tbody>{rh}</tbody></table></div>",
            unsafe_allow_html=True,
        )

        if len(rows) > 1:
            tax_vals = [age_j / max(lin_j, 1e-9)
                        for r in rows
                        for lin_j, age_j in [(float(r.get("linear_j",0) or 0),
                                              float(r.get("agentic_j",0) or 0))]]
            best_r  = min(rows, key=lambda r: float(r.get("agentic_j",0) or 0) /
                          max(float(r.get("linear_j",0) or 1), 1e-9))
            worst_r = max(rows, key=lambda r: float(r.get("agentic_j",0) or 0) /
                          max(float(r.get("linear_j",0) or 1), 1e-9))
            avg_t = sum(tax_vals) / len(tax_vals)
            best_t  = float(best_r.get("agentic_j",0)) / max(float(best_r.get("linear_j",1)), 1e-9)
            worst_t = float(worst_r.get("agentic_j",0)) / max(float(worst_r.get("linear_j",1)), 1e-9)
            c1, c2, c3 = st.columns(3)
            c1.success(f"**✅ Lowest overhead**\n\n{best_r.get('provider','')} · "
                       f"{str(best_r.get('task',''))[:24]}\n\n**{best_t:.2f}×**")
            c2.error(f"**⚠ Highest overhead**\n\n{worst_r.get('provider','')} · "
                     f"{str(worst_r.get('task',''))[:24]}\n\n**{worst_t:.2f}×**")
            c3.info(f"**📈 Average**\n\n{len(rows)} comparisons · **{avg_t:.2f}×** mean tax")

        df  = pd.DataFrame(rows)
        df["label"] = df["provider"].astype(str) + " · " + df["task"].astype(str).str[:22]
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Linear",  x=df["label"], y=df["linear_j"],
                             marker_color="#22c55e", text=df["linear_j"].round(3),
                             textposition="outside", textfont=dict(size=8)))
        fig.add_trace(go.Bar(name="Agentic", x=df["label"], y=df["agentic_j"],
                             marker_color="#ef4444", text=df["agentic_j"].round(3),
                             textposition="outside", textfont=dict(size=8)))
        _pl2 = {k: v for k, v in PL.items() if k != 'margin'}
        fig.update_layout(**_pl2, barmode="group", height=260,
                          title="Linear vs Agentic energy",
                          xaxis_tickangle=20, margin=dict(t=40, b=10))
        import time as _t
        st.plotly_chart(fig, use_container_width=True,
                        key=f'ac_{sid}_{_t.time_ns()}')

        csv = df[["provider", "task", "linear_j", "agentic_j"]].copy()
        csv["tax_x"] = csv["agentic_j"] / csv["linear_j"].clip(lower=1e-9)
        st.download_button("📥 Export CSV", csv.to_csv(index=False),
                           file_name=f"alems_{sid}.csv",
                           mime="text/csv", key=f"csv_{sid}_{__import__('time').time_ns()}")
    else:
        st.info("No tax data found yet. Results appear after the experiment completes.")

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
# REMOTE EXECUTION
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
                st.success("✅ Remote run complete.")
                # Load tax from DB after remote run completes
                gid = data.get("group_id", "")
                summary_rows = _load_tax_from_db(gid) if gid else []
            else:
                st.error(f"Run ended: {status}")
            return (0 if status == "complete" else 1), lines, summary_rows

    st.warning("Polling timed out."); return -1, lines, []


# ══════════════════════════════════════════════════════════════════════════════
# FIX #5 & #6: BEAUTIFUL RUN HISTORY CARD
# ══════════════════════════════════════════════════════════════════════════════

def _history_card(sess: dict, idx: int, expanded: bool):
    """Beautiful history card for Tab 4."""
    status  = sess.get("status", "?")
    is_ok   = status == "complete"
    clr     = "#22c55e" if is_ok else "#ef4444"
    icon    = "✅" if is_ok else "❌"
    name    = sess.get("name", "Run")
    ts      = sess.get("ts", "")
    rows    = sess.get("summary_rows", [])
    gid     = sess.get("group_id", "")
    n_pairs = len(rows)

    # Load from DB if in-memory rows empty
    if not rows and gid:
        rows = _load_tax_from_db(gid)
        n_pairs = len(rows)

    header = f"{icon}  {name}  ·  {ts}  ·  {n_pairs} pair{'s' if n_pairs != 1 else ''}"

    with st.expander(header, expanded=expanded):
        # Card header bar
        st.markdown(
            f"<div style='background:#080c18;border:1px solid {clr}33;"
            f"border-left:4px solid {clr};border-radius:6px;"
            f"padding:8px 14px;margin-bottom:10px;display:flex;"
            f"align-items:center;gap:16px;flex-wrap:wrap;'>"
            f"<span style='font-size:11px;font-weight:700;color:{clr};'>"
            f"{icon} {status.upper()}</span>"
            f"<span style='font-size:11px;color:#c8d8e8;font-weight:600;'>{name}</span>"
            f"<span style='font-size:10px;color:#3d5570;font-family:monospace;'>{ts}</span>"
            f"{'<span style=\"font-size:10px;color:#4b6080;font-family:monospace;\">'+gid+'</span>' if gid else ''}"
            f"</div>",
            unsafe_allow_html=True)

        if rows:
            # Mini tax summary grid
            cols = st.columns(min(len(rows), 3))
            for ci, r in enumerate(rows[:3]):
                lin_j = float(r.get("linear_j", 0) or 0)
                age_j = float(r.get("agentic_j", 0) or 0)
                tax   = age_j / max(lin_j, 1e-9)
                t_clr = "#ef4444" if tax > 10 else "#f59e0b" if tax > 5 else "#22c55e"
                with cols[ci]:
                    st.markdown(
                        f"<div style='background:#07090f;border:1px solid #1e2d45;"
                        f"border-radius:6px;padding:10px 12px;text-align:center;'>"
                        f"<div style='font-size:9px;color:#5a7090;margin-bottom:4px;'>"
                        f"{r.get('provider','')} · {str(r.get('task',''))[:18]}</div>"
                        f"<div style='font-size:22px;font-weight:800;color:{t_clr};"
                        f"font-family:monospace;line-height:1;'>{tax:.2f}×</div>"
                        f"<div style='font-size:8px;color:#3d5570;margin-top:4px;'>"
                        f"Linear {lin_j:.3f}J → Agentic {age_j:.3f}J</div>"
                        f"</div>",
                        unsafe_allow_html=True)

            st.markdown("")
            _analytics_card(dict(sess, summary_rows=rows))
        else:
            st.caption("No tax data available for this run.")


# ══════════════════════════════════════════════════════════════════════════════
# FIX #2: NON-BLOCKING get_conn()
# ══════════════════════════════════════════════════════════════════════════════

def _get_conn_safe() -> dict:
    """get_conn with 2s timeout so startup never hangs."""
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(get_conn)
        try:
            return fut.result(timeout=2)
        except Exception:
            return {}


# ══════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════════════════════

def render(ctx: dict):
    _init_state()
    _load_queue()

    st.title("Execute Run")

    # FIX #2: non-blocking mode banner
    _conn = _get_conn_safe()
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
            f"<span style='color:#3d5570;font-size:9px;'>Tunnel: {_conn.get('url','')}</span></div>",
            unsafe_allow_html=True)
    else:
        st.markdown(
            "<div style='background:#0a0f1a;border:1px solid #1e2d45;"
            "border-left:3px solid #3b82f6;border-radius:4px;"
            "padding:8px 14px;margin-bottom:10px;font-size:11px;'>"
            "⚫ <b style='color:#3b82f6'>LOCAL MODE</b>  ·  "
            "<span style='color:#5a7090'>Runs execute on this machine.</span></div>",
            unsafe_allow_html=True)

    # Queue banner
    # FIX: Only sync session_state.ex_queue → _STORE when NOT running.
    # If running, the thread owns _STORE["queue"]; syncing overwrites pops done by thread
    # and causes infinite re-queuing.
    if 'ex_queue' not in st.session_state:
        st.session_state.ex_queue = _store_get('queue', [])
    elif not _store_get('running', False):
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

    # Live view is now INSIDE tab 2 only — see TAB 2 block below
    st.session_state.ex_running = _store_get('running', False)
    st.session_state.ex_done    = _store_get('done', False)

    all_tasks, _cat_map, _name_map = _load_tasks()

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
                                            format_func=lambda i: task_labels[i],
                                            key="h_task_idx")
                h_task    = all_tasks[h_task_idx]
                h_prov    = st.selectbox("Provider", ["cloud", "local"], key="h_prov")
                h_reps    = st.number_input("Repetitions", 1, 100, 3, key="h_reps")
                h_country = st.selectbox("Region",
                    ["US","DE","FR","NO","IN","AU","GB","CN","BR"],
                    format_func=lambda x: {"US":"🇺🇸 US","DE":"🇩🇪 DE","FR":"🇫🇷 FR",
                        "NO":"🇳🇴 NO","IN":"🇮🇳 IN","AU":"🇦🇺 AU",
                        "GB":"🇬🇧 GB","CN":"🇨🇳 CN","BR":"🇧🇷 BR"}.get(x, x),
                    key="h_country")
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
                        "GB":"🇬🇧 GB","CN":"🇨🇳 CN","BR":"🇧🇷 BR"}.get(x, x),
                    key="b_country")
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

            if c2.button("▶ Run Now", type="primary", use_container_width=True,
                         key="ex_run_now"):
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
                        f"<div style='font-size:12px;font-weight:600;color:#e8f0f8;'>"
                        f"{exp['name']}</div>"
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
                    st.success(f"Queued {len(st.session_state.ex_saved)} experiments")
                    st.rerun()

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
    # TAB 2 — LIVE EXECUTION (start button only)
    # ══════════════════════════════════════════════════════════════════════════
    with tab2:
        conn = _get_conn_safe()

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
                    # FIX: sync remaining queue to _STORE ONCE before starting thread
                    # then clear session queue so rerun loop can't re-add items
                    remaining = list(st.session_state.ex_queue)  # already popped first exp above
                    _store_set('queue', remaining)
                    st.session_state.ex_queue.clear()
                    _save_queue()
                    t = threading.Thread(
                        target=_thread_run_local, args=(exp, sid), daemon=True)
                    t.start()
                    st.session_state.ex_thread = t
                    st.rerun()

        elif not st.session_state.ex_running and not st.session_state.ex_queue:
            if not (st.session_state.ex_running or st.session_state.ex_done):
                st.info("Queue is empty. Go to 📋 Create & Queue to add experiments.")

        if st.session_state.ex_running or st.session_state.ex_done:
            _render_live_view()



    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — SESSION ANALYSIS
    # ══════════════════════════════════════════════════════════════════════════
    with tab3:
        from gui.pages.session_analysis import render_session_analysis
        st.markdown("### 📊 Session Analysis")

        try:
            recent = q("""
                SELECT
                    e.group_id,
                    COUNT(DISTINCT e.exp_id)                                  AS n_exps,
                    COUNT(DISTINCT r.run_number ||'_'|| CAST(e.exp_id AS TEXT)) AS n_runs,
                    MAX(e.created_at)                                          AS latest
                FROM experiments e
                LEFT JOIN runs r ON r.exp_id = e.exp_id
                GROUP BY e.group_id
                ORDER BY MAX(e.exp_id) DESC
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
                render_session_tree(sel_gid, expanded=False, key_suffix="hist")
                render_session_analysis(sel_gid)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 — RUN HISTORY  (FIX #5: always below, beautiful cards)
    # ══════════════════════════════════════════════════════════════════════════
    with tab4:
        sessions = list(st.session_state.ex_sessions or [])

        if not sessions:
            st.markdown(
                "<div style='text-align:center;padding:60px 0;'>"
                "<div style='font-size:48px;margin-bottom:16px;'>📭</div>"
                "<div style='font-size:16px;color:#4b6080;font-weight:600;'>"
                "No runs yet</div>"
                "<div style='font-size:12px;color:#3d5570;margin-top:6px;'>"
                "Completed runs will appear here as expandable cards</div>"
                "</div>",
                unsafe_allow_html=True)
        else:
            # Newest first
            sessions_rev = list(reversed(sessions))
            n = len(sessions_rev)

            st.markdown(
                f"<div style='display:flex;align-items:center;gap:12px;margin-bottom:16px;'>"
                f"<div style='font-size:11px;font-weight:700;color:#7090b0;"
                f"text-transform:uppercase;letter-spacing:.1em;'>Run History</div>"
                f"<div style='background:#1e2d45;border-radius:10px;padding:2px 10px;"
                f"font-size:10px;color:#4fc3f7;font-weight:600;'>{n} run{'s' if n!=1 else ''}</div>"
                f"</div>",
                unsafe_allow_html=True)

            for i, sess in enumerate(sessions_rev):
                _history_card(sess, idx=i, expanded=(i == 0))
