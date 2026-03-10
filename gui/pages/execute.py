"""
gui/pages/execute.py  —  ▶  Execute Run
Render function: render(ctx)
ctx keys: ov, runs, tax, lin, age, avg_lin_j, avg_age_j, tax_mult,
          plan_ms, exec_ms, synth_ms, plan_pct, exec_pct, synth_pct
"""
import subprocess
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from gui.config     import PROJECT_ROOT, DB_PATH, LIVE_API, WF_COLORS, PL
from gui.connection import get_conn, is_online, api_post, api_get
from gui.db      import q, q_safe, q1
from gui.helpers import fl, _human_energy, _human_water, _human_carbon, _gauge_html, _bar_gauge_html

try:
    import requests as _req
    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False
    class _req:
        @staticmethod
        def get(*a, **kw): raise RuntimeError("requests not installed")
try:
    import yaml as _yaml
    _YAML_OK = True
except ImportError:
    _YAML_OK = False


def render(ctx: dict):
    ov        = ctx["ov"]
    runs      = ctx["runs"]
    tax       = ctx["tax"]
    avg_lin_j = ctx["avg_lin_j"]
    avg_age_j = ctx["avg_age_j"]
    tax_mult  = ctx["tax_mult"]
    plan_ms   = ctx["plan_ms"]
    exec_ms   = ctx["exec_ms"]
    synth_ms  = ctx["synth_ms"]
    plan_pct  = ctx["plan_pct"]
    exec_pct  = ctx["exec_pct"]
    synth_pct = ctx["synth_pct"]
    lin       = ctx["lin"]
    age       = ctx["age"]

    st.title("Execute Run")

    # ── Mode banner ──────────────────────────────────────────────────────────
    _conn = get_conn()
    if _conn.get("verified"):
        _hclr = "#22c55e" if _conn.get("harness") else "#f59e0b"
        _hmsg = "Harness ready — runs will execute on the lab machine" \
                if _conn.get("harness") else "Server reachable but harness not loaded"
        st.markdown(
            f"<div style='background:#0a2010;border:1px solid #22c55e33;"
            f"border-left:3px solid #22c55e;border-radius:4px;"
            f"padding:8px 14px;margin-bottom:12px;font-size:11px;'>"
            f"🟢 <b style='color:#22c55e'>LIVE MODE</b>  ·  "
            f"<span style='color:{_hclr}'>{_hmsg}</span><br/>"
            f"<span style='color:#3d5570;font-size:9px;'>Tunnel: {_conn['url']}</span>"
            f"</div>",
            unsafe_allow_html=True)
    else:
        st.markdown(
            "<div style='background:#0a0f1a;border:1px solid #1e2d45;"
            "border-left:3px solid #3b82f6;border-radius:4px;"
            "padding:8px 14px;margin-bottom:12px;font-size:11px;'>"
            "⚫ <b style='color:#3b82f6'>LOCAL MODE</b>  ·  "
            "<span style='color:#5a7090'>Runs execute on this machine. "
            "Connect to Live Lab in the sidebar to trigger remote runs.</span>"
            "</div>",
            unsafe_allow_html=True)
    st.caption(f"Project root: `{PROJECT_ROOT}`  ·  venv must be activated before starting Streamlit")

    # ── Available tasks (from DB + tasks.yaml + presets) ─────────────────────
    _tl = q("SELECT DISTINCT task_name FROM experiments WHERE task_name IS NOT NULL ORDER BY task_name")
    _known_db = _tl.task_name.tolist() if not _tl.empty else []

    # Also load from tasks.yaml for tasks not yet run
    _yaml_tasks = []
    try:
        if _YAML_OK:
            import yaml as _yaml_exec
            _ty = _yaml_exec.safe_load(open(PROJECT_ROOT / "config" / "tasks.yaml"))
            _yaml_tasks = [t.get("id","") for t in (_ty or {}).get("tasks",[]) if t.get("id")]
    except Exception:
        pass

    PRESET_TASKS = ["simple","capital","research_summary","code_generation",
                    "stock_lookup","comparative_research","deep_research"]
    all_tasks = list(dict.fromkeys(PRESET_TASKS + _yaml_tasks + _known_db))
    # Category map for display
    _cat_map = {}
    try:
        if _YAML_OK:
            _ty2 = _yaml_exec.safe_load(open(PROJECT_ROOT / "config" / "tasks.yaml"))
            _cat_map = {t.get("id",""):t.get("category","") for t in (_ty2 or {}).get("tasks",[])}
    except Exception:
        pass

    # ── Two modes: batch (run_experiment) vs single (test_harness) ───────────
    tab_batch, tab_single = st.tabs([
        "⚡ Batch — run_experiment (multi-task, multi-provider)",
        "🔬 Single — test_harness (one task, fine-grained)",
    ])

    # ── Gauge helpers (pure HTML/CSS — no JS needed) ──────────────────────────
    def _gauge_html(value, vmin, vmax, label, unit, color, warn=None, danger=None):
        """Render an SVG arc speedometer gauge."""
        pct   = max(0, min(1, (value - vmin) / max(vmax - vmin, 1e-9)))
        angle = -140 + pct * 280          # arc from -140° to +140°
        rad   = 3.14159265 / 180
        r     = 52
        cx, cy = 60, 62
        # arc end point
        ex = cx + r * __import__('math').sin(angle * rad)
        ey = cy - r * __import__('math').cos(angle * rad)
        large = 1 if pct > 0.5 else 0
        # Determine needle color
        if danger and value >= danger:
            nclr = "#ef4444"
        elif warn and value >= warn:
            nclr = "#f59e0b"
        else:
            nclr = color
        # Background arc
        bx = cx + r * __import__('math').sin(140 * rad)
        by = cy - r * __import__('math').cos(140 * rad)
        ex0 = cx - r * __import__('math').sin(140 * rad)
        ey0 = cy - r * __import__('math').cos(140 * rad)
        return f"""
        <div style="text-align:center;padding:4px 0;">
          <svg width="120" height="90" viewBox="0 0 120 90">
            <path d="M {bx:.1f} {by:.1f} A {r} {r} 0 1 1 {ex0:.1f} {ey0:.1f}"
                  fill="none" stroke="#1e2d45" stroke-width="8" stroke-linecap="round"/>
            <path d="M {bx:.1f} {by:.1f} A {r} {r} 0 {large} 1 {ex:.1f} {ey:.1f}"
                  fill="none" stroke="{nclr}" stroke-width="8" stroke-linecap="round"/>
            <circle cx="{cx}" cy="{cy}" r="4" fill="{nclr}"/>
            <text x="{cx}" y="{cy+4}" text-anchor="middle"
                  font-size="14" font-weight="700" fill="#e8f0f8"
                  font-family="monospace">{value:.1f}</text>
            <text x="{cx}" y="{cy+18}" text-anchor="middle"
                  font-size="7" fill="#7090b0">{unit}</text>
            <text x="{cx}" y="82" text-anchor="middle"
                  font-size="8" font-weight="600" fill="{nclr}">{label}</text>
            <text x="6"  y="72" text-anchor="middle" font-size="6" fill="#3d5570">{vmin}</text>
            <text x="114" y="72" text-anchor="middle" font-size="6" fill="#3d5570">{vmax}</text>
          </svg>
        </div>"""

    def _bar_gauge_html(value, vmax, label, unit, color):
        """Horizontal bar gauge for CPU util / IRQ."""
        pct = max(0, min(100, value / max(vmax, 1) * 100))
        return f"""
        <div style="margin:6px 0 10px;">
          <div style="display:flex;justify-content:space-between;
                      font-size:9px;color:#7090b0;margin-bottom:3px;">
            <span style="font-weight:600;color:#e8f0f8">{label}</span>
            <span style="font-family:monospace;color:{color}">{value:.0f} {unit}</span>
          </div>
          <div style="background:#1e2d45;border-radius:3px;height:8px;overflow:hidden;">
            <div style="background:{color};width:{pct:.1f}%;height:100%;
                        border-radius:3px;transition:width 0.3s;"></div>
          </div>
        </div>"""

    def _stream_and_gauge(cmd_parts, cwd, run_label=""):
        """
        Split-screen execution: terminal log (left 55%) + live gauges (right 45%).
        Polls server.py every 2s for live samples while process runs.
        Falls back gracefully if server is offline.
        """
        import math, time as _tm

        out_col, gauge_col = st.columns([11, 9])

        # ── Left: terminal ────────────────────────────────────────────────────
        with out_col:
            st.markdown(
                "<div style='font-size:10px;font-weight:600;color:#7090b0;"
                "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                "⬛ Terminal output</div>",
                unsafe_allow_html=True)
            prog_ph   = st.progress(0)
            status_ph = st.empty()
            out_ph    = st.empty()

        # ── Right: live gauges ────────────────────────────────────────────────
        with gauge_col:
            st.markdown(
                "<div style='font-size:10px;font-weight:600;color:#7090b0;"
                "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                "⚡ Live telemetry</div>",
                unsafe_allow_html=True)
            phase_ph   = st.empty()
            gauge_ph   = st.empty()
            bar_ph     = st.empty()
            insight_ph = st.empty()
            mini_ph    = st.empty()

        # ── Check server once ─────────────────────────────────────────────────
        _srv_live = False
        if _REQUESTS_OK:
            for _ep in ["/health", "/api/system/status", "/"]:
                try:
                    if _req.get(f"{LIVE_API}{_ep}", timeout=1).status_code < 500:
                        _srv_live = True
                        break
                except Exception:
                    pass

        # Initial gauge state
        _last_pw = _last_tp = _last_util = _last_irq = 0.0
        _last_core_w = _last_dram_w = 0.0
        _last_ipc = 0.0
        _phase_str = "starting"
        _last_rid = int(q1("SELECT COALESCE(MAX(run_id),0) AS n FROM runs").get("n",0))
        _energy_acc = []   # rolling pkg_w samples for human insight

        def _refresh_gauges(rid):
            nonlocal _last_pw, _last_tp, _last_util, _last_irq
            nonlocal _last_core_w, _last_dram_w, _last_ipc, _phase_str
            if not _REQUESTS_OK:
                return
            try:
                _er = _req.get(f"{LIVE_API}/api/runs/{rid}/samples/energy", timeout=2).json()
                _pw_rows = _er.get("power",[]) if isinstance(_er,dict) else []
                if _pw_rows:
                    _lp = _pw_rows[-1]
                    _last_pw     = float(_lp.get("pkg_w",    _last_pw))
                    _last_core_w = float(_lp.get("core_w",   _last_core_w))
                    _last_dram_w = float(_lp.get("dram_w",   _last_dram_w))
                    _energy_acc.append(_last_pw)
                    if len(_energy_acc) > 60: _energy_acc.pop(0)
            except Exception:
                pass
            try:
                _cr = _req.get(f"{LIVE_API}/api/runs/{rid}/samples/cpu", timeout=2).json()
                if isinstance(_cr,list) and _cr:
                    _lc = _cr[-1]
                    _last_tp   = float(_lc.get("package_temp", _last_tp))
                    _last_util = float(_lc.get("cpu_util_percent", _last_util))
                    _last_ipc  = float(_lc.get("ipc", _last_ipc))
            except Exception:
                pass
            try:
                _ir = _req.get(f"{LIVE_API}/api/runs/{rid}/samples/interrupts", timeout=2).json()
                if isinstance(_ir,list) and _ir:
                    _last_irq = float(_ir[-1].get("interrupts_per_sec", _last_irq))
            except Exception:
                pass

        def _draw_gauges():
            # Speedometer row: Pkg W · Core W · Temp °C
            _g1 = _gauge_html(_last_pw,    0, 80,  "Pkg Power",  "W",   "#3b82f6",
                               warn=50, danger=70)
            _g2 = _gauge_html(_last_core_w,0, 60,  "Core Power", "W",   "#22c55e",
                               warn=40, danger=55)
            _g3 = _gauge_html(_last_tp,    30, 105,"Package",    "°C",  "#f59e0b",
                               warn=80, danger=95)
            gauge_ph.markdown(
                f"<div style='display:flex;justify-content:space-around;'>"
                f"{_g1}{_g2}{_g3}</div>",
                unsafe_allow_html=True)

            # Bar gauges: CPU util, IRQ, IPC
            _b1 = _bar_gauge_html(_last_util, 100,  "CPU Util",  "%",    "#38bdf8")
            _b2 = _bar_gauge_html(min(_last_irq,50000), 50000,
                                              "IRQ Rate",  "/s",   "#f59e0b")
            _b3 = _bar_gauge_html(_last_ipc,  3.0,  "IPC",       "inst/cycle","#a78bfa")
            bar_ph.markdown(
                f"<div style='padding:0 8px'>{_b1}{_b2}{_b3}</div>",
                unsafe_allow_html=True)

            # Phase badge
            _pc = {"starting":"#7090b0","planning":"#f59e0b","execution":"#3b82f6",
                   "synthesis":"#a78bfa","llm_wait":"#38bdf8",
                   "complete":"#22c55e","running":"#22c55e"}.get(_phase_str,"#7090b0")
            phase_ph.markdown(
                f"<div style='font-size:10px;padding:4px 10px;background:{_pc}22;"
                f"border:1px solid {_pc};border-radius:4px;display:inline-block;"
                f"color:{_pc};margin-bottom:4px;'>"
                f"● Phase: <b>{_phase_str}</b></div>",
                unsafe_allow_html=True)

            # Human insight
            _avg_pw  = sum(_energy_acc)/len(_energy_acc) if _energy_acc else 0
            _est_j   = _avg_pw * len(_energy_acc) * 2  # ~2s per poll tick
            if _est_j > 0:
                _hi = _human_energy(_est_j)
                insight_ph.markdown(
                    "<div style='font-size:8px;color:#3d5570;margin-top:4px;'>"
                    "So far: "
                    + " · ".join(f"{ic} {d}" for ic, d in _hi[:2])
                    + "</div>", unsafe_allow_html=True)

        # ── Launch process ────────────────────────────────────────────────────
        lines = []
        try:
            proc = subprocess.Popen(
                cmd_parts, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, cwd=str(cwd), bufsize=1,
            )

            _poll_count = 0
            _gauge_every = 4   # update gauges every 4 lines (~2s at typical output rate)

            for raw in iter(proc.stdout.readline, ""):
                line = raw.rstrip()
                if not line:
                    continue
                lines.append(line)

                # Colour-code log lines
                lo = line.lower()
                _line_color = "#e8f0f8"
                if any(k in lo for k in ["error","fail","exception","traceback"]):
                    _line_color = "#ef4444"
                elif any(k in lo for k in ["complete","saved","done","✅"]):
                    _line_color = "#22c55e"
                elif any(k in lo for k in ["planning","plan"]):
                    _line_color = "#f59e0b"
                    _phase_str  = "planning"
                elif any(k in lo for k in ["execut","tool_call","tool call"]):
                    _line_color = "#3b82f6"
                    _phase_str  = "execution"
                elif any(k in lo for k in ["synth","finaliz"]):
                    _line_color = "#a78bfa"
                    _phase_str  = "synthesis"
                elif "run" in lo or "rep" in lo:
                    _phase_str  = "running"

                # Colour-code terminal output
                _colored = "".join(
                    f"<span style='color:{_line_color}'>{l}</span>\n"
                    for l in lines[-60:]
                )
                out_ph.markdown(
                    f"<div style='background:#050810;border:1px solid #1e2d45;"
                    f"border-radius:4px;padding:8px 12px;font-family:monospace;"
                    f"font-size:9px;line-height:1.5;height:340px;overflow-y:auto;'>"
                    f"{_colored}</div>",
                    unsafe_allow_html=True)

                # Progress heuristic
                for pat in ["rep ", "repetition ", "run "]:
                    if pat in lo and "/" in lo:
                        try:
                            seg = lo.split(pat)[-1].split("/")
                            d, t = int(seg[0].strip()), int(seg[1].split()[0])
                            prog_ph.progress(min(d/t, 1.0))
                            status_ph.caption(f"Rep {d}/{t}")
                        except Exception:
                            pass
                        break
                if any(k in lo for k in ["complete","saved","finished","done"]):
                    prog_ph.progress(1.0)
                    _phase_str = "complete"

                # Poll gauges periodically
                _poll_count += 1
                if _srv_live and _poll_count % _gauge_every == 0:
                    # Detect if a new run was created since we started
                    _new_rid = int(q1("SELECT COALESCE(MAX(run_id),0) AS n FROM runs").get("n",0))
                    if _new_rid > _last_rid:
                        _last_rid = _new_rid
                    _refresh_gauges(_last_rid)
                _draw_gauges()

            proc.wait()
            _phase_str = "complete" if proc.returncode == 0 else "error"
            _draw_gauges()

            # ── Final human-insight summary ───────────────────────────────────
            if _energy_acc:
                _total_j = sum(_energy_acc) * 2
                _hi_final = _human_energy(_total_j)
                mini_ph.markdown(
                    "<div style='background:#0f1520;border:1px solid #22c55e33;"
                    "border-radius:6px;padding:8px 12px;margin-top:6px;'>"
                    "<div style='font-size:9px;font-weight:600;color:#22c55e;"
                    "margin-bottom:4px;'>⚡ Run energy summary</div>"
                    + "".join(
                        f"<div style='font-size:9px;color:#b8c8d8;margin:2px 0;'>{ic} {d}</div>"
                        for ic, d in _hi_final
                    ) + "</div>", unsafe_allow_html=True)

            # ── Parse & render MASTER SUMMARY from terminal output ────────────
            # Looks for lines like:
            #   cloud   GSM8K Arithmetic   1.4600   4.1553   3.58x   [-0.82, 7.99]
            # that appear between the === MASTER SUMMARY === header and the next ===
            import re as _re
            _summary_rows = []
            _in_summary   = False
            _saved_file   = None
            for _line in lines:
                _ll = _line.strip()
                if "MASTER SUMMARY" in _ll:
                    _in_summary = True
                    continue
                if _in_summary and _ll.startswith("==="):
                    _in_summary = False
                    continue
                if _in_summary and _ll.startswith("---"):
                    continue
                if _in_summary and _ll and not _ll.startswith("Provider"):
                    # Try to parse a data row:
                    # provider  task_name  linear_j  agentic_j  tax  [ci_lo, ci_hi]
                    _m = _re.match(
                        r'^(\S+)\s+(.*?)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)x?\s*(\[.*?\])?',
                        _ll
                    )
                    if _m:
                        _prov, _task, _lin, _age, _tax, _ci = _m.groups()
                        _lin_j = float(_lin)
                        _age_j = float(_age)
                        _tax_x = float(_tax)
                        _summary_rows.append({
                            "provider":   _prov,
                            "task":       _task.strip(),
                            "linear_j":   _lin_j,
                            "agentic_j":  _age_j,
                            "tax_x":      _tax_x,
                            "ci":         _ci or "",
                        })
                # Detect saved file path
                _fm = _re.search(r'Results saved to[:\s]+(\S+\.json)', _line)
                if _fm:
                    _saved_file = _fm.group(1)

            if _summary_rows:
                # Render the master summary card below the split-screen panel
                st.markdown("---")
                st.markdown(
                    "<div style='font-size:13px;font-weight:700;color:#e8f0f8;"
                    "letter-spacing:.05em;margin-bottom:12px;'>"
                    "📊 Master Summary</div>",
                    unsafe_allow_html=True)

                # Colour scale for tax multiplier
                def _tax_color(tx):
                    if tx >= 10: return "#ef4444"
                    if tx >= 5:  return "#f59e0b"
                    if tx >= 3:  return "#38bdf8"
                    return "#22c55e"

                # Build styled HTML table
                _rows_html = ""
                for _r in _summary_rows:
                    _tc   = _tax_color(_r["tax_x"])
                    _diff = _r["agentic_j"] - _r["linear_j"]
                    _diff_str = f"+{_diff:.2f}J" if _diff > 0 else f"{_diff:.2f}J"
                    _diff_c   = "#ef4444" if _diff > 0 else "#22c55e"
                    # bar widths proportional within row
                    _max_j  = max(_r["linear_j"], _r["agentic_j"], 0.001)
                    _lw     = _r["linear_j"]  / _max_j * 100
                    _aw     = _r["agentic_j"] / _max_j * 100
                    # Human insight for agentic energy
                    _hi_row = _human_energy(_r["agentic_j"])
                    _hi_str = _hi_row[0][1] if _hi_row else ""
                    _rows_html += f"""
                    <tr style="border-bottom:1px solid #1e2d45;">
                      <td style="padding:10px 8px;font-size:10px;color:#7090b0;
                                 white-space:nowrap;">{_r['provider']}</td>
                      <td style="padding:10px 8px;font-size:10px;color:#e8f0f8;
                                 min-width:180px;">{_r['task']}</td>
                      <td style="padding:10px 8px;">
                        <div style="font-size:10px;color:#22c55e;font-family:monospace;
                                    margin-bottom:2px;">{_r['linear_j']:.4f}J</div>
                        <div style="background:#1e2d45;border-radius:2px;height:5px;width:120px;">
                          <div style="background:#22c55e;width:{_lw:.0f}%;height:100%;border-radius:2px;"></div>
                        </div>
                      </td>
                      <td style="padding:10px 8px;">
                        <div style="font-size:10px;color:#ef4444;font-family:monospace;
                                    margin-bottom:2px;">{_r['agentic_j']:.4f}J</div>
                        <div style="background:#1e2d45;border-radius:2px;height:5px;width:120px;">
                          <div style="background:#ef4444;width:{_aw:.0f}%;height:100%;border-radius:2px;"></div>
                        </div>
                      </td>
                      <td style="padding:10px 8px;text-align:center;">
                        <span style="font-size:13px;font-weight:700;color:{_tc};
                                     font-family:monospace;">{_r['tax_x']:.2f}×</span>
                      </td>
                      <td style="padding:10px 8px;font-size:9px;color:#3d5570;
                                 font-family:monospace;">{_r['ci']}</td>
                      <td style="padding:10px 8px;">
                        <span style="font-size:{_diff_c};color:{_diff_c};
                                     font-family:monospace;font-size:9px;">{_diff_str}</span>
                        <div style="font-size:8px;color:#3d5570;margin-top:2px;">{_hi_str}</div>
                      </td>
                    </tr>"""

                st.markdown(f"""
                <div style="background:#0a0e1a;border:1px solid #1e2d45;
                            border-radius:8px;overflow:hidden;margin-bottom:16px;">
                  <table style="width:100%;border-collapse:collapse;">
                    <thead>
                      <tr style="background:#0f1520;border-bottom:2px solid #1e2d45;">
                        <th style="padding:8px;font-size:9px;color:#3d5570;
                                   text-align:left;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">Provider</th>
                        <th style="padding:8px;font-size:9px;color:#3d5570;
                                   text-align:left;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">Task</th>
                        <th style="padding:8px;font-size:9px;color:#22c55e;
                                   text-align:left;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">Linear</th>
                        <th style="padding:8px;font-size:9px;color:#ef4444;
                                   text-align:left;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">Agentic</th>
                        <th style="padding:8px;font-size:9px;color:#f59e0b;
                                   text-align:center;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">Tax</th>
                        <th style="padding:8px;font-size:9px;color:#3d5570;
                                   text-align:left;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">95% CI</th>
                        <th style="padding:8px;font-size:9px;color:#3d5570;
                                   text-align:left;font-weight:600;
                                   text-transform:uppercase;letter-spacing:.08em;">Δ / Insight</th>
                      </tr>
                    </thead>
                    <tbody>{_rows_html}</tbody>
                  </table>
                </div>""", unsafe_allow_html=True)

                # Winner/loser highlights
                if len(_summary_rows) > 1:
                    _best  = min(_summary_rows, key=lambda r: r["tax_x"])
                    _worst = max(_summary_rows, key=lambda r: r["tax_x"])
                    _hcols = st.columns(3)
                    _hcols[0].success(
                        f"**✅ Lowest overhead**\n\n"
                        f"{_best['provider']} · {_best['task'][:28]}\n\n"
                        f"**{_best['tax_x']:.2f}×** tax"
                    )
                    _hcols[1].error(
                        f"**⚠ Highest overhead**\n\n"
                        f"{_worst['provider']} · {_worst['task'][:28]}\n\n"
                        f"**{_worst['tax_x']:.2f}×** tax"
                    )
                    _avg_tax = sum(r["tax_x"] for r in _summary_rows) / len(_summary_rows)
                    _hcols[2].info(
                        f"**📈 Session average**\n\n"
                        f"{len(_summary_rows)} comparisons\n\n"
                        f"**{_avg_tax:.2f}×** mean tax"
                    )

                # Visualise summary inline
                import pandas as _pd_sum
                _sdf = _pd_sum.DataFrame(_summary_rows)
                _sdf["label"] = _sdf["provider"] + " · " + _sdf["task"].str[:20]
                _sfig = go.Figure()
                _sfig.add_trace(go.Bar(
                    name="Linear", x=_sdf["label"], y=_sdf["linear_j"],
                    marker_color="#22c55e", text=_sdf["linear_j"].round(3),
                    textposition="outside", textfont=dict(size=8)))
                _sfig.add_trace(go.Bar(
                    name="Agentic", x=_sdf["label"], y=_sdf["agentic_j"],
                    marker_color="#ef4444", text=_sdf["agentic_j"].round(3),
                    textposition="outside", textfont=dict(size=8)))
                _sfig.update_layout(**PL, barmode="group", height=280,
                    title="Linear vs Agentic energy — this session",
                    xaxis_tickangle=20)
                st.plotly_chart(_sfig, use_container_width=True)

                if _saved_file:
                    st.caption(f"💾 Results saved to `{_saved_file}`")

            return proc.returncode

        except FileNotFoundError:
            out_ph.error(
                f"Cannot find `python`. Activate the venv:\n\n"
                f"```bash\ncd {cwd}\nsource venv/bin/activate\nstreamlit run streamlit_app.py\n```"
            )
            return -1
        except Exception as ex:
            out_ph.error(f"Unexpected error: {ex}")
            return -1


    def _stream_remote_log(session_id: str, base_url: str):
        """
        Poll the remote server's /api/run/status/{sid} and stream
        log lines + gauges into the UI until the run completes.
        """
        import time as _tm
        if not _REQUESTS_OK:
            st.error("pip install requests to stream remote logs")
            return

        out_col, gauge_col = st.columns([11, 9])
        with out_col:
            st.markdown(
                "<div style='font-size:10px;font-weight:600;color:#7090b0;"
                "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                "⬛ Remote terminal</div>", unsafe_allow_html=True)
            prog_ph   = st.progress(0)
            status_ph = st.empty()
            out_ph    = st.empty()

        with gauge_col:
            st.markdown(
                "<div style='font-size:10px;font-weight:600;color:#7090b0;"
                "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                "⚡ Live telemetry</div>", unsafe_allow_html=True)
            phase_ph = st.empty()
            gauge_ph = st.empty()
            bar_ph   = st.empty()

        seen_lines = 0
        _last_pw = _last_core_w = _last_tp = _last_util = _last_irq = _last_ipc = 0.0

        for _attempt in range(300):   # max ~5 min at 1s polls
            _tm.sleep(1)
            try:
                r = _req.get(f"{base_url}/api/run/status/{session_id}", timeout=5)
                data = r.json()
            except Exception as _e:
                status_ph.warning(f"Poll error: {_e}")
                continue

            _status = data.get("status","?")
            _log    = data.get("log", [])
            _prog   = float(data.get("progress", 0))
            prog_ph.progress(min(_prog, 1.0))

            # Show new log lines
            new_lines = _log[seen_lines:]
            seen_lines = len(_log)
            if new_lines:
                _html_lines = []
                for l in new_lines[-30:]:
                    lo = l.lower()
                    clr = ("#ef4444" if any(k in lo for k in ["error","fail","exception"])
                           else "#22c55e" if any(k in lo for k in ["complete","saved","✅"])
                           else "#f59e0b" if "planning" in lo
                           else "#3b82f6" if "execution" in lo
                           else "#a78bfa" if "synthesis" in lo
                           else "#b8c8d8")
                    l_esc = l.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
                    _html_lines.append(
                        f"<div style='color:{clr};font-family:monospace;"
                        f"font-size:11px;line-height:1.5;'>{l_esc}</div>")
                out_ph.markdown(
                    "<div style='background:#060a0f;border:1px solid #1e2d45;"
                    "border-radius:4px;padding:8px;max-height:340px;overflow-y:auto;'>"
                    + "".join(_html_lines) + "</div>", unsafe_allow_html=True)

            # Phase badge
            _phase = ("planning" if any("planning" in l.lower() for l in _log[-5:])
                      else "execution" if any("execution" in l.lower() for l in _log[-5:])
                      else "synthesis" if any("synthesis" in l.lower() for l in _log[-5:])
                      else _status)
            _pc = {"planning":"#f59e0b","execution":"#3b82f6","synthesis":"#a78bfa",
                   "complete":"#22c55e","running":"#22c55e","error":"#ef4444"}.get(_phase,"#7090b0")
            phase_ph.markdown(
                f"<div style='font-size:10px;padding:4px 10px;background:{_pc}22;"
                f"border:1px solid {_pc};border-radius:4px;display:inline-block;"
                f"color:{_pc};'>● {_phase.upper()}</div>", unsafe_allow_html=True)

            # Pull live samples from remote server
            try:
                _er = _req.get(f"{base_url}/api/analytics/stats", timeout=2).json()
                # Use latest energy from analytics if direct run samples not available
            except Exception:
                pass

            status_ph.markdown(
                f"<div style='font-size:9px;color:#5a7090;'>"
                f"Session <code>{session_id}</code> · status: "
                f"<b style='color:{_pc}'>{_status}</b></div>",
                unsafe_allow_html=True)

            if data.get("done") or _status in ("complete","error","cancelled"):
                if _status == "complete":
                    st.success("✅ Remote run complete — the lab DB has been updated.")
                    st.info("💡 The static DB on this dashboard won't update until the "
                            "lab owner pushes a new DB to GitHub.")
                else:
                    st.error(f"Run ended with status: {_status}")
                break
        else:
            st.warning("Polling timed out — check run status manually.")

    # ══ TAB 1: run_experiment ═════════════════════════════════════════════════
    with tab_batch:
        col_cfg, col_out = st.columns([1, 2])

        with col_cfg:
            st.markdown("**Tasks**")

            # ── Task selector: multiselect from DB/yaml + custom entry ────────
            _fmt_task = lambda t: f"{t}  [{_cat_map.get(t,'?')}]" if _cat_map.get(t) else t

            _b_all = st.checkbox("Run ALL tasks", value=False, key="b_all_tasks")

            if _b_all:
                _selected_tasks = all_tasks
                st.caption(f"All {len(all_tasks)} tasks selected")
                tasks_input = "all"
            else:
                _selected_tasks = st.multiselect(
                    "Select tasks",
                    options=all_tasks,
                    default=all_tasks[:2] if len(all_tasks) >= 2 else all_tasks,
                    format_func=_fmt_task,
                    key="b_task_multi",
                    help="Tasks from DB + tasks.yaml. Add custom below.",
                )

                # ── Custom task writer ─────────────────────────────────────────
                with st.expander("➕ Add a custom task", expanded=False):
                    st.caption(
                        "Define a new task inline. It will be saved to `config/tasks.yaml` "
                        "with `category: custom` and added to the run."
                    )
                    _ct_id    = st.text_input("Task ID (no spaces)", key="ct_id",
                                              placeholder="my_custom_task")
                    _ct_name  = st.text_input("Display name", key="ct_name",
                                              placeholder="My Custom Task")
                    _ct_prompt= st.text_area("Prompt", key="ct_prompt", height=80,
                                             placeholder="Explain quantum entanglement in simple terms.")
                    _ct_level = st.selectbox("Complexity level",
                                             ["easy","medium","hard"], index=1, key="ct_level")
                    _ct_tools = st.number_input("Expected tool calls (0 = no tools)",
                                                0, 20, 0, key="ct_tools")
                    _ct_save  = st.button("💾 Save task to tasks.yaml", key="ct_save")

                    if _ct_save:
                        if not _ct_id.strip() or not _ct_prompt.strip():
                            st.error("Task ID and Prompt are required.")
                        elif " " in _ct_id.strip():
                            st.error("Task ID must have no spaces.")
                        else:
                            _new_task = {
                                "id":          _ct_id.strip(),
                                "name":        _ct_name.strip() or _ct_id.strip(),
                                "category":    "custom",
                                "level":       _ct_level,
                                "tool_calls":  int(_ct_tools),
                                "prompt":      _ct_prompt.strip(),
                            }
                            _yaml_path = PROJECT_ROOT / "config" / "tasks.yaml"
                            try:
                                if _YAML_OK:
                                    import yaml as _yaml_w
                                    _existing = {}
                                    if _yaml_path.exists():
                                        _existing = _yaml_w.safe_load(_yaml_path.read_text()) or {}
                                    _tlist = _existing.get("tasks", [])
                                    # Update if exists, append if new
                                    _ids = [t.get("id") for t in _tlist]
                                    if _ct_id.strip() in _ids:
                                        _tlist[_ids.index(_ct_id.strip())] = _new_task
                                        st.success(f"Updated existing task `{_ct_id.strip()}`")
                                    else:
                                        _tlist.append(_new_task)
                                        st.success(f"Added `{_ct_id.strip()}` with category=custom")
                                    _existing["tasks"] = _tlist
                                    _yaml_path.write_text(
                                        _yaml_w.dump(_existing, allow_unicode=True, sort_keys=False)
                                    )
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.error("PyYAML not installed — run: pip install pyyaml")
                            except Exception as _ye:
                                st.error(f"Could not write tasks.yaml: {_ye}")

                tasks_input = ",".join(_selected_tasks) if _selected_tasks else "simple"

            if not _b_all and not _selected_tasks:
                st.warning("Select at least one task.")

            # Show selected task cards
            if not _b_all and _selected_tasks:
                _card_cols = st.columns(min(len(_selected_tasks), 3))
                for _ci, _tn in enumerate(_selected_tasks[:9]):
                    _cat = _cat_map.get(_tn, "?")
                    _cc  = {"reasoning":"#f59e0b","coding":"#3b82f6","qa":"#22c55e",
                             "summarization":"#38bdf8","classification":"#a78bfa",
                             "extraction":"#e879f9","custom":"#ef4444"}.get(_cat, "#7090b0")
                    _card_cols[_ci % 3].markdown(
                        f"<div style='background:#0f1520;border:1px solid #1e2d45;"
                        f"border-left:2px solid {_cc};border-radius:4px;"
                        f"padding:4px 8px;margin:2px 0;font-size:9px;'>"
                        f"<span style='color:#e8f0f8'>{_tn}</span> "
                        f"<span style='color:{_cc}'>{_cat}</span></div>",
                        unsafe_allow_html=True
                    )
                if len(_selected_tasks) > 9:
                    st.caption(f"… and {len(_selected_tasks)-9} more")
            st.markdown("**Providers**")
            b_providers = st.multiselect("Providers", ["cloud","local"], default=["cloud"],
                                         key="b_prov")
            st.markdown("**Run options**")
            b_reps     = st.number_input("Repetitions", 1, 100, 3, key="b_reps")
            b_country  = st.selectbox("Grid region",
                                      ["US","DE","FR","NO","IN","AU","GB","CN","BR"],
                                      format_func=lambda x: {
                                          "US":"🇺🇸 US","DE":"🇩🇪 DE","FR":"🇫🇷 FR",
                                          "NO":"🇳🇴 NO","IN":"🇮🇳 IN","AU":"🇦🇺 AU",
                                          "GB":"🇬🇧 GB","CN":"🇨🇳 CN","BR":"🇧🇷 BR",
                                      }.get(x,x), key="b_country")
            b_cooldown = st.number_input("Cool-down (s)", 0, 120, 5, step=5, key="b_cd")
            b_save_db  = st.checkbox("--save-db",   value=True,  key="b_savedb")
            b_opt      = st.checkbox("--optimizer", value=False, key="b_opt")
            b_warmup   = st.checkbox("--no-warmup", value=False, key="b_warmup")
            b_out      = st.text_input("--output (JSON file, optional)", value="",
                                       key="b_outfile")

            prov_arg = ",".join(b_providers) if b_providers else "cloud"
            b_cmd = [
                "python", "-m", "core.execution.tests.run_experiment",
                "--tasks",       tasks_input.strip(),
                "--providers",   prov_arg,
                "--repetitions", str(int(b_reps)),
                "--country",     b_country,
                "--cool-down",   str(int(b_cooldown)),
            ]
            if b_save_db: b_cmd.append("--save-db")
            if b_opt:     b_cmd.append("--optimizer")
            if b_warmup:  b_cmd.append("--no-warmup")
            if b_out.strip():
                b_cmd += ["--output", b_out.strip()]

            st.divider()
            st.markdown("**Command**")
            st.code(" \\\n  ".join(b_cmd), language="bash")

            b_run  = st.button("▶ Run batch", type="primary", use_container_width=True, key="b_run")
            b_list = st.button("📋 List tasks", use_container_width=True, key="b_list")

        with col_out:
            if b_list:
                with st.spinner("Querying harness…"):
                    r = subprocess.run(
                        ["python","-m","core.execution.tests.run_experiment","--list-tasks"],
                        capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=30,
                    )
                    st.code(r.stdout or r.stderr or "(no output)")
            elif b_run:
                if not b_providers:
                    st.warning("Select at least one provider.")
                else:
                    _conn = get_conn()
                    if _conn.get("verified"):
                        _payload = {
                            "task_id":      b_tasks[0] if len(b_tasks)==1 else "batch",
                            "provider":     b_providers[0] if b_providers else "cloud",
                            "country_code": b_country,
                            "repetitions":  int(b_reps),
                            "cool_down":    int(b_cd),
                            "tasks":        b_tasks,
                            "providers":    b_providers,
                        }
                        with st.spinner("Sending to live lab..."):
                            _resp, _err = api_post("/api/run/start", _payload)
                        if _err:
                            st.error(f"Remote start failed: {_err}")
                        else:
                            _sid = _resp.get("session_id","")
                            st.success(f"✅ Started on live lab — session `{_sid}`")
                            _stream_remote_log(_sid, _conn["url"])
                    else:
                        rc = _stream_and_gauge(b_cmd, PROJECT_ROOT)
                        if rc == 0:
                            st.success("✅ Batch complete — click 🔄 Refresh to see results.")
                            st.cache_data.clear()
                        elif rc != -1:
                            st.error(f"Process exited with code {rc}")
            else:
                st.markdown("**Recent runs**")
                if not runs.empty:
                    _sc = [c for c in ["run_id","workflow_type","task_name","provider",
                                       "country_code","energy_j","ipc","carbon_g"]
                           if c in runs.columns]
                    st.dataframe(runs.head(20)[_sc], use_container_width=True, hide_index=True)

    # ══ TAB 2: test_harness ═══════════════════════════════════════════════════
    with tab_single:
        col_cfg2, col_out2 = st.columns([1, 2])

        with col_cfg2:
            st.markdown("**Single-task harness**")
            h_task    = st.selectbox("Task ID", all_tasks, key="h_task")
            h_prov    = st.selectbox("Provider", ["cloud","local"], key="h_prov")
            h_reps    = st.number_input("Repetitions", 1, 100, 3, key="h_reps")
            h_country = st.selectbox("Grid region",
                                     ["US","DE","FR","NO","IN","AU","GB","CN","BR"],
                                     format_func=lambda x: {
                                         "US":"🇺🇸 US","DE":"🇩🇪 DE","FR":"🇫🇷 FR",
                                         "NO":"🇳🇴 NO","IN":"🇮🇳 IN","AU":"🇦🇺 AU",
                                         "GB":"🇬🇧 GB","CN":"🇨🇳 CN","BR":"🇧🇷 BR",
                                     }.get(x,x), key="h_country")
            h_cd      = st.number_input("Cool-down (s)", 0, 120, 5, step=5, key="h_cd")
            h_save_db = st.checkbox("--save-db",   value=True,  key="h_savedb")
            h_opt     = st.checkbox("--optimizer", value=False, key="h_opt")
            h_warmup  = st.checkbox("--no-warmup", value=False, key="h_warmup")
            h_debug   = st.checkbox("--debug",     value=False, key="h_debug")

            h_cmd = [
                "python", "-m", "core.execution.tests.test_harness",
                "--task-id",     h_task,
                "--provider",    h_prov,
                "--repetitions", str(int(h_reps)),
                "--country",     h_country,
                "--cool-down",   str(int(h_cd)),
            ]
            if h_save_db: h_cmd.append("--save-db")
            if h_opt:     h_cmd.append("--optimizer")
            if h_warmup:  h_cmd.append("--no-warmup")
            if h_debug:   h_cmd.append("--debug")

            st.divider()
            st.markdown("**Command**")
            st.code(" \\\n  ".join(h_cmd), language="bash")

            h_run  = st.button("▶ Run single", type="primary", use_container_width=True, key="h_run")
            h_list = st.button("📋 List tasks", use_container_width=True, key="h_list")

        with col_out2:
            if h_list:
                with st.spinner("Querying harness…"):
                    r = subprocess.run(
                        ["python","-m","core.execution.tests.test_harness","--list-tasks"],
                        capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=30,
                    )
                    st.code(r.stdout or r.stderr or "(no output)")

            elif h_run:
                _conn = get_conn()
                if _conn.get("verified"):
                    # ── REMOTE MODE — send to researcher's laptop via tunnel ──
                    _payload = {
                        "task_id":      h_task,
                        "provider":     h_prov,
                        "country_code": h_country,
                        "repetitions":  int(h_reps),
                        "cool_down":    int(h_cd),
                        "model":        h_prov,
                    }
                    with st.spinner("Sending to live lab..."):
                        _resp, _err = api_post("/api/run/start", _payload)
                    if _err:
                        st.error(f"Remote start failed: {_err}")
                    else:
                        _sid = _resp.get("session_id","")
                        st.success(f"✅ Started on live lab — session `{_sid}`")
                        _stream_remote_log(_sid, _conn["url"])
                else:
                    # ── LOCAL MODE — run subprocess on this machine ──────────
                    rc = _stream_and_gauge(h_cmd, PROJECT_ROOT)
                    if rc == 0:
                        st.success("✅ Run complete — click 🔄 Refresh to see results.")
                        st.cache_data.clear()
                    elif rc != -1:
                        st.error(f"Process exited with code {rc}")
            else:
                st.info("Configure options on the left and click ▶ Run single.")
                st.markdown("**Quick reference**")
                st.code(
                    "# Single task, 5 reps, save to DB\n"
                    "python -m core.execution.tests.test_harness \\\n"
                    "  --task-id research_summary \\\n"
                    "  --repetitions 5 --save-db\n\n"
                    "# Batch: multiple tasks & providers\n"
                    "python -m core.execution.tests.run_experiment \\\n"
                    "  --tasks research_summary,capital \\\n"
                    "  --providers cloud,local \\\n"
                    "  --repetitions 10 --save-db --country IN",
                    language="bash",
                )


# ══════════════════════════════════════════════════════════════════════════════
# SAMPLE EXPLORER  — 100Hz RAPL · cpu_samples · interrupt_samples
# ══════════════════════════════════════════════════════════════════════════════
