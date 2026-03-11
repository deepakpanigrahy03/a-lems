"""
gui/pages/execute.py  —  ▶  Execute Run  (v3 — full rewrite)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Three-tab layout:
  Tab 1 — 📋 Create & Queue  : build experiments, save to session, queue runs
  Tab 2 — ⚡ Live Execution  : speedometers + live log + post-run analytics
  Tab 3 — 📈 Run History     : all sessions this page-load, expandable cards
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import math, subprocess, time as _time, re as _re
from gui.pages.session_analysis import render_session_analysis
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.config     import PROJECT_ROOT, LIVE_API, WF_COLORS, PL
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
# SESSION STATE INIT
# ══════════════════════════════════════════════════════════════════════════════

def _init_state():
    if "ex_saved"    not in st.session_state: st.session_state.ex_saved    = []
    if "ex_queue"    not in st.session_state: st.session_state.ex_queue    = []
    if "ex_sessions" not in st.session_state: st.session_state.ex_sessions = []


# ══════════════════════════════════════════════════════════════════════════════
# SVG GAUGE  (speedometer)
# ══════════════════════════════════════════════════════════════════════════════

def _gauge_svg(value, vmin, vmax, label, unit, color, warn=None, danger=None):
    pct   = max(0.0, min(1.0, (value - vmin) / max(vmax - vmin, 1e-9)))
    angle = -140 + pct * 280
    r     = 52; cx, cy = 60, 62
    ex = cx + r * math.sin(math.radians(angle))
    ey = cy - r * math.cos(math.radians(angle))
    large = 1 if pct > 0.5 else 0
    bx  = cx + r * math.sin(math.radians(-140))
    by  = cy - r * math.cos(math.radians(-140))
    ex0 = cx - r * math.sin(math.radians(-140))
    ey0 = cy - r * math.cos(math.radians(-140))
    nclr = ("#ef4444" if danger and value >= danger
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
# TASK LOADER
# ══════════════════════════════════════════════════════════════════════════════

def _load_tasks():
    _tl = q("SELECT DISTINCT task_name FROM experiments WHERE task_name IS NOT NULL ORDER BY task_name")
    _db = _tl.task_name.tolist() if not _tl.empty else []
    _yt, _cm = [], {}
    try:
        if _YAML_OK:
            _ty = _yaml.safe_load(open(PROJECT_ROOT / "config" / "tasks.yaml"))
            _yt = [t.get("id","") for t in (_ty or {}).get("tasks",[]) if t.get("id")]
            _cm = {t.get("id",""):t.get("category","") for t in (_ty or {}).get("tasks",[])}
    except Exception: pass
    PRESET = ["simple","capital","research_summary","code_generation",
              "stock_lookup","comparative_research","deep_research"]
    return list(dict.fromkeys(PRESET + _yt + _db)), _cm


# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICS CARD  (post-run)
# ══════════════════════════════════════════════════════════════════════════════

def _analytics_card(session: dict):
    rows  = session.get("summary_rows", [])
    lines = session.get("log", [])
    sid   = session.get("sid", "x")

    if rows:
        def _tc(tx): return "#ef4444" if tx>=10 else "#f59e0b" if tx>=5 else "#38bdf8" if tx>=3 else "#22c55e"

        rh = ""
        for r in rows:
            tc = _tc(r["tax_x"])
            mx = max(r["linear_j"], r["agentic_j"], 0.001)
            lw, aw = r["linear_j"]/mx*100, r["agentic_j"]/mx*100
            hi = _human_energy(r["agentic_j"])
            hi_s = hi[0][1] if hi else ""
            rh += (f"<tr style='border-bottom:1px solid #111827;'>"
                   f"<td style='padding:9px 8px;font-size:10px;color:#7090b0;'>{r['provider']}</td>"
                   f"<td style='padding:9px 8px;font-size:10px;color:#e8f0f8;min-width:140px;'>{r['task']}</td>"
                   f"<td style='padding:9px 8px;'>"
                   f"<div style='font-size:11px;color:#22c55e;font-family:monospace;margin-bottom:3px;'>{r['linear_j']:.4f} J</div>"
                   f"<div style='background:#1e2d45;border-radius:2px;height:5px;width:110px;'>"
                   f"<div style='background:#22c55e;width:{lw:.0f}%;height:100%;border-radius:2px;'></div></div></td>"
                   f"<td style='padding:9px 8px;'>"
                   f"<div style='font-size:11px;color:#ef4444;font-family:monospace;margin-bottom:3px;'>{r['agentic_j']:.4f} J</div>"
                   f"<div style='background:#1e2d45;border-radius:2px;height:5px;width:110px;'>"
                   f"<div style='background:#ef4444;width:{aw:.0f}%;height:100%;border-radius:2px;'></div></div></td>"
                   f"<td style='padding:9px 8px;text-align:center;'>"
                   f"<span style='font-size:14px;font-weight:700;color:{tc};font-family:monospace;'>{r['tax_x']:.2f}×</span></td>"
                   f"<td style='padding:9px 8px;font-size:9px;color:#3d5570;font-family:monospace;'>{r.get('ci','')}</td>"
                   f"<td style='padding:9px 8px;font-size:9px;color:#7090b0;'>{hi_s}</td></tr>")

        st.markdown(f"""
        <div style='background:#07090f;border:1px solid #1e2d45;border-radius:8px;overflow:hidden;margin:10px 0;'>
          <div style='background:#0a0e1a;padding:8px 14px;border-bottom:1px solid #1e2d45;
                      font-size:10px;font-weight:700;color:#4fc3f7;letter-spacing:.08em;text-transform:uppercase;'>
            ⚡ Apple-to-Apple Energy Comparison</div>
          <table style='width:100%;border-collapse:collapse;'>
            <thead><tr style='background:#0a0e1a;border-bottom:2px solid #1e2d45;'>
              <th style='padding:7px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;letter-spacing:.06em;'>Provider</th>
              <th style='padding:7px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;letter-spacing:.06em;'>Task</th>
              <th style='padding:7px 8px;font-size:9px;color:#22c55e;text-align:left;text-transform:uppercase;letter-spacing:.06em;'>Linear</th>
              <th style='padding:7px 8px;font-size:9px;color:#ef4444;text-align:left;text-transform:uppercase;letter-spacing:.06em;'>Agentic</th>
              <th style='padding:7px 8px;font-size:9px;color:#f59e0b;text-align:center;text-transform:uppercase;letter-spacing:.06em;'>Orch Tax</th>
              <th style='padding:7px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;letter-spacing:.06em;'>95% CI</th>
              <th style='padding:7px 8px;font-size:9px;color:#3d5570;text-align:left;text-transform:uppercase;letter-spacing:.06em;'>Insight</th>
            </tr></thead>
            <tbody>{rh}</tbody>
          </table>
        </div>""", unsafe_allow_html=True)

        # Highlights
        if len(rows) > 1:
            best  = min(rows, key=lambda r: r["tax_x"])
            worst = max(rows, key=lambda r: r["tax_x"])
            avg_t = sum(r["tax_x"] for r in rows) / len(rows)
            c1,c2,c3 = st.columns(3)
            c1.success(f"**✅ Lowest overhead**\n\n{best['provider']} · {best['task'][:24]}\n\n**{best['tax_x']:.2f}×**")
            c2.error(f"**⚠ Highest overhead**\n\n{worst['provider']} · {worst['task'][:24]}\n\n**{worst['tax_x']:.2f}×**")
            c3.info(f"**📈 Average**\n\n{len(rows)} comparisons\n\n**{avg_t:.2f}×** mean tax")

        # Bar chart
        df = pd.DataFrame(rows)
        df["label"] = df["provider"] + " · " + df["task"].str[:22]
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Linear",  x=df["label"], y=df["linear_j"],
                             marker_color="#22c55e", text=df["linear_j"].round(3),
                             textposition="outside", textfont=dict(size=8)))
        fig.add_trace(go.Bar(name="Agentic", x=df["label"], y=df["agentic_j"],
                             marker_color="#ef4444", text=df["agentic_j"].round(3),
                             textposition="outside", textfont=dict(size=8)))
        fig.update_layout(**PL, barmode="group", height=260,
                          title="Linear vs Agentic energy — this run",
                          xaxis_tickangle=20, margin=dict(t=40,b=10))
        st.plotly_chart(fig, use_container_width=True)

        # CSV export
        csv = df[["provider","task","linear_j","agentic_j","tax_x","ci"]].to_csv(index=False)
        st.download_button("📥 Export CSV", csv,
                           file_name=f"alems_{sid}.csv",
                           mime="text/csv", key=f"csv_{sid}")
    else:
        st.info("No summary rows parsed. Check raw log below.")

    # Raw log
    with st.expander("📋 Raw log", expanded=False):
        log_html = "".join(
            f"<div style='color:{'#ef4444' if any(k in l.lower() for k in ['error','fail']) else '#22c55e' if any(k in l.lower() for k in ['complete','saved','✅']) else '#b8c8d8'};font-family:monospace;font-size:10px;line-height:1.5;'>"
            f"{l.replace('<','&lt;').replace('>','&gt;')}</div>"
            for l in lines)
        st.markdown(
            f"<div style='background:#050810;border:1px solid #1e2d45;border-radius:4px;"
            f"padding:10px;max-height:300px;overflow-y:auto;'>{log_html}</div>",
            unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# LOCAL EXECUTION STREAM
# ══════════════════════════════════════════════════════════════════════════════

def _run_local(exp: dict, sid: str):
    cmd   = exp["cmd"]
    lines = []
    prog_ph   = st.progress(0)
    status_ph = st.empty()
    cols = st.columns([11, 9])
    with cols[0]:
        st.markdown("<div style='font-size:10px;font-weight:600;color:#7090b0;"
                    "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                    "⬛ Terminal</div>", unsafe_allow_html=True)
        out_ph = st.empty()
    with cols[1]:
        st.markdown("<div style='font-size:10px;font-weight:600;color:#7090b0;"
                    "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                    "⚡ Live telemetry</div>", unsafe_allow_html=True)
        phase_ph  = st.empty()
        gauge_ph  = st.empty()
        bar_ph    = st.empty()
        insight_ph= st.empty()

    _pw=_core_w=_dram_w=_tp=_util=_irq=_ipc=0.0
    _phase="starting"; _energy_acc=[]
    _last_rid = int(q1("SELECT COALESCE(MAX(run_id),0) AS n FROM runs").get("n",0))

    def _draw():
        gauge_ph.markdown(
            f"<div style='display:flex;justify-content:space-around;'>"
            f"{_gauge_svg(_pw,0,80,'Pkg Power','W','#3b82f6',warn=50,danger=70)}"
            f"{_gauge_svg(_core_w,0,60,'Core Power','W','#22c55e',warn=40,danger=55)}"
            f"{_gauge_svg(_tp,30,105,'Pkg Temp','°C','#f59e0b',warn=80,danger=95)}"
            f"</div>", unsafe_allow_html=True)
        bar_ph.markdown(
            _bar_gauge(_util,100,"CPU Util","%","#38bdf8") +
            _bar_gauge(min(_irq,50000),50000,"IRQ Rate","/s","#f59e0b") +
            _bar_gauge(_ipc,3.0,"IPC","inst/cyc","#a78bfa"),
            unsafe_allow_html=True)
        pc={"starting":"#7090b0","planning":"#f59e0b","execution":"#3b82f6",
            "synthesis":"#a78bfa","running":"#22c55e","complete":"#22c55e","error":"#ef4444"}.get(_phase,"#7090b0")
        phase_ph.markdown(
            f"<div style='font-size:10px;padding:3px 10px;background:{pc}22;"
            f"border:1px solid {pc};border-radius:4px;display:inline-block;"
            f"color:{pc};margin-bottom:4px;'>● {_phase.upper()}</div>",
            unsafe_allow_html=True)
        if _energy_acc:
            est_j=sum(_energy_acc)*2; hi=_human_energy(est_j)
            insight_ph.markdown(
                "<div style='font-size:8px;color:#3d5570;margin-top:2px;'>So far: "
                +" · ".join(f"{ic} {d}" for ic,d in hi[:2])+"</div>",
                unsafe_allow_html=True)

    def _poll(rid):
        nonlocal _pw,_core_w,_dram_w,_tp,_util,_irq,_ipc
        if not _REQUESTS_OK: return
        try:
            er=_req.get(f"http://127.0.0.1:8765/api/runs/{rid}/samples/energy",timeout=2).json()
            pw_rows=er.get("power",[]) if isinstance(er,dict) else []
            if pw_rows:
                lp=pw_rows[-1]; _pw=float(lp.get("pkg_w",_pw))
                _core_w=float(lp.get("core_w",_core_w)); _dram_w=float(lp.get("dram_w",_dram_w))
                _energy_acc.append(_pw)
                if len(_energy_acc)>60: _energy_acc.pop(0)
        except Exception: pass
        try:
            cr=_req.get(f"http://127.0.0.1:8765/api/runs/{rid}/samples/cpu",timeout=2).json()
            if isinstance(cr,list) and cr:
                lc=cr[-1]; _tp=float(lc.get("package_temp",_tp))
                _util=float(lc.get("cpu_util_percent",_util)); _ipc=float(lc.get("ipc",_ipc))
        except Exception: pass
        try:
            ir=_req.get(f"http://127.0.0.1:8765/api/runs/{rid}/samples/interrupts",timeout=2).json()
            if isinstance(ir,list) and ir: _irq=float(ir[-1].get("interrupts_per_sec",_irq))
        except Exception: pass

    try:
        proc=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
                              text=True,cwd=str(PROJECT_ROOT),bufsize=1)
        pc_ctr=0
        for raw in iter(proc.stdout.readline,""):
            line=raw.rstrip()
            if not line: continue
            lines.append(line); lo=line.lower()
            if   "planning"  in lo: _phase="planning"
            elif "execution" in lo: _phase="execution"
            elif "synth"     in lo: _phase="synthesis"
            elif "rep " in lo or "pair" in lo: _phase="running"
            if any(k in lo for k in ["complete","saved","done","✅"]): _phase="complete"
            for pat in ["rep ","pair ","repetition "]:
                if pat in lo and "/" in lo:
                    try:
                        seg=lo.split(pat)[-1].split("/"); d,t=int(seg[0].strip()),int(seg[1].split()[0])
                        prog_ph.progress(min(d/t,1.0)); status_ph.caption(f"Rep {d} / {t}")
                    except Exception: pass
                    break
            clr=("#ef4444" if any(k in lo for k in ["error","fail","exception","traceback"])
                 else "#22c55e" if any(k in lo for k in ["complete","saved","✅","pair"])
                 else "#f59e0b" if "planning" in lo else "#3b82f6" if "execution" in lo
                 else "#a78bfa" if "synthesis" in lo else "#e8f0f8")
            out_ph.markdown(
                "<div style='background:#050810;border:1px solid #1e2d45;border-radius:4px;"
                "padding:8px 12px;font-family:monospace;font-size:9px;line-height:1.5;"
                "height:340px;overflow-y:auto;'>"
                +"".join(f"<div style='color:{clr}'>{l.replace('<','&lt;').replace('>','&gt;')}</div>"
                         for l in lines[-60:])+"</div>", unsafe_allow_html=True)
            pc_ctr+=1
            if pc_ctr%4==0:
                nr=int(q1("SELECT COALESCE(MAX(run_id),0) AS n FROM runs").get("n",0))
                if nr>_last_rid: _last_rid=nr
                _poll(_last_rid)
            _draw()

        proc.wait(); _phase="complete" if proc.returncode==0 else "error"
        prog_ph.progress(1.0); _draw()
        if _energy_acc:
            total_j=sum(_energy_acc)*2; hi=_human_energy(total_j)
            insight_ph.markdown(
                "<div style='background:#0f1520;border:1px solid #22c55e33;border-radius:6px;"
                "padding:8px 12px;margin-top:4px;'>"
                "<div style='font-size:9px;font-weight:600;color:#22c55e;margin-bottom:4px;'>"
                "⚡ Run energy summary</div>"
                +"".join(f"<div style='font-size:9px;color:#b8c8d8;margin:2px 0;'>{ic} {d}</div>" for ic,d in hi)
                +"</div>", unsafe_allow_html=True)

        # Parse MASTER SUMMARY
        summary_rows=[]; in_sum=False
        for l in lines:
            ll=l.strip()
            if "MASTER SUMMARY" in ll: in_sum=True; continue
            if in_sum and ll.startswith("==="): in_sum=False; continue
            if in_sum and ll.startswith("---"): continue
            if in_sum and ll and not ll.startswith("Provider"):
                m=_re.match(r'^(\S+)\s+(.*?)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)x?\s*(\[.*?\])?',ll)
                if m:
                    pv,tk,ln,ag,tx,ci=m.groups()
                    summary_rows.append({"provider":pv,"task":tk.strip(),
                                         "linear_j":float(ln),"agentic_j":float(ag),
                                         "tax_x":float(tx),"ci":ci or ""})
        return proc.returncode, lines, summary_rows

    except FileNotFoundError:
        st.error(f"Cannot find python. Activate venv:\n```\ncd {PROJECT_ROOT}\nsource venv/bin/activate\n```")
        return -1, lines, []
    except Exception as ex:
        st.error(f"Error: {ex}"); return -1, lines, []


# ══════════════════════════════════════════════════════════════════════════════
# REMOTE EXECUTION STREAM
# ══════════════════════════════════════════════════════════════════════════════

def _run_remote(exp: dict, session_id: str, base_url: str):
    lines=[]; summary_rows=[]
    if not _REQUESTS_OK: st.error("pip install requests"); return -1, lines, summary_rows
    prog_ph=st.progress(0); status_ph=st.empty()
    cols=st.columns([11,9])
    with cols[0]:
        st.markdown("<div style='font-size:10px;font-weight:600;color:#7090b0;"
                    "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                    "⬛ Remote terminal</div>", unsafe_allow_html=True)
        out_ph=st.empty()
    with cols[1]:
        st.markdown("<div style='font-size:10px;font-weight:600;color:#7090b0;"
                    "text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;'>"
                    "⚡ Live telemetry</div>", unsafe_allow_html=True)
        phase_ph=st.empty(); gauge_ph=st.empty(); bar_ph=st.empty()

    _pw=_core_w=_tp=_util=_irq=_ipc=0.0

    def _draw(phase):
        gauge_ph.markdown(
            f"<div style='display:flex;justify-content:space-around;'>"
            f"{_gauge_svg(_pw,0,80,'Pkg Power','W','#3b82f6',warn=50,danger=70)}"
            f"{_gauge_svg(_core_w,0,60,'Core Power','W','#22c55e',warn=40,danger=55)}"
            f"{_gauge_svg(_tp,30,105,'Pkg Temp','°C','#f59e0b',warn=80,danger=95)}"
            f"</div>", unsafe_allow_html=True)
        bar_ph.markdown(
            _bar_gauge(_util,100,"CPU Util","%","#38bdf8")+
            _bar_gauge(min(_irq,50000),50000,"IRQ Rate","/s","#f59e0b")+
            _bar_gauge(_ipc,3.0,"IPC","inst/cyc","#a78bfa"),
            unsafe_allow_html=True)
        pc={"starting":"#7090b0","running":"#22c55e","complete":"#22c55e","error":"#ef4444"}.get(phase,"#7090b0")
        phase_ph.markdown(
            f"<div style='font-size:10px;padding:3px 10px;background:{pc}22;"
            f"border:1px solid {pc};border-radius:4px;display:inline-block;color:{pc};'>"
            f"● {phase.upper()}</div>", unsafe_allow_html=True)

    seen=0
    for _ in range(600):
        _time.sleep(1)
        try:
            r=_req.get(f"{base_url}/api/run/status/{session_id}",timeout=6); data=r.json()
        except Exception as e:
            status_ph.warning(f"Poll error: {e}"); continue
        status=data.get("status","?"); log=data.get("log",[]); prog=float(data.get("progress",0))
        prog_ph.progress(min(prog,1.0))
        new=log[seen:]; seen=len(log)
        for l in new: lines.append(l)
        if lines:
            html="".join(
                f"<div style='color:{'#ef4444' if any(k in l.lower() for k in ['error','fail']) else '#22c55e' if any(k in l.lower() for k in ['complete','✅','saved']) else '#b8c8d8'};font-family:monospace;font-size:10px;line-height:1.5;'>"
                f"{l.replace('<','&lt;').replace('>','&gt;')}</div>" for l in lines[-50:])
            out_ph.markdown(
                "<div style='background:#060a0f;border:1px solid #1e2d45;border-radius:4px;"
                "padding:8px;max-height:340px;overflow-y:auto;'>"+html+"</div>",
                unsafe_allow_html=True)
        _draw(status)
        status_ph.markdown(
            f"<div style='font-size:9px;color:#5a7090;'>Session <code>{session_id}</code>"
            f" · <b style='color:#4fc3f7;'>{status}</b></div>", unsafe_allow_html=True)
        if data.get("done") or status in ("complete","error","cancelled"):
            if status=="complete":
                prog_ph.progress(1.0)
                st.success("✅ Remote run complete — DB updated on lab machine.")
                st.info("💡 Static dashboard DB updates when lab owner pushes to GitHub.")
                in_sum=False
                for l in lines:
                    ll=l.strip()
                    if "MASTER SUMMARY" in ll: in_sum=True; continue
                    if in_sum and ll.startswith("==="): in_sum=False; continue
                    if in_sum and ll and not ll.startswith("Provider"):
                        m=_re.match(r'^(\S+)\s+(.*?)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)x?\s*(\[.*?\])?',ll)
                        if m:
                            pv,tk,ln,ag,tx,ci=m.groups()
                            summary_rows.append({"provider":pv,"task":tk.strip(),
                                                 "linear_j":float(ln),"agentic_j":float(ag),
                                                 "tax_x":float(tx),"ci":ci or ""})
            else:
                st.error(f"Run ended: {status}")
            return (0 if status=="complete" else 1), lines, summary_rows
    st.warning("Polling timed out."); return -1, lines, []


# ══════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════════════════════

def render(ctx: dict):
    _init_state()

    st.title("Execute Run")

    # Mode banner
    _conn=get_conn()
    if _conn.get("verified"):
        _hclr="#22c55e" if _conn.get("harness") else "#f59e0b"
        _hmsg="Harness ready — runs execute on lab machine" if _conn.get("harness") else "Server reachable but harness not loaded"
        st.markdown(
            f"<div style='background:#0a2010;border:1px solid #22c55e33;border-left:3px solid #22c55e;"
            f"border-radius:4px;padding:8px 14px;margin-bottom:10px;font-size:11px;'>"
            f"🟢 <b style='color:#22c55e'>LIVE MODE</b>  ·  <span style='color:{_hclr}'>{_hmsg}</span><br/>"
            f"<span style='color:#3d5570;font-size:9px;'>Tunnel: {_conn['url']}</span></div>",
            unsafe_allow_html=True)
    else:
        st.markdown(
            "<div style='background:#0a0f1a;border:1px solid #1e2d45;border-left:3px solid #3b82f6;"
            "border-radius:4px;padding:8px 14px;margin-bottom:10px;font-size:11px;'>"
            "⚫ <b style='color:#3b82f6'>LOCAL MODE</b>  ·  "
            "<span style='color:#5a7090'>Runs execute on this machine.</span></div>",
            unsafe_allow_html=True)

    # Queue banner
    qlen=len(st.session_state.ex_queue)
    if qlen>0:
        st.markdown(
            f"<div style='background:#0f1a2e;border:1px solid #3b4fd8;border-radius:4px;"
            f"padding:7px 14px;margin-bottom:10px;font-size:11px;color:#93c5fd;'>"
            f"⏳ <b>{qlen}</b> experiment{'s' if qlen>1 else ''} queued</div>",
            unsafe_allow_html=True)

    all_tasks, _cat_map = _load_tasks()

    tab1, tab2, tab3 = st.tabs(["📋 Create & Queue", "⚡ Live Execution", "📈 Run History"])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — CREATE & QUEUE
    # ══════════════════════════════════════════════════════════════════════════
    with tab1:
        left, right = st.columns([1,1])

        with left:
            st.markdown("#### 🔬 Build Experiment")
            exp_name = st.text_input("Name", value="My Experiment", key="ex_name")
            exp_mode = st.radio("Mode", ["Single (test_harness)","Batch (run_experiment)"],
                                horizontal=True, key="ex_mode")

            if "Single" in exp_mode:
                h_task    = st.selectbox("Task ID", all_tasks, key="h_task")
                h_prov    = st.selectbox("Provider", ["cloud","local"], key="h_prov")
                h_reps    = st.number_input("Repetitions", 1, 100, 3, key="h_reps")
                h_country = st.selectbox("Region",
                    ["US","DE","FR","NO","IN","AU","GB","CN","BR"],
                    format_func=lambda x:{"US":"🇺🇸 US","DE":"🇩🇪 DE","FR":"🇫🇷 FR",
                        "NO":"🇳🇴 NO","IN":"🇮🇳 IN","AU":"🇦🇺 AU",
                        "GB":"🇬🇧 GB","CN":"🇨🇳 CN","BR":"🇧🇷 BR"}.get(x,x), key="h_country")
                h_cd      = st.number_input("Cool-down (s)",0,120,5,step=5,key="h_cd")
                h_save_db = st.checkbox("--save-db",   value=True,  key="h_savedb")
                h_opt     = st.checkbox("--optimizer", value=False, key="h_opt")
                h_warmup  = st.checkbox("--no-warmup", value=False, key="h_warmup")
                h_debug   = st.checkbox("--debug",     value=False, key="h_debug")
                cmd=["python","-m","core.execution.tests.test_harness",
                     "--task-id",h_task,"--provider",h_prov,
                     "--repetitions",str(int(h_reps)),"--country",h_country,
                     "--cool-down",str(int(h_cd))]
                if h_save_db: cmd.append("--save-db")
                if h_opt:     cmd.append("--optimizer")
                if h_warmup:  cmd.append("--no-warmup")
                if h_debug:   cmd.append("--debug")
                meta={"name":exp_name,"mode":"single","task":h_task,"provider":h_prov,
                      "reps":int(h_reps),"country":h_country,"cmd":cmd}
            else:
                _b_all=st.checkbox("All tasks",value=False,key="b_all")
                if _b_all:
                    _sel=all_tasks; st.caption(f"All {len(all_tasks)} tasks")
                else:
                    _sel=st.multiselect("Tasks",all_tasks,
                                        default=all_tasks[:2] if len(all_tasks)>=2 else all_tasks,
                                        key="b_task_multi")
                b_prov    = st.multiselect("Providers",["cloud","local"],default=["cloud"],key="b_prov")
                b_reps    = st.number_input("Repetitions",1,100,3,key="b_reps")
                b_country = st.selectbox("Region",
                    ["US","DE","FR","NO","IN","AU","GB","CN","BR"],
                    format_func=lambda x:{"US":"🇺🇸 US","DE":"🇩🇪 DE","FR":"🇫🇷 FR",
                        "NO":"🇳🇴 NO","IN":"🇮🇳 IN","AU":"🇦🇺 AU",
                        "GB":"🇬🇧 GB","CN":"🇨🇳 CN","BR":"🇧🇷 BR"}.get(x,x), key="b_country")
                b_cd      = st.number_input("Cool-down (s)",0,120,5,step=5,key="b_cd")
                b_save_db = st.checkbox("--save-db",value=True,key="b_savedb")
                b_opt     = st.checkbox("--optimizer",value=False,key="b_opt")
                b_warmup  = st.checkbox("--no-warmup",value=False,key="b_warmup")
                prov_arg=",".join(b_prov) if b_prov else "cloud"
                tasks_arg=",".join(_sel) if _sel else "simple"
                cmd=["python","-m","core.execution.tests.run_experiment",
                     "--tasks",tasks_arg,"--providers",prov_arg,
                     "--repetitions",str(int(b_reps)),"--country",b_country,
                     "--cool-down",str(int(b_cd))]
                if b_save_db: cmd.append("--save-db")
                if b_opt:     cmd.append("--optimizer")
                if b_warmup:  cmd.append("--no-warmup")
                meta={"name":exp_name,"mode":"batch","tasks":_sel,"providers":b_prov,
                      "reps":int(b_reps),"country":b_country,"cmd":cmd}

            st.code(" \\\n  ".join(cmd), language="bash")

            c1,c2,c3=st.columns(3)
            if c1.button("💾 Save",use_container_width=True,key="ex_save"):
                st.session_state.ex_saved.append(dict(meta))
                st.success(f"Saved **{exp_name}**")
            if c2.button("▶ Run Now",type="primary",use_container_width=True,key="ex_run_now"):
                st.session_state.ex_queue.insert(0,dict(meta))
                st.success("Queued — go to ⚡ Live Execution"); st.rerun()
            if c3.button("➕ Queue",use_container_width=True,key="ex_queue_btn"):
                st.session_state.ex_queue.append(dict(meta))
                st.success(f"Queued at position {len(st.session_state.ex_queue)}")

        with right:
            st.markdown("#### 📁 Saved Experiments")
            if not st.session_state.ex_saved:
                st.caption("No saved experiments yet.")
            else:
                for i,exp in enumerate(st.session_state.ex_saved):
                    ea,eb,ec=st.columns([3,1,1])
                    ea.markdown(
                        f"<div style='font-size:12px;font-weight:600;color:#e8f0f8;'>{exp['name']}</div>"
                        f"<div style='font-size:10px;color:#7090b0;'>"
                        f"{exp.get('task',', '.join(exp.get('tasks',[])))[:30]} · "
                        f"{exp.get('provider','/'.join(exp.get('providers',[]))) } · "
                        f"{exp.get('reps',3)} reps</div>",
                        unsafe_allow_html=True)
                    if eb.button("▶",key=f"sv_run_{i}",use_container_width=True):
                        st.session_state.ex_queue.insert(0,dict(exp)); st.rerun()
                    if ec.button("🗑",key=f"sv_del_{i}",use_container_width=True):
                        st.session_state.ex_saved.pop(i); st.rerun()
                if st.button("▶▶ Run All Saved",type="primary",use_container_width=True,key="run_all"):
                    for e in st.session_state.ex_saved: st.session_state.ex_queue.append(dict(e))
                    st.success(f"Queued {len(st.session_state.ex_saved)} experiments"); st.rerun()

            st.divider()
            st.markdown("#### ⏳ Queue")
            if not st.session_state.ex_queue:
                st.caption("Queue is empty.")
            else:
                for i,exp in enumerate(st.session_state.ex_queue):
                    qa,qb=st.columns([4,1])
                    qa.markdown(
                        f"<div style='font-size:11px;color:#93c5fd;'>"
                        f"#{i+1} — <b>{exp['name']}</b></div>",
                        unsafe_allow_html=True)
                    if qb.button("✕",key=f"q_del_{i}",use_container_width=True):
                        st.session_state.ex_queue.pop(i); st.rerun()
                if st.button("🗑 Clear queue",use_container_width=True,key="clear_q"):
                    st.session_state.ex_queue.clear(); st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — LIVE EXECUTION
    # ══════════════════════════════════════════════════════════════════════════
    with tab2:
        if not st.session_state.ex_queue:
            st.info("Queue is empty. Go to 📋 Create & Queue to add experiments.")
            # Show last result if available
            if st.session_state.ex_sessions:
                last=st.session_state.ex_sessions[-1]
                st.divider()
                st.markdown(f"### 📊 Last Results — {last['name']} ({last['ts']})")
                _analytics_card(last)
        else:
            next_exp=st.session_state.ex_queue[0]
            rem=len(st.session_state.ex_queue)-1
            st.markdown(
                f"<div style='background:#0a1a0a;border:1px solid #22c55e33;"
                f"border-left:3px solid #22c55e;border-radius:4px;"
                f"padding:8px 14px;margin-bottom:10px;font-size:12px;'>"
                f"▶ Ready: <b style='color:#22c55e'>{next_exp['name']}</b>"
                f"{'  ·  '+str(rem)+' more queued' if rem>0 else ''}"
                f"</div>", unsafe_allow_html=True)

            if st.button(f"▶ Start — {next_exp['name']}",type="primary",
                         use_container_width=True,key="start_next"):
                exp=st.session_state.ex_queue.pop(0)
                sid=f"ses_{int(_time.time()*1000)}"
                conn=get_conn()

                if conn.get("verified"):
                    # Remote
                    payload={
                        "task_id":      exp.get("task", exp.get("tasks",["simple"])[0] if exp.get("tasks") else "simple"),
                        "provider":     exp.get("provider", exp.get("providers",["cloud"])[0] if exp.get("providers") else "cloud"),
                        "country_code": exp.get("country","US"),
                        "repetitions":  exp.get("reps",3),
                        "cool_down":    5,
                        "tasks":        exp.get("tasks",[exp.get("task","simple")]),
                        "providers":    exp.get("providers",[exp.get("provider","cloud")]),
                        "token":        conn.get("token",""),
                    }
                    resp,err=api_post("/api/run/start",payload)
                    if err:
                        st.error(f"Remote start failed: {err}")
                        record={"sid":sid,"name":exp["name"],"status":"error","log":[str(err)],"summary_rows":[],"ts":_time.strftime("%H:%M:%S")}
                    else:
                        rsid=resp.get("session_id","")
                        st.success(f"✅ Started — session `{rsid}`")
                        rc,lines,rows=_run_remote(exp,rsid,conn["url"])
                        record={"sid":sid,"name":exp["name"],
                                "status":"complete" if rc==0 else "error",
                                "log":lines,"summary_rows":rows,"ts":_time.strftime("%H:%M:%S")}
                else:
                    # Local
                    rc,lines,rows=_run_local(exp,sid)
                    record={"sid":sid,"name":exp["name"],
                            "status":"complete" if rc==0 else "error",
                            "log":lines,"summary_rows":rows,"ts":_time.strftime("%H:%M:%S")}
                    if rc==0:
                        st.success("✅ Complete — click 🔄 Refresh to see DB results.")
                        st.cache_data.clear()
                    elif rc!=-1:
                        st.error(f"Process exited with code {rc}")

                st.session_state.ex_sessions.append(record)

                # Show analytics immediately
                if record["status"]=="complete":
                    st.divider()
                    st.markdown(f"### 📊 Results — {exp['name']}")
                    _analytics_card(record)

                if st.session_state.ex_queue:
                    st.info(f"⏳ {len(st.session_state.ex_queue)} more queued — click Start again.")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — RUN HISTORY
    # ══════════════════════════════════════════════════════════════════════════
    with tab3:
        st.markdown("### 📊 Session Analysis")

        try:
            recent = q(
                "SELECT group_id, COUNT(*) as n_exps, "
                "SUM(runs_completed) as n_runs "
                "FROM experiments "
                "GROUP BY group_id "
                "ORDER BY MAX(exp_id) DESC "
                "LIMIT 10"
            )
        except Exception:
            recent = pd.DataFrame()

        if recent.empty:
            st.info("No sessions found. Run an experiment first.")
        else:
            def _fmt_gid(gid):
                rows = recent[recent.group_id == gid]
                if not rows.empty:
                    r = rows.iloc[0]
                    short = gid.replace("session_", "").replace("_", " ", 1)[:15]
                    return f"{short}  ({int(r.n_exps)} exps, {int(r.n_runs or 0)} runs)"
                return gid

            selected_gid = st.selectbox(
                "Select Session",
                recent.group_id.tolist(),
                format_func=_fmt_gid,
                key="analysis_gid",
            )
            st.divider()
            render_session_analysis(selected_gid)
