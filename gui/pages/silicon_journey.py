"""
gui/pages/silicon_journey.py  —  Silicon Journey
─────────────────────────────────────────────────────────────────────────────
Environment-aware LLM interface. Minimal. Human-scale energy insights.

UX philosophy:
  • Conversation dominates the screen
  • Select task from dropdown → prompt fills (same pattern as research_insights.py)
  • Type freely → custom --task
  • Animated st.status() while running — no logs
  • Reveal: "While you were chatting, something interesting happened in silicon…"
  • Human-scale: phone %, LED bulb, WhatsApp messages, water drops, CO₂
  • "See what happened in silicon" → inline deep analysis + charts + PDF
  • Retry on failure

Command (copied exactly from execute.py):
  --task-id  <id>   for predefined tasks
  --task     <text> for custom prompts
  + --provider cloud/local --repetitions 1 --country US --cool-down 5 --save-db

Harness runs BOTH linear + agentic in ONE call — no dual threads.
Run_ids fetched by latest exp_id after completion.
─────────────────────────────────────────────────────────────────────────────
"""

import io
import json
import subprocess
import threading
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, PROJECT_ROOT, WF_COLORS
from gui.db import q, q1
from gui.helpers import fl

# ── Human-scale constants ─────────────────────────────────────────────────────
_PHONE_J = 20_000
_WHATSAPP_J = 0.014
_GOOGLE_J = 0.3
_LED_W = 1.0
_CO2_PER_J = 0.000233  # kg/J global avg grid
_WATER_PER_J = 0.5e-6  # ml/J data centre cooling avg


# ── Config loaders ────────────────────────────────────────────────────────────


def _load_models() -> dict:
    try:
        return json.loads((PROJECT_ROOT / "config" / "models.json").read_text())
    except Exception:
        return {}


def _load_tasks() -> list[dict]:
    """Returns list of {id, name, prompt, category} from tasks.yaml.
    New tasks added to the yaml appear automatically."""
    try:
        import yaml

        cfg = yaml.safe_load((PROJECT_ROOT / "config" / "tasks.yaml").read_text()) or {}
        return cfg.get("tasks", [])
    except Exception:
        return [
            {
                "id": "gsm8k_basic",
                "name": "GSM8K Arithmetic",
                "prompt": "If John has 5 apples and buys 7 more, how many apples does he have?",
                "category": "reasoning",
            },
            {
                "id": "factual_qa",
                "name": "Factual Question",
                "prompt": "Who was the first president of the United States?",
                "category": "qa",
            },
            {
                "id": "code_fibonacci",
                "name": "Fibonacci Function",
                "prompt": "Write a Python function that returns the nth Fibonacci number.",
                "category": "coding",
            },
        ]


# ── CSS ───────────────────────────────────────────────────────────────────────


def _css():
    st.markdown(
        """
<style>
.sj-user-row  { display:flex; justify-content:flex-end; margin:8px 0; }
.sj-user-bbl  { background:#1a2d45; border-radius:18px 18px 4px 18px;
                padding:11px 16px; max-width:75%; font-size:13px;
                color:#c8d8e8; line-height:1.6; }
.sj-bot-row   { display:flex; flex-direction:column; gap:4px; margin:8px 0; }
.sj-bot-lbl   { font-size:8px; color:#5a7090; text-transform:uppercase;
                letter-spacing:.12em; padding-left:2px; }
.sj-reveal    { background:#080e1a; border:0.5px solid #1e2d45;
                border-radius:14px; padding:18px; }
.sj-hook      { font-size:13px; color:#c8d8e8; line-height:1.6;
                margin-bottom:14px; font-style:italic;
                border-left:2px solid #22c55e44; padding-left:12px; }
.sj-wf-grid   { display:grid; grid-template-columns:1fr 1fr; gap:10px;
                margin-bottom:12px; }
.sj-wf-card   { background:#0d1520; border-radius:10px; padding:13px; }
.sj-wf-head   { display:flex; align-items:center; gap:6px; margin-bottom:7px; }
.sj-wf-dot    { width:6px; height:6px; border-radius:50%; flex-shrink:0; }
.sj-wf-name   { font-size:8px; font-weight:700; text-transform:uppercase;
                letter-spacing:.1em; }
.sj-wf-resp   { font-size:11px; color:#c8d8e8; line-height:1.6;
                font-style:italic; margin-bottom:7px;
                border-left:2px solid #1e2d45; padding-left:8px; }
.sj-wf-nodata { font-size:10px; color:#5a7090; font-style:italic; }
.sj-wf-meta   { font-size:9px; color:#5a7090; }
.sj-meters    { display:flex; justify-content:center; gap:22px;
                padding:10px 0 12px; }
.sj-m         { text-align:center; }
.sj-m-val     { font-size:17px; font-weight:600; color:#ffffff; line-height:1; }
.sj-m-icon    { font-size:13px; margin-bottom:3px; color:#ffffff; }
.sj-m-lbl     { font-size:8px; color:#5a7090; margin-top:3px; }
.sj-pills     { display:flex; gap:6px; flex-wrap:wrap; margin-bottom:12px; }
.sj-pill      { font-size:10px; color:#7090b0; background:#0a1018;
                border:0.5px solid #1e2d45; border-radius:8px;
                padding:5px 9px; line-height:1.4; }
.sj-pill b    { color:#c8d8e8; font-weight:500; }
.sj-error-box { background:#080e1a; border:0.5px solid #ef444433;
                border-left:3px solid #ef4444;
                border-radius:0 10px 10px 0; padding:13px 16px; }
.sj-err-msg   { font-size:11px; color:#ef4444; margin-bottom:10px; }
.sj-session-bar { display:flex; gap:8px; justify-content:center;
                  padding:10px 0 4px; border-top:0.5px solid #0d1520;
                  margin-top:8px; font-size:9px; color:#7090b0;
                  flex-wrap:wrap; }
.sj-empty     { text-align:center; padding:48px 20px; }
.sj-silicon   { background:#050a10; border:0.5px solid #1e2d45;
                border-radius:12px; padding:16px; margin-top:10px; }
.sj-si-title  { font-size:9px; color:#5a7090; text-transform:uppercase;
                letter-spacing:.12em; text-align:center; margin-bottom:12px; }
.sj-tax-box   { background:#0d1520; border-radius:10px; padding:13px;
                text-align:center; margin-bottom:12px; }
.sj-tax-val   { font-size:30px; font-weight:700; line-height:1;
                margin-bottom:4px; }
.sj-tax-lbl   { font-size:9px; color:#7090b0; margin-bottom:7px; }
.sj-tax-txt   { font-size:10px; color:#a0b0c0; line-height:1.6; }
.sj-hs-grid   { display:grid; grid-template-columns:1fr 1fr;
                gap:8px; margin-bottom:12px; }
.sj-hs-card   { background:#0d1520; border-radius:8px; padding:11px; }
.sj-hs-head   { font-size:8px; font-weight:700; text-transform:uppercase;
                letter-spacing:.1em; margin-bottom:7px; }
.sj-hs-row    { display:flex; gap:8px; padding:3px 0;
                border-bottom:0.5px solid #080e1a;
                font-size:10px; color:#7090b0; line-height:1.4; }
.sj-hs-row:last-child { border-bottom:none; }
.sj-hs-icon   { flex-shrink:0; width:16px; text-align:center; }
.sj-hs-row span { color:#ffffff; }
</style>
""",
        unsafe_allow_html=True,
    )


# ── Human scale ───────────────────────────────────────────────────────────────


def _hs(joules: float) -> dict:
    j = max(joules, 0)
    return {
        "phone_pct": j / _PHONE_J * 100,
        "led_s": j / _LED_W,
        "whatsapp": j / _WHATSAPP_J,
        "google": j / _GOOGLE_J,
        "water_ul": j * _WATER_PER_J * 1e6,
        "co2_mg": j * _CO2_PER_J * 1e6,
    }


def _hs_html(lin: dict, agt: dict) -> str:
    le = (lin.get("total_energy_uj") or 0) / 1e6
    ae = (agt.get("total_energy_uj") or 0) / 1e6
    lh, ah = _hs(le), _hs(ae)

    def rows(h: dict) -> str:
        return (
            f"<div class='sj-hs-row'><span class='sj-hs-icon'>📱</span>"
            f"<span><span>{h['phone_pct']:.4f}%</span> of a phone charge</span></div>"
            f"<div class='sj-hs-row'><span class='sj-hs-icon'>💡</span>"
            f"<span><span>{h['led_s']:.2f}s</span> of a 1W LED bulb</span></div>"
            f"<div class='sj-hs-row'><span class='sj-hs-icon'>💬</span>"
            f"<span>≈ <span>{h['whatsapp']:,.0f}</span> WhatsApp messages</span></div>"
            f"<div class='sj-hs-row'><span class='sj-hs-icon'>🔍</span>"
            f"<span>≈ <span>{h['google']:.1f}</span> Google searches</span></div>"
            f"<div class='sj-hs-row'><span class='sj-hs-icon'>💧</span>"
            f"<span><span>{h['water_ul']:.2f} µl</span> water"
            f"<span style='color:#5a7090;'> (raindrop≈50µl)</span></span></div>"
            f"<div class='sj-hs-row'><span class='sj-hs-icon'>🌱</span>"
            f"<span><span>{h['co2_mg']:.3f} mg</span> CO₂e</span></div>"
        )

    lt = lin.get("total_tokens") or 0
    at = agt.get("total_tokens") or 0
    ld = (lin.get("duration_ns") or 0) / 1e9
    ad = (agt.get("duration_ns") or 0) / 1e9

    return (
        f"<div class='sj-hs-grid'>"
        f"<div class='sj-hs-card'>"
        f"<div class='sj-hs-head' style='color:#22c55e;'>"
        f"Linear · {le*1000:.3f} mJ · {lt} tok · {ld:.1f}s</div>"
        f"{rows(lh)}</div>"
        f"<div class='sj-hs-card'>"
        f"<div class='sj-hs-head' style='color:#ef4444;'>"
        f"Agentic · {ae*1000:.3f} mJ · {at} tok · {ad:.1f}s</div>"
        f"{rows(ah)}</div>"
        f"</div>"
    )


# ── DB helpers ─────────────────────────────────────────────────────────────────


def _fetch_run(run_id: int) -> dict:
    if not run_id:
        return {}
    row = q1(f"""
        SELECT r.run_id, r.workflow_type,
               r.total_energy_uj, r.dynamic_energy_uj,
               r.pkg_energy_uj, r.core_energy_uj,
               r.uncore_energy_uj, r.dram_energy_uj,
               r.avg_power_watts, r.duration_ns,
               r.total_tokens, r.prompt_tokens, r.completion_tokens,
               r.api_latency_ms, r.dns_latency_ms,
               r.ipc, r.cache_miss_rate, r.frequency_mhz,
               r.package_temp_celsius, r.thermal_delta_c,
               r.c6_time_seconds, r.c7_time_seconds,
               r.c2_time_seconds, r.c3_time_seconds,
               r.rss_memory_mb, r.carbon_g, r.water_ml,
               r.llm_calls, r.tool_calls, r.steps,
               r.planning_time_ms, r.execution_time_ms, r.synthesis_time_ms,
               e.model_name, e.task_name, e.provider,
               ots.orchestration_tax_uj, ots.tax_percent
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        LEFT JOIN orchestration_tax_summary ots
               ON r.run_id = ots.agentic_run_id
        WHERE r.run_id = {run_id}
    """)
    return row or {}


def _fetch_response(run_id: int) -> str | None:
    if not run_id:
        return None
    row = q1(
        f"SELECT response FROM llm_interactions "
        f"WHERE run_id={run_id} ORDER BY interaction_id DESC LIMIT 1"
    )
    if row and row.get("response"):
        return str(row["response"])[:300]
    return None


def _efficiency_rank(run_id: int) -> tuple[int, int]:
    if not run_id:
        return 0, 0
    row = q1(f"""
        SELECT COUNT(*) AS n FROM runs r2
        JOIN runs r1 ON r1.run_id = {run_id}
        WHERE COALESCE(r2.energy_per_token, 0) <
              COALESCE(r1.energy_per_token, 9999)
    """)
    total = (q1("SELECT COUNT(*) AS n FROM runs") or {}).get("n", 1) or 1
    return int((row or {}).get("n", 0)) + 1, int(total)


# ── Subprocess — copied exactly from execute.py ───────────────────────────────


def _run_harness(
    task_arg: str, task_id: str | None, provider: str, store: dict
) -> None:
    """
    Single subprocess call — harness runs BOTH linear + agentic internally.
    Exactly mirrors execute.py cmd construction.
    Results stored in store dict: ok, lin_id, agt_id, err.
    """
    if task_id:
        # predefined task
        cmd = [
            "python",
            "-m",
            "core.execution.tests.test_harness",
            "--task-id",
            task_id,
            "--provider",
            provider,
            "--repetitions",
            "1",
            "--country",
            "US",
            "--cool-down",
            "5",
            "--save-db",
        ]
    else:
        # custom prompt
        cmd = [
            "python",
            "-m",
            "core.execution.tests.test_harness",
            "--task",
            task_arg,
            "--provider",
            provider,
            "--repetitions",
            "1",
            "--country",
            "US",
            "--cool-down",
            "5",
            "--save-db",
        ]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=600
        )
        ok = proc.returncode == 0
        # Fetch run_ids by latest exp_id — harness saves both workflows per exp
        latest = q1("SELECT exp_id FROM experiments ORDER BY exp_id DESC LIMIT 1")
        exp_id = (latest or {}).get("exp_id")
        lin_id = agt_id = None
        if exp_id:
            rows = q(f"""SELECT run_id, workflow_type FROM runs
                         WHERE exp_id = {exp_id}
                         ORDER BY run_id DESC LIMIT 4""")
            if rows is not None:
                rlist = rows if isinstance(rows, list) else rows.to_dict("records")
                lin_id = next(
                    (r["run_id"] for r in rlist if r.get("workflow_type") == "linear"),
                    None,
                )
                agt_id = next(
                    (r["run_id"] for r in rlist if r.get("workflow_type") == "agentic"),
                    None,
                )
        store["ok"] = ok
        store["lin_id"] = lin_id
        store["agt_id"] = agt_id
        store["err"] = f"{proc.stdout[-400:]}\n{proc.stderr[-400:]}" if not ok else ""
    except Exception as e:
        store["ok"] = False
        store["lin_id"] = None
        store["agt_id"] = None
        store["err"] = str(e)


# ── Charts ────────────────────────────────────────────────────────────────────


def _chart_rapl(lin: dict, agt: dict):
    domains = ["PKG", "Core", "Uncore", "DRAM"]
    lv = [
        (lin.get("pkg_energy_uj") or 0) / 1e6,
        (lin.get("core_energy_uj") or 0) / 1e6,
        (lin.get("uncore_energy_uj") or 0) / 1e6,
        (lin.get("dram_energy_uj") or 0) / 1e6,
    ]
    av = [
        (agt.get("pkg_energy_uj") or 0) / 1e6,
        (agt.get("core_energy_uj") or 0) / 1e6,
        (agt.get("uncore_energy_uj") or 0) / 1e6,
        (agt.get("dram_energy_uj") or 0) / 1e6,
    ]
    fig = go.Figure()
    fig.add_bar(
        name="Linear", x=domains, y=lv, marker_color=WF_COLORS["linear"], opacity=0.85
    )
    fig.add_bar(
        name="Agentic", x=domains, y=av, marker_color=WF_COLORS["agentic"], opacity=0.85
    )
    fig.update_layout(
        **{
            **PL,
            "barmode": "group",
            "margin": dict(l=40, r=20, t=36, b=30),
            "title": dict(text="RAPL domain energy (J)", font=dict(size=11)),
        }
    )
    return fig


def _chart_tax(lin: dict, agt: dict):
    le = (lin.get("total_energy_uj") or 0) / 1e6
    ae = (agt.get("total_energy_uj") or 0) / 1e6
    fig = go.Figure()
    fig.add_bar(
        x=["Linear", "Agentic"],
        y=[le, ae],
        marker_color=[WF_COLORS["linear"], WF_COLORS["agentic"]],
        text=[f"{le:.4f}J", f"{ae:.4f}J"],
        textposition="outside",
        opacity=0.9,
    )
    tax = ae / le if le > 0 else 0
    fig.update_layout(
        **{
            **PL,
            "margin": dict(l=40, r=20, t=50, b=30),
            "title": dict(text=f"Orchestration tax: {tax:.1f}×", font=dict(size=11)),
        }
    )
    return fig


def _chart_cstates(lin: dict, agt: dict):
    states = ["C2", "C3", "C6", "C7"]
    lv = [
        lin.get("c2_time_seconds") or 0,
        lin.get("c3_time_seconds") or 0,
        lin.get("c6_time_seconds") or 0,
        lin.get("c7_time_seconds") or 0,
    ]
    av = [
        agt.get("c2_time_seconds") or 0,
        agt.get("c3_time_seconds") or 0,
        agt.get("c6_time_seconds") or 0,
        agt.get("c7_time_seconds") or 0,
    ]
    fig = go.Figure()
    fig.add_bar(
        name="Linear", x=states, y=lv, marker_color=WF_COLORS["linear"], opacity=0.85
    )
    fig.add_bar(
        name="Agentic", x=states, y=av, marker_color=WF_COLORS["agentic"], opacity=0.85
    )
    fig.update_layout(
        **{
            **PL,
            "barmode": "group",
            "margin": dict(l=40, r=20, t=36, b=30),
            "title": dict(text="C-state residency (s)", font=dict(size=11)),
        }
    )
    return fig


def _chart_phases(agt: dict):
    phases = ["Planning", "Execution", "Synthesis"]
    vals = [
        agt.get("planning_time_ms") or 0,
        agt.get("execution_time_ms") or 0,
        agt.get("synthesis_time_ms") or 0,
    ]
    fig = go.Figure()
    fig.add_bar(
        x=phases,
        y=vals,
        marker_color=["#a78bfa", "#ef4444", "#f59e0b"],
        text=[f"{v:.0f}ms" for v in vals],
        textposition="outside",
        opacity=0.9,
    )
    fig.update_layout(
        **{
            **PL,
            "margin": dict(l=40, r=20, t=36, b=30),
            "title": dict(text="Agentic phase breakdown (ms)", font=dict(size=11)),
        }
    )
    return fig


def _chart_vs_history(run_id: int | None, wf: str):
    if not run_id:
        return None
    rows = q(f"""
        SELECT r.run_id,
               COALESCE(r.energy_per_token,
                        r.total_energy_uj / NULLIF(r.total_tokens, 0)) AS ept,
               r.api_latency_ms
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        WHERE e.workflow_type = '{wf}' AND r.total_tokens > 0
        ORDER BY r.run_id DESC LIMIT 200
    """)
    if rows is None:
        return None
    df = pd.DataFrame(rows) if isinstance(rows, list) else rows.copy()
    if df.empty:
        return None
    df = df.dropna(subset=["ept"])
    if df.empty:
        return None
    curr = df[df["run_id"] == run_id]
    rest = df[df["run_id"] != run_id]
    fig = go.Figure()
    if not rest.empty:
        fig.add_scatter(
            x=rest["api_latency_ms"],
            y=rest["ept"],
            mode="markers",
            marker=dict(size=5, color="#2d3f55", opacity=0.5),
            name="Historical",
        )
    if not curr.empty:
        fig.add_scatter(
            x=curr["api_latency_ms"],
            y=curr["ept"],
            mode="markers",
            marker=dict(
                size=13,
                color=WF_COLORS.get(wf, "#22c55e"),
                symbol="star",
                line=dict(width=1.5, color="#e8f0f8"),
            ),
            name="This run",
        )
    fig.update_layout(
        **{
            **PL,
            "margin": dict(l=40, r=20, t=36, b=30),
            "title": dict(
                text=f"{wf.capitalize()} — energy/token vs latency", font=dict(size=11)
            ),
            "xaxis_title": "API latency (ms)",
            "yaxis_title": "Energy/token (µJ)",
        }
    )
    return fig


# ── Deep analysis ─────────────────────────────────────────────────────────────


def _deep_analysis(
    idx: int, lin: dict, agt: dict, lin_id: int | None, agt_id: int | None, task: str
):
    key = f"sj_exp_{idx}"
    if key not in st.session_state:
        st.session_state[key] = False

    le = (lin.get("total_energy_uj") or 0) / 1e6
    ae = (agt.get("total_energy_uj") or 0) / 1e6
    tax = ae / le if le > 0 else 0

    btn_lbl = (
        "▲ Hide silicon story"
        if st.session_state[key]
        else "See what happened in silicon →"
    )
    if st.button(btn_lbl, key=f"sj_see_{idx}", use_container_width=True):
        st.session_state[key] = not st.session_state[key]
        st.rerun()

    if not st.session_state[key]:
        return

    # Silicon story panel
    tax_color = "#22c55e" if tax < 1.5 else ("#f59e0b" if tax < 3 else "#ef4444")
    tax_msg = (
        f"Agentic used {tax:.1f}× more energy for the same answer. "
        f"For '{task[:40]}', orchestration overhead outweighed the benefit."
        if tax > 1.2
        else "Linear and agentic had similar cost — this task suited agentic well."
    )

    st.markdown(
        f"<div class='sj-silicon'>"
        f"<div class='sj-si-title'>Silicon story</div>"
        f"<div class='sj-tax-box'>"
        f"<div class='sj-tax-val' style='color:{tax_color};'>{tax:.1f}×</div>"
        f"<div class='sj-tax-lbl'>orchestration tax — agentic vs linear</div>"
        f"<div class='sj-tax-txt'>{tax_msg}</div>"
        f"</div>"
        f"{_hs_html(lin, agt)}"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Analysis tabs
    rank, total = _efficiency_rank(agt_id or lin_id or 0)

    t1, t2, t3, t4, t5, t6 = st.tabs(
        [
            "🏆 Summary",
            "⚡ Energy",
            "🌡️ Thermal",
            "🧠 CPU",
            "📋 vs History",
            "💾 Export",
        ]
    )

    with t1:
        c1, c2, c3 = st.columns(3)
        c1.metric("Orchestration tax", f"{tax:.1f}×")
        c2.metric("Efficiency rank", f"#{rank} / {total}")
        dt = lin.get("thermal_delta_c") or agt.get("thermal_delta_c") or 0
        c3.metric("Thermal delta", f"+{dt:.1f}°C")
        lc67 = (lin.get("c6_time_seconds") or 0) + (lin.get("c7_time_seconds") or 0)
        ld = (lin.get("duration_ns") or 1) / 1e9
        c67p = min(100, lc67 / max(ld, 0.001) * 100)
        st.info(
            f"C6+C7 residency: {c67p:.0f}% — "
            f"{'good deep sleep between tokens' if c67p > 30 else 'low idle efficiency'}"
        )

    with t2:
        if lin and agt:
            st.plotly_chart(
                _chart_rapl(lin, agt), use_container_width=True, key=f"sj_rapl_{idx}"
            )
            st.plotly_chart(
                _chart_tax(lin, agt), use_container_width=True, key=f"sj_tax_{idx}"
            )

    with t3:
        pt = lin.get("package_temp_celsius") or agt.get("package_temp_celsius")
        if pt:
            st.metric("Package temp", f"{pt:.1f}°C", f"Δ {dt:+.1f}°C from baseline")
        if lin and agt:
            st.plotly_chart(
                _chart_cstates(lin, agt), use_container_width=True, key=f"sj_cst_{idx}"
            )

    with t4:
        c1, c2 = st.columns(2)
        ipc = lin.get("ipc") or agt.get("ipc")
        cmr = lin.get("cache_miss_rate") or agt.get("cache_miss_rate")
        frq = lin.get("frequency_mhz") or agt.get("frequency_mhz")
        if ipc:
            c1.metric("IPC", f"{ipc:.2f}")
        if cmr:
            c2.metric("Cache miss rate", f"{cmr:.1f}%")
        if frq:
            c1.metric("Frequency", f"{frq:.0f} MHz")
        if agt:
            st.plotly_chart(
                _chart_phases(agt), use_container_width=True, key=f"sj_ph_{idx}"
            )

    with t5:
        fl = _chart_vs_history(lin_id, "linear")
        fa = _chart_vs_history(agt_id, "agentic")
        if fl:
            st.plotly_chart(fl, use_container_width=True, key=f"sj_hl_{idx}")
        if fa:
            st.plotly_chart(fa, use_container_width=True, key=f"sj_ha_{idx}")
        if not fl and not fa:
            st.info("Not enough historical runs yet.")

    with t6:
        # PDF
        if st.button(
            "↓ Generate PDF report", key=f"sj_pdf_{idx}", use_container_width=True
        ):
            with st.spinner("Generating PDF…"):
                try:
                    from gui.pages.session_analysis import _generate_pdf

                    run_ids = [x for x in [lin_id, agt_id] if x]
                    if run_ids:
                        ph = ",".join(str(x) for x in run_ids)
                        _r = q(f"SELECT * FROM runs WHERE run_id IN ({ph})")
                        rdf = (
                            _r
                            if isinstance(_r, pd.DataFrame)
                            else pd.DataFrame(_r or [])
                        )
                        _e = q(f"""SELECT e.* FROM experiments e
                            JOIN runs r ON r.exp_id = e.exp_id
                            WHERE r.run_id IN ({ph}) GROUP BY e.exp_id""")
                        edf = (
                            _e
                            if isinstance(_e, pd.DataFrame)
                            else pd.DataFrame(_e or [])
                        )
                        # Tax DataFrame with all columns _generate_pdf expects
                        tdf = pd.DataFrame(q(f"""
                            SELECT ots.*,
                            CASE WHEN rl.total_energy_uj > 0
                                 THEN CAST(ra.total_energy_uj AS REAL)/rl.total_energy_uj
                                 ELSE 1.0 END                     AS tax_multiplier,
                            rl.total_energy_uj/1e6                AS linear_energy_j,
                            ra.total_energy_uj/1e6                AS agentic_energy_j,
                            rl.duration_ns/1e6                    AS linear_ms,
                            ra.duration_ns/1e6                    AS agentic_ms,
                            el.task_name, el.provider, el.model_name,
                            rl.ipc               AS linear_ipc,
                            ra.ipc               AS agentic_ipc,
                            rl.cache_miss_rate   AS linear_cmr,
                            ra.cache_miss_rate   AS agentic_cmr,
                            rl.max_temp_c        AS linear_max_temp,
                            ra.max_temp_c        AS agentic_max_temp,
                            rl.thermal_delta_c   AS linear_tdelta,
                            ra.thermal_delta_c   AS agentic_tdelta,
                            ra.llm_calls, ra.tool_calls, ra.steps,
                            ra.planning_time_ms, ra.execution_time_ms,
                            ra.synthesis_time_ms,
                            ra.carbon_g, ra.water_ml, ra.methane_mg,
                            ots.tax_percent
                            FROM orchestration_tax_summary ots
                            JOIN runs rl ON rl.run_id = ots.linear_run_id
                            JOIN runs ra ON ra.run_id = ots.agentic_run_id
                            JOIN experiments el ON el.exp_id = rl.exp_id
                            WHERE ots.agentic_run_id IN ({ph})
                               OR ots.linear_run_id  IN ({ph})"""))
                        gid = (
                            edf["group_id"].iloc[0]
                            if not edf.empty
                            else "silicon_journey"
                        )
                        pdf_bytes = _generate_pdf(gid, edf, rdf, tdf)
                        st.download_button(
                            "↓ Download PDF",
                            data=pdf_bytes,
                            file_name=(
                                f"silicon_{gid[:16]}_"
                                f"{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
                            ),
                            mime="application/pdf",
                            key=f"sj_dl_pdf_{idx}",
                        )
                except Exception as e:
                    import traceback

                    st.error(f"PDF error: {e}")
                    st.code(traceback.format_exc())

        # CSV
        run_ids = [x for x in [lin_id, agt_id] if x]
        if run_ids:
            ph = ",".join(str(x) for x in run_ids)
            rcsv = q(f"SELECT * FROM runs WHERE run_id IN ({ph})")
            if rcsv is not None:
                cdf = pd.DataFrame(rcsv) if isinstance(rcsv, list) else rcsv
                if not cdf.empty:
                    st.download_button(
                        "↓ Export CSV",
                        data=cdf.to_csv(index=False),
                        file_name=f"silicon_journey_{idx}.csv",
                        mime="text/csv",
                        key=f"sj_csv_{idx}",
                    )

        # Navigation shortcuts
        if st.button(
            "→ Open in Run Explorer", key=f"sj_exp_{idx}_nav", use_container_width=True
        ):
            if lin_id or agt_id:
                st.session_state["explorer_run_id"] = lin_id or agt_id
            st.session_state["nav_section"] = "SESSIONS & RUNS"
            st.session_state["nav_page"] = "explorer"
            st.rerun()

        if st.button(
            "→ Full Session Analysis", key=f"sj_sa_{idx}", use_container_width=True
        ):
            st.session_state["nav_section"] = "SESSIONS & RUNS"
            st.session_state["nav_page"] = "session_analysis"
            st.rerun()


# ── Render a single chat message ───────────────────────────────────────────────


def _render_msg(msg: dict, idx: int):
    role = msg["role"]

    if role == "user":
        st.markdown(
            f"<div class='sj-user-row'>"
            f"<div class='sj-user-bbl'>{msg['text']}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

    elif role == "error":
        st.markdown(
            f"<div class='sj-bot-row'>"
            f"<div class='sj-bot-lbl'>A-LEMS · error</div>"
            f"<div class='sj-error-box'>"
            f"<div class='sj-err-msg'>⚠ {msg.get('err','Run failed')[:300]}</div>",
            unsafe_allow_html=True,
        )
        c1, c2 = st.columns(2)
        with c1:
            if st.button("⟳ Retry", key=f"sj_retry_{idx}", use_container_width=True):
                st.session_state["sj_retry"] = msg.get("task", "")
                st.rerun()
        with c2:
            if st.button(
                "✕ Dismiss", key=f"sj_dismiss_{idx}", use_container_width=True
            ):
                msgs = st.session_state.get("sj_messages", [])
                real_idx = len(msgs) - 1 - idx
                if 0 <= real_idx < len(msgs):
                    msgs.pop(real_idx)
                st.rerun()
        st.markdown("</div></div>", unsafe_allow_html=True)

    elif role == "result":
        lin = msg.get("lin_data", {})
        agt = msg.get("agt_data", {})
        lin_resp = msg.get("lin_response")
        agt_resp = msg.get("agt_response")
        lin_id = msg.get("lin_id")
        agt_id = msg.get("agt_id")
        task = msg.get("task", "")
        model = msg.get("model", "")

        le_uj = lin.get("total_energy_uj") or 0
        ae_uj = agt.get("total_energy_uj") or 0
        te = (le_uj + ae_uj) / 1e3
        tw = (lin.get("water_ml") or 0) + (agt.get("water_ml") or 0)
        tc = (lin.get("carbon_g") or 0) + (agt.get("carbon_g") or 0)
        tt = (lin.get("total_tokens") or 0) + (agt.get("total_tokens") or 0)
        tax = ae_uj / le_uj if le_uj > 0 else 0

        st.markdown(
            f"<div class='sj-bot-row'>"
            f"<div class='sj-bot-lbl'>A-LEMS · {model}</div>"
            f"<div class='sj-reveal'>",
            unsafe_allow_html=True,
        )

        st.markdown(
            "<div class='sj-hook'>"
            "While you were chatting, something interesting happened in silicon…"
            "</div>",
            unsafe_allow_html=True,
        )

        # Side-by-side responses
        col_l, col_a = st.columns(2)
        with col_l:
            lms = (lin.get("duration_ns") or 0) / 1e6
            ltok = lin.get("total_tokens") or 0
            lcl = lin.get("llm_calls") or 1
            st.markdown(
                f"<div class='sj-wf-card'>"
                f"<div class='sj-wf-head'>"
                f"<div class='sj-wf-dot' style='background:#22c55e;'></div>"
                f"<span class='sj-wf-name' style='color:#22c55e;'>Linear</span></div>"
                + (
                    f"<div class='sj-wf-resp'>\"{lin_resp}\"</div>"
                    if lin_resp
                    else "<div class='sj-wf-nodata'>Response not captured — run succeeded</div>"
                )
                + f"<div class='sj-wf-meta'>"
                f"{le_uj/1e3:.2f} mJ · {ltok} tok · {lms:.0f}ms · {lcl} call</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

        with col_a:
            ams = (agt.get("duration_ns") or 0) / 1e6
            atok = agt.get("total_tokens") or 0
            acl = agt.get("llm_calls") or 0
            st.markdown(
                f"<div class='sj-wf-card'>"
                f"<div class='sj-wf-head'>"
                f"<div class='sj-wf-dot' style='background:#ef4444;'></div>"
                f"<span class='sj-wf-name' style='color:#ef4444;'>Agentic</span></div>"
                + (
                    f"<div class='sj-wf-resp'>\"{agt_resp}\"</div>"
                    if agt_resp
                    else "<div class='sj-wf-nodata'>Response not captured — run succeeded</div>"
                )
                + f"<div class='sj-wf-meta'>"
                f"{ae_uj/1e3:.2f} mJ · {atok} tok · {ams:.0f}ms · {acl} calls</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

        # Subtle meters
        st.markdown(
            f"<div class='sj-meters'>"
            f"<div class='sj-m'><div class='sj-m-icon'>⚡</div>"
            f"<div class='sj-m-val'>{te:.2f}</div><div class='sj-m-lbl'>mJ</div></div>"
            f"<div class='sj-m'><div class='sj-m-icon'>💧</div>"
            f"<div class='sj-m-val'>{tw:.3f}</div><div class='sj-m-lbl'>ml water</div></div>"
            f"<div class='sj-m'><div class='sj-m-icon'>🌱</div>"
            f"<div class='sj-m-val'>{tc:.4f}</div><div class='sj-m-lbl'>g CO₂</div></div>"
            f"<div class='sj-m'><div class='sj-m-icon'>◈</div>"
            f"<div class='sj-m-val'>{tt}</div><div class='sj-m-lbl'>tokens</div></div>"
            f"</div>",
            unsafe_allow_html=True,
        )

        # Insight pills
        pills = []
        if tax > 0:
            pills.append(f"Agentic used <b>{tax:.1f}×</b> more energy")
        api_lat = lin.get("api_latency_ms") or agt.get("api_latency_ms")
        if api_lat and api_lat > 200:
            pills.append(f"Network bottleneck at <b>{api_lat:.0f}ms</b>")
        lc67 = (lin.get("c6_time_seconds") or 0) + (lin.get("c7_time_seconds") or 0)
        ld = (lin.get("duration_ns") or 1) / 1e9
        c67p = min(100, lc67 / max(ld, 0.001) * 100)
        if c67p > 20:
            pills.append(f"C6+C7 residency <b>{c67p:.0f}%</b>")
        rank, total_runs = _efficiency_rank(agt_id or lin_id or 0)
        if total_runs > 0:
            pills.append(f"Rank <b>#{rank}</b> of {total_runs} runs")
        if pills:
            st.markdown(
                "<div class='sj-pills'>"
                + "".join(f"<div class='sj-pill'>{p}</div>" for p in pills)
                + "</div>",
                unsafe_allow_html=True,
            )

        st.markdown("</div>", unsafe_allow_html=True)

        # Deep analysis inline
        _deep_analysis(idx, lin, agt, lin_id, agt_id, task)


# ── Session cumulative bar ─────────────────────────────────────────────────────


def _session_bar():
    msgs = st.session_state.get("sj_messages", [])
    te = tw = tc = tt = 0.0
    for m in msgs:
        if m["role"] != "result":
            continue
        for d in [m.get("lin_data", {}), m.get("agt_data", {})]:
            te += (d.get("total_energy_uj") or 0) / 1e3
            tw += d.get("water_ml") or 0
            tc += d.get("carbon_g") or 0
            tt += d.get("total_tokens") or 0
    if te == 0:
        return
    st.markdown(
        f"<div class='sj-session-bar'>"
        f"Session total &nbsp;·&nbsp;"
        f"<b style='color:#f59e0b;'>⚡ {te:.2f} mJ</b> &nbsp;"
        f"<b style='color:#38bdf8;'>💧 {tw:.3f} ml</b> &nbsp;"
        f"<b style='color:#34d399;'>🌱 {tc:.4f}g CO₂</b> &nbsp;"
        f"<b style='color:#a78bfa;'>◈ {int(tt)} tok</b>"
        f"</div>",
        unsafe_allow_html=True,
    )


# ── Input dock ────────────────────────────────────────────────────────────────


def _input_dock(models: dict, tasks: list[dict]):
    """
    Clean input dock — exactly like research_insights.py preset sync pattern.
    Dropdown selects task → prompt fills automatically.
    User edits prompt → becomes custom --task.
    """
    # Build model options: display_name → (env, provider)
    model_options: dict[str, tuple] = {}
    for env, wfs in models.items():
        for wf, cfg in wfs.items():
            if wf == "linear":
                # env is "cloud" or "local" — that's what --provider expects
                model_options[cfg["name"]] = (env, env)

    model_names = list(model_options.keys())
    if "sj_model_idx" not in st.session_state:
        st.session_state["sj_model_idx"] = 0
    if "sj_prev_task" not in st.session_state:
        st.session_state["sj_prev_task"] = ""

    cur_model = model_names[st.session_state["sj_model_idx"] % len(model_names)]

    task_ids = [t["id"] for t in tasks]
    task_labels = {t["id"]: f"{t['name']}  ·  {t['category']}" for t in tasks}
    task_prompts = {t["id"]: t["prompt"].strip() for t in tasks}

    # ── Dropdown ─────────────────────────────────────────────────────────────
    sel_id = st.selectbox(
        "Task",
        options=[""] + task_ids,
        format_func=lambda x: (
            "Select a task…  (or type below)" if x == "" else task_labels.get(x, x)
        ),
        key="sj_task_sel",
        label_visibility="collapsed",
    )

    # ── Sync selected task → text area (same pattern as research_insights.py) ─
    prev = st.session_state.get("sj_prev_task", "")
    if sel_id != prev:
        st.session_state["sj_prev_task"] = sel_id
        if sel_id:
            st.session_state["sj_prompt_text"] = task_prompts[sel_id]
        else:
            st.session_state["sj_prompt_text"] = ""

    # ── Text area ─────────────────────────────────────────────────────────────
    prompt = st.text_area(
        "Your question",
        height=80,
        key="sj_prompt_text",
        placeholder="Ask me anything…  or select a task above",
        label_visibility="collapsed",
    )

    # ── Model + send row ──────────────────────────────────────────────────────
    mc1, mc2, mc3 = st.columns([4, 1, 1])
    with mc1:
        st.markdown(
            f"<div style='font-size:9px;color:#7090b0;padding:5px 2px;'>"
            f"⬡ <b style='color:#a0b8c8;'>{cur_model}</b>"
            f"<span style='color:#7090b0;'> · linear + agentic</span></div>",
            unsafe_allow_html=True,
        )
    with mc2:
        if st.button("⇄ model", key="sj_sw"):
            st.session_state["sj_model_idx"] = (
                st.session_state["sj_model_idx"] + 1
            ) % len(model_names)
            st.rerun()
    with mc3:
        send = st.button(
            "↑ Send", key="sj_send", type="primary", use_container_width=True
        )

    # ── Retry ─────────────────────────────────────────────────────────────────
    retry = st.session_state.pop("sj_retry", None)
    if retry:
        env, provider = list(model_options.values())[0]
        return retry, None, provider, model_names[0]

    if send:
        typed = prompt.strip()
        if not typed and not sel_id:
            st.warning("Type a question or select a task.")
            return None, None, "", ""
        if typed:
            # Check if typed text matches a task prompt → use task-id
            matched_id = next(
                (tid for tid, tp in task_prompts.items() if tp == typed), None
            )
            task_id = matched_id or (sel_id if sel_id else None)
            task_arg = typed
        else:
            # Nothing typed — use selected task prompt
            task_id = sel_id
            task_arg = task_prompts[sel_id]

        _, provider = model_options.get(cur_model, ("cloud", "cloud"))
        # Clear state after send
        st.session_state["sj_prev_task"] = ""
        return task_arg, task_id, provider, cur_model

    return None, None, "", ""


# ── Main render ────────────────────────────────────────────────────────────────


def render(ctx: dict):
    _css()

    # Page header
    st.markdown(
        "<div style='padding:6px 0 14px;border-bottom:0.5px solid #0d1520;"
        "margin-bottom:8px;'>"
        "<div style='display:flex;align-items:center;gap:8px;'>"
        "<span style='font-size:9px;padding:2px 8px;border-radius:100px;"
        "background:#22c55e18;color:#22c55e;border:0.5px solid #22c55e33;'>"
        "SILICON JOURNEY</span></div>"
        "<div style='font-size:11px;color:#5a7090;margin-top:4px;'>"
        "Curious what your query does — from silicon to model? Find out here.</div>"
        "</div>",
        unsafe_allow_html=True,
    )

    # Init state
    defaults = [
        ("sj_messages", []),
        ("sj_running", False),
        ("sj_run_task", None),
        ("sj_run_task_id", None),
        ("sj_run_prov", None),
        ("sj_run_model", None),
        ("sj_did_run", False),
    ]
    for k, v in defaults:
        if k not in st.session_state:
            st.session_state[k] = v

    models = _load_models()
    tasks = _load_tasks()

    # ── Input dock ────────────────────────────────────────────────────────────
    task_arg, task_id, provider, model_name = _input_dock(models, tasks)

    # ── Trigger new run ───────────────────────────────────────────────────────
    if task_arg and not st.session_state["sj_running"]:
        st.session_state["sj_messages"].append({"role": "user", "text": task_arg})
        st.session_state["sj_running"] = True
        st.session_state["sj_did_run"] = False
        st.session_state["sj_run_task"] = task_arg
        st.session_state["sj_run_task_id"] = task_id
        st.session_state["sj_run_prov"] = provider
        st.session_state["sj_run_model"] = model_name
        st.rerun()

    # ── Execute run — only once per trigger ───────────────────────────────────
    if st.session_state["sj_running"] and not st.session_state["sj_did_run"]:
        _task = st.session_state["sj_run_task"]
        _task_id = st.session_state["sj_run_task_id"]
        _prov = st.session_state["sj_run_prov"]
        _model = st.session_state["sj_run_model"]

        st.session_state["sj_did_run"] = True  # prevent re-entry on rerun

        results: dict = {}
        with st.status("Measuring energy on silicon…", expanded=True) as status:
            st.write(f"⚡ Running on {_model}…")
            st.write("⏱ This takes 30–120 seconds. Linear + agentic run in parallel…")
            t = threading.Thread(
                target=_run_harness, args=(_task, _task_id, _prov, results)
            )
            t.start()
            t.join(timeout=600)
            if t.is_alive():
                results["ok"] = False
                results["err"] = "Timed out after 10 minutes"
            st.write("◎ Fetching measurements from DB…")
            status.update(label="Complete", state="complete")

        any_ok = results.get("ok", False)
        lin_id = results.get("lin_id")
        agt_id = results.get("agt_id")

        if not any_ok:
            err = results.get("err") or "Run failed — check terminal for details"
            st.session_state["sj_messages"].append(
                {"role": "error", "err": err, "task": _task}
            )
        else:
            st.session_state["sj_messages"].append(
                {
                    "role": "result",
                    "task": _task,
                    "model": _model,
                    "lin_id": lin_id,
                    "agt_id": agt_id,
                    "lin_data": _fetch_run(lin_id) if lin_id else {},
                    "agt_data": _fetch_run(agt_id) if agt_id else {},
                    "lin_response": _fetch_response(lin_id),
                    "agt_response": _fetch_response(agt_id),
                }
            )

        st.session_state["sj_running"] = False
        st.session_state["sj_did_run"] = False
        st.session_state["sj_run_task"] = None
        st.session_state["sj_run_task_id"] = None
        st.session_state["sj_run_prov"] = None
        st.session_state["sj_run_model"] = None
        st.rerun()

    # ── Chat history — newest first ───────────────────────────────────────────
    msgs = st.session_state.get("sj_messages", [])
    if msgs:
        for i, msg in enumerate(reversed(msgs)):
            _render_msg(msg, len(msgs) - 1 - i)
        _session_bar()
    else:
        st.markdown(
            "<div class='sj-empty'>"
            "<div style='font-size:28px;margin-bottom:12px;opacity:.3;'>⬡</div>"
            "<div style='font-size:16px;color:#7090b0;margin-bottom:8px;'>"
            "How can I help you today?</div>"
            "<div style='font-size:11px;color:#7090b0;line-height:1.7;'>"
            "Select a task or type your own question.<br>"
            "Runs on real silicon. Shows you the energy story.</div>"
            "</div>",
            unsafe_allow_html=True,
        )
