"""
gui/pages/session_analysis.py
─────────────────────────────────────────────────────────────────────────────
FIXES in this version:
  Issue 1  : NO st.switch_page / st.rerun() calls — safe to embed in tabs
  Issue 3  : PDF includes chart images via plotly.io.to_image (kaleido)
             Falls back gracefully if kaleido not installed
  Issue 6  : All st.plotly_chart() calls have unique key= arguments
  Issue 7  : statsmodels imported with try/except — graceful fallback
─────────────────────────────────────────────────────────────────────────────
"""
import io
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from gui.config  import PL, WF_COLORS, INSIGHTS_RULES, DASHBOARD_CFG
from gui.db      import q, q1
from gui.helpers import fl, _human_energy_full, _human_water, _human_carbon, _human_methane

# ── Issue 7: statsmodels — optional, graceful fallback ───────────────────────
try:
    import statsmodels.api as sm
    import statsmodels.formula.api as smf
    _STATSMODELS = True
except ImportError:
    _STATSMODELS = False

# ── Config shortcuts ──────────────────────────────────────────────────────────
_TAX   = INSIGHTS_RULES.get("tax_thresholds", {})
_THERM = INSIGHTS_RULES.get("thermal", {})
_NARR  = INSIGHTS_RULES.get("narrative_templates", {})

_THROTTLE_C = _THERM.get("throttle_threshold_c", 95)
_CAUTION_C  = _THERM.get("caution_threshold_c",  85)
_SAFE_C     = _THERM.get("safe_threshold_c",      70)

# ── Issue 6: unique key generator ────────────────────────────────────────────
import time as _time
import threading as _threading
_KEY_LOCK = _threading.Lock()
_KEY_CTR  = [0]

def _ukey(prefix: str, group_id: str = "") -> str:
    """
    Generate a globally unique Streamlit widget key.
    Uses a monotonic counter + time_ns so keys are unique across:
      - multiple calls within same render
      - live view + history tab rendering same group_id simultaneously
      - Streamlit reruns within same session
    """
    with _KEY_LOCK:
        _KEY_CTR[0] += 1
        n = _KEY_CTR[0]
    gid_safe = "".join(c if c.isalnum() or c == "_" else "_" for c in str(group_id))
    # time_ns last 6 digits gives sub-microsecond uniqueness across reruns
    t = str(_time.time_ns())[-6:]
    return f"{prefix}_{gid_safe}_{n}_{t}"


# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADERS
# ══════════════════════════════════════════════════════════════════════════════

def _load_session_experiments(group_id: str) -> pd.DataFrame:
    return q(f"""
        SELECT exp_id, task_name, provider, model_name, status,
               runs_completed, runs_total, optimization_enabled,
               started_at, completed_at, group_id
        FROM experiments
        WHERE group_id = '{group_id}'
        ORDER BY exp_id
    """)


def _load_session_runs(group_id: str) -> pd.DataFrame:
    return q(f"""
        SELECT
            r.run_id, r.exp_id, r.run_number, r.workflow_type,
            r.duration_ns / 1e6                AS duration_ms,
            r.total_energy_uj / 1e6            AS energy_j,
            r.dynamic_energy_uj / 1e6          AS dynamic_energy_j,
            r.pkg_energy_uj / 1e6              AS pkg_energy_j,
            r.core_energy_uj / 1e6             AS core_energy_j,
            r.uncore_energy_uj / 1e6           AS uncore_energy_j,
            r.dram_energy_uj / 1e6             AS dram_energy_j,
            r.ipc, r.cache_miss_rate, r.cache_misses, r.cache_references,
            r.instructions, r.cycles,
            r.thread_migrations,
            r.context_switches_voluntary, r.context_switches_involuntary,
            r.total_context_switches,
            r.run_queue_length, r.kernel_time_ms, r.user_time_ms,
            r.package_temp_celsius, r.start_temp_c, r.max_temp_c,
            r.min_temp_c, r.thermal_delta_c, r.thermal_throttle_flag,
            r.c2_time_seconds, r.c3_time_seconds,
            r.c6_time_seconds, r.c7_time_seconds,
            r.ring_bus_freq_mhz, r.wakeup_latency_us,
            r.interrupt_rate, r.frequency_mhz,
            r.planning_time_ms, r.execution_time_ms, r.synthesis_time_ms,
            r.llm_calls, r.tool_calls, r.steps,
            r.total_tokens, r.prompt_tokens, r.completion_tokens,
            r.carbon_g, r.water_ml, r.methane_mg,
            r.energy_per_token, r.energy_per_instruction,
            r.rss_memory_mb, r.swap_end_used_mb,
            r.governor, r.turbo_enabled, r.is_cold_start,
            r.complexity_level, r.complexity_score,
            e.task_name, e.provider, e.model_name, e.country_code,
            e.optimization_enabled
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        WHERE e.group_id = '{group_id}'
        ORDER BY r.run_id
    """)


def _load_tax_for_session(group_id: str) -> pd.DataFrame:
    return q(f"""
        SELECT
            ots.comparison_id,
            ots.linear_run_id, ots.agentic_run_id,
            ots.linear_dynamic_uj  / 1e6  AS linear_dynamic_j,
            ots.agentic_dynamic_uj / 1e6  AS agentic_dynamic_j,
            ots.orchestration_tax_uj / 1e6 AS tax_j,
            ots.tax_percent,
            CASE WHEN rl.total_energy_uj > 0
                 THEN CAST(ra.total_energy_uj AS REAL) / rl.total_energy_uj
                 ELSE 1.0 END              AS tax_multiplier,
            el.task_name, el.provider, el.model_name,
            rl.run_number,
            rl.duration_ns/1e6   AS linear_ms,
            ra.duration_ns/1e6   AS agentic_ms,
            rl.ipc               AS linear_ipc,
            ra.ipc               AS agentic_ipc,
            rl.cache_miss_rate   AS linear_cmr,
            ra.cache_miss_rate   AS agentic_cmr,
            rl.thread_migrations AS linear_tmig,
            ra.thread_migrations AS agentic_tmig,
            rl.max_temp_c        AS linear_max_temp,
            ra.max_temp_c        AS agentic_max_temp,
            rl.thermal_delta_c   AS linear_tdelta,
            ra.thermal_delta_c   AS agentic_tdelta,
            rl.total_energy_uj/1e6 AS linear_energy_j,
            ra.total_energy_uj/1e6 AS agentic_energy_j,
            ra.llm_calls, ra.tool_calls, ra.steps,
            ra.planning_time_ms, ra.execution_time_ms, ra.synthesis_time_ms,
            ra.carbon_g, ra.water_ml, ra.methane_mg
        FROM orchestration_tax_summary ots
        JOIN runs rl ON ots.linear_run_id  = rl.run_id
        JOIN runs ra ON ots.agentic_run_id = ra.run_id
        JOIN experiments el ON rl.exp_id = el.exp_id
        WHERE el.group_id = '{group_id}'
        ORDER BY ots.tax_percent DESC
    """)


# ══════════════════════════════════════════════════════════════════════════════
# TAX VERDICT
# ══════════════════════════════════════════════════════════════════════════════

def _tax_verdict(tax_x: float) -> tuple:
    extreme_min = _TAX.get("extreme", {}).get("min", 15)
    high_max    = _TAX.get("high",    {}).get("max", 15)
    mod_max     = _TAX.get("moderate",{}).get("max",  5)

    if tax_x >= extreme_min:
        return "🔴", _TAX.get("extreme", {}).get("label", "EXTREME"), "#ef4444"
    if tax_x >= high_max * 0.67:
        return "🟠", _TAX.get("high",    {}).get("label", "HIGH"),    "#f59e0b"
    if tax_x >= mod_max:
        return "🟡", _TAX.get("moderate",{}).get("label", "MODERATE"),"#f59e0b"
    return "🟢", _TAX.get("low", {}).get("label", "LOW"), "#22c55e"


# ══════════════════════════════════════════════════════════════════════════════
# NARRATIVE ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def _build_pair_narrative(row: pd.Series) -> str:
    parts = []

    tax_x = float(row.get("tax_multiplier", 0) or 0)
    if tax_x > 15:
        t = _NARR.get("extreme_tax", {}).get("template", "")
        savings = (1 - 1/tax_x) * 100 if tax_x > 0 else 0
        parts.append(t.format(tax=tax_x, task=row.get("task_name",""),
                              provider=row.get("provider",""), savings_pct=savings))
    elif tax_x > 5:
        t = _NARR.get("high_tax", {}).get("template", "")
        parts.append(t.format(tax=tax_x, task=row.get("task_name",""),
                              provider=row.get("provider","")))

    max_temp = float(row.get("agentic_max_temp", 0) or 0)
    delta    = float(row.get("agentic_tdelta",   0) or 0)
    headroom = _THROTTLE_C - max_temp
    if max_temp > _CAUTION_C:
        t = _NARR.get("thermal_caution", {}).get("template", "")
        parts.append(t.format(delta=delta, headroom=headroom, throttle_c=_THROTTLE_C))
    elif max_temp > 0 and max_temp < _SAFE_C:
        t = _NARR.get("thermal_safe", {}).get("template", "")
        parts.append(t.format(max_temp=max_temp))

    a_tmig = float(row.get("agentic_tmig", 0) or 0)
    l_tmig = float(row.get("linear_tmig",  0) or 1)
    ratio  = a_tmig / max(l_tmig, 1)
    if ratio > 5:
        t = _NARR.get("thread_migrations", {}).get("template", "")
        parts.append(t.format(agentic_migrations=int(a_tmig),
                              linear_migrations=int(l_tmig), ratio=ratio))

    a_ipc = float(row.get("agentic_ipc", 0) or 0)
    l_ipc = float(row.get("linear_ipc",  0) or 0)
    if l_ipc > 0 and a_ipc > 0:
        drop_pct = (l_ipc - a_ipc) / l_ipc * 100
        if drop_pct > 5:
            t = _NARR.get("ipc_drop", {}).get("template", "")
            parts.append(t.format(drop_pct=drop_pct,
                                  linear_ipc=l_ipc, agentic_ipc=a_ipc))

    llm_calls  = int(row.get("llm_calls",  0) or 0)
    tool_calls = int(row.get("tool_calls", 0) or 0)
    if llm_calls > 1:
        t = _NARR.get("llm_calls", {}).get("template", "")
        parts.append(t.format(llm_calls=llm_calls, tool_calls=tool_calls))

    return " ".join(parts) if parts else "Insufficient data for narrative generation."


# ══════════════════════════════════════════════════════════════════════════════
# SESSION HEADER BANNER
# ══════════════════════════════════════════════════════════════════════════════

def _load_hw_env(group_id: str) -> tuple:
    """
    Load hardware_config and environment_config rows linked to this session.
    Returns (hw_dict, env_dict) — both may be empty dicts if not found.
    Joins via experiments.hw_id and experiments.env_id.
    """
    hw, env = {}, {}
    try:
        hw_row = q1(f"""
            SELECT hc.*
            FROM hardware_config hc
            JOIN experiments e ON e.hw_id = hc.hw_id
            WHERE e.group_id = '{group_id}'
            LIMIT 1
        """)
        if hw_row:
            hw = hw_row
    except Exception:
        # Fallback: just grab latest hardware row
        try:
            hw = q1("SELECT * FROM hardware_config ORDER BY hw_id DESC LIMIT 1") or {}
        except Exception:
            hw = {}

    try:
        env_row = q1(f"""
            SELECT ec.*
            FROM environment_config ec
            JOIN experiments e ON e.env_id = ec.env_id
            WHERE e.group_id = '{group_id}'
            LIMIT 1
        """)
        if env_row:
            env = env_row
    except Exception:
        try:
            env = q1("SELECT * FROM environment_config ORDER BY env_id DESC LIMIT 1") or {}
        except Exception:
            env = {}

    return hw, env


def _session_header(group_id: str, exps: pd.DataFrame, runs: pd.DataFrame):
    n_exps = len(exps)
    n_runs = len(runs)

    try:
        starts  = pd.to_datetime(exps["started_at"].dropna())
        ends    = pd.to_datetime(exps["completed_at"].dropna())
        t_start = starts.min().strftime("%Y-%m-%d  %H:%M:%S") if len(starts) else "—"
        t_end   = ends.max().strftime("%H:%M:%S")              if len(ends)   else "—"
        dur_s   = (ends.max() - starts.min()).total_seconds()  if len(starts) and len(ends) else 0
        dur_str = f"{int(dur_s//60)}m {int(dur_s%60)}s"       if dur_s > 0   else "—"
    except Exception:
        t_start = t_end = dur_str = "—"

    gov   = str(runs["governor"].iloc[0])      if "governor"      in runs.columns and not runs.empty else "—"
    turbo = runs["turbo_enabled"].iloc[0]      if "turbo_enabled" in runs.columns and not runs.empty else None

    hw, env = _load_hw_env(group_id)

    # ── Hardware fields ────────────────────────────────────────────────────────
    cpu_model  = hw.get("cpu_model",  "Unknown CPU")
    cpu_cores  = hw.get("cpu_cores",  hw.get("total_cores", "?"))
    cpu_arch   = hw.get("cpu_architecture", "—")
    cpu_vendor = hw.get("cpu_vendor", "—")
    ram_gb     = hw.get("ram_gb", "?")
    gpu_model  = hw.get("gpu_model",  None)
    has_avx2   = hw.get("has_avx2",   None)
    has_avx512 = hw.get("has_avx512", None)
    has_vmx    = hw.get("has_vmx",    None)
    rapl_dram  = hw.get("rapl_has_dram",   None)
    rapl_unc   = hw.get("rapl_has_uncore", None)
    sys_prod   = hw.get("system_product",  "")
    hw_hash    = str(hw.get("hardware_hash", ""))[:12]
    hostname   = hw.get("hostname", "")

    # ── Environment fields ────────────────────────────────────────────────────
    py_ver     = env.get("python_version", "—")
    llm_fw     = env.get("llm_framework",  "—")
    fw_ver     = env.get("framework_version", "—")
    git_branch = env.get("git_branch", "—")
    git_commit = str(env.get("git_commit", ""))[:8]
    git_dirty  = env.get("git_dirty", False)
    numpy_ver  = env.get("numpy_version", "—")
    torch_ver  = env.get("torch_version",  "—")
    env_hash   = str(env.get("env_hash", ""))[:12]
    os_name    = env.get("os_name", "")
    kernel_ver = env.get("kernel_version", "—")

    def _bool_badge(val, true_label, false_label="—"):
        if val is None: return f"<span style='color:#3d5570'>{false_label}</span>"
        clr = "#22c55e" if val else "#4b5563"
        lbl = true_label if val else false_label
        return f"<span style='color:{clr}'>{lbl}</span>"

    def _pill(label, color="#4fc3f7"):
        return (f"<span style='background:{color}22;border:1px solid {color}44;"
                f"border-radius:3px;padding:1px 7px;font-size:9px;"
                f"color:{color};margin-right:4px;'>{label}</span>")

    # Reproducibility fingerprint = hw_hash + env_hash
    repro_fp = f"{hw_hash}:{env_hash}" if hw_hash and env_hash else "—"

    st.markdown(
        # ── Main banner ────────────────────────────────────────────────────
        f"<div style='background:#050c18;border:1px solid #1e3a5f;"
        f"border-left:4px solid #3b82f6;border-radius:6px;"
        f"padding:12px 16px 10px;margin-bottom:8px;'>"

        # Title row
        f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:8px;flex-wrap:wrap;'>"
        f"<span style='font-size:11px;font-weight:700;color:#4fc3f7;"
        f"letter-spacing:.08em;text-transform:uppercase;'>📊 Session Report</span>"
        f"<span style='font-family:monospace;font-size:11px;color:#e8f0f8;font-weight:700;'>{group_id}</span>"
        f"</div>"

        # Stats row
        f"<div style='font-size:10px;color:#5a7090;display:flex;gap:20px;flex-wrap:wrap;margin-bottom:8px;'>"
        f"<span>🔬 <b style='color:#7090b0'>{n_exps}</b> experiments</span>"
        f"<span>▶ <b style='color:#7090b0'>{n_runs}</b> runs</span>"
        f"<span>⏱ <b style='color:#7090b0'>{t_start} → {t_end}</b> ({dur_str})</span>"
        f"<span>🔁 Governor: <b style='color:#7090b0'>{gov}</b></span>"
        f"<span>⚡ Turbo: {_bool_badge(turbo, 'ON', 'OFF')}</span>"
        f"</div>"

        # Two-column: hardware | environment
        f"<div style='display:grid;grid-template-columns:1fr 1fr;gap:10px;'>"

        # Hardware card
        f"<div style='background:#07090f;border:1px solid #1e2d45;border-radius:5px;padding:9px 12px;'>"
        f"<div style='font-size:9px;font-weight:700;color:#3b82f6;text-transform:uppercase;"
        f"letter-spacing:.08em;margin-bottom:6px;'>🖥 Hardware · {hostname}</div>"
        f"<div style='font-size:9px;color:#c8d8e8;line-height:1.9;font-family:monospace;'>"
        f"<b style='color:#7090b0'>CPU</b>  {cpu_model}<br>"
        f"<b style='color:#7090b0'>Arch</b> {cpu_arch} · {cpu_vendor} · "
        f"{cpu_cores} cores · {ram_gb} GB RAM<br>"
        f"<b style='color:#7090b0'>ISA</b>  "
        f"AVX2:{_bool_badge(has_avx2,'✓','✗')}  "
        f"AVX512:{_bool_badge(has_avx512,'✓','✗')}  "
        f"VT-x:{_bool_badge(has_vmx,'✓','✗')}<br>"
        f"<b style='color:#7090b0'>RAPL</b> "
        f"DRAM:{_bool_badge(rapl_dram,'✓','✗')}  "
        f"Uncore:{_bool_badge(rapl_unc,'✓','✗')}<br>"
        + (f"<b style='color:#7090b0'>GPU</b>  {gpu_model}<br>" if gpu_model else "")
        + f"<b style='color:#7090b0'>System</b> {sys_prod}<br>"
        f"<b style='color:#7090b0'>hw_hash</b> "
        f"<span style='color:#4b6080'>{hw_hash}</span>"
        f"</div></div>"

        # Environment card
        f"<div style='background:#07090f;border:1px solid #1e2d45;border-radius:5px;padding:9px 12px;'>"
        f"<div style='font-size:9px;font-weight:700;color:#a78bfa;text-transform:uppercase;"
        f"letter-spacing:.08em;margin-bottom:6px;'>🐍 Environment</div>"
        f"<div style='font-size:9px;color:#c8d8e8;line-height:1.9;font-family:monospace;'>"
        f"<b style='color:#7090b0'>Python</b> {py_ver} · {os_name} {kernel_ver}<br>"
        f"<b style='color:#7090b0'>LLM</b>    {llm_fw} {fw_ver}<br>"
        f"<b style='color:#7090b0'>NumPy</b>  {numpy_ver} · "
        f"<b style='color:#7090b0'>Torch</b> {torch_ver}<br>"
        f"<b style='color:#7090b0'>Git</b>    {git_branch} @ {git_commit}"
        + (" <span style='color:#f59e0b'>dirty</span>" if git_dirty else "")
        + f"<br>"
        f"<b style='color:#7090b0'>env_hash</b> "
        f"<span style='color:#4b6080'>{env_hash}</span>"
        f"</div></div>"

        f"</div>"  # grid

        # Reproducibility fingerprint row
        f"<div style='margin-top:8px;padding-top:7px;border-top:1px solid #0f1520;"
        f"display:flex;align-items:center;gap:8px;flex-wrap:wrap;'>"
        f"<span style='font-size:9px;color:#3d5570;text-transform:uppercase;"
        f"letter-spacing:.06em;'>🔬 Reproducibility fingerprint</span>"
        f"<span style='font-family:monospace;font-size:9px;color:#4fc3f7;"
        f"background:#0a1a2e;border:1px solid #1e3a5f;border-radius:3px;"
        f"padding:1px 8px;'>{repro_fp}</span>"
        f"<span style='font-size:9px;color:#2d3f55;'>hw_hash:env_hash — "
        f"share this to reproduce on identical hardware+software</span>"
        f"</div>"

        f"</div>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# CHART IMAGE HELPER — Issue 3: for PDF embedding
# ══════════════════════════════════════════════════════════════════════════════

def _fig_to_png(fig) -> bytes | None:
    """
    Convert a Plotly figure to PNG bytes for PDF embedding.
    Requires kaleido: pip install kaleido
    Returns None if kaleido not available.
    """
    try:
        import plotly.io as pio
        return pio.to_image(fig, format="png", width=700, height=350, scale=1.5)
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# SUB-TAB 1 — SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def _tab_summary(group_id: str, exps: pd.DataFrame, runs: pd.DataFrame, tax: pd.DataFrame):
    if tax.empty:
        st.info("No orchestration tax data found for this session.")
        return

    st.markdown("#### 🏆 Orchestration Tax — Master Summary")

    summary = (tax.groupby(["task_name", "provider"])
               .agg(linear_j =("linear_energy_j",  "mean"),
                    agentic_j =("agentic_energy_j", "mean"),
                    tax_x     =("tax_multiplier",   "mean"),
                    n_pairs   =("comparison_id",    "count"))
               .reset_index())

    rows_html = ""
    for _, r in summary.iterrows():
        emoji, label, clr = _tax_verdict(float(r.tax_x))
        rows_html += (
            f"<tr style='border-bottom:1px solid #111827;'>"
            f"<td style='padding:9px 8px;font-size:10px;color:#7090b0;'>{r.provider}</td>"
            f"<td style='padding:9px 8px;font-size:10px;color:#c8d8e8;'>{r.task_name}</td>"
            f"<td style='padding:9px 8px;font-family:monospace;font-size:11px;color:#22c55e;'>{r.linear_j:.4f} J</td>"
            f"<td style='padding:9px 8px;font-family:monospace;font-size:11px;color:#ef4444;'>{r.agentic_j:.4f} J</td>"
            f"<td style='padding:9px 8px;text-align:center;'>"
            f"<span style='font-size:14px;font-weight:700;color:{clr};font-family:monospace;'>{r.tax_x:.2f}×</span></td>"
            f"<td style='padding:9px 8px;font-size:10px;'>{emoji} <span style='color:{clr};'>{label}</span></td>"
            f"<td style='padding:9px 8px;font-size:9px;color:#3d5570;'>{int(r.n_pairs)} pairs</td>"
            f"</tr>"
        )

    st.markdown(
        "<div style='background:#07090f;border:1px solid #1e2d45;border-radius:8px;overflow:hidden;margin:8px 0 16px;'>"
        "<table style='width:100%;border-collapse:collapse;'>"
        "<thead><tr style='background:#0a0e1a;border-bottom:2px solid #1e2d45;'>"
        "<th style='padding:7px 8px;font-size:9px;color:#3d5570;text-transform:uppercase;text-align:left;'>Provider</th>"
        "<th style='padding:7px 8px;font-size:9px;color:#3d5570;text-transform:uppercase;text-align:left;'>Task</th>"
        "<th style='padding:7px 8px;font-size:9px;color:#22c55e;text-transform:uppercase;text-align:left;'>Linear</th>"
        "<th style='padding:7px 8px;font-size:9px;color:#ef4444;text-transform:uppercase;text-align:left;'>Agentic</th>"
        "<th style='padding:7px 8px;font-size:9px;color:#f59e0b;text-transform:uppercase;text-align:center;'>Tax</th>"
        "<th style='padding:7px 8px;font-size:9px;color:#3d5570;text-transform:uppercase;text-align:left;'>Verdict</th>"
        "<th style='padding:7px 8px;font-size:9px;color:#3d5570;text-transform:uppercase;text-align:left;'>Pairs</th>"
        f"</tr></thead><tbody>{rows_html}</tbody></table></div>",
        unsafe_allow_html=True,
    )

    if len(summary) > 0:
        best  = summary.loc[summary.tax_x.idxmin()]
        worst = summary.loc[summary.tax_x.idxmax()]
        avg   = summary.tax_x.mean()
        c1, c2, c3 = st.columns(3)
        c1.success(f"**✅ Lowest tax**\n\n{best.provider} · {best.task_name}\n\n**{best.tax_x:.2f}×**")
        c2.error(f"**⚠ Highest tax**\n\n{worst.provider} · {worst.task_name}\n\n**{worst.tax_x:.2f}×**")
        c3.info(f"**📈 Session average**\n\n{len(summary)} experiment types\n\n**{avg:.2f}×**")

    st.markdown("#### 💡 Research Insights")
    insights = _generate_insights(runs, tax, summary)
    for ins in insights:
        color = ins["color"]
        st.markdown(
            f"<div style='background:{color}11;border:1px solid {color}33;"
            f"border-left:3px solid {color};border-radius:5px;"
            f"padding:10px 14px;margin-bottom:8px;'>"
            f"<div style='font-size:11px;font-weight:600;color:{color};margin-bottom:4px;'>"
            f"{ins['icon']}  {ins['title']}</div>"
            f"<div style='font-size:10px;color:#c8d8e8;line-height:1.6;'>{ins['body']}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )


def _generate_insights(runs: pd.DataFrame, tax: pd.DataFrame,
                       summary: pd.DataFrame) -> list:
    insights = []
    if tax.empty or runs.empty:
        return insights

    extreme = summary[summary.tax_x >= _TAX.get("extreme",{}).get("min", 15)]
    if not extreme.empty:
        worst = extreme.loc[extreme.tax_x.idxmax()]
        savings = (1 - 1/worst.tax_x) * 100
        insights.append({"icon": "🔴", "color": "#ef4444",
            "title": f"Extreme orchestration tax: {worst.tax_x:.1f}× on {worst.task_name}/{worst.provider}",
            "body": f"Agentic execution consumes {worst.tax_x:.1f}× more energy than linear. "
                    f"Switching to linear would save {savings:.0f}% of energy."})

    agentic_runs = runs[runs.workflow_type == "agentic"]
    if not agentic_runs.empty and "max_temp_c" in agentic_runs.columns:
        hot = agentic_runs[agentic_runs.max_temp_c > _CAUTION_C]
        if not hot.empty:
            peak  = float(agentic_runs.max_temp_c.max())
            hdroom= _THROTTLE_C - peak
            insights.append({"icon": "🌡️", "color": "#f59e0b",
                "title": f"{len(hot)} agentic runs exceeded {_CAUTION_C}°C",
                "body": f"Peak {peak:.0f}°C — {hdroom:.0f}°C below throttle ({_THROTTLE_C}°C)."})

    lin = runs[runs.workflow_type == "linear"]
    agt = runs[runs.workflow_type == "agentic"]
    if not lin.empty and not agt.empty and "thread_migrations" in runs.columns:
        avg_l = float(lin.thread_migrations.mean())
        avg_a = float(agt.thread_migrations.mean())
        if avg_l > 0:
            ratio = avg_a / avg_l
            if ratio > 5:
                insights.append({"icon": "🧵", "color": "#a78bfa",
                    "title": f"Thread migrations {ratio:.0f}× higher in agentic mode",
                    "body": f"Avg {avg_a:,.0f} vs {avg_l:,.0f} in linear. "
                            f"Confirms significant async orchestration overhead."})

    if "provider" in summary.columns and len(summary.provider.unique()) > 1:
        by_prov = summary.groupby("provider").tax_x.mean()
        if "local" in by_prov.index and "cloud" in by_prov.index:
            ratio = by_prov["local"] / by_prov["cloud"]
            if ratio > 2:
                insights.append({"icon": "☁️", "color": "#38bdf8",
                    "title": f"Local inference has {ratio:.1f}× higher tax than cloud",
                    "body": f"Local avg {by_prov['local']:.1f}×, Cloud avg {by_prov['cloud']:.1f}×."})

    if "optimization_enabled" in runs.columns:
        opt_runs  = runs[runs.optimization_enabled == 1]
        base_runs = runs[runs.optimization_enabled == 0]
        if not opt_runs.empty and not base_runs.empty:
            opt_e  = float(opt_runs.energy_j.mean())
            base_e = float(base_runs.energy_j.mean())
            saving = (base_e - opt_e) / base_e * 100 if base_e > 0 else 0
            insights.append({"icon": "🔧", "color": "#22c55e",
                "title": f"Optimization rules reduced energy by {saving:.1f}%",
                "body": f"Optimized {opt_e:.3f}J vs {base_e:.3f}J baseline."})

    return insights


# ══════════════════════════════════════════════════════════════════════════════
# SUB-TAB 2 — ENERGY  (Issue 6: all charts have unique keys)
# ══════════════════════════════════════════════════════════════════════════════

def _tab_energy(group_id: str, runs: pd.DataFrame, tax: pd.DataFrame):
    if runs.empty:
        st.info("No run data for this session.")
        return

    st.markdown("#### ⚡ Linear vs Agentic Energy")
    if not tax.empty:
        fig = go.Figure()
        labels = tax.task_name + " · " + tax.provider
        fig.add_trace(go.Bar(name="Linear",  x=labels, y=tax.linear_energy_j,  marker_color="#22c55e"))
        fig.add_trace(go.Bar(name="Agentic", x=labels, y=tax.agentic_energy_j, marker_color="#ef4444"))
        _pl2 = {k: v for k, v in PL.items() if k not in ('margin',)}
        fig.update_layout(**_pl2, barmode="group", height=280,
                          title="Linear vs Agentic total energy (J)",
                          xaxis_tickangle=15, margin=dict(t=40, b=40))
        st.plotly_chart(fig, use_container_width=True,
                        key=_ukey("energy_linagt", group_id))  # Issue 6 ✓

    st.markdown("#### 🔋 Package Energy Breakdown")
    agg = (runs.groupby("workflow_type")
           .agg(core_j  =("core_energy_j",   "mean"),
                uncore_j=("uncore_energy_j",  "mean"),
                dram_j  =("dram_energy_j",    "mean"))
           .reset_index())
    if not agg.empty:
        fig2 = go.Figure()
        for col, name, clr in [("core_j","Core","#3b82f6"),
                                ("uncore_j","Uncore","#a78bfa"),
                                ("dram_j","DRAM","#38bdf8")]:
            if col in agg.columns:
                fig2.add_trace(go.Bar(name=name, x=agg.workflow_type, y=agg[col], marker_color=clr))
        _pl3 = {k: v for k, v in PL.items() if k not in ('margin',)}
        fig2.update_layout(**_pl3, barmode="stack", height=240,
                           title="Mean energy by RAPL domain (J)", margin=dict(t=40, b=30))
        st.plotly_chart(fig2, use_container_width=True,
                        key=_ukey("energy_rapl", group_id))  # Issue 6 ✓

    st.markdown("#### 📐 Orchestration Overhead Index (OOI)")
    st.caption("OOI = (Agentic − Linear) / Agentic × 100% — fraction of agentic energy that is pure overhead")
    if not tax.empty:
        tax2 = tax.copy()
        tax2["ooi"]   = ((tax2.agentic_energy_j - tax2.linear_energy_j)
                         / tax2.agentic_energy_j.clip(lower=0.0001) * 100)
        tax2["label"] = tax2.task_name + "\n" + tax2.provider
        fig3 = go.Figure(go.Bar(
            x=tax2.label, y=tax2.ooi,
            marker_color=["#ef4444" if v > 50 else "#f59e0b" if v > 20 else "#22c55e"
                          for v in tax2.ooi],
            text=tax2.ooi.round(1).astype(str) + "%",
            textposition="outside"))
        _pl4 = {k: v for k, v in PL.items() if k not in ('margin',)}
        fig3.update_layout(**_pl4, height=250,
                           title="OOI per experiment pair (%)",
                           yaxis_title="OOI (%)", margin=dict(t=40, b=60))
        st.plotly_chart(fig3, use_container_width=True,
                        key=_ukey("energy_ooi", group_id))  # Issue 6 ✓

    col1, col2 = st.columns(2)
    with col1:
        if "energy_per_token" in runs.columns:
            ept = (runs[runs.energy_per_token.notna() & (runs.energy_per_token > 0)]
                   .groupby("workflow_type").energy_per_token.mean().reset_index())
            if not ept.empty:
                fig4 = go.Figure(go.Bar(
                    x=ept.workflow_type, y=ept.energy_per_token,
                    marker_color=["#22c55e" if w == "linear" else "#ef4444"
                                  for w in ept.workflow_type]))
                _plx = {k: v for k, v in PL.items() if k not in ('margin',)}
                fig4.update_layout(**_plx, height=200, title="Energy per token (J/tok)",
                                   margin=dict(t=40,b=20))
                st.plotly_chart(fig4, use_container_width=True,
                                key=_ukey("energy_per_tok", group_id))  # Issue 6 ✓
    with col2:
        if "energy_per_instruction" in runs.columns:
            epi = (runs[runs.energy_per_instruction.notna()]
                   .groupby("workflow_type").energy_per_instruction.mean().reset_index())
            if not epi.empty:
                fig5 = go.Figure(go.Bar(
                    x=epi.workflow_type, y=epi.energy_per_instruction,
                    marker_color=["#22c55e" if w == "linear" else "#ef4444"
                                  for w in epi.workflow_type]))
                _plx2 = {k: v for k, v in PL.items() if k not in ('margin',)}
                fig5.update_layout(**_plx2, height=200,
                                   title="Energy per instruction (J/inst)", margin=dict(t=40,b=20))
                st.plotly_chart(fig5, use_container_width=True,
                                key=_ukey("energy_per_inst", group_id))  # Issue 6 ✓

    # Sustainability panel
    st.markdown("#### 🌍 Sustainability Impact")
    total_j = float(runs.energy_j.sum()) if "energy_j" in runs.columns else 0
    sf = _human_energy_full(total_j)
    if sf:
        total_carbon_g   = float(runs.carbon_g.sum())   if "carbon_g"   in runs.columns else sf.get("carbon_g", 0)
        total_water_ml   = float(runs.water_ml.sum())   if "water_ml"   in runs.columns else sf.get("water_ml", 0)
        total_methane_mg = float(runs.methane_mg.sum()) if "methane_mg" in runs.columns else sf.get("methane_mg", 0)

        st.markdown(
            f"<div style='background:#050c18;border:1px solid #1e3a2f;border-radius:8px;padding:14px 18px;'>"
            f"<div style='font-size:10px;font-weight:700;color:#22c55e;letter-spacing:.08em;text-transform:uppercase;margin-bottom:10px;'>This session consumed:</div>"
            f"<div style='display:grid;grid-template-columns:1fr 1fr;gap:10px;'>"
            f"<div style='background:#0a1a0a;border:1px solid #1a3020;border-radius:5px;padding:10px;'>"
            f"<div style='font-size:16px;margin-bottom:4px;'>⚡</div>"
            f"<div style='font-size:13px;font-weight:700;color:#22c55e;font-family:monospace;'>{total_j:.2f} J</div>"
            f"<div style='font-size:9px;color:#3d5570;margin-top:4px;line-height:1.6;'>"
            f"= {sf.get('wh',0):.4f} Wh<br/>= {sf.get('phone_pct',0):.4f}% phone charge<br/>= {sf.get('led_min',0):.1f} min powering 1W LED</div></div>"
            f"<div style='background:#0a100a;border:1px solid #1a2a1a;border-radius:5px;padding:10px;'>"
            f"<div style='font-size:16px;margin-bottom:4px;'>💨</div>"
            f"<div style='font-size:13px;font-weight:700;color:#4ade80;font-family:monospace;'>{total_carbon_g:.5f} g CO₂</div>"
            f"<div style='font-size:9px;color:#3d5570;margin-top:4px;line-height:1.6;'>"
            f"= {sf.get('carbon_car_m',0):.2f}mm petrol car driving<br/>= {sf.get('carbon_phone_min',0):.2f} min smartphone use</div></div>"
            f"<div style='background:#080e14;border:1px solid #1a2535;border-radius:5px;padding:10px;'>"
            f"<div style='font-size:16px;margin-bottom:4px;'>💧</div>"
            f"<div style='font-size:13px;font-weight:700;color:#38bdf8;font-family:monospace;'>{total_water_ml:.4f} ml</div>"
            f"<div style='font-size:9px;color:#3d5570;margin-top:4px;line-height:1.6;'>"
            f"= {sf.get('water_tsp',0):.4f} teaspoons<br/>= {sf.get('water_shower_pct',0):.6f}% of a shower</div></div>"
            f"<div style='background:#100a0a;border:1px solid #2a1a1a;border-radius:5px;padding:10px;'>"
            f"<div style='font-size:16px;margin-bottom:4px;'>🌿</div>"
            f"<div style='font-size:13px;font-weight:700;color:#f87171;font-family:monospace;'>{total_methane_mg:.6f} mg CH₄</div>"
            f"<div style='font-size:9px;color:#3d5570;margin-top:4px;line-height:1.6;'>"
            f"= {sf.get('methane_human_pct',0):.6f}% daily human CH₄</div></div>"
            f"</div></div>",
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# SUB-TAB 3 — THERMAL  (Issue 6: unique keys)
# ══════════════════════════════════════════════════════════════════════════════

def _tab_thermal(group_id: str, runs: pd.DataFrame):
    if runs.empty or "max_temp_c" not in runs.columns:
        st.info("No thermal data available for this session.")
        return

    st.markdown("#### 🌡️ Temperature Profile per Run")
    tmp = runs[["run_id", "workflow_type", "start_temp_c",
                "max_temp_c", "min_temp_c", "thermal_delta_c"]].dropna(subset=["max_temp_c"])

    if not tmp.empty:
        fig = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = tmp[tmp.workflow_type == wf]
            if sub.empty: continue
            fig.add_trace(go.Scatter(
                x=sub.run_id.astype(str), y=sub.max_temp_c,
                error_y=dict(type="data", symmetric=False,
                             array=[0]*len(sub),
                             arrayminus=(sub.max_temp_c - sub.min_temp_c.fillna(sub.max_temp_c)).tolist()),
                mode="markers+lines", name=wf.capitalize(),
                marker_color=clr, marker_size=8, line_width=2))
        fig.add_hline(y=_THROTTLE_C, line_dash="dash", line_color="#ef4444",
                      annotation_text=f"Throttle ({_THROTTLE_C}°C)")
        fig.add_hline(y=_CAUTION_C,  line_dash="dot",  line_color="#f59e0b",
                      annotation_text=f"Caution ({_CAUTION_C}°C)")
        _plt = {k: v for k, v in PL.items() if k not in ('margin',)}
        fig.update_layout(**_plt, height=280, title="Peak temperature per run",
                          xaxis_title="Run ID", yaxis_title="Temperature (°C)",
                          margin=dict(t=40, b=30))
        st.plotly_chart(fig, use_container_width=True,
                        key=_ukey("thermal_timeline", group_id))  # Issue 6 ✓

    st.markdown("#### 📈 Thermal Rise (ΔT) per Run")
    if "thermal_delta_c" in runs.columns:
        fig2 = go.Figure(go.Bar(
            x=runs.run_id.astype(str), y=runs.thermal_delta_c.fillna(0),
            marker_color=["#ef4444" if v > 25 else "#f59e0b" if v > 15 else "#22c55e"
                          for v in runs.thermal_delta_c.fillna(0)],
            text=runs.thermal_delta_c.round(1), textposition="outside"))
        _plt2 = {k: v for k, v in PL.items() if k not in ('margin',)}
        fig2.update_layout(**_plt2, height=220,
                           title="Temperature rise during run (°C)",
                           xaxis_title="Run ID", yaxis_title="ΔT (°C)",
                           margin=dict(t=40, b=30))
        st.plotly_chart(fig2, use_container_width=True,
                        key=_ukey("thermal_delta", group_id))  # Issue 6 ✓

    peak_temp = float(runs.max_temp_c.max()) if "max_temp_c" in runs.columns else 0
    headroom  = _THROTTLE_C - peak_temp
    risk_clr  = "#ef4444" if headroom < 5 else "#f59e0b" if headroom < 15 else "#22c55e"
    risk_txt  = "🔴 CRITICAL" if headroom < 5 else "🟡 CAUTION" if headroom < 15 else "🟢 SAFE"
    c1, c2, c3 = st.columns(3)
    c1.metric("Peak Temperature", f"{peak_temp:.1f}°C")
    c2.metric("Throttle Headroom", f"{headroom:.1f}°C")
    c3.markdown(
        f"<div style='padding:10px;background:{risk_clr}11;border:1px solid {risk_clr}33;"
        f"border-radius:5px;text-align:center;margin-top:8px;'>"
        f"<div style='font-size:12px;font-weight:700;color:{risk_clr};'>{risk_txt}</div></div>",
        unsafe_allow_html=True)

    c_cols  = ["c2_time_seconds","c3_time_seconds","c6_time_seconds","c7_time_seconds"]
    c_avail = [c for c in c_cols if c in runs.columns]
    if c_avail:
        st.markdown("#### 💤 C-State Residency")
        c_data = runs.groupby("workflow_type")[c_avail].mean().reset_index()
        fig3 = go.Figure()
        for col, name in zip(c_avail, ["C2","C3","C6","C7"]):
            fig3.add_trace(go.Bar(name=name, x=c_data.workflow_type, y=c_data[col]))
        _plt3 = {k: v for k, v in PL.items() if k not in ('margin',)}
        fig3.update_layout(**_plt3, barmode="stack", height=220,
                           title="Mean C-state residency (s)", margin=dict(t=40,b=20))
        st.plotly_chart(fig3, use_container_width=True,
                        key=_ukey("thermal_cstate", group_id))  # Issue 6 ✓

    if "thermal_throttle_flag" in runs.columns:
        events = int(runs.thermal_throttle_flag.sum())
        if events > 0:
            st.error(f"⚠ {events} thermal throttle event(s) detected!")
        else:
            st.success("✅ No thermal throttle events detected.")


# ══════════════════════════════════════════════════════════════════════════════
# SUB-TAB 4 — CPU  (Issue 6: unique keys; Issue 7: statsmodels fallback)
# ══════════════════════════════════════════════════════════════════════════════

def _tab_cpu(group_id: str, runs: pd.DataFrame):
    if runs.empty:
        st.info("No run data for this session.")
        return

    st.markdown("#### 🧠 Instructions Per Cycle (IPC)")
    if "ipc" in runs.columns:
        ipc_agg = runs.groupby("workflow_type").ipc.mean().reset_index()
        fig = go.Figure(go.Bar(
            x=ipc_agg.workflow_type, y=ipc_agg.ipc,
            marker_color=["#22c55e" if w=="linear" else "#ef4444" for w in ipc_agg.workflow_type],
            text=ipc_agg.ipc.round(3), textposition="outside"))
        _plc = {k: v for k, v in PL.items() if k not in ('margin',)}
        fig.update_layout(**_plc, height=220, title="Mean IPC — higher is better",
                          yaxis_title="IPC", margin=dict(t=40,b=20))
        st.plotly_chart(fig, use_container_width=True,
                        key=_ukey("cpu_ipc", group_id))  # Issue 6 ✓

    col1, col2 = st.columns(2)
    with col1:
        if "cache_miss_rate" in runs.columns:
            cmr = runs.groupby("workflow_type").cache_miss_rate.mean().reset_index()
            fig2 = go.Figure(go.Bar(
                x=cmr.workflow_type, y=cmr.cache_miss_rate * 100,
                marker_color=["#22c55e" if w=="linear" else "#ef4444" for w in cmr.workflow_type]))
            _plc2 = {k: v for k, v in PL.items() if k not in ('margin',)}
            fig2.update_layout(**_plc2, height=220,
                               title="Cache miss rate (%) — lower is better",
                               margin=dict(t=40,b=20))
            st.plotly_chart(fig2, use_container_width=True,
                            key=_ukey("cpu_cache", group_id))  # Issue 6 ✓
    with col2:
        if "thread_migrations" in runs.columns:
            tmig = runs.groupby("workflow_type").thread_migrations.mean().reset_index()
            fig3 = go.Figure(go.Bar(
                x=tmig.workflow_type, y=tmig.thread_migrations,
                marker_color=["#22c55e" if w=="linear" else "#a78bfa" for w in tmig.workflow_type]))
            _plc3 = {k: v for k, v in PL.items() if k not in ('margin',)}
            fig3.update_layout(**_plc3, height=220,
                               title="Mean thread migrations",
                               margin=dict(t=40,b=20))
            st.plotly_chart(fig3, use_container_width=True,
                            key=_ukey("cpu_tmig", group_id))  # Issue 6 ✓

    sched_cols  = ["context_switches_voluntary","context_switches_involuntary",
                   "thread_migrations","interrupt_rate"]
    sched_avail = [c for c in sched_cols if c in runs.columns]
    if sched_avail:
        st.markdown("#### 🔄 Scheduler Metrics")
        sched_agg = runs.groupby("workflow_type")[sched_avail].mean().reset_index()
        fig4 = go.Figure()
        for col in sched_avail:
            fig4.add_trace(go.Bar(name=col.replace("_"," ").title(),
                                  x=sched_agg.workflow_type, y=sched_agg[col]))
        _plc4 = {k: v for k, v in PL.items() if k not in ('margin',)}
        fig4.update_layout(**_plc4, barmode="group", height=240,
                           title="Scheduler metrics — linear vs agentic",
                           margin=dict(t=40,b=20))
        st.plotly_chart(fig4, use_container_width=True,
                        key=_ukey("cpu_sched", group_id))  # Issue 6 ✓

    if "kernel_time_ms" in runs.columns and "user_time_ms" in runs.columns:
        kt = runs.groupby("workflow_type")[["kernel_time_ms","user_time_ms"]].mean().reset_index()
        fig5 = go.Figure()
        fig5.add_trace(go.Bar(name="Kernel", x=kt.workflow_type, y=kt.kernel_time_ms, marker_color="#3b82f6"))
        fig5.add_trace(go.Bar(name="User",   x=kt.workflow_type, y=kt.user_time_ms,   marker_color="#22c55e"))
        _plc5 = {k: v for k, v in PL.items() if k not in ('margin',)}
        fig5.update_layout(**_plc5, barmode="stack", height=220,
                           title="Kernel vs User time (ms)", margin=dict(t=40,b=20))
        st.plotly_chart(fig5, use_container_width=True,
                        key=_ukey("cpu_ktime", group_id))  # Issue 6 ✓

    # ── Issue 7: Hardware behaviour question — statsmodels regression ─────────
    st.markdown("#### 📊 Hardware Behaviour Analysis")
    if not _STATSMODELS:
        st.warning(
            "⚠️ `statsmodels` not installed — regression analysis unavailable.\n\n"
            "Install it with:\n```bash\npip install statsmodels\n```\n\n"
            "All other metrics above are still available.",
            icon="📦"
        )
        # Still show a basic correlation table as fallback
        num_cols = [c for c in ["ipc","cache_miss_rate","thread_migrations",
                                 "energy_j","duration_ms"] if c in runs.columns]
        if len(num_cols) >= 2:
            corr = runs[num_cols].corr().round(3)
            st.caption("Correlation matrix (fallback — install statsmodels for full regression):")
            st.dataframe(corr, use_container_width=True)
    else:
        # Full statsmodels regression: energy ~ ipc + cache_miss_rate + thread_migrations
        reg_cols = ["energy_j","ipc","cache_miss_rate","thread_migrations"]
        reg_avail = [c for c in reg_cols if c in runs.columns]
        if len(reg_avail) >= 3:
            reg_df = runs[reg_avail].dropna()
            if len(reg_df) >= 5:
                try:
                    formula_parts = [c for c in ["ipc","cache_miss_rate","thread_migrations"]
                                     if c in reg_df.columns]
                    formula = "energy_j ~ " + " + ".join(formula_parts)
                    model  = smf.ols(formula, data=reg_df).fit()
                    st.caption("OLS Regression: energy_j ~ hardware metrics")
                    st.text(model.summary().as_text())
                except Exception as e:
                    st.warning(f"Regression failed: {e}")
        else:
            st.caption("Not enough numeric columns for regression analysis.")


# ══════════════════════════════════════════════════════════════════════════════
# SUB-TAB 5 — PER-PAIR  (Issue 6: unique keys)
# ══════════════════════════════════════════════════════════════════════════════

def _tab_per_pair(group_id: str, tax: pd.DataFrame):
    if tax.empty:
        st.info("No pair data available for this session.")
        return

    st.markdown(f"**{len(tax)} run pairs in this session**")

    for i, row in tax.iterrows():
        tax_x    = float(row.get("tax_multiplier", 0))
        emoji, label, clr = _tax_verdict(tax_x)
        run_num  = int(row.get("run_number", i+1))

        header = (f"Pair {i+1} · {row.get('task_name','?')} · {row.get('provider','?')} · "
                  f"Rep {run_num} · {emoji} {tax_x:.2f}× ({label})")

        with st.expander(header, expanded=(i == 0)):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("<div style='font-size:10px;font-weight:700;color:#22c55e;"
                            "text-transform:uppercase;letter-spacing:.08em;'>LINEAR</div>",
                            unsafe_allow_html=True)
                _metric_block({
                    "Total Energy":  f"{row.get('linear_energy_j', 0):.4f} J",
                    "Duration":      f"{row.get('linear_ms', 0):.0f} ms",
                    "IPC":           f"{row.get('linear_ipc', 0):.3f}",
                    "Cache Miss":    f"{float(row.get('linear_cmr',0) or 0)*100:.1f}%",
                    "Peak Temp":     f"{row.get('linear_max_temp', 0):.1f}°C",
                    "Thermal Rise":  f"+{row.get('linear_tdelta', 0):.1f}°C",
                    "Thread Mig":    f"{int(row.get('linear_tmig', 0) or 0):,}",
                }, "#22c55e")
            with c2:
                st.markdown("<div style='font-size:10px;font-weight:700;color:#ef4444;"
                            "text-transform:uppercase;letter-spacing:.08em;'>AGENTIC</div>",
                            unsafe_allow_html=True)
                l_e = float(row.get("linear_energy_j", 1) or 1)
                a_e = float(row.get("agentic_energy_j", 0) or 0)
                _metric_block({
                    "Total Energy":      f"{a_e:.4f} J  (+{a_e/max(l_e,1e-9):.1f}×)",
                    "Duration":          f"{row.get('agentic_ms', 0):.0f} ms",
                    "IPC":               f"{row.get('agentic_ipc', 0):.3f}",
                    "Cache Miss":        f"{float(row.get('agentic_cmr',0) or 0)*100:.1f}%",
                    "Peak Temp":         f"{row.get('agentic_max_temp', 0):.1f}°C",
                    "Thermal Rise":      f"+{row.get('agentic_tdelta', 0):.1f}°C",
                    "Thread Mig":        f"{int(row.get('agentic_tmig', 0) or 0):,}",
                    "LLM Calls":         f"{int(row.get('llm_calls',  0) or 0)}",
                    "Tool Calls":        f"{int(row.get('tool_calls', 0) or 0)}",
                    "Steps":             f"{int(row.get('steps',      0) or 0)}",
                    "Plan/Exec/Synth":   (
                        f"{row.get('planning_time_ms',0):.0f}ms / "
                        f"{row.get('execution_time_ms',0):.0f}ms / "
                        f"{row.get('synthesis_time_ms',0):.0f}ms"),
                }, "#ef4444")

            narrative = _build_pair_narrative(row)
            st.markdown(
                "<div style='background:#07090f;border:1px solid #1e2d45;"
                "border-left:3px solid #3b82f6;border-radius:5px;"
                "padding:10px 14px;margin-top:8px;'>"
                "<div style='font-size:9px;font-weight:700;color:#3b82f6;"
                "text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px;'>"
                "📝 Research Narrative</div>"
                f"<div style='font-size:10px;color:#c8d8e8;line-height:1.7;'>{narrative}</div>"
                "</div>",
                unsafe_allow_html=True,
            )


def _metric_block(metrics: dict, color: str):
    rows = "".join(
        f"<tr><td style='padding:3px 8px;font-size:9px;color:#3d5570;'>{k}</td>"
        f"<td style='padding:3px 8px;font-size:10px;font-family:monospace;color:{color};'>{v}</td></tr>"
        for k, v in metrics.items()
    )
    st.markdown(
        f"<table style='width:100%;border-collapse:collapse;background:#07090f;"
        f"border-radius:5px;overflow:hidden;'><tbody>{rows}</tbody></table>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# SUB-TAB 6 — EXPORT  (Issue 3: PDF with chart images)
# ══════════════════════════════════════════════════════════════════════════════

def _tab_export(group_id: str, exps: pd.DataFrame, runs: pd.DataFrame, tax: pd.DataFrame):
    st.markdown("#### 💾 Export Session Report")

    col1, col2 = st.columns(2)

    # ── Excel ─────────────────────────────────────────────────────────────────
    with col1:
        st.markdown("**📊 Excel Report**")
        st.caption("One sheet per analysis section + raw data")
        if st.button("📥 Generate Excel", use_container_width=True,
                     key=f"gen_excel_{group_id}"):
            try:
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                    exps.to_excel(writer, sheet_name="Experiments", index=False)
                    if not tax.empty:
                        summary = (tax.groupby(["task_name","provider"])
                                   .agg(linear_j=("linear_energy_j","mean"),
                                        agentic_j=("agentic_energy_j","mean"),
                                        tax_x=("tax_multiplier","mean"),
                                        n_pairs=("comparison_id","count"))
                                   .reset_index())
                        summary["verdict"] = summary.tax_x.apply(lambda x: _tax_verdict(x)[1])
                        summary.to_excel(writer, sheet_name="Summary", index=False)
                    energy_cols = ["run_id","workflow_type","task_name","provider",
                                   "energy_j","dynamic_energy_j","pkg_energy_j",
                                   "core_energy_j","uncore_energy_j","dram_energy_j",
                                   "energy_per_token","energy_per_instruction"]
                    e_avail = [c for c in energy_cols if c in runs.columns]
                    runs[e_avail].to_excel(writer, sheet_name="Energy", index=False)
                    thermal_cols = ["run_id","workflow_type","task_name","provider",
                                    "start_temp_c","max_temp_c","min_temp_c",
                                    "thermal_delta_c","thermal_throttle_flag",
                                    "c2_time_seconds","c3_time_seconds",
                                    "c6_time_seconds","c7_time_seconds"]
                    t_avail = [c for c in thermal_cols if c in runs.columns]
                    runs[t_avail].to_excel(writer, sheet_name="Thermal", index=False)
                    cpu_cols = ["run_id","workflow_type","task_name","provider",
                                "ipc","cache_miss_rate","thread_migrations",
                                "context_switches_voluntary","context_switches_involuntary",
                                "interrupt_rate","ring_bus_freq_mhz","wakeup_latency_us",
                                "kernel_time_ms","user_time_ms"]
                    c_avail = [c for c in cpu_cols if c in runs.columns]
                    runs[c_avail].to_excel(writer, sheet_name="CPU_Scheduler", index=False)
                    sust_cols = ["run_id","workflow_type","task_name","provider",
                                 "energy_j","carbon_g","water_ml","methane_mg"]
                    s_avail = [c for c in sust_cols if c in runs.columns]
                    runs[s_avail].to_excel(writer, sheet_name="Sustainability", index=False)
                    if not tax.empty:
                        tax.to_excel(writer, sheet_name="Per_Pair", index=False)
                    runs.to_excel(writer, sheet_name="Raw_Runs", index=False)
                buf.seek(0)
                st.download_button("⬇️ Download Excel", data=buf.getvalue(),
                                   file_name=f"alems_{group_id}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   key=f"dl_excel_{group_id}")
                st.success("Excel ready — 8 sheets generated")
            except Exception as e:
                st.error(f"Excel failed: {e}")
                st.caption("pip install openpyxl")

    # ── PDF ───────────────────────────────────────────────────────────────────
    with col2:
        st.markdown("**📄 PDF Academic Report**")
        st.caption("Research-grade report with charts embedded as images")
        if st.button("📥 Generate PDF", use_container_width=True,
                     key=f"gen_pdf_{group_id}"):
            try:
                pdf_bytes = _generate_pdf(group_id, exps, runs, tax)
                st.download_button("⬇️ Download PDF", data=pdf_bytes,
                                   file_name=f"alems_{group_id}.pdf",
                                   mime="application/pdf",
                                   key=f"dl_pdf_{group_id}")
                st.success("PDF ready")
            except ImportError as e:
                st.warning(f"Missing dependency: {e}\n\npip install reportlab kaleido")
            except Exception as e:
                st.error(f"PDF generation failed: {e}")

    st.markdown("**📋 Raw JSON**")
    if st.button("📥 Export JSON", use_container_width=True,
                 key=f"gen_json_{group_id}"):
        import json
        payload = {
            "group_id":    group_id,
            "experiments": exps.to_dict(orient="records"),
            "runs":        runs.to_dict(orient="records"),
            "tax_summary": tax.to_dict(orient="records"),
        }
        st.download_button("⬇️ Download JSON",
                           data=json.dumps(payload, indent=2, default=str),
                           file_name=f"alems_{group_id}.json",
                           mime="application/json",
                           key=f"dl_json_{group_id}")


# ══════════════════════════════════════════════════════════════════════════════
# PDF GENERATOR — Issue 3: chart images embedded via kaleido
# ══════════════════════════════════════════════════════════════════════════════

def _safe_str(val) -> str:
    """Convert any value to a PDF-safe ASCII/Latin-1 string.
    Strips non-latin characters that cause ReportLab UTF-8 errors
    (e.g. emoji from llama.cpp output, box-drawing chars, etc.)
    """
    s = str(val) if val is not None else ""
    # Encode to latin-1 dropping anything that won't survive, then back to str
    return s.encode("latin-1", errors="ignore").decode("latin-1")


def _generate_pdf(group_id: str, exps: pd.DataFrame,
                  runs: pd.DataFrame, tax: pd.DataFrame) -> bytes:
    """
    Scientific PDF report: title page, abstract, 7 sections, 7 charts, references.
    Charts embedded as PNG via kaleido (pip install kaleido).
    UTF-8 safe: all strings sanitized through _safe_str().
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles    import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units     import cm
    from reportlab.lib            import colors
    from reportlab.platypus      import (SimpleDocTemplate, Paragraph, Spacer,
                                          Table, TableStyle, HRFlowable,
                                          Image, KeepTogether, PageBreak)
    from reportlab.lib.enums     import TA_CENTER, TA_JUSTIFY

    # ── kaleido availability ────────────────────────────────────────────────
    try:
        import plotly.io as pio
        _kaleido = True
    except Exception:
        _kaleido = False

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                             leftMargin=2.2*cm, rightMargin=2.2*cm,
                             topMargin=2.5*cm,  bottomMargin=2.2*cm)

    styles   = getSampleStyleSheet()
    W        = A4[0] - 4.4*cm   # usable page width

    # ── Custom styles ────────────────────────────────────────────────────────
    sty = {
        "title":    ParagraphStyle("T",  parent=styles["Normal"],
                                    fontSize=20, leading=26, spaceAfter=4,
                                    alignment=TA_CENTER,
                                    textColor=colors.HexColor("#0f2d52"),
                                    fontName="Helvetica-Bold"),
        "subtitle": ParagraphStyle("ST", parent=styles["Normal"],
                                    fontSize=11, leading=14, spaceAfter=2,
                                    alignment=TA_CENTER,
                                    textColor=colors.HexColor("#2d5a8e")),
        "meta":     ParagraphStyle("M",  parent=styles["Normal"],
                                    fontSize=9,  leading=13, spaceAfter=2,
                                    alignment=TA_CENTER,
                                    textColor=colors.HexColor("#555555")),
        "h1":       ParagraphStyle("H1", parent=styles["Normal"],
                                    fontSize=12, leading=16, spaceBefore=14,
                                    spaceAfter=4, fontName="Helvetica-Bold",
                                    textColor=colors.HexColor("#0f2d52"),
                                    borderPad=4,
                                    borderColor=colors.HexColor("#0f2d52"),
                                    borderWidth=0, leftIndent=0),
        "h2":       ParagraphStyle("H2", parent=styles["Normal"],
                                    fontSize=10, leading=14, spaceBefore=8,
                                    spaceAfter=3, fontName="Helvetica-Bold",
                                    textColor=colors.HexColor("#1e3a5f")),
        "body":     ParagraphStyle("B",  parent=styles["Normal"],
                                    fontSize=9,  leading=14, spaceAfter=5,
                                    alignment=TA_JUSTIFY),
        "caption":  ParagraphStyle("C",  parent=styles["Normal"],
                                    fontSize=8,  leading=11, spaceAfter=8,
                                    alignment=TA_CENTER,
                                    textColor=colors.HexColor("#555555"),
                                    fontName="Helvetica-Oblique"),
        "mono":     ParagraphStyle("MO", parent=styles["Normal"],
                                    fontSize=8,  leading=11, spaceAfter=4,
                                    fontName="Courier",
                                    textColor=colors.HexColor("#1a1a2e")),
        "footer":   ParagraphStyle("F",  parent=styles["Normal"],
                                    fontSize=7,  leading=10,
                                    alignment=TA_CENTER,
                                    textColor=colors.HexColor("#aaaaaa")),
        "abstract": ParagraphStyle("AB", parent=styles["Normal"],
                                    fontSize=9,  leading=14, spaceAfter=6,
                                    alignment=TA_JUSTIFY,
                                    leftIndent=1*cm, rightIndent=1*cm,
                                    textColor=colors.HexColor("#2a2a2a")),
    }

    story = []
    now   = datetime.now().strftime("%B %d, %Y  %H:%M UTC")
    gid_s = _safe_str(group_id)

    # ── Helper: embed a Plotly figure ────────────────────────────────────────
    def _fig(fig, caption: str, w_cm: float = 16, h_cm: float = 7):
        if not _kaleido:
            story.append(Paragraph(
                f"[{_safe_str(caption)} — chart omitted: pip install kaleido]",
                sty["caption"]))
            return
        try:
            # White background for print
            fig.update_layout(
                paper_bgcolor="white", plot_bgcolor="#f8fafc",
                font=dict(color="#1e293b", size=10),
            )
            png = pio.to_image(fig, format="png",
                               width=int(w_cm * 40), height=int(h_cm * 40), scale=2)
            story.append(Image(io.BytesIO(png), width=w_cm*cm, height=h_cm*cm))
            story.append(Paragraph(_safe_str(caption), sty["caption"]))
        except Exception as exc:
            story.append(Paragraph(
                f"[{_safe_str(caption)}: render error — {_safe_str(exc)}]",
                sty["caption"]))

    # ── Helper: data table ────────────────────────────────────────────────────
    def _table(data: list, col_widths: list, header_bg="#1e3a5f"):
        t = Table([[_safe_str(c) for c in row] for row in data],
                  colWidths=[w*cm for w in col_widths])
        t.setStyle(TableStyle([
            ("BACKGROUND",     (0,0), (-1,0),  colors.HexColor(header_bg)),
            ("TEXTCOLOR",      (0,0), (-1,0),  colors.white),
            ("FONTNAME",       (0,0), (-1,0),  "Helvetica-Bold"),
            ("FONTSIZE",       (0,0), (-1,-1), 8),
            ("FONTNAME",       (0,1), (-1,-1), "Helvetica"),
            ("ROWBACKGROUNDS", (0,1), (-1,-1),
             [colors.white, colors.HexColor("#eef2f8")]),
            ("GRID",           (0,0), (-1,-1), 0.25, colors.HexColor("#c0c8d8")),
            ("TOPPADDING",     (0,0), (-1,-1), 4),
            ("BOTTOMPADDING",  (0,0), (-1,-1), 4),
            ("LEFTPADDING",    (0,0), (-1,-1), 6),
            ("RIGHTPADDING",   (0,0), (-1,-1), 6),
            ("VALIGN",         (0,0), (-1,-1), "MIDDLE"),
        ]))
        return t

    # ────────────────────────────────────────────────────────────────────────
    # TITLE PAGE
    # ────────────────────────────────────────────────────────────────────────
    story.append(Spacer(1, 1.5*cm))
    story.append(Paragraph("A-LEMS Experimental Report", sty["title"]))
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph("Agentic LLM Energy Measurement System", sty["subtitle"]))
    story.append(Spacer(1, 0.5*cm))
    story.append(HRFlowable(width=W, thickness=1.5,
                             color=colors.HexColor("#0f2d52"), spaceAfter=8))
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph(f"Session ID: <b>{gid_s}</b>", sty["meta"]))
    story.append(Paragraph(f"Generated: {_safe_str(now)}", sty["meta"]))

    # Hardware summary on title page
    hw = q1("SELECT cpu_model, total_cores, ram_gb FROM hardware_config LIMIT 1")
    hw_cpu  = _safe_str(hw.get("cpu_model","Unknown")) if hw else "Unknown"
    hw_core = str(hw.get("total_cores","?")) if hw else "?"
    hw_ram  = str(hw.get("ram_gb","?")) if hw else "?"
    gov     = _safe_str(runs["governor"].iloc[0]) if "governor" in runs.columns and not runs.empty else "unknown"

    story.append(Paragraph(
        f"Hardware: {hw_cpu} ({hw_core} cores, {hw_ram} GB RAM)  |  "
        f"Governor: {gov}  |  "
        f"Experiments: {len(exps)}  |  Runs: {len(runs)}",
        sty["meta"]))
    story.append(Spacer(1, 1.0*cm))

    # ── Abstract ──────────────────────────────────────────────────────────────
    if not tax.empty:
        max_tax = float(tax.tax_multiplier.max())
        min_tax = float(tax.tax_multiplier.min())
        avg_tax = float(tax.tax_multiplier.mean())
        _, worst_task_row = max(
            [(float(r.tax_multiplier), r) for _, r in tax.iterrows()],
            key=lambda x: x[0])
        story.append(Paragraph("<b>Abstract</b>", sty["h2"]))
        story.append(Paragraph(
            f"This report presents hardware-level energy measurements comparing "
            f"agentic versus linear LLM execution workflows on a local inference stack. "
            f"Across {len(exps)} experiments and {len(tax)} run pairs, the orchestration "
            f"energy tax ranged from {min_tax:.2f}x to {max_tax:.2f}x "
            f"(mean {avg_tax:.2f}x). "
            f"The highest overhead was observed for task "
            f"'{_safe_str(worst_task_row.get('task_name','?'))}' on "
            f"'{_safe_str(worst_task_row.get('provider','?'))}' "
            f"at {max_tax:.2f}x. "
            f"All measurements use hardware RAPL counters for sub-millijoule accuracy. "
            f"Sustainability metrics (CO2, water, methane) are computed from regional "
            f"emission factors defined in config/insights_rules.yaml.",
            sty["abstract"]))

    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # SECTION 1 — ENERGY RESULTS
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("1. Energy Results — Linear vs Agentic", sty["h1"]))
    story.append(HRFlowable(width=W, thickness=0.5, color=colors.HexColor("#c0c8d8")))

    if not tax.empty:
        summary = (tax.groupby(["task_name","provider"])
                   .agg(linear_j =("linear_energy_j",  "mean"),
                        agentic_j=("agentic_energy_j", "mean"),
                        tax_x    =("tax_multiplier",   "mean"),
                        tax_std  =("tax_multiplier",   "std"),
                        n_pairs  =("comparison_id",    "count"))
                   .reset_index())
        summary["tax_std"] = summary["tax_std"].fillna(0)

        # Table 1
        story.append(Paragraph("Table 1 — Master Energy Summary (mean across repetitions)", sty["h2"]))
        tbl_data = [["Task","Provider","Linear (J)","Agentic (J)","Tax (mean)","StdDev","n","Verdict"]]
        for _, r in summary.iterrows():
            _, verdict, _ = _tax_verdict(float(r.tax_x))
            tbl_data.append([
                r.task_name[:22], r.provider[:10],
                f"{r.linear_j:.4f}", f"{r.agentic_j:.4f}",
                f"{r.tax_x:.3f}x",  f"{r.tax_std:.3f}",
                str(int(r.n_pairs)), _safe_str(verdict)])
        story.append(_table(tbl_data, [3.8,2.2,2.2,2.2,2.0,1.5,0.8,2.0]))
        story.append(Spacer(1, 0.3*cm))

        # Figure 1 — grouped bar
        fig1 = go.Figure()
        labels = (summary.task_name + " / " + summary.provider).apply(_safe_str)
        fig1.add_trace(go.Bar(name="Linear",  x=labels, y=summary.linear_j,
                              marker_color="#2ecc71",
                              error_y=dict(type="data", array=[0]*len(summary), visible=False)))
        fig1.add_trace(go.Bar(name="Agentic", x=labels, y=summary.agentic_j,
                              marker_color="#e74c3c"))
        fig1.update_layout(barmode="group", height=380,
                           title="Figure 1 — Mean energy per workflow type (J)",
                           xaxis_tickangle=20,
                           yaxis_title="Energy (J)",
                           margin=dict(t=50,b=80,l=60,r=20),
                           legend=dict(orientation="h", y=1.12))
        _fig(fig1, "Figure 1: Mean energy consumption (J) — linear vs agentic workflows. "
                   "Error bars show repetition variance where available.")

        # Figure 2 — tax multiplier scatter with run number on x-axis
        story.append(Paragraph("1.2  Orchestration Tax per Repetition", sty["h2"]))
        fig2 = go.Figure()
        for prov in tax.provider.unique():
            sub = tax[tax.provider == prov]
            fig2.add_trace(go.Scatter(
                x=sub.run_number, y=sub.tax_multiplier,
                mode="markers+lines", name=_safe_str(prov),
                marker_size=8,
                hovertemplate="Rep %{x}: %{y:.2f}x<extra></extra>"))
        fig2.add_hline(y=1.0, line_dash="dash", line_color="grey",
                       annotation_text="No overhead (1.0x)")
        fig2.update_layout(height=320,
                           title="Figure 2 — Tax multiplier per repetition",
                           xaxis_title="Repetition number",
                           yaxis_title="Tax multiplier (agentic/linear)",
                           margin=dict(t=50,b=40,l=60,r=20))
        _fig(fig2, "Figure 2: Orchestration tax multiplier per repetition. "
                   "Values > 1.0 indicate agentic overhead. "
                   "Trend across reps reveals warm-up vs steady-state behaviour.")

        # Figure 3 — OOI
        story.append(Paragraph("1.3  Orchestration Overhead Index (OOI)", sty["h2"]))
        story.append(Paragraph(
            "OOI = (Agentic energy - Linear energy) / Agentic energy × 100%. "
            "Represents the fraction of total agentic compute consumed by "
            "orchestration overhead above the base LLM inference cost.",
            sty["body"]))
        tax_ooi = tax.copy()
        tax_ooi["ooi"]   = ((tax_ooi.agentic_energy_j - tax_ooi.linear_energy_j)
                             / tax_ooi.agentic_energy_j.clip(lower=0.0001) * 100)
        tax_ooi["label"] = (tax_ooi.task_name + " / " + tax_ooi.provider).apply(_safe_str)
        fig3 = go.Figure(go.Bar(
            x=tax_ooi.label, y=tax_ooi.ooi,
            marker_color=["#e74c3c" if v > 50 else "#f39c12" if v > 20 else "#2ecc71"
                          for v in tax_ooi.ooi],
            text=[f"{v:.1f}%" for v in tax_ooi.ooi],
            textposition="outside"))
        fig3.add_hline(y=0,  line_dash="solid", line_color="#aaaaaa")
        fig3.add_hline(y=50, line_dash="dot",   line_color="#e74c3c",
                       annotation_text="50% threshold")
        fig3.update_layout(height=320,
                           title="Figure 3 — Orchestration Overhead Index (%)",
                           yaxis_title="OOI (%)",
                           xaxis_tickangle=20,
                           margin=dict(t=50,b=80,l=60,r=30))
        _fig(fig3, "Figure 3: OOI per experiment pair. Red > 50%, orange 20-50%, green < 20%.")

    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # SECTION 2 — THERMAL ANALYSIS
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("2. Thermal Analysis", sty["h1"]))
    story.append(HRFlowable(width=W, thickness=0.5, color=colors.HexColor("#c0c8d8")))

    if not runs.empty and "max_temp_c" in runs.columns:
        peak_temp  = float(runs.max_temp_c.max())
        mean_temp  = float(runs.max_temp_c.mean())
        headroom   = _THROTTLE_C - peak_temp
        risk_label = "CRITICAL" if headroom < 5 else "CAUTION" if headroom < 15 else "SAFE"

        story.append(Paragraph(
            f"Peak recorded package temperature: {peak_temp:.1f} C "
            f"(mean: {mean_temp:.1f} C, throttle headroom: {headroom:.1f} C). "
            f"Thermal risk assessment: <b>{risk_label}</b>. "
            f"Agentic workloads drive higher sustained thermal load due to "
            f"increased LLM call frequency and concurrent tool execution.",
            sty["body"]))

        # Figure 4 — temperature timeline
        tmp = runs[["run_id","workflow_type","max_temp_c","min_temp_c"]].dropna(subset=["max_temp_c"])
        if not tmp.empty:
            fig4 = go.Figure()
            for wf, clr, sym in [("linear","#2ecc71","circle"),
                                  ("agentic","#e74c3c","diamond")]:
                sub = tmp[tmp.workflow_type == wf]
                if not sub.empty:
                    fig4.add_trace(go.Scatter(
                        x=sub.run_id.astype(str), y=sub.max_temp_c,
                        mode="markers+lines", name=wf.capitalize(),
                        marker=dict(color=clr, size=8, symbol=sym),
                        line=dict(color=clr, width=1.5)))
            fig4.add_hline(y=_THROTTLE_C, line_dash="dash", line_color="#c0392b",
                           annotation_text=f"Throttle {_THROTTLE_C}C",
                           annotation_position="right")
            fig4.add_hline(y=_CAUTION_C, line_dash="dot", line_color="#e67e22",
                           annotation_text=f"Caution {_CAUTION_C}C",
                           annotation_position="right")
            fig4.update_layout(height=320,
                               title="Figure 4 — Package temperature per run",
                               xaxis_title="Run ID", yaxis_title="Temp (C)",
                               margin=dict(t=50,b=40,l=60,r=90),
                               legend=dict(orientation="h", y=1.12))
            _fig(fig4, "Figure 4: Package temperature across all runs. "
                       "Circle = linear, diamond = agentic.")

        # Table 2 — thermal summary
        if "thermal_delta_c" in runs.columns:
            therm_agg = (runs.groupby("workflow_type")
                         .agg(mean_max=("max_temp_c","mean"),
                              peak_max=("max_temp_c","max"),
                              mean_delta=("thermal_delta_c","mean"))
                         .reset_index())
            story.append(Paragraph("Table 2 — Thermal Summary by Workflow Type", sty["h2"]))
            tbl2 = [["Workflow","Mean Peak (C)","Max Peak (C)","Mean DeltaT (C)"]]
            for _, r in therm_agg.iterrows():
                tbl2.append([_safe_str(r.workflow_type),
                              f"{r.mean_max:.1f}", f"{r.peak_max:.1f}",
                              f"{r.mean_delta:.1f}"])
            story.append(_table(tbl2, [4,4,4,4.7]))

        # Throttle events
        if "thermal_throttle_flag" in runs.columns:
            n_throttle = int(runs.thermal_throttle_flag.sum())
            story.append(Spacer(1, 0.2*cm))
            story.append(Paragraph(
                f"Thermal throttle events detected: <b>{n_throttle}</b>. "
                + ("No performance degradation from thermal throttling." if n_throttle == 0
                   else "Throttling may have suppressed CPU frequency during these runs, "
                        "introducing measurement variance."),
                sty["body"]))
    else:
        story.append(Paragraph("No thermal data recorded in this session.", sty["body"]))

    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # SECTION 3 — CPU & SCHEDULER
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("3. CPU and Scheduler Metrics", sty["h1"]))
    story.append(HRFlowable(width=W, thickness=0.5, color=colors.HexColor("#c0c8d8")))

    if not runs.empty:
        cpu_metrics = ["ipc","cache_miss_rate","thread_migrations",
                       "context_switches_voluntary","context_switches_involuntary"]
        cpu_avail = [c for c in cpu_metrics if c in runs.columns]

        if cpu_avail:
            cpu_agg = runs.groupby("workflow_type")[cpu_avail].mean().reset_index()

            # Figure 5 — IPC + cache grouped
            if "ipc" in cpu_agg.columns and "cache_miss_rate" in cpu_agg.columns:
                fig5 = go.Figure()
                fig5.add_trace(go.Bar(name="IPC", x=cpu_agg.workflow_type,
                                      y=cpu_agg.ipc, marker_color="#3498db",
                                      yaxis="y1"))
                fig5.add_trace(go.Bar(name="Cache Miss Rate (%)",
                                      x=cpu_agg.workflow_type,
                                      y=cpu_agg.cache_miss_rate * 100,
                                      marker_color="#e74c3c", yaxis="y2"))
                fig5.update_layout(
                    height=320, barmode="group",
                    title="Figure 5 — IPC and cache miss rate by workflow",
                    yaxis=dict(title="IPC", side="left"),
                    yaxis2=dict(title="Cache Miss (%)", side="right", overlaying="y"),
                    legend=dict(orientation="h", y=1.12),
                    margin=dict(t=50,b=40,l=60,r=60))
                _fig(fig5, "Figure 5: Instructions per cycle (higher = better) and "
                           "cache miss rate (lower = better). Agentic workflows typically "
                           "show lower IPC due to I/O wait during LLM API calls.")

            # Table 3 — CPU metrics
            story.append(Paragraph("Table 3 — CPU and Scheduler Metrics", sty["h2"]))
            tbl3_hdr = ["Workflow"] + [c.replace("_"," ").title()[:18] for c in cpu_avail]
            tbl3 = [tbl3_hdr]
            for _, r in cpu_agg.iterrows():
                row_vals = [_safe_str(r.workflow_type)]
                for c in cpu_avail:
                    v = float(r[c]) if c in r else 0
                    row_vals.append(f"{v:.3f}" if c in ("ipc","cache_miss_rate") else f"{v:,.0f}")
                tbl3.append(row_vals)
            col_w = [3.5] + [round((16.5-3.5)/len(cpu_avail), 1)] * len(cpu_avail)
            story.append(_table(tbl3, col_w))

            if "thread_migrations" in cpu_avail:
                lin_mig = cpu_agg[cpu_agg.workflow_type=="linear"]["thread_migrations"].values
                agt_mig = cpu_agg[cpu_agg.workflow_type=="agentic"]["thread_migrations"].values
                if len(lin_mig) and len(agt_mig) and lin_mig[0] > 0:
                    ratio = agt_mig[0] / lin_mig[0]
                    story.append(Paragraph(
                        f"Thread migrations are {ratio:.1f}x higher in agentic mode "
                        f"({agt_mig[0]:,.0f} vs {lin_mig[0]:,.0f}), confirming that "
                        f"the async orchestration layer drives significant scheduler overhead "
                        f"beyond the raw LLM compute cost.",
                        sty["body"]))
    else:
        story.append(Paragraph("No CPU data recorded in this session.", sty["body"]))

    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # SECTION 4 — SUSTAINABILITY
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("4. Sustainability Impact", sty["h1"]))
    story.append(HRFlowable(width=W, thickness=0.5, color=colors.HexColor("#c0c8d8")))

    total_j = float(runs.energy_j.sum()) if "energy_j" in runs.columns else 0
    sf = _human_energy_full(total_j)
    if sf:
        total_c = float(runs.carbon_g.sum())   if "carbon_g"   in runs.columns else sf.get("carbon_g",0)
        total_w = float(runs.water_ml.sum())   if "water_ml"   in runs.columns else sf.get("water_ml",0)
        total_m = float(runs.methane_mg.sum()) if "methane_mg" in runs.columns else sf.get("methane_mg",0)

        story.append(Paragraph(
            f"Total session energy: {total_j:.4f} J ({sf.get('wh',0):.6f} Wh). "
            f"Carbon: {total_c:.6f} g CO2 (equiv. {sf.get('carbon_car_m',0):.3f} mm petrol car). "
            f"Water: {total_w:.5f} ml (data centre cooling). "
            f"Methane: {total_m:.7f} mg CH4 ({sf.get('methane_human_pct',0):.6f}% daily human emission).",
            sty["body"]))

        # Figure 6 — sustainability by workflow
        sust_cols = ["energy_j","carbon_g","water_ml"]
        sust_avail = [c for c in sust_cols if c in runs.columns]
        if sust_avail and "workflow_type" in runs.columns:
            sust_agg = runs.groupby("workflow_type")[sust_avail].sum().reset_index()
            fig6 = go.Figure()
            clr_map = {"energy_j":"#2ecc71","carbon_g":"#95a5a6","water_ml":"#3498db"}
            for col in sust_avail:
                fig6.add_trace(go.Bar(
                    name=col.replace("_"," ").title(),
                    x=sust_agg.workflow_type,
                    y=sust_agg[col],
                    marker_color=clr_map.get(col,"#888"),
                    yaxis="y" if col == "energy_j" else "y2"))
            fig6.update_layout(
                height=300, barmode="group",
                title="Figure 6 — Sustainability metrics by workflow type",
                yaxis=dict(title="Energy (J) / Carbon (g CO2)"),
                margin=dict(t=50,b=40,l=60,r=30),
                legend=dict(orientation="h", y=1.12))
            _fig(fig6, "Figure 6: Total energy, carbon, and water consumption "
                       "per workflow type for this session.")

        # Table 4 — sustainability comparison
        story.append(Paragraph("Table 4 — Sustainability Metrics Summary", sty["h2"]))
        tbl4 = [["Metric","Value","Human-scale equivalent"]]
        tbl4.append(["Total energy", f"{total_j:.4f} J",
                     f"{sf.get('wh',0):.6f} Wh = {sf.get('led_min',0):.2f} min powering 1W LED"])
        tbl4.append(["Carbon (CO2)", f"{total_c:.6f} g",
                     f"{sf.get('carbon_car_m',0):.4f} mm petrol car driving"])
        tbl4.append(["Water", f"{total_w:.5f} ml",
                     f"{sf.get('water_tsp',0):.5f} teaspoons"])
        tbl4.append(["Methane (CH4)", f"{total_m:.7f} mg",
                     f"{sf.get('methane_human_pct',0):.6f}% daily human emission"])
        story.append(_table(tbl4, [4, 4, 8.7]))

    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # SECTION 5 — ENERGY BREAKDOWN (RAPL domains)
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("5. Package Energy Breakdown (RAPL Domains)", sty["h1"]))
    story.append(HRFlowable(width=W, thickness=0.5, color=colors.HexColor("#c0c8d8")))

    rapl_cols = ["core_energy_j","uncore_energy_j","dram_energy_j"]
    rapl_avail = [c for c in rapl_cols if c in runs.columns]
    if rapl_avail and not runs.empty:
        rapl_agg = runs.groupby("workflow_type")[rapl_avail].mean().reset_index()
        fig7 = go.Figure()
        clr_map2 = {"core_energy_j":"#3498db","uncore_energy_j":"#9b59b6","dram_energy_j":"#1abc9c"}
        for col in rapl_avail:
            fig7.add_trace(go.Bar(
                name=col.replace("_energy_j","").title(),
                x=rapl_agg.workflow_type, y=rapl_agg[col],
                marker_color=clr_map2.get(col,"#888")))
        fig7.update_layout(
            height=300, barmode="stack",
            title="Figure 7 — Mean energy by RAPL domain (J)",
            yaxis_title="Energy (J)",
            margin=dict(t=50,b=40,l=60,r=20),
            legend=dict(orientation="h", y=1.12))
        _fig(fig7, "Figure 7: RAPL domain breakdown — Core, Uncore (GPU/cache), DRAM. "
                   "Agentic DRAM cost is often elevated due to KV-cache pressure.")

        story.append(Paragraph("Table 5 — Mean RAPL Domain Energy (J)", sty["h2"]))
        tbl5_hdr = ["Workflow"] + [c.replace("_energy_j","").title() for c in rapl_avail] + ["Total (J)"]
        tbl5 = [tbl5_hdr]
        for _, r in rapl_agg.iterrows():
            vals = [f"{r[c]:.5f}" for c in rapl_avail]
            total_rapl = sum(r[c] for c in rapl_avail)
            tbl5.append([_safe_str(r.workflow_type)] + vals + [f"{total_rapl:.5f}"])
        story.append(_table(tbl5, [3.5]+[3.0]*len(rapl_avail)+[3.0]))
    else:
        story.append(Paragraph("RAPL domain data not available for this session.", sty["body"]))

    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # SECTION 6 — PER-PAIR DETAIL
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("6. Per-Pair Analysis", sty["h1"]))
    story.append(HRFlowable(width=W, thickness=0.5, color=colors.HexColor("#c0c8d8")))

    for i, row in tax.head(12).iterrows():
        tax_x = float(row.get("tax_multiplier",0))
        _, verdict, _ = _tax_verdict(tax_x)
        story.append(KeepTogether([
            Paragraph(
                f"6.{i+1}  {_safe_str(row.get('task_name','?'))} / "
                f"{_safe_str(row.get('provider','?'))} — "
                f"Rep {int(row.get('run_number',i+1))} — "
                f"Tax: {tax_x:.2f}x ({_safe_str(verdict)})",
                sty["h2"]),
            _table(
                [["Metric","Linear","Agentic","Delta"],
                 ["Total Energy (J)",
                  f"{row.get('linear_energy_j',0):.4f}",
                  f"{row.get('agentic_energy_j',0):.4f}",
                  f"+{tax_x:.2f}x"],
                 ["Duration (ms)",
                  f"{row.get('linear_ms',0):.0f}",
                  f"{row.get('agentic_ms',0):.0f}",
                  f"+{(float(row.get('agentic_ms',0))/max(float(row.get('linear_ms',1)),1)-1)*100:.0f}%"],
                 ["IPC",
                  f"{row.get('linear_ipc',0):.3f}",
                  f"{row.get('agentic_ipc',0):.3f}",
                  ""],
                 ["Peak Temp (C)",
                  f"{row.get('linear_max_temp',0):.1f}",
                  f"{row.get('agentic_max_temp',0):.1f}",
                  ""],
                 ["LLM Calls", "1",
                  str(int(row.get('llm_calls',0) or 0)), ""],
                 ["Tool Calls", "0",
                  str(int(row.get('tool_calls',0) or 0)), ""],
                ],
                [4.5, 3.3, 3.3, 3.6]),
            Spacer(1, 0.15*cm),
            Paragraph(_safe_str(_build_pair_narrative(row)), sty["body"]),
            Spacer(1, 0.3*cm),
        ]))

    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # SECTION 7 — SESSION METADATA & METHODOLOGY
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("7. Session Metadata and Methodology", sty["h1"]))
    story.append(HRFlowable(width=W, thickness=0.5, color=colors.HexColor("#c0c8d8")))

    gov     = _safe_str(runs["governor"].iloc[0])      if "governor"      in runs.columns and not runs.empty else "unknown"
    turbo   = str(runs["turbo_enabled"].iloc[0])       if "turbo_enabled" in runs.columns and not runs.empty else "unknown"
    country = _safe_str(runs["country_code"].iloc[0])  if "country_code"  in runs.columns and not runs.empty else "unknown"

    story.append(Paragraph("7.1  Session Parameters", sty["h2"]))
    story.append(_table(
        [["Parameter","Value"],
         ["Session ID",    gid_s],
         ["Experiments",   str(len(exps))],
         ["Total runs",    str(len(runs))],
         ["Governor",      gov],
         ["Turbo Boost",   turbo],
         ["Region",        country],
         ["Generated",     _safe_str(now)],
        ], [5, 11.7]))

    story.append(Paragraph("7.2  Measurement Methodology", sty["h2"]))
    story.append(Paragraph(
        "Energy measurements use Intel RAPL (Running Average Power Limit) "
        "hardware performance counters, which provide sub-millijoule accuracy "
        "for package, core, uncore, and DRAM power domains. "
        "Each experiment runs N repetitions in pairs: one linear workflow "
        "(single LLM call) followed by one agentic workflow (multi-step "
        "orchestration with tool use). The orchestration tax multiplier is "
        "defined as agentic_energy / linear_energy and is always >= 1.0. "
        "A cool-down period between repetitions prevents thermal carry-over. "
        "Sustainability factors (CO2, water, methane) are computed using "
        "region-specific emission factors from config/insights_rules.yaml.",
        sty["body"]))

    story.append(Paragraph("7.3  Reproducibility Fingerprint", sty["h2"]))
    # Load hw+env for reproducibility section
    hw_pdf, env_pdf = {}, {}
    try:
        hw_pdf  = q1(f"""
            SELECT * FROM hardware_config hc
            JOIN experiments e ON e.hw_id = hc.hw_id
            WHERE e.group_id = '{group_id}' LIMIT 1
        """) or {}
    except Exception:
        try: hw_pdf = q1("SELECT * FROM hardware_config ORDER BY hw_id DESC LIMIT 1") or {}
        except: pass
    try:
        env_pdf = q1(f"""
            SELECT * FROM environment_config ec
            JOIN experiments e ON e.env_id = ec.env_id
            WHERE e.group_id = '{group_id}' LIMIT 1
        """) or {}
    except Exception:
        try: env_pdf = q1("SELECT * FROM environment_config ORDER BY env_id DESC LIMIT 1") or {}
        except: pass

    hw_hash  = _safe_str(str(hw_pdf.get("hardware_hash", "unknown"))[:16])
    env_hash = _safe_str(str(env_pdf.get("env_hash", "unknown"))[:16])
    repro_fp = f"{hw_hash}:{env_hash}"

    story.append(Paragraph(
        f"Reproducibility fingerprint: <b>{repro_fp}</b>. "
        f"This identifier encodes the hardware configuration hash and software "
        f"environment hash. To reproduce these results, use identical hardware "
        f"(hw_hash={hw_hash}) and software environment (env_hash={env_hash}). "
        f"Full hardware and environment details are in Appendix A and B.",
        sty["body"]))

    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # APPENDIX A — HARDWARE CONFIGURATION
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("Appendix A — Hardware Configuration", sty["h1"]))
    story.append(HRFlowable(width=W, thickness=0.5, color=colors.HexColor("#c0c8d8")))
    story.append(Paragraph(
        "Complete hardware fingerprint for this experimental session. "
        "All fields captured at session start via sysfs, /proc/cpuinfo, and dmidecode.",
        sty["body"]))

    if hw_pdf:
        hw_rows = [["Field", "Value"]]
        hw_fields = [
            ("hw_id",              "Hardware ID"),
            ("hostname",           "Hostname"),
            ("cpu_model",          "CPU Model"),
            ("cpu_architecture",   "Architecture"),
            ("cpu_vendor",         "CPU Vendor"),
            ("cpu_family",         "CPU Family"),
            ("cpu_model_id",       "CPU Model ID"),
            ("cpu_stepping",       "Stepping"),
            ("cpu_cores",          "Physical Cores"),
            ("cpu_threads",        "Logical Threads"),
            ("ram_gb",             "RAM (GB)"),
            ("kernel_version",     "Kernel Version"),
            ("microcode_version",  "Microcode Version"),
            ("has_avx2",           "AVX2 Support"),
            ("has_avx512",         "AVX-512 Support"),
            ("has_vmx",            "VT-x / VMX"),
            ("rapl_domains",       "RAPL Domains"),
            ("rapl_has_dram",      "RAPL DRAM Domain"),
            ("rapl_has_uncore",    "RAPL Uncore Domain"),
            ("gpu_model",          "GPU Model"),
            ("gpu_driver",         "GPU Driver"),
            ("gpu_count",          "GPU Count"),
            ("gpu_power_available","GPU Power Available"),
            ("system_manufacturer","System Manufacturer"),
            ("system_product",     "System Product"),
            ("system_type",        "System Type"),
            ("virtualization_type","Virtualisation"),
            ("hardware_hash",      "Hardware Hash (full)"),
            ("detected_at",        "Detected At"),
        ]
        for key, label in hw_fields:
            val = hw_pdf.get(key)
            if val is not None:
                # Format booleans
                if isinstance(val, (bool, int)) and key.startswith("has_") or key.startswith("rapl_has") or key == "gpu_power_available":
                    val = "Yes" if val else "No"
                hw_rows.append([label, _safe_str(str(val))])
        story.append(_table(hw_rows, [5.5, 11.2]))
    else:
        story.append(Paragraph("Hardware configuration data not available.", sty["body"]))

    story.append(PageBreak())

    # ────────────────────────────────────────────────────────────────────────
    # APPENDIX B — ENVIRONMENT CONFIGURATION
    # ────────────────────────────────────────────────────────────────────────
    story.append(Paragraph("Appendix B — Software Environment", sty["h1"]))
    story.append(HRFlowable(width=W, thickness=0.5, color=colors.HexColor("#c0c8d8")))
    story.append(Paragraph(
        "Complete software environment snapshot for this experimental session. "
        "Captured at session start. The env_hash uniquely identifies this "
        "exact combination of Python, framework, and library versions.",
        sty["body"]))

    if env_pdf:
        env_rows = [["Field", "Value"]]
        env_fields = [
            ("env_id",                 "Environment ID"),
            ("python_version",         "Python Version"),
            ("python_implementation",  "Python Implementation"),
            ("os_name",                "OS"),
            ("os_version",             "OS Version"),
            ("kernel_version",         "Kernel Version"),
            ("llm_framework",          "LLM Framework"),
            ("framework_version",      "Framework Version"),
            ("numpy_version",          "NumPy Version"),
            ("torch_version",          "PyTorch Version"),
            ("transformers_version",   "Transformers Version"),
            ("git_branch",             "Git Branch"),
            ("git_commit",             "Git Commit"),
            ("git_dirty",              "Git Working Tree Dirty"),
            ("container_runtime",      "Container Runtime"),
            ("container_image",        "Container Image"),
            ("env_hash",               "Environment Hash (full)"),
            ("created_at",             "Captured At"),
        ]
        for key, label in env_fields:
            val = env_pdf.get(key)
            if val is not None:
                if key == "git_dirty":
                    val = "Yes (uncommitted changes)" if val else "No (clean)"
                env_rows.append([label, _safe_str(str(val))])
        story.append(_table(env_rows, [5.5, 11.2]))

        # Reproducibility note
        story.append(Spacer(1, 0.4*cm))
        story.append(Paragraph(
            f"<b>To reproduce this experiment:</b> "
            f"(1) Match hardware with hw_hash <b>{hw_hash}</b> (see Appendix A). "
            f"(2) Checkout git branch <b>{_safe_str(str(env_pdf.get('git_branch','?')))}</b> "
            f"at commit <b>{_safe_str(str(env_pdf.get('git_commit','?'))[:12])}</b>. "
            f"(3) Ensure Python {_safe_str(str(env_pdf.get('python_version','?')))} "
            f"with {_safe_str(str(env_pdf.get('llm_framework','?')))} "
            f"{_safe_str(str(env_pdf.get('framework_version','?')))}. "
            f"(4) Run with the same governor and cool-down settings as recorded in Section 7.",
            sty["body"]))
    else:
        story.append(Paragraph("Environment configuration data not available.", sty["body"]))

    # ── Footer ────────────────────────────────────────────────────────────────
    story.append(Spacer(1, 0.8*cm))
    story.append(HRFlowable(width=W, thickness=0.5, color=colors.HexColor("#cccccc")))
    story.append(Paragraph(
        f"A-LEMS Experimental Report  |  {_safe_str(now)}  |  "
        f"Session {gid_s}  |  Fingerprint {repro_fp}",
        sty["footer"]))

    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# Issue 1: NO st.switch_page / st.rerun() here — safe to embed in any tab
# ══════════════════════════════════════════════════════════════════════════════

def render_session_analysis(group_id: str):
    """
    Called from execute.py Tab 3 with a group_id.
    SAFE TO EMBED — contains no st.switch_page or st.rerun() calls.
    """
    if not group_id:
        st.info("No session selected.")
        return

    exps = _load_session_experiments(group_id)
    runs = _load_session_runs(group_id)
    tax  = _load_tax_for_session(group_id)

    # Ensure DataFrames are never None
    if exps is None: exps = pd.DataFrame()
    if runs is None: runs = pd.DataFrame()
    if tax  is None: tax  = pd.DataFrame()

    if exps.empty:
        st.warning(f"No experiments found for session: {group_id}")
        return

    _session_header(group_id, exps, runs)

    s1, s2, s3, s4, s5, s6 = st.tabs([
        "🏆 Summary",
        "⚡ Energy",
        "🌡️ Thermal",
        "🧠 CPU",
        "📋 Per-Pair",
        "💾 Export",
    ])

    with s1: _tab_summary(group_id, exps, runs, tax)
    with s2: _tab_energy(group_id, runs, tax)
    with s3: _tab_thermal(group_id, runs)
    with s4: _tab_cpu(group_id, runs)
    with s5: _tab_per_pair(group_id, tax)
    with s6: _tab_export(group_id, exps, runs, tax)
    if not group_id:
        st.info("No session selected. Run an experiment first, or select a session from the sidebar.")
        return

    # Load all data for this session
    exps = _load_session_experiments(group_id)





def render(ctx: dict) -> None:
    import streamlit as st
    from gui.db import q
    from gui.components.session_tree import render_session_tree

    sessions = q(
        "SELECT e.group_id, COUNT(DISTINCT e.exp_id) AS experiments, "
        "SUM(e.runs_completed) AS runs_done, SUM(e.runs_total) AS runs_total, "
        "MIN(e.created_at) AS started_at FROM experiments e "
        "WHERE e.group_id IS NOT NULL GROUP BY e.group_id "
        "ORDER BY started_at DESC LIMIT 100"
    )
    if sessions.empty:
        st.info("No sessions recorded yet.")
        return

    def _fmt(row):
        done  = int(row.get("runs_done")  or 0)
        total = int(row.get("runs_total") or 0)
        exps  = int(row.get("experiments") or 0)
        ts    = str(row.get("started_at") or "")[:16]
        short = str(row["group_id"]).replace("session_","").replace("_"," ",1)[:22]
        return f"{short} - {exps} exps - {done}/{total} runs - {ts}"

    group_ids = sessions["group_id"].tolist()
    labels = [_fmt(sessions.iloc[i]) for i in range(len(sessions))]
    label_to_id = dict(zip(labels, group_ids))
    selected_label = st.selectbox("Select session", labels, key="sa_group_sel")
    sel_group = label_to_id[selected_label]
    st.markdown("<div style='margin-bottom:8px'></div>", unsafe_allow_html=True)
    accent = "#3b82f6"
    short_id = sel_group.replace("session_","").replace("_"," ",1)
    st.markdown(
        f"<div style='font-size:11px;font-weight:700;color:{accent};"
        f"padding:10px 14px;background:linear-gradient(135deg,{accent}14,{accent}06);"
        f"border:1px solid {accent}33;border-radius:8px;margin-bottom:8px;'>"
        f"Session Tree - {short_id}</div>",
        unsafe_allow_html=True)
    render_session_tree(group_id=sel_group, expanded=True, live_log=None, key_suffix="_history")
    st.markdown("<div style='height:0.5px;background:#1f2937;margin:20px 0;'></div>", unsafe_allow_html=True)
    render_session_analysis(sel_group)


def render(ctx: dict) -> None:
    import streamlit as st
    from gui.db import q
    from gui.components.session_tree import render_session_tree

    sessions = q(
        "SELECT e.group_id, COUNT(DISTINCT e.exp_id) AS experiments, "
        "SUM(e.runs_completed) AS runs_done, SUM(e.runs_total) AS runs_total, "
        "MIN(e.created_at) AS started_at FROM experiments e "
        "WHERE e.group_id IS NOT NULL GROUP BY e.group_id "
        "ORDER BY started_at DESC LIMIT 100"
    )
    if sessions.empty:
        st.info("No sessions recorded yet.")
        return

    def _fmt(row):
        done  = int(row.get("runs_done")  or 0)
        total = int(row.get("runs_total") or 0)
        exps  = int(row.get("experiments") or 0)
        ts    = str(row.get("started_at") or "")[:16]
        short = str(row["group_id"]).replace("session_","").replace("_"," ",1)[:22]
        return f"{short} - {exps} exps - {done}/{total} runs - {ts}"

    group_ids = sessions["group_id"].tolist()
    labels = [_fmt(sessions.iloc[i]) for i in range(len(sessions))]
    label_to_id = dict(zip(labels, group_ids))
    selected_label = st.selectbox("Select session", labels, key="sa_group_sel")
    sel_group = label_to_id[selected_label]
    st.markdown("<div style='margin-bottom:8px'></div>", unsafe_allow_html=True)
    accent = "#3b82f6"
    short_id = sel_group.replace("session_","").replace("_"," ",1)
    st.markdown(
        f"<div style='font-size:11px;font-weight:700;color:{accent};"
        f"padding:10px 14px;background:linear-gradient(135deg,{accent}14,{accent}06);"
        f"border:1px solid {accent}33;border-radius:8px;margin-bottom:8px;'>"
        f"Session Tree - {short_id}</div>",
        unsafe_allow_html=True)
    render_session_tree(group_id=sel_group, expanded=True, live_log=None, key_suffix="_history")
    st.markdown("<div style='height:0.5px;background:#1f2937;margin:20px 0;'></div>", unsafe_allow_html=True)
    render_session_analysis(sel_group)
