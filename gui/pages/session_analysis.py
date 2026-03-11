"""
gui/pages/session_analysis.py
─────────────────────────────────────────────────────────────────────────────
Session Analysis — the gold mine.
Called from execute.py Tab 3 with a group_id.

Six sub-tabs:
  🏆 Summary    — master table, verdicts, insight cards, session tree
  ⚡ Energy     — breakdown charts, OOI, orchestration overhead
  🌡️ Thermal    — temperature timeline, throttle risk, C-states
  🧠 CPU        — IPC, cache, thread migrations, scheduler metrics
  📋 Per-Pair   — expandable cards with research narrative per run pair
  💾 Export     — Excel + PDF academic report

All thresholds and sustainability factors from config/insights_rules.yaml.
All comparisons rule-based — templates in config/insights_rules.yaml.
30% comments throughout for researcher readability.
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

# ── Config shortcuts ──────────────────────────────────────────────────────────
_TAX   = INSIGHTS_RULES.get("tax_thresholds", {})
_THERM = INSIGHTS_RULES.get("thermal", {})
_NARR  = INSIGHTS_RULES.get("narrative_templates", {})

# Thermal thresholds (from config, with sensible fallbacks)
_THROTTLE_C = _THERM.get("throttle_threshold_c", 95)
_CAUTION_C  = _THERM.get("caution_threshold_c",  85)
_SAFE_C     = _THERM.get("safe_threshold_c",      70)


# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADERS — all DB queries for a given group_id
# ══════════════════════════════════════════════════════════════════════════════

def _load_session_experiments(group_id: str) -> pd.DataFrame:
    """Load all experiments in the session with their run counts."""
    return q(f"""
        SELECT exp_id, task_name, provider, model_name, status,
               runs_completed, runs_total, optimization_enabled,
               started_at, completed_at, group_id
        FROM experiments
        WHERE group_id = '{group_id}'
        ORDER BY exp_id
    """)


def _load_session_runs(group_id: str) -> pd.DataFrame:
    """
    Load all runs for a session joined to experiments.
    This is the main data source for all sub-tabs.
    """
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
    """Load orchestration tax summary rows for this session."""
    return q(f"""
        SELECT
            ots.comparison_id,
            ots.linear_run_id, ots.agentic_run_id,
            ots.linear_dynamic_uj  / 1e6  AS linear_dynamic_j,
            ots.agentic_dynamic_uj / 1e6  AS agentic_dynamic_j,
            ots.orchestration_tax_uj / 1e6 AS tax_j,
            ots.tax_percent,
            ots.tax_percent / 100.0        AS tax_multiplier,
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
# TAX VERDICT — rule-based label from config thresholds
# ══════════════════════════════════════════════════════════════════════════════

def _tax_verdict(tax_x: float) -> tuple[str, str, str]:
    """
    Return (emoji, label, color) for an orchestration tax multiplier.
    Thresholds loaded from config/insights_rules.yaml tax_thresholds.
    """
    extreme_min = _TAX.get("extreme", {}).get("min", 15)
    high_max    = _TAX.get("high",    {}).get("max", 15)
    mod_max     = _TAX.get("moderate",{}).get("max", 5)

    if tax_x >= extreme_min:
        return "🔴", _TAX.get("extreme", {}).get("label", "EXTREME"), "#ef4444"
    if tax_x >= high_max * 0.67:    # high starts at 5x
        return "🟠", _TAX.get("high",    {}).get("label", "HIGH"),    "#f59e0b"
    if tax_x >= mod_max:
        return "🟡", _TAX.get("moderate",{}).get("label", "MODERATE"),"#f59e0b"
    return "🟢", _TAX.get("low", {}).get("label", "LOW"), "#22c55e"


# ══════════════════════════════════════════════════════════════════════════════
# NARRATIVE ENGINE — rule-based English text from config templates
# ══════════════════════════════════════════════════════════════════════════════

def _build_pair_narrative(row: pd.Series) -> str:
    """
    Generate a research narrative paragraph for one linear/agentic pair.
    Uses templates from config/insights_rules.yaml narrative_templates.
    """
    parts = []

    # Tax narrative
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

    # Thermal narrative
    max_temp = float(row.get("agentic_max_temp", 0) or 0)
    delta    = float(row.get("agentic_tdelta",   0) or 0)
    headroom = _THROTTLE_C - max_temp
    if max_temp > _CAUTION_C:
        t = _NARR.get("thermal_caution", {}).get("template", "")
        parts.append(t.format(delta=delta, headroom=headroom, throttle_c=_THROTTLE_C))
    elif max_temp > 0 and max_temp < _SAFE_C:
        t = _NARR.get("thermal_safe", {}).get("template", "")
        parts.append(t.format(max_temp=max_temp))

    # Thread migration narrative
    a_tmig = float(row.get("agentic_tmig", 0) or 0)
    l_tmig = float(row.get("linear_tmig",  0) or 1)
    ratio  = a_tmig / max(l_tmig, 1)
    if ratio > 5:
        t = _NARR.get("thread_migrations", {}).get("template", "")
        parts.append(t.format(agentic_migrations=int(a_tmig),
                              linear_migrations=int(l_tmig), ratio=ratio))

    # IPC drop narrative
    a_ipc = float(row.get("agentic_ipc", 0) or 0)
    l_ipc = float(row.get("linear_ipc",  0) or 0)
    if l_ipc > 0 and a_ipc > 0:
        drop_pct = (l_ipc - a_ipc) / l_ipc * 100
        if drop_pct > 5:
            t = _NARR.get("ipc_drop", {}).get("template", "")
            parts.append(t.format(drop_pct=drop_pct,
                                  linear_ipc=l_ipc, agentic_ipc=a_ipc))

    # LLM/tool call narrative
    llm_calls  = int(row.get("llm_calls",  0) or 0)
    tool_calls = int(row.get("tool_calls", 0) or 0)
    if llm_calls > 1:
        t = _NARR.get("llm_calls", {}).get("template", "")
        parts.append(t.format(llm_calls=llm_calls, tool_calls=tool_calls))

    return " ".join(parts) if parts else "Insufficient data for narrative generation."


# ══════════════════════════════════════════════════════════════════════════════
# SESSION HEADER BANNER
# ══════════════════════════════════════════════════════════════════════════════

def _session_header(group_id: str, exps: pd.DataFrame, runs: pd.DataFrame):
    """Render the session summary banner at the top of the analysis view."""
    n_exps = len(exps)
    n_runs = len(runs)

    # Parse session duration
    try:
        starts = pd.to_datetime(exps["started_at"].dropna())
        ends   = pd.to_datetime(exps["completed_at"].dropna())
        t_start = starts.min().strftime("%H:%M:%S") if len(starts) else "—"
        t_end   = ends.max().strftime("%H:%M:%S")   if len(ends)   else "—"
        dur_s   = (ends.max() - starts.min()).total_seconds() if len(starts) and len(ends) else 0
        dur_str = f"{int(dur_s//60)}m {int(dur_s%60)}s" if dur_s > 0 else "—"
    except Exception:
        t_start = t_end = dur_str = "—"

    # Hardware info
    hw = q1("SELECT cpu_model, total_cores, ram_gb FROM hardware_config LIMIT 1")
    hw_str = hw.get("cpu_model", "Unknown CPU") if hw else "Hardware info unavailable"

    # Governor / turbo from runs
    gov   = runs["governor"].iloc[0]   if "governor"      in runs and not runs.empty else "—"
    turbo = runs["turbo_enabled"].iloc[0] if "turbo_enabled" in runs and not runs.empty else "—"

    st.markdown(
        f"<div style='background:#050c18;border:1px solid #1e3a5f;"
        f"border-left:4px solid #3b82f6;border-radius:6px;"
        f"padding:12px 16px;margin-bottom:12px;'>"
        f"<div style='font-size:11px;font-weight:700;color:#4fc3f7;"
        f"letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px;'>"
        f"📊 Session Report</div>"
        f"<div style='font-family:monospace;font-size:11px;color:#c8d8e8;"
        f"margin-bottom:4px;'>{group_id}</div>"
        f"<div style='font-size:10px;color:#5a7090;display:flex;gap:20px;flex-wrap:wrap;'>"
        f"<span>🔬 <b style='color:#7090b0'>{n_exps}</b> experiments</span>"
        f"<span>▶ <b style='color:#7090b0'>{n_runs}</b> runs</span>"
        f"<span>⏱ <b style='color:#7090b0'>{t_start} → {t_end}</b>"
        f" ({dur_str})</span>"
        f"</div>"
        f"<div style='font-size:9px;color:#3d5570;margin-top:4px;'>"
        f"🖥 {hw_str} · Governor: {gov} · Turbo: {'ON' if turbo else 'OFF'}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# SUB-TAB 1 — SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def _tab_summary(exps: pd.DataFrame, runs: pd.DataFrame, tax: pd.DataFrame):
    """Master summary table + verdicts + insight cards."""

    if tax.empty:
        st.info("No orchestration tax data found for this session. Run a comparison experiment first.")
        return

    # ── Master summary table ──────────────────────────────────────────────────
    st.markdown("#### 🏆 Orchestration Tax — Master Summary")

    # Aggregate tax by task + provider (mean across repetitions)
    summary = (tax.groupby(["task_name", "provider"])
               .agg(linear_j =("linear_energy_j",  "mean"),
                    agentic_j =("agentic_energy_j", "mean"),
                    tax_x     =("tax_multiplier",   "mean"),
                    n_pairs   =("comparison_id",    "count"))
               .reset_index())

    # Build HTML table
    rows_html = ""
    for _, r in summary.iterrows():
        emoji, label, clr = _tax_verdict(float(r.tax_x))
        rows_html += (
            f"<tr style='border-bottom:1px solid #111827;'>"
            f"<td style='padding:9px 8px;font-size:10px;color:#7090b0;'>{r.provider}</td>"
            f"<td style='padding:9px 8px;font-size:10px;color:#c8d8e8;'>{r.task_name}</td>"
            f"<td style='padding:9px 8px;font-family:monospace;font-size:11px;color:#22c55e;'>"
            f"{r.linear_j:.4f} J</td>"
            f"<td style='padding:9px 8px;font-family:monospace;font-size:11px;color:#ef4444;'>"
            f"{r.agentic_j:.4f} J</td>"
            f"<td style='padding:9px 8px;text-align:center;'>"
            f"<span style='font-size:14px;font-weight:700;color:{clr};"
            f"font-family:monospace;'>{r.tax_x:.2f}×</span></td>"
            f"<td style='padding:9px 8px;font-size:10px;'>"
            f"{emoji} <span style='color:{clr};'>{label}</span></td>"
            f"<td style='padding:9px 8px;font-size:9px;color:#3d5570;'>{int(r.n_pairs)} pairs</td>"
            f"</tr>"
        )

    st.markdown(
        "<div style='background:#07090f;border:1px solid #1e2d45;"
        "border-radius:8px;overflow:hidden;margin:8px 0 16px;'>"
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

    # ── Highlight metric cards ────────────────────────────────────────────────
    if len(summary) > 0:
        best  = summary.loc[summary.tax_x.idxmin()]
        worst = summary.loc[summary.tax_x.idxmax()]
        avg   = summary.tax_x.mean()
        c1, c2, c3 = st.columns(3)
        c1.success(f"**✅ Lowest tax**\n\n{best.provider} · {best.task_name}\n\n**{best.tax_x:.2f}×**")
        c2.error(f"**⚠ Highest tax**\n\n{worst.provider} · {worst.task_name}\n\n**{worst.tax_x:.2f}×**")
        c3.info(f"**📈 Session average**\n\n{len(summary)} experiment types\n\n**{avg:.2f}×**")

    # ── Rule-based insight cards ──────────────────────────────────────────────
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
                       summary: pd.DataFrame) -> list[dict]:
    """
    Generate rule-based insight cards for the session.
    Rules defined here; thresholds from config/insights_rules.yaml.
    Returns list of {icon, title, body, color}.
    """
    insights = []

    if tax.empty or runs.empty:
        return insights

    # Rule 1: Extreme tax
    extreme = summary[summary.tax_x >= _TAX.get("extreme",{}).get("min", 15)]
    if not extreme.empty:
        worst = extreme.loc[extreme.tax_x.idxmax()]
        savings = (1 - 1/worst.tax_x) * 100
        insights.append({"icon": "🔴", "color": "#ef4444",
            "title": f"Extreme orchestration tax: {worst.tax_x:.1f}× on {worst.task_name}/{worst.provider}",
            "body": f"Agentic execution consumes {worst.tax_x:.1f}× more energy than linear for this task. "
                    f"Switching to linear execution would save {savings:.0f}% of energy. "
                    f"This is the key finding of this session."})

    # Rule 2: Thermal risk
    agentic_runs = runs[runs.workflow_type == "agentic"]
    if not agentic_runs.empty and "max_temp_c" in agentic_runs:
        hot = agentic_runs[agentic_runs.max_temp_c > _CAUTION_C]
        if not hot.empty:
            peak  = agentic_runs.max_temp_c.max()
            hdroom= _THROTTLE_C - peak
            insights.append({"icon": "🌡️", "color": "#f59e0b",
                "title": f"{len(hot)} agentic runs exceeded {_CAUTION_C}°C",
                "body": f"Peak temperature reached {peak:.0f}°C — only {hdroom:.0f}°C "
                        f"below throttle threshold ({_THROTTLE_C}°C). "
                        f"Sustained agentic workloads on local inference drive thermal load."})

    # Rule 3: Thread migration ratio
    lin = runs[runs.workflow_type == "linear"]
    agt = runs[runs.workflow_type == "agentic"]
    if not lin.empty and not agt.empty and "thread_migrations" in runs:
        avg_l = lin.thread_migrations.mean()
        avg_a = agt.thread_migrations.mean()
        if avg_l > 0:
            ratio = avg_a / avg_l
            if ratio > 5:
                insights.append({"icon": "🧵", "color": "#a78bfa",
                    "title": f"Thread migrations {ratio:.0f}× higher in agentic mode",
                    "body": f"Average {avg_a:,.0f} thread migrations in agentic vs "
                            f"{avg_l:,.0f} in linear. This confirms significant async "
                            f"orchestration overhead beyond pure LLM compute — "
                            f"the scheduler is working much harder."})

    # Rule 4: Cloud vs local tax gap
    if "provider" in summary.columns and len(summary.provider.unique()) > 1:
        by_prov = summary.groupby("provider").tax_x.mean()
        if "local" in by_prov and "cloud" in by_prov:
            ratio = by_prov["local"] / by_prov["cloud"]
            if ratio > 2:
                insights.append({"icon": "☁️", "color": "#38bdf8",
                    "title": f"Local inference has {ratio:.1f}× higher tax than cloud",
                    "body": f"Local: avg {by_prov['local']:.1f}× tax. "
                            f"Cloud: avg {by_prov['cloud']:.1f}× tax. "
                            f"Local LLM inference carries higher orchestration overhead, "
                            f"likely due to slower token generation amplifying coordination cost."})

    # Rule 5: Optimization comparison (if any runs have optimization_enabled)
    if "optimization_enabled" in runs.columns:
        opt_runs  = runs[runs.optimization_enabled == 1]
        base_runs = runs[runs.optimization_enabled == 0]
        if not opt_runs.empty and not base_runs.empty:
            opt_e  = opt_runs.energy_j.mean()
            base_e = base_runs.energy_j.mean()
            saving = (base_e - opt_e) / base_e * 100 if base_e > 0 else 0
            insights.append({"icon": "🔧", "color": "#22c55e",
                "title": f"Optimization rules reduced energy by {saving:.1f}%",
                "body": f"Optimized runs averaged {opt_e:.3f}J vs "
                        f"{base_e:.3f}J baseline. "
                        f"Rule-based optimization is showing measurable impact."})

    return insights


# ══════════════════════════════════════════════════════════════════════════════
# SUB-TAB 2 — ENERGY
# ══════════════════════════════════════════════════════════════════════════════

def _tab_energy(runs: pd.DataFrame, tax: pd.DataFrame):
    """Energy breakdown charts, OOI, sustainability panel."""

    if runs.empty:
        st.info("No run data for this session.")
        return

    # ── Linear vs Agentic grouped bar ────────────────────────────────────────
    st.markdown("#### ⚡ Linear vs Agentic Energy")
    if not tax.empty:
        fig = go.Figure()
        labels = tax.task_name + " · " + tax.provider
        fig.add_trace(go.Bar(name="Linear",  x=labels, y=tax.linear_energy_j,
                             marker_color="#22c55e"))
        fig.add_trace(go.Bar(name="Agentic", x=labels, y=tax.agentic_energy_j,
                             marker_color="#ef4444"))
        fig.update_layout(**PL, barmode="group", height=280,
                          title="Linear vs Agentic total energy (J)",
                          xaxis_tickangle=15)
        st.plotly_chart(fig, use_container_width=True)

    # ── Package breakdown: Core + Uncore + DRAM ───────────────────────────────
    st.markdown("#### 🔋 Package Energy Breakdown")
    agg = (runs.groupby("workflow_type")
           .agg(core_j  =("core_energy_j",   "mean"),
                uncore_j=("uncore_energy_j",  "mean"),
                dram_j  =("dram_energy_j",    "mean"))
           .reset_index())

    if not agg.empty:
        fig2 = go.Figure()
        for col, name, clr in [("core_j",   "Core",   "#3b82f6"),
                                ("uncore_j", "Uncore", "#a78bfa"),
                                ("dram_j",   "DRAM",   "#38bdf8")]:
            if col in agg.columns:
                fig2.add_trace(go.Bar(name=name, x=agg.workflow_type,
                                      y=agg[col], marker_color=clr))
        fig2.update_layout(**PL, barmode="stack", height=240,
                           title="Mean energy by RAPL domain (J)")
        st.plotly_chart(fig2, use_container_width=True)

    # ── Orchestration Overhead Index (OOI) ────────────────────────────────────
    st.markdown("#### 📐 Orchestration Overhead Index (OOI)")
    st.caption("OOI = (Agentic energy − Linear energy) / Agentic energy · 100%  — "
               "what fraction of agentic energy is pure orchestration overhead?")
    if not tax.empty:
        tax2 = tax.copy()
        tax2["ooi"] = ((tax2.agentic_energy_j - tax2.linear_energy_j)
                       / tax2.agentic_energy_j.clip(lower=0.0001) * 100)
        tax2["label"] = tax2.task_name + "\n" + tax2.provider

        fig3 = go.Figure(go.Bar(
            x=tax2.label, y=tax2.ooi,
            marker_color=["#ef4444" if v > 50 else "#f59e0b" if v > 20 else "#22c55e"
                          for v in tax2.ooi],
            text=tax2.ooi.round(1).astype(str) + "%",
            textposition="outside"))
        fig3.update_layout(**PL, height=250,
                           title="OOI per experiment pair (%)",
                           yaxis_title="OOI (%)")
        st.plotly_chart(fig3, use_container_width=True)

    # ── Energy per token / per instruction ───────────────────────────────────
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
                fig4.update_layout(**PL, height=200, title="Energy per token (J/tok)")
                st.plotly_chart(fig4, use_container_width=True)

    with col2:
        if "energy_per_instruction" in runs.columns:
            epi = (runs[runs.energy_per_instruction.notna()]
                   .groupby("workflow_type").energy_per_instruction.mean().reset_index())
            if not epi.empty:
                fig5 = go.Figure(go.Bar(
                    x=epi.workflow_type, y=epi.energy_per_instruction,
                    marker_color=["#22c55e" if w == "linear" else "#ef4444"
                                  for w in epi.workflow_type]))
                fig5.update_layout(**PL, height=200, title="Energy per instruction (J/inst)")
                st.plotly_chart(fig5, use_container_width=True)

    # ── Sustainability panel ──────────────────────────────────────────────────
    st.markdown("#### 🌍 Sustainability Impact")
    total_j = float(runs.energy_j.sum()) if "energy_j" in runs else 0
    sf = _human_energy_full(total_j)
    if sf:
        # Total carbon, water, methane from DB (more accurate than derived)
        total_carbon_g  = float(runs.carbon_g.sum())   if "carbon_g"   in runs else sf["carbon_g"]
        total_water_ml  = float(runs.water_ml.sum())   if "water_ml"   in runs else sf["water_ml"]
        total_methane_mg= float(runs.methane_mg.sum()) if "methane_mg" in runs else sf["methane_mg"]

        st.markdown(
            f"<div style='background:#050c18;border:1px solid #1e3a2f;"
            f"border-radius:8px;padding:14px 18px;'>"

            f"<div style='font-size:10px;font-weight:700;color:#22c55e;"
            f"letter-spacing:.08em;text-transform:uppercase;margin-bottom:10px;'>"
            f"This session consumed:</div>"

            f"<div style='display:grid;grid-template-columns:1fr 1fr;gap:10px;'>"

            # Energy
            f"<div style='background:#0a1a0a;border:1px solid #1a3020;"
            f"border-radius:5px;padding:10px;'>"
            f"<div style='font-size:16px;margin-bottom:4px;'>⚡</div>"
            f"<div style='font-size:13px;font-weight:700;color:#22c55e;"
            f"font-family:monospace;'>{total_j:.2f} J</div>"
            f"<div style='font-size:9px;color:#3d5570;margin-top:4px;line-height:1.6;'>"
            f"= {sf['wh']:.4f} Wh<br/>"
            f"= {sf['phone_pct']:.4f}% of a phone charge<br/>"
            f"= {sf['led_min']:.1f} min powering a 1W LED bulb</div></div>"

            # Carbon
            f"<div style='background:#0a100a;border:1px solid #1a2a1a;"
            f"border-radius:5px;padding:10px;'>"
            f"<div style='font-size:16px;margin-bottom:4px;'>💨</div>"
            f"<div style='font-size:13px;font-weight:700;color:#4ade80;"
            f"font-family:monospace;'>{total_carbon_g:.5f} g CO₂</div>"
            f"<div style='font-size:9px;color:#3d5570;margin-top:4px;line-height:1.6;'>"
            f"= {sf['carbon_car_m']:.2f}mm of petrol car driving<br/>"
            f"= {sf['carbon_phone_min']:.2f} min of smartphone use</div></div>"

            # Water
            f"<div style='background:#080e14;border:1px solid #1a2535;"
            f"border-radius:5px;padding:10px;'>"
            f"<div style='font-size:16px;margin-bottom:4px;'>💧</div>"
            f"<div style='font-size:13px;font-weight:700;color:#38bdf8;"
            f"font-family:monospace;'>{total_water_ml:.4f} ml</div>"
            f"<div style='font-size:9px;color:#3d5570;margin-top:4px;line-height:1.6;'>"
            f"= {sf['water_tsp']:.4f} teaspoons of water<br/>"
            f"= {sf['water_shower_pct']:.6f}% of a shower</div></div>"

            # Methane — surfaced for first time
            f"<div style='background:#100a0a;border:1px solid #2a1a1a;"
            f"border-radius:5px;padding:10px;'>"
            f"<div style='font-size:16px;margin-bottom:4px;'>🌿</div>"
            f"<div style='font-size:13px;font-weight:700;color:#f87171;"
            f"font-family:monospace;'>{total_methane_mg:.6f} mg CH₄</div>"
            f"<div style='font-size:9px;color:#3d5570;margin-top:4px;line-height:1.6;'>"
            f"= {sf['methane_human_pct']:.6f}% of daily human CH₄ emission<br/>"
            f"<span style='color:#2d3f55;font-style:italic;'>Methane impact — "
            f"first time this metric is shown in A-LEMS</span></div></div>"

            f"</div></div>",
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# SUB-TAB 3 — THERMAL
# ══════════════════════════════════════════════════════════════════════════════

def _tab_thermal(runs: pd.DataFrame):
    """Temperature timeline, throttle risk, C-states, rate of change."""

    if runs.empty or "max_temp_c" not in runs.columns:
        st.info("No thermal data available for this session.")
        return

    # ── Temperature timeline ──────────────────────────────────────────────────
    st.markdown("#### 🌡️ Temperature Profile per Run")
    tmp = runs[["run_id", "workflow_type", "start_temp_c",
                "max_temp_c", "min_temp_c", "thermal_delta_c",
                "task_name", "provider"]].dropna(subset=["max_temp_c"])

    if not tmp.empty:
        fig = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = tmp[tmp.workflow_type == wf]
            if sub.empty: continue
            # Error bars showing min→max range
            fig.add_trace(go.Scatter(
                x=sub.run_id.astype(str),
                y=sub.max_temp_c,
                error_y=dict(
                    type="data", symmetric=False,
                    array=(sub.max_temp_c - sub.max_temp_c).tolist(),
                    arrayminus=(sub.max_temp_c - sub.min_temp_c).tolist()),
                mode="markers+lines",
                name=wf.capitalize(),
                marker_color=clr, marker_size=8,
                line_width=2))

        # Threshold lines from config
        fig.add_hline(y=_THROTTLE_C, line_dash="dash", line_color="#ef4444",
                      annotation_text=f"Throttle ({_THROTTLE_C}°C)", annotation_position="right")
        fig.add_hline(y=_CAUTION_C,  line_dash="dot",  line_color="#f59e0b",
                      annotation_text=f"Caution ({_CAUTION_C}°C)",  annotation_position="right")
        fig.update_layout(**PL, height=280, title="Peak temperature per run",
                          xaxis_title="Run ID", yaxis_title="Temperature (°C)")
        st.plotly_chart(fig, use_container_width=True)

    # ── Thermal delta bar chart ───────────────────────────────────────────────
    st.markdown("#### 📈 Thermal Rise (ΔT) per Run")
    if "thermal_delta_c" in runs.columns:
        fig2 = go.Figure(go.Bar(
            x=runs.run_id.astype(str),
            y=runs.thermal_delta_c,
            marker_color=["#ef4444" if v > 25 else "#f59e0b" if v > 15 else "#22c55e"
                          for v in runs.thermal_delta_c.fillna(0)],
            text=runs.thermal_delta_c.round(1),
            textposition="outside"))
        fig2.update_layout(**PL, height=220,
                           title="Temperature rise during run (°C)",
                           xaxis_title="Run ID", yaxis_title="ΔT (°C)")
        st.plotly_chart(fig2, use_container_width=True)

    # ── Throttle risk gauge ───────────────────────────────────────────────────
    peak_temp = float(runs.max_temp_c.max()) if "max_temp_c" in runs else 0
    headroom  = _THROTTLE_C - peak_temp
    risk_clr  = ("#ef4444" if headroom < 5
                 else "#f59e0b" if headroom < 15 else "#22c55e")
    risk_txt  = ("🔴 CRITICAL" if headroom < 5
                 else "🟡 CAUTION" if headroom < 15 else "🟢 SAFE")

    c1, c2, c3 = st.columns(3)
    c1.metric("Peak Temperature", f"{peak_temp:.1f}°C")
    c2.metric("Throttle Headroom", f"{headroom:.1f}°C", delta=None)
    c3.markdown(
        f"<div style='padding:10px;background:{risk_clr}11;"
        f"border:1px solid {risk_clr}33;border-radius:5px;text-align:center;"
        f"margin-top:8px;'>"
        f"<div style='font-size:12px;font-weight:700;color:{risk_clr};'>"
        f"{risk_txt}</div></div>",
        unsafe_allow_html=True)

    # ── C-State residency ─────────────────────────────────────────────────────
    st.markdown("#### 💤 C-State Residency")
    c_cols = ["c2_time_seconds", "c3_time_seconds", "c6_time_seconds", "c7_time_seconds"]
    c_avail = [c for c in c_cols if c in runs.columns]
    if c_avail:
        c_data = runs.groupby("workflow_type")[c_avail].mean().reset_index()
        fig3 = go.Figure()
        for col, name in zip(c_avail, ["C2", "C3", "C6", "C7"]):
            fig3.add_trace(go.Bar(name=name, x=c_data.workflow_type, y=c_data[col]))
        fig3.update_layout(**PL, barmode="stack", height=220,
                           title="Mean C-state residency (seconds)")
        st.plotly_chart(fig3, use_container_width=True)

    # ── Thermal throttle events ───────────────────────────────────────────────
    if "thermal_throttle_flag" in runs.columns:
        throttle_events = int(runs.thermal_throttle_flag.sum())
        if throttle_events > 0:
            st.error(f"⚠ {throttle_events} thermal throttle event(s) detected in this session!")
        else:
            st.success("✅ No thermal throttle events detected.")


# ══════════════════════════════════════════════════════════════════════════════
# SUB-TAB 4 — CPU & SCHEDULER
# ══════════════════════════════════════════════════════════════════════════════

def _tab_cpu(runs: pd.DataFrame):
    """IPC, cache miss rate, thread migrations, scheduler metrics."""

    if runs.empty:
        st.info("No run data for this session.")
        return

    # ── IPC comparison ────────────────────────────────────────────────────────
    st.markdown("#### 🧠 Instructions Per Cycle (IPC)")
    if "ipc" in runs.columns:
        ipc_agg = runs.groupby("workflow_type").ipc.mean().reset_index()
        fig = go.Figure(go.Bar(
            x=ipc_agg.workflow_type, y=ipc_agg.ipc,
            marker_color=["#22c55e" if w == "linear" else "#ef4444"
                          for w in ipc_agg.workflow_type],
            text=ipc_agg.ipc.round(3), textposition="outside"))
        fig.update_layout(**PL, height=220, title="Mean IPC — higher is better",
                          yaxis_title="IPC")
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    # ── Cache miss rate ───────────────────────────────────────────────────────
    with col1:
        if "cache_miss_rate" in runs.columns:
            cmr = runs.groupby("workflow_type").cache_miss_rate.mean().reset_index()
            fig2 = go.Figure(go.Bar(
                x=cmr.workflow_type, y=cmr.cache_miss_rate * 100,
                marker_color=["#22c55e" if w == "linear" else "#ef4444"
                              for w in cmr.workflow_type]))
            fig2.update_layout(**PL, height=220,
                               title="Cache miss rate (%) — lower is better")
            st.plotly_chart(fig2, use_container_width=True)

    # ── Thread migrations ─────────────────────────────────────────────────────
    with col2:
        if "thread_migrations" in runs.columns:
            tmig = runs.groupby("workflow_type").thread_migrations.mean().reset_index()
            fig3 = go.Figure(go.Bar(
                x=tmig.workflow_type, y=tmig.thread_migrations,
                marker_color=["#22c55e" if w == "linear" else "#a78bfa"
                              for w in tmig.workflow_type]))
            fig3.update_layout(**PL, height=220,
                               title="Mean thread migrations — agentic overhead indicator")
            st.plotly_chart(fig3, use_container_width=True)

    # ── Context switches ──────────────────────────────────────────────────────
    st.markdown("#### 🔄 Scheduler Metrics")
    sched_cols = ["context_switches_voluntary", "context_switches_involuntary",
                  "thread_migrations", "interrupt_rate"]
    sched_avail = [c for c in sched_cols if c in runs.columns]
    if sched_avail:
        sched_agg = runs.groupby("workflow_type")[sched_avail].mean().reset_index()
        fig4 = go.Figure()
        for col in sched_avail:
            fig4.add_trace(go.Bar(name=col.replace("_", " ").title(),
                                  x=sched_agg.workflow_type, y=sched_agg[col]))
        fig4.update_layout(**PL, barmode="group", height=240,
                           title="Scheduler metrics — linear vs agentic")
        st.plotly_chart(fig4, use_container_width=True)

    # ── Ring bus frequency ────────────────────────────────────────────────────
    if "ring_bus_freq_mhz" in runs.columns:
        rbf = runs.groupby("workflow_type").ring_bus_freq_mhz.mean().reset_index()
        st.metric("Ring Bus Freq — Linear",
                  f"{rbf[rbf.workflow_type=='linear'].ring_bus_freq_mhz.values[0]:.0f} MHz"
                  if not rbf[rbf.workflow_type=='linear'].empty else "—")

    # ── Kernel vs user time ───────────────────────────────────────────────────
    if "kernel_time_ms" in runs.columns and "user_time_ms" in runs.columns:
        kt = runs.groupby("workflow_type")[["kernel_time_ms", "user_time_ms"]].mean().reset_index()
        fig5 = go.Figure()
        fig5.add_trace(go.Bar(name="Kernel", x=kt.workflow_type,
                               y=kt.kernel_time_ms, marker_color="#3b82f6"))
        fig5.add_trace(go.Bar(name="User",   x=kt.workflow_type,
                               y=kt.user_time_ms,   marker_color="#22c55e"))
        fig5.update_layout(**PL, barmode="stack", height=220,
                           title="Kernel vs User time (ms)")
        st.plotly_chart(fig5, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# SUB-TAB 5 — PER-PAIR DETAIL
# ══════════════════════════════════════════════════════════════════════════════

def _tab_per_pair(tax: pd.DataFrame):
    """Expandable per-pair cards with full metrics and research narrative."""

    if tax.empty:
        st.info("No pair data available for this session.")
        return

    st.markdown(f"**{len(tax)} run pairs in this session**")

    for i, row in tax.iterrows():
        task     = row.get("task_name", "?")
        provider = row.get("provider",  "?")
        run_num  = int(row.get("run_number", i+1))
        tax_x    = float(row.get("tax_multiplier", 0))
        emoji, label, clr = _tax_verdict(tax_x)

        header = (f"Pair {i+1} · {task} · {provider} · "
                  f"Rep {run_num} · {emoji} {tax_x:.2f}× ({label})")

        with st.expander(header, expanded=(i == 0)):
            # Side-by-side metrics
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(
                    "<div style='font-size:10px;font-weight:700;color:#22c55e;"
                    "text-transform:uppercase;letter-spacing:.08em;'>"
                    "LINEAR</div>", unsafe_allow_html=True)
                _metric_block({
                    "Total Energy":    f"{row.get('linear_energy_j', 0):.4f} J",
                    "Duration":        f"{row.get('linear_ms', 0):.0f} ms",
                    "IPC":             f"{row.get('linear_ipc', 0):.3f}",
                    "Cache Miss":      f"{float(row.get('linear_cmr', 0) or 0)*100:.1f}%",
                    "Peak Temp":       f"{row.get('linear_max_temp', 0):.1f}°C",
                    "Thermal Rise":    f"+{row.get('linear_tdelta', 0):.1f}°C",
                    "Thread Mig":      f"{int(row.get('linear_tmig', 0) or 0):,}",
                }, "#22c55e")

            with c2:
                st.markdown(
                    "<div style='font-size:10px;font-weight:700;color:#ef4444;"
                    "text-transform:uppercase;letter-spacing:.08em;'>"
                    "AGENTIC</div>", unsafe_allow_html=True)
                # Compute deltas vs linear for context
                l_e = float(row.get("linear_energy_j", 1) or 1)
                a_e = float(row.get("agentic_energy_j", 0) or 0)
                _metric_block({
                    "Total Energy":    f"{a_e:.4f} J  (+{a_e/l_e:.1f}×)",
                    "Duration":        f"{row.get('agentic_ms', 0):.0f} ms",
                    "IPC":             f"{row.get('agentic_ipc', 0):.3f}",
                    "Cache Miss":      f"{float(row.get('agentic_cmr', 0) or 0)*100:.1f}%",
                    "Peak Temp":       f"{row.get('agentic_max_temp', 0):.1f}°C",
                    "Thermal Rise":    f"+{row.get('agentic_tdelta', 0):.1f}°C",
                    "Thread Mig":      f"{int(row.get('agentic_tmig', 0) or 0):,}",
                    "LLM Calls":       f"{int(row.get('llm_calls',  0) or 0)}",
                    "Tool Calls":      f"{int(row.get('tool_calls', 0) or 0)}",
                    "Steps":           f"{int(row.get('steps',      0) or 0)}",
                    "Plan / Exec / Synth": (
                        f"{row.get('planning_time_ms',0):.0f}ms / "
                        f"{row.get('execution_time_ms',0):.0f}ms / "
                        f"{row.get('synthesis_time_ms',0):.0f}ms"),
                }, "#ef4444")

            # Research narrative
            narrative = _build_pair_narrative(row)
            st.markdown(
                "<div style='background:#07090f;border:1px solid #1e2d45;"
                "border-left:3px solid #3b82f6;border-radius:5px;"
                "padding:10px 14px;margin-top:8px;'>"
                "<div style='font-size:9px;font-weight:700;color:#3b82f6;"
                "text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px;'>"
                "📝 Research Narrative</div>"
                f"<div style='font-size:10px;color:#c8d8e8;line-height:1.7;'>"
                f"{narrative}</div></div>",
                unsafe_allow_html=True,
            )


def _metric_block(metrics: dict, color: str):
    """Render a compact metrics table inside a per-pair card."""
    rows = "".join(
        f"<tr>"
        f"<td style='padding:3px 8px;font-size:9px;color:#3d5570;'>{k}</td>"
        f"<td style='padding:3px 8px;font-size:10px;font-family:monospace;"
        f"color:{color};'>{v}</td>"
        f"</tr>"
        for k, v in metrics.items()
    )
    st.markdown(
        f"<table style='width:100%;border-collapse:collapse;"
        f"background:#07090f;border-radius:5px;overflow:hidden;'>"
        f"<tbody>{rows}</tbody></table>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# SUB-TAB 6 — EXPORT
# ══════════════════════════════════════════════════════════════════════════════

def _tab_export(group_id: str, exps: pd.DataFrame, runs: pd.DataFrame,
                tax: pd.DataFrame):
    """Excel and PDF export of the full session analysis."""

    st.markdown("#### 💾 Export Session Report")

    col1, col2 = st.columns(2)

    # ── Excel export ──────────────────────────────────────────────────────────
    with col1:
        st.markdown("**📊 Excel Report**")
        st.caption("One sheet per analysis section + raw data")

        if st.button("📥 Generate Excel", use_container_width=True, key="gen_excel"):
            try:
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                    # Sheet 1: Experiments
                    exps.to_excel(writer, sheet_name="Experiments", index=False)

                    # Sheet 2: Master summary
                    if not tax.empty:
                        summary = (tax.groupby(["task_name", "provider"])
                                   .agg(linear_j=("linear_energy_j", "mean"),
                                        agentic_j=("agentic_energy_j", "mean"),
                                        tax_x=("tax_multiplier", "mean"),
                                        n_pairs=("comparison_id", "count"))
                                   .reset_index())
                        summary["verdict"] = summary.tax_x.apply(
                            lambda x: _tax_verdict(x)[1])
                        summary.to_excel(writer, sheet_name="Summary", index=False)

                    # Sheet 3: Energy breakdown
                    energy_cols = ["run_id", "workflow_type", "task_name", "provider",
                                   "energy_j", "dynamic_energy_j", "pkg_energy_j",
                                   "core_energy_j", "uncore_energy_j", "dram_energy_j",
                                   "energy_per_token", "energy_per_instruction"]
                    e_avail = [c for c in energy_cols if c in runs.columns]
                    runs[e_avail].to_excel(writer, sheet_name="Energy", index=False)

                    # Sheet 4: Thermal
                    thermal_cols = ["run_id", "workflow_type", "task_name", "provider",
                                    "start_temp_c", "max_temp_c", "min_temp_c",
                                    "thermal_delta_c", "thermal_throttle_flag",
                                    "c2_time_seconds", "c3_time_seconds",
                                    "c6_time_seconds", "c7_time_seconds"]
                    t_avail = [c for c in thermal_cols if c in runs.columns]
                    runs[t_avail].to_excel(writer, sheet_name="Thermal", index=False)

                    # Sheet 5: CPU & Scheduler
                    cpu_cols = ["run_id", "workflow_type", "task_name", "provider",
                                "ipc", "cache_miss_rate", "thread_migrations",
                                "context_switches_voluntary", "context_switches_involuntary",
                                "interrupt_rate", "ring_bus_freq_mhz", "wakeup_latency_us",
                                "kernel_time_ms", "user_time_ms"]
                    c_avail = [c for c in cpu_cols if c in runs.columns]
                    runs[c_avail].to_excel(writer, sheet_name="CPU_Scheduler", index=False)

                    # Sheet 6: Sustainability
                    sust_cols = ["run_id", "workflow_type", "task_name", "provider",
                                 "energy_j", "carbon_g", "water_ml", "methane_mg"]
                    s_avail = [c for c in sust_cols if c in runs.columns]
                    runs[s_avail].to_excel(writer, sheet_name="Sustainability", index=False)

                    # Sheet 7: Per-pair raw
                    if not tax.empty:
                        tax.to_excel(writer, sheet_name="Per_Pair", index=False)

                    # Sheet 8: All runs raw
                    runs.to_excel(writer, sheet_name="Raw_Runs", index=False)

                buf.seek(0)
                st.download_button(
                    "⬇️ Download Excel",
                    data=buf.getvalue(),
                    file_name=f"alems_{group_id}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_excel",
                )
                st.success("Excel ready — 8 sheets generated")
            except Exception as e:
                st.error(f"Excel generation failed: {e}")
                st.caption("Make sure openpyxl is installed: pip install openpyxl")

    # ── PDF export ────────────────────────────────────────────────────────────
    with col2:
        st.markdown("**📄 PDF Academic Report**")
        st.caption("Formatted report suitable for supervisors/reviewers")

        if st.button("📥 Generate PDF", use_container_width=True, key="gen_pdf"):
            try:
                pdf_bytes = _generate_pdf(group_id, exps, runs, tax)
                st.download_button(
                    "⬇️ Download PDF",
                    data=pdf_bytes,
                    file_name=f"alems_{group_id}.pdf",
                    mime="application/pdf",
                    key="dl_pdf",
                )
                st.success("PDF ready")
            except ImportError:
                st.warning("ReportLab not installed. Install with: pip install reportlab")
            except Exception as e:
                st.error(f"PDF generation failed: {e}")

    # ── JSON export ───────────────────────────────────────────────────────────
    st.markdown("**📋 Raw JSON**")
    if st.button("📥 Export JSON", use_container_width=True, key="gen_json"):
        import json
        payload = {
            "group_id":    group_id,
            "experiments": exps.to_dict(orient="records"),
            "runs":        runs.to_dict(orient="records"),
            "tax_summary": tax.to_dict(orient="records"),
        }
        st.download_button(
            "⬇️ Download JSON",
            data=json.dumps(payload, indent=2, default=str),
            file_name=f"alems_{group_id}.json",
            mime="application/json",
            key="dl_json",
        )


def _generate_pdf(group_id: str, exps: pd.DataFrame,
                  runs: pd.DataFrame, tax: pd.DataFrame) -> bytes:
    """
    Generate an academic-style PDF report using ReportLab.
    Structured as a research document with sections, tables, and narrative.
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles    import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units     import cm
    from reportlab.lib            import colors
    from reportlab.platypus      import (SimpleDocTemplate, Paragraph, Spacer,
                                          Table, TableStyle, HRFlowable)
    from reportlab.lib.enums     import TA_CENTER

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                             leftMargin=2*cm, rightMargin=2*cm,
                             topMargin=2.5*cm, bottomMargin=2*cm)

    styles = getSampleStyleSheet()
    # Custom styles
    title_style = ParagraphStyle("title", parent=styles["Title"],
                                  fontSize=18, spaceAfter=6, textColor=colors.HexColor("#1e3a5f"))
    h1_style    = ParagraphStyle("h1", parent=styles["Heading1"],
                                  fontSize=13, spaceAfter=4, textColor=colors.HexColor("#1e3a5f"))
    h2_style    = ParagraphStyle("h2", parent=styles["Heading2"],
                                  fontSize=11, spaceAfter=3, textColor=colors.HexColor("#2d5a8e"))
    body_style  = ParagraphStyle("body", parent=styles["Normal"],
                                  fontSize=9, leading=14, spaceAfter=6)
    mono_style  = ParagraphStyle("mono", parent=styles["Code"],
                                  fontSize=8, leading=12)

    story = []
    now   = datetime.now().strftime("%B %d, %Y")

    # ── Title page ────────────────────────────────────────────────────────────
    story.append(Paragraph("A-LEMS Experimental Report", title_style))
    story.append(Paragraph("Agentic LLM Energy Measurement System", styles["Normal"]))
    story.append(Spacer(1, 0.3*cm))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#1e3a5f")))
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph(f"<b>Session:</b> {group_id}", body_style))
    story.append(Paragraph(f"<b>Date:</b> {now}", body_style))
    story.append(Paragraph(f"<b>Experiments:</b> {len(exps)}  ·  "
                            f"<b>Runs:</b> {len(runs)}", body_style))
    story.append(Spacer(1, 0.5*cm))

    # ── 1. Executive Summary ──────────────────────────────────────────────────
    story.append(Paragraph("1. Executive Summary", h1_style))
    if not tax.empty:
        max_tax = float(tax.tax_multiplier.max())
        min_tax = float(tax.tax_multiplier.min())
        avg_tax = float(tax.tax_multiplier.mean())
        story.append(Paragraph(
            f"This session measured the orchestration overhead of agentic versus "
            f"linear LLM execution across {len(exps)} experiments. "
            f"Orchestration tax ranged from <b>{min_tax:.2f}×</b> to "
            f"<b>{max_tax:.2f}×</b> (mean: <b>{avg_tax:.2f}×</b>). "
            f"All energy measurements use hardware RAPL counters for accuracy.",
            body_style))
    story.append(Spacer(1, 0.3*cm))

    # ── 2. Energy Results ─────────────────────────────────────────────────────
    story.append(Paragraph("2. Energy Results", h1_style))
    if not tax.empty:
        summary = (tax.groupby(["task_name", "provider"])
                   .agg(linear_j =("linear_energy_j",  "mean"),
                        agentic_j=("agentic_energy_j", "mean"),
                        tax_x    =("tax_multiplier",   "mean"))
                   .reset_index())
        tbl_data = [["Task", "Provider", "Linear (J)", "Agentic (J)", "Tax", "Verdict"]]
        for _, r in summary.iterrows():
            _, verdict, _ = _tax_verdict(float(r.tax_x))
            tbl_data.append([r.task_name[:30], r.provider,
                             f"{r.linear_j:.4f}", f"{r.agentic_j:.4f}",
                             f"{r.tax_x:.2f}×", verdict])
        tbl = Table(tbl_data, colWidths=[4.5*cm, 2.5*cm, 2.5*cm, 2.5*cm, 1.8*cm, 2.5*cm])
        tbl.setStyle(TableStyle([
            ("BACKGROUND",  (0,0), (-1,0),  colors.HexColor("#1e3a5f")),
            ("TEXTCOLOR",   (0,0), (-1,0),  colors.white),
            ("FONTSIZE",    (0,0), (-1,-1), 8),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f0f4f8")]),
            ("GRID",        (0,0), (-1,-1), 0.3, colors.HexColor("#cccccc")),
            ("TOPPADDING",  (0,0), (-1,-1), 4),
            ("BOTTOMPADDING",(0,0),(-1,-1), 4),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 0.3*cm))

    # ── 3. Sustainability ─────────────────────────────────────────────────────
    story.append(Paragraph("3. Sustainability Impact", h1_style))
    total_j = float(runs.energy_j.sum()) if "energy_j" in runs else 0
    sf = _human_energy_full(total_j)
    if sf:
        total_c = float(runs.carbon_g.sum())  if "carbon_g"  in runs else sf["carbon_g"]
        total_w = float(runs.water_ml.sum())  if "water_ml"  in runs else sf["water_ml"]
        total_m = float(runs.methane_mg.sum()) if "methane_mg" in runs else sf["methane_mg"]
        story.append(Paragraph(
            f"Total energy consumed: <b>{total_j:.2f} J</b> ({sf['wh']:.4f} Wh). "
            f"Carbon footprint: <b>{total_c:.5f} g CO₂</b> "
            f"({sf['carbon_car_m']:.2f}mm of petrol car driving). "
            f"Water consumption: <b>{total_w:.4f} ml</b>. "
            f"Methane equivalent: <b>{total_m:.6f} mg CH₄</b> "
            f"({sf['methane_human_pct']:.6f}% of average daily human emission).",
            body_style))
    story.append(Spacer(1, 0.3*cm))

    # ── 4. Per-Pair Details ───────────────────────────────────────────────────
    story.append(Paragraph("4. Per-Pair Analysis", h1_style))
    for i, row in tax.head(10).iterrows():   # cap at 10 pairs for PDF length
        story.append(Paragraph(
            f"Pair {i+1}: {row.get('task_name','')} / {row.get('provider','')} "
            f"— Tax: {float(row.get('tax_multiplier',0)):.2f}×",
            h2_style))
        narrative = _build_pair_narrative(row)
        story.append(Paragraph(narrative, body_style))

    # ── Footer note ───────────────────────────────────────────────────────────
    story.append(Spacer(1, 0.5*cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#cccccc")))
    story.append(Paragraph(
        f"Generated by A-LEMS · {now} · "
        f"Sustainability factors: config/insights_rules.yaml",
        ParagraphStyle("footer", parent=styles["Normal"],
                       fontSize=7, textColor=colors.HexColor("#999999"))))

    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT — called from execute.py Tab 3
# ══════════════════════════════════════════════════════════════════════════════

def render_session_analysis(group_id: str):
    """
    Main entry point. Called from execute.py with a group_id string.
    Loads all data, renders header, then dispatches to 6 sub-tabs.
    """
    if not group_id:
        st.info("No session selected. Run an experiment first, or select a session from the sidebar.")
        return

    # Load all data for this session
    exps = _load_session_experiments(group_id)
    runs = _load_session_runs(group_id)
    tax  = _load_tax_for_session(group_id)

    if exps.empty:
        st.warning(f"No experiments found for session: {group_id}")
        return

    # Session header banner
    _session_header(group_id, exps, runs)

    # Six sub-tabs
    s1, s2, s3, s4, s5, s6 = st.tabs([
        "🏆 Summary",
        "⚡ Energy",
        "🌡️ Thermal",
        "🧠 CPU",
        "📋 Per-Pair",
        "💾 Export",
    ])

    with s1: _tab_summary(exps, runs, tax)
    with s2: _tab_energy(runs, tax)
    with s3: _tab_thermal(runs)
    with s4: _tab_cpu(runs)
    with s5: _tab_per_pair(tax)
    with s6: _tab_export(group_id, exps, runs, tax)
