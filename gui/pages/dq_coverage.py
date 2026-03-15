"""
gui/pages/dq_coverage.py  —  ◫  Sensor Coverage
─────────────────────────────────────────────────────────────────────────────
NULL-count heatmap across all 80+ columns in the runs table.
Groups columns by sensor category so researchers can see at a glance
which sensors are reliable and which drop out under what conditions.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.db     import q, q1
from gui.config import PL

# ── Column groups — maps display category → list of DB column names ───────────
COLUMN_GROUPS = {
    "Energy (RAPL)": [
        "total_energy_uj", "dynamic_energy_uj", "baseline_energy_uj",
        "avg_power_watts", "pkg_energy_uj", "core_energy_uj",
        "uncore_energy_uj", "dram_energy_uj",
    ],
    "Timing": [
        "start_time_ns", "end_time_ns", "duration_ns",
        "kernel_time_ms", "user_time_ms",
    ],
    "CPU Performance": [
        "instructions", "cycles", "ipc", "cache_misses",
        "cache_references", "cache_miss_rate", "frequency_mhz",
        "ring_bus_freq_mhz", "cpu_busy_mhz", "cpu_avg_mhz",
    ],
    "Scheduler": [
        "context_switches_voluntary", "context_switches_involuntary",
        "total_context_switches", "thread_migrations",
        "run_queue_length", "page_faults", "major_page_faults",
        "minor_page_faults",
    ],
    "Thermal": [
        "package_temp_celsius", "baseline_temp_celsius",
        "start_temp_c", "max_temp_c", "min_temp_c",
        "thermal_delta_c", "thermal_throttle_flag",
    ],
    "C-States": [
        "c2_time_seconds", "c3_time_seconds",
        "c6_time_seconds", "c7_time_seconds",
    ],
    "Memory": [
        "rss_memory_mb", "vms_memory_mb",
        "swap_total_mb", "swap_end_used_mb", "swap_end_percent",
    ],
    "Network / Latency": [
        "dns_latency_ms", "api_latency_ms", "compute_time_ms",
    ],
    "Tokens": [
        "total_tokens", "prompt_tokens", "completion_tokens",
        "energy_per_token", "instructions_per_token",
    ],
    "Agentic": [
        "planning_time_ms", "execution_time_ms", "synthesis_time_ms",
        "llm_calls", "tool_calls", "tools_used", "steps",
        "complexity_score",
    ],
    "Sustainability": [
        "carbon_g", "water_ml", "methane_mg",
    ],
    "Interrupts / MSR": [
        "interrupt_rate", "wakeup_latency_us",
        "thermal_throttle_flag", "interrupts_per_second",
    ],
}


def render(ctx: dict) -> None:
    accent = "#f472b6"

    total_runs = q1("SELECT COUNT(*) AS n FROM runs").get("n", 0) or 0

    if total_runs == 0:
        st.info("No runs in database yet.")
        return

    # ── Build NULL counts via one query per group ─────────────────────────────
    # Flatten all columns, build one big SELECT
    all_cols = [c for cols in COLUMN_GROUPS.values() for c in cols]
    # deduplicate preserving order
    seen = set()
    unique_cols = []
    for c in all_cols:
        if c not in seen:
            seen.add(c)
            unique_cols.append(c)

    null_select = ",\n".join(
        f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS null_{c}"
        for c in unique_cols
    )
    null_row = q1(f"SELECT {null_select} FROM runs") or {}

    # Build coverage dict: col → coverage_pct
    coverage = {}
    for c in unique_cols:
        null_count = int(null_row.get(f"null_{c}", 0) or 0)
        coverage[c] = round((1 - null_count / total_runs) * 100, 1)

    # ── Header ────────────────────────────────────────────────────────────────
    overall_coverage = round(sum(coverage.values()) / len(coverage), 1)
    health_clr = "#22c55e" if overall_coverage >= 90 else \
                 "#f59e0b" if overall_coverage >= 70 else "#ef4444"

    st.markdown(
        f"<div style='padding:16px 20px;"
        f"background:linear-gradient(135deg,{accent}12,{accent}06);"
        f"border:1px solid {accent}33;border-radius:12px;margin-bottom:20px;"
        f"display:flex;align-items:center;gap:20px;'>"
        f"<div><div style='font-size:32px;font-weight:800;color:{health_clr};"
        f"font-family:IBM Plex Mono,monospace;line-height:1;'>{overall_coverage}%</div>"
        f"<div style='font-size:10px;color:#94a3b8;text-transform:uppercase;"
        f"letter-spacing:.1em;margin-top:2px;'>Overall coverage</div></div>"
        f"<div style='font-size:13px;color:#94a3b8;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"{len(unique_cols)} columns · {total_runs} runs · "
        f"{sum(1 for v in coverage.values() if v == 100)} fully populated</div>"
        f"</div>",
        unsafe_allow_html=True)

    # ── Coverage heatmap per group ────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;'>"
        f"Coverage by sensor group</div>",
        unsafe_allow_html=True)

    for group_name, cols in COLUMN_GROUPS.items():
        # Filter to cols that exist in coverage dict
        valid_cols = [c for c in cols if c in coverage]
        if not valid_cols:
            continue

        group_avg = round(sum(coverage[c] for c in valid_cols) / len(valid_cols), 1)
        group_clr = "#22c55e" if group_avg >= 90 else \
                    "#f59e0b" if group_avg >= 70 else "#ef4444"

        with st.expander(f"{group_name}  —  {group_avg}% avg coverage", expanded=group_avg < 90):
            rows = []
            for c in valid_cols:
                pct = coverage[c]
                null_n = int((1 - pct / 100) * total_runs)
                rows.append({
                    "Column": c,
                    "Coverage %": pct,
                    "NULL count": null_n,
                    "Status": "✓ Full" if pct == 100
                              else "⚠ Partial" if pct >= 50
                              else "✗ Sparse",
                })
            df = pd.DataFrame(rows).sort_values("Coverage %", ascending=True)

            # Mini bar chart for this group
            fig = go.Figure(go.Bar(
                x=df["Coverage %"],
                y=df["Column"],
                orientation="h",
                marker_color=[
                    "#22c55e" if v == 100 else
                    "#f59e0b" if v >= 50 else
                    "#ef4444"
                    for v in df["Coverage %"]
                ],
                marker_line_width=0,
                text=[f"{v}%" for v in df["Coverage %"]],
                textposition="outside",
                textfont=dict(size=9),
            ))
            fig.update_layout(
                **{**PL, "margin": dict(l=160, r=60, t=10, b=30)},
                height=max(180, len(valid_cols) * 26),
                xaxis_title="Coverage %", xaxis_range=[0, 115],
                
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True,
                            key=f"dq_cov_{group_name}")

    # ── Worst columns table ───────────────────────────────────────────────────
    sparse = {c: v for c, v in coverage.items() if v < 50}
    if sparse:
        st.markdown(
            f"<div style='margin-top:16px;padding:10px 14px;"
            f"background:#2a0c0c;border-left:3px solid #ef4444;"
            f"border-radius:0 8px 8px 0;font-size:11px;"
            f"color:#fca5a5;font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
            f"<b>⚠ {len(sparse)} columns below 50% coverage</b> — "
            f"analyses using these columns may be unreliable.<br>"
            + ", ".join(f"<code>{c}</code>" for c in sorted(sparse.keys()))
            + "</div>",
            unsafe_allow_html=True)
