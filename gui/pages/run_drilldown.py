"""
gui/pages/run_drilldown.py  —  🔍  Run Drilldown
─────────────────────────────────────────────────────────────────────────────
All sensor streams for a single run — the deepest view in A-LEMS.

Data sources used:
  energy_samples    — 537,228 rows — RAPL power over time
  cpu_samples       — 113,981 rows — IPC, freq, util, C-states per sample
  thermal_samples   — 24,729 rows  — all thermal zones, throttle events
  interrupt_samples — available    — interrupt rate time series
  llm_interactions  — populated    — per-step prompt/response/timing

Tab 1: ⚡ Energy stream    — RAPL power timeline, domain breakdown, anomalies
Tab 2: ▣ CPU stream       — IPC, frequency, utilisation, C-state residency
Tab 3: 🌡 Thermal stream   — all zones, wifi_temp, throttle events
Tab 4: ∿ Interrupt stream  — interrupt rate, wakeup latency
Tab 5: 💬 LLM interactions — per-step timing, tokens, response quality
─────────────────────────────────────────────────────────────────────────────
"""

import json

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1
# Strip keys from PL that callers override to avoid "multiple values" TypeError
def _pl(**overrides):
    """PL dict with caller overrides taking precedence."""
    return {k: v for k, v in PL.items()
            if k not in overrides} | overrides



ACCENT = "#3b82f6"

# Max samples to load per stream — keeps UI fast even with 537k rows
MAX_ENERGY_SAMPLES  = 2000
MAX_CPU_SAMPLES     = 2000
MAX_THERMAL_SAMPLES = 1000
MAX_IRQ_SAMPLES     = 1000


def render(ctx: dict) -> None:
    runs = ctx.get("runs", pd.DataFrame())

    # ── Run selector ──────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:12px 16px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:16px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;'>Run Drilldown — all sensor streams</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Build run list with labels
    run_list = q("""
        SELECT r.run_id, r.workflow_type,
               e.task_name, e.model_name, e.provider,
               r.total_energy_uj / 1e6 AS energy_j,
               r.duration_ns / 1e9     AS duration_s
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        ORDER BY r.run_id DESC
        LIMIT 200
    """)

    if run_list.empty:
        st.info("No runs yet.")
        return

    def _run_label(row):
        return (
            f"Run {row['run_id']} · {row['workflow_type']} · "
            f"{row['task_name']} · {row['provider']} · "
            f"{row['energy_j']:.4f}J · {row['duration_s']:.1f}s"
        )

    sc1, sc2 = st.columns([3, 1])
    with sc1:
        sel_run_id = st.selectbox(
            "Select run",
            run_list["run_id"].tolist(),
            format_func=lambda rid: _run_label(
                run_list[run_list["run_id"] == rid].iloc[0]
            ),
            key="drill_run_sel",
        )
    with sc2:
        # Quick jump by run_id
        jump_id = st.number_input(
            "Jump to run_id", min_value=1, value=int(sel_run_id),
            step=1, key="drill_jump",
        )
        if jump_id != sel_run_id and jump_id in run_list["run_id"].values:
            sel_run_id = jump_id

    # ── Load run metadata ─────────────────────────────────────────────────────
    run_meta = q1(f"""
        SELECT r.run_id, r.workflow_type,
               r.total_energy_uj/1e6   AS energy_j,
               r.dynamic_energy_uj/1e6 AS dynamic_j,
               r.duration_ns/1e9       AS duration_s,
               r.avg_power_watts,
               r.ipc, r.cache_miss_rate, r.frequency_mhz,
               r.package_temp_celsius, r.thermal_delta_c,
               r.c6_time_seconds, r.c7_time_seconds,
               r.total_tokens, r.llm_calls, r.tool_calls,
               r.planning_time_ms, r.execution_time_ms, r.synthesis_time_ms,
               r.baseline_id,
               e.task_name, e.model_name, e.provider, e.group_id
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        WHERE r.run_id = {int(sel_run_id)}
    """) or {}

    if not run_meta:
        st.warning(f"Run {sel_run_id} not found.")
        return

    wf      = str(run_meta.get("workflow_type", "?"))
    wf_clr  = WF_COLORS.get(wf, "#94a3b8")
    energy  = float(run_meta.get("energy_j", 0) or 0)
    dur     = float(run_meta.get("duration_s", 0) or 0)
    task    = str(run_meta.get("task_name", "?"))
    model   = str(run_meta.get("model_name", "?"))
    prov    = str(run_meta.get("provider", "?"))

    # ── Run summary banner ────────────────────────────────────────────────────
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    for col, val, label, clr in [
        (m1, f"Run {sel_run_id}", "ID",           ACCENT),
        (m2, wf.capitalize(),    "Workflow",       wf_clr),
        (m3, f"{energy:.4f} J",  "Total energy",  "#f59e0b"),
        (m4, f"{dur:.1f}s",      "Duration",      "#a78bfa"),
        (m5, task[:16],          "Task",          "#94a3b8"),
        (m6, prov,               "Provider",      "#22c55e"),
    ]:
        with col:
            st.markdown(
                f"<div style='padding:8px 10px;background:#111827;"
                f"border:1px solid {clr}33;border-left:2px solid {clr};"
                f"border-radius:6px;margin-bottom:12px;'>"
                f"<div style='font-size:13px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;line-height:1.2;'>{val}</div>"
                f"<div style='font-size:8px;color:#475569;text-transform:uppercase;"
                f"letter-spacing:.08em;margin-top:2px;'>{label}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "⚡  Energy stream",
        "▣  CPU stream",
        "🌡  Thermal stream",
        "∿  Interrupt stream",
        "💬  LLM interactions",
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — ENERGY STREAM
    # ══════════════════════════════════════════════════════════════════════════
    with tab1:
        es = q(f"""
            SELECT time_s,
                   pkg_power_watts, core_power_watts,
                   uncore_power_watts, dram_power_watts,
                   pkg_energy_uj
            FROM energy_samples_with_power
            WHERE run_id = {int(sel_run_id)}
            ORDER BY time_s
            LIMIT {MAX_ENERGY_SAMPLES}
        """)

        if es.empty:
            st.info("No energy_samples for this run.")
        else:
            es["time_s"] -= es["time_s"].min()
            n_samples = len(es)

            # Summary
            e1, e2, e3 = st.columns(3)
            with e1:
                avg_pkg = es["pkg_power_watts"].mean() if "pkg_power_watts" in es.columns else 0
                peak_pkg = es["pkg_power_watts"].max() if "pkg_power_watts" in es.columns else 0
                st.metric("Avg pkg power", f"{avg_pkg:.2f}W", f"Peak {peak_pkg:.2f}W")
            with e2:
                st.metric("Samples loaded", f"{n_samples:,}",
                          f"of up to {MAX_ENERGY_SAMPLES:,}")
            with e3:
                avg_core = es["core_power_watts"].mean() if "core_power_watts" in es.columns else 0
                st.metric("Avg core power", f"{avg_core:.2f}W")

            # Power over time
            fig_e = go.Figure()
            for col_n, label, clr in [
                ("pkg_power_watts",    "Package",  "#f59e0b"),
                ("core_power_watts",   "Core",     "#ef4444"),
                ("uncore_power_watts", "Uncore",   "#3b82f6"),
                ("dram_power_watts",   "DRAM",     "#a78bfa"),
            ]:
                if col_n not in es.columns: continue
                sub = es[es[col_n].notna() & (es[col_n] > 0)]
                if sub.empty: continue
                fig_e.add_trace(go.Scatter(
                    x=sub["time_s"], y=sub[col_n],
                    mode="lines", name=label,
                    line=dict(width=1.5, color=clr),
                ))
            fig_e.update_layout(
                **{k:v for k,v in PL.items() if k not in ("yaxis","xaxis","margin")}, height=300,
                xaxis_title="Time (s)", yaxis_title="Power (W)",
                title=dict(text="RAPL power over time", font=dict(size=11)),
            )
            st.plotly_chart(fig_e, use_container_width=True, key="drill_energy_ts")

            # Cumulative energy
            if "pkg_energy_uj" in es.columns:
                es["cumulative_j"] = es["pkg_energy_uj"].cumsum() / 1e6
                fig_cum = go.Figure(go.Scatter(
                    x=es["time_s"], y=es["cumulative_j"],
                    mode="lines", name="Cumulative energy",
                    line=dict(color="#f59e0b", width=2),
                    fill="tozeroy",
                    fillcolor="rgba(245,158,11,0.07)",
                ))
                fig_cum.update_layout(
                    **{k:v for k,v in PL.items() if k not in ("yaxis","xaxis","margin")}, height=200,
                    xaxis_title="Time (s)", yaxis_title="Cumulative energy (J)",
                    title=dict(text="Cumulative energy", font=dict(size=11)),
                )
                st.plotly_chart(fig_cum, use_container_width=True, key="drill_energy_cum")

            # Power anomaly detection — samples > 2σ above mean
            if "pkg_power_watts" in es.columns:
                mu  = es["pkg_power_watts"].mean()
                sig = es["pkg_power_watts"].std()
                anomalies = es[es["pkg_power_watts"] > mu + 2 * sig]
                if not anomalies.empty:
                    st.markdown(
                        f"<div style='padding:8px 14px;background:#1a0c00;"
                        f"border-left:3px solid #f97316;border-radius:0 8px 8px 0;"
                        f"font-size:11px;color:#fed7aa;"
                        f"font-family:IBM Plex Mono,monospace;'>"
                        f"⚠ {len(anomalies)} power spikes detected (>{mu+2*sig:.2f}W = mean+2σ) "
                        f"— peak {anomalies['pkg_power_watts'].max():.2f}W at "
                        f"t={anomalies['time_s'].iloc[0]:.2f}s</div>",
                        unsafe_allow_html=True,
                    )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — CPU STREAM
    # ══════════════════════════════════════════════════════════════════════════
    with tab2:
        cs = q(f"""
            SELECT cs.timestamp_ns/1e9 AS time_s,
                   cs.cpu_util_percent, cs.package_power,
                   cs.ipc,
                   cs.cpu_avg_mhz AS frequency_mhz,
                   cs.c1_residency, cs.c2_residency, cs.c3_residency,
                   cs.c6_residency, cs.c7_residency,
                   r.cache_miss_rate,
                   r.context_switches_voluntary + r.context_switches_involuntary AS context_switches,
                   r.thread_migrations AS migrations
            FROM cpu_samples cs
            JOIN runs r ON cs.run_id = r.run_id
            WHERE cs.run_id = {int(sel_run_id)}
            ORDER BY cs.timestamp_ns
            LIMIT {MAX_CPU_SAMPLES}
        """)

        if cs.empty:
            st.info("No cpu_samples for this run.")
        else:
            cs["time_s"] -= cs["time_s"].min()

            col1, col2 = st.columns(2)

            with col1:
                # CPU utilisation + IPC
                fig_cpu = go.Figure()
                if "cpu_util_percent" in cs.columns:
                    fig_cpu.add_trace(go.Scatter(
                        x=cs["time_s"], y=cs["cpu_util_percent"],
                        mode="lines", name="CPU util %",
                        line=dict(color="#3b82f6", width=1.5),
                    ))
                if "ipc" in cs.columns:
                    fig_cpu.add_trace(go.Scatter(
                        x=cs["time_s"], y=cs["ipc"],
                        mode="lines", name="IPC",
                        line=dict(color="#22c55e", width=1.5),
                        yaxis="y2",
                    ))
                fig_cpu.update_layout(
                    **{k:v for k,v in PL.items() if k not in ("yaxis","xaxis","margin")}, height=260,
                    xaxis_title="Time (s)",
                    yaxis=dict(title="CPU util %", color="#3b82f6"),
                    yaxis2=dict(title="IPC", overlaying="y", side="right",
                                color="#22c55e"),
                    title=dict(text="CPU utilisation & IPC", font=dict(size=11)),
                )
                st.plotly_chart(fig_cpu, use_container_width=True, key="drill_cpu_util")

            with col2:
                # Frequency over time
                if "frequency_mhz" in cs.columns:
                    fig_freq = go.Figure(go.Scatter(
                        x=cs["time_s"],
                        y=cs["frequency_mhz"],
                        mode="lines", name="Frequency",
                        line=dict(color="#f59e0b", width=1.5),
                        fill="tozeroy",
                        fillcolor="rgba(245,158,11,0.07)",
                    ))
                    fig_freq.update_layout(
                        **{k:v for k,v in PL.items() if k not in ("yaxis","xaxis","margin")}, height=260,
                        xaxis_title="Time (s)",
                        yaxis_title="Frequency (MHz)",
                        title=dict(text="CPU frequency", font=dict(size=11)),
                    )
                    st.plotly_chart(fig_freq, use_container_width=True, key="drill_freq")

            # C-state residency over time — stacked area
            cstate_cols = [c for c in ["c1_residency","c2_residency","c3_residency",
                                       "c6_residency","c7_residency"]
                           if c in cs.columns]
            if cstate_cols:
                fig_cs = go.Figure()
                cstate_colors = {
                    "c1_residency": "#38bdf8",
                    "c2_residency": "#3b82f6",
                    "c3_residency": "#a78bfa",
                    "c6_residency": "#22c55e",
                    "c7_residency": "#f59e0b",
                }
                for cc in cstate_cols:
                    sub = cs[cs[cc].notna()]
                    if sub.empty: continue
                    fig_cs.add_trace(go.Scatter(
                        x=sub["time_s"], y=sub[cc],
                        mode="lines",
                        name=cc.replace("_residency","").upper(),
                        line=dict(color=cstate_colors.get(cc,"#475569"), width=1),
                        stackgroup="cstates",
                    ))
                fig_cs.update_layout(
                    **{k:v for k,v in PL.items() if k not in ("yaxis","xaxis","margin")}, height=240,
                    xaxis_title="Time (s)",
                    yaxis_title="C-state residency %",
                    title=dict(text="C-state residency over time", font=dict(size=11)),
                )
                st.plotly_chart(fig_cs, use_container_width=True, key="drill_cstates")

            # Cache miss rate
            if "cache_miss_rate" in cs.columns:
                fig_cmr = go.Figure(go.Scatter(
                    x=cs["time_s"], y=cs["cache_miss_rate"],
                    mode="lines", name="Cache miss %",
                    line=dict(color="#ef4444", width=1.5),
                ))
                fig_cmr.update_layout(
                    **{k:v for k,v in PL.items() if k not in ("yaxis","xaxis","margin")}, height=200,
                    xaxis_title="Time (s)", yaxis_title="Cache miss rate",
                    title=dict(text="Cache miss rate over time", font=dict(size=11)),
                )
                st.plotly_chart(fig_cmr, use_container_width=True, key="drill_cmr")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — THERMAL STREAM
    # ══════════════════════════════════════════════════════════════════════════
    with tab3:
        ts = q(f"""
            SELECT timestamp_ns/1e9 AS time_s,
                   cpu_temp, system_temp, wifi_temp,
                   throttle_event, all_zones_json, sensor_count
            FROM thermal_samples
            WHERE run_id = {int(sel_run_id)}
            ORDER BY timestamp_ns
            LIMIT {MAX_THERMAL_SAMPLES}
        """)

        if ts.empty:
            st.info("No thermal_samples for this run.")
        else:
            ts["time_s"] -= ts["time_s"].min()

            # Parse JSON zones
            zone_data = {}
            for _, row in ts.iterrows():
                try:
                    zones = json.loads(row.get("all_zones_json") or "{}")
                    for z, v in zones.items():
                        if v is not None:
                            zone_data.setdefault(z, []).append(
                                (row["time_s"], float(v))
                            )
                except Exception:
                    pass

            # Core temp time series
            fig_th = go.Figure()
            for col_n, label, clr in [
                ("cpu_temp",    "CPU Package",  "#ef4444"),
                ("system_temp", "System",       "#3b82f6"),
                ("wifi_temp",   "WiFi",         "#22c55e"),
            ]:
                sub = ts[ts[col_n].notna() & (ts[col_n] > -50)]
                if sub.empty: continue
                fig_th.add_trace(go.Scatter(
                    x=sub["time_s"], y=sub[col_n],
                    mode="lines", name=label,
                    line=dict(color=clr, width=1.5),
                ))

            # Throttle events
            thr = ts[ts["throttle_event"] == 1]
            if not thr.empty:
                fig_th.add_trace(go.Scatter(
                    x=thr["time_s"],
                    y=thr["cpu_temp"].fillna(
                        ts["cpu_temp"].mean() if "cpu_temp" in ts.columns else 50
                    ),
                    mode="markers", name="Throttle",
                    marker=dict(color="#ef4444", size=8, symbol="x"),
                ))

            fig_th.update_layout(
                **{k:v for k,v in PL.items() if k not in ("yaxis","xaxis","margin")}, height=300,
                xaxis_title="Time (s)", yaxis_title="Temperature (°C)",
                title=dict(text="Thermal zones over time", font=dict(size=11)),
            )
            st.plotly_chart(fig_th, use_container_width=True, key="drill_thermal_ts")

            # All JSON zones
            if zone_data:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:#f59e0b;"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"All sensor zones — {len(zone_data)} detected</div>",
                    unsafe_allow_html=True,
                )
                fig_zones = go.Figure()
                colors = ["#ef4444","#f59e0b","#22c55e","#3b82f6","#a78bfa",
                          "#38bdf8","#f97316","#34d399","#60a5fa","#e879f9"]
                for i, (zone, points) in enumerate(zone_data.items()):
                    times = [p[0] for p in points]
                    temps = [p[1] for p in points]
                    fig_zones.add_trace(go.Scatter(
                        x=times, y=temps,
                        mode="lines", name=zone,
                        line=dict(color=colors[i % len(colors)], width=1),
                    ))
                fig_zones.update_layout(
                    **{k:v for k,v in PL.items() if k not in ("yaxis","xaxis","margin")}, height=280,
                    xaxis_title="Time (s)", yaxis_title="Temperature (°C)",
                    title=dict(text="All thermal zones", font=dict(size=11)),
                )
                st.plotly_chart(fig_zones, use_container_width=True, key="drill_all_zones")

            # Throttle summary
            n_throttle = len(thr)
            if n_throttle > 0:
                st.markdown(
                    f"<div style='padding:8px 14px;background:#2a0c0c;"
                    f"border-left:3px solid #ef4444;border-radius:0 8px 8px 0;"
                    f"font-size:11px;color:#fca5a5;"
                    f"font-family:IBM Plex Mono,monospace;'>"
                    f"⚠ {n_throttle} throttle events in this run. "
                    f"Note: if all samples show throttle_event=1, this may be a sensor "
                    f"state flag rather than real throttling — see known issue in handoff doc."
                    f"</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"<div style='padding:8px 14px;background:#052e1a;"
                    f"border-left:3px solid #22c55e;border-radius:0 8px 8px 0;"
                    f"font-size:11px;color:#4ade80;"
                    f"font-family:IBM Plex Mono,monospace;'>"
                    f"✓ No throttle events detected in this run.</div>",
                    unsafe_allow_html=True,
                )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 — INTERRUPT STREAM
    # ══════════════════════════════════════════════════════════════════════════
    with tab4:
        irqs = q(f"""
            SELECT timestamp_ns/1e9 AS time_s,
                   interrupts_per_sec, total_interrupts,
                   context_switches_per_sec
            FROM interrupt_samples
            WHERE run_id = {int(sel_run_id)}
            ORDER BY timestamp_ns
            LIMIT {MAX_IRQ_SAMPLES}
        """)

        if irqs.empty:
            st.info("No interrupt_samples for this run.")
        else:
            irqs["time_s"] -= irqs["time_s"].min()

            col1, col2 = st.columns(2)

            with col1:
                if "interrupts_per_sec" in irqs.columns:
                    fig_irq = go.Figure(go.Scatter(
                        x=irqs["time_s"],
                        y=irqs["interrupts_per_sec"],
                        mode="lines", name="IRQ/s",
                        line=dict(color="#f59e0b", width=1.5),
                        fill="tozeroy",
                        fillcolor="rgba(245,158,11,0.07)",
                    ))
                    fig_irq.update_layout(
                        **{k:v for k,v in PL.items() if k not in ("yaxis","xaxis","margin")}, height=240,
                        xaxis_title="Time (s)", yaxis_title="Interrupts/s",
                        title=dict(text="Interrupt rate", font=dict(size=11)),
                    )
                    st.plotly_chart(fig_irq, use_container_width=True, key="drill_irq")

            with col2:
                if "context_switches_per_sec" in irqs.columns:
                    fig_ctx = go.Figure(go.Scatter(
                        x=irqs["time_s"],
                        y=irqs["context_switches_per_sec"],
                        mode="lines", name="CTX/s",
                        line=dict(color="#a78bfa", width=1.5),
                        fill="tozeroy",
                        fillcolor="rgba(167,139,250,0.07)",
                    ))
                    fig_ctx.update_layout(
                        **{k:v for k,v in PL.items() if k not in ("yaxis","xaxis","margin")}, height=240,
                        xaxis_title="Time (s)", yaxis_title="Context switches/s",
                        title=dict(text="Context switch rate", font=dict(size=11)),
                    )
                    st.plotly_chart(fig_ctx, use_container_width=True, key="drill_ctx")

            # IRQ stats
            if "interrupts_per_sec" in irqs.columns:
                avg_irq = irqs["interrupts_per_sec"].mean()
                peak_irq = irqs["interrupts_per_sec"].max()
                total_irq = irqs["total_interrupts"].max() if "total_interrupts" in irqs.columns else 0

                st.markdown(
                    f"<div style='padding:10px 14px;background:#111827;"
                    f"border:1px solid #f59e0b33;border-radius:8px;"
                    f"font-family:IBM Plex Mono,monospace;font-size:11px;"
                    f"display:flex;gap:20px;'>"
                    f"<span style='color:#94a3b8;'>Avg IRQ/s: "
                    f"<b style='color:#f59e0b;'>{avg_irq:.0f}</b></span>"
                    f"<span style='color:#94a3b8;'>Peak IRQ/s: "
                    f"<b style='color:#f97316;'>{peak_irq:.0f}</b></span>"
                    + (f"<span style='color:#94a3b8;'>Total: "
                       f"<b style='color:#f1f5f9;'>{total_irq:,.0f}</b></span>"
                       if total_irq else "")
                    + "</div>",
                    unsafe_allow_html=True,
                )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 5 — LLM INTERACTIONS
    # ══════════════════════════════════════════════════════════════════════════
    with tab5:
        llm = q(f"""
            SELECT interaction_id, step_index, status,
                   prompt_tokens, completion_tokens, total_tokens,
                   api_latency_ms, non_local_ms, local_compute_ms,
                   preprocess_ms, postprocess_ms,
                   app_throughput_kbps,
                   bytes_sent_approx, bytes_recv_approx,
                   tcp_retransmits,
                   prompt, response, error_message
            FROM llm_interactions
            WHERE run_id = {int(sel_run_id)}
            ORDER BY step_index ASC
        """)

        if llm.empty:
            st.info(
                "No LLM interactions for this run. "
                "Either the interaction logger was not enabled, "
                "or this is an older run from before llm_interactions was populated."
            )
        else:
            n_steps    = len(llm)
            total_toks = int(llm["total_tokens"].sum())
            total_lat  = float(llm["api_latency_ms"].sum())
            n_err      = int((llm["status"] == "error").sum()) if "status" in llm.columns else 0

            # Step summary KPIs
            lk1, lk2, lk3, lk4 = st.columns(4)
            for col, val, label, clr in [
                (lk1, n_steps,         "Steps",           ACCENT),
                (lk2, f"{total_toks:,}","Total tokens",   "#a78bfa"),
                (lk3, f"{total_lat:.0f}ms", "Total latency", "#f59e0b"),
                (lk4, n_err,           "Errors",          "#ef4444" if n_err else "#22c55e"),
            ]:
                with col:
                    st.markdown(
                        f"<div style='padding:8px 10px;background:#111827;"
                        f"border-left:2px solid {clr};border-radius:4px;"
                        f"margin-bottom:12px;'>"
                        f"<div style='font-size:16px;font-weight:700;color:{clr};"
                        f"font-family:IBM Plex Mono,monospace;'>{val}</div>"
                        f"<div style='font-size:9px;color:#475569;'>{label}</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

            # Token and latency per step
            col_l, col_r = st.columns(2)
            with col_l:
                fig_tok = go.Figure()
                if "prompt_tokens" in llm.columns:
                    fig_tok.add_trace(go.Bar(
                        x=llm["step_index"], y=llm["prompt_tokens"],
                        name="Prompt", marker_color="#3b82f6", marker_line_width=0,
                    ))
                if "completion_tokens" in llm.columns:
                    fig_tok.add_trace(go.Bar(
                        x=llm["step_index"], y=llm["completion_tokens"],
                        name="Completion", marker_color="#a78bfa", marker_line_width=0,
                    ))
                fig_tok.update_layout(
                    **{k:v for k,v in PL.items() if k not in ("yaxis","xaxis","margin")}, height=240, barmode="stack",
                    xaxis_title="Step", yaxis_title="Tokens",
                    title=dict(text="Tokens per step", font=dict(size=11)),
                )
                st.plotly_chart(fig_tok, use_container_width=True, key="drill_tok_step")

            with col_r:
                fig_lat = go.Figure()
                if "non_local_ms" in llm.columns:
                    fig_lat.add_trace(go.Bar(
                        x=llm["step_index"], y=llm["non_local_ms"].fillna(0),
                        name="Network wait", marker_color="#f59e0b", marker_line_width=0,
                    ))
                if "local_compute_ms" in llm.columns:
                    fig_lat.add_trace(go.Bar(
                        x=llm["step_index"], y=llm["local_compute_ms"].fillna(0),
                        name="LLM compute", marker_color="#22c55e", marker_line_width=0,
                    ))
                fig_lat.update_layout(
                    **{k:v for k,v in PL.items() if k not in ("yaxis","xaxis","margin")}, height=240, barmode="stack",
                    xaxis_title="Step", yaxis_title="ms",
                    title=dict(text="Latency breakdown per step", font=dict(size=11)),
                )
                st.plotly_chart(fig_lat, use_container_width=True, key="drill_lat_step")

            # Step details
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Step details</div>",
                unsafe_allow_html=True,
            )
            for _, step in llm.iterrows():
                step_idx = int(step.get("step_index", 0))
                status   = str(step.get("status", "?"))
                sc       = "#22c55e" if status == "success" else "#ef4444"
                p_tok    = int(step.get("prompt_tokens", 0) or 0)
                c_tok    = int(step.get("completion_tokens", 0) or 0)
                lat      = float(step.get("api_latency_ms", 0) or 0)
                wait     = float(step.get("non_local_ms", 0) or 0)
                compute  = float(step.get("local_compute_ms", 0) or 0)
                prompt   = str(step.get("prompt") or "")[:300]
                response = str(step.get("response") or "")[:300]

                with st.expander(
                    f"Step {step_idx}  ·  {p_tok}→{c_tok} tok  "
                    f"·  {lat:.0f}ms  ·  {status}",
                    expanded=False,
                ):
                    pc, rc = st.columns(2)
                    with pc:
                        st.markdown(
                            f"<div style='font-size:9px;color:#475569;margin-bottom:4px;'>"
                            f"PROMPT ({p_tok} tokens) · wait {wait:.0f}ms</div>"
                            f"<div style='font-size:11px;color:#94a3b8;line-height:1.6;"
                            f"padding:8px;background:#050c18;border-radius:6px;"
                            f"white-space:pre-wrap;'>{prompt}</div>",
                            unsafe_allow_html=True,
                        )
                    with rc:
                        st.markdown(
                            f"<div style='font-size:9px;color:#475569;margin-bottom:4px;'>"
                            f"RESPONSE ({c_tok} tokens) · compute {compute:.2f}ms</div>"
                            f"<div style='font-size:11px;color:#94a3b8;line-height:1.6;"
                            f"padding:8px;background:#050c18;border-radius:6px;"
                            f"white-space:pre-wrap;'>{response}</div>",
                            unsafe_allow_html=True,
                        )
