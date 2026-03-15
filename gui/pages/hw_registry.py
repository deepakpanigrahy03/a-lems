"""
gui/pages/hw_registry.py  —  ▣  Hardware Registry
─────────────────────────────────────────────────────────────────────────────
Full hardware profile for every registered machine.
Shows CPU spec, RAPL capabilities, AVX/VMX flags, GPU, run statistics.
Foundation for multi-host silicon comparison.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json

from gui.db     import q, q1
from gui.config import PL, STATUS_COLORS

ACCENT = "#fb923c"


def _flag_badge(val, true_label="✓", false_label="✗") -> str:
    if val in (1, True, "1", "true", "True"):
        return (f"<span style='background:#052e1a;color:#4ade80;font-size:9px;"
                f"padding:1px 6px;border-radius:3px;font-weight:700;'>{true_label}</span>")
    return (f"<span style='background:#2a0c0c;color:#f87171;font-size:9px;"
            f"padding:1px 6px;border-radius:3px;font-weight:700;'>{false_label}</span>")


def _cap_badge(val, label) -> str:
    has = val in (1, True, "1", "true", "True")
    bg  = "#052e1a" if has else "#1a1a2e"
    clr = "#4ade80" if has else "#475569"
    return (f"<span style='background:{bg};color:{clr};font-size:9px;"
            f"padding:1px 6px;border-radius:3px;font-weight:700;"
            f"margin-right:4px;'>{label}</span>")


def render(ctx: dict) -> None:
    hw_df = q("""
        SELECT
            h.hw_id, h.hostname, h.cpu_model, h.cpu_cores, h.cpu_threads,
            h.ram_gb, h.cpu_architecture, h.cpu_vendor,
            h.cpu_family, h.cpu_model_id, h.cpu_stepping,
            h.has_avx2, h.has_avx512, h.has_vmx,
            h.rapl_domains, h.rapl_has_dram, h.rapl_has_uncore,
            h.gpu_model, h.gpu_driver, h.gpu_count, h.gpu_power_available,
            h.system_manufacturer, h.system_product, h.system_type,
            h.virtualization_type, h.kernel_version, h.microcode_version,
            h.detected_at,
            COUNT(r.run_id)         AS total_runs,
            COUNT(DISTINCT e.exp_id) AS total_exps,
            MAX(r.start_time_ns)    AS last_run_ns,
            AVG(r.total_energy_uj/1e6) AS avg_energy_j,
            AVG(r.ipc)              AS avg_ipc,
            AVG(r.package_temp_celsius) AS avg_temp
        FROM hardware_config h
        LEFT JOIN runs r ON h.hw_id = r.hw_id
        LEFT JOIN experiments e ON r.exp_id = e.exp_id
        GROUP BY h.hw_id
        ORDER BY h.hw_id
    """)

    if hw_df.empty:
        st.markdown(
            f"<div style='padding:40px;text-align:center;"
            f"border:1px solid {ACCENT}33;border-radius:12px;"
            f"background:{ACCENT}08;'>"
            f"<div style='font-size:28px;margin-bottom:8px;'>▣</div>"
            f"<div style='font-size:14px;color:{ACCENT};"
            f"font-family:IBM Plex Mono,monospace;'>No hardware registered yet</div>"
            f"<div style='font-size:11px;color:#475569;margin-top:4px;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"Hardware is registered automatically on first experiment run.</div>"
            f"</div>",
            unsafe_allow_html=True)
        return

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;"
        f"display:flex;align-items:center;gap:20px;'>"
        f"<div style='font-size:36px;'>▣</div>"
        f"<div>"
        f"<div style='font-size:18px;font-weight:700;color:#f1f5f9;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"{len(hw_df)} Machine{'s' if len(hw_df)>1 else ''} Registered</div>"
        f"<div style='font-size:11px;color:#94a3b8;margin-top:2px;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"Hardware registry — silicon profiles for multi-host experiments</div>"
        f"</div></div>",
        unsafe_allow_html=True)

    # ── Per-machine cards ─────────────────────────────────────────────────────
    for _, hw in hw_df.iterrows():
        hw_id    = hw.get("hw_id", "?")
        hostname = hw.get("hostname") or f"hw_{hw_id}"
        cpu      = hw.get("cpu_model") or "Unknown CPU"
        arch     = hw.get("cpu_architecture") or "?"
        cores    = hw.get("cpu_cores") or "?"
        threads  = hw.get("cpu_threads") or "?"
        ram      = hw.get("ram_gb") or "?"
        sys_type = hw.get("system_type") or "unknown"
        mfr      = hw.get("system_manufacturer") or ""
        product  = hw.get("system_product") or ""
        kernel   = hw.get("kernel_version") or "?"
        virt     = hw.get("virtualization_type") or "none"
        runs_n   = int(hw.get("total_runs") or 0)
        exps_n   = int(hw.get("total_exps") or 0)
        avg_e    = hw.get("avg_energy_j") or 0
        avg_ipc  = hw.get("avg_ipc") or 0
        avg_tmp  = hw.get("avg_temp") or 0

        # RAPL domains
        rapl_raw = hw.get("rapl_domains") or "[]"
        try:
            rapl_domains = json.loads(rapl_raw) if isinstance(rapl_raw, str) else rapl_raw
        except Exception:
            rapl_domains = [rapl_raw]

        # GPU
        gpu_model = hw.get("gpu_model") or "None"
        gpu_count = int(hw.get("gpu_count") or 0)

        st.markdown(
            f"<div style='border:1px solid {ACCENT}33;"
            f"border-top:3px solid {ACCENT};"
            f"border-radius:10px;padding:18px 20px;"
            f"background:#111827;margin-bottom:16px;'>"

            # Title row
            f"<div style='display:flex;align-items:center;gap:12px;margin-bottom:14px;'>"
            f"<div style='font-size:22px;font-weight:800;color:{ACCENT};"
            f"font-family:IBM Plex Mono,monospace;'>hw_{hw_id}</div>"
            f"<div style='font-size:16px;font-weight:600;color:#f1f5f9;"
            f"font-family:IBM Plex Mono,monospace;'>{hostname}</div>"
            f"<div style='margin-left:auto;font-size:10px;color:#475569;"
            f"font-family:IBM Plex Mono,monospace;'>{mfr} {product} · {sys_type}</div>"
            f"</div>"

            # CPU row
            f"<div style='font-size:12px;color:#94a3b8;"
            f"font-family:IBM Plex Mono,monospace;margin-bottom:10px;'>"
            f"<span style='color:#f1f5f9;font-weight:500;'>{cpu}</span>"
            f"  ·  {arch}  ·  {cores}C/{threads}T  ·  {ram}GB RAM"
            f"  ·  kernel {kernel}"
            f"</div>"

            # Capability badges
            f"<div style='margin-bottom:10px;'>"
            + _cap_badge(hw.get("has_avx2"),  "AVX2")
            + _cap_badge(hw.get("has_avx512"), "AVX512")
            + _cap_badge(hw.get("has_vmx"),    "VMX/VT-x")
            + _cap_badge(hw.get("rapl_has_dram"),   "RAPL DRAM")
            + _cap_badge(hw.get("rapl_has_uncore"), "RAPL Uncore")
            + f"<span style='font-size:9px;color:#475569;margin-left:8px;'>"
            f"RAPL: {', '.join(rapl_domains) if rapl_domains else 'unknown'}</span>"
            f"</div>"

            # GPU row
            + (f"<div style='font-size:11px;color:#94a3b8;"
               f"font-family:IBM Plex Mono,monospace;margin-bottom:10px;'>"
               f"GPU: <span style='color:#f1f5f9;'>{gpu_model}</span>"
               f"  ({gpu_count} device{'s' if gpu_count != 1 else ''})"
               f"  ·  Virt: {virt}</div>"
               if gpu_count > 0 or gpu_model != "None" else "")

            + f"</div>",
            unsafe_allow_html=True)

        # Stats row below the card
        stat_cols = st.columns(5)
        for col, (val, label, clr) in zip(stat_cols, [
            (f"{runs_n:,}",        "Total runs",    ACCENT),
            (f"{exps_n:,}",        "Experiments",   "#60a5fa"),
            (f"{avg_e:.2f}J",      "Avg energy",    "#f59e0b"),
            (f"{avg_ipc:.2f}",     "Avg IPC",       "#22c55e"),
            (f"{avg_tmp:.1f}°C",   "Avg pkg temp",  "#ef4444"),
        ]):
            with col:
                st.markdown(
                    f"<div style='padding:8px 12px;background:#1f2937;"
                    f"border:1px solid #374151;border-radius:7px;margin-bottom:12px;'>"
                    f"<div style='font-size:16px;font-weight:700;color:{clr};"
                    f"font-family:IBM Plex Mono,monospace;line-height:1;'>{val}</div>"
                    f"<div style='font-size:8px;color:#475569;text-transform:uppercase;"
                    f"letter-spacing:.08em;margin-top:3px;'>{label}</div>"
                    f"</div>",
                    unsafe_allow_html=True)

        # Energy distribution chart for this hw
        if runs_n > 0:
            with st.expander(f"Energy distribution — hw_{hw_id} ({runs_n} runs)",
                             expanded=False):
                hw_runs = q(f"""
                    SELECT
                        r.total_energy_uj/1e6 AS energy_j,
                        e.workflow_type,
                        e.model_name,
                        e.task_name
                    FROM runs r
                    JOIN experiments e ON r.exp_id = e.exp_id
                    WHERE r.hw_id = {int(hw_id)}
                      AND r.total_energy_uj IS NOT NULL
                    ORDER BY r.run_id DESC
                    LIMIT 500
                """)
                if not hw_runs.empty:
                    fig = go.Figure()
                    wf_colors = {"linear": "#22c55e", "agentic": "#ef4444"}
                    for wf, clr in wf_colors.items():
                        sub = hw_runs[hw_runs["workflow_type"] == wf]["energy_j"].dropna()
                        if sub.empty:
                            continue
                        fig.add_trace(go.Histogram(
                            x=sub, name=wf,
                            marker_color=clr, opacity=0.7, nbinsx=40))
                    fig.update_layout(
                        **{**PL, "margin": dict(l=40, r=20, t=20, b=30)},
                        height=200, barmode="overlay",
                        xaxis_title="Energy (J)",
                        yaxis_title="Run count",
                        showlegend=True)
                    st.plotly_chart(fig, use_container_width=True,
                                   key=f"hw_energy_dist_{hw_id}")

    # ── Baseline profiles ─────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:20px 0 10px;'>"
        f"Idle Baselines</div>",
        unsafe_allow_html=True)

    baselines = q("""
        SELECT baseline_id, timestamp, package_power_watts,
               core_power_watts, uncore_power_watts, dram_power_watts,
               governor, turbo, background_cpu, method
        FROM idle_baselines
        ORDER BY timestamp DESC
        LIMIT 20
    """)

    if baselines.empty:
        st.info("No idle baselines recorded yet.")
    else:
        st.dataframe(baselines.round(4), use_container_width=True, height=250)

        # Baseline power bar chart
        fig2 = go.Figure()
        for col_n, label, clr in [
            ("package_power_watts", "Package", ACCENT),
            ("core_power_watts",    "Core",    "#22c55e"),
            ("uncore_power_watts",  "Uncore",  "#3b82f6"),
            ("dram_power_watts",    "DRAM",    "#a78bfa"),
        ]:
            vals = baselines[col_n].dropna()
            if vals.empty:
                continue
            fig2.add_trace(go.Bar(
                x=list(range(len(baselines))),
                y=vals,
                name=label,
                marker_color=clr,
                marker_line_width=0))
        fig2.update_layout(
            **PL, height=220, barmode="group",
            xaxis_title="Baseline index",
            yaxis_title="Idle power (W)")
        st.plotly_chart(fig2, use_container_width=True, key="hw_baseline_bar")
