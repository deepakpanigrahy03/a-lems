"""
gui/pages/silicon_compare.py  —  ⇌  Cross-Silicon Compare
─────────────────────────────────────────────────────────────────────────────
Compare energy profiles across different silicon (hw_id).
Controlled comparison: same task × model × workflow, different hardware.
Currently shows single-machine profile with multi-machine ready structure.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.db     import q, q1
from gui.config import PL, WF_COLORS

ACCENT = "#fb923c"


def render(ctx: dict) -> None:
    # ── Check how many machines we have ──────────────────────────────────────
    hw_count = q1("SELECT COUNT(*) AS n FROM hardware_config").get("n", 0) or 0

    if hw_count == 0:
        st.info("No hardware registered yet.")
        return

    # ── Load all runs with hw metadata ────────────────────────────────────────
    df = q("""
        SELECT
            r.run_id,
            r.hw_id,
            h.hostname,
            h.cpu_model,
            h.cpu_architecture,
            h.cpu_cores,
            e.workflow_type,
            e.model_name,
            e.task_name,
            e.provider,
            r.total_energy_uj/1e6  AS energy_j,
            r.dynamic_energy_uj/1e6 AS dynamic_energy_j,
            r.ipc,
            r.cache_miss_rate,
            r.package_temp_celsius AS temp_c,
            r.avg_power_watts,
            r.duration_ns/1e6      AS duration_ms,
            r.interrupt_rate,
            r.carbon_g,
            r.energy_per_token
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        JOIN hardware_config h ON r.hw_id = h.hw_id
        WHERE r.total_energy_uj IS NOT NULL
        ORDER BY r.run_id DESC
    """)

    if df.empty:
        st.info("No runs with hardware metadata yet.")
        return

    machines = df["hw_id"].unique()
    hw_labels = {
        row["hw_id"]: f"hw_{row['hw_id']} · {row['hostname'] or 'unknown'}"
        for _, row in df.drop_duplicates("hw_id").iterrows()
    }

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:6px;'>"
        f"Cross-Silicon Compare — {len(machines)} machine{'s' if len(machines)>1 else ''}</div>"
        f"<div style='font-size:12px;color:#94a3b8;font-family:IBM Plex Mono,monospace;'>"
        + "  ·  ".join(hw_labels.values())
        + "</div></div>",
        unsafe_allow_html=True)

    # ── Single machine message if only 1 ─────────────────────────────────────
    if hw_count == 1:
        st.markdown(
            f"<div style='padding:12px 16px;background:#1a1a2e;"
            f"border:1px solid {ACCENT}33;border-left:3px solid {ACCENT};"
            f"border-radius:0 8px 8px 0;margin-bottom:16px;"
            f"font-size:11px;color:#c4b5fd;"
            f"font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
            f"<b>Single machine mode.</b> Connect additional hardware hosts "
            f"to enable true cross-silicon comparison. "
            f"Showing full silicon profile for the current machine.</div>",
            unsafe_allow_html=True)

    # ── Filters ───────────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        tasks = ["All"] + sorted(df["task_name"].dropna().unique().tolist())
        sel_task = st.selectbox("Task", tasks, key="sc_task")
    with col2:
        models = ["All"] + sorted(df["model_name"].dropna().unique().tolist())
        sel_model = st.selectbox("Model", models, key="sc_model")
    with col3:
        workflows = ["All"] + sorted(df["workflow_type"].dropna().unique().tolist())
        sel_wf = st.selectbox("Workflow", workflows, key="sc_wf")

    view = df.copy()
    if sel_task  != "All": view = view[view["task_name"]    == sel_task]
    if sel_model != "All": view = view[view["model_name"]   == sel_model]
    if sel_wf    != "All": view = view[view["workflow_type"] == sel_wf]

    if view.empty:
        st.info("No runs match the selected filters.")
        return

    # ── Energy by hardware ────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
        f"Energy distribution by hardware</div>",
        unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        fig = go.Figure()
        for hw_id in sorted(view["hw_id"].unique()):
            sub  = view[view["hw_id"] == hw_id]["energy_j"].dropna()
            lbl  = hw_labels.get(hw_id, f"hw_{hw_id}")
            if sub.empty: continue
            fig.add_trace(go.Box(
                y=sub, name=lbl,
                marker_color=ACCENT, line_color=ACCENT,
                boxmean=True))
        fig.update_layout(
            **PL, height=280,
            yaxis_title="Energy (J)",
            showlegend=False)
        st.plotly_chart(fig, use_container_width=True, key="sc_energy_box")

    with col2:
        # Energy by workflow on this hardware
        fig2 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = view[view["workflow_type"] == wf]["energy_j"].dropna()
            if sub.empty: continue
            fig2.add_trace(go.Histogram(
                x=sub, name=wf,
                marker_color=clr, opacity=0.7, nbinsx=40))
        fig2.update_layout(
            **PL, height=280, barmode="overlay",
            xaxis_title="Energy (J)",
            yaxis_title="Run count")
        st.plotly_chart(fig2, use_container_width=True, key="sc_energy_hist")

    # ── Key metrics by hardware × workflow ────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Silicon profile — key metrics</div>",
        unsafe_allow_html=True)

    summary = view.groupby(["hw_id", "workflow_type"]).agg(
        runs        =("energy_j",     "count"),
        avg_energy  =("energy_j",     "mean"),
        avg_ipc     =("ipc",          "mean"),
        avg_cmr     =("cache_miss_rate","mean"),
        avg_temp    =("temp_c",       "mean"),
        avg_power   =("avg_power_watts","mean"),
        avg_duration=("duration_ms",  "mean"),
    ).round(3).reset_index()

    summary["hw_label"] = summary["hw_id"].map(hw_labels)
    summary["cell"] = summary["hw_label"] + " · " + summary["workflow_type"]

    metrics = [
        ("avg_energy",   "Avg energy (J)",    "#f59e0b"),
        ("avg_ipc",      "Avg IPC",            "#22c55e"),
        ("avg_cmr",      "Cache miss rate",    "#a78bfa"),
        ("avg_temp",     "Avg pkg temp (°C)",  "#ef4444"),
        ("avg_power",    "Avg power (W)",      "#3b82f6"),
    ]

    fig3 = go.Figure()
    for col_n, label, clr in metrics:
        fig3.add_trace(go.Bar(
            x=summary["cell"],
            y=summary[col_n],
            name=label,
            marker_color=clr,
            marker_line_width=0,
            visible=True if col_n == "avg_energy" else "legendonly",
        ))
    fig3.update_layout(
        **PL, height=300, barmode="group",
        yaxis_title="Value",
        xaxis_tickangle=-20)
    st.plotly_chart(fig3, use_container_width=True, key="sc_metrics_bar")

    # ── Per-task energy profile ───────────────────────────────────────────────
    if "task_name" in view.columns and view["task_name"].notna().any():
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            f"Energy by task × workflow</div>",
            unsafe_allow_html=True)

        task_e = view.groupby(
            ["task_name", "workflow_type"])["energy_j"].mean().reset_index()
        fig4 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = task_e[task_e["workflow_type"] == wf]
            if sub.empty: continue
            fig4.add_trace(go.Bar(
                x=sub["task_name"], y=sub["energy_j"],
                name=wf, marker_color=clr, marker_line_width=0))
        fig4.update_layout(
            **PL, height=260, barmode="group",
            xaxis_title="Task",
            yaxis_title="Avg energy (J)",
            xaxis_tickangle=-30)
        st.plotly_chart(fig4, use_container_width=True, key="sc_task_energy")

    # ── Summary table ─────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Full summary table</div>",
        unsafe_allow_html=True)

    display = summary[[
        "cell", "runs", "avg_energy", "avg_ipc",
        "avg_cmr", "avg_temp", "avg_power", "avg_duration"
    ]].copy()
    display.columns = [
        "Hardware · Workflow", "Runs", "Avg Energy (J)", "Avg IPC",
        "Cache Miss Rate", "Avg Temp (°C)", "Avg Power (W)", "Avg Duration (ms)"
    ]
    st.dataframe(display, use_container_width=True, height=300)

    # ── Multi-host placeholder ────────────────────────────────────────────────
    if hw_count == 1:
        st.markdown(
            f"<div style='margin-top:20px;padding:16px 20px;"
            f"border:1px dashed {ACCENT}44;border-radius:10px;"
            f"background:{ACCENT}06;'>"
            f"<div style='font-size:12px;font-weight:600;color:{ACCENT};"
            f"font-family:IBM Plex Mono,monospace;margin-bottom:6px;'>"
            f"⊕ Multi-Silicon Comparison — Coming Soon</div>"
            f"<div style='font-size:11px;color:#475569;"
            f"font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
            f"Register additional machines via <b>Settings → Multi-Host Config</b>.<br>"
            f"Once connected, this page will show side-by-side energy profiles:<br>"
            f"same experiment · same model · same task · different silicon.<br>"
            f"Compare x86 vs ARM64 vs RISC-V energy cost per token.</div>"
            f"</div>",
            unsafe_allow_html=True)
