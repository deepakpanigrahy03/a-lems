"""
gui/pages/efficiency.py  —  ⚡  Efficiency Explorer
─────────────────────────────────────────────────────────────────────────────
energy_per_token, J/instruction, IPC correlation — find optimal configs.
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1

ACCENT = "#38bdf8"


def render(ctx: dict) -> None:
    runs = ctx.get("runs", pd.DataFrame())

    if runs.empty:
        st.info("No run data available.")
        return

    df = runs.copy()
    has_ept = "energy_per_token" in df.columns and df["energy_per_token"].notna().any()
    has_epi = "energy_per_instruction" in df.columns
    has_ipc = "ipc" in df.columns
    has_task = "task_name" in df.columns
    has_model = "model_name" in df.columns

    # ── KPIs ──────────────────────────────────────────────────────────────────
    df_e = (
        df[df["energy_j"].notna() & (df["energy_j"] > 0)]
        if "energy_j" in df.columns
        else df
    )

    lin = df[df["workflow_type"] == "linear"]
    age = df[df["workflow_type"] == "agentic"]

    best_ept = (
        df[df["energy_per_token"] > 0]["energy_per_token"].min() if has_ept else 0
    )
    avg_ept_l = (
        lin[lin["energy_per_token"] > 0]["energy_per_token"].mean()
        if has_ept and not lin.empty
        else 0
    )
    avg_ept_a = (
        age[age["energy_per_token"] > 0]["energy_per_token"].mean()
        if has_ept and not age.empty
        else 0
    )
    avg_ipc = df["ipc"].mean() if has_ipc else 0

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Efficiency Explorer — {len(df)} runs</div>"
        f"<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:12px;'>"
        + "".join(
            [
                f"<div><div style='font-size:16px;font-weight:700;color:{c};"
                f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
                f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
                f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
                for v, l, c in [
                    (f"{best_ept:.5f}J", "Best J/token", "#22c55e"),
                    (f"{avg_ept_l:.5f}J", "Avg J/token linear", WF_COLORS["linear"]),
                    (f"{avg_ept_a:.5f}J", "Avg J/token agentic", WF_COLORS["agentic"]),
                    (f"{avg_ipc:.2f}", "Avg IPC", ACCENT),
                ]
            ]
        )
        + "</div></div>",
        unsafe_allow_html=True,
    )

    # ── Energy per token by model × workflow ──────────────────────────────────
    if has_ept and has_model:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Energy per token — model × workflow</div>",
            unsafe_allow_html=True,
        )

        ept_df = df[df["energy_per_token"].notna() & (df["energy_per_token"] > 0)]
        model_ept = (
            ept_df.groupby(["model_name", "workflow_type"])["energy_per_token"]
            .mean()
            .reset_index()
        )

        fig = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = model_ept[model_ept["workflow_type"] == wf]
            if sub.empty:
                continue
            fig.add_trace(
                go.Bar(
                    x=sub["model_name"],
                    y=sub["energy_per_token"],
                    name=wf,
                    marker_color=clr,
                    marker_line_width=0,
                )
            )
        fig.update_layout(
            **PL,
            height=260,
            barmode="group",
            xaxis_title="Model",
            yaxis_title="J / token",
            xaxis_tickangle=-30,
        )
        st.plotly_chart(fig, use_container_width=True, key="eff_ept_model")

    # ── Efficiency scatter: IPC vs energy ─────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"IPC vs Energy — efficiency frontier</div>",
            unsafe_allow_html=True,
        )
        fig2 = go.Figure()
        if has_ipc and "energy_j" in df.columns:
            for wf, clr in WF_COLORS.items():
                sub = df[df["workflow_type"] == wf]
                sub = sub[
                    sub["ipc"].notna()
                    & sub["energy_j"].notna()
                    & (sub["ipc"] > 0)
                    & (sub["energy_j"] > 0)
                ]
                if sub.empty:
                    continue
                fig2.add_trace(
                    go.Scatter(
                        x=sub["ipc"],
                        y=sub["energy_j"],
                        mode="markers",
                        name=wf,
                        marker=dict(color=clr, size=5, opacity=0.6),
                    )
                )
        fig2.update_layout(
            **PL,
            height=260,
            xaxis_title="IPC (higher = better)",
            yaxis_title="Energy (J) (lower = better)",
        )
        st.plotly_chart(fig2, use_container_width=True, key="eff_ipc_energy")

    with col2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Energy per token vs total tokens</div>",
            unsafe_allow_html=True,
        )
        fig3 = go.Figure()
        if has_ept and "total_tokens" in df.columns:
            ept_sub = df[
                df["energy_per_token"].notna()
                & df["total_tokens"].notna()
                & (df["energy_per_token"] > 0)
                & (df["total_tokens"] > 0)
            ]
            for wf, clr in WF_COLORS.items():
                sub = ept_sub[ept_sub["workflow_type"] == wf]
                if sub.empty:
                    continue
                fig3.add_trace(
                    go.Scatter(
                        x=sub["total_tokens"],
                        y=sub["energy_per_token"],
                        mode="markers",
                        name=wf,
                        marker=dict(color=clr, size=5, opacity=0.6),
                    )
                )
        fig3.update_layout(
            **PL, height=260, xaxis_title="Total tokens", yaxis_title="J / token"
        )
        st.plotly_chart(fig3, use_container_width=True, key="eff_ept_tokens")

    # ── Efficiency by task ────────────────────────────────────────────────────
    if has_task and has_ept:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            f"Efficiency by task — J/token ranking</div>",
            unsafe_allow_html=True,
        )
        task_eff = (
            df[df["energy_per_token"] > 0]
            .groupby("task_name")
            .agg(
                avg_ept=("energy_per_token", "mean"),
                avg_ipc=("ipc", "mean"),
                runs=("energy_j", "count"),
            )
            .reset_index()
            .sort_values("avg_ept")
        )

        fig4 = go.Figure(
            go.Bar(
                x=task_eff["avg_ept"],
                y=task_eff["task_name"],
                orientation="h",
                marker_color=ACCENT,
                marker_line_width=0,
                text=[f"{v:.5f}J" for v in task_eff["avg_ept"]],
                textposition="outside",
                textfont=dict(size=9),
            )
        )
        fig4.update_layout(
            **{**PL, "margin": dict(l=160, r=80, t=10, b=30)},
            height=max(200, len(task_eff) * 28),
            xaxis_title="Avg J / token",
            showlegend=False,
        )
        st.plotly_chart(fig4, use_container_width=True, key="eff_task_rank")

    # ── Optimal config finder ─────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Top 10 most efficient runs</div>",
        unsafe_allow_html=True,
    )

    if has_ept:
        top10 = (
            df[df["energy_per_token"] > 0]
            .nsmallest(10, "energy_per_token")[
                [
                    "run_id" if "run_id" in df.columns else df.columns[0],
                    "workflow_type",
                    "model_name",
                    "task_name",
                    "energy_j",
                    "energy_per_token",
                    "ipc",
                ]
            ]
            .round(6)
            if "energy_j" in df.columns
            else pd.DataFrame()
        )
        if not top10.empty:
            st.dataframe(top10, use_container_width=True)
