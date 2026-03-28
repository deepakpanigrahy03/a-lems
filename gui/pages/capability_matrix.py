"""
gui/pages/capability_matrix.py  —  ◈  Capability Matrix
─────────────────────────────────────────────────────────────────────────────
PLANNED — Cross-hardware capability and efficiency comparison.

Shows for each (hardware × model × task × workflow):
  • Energy efficiency rank
  • IPC rank
  • Thermal profile
  • Suitability score for that task

Currently shows what single-host data already reveals.
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1

ACCENT = "#a78bfa"


def render(ctx: dict) -> None:
    runs = ctx.get("runs", pd.DataFrame())

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:6px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;'>Capability Matrix</div>"
        f"<div style='font-size:9px;padding:2px 8px;border-radius:4px;"
        f"background:#1a0e2e;color:{ACCENT};border:1px solid {ACCENT}44;'>"
        f"PLANNED</div></div>"
        f"<div style='font-size:12px;color:#94a3b8;'>"
        f"Cross-hardware efficiency comparison — which hardware is best for which task?"
        f"</div></div>",
        unsafe_allow_html=True,
    )

    tab1, tab2, tab3 = st.tabs([
        "◈  Single-host matrix",
        "⬡  Multi-host vision",
        "🔬  Efficiency ranking",
    ])

    with tab1:
        if runs.empty:
            st.info("No run data available.")
            return

        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Task × workflow energy heatmap</div>",
            unsafe_allow_html=True,
        )

        # Energy by task × workflow — heatmap
        if "task_name" in runs.columns and "energy_j" in runs.columns:
            task_wf = (
                runs.groupby(["task_name", "workflow_type"])["energy_j"]
                .mean().reset_index()
            )
            if not task_wf.empty:
                pivot = task_wf.pivot_table(
                    index="workflow_type", columns="task_name",
                    values="energy_j", aggfunc="mean",
                )
                fig = go.Figure(go.Heatmap(
                    z=pivot.values.tolist(),
                    x=list(pivot.columns),
                    y=list(pivot.index),
                    colorscale=[
                        [0.0, "#052e1a"], [0.5, "#854d0e"], [1.0, "#7f1d1d"],
                    ],
                    showscale=True,
                    colorbar=dict(title="Avg J", tickfont=dict(size=9)),
                    texttemplate="%{z:.4f}",
                    textfont=dict(size=9),
                ))
                fig.update_layout(
                    **{**PL, "margin": dict(l=120, r=60, t=20, b=80)},
                    height=max(200, len(pivot) * 50 + 80),
                    xaxis_tickangle=-30,
                )
                st.plotly_chart(fig, use_container_width=True, key="cap_heatmap")

        # IPC by task × workflow
        if "task_name" in runs.columns and "ipc" in runs.columns:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
                f"IPC by task — CPU efficiency signature</div>",
                unsafe_allow_html=True,
            )
            ipc_agg = (
                runs.groupby(["task_name", "workflow_type"])["ipc"]
                .mean().reset_index()
            )
            fig2 = go.Figure()
            for wf, clr in WF_COLORS.items():
                sub = ipc_agg[ipc_agg["workflow_type"] == wf]
                if sub.empty: continue
                fig2.add_trace(go.Bar(
                    x=sub["task_name"], y=sub["ipc"],
                    name=wf, marker_color=clr, marker_line_width=0,
                ))
            fig2.update_layout(
                **PL, height=260, barmode="group",
                xaxis_tickangle=-30,
                yaxis_title="Avg IPC",
            )
            st.plotly_chart(fig2, use_container_width=True, key="cap_ipc")

    with tab2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;'>"
            f"Multi-host capability matrix vision</div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f"<div style='padding:12px 16px;background:#0d1117;"
            f"border:1px solid {ACCENT}33;border-radius:8px;margin-bottom:12px;"
            f"font-size:11px;color:#94a3b8;line-height:1.8;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"When multi-host dispatch is enabled, this page will show:<br>"
            f"• A matrix of <b style='color:#f1f5f9;'>hardware × task</b> with colour-coded "
            f"efficiency scores<br>"
            f"• <b style='color:#f1f5f9;'>Best hardware for each task</b> recommendation<br>"
            f"• <b style='color:#f1f5f9;'>Hardware suitability score</b>: "
            f"energy efficiency × IPC × thermal headroom<br>"
            f"• <b style='color:#f1f5f9;'>Anomaly detection</b>: "
            f"hardware that underperforms for specific task types</div>",
            unsafe_allow_html=True,
        )

        # Mock capability matrix to show the vision
        mock_hardware = ["Lab Workstation", "Dev Laptop", "Edge Device (planned)"]
        mock_tasks    = ["gsm8k", "code_gen", "summarise", "qa"]
        mock_scores   = [
            [0.92, 0.85, 0.78, 0.91],
            [0.71, 0.68, 0.82, 0.74],
            [0.45, 0.40, 0.61, 0.50],
        ]
        fig_mock = go.Figure(go.Heatmap(
            z=mock_scores,
            x=mock_tasks, y=mock_hardware,
            colorscale=[[0,"#2a0c0c"],[0.5,"#854d0e"],[1,"#14532d"]],
            showscale=True,
            colorbar=dict(title="Efficiency", tickfont=dict(size=9)),
            texttemplate="%{z:.2f}",
            textfont=dict(size=10),
        ))
        fig_mock.update_layout(
            **{**PL, "margin": dict(l=180, r=60, t=20, b=40)},
            height=200,
            title=dict(
                text="Vision: capability matrix (mock data — awaiting multi-host)",
                font=dict(size=10, color="#475569"),
            ),
        )
        st.plotly_chart(fig_mock, use_container_width=True, key="cap_mock")

    with tab3:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Current hardware efficiency ranking</div>",
            unsafe_allow_html=True,
        )

        if not runs.empty and "energy_j" in runs.columns:
            # Compute per-task efficiency score = 1/energy_j normalised
            eff_df = (
                runs.groupby("task_name")["energy_j"]
                .agg(["mean","std","count"]).reset_index()
            )
            eff_df.columns = ["task", "avg_j", "std_j", "count"]
            eff_df = eff_df[eff_df["count"] >= 3].sort_values("avg_j")

            if not eff_df.empty:
                fig3 = go.Figure(go.Bar(
                    x=eff_df["task"],
                    y=eff_df["avg_j"],
                    error_y=dict(type="data", array=eff_df["std_j"].fillna(0).tolist()),
                    marker_color=[
                        "#22c55e" if i < len(eff_df)//3 else
                        "#f59e0b" if i < 2*len(eff_df)//3 else
                        "#ef4444"
                        for i in range(len(eff_df))
                    ],
                    marker_line_width=0,
                    text=eff_df["count"].apply(lambda n: f"n={n}"),
                    textposition="outside", textfont=dict(size=9),
                ))
                fig3.update_layout(
                    **PL, height=260,
                    xaxis_tickangle=-30,
                    yaxis_title="Avg energy (J) — lower is better",
                )
                st.plotly_chart(fig3, use_container_width=True, key="cap_rank")
