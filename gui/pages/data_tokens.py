"""
gui/pages/data_tokens.py  —  ⟳  Token Flow
Energy cost of token movement — prompt vs completion, energy per token.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1
from gui.pages._dm_helpers import get_runs, no_data_banner

ACCENT = "#a78bfa"


def render(ctx: dict) -> None:
    df = get_runs(ctx)

    if df.empty or "total_tokens" not in df.columns:
        no_data_banner("No token data available yet.", ACCENT)
        return

    df_tok = df[df["total_tokens"].notna() & (df["total_tokens"] > 0)].copy()

    has_energy = "energy_j" in df_tok.columns
    has_prompt = "prompt_tokens" in df_tok.columns
    has_complete = "completion_tokens" in df_tok.columns
    has_task = "task_name" in df_tok.columns

    avg_tokens = df_tok["total_tokens"].mean() if not df_tok.empty else 0
    avg_prompt = (
        df_tok["prompt_tokens"].mean() if has_prompt and not df_tok.empty else 0
    )
    avg_complete = (
        df_tok["completion_tokens"].mean() if has_complete and not df_tok.empty else 0
    )
    avg_ept = 0
    if has_energy and not df_tok.empty:
        mask = (
            df_tok["energy_j"].notna()
            & (df_tok["energy_j"] > 0)
            & (df_tok["total_tokens"] > 0)
        )
        if mask.any():
            avg_ept = (
                df_tok.loc[mask, "energy_j"] / df_tok.loc[mask, "total_tokens"]
            ).mean()
    avg_latency = (
        df_tok["api_latency_ms"].mean() if "api_latency_ms" in df_tok.columns else 0
    )

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Token Flow — {len(df_tok)} runs with token data</div>"
        f"<div style='display:grid;grid-template-columns:repeat(5,1fr);gap:12px;'>"
        + "".join(
            [
                f"<div><div style='font-size:18px;font-weight:700;color:{c};"
                f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
                f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
                f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
                for v, l, c in [
                    (f"{avg_tokens:.0f}", "Avg total tokens", ACCENT),
                    (f"{avg_prompt:.0f}", "Avg prompt", "#60a5fa"),
                    (f"{avg_complete:.0f}", "Avg completion", "#34d399"),
                    (f"{avg_ept:.4f}J", "Energy per token", "#f59e0b"),
                    (f"{avg_latency:.0f}ms", "Avg API latency", "#94a3b8"),
                ]
            ]
        )
        + "</div></div>",
        unsafe_allow_html=True,
    )

    tab1, tab2 = st.tabs(["Run-level analysis", "LLM Interactions (step-level)"])

    with tab1:
        if df_tok.empty:
            st.info("No token data available.")
        else:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"Total tokens by workflow</div>",
                    unsafe_allow_html=True,
                )
                fig = go.Figure()
                for wf, clr in WF_COLORS.items():
                    sub = df_tok[df_tok["workflow_type"] == wf]["total_tokens"].dropna()
                    if sub.empty:
                        continue
                    fig.add_trace(
                        go.Box(
                            y=sub,
                            name=wf,
                            marker_color=clr,
                            line_color=clr,
                            boxmean=True,
                        )
                    )
                fig.update_layout(
                    **PL, height=260, yaxis_title="Total tokens", showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True, key="dm_tok_box")

            with col2:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"Tokens vs Energy</div>",
                    unsafe_allow_html=True,
                )
                fig2 = go.Figure()
                if has_energy:
                    for wf, clr in WF_COLORS.items():
                        sub = df_tok[df_tok["workflow_type"] == wf]
                        sub = sub[sub["energy_j"].notna() & (sub["energy_j"] > 0)]
                        if sub.empty:
                            continue
                        fig2.add_trace(
                            go.Scatter(
                                x=sub["total_tokens"],
                                y=sub["energy_j"],
                                mode="markers",
                                name=wf,
                                marker=dict(color=clr, size=5, opacity=0.6),
                            )
                        )
                fig2.update_layout(
                    **PL,
                    height=260,
                    xaxis_title="Total tokens",
                    yaxis_title="Energy (J)",
                )
                st.plotly_chart(
                    fig2, use_container_width=True, key="dm_tok_energy_scatter"
                )

            # Energy per token by task
            if has_energy and has_task:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                    f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
                    f"Energy per token by task</div>",
                    unsafe_allow_html=True,
                )
                mask = (
                    df_tok["energy_j"].notna()
                    & (df_tok["energy_j"] > 0)
                    & (df_tok["total_tokens"] > 0)
                )
                df_e = df_tok[mask].copy()
                df_e["ept"] = df_e["energy_j"] / df_e["total_tokens"]
                task_ept = (
                    df_e.groupby(["task_name", "workflow_type"])["ept"]
                    .mean()
                    .reset_index()
                )
                fig3 = go.Figure()
                for wf, clr in WF_COLORS.items():
                    sub = task_ept[task_ept["workflow_type"] == wf]
                    if sub.empty:
                        continue
                    fig3.add_trace(
                        go.Bar(
                            x=sub["task_name"],
                            y=sub["ept"],
                            name=wf,
                            marker_color=clr,
                            marker_line_width=0,
                        )
                    )
                fig3.update_layout(
                    **PL,
                    height=240,
                    barmode="group",
                    yaxis_title="J / token",
                    xaxis_tickangle=-30,
                )
                st.plotly_chart(fig3, use_container_width=True, key="dm_ept_task")

            # Prompt vs completion split
            if has_prompt and has_complete:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                    f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
                    f"Prompt vs completion token split</div>",
                    unsafe_allow_html=True,
                )
                split = (
                    df_tok.groupby("workflow_type")
                    .agg(
                        prompt=("prompt_tokens", "mean"),
                        completion=("completion_tokens", "mean"),
                    )
                    .reset_index()
                )
                fig4 = go.Figure()
                fig4.add_trace(
                    go.Bar(
                        x=split["workflow_type"],
                        y=split["prompt"],
                        name="Prompt",
                        marker_color="#3b82f6",
                        marker_line_width=0,
                    )
                )
                fig4.add_trace(
                    go.Bar(
                        x=split["workflow_type"],
                        y=split["completion"],
                        name="Completion",
                        marker_color="#22c55e",
                        marker_line_width=0,
                    )
                )
                fig4.update_layout(
                    **PL, height=220, barmode="stack", yaxis_title="Avg tokens"
                )
                st.plotly_chart(fig4, use_container_width=True, key="dm_tok_split")

    with tab2:
        llm_count = q1("SELECT COUNT(*) AS n FROM llm_interactions").get("n", 0) or 0
        if llm_count == 0:
            st.markdown(
                f"<div style='padding:40px;text-align:center;"
                f"border:1px solid {ACCENT}33;border-radius:12px;"
                f"background:{ACCENT}08;margin-top:8px;'>"
                f"<div style='font-size:28px;margin-bottom:8px;'>⟳</div>"
                f"<div style='font-size:15px;color:{ACCENT};"
                f"font-family:IBM Plex Mono,monospace;margin-bottom:6px;'>"
                f"LLM Interactions — populating</div>"
                f"<div style='font-size:11px;color:#475569;"
                f"font-family:IBM Plex Mono,monospace;'>"
                f"Data is being collected. Check back soon.</div>"
                f"</div>",
                unsafe_allow_html=True,
            )
        else:
            llm_df = q("""
                SELECT li.run_id, li.step_index, li.workflow_type,
                       li.model_name, li.provider,
                       li.prompt_tokens, li.completion_tokens, li.total_tokens,
                       li.api_latency_ms, li.compute_time_ms
                FROM llm_interactions li
                ORDER BY li.interaction_id DESC LIMIT 500
            """)
            if not llm_df.empty:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
                    f"{llm_count:,} interactions logged</div>",
                    unsafe_allow_html=True,
                )
                st.dataframe(llm_df, use_container_width=True, height=400)
