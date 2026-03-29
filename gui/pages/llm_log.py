"""
gui/pages/llm_log.py  —  💬  LLM Interactions
─────────────────────────────────────────────────────────────────────────────
Full prompt/response log per step with latency and token counts.
Now has real data — llm_interactions table is populated.

Schema columns available:
  interaction_id, run_id, step_index, workflow_type,
  prompt, response, model_name, provider,
  prompt_tokens, completion_tokens, total_tokens,
  api_latency_ms, compute_time_ms, created_at,
  app_throughput_kbps, total_time_ms,
  preprocess_ms, non_local_ms, local_compute_ms, postprocess_ms,
  cpu_percent_during_wait, error_message, status,
  bytes_sent_approx, bytes_recv_approx, tcp_retransmits
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1

ACCENT = "#60a5fa"


def render(ctx: dict) -> None:

    # ── Load summary stats ─────────────────────────────────────────────────────
    meta = q1("""
        SELECT
            COUNT(*)                        AS total_interactions,
            COUNT(DISTINCT run_id)          AS runs_with_interactions,
            AVG(api_latency_ms)             AS avg_api_latency_ms,
            AVG(total_tokens)               AS avg_tokens,
            AVG(non_local_ms)               AS avg_wait_ms,
            AVG(local_compute_ms)           AS avg_compute_ms,
            SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS n_success,
            SUM(CASE WHEN status='error'   THEN 1 ELSE 0 END) AS n_error
        FROM llm_interactions
    """) or {}

    total       = int(meta.get("total_interactions", 0) or 0)
    n_runs      = int(meta.get("runs_with_interactions", 0) or 0)
    avg_lat     = float(meta.get("avg_api_latency_ms", 0) or 0)
    avg_tokens  = float(meta.get("avg_tokens", 0) or 0)
    avg_wait    = float(meta.get("avg_wait_ms", 0) or 0)
    avg_compute = float(meta.get("avg_compute_ms", 0) or 0)
    n_success   = int(meta.get("n_success", 0) or 0)
    n_error     = int(meta.get("n_error", 0) or 0)

    if total == 0:
        st.markdown(
            f"<div style='padding:40px;text-align:center;"
            f"border:1px solid {ACCENT}33;border-radius:12px;margin-top:8px;'>"
            f"<div style='font-size:14px;color:{ACCENT};"
            f"font-family:IBM Plex Mono,monospace;'>No LLM interactions yet</div>"
            f"<div style='font-size:11px;color:#475569;margin-top:6px;'>"
            f"Run experiments with the interaction logger enabled.</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
        return

    # ── Header ────────────────────────────────────────────────────────────────
    error_rate = round(n_error / total * 100, 1) if total else 0
    health_clr = "#22c55e" if error_rate < 5 else "#f59e0b" if error_rate < 20 else "#ef4444"

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"LLM Interactions — {total:,} calls across {n_runs} runs</div>"
        f"<div style='display:grid;grid-template-columns:repeat(5,1fr);gap:12px;'>"
        + "".join([
            f"<div><div style='font-size:16px;font-weight:700;color:{c};"
            f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
            f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
            for v, l, c in [
                (f"{avg_lat:.0f}ms",    "Avg API latency",  ACCENT),
                (f"{avg_tokens:.0f}",   "Avg tokens",       "#a78bfa"),
                (f"{avg_wait:.0f}ms",   "Avg network wait", "#f59e0b"),
                (f"{avg_compute:.1f}ms","Avg LLM compute",  "#22c55e"),
                (f"{error_rate}%",      "Error rate",       health_clr),
            ]
        ])
        + "</div></div>",
        unsafe_allow_html=True,
    )

    # ── Analytics ────────────────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"API latency distribution by workflow</div>",
            unsafe_allow_html=True,
        )
        lat_df = q("""
            SELECT workflow_type, api_latency_ms
            FROM llm_interactions
            WHERE api_latency_ms IS NOT NULL AND api_latency_ms > 0
        """)
        fig1 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = lat_df[lat_df["workflow_type"] == wf]["api_latency_ms"].dropna()
            if sub.empty:
                continue
            fig1.add_trace(go.Box(
                y=sub, name=wf, marker_color=clr,
                line_color=clr, boxmean=True,
            ))
        fig1.update_layout(
            **PL, height=240,
            yaxis_title="API latency (ms)", showlegend=False,
        )
        st.plotly_chart(fig1, use_container_width=True, key="llm_lat_box")

    with col2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Time breakdown — wait vs compute vs other</div>",
            unsafe_allow_html=True,
        )
        time_agg = q("""
            SELECT
                workflow_type,
                AVG(non_local_ms)      AS avg_wait,
                AVG(local_compute_ms)  AS avg_compute,
                AVG(preprocess_ms)     AS avg_pre,
                AVG(postprocess_ms)    AS avg_post
            FROM llm_interactions
            GROUP BY workflow_type
        """)
        fig2 = go.Figure()
        for col_n, label, clr in [
            ("avg_wait",    "Network wait",  "#f59e0b"),
            ("avg_compute", "LLM compute",   "#22c55e"),
            ("avg_pre",     "Preprocess",    "#3b82f6"),
            ("avg_post",    "Postprocess",   "#a78bfa"),
        ]:
            if col_n not in time_agg.columns:
                continue
            fig2.add_trace(go.Bar(
                x=time_agg["workflow_type"],
                y=time_agg[col_n].fillna(0),
                name=label, marker_color=clr, marker_line_width=0,
            ))
        fig2.update_layout(
            **PL, height=240, barmode="stack",
            yaxis_title="Avg ms", showlegend=True,
        )
        st.plotly_chart(fig2, use_container_width=True, key="llm_time_stack")

    # ── Token analysis ────────────────────────────────────────────────────────
    col3, col4 = st.columns(2)

    with col3:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Tokens per step — prompt vs completion</div>",
            unsafe_allow_html=True,
        )
        tok_df = q("""
            SELECT workflow_type, step_index,
                   AVG(prompt_tokens)     AS avg_prompt,
                   AVG(completion_tokens) AS avg_completion
            FROM llm_interactions
            WHERE prompt_tokens IS NOT NULL
            GROUP BY workflow_type, step_index
            ORDER BY workflow_type, step_index
        """)
        fig3 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = tok_df[tok_df["workflow_type"] == wf]
            if sub.empty:
                continue
            fig3.add_trace(go.Scatter(
                x=sub["step_index"],
                y=sub["avg_prompt"],
                mode="lines+markers",
                name=f"{wf} prompt",
                line=dict(color=clr, width=1.5, dash="dot"),
                marker=dict(size=4),
            ))
            fig3.add_trace(go.Scatter(
                x=sub["step_index"],
                y=sub["avg_completion"],
                mode="lines+markers",
                name=f"{wf} completion",
                line=dict(color=clr, width=2),
                marker=dict(size=4),
            ))
        fig3.update_layout(
            **PL, height=240,
            xaxis_title="Step index",
            yaxis_title="Avg tokens",
        )
        st.plotly_chart(fig3, use_container_width=True, key="llm_tok_step")

    with col4:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Throughput vs latency</div>",
            unsafe_allow_html=True,
        )
        tput_df = q("""
            SELECT workflow_type, app_throughput_kbps, api_latency_ms
            FROM llm_interactions
            WHERE app_throughput_kbps IS NOT NULL
              AND api_latency_ms > 0
        """)
        fig4 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = tput_df[tput_df["workflow_type"] == wf].dropna()
            if sub.empty:
                continue
            fig4.add_trace(go.Scatter(
                x=sub["api_latency_ms"],
                y=sub["app_throughput_kbps"],
                mode="markers", name=wf,
                marker=dict(color=clr, size=5, opacity=0.6),
            ))
        fig4.update_layout(
            **PL, height=240,
            xaxis_title="API latency (ms)",
            yaxis_title="Throughput (KB/s)",
        )
        st.plotly_chart(fig4, use_container_width=True, key="llm_tput_scatter")

    # ── Step-by-step log viewer ───────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Step-by-step interaction log</div>",
        unsafe_allow_html=True,
    )

    # Run selector
    run_ids = q("""
        SELECT DISTINCT run_id FROM llm_interactions
        ORDER BY run_id DESC LIMIT 50
    """).get("run_id", pd.Series()).tolist()

    if not run_ids:
        st.info("No run IDs found.")
        return

    sel_run = st.selectbox(
        "Select run", run_ids, key="llm_run_sel",
        format_func=lambda x: f"Run {x}",
    )

    steps = q(f"""
        SELECT
            interaction_id, step_index, workflow_type,
            model_name, provider, status,
            prompt_tokens, completion_tokens, total_tokens,
            api_latency_ms, non_local_ms, local_compute_ms,
            bytes_sent_approx, bytes_recv_approx, tcp_retransmits,
            prompt, response, error_message, created_at
        FROM llm_interactions
        WHERE run_id = {int(sel_run)}
        ORDER BY step_index ASC
    """)

    if steps.empty:
        st.info("No interactions for this run.")
        return

    # Run summary bar
    n_steps    = len(steps)
    total_toks = int(steps["total_tokens"].sum())
    total_lat  = float(steps["api_latency_ms"].sum())
    n_err      = int((steps["status"] == "error").sum())

    st.markdown(
        f"<div style='padding:8px 14px;background:#0c1f3a;"
        f"border:1px solid #3b82f633;border-radius:8px;margin-bottom:12px;"
        f"display:flex;gap:20px;font-family:IBM Plex Mono,monospace;font-size:11px;'>"
        f"<span style='color:#94a3b8;'>Run {sel_run}</span>"
        f"<span style='color:#f1f5f9;'>{n_steps} steps</span>"
        f"<span style='color:#a78bfa;'>{total_toks:,} tokens</span>"
        f"<span style='color:#f59e0b;'>{total_lat:.0f}ms total latency</span>"
        + (f"<span style='color:#ef4444;'>{n_err} errors</span>" if n_err else "")
        + "</div>",
        unsafe_allow_html=True,
    )

    # Render each step
    for _, step in steps.iterrows():
        step_idx = int(step.get("step_index", 0))
        status   = str(step.get("status", "?"))
        status_clr = "#22c55e" if status == "success" else "#ef4444"
        prompt   = str(step.get("prompt") or "")
        response = str(step.get("response") or "")
        lat      = float(step.get("api_latency_ms") or 0)
        wait     = float(step.get("non_local_ms") or 0)
        compute  = float(step.get("local_compute_ms") or 0)
        p_tok    = int(step.get("prompt_tokens") or 0)
        c_tok    = int(step.get("completion_tokens") or 0)
        err      = str(step.get("error_message") or "")

        with st.expander(
            f"Step {step_idx}  ·  {p_tok}→{c_tok} tokens  "
            f"·  {lat:.0f}ms  ·  {status}",
            expanded=False,
        ):
            # Metrics row
            m1, m2, m3, m4 = st.columns(4)
            for col, val, label, clr in [
                (m1, f"{lat:.0f}ms",     "API latency",    "#f59e0b"),
                (m2, f"{wait:.0f}ms",    "Network wait",   "#38bdf8"),
                (m3, f"{compute:.2f}ms", "LLM compute",    "#22c55e"),
                (m4, f"{p_tok+c_tok}",   "Total tokens",   "#a78bfa"),
            ]:
                with col:
                    st.markdown(
                        f"<div style='padding:6px 10px;background:#111827;"
                        f"border-left:2px solid {clr};border-radius:4px;"
                        f"margin-bottom:8px;'>"
                        f"<div style='font-size:14px;font-weight:700;color:{clr};"
                        f"font-family:IBM Plex Mono,monospace;'>{val}</div>"
                        f"<div style='font-size:9px;color:#475569;'>{label}</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

            # Prompt / Response
            p_col, r_col = st.columns(2)
            with p_col:
                st.markdown(
                    "<div style='font-size:10px;color:#475569;"
                    "text-transform:uppercase;letter-spacing:.08em;"
                    "margin-bottom:4px;'>Prompt</div>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f"<div style='font-size:11px;color:#94a3b8;line-height:1.6;"
                    f"padding:8px;background:#050c18;border-radius:6px;"
                    f"max-height:200px;overflow-y:auto;white-space:pre-wrap;'>"
                    f"{prompt[:500] + ('...' if len(prompt) > 500 else '')}"
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with r_col:
                st.markdown(
                    "<div style='font-size:10px;color:#475569;"
                    "text-transform:uppercase;letter-spacing:.08em;"
                    "margin-bottom:4px;'>Response</div>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f"<div style='font-size:11px;color:#94a3b8;line-height:1.6;"
                    f"padding:8px;background:#050c18;border-radius:6px;"
                    f"max-height:200px;overflow-y:auto;white-space:pre-wrap;'>"
                    f"{response[:500] + ('...' if len(response) > 500 else '')}"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            if err and err not in ("None", ""):
                st.markdown(
                    f"<div style='padding:6px 10px;background:#2a0c0c;"
                    f"border-left:3px solid #ef4444;border-radius:4px;"
                    f"font-size:10px;color:#fca5a5;"
                    f"font-family:IBM Plex Mono,monospace;margin-top:8px;'>"
                    f"Error: {err}</div>",
                    unsafe_allow_html=True,
                )
