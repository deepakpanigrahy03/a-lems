"""
gui/pages/llm_log.py  —  💬  LLM Interactions
─────────────────────────────────────────────────────────────────────────────
Full prompt/response log per step with latency and token counts.
Shows step-level detail for debugging agentic workflows.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.db     import q, q1
from gui.config import PL, WF_COLORS

ACCENT = "#94a3b8"


def render(ctx: dict) -> None:
    total = q1("SELECT COUNT(*) AS n FROM llm_interactions").get("n", 0) or 0

    if total == 0:
        st.markdown(
            f"<div style='padding:40px;text-align:center;"
            f"border:1px solid {ACCENT}33;border-radius:12px;"
            f"background:{ACCENT}08;margin-top:8px;'>"
            f"<div style='font-size:28px;margin-bottom:8px;'>💬</div>"
            f"<div style='font-size:15px;color:{ACCENT};"
            f"font-family:IBM Plex Mono,monospace;margin-bottom:6px;'>"
            f"LLM Interactions — no data yet</div>"
            f"<div style='font-size:11px;color:#475569;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"Interactions are logged per-step during agentic runs.<br>"
            f"Run an agentic experiment to populate this log.</div>"
            f"</div>",
            unsafe_allow_html=True)
        return

    # ── KPIs ──────────────────────────────────────────────────────────────────
    stats = q1("""
        SELECT
            COUNT(*)                      AS total,
            COUNT(DISTINCT run_id)        AS unique_runs,
            AVG(total_tokens)             AS avg_tokens,
            AVG(api_latency_ms)           AS avg_latency,
            SUM(total_tokens)             AS total_tokens,
            AVG(compute_time_ms)          AS avg_compute
        FROM llm_interactions
    """) or {}

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"LLM Interactions — {total:,} logged</div>"
        f"<div style='display:grid;grid-template-columns:repeat(5,1fr);gap:12px;'>"
        + "".join([
            f"<div><div style='font-size:18px;font-weight:700;color:{c};"
            f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
            f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
            for v, l, c in [
                (f"{total:,}",                          "Total interactions",  ACCENT),
                (f"{int(stats.get('unique_runs',0)):,}", "Unique runs",         "#60a5fa"),
                (f"{stats.get('avg_tokens',0):.0f}",    "Avg tokens/call",     "#f59e0b"),
                (f"{stats.get('avg_latency',0):.0f}ms", "Avg API latency",     "#a78bfa"),
                (f"{int(stats.get('total_tokens',0)/1e6):.1f}M","Total tokens",    "#22c55e"),
            ]
        ])
        + "</div></div>",
        unsafe_allow_html=True)

    # ── Filters ───────────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        wf_opts = ["All"] + list(q(
            "SELECT DISTINCT workflow_type FROM llm_interactions "
            "WHERE workflow_type IS NOT NULL ORDER BY workflow_type"
        ).get("workflow_type", pd.Series()).tolist())
        sel_wf = st.selectbox("Workflow", wf_opts, key="llm_wf")
    with col2:
        model_opts = ["All"] + list(q(
            "SELECT DISTINCT model_name FROM llm_interactions "
            "WHERE model_name IS NOT NULL ORDER BY model_name"
        ).get("model_name", pd.Series()).tolist())
        sel_model = st.selectbox("Model", model_opts, key="llm_model")
    with col3:
        run_ids = q(
            "SELECT DISTINCT run_id FROM llm_interactions ORDER BY run_id DESC LIMIT 100"
        ).get("run_id", pd.Series()).tolist()
        sel_run = st.selectbox(
            "Run", ["All"] + [str(r) for r in run_ids], key="llm_run")

    # Build WHERE clause
    where_parts = []
    if sel_wf    != "All": where_parts.append(f"workflow_type = '{sel_wf}'")
    if sel_model != "All": where_parts.append(f"model_name = '{sel_model}'")
    if sel_run   != "All": where_parts.append(f"run_id = {sel_run}")
    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    # ── Charts ────────────────────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"API latency distribution</div>", unsafe_allow_html=True)
        lat_df = q(f"""
            SELECT workflow_type, api_latency_ms
            FROM llm_interactions {where}
            WHERE api_latency_ms IS NOT NULL AND api_latency_ms > 0
            LIMIT 2000
        """)
        fig = go.Figure()
        if not lat_df.empty:
            for wf, clr in WF_COLORS.items():
                sub = lat_df[lat_df["workflow_type"] == wf]["api_latency_ms"]
                if sub.empty: continue
                fig.add_trace(go.Histogram(
                    x=sub, name=wf, marker_color=clr, opacity=0.7, nbinsx=40))
        fig.update_layout(**PL, height=240, barmode="overlay",
                          xaxis_title="API latency (ms)",
                          yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True, key="llm_lat_hist")

    with col2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Tokens per interaction</div>", unsafe_allow_html=True)
        tok_df = q(f"""
            SELECT workflow_type, prompt_tokens, completion_tokens, total_tokens
            FROM llm_interactions {where}
            WHERE total_tokens IS NOT NULL AND total_tokens > 0
            LIMIT 2000
        """)
        fig2 = go.Figure()
        if not tok_df.empty:
            for wf, clr in WF_COLORS.items():
                sub = tok_df[tok_df["workflow_type"] == wf]["total_tokens"]
                if sub.empty: continue
                fig2.add_trace(go.Box(
                    y=sub, name=wf,
                    marker_color=clr, line_color=clr, boxmean=True))
        fig2.update_layout(**PL, height=240,
                           yaxis_title="Total tokens",
                           showlegend=False)
        st.plotly_chart(fig2, use_container_width=True, key="llm_tok_box")

    # ── Step-level latency for selected run ───────────────────────────────────
    if sel_run != "All":
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            f"Step timeline — Run {sel_run}</div>", unsafe_allow_html=True)
        steps = q(f"""
            SELECT step_index, api_latency_ms, compute_time_ms,
                   prompt_tokens, completion_tokens, total_tokens,
                   model_name, workflow_type
            FROM llm_interactions
            WHERE run_id = {sel_run}
            ORDER BY step_index
        """)
        if not steps.empty:
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(
                x=steps["step_index"],
                y=steps["api_latency_ms"].fillna(0),
                name="API latency",
                marker_color="#a78bfa", marker_line_width=0))
            fig3.add_trace(go.Bar(
                x=steps["step_index"],
                y=steps["compute_time_ms"].fillna(0),
                name="Compute time",
                marker_color="#22c55e", marker_line_width=0))
            fig3.update_layout(
                **PL, height=240, barmode="stack",
                xaxis_title="Step index",
                yaxis_title="ms")
            st.plotly_chart(fig3, use_container_width=True,
                            key=f"llm_steps_{sel_run}")

    # ── Interaction log table ─────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Interaction log — latest 200</div>", unsafe_allow_html=True)

    log_df = q(f"""
        SELECT
            interaction_id, run_id, step_index, workflow_type,
            model_name, provider,
            prompt_tokens, completion_tokens, total_tokens,
            ROUND(api_latency_ms, 1) AS api_ms,
            ROUND(compute_time_ms, 1) AS compute_ms,
            created_at
        FROM llm_interactions
        {where}
        ORDER BY interaction_id DESC
        LIMIT 200
    """)
    if not log_df.empty:
        st.dataframe(log_df, use_container_width=True, height=350)

    # ── Prompt/response viewer ────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Prompt / response viewer</div>", unsafe_allow_html=True)

    if not log_df.empty:
        sel_id = st.selectbox(
            "Select interaction",
            log_df["interaction_id"].tolist(),
            key="llm_sel_id",
            format_func=lambda x: f"ID {x} — Run {log_df[log_df['interaction_id']==x]['run_id'].values[0]} Step {log_df[log_df['interaction_id']==x]['step_index'].values[0]}"
        )
        detail = q(f"""
            SELECT prompt, response, model_name, provider,
                   prompt_tokens, completion_tokens, api_latency_ms
            FROM llm_interactions WHERE interaction_id = {int(sel_id)}
        """)
        if not detail.empty:
            row = detail.iloc[0]
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(
                    f"<div style='font-size:9px;color:#475569;text-transform:uppercase;"
                    f"letter-spacing:.08em;margin-bottom:4px;'>Prompt "
                    f"({int(row.get('prompt_tokens') or 0)} tokens)</div>",
                    unsafe_allow_html=True)
                st.text_area("", value=str(row.get("prompt") or ""),
                             height=200, key="llm_prompt_view",
                             label_visibility="collapsed")
            with c2:
                st.markdown(
                    f"<div style='font-size:9px;color:#475569;text-transform:uppercase;"
                    f"letter-spacing:.08em;margin-bottom:4px;'>Response "
                    f"({int(row.get('completion_tokens') or 0)} tokens)</div>",
                    unsafe_allow_html=True)
                st.text_area("", value=str(row.get("response") or ""),
                             height=200, key="llm_response_view",
                             label_visibility="collapsed")
