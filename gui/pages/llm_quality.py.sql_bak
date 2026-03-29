"""
gui/pages/llm_quality.py  —  ⭐  LLM Quality
─────────────────────────────────────────────────────────────────────────────
LLM response quality analysis — not just latency and tokens, but HOW GOOD
were the responses? And what did good responses cost in energy?

WHY THIS PAGE EXISTS
─────────────────────
Raw energy data tells us how much a run cost. Quality data tells us whether
it was worth it. A 3× energy overhead for agentic is fine if agentic gives
3× better answers. If it gives the same answer, the overhead is pure waste.

QUALITY SIGNALS AVAILABLE WITHOUT GROUND TRUTH
────────────────────────────────────────────────
1. Response length consistency    — same task should give similar length
2. Linear vs agentic agreement    — do both workflows agree? disagreement = unreliable
3. Token efficiency               — more tokens ≠ better answer
4. Latency-adjusted quality       — response quality per millisecond
5. Energy per quality unit        — J per token, J per character, J per step

Tab 1: ⭐ Response scoring       — automatic quality proxies
Tab 2: ⚡ Energy per quality     — J/token, J/char, efficiency ranking
Tab 3: ⇄ Linear vs agentic      — agreement analysis, where they diverge
Tab 4: 🔬 Step verification      — per-step response deep dive
─────────────────────────────────────────────────────────────────────────────
"""

import re

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1

ACCENT = "#f59e0b"

# Quality scoring weights — configurable
W_LENGTH_CONSISTENCY = 0.30   # how consistent is response length for same task?
W_TOKEN_EFFICIENCY   = 0.25   # completion / prompt ratio (higher = more output per input)
W_STEP_COMPLETION    = 0.25   # did all steps complete without error?
W_LATENCY_EFFICIENCY = 0.20   # tokens per second of latency


def _score_run(interactions: pd.DataFrame, total_energy_j: float) -> dict:
    """
    Compute quality scores for a set of LLM interactions.
    Returns a dict of scores 0-100 and composite score.
    """
    if interactions.empty:
        return {}

    n_steps      = len(interactions)
    n_success    = int((interactions["status"] == "success").sum()) \
                   if "status" in interactions.columns else n_steps
    total_tokens = float(interactions["total_tokens"].sum())
    prompt_toks  = float(interactions["prompt_tokens"].sum()) \
                   if "prompt_tokens" in interactions.columns else 0
    comp_toks    = float(interactions["completion_tokens"].sum()) \
                   if "completion_tokens" in interactions.columns else total_tokens
    total_lat_ms = float(interactions["api_latency_ms"].sum()) \
                   if "api_latency_ms" in interactions.columns else 1

    # Score 1: Step completion rate (0-100)
    completion_score = round(n_success / n_steps * 100, 1) if n_steps else 0

    # Score 2: Token efficiency — completion/prompt ratio, normalised
    # A ratio of ~0.5-2.0 is normal; higher means more output per input
    tok_ratio = comp_toks / max(prompt_toks, 1)
    tok_efficiency_score = min(100, round(tok_ratio * 50, 1))

    # Score 3: Latency efficiency — tokens per second
    tokens_per_sec = total_tokens / (total_lat_ms / 1000) if total_lat_ms > 0 else 0
    # Normalise: 100 tok/s = perfect, <10 tok/s = poor
    latency_score = min(100, round(tokens_per_sec, 1))

    # Score 4: Response richness — avg response length in chars
    avg_resp_len = 0
    if "response" in interactions.columns:
        resp_lens = interactions["response"].dropna().apply(len)
        avg_resp_len = float(resp_lens.mean()) if not resp_lens.empty else 0

    # Composite score weighted sum
    composite = round(
        completion_score   * W_STEP_COMPLETION
        + tok_efficiency_score * W_TOKEN_EFFICIENCY
        + min(100, latency_score)  * W_LATENCY_EFFICIENCY
        + min(100, avg_resp_len / 10) * W_LENGTH_CONSISTENCY,
        1,
    )

    # Energy efficiency: J per completion token
    j_per_token = total_energy_j / max(comp_toks, 1)
    j_per_step  = total_energy_j / max(n_steps, 1)

    return {
        "composite":          min(100, composite),
        "completion_score":   completion_score,
        "tok_efficiency":     tok_efficiency_score,
        "latency_score":      min(100, latency_score),
        "avg_resp_len":       avg_resp_len,
        "n_steps":            n_steps,
        "n_success":          n_success,
        "total_tokens":       total_tokens,
        "tokens_per_sec":     tokens_per_sec,
        "j_per_token":        j_per_token,
        "j_per_step":         j_per_step,
        "tok_ratio":          tok_ratio,
    }


def render(ctx: dict) -> None:
    runs = ctx.get("runs", pd.DataFrame())

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px;'>"
        f"LLM Response Quality Analysis</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>"
        f"Quality proxies · Energy per quality unit · "
        f"Linear vs agentic agreement · Per-step verification</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── Load interactions with run context ────────────────────────────────────
    llm_df = q("""
        SELECT
            li.interaction_id, li.run_id, li.step_index,
            li.status, li.prompt, li.response,
            li.prompt_tokens, li.completion_tokens, li.total_tokens,
            li.api_latency_ms, li.non_local_ms, li.local_compute_ms,
            li.preprocess_ms, li.postprocess_ms,
            li.bytes_sent_approx, li.bytes_recv_approx,
            li.error_message,
            r.workflow_type, r.total_energy_uj/1e6 AS energy_j,
            r.duration_ns/1e9 AS duration_s,
            e.task_name, e.model_name, e.provider
        FROM llm_interactions li
        JOIN runs r        ON li.run_id  = r.run_id
        JOIN experiments e ON r.exp_id   = e.exp_id
        WHERE li.total_tokens > 0
        ORDER BY li.run_id DESC, li.step_index ASC
    """)

    if llm_df.empty:
        st.info(
            "No LLM interaction data yet. "
            "Run experiments with the interaction logger enabled."
        )
        return

    n_interactions = len(llm_df)
    n_runs_with    = llm_df["run_id"].nunique()
    n_tasks        = llm_df["task_name"].nunique() if "task_name" in llm_df.columns else 0

    # ── Quick stats ────────────────────────────────────────────────────────────
    qs1, qs2, qs3, qs4 = st.columns(4)
    for col, val, label, clr in [
        (qs1, f"{n_interactions:,}", "Total interactions", ACCENT),
        (qs2, n_runs_with,           "Runs with data",     "#3b82f6"),
        (qs3, n_tasks,               "Unique tasks",       "#a78bfa"),
        (qs4, int((llm_df["status"] == "error").sum()) if "status" in llm_df.columns else 0,
         "Errors", "#ef4444"),
    ]:
        with col:
            st.markdown(
                f"<div style='padding:8px 12px;background:#111827;"
                f"border:1px solid {clr}33;border-left:3px solid {clr};"
                f"border-radius:8px;margin-bottom:12px;'>"
                f"<div style='font-size:18px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;'>{val}</div>"
                f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
                f"letter-spacing:.08em;'>{label}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs([
        "⭐  Response scoring",
        "⚡  Energy per quality",
        "⇄  Linear vs agentic",
        "🔬  Step verification",
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — RESPONSE SCORING
    # ══════════════════════════════════════════════════════════════════════════
    with tab1:
        st.markdown(
            f"<div style='font-size:11px;color:#94a3b8;margin-bottom:16px;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"Quality proxies without ground truth — using structural signals: "
            f"step completion rate, token efficiency, latency efficiency, response richness."
            f"</div>",
            unsafe_allow_html=True,
        )

        # Compute per-run quality scores
        run_scores = []
        for run_id, run_interactions in llm_df.groupby("run_id"):
            energy_j = float(run_interactions["energy_j"].iloc[0] or 0)
            scores   = _score_run(run_interactions, energy_j)
            if not scores:
                continue
            run_scores.append({
                "run_id":       run_id,
                "workflow":     str(run_interactions["workflow_type"].iloc[0]),
                "task":         str(run_interactions["task_name"].iloc[0]) if "task_name" in run_interactions.columns else "?",
                "provider":     str(run_interactions["provider"].iloc[0]) if "provider" in run_interactions.columns else "?",
                "energy_j":     energy_j,
                **scores,
            })

        if not run_scores:
            st.info("Could not compute scores — check interaction data.")
            return

        scores_df = pd.DataFrame(run_scores)

        # Overall score distribution
        col1, col2 = st.columns(2)

        with col1:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Composite quality score distribution</div>",
                unsafe_allow_html=True,
            )
            fig1 = go.Figure()
            for wf, clr in WF_COLORS.items():
                sub = scores_df[scores_df["workflow"] == wf]["composite"].dropna()
                if sub.empty: continue
                fig1.add_trace(go.Histogram(
                    x=sub, name=wf, marker_color=clr,
                    opacity=0.7, nbinsx=20,
                ))
            fig1.update_layout(
                **PL, height=240, barmode="overlay",
                xaxis_title="Quality score (0-100)",
                yaxis_title="Run count",
            )
            st.plotly_chart(fig1, use_container_width=True, key="qual_dist")

        with col2:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Score components by workflow</div>",
                unsafe_allow_html=True,
            )
            wf_scores = (
                scores_df.groupby("workflow")[
                    ["composite","completion_score","tok_efficiency","latency_score"]
                ].mean().reset_index()
            )
            fig2 = go.Figure()
            for col_n, label, clr in [
                ("composite",         "Composite",    ACCENT),
                ("completion_score",  "Completion",   "#22c55e"),
                ("tok_efficiency",    "Token eff.",   "#3b82f6"),
                ("latency_score",     "Latency eff.", "#a78bfa"),
            ]:
                fig2.add_trace(go.Bar(
                    x=wf_scores["workflow"],
                    y=wf_scores[col_n],
                    name=label,
                    marker_color=clr,
                    marker_line_width=0,
                ))
            fig2.update_layout(
                **PL, height=240, barmode="group",
                yaxis_title="Score (0-100)",
            )
            st.plotly_chart(fig2, use_container_width=True, key="qual_components")

        # Score by task
        if "task" in scores_df.columns:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Avg quality score by task</div>",
                unsafe_allow_html=True,
            )
            task_scores = (
                scores_df.groupby(["task","workflow"])["composite"]
                .mean().reset_index()
            )
            fig3 = go.Figure()
            for wf, clr in WF_COLORS.items():
                sub = task_scores[task_scores["workflow"] == wf]
                if sub.empty: continue
                fig3.add_trace(go.Bar(
                    x=sub["task"], y=sub["composite"],
                    name=wf, marker_color=clr, marker_line_width=0,
                ))
            fig3.update_layout(
                **PL, height=260, barmode="group",
                xaxis_tickangle=-30,
                yaxis_title="Avg composite score",
            )
            st.plotly_chart(fig3, use_container_width=True, key="qual_task")

        # Tokens per second — responsiveness metric
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Tokens/second — throughput (higher = faster responses)</div>",
            unsafe_allow_html=True,
        )
        fig4 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = scores_df[scores_df["workflow"] == wf]["tokens_per_sec"].dropna()
            if sub.empty: continue
            fig4.add_trace(go.Box(
                y=sub, name=wf, marker_color=clr,
                line_color=clr, boxmean=True,
            ))
        fig4.update_layout(
            **PL, height=240,
            yaxis_title="Tokens/second",
            showlegend=False,
        )
        st.plotly_chart(fig4, use_container_width=True, key="qual_tps")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — ENERGY PER QUALITY
    # ══════════════════════════════════════════════════════════════════════════
    with tab2:
        st.markdown(
            f"<div style='font-size:11px;color:#94a3b8;margin-bottom:16px;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"Is the energy overhead worth the quality difference? "
            f"J/token and J/step normalise energy by output produced.</div>",
            unsafe_allow_html=True,
        )

        col1, col2 = st.columns(2)

        with col1:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Energy per token — by workflow</div>",
                unsafe_allow_html=True,
            )
            if "j_per_token" in scores_df.columns:
                fig_jpt = go.Figure()
                for wf, clr in WF_COLORS.items():
                    sub = scores_df[scores_df["workflow"] == wf]["j_per_token"].dropna()
                    sub = sub[sub > 0]
                    if sub.empty: continue
                    fig_jpt.add_trace(go.Box(
                        y=sub, name=wf, marker_color=clr,
                        line_color=clr, boxmean=True,
                    ))
                fig_jpt.update_layout(
                    **PL, height=260,
                    yaxis_title="J per completion token",
                    showlegend=False,
                )
                st.plotly_chart(fig_jpt, use_container_width=True, key="qual_jpt")

        with col2:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Quality score vs energy — is more energy = better quality?</div>",
                unsafe_allow_html=True,
            )
            fig_qe = go.Figure()
            for wf, clr in WF_COLORS.items():
                sub = scores_df[scores_df["workflow"] == wf].dropna(
                    subset=["composite","energy_j"]
                )
                sub = sub[sub["energy_j"] > 0]
                if sub.empty: continue
                fig_qe.add_trace(go.Scatter(
                    x=sub["energy_j"], y=sub["composite"],
                    mode="markers", name=wf,
                    marker=dict(color=clr, size=6, opacity=0.65),
                ))
            fig_qe.update_layout(
                **PL, height=260,
                xaxis_title="Total energy (J)",
                yaxis_title="Composite quality score",
            )
            st.plotly_chart(fig_qe, use_container_width=True, key="qual_qe_scatter")

        # Correlation: energy vs quality
        sub_corr = scores_df[["energy_j","composite"]].dropna()
        sub_corr = sub_corr[sub_corr["energy_j"] > 0]
        if len(sub_corr) >= 5:
            corr = sub_corr["energy_j"].corr(sub_corr["composite"])
            corr_clr = (
                "#22c55e" if corr < -0.3 else
                "#f59e0b" if abs(corr) < 0.3 else
                "#ef4444"
            )
            interpretation = (
                "More energy → better quality (expected for agentic)."
                if corr > 0.3 else
                "More energy → lower quality (orchestration overhead without benefit)."
                if corr < -0.3 else
                "Energy and quality are uncorrelated — quality is not energy-limited."
            )
            st.markdown(
                f"<div style='padding:10px 14px;background:#111827;"
                f"border:1px solid {corr_clr}33;border-left:3px solid {corr_clr};"
                f"border-radius:8px;display:flex;gap:14px;align-items:center;"
                f"margin-bottom:16px;'>"
                f"<div style='font-size:24px;font-weight:800;color:{corr_clr};"
                f"font-family:IBM Plex Mono,monospace;'>{corr:.3f}</div>"
                f"<div style='font-size:11px;color:#94a3b8;'>"
                f"Pearson r (energy vs quality score) · n={len(sub_corr)} runs<br>"
                f"<span style='color:{corr_clr};'>{interpretation}</span>"
                f"</div></div>",
                unsafe_allow_html=True,
            )

        # J/token by task
        if "task" in scores_df.columns and "j_per_token" in scores_df.columns:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Energy per token by task — which tasks are expensive per token?</div>",
                unsafe_allow_html=True,
            )
            task_jpt = (
                scores_df.groupby(["task","workflow"])["j_per_token"]
                .mean().reset_index()
            )
            fig_tjpt = go.Figure()
            for wf, clr in WF_COLORS.items():
                sub = task_jpt[task_jpt["workflow"] == wf]
                if sub.empty: continue
                fig_tjpt.add_trace(go.Bar(
                    x=sub["task"], y=sub["j_per_token"],
                    name=wf, marker_color=clr, marker_line_width=0,
                ))
            fig_tjpt.update_layout(
                **PL, height=260, barmode="group",
                xaxis_tickangle=-30,
                yaxis_title="J per completion token",
            )
            st.plotly_chart(fig_tjpt, use_container_width=True, key="qual_task_jpt")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — LINEAR VS AGENTIC AGREEMENT
    # ══════════════════════════════════════════════════════════════════════════
    with tab3:
        st.markdown(
            f"<div style='font-size:11px;color:#94a3b8;margin-bottom:16px;"
            f"font-family:IBM Plex Mono,monospace;line-height:1.8;'>"
            f"When linear and agentic run the same task, do they agree? "
            f"Agreement proxy: response length similarity, token count ratio, "
            f"latency difference. High disagreement on the same task = "
            f"one workflow is making different decisions.</div>",
            unsafe_allow_html=True,
        )

        # Find paired runs — same exp_id, different workflow
        paired = q("""
            SELECT
                e.exp_id,
                e.task_name,
                e.model_name,
                rl.run_id   AS lin_run_id,
                ra.run_id   AS agt_run_id,
                rl.total_energy_uj/1e6 AS lin_energy_j,
                ra.total_energy_uj/1e6 AS agt_energy_j,
                rl.duration_ns/1e9     AS lin_dur_s,
                ra.duration_ns/1e9     AS agt_dur_s
            FROM experiments e
            JOIN runs rl ON rl.exp_id = e.exp_id AND rl.workflow_type = 'linear'
            JOIN runs ra ON ra.exp_id = e.exp_id AND ra.workflow_type = 'agentic'
            ORDER BY e.exp_id DESC
            LIMIT 200
        """)

        if paired.empty:
            st.info(
                "No paired runs found (same exp_id, both linear and agentic). "
                "Run experiments that produce both workflow types in the same experiment."
            )
        else:
            # For each pair, get last interaction response
            agreement_rows = []
            for _, pair in paired.iterrows():
                lin_resp = q1(
                    f"SELECT response, total_tokens FROM llm_interactions "
                    f"WHERE run_id={int(pair['lin_run_id'])} "
                    f"ORDER BY step_index DESC LIMIT 1"
                ) or {}
                agt_resp = q1(
                    f"SELECT response, total_tokens FROM llm_interactions "
                    f"WHERE run_id={int(pair['agt_run_id'])} "
                    f"ORDER BY step_index DESC LIMIT 1"
                ) or {}

                lin_text = str(lin_resp.get("response") or "")
                agt_text = str(agt_resp.get("response") or "")
                lin_toks = int(lin_resp.get("total_tokens") or 0)
                agt_toks = int(agt_resp.get("total_tokens") or 0)

                # Length similarity score (0-100): how similar are response lengths?
                if lin_text and agt_text:
                    len_diff = abs(len(lin_text) - len(agt_text))
                    len_max  = max(len(lin_text), len(agt_text), 1)
                    length_sim = round((1 - len_diff / len_max) * 100, 1)
                else:
                    length_sim = None

                # Token ratio
                tok_ratio = agt_toks / lin_toks if lin_toks > 0 else None

                # Energy tax
                energy_tax = (
                    float(pair["agt_energy_j"]) / float(pair["lin_energy_j"])
                    if float(pair.get("lin_energy_j") or 0) > 0 else None
                )

                agreement_rows.append({
                    "exp_id":       pair["exp_id"],
                    "task":         pair["task_name"],
                    "lin_run_id":   pair["lin_run_id"],
                    "agt_run_id":   pair["agt_run_id"],
                    "length_sim":   length_sim,
                    "tok_ratio":    tok_ratio,
                    "energy_tax":   energy_tax,
                    "lin_energy_j": pair["lin_energy_j"],
                    "agt_energy_j": pair["agt_energy_j"],
                    "lin_resp_preview": lin_text[:80] + "..." if len(lin_text) > 80 else lin_text,
                    "agt_resp_preview": agt_text[:80] + "..." if len(agt_text) > 80 else agt_text,
                })

            agree_df = pd.DataFrame(agreement_rows)

            col1, col2 = st.columns(2)

            with col1:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"Response length similarity (100 = identical length)</div>",
                    unsafe_allow_html=True,
                )
                sim_data = agree_df["length_sim"].dropna()
                if not sim_data.empty:
                    fig_sim = go.Figure(go.Histogram(
                        x=sim_data, nbinsx=20,
                        marker_color=ACCENT, marker_line_width=0, opacity=0.8,
                    ))
                    fig_sim.add_vline(
                        x=sim_data.mean(), line_dash="dot", line_color="#f1f5f9",
                        annotation_text=f"avg {sim_data.mean():.1f}%",
                        annotation_font_size=9,
                    )
                    fig_sim.update_layout(
                        **PL, height=240,
                        xaxis_title="Length similarity (%)",
                        yaxis_title="Pair count",
                    )
                    st.plotly_chart(fig_sim, use_container_width=True, key="qual_sim")

            with col2:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"Energy tax vs response similarity</div>",
                    unsafe_allow_html=True,
                )
                plot_df = agree_df.dropna(subset=["length_sim","energy_tax"])
                if not plot_df.empty:
                    fig_tax = go.Figure(go.Scatter(
                        x=plot_df["energy_tax"],
                        y=plot_df["length_sim"],
                        mode="markers",
                        text=plot_df["task"],
                        marker=dict(color="#ef4444", size=6, opacity=0.65),
                        hovertemplate="Task: %{text}<br>Tax: %{x:.2f}×<br>Sim: %{y:.1f}%<extra></extra>",
                    ))
                    fig_tax.add_vline(
                        x=1.0, line_dash="dot", line_color="#22c55e",
                        annotation_text="no overhead", annotation_font_size=9,
                    )
                    fig_tax.update_layout(
                        **PL, height=240,
                        xaxis_title="Energy tax (agentic/linear)",
                        yaxis_title="Response length similarity %",
                    )
                    st.plotly_chart(fig_tax, use_container_width=True, key="qual_tax_sim")

            # Disagreement table — pairs where responses diverge most
            if "length_sim" in agree_df.columns:
                diverged = agree_df[
                    agree_df["length_sim"].notna() &
                    (agree_df["length_sim"] < 50)
                ].sort_values("length_sim")

                if not diverged.empty:
                    st.markdown(
                        f"<div style='font-size:11px;font-weight:600;color:#ef4444;"
                        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                        f"High divergence pairs — same task, very different responses</div>",
                        unsafe_allow_html=True,
                    )
                    for _, row in diverged.head(5).iterrows():
                        st.markdown(
                            f"<div style='padding:10px 14px;background:#0d1117;"
                            f"border:1px solid #ef444433;border-left:3px solid #ef4444;"
                            f"border-radius:8px;margin-bottom:8px;'>"
                            f"<div style='font-size:11px;color:#f1f5f9;margin-bottom:6px;'>"
                            f"<b>{row['task']}</b> · exp {row['exp_id']} · "
                            f"similarity {row['length_sim']:.1f}%</div>"
                            f"<div style='display:grid;grid-template-columns:1fr 1fr;gap:8px;'>"
                            f"<div style='font-size:10px;color:#22c55e;'>"
                            f"Linear: {row['lin_resp_preview']}</div>"
                            f"<div style='font-size:10px;color:#ef4444;'>"
                            f"Agentic: {row['agt_resp_preview']}</div>"
                            f"</div></div>",
                            unsafe_allow_html=True,
                        )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 — STEP VERIFICATION
    # ══════════════════════════════════════════════════════════════════════════
    with tab4:
        st.markdown(
            f"<div style='font-size:11px;color:#94a3b8;margin-bottom:16px;'>"
            f"Per-step response analysis — check individual interactions "
            f"for quality signals, errors, and anomalies.</div>",
            unsafe_allow_html=True,
        )

        # Filter controls
        vc1, vc2, vc3 = st.columns(3)
        with vc1:
            task_opts = ["all"] + sorted(
                llm_df["task_name"].dropna().unique().tolist()
            ) if "task_name" in llm_df.columns else ["all"]
            sel_task = st.selectbox("Task filter", task_opts, key="qual_task_filter")
        with vc2:
            wf_opts = ["all"] + sorted(llm_df["workflow_type"].dropna().unique().tolist())
            sel_wf  = st.selectbox("Workflow", wf_opts, key="qual_wf_filter")
        with vc3:
            show_errors_only = st.checkbox("Errors only", key="qual_err_only")

        view = llm_df.copy()
        if sel_task != "all" and "task_name" in view.columns:
            view = view[view["task_name"] == sel_task]
        if sel_wf != "all":
            view = view[view["workflow_type"] == sel_wf]
        if show_errors_only and "status" in view.columns:
            view = view[view["status"] == "error"]

        st.markdown(
            f"<div style='font-size:10px;color:#475569;margin-bottom:12px;'>"
            f"Showing {len(view):,} interactions</div>",
            unsafe_allow_html=True,
        )

        # Quick anomaly detection — interactions with unusual token counts
        if "total_tokens" in view.columns:
            mu  = view["total_tokens"].mean()
            sig = view["total_tokens"].std()
            anomalies = view[
                view["total_tokens"].notna() &
                (view["total_tokens"] > mu + 2 * sig)
            ]
            if not anomalies.empty:
                st.markdown(
                    f"<div style='padding:8px 14px;background:#1a1000;"
                    f"border-left:3px solid #f59e0b;border-radius:0 8px 8px 0;"
                    f"font-size:11px;color:#fcd34d;"
                    f"font-family:IBM Plex Mono,monospace;margin-bottom:12px;'>"
                    f"⚠ {len(anomalies)} interactions have unusually high token counts "
                    f"(>{mu+2*sig:.0f} tokens = mean+2σ). "
                    f"These may indicate runaway responses or context overflow."
                    f"</div>",
                    unsafe_allow_html=True,
                )

        # Step table — paginated
        show_cols = ["run_id", "step_index", "workflow_type"]
        if "task_name"          in view.columns: show_cols.append("task_name")
        if "total_tokens"       in view.columns: show_cols.append("total_tokens")
        if "api_latency_ms"     in view.columns: show_cols.append("api_latency_ms")
        if "status"             in view.columns: show_cols.append("status")
        if "error_message"      in view.columns: show_cols.append("error_message")

        st.dataframe(
            view[show_cols].head(200).round(2),
            use_container_width=True, hide_index=True,
        )

        # Individual step deep dive
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            f"Inspect individual interaction</div>",
            unsafe_allow_html=True,
        )

        if not view.empty:
            sel_interaction = st.selectbox(
                "Interaction ID",
                view["interaction_id"].tolist()[:100],
                key="qual_interaction_sel",
            )
            row = view[view["interaction_id"] == sel_interaction].iloc[0]

            prompt   = str(row.get("prompt")   or "")
            response = str(row.get("response") or "")
            p_toks   = int(row.get("prompt_tokens", 0) or 0)
            c_toks   = int(row.get("completion_tokens", 0) or 0)
            lat      = float(row.get("api_latency_ms", 0) or 0)
            wait     = float(row.get("non_local_ms", 0) or 0)
            compute  = float(row.get("local_compute_ms", 0) or 0)

            # Quality signals for this interaction
            resp_len  = len(response)
            tok_ratio = c_toks / max(p_toks, 1)
            tps       = c_toks / (lat / 1000) if lat > 0 else 0

            sm1, sm2, sm3, sm4 = st.columns(4)
            for col, val, label, clr in [
                (sm1, f"{resp_len:,} chars",  "Response length",  ACCENT),
                (sm2, f"{tok_ratio:.2f}",      "c/p tok ratio",    "#22c55e"),
                (sm3, f"{tps:.1f} tok/s",      "Throughput",       "#3b82f6"),
                (sm4, f"{lat:.0f}ms",          "API latency",      "#f59e0b"),
            ]:
                with col:
                    st.markdown(
                        f"<div style='padding:8px 10px;background:#111827;"
                        f"border-left:2px solid {clr};border-radius:4px;"
                        f"margin-bottom:8px;'>"
                        f"<div style='font-size:14px;font-weight:700;color:{clr};"
                        f"font-family:IBM Plex Mono,monospace;'>{val}</div>"
                        f"<div style='font-size:9px;color:#475569;'>{label}</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

            pc, rc = st.columns(2)
            with pc:
                st.markdown(
                    f"<div style='font-size:9px;color:#475569;margin-bottom:4px;'>"
                    f"PROMPT ({p_toks} tokens)</div>"
                    f"<div style='font-size:11px;color:#94a3b8;line-height:1.6;"
                    f"padding:8px;background:#050c18;border-radius:6px;"
                    f"white-space:pre-wrap;max-height:300px;overflow-y:auto;'>"
                    f"{prompt}</div>",
                    unsafe_allow_html=True,
                )
            with rc:
                st.markdown(
                    f"<div style='font-size:9px;color:#475569;margin-bottom:4px;'>"
                    f"RESPONSE ({c_toks} tokens) · "
                    f"wait {wait:.0f}ms · compute {compute:.2f}ms</div>"
                    f"<div style='font-size:11px;color:#94a3b8;line-height:1.6;"
                    f"padding:8px;background:#050c18;border-radius:6px;"
                    f"white-space:pre-wrap;max-height:300px;overflow-y:auto;'>"
                    f"{response}</div>",
                    unsafe_allow_html=True,
                )
