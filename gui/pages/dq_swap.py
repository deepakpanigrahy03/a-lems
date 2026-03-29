"""
gui/pages/dq_swap.py  —  ⇅  Swap & Memory Pressure
─────────────────────────────────────────────────────────────────────────────
Swap delta analysis — not just a data quality flag but a research signal.

WHY SWAP DELTA MATTERS FOR ML TRAINING
────────────────────────────────────────
swap_delta = swap_end_used_mb - swap_start_used_mb

• swap_delta > 0  → run caused memory pressure → OS swapped pages to disk
                    → extra I/O energy not captured by RAPL
                    → run energy reading is UNDERSTATED
                    → these runs should be weighted differently in ML training

• swap_delta = 0  → no memory pressure → clean run → reliable energy reading

• swap_delta < 0  → OS reclaimed swap during run → system was already under
                    pressure before run started → baseline contaminated

ANALYTICAL VALUE
────────────────
1. Correlation: does swap_delta correlate with energy_j? (energy underreporting)
2. Workflow split: do agentic runs cause more swap pressure than linear?
3. Task split: which tasks trigger swap? (memory-hungry tasks)
4. Model split: local models (load into RAM) vs cloud (no local RAM pressure)
5. ML feature: swap_delta as input feature — "was this run memory-pressured?"
6. Exclusion signal: runs with swap_delta > threshold are unreliable for energy ML

─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.pages._dm_helpers import get_runs, no_data_banner, rgba

ACCENT       = "#38bdf8"
SWAP_WARNING = 50   # MB — runs above this are flagged as high-pressure
SWAP_SEVERE  = 200  # MB — runs above this are flagged as severe


def render(ctx: dict) -> None:

    df = get_runs(ctx)

    # ── Column check ──────────────────────────────────────────────────────────
    has_start = "swap_start_used_mb" in df.columns
    has_end   = "swap_end_used_mb"   in df.columns
    has_pct   = "swap_end_percent"   in df.columns

    if df.empty or (not has_start and not has_end):
        no_data_banner(
            "swap_start_used_mb / swap_end_used_mb not in runs table. "
            "Check load_runs() — these columns were added in the Phase 2 schema.",
            ACCENT,
        )
        return

    # ── Compute swap_delta ─────────────────────────────────────────────────────
    # swap_delta = how much swap changed DURING the run
    # Positive = run consumed swap (memory pressure)
    # Zero     = clean run, no swap pressure
    # Negative = swap was released during run (pre-existing pressure cleared)
    if has_start and has_end:
        df["swap_delta"] = (
            df["swap_end_used_mb"].fillna(0) - df["swap_start_used_mb"].fillna(0)
        )
    elif has_end:
        df["swap_delta"] = df["swap_end_used_mb"].fillna(0)
    else:
        df["swap_delta"] = 0.0

    # Classify runs by swap pressure
    def _classify(delta):
        if delta <= 0:       return "clean"
        if delta < SWAP_WARNING:  return "low_pressure"
        if delta < SWAP_SEVERE:   return "high_pressure"
        return "severe_pressure"

    df["swap_class"] = df["swap_delta"].apply(_classify)

    CLASS_COLORS = {
        "clean":           "#22c55e",
        "low_pressure":    "#f59e0b",
        "high_pressure":   "#f97316",
        "severe_pressure": "#ef4444",
    }
    CLASS_LABELS = {
        "clean":           "Clean (Δ≤0)",
        "low_pressure":    f"Low (0–{SWAP_WARNING}MB)",
        "high_pressure":   f"High ({SWAP_WARNING}–{SWAP_SEVERE}MB)",
        "severe_pressure": f"Severe (>{SWAP_SEVERE}MB)",
    }

    # ── Summary stats ─────────────────────────────────────────────────────────
    total          = len(df)
    n_clean        = int((df["swap_class"] == "clean").sum())
    n_low          = int((df["swap_class"] == "low_pressure").sum())
    n_high         = int((df["swap_class"] == "high_pressure").sum())
    n_severe       = int((df["swap_class"] == "severe_pressure").sum())
    n_pressured    = n_low + n_high + n_severe
    clean_pct      = round(n_clean / total * 100, 1) if total else 0
    avg_delta      = df["swap_delta"].mean()
    max_delta      = df["swap_delta"].max()

    # The zero-delta pattern — how many runs have EXACTLY zero swap delta?
    # This is important: if ALL runs show delta=0, swap monitoring may be broken
    n_exactly_zero = int((df["swap_delta"] == 0).sum())
    all_zero       = n_exactly_zero == total

    # ── Header ────────────────────────────────────────────────────────────────
    health_clr = (
        "#22c55e" if clean_pct >= 90 else
        "#f59e0b" if clean_pct >= 70 else
        "#ef4444"
    )

    st.markdown(
        f"<div style='padding:16px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Swap Delta Analysis — {total} runs</div>"
        f"<div style='display:grid;grid-template-columns:repeat(5,1fr);gap:12px;'>"
        + "".join([
            f"<div><div style='font-size:18px;font-weight:700;color:{c};"
            f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
            f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
            for v, l, c in [
                (f"{clean_pct}%",       "Clean runs",         health_clr),
                (n_pressured,           "Pressured runs",     "#f97316"),
                (n_severe,              "Severe pressure",    "#ef4444"),
                (f"{avg_delta:.1f} MB", "Avg swap delta",     ACCENT),
                (f"{max_delta:.0f} MB", "Max swap delta",     "#a78bfa"),
            ]
        ])
        + "</div></div>",
        unsafe_allow_html=True,
    )

    # ── Zero-delta warning ─────────────────────────────────────────────────────
    # If ALL runs have exactly zero delta, the swap monitoring is likely broken
    # or the machine has no swap configured. Important to flag this.
    if all_zero:
        st.markdown(
            f"<div style='padding:10px 16px;background:#1a1000;"
            f"border-left:3px solid #f59e0b;border-radius:0 8px 8px 0;"
            f"font-size:11px;color:#fcd34d;"
            f"font-family:IBM Plex Mono,monospace;margin-bottom:16px;'>"
            f"⚠ <b>Zero-delta pattern detected:</b> All {total} runs show swap_delta = 0. "
            f"This may mean: (1) machine has no swap configured, "
            f"(2) swap collection is not working, or "
            f"(3) all runs genuinely fit in RAM. "
            f"Check <code>swapon --show</code> on the lab machine."
            f"</div>",
            unsafe_allow_html=True,
        )
    elif n_exactly_zero > total * 0.95:
        st.markdown(
            f"<div style='padding:10px 16px;background:#0c1f3a;"
            f"border-left:3px solid #3b82f6;border-radius:0 8px 8px 0;"
            f"font-size:11px;color:#93c5fd;"
            f"font-family:IBM Plex Mono,monospace;margin-bottom:16px;'>"
            f"ℹ {n_exactly_zero}/{total} runs have swap_delta = 0 — "
            f"system has sufficient RAM for most workloads. "
            f"{n_pressured} runs did trigger swap pressure."
            f"</div>",
            unsafe_allow_html=True,
        )

    # ── ROW 1: Pressure class distribution + swap delta by workflow ───────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Run pressure classification</div>",
            unsafe_allow_html=True,
        )
        class_counts = df["swap_class"].value_counts()
        fig_pie = go.Figure(go.Bar(
            x=[CLASS_LABELS.get(k, k) for k in class_counts.index],
            y=class_counts.values,
            marker_color=[CLASS_COLORS.get(k, "#475569") for k in class_counts.index],
            marker_line_width=0,
        ))
        fig_pie.update_layout(
            **PL, height=240,
            showlegend=False,
            yaxis_title="Run count",
            xaxis_title="Swap pressure class",
        )
        st.plotly_chart(fig_pie, use_container_width=True, key="swap_class_bar")

    with col2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Swap delta by workflow type</div>",
            unsafe_allow_html=True,
        )
        fig_wf = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = df[df["workflow_type"] == wf]["swap_delta"].dropna()
            if sub.empty:
                continue
            fig_wf.add_trace(go.Box(
                y=sub,
                name=wf,
                marker_color=clr,
                boxmean=True,   # show mean as well as median
            ))
        fig_wf.add_hline(
            y=SWAP_WARNING, line_dash="dot", line_color="#f59e0b",
            annotation_text=f"warning ({SWAP_WARNING}MB)",
            annotation_font_size=9,
        )
        fig_wf.update_layout(
            **PL, height=240,
            yaxis_title="Swap delta (MB)",
            showlegend=True,
        )
        st.plotly_chart(fig_wf, use_container_width=True, key="swap_wf_box")

    # ── ROW 2: Swap delta vs energy — the key analytical insight ──────────────
    # If swap_delta correlates with energy_j, it means runs with memory pressure
    # have unreliable energy readings (extra I/O energy not captured by RAPL).
    # This is the most important chart for ML training data quality.
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Swap delta vs energy — memory pressure impact on measurements</div>",
        unsafe_allow_html=True,
    )

    has_energy = "energy_j" in df.columns
    col3, col4 = st.columns(2)

    with col3:
        fig_scatter = go.Figure()
        if has_energy:
            for cls, clr in CLASS_COLORS.items():
                sub = df[df["swap_class"] == cls].dropna(
                    subset=["swap_delta", "energy_j"]
                )
                if sub.empty:
                    continue
                fig_scatter.add_trace(go.Scatter(
                    x=sub["swap_delta"],
                    y=sub["energy_j"],
                    mode="markers",
                    name=CLASS_LABELS.get(cls, cls),
                    marker=dict(color=clr, size=5, opacity=0.65),
                ))
            # Add reference line at swap_delta=0 — the clean/pressured boundary
            fig_scatter.add_vline(
                x=0, line_dash="dot", line_color="#475569",
                annotation_text="Δ=0 boundary",
                annotation_font_size=9,
            )
        fig_scatter.update_layout(
            **PL, height=260,
            xaxis_title="Swap delta (MB)",
            yaxis_title="Energy (J)",
            showlegend=True,
        )
        st.plotly_chart(fig_scatter, use_container_width=True, key="swap_energy_scatter")

    with col4:
        # Mean energy by swap class — quantifies the impact
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Mean energy by pressure class</div>",
            unsafe_allow_html=True,
        )
        if has_energy:
            energy_by_class = (
                df.groupby("swap_class")["energy_j"]
                .agg(["mean", "std", "count"])
                .reset_index()
            )
            energy_by_class.columns = ["swap_class", "mean_j", "std_j", "count"]
            energy_by_class = energy_by_class.sort_values("mean_j")

            fig_ebc = go.Figure(go.Bar(
                x=[CLASS_LABELS.get(r, r) for r in energy_by_class["swap_class"]],
                y=energy_by_class["mean_j"],
                error_y=dict(type="data", array=energy_by_class["std_j"].tolist()),
                marker_color=[
                    CLASS_COLORS.get(r, "#475569")
                    for r in energy_by_class["swap_class"]
                ],
                marker_line_width=0,
                text=energy_by_class["count"].apply(lambda n: f"n={n}"),
                textposition="outside",
                textfont=dict(size=9),
            ))
            fig_ebc.update_layout(
                **PL, height=260,
                yaxis_title="Mean energy (J)",
                showlegend=False,
            )
            st.plotly_chart(fig_ebc, use_container_width=True, key="swap_energy_class")
        else:
            st.info("energy_j column not available.")

    # ── ROW 3: Swap delta by task and by model/provider ───────────────────────
    col5, col6 = st.columns(2)

    with col5:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Avg swap delta by task</div>",
            unsafe_allow_html=True,
        )
        if "task_name" in df.columns:
            task_swap = (
                df.groupby("task_name")["swap_delta"]
                .mean().sort_values(ascending=False)
                .reset_index()
            )
            task_swap.columns = ["task", "avg_delta"]
            task_swap["clr"] = task_swap["avg_delta"].apply(
                lambda v: "#ef4444" if v >= SWAP_SEVERE
                else "#f97316" if v >= SWAP_WARNING
                else "#f59e0b" if v > 0
                else "#22c55e"
            )
            fig_task = go.Figure(go.Bar(
                x=task_swap["task"],
                y=task_swap["avg_delta"],
                marker_color=task_swap["clr"].tolist(),
                marker_line_width=0,
            ))
            fig_task.add_hline(
                y=SWAP_WARNING, line_dash="dot", line_color="#f59e0b",
                annotation_text="warning threshold",
                annotation_font_size=9,
            )
            fig_task.update_layout(
                **PL, height=260,
                yaxis_title="Avg swap delta (MB)",
                xaxis_tickangle=-30,
                showlegend=False,
            )
            st.plotly_chart(fig_task, use_container_width=True, key="swap_task_bar")

    with col6:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Avg swap delta by provider</div>",
            unsafe_allow_html=True,
        )
        if "provider" in df.columns:
            prov_swap = (
                df.groupby("provider")["swap_delta"]
                .agg(["mean", "std", "count"])
                .reset_index()
            )
            prov_swap.columns = ["provider", "mean_delta", "std_delta", "count"]
            fig_prov = go.Figure(go.Bar(
                x=prov_swap["provider"],
                y=prov_swap["mean_delta"],
                error_y=dict(type="data", array=prov_swap["std_delta"].tolist()),
                marker_color=[
                    "#22c55e" if p == "local" else "#3b82f6"
                    for p in prov_swap["provider"]
                ],
                marker_line_width=0,
                text=prov_swap["count"].apply(lambda n: f"n={n}"),
                textposition="outside",
                textfont=dict(size=9),
            ))
            fig_prov.update_layout(
                **PL, height=260,
                yaxis_title="Avg swap delta (MB)",
                showlegend=False,
            )
            st.plotly_chart(fig_prov, use_container_width=True, key="swap_prov_bar")

    # ── ML Feature analysis ───────────────────────────────────────────────────
    # This section directly answers: should swap_delta be in the ML training set?
    # And if so, how should pressured runs be weighted?
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"ML training signal — swap_delta as a feature</div>",
        unsafe_allow_html=True,
    )

    # Compute correlation between swap_delta and key metrics
    ml_cols = ["energy_j", "duration_ms", "ipc", "api_latency_ms"]
    corr_rows = []
    for col in ml_cols:
        if col not in df.columns:
            continue
        sub = df[["swap_delta", col]].dropna()
        sub = sub[sub["swap_delta"].abs() < 1e6]  # sanity filter
        if len(sub) < 10:
            continue
        corr = sub["swap_delta"].corr(sub[col])
        corr_rows.append({
            "Metric":      col,
            "Correlation with swap_delta": round(corr, 4),
            "Strength":    (
                "strong"   if abs(corr) >= 0.5 else
                "moderate" if abs(corr) >= 0.3 else
                "weak"
            ),
            "Direction":   "↑ positive" if corr > 0 else "↓ negative",
            "n runs":      len(sub),
        })

    if corr_rows:
        corr_df = pd.DataFrame(corr_rows)
        st.dataframe(corr_df, use_container_width=True, hide_index=True)

    # Pressured vs clean energy difference — quantify the underreporting
    if has_energy:
        clean_energy    = df[df["swap_class"] == "clean"]["energy_j"].mean()
        pressured_energy = df[df["swap_class"] != "clean"]["energy_j"].mean()

        if pd.notna(clean_energy) and pd.notna(pressured_energy) and clean_energy > 0:
            diff_pct = ((pressured_energy - clean_energy) / clean_energy) * 100

            diff_clr = "#ef4444" if abs(diff_pct) > 20 else "#f59e0b" if abs(diff_pct) > 10 else "#22c55e"
            st.markdown(
                f"<div style='margin-top:12px;padding:12px 16px;"
                f"background:#111827;border:1px solid {diff_clr}33;"
                f"border-left:3px solid {diff_clr};border-radius:8px;"
                f"font-family:IBM Plex Mono,monospace;'>"
                f"<div style='font-size:11px;color:#f1f5f9;margin-bottom:6px;'>"
                f"<b>Energy measurement impact:</b></div>"
                f"<div style='display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;'>"
                f"<div><div style='font-size:16px;font-weight:700;color:#22c55e;'>"
                f"{clean_energy:.4f} J</div>"
                f"<div style='font-size:9px;color:#475569;'>Clean runs avg</div></div>"
                f"<div><div style='font-size:16px;font-weight:700;color:#ef4444;'>"
                f"{pressured_energy:.4f} J</div>"
                f"<div style='font-size:9px;color:#475569;'>Pressured runs avg</div></div>"
                f"<div><div style='font-size:16px;font-weight:700;color:{diff_clr};'>"
                f"{diff_pct:+.1f}%</div>"
                f"<div style='font-size:9px;color:#475569;'>Energy difference</div></div>"
                f"</div></div>",
                unsafe_allow_html=True,
            )

    # ── ML recommendation ─────────────────────────────────────────────────────
    # Based on the data, give a concrete recommendation for how to handle
    # swap_delta in the ML training pipeline.
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"ML training recommendations</div>",
        unsafe_allow_html=True,
    )

    recs = []

    # Recommendation 1: include swap_delta as a feature
    recs.append((
        "Include swap_delta as input feature",
        "swap_delta captures memory pressure state during the run. "
        "The ML model should know whether the run was clean or pressured "
        "so it can learn that pressured runs have less reliable energy readings.",
        "#22c55e",
    ))

    # Recommendation 2: binary pressure flag
    recs.append((
        "Add is_swap_pressured binary flag",
        f"Create a binary column: is_swap_pressured = (swap_delta > {SWAP_WARNING}). "
        "This is a cleaner feature than raw delta for tree-based models.",
        "#3b82f6",
    ))

    # Recommendation 3: conditional weighting
    if n_pressured > 0 and has_energy and clean_energy and pressured_energy:
        diff = abs(pressured_energy - clean_energy) / clean_energy * 100
        if diff > 10:
            recs.append((
                f"Weight pressured runs lower (energy diff: {diff:.1f}%)",
                f"Pressured runs show {diff:.1f}% energy difference vs clean runs. "
                f"For energy prediction tasks, consider sample_weight = 0.5 for pressured runs "
                f"or train a separate model for pressured vs clean execution environments.",
                "#f59e0b",
            ))

    # Recommendation 4: exclude severe cases
    if n_severe > 0:
        recs.append((
            f"Consider excluding {n_severe} severe-pressure runs (>{SWAP_SEVERE}MB)",
            f"Severe swap pressure (>{SWAP_SEVERE}MB delta) means the OS was actively "
            f"swapping during the run — I/O energy is significant but not measured by RAPL. "
            f"These {n_severe} runs may be outliers for energy prediction.",
            "#ef4444",
        ))

    for title, body, clr in recs:
        st.markdown(
            f"<div style='padding:10px 14px;background:#0d1117;"
            f"border:1px solid {clr}33;border-left:3px solid {clr};"
            f"border-radius:0 8px 8px 0;margin-bottom:8px;'>"
            f"<div style='font-size:11px;font-weight:600;color:{clr};"
            f"margin-bottom:4px;'>{title}</div>"
            f"<div style='font-size:10px;color:#94a3b8;line-height:1.6;"
            f"font-family:IBM Plex Mono,monospace;'>{body}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

    # ── Raw flagged runs table ─────────────────────────────────────────────────
    with st.expander(f"View pressured runs ({n_pressured} runs with swap_delta > 0)"):
        pressured_runs = df[df["swap_delta"] > 0].sort_values(
            "swap_delta", ascending=False
        )
        if pressured_runs.empty:
            st.info("No pressured runs.")
        else:
            show_cols = ["run_id", "workflow_type", "swap_delta", "swap_class"]
            if "task_name"  in df.columns: show_cols.append("task_name")
            if "provider"   in df.columns: show_cols.append("provider")
            if has_energy:                 show_cols.append("energy_j")
            if "duration_ms" in df.columns: show_cols.append("duration_ms")
            st.dataframe(
                pressured_runs[show_cols].round(3),
                use_container_width=True,
                hide_index=True,
            )
