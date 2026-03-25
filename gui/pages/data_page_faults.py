"""
gui/pages/data_page_faults.py  —  ⊘  Page Faults
─────────────────────────────────────────────────────────────────────────────
Page fault analysis — now unblocked after PF-1 fix.

Page faults happen when the CPU accesses a memory page not in RAM:
  Minor fault → page exists in memory but not mapped → cheap (~μs)
  Major fault → page must be loaded from disk → expensive (~ms, I/O energy)

WHY THIS MATTERS FOR ENERGY RESEARCH
──────────────────────────────────────
Major page faults = disk I/O = energy not captured by RAPL package domain.
This is the same problem as swap_delta — hidden energy cost.

Tab 1: Overview & validity flags
Tab 2: Energy impact — do faults correlate with higher energy?
Tab 3: Workflow & task analysis — which workloads fault most?
Tab 4: ML feature analysis — how to use page faults in training
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.pages._dm_helpers import get_runs, no_data_banner, rgba

ACCENT = "#f472b6"

# Configurable thresholds
MAJOR_FAULT_WARN   = 100    # major faults per run — worth flagging
MAJOR_FAULT_SEVERE = 1000   # severe memory pressure
MINOR_FAULT_WARN   = 50000  # minor faults — high but usually ok


def render(ctx: dict) -> None:
    df = get_runs(ctx)

    # ── Column check ──────────────────────────────────────────────────────────
    has_pf       = "page_faults"       in df.columns
    has_major_pf = "major_page_faults" in df.columns

    if df.empty or (not has_pf and not has_major_pf):
        no_data_banner(
            "page_faults / major_page_faults not in runs table. "
            "PF-1 fix should have added these — check load_runs() SELECT.",
            ACCENT,
        )
        return

    # Filter to rows with data
    mask = pd.Series(False, index=df.index)
    if has_pf:       mask |= df["page_faults"].notna()
    if has_major_pf: mask |= df["major_page_faults"].notna()
    df = df[mask].copy()

    if df.empty:
        no_data_banner("All page fault values are NULL.", ACCENT)
        return

    # Fill missing columns with 0
    if not has_pf:       df["page_faults"]       = 0
    if not has_major_pf: df["major_page_faults"]  = 0
    df["page_faults"]       = df["page_faults"].fillna(0)
    df["major_page_faults"] = df["major_page_faults"].fillna(0)
    df["minor_page_faults"] = (df["page_faults"] - df["major_page_faults"]).clip(lower=0)

    # Classify severity
    def _classify(row):
        mj = row["major_page_faults"]
        if mj >= MAJOR_FAULT_SEVERE: return "severe"
        if mj >= MAJOR_FAULT_WARN:   return "high"
        if mj > 0:                   return "low"
        return "clean"

    df["fault_class"] = df.apply(_classify, axis=1)

    CLASS_COLORS = {
        "clean":  "#22c55e",
        "low":    "#f59e0b",
        "high":   "#f97316",
        "severe": "#ef4444",
    }

    # ── Summary stats ─────────────────────────────────────────────────────────
    total       = len(df)
    n_clean     = int((df["fault_class"] == "clean").sum())
    n_low       = int((df["fault_class"] == "low").sum())
    n_high      = int((df["fault_class"] == "high").sum())
    n_severe    = int((df["fault_class"] == "severe").sum())
    avg_major   = df["major_page_faults"].mean()
    avg_minor   = df["minor_page_faults"].mean()
    max_major   = df["major_page_faults"].max()
    clean_pct   = round(n_clean / total * 100, 1) if total else 0

    # ── Header ────────────────────────────────────────────────────────────────
    health_clr = (
        "#22c55e" if clean_pct >= 90 else
        "#f59e0b" if clean_pct >= 70 else
        "#ef4444"
    )
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Page Fault Analysis — {total} runs</div>"
        f"<div style='display:grid;grid-template-columns:repeat(5,1fr);gap:12px;'>"
        + "".join([
            f"<div><div style='font-size:16px;font-weight:700;color:{c};"
            f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
            f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
            for v, l, c in [
                (f"{clean_pct}%",      "Clean runs",       health_clr),
                (n_severe,             "Severe (>1k major)", "#ef4444"),
                (n_high,               "High (>100 major)", "#f97316"),
                (f"{avg_major:.1f}",   "Avg major faults", "#f59e0b"),
                (f"{max_major:.0f}",   "Max major faults", "#a78bfa"),
            ]
        ])
        + "</div></div>",
        unsafe_allow_html=True,
    )

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs([
        "◎  Overview & flags",
        "⚡  Energy impact",
        "◈  Workflow & task",
        "🔬  ML features",
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — OVERVIEW & FLAGS
    # ══════════════════════════════════════════════════════════════════════════
    with tab1:

        # Classification bar chart
        col1, col2 = st.columns(2)

        with col1:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Run classification by major fault count</div>",
                unsafe_allow_html=True,
            )
            class_counts = df["fault_class"].value_counts()
            fig1 = go.Figure(go.Bar(
                x=[k.capitalize() for k in class_counts.index],
                y=class_counts.values,
                marker_color=[CLASS_COLORS.get(k, "#475569") for k in class_counts.index],
                marker_line_width=0,
                text=class_counts.values,
                textposition="outside",
                textfont=dict(size=10),
            ))
            fig1.update_layout(
                **PL, height=240,
                yaxis_title="Run count",
                showlegend=False,
            )
            st.plotly_chart(fig1, use_container_width=True, key="pf_class_bar")

        with col2:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Major vs minor faults — avg per run</div>",
                unsafe_allow_html=True,
            )
            wf_labels, major_vals, minor_vals = [], [], []
            for wf in ["linear", "agentic"]:
                sub = df[df["workflow_type"] == wf]
                if sub.empty: continue
                wf_labels.append(wf)
                major_vals.append(sub["major_page_faults"].mean())
                minor_vals.append(sub["minor_page_faults"].mean())

            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=wf_labels, y=major_vals, name="Major (disk I/O)",
                marker_color="#ef4444", marker_line_width=0,
            ))
            fig2.add_trace(go.Bar(
                x=wf_labels, y=minor_vals, name="Minor (remap only)",
                marker_color="#3b82f6", marker_line_width=0,
            ))
            fig2.add_hline(
                y=MAJOR_FAULT_WARN, line_dash="dot", line_color="#f59e0b",
                annotation_text=f"warn ({MAJOR_FAULT_WARN})", annotation_font_size=9,
            )
            fig2.update_layout(
                **PL, height=240, barmode="group",
                yaxis_title="Avg faults per run",
            )
            st.plotly_chart(fig2, use_container_width=True, key="pf_major_minor")

        # Distribution histograms
        col3, col4 = st.columns(2)
        with col3:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Major fault distribution</div>", unsafe_allow_html=True,
            )
            fig3 = go.Figure()
            for wf, clr in WF_COLORS.items():
                sub = df[df["workflow_type"] == wf]["major_page_faults"].dropna()
                if sub.empty: continue
                fig3.add_trace(go.Histogram(
                    x=sub, name=wf, marker_color=clr, opacity=0.7, nbinsx=40,
                ))
            fig3.add_vline(
                x=MAJOR_FAULT_WARN, line_dash="dot", line_color="#f59e0b",
                annotation_text="warn", annotation_font_size=9,
            )
            fig3.update_layout(
                **PL, height=220, barmode="overlay",
                xaxis_title="Major page faults", yaxis_title="Runs",
            )
            st.plotly_chart(fig3, use_container_width=True, key="pf_dist_major")

        with col4:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Minor fault distribution</div>", unsafe_allow_html=True,
            )
            fig4 = go.Figure()
            for wf, clr in WF_COLORS.items():
                sub = df[df["workflow_type"] == wf]["minor_page_faults"].dropna()
                if sub.empty: continue
                fig4.add_trace(go.Histogram(
                    x=sub, name=wf, marker_color=clr, opacity=0.7, nbinsx=40,
                ))
            fig4.update_layout(
                **PL, height=220, barmode="overlay",
                xaxis_title="Minor page faults", yaxis_title="Runs",
            )
            st.plotly_chart(fig4, use_container_width=True, key="pf_dist_minor")

        # Flagged runs table
        severe_runs = df[df["fault_class"].isin(["high", "severe"])].sort_values(
            "major_page_faults", ascending=False
        ).head(50)

        if not severe_runs.empty:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
                f"Flagged runs — high/severe major faults</div>",
                unsafe_allow_html=True,
            )
            show_cols = ["run_id", "workflow_type", "major_page_faults",
                         "minor_page_faults", "fault_class"]
            if "task_name"  in df.columns: show_cols.append("task_name")
            if "provider"   in df.columns: show_cols.append("provider")
            if "energy_j"   in df.columns: show_cols.append("energy_j")
            st.dataframe(
                severe_runs[show_cols].round(2),
                use_container_width=True, hide_index=True,
            )
        else:
            st.markdown(
                f"<div style='padding:16px;text-align:center;"
                f"border:1px solid #22c55e33;border-radius:8px;background:#052e1a22;'>"
                f"<div style='color:#22c55e;font-size:13px;'>✓ No high-severity page fault runs</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — ENERGY IMPACT
    # ══════════════════════════════════════════════════════════════════════════
    with tab2:
        has_energy = "energy_j" in df.columns

        st.markdown(
            f"<div style='font-size:11px;color:#94a3b8;margin-bottom:16px;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"Major page faults = disk I/O = energy not captured by RAPL. "
            f"Runs with high major faults have understated energy readings.</div>",
            unsafe_allow_html=True,
        )

        col1, col2 = st.columns(2)

        with col1:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Major faults vs energy</div>", unsafe_allow_html=True,
            )
            fig5 = go.Figure()
            if has_energy:
                for cls, clr in CLASS_COLORS.items():
                    sub = df[df["fault_class"] == cls].dropna(
                        subset=["major_page_faults", "energy_j"]
                    )
                    sub = sub[sub["energy_j"] > 0]
                    if sub.empty: continue
                    fig5.add_trace(go.Scatter(
                        x=sub["major_page_faults"], y=sub["energy_j"],
                        mode="markers", name=cls.capitalize(),
                        marker=dict(color=clr, size=5, opacity=0.65),
                    ))
                fig5.add_vline(
                    x=MAJOR_FAULT_WARN, line_dash="dot", line_color="#f59e0b",
                    annotation_text="warn", annotation_font_size=9,
                )
            fig5.update_layout(
                **PL, height=260,
                xaxis_title="Major page faults",
                yaxis_title="Energy (J)",
            )
            st.plotly_chart(fig5, use_container_width=True, key="pf_energy_scatter")

        with col2:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Mean energy by fault class</div>", unsafe_allow_html=True,
            )
            if has_energy:
                ebc = (
                    df.groupby("fault_class")["energy_j"]
                    .agg(["mean", "std", "count"]).reset_index()
                )
                ebc.columns = ["class", "mean_j", "std_j", "count"]
                fig6 = go.Figure(go.Bar(
                    x=[c.capitalize() for c in ebc["class"]],
                    y=ebc["mean_j"],
                    error_y=dict(type="data", array=ebc["std_j"].fillna(0).tolist()),
                    marker_color=[CLASS_COLORS.get(c, "#475569") for c in ebc["class"]],
                    marker_line_width=0,
                    text=ebc["count"].apply(lambda n: f"n={n}"),
                    textposition="outside", textfont=dict(size=9),
                ))
                fig6.update_layout(
                    **PL, height=260,
                    yaxis_title="Mean energy (J)", showlegend=False,
                )
                st.plotly_chart(fig6, use_container_width=True, key="pf_energy_class")
            else:
                st.info("energy_j not available.")

        # Correlation coefficient
        if has_energy:
            sub_corr = df[["major_page_faults", "energy_j"]].dropna()
            sub_corr = sub_corr[sub_corr["energy_j"] > 0]
            if len(sub_corr) >= 5:
                corr = sub_corr["major_page_faults"].corr(sub_corr["energy_j"])
                corr_clr = (
                    "#ef4444" if abs(corr) >= 0.5 else
                    "#f59e0b" if abs(corr) >= 0.3 else
                    "#22c55e"
                )
                st.markdown(
                    f"<div style='padding:10px 14px;background:#111827;"
                    f"border:1px solid {corr_clr}33;border-left:3px solid {corr_clr};"
                    f"border-radius:8px;display:flex;gap:14px;align-items:center;"
                    f"margin-bottom:12px;'>"
                    f"<div style='font-size:24px;font-weight:800;color:{corr_clr};"
                    f"font-family:IBM Plex Mono,monospace;'>{corr:.3f}</div>"
                    f"<div style='font-size:11px;color:#94a3b8;'>"
                    f"Pearson r (major_faults vs energy_j) · n={len(sub_corr)} runs<br>"
                    f"<span style='color:{corr_clr};'>"
                    + (
                        "Strong correlation — major faults significantly affect energy readings."
                        if abs(corr) >= 0.5 else
                        "Moderate correlation — faults have measurable energy impact."
                        if abs(corr) >= 0.3 else
                        "Weak correlation — faults have minimal energy impact at current levels."
                    )
                    + "</span></div></div>",
                    unsafe_allow_html=True,
                )

        # Energy comparison: clean vs faulting runs
        if has_energy:
            clean_e  = df[df["fault_class"] == "clean"]["energy_j"].mean()
            faulting = df[df["fault_class"] != "clean"]["energy_j"].mean()
            if pd.notna(clean_e) and pd.notna(faulting) and clean_e > 0:
                diff_pct = (faulting - clean_e) / clean_e * 100
                diff_clr = "#ef4444" if abs(diff_pct) > 20 else "#f59e0b" if abs(diff_pct) > 10 else "#22c55e"
                st.markdown(
                    f"<div style='padding:12px 16px;background:#111827;"
                    f"border:1px solid {diff_clr}33;border-left:3px solid {diff_clr};"
                    f"border-radius:8px;font-family:IBM Plex Mono,monospace;'>"
                    f"<div style='display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;'>"
                    f"<div><div style='font-size:18px;font-weight:700;color:#22c55e;'>"
                    f"{clean_e:.4f} J</div>"
                    f"<div style='font-size:9px;color:#475569;'>Clean runs avg</div></div>"
                    f"<div><div style='font-size:18px;font-weight:700;color:#ef4444;'>"
                    f"{faulting:.4f} J</div>"
                    f"<div style='font-size:9px;color:#475569;'>Faulting runs avg</div></div>"
                    f"<div><div style='font-size:18px;font-weight:700;color:{diff_clr};'>"
                    f"{diff_pct:+.1f}%</div>"
                    f"<div style='font-size:9px;color:#475569;'>Energy difference</div></div>"
                    f"</div></div>",
                    unsafe_allow_html=True,
                )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — WORKFLOW & TASK ANALYSIS
    # ══════════════════════════════════════════════════════════════════════════
    with tab3:

        col1, col2 = st.columns(2)

        with col1:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Major faults by workflow — box plot</div>", unsafe_allow_html=True,
            )
            fig7 = go.Figure()
            for wf, clr in WF_COLORS.items():
                sub = df[df["workflow_type"] == wf]["major_page_faults"].dropna()
                if sub.empty: continue
                fig7.add_trace(go.Box(
                    y=sub, name=wf, marker_color=clr,
                    line_color=clr, boxmean=True,
                ))
            fig7.add_hline(
                y=MAJOR_FAULT_WARN, line_dash="dot", line_color="#f59e0b",
                annotation_text="warn", annotation_font_size=9,
            )
            fig7.update_layout(
                **PL, height=260,
                yaxis_title="Major page faults", showlegend=False,
            )
            st.plotly_chart(fig7, use_container_width=True, key="pf_wf_box")

        with col2:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Avg major faults by task</div>", unsafe_allow_html=True,
            )
            if "task_name" in df.columns:
                task_faults = (
                    df.groupby("task_name")["major_page_faults"]
                    .mean().sort_values(ascending=False).reset_index()
                )
                task_faults.columns = ["task", "avg_major"]
                fig8 = go.Figure(go.Bar(
                    x=task_faults["task"],
                    y=task_faults["avg_major"],
                    marker_color=[
                        "#ef4444" if v >= MAJOR_FAULT_SEVERE else
                        "#f97316" if v >= MAJOR_FAULT_WARN   else
                        "#f59e0b" if v > 0 else
                        "#22c55e"
                        for v in task_faults["avg_major"]
                    ],
                    marker_line_width=0,
                ))
                fig8.add_hline(
                    y=MAJOR_FAULT_WARN, line_dash="dot", line_color="#f59e0b",
                    annotation_text="warn", annotation_font_size=9,
                )
                fig8.update_layout(
                    **PL, height=260,
                    yaxis_title="Avg major faults",
                    xaxis_tickangle=-30, showlegend=False,
                )
                st.plotly_chart(fig8, use_container_width=True, key="pf_task_bar")
            else:
                st.info("task_name not available.")

        # Provider split
        if "provider" in df.columns:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
                f"Faults by provider — local vs cloud</div>", unsafe_allow_html=True,
            )
            prov_faults = (
                df.groupby("provider")[["major_page_faults", "minor_page_faults"]]
                .mean().reset_index()
            )
            fig9 = go.Figure()
            fig9.add_trace(go.Bar(
                x=prov_faults["provider"],
                y=prov_faults["major_page_faults"],
                name="Major", marker_color="#ef4444", marker_line_width=0,
            ))
            fig9.add_trace(go.Bar(
                x=prov_faults["provider"],
                y=prov_faults["minor_page_faults"],
                name="Minor", marker_color="#3b82f6", marker_line_width=0,
            ))
            fig9.update_layout(
                **PL, height=240, barmode="group",
                yaxis_title="Avg faults per run",
            )
            st.plotly_chart(fig9, use_container_width=True, key="pf_prov_bar")

        # Trend over time
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            f"Major fault trend over time</div>", unsafe_allow_html=True,
        )
        trend = df[["run_id", "major_page_faults", "workflow_type"]].sort_values("run_id")
        trend["rolling"] = trend["major_page_faults"].rolling(20, min_periods=5).mean()
        fig10 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = trend[trend["workflow_type"] == wf]
            if sub.empty: continue
            fig10.add_trace(go.Scatter(
                x=sub["run_id"], y=sub["major_page_faults"],
                mode="markers", name=f"{wf} (raw)",
                marker=dict(color=clr, size=3, opacity=0.35),
            ))
        fig10.add_trace(go.Scatter(
            x=trend["run_id"], y=trend["rolling"],
            mode="lines", name="20-run rolling mean",
            line=dict(color=ACCENT, width=2),
        ))
        fig10.add_hline(
            y=MAJOR_FAULT_WARN, line_dash="dot", line_color="#f59e0b",
            annotation_text="warn", annotation_font_size=9,
        )
        fig10.update_layout(
            **PL, height=240,
            xaxis_title="Run ID", yaxis_title="Major page faults",
        )
        st.plotly_chart(fig10, use_container_width=True, key="pf_trend")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 — ML FEATURE ANALYSIS
    # ══════════════════════════════════════════════════════════════════════════
    with tab4:
        st.markdown(
            f"<div style='font-size:11px;color:#94a3b8;margin-bottom:16px;"
            f"font-family:IBM Plex Mono,monospace;line-height:1.8;'>"
            f"Major page faults are a proxy for memory pressure and hidden I/O energy. "
            f"They should be included in ML training as both a quality weight "
            f"(high fault runs are less reliable) and a feature "
            f"(fault rate predicts energy variance).</div>",
            unsafe_allow_html=True,
        )

        # Correlation with all numeric columns
        numeric = df.select_dtypes(include="number")
        corr_rows = []
        for col in ["energy_j", "duration_ms", "ipc", "api_latency_ms", "rss_memory_mb"]:
            if col not in numeric.columns: continue
            sub = numeric[["major_page_faults", col]].dropna()
            if len(sub) < 5: continue
            r = sub["major_page_faults"].corr(sub[col])
            if pd.isna(r): continue
            corr_rows.append({
                "Metric":            col,
                "r (major_faults)":  round(r, 4),
                "Strength":          "strong" if abs(r) >= 0.5 else "moderate" if abs(r) >= 0.3 else "weak",
                "n runs":            len(sub),
            })

        if corr_rows:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Correlation: major_page_faults vs key metrics</div>",
                unsafe_allow_html=True,
            )
            st.dataframe(pd.DataFrame(corr_rows), use_container_width=True, hide_index=True)

        # ML recommendations
        recs = [
            ("Include major_page_faults as input feature",
             "Directly captures memory pressure state. Correlated with hidden I/O energy "
             "not captured by RAPL. Strong signal for energy variance prediction.",
             "#22c55e"),
            ("Add is_memory_pressured binary flag",
             f"is_memory_pressured = (major_page_faults > {MAJOR_FAULT_WARN}). "
             "Cleaner categorical feature for tree-based models.",
             "#3b82f6"),
            ("Add log1p transform for ML",
             "major_page_faults has a heavy right tail — log1p(major_page_faults) "
             "normalises the distribution for linear models and neural networks.",
             "#a78bfa"),
            (f"Weight high-fault runs lower in energy prediction",
             f"Runs with major_page_faults > {MAJOR_FAULT_WARN} have understated energy "
             f"(disk I/O not measured by RAPL). Use sample_weight = 0.5 for these runs "
             f"when training energy prediction models.",
             "#f59e0b"),
        ]
        if n_severe > 0:
            recs.append((
                f"Consider excluding {n_severe} severe-fault runs (>{MAJOR_FAULT_SEVERE})",
                f"Severe fault runs (>{MAJOR_FAULT_SEVERE} major faults) have significant "
                f"unmeasured disk I/O energy. These {n_severe} runs are outliers "
                f"for energy prediction — mark as excluded in the outliers table.",
                "#ef4444",
            ))

        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            f"ML training recommendations</div>",
            unsafe_allow_html=True,
        )
        for title, body, clr in recs:
            st.markdown(
                f"<div style='padding:10px 14px;background:#0d1117;"
                f"border:1px solid {clr}33;border-left:3px solid {clr};"
                f"border-radius:8px;margin-bottom:8px;'>"
                f"<div style='font-size:11px;font-weight:600;color:{clr};"
                f"margin-bottom:4px;'>{title}</div>"
                f"<div style='font-size:10px;color:#94a3b8;line-height:1.6;"
                f"font-family:IBM Plex Mono,monospace;'>{body}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

        # Stats table
        with st.expander("Page fault statistics by workflow"):
            stats = (
                df.groupby("workflow_type")[["major_page_faults", "minor_page_faults"]]
                .describe().round(2)
            )
            st.dataframe(stats, use_container_width=True)
