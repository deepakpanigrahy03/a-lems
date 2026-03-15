"""
gui/pages/ml_features.py  —  ⊟  ML Features
─────────────────────────────────────────────────────────────────────────────
144-column ml_features view — correlation matrix, feature distributions,
export for training. Feature engineering workspace.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.db     import q, q1
from gui.config import PL, WF_COLORS

ACCENT = "#38bdf8"

# Key feature groups for analysis
FEATURE_GROUPS = {
    "Energy targets":    ["total_energy_j", "dynamic_energy_j", "orchestration_tax_j", "energy_per_token",
                          "baseline_package_power"],
    "CPU performance":   ["ipc", "cache_miss_rate", "frequency_mhz", "instructions", "cycles"],
    "Memory":            ["rss_memory_mb", "vms_memory_mb", "page_faults", "major_page_faults"],
    "Network/Latency":   ["api_latency_ms", "dns_latency_ms", "compute_time_ms",
                          "bytes_sent", "bytes_recv"],
    "Thermal":           ["package_temp_celsius", "thermal_delta_c", "start_temp_c"],
    "Agentic":           ["planning_time_ms", "execution_time_ms", "synthesis_time_ms",
                          "llm_calls", "tool_calls", "steps", "complexity_score"],
    "Sustainability":    ["carbon_g", "water_ml", "methane_mg"],
    "System":            ["interrupt_rate", "total_context_switches", "run_queue_length",
                          "background_cpu_percent"],
}


def render(ctx: dict) -> None:
    total = q1("SELECT COUNT(*) AS n FROM ml_features").get("n", 0) or 0

    if total == 0:
        st.info("No data in ml_features view yet.")
        return

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:6px;'>"
        f"ML Features — {total:,} rows</div>"
        f"<div style='font-size:11px;color:#94a3b8;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"Feature engineering workspace · 144 columns · "
        f"Correlation analysis · Distribution explorer</div>"
        f"</div>",
        unsafe_allow_html=True)

    # Load sample for analysis
    df = q("SELECT * FROM ml_features ORDER BY run_id DESC LIMIT 1000")
    if df.empty:
        st.info("No data.")
        return

    # ── Tab layout ────────────────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs([
        "Correlation with energy",
        "Feature distributions",
        "Feature groups"
    ])

    with tab1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
            f"Pearson correlation with total_energy_j</div>",
            unsafe_allow_html=True)

        if "total_energy_j" not in df.columns:
            st.info("total_energy_j not available.")
        else:
            numeric = df.select_dtypes(include="number")
            corrs = []
            for col in numeric.columns:
                if col in ("total_energy_j", "run_id"): continue
                try:
                    pair = numeric[["total_energy_j", col]].dropna()
                    if len(pair) < 10: continue
                    r = pair.corr().iloc[0,1]
                    if pd.isna(r): continue
                    corrs.append((col, round(r, 4)))
                except Exception:
                    pass

            corrs.sort(key=lambda x: abs(x[1]), reverse=True)
            top_corrs = corrs[:30]

            cols_n = [c[0] for c in top_corrs]
            vals   = [c[1] for c in top_corrs]
            colors = ["#22c55e" if v > 0 else "#ef4444" for v in vals]

            fig = go.Figure(go.Bar(
                x=vals, y=cols_n,
                orientation="h",
                marker_color=colors,
                marker_line_width=0,
                text=[f"{v:+.4f}" for v in vals],
                textposition="outside",
                textfont=dict(size=9)))
            fig.add_vline(x=0, line_color="#475569", line_width=1)
            fig.update_layout(
                **{**PL, "margin": dict(l=180, r=80, t=10, b=30)},
                height=max(300, len(top_corrs) * 22),
                xaxis_title="Pearson r with total_energy_j",
                showlegend=False)
            st.plotly_chart(fig, use_container_width=True, key="mlf_corr")

    with tab2:
        sel_feature = st.selectbox(
            "Select feature to explore",
            [c for c in df.select_dtypes(include="number").columns
             if c not in ("run_id",)],
            key="mlf_feat_sel")

        if sel_feature in df.columns:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"Distribution — {sel_feature}</div>",
                    unsafe_allow_html=True)
                fig2 = go.Figure()
                for wf, clr in WF_COLORS.items():
                    if "workflow_type" not in df.columns: break
                    sub = df[df["workflow_type"] == wf][sel_feature].dropna()
                    if sub.empty: continue
                    fig2.add_trace(go.Histogram(
                        x=sub, name=wf, marker_color=clr,
                        opacity=0.7, nbinsx=40))
                fig2.update_layout(**PL, height=260, barmode="overlay",
                                   xaxis_title=sel_feature,
                                   yaxis_title="Count")
                st.plotly_chart(fig2, use_container_width=True, key="mlf_dist")

            with col2:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"{sel_feature} vs total_energy_j</div>",
                    unsafe_allow_html=True)
                fig3 = go.Figure()
                if "total_energy_j" in df.columns:
                    for wf, clr in WF_COLORS.items():
                        if "workflow_type" not in df.columns: break
                        sub = df[df["workflow_type"] == wf]
                        sub = sub[[sel_feature, "total_energy_j"]].dropna()
                        sub = sub[sub["total_energy_j"] > 0]
                        if sub.empty: continue
                        fig3.add_trace(go.Scatter(
                            x=sub[sel_feature], y=sub["total_energy_j"],
                            mode="markers", name=wf,
                            marker=dict(color=clr, size=4, opacity=0.5)))
                fig3.update_layout(**PL, height=260,
                                   xaxis_title=sel_feature,
                                   yaxis_title="Energy (J)")
                st.plotly_chart(fig3, use_container_width=True, key="mlf_scatter")

            # Stats
            stats = df[sel_feature].dropna().describe()
            st.dataframe(pd.DataFrame(stats).T.round(4), use_container_width=True)

    with tab3:
        for group_name, cols in FEATURE_GROUPS.items():
            avail = [c for c in cols if c in df.columns]
            if not avail: continue
            with st.expander(f"{group_name} — {len(avail)} features", expanded=False):
                stats_df = df[avail].describe().T.round(4)
                # Add null count
                stats_df["null_count"] = df[avail].isna().sum()
                stats_df["null_pct"]   = (df[avail].isna().mean() * 100).round(1)
                st.dataframe(stats_df, use_container_width=True)
