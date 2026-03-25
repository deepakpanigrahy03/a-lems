"""
gui/pages/ml_features_page.py  —  ⊟  ML Features
─────────────────────────────────────────────────────────────────────────────
144-column ml_features view — correlation matrix, feature distributions,
export for training. Feature engineering workspace.

Phase 2 update:
  • Updated FEATURE_GROUPS with bytes_sent/recv, baseline_power,
    orchestration_tax_j as target, page faults, swap columns
  • Tab 4: Export workspace — filtered export, quality scoring
  • Tab 5: Target analysis — which columns to predict
  • Deeper correlation explorer with search
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1

ACCENT = "#38bdf8"

# ── Feature groups — updated to include all Phase 2 additions ─────────────────
FEATURE_GROUPS = {
    # Primary prediction targets — what the ML model should output
    "Energy targets (predict these)": [
        "total_energy_j",
        "dynamic_energy_j",
        "orchestration_tax_j",
        "energy_per_token",
        "baseline_package_power",
        "computed_dynamic_j",        # baseline-adjusted energy
    ],

    # CPU performance features
    "CPU performance": [
        "ipc",
        "cache_miss_rate",
        "frequency_mhz",
        "instructions",
        "cycles",
        "c2_time_seconds",
        "c3_time_seconds",
        "c6_time_seconds",
        "c7_time_seconds",
        "efficiency_score",          # C-state efficiency score
    ],

    # Memory pressure features
    "Memory & page faults": [
        "rss_memory_mb",
        "vms_memory_mb",
        "page_faults",
        "major_page_faults",
        "swap_start_used_mb",
        "swap_end_used_mb",
        "swap_delta",                # computed: swap_end - swap_start
    ],

    # Network and I/O features — Phase 2 additions
    "Network & I/O": [
        "api_latency_ms",
        "dns_latency_ms",
        "compute_time_ms",
        "bytes_sent",
        "bytes_recv",
        "tcp_retransmits",
        "bytes_total",               # computed: sent + recv
        "throughput_kbps",           # computed: bytes_total / duration_s
    ],

    # Thermal features
    "Thermal": [
        "package_temp_celsius",
        "thermal_delta_c",
        "start_temp_c",
        "avg_wifi_temp",             # from thermal zones
    ],

    # Agentic orchestration features
    "Agentic orchestration": [
        "planning_time_ms",
        "execution_time_ms",
        "synthesis_time_ms",
        "llm_calls",
        "tool_calls",
        "steps",
        "complexity_score",
        "ooi_time",                  # orchestration overhead index
        "ooi_cpu",
        "ucr",                       # useful compute ratio
        "network_ratio",
    ],

    # Sustainability / environment features
    "Sustainability": [
        "carbon_g",
        "water_ml",
        "methane_mg",
    ],

    # System noise features — quality weights for ML
    "System quality (use as weights)": [
        "interrupt_rate",
        "total_context_switches",
        "background_cpu_percent",
        "baseline_bg_cpu",
        "idle_fraction_pct",         # how much of run was idle baseline
    ],

    # Baseline features — normalisation factors
    "Baseline & hardware": [
        "baseline_pkg_w",
        "baseline_core_w",
        "governor",
        "turbo",
        "hw_id",
        "cpu_model",
        "ram_gb",
    ],
}

# Columns that should be log-transformed before ML training
# (heavy right tails — normalise distribution)
LOG_TRANSFORM_COLS = [
    "major_page_faults", "page_faults", "bytes_sent", "bytes_recv",
    "tcp_retransmits", "api_latency_ms", "total_tokens",
]


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
        f"Correlation analysis · Distribution explorer · Export</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Load sample for analysis
    df = q("SELECT * FROM ml_features ORDER BY run_id DESC LIMIT 1000")
    if df.empty:
        st.info("No data.")
        return

    # Compute derived columns if base columns exist
    if "swap_start_used_mb" in df.columns and "swap_end_used_mb" in df.columns:
        df["swap_delta"] = (
            df["swap_end_used_mb"].fillna(0) - df["swap_start_used_mb"].fillna(0)
        )
    if "bytes_sent" in df.columns and "bytes_recv" in df.columns:
        df["bytes_total"] = (
            df["bytes_sent"].fillna(0) + df["bytes_recv"].fillna(0)
        )

    total_cols    = len(df.columns)
    numeric_cols  = df.select_dtypes(include="number").columns.tolist()
    n_numeric     = len(numeric_cols)
    null_pct_avg  = round(df.isna().mean().mean() * 100, 1)

    # ── Quick stats ────────────────────────────────────────────────────────────
    qs1, qs2, qs3, qs4 = st.columns(4)
    for col, val, label, clr in [
        (qs1, total,        "Rows in view",        ACCENT),
        (qs2, total_cols,   "Total columns",       "#a78bfa"),
        (qs3, n_numeric,    "Numeric columns",     "#22c55e"),
        (qs4, f"{null_pct_avg}%", "Avg null rate", "#f59e0b"),
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

    # ── Tab layout ─────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "◈  Correlation explorer",
        "⊞  Feature distributions",
        "◧  Feature groups",
        "⬡  Export workspace",
        "⚡  Target analysis",
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — CORRELATION EXPLORER
    # ══════════════════════════════════════════════════════════════════════════
    with tab1:

        tc1, tc2 = st.columns([2, 1])
        with tc1:
            target_col = st.selectbox(
                "Correlation target",
                [c for c in ["total_energy_j", "dynamic_energy_j",
                              "orchestration_tax_j", "energy_per_token"]
                 if c in df.columns]
                + [c for c in numeric_cols if c not in
                   ["total_energy_j", "dynamic_energy_j",
                    "orchestration_tax_j", "energy_per_token", "run_id"]],
                key="mlf_target",
            )
        with tc2:
            top_n = st.slider("Top N features", 10, 50, 30, 5, key="mlf_topn")

        if target_col in df.columns:
            numeric = df.select_dtypes(include="number")
            corrs = []
            for col in numeric.columns:
                if col in (target_col, "run_id"):
                    continue
                try:
                    pair = numeric[[target_col, col]].dropna()
                    if len(pair) < 10:
                        continue
                    r = pair.corr().iloc[0, 1]
                    if pd.isna(r):
                        continue
                    corrs.append((col, round(r, 4)))
                except Exception:
                    pass

            corrs.sort(key=lambda x: abs(x[1]), reverse=True)
            top = corrs[:top_n]

            if top:
                cols_n = [c[0] for c in top]
                vals   = [c[1] for c in top]
                colors = [
                    "#22c55e" if v > 0.5 else "#38bdf8" if v > 0 else
                    "#f97316" if v > -0.5 else "#ef4444"
                    for v in vals
                ]

                fig = go.Figure(go.Bar(
                    x=vals, y=cols_n,
                    orientation="h",
                    marker_color=colors, marker_line_width=0,
                    text=[f"{v:+.4f}" for v in vals],
                    textposition="outside", textfont=dict(size=9),
                ))
                fig.add_vline(x=0, line_color="#475569", line_width=1)
                fig.add_vline(x=0.3,  line_dash="dot", line_color="#22c55e",
                              line_width=1, annotation_text="moderate+",
                              annotation_font_size=8)
                fig.add_vline(x=-0.3, line_dash="dot", line_color="#22c55e",
                              line_width=1)
                fig.update_layout(
                    **{**PL, "margin": dict(l=200, r=100, t=10, b=30)},
                    height=max(300, len(top) * 22),
                    xaxis_title=f"Pearson r with {target_col}",
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True, key="mlf_corr")

                # Top 5 summary
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                    f"text-transform:uppercase;letter-spacing:.1em;margin:12px 0 8px;'>"
                    f"Top 5 strongest predictors of {target_col}</div>",
                    unsafe_allow_html=True,
                )
                for col_n, r_val in top[:5]:
                    clr = "#22c55e" if r_val > 0 else "#ef4444"
                    bar_w = int(abs(r_val) * 100)
                    st.markdown(
                        f"<div style='display:flex;align-items:center;gap:10px;"
                        f"padding:6px 0;border-bottom:0.5px solid #1f2937;'>"
                        f"<div style='width:180px;font-size:11px;"
                        f"color:#f1f5f9;font-family:IBM Plex Mono,monospace;'>"
                        f"{col_n}</div>"
                        f"<div style='flex:1;background:#1f2937;border-radius:3px;"
                        f"height:12px;overflow:hidden;'>"
                        f"<div style='width:{bar_w}%;background:{clr};height:100%;'></div>"
                        f"</div>"
                        f"<div style='width:60px;text-align:right;font-size:11px;"
                        f"color:{clr};font-family:IBM Plex Mono,monospace;font-weight:700;'>"
                        f"{r_val:+.4f}</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — FEATURE DISTRIBUTIONS
    # ══════════════════════════════════════════════════════════════════════════
    with tab2:
        sel_feature = st.selectbox(
            "Select feature to explore",
            [c for c in numeric_cols if c not in ("run_id",)],
            key="mlf_feat_sel",
        )

        if sel_feature in df.columns:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"Distribution — {sel_feature}</div>", unsafe_allow_html=True,
                )
                fig2 = go.Figure()
                for wf, clr in WF_COLORS.items():
                    if "workflow_type" not in df.columns: break
                    sub = df[df["workflow_type"] == wf][sel_feature].dropna()
                    if sub.empty: continue
                    fig2.add_trace(go.Histogram(
                        x=sub, name=wf, marker_color=clr, opacity=0.7, nbinsx=40,
                    ))
                fig2.update_layout(
                    **PL, height=260, barmode="overlay",
                    xaxis_title=sel_feature, yaxis_title="Count",
                )
                st.plotly_chart(fig2, use_container_width=True, key="mlf_dist")

            with col2:
                target = "total_energy_j"
                st.markdown(
                    f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                    f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                    f"{sel_feature} vs {target}</div>", unsafe_allow_html=True,
                )
                fig3 = go.Figure()
                if target in df.columns:
                    for wf, clr in WF_COLORS.items():
                        if "workflow_type" not in df.columns: break
                        sub = df[df["workflow_type"] == wf]
                        sub = sub[[sel_feature, target]].dropna()
                        sub = sub[sub[target] > 0]
                        if sub.empty: continue
                        fig3.add_trace(go.Scatter(
                            x=sub[sel_feature], y=sub[target],
                            mode="markers", name=wf,
                            marker=dict(color=clr, size=4, opacity=0.5),
                        ))
                fig3.update_layout(
                    **PL, height=260,
                    xaxis_title=sel_feature, yaxis_title=f"{target} (J)",
                )
                st.plotly_chart(fig3, use_container_width=True, key="mlf_scatter")

            # Stats + log transform suggestion
            stats = df[sel_feature].dropna().describe()
            st.dataframe(pd.DataFrame(stats).T.round(4), use_container_width=True)

            if sel_feature in LOG_TRANSFORM_COLS:
                st.markdown(
                    f"<div style='padding:8px 14px;background:#1a0e00;"
                    f"border-left:3px solid #f59e0b;border-radius:0 8px 8px 0;"
                    f"font-size:11px;color:#fcd34d;"
                    f"font-family:IBM Plex Mono,monospace;margin-top:8px;'>"
                    f"⚠ <b>{sel_feature}</b> has a heavy right tail. "
                    f"Apply <code>log1p({sel_feature})</code> before ML training "
                    f"to normalise the distribution.</div>",
                    unsafe_allow_html=True,
                )

            # Box plot by workflow
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin:12px 0 8px;'>"
                f"Box plot by workflow</div>", unsafe_allow_html=True,
            )
            fig_box = go.Figure()
            for wf, clr in WF_COLORS.items():
                if "workflow_type" not in df.columns: break
                sub = df[df["workflow_type"] == wf][sel_feature].dropna()
                if sub.empty: continue
                fig_box.add_trace(go.Box(
                    y=sub, name=wf, marker_color=clr,
                    line_color=clr, boxmean=True,
                ))
            fig_box.update_layout(
                **PL, height=220, yaxis_title=sel_feature, showlegend=False,
            )
            st.plotly_chart(fig_box, use_container_width=True, key="mlf_box")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — FEATURE GROUPS
    # ══════════════════════════════════════════════════════════════════════════
    with tab3:

        # Null rate heatmap across all groups
        all_group_cols = []
        for cols in FEATURE_GROUPS.values():
            all_group_cols.extend([c for c in cols if c in df.columns])
        all_group_cols = list(dict.fromkeys(all_group_cols))  # deduplicate

        if all_group_cols:
            null_rates = df[all_group_cols].isna().mean() * 100
            fig_null = go.Figure(go.Bar(
                x=all_group_cols,
                y=null_rates.values,
                marker_color=[
                    "#ef4444" if v > 50 else "#f59e0b" if v > 20 else "#22c55e"
                    for v in null_rates.values
                ],
                marker_line_width=0,
            ))
            fig_null.add_hline(
                y=50, line_dash="dot", line_color="#ef4444",
                annotation_text="50% null — unreliable", annotation_font_size=9,
            )
            fig_null.update_layout(
                **PL, height=220,
                xaxis_tickangle=-45,
                yaxis_title="Null %",
                showlegend=False,
            )
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Null rate across feature groups</div>",
                unsafe_allow_html=True,
            )
            st.plotly_chart(fig_null, use_container_width=True, key="mlf_null_heatmap")

        # Per-group expanders with stats
        for group_name, cols in FEATURE_GROUPS.items():
            avail = [c for c in cols if c in df.columns]
            missing = [c for c in cols if c not in df.columns]
            if not avail:
                continue
            with st.expander(
                f"{group_name} — {len(avail)} available"
                + (f", {len(missing)} missing" if missing else ""),
                expanded=False,
            ):
                if missing:
                    st.markdown(
                        f"<div style='font-size:10px;color:#475569;"
                        f"font-family:IBM Plex Mono,monospace;margin-bottom:8px;'>"
                        f"Missing from view: {', '.join(missing)}</div>",
                        unsafe_allow_html=True,
                    )
                stats_df = df[avail].describe().T.round(4)
                stats_df["null_count"] = df[avail].isna().sum()
                stats_df["null_pct"]   = (df[avail].isna().mean() * 100).round(1)
                st.dataframe(stats_df, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 — EXPORT WORKSPACE
    # ══════════════════════════════════════════════════════════════════════════
    with tab4:
        st.markdown(
            f"<div style='font-size:11px;color:#94a3b8;margin-bottom:16px;'>"
            f"Filter, score, and export the ML feature dataset. "
            f"Apply quality filters before training.</div>",
            unsafe_allow_html=True,
        )

        # Quality filters
        ef1, ef2, ef3 = st.columns(3)
        with ef1:
            excl_throttled = st.checkbox(
                "Exclude thermal throttled runs", value=True, key="exp_throttle"
            )
            excl_no_baseline = st.checkbox(
                "Exclude runs without baseline", value=True, key="exp_baseline"
            )
        with ef2:
            excl_high_bg = st.checkbox(
                "Exclude noisy env (BG CPU > 20%)", value=True, key="exp_bg"
            )
            max_major_faults = st.number_input(
                "Max major page faults", 0, 100000, 1000, 100, key="exp_pf"
            )
        with ef3:
            wf_select = st.multiselect(
                "Workflow types",
                ["linear", "agentic"],
                default=["linear", "agentic"],
                key="exp_wf",
            )
            provider_select = st.multiselect(
                "Providers",
                df["provider"].dropna().unique().tolist() if "provider" in df.columns else [],
                default=df["provider"].dropna().unique().tolist() if "provider" in df.columns else [],
                key="exp_prov",
            )

        # Apply filters
        export_df = df.copy()
        if excl_throttled and "thermal_throttle_flag" in export_df.columns:
            export_df = export_df[
                export_df["thermal_throttle_flag"].fillna(0) != 1
            ]
        if excl_no_baseline and "baseline_id" in export_df.columns:
            export_df = export_df[export_df["baseline_id"].notna()]
        if excl_high_bg and "background_cpu_percent" in export_df.columns:
            export_df = export_df[
                export_df["background_cpu_percent"].fillna(0) <= 20
            ]
        if "major_page_faults" in export_df.columns:
            export_df = export_df[
                export_df["major_page_faults"].fillna(0) <= max_major_faults
            ]
        if wf_select and "workflow_type" in export_df.columns:
            export_df = export_df[export_df["workflow_type"].isin(wf_select)]
        if provider_select and "provider" in export_df.columns:
            export_df = export_df[export_df["provider"].isin(provider_select)]

        # Export stats
        n_before = len(df)
        n_after  = len(export_df)
        pct_kept = round(n_after / n_before * 100, 1) if n_before else 0

        st.markdown(
            f"<div style='padding:10px 14px;background:#0c1f3a;"
            f"border:1px solid #3b82f633;border-radius:8px;margin-bottom:12px;"
            f"font-family:IBM Plex Mono,monospace;font-size:11px;'>"
            f"<span style='color:#94a3b8;'>Filter result: </span>"
            f"<span style='color:#f1f5f9;font-weight:700;'>{n_after:,}</span>"
            f"<span style='color:#94a3b8;'> / {n_before:,} rows kept "
            f"({pct_kept}%)</span></div>",
            unsafe_allow_html=True,
        )

        # Column selection for export
        numeric_export = export_df.select_dtypes(include="number").columns.tolist()
        selected_cols = st.multiselect(
            "Columns to export",
            numeric_export,
            default=[c for c in [
                "run_id", "total_energy_j", "dynamic_energy_j", "ipc",
                "cache_miss_rate", "api_latency_ms", "major_page_faults",
                "bytes_sent", "bytes_recv", "planning_time_ms",
                "execution_time_ms", "synthesis_time_ms",
            ] if c in numeric_export],
            key="exp_cols",
        )

        if selected_cols:
            csv_df = export_df[selected_cols].round(6)
            st.download_button(
                "📥 Export filtered CSV",
                csv_df.to_csv(index=False),
                file_name="alems_ml_features.csv",
                mime="text/csv",
                use_container_width=True,
                key="mlf_export_btn",
            )
            st.markdown(
                f"<div style='font-size:10px;color:#475569;margin-top:4px;'>"
                f"{len(selected_cols)} columns · {n_after:,} rows · "
                f"ready for sklearn / PyTorch / XGBoost</div>",
                unsafe_allow_html=True,
            )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 5 — TARGET ANALYSIS
    # ══════════════════════════════════════════════════════════════════════════
    with tab5:
        st.markdown(
            f"<div style='font-size:11px;color:#94a3b8;margin-bottom:16px;'>"
            f"Understand the distribution of prediction targets "
            f"before choosing a loss function and model architecture.</div>",
            unsafe_allow_html=True,
        )

        target_cols = [c for c in [
            "total_energy_j", "dynamic_energy_j", "orchestration_tax_j",
            "energy_per_token", "baseline_package_power",
        ] if c in df.columns]

        if not target_cols:
            st.info("No target columns available in ml_features view.")
            return

        for tgt in target_cols:
            sub = df[tgt].dropna()
            sub = sub[sub > 0]
            if sub.empty:
                continue

            with st.expander(f"Target: {tgt} — {len(sub):,} rows", expanded=(tgt == target_cols[0])):
                ta1, ta2 = st.columns(2)
                with ta1:
                    # Distribution
                    fig_t = go.Figure()
                    for wf, clr in WF_COLORS.items():
                        if "workflow_type" not in df.columns: break
                        s = df[df["workflow_type"] == wf][tgt].dropna()
                        s = s[s > 0]
                        if s.empty: continue
                        fig_t.add_trace(go.Histogram(
                            x=s, name=wf, marker_color=clr,
                            opacity=0.7, nbinsx=40,
                        ))
                    fig_t.update_layout(
                        **PL, height=220, barmode="overlay",
                        xaxis_title=tgt, yaxis_title="Count",
                    )
                    st.plotly_chart(fig_t, use_container_width=True, key=f"tgt_dist_{tgt}")

                with ta2:
                    # Key stats
                    mean_v = sub.mean()
                    std_v  = sub.std()
                    cv     = std_v / mean_v if mean_v > 0 else 0
                    skew   = sub.skew()

                    st.markdown(
                        f"<div style='padding:12px;background:#111827;border-radius:8px;'>"
                        f"<div style='font-size:10px;color:#475569;margin-bottom:8px;"
                        f"text-transform:uppercase;letter-spacing:.08em;'>Statistics</div>"
                        + "".join([
                            f"<div style='display:flex;justify-content:space-between;"
                            f"padding:4px 0;border-bottom:0.5px solid #1f2937;"
                            f"font-size:11px;'>"
                            f"<span style='color:#475569;font-family:IBM Plex Mono,'>{k}</span>"
                            f"<span style='color:{c};font-family:IBM Plex Mono,monospace;"
                            f"font-weight:600;'>{v}</span></div>"
                            for k, v, c in [
                                ("Mean",      f"{mean_v:.6f}",    "#f1f5f9"),
                                ("Std dev",   f"{std_v:.6f}",     "#94a3b8"),
                                ("CV (σ/μ)",  f"{cv:.3f}",
                                 "#f59e0b" if cv > 0.5 else "#22c55e"),
                                ("Skewness",  f"{skew:.3f}",
                                 "#f59e0b" if abs(skew) > 1 else "#22c55e"),
                                ("Min",       f"{sub.min():.6f}", "#94a3b8"),
                                ("Max",       f"{sub.max():.6f}", "#94a3b8"),
                                ("P95",       f"{sub.quantile(.95):.6f}", "#94a3b8"),
                            ]
                        ])
                        + "</div>",
                        unsafe_allow_html=True,
                    )

                    # Loss function recommendation
                    if abs(skew) > 2:
                        rec_loss = "Use log1p transform + MAE or Huber loss — heavy tail."
                        rec_clr  = "#f59e0b"
                    elif cv > 0.5:
                        rec_loss = "High variance — MAPE or relative loss functions work well."
                        rec_clr  = "#38bdf8"
                    else:
                        rec_loss = "Symmetric distribution — MSE or MAE both work."
                        rec_clr  = "#22c55e"

                    st.markdown(
                        f"<div style='padding:8px 10px;background:#0d1117;"
                        f"border-left:3px solid {rec_clr};border-radius:4px;"
                        f"font-size:10px;color:{rec_clr};"
                        f"font-family:IBM Plex Mono,monospace;margin-top:8px;'>"
                        f"Rec: {rec_loss}</div>",
                        unsafe_allow_html=True,
                    )
