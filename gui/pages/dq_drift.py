"""
gui/pages/dq_drift.py  —  ⟳  Data Drift
─────────────────────────────────────────────────────────────────────────────
Detects whether recent runs have drifted from the historical baseline.

WHY THIS MATTERS
────────────────
If avg energy in the last 100 runs is 20% higher than the all-time average,
something changed — hardware throttling, environment noise, a code change,
or sensor drift. This page flags those shifts automatically so the researcher
knows their newer data may not be comparable to older data.

DRIFT CHECKS
────────────
1. Energy drift       — avg energy_j shifted > threshold vs historical
2. Duration drift     — avg duration_ms shifted significantly
3. IPC drift          — CPU efficiency change (could mean code or hw change)
4. Temperature drift  — thermal baseline has shifted
5. API latency drift  — network conditions changed (cloud runs)
6. Token count drift  — model behaviour changed (different response lengths)

Each check compares: last N runs vs all prior runs.
Drift is flagged when the relative shift exceeds the configured threshold.
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.db import q, q1

ACCENT = "#f472b6"

# Columns to check for drift — label, column name, unit, drift threshold %
_DRIFT_CHECKS = [
    ("Energy",      "energy_j",             "J",   20.0),
    ("Duration",    "duration_ms",          "ms",  20.0),
    ("IPC",         "ipc",                  "",    15.0),
    ("Temperature", "package_temp_celsius",  "°C",  10.0),
    ("API Latency", "api_latency_ms",        "ms",  25.0),
    ("Token count", "total_tokens",          "",    20.0),
]


def render(ctx: dict) -> None:

    # ── Page header ───────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}12,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px;'>"
        f"Data Drift Detection</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>"
        f"Compares recent runs vs historical baseline. "
        f"Flags metrics that have shifted beyond threshold — "
        f"indicating environment change, hardware event, or sensor drift."
        f"</div></div>",
        unsafe_allow_html=True,
    )

    # ── Controls ──────────────────────────────────────────────────────────────
    cc1, cc2, cc3 = st.columns(3)
    with cc1:
        window = st.slider(
            "Recent window (runs)", 20, 200, 100, 10,
            help="How many of the latest runs count as 'recent'",
        )
    with cc2:
        threshold_pct = st.slider(
            "Drift threshold (%)", 5, 50, 20, 5,
            help="Flag if recent avg differs from historical avg by more than this %",
        )
    with cc3:
        workflow_filter = st.selectbox(
            "Workflow", ["all", "linear", "agentic"], key="drift_wf",
        )

    # ── Load runs — split into recent vs historical ────────────────────────────
    # We order by run_id DESC so the first `window` rows are the most recent.
    # Everything beyond that is the historical baseline.
    wf_clause = (
        f"AND r.workflow_type = '{workflow_filter}'"
        if workflow_filter != "all" else ""
    )

    all_runs = q(f"""
        SELECT
            r.run_id,
            r.workflow_type,
            r.total_energy_uj / 1e6     AS energy_j,
            r.duration_ns    / 1e6      AS duration_ms,
            r.ipc,
            r.package_temp_celsius,
            r.api_latency_ms,
            r.total_tokens,
            r.start_time_ns
        FROM runs r
        JOIN experiments e ON r.exp_id = e.exp_id
        WHERE 1=1 {wf_clause}
        ORDER BY r.run_id DESC
    """)

    if all_runs.empty or len(all_runs) < window + 10:
        st.warning(
            f"Not enough runs to detect drift. "
            f"Need at least {window + 10} runs — found {len(all_runs)}. "
            f"Reduce the window size or collect more data."
        )
        return

    # Split into recent (top N) and historical (everything else)
    recent_df     = all_runs.iloc[:window].copy()
    historical_df = all_runs.iloc[window:].copy()

    total_runs  = len(all_runs)
    hist_count  = len(historical_df)

    st.markdown(
        f"<div style='font-size:11px;color:#475569;margin-bottom:16px;'>"
        f"Comparing last <b style='color:#f1f5f9'>{window}</b> runs "
        f"(run_id {int(recent_df.run_id.min())}–{int(recent_df.run_id.max())}) "
        f"vs prior <b style='color:#f1f5f9'>{hist_count}</b> runs — "
        f"{total_runs} total.</div>",
        unsafe_allow_html=True,
    )

    # ── Run drift checks ──────────────────────────────────────────────────────
    drift_results = []

    for label, col, unit, default_thresh in _DRIFT_CHECKS:
        # Skip column if not in dataframe (older DB versions)
        if col not in all_runs.columns:
            continue

        rec_vals  = recent_df[col].dropna()
        hist_vals = historical_df[col].dropna()

        # Need enough data in both windows to be meaningful
        if len(rec_vals) < 5 or len(hist_vals) < 5:
            continue

        rec_mean  = float(rec_vals.mean())
        hist_mean = float(hist_vals.mean())
        rec_std   = float(rec_vals.std())
        hist_std  = float(hist_vals.std())

        # Relative drift: how much has the mean shifted as a % of historical
        if hist_mean == 0:
            continue
        drift_pct = ((rec_mean - hist_mean) / abs(hist_mean)) * 100

        # Use the user's threshold slider, not the hardcoded per-column default
        is_drifted = abs(drift_pct) > threshold_pct
        direction  = "↑" if drift_pct > 0 else "↓"

        drift_results.append({
            "label":      label,
            "col":        col,
            "unit":       unit,
            "hist_mean":  hist_mean,
            "rec_mean":   rec_mean,
            "hist_std":   hist_std,
            "rec_std":    rec_std,
            "drift_pct":  drift_pct,
            "direction":  direction,
            "drifted":    is_drifted,
            "rec_vals":   rec_vals,
            "hist_vals":  hist_vals,
        })

    # ── Drift summary cards ───────────────────────────────────────────────────
    n_drifted = sum(1 for r in drift_results if r["drifted"])
    n_stable  = len(drift_results) - n_drifted

    summary_clr = (
        "#22c55e" if n_drifted == 0 else
        "#f59e0b" if n_drifted <= 2 else
        "#ef4444"
    )
    summary_msg = (
        "All metrics stable — recent runs match historical baseline."
        if n_drifted == 0 else
        f"{n_drifted} metric(s) have drifted beyond the {threshold_pct}% threshold."
    )

    st.markdown(
        f"<div style='padding:12px 18px;background:#111827;"
        f"border:1px solid {summary_clr}44;border-left:4px solid {summary_clr};"
        f"border-radius:8px;margin-bottom:16px;display:flex;align-items:center;gap:14px;'>"
        f"<div style='font-size:28px;font-weight:800;color:{summary_clr};"
        f"font-family:IBM Plex Mono,monospace;'>{n_drifted}</div>"
        f"<div>"
        f"<div style='font-size:12px;color:#f1f5f9;font-weight:600;'>{summary_msg}</div>"
        f"<div style='font-size:10px;color:#475569;margin-top:2px;'>"
        f"{n_stable} metric(s) stable · threshold: ±{threshold_pct}%</div>"
        f"</div></div>",
        unsafe_allow_html=True,
    )

    if not drift_results:
        st.info("No metrics could be evaluated — check that run columns are populated.")
        return

    # ── Per-metric drift cards ─────────────────────────────────────────────────
    # Two columns of cards — each card shows historical vs recent mean,
    # drift %, and a mini distribution comparison.
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;'>"
        f"Metric drift breakdown</div>",
        unsafe_allow_html=True,
    )

    card_cols = st.columns(2)
    for i, r in enumerate(drift_results):
        clr = "#ef4444" if (r["drifted"] and abs(r["drift_pct"]) > threshold_pct * 1.5) \
              else "#f59e0b" if r["drifted"] \
              else "#22c55e"
        status_label = f"{r['direction']} {abs(r['drift_pct']):.1f}% drift" \
                       if r["drifted"] else "stable"
        unit_str = f" {r['unit']}" if r["unit"] else ""

        with card_cols[i % 2]:
            st.markdown(
                f"<div style='padding:12px 14px;background:#111827;"
                f"border:1px solid {clr}33;border-left:3px solid {clr};"
                f"border-radius:8px;margin-bottom:10px;'>"
                f"<div style='display:flex;justify-content:space-between;"
                f"align-items:center;margin-bottom:8px;'>"
                f"<div style='font-size:12px;font-weight:600;color:#f1f5f9;'>"
                f"{r['label']}</div>"
                f"<div style='font-size:10px;padding:2px 8px;border-radius:4px;"
                f"background:{clr}22;color:{clr};font-weight:700;'>{status_label}</div>"
                f"</div>"
                f"<div style='display:grid;grid-template-columns:1fr 1fr;gap:8px;'>"
                f"<div style='background:#0d1117;border-radius:6px;padding:8px 10px;'>"
                f"<div style='font-size:9px;color:#475569;text-transform:uppercase;"
                f"letter-spacing:.08em;margin-bottom:2px;'>Historical ({hist_count} runs)</div>"
                f"<div style='font-size:16px;font-weight:700;color:#94a3b8;"
                f"font-family:IBM Plex Mono,monospace;'>"
                f"{r['hist_mean']:.3f}{unit_str}</div>"
                f"<div style='font-size:9px;color:#334155;'>±{r['hist_std']:.3f}</div>"
                f"</div>"
                f"<div style='background:#0d1117;border-radius:6px;padding:8px 10px;"
                f"border:1px solid {clr}22;'>"
                f"<div style='font-size:9px;color:#475569;text-transform:uppercase;"
                f"letter-spacing:.08em;margin-bottom:2px;'>Recent ({window} runs)</div>"
                f"<div style='font-size:16px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;'>"
                f"{r['rec_mean']:.3f}{unit_str}</div>"
                f"<div style='font-size:9px;color:#334155;'>±{r['rec_std']:.3f}</div>"
                f"</div>"
                f"</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # ── Distribution comparison charts for drifted metrics only ───────────────
    drifted_only = [r for r in drift_results if r["drifted"]]
    if drifted_only:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            f"Distribution shift — drifted metrics only</div>",
            unsafe_allow_html=True,
        )

        for r in drifted_only:
            fig = go.Figure()
            # Historical distribution
            fig.add_trace(go.Histogram(
                x=r["hist_vals"],
                name=f"Historical ({hist_count})",
                marker_color="#3b82f6",
                opacity=0.6,
                nbinsx=40,
            ))
            # Recent distribution
            fig.add_trace(go.Histogram(
                x=r["rec_vals"],
                name=f"Recent ({window})",
                marker_color="#ef4444" if r["drift_pct"] > 0 else "#f59e0b",
                opacity=0.7,
                nbinsx=40,
            ))
            # Vertical lines for means
            fig.add_vline(
                x=r["hist_mean"], line_dash="dot", line_color="#3b82f6",
                annotation_text=f"hist mean {r['hist_mean']:.3f}",
                annotation_font_size=9,
            )
            fig.add_vline(
                x=r["rec_mean"], line_dash="dot", line_color="#ef4444",
                annotation_text=f"recent mean {r['rec_mean']:.3f}",
                annotation_font_size=9,
            )
            unit_str = f" ({r['unit']})" if r["unit"] else ""
            fig.update_layout(
                **PL,
                height=220,
                barmode="overlay",
                title=dict(
                    text=f"{r['label']}{unit_str} — {r['direction']} {abs(r['drift_pct']):.1f}% drift",
                    font=dict(size=11, color="#f97316"),
                ),
                xaxis_title=f"{r['label']}{unit_str}",
                yaxis_title="Run count",
                showlegend=True,
            )
            st.plotly_chart(fig, use_container_width=True,
                            key=f"drift_dist_{r['col']}")

    # ── Rolling mean trend — energy over all runs ──────────────────────────────
    # Shows the moving average so the researcher can visually spot
    # when a drift started — was it gradual or a sudden step change?
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Rolling mean — energy over time</div>",
        unsafe_allow_html=True,
    )

    energy_col = "energy_j"
    if energy_col in all_runs.columns:
        roll_df = all_runs[[energy_col, "run_id"]].dropna().sort_values("run_id")

        # 20-run rolling mean to smooth noise
        roll_df["rolling_mean"] = roll_df[energy_col].rolling(20, min_periods=5).mean()

        fig_roll = go.Figure()

        # Raw values — faint
        fig_roll.add_trace(go.Scatter(
            x=roll_df["run_id"],
            y=roll_df[energy_col],
            mode="markers",
            name="Raw energy",
            marker=dict(color="#1e3a5f", size=3, opacity=0.5),
            showlegend=True,
        ))

        # Rolling mean — prominent
        fig_roll.add_trace(go.Scatter(
            x=roll_df["run_id"],
            y=roll_df["rolling_mean"],
            mode="lines",
            name="20-run rolling mean",
            line=dict(color="#3b82f6", width=2),
        ))

        # Shade the recent window to show where "recent" starts
        recent_start_id = int(recent_df.run_id.min())
        fig_roll.add_vrect(
            x0=recent_start_id,
            x1=int(recent_df.run_id.max()),
            fillcolor="#ef4444",
            opacity=0.07,
            layer="below",
            line_width=0,
            annotation_text=f"recent window ({window} runs)",
            annotation_position="top left",
            annotation_font_size=9,
            annotation_font_color="#ef4444",
        )

        fig_roll.update_layout(
            **PL,
            height=260,
            xaxis_title="Run ID (chronological)",
            yaxis_title="Energy (J)",
            showlegend=True,
        )
        st.plotly_chart(fig_roll, use_container_width=True, key="drift_rolling_energy")

    # ── Drift table summary ───────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Drift summary table</div>",
        unsafe_allow_html=True,
    )

    summary_rows = []
    for r in drift_results:
        summary_rows.append({
            "Metric":         r["label"],
            "Historical mean": f"{r['hist_mean']:.4f} {r['unit']}".strip(),
            "Recent mean":     f"{r['rec_mean']:.4f} {r['unit']}".strip(),
            "Drift %":         f"{r['drift_pct']:+.1f}%",
            "Status":          f"⚠ DRIFT {r['direction']}" if r["drifted"] else "✓ stable",
        })

    st.dataframe(
        pd.DataFrame(summary_rows),
        use_container_width=True,
        hide_index=True,
    )

    # ── Researcher guidance ───────────────────────────────────────────────────
    guidance_lines = []
    for r in drifted_only:
        if r["col"] == "energy_j":
            guidance_lines.append(
                f"Energy drifted {r['direction']} {abs(r['drift_pct']):.1f}% — "
                f"check thermal throttle events, CPU governor change, or new background processes."
            )
        elif r["col"] == "api_latency_ms":
            guidance_lines.append(
                f"API latency drifted {r['direction']} {abs(r['drift_pct']):.1f}% — "
                f"network conditions or API provider performance may have changed."
            )
        elif r["col"] == "ipc":
            guidance_lines.append(
                f"IPC drifted {r['direction']} {abs(r['drift_pct']):.1f}% — "
                f"possible code change, compiler flag change, or CPU frequency policy shift."
            )
        elif r["col"] == "package_temp_celsius":
            guidance_lines.append(
                f"Temperature drifted {r['direction']} {abs(r['drift_pct']):.1f}% — "
                f"ambient temperature change or cooling system issue."
            )
        elif r["col"] == "total_tokens":
            guidance_lines.append(
                f"Token count drifted {r['direction']} {abs(r['drift_pct']):.1f}% — "
                f"model behaviour or prompt structure may have changed."
            )

    if guidance_lines:
        st.markdown(
            f"<div style='margin-top:14px;padding:12px 16px;"
            f"background:#1a0e00;border-left:3px solid #f97316;"
            f"border-radius:0 8px 8px 0;font-size:11px;"
            f"color:#fed7aa;font-family:IBM Plex Mono,monospace;line-height:1.9;'>"
            f"<b style='color:#f97316;'>Possible causes:</b><br>"
            + "<br>".join(f"• {l}" for l in guidance_lines)
            + "</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"<div style='margin-top:14px;padding:10px 16px;"
            f"background:#052e1a;border-left:3px solid #22c55e;"
            f"border-radius:0 8px 8px 0;font-size:11px;"
            f"color:#4ade80;font-family:IBM Plex Mono,monospace;'>"
            f"✓ All metrics within threshold — data appears consistent across time.</div>",
            unsafe_allow_html=True,
        )
