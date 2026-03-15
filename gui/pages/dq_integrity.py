"""
gui/pages/dq_integrity.py  —  #  Hash Integrity
─────────────────────────────────────────────────────────────────────────────
Verifies run_state_hash — detects corrupted or tampered run records.
Shows: runs with hash, runs without hash, hash uniqueness check.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.db     import q, q1
from gui.config import PL


def render(ctx: dict) -> None:
    accent = "#f472b6"

    stats = q1("""
        SELECT
            COUNT(*)                                                AS total,
            SUM(CASE WHEN run_state_hash IS NOT NULL THEN 1 ELSE 0 END) AS has_hash,
            SUM(CASE WHEN run_state_hash IS NULL     THEN 1 ELSE 0 END) AS no_hash,
            COUNT(DISTINCT run_state_hash)                          AS unique_hashes
        FROM runs
    """) or {}

    total       = int(stats.get("total", 0))
    has_hash    = int(stats.get("has_hash", 0))
    no_hash     = int(stats.get("no_hash", 0))
    unique_hash = int(stats.get("unique_hashes", 0))

    # Check for duplicate hashes (possible corruption/tampering)
    dup_df = q("""
        SELECT run_state_hash, COUNT(*) AS count
        FROM runs
        WHERE run_state_hash IS NOT NULL
        GROUP BY run_state_hash
        HAVING COUNT(*) > 1
    """)
    duplicates = len(dup_df) if not dup_df.empty else 0

    hash_coverage = round(has_hash / total * 100, 1) if total else 0
    integrity_ok  = duplicates == 0 and no_hash == 0

    # ── Header ────────────────────────────────────────────────────────────────
    status_clr  = "#22c55e" if integrity_ok else "#ef4444"
    status_text = "INTEGRITY OK" if integrity_ok else "ISSUES FOUND"

    st.markdown(
        f"<div style='padding:16px 20px;"
        f"background:linear-gradient(135deg,{accent}12,{accent}06);"
        f"border:1px solid {accent}33;border-radius:12px;margin-bottom:20px;"
        f"display:flex;align-items:center;gap:20px;'>"
        f"<div style='font-size:28px;'>"
        f"{'✓' if integrity_ok else '⚠'}</div>"
        f"<div>"
        f"<div style='font-size:18px;font-weight:700;color:{status_clr};"
        f"font-family:IBM Plex Mono,monospace;'>{status_text}</div>"
        f"<div style='font-size:11px;color:#94a3b8;margin-top:2px;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"{has_hash} of {total} runs have integrity hashes · "
        f"{unique_hash} unique · {duplicates} duplicates</div>"
        f"</div></div>",
        unsafe_allow_html=True)

    # ── KPI row ───────────────────────────────────────────────────────────────
    cols = st.columns(4)
    kpis = [
        ("Total runs",       total,          "#94a3b8"),
        ("Hashed runs",      has_hash,       "#22c55e"),
        ("Missing hash",     no_hash,        "#f59e0b" if no_hash > 0 else "#22c55e"),
        ("Duplicate hashes", duplicates,     "#ef4444" if duplicates > 0 else "#22c55e"),
    ]
    for col, (label, val, clr) in zip(cols, kpis):
        with col:
            st.markdown(
                f"<div style='padding:12px 14px;background:#111827;"
                f"border:1px solid {clr}33;border-left:3px solid {clr};"
                f"border-radius:8px;'>"
                f"<div style='font-size:24px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;line-height:1;'>{val}</div>"
                f"<div style='font-size:9px;color:#94a3b8;margin-top:3px;"
                f"text-transform:uppercase;letter-spacing:.08em;'>{label}</div>"
                f"</div>",
                unsafe_allow_html=True)

    st.markdown("---")

    # ── Hash coverage over time ───────────────────────────────────────────────
    trend = q("""
        SELECT
            r.run_id,
            CASE WHEN r.run_state_hash IS NOT NULL THEN 1 ELSE 0 END AS hashed
        FROM runs r
        ORDER BY r.run_id
    """)

    if not trend.empty:
        trend["cumulative_hashed"] = trend["hashed"].cumsum()
        trend["cumulative_total"]  = range(1, len(trend) + 1)
        trend["rolling_pct"] = (
            trend["cumulative_hashed"] / trend["cumulative_total"] * 100
        ).round(1)

        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{accent};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
            f"Hash coverage over runs</div>",
            unsafe_allow_html=True)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=trend["run_id"],
            y=trend["rolling_pct"],
            mode="lines",
            line=dict(color=accent, width=2),
            fill="tozeroy",
            fillcolor="rgba(244,114,182,0.13)",
            name="Coverage %",
        ))
        fig.add_hline(y=100, line_dash="dot",
                      line_color="#22c55e", line_width=1,
                      annotation_text="100%",
                      annotation_font=dict(size=9, color="#22c55e"))
        fig.update_layout(
            **PL, height=220,
            yaxis_title="Coverage %", yaxis_range=[0, 110],
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True, key="dq_integrity_trend")

    # ── Runs without hash ─────────────────────────────────────────────────────
    if no_hash > 0:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:#f59e0b;"
            f"text-transform:uppercase;letter-spacing:.1em;"
            f"margin:16px 0 10px;'>Runs missing hash</div>",
            unsafe_allow_html=True)
        missing = q("""
            SELECT r.run_id, e.name AS experiment,
                   e.model_name, e.workflow_type, r.run_number
            FROM runs r
            JOIN experiments e ON r.exp_id = e.exp_id
            WHERE r.run_state_hash IS NULL
            ORDER BY r.run_id DESC LIMIT 100
        """)
        if not missing.empty:
            st.dataframe(missing, use_container_width=True, height=250)

    # ── Duplicate hashes ──────────────────────────────────────────────────────
    if duplicates > 0:
        st.markdown(
            f"<div style='margin-top:16px;padding:10px 14px;"
            f"background:#2a0c0c;border-left:3px solid #ef4444;"
            f"border-radius:0 8px 8px 0;font-size:11px;"
            f"color:#fca5a5;font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
            f"<b>⚠ {duplicates} duplicate hashes detected.</b> "
            f"This may indicate corrupted or copied run records. "
            f"Investigate these run_ids before using this data for analysis."
            f"</div>",
            unsafe_allow_html=True)
        st.dataframe(dup_df, use_container_width=True)
    else:
        st.markdown(
            f"<div style='margin-top:16px;padding:10px 14px;"
            f"background:#052e1a;border-left:3px solid #22c55e;"
            f"border-radius:0 8px 8px 0;font-size:11px;"
            f"color:#86efac;font-family:IBM Plex Mono,monospace;'>"
            f"✓ All hashes are unique — no corruption or duplication detected."
            f"</div>",
            unsafe_allow_html=True)
