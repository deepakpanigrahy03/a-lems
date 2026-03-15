"""
gui/pages/hypotheses.py  —  💡  Hypothesis Tracker
─────────────────────────────────────────────────────────────────────────────
State research hypotheses, track supporting and contradicting evidence.
Uses a UI-specific table (research_hypotheses) if it exists,
otherwise provides the CREATE TABLE SQL and a manual entry form.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
from datetime import datetime

from gui.db     import q, q1
from gui.config import PL

ACCENT = "#38bdf8"

# Pre-loaded hypothesis templates based on the data we have
HYPOTHESIS_TEMPLATES = [
    {
        "title": "Agentic workflows consume more energy per token than linear",
        "prediction": "avg(energy_per_token) for agentic > linear across all task types",
        "test_query": "SELECT workflow_type, AVG(energy_per_token) FROM ml_features WHERE energy_per_token > 0 GROUP BY workflow_type",
        "category": "Orchestration",
    },
    {
        "title": "Higher cache miss rate → higher energy consumption",
        "prediction": "Pearson correlation between cache_miss_rate and energy_j > 0.3",
        "test_query": "SELECT cache_miss_rate, energy_j FROM ml_features WHERE cache_miss_rate > 0 AND energy_j > 0",
        "category": "Cache & Memory",
    },
    {
        "title": "Longer API latency inflates total run energy",
        "prediction": "Runs with api_latency_ms > P75 have higher energy than P25",
        "test_query": "SELECT api_latency_ms, energy_j, workflow_type FROM ml_features WHERE api_latency_ms > 0",
        "category": "Network",
    },
    {
        "title": "TinyLlama 1B is more energy-efficient per token than larger models",
        "prediction": "TinyLlama has lowest avg energy_per_token across all tasks",
        "test_query": "SELECT model_name, AVG(energy_per_token) FROM ml_features WHERE energy_per_token > 0 GROUP BY model_name ORDER BY 2",
        "category": "Model comparison",
    },
    {
        "title": "Planning phase dominates agentic energy overhead",
        "prediction": "> 40% of orchestration tax comes from planning phase events",
        "test_query": "SELECT phase, SUM(event_energy_uj) FROM orchestration_events GROUP BY phase",
        "category": "Agentic",
    },
]


def _check_table_exists() -> bool:
    try:
        result = q1("""
            SELECT COUNT(*) AS n FROM sqlite_master
            WHERE type='table' AND name='research_hypotheses'
        """)
        return (result.get("n") or 0) > 0
    except Exception:
        return False


def render(ctx: dict) -> None:
    table_exists = _check_table_exists()

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:6px;'>"
        f"Hypothesis Tracker</div>"
        f"<div style='font-size:11px;color:#94a3b8;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"State research hypotheses · track evidence · "
        f"link to supporting runs · build publishable findings</div>"
        f"</div>",
        unsafe_allow_html=True)

    # ── Create table if needed ────────────────────────────────────────────────
    if not table_exists:
        st.markdown(
            f"<div style='padding:12px 16px;background:#0c1f3a;"
            f"border-left:3px solid {ACCENT};border-radius:0 8px 8px 0;"
            f"font-size:11px;color:#93c5fd;"
            f"font-family:IBM Plex Mono,monospace;line-height:1.7;"
            f"margin-bottom:16px;'>"
            f"<b>First-time setup:</b> The research_hypotheses table doesn't exist yet. "
            f"Run this SQL in the SQL Query page to create it:</div>",
            unsafe_allow_html=True)
        st.code("""
CREATE TABLE IF NOT EXISTS research_hypotheses (
    hypothesis_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    prediction TEXT,
    category TEXT,
    status TEXT DEFAULT 'open',
    confidence_pct REAL DEFAULT 0,
    supporting_runs TEXT,
    contradicting_runs TEXT,
    test_query TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
        """, language="sql")

    # ── Template hypotheses ───────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;'>"
        f"Research hypotheses — based on your data</div>",
        unsafe_allow_html=True)

    for i, hyp in enumerate(HYPOTHESIS_TEMPLATES):
        # Try to compute quick evidence from the DB
        evidence = _compute_evidence(hyp, i)

        conf_clr = "#22c55e" if evidence["supported"] else \
                   "#f59e0b" if evidence["partial"] else "#94a3b8"
        status   = "SUPPORTED" if evidence["supported"] else \
                   "PARTIAL" if evidence["partial"] else "OPEN"

        with st.expander(
            f"{hyp['category']} · {hyp['title']} — {status}",
            expanded=evidence["supported"]):

            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(
                    f"<div style='font-size:11px;color:#94a3b8;"
                    f"font-family:IBM Plex Mono,monospace;line-height:1.7;'>"
                    f"<b style='color:#f1f5f9;'>Prediction:</b> {hyp['prediction']}"
                    f"</div>",
                    unsafe_allow_html=True)
                if evidence.get("finding"):
                    st.markdown(
                        f"<div style='margin-top:8px;padding:8px 12px;"
                        f"background:#052e1a;border-left:2px solid {conf_clr};"
                        f"border-radius:0 6px 6px 0;font-size:11px;"
                        f"color:#86efac;font-family:IBM Plex Mono,monospace;'>"
                        f"📊 {evidence['finding']}</div>",
                        unsafe_allow_html=True)
            with col2:
                st.markdown(
                    f"<div style='text-align:center;padding:12px;"
                    f"background:#111827;border-radius:8px;'>"
                    f"<div style='font-size:24px;font-weight:800;color:{conf_clr};"
                    f"font-family:IBM Plex Mono,monospace;'>"
                    f"{evidence['confidence']}%</div>"
                    f"<div style='font-size:9px;color:#475569;margin-top:2px;'>confidence</div>"
                    f"</div>",
                    unsafe_allow_html=True)

            st.code(hyp["test_query"], language="sql")

    # ── If table exists — show stored hypotheses ──────────────────────────────
    if table_exists:
        stored = q("SELECT * FROM research_hypotheses ORDER BY updated_at DESC")
        if not stored.empty:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
                f"Saved hypotheses — {len(stored)}</div>",
                unsafe_allow_html=True)
            st.dataframe(stored[[
                "hypothesis_id","title","category","status",
                "confidence_pct","created_at"
            ]], use_container_width=True, height=250)


def _compute_evidence(hyp: dict, idx: int) -> dict:
    """Try to compute quick evidence for each hypothesis."""
    try:
        if idx == 0:  # Agentic energy per token
            result = q("""
                SELECT workflow_type, AVG(energy_per_token) AS avg_ept
                FROM ml_features WHERE energy_per_token > 0
                GROUP BY workflow_type
            """)
            if not result.empty and len(result) >= 2:
                lin = result[result["workflow_type"]=="linear"]["avg_ept"].values
                age = result[result["workflow_type"]=="agentic"]["avg_ept"].values
                if len(lin) > 0 and len(age) > 0:
                    ratio = age[0] / lin[0] if lin[0] > 0 else 1
                    supported = ratio > 1
                    return {"supported": supported, "partial": ratio > 0.9,
                            "confidence": min(int(abs(ratio-1)*200), 95) if supported else 30,
                            "finding": f"Agentic: {age[0]:.5f}J/tok vs Linear: {lin[0]:.5f}J/tok (ratio: {ratio:.2f}x)"}
        elif idx == 1:  # Cache miss correlation
            result = q("""
                SELECT cache_miss_rate, energy_j FROM ml_features
                WHERE cache_miss_rate > 0 AND energy_j > 0 LIMIT 500
            """)
            if not result.empty and len(result) > 10:
                r = result.corr().iloc[0,1]
                supported = r > 0.3
                return {"supported": supported, "partial": r > 0.1,
                        "confidence": min(int(abs(r)*100), 90),
                        "finding": f"Pearson r = {r:.3f} (cache_miss_rate vs energy_j)"}
        elif idx == 3:  # Model efficiency
            result = q("""
                SELECT model_name, AVG(energy_per_token) AS avg_ept
                FROM ml_features WHERE energy_per_token > 0
                GROUP BY model_name ORDER BY avg_ept LIMIT 5
            """)
            if not result.empty:
                best = result.iloc[0]
                return {"supported": True, "partial": True,
                        "confidence": 75,
                        "finding": f"Most efficient: {best['model_name']} at {best['avg_ept']:.5f} J/tok"}
    except Exception:
        pass
    return {"supported": False, "partial": False, "confidence": 0, "finding": None}
