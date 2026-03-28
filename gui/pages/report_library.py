"""
gui/pages/report_library.py
─────────────────────────────────────────────────────────────────────────────
REPORTS → Report Library

Browse all generated reports. One row per report_run record.
Features:
  - Filter by goal, confidence, verdict, date
  - Click a row to expand full metadata + narrative preview
  - Re-run button (pre-fills Report Builder with same config)
  - Download PDF / HTML directly from this page
  - Side-by-side confidence trend chart
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import json, sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, DB_PATH


# ── Colour helpers ─────────────────────────────────────────────────────────────
_CONF_COL = {"HIGH": "#22c55e", "MEDIUM": "#f59e0b", "LOW": "#ef4444"}
_VERD_COL = {
    "SUPPORTED":        "#22c55e",
    "REJECTED":         "#ef4444",
    "INCONCLUSIVE":     "#f59e0b",
    "INSUFFICIENT_DATA":"#7090b0",
}


def _badge(text: str, color: str) -> str:
    return (
        f"<span style='background:{color}22;color:{color};"
        f"border:1px solid {color};border-radius:10px;"
        f"padding:2px 9px;font-size:.7rem;"
        f"font-family:IBM Plex Mono,monospace;'>{text}</span>"
    )


# ── Header ─────────────────────────────────────────────────────────────────────

def _header() -> None:
    st.markdown("""
    <div style="background:linear-gradient(90deg,#0f1520,#1a1f35);
                padding:1.5rem 1.8rem;border-radius:10px;
                border-left:4px solid #a78bfa;margin-bottom:1.5rem;">
      <h2 style="margin:0;color:#e8f0f8;font-family:'IBM Plex Mono',monospace;
                 font-size:1.25rem;">≡  Report Library</h2>
      <p style="margin:.4rem 0 0;color:#7090b0;font-size:.82rem;
                font-family:'IBM Plex Mono',monospace;">
        All generated reports · filter · download · re-run with fresh data
      </p>
    </div>
    """, unsafe_allow_html=True)


# ── Load reports from DB ────────────────────────────────────────────────────────

def _load_reports() -> pd.DataFrame:
    try:
        conn = sqlite3.connect(str(DB_PATH))
        df = pd.read_sql_query("""
            SELECT
                report_id,
                goal_id,
                report_type,
                title,
                confidence_level,
                hypothesis_verdict,
                run_count,
                generated_at,
                generator_version,
                reproducibility_hash,
                output_paths_json,
                narrative_json,
                stat_results_json,
                notes
            FROM report_runs
            ORDER BY generated_at DESC
        """, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Could not load report_runs table: {e}")
        return pd.DataFrame()


# ── KPI strip ──────────────────────────────────────────────────────────────────

def _kpi_strip(df: pd.DataFrame) -> None:
    if df.empty:
        return
    c1, c2, c3, c4 = st.columns(4)
    high = (df["confidence_level"] == "HIGH").sum()
    supported = (df["hypothesis_verdict"] == "SUPPORTED").sum()
    goals_covered = df["goal_id"].nunique()

    for col, val, label, color in [
        (c1, len(df),        "total reports",       "#a78bfa"),
        (c2, high,           "high confidence",      "#22c55e"),
        (c3, supported,      "hypotheses supported", "#38bdf8"),
        (c4, goals_covered,  "goals covered",        "#f59e0b"),
    ]:
        col.markdown(f"""
        <div style="background:#0d1828;border:1px solid #1e2d45;border-radius:8px;
                    padding:.9rem 1rem;text-align:center;">
          <div style="font-size:1.4rem;font-weight:600;color:{color};
                      font-family:'IBM Plex Mono',monospace;">{val}</div>
          <div style="font-size:.72rem;color:#7090b0;margin-top:.2rem;
                      font-family:'IBM Plex Mono',monospace;">{label}</div>
        </div>""", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom:1rem'></div>", unsafe_allow_html=True)


# ── Confidence trend chart ──────────────────────────────────────────────────────

def _confidence_trend(df: pd.DataFrame) -> None:
    if df.empty or len(df) < 2:
        return

    df2 = df.copy()
    df2["generated_at"] = pd.to_datetime(df2["generated_at"], errors="coerce")
    df2 = df2.dropna(subset=["generated_at"]).sort_values("generated_at")

    conf_map = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
    df2["conf_num"] = df2["confidence_level"].map(conf_map).fillna(1)

    fig = go.Figure()
    for conf, color in _CONF_COL.items():
        sub = df2[df2["confidence_level"] == conf]
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub["generated_at"], y=sub["conf_num"],
            mode="markers",
            name=conf,
            marker=dict(color=color, size=8, opacity=0.8),
            text=sub["title"],
            hovertemplate="<b>%{text}</b><br>%{x}<extra></extra>",
        ))

    fig.update_layout(
        **PL,
        title=dict(text="Report confidence over time", font=dict(size=11)),
        yaxis=dict(
            tickvals=[1, 2, 3], ticktext=["LOW", "MEDIUM", "HIGH"],
            gridcolor="#1e2d45",
        ),
        height=220,
    )
    st.plotly_chart(fig, use_container_width=True)


# ── Filter sidebar ──────────────────────────────────────────────────────────────

def _apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    with st.expander("🔍  Filter reports", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            goals = ["(all)"] + sorted(df["goal_id"].dropna().unique().tolist())
            sel_goal = st.selectbox("Goal", goals, key="rl_filter_goal")
        with col2:
            conf_opts = ["(all)", "HIGH", "MEDIUM", "LOW"]
            sel_conf = st.selectbox("Confidence", conf_opts, key="rl_filter_conf")
        with col3:
            verd_opts = ["(all)", "SUPPORTED", "REJECTED", "INCONCLUSIVE", "INSUFFICIENT_DATA"]
            sel_verd = st.selectbox("Verdict", verd_opts, key="rl_filter_verd")

    if sel_goal != "(all)":
        df = df[df["goal_id"] == sel_goal]
    if sel_conf != "(all)":
        df = df[df["confidence_level"] == sel_conf]
    if sel_verd != "(all)":
        df = df[df["hypothesis_verdict"] == sel_verd]
    return df


# ── Report row expander ─────────────────────────────────────────────────────────

def _render_report_row(row: pd.Series) -> None:
    conf_color  = _CONF_COL.get(row.get("confidence_level", "LOW"), "#7090b0")
    verd_color  = _VERD_COL.get(row.get("hypothesis_verdict", ""), "#7090b0")
    conf_badge  = _badge(row.get("confidence_level", "?"), conf_color)
    verd_badge  = _badge(row.get("hypothesis_verdict", "?"), verd_color)
    generated   = str(row.get("generated_at", ""))[:16]

    label = (
        f"{row.get('title','Untitled')[:55]}  "
        f"·  {row.get('goal_id','')}  "
        f"·  {generated}"
    )

    with st.expander(label, expanded=False):
        # Metadata row
        st.markdown(
            f"{conf_badge} &nbsp; {verd_badge} &nbsp; "
            f"<span style='font-size:.72rem;color:#7090b0;"
            f"font-family:IBM Plex Mono,monospace;'>"
            f"{row.get('run_count',0)} runs · "
            f"hash: {str(row.get('reproducibility_hash',''))[:12]}…"
            f"</span>",
            unsafe_allow_html=True,
        )
        st.markdown("")

        col1, col2 = st.columns(2)

        # Narrative preview
        with col1:
            st.markdown("**Executive summary**")
            try:
                narr = json.loads(row.get("narrative_json") or "{}")
                summary = narr.get("executive_summary", "No narrative available.")
                st.markdown(
                    f"<div style='font-size:.78rem;color:#c8d8e8;"
                    f"font-family:IBM Plex Mono,monospace;line-height:1.6;"
                    f"background:#0d1828;padding:.8rem;border-radius:6px;'>"
                    f"{summary[:500]}{'…' if len(summary) > 500 else ''}"
                    f"</div>",
                    unsafe_allow_html=True,
                )
                findings = narr.get("key_findings", [])
                if findings:
                    st.markdown("**Key findings**")
                    for f in findings[:3]:
                        st.markdown(
                            f"<div style='font-size:.75rem;color:#7090b0;"
                            f"font-family:IBM Plex Mono,monospace;margin:.2rem 0;'>"
                            f"· {f[:120]}</div>",
                            unsafe_allow_html=True,
                        )
            except Exception:
                st.caption("Narrative not available.")

        # Stat results mini-table
        with col2:
            st.markdown("**Statistical results**")
            try:
                stats = json.loads(row.get("stat_results_json") or "[]")
                if stats:
                    rows_ = []
                    for r in stats[:6]:
                        rows_.append({
                            "Metric": r.get("metric", "")[:20],
                            "Δ%": f"{r.get('pct_diff', 0):+.1f}%",
                            "p": f"{r.get('p', 1):.4f}",
                            "d": f"{r.get('d', 0):.2f}",
                            "Sig": "✓" if r.get("sig") else "✗",
                        })
                    st.dataframe(
                        pd.DataFrame(rows_),
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.caption("No stat results stored.")
            except Exception:
                st.caption("Stat results not available.")

        st.markdown("")

        # Action buttons
        btn_col1, btn_col2, btn_col3, _ = st.columns([1, 1, 1, 2])

        # Download buttons
        try:
            paths = json.loads(row.get("output_paths_json") or "{}")
            for fmt, path in paths.items():
                p = Path(path)
                if p.exists():
                    mime = ("application/pdf" if fmt == "pdf"
                            else "text/html" if fmt == "html"
                            else "application/json")
                    with open(p, "rb") as fh:
                        btn_col1.download_button(
                            f"⬇ {fmt.upper()}",
                            data=fh.read(),
                            file_name=p.name,
                            mime=mime,
                            key=f"dl_{row['report_id']}_{fmt}",
                            use_container_width=True,
                        )
        except Exception:
            pass

        # Re-run button — navigates to Report Builder with pre-filled config
        if btn_col2.button(
            "↻ Re-run",
            key=f"rerun_{row['report_id']}",
            use_container_width=True,
            help="Opens Report Builder pre-filled with this report's configuration",
        ):
            try:
                filt = json.loads(row.get("run_filter_json") or "{}")
                st.session_state["rb_current_cfg"] = {
                    "goal_id":        row.get("goal_id", ""),
                    "report_title":   f"[Re-run] {row.get('title', '')}",
                    "report_type":    row.get("report_type", "goal"),
                    "output_formats": ["pdf", "html"],
                    "pdf_watermark":  None,
                    "filters":        filt,
                    "sections":       SECTION_OPTIONS_DEFAULT,
                }
                st.session_state["nav_page"] = "report_builder"
                st.rerun()
            except Exception as e:
                st.error(f"Could not load config for re-run: {e}")

        # Delete button
        if btn_col3.button(
            "🗑 Delete",
            key=f"del_{row['report_id']}",
            use_container_width=True,
            help="Removes the report record from the database (output files are kept)",
        ):
            try:
                conn = sqlite3.connect(str(DB_PATH))
                conn.execute(
                    "DELETE FROM report_runs WHERE report_id = ?",
                    (row["report_id"],)
                )
                conn.commit()
                conn.close()
                st.success("Report record deleted.")
                st.rerun()
            except Exception as e:
                st.error(f"Delete failed: {e}")


SECTION_OPTIONS_DEFAULT = [
    "title_page", "executive_summary", "goal_and_hypothesis",
    "system_profile", "experiment_setup", "results_table",
    "visualizations", "diagrams", "hypothesis_verdict",
    "goal_analysis", "interpretation", "conclusion", "appendix",
]


# ── Main render ────────────────────────────────────────────────────────────────

def render(ctx: dict) -> None:
    _header()

    df = _load_reports()

    if df.empty:
        st.info(
            "No reports generated yet. "
            "Go to **Reports → Report Builder** to generate your first report."
        )
        if st.button("→ Open Report Builder"):
            st.session_state["nav_page"] = "report_builder"
            st.rerun()
        return

    _kpi_strip(df)

    tab1, tab2 = st.tabs(["≡  All Reports", "◎  Trends"])

    with tab1:
        df_filtered = _apply_filters(df)

        st.markdown(
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:.78rem;"
            f"color:#7090b0;margin-bottom:.8rem;'>"
            f"Showing {len(df_filtered)} of {len(df)} reports</div>",
            unsafe_allow_html=True,
        )

        if df_filtered.empty:
            st.info("No reports match the current filters.")
        else:
            for _, row in df_filtered.iterrows():
                _render_report_row(row)

    with tab2:
        st.markdown("#### ◎  Confidence & Coverage Trends")
        _confidence_trend(df)

        # Goal coverage bar
        goal_counts = df["goal_id"].value_counts().reset_index()
        goal_counts.columns = ["goal_id", "count"]
        if not goal_counts.empty:
            fig = go.Figure(go.Bar(
                x=goal_counts["goal_id"],
                y=goal_counts["count"],
                marker_color="#a78bfa",
                text=goal_counts["count"],
                textposition="outside",
                textfont=dict(size=9, color="#7090b0"),
            ))
            fig.update_layout(
                **PL,
                title=dict(text="Reports per goal", font=dict(size=11)),
                xaxis=dict(tickangle=-30, gridcolor="#1e2d45"),
                height=260,
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── Researcher note ────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:.72rem;"
        "color:#4a5568;text-align:center;'>"
        "Report Library · All records stored in report_runs table · "
        "Output files persist in the configured output directory"
        "</div>",
        unsafe_allow_html=True,
    )
