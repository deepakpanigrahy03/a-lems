"""
gui/pages/dq_schema.py  —  ≡  Schema Log
─────────────────────────────────────────────────────────────────────────────
Schema version history — migration log, applied changes, current version.
─────────────────────────────────────────────────────────────────────────────
"""

from datetime import datetime

import pandas as pd
import streamlit as st

from gui.config import PL
from gui.db import q, q1


def render(ctx: dict) -> None:
    accent = "#f472b6"

    # ── Current version ───────────────────────────────────────────────────────
    current = q1("""
        SELECT version, applied_at, description
        FROM schema_version
        ORDER BY version DESC
        LIMIT 1
    """) or {}

    version = current.get("version", "—")
    applied_at = current.get("applied_at", "—")
    desc = current.get("description", "No description")

    st.markdown(
        f"<div style='padding:16px 20px;"
        f"background:linear-gradient(135deg,{accent}12,{accent}06);"
        f"border:1px solid {accent}33;border-radius:12px;margin-bottom:20px;"
        f"display:flex;align-items:center;gap:20px;'>"
        f"<div>"
        f"<div style='font-size:36px;font-weight:800;color:{accent};"
        f"font-family:IBM Plex Mono,monospace;line-height:1;'>v{version}</div>"
        f"<div style='font-size:10px;color:#94a3b8;text-transform:uppercase;"
        f"letter-spacing:.1em;margin-top:2px;'>Current schema version</div>"
        f"</div>"
        f"<div>"
        f"<div style='font-size:13px;color:#f1f5f9;"
        f"font-family:IBM Plex Mono,monospace;margin-bottom:3px;'>{desc}</div>"
        f"<div style='font-size:10px;color:#475569;"
        f"font-family:IBM Plex Mono,monospace;'>Applied: {applied_at}</div>"
        f"</div></div>",
        unsafe_allow_html=True,
    )

    # ── Full version history ──────────────────────────────────────────────────
    history = q("""
        SELECT version, applied_at, description
        FROM schema_version
        ORDER BY version DESC
    """)

    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;'>"
        f"Migration history — {len(history)} versions</div>",
        unsafe_allow_html=True,
    )

    if history.empty:
        st.info("No schema version records found.")
    else:
        for _, row in history.iterrows():
            is_current = row["version"] == version
            brd_clr = accent if is_current else "#1f2937"
            txt_clr = "#f1f5f9" if is_current else "#94a3b8"
            st.markdown(
                f"<div style='padding:10px 14px;background:#111827;"
                f"border:1px solid {brd_clr}44;border-left:3px solid {brd_clr};"
                f"border-radius:0 8px 8px 0;margin-bottom:6px;"
                f"font-family:IBM Plex Mono,monospace;'>"
                f"<div style='display:flex;align-items:center;gap:12px;'>"
                f"<span style='font-size:14px;font-weight:700;color:{accent};'>"
                f"v{row['version']}</span>"
                f"{'<span style=\"font-size:8px;padding:1px 6px;border-radius:3px;background:#052e1a;color:#4ade80;font-weight:700;\">CURRENT</span>' if is_current else ''}"
                f"<span style='font-size:11px;color:{txt_clr};flex:1;'>"
                f"{row.get('description','—')}</span>"
                f"<span style='font-size:10px;color:#475569;'>{row.get('applied_at','')}</span>"
                f"</div></div>",
                unsafe_allow_html=True,
            )

    # ── Table inventory ───────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{accent};"
        f"text-transform:uppercase;letter-spacing:.1em;"
        f"margin:20px 0 12px;'>Live table inventory</div>",
        unsafe_allow_html=True,
    )

    tables_q = q("""
        SELECT name AS table_name
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name
    """)

    if not tables_q.empty:
        row_counts = []
        for tbl in tables_q["table_name"]:
            try:
                cnt = q1(f"SELECT COUNT(*) AS n FROM {tbl}")
                row_counts.append(
                    {
                        "Table": tbl,
                        "Rows": cnt.get("n", 0) if cnt else 0,
                    }
                )
            except Exception:
                row_counts.append({"Table": tbl, "Rows": "error"})

        inv_df = pd.DataFrame(row_counts)
        st.dataframe(inv_df, use_container_width=True, height=300)

    # ── Views ─────────────────────────────────────────────────────────────────
    views_q = q("""
        SELECT name AS view_name
        FROM sqlite_master
        WHERE type='view'
        ORDER BY name
    """)

    if not views_q.empty:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{accent};"
            f"text-transform:uppercase;letter-spacing:.1em;"
            f"margin:16px 0 8px;'>Views</div>",
            unsafe_allow_html=True,
        )
        for v in views_q["view_name"]:
            st.markdown(
                f"<div style='padding:6px 12px;background:#111827;"
                f"border:1px solid #1f2937;border-radius:6px;margin-bottom:4px;"
                f"font-size:11px;color:#60a5fa;"
                f"font-family:IBM Plex Mono,monospace;'>{v}</div>",
                unsafe_allow_html=True,
            )
