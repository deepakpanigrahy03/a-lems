"""
gui/pages/_dm_helpers.py
Shared helpers for data movement pages.
Uses CTX runs (from load_runs) — no separate DB query.
"""

import pandas as pd
import streamlit as st


def rgba(hex6: str, alpha: float = 0.13) -> str:
    """Convert '#rrggbb' to 'rgba(r,g,b,alpha)' for Plotly fillcolor."""
    h = hex6.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def col_exists(df: pd.DataFrame, *cols) -> bool:
    return all(c in df.columns for c in cols)


def get_runs(ctx: dict) -> pd.DataFrame:
    """Return runs df from CTX. Never raises — returns empty df on missing."""
    runs = ctx.get("runs", pd.DataFrame())
    return runs if runs is not None else pd.DataFrame()


def no_data_banner(message: str, accent: str = "#a78bfa") -> None:
    st.markdown(
        f"<div style='padding:40px;text-align:center;"
        f"border:1px solid {accent}33;border-radius:12px;"
        f"background:{accent}08;margin-top:8px;'>"
        f"<div style='font-size:28px;margin-bottom:8px;'>◧</div>"
        f"<div style='font-size:14px;color:{accent};"
        f"font-family:IBM Plex Mono,monospace;margin-bottom:6px;'>No data yet</div>"
        f"<div style='font-size:11px;color:#475569;"
        f"font-family:IBM Plex Mono,monospace;'>{message}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )
