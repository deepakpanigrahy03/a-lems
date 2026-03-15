"""
gui/pages/carbon_country.py  —  🌍  Carbon by Country
─────────────────────────────────────────────────────────────────────────────
Grid intensity × country_code — carbon cost mapped by geography.
5 distinct countries, 1,194 runs with carbon data.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.db     import q, q1
from gui.config import PL, WF_COLORS

ACCENT = "#34d399"

# Reference grid carbon intensity (gCO2/kWh) — approximate 2024 values
GRID_INTENSITY = {
    "US": 386, "DE": 385, "FR": 56,  "GB": 233,
    "IN": 713, "CN": 581, "AU": 656, "CA": 130,
    "BR": 109, "JP": 471, "KR": 415, "SG": 408,
    "NL": 283, "SE": 13,  "NO": 26,  "CH": 30,
}


def render(ctx: dict) -> None:
    runs = ctx.get("runs", pd.DataFrame())

    if runs.empty or "country_code" not in runs.columns:
        st.info("No country data available.")
        return

    df = runs[runs["carbon_g"].notna() & (runs["carbon_g"] > 0)].copy() \
         if "carbon_g" in runs.columns else pd.DataFrame()

    if df.empty:
        st.info("No carbon data in runs yet.")
        return

    # ── KPIs ──────────────────────────────────────────────────────────────────
    total_carbon = df["carbon_g"].sum()
    countries    = df["country_code"].dropna().unique().tolist()
    avg_carbon   = df["carbon_g"].mean()
    best_country = df.groupby("country_code")["carbon_g"].mean().idxmin() \
                   if len(countries) > 1 else (countries[0] if countries else "?")

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Carbon by Country — {len(countries)} regions</div>"
        f"<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:12px;'>"
        + "".join([
            f"<div><div style='font-size:18px;font-weight:700;color:{c};"
            f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
            f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
            for v, l, c in [
                (f"{total_carbon:.1f}g",  "Total CO₂",         ACCENT),
                (f"{avg_carbon:.3f}g",    "Avg per run",        "#60a5fa"),
                (f"{len(countries)}",     "Countries",          "#f59e0b"),
                (best_country,            "Lowest carbon",      "#22c55e"),
            ]
        ])
        + "</div></div>",
        unsafe_allow_html=True)

    # ── Carbon by country ─────────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Avg carbon per run by country</div>", unsafe_allow_html=True)
        country_carbon = df.groupby("country_code")["carbon_g"].agg(
            ["mean","sum","count"]).reset_index().sort_values("mean")
        country_carbon.columns = ["country", "avg_g", "total_g", "runs"]
        # Add grid intensity reference
        country_carbon["grid_intensity"] = country_carbon["country"].map(GRID_INTENSITY)

        fig = go.Figure(go.Bar(
            x=country_carbon["avg_g"],
            y=country_carbon["country"],
            orientation="h",
            marker_color=[
                "#22c55e" if v < avg_carbon else "#f59e0b" if v < avg_carbon*1.5 else "#ef4444"
                for v in country_carbon["avg_g"]
            ],
            marker_line_width=0,
            text=[f"{v:.3f}g" for v in country_carbon["avg_g"]],
            textposition="outside", textfont=dict(size=9)))
        fig.update_layout(
            **{**PL, "margin": dict(l=60,r=80,t=10,b=30)},
            height=max(200, len(country_carbon)*40),
            xaxis_title="Avg CO₂ per run (g)",
            showlegend=False)
        st.plotly_chart(fig, use_container_width=True, key="cc_country_bar")

    with col2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Carbon by workflow × country</div>", unsafe_allow_html=True)
        fig2 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = df[df["workflow_type"] == wf]
            if sub.empty: continue
            cg = sub.groupby("country_code")["carbon_g"].mean().reset_index()
            fig2.add_trace(go.Bar(
                x=cg["country_code"], y=cg["carbon_g"],
                name=wf, marker_color=clr, marker_line_width=0))
        fig2.update_layout(**PL, height=280, barmode="group",
                           xaxis_title="Country",
                           yaxis_title="Avg CO₂ (g)")
        st.plotly_chart(fig2, use_container_width=True, key="cc_wf_bar")

    # ── Grid intensity reference ───────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Grid carbon intensity reference (gCO₂/kWh)</div>",
        unsafe_allow_html=True)

    grid_df = pd.DataFrame([
        {"Country": k, "Grid intensity (gCO₂/kWh)": v,
         "In your data": "✓" if k in countries else "—"}
        for k, v in sorted(GRID_INTENSITY.items(), key=lambda x: x[1])
    ])
    st.dataframe(grid_df, use_container_width=True, height=280)

    # ── Carbon per token by country ───────────────────────────────────────────
    if "total_tokens" in df.columns:
        df_tok = df[df["total_tokens"].notna() & (df["total_tokens"] > 0)].copy()
        df_tok["carbon_per_token_mg"] = df_tok["carbon_g"] / df_tok["total_tokens"] * 1000
        if not df_tok.empty:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
                f"Carbon per token (mg CO₂/token) by country</div>",
                unsafe_allow_html=True)
            cpt = df_tok.groupby("country_code")["carbon_per_token_mg"].mean() \
                        .reset_index().sort_values("carbon_per_token_mg")
            fig3 = go.Figure(go.Bar(
                x=cpt["country_code"], y=cpt["carbon_per_token_mg"],
                marker_color=ACCENT, marker_line_width=0))
            fig3.update_layout(**PL, height=220,
                               xaxis_title="Country",
                               yaxis_title="mg CO₂ / token",
                               showlegend=False)
            st.plotly_chart(fig3, use_container_width=True, key="cc_cpt")
