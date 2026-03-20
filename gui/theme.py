"""
theme.py  —  A-LEMS global theme injection
Drop this in gui/ and call inject_theme() at the top of every page.
Handles dark / light mode toggle stored in st.session_state["theme"].
"""

import streamlit as st

# ── Colour tokens ─────────────────────────────────────────────────────────────
DARK = {
    "bg0":    "#0d1117",   # page background
    "bg1":    "#111827",   # card background
    "bg2":    "#1f2937",   # inset / secondary surface
    "bg3":    "#374151",   # borders, tracks, empty bars
    "t1":     "#f1f5f9",   # primary text
    "t2":     "#94a3b8",   # secondary text / tabs / sidebar labels
    "t3":     "#475569",   # muted / metric labels
    "brd":    "#1f2937",   # card border
    "brd2":   "#374151",   # input border / dividers
    "accent": "#3b82f6",
}

LIGHT = {
    "bg0":    "#f3f4f6",   # soft off-white page bg — less harsh than pure white
    "bg1":    "#ffffff",   # cards pop off page bg with contrast
    "bg2":    "#f9fafb",   # input / secondary surface
    "bg3":    "#d1d5db",   # borders / tracks — visible but not harsh
    "t1":     "#1f2937",   # main text — softer than pure black
    "t2":     "#4b5563",   # secondary text / tabs / sidebar
    "t3":     "#6b7280",   # muted labels — readable on bg1 and bg2
    "brd":    "#d1d5db",   # card border — clearly visible
    "brd2":   "#9ca3af",   # input border / subtle lines
    "accent": "#3b82f6",
}


def _tokens(dark: bool) -> dict:
    return DARK if dark else LIGHT


def inject_theme():
    """
    Call once at the top of every page module.
    Reads st.session_state["theme"] ("dark" | "light").
    Injects global CSS overriding Streamlit's default colours.
    """
    dark = st.session_state.get("theme", "dark") == "dark"
    t    = _tokens(dark)

    st.markdown(f"""
    <style>
    /* ── Page & app background ──────────────────────────────────── */
    .stApp, [data-testid="stAppViewContainer"] {{
        background-color: {t["bg0"]} !important;
    }}
    [data-testid="stHeader"] {{
        background-color: {t["bg0"]} !important;
        border-bottom: 0.5px solid {t["brd"]};
    }}

    /* ── Sidebar ────────────────────────────────────────────────── */
    [data-testid="stSidebar"], [data-testid="stSidebarContent"] {{
        background-color: {t["bg1"]} !important;
        border-right: 0.5px solid {t["brd"]};
    }}
    [data-testid="stSidebar"] * {{
        color: {t["t2"]} !important;
    }}
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {{
        color: {t["t1"]} !important;
    }}

    /* ── Generic text ────────────────────────────────────────────── */
    .stMarkdown, .stText, p, span, div {{
        color: {t["t1"]};
    }}

    /* ── Metric cards ────────────────────────────────────────────── */
    [data-testid="stMetric"] {{
        background: {t["bg1"]};
        border: 0.5px solid {t["brd"]};
        border-top: 3px solid {t["accent"]};
        border-radius: 8px;
        padding: 10px 14px;
    }}
    [data-testid="stMetricLabel"] {{ color: {t["t3"]} !important; font-size: 11px !important; }}
    [data-testid="stMetricValue"] {{ color: {t["t1"]} !important; }}
    [data-testid="stMetricDelta"] {{ font-size: 10px !important; }}

    /* ── Expanders ───────────────────────────────────────────────── */
    [data-testid="stExpander"] {{
        background: {t["bg1"]};
        border: 0.5px solid {t["brd"]};
        border-radius: 8px;
    }}

    /* ── Tabs ────────────────────────────────────────────────────── */
    [data-testid="stTabs"] [data-baseweb="tab-list"] {{
        background: {t["bg1"]};
        border-bottom: 0.5px solid {t["brd"]};
    }}
    [data-testid="stTabs"] [data-baseweb="tab"] {{
        color: {t["t2"]} !important;
        font-size: 11px;
    }}
    [data-testid="stTabs"] [aria-selected="true"] {{
        color: {t["t1"]} !important;
        border-bottom: 2px solid {t["accent"]} !important;
    }}

    /* ── Inputs & selects ────────────────────────────────────────── */
    [data-testid="stTextInput"] input,
    [data-testid="stSelectbox"] > div,
    [data-testid="stNumberInput"] input {{
        background: {t["bg2"]} !important;
        color: {t["t1"]} !important;
        border: 0.5px solid {t["brd2"]} !important;
        border-radius: 6px;
    }}

    /* ── Buttons ─────────────────────────────────────────────────── */
    [data-testid="stButton"] > button {{
        background: {t["bg2"]};
        color: {t["t1"]};
        border: 0.5px solid {t["brd2"]};
        border-radius: 6px;
        font-size: 12px;
    }}
    [data-testid="stButton"] > button:hover {{
        background: {t["bg3"]};
        border-color: {t["accent"]};
    }}

    /* ── DataFrames / tables ─────────────────────────────────────── */
    [data-testid="stDataFrame"] {{
        background: {t["bg1"]};
        border: 0.5px solid {t["brd"]};
        border-radius: 8px;
    }}

    /* ── Plotly chart backgrounds ────────────────────────────────── */
    .js-plotly-plot .plotly .bg {{
        fill: {t["bg1"]} !important;
    }}

    /* ── Custom card utility class ───────────────────────────────── */
    .alems-card {{
        background: {t["bg1"]};
        border: 0.5px solid {t["brd"]};
        border-radius: 10px;
        padding: 14px 16px;
        margin-bottom: 10px;
    }}
    .alems-card-red {{ border-top: 3px solid #dc2626; }}
    .alems-card-blue {{ border-top: 3px solid #3b82f6; }}
    .alems-card-green {{ border-top: 3px solid #16a34a; }}
    .alems-card-amber {{ border-top: 3px solid #d97706; }}

    /* ── Scrollbar ───────────────────────────────────────────────── */
    ::-webkit-scrollbar {{ width: 5px; height: 5px; }}
    ::-webkit-scrollbar-track {{ background: {t["bg0"]}; }}
    ::-webkit-scrollbar-thumb {{ background: {t["bg3"]}; border-radius: 3px; }}
    </style>
    """, unsafe_allow_html=True)


def theme_toggle_button():
    """
    Renders a small ☀/☾ toggle button in sidebar or top bar.
    Call after inject_theme().
    """
    dark = st.session_state.get("theme", "dark") == "dark"
    label = "☀  Light mode" if dark else "☾  Dark mode"
    if st.button(label, key="theme_toggle_btn"):
        st.session_state["theme"] = "light" if dark else "dark"
        st.rerun()


def plotly_layout(dark: bool = None) -> dict:
    """
    Returns a Plotly layout dict matching the current theme.
    Merge with fig.update_layout(**plotly_layout()).
    """
    if dark is None:
        dark = st.session_state.get("theme", "dark") == "dark"
    t = _tokens(dark)
    return dict(
        paper_bgcolor = t["bg1"],
        plot_bgcolor  = t["bg2"],
        font          = dict(color=t["t1"], size=10),
        xaxis         = dict(gridcolor=t["brd2"], zerolinecolor=t["brd2"],
                             tickfont=dict(color=t["t3"])),
        yaxis         = dict(gridcolor=t["brd2"], zerolinecolor=t["brd2"],
                             tickfont=dict(color=t["t3"])),
        legend        = dict(bgcolor=t["bg1"], bordercolor=t["brd"],
                             borderwidth=0.5, font=dict(color=t["t2"])),
        margin        = dict(t=40, b=40, l=50, r=20),
    )
