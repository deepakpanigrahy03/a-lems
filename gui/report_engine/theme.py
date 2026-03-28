"""
gui/report_engine/theme.py
─────────────────────────────────────────────────────────────────────────────
Single source of truth for ALL visual configuration in the report engine.

Rules:
  - Every colour, font, margin, size lives here
  - No hex codes anywhere else in the engine
  - Change this file → changes every chart, PDF, and HTML output
  - All Plotly layouts built via make_layout() — never ** unpack PL directly
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any


# ══════════════════════════════════════════════════════════════════════════════
# COLOUR PALETTE
# Edit these to retheme the entire engine.
# ══════════════════════════════════════════════════════════════════════════════

class Colours:
    # ── Chart canvas ──────────────────────────────────────────────────────────
    PAPER_BG        = "#0f1520"
    PLOT_BG         = "#090d13"
    GRID            = "#1e2d45"
    AXIS_LINE       = "#1e2d45"

    # ── Text ──────────────────────────────────────────────────────────────────
    TEXT_PRIMARY    = "#c8d8e8"
    TEXT_SECONDARY  = "#7090b0"
    TEXT_MUTED      = "#4a6080"
    TEXT_WHITE      = "#e8f0f8"

    # ── Workflow colours ──────────────────────────────────────────────────────
    LINEAR          = "#22c55e"
    AGENTIC         = "#ef4444"

    # ── Semantic ──────────────────────────────────────────────────────────────
    SUCCESS         = "#22c55e"
    WARNING         = "#f59e0b"
    DANGER          = "#ef4444"
    INFO            = "#38bdf8"
    PURPLE          = "#a78bfa"

    # ── Chart colour sequence (used in order for multi-series) ────────────────
    SEQUENCE        = ["#22c55e", "#ef4444", "#3b82f6", "#f59e0b", "#38bdf8", "#a78bfa"]

    # ── Confidence level ──────────────────────────────────────────────────────
    CONFIDENCE      = {"HIGH": "#22c55e", "MEDIUM": "#f59e0b", "LOW": "#ef4444"}

    # ── Hypothesis verdict ────────────────────────────────────────────────────
    VERDICT         = {
        "SUPPORTED":        "#22c55e",
        "REJECTED":         "#ef4444",
        "INCONCLUSIVE":     "#f59e0b",
        "INSUFFICIENT_DATA":"#7090b0",
    }

    # ── Goal category colours ─────────────────────────────────────────────────
    CATEGORY        = {
        "efficiency":   "#22c55e",
        "latency":      "#3b82f6",
        "cost":         "#f59e0b",
        "thermal":      "#ef4444",
        "quality":      "#a78bfa",
        "network":      "#38bdf8",
        "memory":       "#f472b6",
        "tokens":       "#fb923c",
        "carbon":       "#34d399",
        "comparative":  "#94a3b8",
        "custom":       "#7090b0",
    }

    # ── PDF colours (ReportLab) ───────────────────────────────────────────────
    PDF_BG          = "#0f1520"
    PDF_SURFACE     = "#0d1828"
    PDF_BORDER      = "#1e2d45"
    PDF_TEXT        = "#c8d8e8"
    PDF_MUTED       = "#7090b0"
    PDF_WHITE       = "#e8f0f8"

    # ── HTML report ───────────────────────────────────────────────────────────
    HTML_ACCENT     = "#a78bfa"
    HTML_NAV_BG     = "#0d1828"


# ══════════════════════════════════════════════════════════════════════════════
# TYPOGRAPHY
# ══════════════════════════════════════════════════════════════════════════════

class Typography:
    CHART_FONT      = "IBM Plex Mono, monospace"
    PDF_FONT_MONO   = "Courier"          # ReportLab built-in nearest to IBM Plex Mono
    PDF_FONT_SERIF  = "Times-Roman"
    PDF_FONT_SANS   = "Helvetica"

    CHART_SIZE_BASE = 10
    CHART_SIZE_SM   = 9
    CHART_SIZE_TICK = 9

    PDF_SIZE_TITLE  = 22
    PDF_SIZE_H1     = 18
    PDF_SIZE_H2     = 13
    PDF_SIZE_H3     = 11
    PDF_SIZE_BODY   = 8.5
    PDF_SIZE_SM     = 7.5
    PDF_SIZE_CAPTION= 7


# ══════════════════════════════════════════════════════════════════════════════
# CHART MARGINS
# ══════════════════════════════════════════════════════════════════════════════

class Margins:
    CHART_DEFAULT   = dict(l=55, r=20, t=40, b=45)
    CHART_HEATMAP   = dict(l=130, r=20, t=40, b=90)
    CHART_TABLE     = dict(l=0, r=0, t=8, b=8)
    CHART_GAUGE     = dict(l=20, r=20, t=40, b=20)
    CHART_COMPACT   = dict(l=40, r=20, t=30, b=40)

    PDF_PAGE_MM     = 20     # page margin in mm
    PDF_COL_W_MM    = 170    # content width in mm (A4 - 2 * margin)


# ══════════════════════════════════════════════════════════════════════════════
# CHART SIZES
# ══════════════════════════════════════════════════════════════════════════════

class Sizes:
    CHART_HEIGHT_DEFAULT  = 400
    CHART_HEIGHT_COMPACT  = 220
    CHART_HEIGHT_TABLE    = 320
    CHART_HEIGHT_GAUGE    = 250
    CHART_HEIGHT_HEATMAP  = 420

    MARKER_SIZE_DEFAULT   = 6
    MARKER_SIZE_STRIP     = 3
    LINE_WIDTH_DEFAULT    = 1.5
    LINE_WIDTH_THIN       = 1.0

    PNG_WIDTH             = 900    # Plotly → PNG width for PDF
    PNG_HEIGHT            = 500    # Plotly → PNG height for PDF
    PNG_SCALE             = 2      # scale factor for 300 DPI


# ══════════════════════════════════════════════════════════════════════════════
# PLOTLY LAYOUT BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def make_layout(
    title: str = "",
    height: int | None = None,
    margin: dict | None = None,
    xaxis_title: str = "",
    yaxis_title: str = "",
    barmode: str | None = None,
    extra: dict | None = None,
) -> dict:
    """
    Build a complete Plotly layout dict using theme constants.

    NEVER use **PL or spread a layout dict directly into update_layout().
    Always call make_layout() — it assembles everything correctly with
    no duplicate keys.

    Args:
        title:       Chart title string
        height:      Chart height in px (default: Sizes.CHART_HEIGHT_DEFAULT)
        margin:      Margin dict (default: Margins.CHART_DEFAULT)
        xaxis_title: X-axis label
        yaxis_title: Y-axis label
        barmode:     'stack' | 'group' | None
        extra:       Any additional layout keys to merge in

    Returns:
        Complete layout dict ready to pass to fig.update_layout(**make_layout(...))
    """
    layout: dict[str, Any] = {
        "paper_bgcolor": Colours.PAPER_BG,
        "plot_bgcolor":  Colours.PLOT_BG,
        "font": dict(
            family=Typography.CHART_FONT,
            size=Typography.CHART_SIZE_BASE,
            color=Colours.TEXT_SECONDARY,
        ),
        "margin": margin or Margins.CHART_DEFAULT,
        "height": height or Sizes.CHART_HEIGHT_DEFAULT,
        "legend": dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=Typography.CHART_SIZE_SM),
        ),
        "colorway": Colours.SEQUENCE,
        "xaxis": dict(
            title=xaxis_title,
            gridcolor=Colours.GRID,
            linecolor=Colours.AXIS_LINE,
            tickfont=dict(size=Typography.CHART_SIZE_TICK),
        ),
        "yaxis": dict(
            title=yaxis_title,
            gridcolor=Colours.GRID,
            linecolor=Colours.AXIS_LINE,
            tickfont=dict(size=Typography.CHART_SIZE_TICK),
        ),
    }
    if title:
        layout["title"] = dict(
            text=title,
            font=dict(size=11, color=Colours.TEXT_SECONDARY),
        )
    if barmode:
        layout["barmode"] = barmode
    if extra:
        _deep_merge(layout, extra)
    return layout


def _deep_merge(base: dict, override: dict) -> None:
    """Merge override into base in-place, recursing into nested dicts."""
    for k, v in override.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


# ══════════════════════════════════════════════════════════════════════════════
# WORKFLOW COLOURS HELPER
# ══════════════════════════════════════════════════════════════════════════════

def workflow_color(workflow_type: str) -> str:
    """Return the canonical colour for a workflow type."""
    return {
        "linear":  Colours.LINEAR,
        "agentic": Colours.AGENTIC,
    }.get(workflow_type.lower(), Colours.SEQUENCE[0])


def category_color(category: str) -> str:
    """Return the canonical colour for a goal category."""
    return Colours.CATEGORY.get(category.lower(), Colours.TEXT_SECONDARY)


def confidence_color(level: str) -> str:
    return Colours.CONFIDENCE.get(level.upper(), Colours.TEXT_SECONDARY)


def verdict_color(verdict: str) -> str:
    return Colours.VERDICT.get(verdict.upper(), Colours.TEXT_SECONDARY)


# ══════════════════════════════════════════════════════════════════════════════
# STREAMLIT CARD HELPERS  (inline CSS strings for st.markdown)
# ══════════════════════════════════════════════════════════════════════════════

def header_html(title: str, subtitle: str, accent: str | None = None) -> str:
    """Standard A-LEMS page header banner."""
    color = accent or Colours.PURPLE
    return f"""
<div style="background:linear-gradient(90deg,{Colours.PDF_BG},{Colours.PDF_SURFACE});
            padding:1.5rem 1.8rem;border-radius:10px;
            border-left:4px solid {color};margin-bottom:1.5rem;">
  <h2 style="margin:0;color:{Colours.TEXT_WHITE};
             font-family:{Typography.CHART_FONT};font-size:1.25rem;">{title}</h2>
  <p style="margin:.4rem 0 0;color:{Colours.TEXT_SECONDARY};font-size:.82rem;
            font-family:{Typography.CHART_FONT};">{subtitle}</p>
</div>"""


def kpi_card_html(value: str, label: str, color: str) -> str:
    """Single KPI card."""
    return f"""
<div style="background:{Colours.PDF_SURFACE};border:1px solid {Colours.PDF_BORDER};
            border-radius:8px;padding:.9rem 1rem;text-align:center;">
  <div style="font-size:1.4rem;font-weight:600;color:{color};
              font-family:{Typography.CHART_FONT};">{value}</div>
  <div style="font-size:.72rem;color:{Colours.TEXT_SECONDARY};margin-top:.2rem;
              font-family:{Typography.CHART_FONT};">{label}</div>
</div>"""


def badge_html(text: str, color: str) -> str:
    return (
        f"<span style='background:{color}22;color:{color};"
        f"border:1px solid {color};border-radius:10px;"
        f"padding:2px 9px;font-size:.7rem;"
        f"font-family:{Typography.CHART_FONT};'>{text}</span>"
    )


def inline_card_html(rows: list[tuple[str, str]], accent_color: str | None = None) -> str:
    """Key-value info card."""
    border = f"border-left:3px solid {accent_color};" if accent_color else ""
    rows_html = "".join(
        f"<div style='display:flex;justify-content:space-between;"
        f"font-size:.75rem;padding:3px 0;border-bottom:1px solid {Colours.PDF_BORDER};'>"
        f"<span style='color:{Colours.TEXT_SECONDARY};'>{k}</span>"
        f"<span style='color:{Colours.TEXT_PRIMARY};'>{v}</span></div>"
        for k, v in rows
    )
    return (
        f"<div style='background:{Colours.PDF_SURFACE};"
        f"border:1px solid {Colours.PDF_BORDER};{border}"
        f"border-radius:10px;padding:1rem 1.2rem;"
        f"font-family:{Typography.CHART_FONT};'>{rows_html}</div>"
    )
