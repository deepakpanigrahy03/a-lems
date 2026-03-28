"""
gui/report_engine/chart_builder.py
─────────────────────────────────────────────────────────────────────────────
Chart builder — Plotly charts for all goal types.

Rules:
  - ALL colours come from theme.Colours
  - ALL layouts built via theme.make_layout() — never **PL spread
  - make_layout() accepts margin= as named arg, no duplicate key crashes
  - Returns PNG bytes (for PDF) or JSON string (for HTML)
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import io, json, logging
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .theme import (
    Colours, Typography, Margins, Sizes,
    make_layout, workflow_color,
)

log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# RENDER HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _rgba(hex_color: str, alpha: float) -> str:
    """Convert 6-char hex + alpha to rgba() — Plotly rejects 8-char hex alpha."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _to_png(fig: go.Figure, width: int = Sizes.PNG_WIDTH,
             height: int = Sizes.PNG_HEIGHT) -> bytes:
    try:
        return fig.to_image(
            format="png", width=width,
            height=height, scale=Sizes.PNG_SCALE,
        )
    except Exception as e:
        log.error(f"PNG render failed (is kaleido installed?): {e}")
        return b""


def _to_json(fig: go.Figure) -> str:
    return fig.to_json()


# ══════════════════════════════════════════════════════════════════════════════
# CHART FACTORIES
# Each function returns a go.Figure using make_layout() — no ** spreading.
# ══════════════════════════════════════════════════════════════════════════════

def violin_by_workflow(
    df: pd.DataFrame,
    metric_col: str,
    metric_name: str,
    unit: str = "",
    title: str | None = None,
) -> go.Figure:
    fig = go.Figure()
    for wf in df["workflow_type"].dropna().unique():
        sub  = df[df["workflow_type"] == wf][metric_col].dropna()
        col  = workflow_color(wf)
        fill = _rgba(col, 0.15)
        fig.add_trace(go.Violin(
            y=sub, name=wf.capitalize(),
            box_visible=True, meanline_visible=True,
            points="outliers",
            line_color=col,
            fillcolor=fill,
            marker=dict(size=Sizes.MARKER_SIZE_STRIP, color=col, opacity=0.6),
        ))
    y_label = f"{metric_name} ({unit})" if unit else metric_name
    fig.update_layout(**make_layout(
        title=title or f"{metric_name} by workflow",
        yaxis_title=y_label,
        extra={"violingap": 0.3, "violingroupgap": 0.1},
    ))
    return fig


def box_by_workflow(
    df: pd.DataFrame,
    metric_col: str,
    metric_name: str,
    unit: str = "",
) -> go.Figure:
    fig = go.Figure()
    for wf in df["workflow_type"].dropna().unique():
        sub = df[df["workflow_type"] == wf][metric_col].dropna()
        col = workflow_color(wf)
        fig.add_trace(go.Box(
            y=sub, name=wf.capitalize(),
            marker_color=col, line_color=col,
            fillcolor=_rgba(col, 0.2),
            boxpoints="outliers",
            marker_size=Sizes.MARKER_SIZE_STRIP,
        ))
    fig.update_layout(**make_layout(
        title=f"{metric_name} distribution",
        yaxis_title=f"{metric_name} ({unit})" if unit else metric_name,
    ))
    return fig


def scatter_energy_duration(
    df: pd.DataFrame,
    x_col: str = "duration_ns",
    y_col: str = "total_energy_uj",
    x_label: str = "Duration (ms)",
    y_label: str = "Energy (µJ)",
) -> go.Figure:
    fig = go.Figure()
    for wf in df["workflow_type"].dropna().unique():
        sub = df[df["workflow_type"] == wf].copy()
        xv  = sub[x_col] / 1e6 if x_col == "duration_ns" else sub[x_col]
        fig.add_trace(go.Scatter(
            x=xv, y=sub[y_col],
            mode="markers",
            name=wf.capitalize(),
            marker=dict(
                color=workflow_color(wf),
                size=Sizes.MARKER_SIZE_DEFAULT,
                opacity=0.7,
            ),
        ))
    fig.update_layout(**make_layout(
        title=f"{y_label} vs {x_label}",
        xaxis_title=x_label,
        yaxis_title=y_label,
    ))
    return fig


def bar_metric_comparison(
    means: dict[str, float],
    metric_name: str,
    unit: str = "",
    error_bars: dict[str, float] | None = None,
) -> go.Figure:
    labels = list(means.keys())
    values = list(means.values())
    colors = [
        workflow_color(k) if k.lower() in ("linear", "agentic")
        else Colours.SEQUENCE[i % len(Colours.SEQUENCE)]
        for i, k in enumerate(labels)
    ]
    errors = [error_bars.get(k, 0) for k in labels] if error_bars else None
    fig = go.Figure(go.Bar(
        x=labels, y=values,
        marker_color=colors,
        error_y=dict(type="data", array=errors, visible=errors is not None,
                     color=Colours.TEXT_MUTED),
        text=[f"{v:.2g}" for v in values],
        textposition="outside",
        textfont=dict(size=Typography.CHART_SIZE_SM, color=Colours.TEXT_SECONDARY),
    ))
    fig.update_layout(**make_layout(
        title=f"{metric_name} — group comparison",
        yaxis_title=f"{metric_name} ({unit})" if unit else metric_name,
    ))
    return fig


def heatmap_model_task(
    df: pd.DataFrame,
    value_col: str = "total_energy_uj",
    label: str = "Energy (µJ)",
) -> go.Figure:
    if "model_name" not in df.columns or "task_name" not in df.columns:
        return go.Figure()
    pivot = df.groupby(["model_name", "task_name"])[value_col].mean().unstack(fill_value=0)
    if pivot.empty:
        return go.Figure()
    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=list(pivot.columns),
        y=list(pivot.index),
        colorscale="Viridis",
        text=[[f"{v:.2g}" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=Typography.CHART_SIZE_SM),
        colorbar=dict(
            tickfont=dict(color=Colours.TEXT_SECONDARY, size=Typography.CHART_SIZE_SM),
            title=label,
        ),
    ))
    fig.update_layout(**make_layout(
        title=f"{label} — model × task",
        margin=Margins.CHART_HEATMAP,
        height=Sizes.CHART_HEIGHT_HEATMAP,
        extra={
            "xaxis": {"tickangle": -30},
        },
    ))
    return fig


def timeseries_energy(
    df: pd.DataFrame,
    time_col: str = "started_at",
    value_col: str = "total_energy_uj",
) -> go.Figure:
    fig = go.Figure()
    df2 = df.copy()
    df2[time_col] = pd.to_datetime(df2[time_col], errors="coerce")
    df2 = df2.dropna(subset=[time_col, value_col]).sort_values(time_col)
    for wf in df2["workflow_type"].dropna().unique():
        sub = df2[df2["workflow_type"] == wf]
        col = workflow_color(wf)
        fig.add_trace(go.Scatter(
            x=sub[time_col], y=sub[value_col],
            mode="lines+markers",
            name=wf.capitalize(),
            line=dict(color=col, width=Sizes.LINE_WIDTH_DEFAULT),
            marker=dict(size=Sizes.MARKER_SIZE_STRIP, color=col),
        ))
    fig.update_layout(**make_layout(
        title="Energy over time",
        xaxis_title="Date",
        yaxis_title="Total Energy (µJ)",
    ))
    return fig


def stat_summary_table(results: list) -> go.Figure:
    """Table figure showing statistical test results."""
    if not results:
        return go.Figure()
    rows = []
    for r in results:
        rows.append([
            r.metric_name[:24],
            f"{r.group_a_mean:.3g} {r.unit}",
            f"{r.group_b_mean:.3g} {r.unit}",
            f"{r.pct_difference():+.1f}%",
            f"{r.p_value:.4f}",
            f"{r.effect_size:.2f} ({r.effect_label.value})",
            "✓" if r.significant else "✗",
        ])
    header_vals = [
        "Metric",
        f"{results[0].group_a_label} mean",
        f"{results[0].group_b_label} mean",
        "Δ%", "p-value", "Cohen's d", "Sig",
    ]
    fig = go.Figure(go.Table(
        header=dict(
            values=[f"<b>{h}</b>" for h in header_vals],
            fill_color=Colours.GRID,
            font=dict(
                color=Colours.TEXT_WHITE,
                size=Typography.CHART_SIZE_SM,
                family=Typography.CHART_FONT,
            ),
            align="left", height=28,
        ),
        cells=dict(
            values=list(zip(*rows)) if rows else [[] for _ in header_vals],
            fill_color=[
                [Colours.PLOT_BG, Colours.PDF_SURFACE][i % 2]
                for i in range(len(rows))
            ],
            font=dict(
                color=Colours.TEXT_SECONDARY,
                size=Typography.CHART_SIZE_SM,
                family=Typography.CHART_FONT,
            ),
            align="left", height=24,
        ),
    ))
    fig.update_layout(**make_layout(
        margin=Margins.CHART_TABLE,
        height=Sizes.CHART_HEIGHT_TABLE,
    ))
    return fig


def latency_phase_stacked_bar(df: pd.DataFrame) -> go.Figure:
    phases = [
        ("plan_ms",  "Planning",  Colours.INFO),
        ("exec_ms",  "Execution", Colours.LINEAR),
        ("synth_ms", "Synthesis", Colours.WARNING),
    ]
    fig = go.Figure()
    groups = df["workflow_type"].dropna().unique().tolist()
    for col, label, color in phases:
        if col not in df.columns:
            continue
        means = [df[df["workflow_type"] == g][col].mean() for g in groups]
        fig.add_trace(go.Bar(
            name=label,
            x=[g.capitalize() for g in groups],
            y=means,
            marker_color=color,
        ))
    fig.update_layout(**make_layout(
        title="Latency phase breakdown",
        yaxis_title="Time (ms)",
        barmode="stack",
    ))
    return fig


def ooi_ucr_scatter(df: pd.DataFrame) -> go.Figure:
    if "ooi_cpu" not in df.columns or "ucr" not in df.columns:
        return go.Figure()
    fig = go.Figure()
    for wf in df["workflow_type"].dropna().unique():
        sub = df[df["workflow_type"] == wf].dropna(subset=["ooi_cpu", "ucr"])
        fig.add_trace(go.Scatter(
            x=sub["ooi_cpu"], y=sub["ucr"],
            mode="markers",
            name=wf.capitalize(),
            marker=dict(
                color=workflow_color(wf),
                size=Sizes.MARKER_SIZE_DEFAULT,
                opacity=0.75,
            ),
        ))
    fig.add_hline(y=0.5, line_dash="dot", line_color=Colours.GRID, line_width=1)
    fig.add_vline(x=0.5, line_dash="dot", line_color=Colours.GRID, line_width=1)
    fig.update_layout(**make_layout(
        title="Orchestration CPU vs useful compute",
        xaxis_title="OOI_cpu (orchestration CPU fraction)",
        yaxis_title="UCR (useful compute ratio)",
    ))
    return fig


def carbon_bar(df: pd.DataFrame, carbon_g_per_kwh: float = 233.0) -> go.Figure:
    if "total_energy_uj" not in df.columns:
        return go.Figure()
    df2 = df.copy()
    df2["co2_ug"] = (df2["total_energy_uj"] / 3.6e15) * carbon_g_per_kwh * 1e6
    means = df2.groupby("workflow_type")["co2_ug"].mean().to_dict()
    stds  = {
        k: df2[df2["workflow_type"] == k]["co2_ug"].std()
        for k in means
    }
    return bar_metric_comparison(means, "CO₂ estimate", "µg", stds)


def query_result_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    chart_type: str = "bar",
    color_col: str | None = None,
    title: str = "",
    x_label: str = "",
    y_label: str = "",
) -> go.Figure:
    """
    Generic chart builder for Query Registry results.
    Accepts any DataFrame from any named query.
    chart_type: 'bar' | 'scatter' | 'line' | 'violin'
    """
    fig = go.Figure()

    if color_col and color_col in df.columns:
        groups = df[color_col].unique()
        for i, grp in enumerate(groups):
            sub = df[df[color_col] == grp]
            color = workflow_color(grp) if grp in ("linear", "agentic") \
                    else Colours.SEQUENCE[i % len(Colours.SEQUENCE)]
            if chart_type == "bar":
                fig.add_trace(go.Bar(x=sub[x_col], y=sub[y_col], name=str(grp), marker_color=color))
            elif chart_type == "scatter":
                fig.add_trace(go.Scatter(x=sub[x_col], y=sub[y_col], mode="markers",
                                         name=str(grp), marker=dict(color=color, size=6)))
            elif chart_type == "line":
                fig.add_trace(go.Scatter(x=sub[x_col], y=sub[y_col], mode="lines+markers",
                                         name=str(grp), line=dict(color=color)))
    else:
        colors = [Colours.SEQUENCE[i % len(Colours.SEQUENCE)] for i in range(len(df))]
        if chart_type == "bar":
            fig.add_trace(go.Bar(x=df[x_col], y=df[y_col], marker_color=colors))
        elif chart_type in ("scatter", "line"):
            mode = "markers" if chart_type == "scatter" else "lines+markers"
            fig.add_trace(go.Scatter(x=df[x_col], y=df[y_col], mode=mode,
                                     marker=dict(color=Colours.PURPLE)))

    fig.update_layout(**make_layout(
        title=title,
        xaxis_title=x_label or x_col.replace("_", " ").title(),
        yaxis_title=y_label or y_col.replace("_", " ").title(),
    ))
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# DISPATCHER — used by report_runner
# ══════════════════════════════════════════════════════════════════════════════

def build_chart(
    spec: dict,
    df: pd.DataFrame,
    stat_results: list | None = None,
    as_png: bool = True,
) -> bytes | str:
    chart_type = spec.get("type", "violin")
    fig = None
    try:
        if chart_type == "violin":
            fig = violin_by_workflow(
                df, spec.get("y", "total_energy_uj"),
                spec.get("title", spec.get("y", "")),
                spec.get("unit", ""),
            )
        elif chart_type == "box":
            fig = box_by_workflow(df, spec.get("y", "total_energy_uj"),
                                  spec.get("title", ""), spec.get("unit", ""))
        elif chart_type == "scatter":
            fig = scatter_energy_duration(
                df, spec.get("x", "duration_ns"), spec.get("y", "total_energy_uj"))
        elif chart_type == "bar":
            means = df.groupby("workflow_type")[spec.get("y", "total_energy_uj")].mean().to_dict()
            fig = bar_metric_comparison(means, spec.get("title", ""), spec.get("unit", ""))
        elif chart_type == "heatmap":
            fig = heatmap_model_task(df, spec.get("y", "total_energy_uj"))
        elif chart_type == "timeseries":
            fig = timeseries_energy(df)
        elif chart_type == "stat_table" and stat_results:
            fig = stat_summary_table(stat_results)
        elif chart_type == "latency_phases":
            fig = latency_phase_stacked_bar(df)
        elif chart_type == "ooi_ucr":
            fig = ooi_ucr_scatter(df)
        elif chart_type == "carbon":
            fig = carbon_bar(df)
        elif chart_type == "query_result":
            fig = query_result_chart(
                df,
                x_col=spec.get("x_col", df.columns[0]),
                y_col=spec.get("y_col", df.columns[1] if len(df.columns) > 1 else df.columns[0]),
                chart_type=spec.get("render_as", "bar"),
                color_col=spec.get("color_col"),
                title=spec.get("title", ""),
            )
        else:
            log.warning(f"Unknown chart type: {chart_type}")
            return b"" if as_png else "{}"
    except Exception as e:
        log.error(f"Chart build failed ({chart_type}): {e}")
        return b"" if as_png else "{}"

    if fig is None:
        return b"" if as_png else "{}"
    return _to_png(fig) if as_png else _to_json(fig)


def build_goal_charts(
    goal_id: str,
    df: pd.DataFrame,
    stat_results: list,
    query_results: list | None = None,   # list of (title, QueryResult)
    as_png: bool = True,
) -> list[tuple[str, bytes | str]]:
    """
    Curated chart set for a goal. Query results are additional charts
    from the Query Registry, injected alongside the standard charts.
    """
    charts = []

    # Universal
    if "total_energy_uj" in df.columns:
        data = build_chart(
            {"type": "violin", "y": "total_energy_uj", "unit": "µJ",
             "title": "Energy distribution by workflow"},
            df, as_png=as_png,
        )
        charts.append(("Energy distribution by workflow", data))

    if "duration_ns" in df.columns and "total_energy_uj" in df.columns:
        data = build_chart({"type": "scatter"}, df, as_png=as_png)
        charts.append(("Energy vs duration", data))

    # Goal-specific
    if goal_id == "orchestration_overhead" and "ooi_cpu" in df.columns:
        data = build_chart({"type": "ooi_ucr"}, df, as_png=as_png)
        charts.append(("OOI_cpu vs UCR", data))

    if goal_id == "latency_breakdown":
        data = build_chart({"type": "latency_phases"}, df, as_png=as_png)
        charts.append(("Latency phase breakdown", data))

    if goal_id == "carbon_footprint":
        data = build_chart({"type": "carbon"}, df, as_png=as_png)
        charts.append(("CO₂ estimate by workflow", data))

    if goal_id == "model_comparison":
        data = build_chart({"type": "heatmap", "y": "total_energy_uj"}, df, as_png=as_png)
        charts.append(("Energy heatmap — model × task", data))

    # Query Registry results → additional charts
    if query_results:
        for (caption, qr) in query_results:
            if not qr.ok():
                continue
            # Auto-detect numeric columns for charting
            num_cols = [c.column for c in qr.columns if c.col_type in ("float", "integer")]
            txt_cols = [c.column for c in qr.columns if c.col_type == "text"]
            if num_cols and txt_cols:
                fig = query_result_chart(
                    qr.df, x_col=txt_cols[0], y_col=num_cols[0],
                    chart_type="bar", title=caption,
                )
                data = _to_png(fig) if as_png else _to_json(fig)
                charts.append((caption, data))

    # Always end with stat table
    if stat_results:
        data = build_chart({"type": "stat_table"}, df, stat_results, as_png=as_png)
        charts.append(("Statistical results", data))

    return charts
