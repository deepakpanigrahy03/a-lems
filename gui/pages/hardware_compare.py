"""
gui/pages/hardware_compare.py  —  ⚙  Hardware Comparison Matrix
────────────────────────────────────────────────────────────────────────────
Cross-hardware analysis: same task + model + workflow → compare hardware.
Cross-OS analysis: same task + model + workflow + hardware → compare OS.
Full matrix: all combinations.

SERVER mode: reads PostgreSQL — all machines' data.
LOCAL mode:  reads local SQLite — single machine, limited comparison value.
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from gui.db import q, q1, q_safe, is_server_mode
from gui.config import PL

ACCENT = "#f59e0b"

_METRICS = {
    "Energy (J)":        ("avg_energy_j",    "AVG(r.total_energy_uj)/1e6"),
    "Duration (ms)":     ("avg_duration_ms",  "AVG(r.duration_ns)/1e6"),
    "IPC":               ("avg_ipc",          "AVG(r.ipc)"),
    "Temp (°C)":         ("avg_temp_c",       "AVG(r.package_temp_celsius)"),
    "Carbon (mg)":       ("avg_carbon_mg",    "AVG(r.carbon_g)*1000"),
    "Energy/token (μJ)": ("avg_e_per_tok",    "AVG(r.energy_per_token)*1e6"),
    "Cache miss %":      ("avg_cache_miss",   "AVG(r.cache_miss_rate)*100"),
}


def render(ctx: dict) -> None:
    st.markdown(
        f"<div style='padding:14px 20px;background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:16px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:3px;'>⚙ Hardware Comparison Matrix</div>"
        f"<div style='font-size:12px;color:#94a3b8;'>"
        f"Same task · same model · same workflow → compare across hardware and OS.</div></div>",
        unsafe_allow_html=True,
    )

    if not is_server_mode():
        st.info(
            "💡 This page is most useful in **server mode** (port 8502) where data from "
            "all machines is available in PostgreSQL. In local mode you only see this machine.",
            icon="ℹ️",
        )

    tab1, tab2, tab3 = st.tabs([
        "⚙ Hardware vs Hardware",
        "🖥 OS vs OS",
        "🔀 Full Matrix",
    ])

    with tab1:
        _hw_vs_hw()
    with tab2:
        _os_vs_os()
    with tab3:
        _full_matrix()


# ── Shared filters ─────────────────────────────────────────────────────────────

def _get_filter_options() -> dict:
    tasks = q("SELECT DISTINCT task_name FROM experiments WHERE task_name IS NOT NULL ORDER BY task_name")
    models = q("SELECT DISTINCT model_name FROM experiments WHERE model_name IS NOT NULL ORDER BY model_name")
    return {
        "tasks":    tasks.task_name.tolist() if not tasks.empty else [],
        "models":   models.model_name.tolist() if not models.empty else [],
        "workflows": ["linear", "agentic"],
    }


def _filters(key_prefix: str, opts: dict, show_os_filter: bool = False) -> tuple:
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        task = st.selectbox("Task", opts["tasks"], key=f"{key_prefix}_task") if opts["tasks"] else None
    with c2:
        model = st.selectbox("Model", ["(all)"] + opts["models"], key=f"{key_prefix}_model")
        model = None if model == "(all)" else model
    with c3:
        wf = st.selectbox("Workflow", ["(all)", "linear", "agentic"], key=f"{key_prefix}_wf")
        wf = None if wf == "(all)" else wf
    with c4:
        min_runs = st.number_input("Min runs", 1, 50, 3, key=f"{key_prefix}_minruns")
    return task, model, wf, int(min_runs)


def _build_where(task, model, wf, extra: str = "") -> tuple[str, list]:
    clauses, params = [], []
    if task:
        clauses.append("e.task_name = ?")
        params.append(task)
    if model:
        clauses.append("e.model_name = ?")
        params.append(model)
    if wf:
        clauses.append("r.workflow_type = ?")
        params.append(wf)
    if extra:
        clauses.append(extra)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    return where, params


def _metric_selector(key: str) -> tuple[str, str]:
    label = st.selectbox("Metric", list(_METRICS.keys()), key=key)
    col, expr = _METRICS[label]
    return label, col, expr


def _bar_chart(df: pd.DataFrame, x_col: str, y_col: str, metric_label: str, title: str) -> None:
    if df.empty:
        st.warning("No data for selected filters.")
        return

    # Colour by rank — best (lowest energy/duration/temp) = green
    vals   = df[y_col].fillna(0).tolist()
    best   = min(vals) if vals else 1
    colors = [
        "#22c55e" if v == best else
        "#f59e0b" if v < best * 1.2 else
        "#ef4444"
        for v in vals
    ]

    fig = go.Figure(go.Bar(
        x=df[x_col].astype(str),
        y=df[y_col].round(4),
        marker_color=colors,
        text=df[y_col].round(3).astype(str),
        textposition="outside",
    ))
    fig.update_layout(
        **{k: v for k, v in PL.items() if k != "margin"},
        title=dict(text=title, font=dict(size=12, color="#94a3b8")),
        xaxis_title=x_col,
        yaxis_title=metric_label,
        height=380,
        showlegend=False,
        margin=dict(t=50, b=60, l=50, r=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Delta table vs best
    df2 = df.copy()
    df2["delta_%"] = ((df2[y_col] - best) / best * 100).round(1)
    df2["rank"]    = df2[y_col].rank().astype(int)
    df2 = df2.sort_values("rank")
    st.dataframe(
        df2[[x_col, y_col, "delta_%", "run_count"]].rename(columns={
            x_col: x_col, y_col: metric_label, "delta_%": "Δ% vs best", "run_count": "Runs"
        }),
        use_container_width=True, hide_index=True,
    )


# ── Tab 1 — Hardware vs Hardware ───────────────────────────────────────────────

def _hw_vs_hw() -> None:
    st.markdown(
        "<div style='font-size:11px;color:#64748b;margin-bottom:12px;'>"
        "Fix: task + model + workflow + OS. Variable: hardware (cpu_model).</div>",
        unsafe_allow_html=True,
    )
    opts = _get_filter_options()
    if not opts["tasks"]:
        st.warning("No experiment data available.")
        return

    task, model, wf, min_runs = _filters("hw", opts)
    metric_label, y_col, metric_expr = _metric_selector("hw_metric")

    # OS filter — fix OS, vary hardware
    os_opts = q("SELECT DISTINCT ec.os_name FROM environment_config ec WHERE ec.os_name IS NOT NULL ORDER BY ec.os_name")
    os_list = os_opts.os_name.tolist() if not os_opts.empty else []
    os_filter = st.selectbox("Fix OS (optional)", ["(all)"] + os_list, key="hw_os_filter")
    os_fix = None if os_filter == "(all)" else os_filter

    extra = "ec.os_name = ?" if os_fix else ""
    where, params = _build_where(task, model, wf, extra)
    if os_fix:
        params.append(os_fix)

    sql = f"""
        SELECT
            h.cpu_model   AS hardware,
            h.hostname,
            {metric_expr} AS {y_col},
            COUNT(r.run_id) AS run_count
        FROM runs r
        JOIN experiments e   ON r.exp_id   = e.exp_id
        JOIN hardware_config h ON r.hw_id  = h.hw_id
        LEFT JOIN environment_config ec ON e.env_id = ec.env_id
        {where}
        GROUP BY h.cpu_model, h.hostname
        HAVING COUNT(r.run_id) >= {min_runs}
        ORDER BY {y_col} ASC
    """
    df, err = q_safe(sql, tuple(params))
    if err:
        st.error(f"Query error: {err}")
        return
    if df.empty:
        st.info(f"No hardware groups with ≥{min_runs} runs for these filters.")
        return

    df["hardware"] = df["hardware"].fillna("unknown") + " / " + df["hostname"].fillna("")
    _bar_chart(df, "hardware", y_col, metric_label,
               f"{metric_label} by Hardware — {task or 'all tasks'}")


# ── Tab 2 — OS vs OS ───────────────────────────────────────────────────────────

def _os_vs_os() -> None:
    st.markdown(
        "<div style='font-size:11px;color:#64748b;margin-bottom:12px;'>"
        "Fix: task + model + workflow + hardware. Variable: OS (os_name + os_version).<br>"
        "⚠ Cross-OS comparison is only meaningful when same hardware runs different OS "
        "(e.g. Linux vs macOS on same chip).</div>",
        unsafe_allow_html=True,
    )
    opts = _get_filter_options()
    if not opts["tasks"]:
        st.warning("No experiment data available.")
        return

    task, model, wf, min_runs = _filters("os", opts)
    metric_label, y_col, metric_expr = _metric_selector("os_metric")

    # Hardware filter — fix hardware, vary OS
    hw_opts = q("SELECT DISTINCT cpu_model FROM hardware_config WHERE cpu_model IS NOT NULL ORDER BY cpu_model")
    hw_list = hw_opts.cpu_model.tolist() if not hw_opts.empty else []
    hw_filter = st.selectbox("Fix Hardware (optional)", ["(all)"] + hw_list, key="os_hw_filter")
    hw_fix = None if hw_filter == "(all)" else hw_filter

    extra = "h.cpu_model = ?" if hw_fix else ""
    where, params = _build_where(task, model, wf, extra)
    if hw_fix:
        params.append(hw_fix)

    sql = f"""
        SELECT
            COALESCE(ec.os_name, 'unknown') || ' ' ||
            COALESCE(ec.os_version, '')     AS os_label,
            {metric_expr}                   AS {y_col},
            COUNT(r.run_id)                 AS run_count
        FROM runs r
        JOIN experiments e       ON r.exp_id = e.exp_id
        JOIN hardware_config h   ON r.hw_id  = h.hw_id
        LEFT JOIN environment_config ec ON e.env_id = ec.env_id
        {where}
        GROUP BY ec.os_name, ec.os_version
        HAVING COUNT(r.run_id) >= {min_runs}
        ORDER BY {y_col} ASC
    """
    df, err = q_safe(sql, tuple(params))
    if err:
        st.error(f"Query error: {err}")
        return
    if df.empty:
        st.info(f"No OS groups with ≥{min_runs} runs for these filters. "
                "Run experiments on multiple OS environments first.")
        return

    _bar_chart(df, "os_label", y_col, metric_label,
               f"{metric_label} by OS — {task or 'all tasks'}")

    st.info(
        "💡 **macOS support:** When you run experiments on macOS, the `os_name` will be "
        "detected automatically via `platform.system()`. No extra configuration needed.",
        icon="🍎",
    )


# ── Tab 3 — Full Matrix ────────────────────────────────────────────────────────

def _full_matrix() -> None:
    st.markdown(
        "<div style='font-size:11px;color:#64748b;margin-bottom:12px;'>"
        "All metrics across all hardware + OS combinations for a fixed task/model/workflow.</div>",
        unsafe_allow_html=True,
    )
    opts = _get_filter_options()
    if not opts["tasks"]:
        st.warning("No experiment data available.")
        return

    task, model, wf, min_runs = _filters("mx", opts)
    where, params = _build_where(task, model, wf)

    metric_exprs = ",\n            ".join(
        f"{expr} AS {col}" for col, expr in [(c, e) for c, e in [v for v in _METRICS.values()]]
    )

    sql = f"""
        SELECT
            COALESCE(h.cpu_model, 'unknown')  AS hardware,
            COALESCE(ec.os_name,  'unknown')  AS os,
            h.hostname,
            {metric_exprs},
            COUNT(r.run_id) AS run_count
        FROM runs r
        JOIN experiments e       ON r.exp_id = e.exp_id
        JOIN hardware_config h   ON r.hw_id  = h.hw_id
        LEFT JOIN environment_config ec ON e.env_id = ec.env_id
        {where}
        GROUP BY h.cpu_model, ec.os_name, h.hostname
        HAVING COUNT(r.run_id) >= {min_runs}
        ORDER BY avg_energy_j ASC
    """
    df, err = q_safe(sql, tuple(params))
    if err:
        st.error(f"Query error: {err}")
        return
    if df.empty:
        st.info(f"No groups with ≥{min_runs} runs for these filters.")
        return

    df["hw + os"] = df["hardware"].fillna("?") + " / " + df["os"].fillna("?")

    # Heatmap — normalise each metric column 0→1 for colour
    metric_cols = [col for col, _ in _METRICS.values()]
    display_cols = ["hw + os", "run_count"] + metric_cols
    df_show = df[[c for c in display_cols if c in df.columns]].copy()

    # Rename for display
    df_show = df_show.rename(columns={col: lbl for lbl, (col, _) in _METRICS.items()})
    df_show = df_show.rename(columns={"run_count": "Runs"})

    st.dataframe(
        df_show.style.background_gradient(
            subset=[lbl for lbl in _METRICS.keys() if lbl in df_show.columns],
            cmap="RdYlGn_r",
        ),
        use_container_width=True, hide_index=True,
    )

    st.caption(
        "🟢 Best (lowest) · 🔴 Worst (highest) · "
        "Gradient applied independently per metric column."
    )
