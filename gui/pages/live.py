"""
gui/pages/live.py  —  📼  Run Replay
Render function: render(ctx)
ctx keys: ov, runs, tax, lin, age, avg_lin_j, avg_age_j, tax_mult,
          plan_ms, exec_ms, synth_ms, plan_pct, exec_pct, synth_pct
"""

import subprocess

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from gui.config import DB_PATH, LIVE_API, PL, PROJECT_ROOT, WF_COLORS
from gui.db import q, q1, q_safe
from gui.helpers import (_bar_gauge_html, _gauge_html, _human_carbon,
                         _human_energy, _human_water, fl)

try:
    import requests as _req

    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False

    class _req:
        @staticmethod
        def get(*a, **kw):
            raise RuntimeError("requests not installed")


try:
    import yaml as _yaml

    _YAML_OK = True
except ImportError:
    _YAML_OK = False


def render(ctx: dict):
    ov = ctx["ov"]
    runs = ctx["runs"]
    tax = ctx["tax"]
    avg_lin_j = ctx["avg_lin_j"]
    avg_age_j = ctx["avg_age_j"]
    tax_mult = ctx["tax_mult"]
    plan_ms = ctx["plan_ms"]
    exec_ms = ctx["exec_ms"]
    synth_ms = ctx["synth_ms"]
    plan_pct = ctx["plan_pct"]
    exec_pct = ctx["exec_pct"]
    synth_pct = ctx["synth_pct"]
    lin = ctx["lin"]
    age = ctx["age"]

    st.title("📼 Run Replay")
    st.caption(
        "Inspect any completed run as a full timeline · "
        "Live gauges appear automatically in Execute Run during recording"
    )

    # ── Server status ─────────────────────────────────────────────────────────
    _srv_ok2 = False
    if _REQUESTS_OK:
        for _ep in ["/health", "/api/system/status", "/"]:
            try:
                if _req.get(f"{LIVE_API}{_ep}", timeout=1.5).status_code < 500:
                    _srv_ok2 = True
                    break
            except Exception:
                pass

    if not _srv_ok2:
        st.warning(
            "server.py is offline — start it for richer data: "
            "uvicorn server:app --host 0.0.0.0 --port 8765 --reload  "
            "(you can still browse completed runs from the local DB below)"
        )

    # ── Run picker ────────────────────────────────────────────────────────────
    _rp_runs, _rp_err = q_safe("""
        SELECT r.run_id, r.workflow_type, r.run_number,
               e.task_name, e.provider,
               ROUND(r.total_energy_uj/1e6,4) AS energy_j,
               ROUND(r.duration_ns/1e9,2)     AS duration_s,
               r.ipc, r.total_tokens
        FROM runs r JOIN experiments e ON r.exp_id=e.exp_id
        ORDER BY r.run_id DESC LIMIT 100
    """)

    if _rp_err or _rp_runs.empty:
        st.info("No runs in DB yet — run an experiment first.")
    else:

        def _rp_lbl(row):
            return (
                f"#{int(row.run_id):>4}  {str(row.workflow_type):<8}  "
                f"{str(row.task_name or '?'):<22}  "
                f"{row.energy_j:.3f}J  {row.duration_s:.1f}s"
            )

        _rp_opts = {_rp_lbl(r): int(r.run_id) for _, r in _rp_runs.iterrows()}
        _rp_sel = st.selectbox(
            "Select run to inspect", list(_rp_opts.keys()), key="rp_sel"
        )
        _rp_rid = _rp_opts[_rp_sel]
        _rp_row = _rp_runs[_rp_runs.run_id == _rp_rid].iloc[0]

        # KPI banner
        _wf = str(_rp_row.workflow_type)
        _clr = "#22c55e" if _wf == "linear" else "#ef4444"
        st.markdown(
            f"""
        <div style="background:#0f1520;border:1px solid #1e2d45;border-radius:6px;
                    padding:10px 16px;display:flex;gap:24px;flex-wrap:wrap;
                    margin-bottom:8px;border-left:3px solid {_clr};">
          <span style="font-size:11px;color:#7090b0;">Run <b style="color:#e8f0f8">#{_rp_rid}</b></span>
          <span style="font-size:11px;font-weight:600;color:{_clr}">{_wf}</span>
          <span style="font-size:11px;color:#7090b0;">task: <b style="color:#e8f0f8">{_rp_row.task_name}</b></span>
          <span style="font-size:11px;color:#7090b0;">provider: <b style="color:#e8f0f8">{_rp_row.provider}</b></span>
          <span style="font-size:11px;color:#7090b0;">energy: <b style="color:#f59e0b">{_rp_row.energy_j:.4f}J</b></span>
          <span style="font-size:11px;color:#7090b0;">duration: <b style="color:#e8f0f8">{_rp_row.duration_s:.2f}s</b></span>
        </div>""",
            unsafe_allow_html=True,
        )

        # Human insight for this run
        _rp_hi = _human_energy(float(_rp_row.energy_j))
        if _rp_hi:
            st.markdown(
                "<div style='font-size:9px;color:#3d5570;margin-bottom:12px;'>"
                + " &nbsp;·&nbsp; ".join(f"{ic} {d}" for ic, d in _rp_hi)
                + "</div>",
                unsafe_allow_html=True,
            )

        st.divider()

        # ── Load sample data (server if available, else direct DB) ────────────
        _load_src = "server" if _srv_ok2 else "db"
        _e_rows, _c_rows, _i_rows = [], [], []

        if _srv_ok2:
            try:
                _er = _req.get(
                    f"{LIVE_API}/api/runs/{_rp_rid}/samples/energy", timeout=5
                ).json()
                _e_rows = _er.get("power", []) if isinstance(_er, dict) else []
                _c_rows = _req.get(
                    f"{LIVE_API}/api/runs/{_rp_rid}/samples/cpu", timeout=5
                ).json()
                _i_rows = _req.get(
                    f"{LIVE_API}/api/runs/{_rp_rid}/samples/interrupts", timeout=5
                ).json()
                if not isinstance(_c_rows, list):
                    _c_rows = []
                if not isinstance(_i_rows, list):
                    _i_rows = []
            except Exception as _ex:
                st.warning(f"Server fetch failed ({_ex}) — falling back to direct DB")
                _load_src = "db"

        if _load_src == "db":
            _e_df, _ = q_safe(f"""
                SELECT ROUND((timestamp_ns-MIN(timestamp_ns) OVER (PARTITION BY run_id))/1e6,1) AS elapsed_ms,
                       ROUND(pkg_energy_uj/1e6,6)    AS pkg_j,
                       ROUND(core_energy_uj/1e6,6)   AS core_j,
                       ROUND(dram_energy_uj/1e6,6)   AS dram_j
                FROM energy_samples WHERE run_id={_rp_rid}
                ORDER BY timestamp_ns
            """)
            # Compute instantaneous watts from cumulative J (MAX-MIN approach)
            if not _e_df.empty:
                _e_df["pkg_w"] = _e_df["pkg_j"].diff() / (
                    _e_df["elapsed_ms"].diff() / 1000
                ).replace(0, float("nan"))
                _e_df["core_w"] = _e_df["core_j"].diff() / (
                    _e_df["elapsed_ms"].diff() / 1000
                ).replace(0, float("nan"))
                _e_df["dram_w"] = _e_df["dram_j"].diff() / (
                    _e_df["elapsed_ms"].diff() / 1000
                ).replace(0, float("nan"))
                _e_rows = _e_df.dropna().to_dict("records")

            _c_df, _ = q_safe(f"""
                SELECT ROUND((timestamp_ns-MIN(timestamp_ns) OVER (PARTITION BY run_id))/1e6,1) AS elapsed_ms,
                       cpu_util_percent, package_temp, ipc, c6_residency, c1_residency
                FROM cpu_samples WHERE run_id={_rp_rid} ORDER BY timestamp_ns
            """)
            _c_rows = _c_df.to_dict("records") if not _c_df.empty else []

            _i_df, _ = q_safe(f"""
                SELECT ROUND((timestamp_ns-MIN(timestamp_ns) OVER (PARTITION BY run_id))/1e6,1) AS elapsed_ms,
                       interrupts_per_sec
                FROM interrupt_samples WHERE run_id={_rp_rid} ORDER BY timestamp_ns
            """)
            _i_rows = _i_df.to_dict("records") if not _i_df.empty else []

        # ── Timeline scrubber ─────────────────────────────────────────────────
        _max_t = 0
        if _e_rows:
            _max_t = max(_max_t, _e_rows[-1].get("elapsed_ms", 0))
        if _c_rows:
            _max_t = max(_max_t, _c_rows[-1].get("elapsed_ms", 0))

        if _max_t > 0:
            _t_range = st.slider(
                "Timeline window (ms)",
                min_value=0,
                max_value=int(_max_t),
                value=(0, int(_max_t)),
                step=max(1, int(_max_t // 200)),
                key="rp_trange",
                help="Drag to zoom into a time window",
            )
            _t0, _t1 = _t_range

            def _trim(rows, t0, t1):
                return [r for r in rows if t0 <= r.get("elapsed_ms", 0) <= t1]

            _e_trim = _trim(_e_rows, _t0, _t1)
            _c_trim = _trim(_c_rows, _t0, _t1)
            _i_trim = _trim(_i_rows, _t0, _t1)
        else:
            _e_trim, _c_trim, _i_trim = _e_rows, _c_rows, _i_rows

        # ── Charts ────────────────────────────────────────────────────────────
        if not _e_trim and not _c_trim and not _i_trim:
            st.info(
                f"No sample data found for run #{_rp_rid}. "
                "Samples are only stored when `--save-db` is used with high-frequency logging enabled."
            )
        else:
            st.markdown(
                f"**Sample source: `{_load_src}` · "
                f"{len(_e_trim)} energy · {len(_c_trim)} CPU · {len(_i_trim)} IRQ samples "
                f"in window**"
            )

            def _replay_chart(rows, xcol, ycols, names, colors, ytitle, height=200):
                if not rows:
                    return None
                _df = pd.DataFrame(rows)
                fig = go.Figure()
                for yc, nm, clr in zip(ycols, names, colors):
                    if yc not in _df.columns:
                        continue
                    _sub = _df[[xcol, yc]].dropna()
                    if _sub.empty:
                        continue
                    _r, _g, _b = int(clr[1:3], 16), int(clr[3:5], 16), int(clr[5:7], 16)
                    fig.add_trace(
                        go.Scatter(
                            x=_sub[xcol],
                            y=_sub[yc],
                            name=nm,
                            line=dict(color=clr, width=1.5),
                            fill="tozeroy" if nm == names[0] else None,
                            fillcolor=(
                                f"rgba({_r},{_g},{_b},0.07)" if nm == names[0] else None
                            ),
                        )
                    )
                fig.update_layout(
                    **PL, height=height, xaxis_title="elapsed ms", yaxis_title=ytitle
                )
                return fig

            r1c1, r1c2 = st.columns(2)
            with r1c1:
                st.markdown("**Power draw (W)**")
                _f = _replay_chart(
                    _e_trim,
                    "elapsed_ms",
                    ["pkg_w", "core_w", "dram_w"],
                    ["Pkg", "Core", "DRAM"],
                    ["#3b82f6", "#22c55e", "#a78bfa"],
                    "Watts",
                )
                if _f:
                    st.plotly_chart(_f, use_container_width=True)

            with r1c2:
                st.markdown("**Temperature (°C)**")
                _f2 = _replay_chart(
                    _c_trim,
                    "elapsed_ms",
                    ["package_temp"],
                    ["Pkg Temp"],
                    ["#ef4444"],
                    "°C",
                )
                if _f2:
                    st.plotly_chart(_f2, use_container_width=True)

            r2c1, r2c2 = st.columns(2)
            with r2c1:
                st.markdown("**CPU utilisation (%)**")
                _f3 = _replay_chart(
                    _c_trim,
                    "elapsed_ms",
                    ["cpu_util_percent"],
                    ["CPU Util"],
                    ["#38bdf8"],
                    "% util",
                )
                if _f3:
                    st.plotly_chart(_f3, use_container_width=True)

            with r2c2:
                st.markdown("**IRQ rate**")
                _f4 = _replay_chart(
                    _i_trim,
                    "elapsed_ms",
                    ["interrupts_per_sec"],
                    ["IRQ/s"],
                    ["#f59e0b"],
                    "IRQ/s",
                )
                if _f4:
                    st.plotly_chart(_f4, use_container_width=True)

            # C-state breakdown
            if _c_trim and "c6_residency" in (_c_trim[0] if _c_trim else {}):
                st.markdown("**C-state residency over time**")
                _f5 = _replay_chart(
                    _c_trim,
                    "elapsed_ms",
                    ["c6_residency", "c1_residency"],
                    ["C6 (deep sleep)", "C1 (light idle)"],
                    ["#22c55e", "#f59e0b"],
                    "Residency %",
                    height=160,
                )
                if _f5:
                    st.plotly_chart(_f5, use_container_width=True)

            # Orchestration events timeline
            _ev, _ev_e = q_safe(f"""
                SELECT step_index, phase, event_type,
                       ROUND((start_time_ns - MIN(start_time_ns) OVER ())/1e6,1) AS start_ms,
                       ROUND(duration_ns/1e6,1)        AS duration_ms,
                       ROUND(event_energy_uj/1e6,6)    AS event_j,
                       ROUND(power_watts,2)             AS power_w
                FROM orchestration_events
                WHERE run_id={_rp_rid}
                ORDER BY start_time_ns
            """)
            if not _ev.empty:
                st.divider()
                st.markdown("**Orchestration events timeline**")
                PHASE_C = {
                    "planning": "#f59e0b",
                    "execution": "#3b82f6",
                    "synthesis": "#a78bfa",
                    "llm_wait": "#38bdf8",
                }
                _ev_fig = go.Figure()
                for _ph_name, _ph_clr in PHASE_C.items():
                    _ph_rows = _ev[_ev.phase == _ph_name]
                    if _ph_rows.empty:
                        continue
                    _r, _g, _b = (
                        int(_ph_clr[1:3], 16),
                        int(_ph_clr[3:5], 16),
                        int(_ph_clr[5:7], 16),
                    )
                    _ev_fig.add_trace(
                        go.Bar(
                            name=_ph_name.capitalize(),
                            x=_ph_rows.start_ms,
                            y=_ph_rows.duration_ms,
                            marker_color=_ph_clr,
                            marker_line_width=0,
                            width=max(20, float(_ph_rows.duration_ms.mean()) * 0.8),
                            hovertemplate=(
                                "<b>%{customdata[0]}</b><br>"
                                "start: %{x}ms<br>duration: %{y}ms<br>"
                                "energy: %{customdata[1]:.6f}J<br>"
                                "power: %{customdata[2]:.2f}W<extra></extra>"
                            ),
                            customdata=_ph_rows[
                                ["event_type", "event_j", "power_w"]
                            ].values,
                        )
                    )
                _ev_fig.update_layout(
                    **PL,
                    height=220,
                    barmode="overlay",
                    xaxis_title="elapsed ms",
                    yaxis_title="event duration ms",
                )
                st.plotly_chart(_ev_fig, use_container_width=True)
                st.dataframe(
                    _ev[
                        [
                            "step_index",
                            "phase",
                            "event_type",
                            "start_ms",
                            "duration_ms",
                            "event_j",
                            "power_w",
                        ]
                    ].round(4),
                    use_container_width=True,
                    hide_index=True,
                )
