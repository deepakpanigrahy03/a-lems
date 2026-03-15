"""
gui/pages/env_config.py  —  ⚙  Environment Config
─────────────────────────────────────────────────────────────────────────────
Python / framework / git version fingerprint per run.
Shows environment drift — which runs used different code versions.
─────────────────────────────────────────────────────────────────────────────
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from gui.db     import q, q1
from gui.config import PL

ACCENT = "#94a3b8"


def render(ctx: dict) -> None:
    envs = q("""
        SELECT
            ec.env_id,
            ec.python_version,
            ec.python_implementation,
            ec.os_name,
            ec.os_version,
            ec.kernel_version,
            ec.llm_framework,
            ec.framework_version,
            ec.git_commit,
            ec.git_branch,
            ec.git_dirty,
            ec.numpy_version,
            ec.torch_version,
            ec.transformers_version,
            ec.container_runtime,
            ec.env_hash,
            ec.created_at,
            COUNT(r.run_id)          AS run_count,
            COUNT(DISTINCT e.exp_id) AS exp_count,
            MIN(r.start_time_ns)     AS first_run_ns,
            MAX(r.start_time_ns)     AS last_run_ns
        FROM environment_config ec
        LEFT JOIN experiments e ON ec.env_id = e.env_id
        LEFT JOIN runs r ON e.exp_id = r.exp_id
        GROUP BY ec.env_id
        ORDER BY ec.env_id DESC
    """)

    if envs.empty:
        st.info("No environment records yet.")
        return

    total_envs = len(envs)
    dirty_envs = envs[envs["git_dirty"].isin([1, True, "1", "true"])].shape[0]
    branches   = envs["git_branch"].dropna().unique().tolist()
    commits    = envs["git_commit"].dropna().unique().tolist()

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Environment Fingerprints — {total_envs} recorded</div>"
        f"<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:12px;'>"
        + "".join([
            f"<div><div style='font-size:18px;font-weight:700;color:{c};"
            f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
            f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
            for v, l, c in [
                (total_envs,       "Environments",   ACCENT),
                (len(commits),     "Unique commits", "#60a5fa"),
                (len(branches),    "Git branches",   "#22c55e"),
                (dirty_envs,       "Dirty builds",   "#f59e0b" if dirty_envs > 0 else "#22c55e"),
            ]
        ])
        + "</div></div>",
        unsafe_allow_html=True)

    # Dirty build warning
    if dirty_envs > 0:
        st.markdown(
            f"<div style='padding:8px 14px;background:#2a1a0c;"
            f"border-left:3px solid #f59e0b;border-radius:0 8px 8px 0;"
            f"font-size:11px;color:#fcd34d;"
            f"font-family:IBM Plex Mono,monospace;margin-bottom:12px;'>"
            f"⚠ {dirty_envs} environment{'s' if dirty_envs>1 else ''} "
            f"had uncommitted changes (git_dirty=1). "
            f"Runs from dirty builds may not be fully reproducible.</div>",
            unsafe_allow_html=True)

    # ── Environment cards ─────────────────────────────────────────────────────
    for _, env in envs.iterrows():
        env_id    = env.get("env_id", "?")
        py_ver    = env.get("python_version") or "?"
        py_impl   = env.get("python_implementation") or "?"
        os_name   = env.get("os_name") or "?"
        kernel    = env.get("kernel_version") or "?"
        framework = env.get("llm_framework") or "?"
        fw_ver    = env.get("framework_version") or "?"
        commit    = (env.get("git_commit") or "")[:8] or "?"
        branch    = env.get("git_branch") or "?"
        dirty     = env.get("git_dirty") in (1, True, "1", "true")
        numpy_v   = env.get("numpy_version") or "—"
        torch_v   = env.get("torch_version") or "—"
        trans_v   = env.get("transformers_version") or "—"
        runs_n    = int(env.get("run_count") or 0)
        exps_n    = int(env.get("exp_count") or 0)
        created   = str(env.get("created_at") or "")[:16]
        env_hash  = (env.get("env_hash") or "")[:12]

        dirty_badge = (
            "<span style='background:#2a1a0c;color:#f59e0b;font-size:8px;"
            "padding:1px 5px;border-radius:3px;font-weight:700;margin-left:6px;'>DIRTY</span>"
            if dirty else
            "<span style='background:#052e1a;color:#4ade80;font-size:8px;"
            "padding:1px 5px;border-radius:3px;font-weight:700;margin-left:6px;'>CLEAN</span>"
        )

        st.markdown(
            f"<div style='border:1px solid {ACCENT}22;"
            f"border-left:3px solid {ACCENT};"
            f"border-radius:0 8px 8px 0;padding:12px 16px;"
            f"background:#111827;margin-bottom:8px;"
            f"font-family:IBM Plex Mono,monospace;'>"

            f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:8px;'>"
            f"<span style='font-size:13px;font-weight:700;color:{ACCENT};'>env_{env_id}</span>"
            f"<span style='font-size:11px;color:#f1f5f9;'>{branch}</span>"
            f"<span style='font-size:10px;color:#60a5fa;'>@{commit}</span>"
            f"{dirty_badge}"
            f"<span style='margin-left:auto;font-size:9px;color:#475569;'>{created}</span>"
            f"</div>"

            f"<div style='display:grid;grid-template-columns:repeat(3,1fr);gap:8px;'>"

            f"<div>"
            f"<div style='font-size:9px;color:#475569;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-bottom:2px;'>Runtime</div>"
            f"<div style='font-size:11px;color:#94a3b8;'>"
            f"{py_impl} {py_ver} · {os_name} · {kernel}</div>"
            f"</div>"

            f"<div>"
            f"<div style='font-size:9px;color:#475569;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-bottom:2px;'>Libraries</div>"
            f"<div style='font-size:11px;color:#94a3b8;'>"
            f"numpy {numpy_v} · torch {torch_v} · transformers {trans_v}</div>"
            f"</div>"

            f"<div>"
            f"<div style='font-size:9px;color:#475569;text-transform:uppercase;"
            f"letter-spacing:.08em;margin-bottom:2px;'>Runs</div>"
            f"<div style='font-size:11px;color:#94a3b8;'>"
            f"{runs_n} runs · {exps_n} exps · hash: {env_hash}</div>"
            f"</div>"

            f"</div></div>",
            unsafe_allow_html=True)

    # ── Runs per environment ───────────────────────────────────────────────────
    if len(envs) > 1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            f"Runs per environment</div>",
            unsafe_allow_html=True)

        fig = go.Figure(go.Bar(
            x=[f"env_{r['env_id']} @{str(r.get('git_commit') or '')[:6]}"
               for _, r in envs.iterrows()],
            y=envs["run_count"].fillna(0),
            marker_color=[
                "#f59e0b" if r["git_dirty"] in (1, True, "1", "true")
                else ACCENT
                for _, r in envs.iterrows()
            ],
            marker_line_width=0,
        ))
        fig.update_layout(
            **PL, height=220,
            xaxis_title="Environment",
            yaxis_title="Run count",
            showlegend=False)
        st.plotly_chart(fig, use_container_width=True, key="env_runs_bar")

    # ── Git commit timeline ───────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Environment drift — commit history</div>",
        unsafe_allow_html=True)

    for _, env in envs.sort_values("env_id").iterrows():
        commit  = (env.get("git_commit") or "")[:12]
        branch  = env.get("git_branch") or "?"
        created = str(env.get("created_at") or "")[:16]
        runs_n  = int(env.get("run_count") or 0)
        dirty   = env.get("git_dirty") in (1, True, "1", "true")
        clr     = "#f59e0b" if dirty else "#22c55e"

        st.markdown(
            f"<div style='display:flex;align-items:center;gap:10px;"
            f"padding:5px 10px;border-radius:6px;margin-bottom:3px;'>"
            f"<div style='width:8px;height:8px;border-radius:50%;"
            f"background:{clr};flex-shrink:0;'></div>"
            f"<code style='font-size:11px;color:#60a5fa;'>{commit}</code>"
            f"<span style='font-size:11px;color:#94a3b8;'>{branch}</span>"
            f"<span style='font-size:10px;color:#475569;'>{created}</span>"
            f"<span style='margin-left:auto;font-size:10px;color:#475569;'>"
            f"{runs_n} runs</span>"
            f"</div>",
            unsafe_allow_html=True)
