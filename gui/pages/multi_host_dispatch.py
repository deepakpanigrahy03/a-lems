"""
gui/pages/multi_host_dispatch.py  —  ⬡  Multi-Host Dispatch
─────────────────────────────────────────────────────────────────────────────
PLANNED — Dispatch experiments to multiple lab machines simultaneously.

Architecture vision:
  Coordinator (this GUI) → dispatch queue → agent on each host
  Each host runs test_harness locally → saves to shared DB (or syncs)
  Results aggregated back here for cross-host comparison

This stub shows the roadmap and what single-host data already reveals
that motivates multi-host collection.
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL
from gui.db import q, q1

ACCENT = "#f59e0b"


def render(ctx: dict) -> None:

    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:6px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;'>Multi-Host Dispatch</div>"
        f"<div style='font-size:9px;padding:2px 8px;border-radius:4px;"
        f"background:#1a1000;color:{ACCENT};border:1px solid {ACCENT}44;'>"
        f"PLANNED</div></div>"
        f"<div style='font-size:12px;color:#94a3b8;'>"
        f"Dispatch experiments to multiple lab machines in parallel — "
        f"cross-hardware energy comparison at scale."
        f"</div></div>",
        unsafe_allow_html=True,
    )

    tab1, tab2, tab3 = st.tabs([
        "◎  Current state",
        "⬡  Architecture",
        "⚡  What this enables",
    ])

    with tab1:
        # Show what single-host data already reveals
        hw_summary = q("""
            SELECT
                h.hw_id, h.hostname,
                h.cpu_model, h.ram_gb,
                COUNT(DISTINCT r.run_id)   AS total_runs,
                AVG(r.total_energy_uj/1e6) AS avg_energy_j
            FROM hardware_config h
            LEFT JOIN runs r ON r.hw_id = h.hw_id
            GROUP BY h.hw_id
            ORDER BY total_runs DESC
        """)

        if not hw_summary.empty:
            st.markdown(
                f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
                f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
                f"Current hardware inventory — {len(hw_summary)} host(s)</div>",
                unsafe_allow_html=True,
            )
            for _, row in hw_summary.iterrows():
                clr = "#22c55e" if row["total_runs"] > 100 else "#f59e0b"
                st.markdown(
                    f"<div style='padding:12px 16px;background:#0d1117;"
                    f"border:1px solid {clr}33;border-left:3px solid {clr};"
                    f"border-radius:8px;margin-bottom:8px;'>"
                    f"<div style='font-size:12px;font-weight:600;color:#f1f5f9;"
                    f"margin-bottom:4px;'>{row.get('hostname','hw_'+str(row['hw_id']))}</div>"
                    f"<div style='display:flex;gap:20px;font-size:10px;"
                    f"font-family:IBM Plex Mono,monospace;'>"
                    f"<span style='color:#94a3b8;'>CPU: "
                    f"<b style='color:#f1f5f9;'>{row.get('cpu_model','?')}</b></span>"
                    f"<span style='color:#94a3b8;'>RAM: "
                    f"<b style='color:#f1f5f9;'>{row.get('ram_gb','?')}GB</b></span>"
                    f"<span style='color:{clr};'>{int(row['total_runs'])} runs</span>"
                    + (f"<span style='color:#94a3b8;'>Avg energy: "
                       f"<b style='color:#f59e0b;'>{row['avg_energy_j']:.4f}J</b></span>"
                       if row["avg_energy_j"] else "")
                    + "</div></div>",
                    unsafe_allow_html=True,
                )
        else:
            st.info("No hardware_config records yet.")

        st.markdown(
            f"<div style='padding:10px 14px;background:#0c1f3a;"
            f"border-left:3px solid #3b82f6;border-radius:0 8px 8px 0;"
            f"font-size:11px;color:#93c5fd;"
            f"font-family:IBM Plex Mono,monospace;margin-top:12px;line-height:1.8;'>"
            f"Currently running on a single host. Multi-host dispatch would allow "
            f"running the same experiment matrix on multiple machines simultaneously, "
            f"revealing per-hardware energy differences."
            f"</div>",
            unsafe_allow_html=True,
        )

    with tab2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;'>"
            f"Planned architecture</div>",
            unsafe_allow_html=True,
        )
        steps = [
            ("1. Host registry",
             "Register lab machines in hardware_config with SSH credentials. "
             "Each host runs a lightweight A-LEMS agent.",
             "#3b82f6"),
            ("2. Dispatch queue",
             "GUI creates a job batch: {task, provider, repetitions, target_hosts[]}. "
             "Queue stored in dispatch_jobs table (new).",
             "#f59e0b"),
            ("3. Agent execution",
             "Each host agent polls the queue, runs test_harness locally, "
             "saves results to its own local DB.",
             "#22c55e"),
            ("4. DB sync",
             "Periodic rsync or SQLite merge pulls remote DBs into central DB. "
             "run_id namespace per hw_id prevents collisions.",
             "#a78bfa"),
            ("5. Cross-host analysis",
             "Capability Matrix page compares same task across hw_ids. "
             "Multi-Host Status shows live progress per machine.",
             "#38bdf8"),
        ]
        for title, body, clr in steps:
            st.markdown(
                f"<div style='padding:10px 14px;background:#0d1117;"
                f"border:1px solid {clr}33;border-left:3px solid {clr};"
                f"border-radius:8px;margin-bottom:8px;'>"
                f"<div style='font-size:11px;font-weight:600;color:{clr};"
                f"margin-bottom:3px;'>{title}</div>"
                f"<div style='font-size:10px;color:#94a3b8;line-height:1.6;"
                f"font-family:IBM Plex Mono,monospace;'>{body}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    with tab3:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px;'>"
            f"Research questions multi-host enables</div>",
            unsafe_allow_html=True,
        )
        questions = [
            ("Hardware generalisation",
             "Does the agentic orchestration tax hold on ARM vs x86? "
             "Low-power laptop vs workstation?", "#22c55e"),
            ("Cross-hardware normalisation",
             "energy_per_token normalises for model size — "
             "does it also normalise for hardware?", "#3b82f6"),
            ("Parallel throughput measurement",
             "Run 30-run cells on 3 machines simultaneously — "
             "cut data collection time by 3×.", "#f59e0b"),
            ("Thermal variance across hardware",
             "Is thermal throttling a per-machine artifact "
             "or a workload characteristic?", "#a78bfa"),
        ]
        for title, body, clr in questions:
            st.markdown(
                f"<div style='padding:10px 14px;background:#0d1117;"
                f"border:1px solid {clr}33;border-left:3px solid {clr};"
                f"border-radius:8px;margin-bottom:8px;'>"
                f"<div style='font-size:11px;font-weight:600;color:{clr};"
                f"margin-bottom:3px;'>{title}</div>"
                f"<div style='font-size:10px;color:#94a3b8;line-height:1.6;'>{body}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )
