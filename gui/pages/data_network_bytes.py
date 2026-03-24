"""
gui/pages/data_network_bytes.py  —  ⊞  Network Bytes
─────────────────────────────────────────────────────────────────────────────
bytes_sent / bytes_recv / tcp_retransmits per run.
Shows actual data volume transferred — complementary to data_network.py
which covers latency. This page covers throughput and retransmission cost.
─────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, WF_COLORS
from gui.pages._dm_helpers import col_exists, get_runs, no_data_banner, rgba

ACCENT = "#38bdf8"


def _human_bytes(b: float) -> str:
    """Format bytes to human-readable string."""
    if b >= 1_000_000:
        return f"{b/1_000_000:.2f} MB"
    if b >= 1_000:
        return f"{b/1_000:.1f} KB"
    return f"{b:.0f} B"


def render(ctx: dict) -> None:
    df = get_runs(ctx)

    # ── Column check ──────────────────────────────────────────────────────────
    has_sent = "bytes_sent" in df.columns
    has_recv = "bytes_recv" in df.columns
    has_tcp  = "tcp_retransmits" in df.columns

    if df.empty or (not has_sent and not has_recv):
        no_data_banner(
            "bytes_sent / bytes_recv not yet in runs table. "
            "Check load_runs() — add these columns if missing from SELECT.",
            ACCENT,
        )
        return

    # ── Filter to rows that have at least one byte column populated ───────────
    mask = pd.Series(False, index=df.index)
    if has_sent:
        mask |= df["bytes_sent"].notna() & (df["bytes_sent"] > 0)
    if has_recv:
        mask |= df["bytes_recv"].notna() & (df["bytes_recv"] > 0)
    df = df[mask].copy()

    if df.empty:
        no_data_banner("All bytes_sent / bytes_recv values are zero or NULL.", ACCENT)
        return

    # ── Derived columns ───────────────────────────────────────────────────────
    if has_sent and has_recv:
        df["bytes_total"] = (
            df["bytes_sent"].fillna(0) + df["bytes_recv"].fillna(0)
        )
    elif has_sent:
        df["bytes_total"] = df["bytes_sent"].fillna(0)
    else:
        df["bytes_total"] = df["bytes_recv"].fillna(0)

    # Throughput: bytes_total / duration_s
    if "duration_ns" in df.columns:
        df["duration_s"] = df["duration_ns"] / 1e9
        df["throughput_kbps"] = df.apply(
            lambda r: (r["bytes_total"] / 1000) / max(r["duration_s"], 0.001)
            if r["duration_s"] > 0 else 0,
            axis=1,
        )
    else:
        df["duration_s"] = 0
        df["throughput_kbps"] = 0

    lin = df[df["workflow_type"] == "linear"]
    age = df[df["workflow_type"] == "agentic"]

    # ── Summary stats ─────────────────────────────────────────────────────────
    avg_sent_lin = lin["bytes_sent"].mean() if has_sent and not lin.empty else 0
    avg_sent_age = age["bytes_sent"].mean() if has_sent and not age.empty else 0
    avg_recv_lin = lin["bytes_recv"].mean() if has_recv and not lin.empty else 0
    avg_recv_age = age["bytes_recv"].mean() if has_recv and not age.empty else 0
    avg_tcp      = df["tcp_retransmits"].mean() if has_tcp else 0
    total_bytes  = df["bytes_total"].sum()

    # ── Header ────────────────────────────────────────────────────────────────
    kpi_items = [
        (_human_bytes(avg_sent_lin), "sent · linear",  WF_COLORS["linear"]),
        (_human_bytes(avg_sent_age), "sent · agentic", WF_COLORS["agentic"]),
        (_human_bytes(avg_recv_lin), "recv · linear",  WF_COLORS["linear"]),
        (_human_bytes(avg_recv_age), "recv · agentic", WF_COLORS["agentic"]),
    ]
    if has_tcp:
        kpi_items.append((f"{avg_tcp:.2f}", "avg tcp retransmits", "#f97316"))

    kpi_html = "".join(
        f"<div><div style='font-size:16px;font-weight:700;color:{c};"
        f"font-family:IBM Plex Mono,monospace;line-height:1;'>{v}</div>"
        f"<div style='font-size:9px;color:#94a3b8;text-transform:uppercase;"
        f"letter-spacing:.08em;margin-top:3px;'>{l}</div></div>"
        for v, l, c in kpi_items
    )

    cols_count = len(kpi_items)
    st.markdown(
        f"<div style='padding:14px 20px;"
        f"background:linear-gradient(135deg,{ACCENT}14,{ACCENT}06);"
        f"border:1px solid {ACCENT}33;border-radius:12px;margin-bottom:20px;'>"
        f"<div style='font-size:11px;font-weight:700;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px;'>"
        f"Network Bytes — {len(df)} runs · {_human_bytes(total_bytes)} total</div>"
        f"<div style='display:grid;grid-template-columns:repeat({cols_count},1fr);gap:12px;'>"
        f"{kpi_html}</div></div>",
        unsafe_allow_html=True,
    )

    # ── ROW 1: Bytes sent/recv grouped bar + Throughput ───────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Avg bytes sent vs received</div>",
            unsafe_allow_html=True,
        )
        categories = []
        sent_vals  = []
        recv_vals  = []
        for wf in ["linear", "agentic"]:
            sub = df[df["workflow_type"] == wf]
            if sub.empty:
                continue
            categories.append(wf)
            sent_vals.append(sub["bytes_sent"].mean() if has_sent else 0)
            recv_vals.append(sub["bytes_recv"].mean() if has_recv else 0)

        fig1 = go.Figure()
        if has_sent:
            fig1.add_trace(go.Bar(
                x=categories, y=sent_vals, name="Sent",
                marker_color="#3b82f6", marker_line_width=0,
            ))
        if has_recv:
            fig1.add_trace(go.Bar(
                x=categories, y=recv_vals, name="Recv",
                marker_color="#22c55e", marker_line_width=0,
            ))
        fig1.update_layout(
            **PL, height=260, barmode="group",
            yaxis_title="Bytes", yaxis_tickformat=".2s",
            showlegend=True,
        )
        st.plotly_chart(fig1, use_container_width=True, key="dnb_bar_srgroup")

    with col2:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Effective throughput (KB/s)</div>",
            unsafe_allow_html=True,
        )
        fig2 = go.Figure()
        for wf, clr in WF_COLORS.items():
            sub = df[df["workflow_type"] == wf]
            sub = sub[sub["throughput_kbps"] > 0]
            if sub.empty:
                continue
            fig2.add_trace(go.Histogram(
                x=sub["throughput_kbps"], name=wf,
                marker_color=clr, opacity=0.7, nbinsx=40,
            ))
        fig2.update_layout(
            **PL, height=260, barmode="overlay",
            xaxis_title="Throughput (KB/s)", yaxis_title="Run count",
        )
        st.plotly_chart(fig2, use_container_width=True, key="dnb_throughput_hist")

    # ── ROW 2: bytes_total vs energy scatter + bytes over time ───────────────
    col3, col4 = st.columns(2)

    with col3:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Total bytes vs energy</div>",
            unsafe_allow_html=True,
        )
        has_energy = "energy_j" in df.columns
        fig3 = go.Figure()
        if has_energy:
            for wf, clr in WF_COLORS.items():
                sub = df[df["workflow_type"] == wf].dropna(
                    subset=["bytes_total", "energy_j"]
                )
                sub = sub[(sub["bytes_total"] > 0) & (sub["energy_j"] > 0)]
                if sub.empty:
                    continue
                fig3.add_trace(go.Scatter(
                    x=sub["bytes_total"], y=sub["energy_j"],
                    mode="markers", name=wf,
                    marker=dict(color=clr, size=5, opacity=0.6),
                ))
            fig3.update_layout(
                **PL, height=260,
                xaxis_title="Total bytes", xaxis_tickformat=".2s",
                yaxis_title="Energy (J)",
            )
        else:
            fig3.add_annotation(
                text="energy_j not available",
                xref="paper", yref="paper", x=0.5, y=0.5,
                showarrow=False,
                font=dict(color="#475569", size=12),
            )
            fig3.update_layout(**PL, height=260)
        st.plotly_chart(fig3, use_container_width=True, key="dnb_bytes_energy")

    with col4:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px;'>"
            f"Bytes breakdown — sent / recv split</div>",
            unsafe_allow_html=True,
        )
        # Stacked bar: mean sent + recv per workflow
        fig4 = go.Figure()
        wf_labels = []
        sent_means = []
        recv_means = []
        for wf in ["linear", "agentic"]:
            sub = df[df["workflow_type"] == wf]
            if sub.empty:
                continue
            wf_labels.append(wf)
            sent_means.append(sub["bytes_sent"].mean() if has_sent else 0)
            recv_means.append(sub["bytes_recv"].mean() if has_recv else 0)

        if has_sent:
            fig4.add_trace(go.Bar(
                x=wf_labels, y=sent_means, name="Sent",
                marker_color="#3b82f6", marker_line_width=0,
            ))
        if has_recv:
            fig4.add_trace(go.Bar(
                x=wf_labels, y=recv_means, name="Recv",
                marker_color=ACCENT, marker_line_width=0,
            ))
        fig4.update_layout(
            **PL, height=260, barmode="stack",
            yaxis_title="Bytes", yaxis_tickformat=".2s",
        )
        st.plotly_chart(fig4, use_container_width=True, key="dnb_stacked_split")

    # ── TCP Retransmits ───────────────────────────────────────────────────────
    if has_tcp:
        tcp_df = df[df["tcp_retransmits"].notna()].copy()
        tcp_any = tcp_df["tcp_retransmits"].sum()

        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
            f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
            f"TCP Retransmits</div>",
            unsafe_allow_html=True,
        )

        if tcp_any == 0:
            st.markdown(
                f"<div style='padding:12px 16px;background:#052e1a;"
                f"border-left:3px solid #22c55e;border-radius:0 8px 8px 0;"
                f"font-size:11px;color:#4ade80;"
                f"font-family:IBM Plex Mono,monospace;margin-bottom:12px;'>"
                f"✓ Zero TCP retransmits across all {len(tcp_df)} runs — "
                f"clean network path.</div>",
                unsafe_allow_html=True,
            )
        else:
            col5, col6 = st.columns(2)
            with col5:
                # Retransmits per run histogram
                fig5 = go.Figure()
                for wf, clr in WF_COLORS.items():
                    sub = tcp_df[tcp_df["workflow_type"] == wf]["tcp_retransmits"]
                    if sub.empty:
                        continue
                    fig5.add_trace(go.Histogram(
                        x=sub, name=wf, marker_color=clr, opacity=0.7, nbinsx=30,
                    ))
                fig5.update_layout(
                    **PL, height=240, barmode="overlay",
                    xaxis_title="TCP retransmits", yaxis_title="Runs",
                )
                st.plotly_chart(fig5, use_container_width=True, key="dnb_tcp_hist")

            with col6:
                # Retransmits vs energy
                fig6 = go.Figure()
                if "energy_j" in tcp_df.columns:
                    for wf, clr in WF_COLORS.items():
                        sub = tcp_df[tcp_df["workflow_type"] == wf].dropna(
                            subset=["tcp_retransmits", "energy_j"]
                        )
                        if sub.empty:
                            continue
                        fig6.add_trace(go.Scatter(
                            x=sub["tcp_retransmits"], y=sub["energy_j"],
                            mode="markers", name=wf,
                            marker=dict(color=clr, size=5, opacity=0.65),
                        ))
                    fig6.update_layout(
                        **PL, height=240,
                        xaxis_title="TCP retransmits", yaxis_title="Energy (J)",
                    )
                    st.plotly_chart(fig6, use_container_width=True, key="dnb_tcp_energy")

    # ── Percentile table ──────────────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;font-weight:600;color:{ACCENT};"
        f"text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;'>"
        f"Byte transfer percentiles by workflow</div>",
        unsafe_allow_html=True,
    )
    pct_rows = []
    for wf in df["workflow_type"].dropna().unique():
        sub = df[df["workflow_type"] == wf]
        row: dict = {"Workflow": wf}
        if has_sent:
            s = sub["bytes_sent"].dropna()
            row.update({
                "Sent P50": _human_bytes(s.quantile(0.50)) if not s.empty else "—",
                "Sent P90": _human_bytes(s.quantile(0.90)) if not s.empty else "—",
                "Sent Max": _human_bytes(s.max()) if not s.empty else "—",
            })
        if has_recv:
            r = sub["bytes_recv"].dropna()
            row.update({
                "Recv P50": _human_bytes(r.quantile(0.50)) if not r.empty else "—",
                "Recv P90": _human_bytes(r.quantile(0.90)) if not r.empty else "—",
                "Recv Max": _human_bytes(r.max()) if not r.empty else "—",
            })
        if has_tcp:
            t = sub["tcp_retransmits"].dropna()
            row["TCP Retransmits (avg)"] = f"{t.mean():.2f}" if not t.empty else "—"
        pct_rows.append(row)

    if pct_rows:
        st.dataframe(pd.DataFrame(pct_rows), use_container_width=True, hide_index=True)

    # ── Insight strip ─────────────────────────────────────────────────────────
    sent_ratio = avg_sent_age / avg_sent_lin if (has_sent and avg_sent_lin > 0) else 1
    recv_ratio = avg_recv_age / avg_recv_lin if (has_recv and avg_recv_lin > 0) else 1

    insight_lines = []
    if has_sent and avg_sent_lin > 0:
        insight_lines.append(
            f"Agentic sends <b>{sent_ratio:.1f}×</b> more bytes than linear "
            f"({_human_bytes(avg_sent_age)} vs {_human_bytes(avg_sent_lin)})"
        )
    if has_recv and avg_recv_lin > 0:
        insight_lines.append(
            f"Agentic receives <b>{recv_ratio:.1f}×</b> more bytes than linear "
            f"({_human_bytes(avg_recv_age)} vs {_human_bytes(avg_recv_lin)})"
        )
    if has_tcp and avg_tcp > 0:
        insight_lines.append(
            f"Average <b>{avg_tcp:.1f}</b> TCP retransmit(s) per run — "
            f"retransmits force CPU wakeups and extend API wait, increasing energy."
        )
    if not insight_lines:
        insight_lines.append(
            "Collect more runs with both workflow types to compare byte transfer costs."
        )

    st.markdown(
        f"<div style='margin-top:14px;padding:10px 16px;"
        f"background:{rgba(ACCENT, 0.07)};border-left:3px solid {ACCENT};"
        f"border-radius:0 8px 8px 0;font-size:11px;"
        f"color:#bae6fd;font-family:IBM Plex Mono,monospace;line-height:1.8;'>"
        + "<br>".join(insight_lines)
        + "</div>",
        unsafe_allow_html=True,
    )
