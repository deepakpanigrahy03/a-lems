"""
gui/pages/system_profile_page.py
─────────────────────────────────────────────────────────────────────────────
REPORTS → System Profile

Auto-detects and displays hardware profile.
Eliminates "Unknown hardware" from every report permanently.

Three tabs:
  ▣  Current Profile   — live hardware card
  ◎  Profile History   — all stored profiles with timestamps
  ⚡  RAPL Zones        — detected power domains + idle baseline
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import json, sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gui.config import PL, DB_PATH
from gui.report_engine.system_profiler import (
    collect_profile, save_profile, load_latest_profile,
)
from gui.report_engine.models import SystemProfile, EnvType


# ── Header ─────────────────────────────────────────────────────────────────────

def _header() -> None:
    st.markdown("""
    <div style="background:linear-gradient(90deg,#0f1520,#1a1f35);
                padding:1.5rem 1.8rem;border-radius:10px;
                border-left:4px solid #a78bfa;margin-bottom:1.5rem;">
      <h2 style="margin:0;color:#e8f0f8;font-family:'IBM Plex Mono',monospace;
                 font-size:1.25rem;">▣  System Profile</h2>
      <p style="margin:.4rem 0 0;color:#7090b0;font-size:.82rem;
                font-family:'IBM Plex Mono',monospace;">
        Auto-detected hardware · CPU · RAM · RAPL power domains · environment type ·
        injected into every report
      </p>
    </div>
    """, unsafe_allow_html=True)


# ── Env type badge ─────────────────────────────────────────────────────────────

_ENV_COLORS = {
    EnvType.LOCAL:  "#22c55e",
    EnvType.DOCKER: "#3b82f6",
    EnvType.VM:     "#f59e0b",
    EnvType.CLOUD:  "#a78bfa",
}


def _env_badge(env: EnvType) -> str:
    color = _ENV_COLORS.get(env, "#7090b0")
    return (
        f"<span style='background:{color}22;color:{color};"
        f"border:1px solid {color};border-radius:10px;"
        f"padding:2px 10px;font-size:.75rem;"
        f"font-family:IBM Plex Mono,monospace;'>"
        f"{env.value}</span>"
    )


# ── Profile hardware card ───────────────────────────────────────────────────────

def _hardware_card(profile: SystemProfile) -> None:
    env_badge = _env_badge(profile.env_type)
    st.markdown(f"""
    <div style="background:#0d1828;border:1px solid #1e2d45;border-radius:12px;
                padding:1.4rem 1.6rem;font-family:'IBM Plex Mono',monospace;">
      <div style="display:flex;align-items:center;gap:1rem;margin-bottom:1rem;">
        <div>
          <div style="font-size:1rem;color:#e8f0f8;font-weight:600;">
            {profile.cpu_model}
          </div>
          <div style="font-size:.75rem;color:#7090b0;margin-top:.2rem;">
            {profile.cpu_cores_physical}P / {profile.cpu_cores_logical}L cores ·
            {profile.cpu_freq_max_mhz:.0f} MHz max ·
            {profile.ram_gb:.1f} GB RAM
          </div>
        </div>
        <div style="margin-left:auto;">{env_badge}</div>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:.5rem;">
        <div>
          <div style="font-size:.68rem;color:#4a6080;margin-bottom:.2rem;">OS</div>
          <div style="font-size:.78rem;color:#c8d8e8;">{profile.os_name}</div>
        </div>
        <div>
          <div style="font-size:.68rem;color:#4a6080;margin-bottom:.2rem;">GPU</div>
          <div style="font-size:.78rem;color:#c8d8e8;">{profile.gpu_model or 'None detected'}</div>
        </div>
        <div>
          <div style="font-size:.68rem;color:#4a6080;margin-bottom:.2rem;">TDP</div>
          <div style="font-size:.78rem;color:#c8d8e8;">
            {f'{profile.thermal_tdp_w:.0f} W' if profile.thermal_tdp_w else 'Unknown'}
          </div>
        </div>
        <div>
          <div style="font-size:.68rem;color:#4a6080;margin-bottom:.2rem;">Disk</div>
          <div style="font-size:.78rem;color:#c8d8e8;">
            {f'{profile.disk_gb:.0f} GB' if profile.disk_gb else 'Unknown'}
          </div>
        </div>
        <div>
          <div style="font-size:.68rem;color:#4a6080;margin-bottom:.2rem;">Collected</div>
          <div style="font-size:.78rem;color:#c8d8e8;">
            {profile.collected_at.strftime('%Y-%m-%d %H:%M UTC')}
          </div>
        </div>
        <div>
          <div style="font-size:.68rem;color:#4a6080;margin-bottom:.2rem;">Profile ID</div>
          <div style="font-size:.78rem;color:#c8d8e8;">{profile.profile_id[:18]}…</div>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)


# ── RAPL zones visualisation ────────────────────────────────────────────────────

def _rapl_zones_chart(zones: list[str]) -> None:
    if not zones or zones == ["RAPL not available"]:
        st.warning("RAPL power sensors not available on this system.")
        st.caption(
            "RAPL (Running Average Power Limit) requires Linux with Intel or AMD CPU "
            "and the `powercap` kernel module loaded. "
            "Energy measurements will be unavailable without RAPL access."
        )
        return

    # Display zones as a visual list
    st.markdown("**Detected RAPL power domains:**")
    zone_cols = st.columns(min(len(zones), 4))
    zone_colors = ["#22c55e", "#3b82f6", "#f59e0b", "#a78bfa", "#38bdf8", "#f472b6"]
    for i, zone in enumerate(zones):
        zone_cols[i % 4].markdown(
            f"<div style='background:#0d1828;border:1px solid #1e2d45;"
            f"border-left:3px solid {zone_colors[i % len(zone_colors)]};"
            f"border-radius:0 6px 6px 0;padding:.5rem .8rem;margin-bottom:.4rem;"
            f"font-family:IBM Plex Mono,monospace;font-size:.78rem;color:#c8d8e8;'>"
            f"{zone}</div>",
            unsafe_allow_html=True,
        )

    # Fetch idle baseline from DB if available
    try:
        conn = sqlite3.connect(str(DB_PATH))
        baselines = conn.execute("""
            SELECT baseline_id, package_power_watts, governor, turbo, measured_at
            FROM idle_baselines
            ORDER BY measured_at DESC
            LIMIT 10
        """).fetchall()
        conn.close()

        if baselines:
            st.markdown("**Idle baselines recorded:**")
            rows = []
            for b in baselines:
                rows.append({
                    "ID": b[0],
                    "Package Power (W)": f"{b[1]:.2f}",
                    "Governor": b[2] or "unknown",
                    "Turbo": "on" if b[3] else "off",
                    "Measured At": str(b[4])[:16],
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            # Power gauge
            latest_w = baselines[0][1]
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=latest_w,
                title={"text": "Idle package power (W)",
                       "font": {"color": "#7090b0", "size": 11,
                                "family": "IBM Plex Mono"}},
                gauge={
                    "axis": {"range": [0, 65], "tickcolor": "#1e2d45"},
                    "bar": {"color": "#22c55e"},
                    "bgcolor": "#090d13",
                    "bordercolor": "#1e2d45",
                    "steps": [
                        {"range": [0, 15],  "color": "#0d1828"},
                        {"range": [15, 35], "color": "#0f2030"},
                        {"range": [35, 65], "color": "#1a1520"},
                    ],
                    "threshold": {
                        "line": {"color": "#ef4444", "width": 2},
                        "thickness": 0.75,
                        "value": 45,
                    },
                },
                number={"font": {"color": "#22c55e",
                                 "family": "IBM Plex Mono"}, "suffix": " W"},
            ))
            fig.update_layout(
                paper_bgcolor="#0f1520",
                font=dict(family="IBM Plex Mono", color="#7090b0"),
                height=250,
                margin=dict(l=20, r=20, t=40, b=20),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("No idle baselines recorded yet. Run an idle baseline experiment first.")
    except Exception:
        st.caption("idle_baselines table not yet available.")


# ── Tab 1: Current Profile ──────────────────────────────────────────────────────

def _tab_current(profile: SystemProfile | None) -> None:
    if profile is None:
        st.warning(
            "No system profile found in the database. "
            "Click **Scan Hardware** below to collect one."
        )
    else:
        _hardware_card(profile)
        st.markdown("")

        # Report injection preview
        st.markdown("**How this appears in reports:**")
        st.code(
            f"System Profile: {profile.summary_line()}",
            language=None,
        )
        st.markdown(
            "<div style='font-size:.74rem;color:#7090b0;"
            "font-family:IBM Plex Mono,monospace;'>"
            "This line appears in the System Profile section of every generated report. "
            "RAPL zones are also listed in the Methodology section."
            "</div>",
            unsafe_allow_html=True,
        )
        st.markdown("")

    # Scan button
    col1, col2 = st.columns([1, 3])
    if col1.button(
        "⟳  Scan Hardware",
        type="primary" if profile is None else "secondary",
        use_container_width=True,
        key="sp_scan_btn",
    ):
        with st.spinner("Collecting hardware profile…"):
            try:
                new_profile = collect_profile()
                conn = sqlite3.connect(str(DB_PATH))
                save_profile(new_profile, conn)
                conn.close()
                st.success(
                    f"✓ Profile collected: {new_profile.summary_line()}"
                )
                st.rerun()
            except Exception as e:
                st.error(f"Hardware scan failed: {e}")
                import traceback
                st.code(traceback.format_exc())

    col2.caption(
        "Scan runs automatically on first startup. "
        "Re-scan after hardware changes (RAM upgrade, new CPU, container migration)."
    )

    # Environment explanation
    if profile:
        env_info = {
            EnvType.LOCAL:  "Running directly on physical hardware. RAPL sensors have full access.",
            EnvType.DOCKER: "Running inside a Docker container. RAPL access depends on host configuration.",
            EnvType.VM:     "Running inside a virtual machine. RAPL sensors may be emulated or unavailable.",
            EnvType.CLOUD:  "Running on cloud infrastructure. RAPL sensors typically unavailable; energy estimated.",
        }
        info = env_info.get(profile.env_type, "")
        if info:
            st.info(f"**{profile.env_type.value} environment:** {info}")


# ── Tab 2: Profile History ──────────────────────────────────────────────────────

def _tab_history() -> None:
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute("""
            SELECT profile_id, cpu_model, cpu_cores_logical, ram_gb,
                   env_type, os_name, rapl_zones_json, collected_at
            FROM system_profiles
            ORDER BY collected_at DESC
        """).fetchall()
        conn.close()
    except Exception as e:
        st.warning(f"Cannot read system_profiles: {e}")
        return

    if not rows:
        st.info("No profiles stored yet. Click Scan Hardware on the Current Profile tab.")
        return

    st.markdown(f"**{len(rows)} profile(s) recorded**")

    for row in rows:
        profile_id, cpu, cores, ram, env, os_name, rapl_json, collected = row
        try:
            rapl_zones = json.loads(rapl_json or "[]")
        except Exception:
            rapl_zones = []

        with st.expander(
            f"{str(collected)[:16]}  ·  {cpu}  ·  {env}",
            expanded=(row == rows[0]),
        ):
            cols = st.columns(2)
            info = [
                ("Profile ID", profile_id[:22] + "…"),
                ("CPU", cpu),
                ("Cores (logical)", str(cores)),
                ("RAM", f"{ram:.1f} GB" if ram else "Unknown"),
                ("Environment", env),
                ("OS", os_name or "Unknown"),
                ("RAPL zones", str(len(rapl_zones))),
                ("Collected", str(collected)[:19]),
            ]
            for i, (k, v) in enumerate(info):
                with cols[i % 2]:
                    st.markdown(
                        f"<div style='font-family:IBM Plex Mono,monospace;"
                        f"font-size:.76rem;padding:3px 0;"
                        f"border-bottom:1px solid #1e2d45;'>"
                        f"<span style='color:#7090b0;'>{k}:</span>"
                        f" <span style='color:#c8d8e8;'>{v}</span></div>",
                        unsafe_allow_html=True,
                    )
            if rapl_zones:
                st.markdown(
                    "<div style='font-family:IBM Plex Mono,monospace;"
                    "font-size:.74rem;color:#7090b0;margin-top:.5rem;'>"
                    "RAPL: " + " · ".join(rapl_zones[:8]) + "</div>",
                    unsafe_allow_html=True,
                )


# ── Tab 3: RAPL Zones ───────────────────────────────────────────────────────────

def _tab_rapl(profile: SystemProfile | None) -> None:
    if profile is None:
        st.info("No system profile available. Scan hardware first.")
        return

    st.markdown("#### ⚡  RAPL Power Domains")
    st.markdown(
        "<div style='font-size:.78rem;color:#7090b0;"
        "font-family:IBM Plex Mono,monospace;margin-bottom:1rem;'>"
        "RAPL (Running Average Power Limit) exposes per-domain energy counters "
        "via Linux powercap interface. A-LEMS reads these every measurement cycle "
        "to produce the energy measurements in your database."
        "</div>",
        unsafe_allow_html=True,
    )
    _rapl_zones_chart(profile.rapl_zones)

    # ML feature note
    st.markdown("---")
    st.markdown("#### ◈  Report Impact")
    impact_rows = [
        ("Title Page",          "Hardware: {cpu} · {cores} cores · {ram} GB RAM"),
        ("System Profile",      "Full hardware table with all detected fields"),
        ("Methodology",         "RAPL zones listed, env type noted"),
        ("Limitations",         "Missing RAPL zones flagged as measurement caveat"),
        ("Reproducibility hash","Profile ID included in hash computation"),
    ]
    for section, impact in impact_rows:
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;"
            f"font-family:IBM Plex Mono,monospace;font-size:.76rem;"
            f"padding:4px 0;border-bottom:1px solid #1e2d45;'>"
            f"<span style='color:#a78bfa;'>{section}</span>"
            f"<span style='color:#7090b0;max-width:60%;text-align:right;'>{impact}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )


# ── Main render ────────────────────────────────────────────────────────────────

def render(ctx: dict) -> None:
    _header()

    # Load latest profile from DB
    try:
        conn = sqlite3.connect(str(DB_PATH))
        profile = load_latest_profile(conn)
        conn.close()
    except Exception:
        profile = None

    # KPI strip
    c1, c2, c3, c4 = st.columns(4)
    for col, val, label, color in [
        (c1, profile.cpu_model[:22] + "…" if profile and len(profile.cpu_model) > 22
             else (profile.cpu_model if profile else "Not scanned"),
             "CPU",          "#e8f0f8"),
        (c2, f"{profile.ram_gb:.0f} GB" if profile else "—",
             "RAM",          "#22c55e"),
        (c3, profile.env_type.value if profile else "—",
             "Environment",  _ENV_COLORS.get(profile.env_type, "#7090b0") if profile else "#7090b0"),
        (c4, len(profile.rapl_zones) if profile else 0,
             "RAPL zones",  "#f59e0b"),
    ]:
        col.markdown(f"""
        <div style="background:#0d1828;border:1px solid #1e2d45;border-radius:8px;
                    padding:.9rem 1rem;text-align:center;">
          <div style="font-size:.95rem;font-weight:600;color:{color};
                      font-family:'IBM Plex Mono',monospace;word-break:break-all;">
            {val}
          </div>
          <div style="font-size:.72rem;color:#7090b0;margin-top:.2rem;
                      font-family:'IBM Plex Mono',monospace;">{label}</div>
        </div>""", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom:1rem'></div>", unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs([
        "▣  Current Profile",
        "◎  Profile History",
        "⚡  RAPL Zones",
    ])

    with tab1:
        _tab_current(profile)

    with tab2:
        _tab_history()

    with tab3:
        _tab_rapl(profile)

    st.markdown("---")
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:.72rem;"
        "color:#4a5568;text-align:center;'>"
        "System Profile · Stored in system_profiles table · "
        "Auto-collected at first startup via db_migrations.py · "
        "Injected into every PDF and HTML report automatically"
        "</div>",
        unsafe_allow_html=True,
    )
