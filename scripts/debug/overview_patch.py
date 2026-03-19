"""
overview_patch.py
─────────────────────────────────────────────────────────────────────────────
Run this script ONCE to patch overview.py:

  1. Replaces the 6 st.metric() KPI row with uniform HTML cards
  2. Adds Data Health strip after KPI row
  3. Removes Row 4 scatter charts (Duration vs Energy + IPC vs Cache Miss)

Usage:
    cd ~/mydrive/a-lems
    python3 overview_patch.py

Safe to re-run — checks if already patched before modifying.
─────────────────────────────────────────────────────────────────────────────
"""

import re
from pathlib import Path

OVERVIEW = Path(__file__).parent / "gui" / "pages" / "overview.py"

src = OVERVIEW.read_text()

# ── Guard: already patched? ───────────────────────────────────────────────────
if "# PATCHED_KPI_V1" in src:
    print("✅ overview.py already patched — nothing to do.")
    exit(0)

# ─────────────────────────────────────────────────────────────────────────────
# PATCH 1: Replace st.metric() KPI row with uniform HTML cards
# ─────────────────────────────────────────────────────────────────────────────
OLD_KPI = """    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Runs",   ov.get("total_runs","—"))
    c2.metric("Tax Multiple", f"{tax_mult:.1f}×",
              delta=f"{(tax_mult-1)*100:.0f}% overhead", delta_color="inverse")
    c3.metric("Avg Planning", f"{plan_ms:.0f}ms",
              delta=f"{plan_pct:.0f}% of agentic time", delta_color="inverse")
    c4.metric("Peak IPC",     f"{ov.get('max_ipc', 0) or 0:.3f}")
    c5.metric("Avg Carbon",   f"{ov.get('avg_carbon_mg', 0) or 0:.3f}mg")
    c6.metric("Total Energy", f"{ov.get('total_energy_j', 0) or 0:.1f}J")
    st.markdown("<div style='margin-bottom:12px'></div>", unsafe_allow_html=True)"""

NEW_KPI = '''    # PATCHED_KPI_V1
    # ── Uniform KPI cards — 6 equal-height boxes ──────────────────────────────
    _kpi_runs    = ov.get("total_runs", "—")
    _kpi_tax     = f"{tax_mult:.1f}×"
    _kpi_plan    = f"{plan_ms:.0f}ms"
    _kpi_ipc     = f"{ov.get('max_ipc', 0) or 0:.3f}"
    _kpi_carbon  = f"{ov.get('avg_carbon_mg', 0) or 0:.3f}mg"
    _kpi_energy  = f"{ov.get('total_energy_j', 0) or 0:.1f}J"

    _kpi_sub_tax    = f"{(tax_mult-1)*100:.0f}% overhead"
    _kpi_sub_plan   = f"{plan_pct:.0f}% agentic time"
    _kpi_sub_ipc    = "instructions/cycle"
    _kpi_sub_carbon = "avg per run"
    _kpi_sub_energy = "total measured"

    def _kpi_card(val, label, sub, accent="#3b82f6"):
        return (
            f"<div style=\'background:{t[\"bg1\"]};border:0.5px solid {t[\"brd\"]};"
            f"border-top:3px solid {accent};border-radius:10px;"
            f"padding:14px 16px;height:88px;box-sizing:border-box;"
            f"display:flex;flex-direction:column;justify-content:space-between;\'>"
            f"<div style=\'font-size:22px;font-weight:700;color:{accent};"
            f"font-family:IBM Plex Mono,monospace;line-height:1;\'>{val}</div>"
            f"<div>"
            f"<div style=\'font-size:9px;font-weight:600;color:{t[\"t1\"]};"
            f"text-transform:uppercase;letter-spacing:.08em;\'>{label}</div>"
            f"<div style=\'font-size:8px;color:{t[\"t3\"]};margin-top:1px;\'>{sub}</div>"
            f"</div></div>"
        )

    _kc1, _kc2, _kc3, _kc4, _kc5, _kc6 = st.columns(6)
    with _kc1: st.markdown(_kpi_card(_kpi_runs,   "Total Runs",    "experiments run",  "#22c55e"), unsafe_allow_html=True)
    with _kc2: st.markdown(_kpi_card(_kpi_energy, "Total Energy",  _kpi_sub_energy,    "#f59e0b"), unsafe_allow_html=True)
    with _kc3: st.markdown(_kpi_card(_kpi_tax,    "Tax Multiple",  _kpi_sub_tax,       "#ef4444"), unsafe_allow_html=True)
    with _kc4: st.markdown(_kpi_card(_kpi_plan,   "Avg Planning",  _kpi_sub_plan,      "#3b82f6"), unsafe_allow_html=True)
    with _kc5: st.markdown(_kpi_card(_kpi_ipc,    "Peak IPC",      _kpi_sub_ipc,       "#a78bfa"), unsafe_allow_html=True)
    with _kc6: st.markdown(_kpi_card(_kpi_carbon, "Avg Carbon",    _kpi_sub_carbon,    "#34d399"), unsafe_allow_html=True)

    st.markdown("<div style=\'margin-bottom:8px\'></div>", unsafe_allow_html=True)

    # ── Data Health strip ─────────────────────────────────────────────────────
    try:
        from gui.db import q1 as _q1
        _dh_total   = int(_q1("SELECT COUNT(*) AS n FROM runs").get("n", 0) or 0)
        _dh_invalid = int(_q1(
            "SELECT COUNT(*) AS n FROM runs WHERE COALESCE(experiment_valid,1)=0"
        ).get("n", 0) or 0)
        _dh_throttle = int(_q1(
            "SELECT COUNT(*) AS n FROM runs WHERE COALESCE(thermal_throttle_flag,0)=1"
        ).get("n", 0) or 0)
        _dh_nobase  = int(_q1(
            "SELECT COUNT(*) AS n FROM runs WHERE baseline_id IS NULL"
        ).get("n", 0) or 0)
        _dh_noisy   = int(_q1(
            "SELECT COUNT(*) AS n FROM runs WHERE COALESCE(background_cpu_percent,0)>20"
        ).get("n", 0) or 0)

        # Sufficiency: cells with >= 30 runs
        _suf = _q1("""
            SELECT
                COUNT(*) AS total_cells,
                SUM(CASE WHEN run_count >= 30 THEN 1 ELSE 0 END) AS sufficient_cells
            FROM (
                SELECT COUNT(*) AS run_count
                FROM runs r JOIN experiments e ON r.exp_id=e.exp_id
                WHERE e.model_name IS NOT NULL AND e.task_name IS NOT NULL
                  AND e.workflow_type IS NOT NULL
                GROUP BY r.hw_id, e.model_name, e.task_name, e.workflow_type
            )
        """) or {}
        _total_cells = int(_suf.get("total_cells", 1) or 1)
        _suf_cells   = int(_suf.get("sufficient_cells", 0) or 0)
        _suf_pct     = round(_suf_cells / _total_cells * 100, 0)

        # Build chips
        def _dh_chip(val, label, clr, bg):
            return (
                f"<span style=\'display:inline-flex;align-items:center;gap:5px;"
                f"padding:4px 10px;border-radius:5px;background:{bg};"
                f"border:1px solid {clr}33;margin-right:6px;\'>"
                f"<span style=\'font-size:13px;font-weight:700;color:{clr};"
                f"font-family:IBM Plex Mono,monospace;\'>{val}</span>"
                f"<span style=\'font-size:9px;color:{clr};opacity:.8;"
                f"text-transform:uppercase;letter-spacing:.06em;\'>{label}</span>"
                f"</span>"
            )

        _chips = ""
        _chips += _dh_chip(
            _dh_invalid,
            "invalid runs",
            "#ef4444" if _dh_invalid > 0 else "#22c55e",
            "#2a0c0c" if _dh_invalid > 0 else "#052e1a"
        )
        _chips += _dh_chip(
            _dh_throttle,
            "throttled",
            "#f97316" if _dh_throttle > 0 else "#22c55e",
            "#2a1000" if _dh_throttle > 0 else "#052e1a"
        )
        _chips += _dh_chip(
            _dh_nobase,
            "no baseline",
            "#f59e0b" if _dh_nobase > 0 else "#22c55e",
            "#2a1a00" if _dh_nobase > 0 else "#052e1a"
        )
        _chips += _dh_chip(
            _dh_noisy,
            "noisy env",
            "#a78bfa" if _dh_noisy > 0 else "#22c55e",
            "#1a0e40" if _dh_noisy > 0 else "#052e1a"
        )
        _chips += _dh_chip(
            f"{_suf_pct:.0f}%",
            "data sufficient",
            "#22c55e" if _suf_pct >= 80 else "#f59e0b" if _suf_pct >= 40 else "#ef4444",
            "#052e1a" if _suf_pct >= 80 else "#2a1a00" if _suf_pct >= 40 else "#2a0c0c"
        )

        st.markdown(
            f"<div style=\'display:flex;align-items:center;padding:8px 14px;"
            f"background:{t[\'bg2\']};border:0.5px solid {t[\'brd\']};"
            f"border-radius:8px;margin-bottom:12px;\'>"
            f"<span style=\'font-size:9px;font-weight:700;color:{t[\'t3\']};"
            f"text-transform:uppercase;letter-spacing:.1em;margin-right:12px;"
            f"flex-shrink:0;\'>Data Health</span>"
            f"{_chips}"
            f"</div>",
            unsafe_allow_html=True
        )
    except Exception:
        pass

    st.markdown("<div style=\'margin-bottom:4px\'></div>", unsafe_allow_html=True)'''

# ─────────────────────────────────────────────────────────────────────────────
# PATCH 2: Remove Row 4 scatter charts
# These belong in Energy Lab and Efficiency Explorer pages
# ─────────────────────────────────────────────────────────────────────────────
OLD_ROW4 = """    # ── ROW 4: Duration vs Energy + IPC vs Cache Miss (original, themed) ───────
    if not runs.empty and "energy_j" in runs.columns:"""

NEW_ROW4 = """    # ── ROW 4: Removed — Duration vs Energy lives in Energy Lab
    #           IPC vs Cache Miss lives in Efficiency Explorer
    if False and not runs.empty and "energy_j" in runs.columns:"""

# ── Apply patches ─────────────────────────────────────────────────────────────
patched = src

if OLD_KPI in patched:
    patched = patched.replace(OLD_KPI, NEW_KPI)
    print("✅ Patch 1 applied: KPI row → uniform cards + data health strip")
else:
    print("⚠️  Patch 1 not applied: KPI row pattern not found (may have changed)")

if OLD_ROW4 in patched:
    patched = patched.replace(OLD_ROW4, NEW_ROW4)
    print("✅ Patch 2 applied: Row 4 scatter charts removed")
else:
    print("⚠️  Patch 2 not applied: Row 4 pattern not found")

# ── Write backup + patched file ───────────────────────────────────────────────
backup = OVERVIEW.with_suffix(".py.bak_overview_patch")
if not backup.exists():
    backup.write_text(src)
    print(f"📦 Backup saved: {backup}")

OVERVIEW.write_text(patched)
print(f"✅ overview.py patched successfully")
print("\nRestart Streamlit to see changes:")
print("  streamlit run streamlit_app.py")
