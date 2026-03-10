"""
gui/helpers.py
Shared helpers:
  - _human_energy / _human_water / _human_carbon  — readable comparisons
  - fl()                                           — apply Plotly dark theme
  - _gauge_html()                                  — SVG arc speedometer
  - _bar_gauge_html()                              — horizontal bar gauge
"""
import math

from gui.config import (
    PL,
    _PHONE_CHARGE_J, _WHATSAPP_MSG_J, _GOOGLE_SEARCH_J, _BABY_FEED_ML,
)


# ── Human-insight formatters ──────────────────────────────────────────────────
def _human_energy(joules: float) -> list:
    """Return list of (emoji, description) for a joule value."""
    if joules <= 0:
        return []
    ins = []
    phone_pct = joules / _PHONE_CHARGE_J * 100
    ins.append(("📱", f"{phone_pct:.5f}% of a full phone charge"))
    led_ms = joules / 10 * 1000
    ins.append(("💡", f"{led_ms:.1f}ms of a 10W LED bulb"
                      if led_ms < 1000 else f"{led_ms/1000:.2f}s of a 10W LED"))
    msgs = joules / _WHATSAPP_MSG_J
    ins.append(("💬", f"≈{msgs:.0f} WhatsApp messages"))
    searches = joules / _GOOGLE_SEARCH_J
    ins.append(("🔍", f"≈{searches:.3f} Google searches"))
    return ins


def _human_water(ml: float) -> str:
    if not ml or ml <= 0:
        return "—"
    if ml < 1:
        return f"{ml*1000:.1f}µl (raindrop≈50µl)"
    if ml < 150:
        return f"{ml:.2f}ml ({ml/_BABY_FEED_ML*100:.1f}% of one baby feed)"
    return f"{ml:.1f}ml ({ml/_BABY_FEED_ML:.1f}× baby feeds)"


def _human_carbon(mg: float) -> str:
    if not mg or mg <= 0:
        return "—"
    car_mm = mg / 1000 / 120 * 1e6
    return f"{mg:.3f}mg CO₂e ≈ {car_mm:.2f}mm of car driving"


# ── Plotly theme helper ───────────────────────────────────────────────────────
def fl(fig, **kw):
    """Apply A-LEMS dark Plotly theme to any figure."""
    fig.update_layout(**PL, **kw)
    return fig


# ── SVG gauge widgets ─────────────────────────────────────────────────────────
def _gauge_html(value: float, vmin: float, vmax: float,
                label: str, unit: str, color: str,
                warn: float = None, danger: float = None) -> str:
    """
    Render an SVG arc speedometer gauge (120×90 px).
    warn / danger thresholds change the needle colour.
    """
    pct   = max(0.0, min(1.0, (value - vmin) / max(vmax - vmin, 1e-9)))
    angle = -140 + pct * 280          # arc from -140° to +140°
    rad   = math.pi / 180
    r_arc = 52
    cx, cy = 60, 62

    ex = cx + r_arc * math.sin(angle * rad)
    ey = cy - r_arc * math.cos(angle * rad)
    large = 1 if pct > 0.5 else 0

    if danger and value >= danger:
        nclr = "#ef4444"
    elif warn and value >= warn:
        nclr = "#f59e0b"
    else:
        nclr = color

    bx  = cx + r_arc * math.sin(140 * rad)
    by  = cy - r_arc * math.cos(140 * rad)
    ex0 = cx - r_arc * math.sin(140 * rad)
    ey0 = cy - r_arc * math.cos(140 * rad)

    return f"""
    <div style="text-align:center;padding:4px 0;">
      <svg width="120" height="90" viewBox="0 0 120 90">
        <path d="M {bx:.1f} {by:.1f} A {r_arc} {r_arc} 0 1 1 {ex0:.1f} {ey0:.1f}"
              fill="none" stroke="#1e2d45" stroke-width="8" stroke-linecap="round"/>
        <path d="M {bx:.1f} {by:.1f} A {r_arc} {r_arc} 0 {large} 1 {ex:.1f} {ey:.1f}"
              fill="none" stroke="{nclr}" stroke-width="8" stroke-linecap="round"/>
        <circle cx="{cx}" cy="{cy}" r="4" fill="{nclr}"/>
        <text x="{cx}" y="{cy+4}" text-anchor="middle"
              font-size="14" font-weight="700" fill="#e8f0f8"
              font-family="monospace">{value:.1f}</text>
        <text x="{cx}" y="{cy+18}" text-anchor="middle"
              font-size="7" fill="#7090b0">{unit}</text>
        <text x="{cx}" y="82" text-anchor="middle"
              font-size="8" font-weight="600" fill="{nclr}">{label}</text>
        <text x="6"   y="72" text-anchor="middle" font-size="6" fill="#3d5570">{vmin}</text>
        <text x="114" y="72" text-anchor="middle" font-size="6" fill="#3d5570">{vmax}</text>
      </svg>
    </div>"""


def _bar_gauge_html(value: float, vmax: float,
                    label: str, unit: str, color: str) -> str:
    """Horizontal progress-bar gauge for CPU util / IRQ / IPC."""
    pct = max(0.0, min(100.0, value / max(vmax, 1) * 100))
    return f"""
    <div style="margin:6px 0 10px;">
      <div style="display:flex;justify-content:space-between;
                  font-size:9px;color:#7090b0;margin-bottom:3px;">
        <span style="font-weight:600;color:#e8f0f8">{label}</span>
        <span style="font-family:monospace;color:{color}">{value:.0f} {unit}</span>
      </div>
      <div style="background:#1e2d45;border-radius:3px;height:8px;overflow:hidden;">
        <div style="background:{color};width:{pct:.1f}%;height:100%;
                    border-radius:3px;transition:width 0.3s;"></div>
      </div>
    </div>"""
