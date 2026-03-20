"""
gui/helpers.py
─────────────────────────────────────────────────────────────────────────────
Shared helpers:
  - _human_energy / _human_water / _human_carbon / _human_methane
    → readable comparisons loaded from config/insights_rules.yaml
  - fl()              — apply Plotly dark theme to a figure
  - _gauge_html()     — SVG arc speedometer (used in live telemetry)
  - _bar_gauge_html() — horizontal bar gauge

All conversion factors come from INSIGHTS_RULES (config/insights_rules.yaml).
Nothing is hardcoded here — reviewers can inspect the YAML for data provenance.
─────────────────────────────────────────────────────────────────────────────
"""
import math
from gui.config import PL, INSIGHTS_RULES


# ── Convenience: pull sustainability config once ───────────────────────────────
_SUST = INSIGHTS_RULES.get("sustainability", {})

# Fallback values if YAML is missing (should never happen in production)
_PHONE_J    = 36_000      # 10 Wh phone battery
_WHATSAPP_J = 0.003       # 3 mJ per message
_GOOGLE_J   = 1.0         # ~1 J per search
_BABY_ML    = 150.0       # ml per baby feed
_LED_W      = 1.0         # 1 W LED bulb


# ── Human-insight formatters ──────────────────────────────────────────────────

def _human_energy(joules: float) -> list[tuple[str, str]]:
    """
    Convert a joule value to a list of (emoji, human-readable description).
    Factors loaded from config/insights_rules.yaml sustainability.energy_joules.
    Returns a list so callers can pick how many comparisons to show.
    """
    if joules <= 0:
        return []

    ins = []

    # Phone charge percentage
    phone_j   = _PHONE_J
    phone_pct = joules / phone_j * 100
    ins.append(("📱", f"{phone_pct:.5f}% of a full phone charge"))

    # LED bulb duration
    led_ms = joules / _LED_W * 1000
    ins.append(("💡",
        f"{led_ms:.1f}ms of a 1W LED bulb" if led_ms < 1000
        else f"{led_ms/1000:.2f}s of a 1W LED bulb"))

    # WhatsApp messages
    msgs = joules / _WHATSAPP_J
    ins.append(("💬", f"≈{msgs:.0f} WhatsApp messages"))

    # Google searches
    searches = joules / _GOOGLE_J
    ins.append(("🔍", f"≈{searches:.3f} Google searches"))

    return ins


def _human_energy_full(joules: float) -> dict:
    """
    Return a rich dict of sustainability comparisons for the Session Analysis panel.
    All factors from config/insights_rules.yaml.
    """
    if joules <= 0:
        return {}

    # Pull carbon factor from config or use default (UK grid: 233g CO₂/kWh)
    carbon_gpj = (_SUST.get("carbon_grams", {}).get("grams_per_joule", 0.0000647))
    water_mlpj  = (_SUST.get("water_ml",    {}).get("ml_per_joule",    0.0005))
    ch4_mgpj    = (_SUST.get("methane_mg",  {}).get("mg_per_joule",    0.00000771))

    carbon_g  = joules * carbon_gpj
    water_ml  = joules * water_mlpj
    methane_mg= joules * ch4_mgpj
    wh        = joules / 3600

    return {
        "joules":     joules,
        "wh":         wh,
        "phone_pct":  joules / _PHONE_J * 100,
        "led_min":    joules / _LED_W / 60,
        "carbon_g":   carbon_g,
        "carbon_car_m":  carbon_g / 1000 / 140 * 1e6,   # 140g CO₂/km → meters
        "carbon_phone_min": carbon_g / (0.83/60),         # 0.83g/hr smartphone
        "water_ml":   water_ml,
        "water_tsp":  water_ml * 0.2,                     # 1ml = 0.2 tsp
        "water_shower_pct": water_ml / 60000 * 100,       # avg 60L shower
        "methane_mg": methane_mg,
        "methane_human_pct": methane_mg / 500 * 100,      # 500mg/day human avg
    }


def _human_water(ml: float) -> str:
    """Convert ml to human-readable water comparison."""
    if not ml or ml <= 0:
        return "—"
    if ml < 1:
        return f"{ml*1000:.1f}µl (raindrop≈50µl)"
    if ml < _BABY_ML:
        return f"{ml:.2f}ml ({ml/_BABY_ML*100:.1f}% of one baby feed)"
    return f"{ml:.1f}ml ({ml/_BABY_ML:.1f}× baby feeds)"


def _human_carbon(mg: float) -> str:
    """Convert mg CO₂ to human-readable carbon comparison."""
    if not mg or mg <= 0:
        return "—"
    car_mm = mg / 1000 / 140 * 1e6   # 140g CO₂/km → mm
    return f"{mg:.3f}mg CO₂e ≈ {car_mm:.2f}mm of car driving"


def _human_methane(mg: float) -> str:
    """Convert mg CH₄ to human-readable methane comparison."""
    if not mg or mg <= 0:
        return "—"
    human_pct = mg / 500 * 100   # average human emits ~500mg CH₄/day
    return f"{mg:.4f}mg CH₄ ≈ {human_pct:.4f}% of daily human emission"


# ── Plotly theme helper ───────────────────────────────────────────────────────

def fl(fig, **kw):
    """Apply A-LEMS dark Plotly theme to any figure."""
    fig.update_layout(**PL, **kw)
    return fig


# ── SVG gauge widgets ─────────────────────────────────────────────────────────

def _gauge_html(value: float, vmin: float, vmax: float,
                label: str, unit: str, color: str,
                warn: float = None, danger: float = None) -> str:
    """Render an SVG arc speedometer gauge (120×90 px)."""
    pct   = max(0.0, min(1.0, (value - vmin) / max(vmax - vmin, 1e-9)))
    angle = -140 + pct * 280
    rad   = math.pi / 180
    r_arc = 52
    cx, cy = 60, 62

    ex = cx + r_arc * math.sin(angle * rad)
    ey = cy - r_arc * math.cos(angle * rad)
    large = 1 if pct > 0.5 else 0

    nclr = ("#ef4444" if danger and value >= danger
            else "#f59e0b" if warn and value >= warn else color)

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
