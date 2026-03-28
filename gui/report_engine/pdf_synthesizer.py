"""
A-LEMS Report Engine — PDF Synthesizer
Publication-quality PDF in a clean academic paper style.
White background, Times-Roman body text, Helvetica headings,
color used only for semantic indicators (confidence, verdict, significance).
"""

from __future__ import annotations
import io, logging
from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, Image, KeepTogether,
)
from reportlab.pdfgen import canvas as rl_canvas

from .models import (
    ResearchGoal, ReportNarrative, StatTestResult, SystemProfile,
    ConfidenceLevel, HypothesisVerdict, ReportConfig,
)

log = logging.getLogger(__name__)

# ── Colour palette (print-safe, white paper) ──────────────────────────────────
C_BLACK     = colors.HexColor("#111111")   # body text
C_HEADING   = colors.HexColor("#1a2744")   # section headings — dark navy
C_SUBHEAD   = colors.HexColor("#2d3f6b")   # subsection headings
C_MUTED     = colors.HexColor("#555555")   # captions, meta
C_FAINT     = colors.HexColor("#888888")   # footer, small labels
C_RULE      = colors.HexColor("#cccccc")   # horizontal rules, table borders
C_STRIPE    = colors.HexColor("#f5f7fa")   # alternating table row tint
C_HEADER_BG = colors.HexColor("#1a2744")   # table header background
C_HEADER_FG = colors.white                 # table header text

# Semantic — used sparingly
C_GREEN     = colors.HexColor("#15803d")   # significant / supported / HIGH
C_AMBER     = colors.HexColor("#b45309")   # medium / inconclusive
C_RED       = colors.HexColor("#dc2626")   # rejected / LOW
C_BLUE      = colors.HexColor("#1d4ed8")   # informational accent

_CONF_COLORS = {
    ConfidenceLevel.HIGH:   C_GREEN,
    ConfidenceLevel.MEDIUM: C_AMBER,
    ConfidenceLevel.LOW:    C_RED,
}
_VERDICT_COLORS = {
    HypothesisVerdict.SUPPORTED:    C_GREEN,
    HypothesisVerdict.REJECTED:     C_RED,
    HypothesisVerdict.INCONCLUSIVE: C_AMBER,
    HypothesisVerdict.INSUFFICIENT: C_MUTED,
}

# ── Typography ────────────────────────────────────────────────────────────────
_SERIF = "Times-Roman"
_SERIF_B = "Times-Bold"
_SERIF_I = "Times-Italic"
_SANS  = "Helvetica"
_SANS_B = "Helvetica-Bold"
_MONO  = "Courier"

W, H = A4
MARGIN = 22 * mm
COL_W  = W - 2 * MARGIN


# ── Style sheet ───────────────────────────────────────────────────────────────

def _styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()

    def s(name, parent="Normal", **kw) -> ParagraphStyle:
        return ParagraphStyle(name, parent=base[parent], **kw)

    return {
        # Headings
        "h1": s("H1", fontName=_SANS_B, fontSize=14, textColor=C_HEADING,
                spaceAfter=4, spaceBefore=14, leading=18),
        "h2": s("H2", fontName=_SANS_B, fontSize=11, textColor=C_SUBHEAD,
                spaceAfter=3, spaceBefore=10, leading=14),
        "h3": s("H3", fontName=_SANS_B, fontSize=10, textColor=C_SUBHEAD,
                spaceAfter=2, spaceBefore=7, leading=13),

        # Body
        "body": s("Body", fontName=_SERIF, fontSize=10, textColor=C_BLACK,
                  leading=15, spaceAfter=6, alignment=TA_JUSTIFY),
        "body_sm": s("BodySm", fontName=_SERIF, fontSize=9, textColor=C_MUTED,
                     leading=13, spaceAfter=4),
        "bullet": s("Bullet", fontName=_SERIF, fontSize=10, textColor=C_BLACK,
                    leading=14, spaceAfter=3, leftIndent=14,
                    bulletIndent=0, firstLineIndent=-14),

        # Specialised
        "code": s("Code", fontName=_MONO, fontSize=8, textColor=C_BLACK,
                  backColor=C_STRIPE, leading=11, spaceAfter=4,
                  leftIndent=8, rightIndent=8),
        "caption": s("Caption", fontName=_SANS, fontSize=8, textColor=C_MUTED,
                     alignment=TA_CENTER, spaceAfter=6, leading=11,
                     fontStyle="italic" if hasattr(ParagraphStyle, "fontStyle") else None),
        "toc_h1": s("TOCH1", fontName=_SANS, fontSize=10, textColor=C_BLACK,
                    leading=14, leftIndent=0, spaceAfter=2),
        "toc_h2": s("TOCH2", fontName=_SANS, fontSize=9, textColor=C_MUTED,
                    leading=13, leftIndent=14, spaceAfter=1),
    }


# ── Page template (header / footer) ──────────────────────────────────────────

class _PageTemplate:
    def __init__(self, title: str, goal_name: str, version: str):
        self.title     = title[:70]
        self.goal_name = goal_name
        self.version   = version

    def on_page(self, canv: rl_canvas.Canvas, doc):
        canv.saveState()
        # ─ thin top rule + running header
        canv.setStrokeColor(C_RULE)
        canv.setLineWidth(0.5)
        canv.line(MARGIN, H - 13*mm, W - MARGIN, H - 13*mm)
        canv.setFont(_SANS, 7.5)
        canv.setFillColor(C_FAINT)
        canv.drawString(MARGIN, H - 10.5*mm, f"A-LEMS · {self.goal_name}")
        canv.drawRightString(W - MARGIN, H - 10.5*mm, self.title)
        # ─ thin bottom rule + page number
        canv.line(MARGIN, 13*mm, W - MARGIN, 13*mm)
        canv.setFont(_SANS, 7.5)
        canv.drawString(
            MARGIN, 9.5*mm,
            f"Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}  ·  {self.version}",
        )
        canv.drawRightString(W - MARGIN, 9.5*mm, f"Page {doc.page}")
        canv.restoreState()

    def on_first_page(self, canv, doc):
        pass   # title page — no header/footer


# ── Flowable helpers ──────────────────────────────────────────────────────────

def _hr(weight: float = 0.5, color=C_RULE) -> HRFlowable:
    return HRFlowable(
        width="100%", thickness=weight, color=color,
        spaceAfter=3, spaceBefore=3,
    )


def _thick_hr(color=C_HEADING) -> HRFlowable:
    return HRFlowable(
        width="100%", thickness=1.5, color=color,
        spaceAfter=4, spaceBefore=2,
    )


def _heading(text: str, level: int, styles: dict) -> list:
    key = f"h{min(level, 3)}"
    elems: list = [Paragraph(text, styles[key])]
    if level == 1:
        elems.append(_hr())
    return elems


def _esc(text: str) -> str:
    """Escape XML special characters for ReportLab Paragraph."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _para(text: str, styles: dict, style_key: str = "body") -> Paragraph:
    return Paragraph(_esc(text), styles[style_key])


def _bullet_list(items: list[str], styles: dict) -> list:
    return [Paragraph(f"• {_esc(item)}", styles["bullet"]) for item in items]


# ── Confidence badge (inline table, no background fill) ───────────────────────

def _confidence_badge(level: ConfidenceLevel, rationale: str, styles: dict) -> Table:
    col = _CONF_COLORS.get(level, C_MUTED)
    label_style = ParagraphStyle(
        "badge_label", fontName=_SANS_B, fontSize=9,
        textColor=col, leading=12,
    )
    note_style = ParagraphStyle(
        "badge_note", fontName=_SERIF_I, fontSize=9,
        textColor=C_MUTED, leading=12,
    )
    data = [[
        Paragraph(f"Confidence: {level.value}", label_style),
        Paragraph(_esc(rationale[:160]), note_style),
    ]]
    t = Table(data, colWidths=[52*mm, COL_W - 52*mm])
    t.setStyle(TableStyle([
        ("LINEABOVE",    (0, 0), (-1, 0), 1.5, col),
        ("LINEBELOW",    (0, 0), (-1, 0), 0.5, C_RULE),
        ("LEFTPADDING",  (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING",   (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 6),
        ("VALIGN",       (0, 0), (-1, -1), "TOP"),
    ]))
    return t


# ── Statistical results table ─────────────────────────────────────────────────

def _stat_table(results: list[StatTestResult], styles: dict) -> list:
    if not results:
        return [_para("No statistical results available.", styles, "body_sm")]

    header = ["Metric", "Linear (mean)", "Agentic (mean)", "Δ%", "p-value", "Cohen's d", "Sig."]
    rows = [header]
    for r in results:
        rows.append([
            r.metric_name[:26],
            f"{r.group_a_mean:.3g}",
            f"{r.group_b_mean:.3g}",
            f"{r.pct_difference():+.1f}%",
            f"{r.p_value:.4f}",
            f"{r.effect_size:.2f} ({r.effect_label.value[:3]})",
            "✓" if r.significant else "–",
        ])

    col_widths = [48*mm, 24*mm, 24*mm, 16*mm, 18*mm, 32*mm, 10*mm]
    t = Table(rows, colWidths=col_widths, repeatRows=1)

    style = [
        # Header row
        ("BACKGROUND",   (0, 0), (-1, 0),  C_HEADER_BG),
        ("TEXTCOLOR",    (0, 0), (-1, 0),  C_HEADER_FG),
        ("FONTNAME",     (0, 0), (-1, 0),  _SANS_B),
        ("FONTSIZE",     (0, 0), (-1, 0),  8),
        # Body rows
        ("FONTNAME",     (0, 1), (-1, -1), _SERIF),
        ("FONTSIZE",     (0, 1), (-1, -1), 8.5),
        ("TEXTCOLOR",    (0, 1), (-1, -1), C_BLACK),
        # Alternating stripes
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, C_STRIPE]),
        # Grid
        ("GRID",         (0, 0), (-1, -1), 0.4, C_RULE),
        # Padding
        ("LEFTPADDING",  (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING",   (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 4),
        ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
    ]
    # Colour the significance column only
    for i, r in enumerate(results, start=1):
        col = C_GREEN if r.significant else C_FAINT
        style.append(("TEXTCOLOR", (-1, i), (-1, i), col))
        style.append(("FONTNAME",  (-1, i), (-1, i), _SANS_B if r.significant else _SANS))
    t.setStyle(TableStyle(style))
    return [t]


# ── System profile table ──────────────────────────────────────────────────────

def _system_profile_table(profile: SystemProfile, styles: dict) -> list:
    rows = [
        ["CPU model",   profile.cpu_model],
        ["Cores",       f"{profile.cpu_cores_physical} physical / {profile.cpu_cores_logical} logical"],
        ["Max freq.",   f"{profile.cpu_freq_max_mhz:.0f} MHz"],
        ["RAM",         f"{profile.ram_gb:.1f} GB"],
        ["Environment", profile.env_type.value],
        ["OS",          profile.os_name],
        ["RAPL zones",  ", ".join(profile.rapl_zones[:6])],
        ["GPU",         profile.gpu_model or "None detected"],
        ["TDP",         f"{profile.thermal_tdp_w:.0f} W" if profile.thermal_tdp_w else "Unknown"],
    ]
    t = Table(rows, colWidths=[44*mm, COL_W - 44*mm])
    t.setStyle(TableStyle([
        ("FONTNAME",      (0, 0), (0, -1), _SANS_B),
        ("FONTSIZE",      (0, 0), (-1, -1), 9),
        ("TEXTCOLOR",     (0, 0), (0, -1), C_HEADING),
        ("TEXTCOLOR",     (1, 0), (1, -1), C_BLACK),
        ("ROWBACKGROUNDS",(0, 0), (-1, -1), [colors.white, C_STRIPE]),
        ("GRID",          (0, 0), (-1, -1), 0.4, C_RULE),
        ("LEFTPADDING",   (0, 0), (-1, -1), 8),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return [t]


# ── Image / chart flowable ────────────────────────────────────────────────────

def _image_flowable(png_bytes: bytes, caption: str, styles: dict,
                    max_w_mm: float = 155) -> list:
    if not png_bytes:
        return [_para(f"[Figure not available: {caption}]", styles, "body_sm")]
    img = Image(io.BytesIO(png_bytes))
    scale = min(1.0, (max_w_mm * mm) / img.drawWidth)
    img.drawWidth  *= scale
    img.drawHeight *= scale
    return [
        Spacer(1, 2*mm),
        img,
        Paragraph(_esc(caption), styles["caption"]),
        Spacer(1, 4*mm),
    ]


# ── Title page ────────────────────────────────────────────────────────────────

def _title_page(
    story: list,
    config: ReportConfig,
    goal: ResearchGoal,
    narrative: ReportNarrative,
    profile: SystemProfile | None,
    styles: dict,
) -> None:
    story.append(Spacer(1, 30*mm))

    # Main title
    story.append(Paragraph(_esc(config.title), ParagraphStyle(
        "pg_title", fontName=_SANS_B, fontSize=24, textColor=C_HEADING,
        leading=30, alignment=TA_CENTER, spaceAfter=6,
    )))
    story.append(_thick_hr(C_HEADING))
    story.append(Spacer(1, 6*mm))

    # Subtitle line — goal name
    story.append(Paragraph(_esc(goal.name), ParagraphStyle(
        "pg_subtitle", fontName=_SANS, fontSize=13, textColor=C_SUBHEAD,
        leading=17, alignment=TA_CENTER, spaceAfter=4,
    )))
    story.append(Spacer(1, 8*mm))

    # Metadata block
    meta_style = ParagraphStyle(
        "pg_meta", fontName=_SERIF, fontSize=10, textColor=C_MUTED,
        leading=15, alignment=TA_CENTER,
    )
    meta_lines = [
        f"Report type: {config.report_type.value.capitalize()}",
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        f"Confidence: {narrative.confidence_level.value}   ·   "
        f"Verdict: {narrative.hypothesis_verdict.value}",
    ]
    if profile:
        meta_lines.append(f"Hardware: {_esc(profile.summary_line())}")
    for line in meta_lines:
        story.append(Paragraph(line, meta_style))

    if config.pdf_watermark:
        story.append(Spacer(1, 4*mm))
        story.append(Paragraph(
            f"[ {config.pdf_watermark} ]",
            ParagraphStyle("wm", fontName=_SANS_B, fontSize=11,
                           textColor=C_AMBER, alignment=TA_CENTER, leading=14),
        ))

    # Hypothesis
    if goal.hypothesis:
        story.append(Spacer(1, 10*mm))
        story.append(_hr())
        story.append(Spacer(1, 4*mm))
        story.append(Paragraph("Hypothesis", ParagraphStyle(
            "hyp_lbl", fontName=_SANS_B, fontSize=9, textColor=C_FAINT,
            alignment=TA_CENTER, leading=12, spaceAfter=3,
        )))
        hyp_safe = _esc(goal.hypothesis)
        story.append(Paragraph(
            f"\u201c{hyp_safe}\u201d",
            ParagraphStyle(
                "hyp_txt", fontName=_SERIF_I, fontSize=10.5, textColor=C_BLACK,
                leading=16, alignment=TA_CENTER,
                leftIndent=24, rightIndent=24, spaceAfter=4,
            ),
        ))

    story.append(PageBreak())


# ── Main PDF builder ──────────────────────────────────────────────────────────

def build_pdf(
    config: ReportConfig,
    goal: ResearchGoal,
    narrative: ReportNarrative,
    stat_results: list[StatTestResult],
    system_profile: SystemProfile | None,
    chart_images: list[tuple[str, bytes]],
    diagram_images: list[tuple[str, bytes]],
    doc_sections: list,
    output_path: str | Path,
    reproducibility_hash: str = "",
) -> Path:
    """Build and write the PDF to output_path. Returns the path on success."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    styles   = _styles()
    template = _PageTemplate(config.title, goal.name, config.version)

    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=20*mm, bottomMargin=18*mm,
        title=config.title,
        author="A-LEMS Report Engine",
        subject=goal.name,
        creator=f"A-LEMS v{config.version}",
    )

    story: list = []

    # ── Title page ─────────────────────────────────────────────────────────
    _title_page(story, config, goal, narrative, system_profile, styles)

    # ── Executive summary ──────────────────────────────────────────────────
    story.extend(_heading("1.  Executive Summary", 1, styles))
    story.append(_para(narrative.executive_summary, styles))
    story.append(Spacer(1, 4*mm))
    story.append(_confidence_badge(
        narrative.confidence_level, narrative.confidence_rationale, styles
    ))
    story.append(Spacer(1, 5*mm))
    story.extend(_heading("Key Findings", 2, styles))
    story.extend(_bullet_list(narrative.key_findings, styles))
    if narrative.anomaly_flags:
        story.append(Spacer(1, 3*mm))
        story.extend(_heading("Anomaly Flags", 3, styles))
        story.extend(_bullet_list(narrative.anomaly_flags, styles))
    story.append(PageBreak())

    # ── Goal and hypothesis ────────────────────────────────────────────────
    story.extend(_heading("2.  Goal & Hypothesis", 1, styles))
    story.append(_para(
        f"Category: {goal.category.value.capitalize()}", styles, "body_sm"
    ))
    story.append(Spacer(1, 2*mm))
    story.append(_para(goal.description, styles))

    if goal.hypothesis:
        story.append(Spacer(1, 4*mm))
        story.extend(_heading("Stated Hypothesis", 2, styles))
        story.append(Paragraph(
            f"\u201c{_esc(goal.hypothesis)}\u201d",
            ParagraphStyle(
                "hyp_inline", fontName=_SERIF_I, fontSize=10, textColor=C_BLACK,
                leading=15, leftIndent=16, rightIndent=16, spaceAfter=4,
            ),
        ))

    story.append(Spacer(1, 4*mm))
    story.extend(_heading("Verdict", 2, styles))
    verdict_col = _VERDICT_COLORS.get(narrative.hypothesis_verdict, C_MUTED)
    story.append(Paragraph(
        f"<b>{narrative.hypothesis_verdict.value}:</b>  {_esc(narrative.verdict_explanation)}",
        ParagraphStyle(
            "verdict", fontName=_SERIF, fontSize=10,
            textColor=verdict_col, leading=15,
        ),
    ))
    story.append(PageBreak())

    # ── System profile ─────────────────────────────────────────────────────
    story.extend(_heading("3.  System Profile", 1, styles))
    if system_profile:
        story.extend(_system_profile_table(system_profile, styles))
    else:
        story.append(_para(
            "No system profile available. Run System Profile → Scan to collect hardware information.",
            styles, "body_sm",
        ))
    story.append(Spacer(1, 5*mm))

    # ── Experiment setup ───────────────────────────────────────────────────
    story.extend(_heading("4.  Experiment Setup & Methodology", 1, styles))
    setup_text = narrative.section_narratives.get("experiment_setup", "")
    if setup_text:
        story.append(_para(setup_text, styles))
    if doc_sections:
        story.append(Spacer(1, 3*mm))
        story.extend(_heading("Measurement Methodology", 2, styles))
        for ds in doc_sections[:3]:
            story.extend(_heading(ds.heading, 3, styles))
            content = ds.content[:800] + ("\u2026" if len(ds.content) > 800 else "")
            story.append(_para(content, styles, "body_sm"))
            for math in ds.math_blocks[:2]:
                story.append(_para(f"Formula: {math}", styles, "code"))
    story.append(PageBreak())

    # ── Statistical results ────────────────────────────────────────────────
    story.extend(_heading("5.  Statistical Results", 1, styles))
    story.extend(_stat_table(stat_results, styles))
    story.append(Spacer(1, 4*mm))
    test_note = (
        f"Test: {goal.eval_criteria.stat_test.value.replace('_', '-').upper()}  ·  "
        f"\u03b1 = {goal.eval_criteria.alpha}  ·  "
        f"Effect size: Cohen\u2019s d  ·  "
        f"CI level: {goal.eval_criteria.ci_level * 100:.0f}%"
    )
    story.append(_para(test_note, styles, "body_sm"))
    story.append(PageBreak())

    # ── Visualisations ─────────────────────────────────────────────────────
    if chart_images:
        story.extend(_heading("6.  Visualisations", 1, styles))
        for i, (caption, png_bytes) in enumerate(chart_images):
            story.extend(_image_flowable(
                png_bytes, f"Figure {i + 1}: {caption}", styles
            ))
        story.append(PageBreak())

    # ── Architecture diagrams ──────────────────────────────────────────────
    if diagram_images:
        story.extend(_heading("7.  System Architecture Diagrams", 1, styles))
        for name, png_bytes in diagram_images:
            story.extend(_image_flowable(
                png_bytes, name.replace("-", " ").title(), styles, max_w_mm=145
            ))
        story.append(PageBreak())

    # ── Goal analysis ──────────────────────────────────────────────────────
    story.extend(_heading("8.  Goal-Based Analysis", 1, styles))
    goal_text = narrative.section_narratives.get("goal_analysis", "")
    if goal_text:
        story.append(_para(goal_text, styles))
    story.append(Spacer(1, 4*mm))

    # ── Interpretation ─────────────────────────────────────────────────────
    story.extend(_heading("9.  Interpretation", 1, styles))
    interp_text = narrative.section_narratives.get("interpretation", "")
    if interp_text:
        story.append(_para(interp_text, styles))
    story.append(PageBreak())

    # ── Recommendations ────────────────────────────────────────────────────
    story.extend(_heading("10.  Recommendations", 1, styles))
    story.extend(_bullet_list(narrative.recommendations, styles))
    story.append(Spacer(1, 5*mm))

    # ── Limitations ────────────────────────────────────────────────────────
    story.extend(_heading("11.  Limitations", 1, styles))
    story.extend(_bullet_list(narrative.limitations, styles))
    story.append(PageBreak())

    # ── Conclusion ─────────────────────────────────────────────────────────
    story.extend(_heading("12.  Conclusion", 1, styles))
    concl_text = narrative.section_narratives.get("conclusion", "")
    if concl_text:
        story.append(_para(concl_text, styles))
    story.append(PageBreak())

    # ── Appendix ───────────────────────────────────────────────────────────
    story.extend(_heading("Appendix", 1, styles))
    story.extend(_heading("A.  Report Configuration", 2, styles))
    story.append(_para(f"Goal ID: {goal.goal_id}  v{goal.version}", styles, "code"))
    story.append(_para(f"Filters: {config.filters}", styles, "code"))
    story.append(_para(
        f"Reproducibility hash: {reproducibility_hash or 'not computed'}",
        styles, "code",
    ))
    story.append(Spacer(1, 4*mm))

    story.extend(_heading("B.  Run Manifest", 2, styles))
    story.append(_para(
        f"Report generated from runs matching configured filters. "
        f"See report_runs table (report_id: {config.report_id}) for the full manifest.",
        styles, "body_sm",
    ))
    story.append(Spacer(1, 4*mm))

    story.extend(_heading("C.  References", 2, styles))
    story.append(_para(
        "[References to be populated in a future version. "
        "Citation slots are reserved throughout this document.]",
        styles, "body_sm",
    ))

    # ── Build ──────────────────────────────────────────────────────────────
    try:
        doc.build(
            story,
            onFirstPage=template.on_first_page,
            onLaterPages=template.on_page,
        )
        log.info(f"PDF written: {output_path} ({output_path.stat().st_size / 1024:.0f} KB)")
        return output_path
    except Exception as e:
        log.error(f"PDF build failed: {e}")
        raise
