"""
A-LEMS Report Engine — HTML Synthesizer
Produces a self-contained interactive HTML report.
- Dark theme matching A-LEMS GUI (IBM Plex Mono, same colour palette)
- Embedded Plotly.js charts (fully interactive — hover, zoom, filter)
- Sticky navigation sidebar with section anchors
- Responsive layout
- Confidence badge, verdict badge, stat results table
- All content inline — single .html file, no external dependencies
  (Plotly.js loaded from CDN; self-contained mode embeds it fully)
"""

from __future__ import annotations
import json, html, logging
from datetime import datetime
from pathlib import Path
from .models import (
    ResearchGoal, ReportNarrative, StatTestResult, SystemProfile,
    ConfidenceLevel, HypothesisVerdict, ReportConfig,
)

log = logging.getLogger(__name__)

_CONF_CSS = {
    ConfidenceLevel.HIGH:   "#22c55e",
    ConfidenceLevel.MEDIUM: "#f59e0b",
    ConfidenceLevel.LOW:    "#ef4444",
}
_VERDICT_CSS = {
    HypothesisVerdict.SUPPORTED:    "#22c55e",
    HypothesisVerdict.REJECTED:     "#ef4444",
    HypothesisVerdict.INCONCLUSIVE: "#f59e0b",
    HypothesisVerdict.INSUFFICIENT: "#7090b0",
}


def _e(text: str) -> str:
    """HTML-escape a string."""
    return html.escape(str(text))


def _section(anchor: str, title: str, content: str, number: str = "") -> str:
    label = f"{number}. {title}" if number else title
    return f"""
<section id="{anchor}" class="report-section">
  <h2>{_e(label)}</h2>
  {content}
</section>"""


def _bullet_list(items: list[str]) -> str:
    lis = "".join(f"<li>{_e(item)}</li>" for item in items)
    return f"<ul>{lis}</ul>"


def _stat_table_html(results: list[StatTestResult]) -> str:
    if not results:
        return "<p class='muted'>No statistical results available.</p>"
    rows = ""
    for r in results:
        sig_class = "sig-yes" if r.significant else "sig-no"
        sig_text = "✓" if r.significant else "✗"
        rows += f"""
<tr>
  <td>{_e(r.metric_name)}</td>
  <td>{r.group_a_mean:.3g} <span class='unit'>{_e(r.unit)}</span></td>
  <td>{r.group_b_mean:.3g} <span class='unit'>{_e(r.unit)}</span></td>
  <td class="{'pos' if r.pct_difference() > 0 else 'neg'}">{r.pct_difference():+.1f}%</td>
  <td>{r.p_value:.4f}</td>
  <td>{r.effect_size:.2f} <span class='unit'>({_e(r.effect_label.value)})</span></td>
  <td class="{sig_class}">{sig_text}</td>
</tr>"""
    header = "".join(
        f"<th>{h}</th>" for h in [
            "Metric", f"{results[0].group_a_label} mean",
            f"{results[0].group_b_label} mean",
            "Δ%", "p-value", "Cohen's d", "Sig"
        ]
    )
    return f"""
<div class="table-wrapper">
<table class="stat-table">
  <thead><tr>{header}</tr></thead>
  <tbody>{rows}</tbody>
</table>
</div>"""


def _profile_table_html(profile: SystemProfile) -> str:
    rows_data = [
        ("CPU", profile.cpu_model),
        ("Cores", f"{profile.cpu_cores_physical}P / {profile.cpu_cores_logical}L"),
        ("Max Frequency", f"{profile.cpu_freq_max_mhz:.0f} MHz"),
        ("RAM", f"{profile.ram_gb:.1f} GB"),
        ("Environment", profile.env_type.value),
        ("OS", profile.os_name),
        ("RAPL Zones", ", ".join(profile.rapl_zones[:8])),
        ("GPU", profile.gpu_model or "None detected"),
        ("TDP", f"{profile.thermal_tdp_w:.0f} W" if profile.thermal_tdp_w else "Unknown"),
    ]
    rows = "".join(
        f"<tr><td class='key'>{_e(k)}</td><td>{_e(v)}</td></tr>"
        for k, v in rows_data
    )
    return f"<table class='profile-table'><tbody>{rows}</tbody></table>"


def _chart_div(chart_id: str, plotly_json: str, caption: str) -> str:
    if not plotly_json or plotly_json in ("{}", "null", ""):
        return f"<p class='muted'>[Chart '{_e(caption)}' — no data]</p>"
    return f"""
<figure class="chart-fig">
  <div id="{chart_id}" class="plotly-chart"></div>
  <figcaption>{_e(caption)}</figcaption>
</figure>
<script>
(function(){{
  var spec = {plotly_json};
  Plotly.react('{chart_id}', spec.data, spec.layout, {{responsive: true, displayModeBar: true}});
}})();
</script>"""


def _nav_items(sections: list[tuple[str, str]]) -> str:
    return "".join(
        f'<a href="#{anchor}" class="nav-item">{_e(label)}</a>'
        for anchor, label in sections
    )


# ── CSS ───────────────────────────────────────────────────────────────────────

_CSS = """
:root {
  --bg: #0f1520; --surface: #0d1828; --border: #1e2d45;
  --text: #c8d8e8; --muted: #7090b0; --white: #e8f0f8;
  --green: #22c55e; --red: #ef4444; --amber: #f59e0b; --blue: #3b82f6;
  --font: 'IBM Plex Mono', 'Courier New', monospace;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); color: var(--text); font-family: var(--font);
       font-size: 13px; line-height: 1.7; }
a { color: var(--blue); text-decoration: none; }
a:hover { text-decoration: underline; }

.layout { display: flex; min-height: 100vh; }

/* Sidebar nav */
.sidebar {
  width: 220px; flex-shrink: 0; background: var(--surface);
  border-right: 1px solid var(--border);
  position: sticky; top: 0; height: 100vh; overflow-y: auto;
  padding: 1.5rem 0;
}
.sidebar-title {
  font-size: 10px; font-weight: 600; letter-spacing: .1em;
  color: var(--muted); text-transform: uppercase;
  padding: 0 1rem 1rem;
}
.nav-item {
  display: block; padding: .5rem 1rem; font-size: 11px;
  color: var(--muted); border-left: 2px solid transparent;
  transition: all .15s;
}
.nav-item:hover { color: var(--text); border-left-color: var(--green);
  background: rgba(34,197,94,.05); text-decoration: none; }

/* Main content */
.main { flex: 1; padding: 2rem 2.5rem; max-width: 860px; }

.report-header { margin-bottom: 2.5rem; }
.report-title { font-size: 22px; color: var(--white); line-height: 1.3; margin-bottom: .5rem; }
.report-meta { font-size: 11px; color: var(--muted); line-height: 1.8; }

.report-section { margin-bottom: 3rem; }
.report-section h2 {
  font-size: 15px; color: var(--white); margin-bottom: 1rem;
  padding-bottom: .5rem; border-bottom: 1px solid var(--border);
}
.report-section h3 { font-size: 12px; color: var(--text); margin: 1rem 0 .5rem; }

p { margin-bottom: .75rem; }
ul { padding-left: 1.2rem; margin-bottom: .75rem; }
li { margin-bottom: .35rem; font-size: 12px; }
.muted { color: var(--muted); font-size: 11px; }

/* Badges */
.badge {
  display: inline-block; padding: 3px 10px; border-radius: 12px;
  font-size: 10px; font-weight: 600; letter-spacing: .05em;
}
.badge-conf-HIGH   { background: rgba(34,197,94,.15); color: var(--green); border: 1px solid var(--green); }
.badge-conf-MEDIUM { background: rgba(245,158,11,.15); color: var(--amber); border: 1px solid var(--amber); }
.badge-conf-LOW    { background: rgba(239,68,68,.15);  color: var(--red);   border: 1px solid var(--red); }
.conf-row { display: flex; align-items: center; gap: 10px; margin-bottom: 1rem; }
.conf-rationale { font-size: 11px; color: var(--muted); }

.verdict-box {
  border-left: 3px solid var(--border); padding: .75rem 1rem;
  background: var(--surface); border-radius: 0 6px 6px 0; margin: .75rem 0;
}

/* Stat table */
.table-wrapper { overflow-x: auto; margin-bottom: 1rem; }
.stat-table { width: 100%; border-collapse: collapse; font-size: 11px; }
.stat-table th {
  background: var(--border); color: var(--white); padding: 6px 10px;
  text-align: left; font-weight: 500; white-space: nowrap;
}
.stat-table td {
  padding: 5px 10px; border-bottom: 1px solid var(--border);
  white-space: nowrap;
}
.stat-table tr:nth-child(even) { background: var(--surface); }
.stat-table .sig-yes { color: var(--green); font-weight: 600; }
.stat-table .sig-no  { color: var(--muted); }
.stat-table .pos     { color: var(--red); }
.stat-table .neg     { color: var(--green); }
.stat-table .unit    { color: var(--muted); font-size: 10px; }

/* Profile table */
.profile-table { width: 100%; border-collapse: collapse; font-size: 11px; }
.profile-table td { padding: 5px 10px; border-bottom: 1px solid var(--border); }
.profile-table td.key { color: var(--muted); width: 140px; }
.profile-table tr:nth-child(even) { background: var(--surface); }

/* Charts */
.plotly-chart { width: 100%; height: 380px; }
.chart-fig { margin-bottom: 1.5rem; }
.chart-fig figcaption {
  font-size: 10px; color: var(--muted); text-align: center; margin-top: .4rem;
}

/* Anomaly flags */
.anomaly-item { color: var(--amber); font-size: 11px; margin-bottom: .3rem; }

/* Footer */
.report-footer {
  margin-top: 3rem; padding-top: 1rem; border-top: 1px solid var(--border);
  font-size: 10px; color: var(--muted);
}

/* Citation placeholder */
.citation-ph {
  font-size: 10px; color: var(--amber); margin: .5rem 0;
  padding: 4px 8px; border-left: 2px solid var(--amber);
}

@media (max-width: 700px) {
  .sidebar { display: none; }
  .main { padding: 1.5rem; }
}
"""


# ── Main builder ──────────────────────────────────────────────────────────────

def build_html(
    config: ReportConfig,
    goal: ResearchGoal,
    narrative: ReportNarrative,
    stat_results: list[StatTestResult],
    system_profile: SystemProfile | None,
    chart_jsons: list[tuple[str, str]],      # (caption, plotly_json)
    diagram_images: list[tuple[str, bytes]],  # HTML: SVG bytes decoded as text if SVG
    doc_sections: list,
    output_path: str | Path,
    reproducibility_hash: str = "",
) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    conf_level = narrative.confidence_level.value
    conf_color = _CONF_CSS.get(narrative.confidence_level, "#7090b0")
    verdict_color = _VERDICT_CSS.get(narrative.hypothesis_verdict, "#7090b0")

    # ── Sections ──────────────────────────────────────────────────────────
    nav_sections = [
        ("exec-summary", "Executive Summary"),
        ("goal", "Goal & Hypothesis"),
        ("system-profile", "System Profile"),
        ("methodology", "Methodology"),
        ("stat-results", "Statistical Results"),
        ("visualisations", "Visualisations"),
        ("goal-analysis", "Goal Analysis"),
        ("interpretation", "Interpretation"),
        ("recommendations", "Recommendations"),
        ("limitations", "Limitations"),
        ("conclusion", "Conclusion"),
        ("appendix", "Appendix"),
    ]

    # 1 — Executive summary
    s_exec = _section("exec-summary", "Executive Summary", f"""
<div class="conf-row">
  <span class="badge badge-conf-{conf_level}">{conf_level}</span>
  <span class="conf-rationale">{_e(narrative.confidence_rationale)}</span>
</div>
<p>{_e(narrative.executive_summary)}</p>
<h3>Key Findings</h3>
{_bullet_list(narrative.key_findings)}
{'<h3>Anomaly Flags</h3>' + ''.join(f'<p class="anomaly-item">⚠ {_e(a)}</p>' for a in narrative.anomaly_flags) if narrative.anomaly_flags else ''}
""", "1")

    # 2 — Goal and hypothesis
    hyp_html = ""
    if goal.hypothesis:
        hyp_html = f"""
<h3>Stated Hypothesis</h3>
<blockquote style="border-left:3px solid var(--border);padding:.5rem 1rem;
  color:var(--text);font-style:italic;margin:.75rem 0">
  "{_e(goal.hypothesis)}"
</blockquote>"""
    verdict_html = f"""
<div class="verdict-box" style="border-left-color:{verdict_color}">
  <strong style="color:{verdict_color}">{narrative.hypothesis_verdict.value}</strong>:
  {_e(narrative.verdict_explanation)}
</div>"""
    s_goal = _section("goal", "Goal & Hypothesis", f"""
<p><span class="muted">Category:</span> {_e(goal.category.value.capitalize())}</p>
<p>{_e(goal.description)}</p>
{hyp_html}
<h3>Verdict</h3>{verdict_html}
""", "2")

    # 3 — System profile
    profile_html = (
        _profile_table_html(system_profile)
        if system_profile
        else "<p class='muted'>No system profile available. Run System Profile → Scan.</p>"
    )
    s_profile = _section("system-profile", "System Profile", profile_html, "3")

    # 4 — Methodology
    setup_text = narrative.section_narratives.get("experiment_setup", "")
    doc_html = ""
    if doc_sections:
        doc_html = "<h3>Measurement Methodology</h3>"
        for ds in doc_sections[:3]:
            doc_html += f"<h3>{_e(ds.heading)}</h3><p>{_e(ds.content[:600])}{'…' if len(ds.content) > 600 else ''}</p>"
    s_method = _section("methodology", "Experiment Setup & Methodology", f"""
<p>{_e(setup_text)}</p>
{doc_html}
<p class="citation-ph">[REF] Citation placeholder — reference to be added.</p>
""", "4")

    # 5 — Stat results
    s_stats = _section("stat-results", "Statistical Results", f"""
{_stat_table_html(stat_results)}
<p class="muted">
  Test: {goal.eval_criteria.stat_test.value.replace('_','-').upper()} ·
  α={goal.eval_criteria.alpha} · Cohen's d · CI={goal.eval_criteria.ci_level*100:.0f}%
</p>
""", "5")

    # 6 — Visualisations
    charts_html = ""
    for i, (caption, pj) in enumerate(chart_jsons):
        charts_html += _chart_div(f"chart_{i}", pj, caption)
    s_viz = _section("visualisations", "Visualisations",
                     charts_html or "<p class='muted'>No charts generated.</p>", "6")

    # 7 — Goal analysis
    s_goal_analysis = _section("goal-analysis", "Goal-Based Analysis",
        f"<p>{_e(narrative.section_narratives.get('goal_analysis', ''))}</p>", "7")

    # 8 — Interpretation
    s_interp = _section("interpretation", "Interpretation",
        f"<p>{_e(narrative.section_narratives.get('interpretation', ''))}</p>", "8")

    # 9 — Recommendations
    s_recs = _section("recommendations", "Recommendations",
        _bullet_list(narrative.recommendations), "9")

    # 10 — Limitations
    s_lims = _section("limitations", "Limitations",
        _bullet_list(narrative.limitations), "10")

    # 11 — Conclusion
    s_concl = _section("conclusion", "Conclusion", f"""
<p>{_e(narrative.section_narratives.get('conclusion', ''))}</p>
<p class="citation-ph">[REF] Citation placeholder — reference to be added.</p>
""", "11")

    # 12 — Appendix
    s_appendix = _section("appendix", "Appendix", f"""
<h3>A. Report Configuration</h3>
<pre style="font-size:10px;color:var(--muted);overflow-x:auto">Goal ID: {_e(goal.goal_id)} v{_e(goal.version)}
Report type: {_e(config.report_type.value)}
Reproducibility hash: {_e(reproducibility_hash or 'not computed')}
Generated: {datetime.utcnow().isoformat()}Z</pre>
<h3>B. References</h3>
<p class="citation-ph">[References to be populated in a future version.]</p>
""", "12")

    # ── Assemble ──────────────────────────────────────────────────────────
    all_sections = (
        s_exec + s_goal + s_profile + s_method + s_stats +
        s_viz + s_goal_analysis + s_interp + s_recs +
        s_lims + s_concl + s_appendix
    )

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_e(config.title)} — A-LEMS Report</title>
<style>{_CSS}</style>
<script src="https://cdn.jsdelivr.net/npm/plotly.js-dist-min@2/plotly.min.js"></script>
</head>
<body>
<div class="layout">
  <nav class="sidebar">
    <div class="sidebar-title">A-LEMS Report</div>
    {_nav_items(nav_sections)}
  </nav>
  <main class="main">
    <div class="report-header">
      <h1 class="report-title">{_e(config.title)}</h1>
      <div class="report-meta">
        Goal: {_e(goal.name)} &nbsp;·&nbsp;
        Type: {_e(config.report_type.value.capitalize())} &nbsp;·&nbsp;
        Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} &nbsp;·&nbsp;
        Confidence: <span style="color:{conf_color}">{conf_level}</span>
      </div>
    </div>
    {all_sections}
    <div class="report-footer">
      A-LEMS Report Engine v{_e(config.version)} &nbsp;·&nbsp;
      Reproducibility hash: {_e(reproducibility_hash or 'n/a')}
    </div>
  </main>
</div>
</body>
</html>"""

    output_path.write_text(html_doc, encoding="utf-8")
    log.info(f"HTML written: {output_path} ({output_path.stat().st_size / 1024:.0f} KB)")
    return output_path
