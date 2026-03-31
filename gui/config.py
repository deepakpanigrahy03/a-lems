"""
gui/config.py
─────────────────────────────────────────────────────────────────────────────
Central configuration for A-LEMS.

3-layer navigation:
  Layer 1 — Sidebar:         11 section buttons  (SECTIONS)
  Layer 2 — Section landing: card grid            (SECTION_PAGES)
  Layer 3 — Page:            actual page module

Session state keys (set by sidebar.py, read by streamlit_app.py):
  nav_section  — active section name | None  → None means Overview
  nav_page     — active page_id      | None  → None means section landing
  nav_last     — dict[section → last page_id visited]  (resume chip)
─────────────────────────────────────────────────────────────────────────────
"""

from pathlib import Path

import yaml

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "experiments.db"
LIVE_API = "http://localhost:8765"
CONFIG_DIR = PROJECT_ROOT / "config"

# ── Human-readable energy comparisons ────────────────────────────────────────
_PHONE_CHARGE_J = 20000
_WHATSAPP_MSG_J = 0.014
_GOOGLE_SEARCH_J = 0.3
_BABY_FEED_ML = 200
_CO2_TREE_SEQ_KG_PER_YEAR = 22
_WATER_BOTTLE_ML = 500


# ── YAML config loader ────────────────────────────────────────────────────────
def _load_yaml(filename: str) -> dict:
    path = CONFIG_DIR / filename
    try:
        with open(path) as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"[config] Warning: could not load {filename}: {e}")
        return {}


INSIGHTS_RULES = _load_yaml("insights_rules.yaml")
DASHBOARD_CFG = _load_yaml("dashboard.yaml")
DESIGNER_CFG = _load_yaml("experiment_designer.yaml")
TEMPLATES_CFG = _load_yaml("experiment_templates.yaml")
GAP_RULES = _load_yaml("gap_detection.yaml")

# ── Plotly dark theme ─────────────────────────────────────────────────────────
PL = dict(
    paper_bgcolor="#0f1520",
    plot_bgcolor="#090d13",
    font=dict(family="IBM Plex Mono, monospace", size=10, color="#7090b0"),
    margin=dict(l=40, r=20, t=30, b=30),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=9)),
    colorway=["#22c55e", "#ef4444", "#3b82f6", "#f59e0b", "#38bdf8", "#a78bfa"],
    xaxis=dict(gridcolor="#1e2d45", linecolor="#1e2d45", tickfont=dict(size=9)),
    yaxis=dict(gridcolor="#1e2d45", linecolor="#1e2d45", tickfont=dict(size=9)),
)

# ── Colour maps ───────────────────────────────────────────────────────────────
WF_COLORS = {"linear": "#22c55e", "agentic": "#ef4444"}

STATUS_COLORS = {
    "completed": "#3b82f6",
    "running": "#22c55e",
    "pending": "#f59e0b",
    "not_started": "#4b5563",
    "failed": "#ef4444",
    "partial": "#f97316",
}

STATUS_ICONS = {
    "completed": "●",
    "running": "🟢",
    "pending": "🟡",
    "not_started": "○",
    "failed": "🔴",
    "partial": "◑",
}

# ── Section accent colours ────────────────────────────────────────────────────
SECTION_ACCENTS = {
    "COMMAND CENTRE": "#22c55e",
    "ENERGY & SILICON": "#f59e0b",
    "AGENTIC INTELLIGENCE": "#ef4444",
    "DATA MOVEMENT": "#a78bfa",
    "SESSIONS & RUNS": "#3b82f6",
    "RESEARCH & INSIGHTS": "#38bdf8",
    "REPORTS": "#a78bfa", 
    "ENVIRONMENT": "#34d399",
    "DATA QUALITY": "#f472b6",
    "SILICON LAB": "#fb923c",
    "DEVELOPER TOOLS": "#94a3b8",
    "SETTINGS": "#475569",
}

# ── Page status constants ─────────────────────────────────────────────────────
EXISTS = "exists"
NEW = "new"
PLANNED = "planned"
BLOCKED = "blocked"

# ── SECTION_PAGES ─────────────────────────────────────────────────────────────
# Single source of truth for all navigation metadata.
# Used by: section_landing.py, streamlit_app.py, sidebar.py
#
# Section keys: accent, icon, who, description, pages[]
# Page keys:    id, label, icon, desc, status, blocked_reason (optional)
#
SECTION_PAGES = {
    "COMMAND CENTRE": {
        "accent": "#22c55e",
        "icon": "◈",
        "who": "All personas — start every session here",
        "description": "Live session state, KPIs, experiment control, and multi-host dispatch.",
        "pages": [
            {
                "id": "overview",
                "label": "Overview",
                "icon": "◈",
                "desc": "Live session KPIs, energy summary, data sufficiency banner.",
                "status": EXISTS,
            },
            {
                "id": "silicon_journey",
                "label": "Ask Me",
                "icon": "◑",
                "desc": "Chat-style experiment — run on silicon, see energy, get insights.",
                "status": NEW,
            },
            {
                "id": "execute",
                "label": "Execute Run",
                "icon": "▶",
                "desc": "Launch experiments against local or remote lab targets.",
                "status": EXISTS,
            },
            {
                "id": "designer",
                "label": "Experiment Designer",
                "icon": "🧪",
                "desc": "Design conditional multi-container experiment plans.",
                "status": EXISTS,
            },
            {
                "id": "experiments",
                "label": "Experiments",
                "icon": "≡",
                "desc": "Browse all experiments with status, runs, and group metadata.",
                "status": EXISTS,
            },
            {
                "id":"experiment_planner",
                "label":"Experiment Planner",
                "icon":"◈",
                "desc":"Auto-suggest next experiments + estimate energy budget before running.",
                "status":NEW,              
            },
            {
                "id":    "fleet",
                "label": "Fleet Control",
                "icon":  "◈",
                "desc":  "All machines, dispatch jobs, job queue, sync health, connect/disconnect.",
                "status": NEW,
            },

        ],
    },
    "ENERGY & SILICON": {
        "accent": "#f59e0b",
        "icon": "⚡",
        "who": "Silicon developers · chip engineers · CPU/GPU vendors",
        "description": "RAPL domain breakdown, C-state residency, thermal behaviour, and idle baseline drift.",
        "pages": [
            {
                "id": "energy",
                "label": "Energy Lab",
                "icon": "⚡",
                "desc": "pkg / core / uncore / dram RAPL energy per run, power over time.",
                "status": EXISTS,
            },
            {
                "id": "cpu",
                "label": "CPU & C-States",
                "icon": "▣",
                "desc": "Core frequency, C-state residency, IPC, ring bus, context switches.",
                "status": EXISTS,
            },
            {
                "id": "thermal",
                "label": "Thermal Analysis",
                "icon": "🌡",
                "desc": "Throttle events, per-run temperature time series, thermal delta.",
                "status": NEW,
            },
            {
                "id": "baseline",
                "label": "Baseline & Idle",
                "icon": "⊟",
                "desc": "Idle baseline drift, governor state, turbo on/off energy impact.",
                "status": NEW,
            },
            {
                "id": "anomalies",
                "label": "Anomalies",
                "icon": "⚠",
                "desc": "Statistical outliers across all energy and performance metrics.",
                "status": EXISTS,
            },
        ],
    },
    "AGENTIC INTELLIGENCE": {
        "accent": "#ef4444",
        "icon": "⇌",
        "who": "Orchestration engineers · MLOps · framework developers",
        "description": "True cost of agentic overhead — planning, execution, synthesis phases, and orchestration tax.",
        "pages": [
            {
                "id": "agentic_linear",
                "label": "Agentic vs Linear",
                "icon": "⇌",
                "desc": "Side-by-side energy: agentic overhead vs linear baseline.",
                "status": EXISTS,
            },
            {
                "id": "tax",
                "label": "Tax Attribution",
                "icon": "▲",
                "desc": "Orchestration tax breakdown — what percentage is pure overhead.",
                "status": EXISTS,
            },
            {
                "id": "models",
                "label": "Apple-to-Apple",
                "icon": "🍎",
                "desc": "Controlled model comparison — same task, different models, same hardware.",
                "status": EXISTS,
            },
            {
                "id": "phase_drilldown",
                "label": "Phase Drilldown",
                "icon": "⬡",
                "desc": "Per-step planning/execution/synthesis energy from orchestration_events.",
                "status": NEW,
            },
        ],
    },
    "DATA MOVEMENT": {
        "accent": "#a78bfa",
        "icon": "◧",
        "who": "Infrastructure engineers · cloud architects",
        "description": "Energy cost per data layer — cache, memory, network, tokens, swap, interrupts.",
        "pages": [
            {
                "id": "data_cache",
                "label": "Cache & Memory",
                "icon": "◧",
                "desc": "Cache miss rate, rss/vms memory, IPC correlation with energy.",
                "status": NEW,
            },
            {
                "id": "data_tokens",
                "label": "Token Flow",
                "icon": "⟳",
                "desc": "LLM token movement, prompt/completion cost, latency per step.",
                "status": NEW,
            },
            {
                "id": "data_network",
                "label": "Network & API",
                "icon": "⌁",
                "desc": "DNS and API latency, idle energy cost of waiting for responses.",
                "status": NEW,
            },
            {
                "id": "data_swap",
                "label": "Swap & Paging",
                "icon": "⇅",
                "desc": "Swap usage, memory pressure, energy impact of paging activity.",
                "status": NEW,
            },
            {
                "id": "data_interrupts",
                "label": "Interrupts",
                "icon": "∿",
                "desc": "Interrupt rate time series, wakeup latency, per-run interrupt cost.",
                "status": NEW,
            },
            {
                "id": "data_page_faults",
                "label": "Page Faults",
                "icon": "⊘",
                "desc": "Major/minor page fault energy impact.",
                "status": NEW,
                
            },
            {
                "id": "data_network_bytes",
                "label": "Network Bytes",
                "icon": "⊞",
                "desc": "bytes_sent / bytes_recv, TCP retransmits, effective throughput.",
                "status": NEW,
               
            },
        ],
    },
    "SESSIONS & RUNS": {
        "accent": "#3b82f6",
        "icon": "⬡",
        "who": "All personas — browse, filter, compare, deep-dive",
        "description": "Session management, run comparison, and single-run sensor stream drilldown.",
        "pages": [
            {
                "id": "sessions",
                "label": "Sessions",
                "icon": "⬡",
                "desc": "All sessions with metadata, status, experiment count, run totals.",
                "status": EXISTS,
            },
            {
                "id": "session_analysis",
                "label": "Session Analysis",
                "icon": "◑",
                "desc": "Deep statistical analysis of a selected session — trends, regression.",
                "status": EXISTS,
            },
            {
                "id": "explorer",
                "label": "Run Explorer",
                "icon": "⊞",
                "desc": "Filter and compare runs across experiments, models, and task types.",
                "status": EXISTS,
            },
            {
                "id": "run_drilldown",
                "label": "Run Drilldown",
                "icon": "🔍",
                "desc": "All sensor streams for one run — energy, CPU, thermal, interrupts, LLM.",
                "status": NEW,
            },
        ],
    },
    "RESEARCH & INSIGHTS": {
        "accent": "#38bdf8",
        "icon": "🔬",
        "who": "Domain researchers · AI scientists · ML engineers",
        "description": "Cross-run analysis, efficiency ratios, domain energy mapping, and ML feature workspace.",
        "pages": [
            {
                "id": "research_insights",
                "label": "Research Insights",
                "icon": "🔬",
                "desc": "Automated insight generation across all collected runs.",
                "status": EXISTS,
            },
            {
                "id":"research_metrics",
                "label":"Orchestration Metrics",
                "icon":"🔬",
                "desc":"OOI · UCR · Network Ratio — publication-ready analysis.",
                "status":NEW
            },
            {
                "id": "query_analysis",
                "label": "Query Analysis",
                "icon": "◑",
                "desc": "Query class clustering, energy per query type, complexity scaling.",
                "status": EXISTS,
            },
            {
                "id": "domains",
                "label": "Domains",
                "icon": "◉",
                "desc": "Task domain energy profiles — coding vs writing vs reasoning vs math.",
                "status": EXISTS,
            },
            {
                "id": "scheduler",
                "label": "Scheduler",
                "icon": "〜",
                "desc": "Run scheduling analysis, queue length, kernel/user time split.",
                "status": EXISTS,
            },
            {
                "id": "efficiency",
                "label": "Efficiency Explorer",
                "icon": "⚡",
                "desc": "energy_per_token, J/instruction, IPC correlation — find optimal configs.",
                "status": NEW,
            },
            {
                "id": "ml_features",
                "label": "ML Features",
                "icon": "⊟",
                "desc": "144-column ml_features view — correlation matrix, export for training.",
                "status": NEW,
            },
            {
                "id": "hypotheses",
                "label": "Hypothesis Tracker",
                "icon": "💡",
                "desc": "State research hypotheses, track supporting and contradicting evidence.",
                "status": NEW,
            },
            {
                "id":"llm_quality",
                "label":"LLM Quality",
                "icon":"⭐",
                "desc":"Response quality scoring, energy per quality unit, linear vs agentic agreement.",
                "status":NEW
            },
        ],
    },


    "REPORTS": {
        "accent": "#a78bfa",
        "icon": "◎",
        "who": "Researchers · PhD students · Lab leads · Authors",
        "description": (
            "Goal-based experimental report generation. Combines measured data, "
            "documentation, and architecture diagrams into publication-quality "
            "PDFs and interactive HTML reports."
        ),
        "pages": [
            {
                "id": "report_builder",
                "label": "Report Builder",
                "icon": "◈",
                "desc": (
                    "Configure goal, filters, and sections — then generate "
                    "a PDF + interactive HTML report in one click."
                ),
                "status": NEW,
            },
            {
                "id": "report_library",
                "label": "Report Library",
                "icon": "≡",
                "desc": (
                    "Browse all generated reports. Re-run with fresh data, "
                    "compare confidence levels, download PDF or HTML."
                ),
                "status": NEW,
            },
            {
                "id": "goal_registry_page",
                "label": "Goal Registry",
                "icon": "⊟",
                "desc": (
                    "Define and manage research goals — metrics, thresholds, "
                    "statistical criteria, narrative persona."
                ),
                "status": NEW,
            },
            {
                "id": "system_profile_page",
                "label": "System Profile",
                "icon": "▣",
                "desc": (
                    "Auto-detect CPU model, cores, RAM, RAPL zones, and "
                    "environment type. Stored in DB, injected into every report."
                ),
                "status": NEW,
            },
        ],
    },    

    "ENVIRONMENT": {
        "accent": "#34d399",
        "icon": "♻",
        "who": "Sustainability teams · policy researchers · CTO offices",
        "description": "Carbon, water, and methane cost of AI workloads by model, task, and geography.",
        "pages": [
            {
                "id": "sustainability",
                "label": "Sustainability",
                "icon": "♻",
                "desc": "Full environmental footprint per run — carbon, water, methane.",
                "status": EXISTS,
            },
            {
                "id": "carbon_country",
                "label": "Carbon by Country",
                "icon": "🌍",
                "desc": "Grid intensity × country_code — carbon cost mapped by geography.",
                "status": NEW,
            },
            {
                "id": "water_methane",
                "label": "Water & Methane",
                "icon": "💧",
                "desc": "Separate water_ml and methane_mg visualisation and trends.",
                "status": NEW,
            },
        ],
    },
    "DATA QUALITY": {
        "accent": "#f472b6",
        "icon": "✓",
        "who": "All personas — trust your data before you analyse it",
        "description": "Run validity, sensor completeness, statistical sufficiency, and hash integrity.",
        "pages": [
            {
                "id": "dq_validity",
                "label": "Run Validity",
                "icon": "✓",
                "desc": "Flags invalid runs — thermal throttle, bad baselines, experiment_valid.",
                "status": NEW,
            },
            {
                "id": "dq_coverage",
                "label": "Sensor Coverage",
                "icon": "◫",
                "desc": "NULL-count heatmap across 80+ columns — which sensors drop out when.",
                "status": NEW,
            },
            {
                "id": "dq_sufficiency",
                "label": "Sufficiency Advisor",
                "icon": "◈",
                "desc": "How many more experiments needed for statistical significance per cell.",
                "status": NEW,
            },
            {
                "id": "dq_integrity",
                "label": "Hash Integrity",
                "icon": "#",
                "desc": "run_state_hash verification — detect corrupted or tampered run records.",
                "status": NEW,
            },
            {
                "id": "dq_swap",
                "label": "Swap Analysis",
                "icon": "⇅",
                "desc": "Swap delta as memory pressure signal — ML training data quality.",
                "status": NEW,
            },

            {
                "id": "dq_drift",
                "label": "Data Drift",
                "icon": "⟳",
                "desc": "Detects if recent runs have drifted from historical baseline.",
                "status": NEW,
            },
            {
                "id": "dq_schema",
                "label": "Schema Log",
                "icon": "≡",
                "desc": "Schema version history — migration log and applied changes.",
                "status": NEW,
            },        
        ],
    },
    "SILICON LAB": {
        "accent": "#fb923c",
        "icon": "▣",
        "who": "Silicon developers · cloud engineers · HPC teams",
        "description": "Hardware profiles, cross-silicon comparison, and multi-host experiment infrastructure.",
        "pages": [
            {
                "id": "hw_registry",
                "label": "Hardware Registry",
                "icon": "▣",
                "desc": "All hw_id records — full CPU spec, AVX/VMX flags, RAPL capabilities.",
                "status": NEW,
            },
            {
                "id": "silicon_compare",
                "label": "Cross-Silicon Compare",
                "icon": "⇌",
                "desc": "Same experiment, different chips — controlled silicon comparison.",
                "status": NEW,
            },
            {
                "id": "hardware_compare",
                "label": "HW/OS Compare",
                "icon": "⚙",
                "desc": "Same task+model+workflow — compare energy/IPC/temp across hardware and OS.",
                "status": NEW,
            },

            {
                "id": "capability_matrix",
                "label": "Capability Matrix",
                "icon": "◫",
                "desc": "AVX2/AVX512/VMX/RAPL flags across all registered hardware.",
                "status": NEW,
            },
        ],
    },
    "RESEARCHER TOOLS": {
        "accent": "#94a3b8",
        "icon": "📋",
        "who": "Engineers and developers building on A-LEMS",
        "description": "Schema docs, SQL console, run replay, LLM call log, environment fingerprint, ML export.",
        "pages": [
            {
                "id": "live",
                "label": "Run Replay",
                "icon": "📼",
                "desc": "Post-run replay of energy and CPU samples with time scrubbing.",
                "status": EXISTS,
            },
            {
                "id": "schema_docs",
                "label": "Schema & Docs",
                "icon": "📋",
                "desc": "Full database schema documentation with column descriptions.",
                "status": EXISTS,
            },
            {
                "id": "sql_query",
                "label": "SQL Query",
                "icon": "🔍",
                "desc": "Raw SQL console against the experiments database.",
                "status": EXISTS,
            },
            {
                "id": "env_config",
                "label": "Environment Config",
                "icon": "⚙",
                "desc": "Python / framework / git version fingerprint per run.",
                "status": NEW,
            },
            {
                "id": "llm_log",
                "label": "LLM Interactions",
                "icon": "💬",
                "desc": "Full prompt/response log per step with latency and token counts.",
                "status": NEW,
            },
            {
                "id": "ml_export",
                "label": "ML Export",
                "icon": "⬡",
                "desc": "Export quality-scored, filtered datasets for model training pipelines.",
                "status": NEW,
            },
        ],
    },
    "SETTINGS": {
        "accent": "#475569",
        "icon": "⚙",
        "who": "All users",
        "description": "Theme, database connection, and host configuration.",
        "pages": [
            {
                "id": "settings",
                "label": "Settings",
                "icon": "⚙",
                "desc": "Theme toggle, database path, Live Lab connection, host config.",
                "status": EXISTS,
            },
        ],
    },
}

# ── Derived lookups (computed once at import) ─────────────────────────────────

# Ordered list of section names
SECTIONS: list[str] = list(SECTION_PAGES.keys())

# page_id → section name
PAGE_TO_SECTION: dict[str, str] = {
    page["id"]: section
    for section, data in SECTION_PAGES.items()
    for page in data["pages"]
}

# page_id → full page metadata dict
PAGE_META: dict[str, dict] = {
    page["id"]: {**page, "section": section}
    for section, data in SECTION_PAGES.items()
    for page in data["pages"]
}

# Set of page_ids with real modules in gui/pages/
PAGES_EXISTING: set[str] = {
    pid for pid, meta in PAGE_META.items() if meta["status"] == EXISTS
}

# Blocked page reasons
PAGES_BLOCKED: dict[str, str] = {
    pid: meta["blocked_reason"]
    for pid, meta in PAGE_META.items()
    if meta.get("blocked_reason")
}

# ── Backward-compat NAV_GROUPS ────────────────────────────────────────────────
# Any code still importing NAV_GROUPS will not break.
NAV_GROUPS = [
    item
    for section, data in SECTION_PAGES.items()
    for item in [(section, None)] + [(p["label"], p["id"]) for p in data["pages"]]
]
