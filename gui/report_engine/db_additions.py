"""
A-LEMS Report Engine — DB Migration Additions
Add these 3 CREATE TABLE blocks inside your existing ensure_gui_tables()
function in gui/db_migrations.py.

Usage: copy the SQL strings into ensure_gui_tables() alongside the existing
coverage_matrix, hypotheses, saved_experiments, tags, outliers tables.
"""

# ── Paste these 3 blocks into ensure_gui_tables() ─────────────────────────────

SYSTEM_PROFILES_DDL = """
CREATE TABLE IF NOT EXISTS system_profiles (
    profile_id        TEXT PRIMARY KEY,
    cpu_model         TEXT NOT NULL DEFAULT 'Unknown',
    cpu_cores_phys    INTEGER,
    cpu_cores_logical INTEGER,
    cpu_freq_max_mhz  REAL,
    ram_gb            REAL,
    env_type          TEXT DEFAULT 'LOCAL',
    os_name           TEXT,
    kernel            TEXT,
    rapl_zones_json   TEXT DEFAULT '[]',
    gpu_model         TEXT,
    thermal_tdp_w     REAL,
    disk_gb           REAL,
    collected_at      TEXT NOT NULL
);
"""

RESEARCH_GOALS_DDL = """
CREATE TABLE IF NOT EXISTS research_goals (
    goal_id              TEXT PRIMARY KEY,
    name                 TEXT NOT NULL,
    category             TEXT NOT NULL DEFAULT 'custom',
    description          TEXT,
    hypothesis           TEXT,
    metrics_json         TEXT NOT NULL DEFAULT '[]',
    thresholds_json      TEXT DEFAULT '{}',
    eval_criteria_json   TEXT DEFAULT '{}',
    narrative_persona    TEXT DEFAULT 'research_engineer',
    doc_sections_json    TEXT DEFAULT '[]',
    diagram_ids_json     TEXT DEFAULT '[]',
    report_sections_json TEXT DEFAULT '[]',
    version              TEXT DEFAULT '1.0.0',
    tags_json            TEXT DEFAULT '[]',
    source               TEXT DEFAULT 'yaml',   -- 'yaml' | 'user_defined'
    created_at           TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at           TEXT
);
"""

REPORT_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS report_runs (
    report_id             TEXT PRIMARY KEY,
    goal_id               TEXT NOT NULL,
    secondary_goals_json  TEXT DEFAULT '[]',
    report_type           TEXT NOT NULL DEFAULT 'goal',
    title                 TEXT NOT NULL,
    run_filter_json       TEXT NOT NULL DEFAULT '{}',
    config_yaml           TEXT,
    narrative_json        TEXT,
    stat_results_json     TEXT DEFAULT '[]',
    confidence_level      TEXT DEFAULT 'LOW',
    confidence_rationale  TEXT,
    hypothesis_verdict    TEXT,
    output_paths_json     TEXT DEFAULT '{}',
    run_count             INTEGER DEFAULT 0,
    generated_at          TEXT NOT NULL DEFAULT (datetime('now')),
    generator_version     TEXT DEFAULT '1.0.0',
    reproducibility_hash  TEXT,
    notes                 TEXT,
    FOREIGN KEY (goal_id) REFERENCES research_goals(goal_id)
);
"""

# ── Example: how to integrate into existing ensure_gui_tables() ───────────────
INTEGRATION_EXAMPLE = '''
# In gui/db_migrations.py, inside ensure_gui_tables():

def ensure_gui_tables(db_path=None):
    """Idempotent — safe to call at every Streamlit startup."""
    if db_path is None:
        db_path = get_db_path()   # your existing helper
    
    conn = sqlite3.connect(str(db_path))
    errors = []
    
    tables = [
        # --- existing tables ---
        ("coverage_matrix",  COVERAGE_MATRIX_DDL),
        ("hypotheses",       HYPOTHESES_DDL),
        ("saved_experiments",SAVED_EXPERIMENTS_DDL),
        ("tags",             TAGS_DDL),
        ("outliers",         OUTLIERS_DDL),
        # --- NEW report engine tables ---
        ("system_profiles",  SYSTEM_PROFILES_DDL),
        ("research_goals",   RESEARCH_GOALS_DDL),
        ("report_runs",      REPORT_RUNS_DDL),
    ]
    
    for name, ddl in tables:
        try:
            conn.execute(ddl)
        except Exception as e:
            errors.append(f"{name}: {e}")
    
    conn.commit()
    conn.close()
    
    # Collect system profile on first run
    try:
        from gui.report_engine.system_profiler import get_or_collect_profile
        get_or_collect_profile(db_path)
    except Exception as e:
        errors.append(f"system_profile: {e}")
    
    return {"status": "ok", "errors": errors}
'''


# ── config.py addition ────────────────────────────────────────────────────────
CONFIG_ADDITION = '''
# In gui/config.py — add to SECTION_PAGES dict
# Position: after "Research & Insights", before "Researcher Tools"

"◎  Reports": [
    ("report_builder",      "Report Builder"),
    ("report_library",      "Report Library"),
    ("goal_registry_page",  "Goal Registry"),
    ("system_profile_page", "System Profile"),
],
'''

# ── streamlit_app.py addition ─────────────────────────────────────────────────
APP_ADDITION = '''
# In streamlit_app.py — add to _PAGE_MODULES dict

# ◎  Reports
"report_builder":      "gui.pages.report_builder",
"report_library":      "gui.pages.report_library",
"goal_registry_page":  "gui.pages.goal_registry_page",
"system_profile_page": "gui.pages.system_profile_page",
'''
