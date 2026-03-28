"""
gui/db_migrations.py
─────────────────────────────────────────────────────────────────────────────
GUI-side database migrations for A-LEMS.

WHY THIS FILE EXISTS
────────────────────
The experiment harness (core/) creates and populates the main measurement
tables (runs, experiments, energy_samples, etc.) automatically when
experiments run.

But the GUI needs its own tables — for saving hypotheses, tagging runs,
tracking outliers, persisting experiment configs — that the harness
never touches. These tables must exist before the GUI can function.

HOW IT WORKS
────────────
Call ensure_gui_tables() once at app startup (streamlit_app.py).
Every statement uses CREATE TABLE IF NOT EXISTS — so it is completely
safe to call on every restart. Existing data is never touched.

New git checkout → streamlit run → tables auto-created → works immediately.
No manual steps. No "did you run schema.py?" confusion.

ADDING A NEW TABLE
──────────────────
1. Add a CREATE TABLE IF NOT EXISTS block to _GUI_TABLE_MIGRATIONS below.
2. Add a record to _log_migration() call at the bottom of ensure_gui_tables().
3. That's it — next app restart creates it automatically.
─────────────────────────────────────────────────────────────────────────────
"""

import sqlite3
import traceback
from datetime import datetime
from pathlib import Path

# ── Database path (same as gui/config.py — duplicated here to avoid circular import)
_DB_PATH = Path(__file__).parent.parent / "data" / "experiments.db"


# ══════════════════════════════════════════════════════════════════════════════
# TABLE DEFINITIONS
# All statements use IF NOT EXISTS — idempotent, safe to run every startup.
# ══════════════════════════════════════════════════════════════════════════════

_GUI_TABLE_MIGRATIONS = [

    # ── 1. COVERAGE MATRIX ────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS coverage_matrix (
        hw_id         INTEGER NOT NULL,
        model_name    TEXT    NOT NULL,
        task_name     TEXT    NOT NULL,
        workflow_type TEXT    NOT NULL,
        run_count     INTEGER NOT NULL DEFAULT 0,
        last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (hw_id, model_name, task_name, workflow_type)
    )
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_coverage_run_count
        ON coverage_matrix (run_count)
    """,

    # ── 2. HYPOTHESES ─────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS hypotheses (
        hypothesis_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        title            TEXT    NOT NULL,
        description      TEXT,
        status           TEXT    NOT NULL DEFAULT 'open',
        evidence_for     TEXT,
        evidence_against TEXT,
        key_run_id       INTEGER REFERENCES runs(run_id),
        key_exp_id       INTEGER REFERENCES experiments(exp_id),
        created_by       TEXT    DEFAULT 'researcher',
        created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,

    # ── 3. SAVED EXPERIMENTS ──────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS saved_experiments (
        saved_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        name         TEXT    NOT NULL,
        config_json  TEXT    NOT NULL,
        notes        TEXT,
        created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,

    # ── 4. TAGS ───────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS tags (
        tag_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id       INTEGER REFERENCES runs(run_id),
        exp_id       INTEGER REFERENCES experiments(exp_id),
        label        TEXT    NOT NULL,
        category     TEXT    DEFAULT 'general',
        note         TEXT,
        tagged_by    TEXT    DEFAULT 'researcher',
        tagged_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_tags_run_id
        ON tags (run_id)
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_tags_exp_id
        ON tags (exp_id)
    """,

    # ── 5. OUTLIERS ───────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS outliers (
        outlier_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id       INTEGER NOT NULL REFERENCES runs(run_id),
        column_name  TEXT    NOT NULL,
        value        REAL,
        mean         REAL,
        std_dev      REAL,
        sigma        REAL,
        severity     TEXT    NOT NULL DEFAULT 'mild',
        excluded     INTEGER NOT NULL DEFAULT 0,
        reason       TEXT,
        detected_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_outliers_run_id
        ON outliers (run_id)
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_outliers_excluded
        ON outliers (excluded)
    """,

    # ── 6. SYSTEM PROFILES ────────────────────────────────────────────────────
    # Auto-detected hardware profile — CPU, RAM, RAPL zones, env type.
    # Collected once at startup, injected into every generated report.
    # Fixes "Unknown hardware" in all PDF/HTML report outputs forever.
    """
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
    )
    """,

    # ── 7. RESEARCH GOALS ─────────────────────────────────────────────────────
    # Stores researcher-defined and YAML-loaded research goals.
    # Each goal drives a full report: metrics, thresholds, stat config,
    # narrative persona, doc sections, diagram IDs.
    # Populated at startup from gui/report_engine/goals/*.yaml
    """
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
        source               TEXT DEFAULT 'yaml',
        created_at           TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at           TEXT
    )
    """,

    # ── 8. REPORT RUNS ────────────────────────────────────────────────────────
    # One row per generated report. Stores the full config used, narrative
    # output, stat results, confidence level, and output file paths.
    # Powers the Report Library page — browse, re-run, download.
    """
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
        notes                 TEXT
    )
    """,

]


# ══════════════════════════════════════════════════════════════════════════════
# MIGRATION RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def ensure_gui_tables() -> dict:
    """
    Create all GUI-side tables if they don't already exist.

    Called once at app startup from streamlit_app.py.
    Safe to call every restart — IF NOT EXISTS means no data is ever lost.

    Returns a status dict so streamlit_app.py can log or display results.
    """

    status = {
        "success": False,
        "tables_checked": 0,
        "errors": [],
        "timestamp": datetime.now().isoformat(),
    }

    # Check the database file exists before trying to connect
    if not _DB_PATH.exists():
        status["errors"].append(
            f"Database not found at {_DB_PATH}. "
            f"Run an experiment first to create it."
        )
        return status

    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()

        for statement in _GUI_TABLE_MIGRATIONS:
            sql = statement.strip()
            if not sql:
                continue
            try:
                cursor.execute(sql)
                status["tables_checked"] += 1
            except sqlite3.Error as e:
                error_msg = f"Migration error: {e}\nSQL: {sql[:80]}..."
                status["errors"].append(error_msg)
                print(f"[db_migrations] WARNING: {error_msg}")

        conn.commit()

        # ── Collect system profile on first run ────────────────────────────
        # After tables exist, auto-collect hardware profile if not yet done.
        # This is what eliminates "Unknown hardware" from all reports.
        try:
            _ensure_system_profile(conn)
        except Exception as e:
            # Non-fatal — report engine degrades gracefully without it
            status["errors"].append(f"system_profile collection: {e}")
            print(f"[db_migrations] system_profile warning: {e}")

        _log_migration(conn, status)
        conn.close()
        status["success"] = len(status["errors"]) == 0

    except Exception as e:
        status["errors"].append(f"Connection failed: {e}")
        print(f"[db_migrations] CRITICAL: {traceback.format_exc()}")

    return status


def _ensure_system_profile(conn: sqlite3.Connection) -> None:
    """
    Collect and store a system profile if none exists yet.
    Called once after tables are created — idempotent.
    """
    existing = conn.execute(
        "SELECT profile_id FROM system_profiles LIMIT 1"
    ).fetchone()

    if existing:
        return  # Already have a profile — nothing to do

    # Import here to avoid circular imports at module level
    try:
        from gui.report_engine.system_profiler import collect_profile
        import json

        profile = collect_profile()
        conn.execute("""
            INSERT OR IGNORE INTO system_profiles VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """, (
            profile.profile_id,
            profile.cpu_model,
            profile.cpu_cores_physical,
            profile.cpu_cores_logical,
            profile.cpu_freq_max_mhz,
            profile.ram_gb,
            profile.env_type.value,
            profile.os_name,
            profile.kernel,
            json.dumps(profile.rapl_zones),
            profile.gpu_model,
            profile.thermal_tdp_w,
            profile.disk_gb,
            profile.collected_at.isoformat(),
        ))
        conn.commit()
        print(f"[db_migrations] System profile collected: {profile.summary_line()}")
    except ImportError:
        # report_engine not yet installed — skip silently
        print("[db_migrations] report_engine not found — skipping system profile collection")


def _log_migration(conn: sqlite3.Connection, status: dict) -> None:
    """
    Write a record to schema_version so you can see migration history.
    Silently skips if schema_version table doesn't exist yet.
    """
    try:
        conn.execute("""
            INSERT INTO schema_version (version, description, applied_at)
            VALUES (?, ?, ?)
        """, (
            "gui_tables_v2",
            f"GUI tables ensured: {status['tables_checked']} statements, "
            f"{len(status['errors'])} errors",
            status["timestamp"],
        ))
    except sqlite3.Error:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: REFRESH COVERAGE MATRIX
# ══════════════════════════════════════════════════════════════════════════════

def refresh_coverage_matrix() -> int:
    """
    Recompute and upsert the coverage_matrix table from live run data.
    Returns the number of cells updated.
    """
    if not _DB_PATH.exists():
        return 0

    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")

        conn.execute("""
            INSERT OR REPLACE INTO coverage_matrix
                (hw_id, model_name, task_name, workflow_type, run_count, last_updated)
            SELECT
                r.hw_id,
                e.model_name,
                e.task_name,
                r.workflow_type,
                COUNT(*)            AS run_count,
                CURRENT_TIMESTAMP   AS last_updated
            FROM runs r
            JOIN experiments e ON r.exp_id = e.exp_id
            WHERE e.model_name IS NOT NULL
              AND e.task_name  IS NOT NULL
              AND r.workflow_type IS NOT NULL
              AND r.hw_id IS NOT NULL
            GROUP BY r.hw_id, e.model_name, e.task_name, r.workflow_type
        """)

        rows_updated = conn.total_changes
        conn.commit()
        conn.close()
        return rows_updated

    except Exception as e:
        print(f"[db_migrations] refresh_coverage_matrix error: {e}")
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: DETECT AND STORE OUTLIERS
# ══════════════════════════════════════════════════════════════════════════════

def detect_and_store_outliers(
    columns: list = None,
    mild_sigma: float = 2.0,
    severe_sigma: float = 3.0,
) -> int:
    """
    Run outlier detection across key energy/performance columns.
    Writes results to the outliers table.
    Returns number of new outliers written.
    """
    if columns is None:
        columns = [
            "energy_j",
            "duration_ms",
            "ipc",
            "cache_miss_rate",
            "api_latency_ms",
            "package_temp_celsius",
            "avg_power_watts",
            "total_tokens",
        ]

    if not _DB_PATH.exists():
        return 0

    written = 0

    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row

        for col in columns:
            try:
                stats_rows = conn.execute(f"""
                    SELECT
                        r.workflow_type,
                        AVG(r.{col})                        AS mean_val,
                        AVG(r.{col} * r.{col}) -
                            AVG(r.{col}) * AVG(r.{col})     AS variance,
                        COUNT(*)                            AS n
                    FROM runs r
                    WHERE r.{col} IS NOT NULL
                      AND r.{col} > 0
                    GROUP BY r.workflow_type
                    HAVING COUNT(*) >= 5
                """).fetchall()
            except sqlite3.OperationalError:
                continue

            for stat in stats_rows:
                workflow = stat["workflow_type"]
                mean_val = stat["mean_val"]
                variance = max(stat["variance"] or 0, 0)
                std_dev  = variance ** 0.5

                if std_dev < 1e-9:
                    continue

                outlier_runs = conn.execute(f"""
                    SELECT r.run_id, r.{col} AS value
                    FROM runs r
                    WHERE r.workflow_type = ?
                      AND r.{col} IS NOT NULL
                      AND r.{col} > 0
                      AND ABS(r.{col} - ?) > ? * ?
                """, (workflow, mean_val, mild_sigma, std_dev)).fetchall()

                for run in outlier_runs:
                    run_id   = run["run_id"]
                    value    = run["value"]
                    sigma    = abs(value - mean_val) / std_dev
                    severity = "severe" if sigma >= severe_sigma else "mild"
                    reason   = f"auto:{sigma:.1f}σ above mean {mean_val:.3f}"

                    existing = conn.execute("""
                        SELECT outlier_id FROM outliers
                        WHERE run_id = ? AND column_name = ?
                    """, (run_id, col)).fetchone()

                    if existing:
                        conn.execute("""
                            UPDATE outliers
                            SET sigma = ?, severity = ?, reason = ?,
                                detected_at = CURRENT_TIMESTAMP
                            WHERE run_id = ? AND column_name = ?
                        """, (sigma, severity, reason, run_id, col))
                    else:
                        conn.execute("""
                            INSERT INTO outliers
                                (run_id, column_name, value, mean, std_dev,
                                 sigma, severity, excluded, reason)
                            VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                        """, (run_id, col, value, mean_val, std_dev,
                              sigma, severity, reason))
                        written += 1

        conn.commit()
        conn.close()

    except Exception as e:
        print(f"[db_migrations] detect_and_store_outliers error: {e}")

    return written
