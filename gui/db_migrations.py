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
    # Tracks how many runs exist for each combination of:
    #   hardware × model × task × workflow type
    # Powers the Sufficiency Advisor (dq_sufficiency.py) — tells you which
    # cells need more experiments to reach statistical significance (30+ runs).
    # Updated by the GUI after each experiment completes.
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

    # Index for fast sufficiency queries — "which cells have < 30 runs?"
    """
    CREATE INDEX IF NOT EXISTS idx_coverage_run_count
        ON coverage_matrix (run_count)
    """,

    # ── 2. HYPOTHESES ─────────────────────────────────────────────────────────
    # Research hypothesis tracker — lets you state a hypothesis, then mark
    # it as supported / rejected / inconclusive as evidence accumulates.
    # Persists across restarts unlike session_state.
    # Used by: gui/pages/hypotheses.py
    """
    CREATE TABLE IF NOT EXISTS hypotheses (
        hypothesis_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        title            TEXT    NOT NULL,
        description      TEXT,

        -- Current state of the hypothesis
        status           TEXT    NOT NULL DEFAULT 'open',
        -- Allowed values: open | supported | rejected | inconclusive

        -- Free-text evidence fields — researcher fills these in via the GUI
        evidence_for     TEXT,
        evidence_against TEXT,

        -- Optional link to a specific run or experiment that is most relevant
        key_run_id       INTEGER REFERENCES runs(run_id),
        key_exp_id       INTEGER REFERENCES experiments(exp_id),

        -- Who and when
        created_by       TEXT    DEFAULT 'researcher',
        created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,

    # ── 3. SAVED EXPERIMENTS ─────────────────────────────────────────────────
    # Persists experiment configs saved in the Execute tab (Tab 1).
    # Currently these live only in st.session_state and are lost on refresh.
    # With this table they survive restarts, reboots, and new sessions.
    # Used by: gui/pages/execute.py
    """
    CREATE TABLE IF NOT EXISTS saved_experiments (
        saved_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        name         TEXT    NOT NULL,

        -- Full experiment config stored as JSON
        -- Matches the dict structure built in execute.py (task, provider, cmd, etc.)
        config_json  TEXT    NOT NULL,

        -- Optional notes the researcher adds when saving
        notes        TEXT,

        created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,

    # ── 4. TAGS ───────────────────────────────────────────────────────────────
    # Flexible tagging system — attach labels to any run or experiment.
    # Examples: "baseline", "paper-ready", "anomaly", "thermal-issue",
    #           "exclude", "best-result", "rerun-needed"
    # Used by: any page that displays run lists
    """
    CREATE TABLE IF NOT EXISTS tags (
        tag_id       INTEGER PRIMARY KEY AUTOINCREMENT,

        -- A tag can target either a run or an experiment (not both)
        run_id       INTEGER REFERENCES runs(run_id),
        exp_id       INTEGER REFERENCES experiments(exp_id),

        -- The label itself, e.g. "paper-ready", "anomaly", "exclude"
        label        TEXT    NOT NULL,

        -- Optional category groups labels: "quality", "status", "research"
        category     TEXT    DEFAULT 'general',

        -- Optional free-text note attached to this tag
        note         TEXT,

        tagged_by    TEXT    DEFAULT 'researcher',
        tagged_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,

    # Fast lookup: all tags for a given run
    """
    CREATE INDEX IF NOT EXISTS idx_tags_run_id
        ON tags (run_id)
    """,

    # Fast lookup: all tags for a given experiment
    """
    CREATE INDEX IF NOT EXISTS idx_tags_exp_id
        ON tags (exp_id)
    """,

    # ── 5. OUTLIERS ───────────────────────────────────────────────────────────
    # Stores statistically detected outlier runs — runs where a metric
    # is more than 2 or 3 standard deviations from the population mean.
    #
    # The outlier detection job runs in dq_validity.py and writes here.
    # The excluded flag lets you soft-exclude a run from analysis without
    # deleting it — the raw data is always preserved.
    #
    # severity levels:
    #   mild   → 2–3σ  (flag for review)
    #   severe → >3σ   (likely bad data or hardware event)
    """
    CREATE TABLE IF NOT EXISTS outliers (
        outlier_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id       INTEGER NOT NULL REFERENCES runs(run_id),

        -- Which metric triggered the outlier flag
        column_name  TEXT    NOT NULL,

        -- The actual value that was flagged
        value        REAL,

        -- Population statistics at time of detection
        mean         REAL,
        std_dev      REAL,

        -- How many standard deviations away from the mean
        sigma        REAL,

        -- mild = 2-3σ, severe = >3σ
        severity     TEXT    NOT NULL DEFAULT 'mild',

        -- 0 = flagged but still included in analysis
        -- 1 = excluded from analysis (researcher decision)
        excluded     INTEGER NOT NULL DEFAULT 0,

        -- Human-readable reason: "auto:3.4σ" or "manual:thermal event"
        reason       TEXT,

        detected_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,

    # Fast lookup: all outliers for a given run
    """
    CREATE INDEX IF NOT EXISTS idx_outliers_run_id
        ON outliers (run_id)
    """,

    # Fast lookup: excluded outliers only (used to filter analysis queries)
    """
    CREATE INDEX IF NOT EXISTS idx_outliers_excluded
        ON outliers (excluded)
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
        # Use a direct sqlite3 connection here — NOT the cached gui/db.py
        # connection, because we need DDL (CREATE TABLE) not just SELECT.
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")  # Better concurrent access
        cursor = conn.cursor()

        for statement in _GUI_TABLE_MIGRATIONS:
            sql = statement.strip()
            if not sql:
                continue
            try:
                cursor.execute(sql)
                status["tables_checked"] += 1
            except sqlite3.Error as e:
                # Log the error but keep going — one bad statement
                # should not block the rest of the migrations.
                error_msg = f"Migration error: {e}\nSQL: {sql[:80]}..."
                status["errors"].append(error_msg)
                print(f"[db_migrations] WARNING: {error_msg}")

        conn.commit()

        # Log this migration run into schema_version if that table exists.
        # This gives you an audit trail of when migrations ran.
        _log_migration(conn, status)

        conn.close()
        status["success"] = len(status["errors"]) == 0

    except Exception as e:
        status["errors"].append(f"Connection failed: {e}")
        print(f"[db_migrations] CRITICAL: {traceback.format_exc()}")

    return status


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
            "gui_tables_v1",
            f"GUI tables ensured: {status['tables_checked']} statements, "
            f"{len(status['errors'])} errors",
            status["timestamp"],
        ))
    except sqlite3.Error:
        # schema_version table may not exist or have different columns — that's fine.
        pass


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: REFRESH COVERAGE MATRIX
# ══════════════════════════════════════════════════════════════════════════════

def refresh_coverage_matrix() -> int:
    """
    Recompute and upsert the coverage_matrix table from live run data.

    Call this after experiments complete, or from the Data Quality section.
    Returns the number of cells updated.

    This query counts runs grouped by hw_id × model × task × workflow,
    then upserts into coverage_matrix using INSERT OR REPLACE.
    """
    if not _DB_PATH.exists():
        return 0

    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")

        # Count runs per combination — the core sufficiency calculation
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

    Uses population mean and std_dev per (workflow_type, column).
    Any run where |value - mean| > sigma * std_dev is flagged.

    Returns number of new outliers written.

    Called from: gui/pages/dq_validity.py
    """

    # Default columns to check — the most meaningful for energy research
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
            # Compute per-workflow mean and std_dev for this column.
            # We compute per workflow_type so agentic and linear
            # are compared to their own populations — not mixed.
            try:
                stats_rows = conn.execute(f"""
                    SELECT
                        r.workflow_type,
                        AVG(r.{col})                        AS mean_val,
                        -- SQLite has no STDEV — compute manually
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
                # Column doesn't exist in this DB version — skip it
                continue

            for stat in stats_rows:
                workflow    = stat["workflow_type"]
                mean_val    = stat["mean_val"]
                variance    = max(stat["variance"] or 0, 0)
                std_dev     = variance ** 0.5

                # Skip if std_dev is essentially zero — no spread to measure
                if std_dev < 1e-9:
                    continue

                # Find runs outside the threshold for this workflow + column
                outlier_runs = conn.execute(f"""
                    SELECT r.run_id, r.{col} AS value
                    FROM runs r
                    WHERE r.workflow_type = ?
                      AND r.{col} IS NOT NULL
                      AND r.{col} > 0
                      AND ABS(r.{col} - ?) > ? * ?
                """, (workflow, mean_val, mild_sigma, std_dev)).fetchall()

                for run in outlier_runs:
                    run_id = run["run_id"]
                    value  = run["value"]
                    sigma  = abs(value - mean_val) / std_dev
                    severity = "severe" if sigma >= severe_sigma else "mild"
                    reason   = f"auto:{sigma:.1f}σ above mean {mean_val:.3f}"

                    # Only insert if not already recorded for this run+column
                    existing = conn.execute("""
                        SELECT outlier_id FROM outliers
                        WHERE run_id = ? AND column_name = ?
                    """, (run_id, col)).fetchone()

                    if existing:
                        # Update sigma in case population has grown
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
