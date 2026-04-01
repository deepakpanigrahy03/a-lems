-- alems/migrations/009_research_goals_and_events.sql
-- Run: python -m alems.migrations.run_migrations
-- Idempotent: safe to run multiple times

-- ── 1. query_execution ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS query_execution (
    exec_id          BIGSERIAL PRIMARY KEY,
    run_id           BIGINT REFERENCES runs(global_run_id),
    goal_description TEXT,
    start_time       TIMESTAMP DEFAULT NOW(),
    end_time         TIMESTAMP,
    success          BOOLEAN,
    total_energy_uj  BIGINT,
    num_attempts     INTEGER DEFAULT 1,
    created_at       TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_qe_run_id ON query_execution(run_id);

-- ── 2. query_attempt ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS query_attempt (
    attempt_id     BIGSERIAL PRIMARY KEY,
    exec_id        BIGINT REFERENCES query_execution(exec_id),
    attempt_number INTEGER NOT NULL,
    status         TEXT CHECK(status IN ('success','failure','retry')),
    failure_type   TEXT,
    energy_uj      BIGINT,
    latency_ms     REAL,
    created_at     TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_qa_exec_id ON query_attempt(exec_id);

-- ── 3. hallucination_events ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hallucination_events (
    id                BIGSERIAL PRIMARY KEY,
    attempt_id        BIGINT REFERENCES query_attempt(attempt_id),
    is_hallucination  BOOLEAN,
    detection_method  TEXT,
    confidence_score  REAL,
    verified_answer   TEXT,
    created_at        TIMESTAMP DEFAULT NOW()
);

-- ── 4. users ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    user_id        BIGSERIAL PRIMARY KEY,
    email          TEXT UNIQUE NOT NULL,
    name           TEXT,
    role           TEXT DEFAULT 'researcher'
                   CHECK(role IN ('admin','researcher','reviewer','public')),
    institution    TEXT,
    created_at     TIMESTAMP DEFAULT NOW()
);

-- ── 5. published_runs ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS published_runs (
    id             BIGSERIAL PRIMARY KEY,
    run_id         BIGINT,
    published_by   BIGINT REFERENCES users(user_id),
    published_at   TIMESTAMP DEFAULT NOW(),
    visibility     TEXT DEFAULT 'public'
                   CHECK(visibility IN ('public','institution','private'))
);
CREATE INDEX IF NOT EXISTS idx_pr_run_id ON published_runs(run_id);
