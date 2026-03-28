-- ============================================================
-- A-LEMS Migration 007 — Distributed Identity
-- Run by: python -m alems.migrations.run_migrations
-- Safe to run multiple times (idempotent via IF NOT EXISTS / OR IGNORE)
-- ============================================================

-- experiments: add global UUID
ALTER TABLE experiments ADD COLUMN global_exp_id TEXT;

-- runs: add global UUID + sync tracking
ALTER TABLE runs ADD COLUMN global_run_id  TEXT;
ALTER TABLE runs ADD COLUMN sync_status    INTEGER DEFAULT 0;
-- sync_status values:
--   0 = unsynced (default for all new rows)
--   1 = synced successfully
--   2 = sync failed (will retry)
--   3 = skipped (local mode, user opted out)

-- child tables: denormalised global_run_id for fast bulk-sync
--   (avoids JOIN to runs on every sync payload build)
ALTER TABLE energy_samples        ADD COLUMN global_run_id TEXT;
ALTER TABLE cpu_samples           ADD COLUMN global_run_id TEXT;
ALTER TABLE thermal_samples       ADD COLUMN global_run_id TEXT;
ALTER TABLE interrupt_samples     ADD COLUMN global_run_id TEXT;
ALTER TABLE orchestration_events  ADD COLUMN global_run_id TEXT;
ALTER TABLE llm_interactions      ADD COLUMN global_run_id TEXT;
ALTER TABLE orchestration_tax_summary ADD COLUMN global_run_id TEXT;

-- hardware_config: agent tracking columns (server populates, local ignores)
ALTER TABLE hardware_config ADD COLUMN last_seen     TIMESTAMP;
ALTER TABLE hardware_config ADD COLUMN agent_status  TEXT DEFAULT 'offline';
-- agent_status: offline | idle | busy | syncing
ALTER TABLE hardware_config ADD COLUMN agent_version TEXT;
ALTER TABLE hardware_config ADD COLUMN server_hw_id  INTEGER;
-- server_hw_id: the hw_id assigned by the PostgreSQL server (stored locally for reference)

-- Performance indexes for sync queries
CREATE INDEX IF NOT EXISTS idx_runs_sync_status  ON runs(sync_status);
CREATE INDEX IF NOT EXISTS idx_runs_global_id    ON runs(global_run_id);
CREATE INDEX IF NOT EXISTS idx_exp_global_id     ON experiments(global_exp_id);

-- Schema version bump
INSERT OR IGNORE INTO schema_version(version, description)
VALUES (7, 'distributed identity: global_run_id, global_exp_id, sync_status, agent tracking');
