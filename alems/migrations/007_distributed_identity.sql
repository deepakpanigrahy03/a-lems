-- ============================================================
-- A-LEMS Migration 007 — Distributed Identity
-- Run by: python -m alems.migrations.run_migrations
-- Safe to run multiple times (idempotent via IF NOT EXISTS / OR IGNORE)
-- ============================================================

-- Migration 007: sync_status + agent tracking only (no UUIDs)
ALTER TABLE runs ADD COLUMN sync_status INTEGER DEFAULT 0;
ALTER TABLE hardware_config ADD COLUMN last_seen     TIMESTAMP;
ALTER TABLE hardware_config ADD COLUMN agent_status  TEXT DEFAULT 'offline';
ALTER TABLE hardware_config ADD COLUMN agent_version TEXT;
ALTER TABLE hardware_config ADD COLUMN server_hw_id  INTEGER;
ALTER TABLE hardware_config ADD COLUMN api_key       TEXT;
ALTER TABLE runs ADD COLUMN sync_samples_status INTEGER DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_runs_samples_status ON runs(sync_samples_status);
CREATE INDEX IF NOT EXISTS idx_runs_sync_status ON runs(sync_status);
INSERT OR IGNORE INTO schema_version(version, description)
VALUES (7, 'distributed identity: sync_status, agent tracking (no UUIDs)');
