-- Track schema versions
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

-- Insert initial version
INSERT OR IGNORE INTO schema_version (version, description) VALUES (1, 'Initial schema');
