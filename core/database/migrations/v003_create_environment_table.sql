-- Create environment_config table
CREATE TABLE environment_config (
    env_id INTEGER PRIMARY KEY AUTOINCREMENT,
    python_version TEXT,
    python_implementation TEXT,
    os_name TEXT,
    os_version TEXT,
    kernel_version TEXT,
    llm_framework TEXT,
    framework_version TEXT,
    git_commit TEXT,
    git_branch TEXT,
    git_dirty BOOLEAN,
    numpy_version TEXT,
    torch_version TEXT,
    transformers_version TEXT,
    container_runtime TEXT,
    container_image TEXT,
    env_hash TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add env_id to experiments
ALTER TABLE experiments ADD COLUMN env_id INTEGER REFERENCES environment_config(env_id);

-- Update schema version
INSERT INTO schema_version (version, description) VALUES (3, 'Add environment tracking');
