ALTER TABLE experiments ADD COLUMN hw_id INTEGER REFERENCES hardware_config(hw_id);
ALTER TABLE experiments ADD COLUMN env_id INTEGER REFERENCES environment_config(env_id);
