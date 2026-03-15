-- Add network metrics to runs table
ALTER TABLE runs ADD COLUMN bytes_sent INTEGER;
ALTER TABLE runs ADD COLUMN bytes_recv INTEGER;
ALTER TABLE runs ADD COLUMN tcp_retransmits INTEGER;

-- Update schema version
INSERT INTO schema_version (version, description) VALUES (8, 'Add network metrics to runs table');
