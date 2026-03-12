-- Add columns one by one with IF NOT EXISTS checks
ALTER TABLE hardware_config ADD COLUMN hardware_hash TEXT;
ALTER TABLE hardware_config ADD COLUMN cpu_architecture TEXT;
ALTER TABLE hardware_config ADD COLUMN cpu_vendor TEXT;
ALTER TABLE hardware_config ADD COLUMN cpu_family INTEGER;
ALTER TABLE hardware_config ADD COLUMN cpu_model_id INTEGER;
ALTER TABLE hardware_config ADD COLUMN cpu_stepping INTEGER;
ALTER TABLE hardware_config ADD COLUMN has_avx2 BOOLEAN;
ALTER TABLE hardware_config ADD COLUMN has_avx512 BOOLEAN;
ALTER TABLE hardware_config ADD COLUMN has_vmx BOOLEAN;
ALTER TABLE hardware_config ADD COLUMN gpu_model TEXT;
ALTER TABLE hardware_config ADD COLUMN gpu_driver TEXT;
ALTER TABLE hardware_config ADD COLUMN gpu_count INTEGER;
ALTER TABLE hardware_config ADD COLUMN gpu_power_available BOOLEAN;
ALTER TABLE hardware_config ADD COLUMN rapl_has_dram BOOLEAN;
ALTER TABLE hardware_config ADD COLUMN rapl_has_uncore BOOLEAN;
ALTER TABLE hardware_config ADD COLUMN system_manufacturer TEXT;
ALTER TABLE hardware_config ADD COLUMN system_product TEXT;
ALTER TABLE hardware_config ADD COLUMN system_type TEXT;
ALTER TABLE hardware_config ADD COLUMN virtualization_type TEXT;
ALTER TABLE hardware_config ADD COLUMN detected_at TIMESTAMP;

-- Add UNIQUE index separately (works even if column exists)
CREATE UNIQUE INDEX IF NOT EXISTS idx_hardware_hash ON hardware_config(hardware_hash);

-- Update schema version
INSERT INTO schema_version (version, description) VALUES (2, 'Add hardware columns');
