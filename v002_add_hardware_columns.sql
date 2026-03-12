-- Remove UNIQUE from this line
ALTER TABLE hardware_config ADD COLUMN hardware_hash TEXT;  -- ✅ No UNIQUE

-- Add all other columns normally
ALTER TABLE hardware_config ADD COLUMN cpu_architecture TEXT;
-- ... rest of columns ...
