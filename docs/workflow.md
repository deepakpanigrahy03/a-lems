# A-LEMS Hardware & Environment Tracking Workflow

## ONE-TIME SETUP (When hardware or code changes)

```bash
# 1. Detect hardware (run after hardware changes)
python scripts/detect_hardware.py --output config/hw_config.json

# 2. Detect environment (run after code changes)
python scripts/detect_environment.py

# 3. Load to database (creates session file with IDs)
python scripts/load_configs_to_db.py
