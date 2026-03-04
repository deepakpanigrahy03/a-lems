# A-LEMS Module 0

**Installed:** 2026-02-19 20:35:16
**Project:** /home/dpani/mydrive/a-lems
**Virtual Env:** /home/dpani/mydrive/a-lems/venv

## Next Steps

1. **Log out and log back in** (required for group changes).
2. Activate environment: `source /home/dpani/mydrive/a-lems/venv/bin/activate`
3. Verify hardware: `python scripts/verify_hardware.py`
4. Set up API keys: `cp .env.example .env` and edit.
5. (Optional) Sync latest data: `python scripts/sync_configs.py --dry-run`

All configuration files are in `config/`. Missing data is `null` – Module 2 will apply world averages and log them.

See the installation log: `install_log_20260219_203256.txt`
