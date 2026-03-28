# Distributed Setup — Troubleshooting & FAQ

## Debugging steps (in order when something breaks)

### FastAPI service not starting

```bash
# 1. Check service status
sudo systemctl status alems-api

# 2. Check last 30 lines of logs
sudo journalctl -u alems-api --no-pager -n 30

# 3. Verify environment variable is set AND quoted correctly
grep ALEMS_DB_URL /etc/systemd/system/alems-api.service
# Must look exactly like:
# Environment="ALEMS_DB_URL=postgresql://alems:password@localhost/alems_central"
# Common mistake: missing closing quote → env var not read → "not set" error

# 4. Test DB connection manually
ALEMS_DB_URL=postgresql://alems:password@localhost/alems_central \
    python -c "
from alems.shared.db_layer import get_engine
e = get_engine()
with e.connect() as c:
    print('DB connected OK')
"

# 5. Restart after any service file change
sudo systemctl daemon-reload
sudo systemctl restart alems-api
sleep 3
curl http://localhost:8000/health
```

### Common errors and fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `ALEMS_DB_URL environment variable not set` | Missing or broken closing quote in service file | Check `grep ALEMS_DB_URL /etc/systemd/system/alems-api.service` — ensure closing `"` present |
| `could not translate host name "password@localhost"` | Password contains `@` — breaks URL parsing | Use password without special chars (`@`, `#`, `$`, `%`) or URL-encode them |
| `no such column: global_exp_id` | Migration 007 skipped but columns missing | Run `python -m alems.migrations.run_migrations` — self-heals automatically |
| `ModuleNotFoundError: alems` | Project root not in sys.path | Add `sys.path.insert(0, str(Path(__file__).parent))` to `streamlit_app.py` |
| `Invalid api_key` | Empty or wrong api_key in agent.conf | Delete api_key value in `~/.alems/agent.conf`, restart agent to re-register |
| `UUID invalid literal for int` | Namespace UUID contained non-hex chars (`m`, `l` etc) | Use `a1e05000-0000-4000-8000-000000000001` as namespace in uuid_gen.py |

---

## FAQ

### Q: Is the 15-table count in setup hardcoded?

No. It is counted dynamically at setup time:
```sql
SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'
```
The number reflects whatever tables exist after applying
`001_postgres_initial.sql`. Currently 15. Will increase automatically
as new tables are added to the migration file.

### Q: How do I add a new column to the local SQLite runs table and propagate it to PostgreSQL?

Follow this process every time:

```
1. Create new migration file:
   alems/migrations/008_add_new_column.sql
   Content: ALTER TABLE runs ADD COLUMN new_col TYPE;

2. Register it in run_migrations.py:
   Add apply_sqlite_008() function following same pattern as 007.
   Check column exists before ALTER (idempotent).

3. Add column to PostgreSQL schema:
   Edit alems/migrations/001_postgres_initial.sql
   Add the column to the runs table definition.

4. git commit + push

5. Every local machine runs:
   python -m alems.migrations.run_migrations

6. Oracle VM runs:
   python -m alems.migrations.run_migrations --postgres

7. Sync payload is automatic:
   sync_client.py uses SELECT * so new columns are included
   in bulk-sync without any code changes.
```

### Q: How does 001_postgres_initial.sql get created/updated?

It is a **manually maintained** file — not auto-generated from SQLite.
Reason: SQLite and PostgreSQL have different type systems
(`REAL` → `DOUBLE PRECISION`, `INTEGER` → `BIGINT`, etc.) and
PostgreSQL-specific features (`BIGSERIAL`, `gen_random_uuid()`,
`ON CONFLICT`, `NOW()`).

When adding new columns to SQLite schema:
- Add to the SQLite migration file (e.g. `008_*.sql`)
- Also manually add to `001_postgres_initial.sql` with correct PG types
- Both files must stay in sync

### Q: Does Oracle VM need its own SQLite migration?

Yes — Oracle VM runs experiments locally too (with model-based energy
estimation instead of RAPL measurement). So it needs the same SQLite
schema as any researcher laptop.

Run on Oracle VM before running any experiments:
```bash
python -m alems.migrations.run_migrations
```

This is separate from the PostgreSQL setup (`--postgres` flag).

### Q: What password characters are safe in ALEMS_DB_URL?

Avoid: `@ # $ % & + , / : ; = ? @`
These break URL parsing when embedded in the connection string.

Safe: alphanumeric + `_` `-` `.`

Good password example: `Alems_Lab_2026`
Bad password example: `Ganesh@123` ← the `@` breaks host parsing

If you must use special chars, URL-encode them:
- `@` → `%40`
- `#` → `%23`
- `$` → `%24`

Or store in `.env` file and reference separately (recommended):
```bash
# .env (never commit to git)
DB_PASS=your_password_here
ALEMS_DB_URL=postgresql://alems:${DB_PASS}@localhost/alems_central
```

### Q: What is the .env approach for secrets?

Never hardcode passwords in scripts or service files.
Standard approach:

```bash
# 1. Create .env file (on Oracle VM)
cat > ~/mydrive/a-lems/.env << 'EOF'
ALEMS_DB_PASSWORD=your_strong_password
ALEMS_DB_URL=postgresql://alems:your_strong_password@localhost/alems_central
EOF
chmod 600 ~/mydrive/a-lems/.env  # owner read-only

# 2. Add to .gitignore
echo ".env" >> ~/mydrive/a-lems/.gitignore

# 3. Source before running anything
source ~/mydrive/a-lems/.env

# 4. systemd service reads from EnvironmentFile
# In /etc/systemd/system/alems-api.service:
# EnvironmentFile=/home/dpani/mydrive/a-lems/.env
```

Full .env-based systemd service block:
```ini
[Service]
EnvironmentFile=/home/dpani/mydrive/a-lems/.env
ExecStart=/home/dpani/mydrive/a-lems/venv/bin/uvicorn alems.server.main:app ...
```

This is cleaner than inline `Environment=` lines and keeps secrets
out of systemd unit files (which are world-readable via journalctl).

### Q: Agent shows "local" mode — how to switch to connected?

```bash
python -m alems.agent set-mode connected
# Agent detects config change within next poll cycle (≤10s)
# No restart needed
```

### Q: How to verify sync is working after connecting?

```bash
# Check unsynced count in SQLite
sqlite3 data/experiments.db \
  "SELECT sync_status, COUNT(*) as n FROM runs GROUP BY sync_status;"
# 0=unsynced  1=synced  2=failed

# Check server received data
curl http://129.153.71.47:8000/machines
# Should show your machine with total_runs count
```
