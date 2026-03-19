# Troubleshooting Guide

Common issues and their solutions when using A-LEMS.

---

## 🚀 Quick Diagnostic

Run the built-in diagnostic tool first:

```bash
python scripts/tools/issue_tracer.py
```

This checks:

- ✅ Hardware access
- ✅ Database integrity
- ✅ Environment configuration
- ✅ Model availability
- ✅ Sample data consistency

---

## 🔧 Installation Issues

### Permission Denied

**Symptoms:**
- `PermissionError` when accessing hardware
- `cat /sys/class/powercap/*` shows permission denied

**Solution:**

```bash
sudo ./scripts/fix_permissions.sh
```

### Module Not Found

**Symptoms:**
```
ModuleNotFoundError: No module named 'core'
```

**Solution:**

```bash
# Activate virtual environment
source venv/bin/activate

# Reinstall if needed
pip install -e .
```

### RAPL Not Found

**Symptoms:**
- No RAPL domains detected
- Energy readings all zero

**Solution:**

```bash
# Check CPU support
cat /proc/cpuinfo | grep rapl

# Load modules
sudo modprobe intel_rapl_common
sudo modprobe intel_rapl_msr
```

---

## 🗄️ Database Issues

### Database Locked

**Symptoms:**
```
sqlite3.OperationalError: database is locked
```

**Solution:**

```bash
# Find processes using database
lsof data/experiments.db

# Kill the process
kill -9 <PID>

# Remove lock files (if no processes running)
rm -f data/experiments.db-journal
rm -f data/experiments.db-wal
rm -f data/experiments.db-shm
```

### Foreign Key Constraint Failed

**Symptoms:**
```
FOREIGN KEY constraint failed
```

**Solution:**

```bash
# Ensure hardware and environment are loaded
python scripts/tools/load_configs_to_db.py

# Check current IDs
sqlite3 data/experiments.db "SELECT hw_id, env_id FROM experiments ORDER BY exp_id DESC LIMIT 1;"
```

### Missing Tables

**Symptoms:**
```
no such table: experiments
```

**Solution:**

```bash
# Run migrations
python scripts/tools/migration_helper.py --migrate

# Or create tables manually
python -c "
from core.database.manager import DatabaseManager
db = DatabaseManager()
db.create_tables()
"
```

---

## 🤖 Model Issues

### Local Model Fails to Load

**Symptoms:**
- Error loading model
- `llama_cpp` import errors

**Solution:**

```bash
# Install llama-cpp-python
pip install llama-cpp-python

# Check model path
ls -l ~/mydrive/models/tinyllama.gguf

# Update path in config/models.json
```

### Cloud API Rate Limited

**Symptoms:**
- `429 Too Many Requests`
- Intermittent failures

**Solution:**

```bash
# Add delay between requests
sleep 2

# Check rate limits
echo $GROQ_API_KEY
# Visit console.groq.com for limits
```

### API Key Not Found

**Symptoms:**
- No API key available
- Authentication errors

**Solution:**

```bash
# Check if key is set
echo $GROQ_API_KEY

# Set it
export GROQ_API_KEY="your-key-here"

# Add to .env file
echo "GROQ_API_KEY=your-key-here" >> .env
```

---

## 📊 Experiment Issues

### No Results in Database

**Symptoms:**
- Experiment completes but no data
- `--save-db` flag used but nothing saved

**Solution:**

```bash
# Check if flag was used
# Must include --save-db in command

# Verify database path
ls -l data/experiments.db

# Check permissions
chmod 644 data/experiments.db
```

### Energy Values Show 0.0J

**Symptoms:**
- All energy values zero
- Display shows 0.0 but hardware readings exist

**Solution:**

```bash
# Check baseline subtraction
sqlite3 data/experiments.db "
SELECT run_id, pkg_energy_uj/1e6, dynamic_energy_uj/1e6 
FROM runs 
WHERE exp_id = (SELECT MAX(exp_id) FROM experiments);
"

# Remeasure baseline
python -c "
from core.energy_engine import EnergyEngine
engine = EnergyEngine()
engine.measure_idle_baseline(force_remeasure=True)
"
```

### Tax Calculation Wrong

**Symptoms:**
- Tax values showing integers (1, 2, 16 instead of 1.5, 2.3)
- Mismatch between runs_tax and tax_summary_x

**Solution:**

```bash
# Fix integer division in queries
# Use * 1.0 to force floating point
sqlite3 data/experiments.db "
SELECT 
    run_number,
    MAX(CASE WHEN workflow_type='linear' THEN dynamic_energy_uj*1.0 END) as linear,
    MAX(CASE WHEN workflow_type='agentic' THEN dynamic_energy_uj*1.0 END) as agentic,
    agentic / NULLIF(linear, 0) as tax
FROM runs
WHERE exp_id = (SELECT MAX(exp_id) FROM experiments)
GROUP BY run_number;
"
```

---

## 🖥️ GUI Issues

### GUI Won't Start

**Symptoms:**
- `streamlit: command not found`
- Import errors

**Solution:**

```bash
# Install Streamlit
pip install streamlit

# Activate venv
source venv/bin/activate

# Run with full path
python -m streamlit run streamlit_app.py
```

### PDF Button Not Working

**Symptoms:**
- Clicking "Generate PDF" does nothing
- PDF generation fails silently

**Solution:**

```bash
# Install required packages
pip install reportlab kaleido

# Check for errors in terminal
streamlit run streamlit_app.py
```

### CORS Warnings

**Symptoms:**
```
Warning: the config option 'server.enableCORS=false' is not compatible with...
```

**Solution:**
These warnings are harmless. The GUI works normally. They can be ignored.

### Blank Page in Browser

**Symptoms:**
- Browser shows white screen
- Console shows errors

**Solution:**

```bash
# Clear browser cache
# Try incognito/private mode
# Check if port 8501 is free
netstat -tulpn | grep 8501
```

---

## 📊 Sample Issues

### Duplicate Samples

**Check:**

```sql
SELECT 
    timestamp_ns,
    COUNT(*) as count,
    GROUP_CONCAT(run_id) as runs
FROM energy_samples
GROUP BY timestamp_ns
HAVING COUNT(*) > 1;
```

**Fix:** The issue is fixed in recent versions. If you have old data:

```sql
DELETE FROM energy_samples
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM energy_samples
    GROUP BY run_id, timestamp_ns
);
```

### Sample Count Mismatch

**Check:**

```sql
SELECT 
    run_id,
    (SELECT COUNT(*) FROM energy_samples WHERE run_id = r.run_id) as energy,
    (SELECT COUNT(*) FROM cpu_samples WHERE run_id = r.run_id) as cpu,
    (SELECT COUNT(*) FROM interrupt_samples WHERE run_id = r.run_id) as irq
FROM runs r
WHERE exp_id = (SELECT MAX(exp_id) FROM experiments);
```

**Normal ranges:**

- Energy: 100-200 samples (100Hz)
- CPU: 10-20 samples (10Hz)
- Interrupt: 10-20 samples (10Hz)

---

## 🔧 Hardware Issues

### MSR Access Failed

**Symptoms:**
- C-state counters not available
- Failed to open `/dev/cpu/0/msr`

**Solution:**

```bash
# Load MSR module
sudo modprobe msr

# Check permissions
ls -l /dev/cpu/*/msr
sudo ./scripts/fix_permissions.sh
```

### Turbostat Missing

**Symptoms:**
- "Turbostat: Not available" in hardware detection
- No CPU frequency data

**Solution:**

```bash
# Ubuntu/Debian
sudo apt install linux-tools-common linux-tools-generic

# Fedora/RHEL
sudo dnf install kernel-tools

# Arch
sudo pacman -S linux-tools
```

### No Thermal Zones

**Symptoms:**
- No temperature readings
- `thermal_samples` table empty

**Solution:**

```bash
# Install sensors
sudo apt install lm-sensors  # Ubuntu/Debian
sudo dnf install lm_sensors  # Fedora

# Detect sensors
sudo sensors-detect

# Load modules
sudo modprobe coretemp
```

---

## 🔍 Getting Help

### Verbose Logging

```bash
# Enable debug output
export A_LEMS_DEBUG=1

# Run with verbose flag
python -m core.execution.tests.run_experiment \
    --tasks gsm8k_basic \
    --verbose
```

### Check Logs

```bash
# View recent logs
tail -50 tmp/log.txt

# Search for errors
grep -i error tmp/log.txt

# Follow live logs
tail -f tmp/log.txt
```

### Collect System Info

```bash
# For GitHub issues, include:
python scripts/tools/issue_tracer.py --output report.txt
uname -a
cat /etc/os-release
pip list | grep -E "torch|numpy|streamlit"
```

---

## ✅ Still Having Issues?

1. **Run diagnostics:** `python scripts/tools/issue_tracer.py`
2. **Check logs:** `tail -100 tmp/log.txt`
3. **Search existing issues:** [GitHub Issues](https://github.com/deepakpanigrahy03/a-lems/issues)
4. **Open new issue with:**
   - Diagnostic report
   - Command that failed
   - Error messages
   - System info (`uname -a`)

---

## ✅ **All 6 User Guide Files Complete**

| File | Status |
|------|--------|
| `01-running.md` | ✅ Done |
| `02-understanding-metrics.md` | ✅ Done |
| `03-batch-experiments.md` | ✅ Done |
| `04-gui-usage.md` | ✅ Done |
| `05-generating-reports.md` | ✅ Done |
| `06-troubleshooting.md` | ✅ Done |