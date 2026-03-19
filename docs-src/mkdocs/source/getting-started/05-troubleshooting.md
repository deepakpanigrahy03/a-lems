# ⚠️ Step 11: Troubleshooting Guide

Common issues and their solutions.

---

## 🔧 Quick Reference Table

| Issue | Likely Cause | Solution |
|-------|--------------|----------|
| **Permission denied** | Hardware access restricted | `sudo ./scripts/fix_permissions.sh` |
| **ModuleNotFoundError** | Virtual env not activated | `source venv/bin/activate` |
| **RAPL not found** | CPU doesn't support RAPL | `cat /proc/cpuinfo \| grep rapl` |
| **MSR access failed** | msr module not loaded | `sudo modprobe msr` |
| **Turbostat missing** | linux-tools not installed | Install kernel-tools package for your distro |
| **No thermal zones** | Sensors not detected | `sudo sensors-detect` |
| **GPU not detected** | Missing drivers | Install appropriate GPU drivers |
| **Database locked** | Another process using DB | `rm -f data/experiments.db-journal` |
| **API key invalid** | Wrong or expired key | Check `.env` file |

---

## 🛠️ Diagnostic Tool

Run the automatic issue tracer:

```bash
python scripts/tools/issue_tracer.py
```

This will check:

- ✅ Hardware accessibility
- ✅ Database integrity
- ✅ API connectivity
- ✅ Documentation builds
- ✅ Code quality

---

## 📋 Common Error Messages

### "No module named 'core'"

```bash
# Make sure you're in project root
cd ~/mydrive/a-lems

# Activate virtual environment
source venv/bin/activate
```

### "Permission denied" on hardware access

```bash
sudo ./scripts/fix_permissions.sh
# Logout and login again for changes to take effect
```

### "Database is locked"

```bash
# Remove lock files (only if no experiments are running)
rm -f data/experiments.db-journal
rm -f data/experiments.db-wal
rm -f data/experiments.db-shm
```

### "401 Unauthorized" from Groq

```bash
# Check your API key
echo $GROQ_API_KEY

# If empty, reload .env
set -a; source .env; set +a
```

---

## 🔍 Still Having Issues?

### 1. Run the issue tracer

```bash
python scripts/tools/issue_tracer.py
```

### 2. Check the logs

```bash
cat tmp/log.txt | tail -50
```

### 3. Open an issue on GitHub

Include:

- Your `hw_config.json` (redact personal info)
- The full error message
- Output of `python scripts/verify_hardware.py`
- Output of `python scripts/tools/issue_tracer.py`

---

## ✅ Installation Complete!

You've successfully set up A-LEMS and are ready to start experimenting! 🎉

Proceed to:
- [Quick Start Guide](04-quick-start.md)
- [Understanding Metrics](../user-guide/02-understanding-metrics.md)
- [Using the GUI](../user-guide/04-gui-usage.md)