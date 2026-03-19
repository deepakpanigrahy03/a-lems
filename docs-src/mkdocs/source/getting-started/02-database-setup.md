# 🗄️ Step 8: Database Setup

After hardware detection, we need to initialize the database and load baseline measurements.

---

## 📊 Create Database Tables

Tables are created automatically on first run, but you can verify:

```bash
# Check if database exists
ls -la data/experiments.db

# List all tables
sqlite3 data/experiments.db ".tables"
```

**Expected tables:**

- `experiments`
- `hardware_config`
- `environment_config`
- `runs`
- `energy_samples`
- `cpu_samples`
- `interrupt_samples`
- `thermal_samples`
- `orchestration_events`
- `llm_interactions`
- `orchestration_tax_summary`
- `idle_baselines`
- `task_categories`

---

## ⏱️ Measure Idle Baseline

This measures your system's idle power consumption:

```bash
python -c "
from core.energy_engine import EnergyEngine
from core.config_loader import ConfigLoader
from core.utils.baseline_manager import BaselineManager

engine = EnergyEngine(ConfigLoader().get_hardware_config())
baseline = engine.measure_idle_baseline(duration_seconds=10, num_samples=3)

bm = BaselineManager()
bm.save(baseline)

print(f'✅ Baseline saved: {baseline.baseline_id}')
print(f'   Package idle power: {baseline.power_watts.get(\"package-0\", 0):.3f} W')
"
```

> **Important:** Keep your system idle during measurement (no mouse/keyboard/movement, close background apps).

---

## 💾 Load Hardware & Environment to Database

```bash
python scripts/load_configs_to_db.py
```

This creates `config/current_session.json` with your:

- `hw_id` (hardware fingerprint)
- `env_id` (environment fingerprint)

---

## ✅ Verify Database Setup

```bash
# Check hardware record
sqlite3 data/experiments.db "SELECT hw_id, cpu_model, hardware_hash FROM hardware_config;"

# Check environment record
sqlite3 data/experiments.db "SELECT env_id, git_commit, env_hash FROM environment_config;"

# Check baseline
sqlite3 data/experiments.db "SELECT baseline_id, package_power_watts FROM idle_baselines ORDER BY timestamp DESC LIMIT 1;"
```

---

## 🔄 Next Step

Proceed to **[Model Configuration](03-model-config.md)** to set up your LLM API keys.