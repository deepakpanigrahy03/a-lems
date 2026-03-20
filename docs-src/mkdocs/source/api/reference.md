# API Reference

A-LEMS provides a comprehensive Python API for energy measurement and analysis. This section documents the core modules, hardware readers, and analysis tools.

---

## 📚 API Overview

The API is organized into three main packages:

| Package | Description | Key Classes |
|---------|-------------|-------------|
| **`core`** | Main energy measurement engine | `EnergyEngine`, `RAPLReader`, `MSRReader`, `PerfReader` |
| **`gui`** | Dashboard components | `SessionAnalysis`, `EnergyPage`, `TaxAnalyzer` |
| **`scripts.tools`** | Developer utilities | `LLMContext`, `QualityChecker`, `ColumnFlow` |

---

## 🔧 Core Modules

### `core.energy_engine.EnergyEngine`

The main orchestrator for energy measurement.

```python
from core.energy_engine import EnergyEngine

engine = EnergyEngine(config)

with engine.start_measurement():
    # Your AI workload here
    result = llm.invoke(prompt)

raw_measurement = engine.stop_measurement()
```

### `core.readers`

Hardware readers for different metrics:

| Reader | Metrics | Frequency |
|--------|---------|-----------|
| `RAPLReader` | Package, core, uncore, DRAM energy | 100Hz |
| `MSRReader` | C-state counters, ring bus frequency | Snapshots |
| `PerfReader` | Instructions, cycles, cache misses | Process-attached |
| `TurbostatReader` | CPU frequency, temperature | 10Hz |

---

## 📊 Analysis Modules

### `core.analysis.energy_analyzer`

Computes derived metrics from raw measurements:

- **Workload Energy** = Package energy - Idle baseline
- **Reasoning Energy** = Core energy - Idle core
- **Orchestration Tax** = Workload - Reasoning

### `core.sustainability.calculator`

Converts energy to environmental impact:

```python
sustainability = calculator.calculate(energy_j, country_code)
print(f"Carbon: {sustainability.carbon_g:.6f} g")
print(f"Water: {sustainability.water_ml:.6f} ml")
```

---

## 🖥️ GUI Modules

### `gui.pages`

Each page is a Streamlit component:

| Page | Purpose |
|------|---------|
| `overview` | Dashboard home |
| `energy` | Energy visualization |
| `tax` | Orchestration tax analysis |
| `sustainability` | Environmental metrics |

---

## 🛠️ Developer Tools

The `scripts.tools` directory contains utilities:

| Tool | Purpose |
|------|---------|
| `llm_context.py` | Generate context for LLM prompts |
| `quality_check.py` | Run code quality checks |
| `column_flow.py` | Trace database column data flow |
| `issue_tracer.py` | System diagnostics |

---

## 📖 Full API Documentation

For complete API details, including all classes, methods, and parameters, see the [Full API Docs](../sphinx/).

---

## 🔗 Related Documentation

- [Developer Guide](../developer-guide/01-architecture.md) — System architecture
- [Adding New Readers](../developer-guide/05-adding-readers.md) — Extend hardware support
- [Developer Tools](../developer-guide/07-tools-overview.md) — Tool usage guide

---

*Last updated: March 2026*