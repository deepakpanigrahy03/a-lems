# A‑LEMS: Agentic LLM Energy Measurement System

A cross-layer measurement and profiling framework for AI workloads.

[![GitHub](https://img.shields.io/badge/GitHub-Repository-blue)](https://github.com/deepakpanigrahy03/a-lems)
[![Python](https://img.shields.io/badge/Python-3.10%2B-green)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)](https://a-lems-dash.streamlit.app/)
[![Render](https://img.shields.io/badge/Render-Dashboard-purple)](https://a-lems-dashboard.onrender.com/)

---

## 🎯 Overview

**A‑LEMS** is a research-grade measurement and profiling framework for AI workloads. It captures telemetry across **hardware, system, orchestration, and workload levels**, enabling energy-aware AI systems research and evaluation of model behavior.

---

## ✨ Key Capabilities

| Level | Metrics Captured |
|-------|------------------|
| **Hardware** | CPU package, core, uncore, DRAM energy via RAPL |
| **Performance** | Instructions, cycles, IPC, cache activity |
| **System** | Context switches, interrupts, memory faults |
| **Thermal/Network** | Temperature rise, inference latency |
| **Workload** | Prompt tokens, completion tokens, execution time |
| **Orchestration** | Planning, execution, synthesis phases with per-phase energy |

**~144 features per run** combining hardware, system, network, LLM, and orchestration metrics.

---

## 🌍 Sustainability Layer

Translates energy into environmental impact:

- **Carbon** (g CO₂) using region-aware grid factors
- **Water** (ml) for data center cooling
- **Methane** (mg CH₄) with IPCC AR6 factors

---

## 👥 Who Is This For?

| User | Use Case |
|------|----------|
| **Silicon Developers** | Analyze energy/thermal behavior at hardware level |
| **Orchestration Teams** | Evaluate multi-agent workflow overhead |
| **ML Engineers** | Capture LLM telemetry for model optimization |
| **Sustainability Teams** | Translate energy to carbon/water/methane impact |
| **Cloud Architects** | Manage multi-host experiments across platforms |

---

## 🧪 Experiment Design

- **Structured templates** for systematic variation of models, tasks, workflows
- **16 configurable task categories** — easily extensible
- **Multi-host dispatch** across multiple machines
- **Per-query normalization** for cross-model comparison

---

## 📊 Output & Reporting

After each experiment, A‑LEMS generates a **detailed PDF lab report** summarizing:

- Hardware-level energy breakdown
- Orchestration tax analysis
- Thermal profiles
- Sustainability metrics
- Task-level performance

---

## 🚀 Live Demos

Try the full-featured interface:

| Platform | URL |
|----------|-----|
| **Streamlit** | [https://a-lems-dash.streamlit.app/](https://a-lems-dash.streamlit.app/) |
| **Render** | [https://a-lems-dashboard.onrender.com/](https://a-lems-dashboard.onrender.com/) |

---

## 📖 Documentation Sections

| Section | Description |
|---------|-------------|
| **[Getting Started](getting-started/01-installation.md)** | Complete setup guide |
| **[User Guide](user-guide/01-running.md)** | Running experiments |
| **[Developer Guide](developer-guide/01-architecture.md)** | Extending the system |
| **[API Reference](api/reference.md)** | Technical docs |
| **[Research](research/01-orchestration-tax.md)** | Findings & publications |

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

---

*Built for energy-aware AI research*
