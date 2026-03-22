<div align="center">
  
  # ⚡ A-LEMS
  ### **Agentic LLM Energy Measurement System**
  
  [![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python)](https://python.org)
  [![License](https://img.shields.io/badge/License-Apache%202.0-green?style=for-the-badge)](LICENSE)
  [![Streamlit](https://img.shields.io/badge/GUI-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit)](https://streamlit.io)
  [![Documentation](https://img.shields.io/badge/Documentation-live-brightgreen?style=for-the-badge)](https://deepakpanigrahy03.github.io/a-lems)
  [![Live Demo](https://img.shields.io/badge/Live%20Demo-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit)](https://a-lems-dash.streamlit.app/)
  
  **Quantifying the energy cost of agentic AI workflows**
  
  <a href="https://deepakpanigrahy03.github.io/a-lems" target="_blank">📖 Documentation</a> • 
  <a href="https://a-lems-dash.streamlit.app/" target="_blank">📊 Live Demo</a>
  
</div>

---

## 🔬 **What is A-LEMS?**

A research platform that measures **hardware-level energy consumption** of AI workflows:

| **Linear** | **Agentic** |
|------------|-------------|
| Single LLM call | Planning + Tools + Synthesis |
| 1 API request | Multiple steps + reasoning |

**Core contribution:** Quantifying the **orchestration tax** — the energy overhead of agentic coordination.

---

## ✨ **Key Features**

✅ **100Hz hardware sampling** — RAPL, MSR, perf counters  
✅ **3-layer data model** — Raw → Baseline → Derived (immutable)  
✅ **80+ metrics per run** — ML-ready dataset  
✅ **Sustainability metrics** — Carbon, water, methane per query  
✅ **Multi-provider** — Groq, OpenRouter, Ollama  
✅ **11 developer tools** — Code analysis, docs, diagnostics  

---

## 🚀 **Quick Start**

```
bash
# 1. Install
git clone https://github.com/deepakpanigrahy03/a-lems.git
cd a-lems
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Detect hardware
sudo python scripts/detect_hardware.py

# 3. Run first experiment
python -m core.execution.tests.test_harness --task-id simple --repetitions 1 --provider local --save-db

# 4. Launch dashboard
streamlit run streamlit_app.py
```
---

## 📊 **Live Demo**

Try the dashboard: [a-lems-dash.streamlit.app](https:///a-lems-dash.streamlit.app)  (or)
Alternatively on : [a-lems-dashboard.onrender.com](https://a-lems-dashboard.onrender.com/)

*(No installation needed — runs in your browser)*

---

## 📚 **Documentation**

- [Getting Started](docs-src/mkdocs/source/getting-started/01-installation.md)
- [User Guide](docs-src/mkdocs/source/user-guide/01-running.md)
- [Developer Guide](docs-src/mkdocs/source/developer-guide/01-architecture.md)
- [API Reference](https://deepakpanigrahy03.github.io/a-lems)

---

## 📄 **License**

Apache License 2.0 — see [LICENSE](LICENSE)

---

## 📝 **Citation**

```bibtex
@software{panigrahy2026alems,
  title={A-LEMS: Agentic LLM Energy Measurement System},
  author={Panigrahy, Deepak},
  year={2026},
  url={https://github.com/deepakpanigrahy03/a-lems}
}
<div align="center"> Built with ⚡ for sustainable AI research </div> ```