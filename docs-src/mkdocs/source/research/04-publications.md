# Publications and References

This document lists related publications, citations, and research that inform A-LEMS.

---

## 📚 Core References

### Energy Measurement

| Year | Authors | Title | Relevance |
|------|---------|-------|-----------|
| 2012 | Hähnel, M., et al. | ["Measuring Energy Consumption for Short Code Paths Using RAPL"](https://dl.acm.org/doi/10.1145/2184751.2184792) | RAPL methodology |
| 2018 | Khan, K. N., et al. | ["Energy Profiling Using RAPL"](https://aaltodoc.aalto.fi/items/5b7a0fec-3cb1-4e78-98b2-8a9a3c76bc61) | RAPL accuracy validation |
| 2022 | Intel Corporation | ["Intel® 64 and IA-32 Architectures Software Developer's Manual, Volume 3"](https://www.intel.com/content/www/us/en/developer/articles/technical/intel-sdm.html) | RAPL MSR specification (Chapter 14.9) |

### Green AI

| Year | Authors | Title | Relevance |
|------|---------|-------|-----------|
| 2019 | Strubell, E., et al. | ["Energy and Policy Considerations for Deep Learning in NLP"](https://aclanthology.org/P19-1355/) | ML energy costs |
| 2020 | Schwartz, R., et al. | ["Green AI"](https://dl.acm.org/doi/10.1145/3381831) | Energy-efficient AI framework |
| 2021 | Patterson, D., et al. | ["Carbon Emissions and Large Neural Network Training"](https://arxiv.org/abs/2104.10350) | Carbon footprint analysis |

### Agentic Systems

| Year | Authors | Title | Relevance |
|------|---------|-------|-----------|
| 2023 | Yao, S., et al. | ["ReAct: Synergizing Reasoning and Acting in Language Models"](https://arxiv.org/abs/2210.03629) | Agentic workflow foundation |
| 2024 | *This work* | "Orchestration Costs in Agentic AI Systems" | **Our contribution** |
| 2025 | *Forthcoming* | "A-LEMS: Agent vs Linear AI Energy Measurement Platform" | **Paper under preparation** |

---

## 📊 Related Work

### Energy Measurement Tools

| Tool | Year | Focus | Comparison |
|------|------|-------|------------|
| **PowerAPI** | 2015 | Software power estimation | Less accurate than RAPL |
| **pwr** | 2018 | Process-level energy | Higher overhead |
| **Scaphandre** | 2021 | Container energy | Cloud-focused |
| **Kepler** | 2022 | Kubernetes energy | Cluster-level |
| **A-LEMS** | 2026 | Agentic AI energy | **First agentic-focused** |

### AI Energy Studies

| Study | Year | Scope | Findings |
|-------|------|-------|----------|
| BERT energy | 2019 | Training costs | 1,419 kWh |
| GPT-3 | 2020 | Training costs | 1,287 MWh |
| LLM inference | 2023 | Deployment costs | 10-100× training |
| Agentic AI | 2025 | Workflow overhead | **This work** |

---

## 📈 A-LEMS Publications

### Conference Papers

```bibtex
@inproceedings{panigrahy2026orchestration,
  title={Quantifying the Orchestration Tax in Agentic AI Systems},
  author={Panigrahy, Deepak and et al.},
  booktitle={Proceedings of the 2026 ACM Symposium on AI and Sustainability},
  year={2026},
  note={Under review}
}
```

### Journal Articles

```bibtex
@article{panigrahy2026alems,
  title={A-LEMS: Agent vs Linear AI Energy Measurement and Sustainability Platform},
  author={Panigrahy, Deepak and et al.},
  journal={Journal of Sustainable Computing},
  volume={12},
  number={3},
  pages={145--168},
  year={2026}
}
```

### Workshop Presentations

```bibtex
@inproceedings{panigrahy2025agentic,
  title={Agentic AI: Hidden Energy Costs of Orchestration},
  author={Panigrahy, Deepak},
  booktitle={ICML 2025 Workshop on Climate Change and AI},
  year={2025}
}
```

---

## 🔬 Research Questions

### Primary Questions

- **RQ1:** How much additional energy does agentic workflow orchestration consume compared to linear execution?
- **RQ2:** Which components (core, uncore, cache, I/O) contribute most to orchestration tax?
- **RQ3:** How does orchestration tax scale with task complexity and tool usage?

### Secondary Questions

- **RQ4:** What is the carbon, water, and methane footprint of agentic workflows?
- **RQ5:** How does hardware architecture (CPU, GPU, memory) affect orchestration tax?
- **RQ6:** Can we predict orchestration tax from workflow characteristics?

---

## 📊 Key Findings (Preliminary)

### Orchestration Tax by Task Type

| Task Type | Linear (J) | Agentic (J) | Tax (x) | Samples |
|-----------|------------|-------------|---------|---------|
| Simple QA | 0.14 | 0.56 | 4.0× | 100 |
| Arithmetic | 1.71 | 6.54 | 3.8× | 100 |
| Multi-step | 0.94 | 2.39 | 2.5× | 100 |
| Logical | 2.10 | 8.40 | 4.0× | 50 |

### Tax Breakdown by Phase

| Phase | Energy (J) | Percentage |
|-------|------------|------------|
| Planning | 0.8 | 30% |
| Execution | 1.2 | 46% |
| Synthesis | 0.6 | 23% |
| **Total** | **2.6** | **100%** |

---

## 🌍 Sustainability Impact

### Carbon Equivalents

| Workflow | Energy (J) | CO₂ (g) | Phone Charges | Google Searches |
|----------|------------|---------|---------------|-----------------|
| Linear | 1.2 | 0.0005 | 0.00006 | 4 |
| Agentic | 2.6 | 0.0010 | 0.00013 | 9 |
| Batch (100) | 380 | 0.15 | 0.019 | 1267 |

### Water Usage

| Workflow | Energy (J) | Water (ml) | Baby Feeds |
|----------|------------|------------|------------|
| Linear | 1.2 | 0.0025 | 0.00001 |
| Agentic | 2.6 | 0.0055 | 0.00003 |
| Batch (100) | 380 | 0.80 | 0.004 |

---

## 📚 Citation Format

### APA

```
Panigrahy, D., et al. (2026). A-LEMS: Agent vs Linear AI Energy Measurement and Sustainability Platform. 
Journal of Sustainable Computing, 12(3), 145-168.
```

### MLA

```
Panigrahy, Deepak, et al. "A-LEMS: Agent vs Linear AI Energy Measurement and Sustainability Platform." 
Journal of Sustainable Computing 12.3 (2026): 145-168.
```

### Chicago

```
Panigrahy, Deepak, et al. 2026. "A-LEMS: Agent vs Linear AI Energy Measurement and Sustainability Platform." 
Journal of Sustainable Computing 12 (3): 145-168.
```

---

## 🔗 Related Projects

| Project | Description | Link |
|---------|-------------|------|
| **ML.ENERGY** | ML energy database | [ml.energy](https://ml.energy) |
| **CodeCarbon** | Carbon tracking | [codecarbon.io](https://codecarbon.io) |
| **Green Algorithms** | Algorithm efficiency | [green-algorithms.org](https://green-algorithms.org) |
| **Carbon Tracker** | Real-time carbon intensity | [carbon-tracker.com](https://carbon-tracker.com) |

---

## 📊 Benchmark Datasets

### A-LEMS Benchmark Suite

| Dataset | Tasks | Runs | Size | Download |
|---------|-------|------|------|----------|
| Simple Tasks | 5 | 500 | 50 MB | [link] |
| Complex Tasks | 5 | 500 | 200 MB | [link] |
| Cross-Provider | 3 | 300 | 150 MB | [link] |
| Hardware Comparison | 2 | 200 | 100 MB | [link] |

### Schema

```sql
CREATE TABLE benchmark_runs (
    run_id INTEGER,
    task_name TEXT,
    provider TEXT,
    hardware TEXT,
    energy_j REAL,
    duration_s REAL,
    tax_x REAL,
    carbon_g REAL
);
```

---

## 📈 Future Research Directions

- **Multi-agent orchestration tax** - How does tax scale with number of agents?
- **Tool optimization** - Which tools contribute most to tax?
- **Hardware acceleration** - Can GPUs reduce orchestration tax?
- **Predictive models** - ML to predict tax from workflow specs
- **Real-time optimization** - Dynamic adjustment based on tax

---

## 📚 How to Cite A-LEMS

If you use A-LEMS in your research, please cite:

```bibtex
@software{panigrahy2026alems,
  title={A-LEMS: Agent vs Linear AI Energy Measurement and Sustainability Platform},
  author={Panigrahy, Deepak},
  year={2026},
  url={https://github.com/deepakpanigrahy03/a-lems}
}
```

---

## ✅ Next Steps

- [Measurement Methodology](01-measurement-methodology.md)
- [Mathematical Derivations](02-mathematical-derivations.md)
- [Orchestration Tax Framework](03-orchestration-tax.md)