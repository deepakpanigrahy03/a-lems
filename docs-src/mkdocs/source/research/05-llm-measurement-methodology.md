# LLM Measurement Methodology

This document describes the measurement methodology for LLM interactions, including phase separation, metrics calculation, and aggregation strategies.

---

## 📐 Measurement Model

A-LEMS decomposes total execution time into three components:

$$T_{total} = T_{wait} + T_{compute} + T_{orchestration}$$

Where:
- $T_{wait}$: Network latency + remote inference (idle time)
- $T_{compute}$: Local model inference (active computation)
- $T_{orchestration}$: Planning, parsing, control logic, coordination (active CPU)

**Note:** Orchestration overhead exists for ALL workflows, but is significantly higher for agentic workflows.

---

## 📊 Phase Separation

Each LLM call is measured across four phases:

| Phase | Metric | Description | CPU State | Component |
|-------|--------|-------------|-----------|-----------|
| 1 | `preprocess_ms` | Prompt serialization, JSON building | Active | $T_{orchestration}$ |
| 2 | `non_local_ms` | Network + remote inference | Idle | $T_{wait}$ |
| 3 | `local_compute_ms` | Local model inference | Active | $T_{compute}$ |
| 4 | `postprocess_ms` | Response parsing, token extraction | Active | $T_{orchestration}$ |

### Timing Boundaries

$$t_0 \xrightarrow{\text{preprocess}} t_1 \xrightarrow{\text{wait}} t_2 \xrightarrow{\text{compute}} t_3 \xrightarrow{\text{postprocess}} t_4$$

Where:
- $t_0$: Start of LLM call
- $t_1$: After prompt serialization
- $t_2$: After receiving response
- $t_3$: After local model inference (local only)
- $t_4$: After response parsing

### Phase Calculations

$$t_{\text{pre}} = (t_1 - t_0) \times 1000$$

$$t_{\text{wait}} = (t_2 - t_1) \times 1000$$

$$t_{\text{compute}} = (t_3 - t_2) \times 1000$$

$$t_{\text{post}} = (t_4 - t_3) \times 1000$$

$$t_{\text{total}} = t_{\text{pre}} + t_{\text{wait}} + t_{\text{compute}} + t_{\text{post}}$$

---

## 📊 Token Metrics

### From API Response

$$\text{prompt\_tokens} = \text{usage.prompt\_tokens}$$

$$\text{completion\_tokens} = \text{usage.completion\_tokens}$$

$$\text{total\_tokens} = \text{prompt\_tokens} + \text{completion\_tokens}$$

### Fallback Estimation (when API doesn't return tokens)

$$prompt\_tokens \approx \lceil \frac{\text{len(prompt)}}{4} \rceil$$

$$completion\_tokens \approx \lceil \frac{\text{len(response)}}{4} \rceil$$

---

## 🌐 Throughput Calculation

### Application-Level Throughput

$$\text{total\_bytes} = \text{len(prompt)} + \text{len(response)}$$

$$\text{app\_throughput\_kbps} = \begin{cases}

\frac{\text{total\_bytes} \times 8}{t_{\text{wait}} / 1000} / 1000 & \text{if } t_{\text{wait}} > 0 \\
0 & \text{otherwise}
\end{cases}$$
---

## 📡 Network Metrics

Captured before and after API call:

$$\text{bytes\_sent} = \text{net}_{\text{after}}[\text{"bytes\_sent"}] - \text{net}_{\text{before}}[\text{"bytes\_sent"}]$$

$$\text{bytes\_recv} = \text{net}_{\text{after}}[\text{"bytes\_recv"}] - \text{net}_{\text{before}}[\text{"bytes\_recv"}]$$

$$\text{tcp\_retransmits} = \text{net}_{\text{after}}[\text{"tcp\_retransmits"}] - \text{net}_{\text{before}}[\text{"tcp\_retransmits"}]$$

**Note:** For local runs ($provider \in \{\text{local}, \text{ollama}\}$), these are set to 0.

---

## 💻 CPU During Wait

$$\text{cpu\_percent\_during\_wait} = \begin{cases}
\text{psutil.cpu\_percent}(interval = t_{\text{wait}} / 1000) & \text{if } t_{\text{wait}} > 0 \\
0 & \text{otherwise}
\end{cases}$$
---

## 🔄 Workflow Aggregation (Agentic)

For workflows with multiple LLM calls:

$$t_{\text{pre,total}} = \sum_{i} t_{\text{pre},i}$$

$$t_{\text{wait,total}} = \sum_{i} t_{\text{wait},i}$$

$$t_{\text{post,total}} = \sum_{i} t_{\text{post},i}$$

$$t_{\text{compute,total}} = \sum_{i} t_{\text{compute},i}$$

$$\text{bytes\_sent,total} = \sum_{i} \text{bytes\_sent\_approx}_i$$

$$\text{bytes\_recv,total} = \sum_{i} \text{bytes\_recv\_approx}_i$$

### Orchestration CPU

$$T_{\text{orchestration}} = T_{\text{workflow}} - t_{\text{compute,total}} - t_{\text{wait,total}}$$

$$\text{compute\_time\_ms} = t_{\text{pre,total}} + t_{\text{post,total}} + T_{\text{orchestration}}$$

Where $T_{workflow}$ is the total workflow execution time from start to end.

**Assumption:** `preprocess_ms` and `postprocess_ms` are part of orchestration overhead, not model computation.

### Workflow Compute Time

$$T_{\text{compute\_total}} = t_{\text{pre,total}} + t_{\text{post,total}} + T_{\text{orchestration}}$$

### Effective Throughput

$$\text{effective\_throughput\_kbps} = \begin{cases}
\frac{(\text{bytes\_sent,total} + \text{bytes\_recv,total}) \times 8}{t_{\text{wait,total}} / 1000} / 1000 & \text{if } t_{\text{wait,total}} > 0 \\
0 & \text{otherwise}
\end{cases}$$

---

## 📈 Orchestration Overhead Index (OOI)

### Time-Based OOI

$$OOI_{\text{time}} = \frac{T_{\text{orchestration}}}{T_{\text{total}}}$$

### CPU-Based OOI

$$OOI_{\text{cpu}} = \frac{T_{\text{orchestration}}}{\text{compute\_time\_ms}}$$

**Interpretation:**

| OOI Value | Meaning |
|-----------|---------|
| ~0 | Minimal orchestration (linear-like) |
| 0.1–0.3 | Moderate orchestration |
| 0.3–0.6 | Heavy agent coordination |
| >0.6 | Orchestration-dominated system |

---

## 📊 Useful Compute Ratio (UCR)

$$UCR = \frac{t_{\text{compute,total}}}{T_{\text{total}}}$$

**Interpretation:**

| Provider | UCR | Meaning |
|----------|-----|---------|
| Cloud | ~0 | No local computation |
| Local | >0 | Local model inference dominates |

---

## 🏭 Provider-Specific Behavior

| Provider | $T_{wait}$ | $T_{compute}$ | $T_{orchestration}$ | Network Metrics |
|----------|------------|---------------|---------------------|-----------------|
| Cloud (Groq/OpenRouter) | $>0$ | $0$ | $>0$ | Captured |
| Local (llama-cpp) | $0$ | $>0$ | $>0$ | $0$ |
| Ollama | $0$ | $>0$ | $>0$ | $0$ |

---

## 🔗 Legacy Compatibility

For backward compatibility with existing analyses:

$$\text{api\_latency\_ms} = t_{\text{wait,total}}$$

$$\text{compute\_time\_ms} = T_{\text{compute\_total}}$$

---

## 🧪 Validation Checks

### Time Consistency

$$T_{total} \stackrel{?}{=} T_{wait} + T_{compute} + T_{orchestration}$$

### Workflow Consistency

$$T_{workflow} \geq total\_wait + total\_compute$$

### Network Consistency (Cloud)

$$bytes\_sent \stackrel{?}{>} 0$$

$$bytes\_recv \stackrel{?}{>} 0$$

### Network Consistency (Local)

$$bytes\_sent \stackrel{?}{=} 0$$

$$bytes\_recv \stackrel{?}{=} 0$$

### Compute Consistency (Cloud)

$$local\_compute\_ms \stackrel{?}{=} 0$$

### Compute Consistency (Local)

$$local\_compute\_ms \stackrel{?}{>} 0$$

---

## 🔧 Implementation Mapping

This section maps mathematical notation to actual code variables in A-LEMS.

### Phase Variables

| Mathematical Notation | Code Variable | Location |
|-----------------------|---------------|----------|
| $t_{\text{pre}}$ | `preprocess_ms` | `_call_llm()` in `agentic.py`, `linear.py` |
| $t_{\text{wait}}$ | `non_local_ms` | `_call_llm()` in `agentic.py`, `linear.py` |
| $t_{\text{compute}}$ | `local_compute_ms` | `_call_llm()` in `agentic.py`, `linear.py` |
| $t_{\text{post}}$ | `postprocess_ms` | `_call_llm()` in `agentic.py`, `linear.py` |
| $t_{\text{total}}$ | `total_time_ms` | `_call_llm()` return value |

### Workflow Aggregation

| Mathematical Notation | Code Variable | Location |
|-----------------------|---------------|----------|
| $t_{\text{pre,total}}$ | `total_pre_ms` | `agentic.py` aggregation loop |
| $t_{\text{wait,total}}$ | `total_workflow_non_local_ms` | `agentic.py` aggregation loop |
| $t_{\text{post,total}}$ | `total_post_ms` | `agentic.py` aggregation loop |
| $t_{\text{compute,total}}$ | `total_llm_compute_ms` | `agentic.py` aggregation loop |

### Workflow-Level Metrics

| Mathematical Notation | Code Variable | Table |
|-----------------------|---------------|-------|
| $T_{\text{workflow}}$ | `total_time_ms` (workflow) | `runs` table |
| $T_{\text{orchestration}}$ | `orchestration_cpu_ms` | `runs` table |
| $T_{\text{compute\_total}}$ | `compute_time_ms` | `runs` table |
| $t_{\text{wait,total}}$ | `total_workflow_non_local_ms` | `runs` table |
| $\text{bytes\_sent,total}$ | `total_bytes_sent` → `bytes_sent` | `runs` table |
| $\text{bytes\_recv,total}$ | `total_bytes_recv` → `bytes_recv` | `runs` table |

### Database Storage

| Metric | Table | Column |
|--------|-------|--------|
| Per-call phases | `llm_interactions` | `preprocess_ms`, `non_local_ms`, `local_compute_ms`, `postprocess_ms` |
| Workflow aggregation | `runs` | `orchestration_cpu_ms`, `compute_time_ms`, `bytes_sent`, `bytes_recv` |

### Derived Metrics (Computed in Analysis)

| Mathematical Notation | Formula | Calculation |
|-----------------------|---------|-------------|
| $OOI_{\text{time}}$ | $\frac{T_{\text{orchestration}}}{T_{\text{total}}}$ | `orchestration_cpu_ms / total_time_ms` |
| $OOI_{\text{cpu}}$ | $\frac{T_{\text{orchestration}}}{T_{\text{compute\_total}}}$ | `orchestration_cpu_ms / compute_time_ms` |
| $UCR$ | $\frac{t_{\text{compute,total}}}{T_{\text{total}}}$ | `total_llm_compute_ms / total_time_ms` |

---

## ⚠️ Failure Handling

Failed LLM calls are still recorded:

$$status = \text{"failed"}$$

$$error\_message = \text{str}(e)$$

$$T_{total} = (t_{error} - t_0) \times 1000$$

$$preprocess\_ms = (t_1 - t_0) \times 1000 \text{ (if available)}$$

---

## 📊 Empirical Results (Preliminary)

The methodology described above has been validated with initial experiments. The following results are from a small sample (25-30 runs per configuration) and demonstrate the types of insights A-LEMS can provide.

**Note:** These are illustrative results from a limited dataset. Full-scale experiments with statistical significance will be conducted for publication.

| Workflow | Provider | OOI_time | OOI_cpu | UCR | Network Ratio |
|----------|----------|----------|---------|-----|---------------|
| **Agentic** | cloud | 0.065 | 0.92 | 0.0 | 0.41 |
| **Agentic** | local | 0.008 | 1.00 | 0.49 | 0.0 |
| **Linear** | cloud | 0.0 | 0.0 | 0.0 | 0.33 |
| **Linear** | local | 0.0 | 0.0 | 0.47 | 0.0 |

### Preliminary Interpretation

- **Agentic workflows appear to consume nearly all local CPU resources for orchestration** (OOI_cpu ≈ 0.92-1.00)
- This overhead accounts for **1-7% of total execution time** (OOI_time)
- **Cloud workloads show significant network waiting time** (33-41% of total time)

### Next Steps

Full-scale experiments will:
- Increase statistical power with 100+ runs per configuration
- Vary task complexity (simple, multi-step, reasoning)
- Compare across different model sizes and providers
- Analyze scaling behavior with increasing steps/tools

See [Publication Roadmap](04-publications.md) for detailed plan.


## 📚 References

1. A-LEMS Technical Documentation: [System Architecture](../developer-guide/01-architecture.md)
2. A-LEMS Database Schema: [Database Design](../developer-guide/03-database-schema.md)
3. Orchestration Tax Analysis: [Mathematical Derivations](02-mathematical-derivations.md)   