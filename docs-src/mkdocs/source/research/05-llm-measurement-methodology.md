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

$$preprocess\_ms = (t_1 - t_0) \times 1000$$

$$non\_local\_ms = (t_2 - t_1) \times 1000$$

$$local\_compute\_ms = (t_3 - t_2) \times 1000$$

$$postprocess\_ms = (t_4 - t_3) \times 1000$$

$$total\_time\_ms = preprocess\_ms + non\_local\_ms + local\_compute\_ms + postprocess\_ms$$

---

## 📊 Token Metrics

### From API Response

$$prompt\_tokens = \text{usage.prompt\_tokens}$$

$$completion\_tokens = \text{usage.completion\_tokens}$$

$$total\_tokens = prompt\_tokens + completion\_tokens$$

### Fallback Estimation (when API doesn't return tokens)

$$prompt\_tokens \approx \lceil \frac{\text{len(prompt)}}{4} \rceil$$

$$completion\_tokens \approx \lceil \frac{\text{len(response)}}{4} \rceil$$

---

## 🌐 Throughput Calculation

### Application-Level Throughput

$$total\_bytes = \text{len(prompt)} + \text{len(response)}$$

$$app\_throughput\_kbps = \begin{cases}
\frac{total\_bytes \times 8}{non\_local\_ms / 1000} / 1000 & \text{if } non\_local\_ms > 0 \\
0 & \text{otherwise}
\end{cases}$$

---

## 📡 Network Metrics

Captured before and after API call:

$$bytes\_sent = net_{after}["bytes\_sent"] - net_{before}["bytes\_sent"]$$

$$bytes\_recv = net_{after}["bytes\_recv"] - net_{before}["bytes\_recv"]$$

$$tcp\_retransmits = net_{after}["tcp\_retransmits"] - net_{before}["tcp\_retransmits"]$$

**Note:** For local runs ($provider \in \{\text{local}, \text{ollama}\}$), these are set to 0.

---

## 💻 CPU During Wait

$$cpu\_percent\_during\_wait = \begin{cases}
\text{psutil.cpu\_percent}(interval = non\_local\_ms / 1000) & \text{if } non\_local\_ms > 0 \\
0 & \text{otherwise}
\end{cases}$$

---

## 🔄 Workflow Aggregation (Agentic)

For workflows with multiple LLM calls:

$$total\_pre = \sum_{i} preprocess\_ms_i$$

$$total\_wait = \sum_{i} non\_local\_ms_i$$

$$total\_post = \sum_{i} postprocess\_ms_i$$

$$total\_compute = \sum_{i} local\_compute\_ms_i$$

$$total\_bytes\_sent = \sum_{i} bytes\_sent\_approx_i$$

$$total\_bytes\_recv = \sum_{i} bytes\_recv\_approx_i$$

### Orchestration CPU

$$T_{orchestration} = T_{workflow} - total\_compute - total\_wait$$

Where $T_{workflow}$ is the total workflow execution time from start to end.

**Assumption:** `preprocess_ms` and `postprocess_ms` are part of orchestration overhead, not model computation.

### Workflow Compute Time

$$T_{compute\_total} = total\_pre + total\_post + T_{orchestration}$$

### Effective Throughput

$$effective\_throughput\_kbps = \begin{cases}
\frac{(total\_bytes\_sent + total\_bytes\_recv) \times 8}{total\_wait / 1000} / 1000 & \text{if } total\_wait > 0 \\
0 & \text{otherwise}
\end{cases}$$

---

## 📈 Orchestration Overhead Index (OOI)

### Time-Based OOI

$$OOI_{time} = \frac{T_{orchestration}}{T_{total}}$$

### CPU-Based OOI

$$OOI_{cpu} = \frac{T_{orchestration}}{T_{compute\_total}}$$

**Interpretation:**

| OOI Value | Meaning |
|-----------|---------|
| ~0 | Minimal orchestration (linear-like) |
| 0.1–0.3 | Moderate orchestration |
| 0.3–0.6 | Heavy agent coordination |
| >0.6 | Orchestration-dominated system |

---

## 📊 Useful Compute Ratio (UCR)

$$UCR = \frac{T_{compute}}{T_{total}}$$

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

$$api\_latency\_ms = total\_wait\_time = \sum_{i} non\_local\_ms_i$$

$$compute\_time\_ms = T_{compute\_total}$$

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

## ⚠️ Failure Handling

Failed LLM calls are still recorded:

$$status = \text{"failed"}$$

$$error\_message = \text{str}(e)$$

$$T_{total} = (t_{error} - t_0) \times 1000$$

$$preprocess\_ms = (t_1 - t_0) \times 1000 \text{ (if available)}$$

---

## 📚 References

1. A-LEMS Technical Documentation: [System Architecture](../developer-guide/01-architecture.md)
2. A-LEMS Database Schema: [Database Design](../developer-guide/03-database-schema.md)
3. Orchestration Tax Analysis: [Mathematical Derivations](02-mathematical-derivations.md)