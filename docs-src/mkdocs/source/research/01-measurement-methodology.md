# Measurement Methodology

This document describes the core measurement principles and methodologies used in A-LEMS.

---

## 🎯 Core Principles

### 1. Immutable Raw Data

All raw measurements are **never modified**. This ensures:
- **Reproducibility** - Original data always available
- **Auditability** - Calculations can be verified
- **Flexibility** - New analysis methods can be applied retrospectively

### 2. Three-Layer Architecture

```
Raw Measurements (immutable)
        │
        ▼
Baseline Subtraction
        │
        ▼
Derived Metrics (workload, tax, etc.)
```

### 3. Timestamp Precision

All samples have **nanosecond-precision timestamps** for:
- Exact correlation with AI workflow phases
- Accurate power calculation
- Precise energy attribution

---

## ⚡ RAPL Energy Measurement

### How RAPL Works

Intel's Running Average Power Limit (RAPL) provides energy counters for different CPU domains:

| Domain | Description | Energy Counter |
|--------|-------------|----------------|
| **Package** | Entire CPU package (cores + uncore) | `package_energy_uj` |
| **Core** | All CPU cores combined | `core_energy_uj` |
| **Uncore** | Cache, memory controller, I/O | `uncore_energy_uj` |
| **DRAM** | Memory subsystem | `dram_energy_uj` |

Each domain has a cumulative energy counter in **microjoules (µJ)**:

$$E_{domain}(t) = \text{counter}_t$$

### Energy Calculation

Energy consumed during measurement interval:

$$\Delta E_{domain} = E_{domain}(t_2) - E_{domain}(t_1)$$

Average power:

$$P_{avg} = \frac{\Delta E_{domain}}{\Delta t} = \frac{E_{domain}(t_2) - E_{domain}(t_1)}{t_2 - t_1}$$

### 100Hz Sampling

The sampling thread runs at 100Hz (configurable):

$$\text{sampling interval} = \frac{1}{f_s} = 10\text{ ms}$$

This captures energy dynamics at:
- 50Hz Nyquist frequency
- Transient spikes down to 20ms duration
- Sub-milliwatt power resolution

---

## 📏 Idle Baseline Methodology

### Why Baseline Matters

Total measured energy includes both workload and system overhead:

$$E_{total} = E_{workload} + E_{idle}$$

To isolate workload energy, we measure idle power:

$$E_{workload} = E_{total} - P_{idle} \cdot \Delta t$$

### Baseline Measurement Protocol

1. **Core pinning**: Pin measurement thread to dedicated core
2. **Pre-wait**: Allow system to enter deep idle (5-10 seconds)
3. **Multiple samples**: 3-10 samples of 10-30 seconds each
4. **Statistical processing**: Mean and standard deviation per domain

### Confidence Interval Approach

Rather than using mean power, we use the **lower bound of 95% confidence interval**:

$$P_{idle,min} = \mu - 2\sigma$$

This prevents over-subtraction of idle energy.

---

## 🧮 Derived Metrics

### Workload Energy

$$E_{workload} = E_{package} - P_{idle,package} \cdot \Delta t$$

### Reasoning Energy

$$E_{reasoning} = E_{core} - P_{idle,core} \cdot \Delta t$$

### Orchestration Tax (Mathematical Proof)

$$
\begin{aligned}
E_{tax} &= E_{workload} - E_{reasoning} \\
&= (E_{pkg} - P_{idle,pkg}\Delta t) - (E_{core} - P_{idle,core}\Delta t) \\
&= (E_{pkg} - E_{core}) - (P_{idle,pkg} - P_{idle,core})\Delta t \\
&= E_{uncore} - P_{idle,uncore}\Delta t
\end{aligned}
$$

**Conclusion:** Orchestration tax equals uncore energy minus idle uncore energy. This is where cache, memory controller, and I/O overhead appear.

---

## 🌡️ Thermal Measurement

### Temperature Sampling

Thermal zones sampled at 1Hz:

$$T_i = \text{temperature at time } t_i$$

### Thermal Gradient

Rate of temperature change:

$$\nabla T = \frac{T_{max} - T_{start}}{t_{rise}}$$

### Thermal-Energy Coupling

At high temperatures, leakage current increases:

$$P_{leak} \propto T^2 e^{-E_a/kT}$$

Total power:

$$P_{total} = P_{dynamic} + P_{leak}(T)$$

---

## 📊 Performance Counter Methodology

### Instructions Per Cycle

$$IPC = \frac{\text{instructions}}{\text{cycles}}$$

IPC indicates CPU efficiency:
- **< 1.0**: Memory-bound or stalled
- **1.0 - 2.0**: Mixed workload
- **> 2.0**: Compute-bound, efficient

### Cache Miss Rate

$$\text{MissRate} = \frac{\text{cache\_misses}}{\text{cache\_references}} \times 100\%$$

High miss rates indicate:
- Poor data locality
- Random access patterns
- Working set exceeds cache size

---

## 🔄 C-State Measurement

### C-State Residency

Time spent in each C-state:

$$t_{Cx} = \sum_{i} \Delta t_i \cdot \mathbb{1}_{state=Cx}$$

Residency percentage:

$$\%Cx = \frac{t_{Cx}}{t_{total}} \times 100\%$$

### Power Savings

Energy saved by entering deep C-states:

$$E_{saved} = \sum_{x} (P_{C0} - P_{Cx}) \cdot t_{Cx}$$

---

## 📈 Statistical Methodology

### Sample Size Determination

For desired effect size $\delta$, power $1-\beta$, and significance $\alpha$:

$$n = \frac{(z_{1-\alpha/2} + z_{1-\beta})^2 \sigma^2}{\delta^2}$$

### Confidence Intervals

95% confidence interval for mean:

$$CI = \bar{x} \pm 1.96 \cdot \frac{\sigma}{\sqrt{n}}$$

### Outlier Detection

Values outside $\mu \pm 3\sigma$ are flagged for review.

---

## 🎯 Measurement Validation

### Repeatability

Coefficient of variation across identical runs:

$$CV = \frac{\sigma}{\mu} \times 100\%$$

Acceptable: < 5%

### Accuracy Validation

Comparison with external power meter:

$$\text{error} = \frac{|E_{RAPL} - E_{meter}|}{E_{meter}} \times 100\%$$

Acceptable: < 2%

---

## 📚 References

1. Intel Corporation. (2012). "Intel® 64 and IA-32 Architectures Software Developer's Manual"
2. Hähnel, M., et al. (2012). "Measuring Energy Consumption for Short Code Paths Using RAPL"
3. Khan, K. N., et al. (2018). "Energy Profiling Using RAPL"