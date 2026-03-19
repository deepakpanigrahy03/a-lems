# Mathematical Derivations

This document contains all mathematical formulas and proofs used in A-LEMS.

---

## 📐 Core Energy Equations

### RAPL Counter Reading

$$E_{domain}(t) = \text{value}_t \quad [\mu J]$$

### Energy Difference

$$\Delta E_{domain} = E_{domain}(t_2) - E_{domain}(t_1)$$

### Average Power

$$P_{avg} = \frac{\Delta E_{domain}}{\Delta t} = \frac{E_{domain}(t_2) - E_{domain}(t_1)}{t_2 - t_1}$$

---

## 🧮 Workload Isolation

### Total Energy Decomposition

$$E_{total} = E_{workload} + E_{idle} + E_{noise}$$

### Baseline Subtraction

$$E_{workload} = E_{total} - P_{idle} \cdot \Delta t$$

### With Confidence Interval

$$E_{workload} = E_{total} - (\mu_{idle} - 2\sigma_{idle}) \cdot \Delta t$$

---

## 🔍 Orchestration Tax Derivation

### Step 1: Workload Energy

$$E_{workload} = E_{pkg} - P_{idle,pkg}\Delta t$$

### Step 2: Reasoning Energy

$$E_{reasoning} = E_{core} - P_{idle,core}\Delta t$$

### Step 3: Tax Definition

$$E_{tax} = E_{workload} - E_{reasoning}$$

### Step 4: Substitution

$$
\begin{aligned}
E_{tax} &= (E_{pkg} - P_{idle,pkg}\Delta t) - (E_{core} - P_{idle,core}\Delta t) \\
&= (E_{pkg} - E_{core}) - (P_{idle,pkg} - P_{idle,core})\Delta t
\end{aligned}
$$

### Step 5: Package Decomposition

$$E_{pkg} = E_{core} + E_{uncore}$$

$$P_{idle,pkg} = P_{idle,core} + P_{idle,uncore}$$

### Step 6: Final Form

$$
\begin{aligned}
E_{tax} &= E_{uncore} - P_{idle,uncore}\Delta t \\
&= \text{(uncore energy)} - \text{(idle uncore energy)}
\end{aligned}
$$

---

## 🔄 C-State Energy Model

### C-State Power Levels

$$P_{Cx} = P_{static} + P_{dynamic}(f,V) \cdot \text{activity}_x$$

### Residency Calculation

$$t_{Cx} = \sum_{i=1}^{n} \Delta t_i \cdot \mathbb{1}_{state=Cx}$$

### Energy Saved

$$E_{saved} = \sum_{x} (P_{C0} - P_{Cx}) \cdot t_{Cx}$$

---

## 🌡️ Thermal Model

### Newton's Law of Cooling with Power Input

$$\frac{dT}{dt} = \frac{1}{C_{th}} \left(P(t) - \frac{T(t) - T_{amb}}{R_{th}}\right)$$

Where:
- $C_{th}$ = thermal capacitance (J/K)
- $R_{th}$ = thermal resistance (K/W)
- $T_{amb}$ = ambient temperature

### Discrete Time Solution

$$T_{i+1} = T_i + \frac{\Delta t}{C_{th}} \left(P_i - \frac{T_i - T_{amb}}{R_{th}}\right)$$

### Thermal Gradient

$$\nabla T = \frac{T_{max} - T_{start}}{t_{rise}}$$

---

## 📈 Statistical Formulas

### Sample Mean

$$\bar{x} = \frac{1}{n}\sum_{i=1}^{n} x_i$$

### Sample Standard Deviation

$$s = \sqrt{\frac{1}{n-1}\sum_{i=1}^{n} (x_i - \bar{x})^2}$$

### Standard Error

$$SE = \frac{s}{\sqrt{n}}$$

### Confidence Interval (95%)

$$CI = \bar{x} \pm t_{n-1, 0.975} \cdot \frac{s}{\sqrt{n}}$$

For large n ($n \geq 30$):

$$CI \approx \bar{x} \pm 1.96 \cdot \frac{s}{\sqrt{n}}$$

---

## 🎯 Sample Size Calculation

### For Desired Effect Size $\delta$

$$n = \frac{(z_{1-\alpha/2} + z_{1-\beta})^2 \sigma^2}{\delta^2}$$

Where:
- $\alpha$ = significance level (typically 0.05)
- $1-\beta$ = desired power (typically 0.8)
- $\sigma$ = estimated standard deviation

### Common Values

| Power | $\alpha = 0.05$ | $\alpha = 0.01$ |
|-------|-----------------|-----------------|
| 0.80 | $z = 2.80$ | $z = 3.42$ |
| 0.90 | $z = 3.24$ | $z = 3.86$ |
| 0.95 | $z = 3.60$ | $z = 4.22$ |

---

## 📊 Efficiency Metrics

### Energy per Instruction

$$EPI = \frac{E_{workload}}{\text{instructions}}$$

### Energy per Token

$$EPT = \frac{E_{workload}}{\text{total\_tokens}}$$

### Instructions per Token

$$IPT = \frac{\text{instructions}}{\text{total\_tokens}}$$

### Energy Efficiency Ratio

$$EER = \frac{\text{linear\_energy}}{\text{agentic\_energy}}$$

---

## 🧪 Error Analysis

### Measurement Uncertainty

$$\epsilon_{RAPL} = \pm 1 \mu J$$

$$\epsilon_t = \pm 1 \text{ ns}$$

### Combined Relative Error

$$\frac{\Delta P}{P} = \sqrt{\left(\frac{\epsilon_{RAPL}}{E}\right)^2 + \left(\frac{\epsilon_t}{\Delta t}\right)^2}$$

### Error Propagation in Tax Calculation

$$
\begin{aligned}
\sigma_{tax}^2 &= \sigma_{workload}^2 + \sigma_{reasoning}^2 \\
&= \left(\frac{\partial tax}{\partial workload}\right)^2 \sigma_{workload}^2 + \left(\frac{\partial tax}{\partial reasoning}\right)^2 \sigma_{reasoning}^2 \\
&= \sigma_{workload}^2 + \sigma_{reasoning}^2
\end{aligned}
$$

---

## 🔬 Advanced Metrics

### Thermal-Energy Coupling Coefficient

$$\kappa = \frac{\Delta P_{leak}}{\Delta T} \quad [W/K]$$

### C-State Transition Latency

$$L_{Cx \to C0} = t_{wake} - t_{request}$$

### Orchestration Overhead Index

$$OOI = \frac{E_{agentic}}{E_{linear}} \cdot \left(1 + \frac{t_{planning} + t_{synthesis}}{t_{execution}}\right)$$

---

## 📚 References

1. Papoulis, A. (1991). "Probability, Random Variables and Stochastic Processes"
2. Taylor, J. R. (1997). "An Introduction to Error Analysis"
3. Hennessy, J. L., & Patterson, D. A. (2017). "Computer Architecture: A Quantitative Approach"