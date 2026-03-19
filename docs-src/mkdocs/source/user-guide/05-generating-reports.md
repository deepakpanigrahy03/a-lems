# Generating PDF Reports

This guide explains how to generate publication-quality PDF reports from your experiment data.

---

## 📄 Report Overview

A-LEMS generates comprehensive PDF reports containing:

- **Experiment metadata** - Task, provider, date, duration
- **Energy analysis** - Linear vs Agentic comparison
- **Orchestration tax** - Overhead calculation
- **Thermal profiles** - Temperature over time
- **CPU metrics** - Frequency, C-states, IPC
- **Scheduler metrics** - Context switches, interrupts
- **Sustainability impact** - Carbon, water, methane
- **Hardware fingerprint** - Complete system specs
- **Environment fingerprint** - Software versions, git commit

---

## 🚀 Generating Reports via GUI

### Step 1: Launch the GUI

```bash
streamlit run streamlit_app.py
```

### Step 2: Navigate to Session Analysis

- Click **Session Analysis** in sidebar
- All experiments are listed with:
  - Experiment ID
  - Task name
  - Provider
  - Date/time
  - Run count
  - Status

### Step 3: Select an Experiment

- Click on any experiment row
- Experiment details appear in main panel

### Step 4: Generate PDF

- Click **"Generate PDF Report"** button
- Report generates automatically
- Downloads as `experiment_XXX_report.pdf`

---

## 📊 Report Contents

### Cover Page

```
╔═══════════════════════════════════════════════════════╗
║                 A-LEMS Experiment Report              ║
╠═══════════════════════════════════════════════════════╣
║ Experiment ID: 185                                    ║
║ Task: GSM8K Arithmetic                                ║
║ Provider: cloud                                       ║
║ Date: 2026-03-12 10:41:13                             ║
║ Duration: 3.2 seconds                                 ║
║ Repetitions: 2                                        ║
╚═══════════════════════════════════════════════════════╝
```

### Energy Analysis Page

```
ENERGY CONSUMPTION
═══════════════════════════════════════════════════════

Linear Workflow
───────────────
  Run 1: 15.601 J
  Run 2:  9.870 J
  Mean:  12.736 ± 2.865 J

Agentic Workflow
────────────────
  Run 1: 27.358 J
  Run 2:  5.973 J
  Mean:  16.666 ± 10.693 J

Orchestration Tax
─────────────────
  Run 1: 1.75x (75% overhead)
  Run 2: 0.61x (-39% savings)
  Mean:  1.18x ± 0.57x
```

### Charts Page

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│   Energy Comparison Chart                            │
│                                                     │
│  30 ├───────•                                       │
│     │       •                                       │
│  20 │   •   •   •                                   │
│     │   •   •   •                                   │
│  10 │   •   •   •   •                               │
│     │   •   •   •   •                               │
│   0 └───•───•───•───•───•───•───•───•───•───•───• │
│       1   2   3   4   5   6   7   8   9  10         │
│                                                     │
│   Linear (green)   Agentic (red)                    │
└─────────────────────────────────────────────────────┘
```

### Thermal Profile Page

```
THERMAL ANALYSIS
═══════════════════════════════════════════════════════

Temperature Profile
───────────────────
  Start: 64.0°C
  Peak:  94.0°C
  Rise:  +30.0°C
  Rate:  2.5°C/s

Thermal Throttling
──────────────────
  Time > 90°C: 1.2s
  Throttle events: 0

┌─────────────────────────────────────────────────────┐
│                                                     │
│  100├───────────────────────•                       │
│     │                   •───•                       │
│   80│               •───•                           │
│     │           •───•                               │
│   60│       •───•                                   │
│     │   •───•                                       │
│   40└───•───•───•───•───•───•───•───•───•───•───• │
│       0   2   4   6   8  10  12  14  16  18  20     │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### Sustainability Page

```
SUSTAINABILITY IMPACT
═══════════════════════════════════════════════════════

Grid: US (389 gCO₂/kWh)

Carbon Footprint
────────────────
  Total: 12.4 g CO₂
  Equivalent to:
    • 0.6 Google searches
    • 885 WhatsApp messages
    • 0.0006 phone charges

Water Usage
───────────
  Total: 26.2 ml
  Equivalent to:
    • 0.13 baby feeds
    • 0.05 water bottles

Methane Emissions
─────────────────
  Total: 0.008 mg
  20-year CO₂e: 0.27 mg
  100-year CO₂e: 0.20 mg
```

### Hardware Fingerprint Page

```
HARDWARE CONFIGURATION
═══════════════════════════════════════════════════════

CPU
───
  Model: 11th Gen Intel i7-1165G7 @ 2.80GHz
  Cores: 4 physical, 8 logical
  Architecture: x86_64
  Vendor: GenuineIntel
  Family: 6, Model: 140, Stepping: 1
  Features: AVX2, AVX512, VMX

GPU
───
  Model: Iris Xe Graphics
  Driver: i915
  Count: 1

Memory
──────
  RAM: 6 GB
  Kernel: 6.17.0-14-generic
  Microcode: 0xbe

System
──────
  Manufacturer: LENOVO
  Product: 20TDCTO1WW
  Type: laptop
  Virtualization: none
```

### Software Environment Page

```
ENVIRONMENT CONFIGURATION
═══════════════════════════════════════════════════════

Python
──────
  Version: 3.13.7
  Implementation: CPython

OS
───
  Name: Linux
  Version: #14-Ubuntu SMP PREEMPT_DYNAMIC
  Kernel: 6.17.0-14-generic

Git
───
  Commit: 632c2981eab9d19e
  Branch: feature/per-pair-insertion
  Dirty: true

Dependencies
────────────
  numpy: 1.26.4
  torch: Not installed
  transformers: Not installed
```

---

## 🔧 Customizing Reports

### Report Configuration

Create `config/report.yaml`:

```yaml
# config/report.yaml
report:
  title: "A-LEMS Experiment Report"
  author: "Your Name"
  institution: "Your Institution"
  
  sections:
    - cover
    - energy
    - tax
    - thermal
    - cpu
    - sustainability
    - hardware
    - environment
  
  charts:
    energy_comparison: true
    tax_distribution: true
    thermal_profile: true
    cstate_residency: true
  
  sustainability:
    comparisons: true
    equivalents: true
```

### Command Line Report Generation

```bash
# Generate report for specific experiment
python scripts/tools/report_generator.py --exp-id 185

# Generate report for latest experiment
python scripts/tools/report_generator.py --latest

# Custom output location
python scripts/tools/report_generator.py --exp-id 185 --output ~/reports/

# Custom config
python scripts/tools/report_generator.py --exp-id 185 --config custom.yaml
```

---

## 📊 Batch Report Generation

Generate reports for multiple experiments:

```bash
#!/bin/bash
# generate_all_reports.sh

# Get all experiment IDs
EXPS=$(sqlite3 data/experiments.db "SELECT exp_id FROM experiments ORDER BY exp_id")

for exp in $EXPS; do
    echo "Generating report for experiment $exp..."
    python scripts/tools/report_generator.py --exp-id $exp
done
```

---

## 📈 Including in Papers

### Figure Export

Charts can be exported as:

- **PNG** (raster, 300 DPI)
- **SVG** (vector, editable)
- **PDF** (vector, publication-ready)

### Table Export

Data tables export as:

- **CSV** (raw data)
- **LaTeX** (publication-ready)
- **Markdown** (documentation)

---

## ✅ Next Steps

- [Explore the GUI](04-gui-usage.md)
- [Understand metrics](02-understanding-metrics.md)
- [Run batch experiments](03-batch-experiments.md)