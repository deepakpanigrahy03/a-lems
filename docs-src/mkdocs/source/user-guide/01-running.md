# Running Experiments

This guide explains how to run experiments in A-LEMS, from simple single runs to complex batch experiments.

---

## 🚀 Quick Start

Run a simple experiment with one repetition:

```bash
python -m core.execution.tests.run_experiment \
    --tasks gsm8k_basic \
    --repetitions 1 \
    --providers local \
    --save-db
```

---

## 📋 Available Tasks

| ID | Name | Category | Level | Description |
|----|------|----------|-------|-------------|
| `gsm8k_basic` | GSM8K Arithmetic | reasoning | 1 | Grade school arithmetic |
| `gsm8k_multi_step` | Multi-step Arithmetic | reasoning | 2 | Multi-step math problems |
| `logical_reasoning` | Logical Deduction | reasoning | 2 | Deductive reasoning |
| `commonsense_reasoning` | Commonsense Reasoning | reasoning | 1 | Basic commonsense |
| `code_fibonacci` | Fibonacci Function | coding | 2 | Generate Python function |
| `code_sorting` | Sorting Algorithm | coding | 2 | Implement quicksort |
| `bug_fixing` | Bug Fixing | coding | 2 | Fix syntax errors |
| `factual_qa` | Factual Question | qa | 1 | Simple factual QA |
| `science_qa` | Science Question | qa | 1 | Basic science |
| `geography_qa` | Geography Question | qa | 1 | Geography knowledge |
| `news_summary` | News Summary | summarization | 1 | Summarize articles |
| `research_summary` | Research Summary | summarization | 2 | Academic abstracts |
| `sentiment_analysis` | Sentiment Analysis | classification | 1 | Classify sentiment |
| `topic_classification` | Topic Classification | classification | 1 | Classify topics |
| `entity_extraction` | Entity Extraction | extraction | 1 | Extract named entities |
| `keyword_extraction` | Keyword Extraction | extraction | 1 | Extract key terms |

### Task Categories

| Category | Description | Example Tasks |
|----------|-------------|---------------|
| **reasoning** | Math, logic, and commonsense problems | `gsm8k_basic`, `logical_reasoning` |
| **coding** | Code generation and debugging | `code_fibonacci`, `bug_fixing` |
| **qa** | Factual and knowledge-based questions | `factual_qa`, `science_qa` |
| **summarization** | Text summarization tasks | `news_summary`, `research_summary` |
| **classification** | Text classification | `sentiment_analysis`, `topic_classification` |
| **extraction** | Information extraction | `entity_extraction`, `keyword_extraction` |

### Level Meaning

| Level | Description |
|-------|-------------|
| **1** | Simple tasks, single-step, minimal tools |
| **2** | Complex tasks, multi-step, may use tools |
| **3** | Advanced tasks, multiple tools, synthesis |

**Total Tasks: 16**

---

## 🎯 Running Different Task Types

### Simple Task (Level 1)

```bash
python -m core.execution.tests.run_experiment \
    --tasks factual_qa \
    --repetitions 3 \
    --providers local \
    --save-db
```

### Complex Task (Level 2)

```bash
python -m core.execution.tests.run_experiment \
    --tasks gsm8k_multi_step \
    --repetitions 5 \
    --providers cloud \
    --save-db
```

---

## ☁️ Choosing Providers

### Local Provider (No API Key)

```bash
python -m core.execution.tests.run_experiment \
    --tasks gsm8k_basic \
    --providers local \
    --save-db
```

### Cloud Provider (Requires API Key)

```bash
# Set API key first
export GROQ_API_KEY="your-key-here"

python -m core.execution.tests.run_experiment \
    --tasks gsm8k_basic \
    --providers cloud \
    --save-db
```

### Multiple Providers

```bash
python -m core.execution.tests.run_experiment \
    --tasks gsm8k_basic \
    --providers cloud,local \
    --repetitions 3 \
    --save-db
```

---

## 🔁 Repetitions for Statistical Significance

| Repetitions | Purpose | When to Use |
|-------------|---------|-------------|
| 1 | Quick test | Debugging, verifying setup |
| 3-5 | Initial results | Exploratory analysis |
| 10-30 | Statistical significance | Research papers, final results |
| 100+ | Production benchmarking | Large-scale studies |

```bash
# 30 repetitions for statistical power
python -m core.execution.tests.run_experiment \
    --tasks gsm8k_basic \
    --repetitions 30 \
    --providers local \
    --save-db
```

---

## 🧪 Test Harness (Quick Testing)

For faster iteration during development:

```bash
python -m core.execution.tests.test_harness \
    --task-id gsm8k_basic \
    --repetitions 1 \
    --provider local \
    --verbose
```

**Differences from `run_experiment`:**

- ✅ Faster (less overhead)
- ✅ Shows real-time hardware telemetry
- ❌ Not for production results
- ❌ Limited batch capabilities

---

## 📊 Real-time Progress

While running, you'll see:

```
📊 Progress: 2/6 runs
  Rep 1/3
    Linear: 1.2043 J
    Agentic: 2.5945 J
    Tax: 2.15x
  ✅ Pair 1 saved (linear: 123, agentic: 124)
```

---

## ❄️ Cool-down Periods

Experiments include automatic cool-down between runs:

```bash
# Default 2 seconds
python -m core.execution.tests.run_experiment --tasks gsm8k_basic --repetitions 3

# Custom cool-down
python -m core.execution.tests.run_experiment --tasks gsm8k_basic --cool-down 5
```

---

## 🔧 Advanced Options

### Disable Warm-up

```bash
python -m core.execution.tests.run_experiment \
    --tasks gsm8k_basic \
    --no-warmup
```

### Specify Country for Carbon Intensity

```bash
python -m core.execution.tests.run_experiment \
    --tasks gsm8k_basic \
    --country IN  # India grid intensity
```

### Enable Optimizer (Experimental)

```bash
python -m core.execution.tests.run_experiment \
    --tasks gsm8k_basic \
    --optimizer
```

---

## 📝 Command Reference

| Option | Description | Example |
|--------|-------------|---------|
| `--tasks` | Comma-separated task IDs | `--tasks gsm8k_basic,factual_qa` |
| `--repetitions` | Number of repetitions | `--repetitions 10` |
| `--providers` | Comma-separated providers | `--providers cloud,local` |
| `--save-db` | Save results to database | `--save-db` |
| `--verbose` | Detailed output | `--verbose` |
| `--cool-down` | Seconds between runs | `--cool-down 5` |
| `--no-warmup` | Skip warm-up runs | `--no-warmup` |
| `--country` | Country code for carbon | `--country US` |
| `--optimizer` | Enable optimizer | `--optimizer` |
| `--list-tasks` | Show available tasks | `--list-tasks` |

---

## ⚠️ Common Issues

| Issue | Solution |
|-------|----------|
| No valid tasks selected | Check task ID with `--list-tasks` |
| API key not found | Set environment variable: `export GROQ_API_KEY="key"` |
| Permission denied | Run `sudo ./scripts/fix_permissions.sh` |
| Database locked | Wait or remove lock file |
| No baseline | First run measures automatically |

---

## ✅ Next Steps

- [View results in GUI](../user-guide/04-gui-usage.md)
- [Understand metrics](../user-guide/02-understanding-metrics.md)
- [Generate reports](../user-guide/05-generating-reports.md)