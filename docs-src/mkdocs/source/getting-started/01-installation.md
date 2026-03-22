# Installation Guide

Complete step-by-step setup for A-LEMS. This guide works on any Linux distribution with Intel 6th gen+ processors.

---

## 📋 Prerequisites

### System Requirements

| Requirement | Minimum | Recommended |
|------------|---------|-------------|
| **OS** | Any Linux distribution | Ubuntu 24.04 / Debian 12 |
| **CPU** | Intel 6th gen+ (RAPL support) | Intel 12th gen+ |
| **RAM** | 8 GB | 16 GB |
| **Storage** | 10 GB free | 20 GB free |
| **Python** | 3.10 | 3.12 |

> **Note:** A-LEMS uses Intel RAPL (Running Average Power Limit) for energy measurement. AMD processors are currently not supported.

---

### Automatic Hardware Detection

A-LEMS includes intelligent hardware detection that automatically:
- 🔍 Detects your CPU model and capabilities
- 🌡️ Maps thermal zones dynamically (not hardcoded)
- ⚡ Identifies available RAPL domains
- 🔧 Configures MSR access for your specific CPU
- 🖥️ Works across different Linux distributions

The system adapts to your hardware - no manual configuration needed!

---

### Distribution-Specific Package Installation

Choose your distribution:

<details>
<summary><b>🐧 Ubuntu / Debian</b></summary>

```bash
sudo apt update
sudo apt install -y \
    python3-pip \
    python3-venv \
    git \
    build-essential \
    linux-tools-common \
    linux-tools-generic \
    msr-tools \
    lm-sensors
```
</details>

<details>
<summary><b>📦 Fedora / RHEL / CentOS</b></summary>

```bash
sudo dnf install -y \
    python3-pip \
    python3-virtualenv \
    git \
    gcc \
    make \
    kernel-tools \
    msr-tools \
    lm_sensors
```
</details>

<details>
<summary><b>🏔️ Arch Linux / Manjaro</b></summary>

```bash
sudo pacman -S --noconfirm \
    python-pip \
    python-virtualenv \
    git \
    base-devel \
    linux-tools \
    msr-tools \
    lm_sensors
```
</details>

<details>
<summary><b>🔄 openSUSE</b></summary>

```bash
sudo zypper install -y \
    python3-pip \
    python3-virtualenv \
    git \
    gcc \
    make \
    kernel-tools \
    msr-tools \
    lm_sensors
```
</details>

---

## 🚀 Step 1: Clone Repository

```bash
git clone https://github.com/deepakpanigrahy03/a-lems.git
cd a-lems
```

---

## 🐍 Step 2: Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

*On some systems, use `python` instead of `python3`.*

---

## 📦 Step 3: Install Python Dependencies

```bash
# Core requirements (always needed)
pip install -r requirements.txt

# Optional: GUI dashboard (for web interface)
pip install -r requirements-gui.txt

# Optional: Developer tools (for contributors)
pip install -r requirements-tools.txt

# Install library for local LLM
pip install llama-cpp-python
```

---

## 🔧 Step 4: Fix Permissions

The `fix_permissions.sh` script grants necessary access to hardware interfaces:

```bash
sudo ./scripts/fix_permissions.sh
```

**What this does:**

- Grants read access to RAPL energy counters (`/sys/class/powercap/`)
- Allows MSR register access for C-state monitoring
- Enables turbostat for CPU frequency sampling
- Provides access to thermal sensors

*You only need to run this once after installation.*

---

## 🖥️ Step 5: Verify Installation

Run the hardware verification tool to check if everything is working:

```bash
python scripts/verify_hardware.py
```

*Expected output: All 8 checks should pass with ✅ indicators.*

---

## 🏗️ Step 6: Hardware Detection

A-LEMS automatically detects your hardware configuration:

```bash
# First run (requires sudo for MSR/turbostat access)
python scripts/detect_hardware.py --output config/hw_config.json --merge --verbose

# Fix permissions on generated config
sudo ./scripts/fix_permissions.sh
```

**What gets detected:**

- ✅ CPU model, cores, threads, flags (AVX2, AVX512)
- ✅ GPU model and driver
- ✅ RAPL energy domains (package, core, uncore, dram)
- ✅ Thermal zones (dynamic mapping, not hardcoded)
- ✅ MSR registers and C-state capabilities
- ✅ System manufacturer and type

The generated `config/hw_config.json` contains your complete hardware fingerprint.

---

## 🌍 Step 7: Environment Detection

Capture your software environment for reproducibility:

```bash
python scripts/detect_environment.py --verbose
```

**What gets tracked:**

- ✅ Python version and implementation
- ✅ Git commit hash and branch
- ✅ Dependency versions (numpy, torch, etc.)
- ✅ OS name and kernel version

This creates `config/environment.json` with a unique `env_hash` for your environment.

---
## 🏗️ Step 8: Measure baselines

Measure baseline MSR and idle power states before running experiments:

```bash
python -m core.utils.idle_baseline --duration 10 --samples 3
python scripts/measure_msr_baseline.py
```
**What it does:**

- ✅ Captures idle power consumption baseline
- ✅ Records MSR C-state baseline for accurate measurements
- ✅ Establishes reference for energy calculations

---

## ✅ Installation Complete!

Your A-LEMS installation is now ready. Next steps:

- 📘 [Quick Start Guide](04-quick-start.md) - Run your first experiment in 5 minutes
- 🔑 [Configuration Guide](03-model-config.md) - Set up API keys for cloud models
- 📊 [Understanding Metrics](../user-guide/02-understanding-metrics.md) - Learn what the numbers mean

---

## 🔄 Post-Installation Workflow

After installation, your daily workflow is simple:

```bash
cd a-lems
source venv/bin/activate

# Load API keys (if using cloud models)
cp core/.env.example core/.env
# Edit core/.env with your API keys
nano core/.env

# Test LLM (verify everything works)
python -m core.execution.tests.test_llm_setup --provider local --verbose
python -m core.execution.tests.test_llm_setup --provider cloud --verbose

# Run experiments
python -m core.execution.tests.run_experiment --tasks gsm8k_basic --repetitions 3
```

---

## ⚠️ Troubleshooting

| Issue | Likely Cause | Solution |
|-------|--------------|----------|
| Permission denied | Hardware access restricted | Run `sudo ./scripts/fix_permissions.sh` |
| `ModuleNotFoundError` | Virtual env not activated | `source venv/bin/activate` |
| RAPL not found | CPU doesn't support RAPL | Check: `cat /proc/cpuinfo \| grep rapl` |
| MSR access failed | msr module not loaded | `sudo modprobe msr` |
| Turbostat missing | linux-tools not installed | Install kernel-tools package for your distro |
| No thermal zones | Sensors not detected | Install lm-sensors and run `sudo sensors-detect` |
| GPU not detected | Missing drivers | Install appropriate GPU drivers |

### Still having issues?

Run the diagnostic tool:

```bash
python scripts/tools/issue_tracer.py
```

This will automatically check your system and suggest fixes.

---

> **Note:** A-LEMS is designed to work on any Linux system with Intel processors. The hardware detection automatically adapts to your specific configuration - no manual tweaking required!