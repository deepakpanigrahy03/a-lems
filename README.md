⚡ A-LEMS — Agentic LLM Energy Measurement System
PhD Research Platform — Measuring the Orchestration Tax of Agentic AI

A-LEMS is a research-grade instrumentation framework that measures, analyzes, and optimizes energy consumption in AI workflows. It compares traditional linear LLM calls with modern agentic AI systems (planning, tool use, reflection) to quantify the "orchestration tax" — the energy overhead of agentic reasoning.

🏆 Breakthrough: Achieved 8.6% energy reduction in agentic workflows through phase-aware system optimization.

📋 Table of Contents
Research Problem

System Architecture

Hardware Measurement

AI Execution Framework

Sustainability Metrics

Optimizer Module — 8.6% Energy Reduction

Complete Feature Set

GUI Overview (18 Pages)

Quick Start

Database Schema

Project Structure

For Researchers (Using the GUI)

For Lab Owners (Running Live Experiments)

Security

Research Publications

Contributing

License

Citation

🔬 The Research Problem
Agentic AI systems spend 35-67% of execution time waiting (LLM API calls, between-step pauses). This creates an orchestration tax — energy consumed by uncore components (cache, memory controller, I/O) during wait states.

Key Insight: orchestration_tax = uncore_energy - idle_uncore_energy

A-LEMS provides the first empirical measurement of this phenomenon.

🏗️ System Architecture
text
┌─────────────────────────────────────────────────────────────────┐
│                     3-LAYER DATA MODEL                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Layer 1: RAW MEASUREMENT (Immutable)                            │
│  • RAPL energy counters (package, core, uncore, dram) @ 100Hz    │
│  • MSR registers (C-states, ring bus, wakeup latency)            │
│  • perf counters (instructions, cycles, cache misses)            │
│  • turbostat telemetry (frequency, temperature)                  │
│  • Scheduler metrics (context switches, interrupts)              │
│                                                                   │
│  Layer 2: BASELINE (Idle Reference)                               │
│  • Idle power per domain (mean - 2σ for cloud runs)              │
│  • System state (governor, turbo, process count)                 │
│                                                                   │
│  Layer 3: DERIVED (Corrected Metrics)                             │
│  • Workload energy = raw - baseline                               │
│  • Reasoning energy = core - idle_core                            │
│  • Orchestration tax = workload - reasoning                       │
│  • Instructions per cycle, cache miss rates                       │
│  • Thermal metrics, C-state residency                             │
└─────────────────────────────────────────────────────────────────┘
📊 Hardware Measurement Capabilities
Reader	Metrics	Sampling	Source
RAPLReader	Package, core, uncore, DRAM energy	100Hz	/sys/class/powercap/
MSRReader	C-state counters, ring bus frequency	Snapshots	Model-specific registers
PerfReader	Instructions, cycles, cache misses	Process-attached	perf_event_open
TurbostatReader	CPU frequency, C-state %, temperature	10Hz	turbostat subprocess
SchedulerMonitor	Context switches, interrupts, run queue	10Hz	/proc/stat
SensorReader	Thermal zone temperatures	1Hz	/sys/class/thermal
Total Features: 80+ metrics per run

🤖 AI Execution Framework
Linear Executor
python
# Single-pass LLM call (baseline)
response = llm.complete(prompt)
Agentic Executor
python
# Multi-step reasoning with orchestration
plan = planner.create(task)           # Phase 1: Planning
for step in plan.steps:                # Phase 2: Execution
    result = tool.execute(step)        
synthesis = synthesizer.generate()     # Phase 3: Synthesis
Orchestration Events Tracked:

Planning time, LLM wait states

Tool execution, between-step pauses

Synthesis phase

Reflection cycles

🌍 Sustainability Metrics
Metric	Coverage	Source
Carbon intensity	216 countries	Grid intensity 2026
Water consumption	Per kWh	Regional factors
Methane leakage	IPCC AR6 GWP	20/100-year values
⚡ Optimizer Module — 8.6% Energy Reduction
A-LEMS now includes a phase-aware system optimizer that dynamically adjusts CPU settings during wait states:

Phase Detection
python
if elapsed < 2:           # Planning phase
    governor = "powersave"
    c_states = "deep"
elif 2 <= elapsed < 5:    # LLM wait
    governor = "powersave" 
    interrupt_coalescing = "enabled"
elif 5 <= elapsed < 7:    # Tool execution
    governor = "performance"  # No optimization
Results
Metric	Before	After	Improvement
Linear Energy	101.32 J	97.70 J	3.6% ↓
Agentic Energy	874.45 J	799.54 J	8.6% ↓
Total Savings	-	74.9 J/run	Significant!
Key Insight: Optimizing wait states yields savings without affecting computation performance.

📈 Complete Feature Set
Core Features
✅ 100Hz RAPL sampling with perfect synchronization

✅ 3-layer immutable data model (scientific rigor)

✅ Per-pair insertion (clean experiment separation)

✅ Multi-provider support (Groq, OpenRouter, Ollama)

✅ Orchestration tax calculation (tax = uncore - idle_uncore)

✅ Phase-aware optimization (8.6% savings)

Analysis Features
✅ Energy breakdown (core vs uncore vs DRAM)

✅ Sustainability calculator (carbon, water, methane)

✅ Thermal telemetry (1Hz sampling, throttle detection)

✅ ML-ready dataset (80+ features)

✅ Statistical significance (mean, std, 95% CI)

🖥️ GUI Overview (18 Pages)
Page	Description
Overview	System summary, recent experiments
Energy Dashboard	Real-time + historical energy breakdown
CPU Monitor	Frequency, C-states, temperature
Scheduler	Context switches, interrupts, run queue
Agentic vs Linear	Side-by-side comparison
Tax Analysis	Orchestration tax with confidence intervals
Sustainability	Carbon, water, methane footprints
Thermal Telemetry	Temperature trends, throttle events
Research Insights	Key findings, anomaly detection
SQL Query	Direct database access
Experiments	Browse all runs
Live Lab	Connect to remote measurement laptop
Execute	Trigger new experiments
Settings	Configure system
Schema Docs	Database documentation
Anomalies	Detect outliers
Domains	RAPL domain breakdown
Query Analysis	SQL performance insights
🚀 Quick Start
Prerequisites
Intel CPU with RAPL support (6th gen+)

Linux kernel 5.4+

Python 3.9+

Installation
bash
git clone https://github.com/deepakpanigrahy03/a-lems.git
cd a-lems
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
Verify Hardware
bash
sudo python scripts/detect_hardware.py
Run Your First Experiment
bash
# Local test (no API keys needed)
python -m core.execution.tests.test_harness \
    --task-id simple \
    --repetitions 2 \
    --provider local \
    --save-db

# Cloud test (with Groq/OpenRouter)
python -m core.execution.tests.test_harness \
    --task-id capital \
    --repetitions 3 \
    --provider cloud \
    --save-db
Launch GUI
bash
streamlit run streamlit_app.py
💾 Database Schema (10 Tables)
sql
experiments            -- Experiment metadata
runs                   -- 80+ metrics per run
energy_samples         -- 100Hz RAPL samples
cpu_samples            -- 10Hz turbostat samples
interrupt_samples      -- 10Hz interrupt samples
thermal_samples        -- 1Hz temperature samples
orchestration_events   -- Step-by-step agent events
orchestration_tax_summary -- Per-pair tax storage
idle_baselines         -- Idle power references
hardware_config        -- System configuration
📁 Project Structure
text
a-lems/
├── streamlit_app.py              # GUI entry point
├── server.py                     # FastAPI backend for live mode
├── tunnel_agent.py               # Cloudflare tunnel manager
├── requirements.txt
├── config/
│   ├── hw_config.json            # Hardware detection
│   ├── models.json               # LLM provider configs
│   ├── tunnel.yaml                # Tunnel configuration
│   └── research_insights.yaml     # Key findings
├── core/
│   ├── energy_engine.py           # Main orchestrator
│   ├── readers/                   # Hardware readers
│   │   ├── rapl_reader.py
│   │   ├── msr_reader.py
│   │   ├── perf_reader.py
│   │   ├── turbostat_reader.py
│   │   ├── sensor_reader.py
│   │   └── scheduler_monitor.py
│   ├── models/                     # 3-layer data models
│   │   ├── raw_energy_measurement.py
│   │   ├── baseline_measurement.py
│   │   └── derived_energy_measurement.py
│   ├── analysis/
│   │   └── energy_analyzer.py      # Tax calculation
│   ├── sustainability/              # Carbon, water, methane
│   │   └── calculator.py
│   ├── execution/                   # AI executors
│   │   ├── linear.py
│   │   ├── agentic.py
│   │   └── optimizer/               # 8.6% savings module
│   └── database/                     # Storage layer
│       ├── schema.py
│       └── repositories/
├── gui/                              # Streamlit GUI (18 pages)
│   ├── connection.py                 # Tunnel connection
│   ├── sidebar.py                    # Navigation
│   ├── config.py
│   ├── db.py
│   ├── helpers.py
│   └── pages/                        # 18 individual pages
│       ├── overview.py
│       ├── energy.py
│       ├── cpu.py
│       ├── scheduler.py
│       ├── agentic_linear.py
│       ├── tax.py
│       ├── sustainability.py
│       ├── thermal.py
│       ├── research_insights.py
│       ├── sql_query.py
│       ├── experiments.py
│       ├── live.py
│       ├── execute.py
│       ├── settings.py
│       ├── schema_docs.py
│       ├── anomalies.py
│       ├── domains.py
│       └── query_analysis.py
├── data/
│   └── experiments.db              # SQLite database
└── scripts/
    └── detect_hardware.py
👨‍🔬 For Researchers (Using the GUI)
Two Modes
⚫ Offline (always on) — The full analysis dashboard runs 24/7 directly from this Space. All 18 pages work: energy breakdowns, sustainability metrics, SQL lab, research insights, anomaly detection, and more. Reads from the data/experiments.db file in this repo.

🟢 Live (when the lab is online) — When the lab owner runs tunnel_agent.py on their measurement laptop, researchers can connect via the sidebar, trigger real experiments remotely, and watch live telemetry stream back in real time.

Getting Started
Open this Space: https://huggingface.co/spaces/YOUR_USERNAME/a-lems

Everything works immediately in offline mode

To trigger live experiments, click ⚡ Connect to Live Lab in the sidebar

Enter the tunnel URL and token shared by the lab owner

The URL is permanent — bookmark it once and it works every time the lab is online

🖥️ For Lab Owners (Running Live Experiments)
One-Time Setup (~5 min)
1. Clone this Space and set up Git LFS
bash
git clone https://huggingface.co/spaces/YOUR_USERNAME/a-lems
cd a-lems
git lfs install
pip install -r requirements.txt pyyaml uvicorn fastapi
2. Install cloudflared
bash
wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb
sudo dpkg -i cloudflared-linux-amd64.deb
3. Create your permanent tunnel (one time only)
bash
cloudflared tunnel login          # opens browser → click Authorise
cloudflared tunnel create a-lems  # prints a UUID — copy it
4. Configure config/tunnel.yaml
yaml
tunnel_name: "a-lems"
tunnel_uuid: "paste-your-UUID-here"
token: "alems-choose-a-passphrase"   # researchers enter this in the sidebar
custom_hostname: ""                  # optional: map to your own domain
Generate a token:

bash
python -c "import secrets; print('alems-' + secrets.token_urlsafe(12))"
5. Go live
bash
source venv/bin/activate
python tunnel_agent.py
Output:

text
╔══════════════════════════════════════════════════════════════╗
║  🟢  A-LEMS is LIVE                                         ║
║                                                              ║
║  Permanent URL  →  https://xxxxxxxx-xxxx.cfargotunnel.com  ║
║  Token          →  alems-yourpassphrase                     ║
║                                                              ║
║  Share these once by email. URL never changes.              ║
╚══════════════════════════════════════════════════════════════╝
Share the URL and token once by email. Done forever.

Run in background (survives terminal close)
bash
nohup python tunnel_agent.py > logs/tunnel.log 2>&1 &
tail -f logs/tunnel.log
Update the database after experiments
bash
# On your laptop after running experiments:
git add data/experiments.db
git commit -m "db: +24 runs — factual_qa, reasoning tasks"
git push
# HF Space updates automatically within ~60 seconds
🔐 Security
What	How
View data	Public — anyone with the Space URL
Trigger experiments	Requires shared token
Revoke access	Change token in tunnel.yaml, restart tunnel_agent.py
Credentials	.cloudflared/*.json and .env are in .gitignore — never committed
🔬 Research Publications
2026 Findings
Orchestration Tax: Agentic workflows consume 3-8× more energy than linear

Wait-State Optimization: 8.6% energy reduction with zero code changes

Thermal-Energy Coupling: Agentic workflows create higher thermal stress

👥 Contributing
Contributions welcome! Areas needing help:

AMD/ARM hardware support

ML model training on 80+ features

Additional agent frameworks

PostgreSQL adapter

See CONTRIBUTING.md

📄 License
Apache License 2.0 — See LICENSE

📝 Citation
bibtex
@misc{alems2026,
  title  = {A-LEMS: Agentic LLM Energy Measurement System},
  author = {Panigrahy, Deepak},
  year   = {2026},
  url    = {https://github.com/deepakpanigrahy03/a-lems}
}
🙏 Acknowledgments
Intel RAPL documentation

Linux kernel community

Cloudflare for tunnel infrastructure

Hugging Face Spaces for hosting

Built with ⚡ for sustainable AI research
© 2026 Deepak Panigrahy — PhD Research