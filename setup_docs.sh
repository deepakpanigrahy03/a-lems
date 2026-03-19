#!/bin/bash

echo "Resetting documentation..."

rm -rf alems-docs
mkdir alems-docs

cd alems-docs

pip install mkdocs mkdocs-material >/dev/null 2>&1

mkdocs new . >/dev/null 2>&1

mkdir -p docs/architecture
mkdir -p docs/experiments
mkdir -p docs/measurement
mkdir -p docs/dashboard
mkdir -p docs/research

cat <<EOT > mkdocs.yml
site_name: A-LEMS Documentation
site_description: Agentic LLM Energy Measurement System
site_author: Deepak Panigrahy

theme:
  name: material

nav:
  - Home: index.md

  - Architecture:
      - Overview: architecture/overview.md

  - Experiments:
      - Experiment Pipeline: experiments/pipeline.md

  - Measurement:
      - Energy Pipeline: measurement/energy_pipeline.md

  - Dashboard:
      - UI Overview: dashboard/overview.md

  - Research:
      - Orchestration Tax: research/orchestration_tax.md
EOT

cat <<EOT > docs/index.md
# A-LEMS

Agentic LLM Energy Measurement System

A research framework for measuring the energy footprint of AI workflows.
EOT

cat <<EOT > docs/architecture/overview.md
# System Architecture

A-LEMS consists of the following layers:

1. Experiment Execution
2. Energy Measurement
3. Database Storage
4. Sustainability Metrics
5. Research Insights
EOT

cat <<EOT > docs/experiments/pipeline.md
# Experiment Pipeline

Query → Optimizer → Executor → Measurement → Metrics → Database
EOT

cat <<EOT > docs/measurement/energy_pipeline.md
# Energy Measurement Pipeline

RAPL / Perf / Turbostat → Telemetry → Energy Engine → Database
EOT

cat <<EOT > docs/dashboard/overview.md
# Dashboard

The dashboard visualizes:

• experiment runs  
• energy consumption  
• sustainability metrics  
• orchestration overhead
EOT

cat <<EOT > docs/research/orchestration_tax.md
# Orchestration Tax

A-LEMS measures the energy overhead caused by AI orchestration layers.
EOT

echo "Documentation ready."

echo ""
echo "Run:"
echo "cd ~/mydrive/a-lems/alems-docs"
echo "mkdocs serve -a 127.0.0.1:8001"
