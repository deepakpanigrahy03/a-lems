#!/bin/bash

mkdir -p diagrams

echo "Generating A-LEMS research diagrams..."

############################
# 1 SYSTEM ARCHITECTURE
############################

cat <<EOF > diagrams/system_architecture.dot
digraph G {

rankdir=TB;
node [shape=box style=filled color=lightblue];

User;
Dashboard;
ExperimentRunner;
Optimizer;
EnergyReaders;
Database;
Sustainability;
Insights;

User -> Dashboard;
Dashboard -> ExperimentRunner;
ExperimentRunner -> Optimizer;
ExperimentRunner -> EnergyReaders;
EnergyReaders -> Database;
Database -> Sustainability;
Database -> Insights;

}
EOF

dot -Tsvg diagrams/system_architecture.dot -o diagrams/system_architecture.svg


############################
# 2 EXPERIMENT PIPELINE
############################

cat <<EOF > diagrams/experiment_pipeline.dot
digraph G {

rankdir=LR;
node [shape=box style=filled color=lightgreen];

Query;
Optimizer;
Executor;
Measurement;
Metrics;
Database;

Query -> Optimizer;
Optimizer -> Executor;
Executor -> Measurement;
Measurement -> Metrics;
Metrics -> Database;

}
EOF

dot -Tsvg diagrams/experiment_pipeline.dot -o diagrams/experiment_pipeline.svg


############################
# 3 ENERGY MEASUREMENT PIPELINE
############################

cat <<EOF > diagrams/energy_pipeline.dot
digraph G {

rankdir=LR;
node [shape=box style=filled color=orange];

RAPL;
Perf;
Turbostat;
Telemetry;
EnergyEngine;
Database;

RAPL -> Telemetry;
Perf -> Telemetry;
Turbostat -> Telemetry;

Telemetry -> EnergyEngine;
EnergyEngine -> Database;

}
EOF

dot -Tsvg diagrams/energy_pipeline.dot -o diagrams/energy_pipeline.svg


echo "Diagrams generated in diagrams/"
