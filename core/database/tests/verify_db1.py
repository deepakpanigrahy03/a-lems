import sqlite3

conn = sqlite3.connect("data/experiments.db")

# Get latest experiment
exp = conn.execute(
    "SELECT exp_id, status, runs_completed FROM experiments ORDER BY exp_id DESC LIMIT 1"
).fetchone()
print(f"\n📊 Latest Experiment: {exp[0]} - Status: {exp[1]} - Runs: {exp[2]}/2")

# Check runs
runs = conn.execute(
    "SELECT run_id, workflow_type, run_number FROM runs WHERE exp_id = ?", (exp[0],)
).fetchall()
print(f"\n📋 Runs in experiment {exp[0]}:")
for run in runs:
    print(f"   Run {run[0]}: {run[1]} (rep {run[2]})")

# Sample counts
for table in ["energy_samples", "cpu_samples", "interrupt_samples", "thermal_samples"]:
    counts = conn.execute(
        f"SELECT run_id, COUNT(*) FROM {table} WHERE run_id IN (SELECT run_id FROM runs WHERE exp_id=?) GROUP BY run_id",
        (exp[0],),
    ).fetchall()
    print(f"\n📊 {table}:")
    for run_id, count in counts:
        print(f"   Run {run_id}: {count} samples")

# Tax summary
tax = conn.execute(
    "SELECT * FROM orchestration_tax_summary ORDER BY comparison_id DESC LIMIT 1"
).fetchone()
if tax:
    print(f"\n💰 Latest Tax Summary:")
    print(f"   Linear: {tax[2]/1e6:.3f}J, Agentic: {tax[3]/1e6:.3f}J")
    print(f"   Tax: {tax[5]:.1f}%")
