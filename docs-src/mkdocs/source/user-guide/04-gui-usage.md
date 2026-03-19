# Using the GUI Dashboard

This guide explains how to use the A-LEMS web-based GUI for experiment monitoring, visualization, and analysis.

---

## 🚀 Launching the GUI

```bash
# From project root with virtualenv activated
streamlit run streamlit_app.py
```

**Expected output:**

```
You can now view your Streamlit app in your browser.

Local URL: http://localhost:8501
Network URL: http://192.168.0.166:8501
```

> **Note:** CORS warnings are harmless and can be ignored.

---

## 🖥️ GUI Overview

The A-LEMS dashboard is organized with a sidebar navigation menu containing:

| Section | Pages |
|---------|-------|
| **Overview** | Dashboard home |
| **Execute Run** | Run new experiments |
| **Experiments** | Browse past experiments |
| **Energy** | Energy consumption analysis |
| **Sustainability** | Carbon, water, methane metrics |
| **Tax** | Orchestration tax analysis |
| **Run Explorer** | Deep dive into individual runs |
| **SQL Query** | Custom database queries |

---

## 📋 Navigation

### Sidebar Sections

| Section | Pages | Purpose |
|---------|-------|---------|
| **Overview** | Overview | Dashboard home |
| **Experiment Control** | Execute Run, Experiments, Settings | Run and manage experiments |
| **Energy & Compute** | Energy, Domains, Sustainability | Visualize metrics |
| **Orchestration** | Tax, Agentic vs Linear | Tax analysis |
| **System Behavior** | CPU, Scheduler | Hardware telemetry |
| **Exploration** | Run Explorer, Sessions | Data exploration |
| **Research** | Research Insights | Analysis tools |

---

## 🎯 Key Pages

### 1. Overview Page

Dashboard home showing:

- Recent experiments
- System status
- Quick stats (total runs, energy saved, carbon avoided)
- Latest results

### 2. Execute Run Page

Run experiments directly from the GUI with a simple form:

| Field | Options | Description |
|-------|---------|-------------|
| **Task** | `gsm8k_basic` (default) | Select from 16+ predefined tasks |
| **Provider** | `local` / `cloud` | Local TinyLlama or cloud (Groq/OpenRouter) |
| **Repetitions** | `3` (configurable) | Number of runs for statistical significance |
| **Optimizer** | Checkbox | Enable runtime optimization (experimental) |

**Controls:**
- `🚀 RUN EXPERIMENT` - Start the experiment

**Live Progress Display:**

```
Progress: ████████░░░░ 4/8 runs completed
```

**Real-time Results:**

| Metric | Value |
|--------|-------|
| Linear Energy | 1.2 J |
| Agentic Energy | 2.6 J |
| Orchestration Tax | 2.2x |

### 3. Experiments Page

Browse and filter all experiments:

- **Filter by:** Task, provider, date, status
- **Sort by:** ID, date, energy
- **Actions:** View details, compare, export
- **Statistics:** Summary of selected experiments

### 4. Energy Page

Visualize energy consumption:

- **Bar charts:** Linear vs Agentic comparison
- **Line charts:** Energy over time
- **Distribution:** Histogram of energy values
- **Raw data:** Sample-level view

### 5. Tax Page

Orchestration tax analysis with statistical summaries:

| Metric | Value | 95% Confidence Interval |
|--------|-------|------------------------|
| **Mean Tax** | 12.5x | [10.2x, 14.8x] |
| **Median Tax** | 11.8x | - |
| **Std Deviation** | 2.3x | - |
| **Min/Max** | 6.2x / 18.9x | - |

### 6. Sustainability Page

Environmental impact metrics:

- Carbon footprint by experiment
- Water usage
- Methane emissions
- Comparison to everyday activities
- Country-specific grid intensity

### 7. Run Explorer Page

Deep dive into individual runs:

- Select run ID
- View all 80+ metrics
- Plot energy curves
- Export raw data
- Compare with other runs

### 8. SQL Query Page

Advanced users can run custom SQL:

```sql
SELECT 
    run_id,
    workflow_type,
    dynamic_energy_uj/1e6 as energy_j
FROM runs
WHERE exp_id = 185
ORDER BY run_id;
```

Results exportable to CSV.

---

## 📊 Interactive Visualizations

### Charts Support:

| Feature | Action |
|---------|--------|
| **Zoom/Pan** | Click and drag |
| **Hover** | See exact values |
| **Legend** | Toggle series |
| **Download** | Save as PNG |
| **Reset** | Return to original view |

### Chart Types:

| Chart | Use For |
|-------|---------|
| **Bar** | Comparisons |
| **Line** | Trends over time |
| **Scatter** | Correlations |
| **Histogram** | Distributions |
| **Box plot** | Statistical spread |

---

## 📄 Generating PDF Reports

1. Navigate to **Session Analysis** page
2. Select an experiment from the list
3. Click **"Generate PDF Report"**
4. Report downloads automatically

**Report contains:**

- Experiment metadata
- Energy comparison charts
- Orchestration tax analysis
- Thermal profiles
- CPU and scheduler metrics
- Sustainability impact
- RAPL domain breakdown
- Hardware and software fingerprints

---

## 🔍 Searching and Filtering

### Experiments Page Filters

| Filter | Options |
|--------|---------|
| **Task** | All tasks, specific task |
| **Provider** | All, cloud, local |
| **Date Range** | Today, week, month, custom |
| **Status** | Completed, running, failed |
| **Energy Range** | Min-max slider |

### Run Explorer Filters

- Run ID range
- Workflow type
- Energy threshold
- Tax threshold
- Date/time

---

## 📈 Custom Queries

The SQL Query page offers:

- **Auto-complete:** Table and column names
- **Syntax highlighting:** SQL keywords
- **Query history:** Recent queries
- **Export:** CSV, JSON, Excel
- **Visualize:** Plot results directly

### Example Queries

**Top 10 highest energy runs:**

```sql
SELECT run_id, workflow_type, dynamic_energy_uj/1e6 as J
FROM runs
ORDER BY dynamic_energy_uj DESC
LIMIT 10;
```

**Average tax by task:**

```sql
SELECT 
    e.task_name,
    AVG(ots.tax_percent) as avg_tax
FROM orchestration_tax_summary ots
JOIN runs r ON ots.linear_run_id = r.run_id
JOIN experiments e ON r.exp_id = e.exp_id
GROUP BY e.task_name;
```

---

## 🎨 Theme and Appearance

- **Dark theme** by default for reduced eye strain
- **Color coding:**
  - 🟢 Linear: Green (`#22c55e`)
  - 🔴 Agentic: Red (`#ef4444`)
  - 🔵 Tax/Other: Blue (`#3b82f6`)
- **Responsive design** works on mobile/tablet

---

## ⚡ Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+Enter` | Run query (SQL page) |
| `Ctrl+S` | Save current view |
| `Ctrl+E` | Export data |
| `Ctrl+P` | Print/PDF |
| `Esc` | Clear selection |

---

## 🔧 Configuration

### Settings Page

- **Database path:** Change SQLite location
- **Refresh interval:** How often to update
- **Chart defaults:** Colors, sizes
- **Export format:** CSV, JSON, Excel
- **Theme:** Dark/Light toggle

---

## 🐛 Troubleshooting GUI

| Issue | Solution |
|-------|----------|
| GUI won't start | `pip install streamlit` |
| Blank page | Clear browser cache |
| Charts empty | Check database path |
| Slow loading | Add database indices |
| CORS warnings | Safe to ignore |
| PDF not generating | `pip install reportlab kaleido` |

---

## ✅ Next Steps

- [Generate your first report](05-generating-reports.md)
- [Explore run data](01-running.md)
- [Run batch experiments](03-batch-experiments.md)