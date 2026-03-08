/* ═══════════════════════════════════════════════════════════
   A-LEMS  —  charts.js
   All Chart.js instances live here.
   Each page has a dedicated section.
   ═══════════════════════════════════════════════════════════ */

// ── CHART REGISTRY ───────────────────────────────────────────
// Keeps references so we can destroy before re-creating
const CHARTS = {};

function destroyChart(id) {
  if (CHARTS[id]) { CHARTS[id].destroy(); delete CHARTS[id]; }
}

function makeChart(id, type, data, opts = {}) {
  destroyChart(id);
  const el = document.getElementById(id);
  if (!el) return null;

  // Read current theme from document
  const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
  const GRID   = isDark ? 'rgba(30,45,69,.8)'   : 'rgba(200,215,230,.7)';
  const TICK   = isDark ? '#3d5570'              : '#8aa0b8';

  const BASE = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 350 },
    plugins: {
      legend: {
        labels: {
          font: { family: "'IBM Plex Mono', monospace", size: 9 },
          color: TICK,
          boxWidth: 10,
          padding: 10,
        },
      },
      tooltip: {
        backgroundColor: isDark ? '#1a2438' : '#fff',
        titleColor:  isDark ? '#e8f0f8' : '#0f1e30',
        bodyColor:   isDark ? '#7090b0' : '#5a7090',
        borderColor: isDark ? '#253652' : '#d8e2ed',
        borderWidth: 1,
        titleFont: { family: "'IBM Plex Mono', monospace", size: 10 },
        bodyFont:  { family: "'IBM Plex Mono', monospace", size: 9 },
        padding: 8,
      },
    },
    scales: {
      x: {
        grid:  { color: GRID },
        ticks: { color: TICK, font: { family: "'IBM Plex Mono', monospace", size: 9 } },
      },
      y: {
        grid:  { color: GRID },
        ticks: { color: TICK, font: { family: "'IBM Plex Mono', monospace", size: 9 } },
        beginAtZero: true,
      },
    },
  };

  // Deep-merge opts into BASE
  const merged = deepMerge(BASE, opts);
  CHARTS[id] = new Chart(el, { type, data, options: merged });
  return CHARTS[id];
}

// Deep merge utility (opts wins)
function deepMerge(base, over) {
  const out = { ...base };
  for (const k of Object.keys(over || {})) {
    if (over[k] && typeof over[k] === 'object' && !Array.isArray(over[k]) && base[k] && typeof base[k] === 'object') {
      out[k] = deepMerge(base[k], over[k]);
    } else {
      out[k] = over[k];
    }
  }
  return out;
}

// Rebuild all active charts when theme changes
function rebuildAllCharts() {
  // Just re-run the render for the active page
  const activePage = document.querySelector('.page.active');
  if (activePage) {
    const pageId = activePage.id.replace('p-', '');
    PAGE_RENDERERS[pageId]?.();
  }
}

// ── PALETTE ──────────────────────────────────────────────────
// Functions so they re-read CSS vars after theme change
function C(name, alpha = 1) {
  const map = {
    green:  alpha < 1 ? `rgba(34,197,94,${alpha})`   : '#22c55e',
    red:    alpha < 1 ? `rgba(239,68,68,${alpha})`   : '#ef4444',
    amber:  alpha < 1 ? `rgba(245,158,11,${alpha})`  : '#f59e0b',
    blue:   alpha < 1 ? `rgba(59,130,246,${alpha})`  : '#3b82f6',
    sky:    alpha < 1 ? `rgba(56,189,248,${alpha})`  : '#38bdf8',
    purple: alpha < 1 ? `rgba(167,139,250,${alpha})` : '#a78bfa',
    // light theme variants (same hue, slightly deeper)
    lgreen: alpha < 1 ? `rgba(22,163,74,${alpha})`   : '#16a34a',
    lred:   alpha < 1 ? `rgba(220,38,38,${alpha})`   : '#dc2626',
    lamber: alpha < 1 ? `rgba(217,119,6,${alpha})`   : '#d97706',
    lblue:  alpha < 1 ? `rgba(37,99,235,${alpha})`   : '#2563eb',
  };
  const isLight = document.documentElement.getAttribute('data-theme') === 'light';
  if (isLight) {
    if (name === 'green')  return map.lgreen;
    if (name === 'red')    return map.lred;
    if (name === 'amber')  return map.lamber;
    if (name === 'blue')   return map.lblue;
  }
  return map[name] ?? '#888';
}

// Convenience: solid + semi-transparent fill pair
const PAL = {
  linear:    () => ({ border: C('green'),  fill: C('green',  .12) }),
  agentic:   () => ({ border: C('red'),    fill: C('red',    .1)  }),
  tax:       () => ({ border: C('amber'),  fill: C('amber',  .1)  }),
  planning:  () => ({ border: C('amber'),  fill: C('amber',  .12) }),
  execution: () => ({ border: C('blue'),   fill: C('blue',   .1)  }),
  synthesis: () => ({ border: C('purple'), fill: C('purple', .1)  }),
  cloud:     () => ({ border: C('blue'),   fill: C('blue',   .12) }),
  local:     () => ({ border: C('red'),    fill: C('red',    .1)  }),
};

// ── SCALES PRESETS ───────────────────────────────────────────
function logY() {
  return { y: { type: 'logarithmic', ticks: { font: { family: "'IBM Plex Mono', monospace", size: 9 } } } };
}
function dualY(leftLabel, rightLabel) {
  return {
    y:  { position: 'left',  title: { display: true, text: leftLabel,  color: '#7090b0', font: { size: 9 } } },
    y1: { position: 'right', title: { display: true, text: rightLabel, color: '#7090b0', font: { size: 9 } }, grid: { drawOnChartArea: false } },
  };
}
function xyLabels(xLabel, yLabel) {
  return {
    x: { title: { display: true, text: xLabel, color: '#7090b0', font: { size: 9 } } },
    y: { title: { display: true, text: yLabel, color: '#7090b0', font: { size: 9 } } },
  };
}
function linearX() {
  return { x: { type: 'linear' } };
}
function noLegend() { return { plugins: { legend: { display: false } } }; }
function hiddenX()  { return { scales: { x: { ticks: { display: false } } } }; }

// ─────────────────────────────────────────────────────────────
// PAGE: OVERVIEW
// ─────────────────────────────────────────────────────────────
function renderOverviewCharts() {
  const linear  = D.linear();
  const agentic = D.agentic();

  // IPC vs Cache Miss scatter
  makeChart('c-ov-scatter', 'scatter', {
    datasets: [
      {
        label: 'Linear',
        data: linear.map(r => ({ x: +(r.cache_miss_rate || 0) * 100, y: +(r.ipc || 0) })),
        backgroundColor: C('green', .8),
        pointRadius: 5,
        pointHoverRadius: 7,
      },
      {
        label: 'Agentic',
        data: agentic.map(r => ({ x: +(r.cache_miss_rate || 0) * 100, y: +(r.ipc || 0) })),
        backgroundColor: C('red', .8),
        pointRadius: 5,
        pointHoverRadius: 7,
      },
    ],
  }, {
    scales: {
      ...xyLabels('Cache Miss %', 'IPC'),
    },
  });

  // Duration vs Energy scatter
  makeChart('c-ov-dur', 'scatter', {
    datasets: [
      {
        label: 'Linear',
        data: linear.filter(r => r.energy_j).map(r => ({ x: +(r.duration_ms || 0) / 1000, y: +(r.energy_j) })),
        backgroundColor: C('green', .8),
        pointRadius: 5,
      },
      {
        label: 'Agentic',
        data: agentic.filter(r => r.energy_j).map(r => ({ x: +(r.duration_ms || 0) / 1000, y: +(r.energy_j) })),
        backgroundColor: C('red', .8),
        pointRadius: 5,
      },
    ],
  }, {
    scales: xyLabels('Duration (s)', 'Energy (J)'),
  });
}

// ─────────────────────────────────────────────────────────────
// PAGE: ENERGY
// ─────────────────────────────────────────────────────────────
function renderEnergyCharts() {
  const linear  = D.linear();
  const agentic = D.agentic();

  // Energy per run sorted
  const sorted = [...STATE.runs].filter(r => r.energy_j).sort((a, b) => +a.energy_j - +b.energy_j);
  makeChart('c-en-sorted', 'bar', {
    labels: sorted.map(r => `R${r.run_id}`),
    datasets: [{
      label: 'Energy J',
      data: sorted.map(r => +r.energy_j),
      backgroundColor: sorted.map(r => r.workflow_type === 'agentic' ? C('red', .25) : C('green', .2)),
      borderColor:     sorted.map(r => r.workflow_type === 'agentic' ? C('red') : C('green')),
      borderWidth: 1,
    }],
  }, { ...noLegend(), scales: { ...hiddenX().scales, y: { type: 'logarithmic' } } });

  // Energy/token by segment
  const segs = [
    { label: 'Cloud Linear',   filter: r => r.provider !== 'local' && r.workflow_type === 'linear'  && r.energy_per_token > 0 },
    { label: 'Cloud Agentic',  filter: r => r.provider !== 'local' && r.workflow_type === 'agentic' && r.energy_per_token > 0 },
    { label: 'Local Linear',   filter: r => r.provider === 'local' && r.workflow_type === 'linear'  && r.energy_per_token > 0 },
    { label: 'Local Agentic',  filter: r => r.provider === 'local' && r.workflow_type === 'agentic' && r.energy_per_token > 0 },
  ];
  const colors = [PAL.cloud(), PAL.agentic(), PAL.local(), { border: C('purple'), fill: C('purple', .1) }];
  makeChart('c-en-ept', 'bar', {
    labels: segs.map(s => s.label),
    datasets: [{
      label: 'J / token',
      data: segs.map(s => avg(STATE.runs.filter(s.filter).map(r => +r.energy_per_token))),
      backgroundColor: colors.map(c => c.fill),
      borderColor:     colors.map(c => c.border),
      borderWidth: 1,
    }],
  }, { scales: { y: { type: 'logarithmic' } } });

  // Carbon by group
  const groups = {};
  STATE.runs.forEach(r => {
    if (!r.carbon_g) return;
    const k = `${r.provider}·${r.country_code || '?'}`;
    (groups[k] = groups[k] || []).push(+(r.carbon_g) * 1000);
  });
  const gKeys = Object.keys(groups);
  makeChart('c-en-carbon', 'bar', {
    labels: gKeys,
    datasets: [{
      label: 'avg carbon mg CO₂e',
      data: gKeys.map(k => avg(groups[k])),
      backgroundColor: gKeys.map(k => k.includes('IN') ? C('amber', .2) : k.includes('local') ? C('red', .2) : C('green', .2)),
      borderColor:     gKeys.map(k => k.includes('IN') ? C('amber') : k.includes('local') ? C('red') : C('green')),
      borderWidth: 1,
    }],
  }, { scales: { y: { type: 'logarithmic' } } });

  // Energy vs API latency scatter
  makeChart('c-en-api', 'scatter', {
    datasets: [
      { label: 'Cloud', data: STATE.runs.filter(r => r.provider !== 'local' && r.api_latency_ms && r.energy_j).map(r => ({ x: +(r.api_latency_ms) / 1000, y: +(r.energy_j) })), backgroundColor: C('blue', .8), pointRadius: 4 },
      { label: 'Local', data: STATE.runs.filter(r => r.provider === 'local' && r.energy_j).map(r => ({ x: +(r.api_latency_ms || 0) / 1000, y: +(r.energy_j) })), backgroundColor: C('red', .8), pointRadius: 4, pointStyle: 'triangle' },
    ],
  }, { scales: { ...xyLabels('API Latency (s)', 'Energy J'), y: { type: 'logarithmic' } } });
}

// ─────────────────────────────────────────────────────────────
// PAGE: CPU & C-STATES
// ─────────────────────────────────────────────────────────────
function renderCpuCharts() {
  const linear  = D.linear();
  const agentic = D.agentic();

  // IPC histogram
  const bins = [0.4, 0.6, 0.8, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0];
  const binFn = (arr) => {
    const counts = new Array(bins.length - 1).fill(0);
    arr.forEach(r => {
      for (let i = 0; i < bins.length - 1; i++) {
        if ((+(r.ipc || 0)) >= bins[i] && (+(r.ipc || 0)) < bins[i + 1]) { counts[i]++; break; }
      }
    });
    return counts;
  };
  makeChart('c-cpu-ipc', 'bar', {
    labels: bins.slice(0, -1).map(v => v.toFixed(1)),
    datasets: [
      { label: 'Linear',  data: binFn(linear),  backgroundColor: C('green', .2), borderColor: C('green'), borderWidth: 1 },
      { label: 'Agentic', data: binFn(agentic), backgroundColor: C('red',   .2), borderColor: C('red'),   borderWidth: 1 },
    ],
  }, {});

  // IPC vs frequency
  makeChart('c-cpu-ipc-freq', 'scatter', {
    datasets: [
      { label: 'Linear',  data: linear.filter(r => r.frequency_mhz).map(r => ({ x: +(r.frequency_mhz) / 1000, y: +(r.ipc || 0) })),  backgroundColor: C('green', .8), pointRadius: 4 },
      { label: 'Agentic', data: agentic.filter(r => r.frequency_mhz).map(r => ({ x: +(r.frequency_mhz) / 1000, y: +(r.ipc || 0) })), backgroundColor: C('red',   .8), pointRadius: 4 },
    ],
  }, { scales: xyLabels('Freq (GHz)', 'IPC') });

  // Thermal delta
  const thermalRuns = STATE.runs.filter(r => r.thermal_delta_c > 0).sort((a, b) => +a.thermal_delta_c - +b.thermal_delta_c);
  if (thermalRuns.length) {
    makeChart('c-cpu-thermal', 'bar', {
      labels: thermalRuns.map(r => `R${r.run_id}`),
      datasets: [{
        label: 'Thermal Δ°C',
        data: thermalRuns.map(r => +r.thermal_delta_c),
        backgroundColor: thermalRuns.map(r => +r.thermal_delta_c > 40 ? C('red', .2) : +r.thermal_delta_c > 25 ? C('amber', .2) : C('green', .2)),
        borderColor:     thermalRuns.map(r => +r.thermal_delta_c > 40 ? C('red')     : +r.thermal_delta_c > 25 ? C('amber')     : C('green')),
        borderWidth: 1,
      }],
    }, { ...noLegend(), ...hiddenX() });
  }

  // Cache miss vs energy
  makeChart('c-cpu-miss', 'scatter', {
    datasets: [
      { label: 'Linear',  data: linear.filter(r => r.energy_j).map(r => ({ x: +(r.cache_miss_rate || 0) * 100, y: +(r.energy_j) })),  backgroundColor: C('green', .8), pointRadius: 4 },
      { label: 'Agentic', data: agentic.filter(r => r.energy_j).map(r => ({ x: +(r.cache_miss_rate || 0) * 100, y: +(r.energy_j) })), backgroundColor: C('red',   .8), pointRadius: 4 },
    ],
  }, { scales: { ...xyLabels('Cache Miss %', 'Energy J'), y: { type: 'logarithmic' } } });
}

// ─────────────────────────────────────────────────────────────
// PAGE: SCHEDULER
// ─────────────────────────────────────────────────────────────
function renderSchedulerCharts() {
  const linear  = D.linear();
  const agentic = D.agentic();

  // Thread migrations vs duration
  makeChart('c-sched-mig', 'scatter', {
    datasets: [
      { label: 'Linear',  data: linear.filter(r => r.thread_migrations != null).map(r => ({ x: +(r.duration_ms || 0) / 1000, y: +r.thread_migrations })),  backgroundColor: C('green', .8), pointRadius: 4 },
      { label: 'Agentic', data: agentic.filter(r => r.thread_migrations != null).map(r => ({ x: +(r.duration_ms || 0) / 1000, y: +r.thread_migrations })), backgroundColor: C('red',   .8), pointRadius: 4 },
    ],
  }, { scales: xyLabels('Duration (s)', 'Migrations') });

  // Voluntary vs involuntary context switches
  const ctxTop = [...STATE.runs].filter(r => r.total_context_switches).sort((a, b) => +b.total_context_switches - +a.total_context_switches).slice(0, 20);
  makeChart('c-sched-ctx', 'bar', {
    labels: ctxTop.map(r => `R${r.run_id}`),
    datasets: [
      { label: 'Voluntary',   data: ctxTop.map(r => +(r.context_switches_voluntary   || 0)), backgroundColor: C('green', .2), borderColor: C('green'), borderWidth: 1, stack: 's' },
      { label: 'Involuntary', data: ctxTop.map(r => +(r.context_switches_involuntary || 0)), backgroundColor: C('red',   .2), borderColor: C('red'),   borderWidth: 1, stack: 's' },
    ],
  }, {});

  // IRQ rate
  const irqRuns = [...STATE.runs].filter(r => r.interrupt_rate).sort((a, b) => +a.interrupt_rate - +b.interrupt_rate);
  makeChart('c-sched-irq', 'bar', {
    labels: irqRuns.map(r => `R${r.run_id}`),
    datasets: [{
      label: 'IRQ/s',
      data: irqRuns.map(r => +r.interrupt_rate),
      backgroundColor: irqRuns.map(r => +r.interrupt_rate > 50000 ? C('red', .2) : +r.interrupt_rate > 10000 ? C('amber', .2) : C('green', .2)),
      borderColor:     irqRuns.map(r => +r.interrupt_rate > 50000 ? C('red')     : +r.interrupt_rate > 10000 ? C('amber')     : C('green')),
      borderWidth: 1,
    }],
  }, { ...noLegend(), scales: { ...hiddenX().scales, y: { type: 'logarithmic' } } });

  // Migrations → cache miss
  makeChart('c-sched-mig-miss', 'scatter', {
    datasets: [
      { label: 'Linear',  data: linear.filter(r => r.thread_migrations != null).map(r => ({ x: +r.thread_migrations, y: +(r.cache_miss_rate || 0) * 100 })),  backgroundColor: C('green', .8), pointRadius: 4 },
      { label: 'Agentic', data: agentic.filter(r => r.thread_migrations != null).map(r => ({ x: +r.thread_migrations, y: +(r.cache_miss_rate || 0) * 100 })), backgroundColor: C('red',   .8), pointRadius: 4 },
    ],
  }, { scales: xyLabels('Thread Migrations', 'Cache Miss %') });
}

// ─────────────────────────────────────────────────────────────
// PAGE: DOMAINS
// ─────────────────────────────────────────────────────────────
function renderDomainCharts() {
  const top = STATE.domains.slice(0, 30);
  if (!top.length) return;

  makeChart('c-dom-stacked', 'bar', {
    labels: top.map(d => `R${d.run_id}`),
    datasets: [
      { label: 'Core J',   data: top.map(d => +(d.core_energy_j   || 0)), backgroundColor: C('blue',   .2), borderColor: C('blue'),   borderWidth: 1, stack: 's' },
      { label: 'Uncore J', data: top.map(d => +(d.uncore_energy_j || 0)), backgroundColor: C('sky',    .2), borderColor: C('sky'),    borderWidth: 1, stack: 's' },
      { label: 'DRAM J',   data: top.map(d => +(d.dram_energy_j   || 0)), backgroundColor: C('purple', .2), borderColor: C('purple'), borderWidth: 1, stack: 's' },
    ],
  }, {});

  makeChart('c-dom-tax', 'bar', {
    labels: top.map(d => `R${d.run_id}`),
    datasets: [
      { label: 'Workload J', data: top.map(d => +(d.workload_energy_j      || 0)), backgroundColor: C('green', .2), borderColor: C('green'), borderWidth: 1, stack: 's' },
      { label: 'Tax J',      data: top.map(d => +(d.orchestration_tax_j || 0)), backgroundColor: C('red',   .2), borderColor: C('red'),   borderWidth: 1, stack: 's' },
    ],
  }, {});
}

// ─────────────────────────────────────────────────────────────
// PAGE: TAX
// ─────────────────────────────────────────────────────────────
function renderTaxCharts() {
  // Tax % distribution histogram
  const bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
  const counts = new Array(bins.length - 1).fill(0);
  STATE.tax.forEach(t => {
    const v = +(t.tax_percent || 0);
    for (let i = 0; i < bins.length - 1; i++) {
      if (v >= bins[i] && v < bins[i + 1]) { counts[i]++; break; }
    }
  });
  makeChart('c-tax-dist', 'bar', {
    labels: bins.slice(0, -1).map(v => `${v}–${v + 10}%`),
    datasets: [{ label: 'Pairs', data: counts, backgroundColor: C('blue', .2), borderColor: C('blue'), borderWidth: 1 }],
  }, { ...noLegend() });

  // Tax vs LLM calls
  makeChart('c-tax-calls', 'scatter', {
    datasets: [{
      label: 'Tax vs LLM calls',
      data: STATE.tax.filter(t => t.llm_calls).map(t => ({ x: +(t.llm_calls), y: +(t.tax_percent || 0) })),
      backgroundColor: C('amber', .8),
      pointRadius: 5,
    }],
  }, { scales: xyLabels('LLM Calls', 'Tax %') });
}

// ─────────────────────────────────────────────────────────────
// PAGE: ANOMALIES — no charts, just table+cards
// ─────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────
// PAGE: EXECUTE — live chart
// ─────────────────────────────────────────────────────────────
function initLiveChart() {
  makeChart('c-live', 'line', {
    datasets: [
      { label: 'Linear J',  data: [], borderColor: C('green'), borderWidth: 1.5, pointRadius: 0, fill: true, backgroundColor: C('green', .08), tension: .3, yAxisID: 'y' },
      { label: 'Agentic J', data: [], borderColor: C('red'),   borderWidth: 1.5, pointRadius: 0, fill: true, backgroundColor: C('red',   .06), tension: .3, yAxisID: 'y' },
    ],
  }, {
    animation: false,
    scales: {
      x:  { type: 'linear', ...xyLabels('elapsed ms', '').x },
      y:  { title: { display: true, text: 'Cumulative J', color: '#7090b0', font: { size: 9 } }, position: 'left' },
    },
  });
}

function pushLiveSample(workflow, elapsed_ms, pkg_j) {
  const ch = CHARTS['c-live'];
  if (!ch) return;
  const dsIdx = workflow === 'linear' ? 0 : 1;
  ch.data.datasets[dsIdx].data.push({ x: Math.round(elapsed_ms), y: pkg_j });
  // Keep last 600 points
  if (ch.data.datasets[dsIdx].data.length > 600) ch.data.datasets[dsIdx].data.shift();
  ch.update('none');
}

// ─────────────────────────────────────────────────────────────
// PAGE: SAMPLE EXPLORER — per-run timeseries
// ─────────────────────────────────────────────────────────────
function renderExplorerCharts(energySamples, cpuSamples, irqSamples) {
  // Power (watts derived from cumulative J)
  if (energySamples.length > 1) {
    makeChart('c-exp-power', 'line', {
      datasets: [
        { label: 'PKG W',    data: energySamples.slice(1).map(s => ({ x: Math.round(s.elapsed_ms), y: +(s.pkg_watts  || 0).toFixed(3) })), borderColor: C('blue'),   borderWidth: 1.5, pointRadius: 0, fill: true, backgroundColor: C('blue',   .08), tension: .3 },
        { label: 'Core W',   data: energySamples.slice(1).map(s => ({ x: Math.round(s.elapsed_ms), y: +(s.core_watts || 0).toFixed(3) })), borderColor: C('green'),  borderWidth: 1,   pointRadius: 0, fill: false, tension: .3 },
        { label: 'DRAM W',   data: energySamples.slice(1).map(s => ({ x: Math.round(s.elapsed_ms), y: +(s.dram_watts || 0).toFixed(3) })), borderColor: C('sky'),    borderWidth: 1,   pointRadius: 0, fill: false, tension: .3 },
      ],
    }, { scales: { ...linearX(), ...xyLabels('elapsed ms', 'Watts') } });
  }

  // IPC + CPU util dual-axis
  if (cpuSamples.length > 0) {
    makeChart('c-exp-ipc', 'line', {
      datasets: [
        { label: 'IPC',       data: cpuSamples.map(s => ({ x: Math.round(s.elapsed_ms), y: +(s.ipc             || 0) })), borderColor: C('green'), borderWidth: 1.5, pointRadius: 0, fill: false, tension: .3, yAxisID: 'y' },
        { label: 'CPU Util%', data: cpuSamples.map(s => ({ x: Math.round(s.elapsed_ms), y: +(s.cpu_util_percent || 0) })), borderColor: C('amber'), borderWidth: 1,   pointRadius: 0, fill: false, tension: .3, yAxisID: 'y1' },
      ],
    }, { scales: { ...linearX(), ...dualY('IPC', 'CPU %') } });

    // C-state residency
    makeChart('c-exp-cstate', 'line', {
      datasets: [
        { label: 'C6%', data: cpuSamples.map(s => ({ x: Math.round(s.elapsed_ms), y: +(s.c6_residency || 0) })), borderColor: C('green'), borderWidth: 1.5, pointRadius: 0, fill: true,  backgroundColor: C('green', .1), tension: .3 },
        { label: 'C2%', data: cpuSamples.map(s => ({ x: Math.round(s.elapsed_ms), y: +(s.c2_residency || 0) })), borderColor: C('sky'),   borderWidth: 1,   pointRadius: 0, fill: false, tension: .3 },
        { label: 'C1%', data: cpuSamples.map(s => ({ x: Math.round(s.elapsed_ms), y: +(s.c1_residency || 0) })), borderColor: C('blue'),  borderWidth: 1,   pointRadius: 0, fill: false, tension: .3 },
      ],
    }, { scales: { ...linearX(), ...xyLabels('elapsed ms', 'Residency %') } });
  }

  // IRQ
  if (irqSamples.length > 0) {
    makeChart('c-exp-irq', 'line', {
      datasets: [{ label: 'IRQ/s', data: irqSamples.map(s => ({ x: Math.round(s.elapsed_ms), y: +(s.interrupts_per_sec || 0) })), borderColor: C('red'), borderWidth: 1.5, pointRadius: 0, fill: true, backgroundColor: C('red', .08), tension: .3 }],
    }, { ...noLegend(), scales: { ...linearX(), ...xyLabels('elapsed ms', 'IRQ/s') } });
  }
}

// Map of page id → render function (used by rebuildAllCharts on theme change)
const PAGE_RENDERERS = {
  overview:  renderOverviewCharts,
  energy:    renderEnergyCharts,
  cpu:       renderCpuCharts,
  scheduler: renderSchedulerCharts,
  domains:   renderDomainCharts,
  tax:       renderTaxCharts,
};
