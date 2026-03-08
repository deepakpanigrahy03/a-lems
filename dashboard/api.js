/* ═══════════════════════════════════════════════════════════
   A-LEMS  —  api.js
   All backend communication lives here.
   Change BASE_URL once to point at any server.
   ═══════════════════════════════════════════════════════════ */

// ── CONFIG ────────────────────────────────────────────────────
// Empty string = same origin (server.py serving dashboard).
// Set to 'http://192.168.x.x:8765' for direct network access.
// SSH tunnel:  ssh -L 8765:localhost:8765 user@host  → keep empty.
const BASE_URL = '';

// WebSocket base derived automatically
const WS_BASE = (() => {
  if (BASE_URL) return BASE_URL.replace(/^http/, 'ws');
  return `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}`;
})();

// ── LOW-LEVEL HELPERS ─────────────────────────────────────────
async function get(path) {
  const res = await fetch(BASE_URL + path);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} — ${path}`);
  return res.json();
}

async function post(path, body) {
  const res = await fetch(BASE_URL + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return res.json();
}

function openWS(path) {
  return new WebSocket(WS_BASE + path);
}

// ── PUBLIC API ────────────────────────────────────────────────
const API = {

  // ── System ─────────────────────────────────────────────────
  capabilities: ()    => get('/api/capabilities'),
  health:       ()    => get('/health'),

  // ── Experiments ────────────────────────────────────────────
  experiments:  ()    => get('/api/experiments'),
  experiment:   (id)  => get(`/api/experiments/${id}`),

  // ── Runs ───────────────────────────────────────────────────
  // Returns ml_features view rows — all derived metrics flat
  runs: (filters = {}) => {
    const p = new URLSearchParams();
    if (filters.exp_id)   p.set('exp_id',   filters.exp_id);
    if (filters.workflow) p.set('workflow',  filters.workflow);
    if (filters.provider) p.set('provider',  filters.provider);
    if (filters.country)  p.set('country',   filters.country);
    if (filters.limit)    p.set('limit',     filters.limit);
    const qs = p.toString();
    return get('/api/runs' + (qs ? '?' + qs : ''));
  },
  run:          (id)  => get(`/api/runs/${id}`),

  // ── Timeseries samples ─────────────────────────────────────
  energySamples:   (runId) => get(`/api/runs/${runId}/samples/energy`),
  cpuSamples:      (runId) => get(`/api/runs/${runId}/samples/cpu`),
  irqSamples:      (runId) => get(`/api/runs/${runId}/samples/interrupts`),
  events:          (runId) => get(`/api/runs/${runId}/events`),

  // ── Analysis ───────────────────────────────────────────────
  overview:  () => get('/api/analysis/overview'),
  tax:       () => get('/api/analysis/tax'),
  domains:   () => get('/api/analysis/domains'),
  cstates:   () => get('/api/analysis/cstates'),
  anomalies: () => get('/api/analysis/anomalies'),
  baselines: () => get('/api/analysis/baselines'),

  // ── Execute ────────────────────────────────────────────────
  execute: (payload) => post('/api/execute', payload),
  runStatus: (expId) => get(`/api/execute/status/${expId}`),

  // ── WebSocket for live run streaming ──────────────────────
  liveRunWS: (expId) => openWS(`/ws/run/${expId}`),
};

// ── APP STATE — single source of truth ───────────────────────
// All pages read from STATE; boot() populates it once.
const STATE = {
  runs:        [],   // ml_features rows
  exps:        [],   // experiments with aggregates
  tax:         [],   // orchestration_tax_summary joined
  domains:     [],   // orchestration_analysis view
  cstates:     [],   // cpu_samples aggregate by provider/workflow
  anomalies:   [],   // flagged outlier runs
  overview:    {},   // single-row aggregate
  caps:        {},   // {harness_available, mode, db_path}
  loaded:      false,
  error:       null,
};

// Derived helpers used across pages
const D = {
  linear:  () => STATE.runs.filter(r => r.workflow_type === 'linear'),
  agentic: () => STATE.runs.filter(r => r.workflow_type === 'agentic'),
  byProvider: (p) => STATE.runs.filter(r => r.provider === p),

  avgLinearJ:  () => avg(D.linear().map(r  => +(r.energy_j || 0))),
  avgAgenticJ: () => avg(D.agentic().map(r => +(r.energy_j || 0))),
  taxMultiple: () => {
    const l = D.avgLinearJ(), a = D.avgAgenticJ();
    return l > 0 ? a / l : 0;
  },

  // phase times (agentic only)
  avgPlanMs:  () => avg(D.agentic().filter(r => r.planning_time_ms).map(r => +(r.planning_time_ms  || 0))),
  avgExecMs:  () => avg(D.agentic().filter(r => r.execution_time_ms).map(r => +(r.execution_time_ms || 0))),
  avgSynthMs: () => avg(D.agentic().filter(r => r.synthesis_time_ms).map(r => +(r.synthesis_time_ms || 0))),
  phasePcts:  () => {
    const plan = D.avgPlanMs(), exec = D.avgExecMs(), synth = D.avgSynthMs();
    const total = plan + exec + synth || 1;
    return {
      plan:  +(plan  / total * 100).toFixed(1),
      exec:  +(exec  / total * 100).toFixed(1),
      synth: +(synth / total * 100).toFixed(1),
    };
  },
};

// ── MATH UTILS ────────────────────────────────────────────────
function avg(arr) {
  const clean = arr.filter(v => v != null && !isNaN(v));
  return clean.length ? clean.reduce((s, v) => s + v, 0) / clean.length : 0;
}
function maxVal(arr) { return arr.length ? Math.max(...arr) : 0; }
function minVal(arr) { return arr.length ? Math.min(...arr) : 0; }

// ── FORMAT UTILS ─────────────────────────────────────────────
const FMT = {
  j:   (v, d = 3) => v != null ? (+v).toFixed(d) + 'J'  : '—',
  ms:  (v, d = 0) => v != null ? (+v).toFixed(d) + 'ms' : '—',
  pct: (v, d = 1) => v != null ? (+v).toFixed(d) + '%'  : '—',
  num: (v, d = 2) => v != null ? (+v).toFixed(d)         : '—',
  k:   (v)        => v != null ? (+v).toLocaleString()    : '—',
  mult:(v, d = 1) => v != null ? (+v).toFixed(d) + '×'   : '—',
  mg:  (v, d = 3) => v != null ? (+v).toFixed(d) + 'mg'  : '—',
};
