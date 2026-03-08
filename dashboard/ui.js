/* ═══════════════════════════════════════════════════════════
   A-LEMS  —  ui.js
   All DOM rendering and page-level logic.
   Pure functions where possible — read from STATE, write to DOM.
   ═══════════════════════════════════════════════════════════ */

// ── THEME ─────────────────────────────────────────────────────
function initTheme() {
  const saved = localStorage.getItem('alems-theme') || 'dark';
  document.documentElement.setAttribute('data-theme', saved);
}
function toggleTheme() {
  const cur  = document.documentElement.getAttribute('data-theme');
  const next = cur === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('alems-theme', next);
  document.getElementById('theme-label').textContent = next === 'dark' ? 'Dark' : 'Light';
  // Rebuild charts with new palette
  rebuildAllCharts();
}

// ── NAVIGATION ────────────────────────────────────────────────
function nav(el, pageId) {
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  el.classList.add('active');
  const page = document.getElementById('p-' + pageId);
  if (page) page.classList.add('active');
}

// ── TOPBAR ────────────────────────────────────────────────────
function updateTopbar() {
  const ov = STATE.overview;
  const taxMult = D.taxMultiple();

  setText('tb-exps', ov.total_experiments || STATE.exps.length || '—');
  setText('tb-runs', ov.total_runs        || STATE.runs.length || '—');
  setText('tb-lin',  FMT.j(D.avgLinearJ(),  3));
  setText('tb-age',  FMT.j(D.avgAgenticJ(), 3));
  setText('tb-tax',  FMT.mult(taxMult > 0 ? taxMult : null));
  setText('tb-peak', FMT.j(ov.max_energy_j));

  const modeBadge = document.getElementById('mode-badge');
  if (modeBadge) {
    modeBadge.textContent = STATE.caps.mode === 'full' ? '● Full Mode' : '● Read-Only';
    modeBadge.className   = 'mode-badge ' + (STATE.caps.mode === 'full' ? 'mode-full' : 'mode-ro');
  }

  // Sidebar badge counts
  setBadge('nb-runs',    ov.total_runs    || '—');
  setBadge('nb-tax',     STATE.tax.length || '—');
  setBadge('nb-anom',    STATE.anomalies.length || '0');
  setBadge('nb-exps',    STATE.exps.length || '—');

  // Sidebar footer system info
  setText('si-mode',  STATE.caps.mode || '—');
  const dbName = (STATE.caps.db_path || '').split('/').pop() || '—';
  setText('si-db', dbName);
  const runWithGov = STATE.runs.find(r => r.governor);
  setText('si-gov',   runWithGov?.governor       || '—');
  setText('si-turbo', runWithGov?.turbo_enabled != null ? (runWithGov.turbo_enabled ? 'ON' : 'OFF') : '—');

  document.getElementById('theme-label').textContent =
    document.documentElement.getAttribute('data-theme') === 'light' ? 'Light' : 'Dark';
}

// ══════════════════════════════════════════════════════════════
// PAGE: OVERVIEW
// ══════════════════════════════════════════════════════════════
function renderOverview() {
  const ov       = STATE.overview;
  const taxMult  = D.taxMultiple();
  const linJ     = D.avgLinearJ();
  const ageJ     = D.avgAgenticJ();
  const phases   = D.phasePcts();

  // ── Hero energy story ──────────────────────────────────────
  const hero = document.getElementById('ov-energy-story');
  if (hero) {
    const maxBar = Math.max(linJ, ageJ) || 1;
    hero.innerHTML = `
      <div class="energy-story-title">Agentic costs <span style="color:var(--red);font-family:'IBM Plex Mono',monospace;">${FMT.mult(taxMult)}</span> more energy than linear for the same task</div>
      <div class="energy-story-sub">Measured across ${ov.total_runs || STATE.runs.length} runs · ${ov.total_experiments || STATE.exps.length} experiments</div>

      <div class="es-row">
        <div class="es-label">Linear</div>
        <div class="es-bar-wrap">
          <div class="es-bar-track">
            <div class="es-bar-fill linear" style="width:${(linJ / maxBar * 100).toFixed(1)}%;">${FMT.j(linJ, 3)}</div>
          </div>
        </div>
        <div class="es-value">${FMT.j(linJ, 3)}</div>
        <div class="es-mult">1×</div>
      </div>

      <div class="es-row">
        <div class="es-label">Agentic</div>
        <div class="es-bar-wrap">
          <div class="es-bar-track">
            <div class="es-bar-fill agentic" style="width:${(ageJ / maxBar * 100).toFixed(1)}%;">${FMT.j(ageJ, 3)}</div>
          </div>
        </div>
        <div class="es-value">${FMT.j(ageJ, 3)}</div>
        <div class="es-mult" style="color:var(--red);">${FMT.mult(taxMult)}</div>
      </div>

      ${phases.plan > 0 ? `
      <div class="phase-bar-wrap">
        <div class="phase-bar-label">Where the overhead goes — agentic time breakdown</div>
        <div class="phase-bar">
          <div class="phase-seg planning"  style="width:${phases.plan}%;">${phases.plan > 8 ? phases.plan + '% plan' : ''}</div>
          <div class="phase-seg execution" style="width:${phases.exec}%;">${phases.exec > 8 ? phases.exec + '% exec' : ''}</div>
          <div class="phase-seg synthesis" style="width:${phases.synth}%;">${phases.synth > 8 ? phases.synth + '% synth' : ''}</div>
        </div>
        <div class="phase-legend">
          <div class="phase-legend-item"><div class="phase-dot" style="background:var(--planning)"></div>Planning ${FMT.ms(D.avgPlanMs(), 0)} — pure overhead</div>
          <div class="phase-legend-item"><div class="phase-dot" style="background:var(--execution)"></div>Execution ${FMT.ms(D.avgExecMs(), 0)} — tool latency</div>
          <div class="phase-legend-item"><div class="phase-dot" style="background:var(--synthesis)"></div>Synthesis ${FMT.ms(D.avgSynthMs(), 0)} — context merge</div>
        </div>
      </div>
      ` : ''}
    `;
  }

  // ── KPI row ────────────────────────────────────────────────
  const kpiEl = document.getElementById('ov-kpis');
  if (kpiEl) {
    kpiEl.innerHTML = heroMetric('c-green',  'Total Runs',     ov.total_runs || '—',           `${ov.linear_runs || 0} linear · ${ov.agentic_runs || 0} agentic`)
      + heroMetric('c-red',    'Agentic Tax',     FMT.mult(taxMult),      `avg ${FMT.j(ageJ)} vs ${FMT.j(linJ)} linear`, taxMult > 3 ? { cls:'red', txt:'HIGH' } : null)
      + heroMetric('c-amber',  'Avg Planning',    FMT.ms(ov.avg_planning_ms, 0),  `${phases.plan}% of agentic time — pure overhead`)
      + heroMetric('c-blue',   'Peak IPC',        FMT.num(ov.max_ipc, 3),  `avg ${FMT.num(ov.avg_ipc, 3)} · cache miss ${FMT.pct(ov.avg_cache_miss_pct)}`)
      + heroMetric('c-purple', 'Avg Carbon',      FMT.mg(ov.avg_carbon_mg, 3), `total ${FMT.mg(ov.total_carbon_mg, 2)} CO₂e all runs`)
      + heroMetric('c-sky',    'Total Energy',    FMT.j(ov.total_energy_j, 1),   `across all experiments`);
  }

  // ── Split bars ─────────────────────────────────────────────
  const splitsEl = document.getElementById('ov-splits');
  if (splitsEl && phases.plan > 0) {
    const linMig = avg(D.linear().map(r => +(r.thread_migrations || 0)));
    const ageMig = avg(D.agentic().map(r => +(r.thread_migrations || 0)));
    const linMiss = avg(D.linear().map(r => +(r.cache_miss_rate || 0))) * 100;
    const ageMiss = avg(D.agentic().map(r => +(r.cache_miss_rate || 0))) * 100;
    const avgTaxPct = avg(STATE.tax.map(t => +(t.tax_percent || 0)));

    splitsEl.innerHTML =
      splitRow('Planning overhead',  phases.plan,  'var(--planning)',  `${phases.plan}% of agentic time`)
    + splitRow('Tool API latency',   phases.exec,  'var(--execution)', `${FMT.ms(D.avgExecMs(), 0)} avg`)
    + splitRow('Synthesis cost',     phases.synth, 'var(--synthesis)', `${FMT.ms(D.avgSynthMs(), 0)} avg`)
    + splitRow('Energy tax avg',     Math.min(100, avgTaxPct), 'var(--red)',  `${avgTaxPct.toFixed(1)}%`)
    + splitRow('Extra migrations',   Math.min(100, (ageMig - linMig) / (linMig || 1) * 40), 'var(--sky)', `+${(ageMig - linMig).toFixed(0)} avg`)
    + splitRow('Cache miss Δ',       Math.min(100, Math.abs(ageMiss - linMiss) * 5), 'var(--amber)', `+${(ageMiss - linMiss).toFixed(1)}pp`);
  } else if (splitsEl) {
    splitsEl.innerHTML = '<div class="empty-state">No agentic run data with phase breakdown yet</div>';
  }

  renderOverviewCharts();
}

// ══════════════════════════════════════════════════════════════
// PAGE: ENERGY
// ══════════════════════════════════════════════════════════════
function renderEnergy() {
  const ov = STATE.overview;
  const kpiEl = document.getElementById('en-kpis');
  if (kpiEl) {
    kpiEl.innerHTML =
      heroMetric('c-green',  'Min Energy',    FMT.j(ov.min_energy_j, 3), 'best single run')
    + heroMetric('c-red',    'Max Energy',    FMT.j(ov.max_energy_j, 3), 'worst single run')
    + heroMetric('c-blue',   'Total Measured',FMT.j(ov.total_energy_j, 1), 'all runs combined')
    + heroMetric('c-purple', 'Avg Carbon',    FMT.mg(ov.avg_carbon_mg, 3), `${FMT.mg(ov.total_carbon_mg, 2)} total CO₂e`)
    + heroMetric('c-sky',    'Avg Water',     `${FMT.num(ov.avg_water_ml, 3)}ml`, 'per run');
  }
  renderEnergyCharts();
}

// ══════════════════════════════════════════════════════════════
// PAGE: CPU & C-STATES
// ══════════════════════════════════════════════════════════════
function renderCpu() {
  // C-state panels from real aggregate
  const panel = document.getElementById('cstate-panels');
  const totalSamples = STATE.cstates.reduce((s, r) => s + (r.sample_count || 0), 0);
  setText('cs-sample-count', `${totalSamples.toLocaleString()} cpu_samples`);

  if (panel && STATE.cstates.length) {
    panel.innerHTML = STATE.cstates.map(r => {
      const c0 = Math.max(0, 100 - (r.avg_c1||0) - (r.avg_c2||0) - (r.avg_c3||0) - (r.avg_c6||0) - (r.avg_c7||0));
      const states = [
        { k:'C0', v: c0,          col:'var(--red)'    },
        { k:'C1', v: r.avg_c1||0, col:'var(--sky)'    },
        { k:'C2', v: r.avg_c2||0, col:'var(--acc)'    },
        { k:'C3', v: r.avg_c3||0, col:'var(--purple)' },
        { k:'C6', v: r.avg_c6||0, col:'var(--grn)'    },
        { k:'C7', v: r.avg_c7||0, col:'var(--amb)'    },
      ];
      return `
        <div>
          <div style="font-size:11px;font-weight:600;color:var(--wht);margin-bottom:10px;">${r.provider} · ${r.workflow_type}</div>
          ${states.map(s => `
            <div class="cstate-row">
              <div class="cstate-key">${s.k}</div>
              <div class="cstate-track"><div class="cstate-fill" style="width:${Math.min(100, Math.abs(s.v)).toFixed(1)}%;background:${s.col};"></div></div>
              <div class="cstate-val">${s.v.toFixed(1)}%</div>
            </div>`).join('')}
          <div style="font-size:9px;color:var(--txt3);margin-top:6px;">${(r.avg_pkg_watts||0).toFixed(2)}W pkg · ${(r.sample_count||0).toLocaleString()} samples</div>
        </div>`;
    }).join('');
  } else if (panel) {
    panel.innerHTML = '<div class="empty-state"><div class="empty-icon">▣</div>No cpu_samples yet — run experiments to populate</div>';
  }

  renderCpuCharts();
}

// ══════════════════════════════════════════════════════════════
// PAGE: SCHEDULER
// ══════════════════════════════════════════════════════════════
function renderScheduler() {
  const all   = STATE.runs.filter(r => r.thread_migrations != null);
  const linR  = all.filter(r => r.workflow_type === 'linear');
  const ageR  = all.filter(r => r.workflow_type === 'agentic');
  const kpiEl = document.getElementById('sched-kpis');
  if (kpiEl) {
    const maxMig = maxVal(all.map(r => +r.thread_migrations));
    const avgLin = avg(linR.map(r => +r.thread_migrations));
    const avgAge = avg(ageR.map(r => +r.thread_migrations));
    const maxIrq = maxVal(STATE.runs.filter(r => r.interrupt_rate).map(r => +r.interrupt_rate));
    kpiEl.innerHTML =
      heroMetric('c-red',   'Max Migrations',   FMT.k(maxMig), 'single run peak')
    + heroMetric('c-green', 'Linear avg',        FMT.num(avgLin, 0), 'thread migrations')
    + heroMetric('c-amber', 'Agentic avg',       FMT.num(avgAge, 0), `${FMT.mult(avgLin > 0 ? avgAge/avgLin : null)} vs linear`, avgAge > avgLin * 2 ? { cls:'red', txt:'HIGH' } : null)
    + heroMetric('c-sky',   'Max IRQ/s',         FMT.k(maxIrq), 'interrupt rate peak')
    + heroMetric('c-blue',  'Avg Cache Miss',    FMT.pct(STATE.overview.avg_cache_miss_pct), 'across all runs');
  }
  renderSchedulerCharts();
}

// ══════════════════════════════════════════════════════════════
// PAGE: DOMAINS
// ══════════════════════════════════════════════════════════════
function renderDomains() {
  if (!STATE.domains.length) {
    document.getElementById('dom-kpis').innerHTML =
      '<div class="empty-state" style="grid-column:1/-1">No baseline-corrected domain data — idle_baselines must be linked to runs</div>';
    document.getElementById('tbody-domains').innerHTML =
      '<tr><td colspan="11" class="empty-state">No data</td></tr>';
    return;
  }

  const avgCoreShare   = avg(STATE.domains.map(d => +(d.core_share   || 0))) * 100;
  const avgUncoreShare = avg(STATE.domains.map(d => +(d.uncore_share || 0))) * 100;
  const avgWorkload    = avg(STATE.domains.map(d => +(d.workload_energy_j    || 0)));
  const avgTax         = avg(STATE.domains.map(d => +(d.orchestration_tax_j || 0)));

  const kpiEl = document.getElementById('dom-kpis');
  if (kpiEl) {
    kpiEl.innerHTML =
      heroMetric('c-blue',   'Avg Core Share',   FMT.pct(avgCoreShare,   1), 'of total PKG energy')
    + heroMetric('c-sky',    'Avg Uncore Share', FMT.pct(avgUncoreShare, 1), 'interconnect + LLC')
    + heroMetric('c-green',  'Avg Workload',     FMT.j(avgWorkload, 3), 'PKG − idle baseline')
    + heroMetric('c-red',    'Avg Tax',          FMT.j(avgTax, 3),      'uncore overhead above baseline');
  }

  const tbody = document.getElementById('tbody-domains');
  if (tbody) {
    tbody.innerHTML = STATE.domains.map(d => `
      <tr>
        <td class="mono c-blue">${d.run_id}</td>
        <td><span class="badge ${d.workflow_type === 'linear' ? 'badge-green' : 'badge-red'}">${d.workflow_type}</span></td>
        <td>${d.task_name || '—'}</td>
        <td class="mono">${(+(d.pkg_energy_j    || 0)).toFixed(3)}</td>
        <td class="mono">${(+(d.core_energy_j   || 0)).toFixed(3)}</td>
        <td class="mono">${(+(d.uncore_energy_j || 0)).toFixed(3)}</td>
        <td class="mono">${(+(d.dram_energy_j   || 0)).toFixed(3)}</td>
        <td class="mono ${+(d.workload_energy_j||0) > 10 ? 'c-red' : +(d.workload_energy_j||0) > 3 ? 'c-amber' : 'c-green'}">${(+(d.workload_energy_j||0)).toFixed(3)}</td>
        <td class="mono ${+(d.orchestration_tax_j||0) > 5 ? 'c-red' : 'c-amber'}">${(+(d.orchestration_tax_j||0)).toFixed(3)}</td>
        <td class="mono">${((+(d.core_share   || 0)) * 100).toFixed(1)}%</td>
        <td class="mono">${((+(d.uncore_share || 0)) * 100).toFixed(1)}%</td>
      </tr>`).join('');
  }

  renderDomainCharts();
}

// ══════════════════════════════════════════════════════════════
// PAGE: TAX
// ══════════════════════════════════════════════════════════════
function renderTax() {
  const avgTax  = avg(STATE.tax.map(t => +(t.tax_percent || 0)));
  const maxTax  = maxVal(STATE.tax.map(t => +(t.tax_percent || 0)));
  const avgPlan = D.avgPlanMs();
  const avgExec = D.avgExecMs();

  setText('tax-pair-count', `${STATE.tax.length} pairs`);

  const cardsEl = document.getElementById('tax-cards');
  if (cardsEl) {
    cardsEl.innerHTML = `
      <div class="analysis-card" style="border-left-color:var(--planning);">
        <div class="ac-title" style="color:var(--planning);">① Planning Phase Tax</div>
        <div class="ac-finding">Average planning: <strong>${avgPlan.toFixed(0)}ms</strong>. For simple/L1 tasks this often exceeds actual inference. The LLM constructs a full plan before any useful work begins.</div>
        <div class="ac-finding">Fix: <em>memoize plans</em> for repeated task types. Hit rate could exceed 40% on repeated query patterns — planning phase eliminated entirely on cache hit.</div>
      </div>
      <div class="analysis-card" style="border-left-color:var(--execution);">
        <div class="ac-title" style="color:var(--execution);">② Tool API Latency Tax</div>
        <div class="ac-finding">Execution phase: <strong>${avgExec.toFixed(0)}ms</strong> avg. CPU spins in user-space wait during API calls. RAPL charges full idle drain (sampling every 100ms).</div>
        <div class="ac-finding">Fix: <em>async tool dispatch</em> + result caching. Sequential tool calls are the main target — each saved call ≈ 2–4J.</div>
      </div>
      <div class="analysis-card" style="border-left-color:var(--red);">
        <div class="ac-title" style="color:var(--red);">③ Measured Tax: avg ${avgTax.toFixed(1)}% · peak ${maxTax.toFixed(1)}%</div>
        <div class="ac-finding">Tax = energy delta between agentic and linear for identical tasks. Planning + tool overhead + synthesis together account for the full measured tax.</div>
        <div class="ac-finding">Fix: <em>route simple tasks linearly</em> — removes planning + synthesis entirely. Classifier overhead adds &lt;1ms.</div>
      </div>
    `;
  }

  const tbody = document.getElementById('tbody-tax');
  if (tbody) {
    tbody.innerHTML = STATE.tax.map(t => `
      <tr>
        <td class="mono c-blue">${t.comparison_id}</td>
        <td>${t.task_name || '—'}</td>
        <td>${t.provider || '—'}</td>
        <td>${t.country_code || '—'}</td>
        <td class="mono c-green">${(+(t.linear_dynamic_j  || 0)).toFixed(3)}</td>
        <td class="mono c-red">${(+(t.agentic_dynamic_j || 0)).toFixed(3)}</td>
        <td class="mono c-amber">${(+(t.tax_j            || 0)).toFixed(3)}</td>
        <td class="mono"><span class="badge ${+(t.tax_percent||0)>50?'badge-red':+(t.tax_percent||0)>20?'badge-amber':'badge-green'}">${(+(t.tax_percent||0)).toFixed(1)}%</span></td>
        <td class="mono">${t.planning_time_ms  ? Math.round(t.planning_time_ms)  : '—'}</td>
        <td class="mono">${t.execution_time_ms ? Math.round(t.execution_time_ms) : '—'}</td>
        <td class="mono">${t.synthesis_time_ms ? Math.round(t.synthesis_time_ms) : '—'}</td>
        <td class="mono">${t.llm_calls  || '—'}</td>
        <td class="mono">${t.tool_calls || '—'}</td>
      </tr>`).join('')
    || '<tr><td colspan="13" class="empty-state">No tax pairs yet — run comparison experiments</td></tr>';
  }

  renderTaxCharts();
}

// ══════════════════════════════════════════════════════════════
// PAGE: ANOMALIES
// ══════════════════════════════════════════════════════════════
function renderAnomalies() {
  const tbody = document.getElementById('tbody-anom');
  if (tbody) {
    tbody.innerHTML = STATE.anomalies.map(r => {
      const flags = [
        r.flag_high_energy ? '<span class="badge badge-red">HIGH-E</span>'    : '',
        r.flag_low_ipc     ? '<span class="badge badge-amber">LOW-IPC</span>' : '',
        r.flag_high_miss   ? '<span class="badge badge-amber">HIGH-MISS</span>':'',
        r.flag_thermal     ? '<span class="badge badge-red">THERMAL</span>'   : '',
        r.flag_irq         ? '<span class="badge badge-sky">HIGH-IRQ</span>'  : '',
      ].filter(Boolean).join(' ');
      return `<tr>
        <td class="mono c-blue">${r.run_id}</td>
        <td class="mono">${r.exp_id}</td>
        <td><span class="badge ${r.workflow_type==='linear'?'badge-green':'badge-red'}">${r.workflow_type}</span></td>
        <td>${r.task_name  || '—'}</td>
        <td>${r.provider   || '—'}</td>
        <td class="mono ${r.flag_high_energy ? 'c-red'   : ''}">${(+(r.energy_j       || 0)).toFixed(3)}</td>
        <td class="mono ${r.flag_low_ipc     ? 'c-amber' : ''}">${(+(r.ipc            || 0)).toFixed(3)}</td>
        <td class="mono ${r.flag_high_miss   ? 'c-amber' : ''}">${(+(r.cache_miss_pct || 0)).toFixed(1)}%</td>
        <td class="mono ${r.flag_thermal     ? 'c-red'   : ''}">${r.thermal_delta_c   ? +(r.thermal_delta_c).toFixed(1) + '°C' : '—'}</td>
        <td class="mono ${r.flag_irq         ? 'c-sky'   : ''}">${r.interrupt_rate    ? (+r.interrupt_rate).toLocaleString() : '—'}</td>
        <td>${flags}</td>
      </tr>`;
    }).join('') || '<tr><td colspan="11" class="empty-state">No anomalies detected — data looks clean</td></tr>';
  }

  const cardsEl = document.getElementById('anom-cards');
  if (cardsEl) {
    const highE  = STATE.anomalies.filter(r => r.flag_high_energy).length;
    const lowIPC = STATE.anomalies.filter(r => r.flag_low_ipc).length;
    const therm  = STATE.anomalies.filter(r => r.flag_thermal).length;
    cardsEl.innerHTML = `
      <div class="analysis-card" style="border-left-color:var(--red);">
        <div class="ac-title" style="color:var(--red);">${highE} High-Energy Outliers</div>
        <div class="ac-finding">Runs with energy &gt;2σ above mean. Likely causes: long agentic runs, cold RAPL counter, burst IRQ storms, or thermal throttle recovery.</div>
      </div>
      <div class="analysis-card" style="border-left-color:var(--amber);">
        <div class="ac-title" style="color:var(--amb);">${lowIPC} Low-IPC Outliers</div>
        <div class="ac-finding">IPC below 50% of mean — excessive cache thrashing, heavy involuntary context switching, or IO-bound stall cycles dominating.</div>
      </div>
      <div class="analysis-card" style="border-left-color:var(--sky);">
        <div class="ac-title" style="color:var(--sky);">${therm} Thermal Throttle Events</div>
        <div class="ac-finding"><em>thermal_throttle_flag=1</em>: CPU reduced frequency mid-run. Energy measurement valid but IPC/duration are affected — mark for re-run under cooler conditions.</div>
      </div>`;
  }
}

// ══════════════════════════════════════════════════════════════
// PAGE: EXECUTE
// ══════════════════════════════════════════════════════════════
let _running = false;
let _liveWS  = null;
let _curCpx  = 'simple';

function initExecutePage() {
  // Populate model/provider dropdown from run data
  const sel = document.getElementById('sel-model');
  if (sel) {
    const providers = [...new Set(STATE.runs.map(r => r.provider).filter(Boolean))];
    sel.innerHTML = (providers.length ? providers : ['cloud', 'local']).map(p =>
      `<option value="${p}">${p === 'cloud' ? '☁ Cloud (Groq/API)' : '⬡ Local (Ollama)'}</option>`
    ).join('');
  }

  const btn = document.getElementById('run-btn');
  if (btn && !STATE.caps.harness_available) {
    btn.disabled = true;
    setText('exec-mode-sub', '⚠ Read-only mode — SSH to A-LEMS machine to enable execution');
    setText('exec-status', 'Harness not available on this server');
  } else if (btn) {
    setText('exec-mode-sub', 'Harness available — live execution enabled');
  }
}

function setCpx(el, lvl) {
  document.querySelectorAll('.cpx-tab').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  _curCpx = lvl;
}
function selTask(el) {
  document.querySelectorAll('.task-option').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
}

async function doRun() {
  if (_running) return;
  _running = true;

  const btn    = document.getElementById('run-btn');
  const prog   = document.getElementById('exec-prog');
  const status = document.getElementById('exec-status');
  const termL  = document.getElementById('term-lin');
  const termA  = document.getElementById('term-age');

  btn.disabled = true;
  termL.innerHTML = termA.innerHTML = '';
  document.getElementById('exec-results').style.display = 'none';
  document.getElementById('exec-live-card').style.display = 'block';
  initLiveChart();

  termLog(termL, 0, 'Initialising linear executor…');
  termLog(termA, 0, 'Queued — awaiting linear completion…', 'term-warn');

  const provider = document.getElementById('sel-model')?.value || 'cloud';
  const country  = document.getElementById('sel-region')?.value || 'US';
  const reps     = parseInt(document.getElementById('sel-reps')?.value || '3');
  const taskEl   = document.querySelector('.task-option.active');
  const taskName = taskEl ? taskEl.dataset.task : 'simple';

  const caps = await API.capabilities().catch(() => ({ harness_available: false }));

  if (!caps.harness_available) {
    termLog(termL, 50, '⚠ Harness unavailable — running simulation', 'term-warn');
    _simulateRun(termL, termA, prog, status, btn, taskName, _curCpx, provider, country, reps);
    return;
  }

  termLog(termL, 0, `Submitting: ${taskName} · ${_curCpx} · ${provider} · ${country} · ${reps} reps`);

  try {
    const res   = await API.execute({ task_id: taskName, provider, country_code: country, repetitions: reps });
    const expId = res.exp_id;
    status.textContent = `Experiment ${expId} started…`;
    termLog(termL, 0, `Experiment ${expId} created`, 'term-hi');

    if (_liveWS) _liveWS.close();
    _liveWS = API.liveRunWS(expId);
    let lastLogLen = 0;

    _liveWS.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      if (msg.type === 'status') {
        prog.style.width = msg.progress + '%';
        status.textContent = `Rep ${msg.completed}/${msg.reps}`;
      } else if (msg.type === 'pair') {
        termLog(termL, msg.linear_ms  || 0, `Rep ${msg.rep}: ${(msg.linear_j  || 0).toFixed(3)}J · ${Math.round(msg.linear_ms  || 0)}ms`, 'term-hi');
        termLog(termA, msg.agentic_ms || 0, `Rep ${msg.rep}: ${(msg.agentic_j || 0).toFixed(3)}J · ${Math.round(msg.agentic_ms || 0)}ms · tax ${(msg.tax_pct || 0).toFixed(0)}%`, 'term-warn');
      } else if (msg.type === 'sample') {
        pushLiveSample(msg.workflow_type, msg.elapsed_ms, msg.pkg_j || 0);
      } else if (msg.type === 'done') {
        prog.style.width = '100%';
        status.textContent = 'Complete — reloading data…';
        _liveWS.close();
        setTimeout(() => { boot().then(() => { _running = false; btn.disabled = false; }); }, 1500);
      }
    };
    _liveWS.onerror = () => {
      termLog(termL, 0, 'WebSocket error', 'term-err');
      _running = false; btn.disabled = false;
    };
  } catch (err) {
    termLog(termL, 0, `Error: ${err.message}`, 'term-err');
    _running = false; btn.disabled = false;
  }
}

function _simulateRun(termL, termA, prog, status, btn, task, cpx, provider, country, reps) {
  const dur  = cpx === 'L2' ? 12000 : cpx === 'L1' ? 3500 : 700;
  const linJ = dur * 0.0012;
  const mult = cpx === 'L2' ? 7.8 : cpx === 'L1' ? 4.3 : 2.4;
  const ageJ = linJ * mult;

  const linSteps = [
    [0,    'RAPL PKG snapshot taken'],
    [20,   'perf counters armed'],
    [dur * .3 | 0, 'TTFT received — streaming tokens'],
    [dur,  `Complete: ${linJ.toFixed(3)}J · ${dur}ms`, 'term-hi'],
  ];
  const ageSteps = [
    [0,    `Agentic loop init · complexity=${cpx}`],
    [cpx !== 'simple' ? dur*.2|0 : 10, cpx !== 'simple' ? `Planning: ${(dur*.2).toFixed(0)}ms` : 'Single-shot (no planning)', cpx !== 'simple' ? 'term-warn' : undefined],
    [dur * .7 | 0, 'Execution phase — tool calls dispatching'],
    [dur * .9 | 0, 'Synthesis — merging context'],
    [dur * mult | 0, `Complete: ${ageJ.toFixed(3)}J · tax=${((mult-1)*100).toFixed(0)}%`, 'term-err'],
  ];

  let i = 0;
  const linInt = setInterval(() => {
    if (i < linSteps.length) {
      const [ts, msg, cls] = linSteps[i++];
      termLog(termL, ts, msg, cls);
      prog.style.width = Math.min(40, i / linSteps.length * 40) + '%';
    } else clearInterval(linInt);
  }, 450);

  setTimeout(() => {
    let j = 0;
    const ageInt = setInterval(() => {
      if (j < ageSteps.length) {
        const [ts, msg, cls] = ageSteps[j++];
        termLog(termA, ts, msg, cls);
        prog.style.width = Math.min(95, 40 + j / ageSteps.length * 55) + '%';
      } else {
        clearInterval(ageInt);
        prog.style.width = '100%';
        status.textContent = 'Simulation complete';
        _showSimResults(linJ, ageJ, mult, cpx);
        _running = false; btn.disabled = false;
      }
    }, 500);
  }, linSteps.length * 450 + 300);
}

function _showSimResults(linJ, ageJ, mult, cpx) {
  const el = document.getElementById('exec-results');
  if (!el) return;
  el.style.display = 'block';
  const kpiEl = document.getElementById('exec-kpis');
  if (kpiEl) {
    kpiEl.innerHTML =
      heroMetric('c-green', 'Linear',  FMT.j(linJ, 3), 'single-pass execution')
    + heroMetric('c-red',   'Agentic', FMT.j(ageJ, 3), `${mult.toFixed(1)}× overhead vs linear`)
    + heroMetric('c-amber', 'Tax',     FMT.pct((mult-1)*100, 0), 'orchestration overhead')
    + heroMetric('c-blue',  'Mode',    'Simulated', 'harness not connected');
  }
  const maxE = Math.max(linJ, ageJ);
  const barsEl = document.getElementById('exec-bars');
  if (barsEl) {
    barsEl.innerHTML =
      splitRow('Linear',  linJ / maxE * 100, 'var(--energy-linear)',  FMT.j(linJ, 3))
    + splitRow('Agentic', ageJ / maxE * 100, 'var(--energy-agentic)', FMT.j(ageJ, 3));
  }
  const attrEl = document.getElementById('exec-attribution');
  if (attrEl) {
    attrEl.innerHTML = cpx === 'simple'
      ? '<div class="callout info">Simple task — even minimal orchestration adds planning init + synthesis step. Route to linear for zero-overhead execution on stateless tasks.</div>'
      : cpx === 'L1'
        ? '<div class="callout warn">L1 task: planning phase is the primary driver (~29% of runtime). Single tool call adds measurable API wait. Async dispatch could cut this 40%.</div>'
        : '<div class="callout alert">L2 task: multi-tool sequential chain. Each tool call = independent API wait cycle. Async dispatch + result caching could recover 50–60% of tax.</div>';
  }
}

// ══════════════════════════════════════════════════════════════
// PAGE: SAMPLE EXPLORER
// ══════════════════════════════════════════════════════════════
function buildExplorerList() {
  const el = document.getElementById('explorer-run-list');
  if (!el) return;
  const sorted = [...STATE.runs].sort((a, b) => +b.run_id - +a.run_id);
  el.innerHTML = sorted.map(r => `
    <div class="exp-item" onclick="loadExplorer(${r.run_id}, this)"
         data-search="${r.run_id} ${r.workflow_type} ${r.provider} ${r.country_code} ${r.task_name}">
      <div class="exp-dot" style="background:${r.workflow_type === 'agentic' ? 'var(--red)' : 'var(--grn)'};"></div>
      <div style="min-width:0;flex:1;">
        <div style="font-size:11px;color:var(--txt);">Run ${r.run_id}</div>
        <div style="font-size:9px;color:var(--txt3);font-family:'IBM Plex Mono',monospace;">${r.workflow_type} · ${r.provider} · ${r.country_code}</div>
      </div>
      <div style="font-size:9px;font-family:'IBM Plex Mono',monospace;color:var(--txt3);flex-shrink:0;">${FMT.j(r.energy_j, 2)}</div>
    </div>`).join('');
}

function filterExplorerRuns(q) {
  document.querySelectorAll('#explorer-run-list .exp-item').forEach(el => {
    el.style.display = el.dataset.search.toLowerCase().includes(q.toLowerCase()) ? 'flex' : 'none';
  });
}

async function loadExplorer(runId, el) {
  document.querySelectorAll('#explorer-run-list .exp-item').forEach(e => e.classList.remove('active'));
  el.classList.add('active');

  const detail = document.getElementById('explorer-detail');
  detail.innerHTML = '<div class="card card-pad"><div class="loading-state"><div class="spinner"></div>Loading samples…</div></div>';

  try {
    const [run, energy, cpu, irq, events] = await Promise.all([
      API.run(runId),
      API.energySamples(runId),
      API.cpuSamples(runId),
      API.irqSamples(runId),
      API.events(runId),
    ]);

    const totalJ = (+(run.total_energy_uj || 0) / 1e6).toFixed(3);
    const hz     = energy.length && run.duration_ns ? (energy.length / ((+run.duration_ns) / 1e9)).toFixed(0) : '—';

    detail.innerHTML = `
      <div class="grid grid-4 mb">
        ${heroMetric('c-blue',   `Run ${runId}`,       `${totalJ}J`, `${run.workflow_type} · ${run.provider}`)}
        ${heroMetric('c-sky',    'Energy Samples',     energy.length, `≈${hz} Hz avg sampling rate`)}
        ${heroMetric('c-green',  'CPU Samples',        cpu.length,    'turbostat readings')}
        ${heroMetric('c-amber',  'Orch Events',        events.length, 'orchestration_events table')}
      </div>

      <div class="grid grid-2 mb">
        <div class="card card-pad">
          <div class="card-header"><div class="card-title">Power over time (W)</div><div class="card-sub">${energy.length} samples</div></div>
          <div class="chart-container chart-md"><canvas id="c-exp-power"></canvas></div>
        </div>
        <div class="card card-pad">
          <div class="card-header"><div class="card-title">IPC + CPU utilisation</div><div class="card-sub">${cpu.length} cpu_samples</div></div>
          <div class="chart-container chart-md"><canvas id="c-exp-ipc"></canvas></div>
        </div>
      </div>

      ${events.length ? `
      <div class="card card-pad mb">
        <div class="card-header"><div class="card-title">Orchestration Event Timeline</div><div class="card-sub">${events.length} events</div></div>
        <div id="gantt-wrap"></div>
        <div class="callout info" style="margin-top:10px;">Total span: ${(Math.max(...events.map(ev => +(ev.start_ms||0) + +(ev.duration_ms||0)))).toFixed(0)}ms. Colors: <span style="color:var(--planning)">■ planning</span> <span style="color:var(--execution)">■ execution</span> <span style="color:var(--synthesis)">■ synthesis</span></div>
      </div>` : ''}

      <div class="grid grid-2">
        <div class="card card-pad">
          <div class="card-header"><div class="card-title">C-state residency</div><div class="card-sub">${cpu.length} cpu_samples</div></div>
          <div class="chart-container chart-sm"><canvas id="c-exp-cstate"></canvas></div>
        </div>
        <div class="card card-pad">
          <div class="card-header"><div class="card-title">IRQ rate</div><div class="card-sub">${irq.length} samples</div></div>
          <div class="chart-container chart-sm"><canvas id="c-exp-irq"></canvas></div>
        </div>
      </div>
    `;

    renderExplorerCharts(energy, cpu, irq);

    // Gantt
    if (events.length) {
      const maxMs = Math.max(...events.map(ev => +(ev.start_ms || 0) + +(ev.duration_ms || 0)));
      const phaseColor = { planning: 'var(--planning)', execution: 'var(--execution)', synthesis: 'var(--synthesis)' };
      document.getElementById('gantt-wrap').innerHTML = `
        <div class="gantt" style="padding:8px 0;">
          ${events.map(ev => `
            <div class="gantt-row">
              <div class="gantt-label">${ev.phase || '—'}</div>
              <div class="gantt-track">
                <div class="gantt-seg" style="left:${(+(ev.start_ms||0)/maxMs*100).toFixed(1)}%;width:${Math.max(1,(+(ev.duration_ms||0)/maxMs*100)).toFixed(1)}%;background:${phaseColor[ev.phase] || 'var(--acc)'};">${ev.event_type || ''}</div>
              </div>
              <div class="gantt-dur">${Math.round(+(ev.duration_ms || 0))}ms</div>
            </div>`).join('')}
        </div>`;
    }
  } catch (err) {
    detail.innerHTML = `<div class="card card-pad"><div class="callout alert"><strong>Error loading run ${runId}:</strong> ${err.message}</div></div>`;
  }
}

// ══════════════════════════════════════════════════════════════
// PAGE: EXPERIMENTS
// ══════════════════════════════════════════════════════════════
function buildExperimentsList() {
  const el = document.getElementById('exp-list');
  if (!el) return;
  el.innerHTML = STATE.exps.map(e => `
    <div class="exp-item" onclick="showExperiment(${e.exp_id}, this)"
         data-search="${e.exp_id} ${e.name} ${e.provider} ${e.task_name} ${e.status}">
      <div class="exp-dot" style="background:${e.status==='completed'?'var(--grn)':e.status==='error'?'var(--red)':'var(--amb)'};"></div>
      <div style="min-width:0;flex:1;overflow:hidden;">
        <div style="font-size:11px;color:var(--txt);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${e.name || 'exp-' + e.exp_id}</div>
        <div style="font-size:9px;color:var(--txt3);font-family:'IBM Plex Mono',monospace;">${e.task_name||'—'} · ${e.provider||'—'} · ${e.run_count||0} runs</div>
      </div>
    </div>`).join('');
}

function filterExperiments(q) {
  document.querySelectorAll('#exp-list .exp-item').forEach(el => {
    el.style.display = el.dataset.search.toLowerCase().includes(q.toLowerCase()) ? 'flex' : 'none';
  });
}

async function showExperiment(expId, el) {
  document.querySelectorAll('#exp-list .exp-item').forEach(e => e.classList.remove('active'));
  el.classList.add('active');

  const detail = document.getElementById('exp-detail');
  detail.innerHTML = '<div class="card card-pad"><div class="loading-state"><div class="spinner"></div>Loading…</div></div>';

  try {
    const exp    = await API.experiment(expId);
    const linRns = exp.runs.filter(r => r.workflow_type === 'linear');
    const ageRns = exp.runs.filter(r => r.workflow_type === 'agentic');
    const avgL   = avg(linRns.map(r => +(r.total_energy_uj || 0) / 1e6));
    const avgA   = avg(ageRns.map(r => +(r.total_energy_uj || 0) / 1e6));

    detail.innerHTML = `
      <div class="card card-pad mb">
        <div class="card-header">
          <div class="card-title">${exp.name || 'exp-' + expId}</div>
          <span class="badge ${exp.status==='completed'?'badge-green':exp.status==='error'?'badge-red':'badge-amber'}">${exp.status}</span>
        </div>
        <div class="grid grid-4 mb">
          ${heroMetric('c-sky',   'Task',         exp.task_name || '—',     `provider: ${exp.provider || '—'}`)}
          ${heroMetric('c-green', 'Avg Linear',   FMT.j(avgL, 3),           `${linRns.length} linear runs`)}
          ${heroMetric('c-red',   'Avg Agentic',  FMT.j(avgA, 3),           `${ageRns.length} agentic runs`)}
          ${heroMetric('c-amber', 'Tax Multiple', avgL > 0 ? FMT.mult(avgA/avgL) : '—', 'agentic ÷ linear energy')}
        </div>
        <div class="table-wrap" style="max-height:320px;">
          <table>
            <thead><tr><th>Run</th><th>Type</th><th>Run#</th><th>Duration</th><th>Energy J</th><th>IPC</th><th>Cache Miss%</th><th>Migrations</th><th>Carbon mg</th><th>Temp°C</th></tr></thead>
            <tbody>${exp.runs.map(r => `<tr>
              <td class="mono c-blue">${r.run_id}</td>
              <td><span class="badge ${r.workflow_type==='linear'?'badge-green':'badge-red'}">${r.workflow_type}</span></td>
              <td class="mono">${r.run_number || '—'}</td>
              <td class="mono">${((+(r.duration_ns||0))/1e6).toFixed(0)}ms</td>
              <td class="mono ${+(r.total_energy_uj||0)/1e6>50?'c-red':+(r.total_energy_uj||0)/1e6>10?'c-amber':'c-green'}">${((+(r.total_energy_uj||0))/1e6).toFixed(3)}</td>
              <td class="mono ${+(r.ipc||0)>2?'c-sky':+(r.ipc||0)>1.3?'c-green':''}">${(+(r.ipc||0)).toFixed(3)}</td>
              <td class="mono ${+(r.cache_miss_rate||0)>.4?'c-red':+(r.cache_miss_rate||0)>.3?'c-amber':'c-green'}">${((+(r.cache_miss_rate||0))*100).toFixed(1)}%</td>
              <td class="mono">${(+(r.thread_migrations||0)).toLocaleString()}</td>
              <td class="mono">${((+(r.carbon_g||0))*1000).toFixed(4)}</td>
              <td class="mono ${+(r.package_temp_celsius||0)>80?'c-red':+(r.package_temp_celsius||0)>70?'c-amber':''}">${r.package_temp_celsius ? (+r.package_temp_celsius).toFixed(1) : '—'}</td>
            </tr>`).join('')}</tbody>
          </table>
        </div>
        ${exp.tax.length ? `
        <div class="divider"></div>
        <div style="font-size:10px;color:var(--txt3);margin-bottom:8px;">Tax pairs (${exp.tax.length})</div>
        <div class="table-wrap"><table>
          <thead><tr><th>Linear J</th><th>Agentic J</th><th>Tax J</th><th>Tax %</th></tr></thead>
          <tbody>${exp.tax.map(t => `<tr>
            <td class="mono c-green">${(+(t.linear_dynamic_j||0)).toFixed(3)}</td>
            <td class="mono c-red">${(+(t.agentic_dynamic_j||0)).toFixed(3)}</td>
            <td class="mono c-amber">${(+(t.tax_j||0)).toFixed(3)}</td>
            <td class="mono"><span class="badge ${+(t.tax_percent||0)>50?'badge-red':+(t.tax_percent||0)>20?'badge-amber':'badge-green'}">${(+(t.tax_percent||0)).toFixed(1)}%</span></td>
          </tr>`).join('')}</tbody>
        </table></div>` : ''}
      </div>`;
  } catch (err) {
    detail.innerHTML = `<div class="card card-pad"><div class="callout alert"><strong>Error:</strong> ${err.message}</div></div>`;
  }
}

// ══════════════════════════════════════════════════════════════
// DOM HELPERS
// ══════════════════════════════════════════════════════════════
function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}
function setBadge(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

// Build a hero-metric card HTML string
function heroMetric(colorClass, label, value, sub, badge = null) {
  return `
    <div class="hero-metric ${colorClass}">
      <div class="hm-label">${label}</div>
      <div class="hm-value">${value}</div>
      <div class="hm-sub">${sub}</div>
      ${badge ? `<div class="hm-badge ${badge.cls}">${badge.txt}</div>` : ''}
    </div>`;
}

// Build a split-bar row HTML string
function splitRow(label, pct, color, valueText) {
  return `
    <div class="split-row">
      <div class="split-label">${label}</div>
      <div class="split-track"><div class="split-fill" style="width:${Math.min(100, Math.max(0, pct)).toFixed(1)}%;background:${color};"></div></div>
      <div class="split-value">${valueText}</div>
    </div>`;
}

// Append a line to a terminal div
function termLog(termEl, ts, msg, cls = '') {
  if (!termEl) return;
  const line = document.createElement('div');
  line.className = 'term-line';
  const tsStr = String(Math.round(ts || 0)).padStart(6, '0');
  line.innerHTML = `<span class="term-ts">[${tsStr}]</span>${cls ? `<span class="${cls}">${msg}</span>` : msg}`;
  termEl.appendChild(line);
  termEl.scrollTop = 9999;
}
