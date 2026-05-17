// game.js — Vanilla JS game client. No React/Vue/Angular (Section 37).
// Connects via SSE, submits orders via REST, renders map and units.
'use strict';

// ===================== STATE =====================
const state = {
  playerID: '',
  side: '',          // 'light' or 'dark'
  serverURL: '',
  turn: 0,
  units: {},
  regions: {},
  paths: {},
  selectedUnitID: null,
  availableOrders: [],
  eventSource: null,
  timerInterval: null,
  timerRemaining: 60,
};

// ===================== SIDE SELECTION =====================
function chooseSide(side) {
  state.side = side;
  document.querySelectorAll('.btn-side').forEach(b => b.classList.remove('selected'));
  document.getElementById(`btn-${side}-side`).classList.add('selected');

  // Pre-fill player ID placeholder.
  const input = document.getElementById('player-id');
  if (!input.value) {
    input.value = side === 'light' ? 'light-player1' : 'dark-player1';
  }
}

// ===================== CONNECTION =====================
function connect() {
  const url = document.getElementById('server-url').value.trim();
  const pid = document.getElementById('player-id').value.trim();

  if (!pid) { showToast('Enter a Player ID', 'error'); return; }
  if (!state.side) { showToast('Choose a side first', 'error'); return; }

  state.serverURL = url;
  state.playerID = pid;

  // Show game screen.
  document.getElementById('login-screen').classList.remove('active');
  document.getElementById('game-screen').classList.add('active');

  updateSideLabel();
  setStatus('connecting');

  // Connect SSE.
  connectSSE();

  // Fetch initial state.
  fetchGameState();
}

function connectSSE() {
  const url = `${state.serverURL}/events?playerId=${encodeURIComponent(state.playerID)}`;
  state.eventSource = new EventSource(url);

  state.eventSource.onopen = () => {
    setStatus('connected');
    logEvent('Connected to game server', 'system');
  };

  state.eventSource.onmessage = (e) => {
    try {
      const data = JSON.parse(e.data);
      handleServerEvent(data);
    } catch (err) {
      // non-JSON message
    }
  };

  state.eventSource.onerror = () => {
    setStatus('error');
    logEvent('Connection lost — reconnecting…', 'system');
    // EventSource auto-reconnects.
  };
}

function handleServerEvent(data) {
  if (data.turn !== undefined) {
    updateWorldState(data);
  }
  if (data.winner) {
    showGameOver(data.winner, data.cause);
  }
  if (data.type === 'RingBearerDetected') {
    logEvent(`🔴 Ring Bearer DETECTED at ${data.regionId}!`, 'detection');
    showToast(`Ring Bearer detected at ${data.regionId}!`, 'error');
  }
  if (data.type === 'RingBearerMoved') {
    // Light Side only.
    logEvent(`💍 Ring Bearer moved to ${data.trueRegion}`, 'movement');
  }
}

// ===================== GAME STATE =====================
async function fetchGameState() {
  try {
    const res = await fetch(`${state.serverURL}/game/state?playerId=${encodeURIComponent(state.playerID)}`);
    if (!res.ok) return;
    const data = await res.json();
    updateWorldState(data);
  } catch (e) {
    // Server not running — show demo data.
    loadDemoState();
  }
}

function updateWorldState(data) {
  if (data.turn !== undefined) {
    state.turn = data.turn;
    document.getElementById('turn-number').textContent = data.turn;
  }
  if (data.units) {
    if (Array.isArray(data.units)) {
      data.units.forEach(u => { state.units[u.id] = u; });
    } else {
      state.units = data.units;
    }
  }
  if (data.regions) {
    if (Array.isArray(data.regions)) {
      data.regions.forEach(r => { state.regions[r.id] = r; });
    } else {
      state.regions = data.regions;
    }
  }
  renderUnits();
  renderMapMarkers();
  startTurnTimer();
}

// Demo state when server is not running.
function loadDemoState() {
  state.turn = 1;
  document.getElementById('turn-number').textContent = 1;

  const side = state.side;
  const demoUnits = [
    { id:'ring-bearer', name:'Frodo Baggins',   class:'RingBearer',      side:'FREE_PEOPLES', currentRegion: side==='light' ? 'the-shire' : '', strength:1, status:'ACTIVE' },
    { id:'aragorn',     name:'Aragorn',          class:'FellowshipGuard', side:'FREE_PEOPLES', currentRegion:'bree',         strength:5, status:'ACTIVE' },
    { id:'legolas',     name:'Legolas',          class:'FellowshipGuard', side:'FREE_PEOPLES', currentRegion:'rivendell',     strength:3, status:'ACTIVE' },
    { id:'gimli',       name:'Gimli',            class:'FellowshipGuard', side:'FREE_PEOPLES', currentRegion:'rivendell',     strength:3, status:'ACTIVE' },
    { id:'gandalf',     name:'Gandalf',          class:'Maia',            side:'FREE_PEOPLES', currentRegion:'rivendell',     strength:4, status:'ACTIVE' },
    { id:'gondor-army', name:'Army of Gondor',   class:'GondorArmy',      side:'FREE_PEOPLES', currentRegion:'minas-tirith', strength:5, status:'ACTIVE' },
    { id:'rohan-cavalry',name:'Riders of Rohan', class:'FellowshipGuard', side:'FREE_PEOPLES', currentRegion:'edoras',       strength:4, status:'ACTIVE' },
    { id:'witch-king',  name:'The Witch-King',   class:'Nazgul',          side:'SHADOW',       currentRegion:'minas-morgul', strength:5, status:'ACTIVE' },
    { id:'nazgul-2',    name:'The Dark Marshal', class:'Nazgul',          side:'SHADOW',       currentRegion:'minas-morgul', strength:3, status:'ACTIVE' },
    { id:'nazgul-3',    name:'The Betrayer',     class:'Nazgul',          side:'SHADOW',       currentRegion:'minas-morgul', strength:3, status:'ACTIVE' },
    { id:'uruk-hai-legion',name:'Uruk-hai Legion',class:'UrukHaiLegion',  side:'SHADOW',       currentRegion:'isengard',     strength:5, status:'ACTIVE' },
    { id:'saruman',     name:'Saruman',           class:'Maia',            side:'SHADOW',       currentRegion:'isengard',     strength:4, status:'ACTIVE' },
    { id:'sauron',      name:'Sauron',            class:'Maia',            side:'SHADOW',       currentRegion:'mordor',       strength:5, status:'ACTIVE' },
  ];
  demoUnits.forEach(u => { state.units[u.id] = u; });

  logEvent('Running in DEMO mode — no server connection', 'system');
  renderUnits();
  renderMapMarkers();
  startTurnTimer();
}

// ===================== RENDER UNITS =====================
function renderUnits() {
  const list = document.getElementById('units-list');
  const myUnits = Object.values(state.units).filter(u =>
    (state.side === 'light' && u.side === 'FREE_PEOPLES') ||
    (state.side === 'dark'  && u.side === 'SHADOW')
  );

  list.innerHTML = '';
  myUnits.forEach(u => {
    const div = document.createElement('div');
    div.className = `unit-card${u.status === 'DESTROYED' ? ' destroyed' : ''}${state.selectedUnitID === u.id ? ' selected' : ''}`;
    div.id = `unit-card-${u.id}`;
    const maxStr = getMaxStrength(u);
    const pct = maxStr ? Math.round((u.strength / maxStr) * 100) : 0;
    div.innerHTML = `
      <div class="unit-name">${u.name}</div>
      <div class="unit-meta">
        <span class="unit-strength">⚔️ ${u.strength}</span>
        <span class="unit-region">📍 ${u.currentRegion || '???'}</span>
        ${u.status !== 'ACTIVE' ? `<span class="unit-status">${u.status}</span>` : ''}
      </div>
      <div class="strength-bar-wrap">
        <div class="strength-bar" style="width:${pct}%"></div>
      </div>
    `;
    div.addEventListener('click', () => selectUnit(u.id));
    list.appendChild(div);
  });
}

function getMaxStrength(u) {
  const maxMap = {
    'ring-bearer':1,'aragorn':5,'legolas':3,'gimli':3,'gandalf':4,
    'gondor-army':5,'rohan-cavalry':4,'witch-king':5,'nazgul-2':3,
    'nazgul-3':3,'uruk-hai-legion':5,'saruman':4,'sauron':5
  };
  return maxMap[u.id] || u.strength;
}

// ===================== UNIT SELECTION =====================
async function selectUnit(unitID) {
  state.selectedUnitID = unitID;
  const u = state.units[unitID];
  if (!u) return;

  // Update visual selection.
  document.querySelectorAll('.unit-card').forEach(c => c.classList.remove('selected'));
  const card = document.getElementById(`unit-card-${unitID}`);
  if (card) card.classList.add('selected');

  // Show order form.
  document.getElementById('selected-unit-info').classList.add('hidden');
  const form = document.getElementById('order-form');
  form.classList.remove('hidden');
  document.getElementById('sel-unit-name').textContent = u.name;

  // Fetch available orders.
  try {
    const res = await fetch(`${state.serverURL}/orders/available?unitId=${unitID}&playerId=${encodeURIComponent(state.playerID)}`);
    if (res.ok) {
      const data = await res.json();
      state.availableOrders = data.orders || [];
    }
  } catch {
    // Demo: show common orders.
    state.availableOrders = getDefaultOrders(u);
  }

  renderOrderTypeSelect();
}

function getDefaultOrders(u) {
  const orders = ['ASSIGN_ROUTE', 'REDIRECT_UNIT'];
  if (u.class === 'Maia')      orders.push('MAIA_ABILITY');
  if (u.class === 'GondorArmy') orders.push('FORTIFY_REGION');
  if (u.side === 'SHADOW')     orders.push('BLOCK_PATH', 'SEARCH_PATH');
  if (u.class === 'RingBearer') orders.push('DESTROY_RING');
  orders.push('ATTACK_REGION', 'REINFORCE_REGION');
  return orders;
}

function renderOrderTypeSelect() {
  const sel = document.getElementById('order-type');
  sel.innerHTML = state.availableOrders.map(o => `<option value="${o}">${formatOrderName(o)}</option>`).join('');
  onOrderTypeChange();
}

function formatOrderName(o) {
  return o.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function onOrderTypeChange() {
  const orderType = document.getElementById('order-type').value;
  const params = document.getElementById('order-params');

  const paramTemplates = {
    'ASSIGN_ROUTE':    '<label>Path IDs (comma-separated)</label><input id="param-pathIds" class="input-field" placeholder="shire-to-bree,bree-to-weathertop" />',
    'REDIRECT_UNIT':   '<label>New Path IDs (comma-separated)</label><input id="param-newPathIds" class="input-field" placeholder="shire-to-tharbad,tharbad-to-fords-of-isen" />',
    'BLOCK_PATH':      '<label>Path ID</label><input id="param-pathId" class="input-field" placeholder="lothlorien-to-emyn-muil" />',
    'SEARCH_PATH':     '<label>Path ID</label><input id="param-pathId" class="input-field" placeholder="bree-to-weathertop" />',
    'ATTACK_REGION':   '<label>Target Region ID</label><input id="param-targetRegion" class="input-field" placeholder="isengard" />',
    'REINFORCE_REGION':'<label>Target Region ID</label><input id="param-targetRegion" class="input-field" placeholder="edoras" />',
    'MAIA_ABILITY':    '<label>Target Path ID</label><input id="param-targetPathId" class="input-field" placeholder="fords-of-isen-to-edoras" />',
    'DEPLOY_NAZGUL':   '<label>Target Region ID</label><input id="param-targetRegion" class="input-field" placeholder="bree" />',
    'FORTIFY_REGION':  '<p class="muted">Fortifies current region. No params needed.</p>',
    'DESTROY_RING':    '<p class="muted">Destroys the Ring at Mount Doom. No params needed.</p>',
  };

  params.innerHTML = paramTemplates[orderType] || '';
}

// ===================== SUBMIT ORDER =====================
async function submitOrder() {
  if (!state.selectedUnitID) { showToast('No unit selected', 'error'); return; }

  const orderType = document.getElementById('order-type').value;
  const payload = buildPayload(orderType);

  const order = {
    orderType,
    playerId: state.playerID,
    unitId:   state.selectedUnitID,
    turn:     state.turn,
    payload,
  };

  try {
    const res = await fetch(`${state.serverURL}/order`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(order),
    });

    if (res.status === 202) {
      showToast(`Order submitted: ${formatOrderName(orderType)}`, 'success');
      logEvent(`📤 ${state.units[state.selectedUnitID]?.name}: ${formatOrderName(orderType)}`, 'movement');
    } else {
      const err = await res.json().catch(() => ({}));
      showToast(`Order rejected: ${err.errorCode || res.status}`, 'error');
    }
  } catch (e) {
    showToast('Demo mode — order noted locally', 'info');
    logEvent(`📤 ${state.units[state.selectedUnitID]?.name}: ${formatOrderName(orderType)} (demo)`, 'movement');
  }
}

function buildPayload(orderType) {
  const get = id => document.getElementById(id)?.value || '';
  switch (orderType) {
    case 'ASSIGN_ROUTE':    return { pathIds: get('param-pathIds').split(',').map(s=>s.trim()).filter(Boolean) };
    case 'REDIRECT_UNIT':   return { newPathIds: get('param-newPathIds').split(',').map(s=>s.trim()).filter(Boolean) };
    case 'BLOCK_PATH':
    case 'SEARCH_PATH':     return { pathId: get('param-pathId') };
    case 'ATTACK_REGION':
    case 'REINFORCE_REGION':
    case 'DEPLOY_NAZGUL':   return { targetRegion: get('param-targetRegion') };
    case 'MAIA_ABILITY':    return { targetPathId: get('param-targetPathId') };
    default:                return {};
  }
}

// ===================== ANALYSIS =====================
async function requestAnalysis() {
  const ph = document.getElementById('analysis-placeholder');
  const content = document.getElementById('analysis-content');
  ph.textContent = '⏳ Running analysis…';

  const endpoint = state.side === 'light' ? '/analysis/routes' : '/analysis/intercept';

  try {
    const res = await fetch(`${state.serverURL}${endpoint}?playerId=${encodeURIComponent(state.playerID)}`);
    const data = await res.json();
    renderAnalysis(data);
  } catch {
    // Demo analysis output.
    renderDemoAnalysis();
  }
}

function renderAnalysis(data) {
  const content = document.getElementById('analysis-content');
  const ph = document.getElementById('analysis-placeholder');
  ph.classList.add('hidden');
  content.classList.remove('hidden');

  if (state.side === 'light' && data.routes) {
    content.innerHTML = data.routes.map((r, i) => `
      <div class="route-item${i === data.recommended ? ' recommended' : ''}">
        <strong>Route ${i+1}</strong> <span class="route-score">Risk: ${r.riskScore}</span>
        ${i === data.recommended ? '<span style="color:var(--green)"> ★ Recommended</span>' : ''}
        ${r.warnings?.length ? `<div class="route-warnings">⚠️ ${r.warnings.join(', ')}</div>` : ''}
      </div>
    `).join('');
  } else if (state.side === 'dark' && data.byUnit) {
    content.innerHTML = data.byUnit.map(r => `
      <div class="intercept-item">
        <span>${r.unitId} → ${r.targetRegion}</span>
        <span style="color:var(--gold)">Score: ${r.score.toFixed(2)}</span>
      </div>
    `).join('');
  }
}

function renderDemoAnalysis() {
  const content = document.getElementById('analysis-content');
  const ph = document.getElementById('analysis-placeholder');
  ph.classList.add('hidden');
  content.classList.remove('hidden');

  if (state.side === 'light') {
    content.innerHTML = `
      <div class="route-item recommended"><strong>Route 2 — Northern Bypass</strong><span class="route-score">Risk: 12</span><span style="color:var(--green)"> ★ Recommended</span></div>
      <div class="route-item"><strong>Route 1 — Fellowship</strong><span class="route-score">Risk: 18</span></div>
      <div class="route-item"><strong>Route 4 — Southern Corridor</strong><span class="route-score">Risk: 24</span><div class="route-warnings">⚠️ Saruman-corrupted paths</div></div>
      <div class="route-item"><strong>Route 3 — Dark Route</strong><span class="route-score">Risk: 30</span><div class="route-warnings">⚠️ Multiple BLOCKED paths</div></div>
    `;
  } else {
    content.innerHTML = `
      <div class="intercept-item"><span>witch-king → emyn-muil</span><span style="color:var(--gold)">Score: 0.87</span></div>
      <div class="intercept-item"><span>nazgul-2 → lothlorien</span><span style="color:var(--gold)">Score: 0.65</span></div>
      <div class="intercept-item"><span>nazgul-3 → bree</span><span style="color:var(--gold)">Score: 0.41</span></div>
    `;
  }
}

// ===================== MAP MARKERS =====================
// Region positions on the SVG map (approximate, tuned for MiddleEarthMap.svg).
const REGION_POSITIONS = {
  'the-shire':     { x: 8,  y: 18 },
  'bree':          { x: 22, y: 20 },
  'tharbad':       { x: 18, y: 35 },
  'weathertop':    { x: 30, y: 22 },
  'rivendell':     { x: 38, y: 18 },
  'fangorn':       { x: 28, y: 45 },
  'fords-of-isen': { x: 20, y: 48 },
  'rohan-plains':  { x: 35, y: 52 },
  'moria':         { x: 42, y: 32 },
  'helms-deep':    { x: 22, y: 58 },
  'isengard':      { x: 25, y: 52 },
  'edoras':        { x: 38, y: 60 },
  'lothlorien':    { x: 48, y: 40 },
  'dead-marshes':  { x: 58, y: 50 },
  'emyn-muil':     { x: 55, y: 42 },
  'minas-tirith':  { x: 52, y: 65 },
  'ithilien':      { x: 62, y: 60 },
  'osgiliath':     { x: 60, y: 68 },
  'minas-morgul':  { x: 68, y: 70 },
  'cirith-ungol':  { x: 72, y: 60 },
  'mordor':        { x: 78, y: 72 },
  'mount-doom':    { x: 82, y: 78 },
};

function renderMapMarkers() {
  const container = document.getElementById('unit-markers');
  container.innerHTML = '';

  const unitsByRegion = {};
  Object.values(state.units).forEach(u => {
    if (!u.currentRegion) return;
    if (!unitsByRegion[u.currentRegion]) unitsByRegion[u.currentRegion] = [];
    unitsByRegion[u.currentRegion].push(u);
  });

  Object.entries(unitsByRegion).forEach(([regionID, units]) => {
    const pos = REGION_POSITIONS[regionID];
    if (!pos) return;

    units.forEach((u, i) => {
      const marker = document.createElement('div');
      const isRingBearer = u.class === 'RingBearer';
      const isLight = u.side === 'FREE_PEOPLES';
      marker.className = `unit-marker ${isRingBearer ? 'ring-bearer' : isLight ? 'light' : 'dark'}`;

      const offsetX = (i % 3) * 14 - 14;
      const offsetY = Math.floor(i / 3) * 14;
      marker.style.left = `${pos.x + offsetX / 5}%`;
      marker.style.top  = `${pos.y + offsetY / 5}%`;
      marker.title = `${u.name} (${u.strength}⚔️)`;
      marker.textContent = isRingBearer ? '💍' : (isLight ? '⚔' : '👁');
      marker.addEventListener('click', () => selectUnit(u.id));
      container.appendChild(marker);
    });
  });
}

// ===================== TIMER =====================
function startTurnTimer() {
  clearInterval(state.timerInterval);
  state.timerRemaining = 60;
  updateTimerBar();

  state.timerInterval = setInterval(() => {
    state.timerRemaining--;
    updateTimerBar();
    if (state.timerRemaining <= 0) {
      clearInterval(state.timerInterval);
      logEvent('⏰ Turn ended', 'system');
    }
  }, 1000);
}

function updateTimerBar() {
  const pct = (state.timerRemaining / 60) * 100;
  document.getElementById('timer-bar').style.width = pct + '%';
}

// ===================== GAME OVER =====================
function showGameOver(winner, cause) {
  const overlay = document.getElementById('game-over-overlay');
  const title = document.getElementById('game-over-title');
  const desc = document.getElementById('game-over-desc');
  const icon = document.getElementById('game-over-icon');

  overlay.classList.remove('hidden');

  if (winner === 'FREE_PEOPLES') {
    icon.textContent = '💍';
    title.textContent = 'The Ring is Destroyed!';
    title.style.color = 'var(--light-blue)';
    desc.textContent = 'The Light Side wins. Frodo has destroyed the One Ring in the fires of Mount Doom.';
  } else if (winner === 'SHADOW') {
    icon.textContent = '👁️';
    title.textContent = 'The Shadow Falls!';
    title.style.color = 'var(--crimson-bright)';
    desc.textContent = 'The Dark Side wins. The Ring Bearer has been captured by the Nazgul.';
  } else {
    icon.textContent = '⚖️';
    title.textContent = 'A Draw';
    desc.textContent = `40 turns passed with no winner. Cause: ${cause}`;
  }
}

// ===================== UTILITIES =====================
function updateSideLabel() {
  const label = document.getElementById('side-label');
  if (state.side === 'light') {
    label.textContent = '⚔️ Light Side';
    label.style.color = 'var(--light-blue)';
  } else {
    label.textContent = '👁️ Dark Side';
    label.style.color = 'var(--crimson-bright)';
  }
  document.getElementById('player-label').textContent = state.playerID;
}

function setStatus(s) {
  const dot = document.getElementById('status-dot');
  const lbl = document.getElementById('status-label');
  dot.className = `status-dot ${s}`;
  lbl.textContent = { connecting: 'Connecting…', connected: 'Connected', error: 'Disconnected' }[s] || s;
}

function logEvent(msg, type = 'system') {
  const entries = document.getElementById('event-entries');
  const div = document.createElement('div');
  div.className = `event-entry ${type}`;
  const t = new Date().toLocaleTimeString('en-GB', { hour12: false });
  div.textContent = `[${t}] ${msg}`;
  entries.prepend(div);
  // Keep max 50 entries.
  while (entries.children.length > 50) entries.lastChild.remove();
}

function showToast(msg, type = 'info') {
  const area = document.getElementById('toast-area');
  const t = document.createElement('div');
  t.className = `toast ${type}`;
  t.textContent = msg;
  area.appendChild(t);
  setTimeout(() => t.remove(), 4000);
}
