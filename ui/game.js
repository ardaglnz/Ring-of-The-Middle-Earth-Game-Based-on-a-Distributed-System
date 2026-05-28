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
  hasSubmittedOrder: false,
  builtRoute: [],
  builtRouteRegions: [], // cached for visual preview
  zoomLevel: 1,
  panX: 0,
  panY: 0,
  isPanning: false,
  startX: 0,
  startY: 0,
  lastDetectedRegion: null, // Dark side: last seen ring-bearer region
  lastDetectedTurn: 0,
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
// ===================== CONNECTION =====================
function showHowToPlayModal() {
  const modal = document.getElementById('how-to-play-modal');
  const victoryDesc = document.getElementById('how-to-play-victory-desc');
  const tipsTitle = document.getElementById('how-to-play-tips-title');
  const tipsList = document.getElementById('how-to-play-tips-list');

  if (state.side === 'light') {
    victoryDesc.innerHTML =
      'Move <strong>Frodo</strong> from <strong>The Shire</strong> to <strong>Mount Doom</strong>. ' +
      'To win, end a turn with Frodo at Mount Doom, with <strong>no Shadow unit there</strong>, ' +
      'and submit <code>DESTROY_RING</code> on that same turn. ' +
      'Only YOU see Frodo\'s real position — keep it that way.';
    tipsTitle.textContent = '💡 Light Side Tips';
    tipsList.innerHTML = `
      <li>Plan Frodo\'s entire route up front — he advances one step per turn automatically.</li>
      <li>Escort Frodo with <strong>Fellowship Guards</strong> (Aragorn, Legolas, Gimli) along the path endpoints — they prevent Nazgul from blocking the route.</li>
      <li>If a path is BLOCKED, move <strong>Gandalf</strong> adjacent and use <code>MAIA_ABILITY</code> to open it for 2 turns.</li>
      <li>Keep the <strong>Gondor Army</strong> at Minas Tirith and <code>FORTIFY_REGION</code> — Uruk-hai alone can\'t breach it.</li>
      <li>Use the 🔮 analysis to compare the 4 canonical routes by risk score.</li>
      <li>If the northern corridor is threatened, switch Frodo to the southern corridor via Tharbad.</li>
      <li>On the turn you expect Frodo to arrive at Mount Doom, submit <code>DESTROY_RING</code> at the same time — auto-advance happens before the win check.</li>
    `;
  } else {
    victoryDesc.innerHTML =
      'Find <strong>Frodo</strong> and corner him before he reaches Mount Doom. ' +
      'To win, end a turn with a <strong>Nazgul co-located with Frodo</strong> AND Frodo <strong>exposed</strong> ' +
      '(within Nazgul detection range OR crossing a surveilled path). ' +
      'Detection is suppressed for the first 3 turns.';
    tipsTitle.textContent = '💡 Shadow Tips';
    tipsList.innerHTML = `
      <li>Position <strong>Nazgul 2 & 3</strong> at chokepoints (Bree, Lothlórien, Emyn Muil) <em>before turn 4</em> — that\'s when detection kicks in.</li>
      <li>Use <code>SEARCH_PATH</code> to raise surveillance on likely Frodo paths. Crossing a surveilled path exposes him for that turn.</li>
      <li>When a detection event fires, race the <strong>Witch-King</strong> (range 2) to that region — he\'s indestructible.</li>
      <li>Saruman should <code>MAIA_ABILITY</code> a Route 4 path early (e.g. <code>fords-of-isen-to-edoras</code>) — permanent surveillance.</li>
      <li>Keep <strong>Sauron in Mordor</strong> — his passive Eye gives every Nazgul +1 detection range.</li>
      <li>Use the 🔮 analysis to see intercept scores for each Nazgul.</li>
      <li>To break a fortified Minas Tirith, attack with the <strong>Witch-King + Uruk-hai together</strong> — neither alone is enough.</li>
    `;
  }

  modal.classList.remove('hidden');
}

async function closeHowToPlayModal() {
  document.getElementById('how-to-play-modal').classList.add('hidden');
  
  // Show game screen.
  document.getElementById('login-screen').classList.remove('active');
  document.getElementById('game-screen').classList.add('active');

  updateSideLabel();
  setStatus('connecting');

  // Start the game on the server.
  try {
    await fetch(`${state.serverURL}/game/start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode: 'HVH' }),
    });
  } catch (e) {
    // Server might not be running — continue with demo mode.
  }

  // Connect SSE.
  connectSSE();

  // Fetch initial state.
  fetchGameState();
}

async function connect() {
  const url = document.getElementById('server-url').value.trim();
  const pid = document.getElementById('player-id').value.trim();

  if (!pid) { showToast('Enter a Player ID', 'error'); return; }
  if (!state.side) { showToast('Choose a side first', 'error'); return; }

  state.serverURL = url;
  state.playerID = pid;

  // Show how to play popup before entering.
  showHowToPlayModal();
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
    const where = friendlyRegion(data.regionId);
    logEvent(`🔴 Ring Bearer DETECTED at ${where}!`, 'detection');
    showToast(`Ring Bearer detected at ${where}!`, 'error');
    // Dark Side: remember last seen position to show a ghost marker on the map.
    state.lastDetectedRegion = data.regionId;
    state.lastDetectedTurn = state.turn;
    renderMapMarkers();
  }
  if (data.type === 'RingBearerMoved') {
    // Light Side only.
    logEvent(`💍 Ring Bearer moved to ${friendlyRegion(data.trueRegion)}`, 'movement');
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
    if (data.turn !== state.turn) {
      state.hasSubmittedOrder = false;
      state.builtRoute = []; // clear route builder on new turn
      const btn = document.getElementById('btn-fast-forward');
      if (btn) {
        btn.disabled = false;
        btn.style.opacity = '1';
        btn.title = "Fast Forward Turn";
      }
    }
    state.turn = data.turn;
    document.getElementById('turn-number').textContent = data.turn;
    
    // Update turn-side badge dynamically.
    const sideBadge = document.getElementById('turn-side-badge');
    if (sideBadge) {
      if (data.turn <= 2) {
        sideBadge.textContent = "Setup Phase — Both Sides Planning";
      } else {
        sideBadge.textContent = "Simultaneous Turn Phase — Both Sides Active";
      }
    }
    
    // Update player planning status.
    updatePlayerPlanningStatus();
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
  if (data.paths) {
    if (Array.isArray(data.paths)) {
      data.paths.forEach(p => { state.paths[p.id] = p; });
    } else {
      state.paths = data.paths;
    }
  }
  renderUnits();
  renderMapMarkers();
  startTurnTimer();
}

function updatePlayerPlanningStatus() {
  const statusBadge = document.getElementById('player-turn-status');
  if (statusBadge) {
    if (state.hasSubmittedOrder) {
      statusBadge.textContent = "WAITING FOR OPPONENT... ⏳";
      statusBadge.className = "player-turn-status ready";
    } else {
      statusBadge.textContent = "YOUR TURN TO PLAN (Simultaneous) ✍️";
      statusBadge.className = "player-turn-status planning";
    }
  }
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
function formatUnitName(name) {
  if (!name) return "";
  let n = name.split(',')[0].trim();
  const heroes = ["Frodo", "Aragorn", "Legolas", "Gimli", "Gandalf", "Saruman", "Sauron", "Boromir", "Sam", "Merry", "Pippin", "Gollum", "Galadriel", "Elrond", "Faramir"];
  for (let h of heroes) {
    if (n.startsWith(h)) return h;
  }
  return n;
}

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
      <div class="unit-name">${formatUnitName(u.name)}</div>
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

  document.querySelectorAll('.unit-marker').forEach(m => m.classList.remove('selected-marker'));
  const marker = document.getElementById(`marker-${unitID}`);
  if (marker) marker.classList.add('selected-marker');

  // Check if it's an enemy unit
  const isEnemy = (state.side === 'light' && u.side === 'SHADOW') || 
                  (state.side === 'dark' && u.side === 'FREE_PEOPLES');

  if (isEnemy) {
    document.getElementById('order-form').classList.add('hidden');
    document.getElementById('selected-unit-info').classList.remove('hidden');
    document.getElementById('selected-unit-info').innerHTML = `
      <p style="color: var(--crimson-bright); font-weight: bold;">Enemy Unit</p>
      <p>${formatUnitName(u.name)} (${u.strength}⚔️)</p>
      <p class="muted">You cannot issue orders to the opponent's forces.</p>
    `;
    return;
  }

  // Show order form for own unit.
  document.getElementById('selected-unit-info').classList.add('hidden');
  const form = document.getElementById('order-form');
  form.classList.remove('hidden');
  document.getElementById('sel-unit-name').textContent = formatUnitName(u.name);

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
  const u = state.units[state.selectedUnitID];
  if (!u) return;

  params.innerHTML = ''; // Clear params

  if (orderType === 'ASSIGN_ROUTE' || orderType === 'REDIRECT_UNIT') {
    state.builtRoute = [];
    renderRouteBuilder(orderType, u);
  } else if (orderType === 'BLOCK_PATH' || orderType === 'SEARCH_PATH') {
    // Filter paths connected to current region
    const connectedPaths = ALL_PATHS.filter(p => p.from === u.currentRegion || p.to === u.currentRegion);
    let options = connectedPaths.map(p => {
      const nextRegId = p.from === u.currentRegion ? p.to : p.from;
      const nextRegName = ALL_REGIONS.find(r => r.id === nextRegId)?.name || nextRegId;
      return `<option value="${p.id}">➡️ ${nextRegName}</option>`;
    }).join('');
    if (connectedPaths.length === 0) {
      options = ALL_PATHS.map(p => `<option value="${p.id}">${p.id}</option>`).join('');
    }
    params.innerHTML = `
      <div class="form-group">
        <label>Select Connected Path</label>
        <select id="param-pathId" class="input-field" onchange="highlightCurrentTarget()">
          <option value="" disabled selected>Select a path...</option>
          ${options}
        </select>
      </div>
    `;
  } else if (orderType === 'MAIA_ABILITY') {
    let maiaAbilityPaths = [];
    if (u.id === 'saruman') {
      maiaAbilityPaths = ["fangorn-to-isengard", "helms-deep-to-isengard", "fords-of-isen-to-isengard", "tharbad-to-fords-of-isen", "fords-of-isen-to-edoras"];
    }
    let options = "";
    if (maiaAbilityPaths.length > 0) {
      options = maiaAbilityPaths.map(pid => {
        const path = ALL_PATHS.find(p => p.id === pid) || { from: "?", to: "?" };
        const nextRegId = path.from === u.currentRegion ? path.to : path.from;
        const nextRegName = ALL_REGIONS.find(r => r.id === nextRegId)?.name || nextRegId;
        return `<option value="${pid}">➡️ ${nextRegName} (${pid})</option>`;
      }).join('');
    } else {
      options = ALL_PATHS.map(p => {
        const nextRegId = p.from === u.currentRegion ? p.to : p.from;
        const nextRegName = ALL_REGIONS.find(r => r.id === nextRegId)?.name || nextRegId;
        return `<option value="${p.id}">➡️ ${nextRegName} (${p.id})</option>`;
      }).join('');
    }
    params.innerHTML = `
      <div class="form-group">
        <label>Select Target Path</label>
        <select id="param-targetPathId" class="input-field" onchange="highlightCurrentTarget()">
          <option value="" disabled selected>Select a path...</option>
          ${options}
        </select>
      </div>
    `;
  } else if (orderType === 'ATTACK_REGION' || orderType === 'REINFORCE_REGION' || orderType === 'DEPLOY_NAZGUL') {
    const options = ALL_REGIONS.map(r => `<option value="${r.id}">${r.name}</option>`).join('');
    params.innerHTML = `
      <div class="form-group">
        <label>Select Target Region</label>
        <select id="param-targetRegion" class="input-field" onchange="highlightCurrentTarget()">
          <option value="" disabled selected>Select a region...</option>
          ${options}
        </select>
      </div>
    `;
  } else if (orderType === 'FORTIFY_REGION') {
    params.innerHTML = '<p class="muted">Fortifies current region. No params needed.</p>';
    highlightRegion(u.currentRegion);
  } else if (orderType === 'DESTROY_RING') {
    params.innerHTML = '<p class="muted">Destroys the Ring at Mount Doom. No params needed.</p>';
    highlightRegion('mount-doom');
  }

  // Clear previous highlights and trigger initial highlight if needed
  if (orderType !== 'FORTIFY_REGION' && orderType !== 'DESTROY_RING') {
    clearHighlights();
  }
}

// ===================== ROUTE BUILDER HELPERS =====================
function getAdjacentPaths(regionId) {
  if (!regionId) return [];
  return ALL_PATHS.filter(p => p.from === regionId || p.to === regionId).map(p => {
    const nextRegion = p.from === regionId ? p.to : p.from;
    const nextRegionName = ALL_REGIONS.find(r => r.id === nextRegion)?.name || nextRegion;
    return {
      pathId: p.id,
      nextRegion: nextRegion,
      nextRegionName: nextRegionName
    };
  });
}

function renderRouteBuilder(orderType, u) {
  const params = document.getElementById('order-params');
  let currentRegion = u.currentRegion;
  // Ring Bearer's true region is hidden — but for the Light Side it should be
  // populated from /game/state. As a fallback fall back to the-shire (turn 1 start).
  if (u.class === 'RingBearer' && !currentRegion) {
    currentRegion = 'the-shire';
  }

  // Render once; the rest is filled by updateRouteBuilder().
  const hiddenId = orderType === 'ASSIGN_ROUTE' ? 'param-pathIds' : 'param-newPathIds';
  params.innerHTML = `
    <input type="hidden" id="${hiddenId}" value="" />
    <div class="route-builder-container">
      <div class="route-builder-header">
        <span class="route-builder-title">Built route</span>
        <button type="button" class="route-builder-clear" onclick="clearBuiltRoute()">Clear</button>
      </div>
      <div id="route-builder-chips" class="route-builder-list">
        <span class="route-builder-empty">No steps yet</span>
      </div>
      <div class="route-builder-select-wrap">
        <select id="route-next-step" class="input-field"></select>
        <button type="button" class="btn-icon" onclick="appendRouteStep()" title="Add step">➕</button>
      </div>
      <div style="font-size:0.78rem; color:var(--text-muted);">
        Units auto-advance one step per turn — chain multiple paths for a full route.
      </div>
    </div>
  `;

  // Initialise built route from current state if we were in the middle of editing.
  state.builtRoute = [];
  state.builtRouteRegions = [currentRegion];
  updateRouteBuilder(hiddenId);
}

function currentRouteTip() {
  // Last region in the built chain.
  const arr = state.builtRouteRegions;
  return arr[arr.length - 1];
}

function updateRouteBuilder(hiddenId) {
  // Refresh the dropdown of next-step options based on the tip of the built chain.
  const tip = currentRouteTip();
  const adj = getAdjacentPaths(tip);
  const sel = document.getElementById('route-next-step');
  if (sel) {
    sel.innerHTML = `<option value="" disabled selected>Add step from ${friendlyRegion(tip)}…</option>` +
      adj.map(a => `<option value="${a.pathId}|${a.nextRegion}">➡️ ${a.nextRegionName}</option>`).join('');
  }

  const chips = document.getElementById('route-builder-chips');
  if (chips) {
    if (state.builtRoute.length === 0) {
      chips.innerHTML = `<span class="route-builder-empty">No steps yet — start from ${friendlyRegion(state.builtRouteRegions[0])}</span>`;
    } else {
      const parts = [`<span class="route-builder-chip">${friendlyRegion(state.builtRouteRegions[0])}</span>`];
      state.builtRoute.forEach((pid, i) => {
        parts.push('<span class="route-builder-arrow">→</span>');
        parts.push(`<span class="route-builder-chip" title="${pid}">${friendlyRegion(state.builtRouteRegions[i + 1])}</span>`);
      });
      chips.innerHTML = parts.join(' ');
    }
  }

  // Write to hidden input so submitOrder picks it up.
  const hidden = document.getElementById(hiddenId);
  if (hidden) {
    hidden.value = state.builtRoute.join(',');
  }

  // Repaint the preview line.
  redrawRoutePreview();
}

function appendRouteStep() {
  const sel = document.getElementById('route-next-step');
  if (!sel || !sel.value) return;
  const [pathId, nextRegion] = sel.value.split('|');
  state.builtRoute.push(pathId);
  state.builtRouteRegions.push(nextRegion);

  // Auto-detect hidden id from DOM.
  const hiddenId = document.getElementById('param-pathIds')
    ? 'param-pathIds'
    : 'param-newPathIds';
  updateRouteBuilder(hiddenId);
}

function clearBuiltRoute() {
  const u = state.units[state.selectedUnitID];
  if (!u) return;
  let currentRegion = u.currentRegion;
  if (u.class === 'RingBearer' && !currentRegion) currentRegion = 'the-shire';
  state.builtRoute = [];
  state.builtRouteRegions = [currentRegion];
  const hiddenId = document.getElementById('param-pathIds')
    ? 'param-pathIds'
    : 'param-newPathIds';
  updateRouteBuilder(hiddenId);
}

function friendlyRegion(id) {
  return ALL_REGIONS.find(r => r.id === id)?.name || id || '???';
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
      state.hasSubmittedOrder = true;
      // Reset transient route-builder state so the next order starts fresh.
      state.builtRoute = [];
      state.builtRouteRegions = [];
      updatePlayerPlanningStatus();
    } else {
      const err = await res.json().catch(() => ({}));
      const code = err.errorCode || res.status;
      const msg = err.errorMessage ? ` — ${err.errorMessage}` : '';
      showToast(`Order rejected: ${code}${msg}`, 'error');
    }
  } catch (e) {
    showToast('Demo mode — order noted locally', 'info');
    logEvent(`📤 ${state.units[state.selectedUnitID]?.name}: ${formatOrderName(orderType)} (demo)`, 'movement');
    state.hasSubmittedOrder = true;
    updatePlayerPlanningStatus();
  }
}

// ===================== FAST FORWARD =====================
async function requestFastForward() {
  const btn = document.getElementById('btn-fast-forward');
  if (btn) {
    btn.disabled = true;
    btn.style.opacity = '0.5';
    btn.title = "Waiting for opponent to fast forward...";
  }
  
  try {
    await fetch(`${state.serverURL}/game/fast-forward`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ playerId: state.playerID, turn: state.turn })
    });
    showToast("Fast forward requested. Waiting for opponent...", "info");
  } catch (e) {
    showToast("Server not responding for fast forward", "error");
  }
}

function buildPayload(orderType) {
  const get = id => document.getElementById(id)?.value || '';
  switch (orderType) {
    case 'ASSIGN_ROUTE': {
      const ids = state.builtRoute && state.builtRoute.length
        ? state.builtRoute.slice()
        : get('param-pathIds').split(',').map(s => s.trim()).filter(Boolean);
      return { pathIds: ids };
    }
    case 'REDIRECT_UNIT': {
      const ids = state.builtRoute && state.builtRoute.length
        ? state.builtRoute.slice()
        : get('param-newPathIds').split(',').map(s => s.trim()).filter(Boolean);
      return { newPathIds: ids };
    }
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

// Dynamic data constants for path selection and routing.
const ALL_REGIONS = [
  { id: "the-shire", name: "The Shire" },
  { id: "bree", name: "Bree" },
  { id: "tharbad", name: "Tharbad" },
  { id: "weathertop", name: "Weathertop" },
  { id: "rivendell", name: "Rivendell" },
  { id: "fangorn", name: "Fangorn" },
  { id: "fords-of-isen", name: "Fords of Isen" },
  { id: "rohan-plains", name: "Rohan Plains" },
  { id: "moria", name: "Moria" },
  { id: "helms-deep", name: "Helm's Deep" },
  { id: "isengard", name: "Isengard" },
  { id: "edoras", name: "Edoras" },
  { id: "lothlorien", name: "Lothlórien" },
  { id: "dead-marshes", name: "Dead Marshes" },
  { id: "emyn-muil", name: "Emyn Muil" },
  { id: "minas-tirith", name: "Minas Tirith" },
  { id: "ithilien", name: "Ithilien" },
  { id: "osgiliath", name: "Osgiliath" },
  { id: "minas-morgul", name: "Minas Morgul" },
  { id: "cirith-ungol", name: "Cirith Ungol" },
  { id: "mordor", name: "Mordor" },
  { id: "mount-doom", name: "Mount Doom" }
];

const ALL_PATHS = [
  { id: "shire-to-bree",              from: "the-shire",    to: "bree" },
  { id: "bree-to-weathertop",         from: "bree",         to: "weathertop" },
  { id: "bree-to-rivendell",          from: "bree",         to: "rivendell" },
  { id: "bree-to-tharbad",            from: "bree",         to: "tharbad" },
  { id: "shire-to-tharbad",           from: "the-shire",    to: "tharbad" },
  { id: "weathertop-to-rivendell",    from: "weathertop",   to: "rivendell" },
  { id: "rivendell-to-moria",         from: "rivendell",    to: "moria" },
  { id: "rivendell-to-lothlorien",    from: "rivendell",    to: "lothlorien" },
  { id: "moria-to-lothlorien",        from: "moria",        to: "lothlorien" },
  { id: "lothlorien-to-emyn-muil",    from: "lothlorien",   to: "emyn-muil" },
  { id: "lothlorien-to-rohan-plains", from: "lothlorien",   to: "rohan-plains" },
  { id: "rohan-plains-to-fangorn",    from: "rohan-plains", to: "fangorn" },
  { id: "rohan-plains-to-edoras",     from: "rohan-plains", to: "edoras" },
  { id: "rohan-plains-to-minas-tirith",from: "rohan-plains",to: "minas-tirith" },
  { id: "fangorn-to-isengard",        from: "fangorn",      to: "isengard" },
  { id: "isengard-to-rohan-plains",   from: "isengard",     to: "rohan-plains" },
  { id: "tharbad-to-fords-of-isen",   from: "tharbad",      to: "fords-of-isen" },
  { id: "fords-of-isen-to-isengard",  from: "fords-of-isen",to: "isengard" },
  { id: "fords-of-isen-to-helms-deep",from: "fords-of-isen",to: "helms-deep" },
  { id: "fords-of-isen-to-edoras",    from: "fords-of-isen",to: "edoras" },
  { id: "edoras-to-helms-deep",       from: "edoras",       to: "helms-deep" },
  { id: "helms-deep-to-isengard",     from: "helms-deep",   to: "isengard" },
  { id: "edoras-to-minas-tirith",     from: "edoras",       to: "minas-tirith" },
  { id: "emyn-muil-to-dead-marshes",  from: "emyn-muil",    to: "dead-marshes" },
  { id: "emyn-muil-to-ithilien",      from: "emyn-muil",    to: "ithilien" },
  { id: "dead-marshes-to-ithilien",   from: "dead-marshes", to: "ithilien" },
  { id: "dead-marshes-to-mordor",     from: "dead-marshes", to: "mordor" },
  { id: "ithilien-to-minas-tirith",   from: "ithilien",     to: "minas-tirith" },
  { id: "ithilien-to-osgiliath",      from: "ithilien",     to: "osgiliath" },
  { id: "ithilien-to-cirith-ungol",   from: "ithilien",     to: "cirith-ungol" },
  { id: "minas-tirith-to-osgiliath",  from: "minas-tirith", to: "osgiliath" },
  { id: "osgiliath-to-minas-morgul",  from: "osgiliath",    to: "minas-morgul" },
  { id: "minas-morgul-to-cirith-ungol",from: "minas-morgul",to: "cirith-ungol" },
  { id: "minas-morgul-to-mordor",     from: "minas-morgul", to: "mordor" },
  { id: "cirith-ungol-to-mordor",     from: "cirith-ungol", to: "mordor" },
  { id: "cirith-ungol-to-mount-doom", from: "cirith-ungol", to: "mount-doom" },
  { id: "mordor-to-mount-doom",       from: "mordor",       to: "mount-doom" }
];

function renderMapMarkers() {
  const container = document.getElementById('unit-markers');
  container.innerHTML = '';

  // Dark Side: ghost marker for the last-detected Ring Bearer region
  // (lingers for 2 turns after the detection so the player can react).
  if (state.side === 'dark' && state.lastDetectedRegion &&
      state.turn - state.lastDetectedTurn <= 2) {
    const pos = REGION_POSITIONS[state.lastDetectedRegion];
    if (pos) {
      const ghost = document.createElement('div');
      ghost.className = 'ring-bearer-ghost';
      ghost.title = `Last seen at ${friendlyRegion(state.lastDetectedRegion)} (turn ${state.lastDetectedTurn})`;
      ghost.style.left = `${pos.x}%`;
      ghost.style.top = `${pos.y}%`;
      ghost.textContent = '👁️?';
      container.appendChild(ghost);
    }
  }

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
      marker.id = `marker-${u.id}`;
      marker.className = `unit-marker ${isRingBearer ? 'ring-bearer' : isLight ? 'light' : 'dark'}`;

      // Arrange markers in a circle if there are multiple
      const total = units.length;
      let offsetX = 0;
      let offsetY = 0;
      if (total > 1) {
        const angle = (i / total) * Math.PI * 2 - (Math.PI / 2); // Start at top
        const radius = 22 + (total > 4 ? Math.floor(total/2)*4 : 0); // Dynamic radius
        offsetX = Math.cos(angle) * radius;
        offsetY = Math.sin(angle) * radius;
      }

      marker.style.left = `calc(${pos.x}% + ${offsetX}px)`;
      marker.style.top  = `calc(${pos.y}% + ${offsetY}px)`;
      
      marker.title = `${formatUnitName(u.name)} (${u.strength}⚔️)`;
      marker.textContent = isRingBearer ? '💍' : (isLight ? '⚔' : '👁');

      const label = document.createElement('div');
      label.className = 'marker-label';
      label.textContent = formatUnitName(u.name);
      marker.appendChild(label);

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

// ===================== MAP CONTROLS =====================
function setupMapControls() {
  const container = document.getElementById('map-container');
  if (!container) return;

  container.addEventListener('mousedown', (e) => {
    // Only drag with left click and when not clicking on a button or marker
    if (e.button !== 0 || e.target.closest('button') || e.target.closest('.unit-marker')) return;
    state.isPanning = true;
    state.startX = e.clientX - state.panX;
    state.startY = e.clientY - state.panY;
    container.style.cursor = 'grabbing';
  });

  window.addEventListener('mouseup', () => {
    state.isPanning = false;
    container.style.cursor = 'default';
  });

  window.addEventListener('mousemove', (e) => {
    if (!state.isPanning) return;
    e.preventDefault();
    state.panX = e.clientX - state.startX;
    state.panY = e.clientY - state.startY;
    updateMapTransform();
  });

  container.addEventListener('wheel', (e) => {
    e.preventDefault();
    const zoomDelta = e.deltaY < 0 ? 0.1 : -0.1;
    zoomMap(zoomDelta);
  });
}

function zoomMap(delta) {
  state.zoomLevel = Math.max(0.5, Math.min(3, state.zoomLevel + delta));
  updateMapTransform();
}

function resetMap() {
  state.zoomLevel = 1;
  state.panX = 0;
  state.panY = 0;
  updateMapTransform();
}

function updateMapTransform() {
  const wrapper = document.getElementById('map-wrapper');
  if (wrapper) {
    wrapper.style.transform = `translate(${state.panX}px, ${state.panY}px) scale(${state.zoomLevel})`;
  }
}

// ===================== HIGHLIGHTS =====================
function clearHighlights() {
  document.querySelectorAll('.target-highlight').forEach(el => el.remove());
}

function highlightRegion(regionId) {
  const pos = REGION_POSITIONS[regionId];
  if (!pos) return;
  const container = document.getElementById('unit-markers');
  const hl = document.createElement('div');
  hl.className = 'target-highlight';
  hl.style.left = `${pos.x}%`;
  hl.style.top = `${pos.y}%`;
  container.appendChild(hl);
}

function highlightPath(pathId) {
  const path = ALL_PATHS.find(p => p.id === pathId);
  if (!path) return;
  highlightRegion(path.from);
  highlightRegion(path.to);
}

function highlightCurrentTarget() {
  clearHighlights();
  const orderType = document.getElementById('order-type').value;
  
  if (orderType === 'ATTACK_REGION' || orderType === 'REINFORCE_REGION' || orderType === 'DEPLOY_NAZGUL') {
    const el = document.getElementById('param-targetRegion');
    if (el && el.value) highlightRegion(el.value);
  } else if (orderType === 'BLOCK_PATH' || orderType === 'SEARCH_PATH') {
    const el = document.getElementById('param-pathId');
    if (el && el.value) highlightPath(el.value);
  } else if (orderType === 'MAIA_ABILITY') {
    const el = document.getElementById('param-targetPathId');
    if (el && el.value) highlightPath(el.value);
  } else if (orderType === 'ASSIGN_ROUTE' || orderType === 'REDIRECT_UNIT') {
    const el = document.getElementById(orderType === 'ASSIGN_ROUTE' ? 'param-pathIds' : 'param-newPathIds');
    if (el && el.value) highlightPath(el.value);
  }
}

// ===================== ROUTE PREVIEW (panel-only) =====================
// Canvas-based map overlays were removed because they fight with the SVG map's
// own path rendering and produce misaligned lines under zoom/pan. The route
// chips in the order panel already show the planned path clearly.
function redrawRoutePreview() { /* no-op */ }

function isOwnUnit(u) {
  return (state.side === 'light' && u.side === 'FREE_PEOPLES') ||
         (state.side === 'dark' && u.side === 'SHADOW');
}

// Initialize map controls on load
document.addEventListener('DOMContentLoaded', setupMapControls);
