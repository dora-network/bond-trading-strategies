// dora-agent chat UI — minimal client for testing the HTTP API.
//
// Persists baseUrl + apiKey in localStorage. Streams SSE deltas inline
// using fetch + ReadableStream (EventSource can't POST, and we want
// to send a JSON body).

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

const state = {
  baseUrl: localStorage.getItem('dora-agent.baseUrl') || 'http://127.0.0.1:8081',
  apiKey: localStorage.getItem('dora-agent.apiKey') || '',
  sessions: [],
  activeSessionId: null,
  activeAbort: null,
  activeStrategyId: null,
  activeStrategyName: '',
  activeStrategyHeadRevision: '',
  deployments: [],
  activeDeploymentId: '',
  strategyVersions: [],
  selectedRevisionId: '',
  manifestRaw: '',
  manifestOrderBooks: [],
  manifestResolutions: [],
  manifestParamsSchema: {},
  deploymentRequestVersion: 0,
  deploymentBusy: false,
};

// --- Model catalog ------------------------------------------------------
//
// Per-provider model lists, sourced directly from the OpenAPI spec's
// OpenAIModel, AnthropicModel, and OpenRouterModel enums. The UI does no
// prefix logic: openai and anthropic take bare family names; openrouter
// uses the prefixed `provider/model` form. The merged Model enum lists
// every accepted value (external docs browse that for the union).
//
// If the spec is unreachable at startup, the dropdowns fall back to an
// empty list and the user sees the error — better than hard-coding a
// stale list that diverges from what the server actually accepts.
const MODEL_CATALOG = { openai: [], anthropic: [], openrouter: [] };
const OPENROUTER_FREE_MODELS = [];

async function loadModelCatalog() {
  try {
    const spec = await fetch(state.baseUrl + '/v1/openapi').then((r) => {
      if (!r.ok) throw new Error(`${r.status}: ${r.statusText}`);
      return r.json();
    });
    const schemas = (spec.components && spec.components.schemas) || {};
    MODEL_CATALOG.openai     = (schemas.OpenAIModel     && schemas.OpenAIModel.enum)     || [];
    MODEL_CATALOG.anthropic  = (schemas.AnthropicModel  && schemas.AnthropicModel.enum)  || [];
    MODEL_CATALOG.openrouter = (schemas.OpenRouterModel && schemas.OpenRouterModel.enum) || [];
    // OpenRouterFreeModel is a separate enum in the spec; we merge it
    // into the openrouter dropdown at render time so the user sees
    // free + paid together with a visual separator. The free IDs
    // are still valid OpenRouterModel entries (they share the merged
    // Model enum on the wire), so passing one through is server-OK.
    const free = (schemas.OpenRouterFreeModel && schemas.OpenRouterFreeModel.enum) || [];
    OPENROUTER_FREE_MODELS.length = 0;
    OPENROUTER_FREE_MODELS.push(...free);
  } catch (err) {
    flashError('load model catalog', err);
  }
}

// populateModels rebuilds a model <select> for the given provider.
// Provider -> list mapping is exactly what the OpenAPI spec exposes;
// no transformation, no slug derivation. For provider=openrouter we
// merge in the free-tier list (OpenRouterFreeModel) with an optgroup
// separator so users can see free + paid in one place without the UI
// deciding which to show.
function populateModels(selectEl, provider) {
  selectEl.innerHTML = '';
  const list = MODEL_CATALOG[provider] || [];
  for (const m of list) {
    const opt = document.createElement('option');
    opt.value = m;
    opt.textContent = m;
    selectEl.appendChild(opt);
  }
  if (provider === 'openrouter' && OPENROUTER_FREE_MODELS.length > 0) {
    const sep = document.createElement('option');
    sep.disabled = true;
    sep.textContent = '── free tier ──';
    selectEl.appendChild(sep);
    for (const m of OPENROUTER_FREE_MODELS) {
      const opt = document.createElement('option');
      opt.value = m;
      opt.textContent = m;
      selectEl.appendChild(opt);
    }
  }
}

// --- Persistence ---------------------------------------------------------

function saveAuth() {
  state.baseUrl = $('#baseUrl').value.trim().replace(/\/$/, '') || state.baseUrl;
  state.apiKey = $('#apiKey').value.trim();
  localStorage.setItem('dora-agent.baseUrl', state.baseUrl);
  localStorage.setItem('dora-agent.apiKey', state.apiKey);
  flash(`saved: ${state.baseUrl}`);
  refreshSessions();
}

function loadAuth() {
  $('#baseUrl').value = state.baseUrl;
  $('#apiKey').value = state.apiKey;
  $('#currentBaseUrl').textContent = state.baseUrl;
}

// --- Status panel --------------------------------------------------------

// flash surfaces a short status line at the top of the page. Used for
// "saved", "network error: ...", and the like. Replaces the previous
// console.log-only path so the user can see what failed without DevTools.
function flash(msg, kind) {
  const el = $('#status');
  el.textContent = msg;
  el.className = 'status ' + (kind || '');
  // Don't auto-clear errors — the user should see them.
  if (kind !== 'error') {
    setTimeout(() => { if (el.textContent === msg) el.textContent = ''; }, 5000);
  }
}

function flashError(prefix, err) {
  let msg = err && err.message ? err.message : String(err);
  // The fetch TypeError for cross-origin / DNS / refused-connection
  // arrives with a bare message; surface the most useful part.
  flash(`${prefix}: ${msg}`, 'error');
}

// --- HTTP ---------------------------------------------------------------

async function api(method, path, body) {
  const r = await fetch(state.baseUrl + path, {
    method,
    headers: {
      'Authorization': 'ApiKey ' + state.apiKey,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await r.text();
  if (!r.ok) {
    throw new Error(`${r.status}: ${text}`);
  }
  return text ? JSON.parse(text) : null;
}

// --- Sessions -----------------------------------------------------------

async function refreshSessions() {
  try {
    state.sessions = await api('GET', '/v1/agent/sessions') || [];
  } catch (e) {
    flashError('refreshSessions', e);
    state.sessions = [];
  }
  renderSessions();
}

function renderSessions() {
  const ul = $('#sessions');
  ul.innerHTML = '';
  for (const s of state.sessions) {
    const li = document.createElement('li');
    li.className = 'session' + (s.id === state.activeSessionId ? ' active' : '');
    li.innerHTML = `
      <div>
        <div>${escapeHtml(s.title || '(untitled)')}</div>
        <div class="meta">${escapeHtml(s.provider)}/${escapeHtml(s.model)}</div>
      </div>
    `;
    li.onclick = () => selectSession(s.id);
    ul.appendChild(li);
  }
}

async function selectSession(id) {
  if (state.activeAbort) state.activeAbort.abort();
  state.activeSessionId = id;
  // Invalidate any in-flight deployment responses from the prior session
  // and render the empty panel before the async loadMessages call resolves.
  state.deploymentRequestVersion++;
  clearDeploymentPanel();
  renderSessions();
  await loadMessages();
}

async function newSession(provider, model, title) {
  const body = { provider, model };
  if (title) body.title = title;
  const resp = await api('POST', '/v1/agent/sessions', body);
  state.activeSessionId = resp.session_id;
  // Invalidate any in-flight deployment responses from the prior session
  // and blank the old strategy panel before the async session load renders.
  state.deploymentRequestVersion++;
  clearDeploymentPanel();
  await refreshSessions();
  await loadMessages();
}

async function deleteSession() {
  if (!state.activeSessionId) return;
  if (!confirm('Delete this session?')) return;
  await api('DELETE', '/v1/agent/sessions/' + state.activeSessionId);
  state.activeSessionId = null;
  $('#messages').innerHTML = '';
  $('#chatTitle').textContent = 'no session';
  state.deploymentRequestVersion++;
  clearDeploymentPanel();
  await refreshSessions();
}

// --- Deployment panel --------------------------------------------------

// isCurrentDeploymentRequest returns true only when the given request
// version matches the latest selection generation. Every deployment
// fetch checks this after each await so a stale response from a
// previously selected session can never render.
function isCurrentDeploymentRequest(version) {
  return version === state.deploymentRequestVersion;
}

// clearDeploymentPanel resets all deployment/manifest state and
// re-renders the empty panel. Called on session change and delete.
function clearDeploymentPanel() {
  state.activeStrategyId = '';
  state.activeStrategyName = '';
  state.activeStrategyHeadRevision = '';
  state.deployments = [];
  state.activeDeploymentId = '';
  state.strategyVersions = [];
  state.selectedRevisionId = '';
  state.manifestRaw = '';
  state.manifestOrderBooks = [];
  state.manifestResolutions = [];
  state.manifestParamsSchema = {};
  // Clear any in-flight busy flag so a session switch/new/delete cannot
  // leave the new session's controls disabled by an old action.
  state.deploymentBusy = false;
  // Clear any stale inline error / manifest notice so a session switch
  // cannot retain errors from the prior session's deployment loads.
  const err = $('#deploymentError');
  if (err) { err.hidden = true; err.textContent = ''; }
  const notice = $('#manifestNotice');
  if (notice) { notice.hidden = true; notice.textContent = ''; }
  renderDeploymentPanel();
}

// isDeployableVersion filters the version list to go-wasm revisions
// that have both a wasm artifact and a manifest — the only ones the
// deploy endpoint accepts.
function isDeployableVersion(v) {
  return v && v.target === 'go-wasm' && v.wasm_ref && v.manifest_hash;
}

// chooseDeploymentRevision picks the revision the deploy form should
// show. Preference order: (1) the current selectedRevisionId if it is
// still in the version list (preserves a manual choice across refresh/
// stop/resume reloads); (2) the selected deployment's revision if it is
// in the version list (prefers a valid selected deployment); (3) the
// first available deployable revision. Session reset clears
// selectedRevisionId so a fresh panel starts from (2)/(3).
function chooseDeploymentRevision() {
  if (state.selectedRevisionId && state.strategyVersions.some((v) => v.revision === state.selectedRevisionId)) {
    return state.selectedRevisionId;
  }
  const selected = state.deployments.find((d) => d.deployment_id === state.activeDeploymentId);
  if (selected && state.strategyVersions.some((v) => v.revision === selected.revision)) {
    return selected.revision;
  }
  return state.strategyVersions.length ? state.strategyVersions[0].revision : '';
}

// loadDeploymentPanel takes the newest-first strategy array from the
// session response, uses the first entry, and fetches versions +
// deployments in parallel. Each await is followed by a generation
// check so a newer session selection aborts rendering.
async function loadDeploymentPanel(strategies) {
  // Clear any stale busy flag so a manual refresh during an in-flight
  // action can't strand the panel in a disabled state.
  state.deploymentBusy = false;
  const requestVersion = ++state.deploymentRequestVersion;
  if (!Array.isArray(strategies) || !strategies.length) {
    if (isCurrentDeploymentRequest(requestVersion)) clearDeploymentPanel();
    return;
  }
  const strategy = strategies[0];
  state.activeStrategyId = strategy.id;
  state.activeStrategyName = strategy.name || strategy.id;
  state.activeStrategyHeadRevision = strategy.head_revision || '';
  state.deployments = [];
  state.strategyVersions = [];
  renderDeploymentPanel();
  try {
    const [versionsResp, deploymentsResp] = await Promise.all([
      api('GET', '/v1/agent/strategies/' + state.activeStrategyId + '/versions'),
      api('GET', '/v1/agent/strategies/' + state.activeStrategyId + '/deployments'),
    ]);
    if (!isCurrentDeploymentRequest(requestVersion)) return;
    state.strategyVersions = (versionsResp.versions || []).filter(isDeployableVersion);
    state.deployments = deploymentsResp.deployments || [];
    state.activeDeploymentId = state.deployments[0] ? state.deployments[0].deployment_id : '';
    state.selectedRevisionId = chooseDeploymentRevision();
    renderDeploymentPanel();
    if (state.activeDeploymentId) {
      await loadDeploymentDetail(state.activeDeploymentId, requestVersion);
    }
    if (state.selectedRevisionId) {
      await loadRevisionManifest(state.selectedRevisionId, requestVersion);
    }
  } catch (e) {
    if (isCurrentDeploymentRequest(requestVersion)) showDeploymentError(e);
  }
}

// loadDeploymentDetail fetches the full deployment record and splices
// it into the deployments array so the detail view shows fresh status.
async function loadDeploymentDetail(id, requestVersion) {
  try {
    const detail = await api('GET', '/v1/agent/strategies/' + state.activeStrategyId + '/deployments/' + id);
    if (!isCurrentDeploymentRequest(requestVersion)) return;
    const index = state.deployments.findIndex((d) => d.deployment_id === id);
    if (index >= 0) state.deployments[index] = detail;
    renderDeploymentPanel();
  } catch (e) {
    if (isCurrentDeploymentRequest(requestVersion)) showDeploymentError(e);
  }
}

// loadRevisionManifest fetches a version and hands its manifest.json
// file content to the manifest renderer.
async function loadRevisionManifest(revisionId, requestVersion) {
  try {
    const detail = await api('GET', '/v1/agent/strategies/' + state.activeStrategyId + '/versions/' + revisionId);
    if (!isCurrentDeploymentRequest(requestVersion)) return;
    renderDeploymentManifest(detail.files && detail.files['manifest.json']);
  } catch (e) {
    if (isCurrentDeploymentRequest(requestVersion)) showDeploymentError(e);
  }
}

// --- Deployment panel rendering ----------------------------------------
//
// parseManifest is the safe JSON.parse fallback the manifest loader
// calls; renderDeploymentManifest stores the parsed result;
// renderDeploymentPanel populates text spans, selects, and lifecycle
// buttons from state; showDeploymentError surfaces failures without
// alert(). Manifest-derived inputs, deploy submit, and stop/start
// actions are implemented below (full contracts).

function parseManifest(raw) {
  if (!raw) return { available: false, orderBooks: [], resolutions: [], params: {} };
  try {
    const manifest = JSON.parse(raw);
    const capabilities = (manifest && manifest.capabilities) || {};
    const hasParams = manifest && manifest.params_schema && typeof manifest.params_schema === 'object';
    // The notice ("Manifest unavailable; only order book and resolution
    // are available") shows when JSON is malformed OR params_schema is
    // absent/non-object. Order book/resolution text inputs still render
    // from capabilities when present — only the notice visibility flips.
    return {
      available: hasParams,
      orderBooks: Array.isArray(capabilities.order_books) ? capabilities.order_books : [],
      resolutions: Array.isArray(capabilities.resolutions) ? capabilities.resolutions : [],
      params: hasParams ? manifest.params_schema : {},
    };
  } catch (_) {
    return { available: false, orderBooks: [], resolutions: [], params: {} };
  }
}

function renderDeploymentManifest(raw) {
  const parsed = parseManifest(raw);
  state.manifestRaw = raw || '';
  state.manifestOrderBooks = parsed.orderBooks;
  state.manifestResolutions = parsed.resolutions;
  state.manifestParamsSchema = parsed.params;
  const notice = $('#manifestNotice');
  if (notice) {
    notice.hidden = parsed.available;
    notice.textContent = parsed.available
      ? ''
      : 'Manifest unavailable; only order book and resolution are available';
  }
  renderDeploymentPanel();
}

function valueFromControl(container) {
  const control = container.querySelector('input, select');
  return control ? String(control.value).trim() : '';
}

// addRequiredSelect rebuilds container with a required <select> whose
// options are the manifest-declared values. A placeholder "select …"
// option (value "", selected) forces an explicit choice — no default.
function addRequiredSelect(container, labelText, values) {
  const label = document.createElement('label');
  const text = document.createElement('span');
  text.textContent = labelText;
  label.appendChild(text);
  const select = document.createElement('select');
  select.required = true;
  select.append(new Option('select ' + labelText, '', true, true));
  for (const value of values) select.append(new Option(String(value), String(value)));
  label.appendChild(select);
  container.replaceChildren(label);
}

// addRequiredText rebuilds container with a required text input.
// Used when the manifest offers no option set for order book / resolution.
function addRequiredText(container, labelText) {
  const label = document.createElement('label');
  const text = document.createElement('span');
  text.textContent = labelText;
  const input = document.createElement('input');
  input.required = true;
  input.placeholder = labelText;
  label.append(text, input);
  container.replaceChildren(label);
}

// addDeploymentParam appends one manifest parameter field. Type picks
// the control: int/float -> number, bool -> checkbox, else text. No
// default value is set on any field.
function addDeploymentParam(container, name, type) {
  const label = document.createElement('label');
  label.className = 'param-field';
  const text = document.createElement('span');
  text.textContent = name;
  const input = document.createElement('input');
  input.dataset.paramName = name;
  input.dataset.paramType = type;
  if (type === 'int' || type === 'float') input.type = 'number';
  if (type === 'bool') input.type = 'checkbox';
  label.append(text, input);
  container.appendChild(label);
}

// captureDeployFormValues snapshots the current deploy-form control
// values so they survive a manifest-input rebuild. Returns null when
// the controls aren't present yet (first render). Bool params store
// checked state separately from the textual value.
function captureDeployFormValues() {
  if (!$('#deployOrderBookControl') || !$('#deployResolutionControl')) return null;
  const orderBookCtl = valueFromControl($('#deployOrderBookControl'));
  const resolutionCtl = valueFromControl($('#deployResolutionControl'));
  const params = {};
  for (const input of $('#deployParams').querySelectorAll('input[data-param-name]')) {
    params[input.dataset.paramName] = {
      value: String(input.value),
      checked: input.checked,
    };
  }
  return { orderBook: orderBookCtl, resolution: resolutionCtl, params };
}

// restoreDeployFormValues writes a previously captured snapshot back
// onto the freshly rebuilt controls. Missing controls / params are
// skipped, so a manifest change between capture and restore is safe.
// Bool (checkbox) params restore via checked; others via value.
function restoreDeployFormValues(saved) {
  if (!saved) return;
  const ob = $('#deployOrderBookControl') && $('#deployOrderBookControl').querySelector('input, select');
  if (ob && saved.orderBook) ob.value = saved.orderBook;
  const res = $('#deployResolutionControl') && $('#deployResolutionControl').querySelector('input, select');
  if (res && saved.resolution) res.value = saved.resolution;
  for (const input of $('#deployParams').querySelectorAll('input[data-param-name]')) {
    const snap = saved.params[input.dataset.paramName];
    if (!snap) continue;
    if (input.type === 'checkbox') {
      input.checked = snap.checked;
    } else {
      input.value = snap.value;
    }
  }
}

// renderDeploymentManifestInputs builds the order-book, resolution, and
// params inputs from state. Order book / resolution use a <select> when
// the manifest declares values, otherwise a required text input. Params
// come from params_schema (name -> type string), sorted by name. User-
// entered values are captured before the rebuild and restored after so
// a busy-state re-render never wipes the form.
function renderDeploymentManifestInputs() {
  const orderBookCtl = $('#deployOrderBookControl');
  const resolutionCtl = $('#deployResolutionControl');
  if (!orderBookCtl || !resolutionCtl) return;
  const saved = captureDeployFormValues();
  if (state.manifestOrderBooks.length) {
    addRequiredSelect(orderBookCtl, 'order book', state.manifestOrderBooks);
  } else {
    addRequiredText(orderBookCtl, 'order book');
  }
  if (state.manifestResolutions.length) {
    addRequiredSelect(resolutionCtl, 'resolution', state.manifestResolutions);
  } else {
    addRequiredText(resolutionCtl, 'resolution');
  }
  const params = $('#deployParams');
  params.replaceChildren();
  const names = Object.keys(state.manifestParamsSchema).sort();
  for (const name of names) {
    const schema = state.manifestParamsSchema[name];
    // params_schema values are bare type strings (string/int/float/bool);
    // an object-valued manifest degrades to a text input, never treated
    // as a type itself.
    const type = schema && typeof schema === 'string' ? schema : '';
    addDeploymentParam(params, name, type);
  }
  restoreDeployFormValues(saved);
}

// renderDeploymentPanel writes deployment state into the sidebar DOM:
// detail text spans, deployment/version selects, lifecycle button
// enablement, and manifest-derived inputs (via renderDeploymentManifestInputs).
function renderDeploymentPanel() {
  if (!$('#deploymentPanel')) return;
  $('#deploymentStrategy').textContent = state.activeStrategyName || '—';
  $('#deploymentHeadRevision').textContent = state.activeStrategyHeadRevision || '—';
  const select = $('#deploymentSelect');
  select.replaceChildren();
  if (!state.deployments.length) {
    select.append(new Option('no deployment', ''));
  } else {
    for (const d of state.deployments) {
      select.append(new Option(`${d.revision} — ${d.status}`, d.deployment_id));
    }
  }
  select.value = state.activeDeploymentId;
  const detail = state.deployments.find((d) => d.deployment_id === state.activeDeploymentId);
  $('#deploymentStatus').textContent = detail ? detail.status : '—';
  $('#deploymentOrderBook').textContent = detail ? (detail.order_book_id || '—') : '—';
  $('#deploymentResolution').textContent = detail ? (detail.resolution || '—') : '—';
  $('#deploymentStartedAt').textContent = detail && detail.started_at
    ? formatTimestamp(detail.started_at) : '—';
  $('#deploymentRestartCount').textContent = detail ? String(detail.restart_count || 0) : '—';
  $('#deploymentStoppedReason').textContent = detail ? (detail.stopped_reason || '—') : '—';
  $('#stopDeployment').disabled = !(detail && detail.status === 'running' && !state.deploymentBusy);
  $('#startDeployment').disabled = !(detail && detail.status !== 'running' && !state.deploymentBusy);
  const versionSelect = $('#deployRevision');
  versionSelect.replaceChildren();
  for (const v of state.strategyVersions) {
    versionSelect.append(new Option(v.revision, v.revision));
  }
  versionSelect.value = state.selectedRevisionId;
  $('#deployButton').disabled = state.deploymentBusy || !state.selectedRevisionId;
  renderDeploymentManifestInputs();
}

// showDeploymentError surfaces a fetch/API failure in the panel's
// inline error span. Never uses alert(); never clears the form.
function showDeploymentError(err) {
  const el = $('#deploymentError');
  if (!el) return;
  el.hidden = false;
  el.textContent = err && err.message ? err.message : String(err);
}

// clearDeploymentError hides and empties the inline deployment error
// span. Called at the start of a deploy/stop/start attempt so a prior
// failure doesn't linger through a new action; form values are preserved.
function clearDeploymentError() {
  const el = $('#deploymentError');
  if (!el) return;
  el.hidden = true;
  el.textContent = '';
}

// submitDeployment collects required order book/resolution + nonblank
// params (bool serialized as "true"/"false") and POSTs the deploy route.
// On 202 it sets the active deployment, reloads the strategy panel, and
// flashes the returned deployment id. On failure the form is preserved
// and the error surfaces inline. deploymentBusy resets in finally.
async function submitDeployment(event) {
  event.preventDefault();
  clearDeploymentError();
  const requestVersion = state.deploymentRequestVersion;
  if (!state.activeStrategyId || !state.selectedRevisionId) {
    return showDeploymentError(new Error('select a deployable revision'));
  }
  const orderBook = valueFromControl($('#deployOrderBookControl'));
  const resolution = valueFromControl($('#deployResolutionControl'));
  if (!orderBook || !resolution) {
    return showDeploymentError(new Error('order_book_id and resolution are required'));
  }
  const params = {};
  for (const input of $('#deployParams').querySelectorAll('input[data-param-name]')) {
    const value = input.type === 'checkbox'
      ? (input.checked ? 'true' : 'false')
      : String(input.value);
    if (value !== '') params[input.dataset.paramName] = value;
  }
  state.deploymentBusy = true;
  renderDeploymentPanel();
  try {
    const response = await api('POST', '/v1/agent/strategies/' + state.activeStrategyId + '/versions/' + state.selectedRevisionId + '/deploy', {
      order_book_id: orderBook,
      resolution,
      params,
    });
    // If the session switched while the deploy request was in flight,
    // do not reload — the new session owns the panel now.
    if (!isCurrentDeploymentRequest(requestVersion)) return;
    state.activeDeploymentId = response.deployment_id;
    // Clear busy and render enabled controls at this generation BEFORE
    // loadDeploymentPanel bumps it; the reload then refreshes the data.
    state.deploymentBusy = false;
    renderDeploymentPanel();
    await loadDeploymentPanel([{ id: state.activeStrategyId, name: state.activeStrategyName, head_revision: state.activeStrategyHeadRevision }]);
    flash('deployed ' + response.deployment_id);
  } catch (e) {
    // Stale errors must not cross a session switch: only surface the
    // failure if the panel still reflects the request that started it.
    if (!isCurrentDeploymentRequest(requestVersion)) return;
    // A 409 means the strategy already has an active deployment; show
    // the approved guidance instead of the raw status body.
    const msg = e && e.message ? e.message : String(e);
    if (msg.startsWith('409:')) {
      showDeploymentError(new Error('This strategy already has an active deployment; stop or select a different deployment workflow.'));
    } else {
      showDeploymentError(e);
    }
  } finally {
    // Reset busy and render only for the captured generation: a stale
    // action (session switched mid-action) skips both so it can't clear
    // a newer session/action's busy flag or render into the new panel.
    if (isCurrentDeploymentRequest(requestVersion)) {
      state.deploymentBusy = false;
      renderDeploymentPanel();
    }
  }
}

// runDeploymentAction calls the stop or resume route on the active
// deployment, then reloads the strategy panel. The caller (event
// handler) gates stop behind confirm(); start uses the /resume route.
async function runDeploymentAction(action) {
  if (!state.activeDeploymentId) return;
  clearDeploymentError();
  const requestVersion = state.deploymentRequestVersion;
  state.deploymentBusy = true;
  renderDeploymentPanel();
  try {
    const suffix = action === 'stop' ? '/stop' : '/resume';
    await api('POST', '/v1/agent/strategies/' + state.activeStrategyId + '/deployments/' + state.activeDeploymentId + suffix);
    // If the session switched while the action was in flight, do not
    // reload — the new session owns the panel now.
    if (!isCurrentDeploymentRequest(requestVersion)) return;
    // Clear busy and render enabled controls at this generation BEFORE
    // loadDeploymentPanel bumps it; the reload then refreshes the data.
    state.deploymentBusy = false;
    renderDeploymentPanel();
    await loadDeploymentPanel([{ id: state.activeStrategyId, name: state.activeStrategyName, head_revision: state.activeStrategyHeadRevision }]);
  } catch (e) {
    // Stale errors must not cross a session switch: only surface the
    // failure if the panel still reflects the request that started it.
    if (!isCurrentDeploymentRequest(requestVersion)) return;
    showDeploymentError(e);
  } finally {
    // Reset busy and render only for the captured generation: a stale
    // action skips both so it can't clear a newer session/action's busy
    // flag or render into the new panel.
    if (isCurrentDeploymentRequest(requestVersion)) {
      state.deploymentBusy = false;
      renderDeploymentPanel();
    }
  }
}

// --- Messages -----------------------------------------------------------

async function loadMessages() {
  if (!state.activeSessionId) return;
  const sessionID = state.activeSessionId;
  const resp = await api('GET', '/v1/agent/sessions/' + sessionID);
  if (state.activeSessionId !== sessionID) return;
  const meta = state.sessions.find((s) => s.id === state.activeSessionId);
  $('#chatTitle').textContent = meta
    ? `${meta.title || '(untitled)'} — ${meta.provider}/${meta.model}`
    : 'session';

  const messagesEl = $('#messages');
  messagesEl.innerHTML = '';
  for (const m of resp.messages) {
    appendMsg(m.role, m.content, m.role === 'assistant' ? 'assistant' : 'user', m.created_at);
  }
  // Render every strategy this session produced, newest first.
  // Each renders as a "view code" panel that fetches the head
  // version on demand. Makes historically captured strategies
  // recoverable even when the SSE strategy_saved event was
  // missed (binary restart, event missed, or capture happened
  // before the SSE wiring landed).
  if (Array.isArray(resp.strategies)) {
    for (const st of resp.strategies) {
      renderStrategyFiles(st.id, st.head_revision, st.name, st.summary || '', null);
    }
    await loadDeploymentPanel(resp.strategies);
  } else {
    await loadDeploymentPanel([]);
  }
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function appendMsg(role, content, type, createdAt) {
  const div = document.createElement('div');
  div.className = 'msg ' + type;
  const header = document.createElement('div');
  header.className = 'header';
  const roleEl = document.createElement('span');
  roleEl.className = 'role';
  roleEl.textContent = role;
  const tsEl = document.createElement('span');
  tsEl.className = 'timestamp';
  tsEl.textContent = formatTimestamp(createdAt || new Date());
  tsEl.title = tsEl.textContent; // hover for full RFC3339
  header.appendChild(roleEl);
  header.appendChild(tsEl);
  const body = document.createElement('div');
  body.className = 'body';
  if (type === 'assistant') {
    body.innerHTML = renderMarkdown(content);
    body.classList.add('markdown');
  } else {
    body.textContent = content;
  }
  div.appendChild(header);
  div.appendChild(body);
  $('#messages').appendChild(div);
  $('#messages').scrollTop = $("#messages").scrollHeight;
  return body;
}

// appendStreamingBubble creates an empty assistant bubble with an
// animated "thinking" indicator. The handleEvent('delta') path removes
// the indicator on the first SSE delta and appends text into the
// body instead. The indicator is the user's feedback that the agent
// is working and the response is in flight.
function appendStreamingBubble() {
  const div = document.createElement('div');
  div.className = 'msg assistant';
  const header = document.createElement('div');
  header.className = 'header';
  const roleEl = document.createElement('span');
  roleEl.className = 'role';
  roleEl.textContent = 'assistant';
  const tsEl = document.createElement('span');
  tsEl.className = 'timestamp';
  tsEl.textContent = formatTimestamp(new Date());
  tsEl.title = tsEl.textContent;
  header.appendChild(roleEl);
  header.appendChild(tsEl);
  const body = document.createElement('div');
  body.className = 'body';
  const thinking = document.createElement('span');
  thinking.className = 'thinking';
  thinking.setAttribute('aria-label', 'agent is thinking');
  thinking.appendChild(document.createTextNode('thinking'));
  const dots = document.createElement('span');
  dots.className = 'thinking-dots';
  for (let i = 0; i < 3; i++) {
    dots.appendChild(document.createElement('span'));
  }
  thinking.appendChild(dots);
  body.appendChild(thinking);
  div.appendChild(header);
  div.appendChild(body);
  $('#messages').appendChild(div);
  $('#messages').scrollTop = $("#messages").scrollHeight;
  return body;
}

// formatTimestamp returns a short local-time string (HH:MM:SS) for the
// message list. Hover the timestamp to see the full RFC3339 value.
function formatTimestamp(ts) {
  const d = ts instanceof Date ? ts : new Date(ts);
  if (Number.isNaN(d.getTime())) return '';
  return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

async function sendMessage(prompt) {
  if (!state.activeSessionId) return alert('Create or select a session first');
  if (!state.apiKey) return alert('Set Dora ApiKey first');

  appendMsg('user', prompt, 'user');
  const assistantBody = appendStreamingBubble();

  const ctrl = new AbortController();
  state.activeAbort = ctrl;

  let resp;
  try {
    resp = await fetch(state.baseUrl + '/v1/agent/sessions/' + state.activeSessionId + '/messages', {
      method: 'POST',
      headers: {
        'Authorization': 'ApiKey ' + state.apiKey,
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream',
      },
      body: JSON.stringify({ prompt }),
      signal: ctrl.signal,
    });
  } catch (err) {
    assistantBody.parentElement.remove();
    if (err.name === 'AbortError') return;
    flashError('send', err);
    return;
  }

  if (!resp.ok) {
    assistantBody.parentElement.remove();
    appendMsg('error', `${resp.status}: ${await resp.text()}`, 'error');
    state.activeAbort = null;
    return;
  }

  const reader = resp.body.getReader();
  const dec = new TextDecoder();
  let buf = '';
  let currentEvent = '';

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    let nl;
    while ((nl = buf.indexOf('\n')) !== -1) {
      const line = buf.slice(0, nl).replace(/\r$/, '');
      buf = buf.slice(nl + 1);
      if (line.startsWith('event: ')) {
        currentEvent = line.slice(7).trim();
      } else if (line.startsWith('data: ')) {
        const dataStr = line.slice(6);
        let payload = {};
        try { payload = JSON.parse(dataStr); } catch {}
        handleEvent(currentEvent, payload, assistantBody);
      } else if (line === '') {
        currentEvent = '';
      }
    }
  }
  // Stream complete: convert the accumulated raw text to rendered
  // markdown. During streaming we append via textContent (safe, no
  // injection); the fence is complete now so code blocks parse.
  // Skipped if refusal/error already removed the bubble.
  if (assistantBody.parentElement) {
    assistantBody.innerHTML = renderMarkdown(assistantBody.textContent);
    assistantBody.classList.add('markdown');
    $('#messages').scrollTop = $('#messages').scrollHeight;
  }
  state.activeAbort = null;
}

function handleEvent(name, payload, assistantBody) {
  if (name === 'delta' && payload.text) {
    // First delta: remove the thinking indicator and start writing
    // real text into the body. The indicator only survives until the
    // first byte of the response arrives.
    const thinking = assistantBody.querySelector('.thinking');
    if (thinking) {
      assistantBody.textContent = '';
    }
    assistantBody.textContent += payload.text;
    $('#messages').scrollTop = $('#messages').scrollHeight;
  } else if (name === 'tool_call') {
    appendMsg('tool_call', `${payload.name}(${JSON.stringify(payload.input)})`, 'tool_call');
  } else if (name === 'refusal') {
    assistantBody.parentElement.remove();
    appendMsg('refusal', payload.reason || 'classifier refused', 'refusal');
  } else if (name === 'error') {
    assistantBody.parentElement.remove();
    appendMsg('error', payload.message || 'unknown error', 'error');
  } else if (name === 'strategy_saved') {
    // Server captured a strategy version. Render a collapsible
    // panel that fetches the source files when expanded. Lazy
    // fetch keeps the SSE stream independent of payload size.
    renderStrategyFiles(payload.strategy_id, payload.revision,
      payload.module_name, payload.summary, assistantBody);
  } else if (name === 'done') {
    // stream end marker
  }
}

// renderStrategyFiles renders a collapsible "view generated source"
// panel. The outer <details> collapses by default; expanding it
// triggers a lazy fetch of GET /v1/agent/strategies/{id}/versions/{rev}
// and renders each path in its own nested <details><pre><code>
// block. The fetch runs once per panel so toggling doesn't refetch.
// assistantBody is optional: pass the streaming bubble to attach
// under it (SSE strategy_saved event); pass null/undefined to
// append into the messages list (session-load path).
function renderStrategyFiles(strategyId, revision, moduleName, summary, assistantBody) {
  const wrap = document.createElement('div');
  wrap.className = 'strategy-files';
  const header = document.createElement('div');
  header.className = 'strategy-files-header';
  const module = document.createElement('strong');
  module.textContent = moduleName || 'strategy';
  header.appendChild(module);
  if (summary) {
    const sm = document.createElement('span');
    sm.className = 'strategy-summary';
    sm.textContent = summary;
    header.appendChild(sm);
  }
  const rev = document.createElement('span');
  rev.className = 'strategy-rev';
  rev.textContent = ' ' + (revision || '').slice(0, 8);
  header.appendChild(rev);
  wrap.appendChild(header);

  const outer = document.createElement('details');
  outer.className = 'strategy-files-toggle';
  const outerSummary = document.createElement('summary');
  outerSummary.textContent = 'view generated source';
  outer.appendChild(outerSummary);
  const body = document.createElement('div');
  body.className = 'strategy-files-body';
  body.textContent = 'loading...';
  outer.appendChild(body);
  wrap.appendChild(outer);

  let fetched = false;
  outer.addEventListener('toggle', async () => {
    if (!outer.open || fetched) return;
    fetched = true;
    try {
      const v = await api('GET', '/v1/agent/strategies/' + strategyId + '/versions/' + revision);
      body.textContent = '';
      const files = v && v.files ? v.files : {};
      const paths = Object.keys(files).sort();
      if (paths.length === 0) {
        body.textContent = '(no files)';
        return;
      }
      for (const path of paths) {
        const det = document.createElement('details');
        det.className = 'file';
        det.open = paths.length <= 2; // open small modules by default
        const sum = document.createElement('summary');
        sum.textContent = path;
        det.appendChild(sum);
        const pre = document.createElement('pre');
        const code = document.createElement('code');
        code.textContent = files[path];
        pre.appendChild(code);
        det.appendChild(pre);
        body.appendChild(det);
      }
    } catch (e) {
      body.textContent = 'error: ' + (e && e.message ? e.message : String(e));
    }
  });

  // Attach under the streaming bubble (SSE event) or into the messages
  // list (session-load renders historical strategies).
  if (assistantBody && assistantBody.parentElement) {
    assistantBody.parentElement.appendChild(wrap);
  } else {
    const holder = document.createElement('div');
    holder.className = 'msg assistant strategy-files-host';
    const role = document.createElement('div');
    role.className = 'role';
    role.textContent = 'assistant';
    holder.appendChild(role);
    holder.appendChild(wrap);
    $('#messages').appendChild(holder);
  }
  $('#messages').scrollTop = $("#messages").scrollHeight;
}

// --- Provider config ---------------------------------------------------

async function setProvider(form) {
  const fd = new FormData(form);
  const body = {
    provider: fd.get('provider'),
    api_key: fd.get('api_key'),
    default_model: fd.get('default_model'),
  };
  const base = fd.get('base_url');
  if (base) body.base_url = base;
  await api('POST', '/v1/agent/provider-config', body);
  flash('provider config saved');
}

async function listProviders() {
  try {
    const out = await api('GET', '/v1/agent/provider-config');
    $('#providerOut').textContent = JSON.stringify(out, null, 2);
  } catch (e) {
    $('#providerOut').textContent = 'error: ' + e.message;
    flashError('listProviders', e);
  }
}

async function deleteCurrentProvider() {
  const sel = $('#setProviderForm select[name=provider]').value;
  if (!confirm(`Delete provider config for "${sel}"?`)) return;
  await api('DELETE', '/v1/agent/provider-config/' + sel);
  flash('provider config deleted');
}

// --- Misc ---------------------------------------------------------------

function escapeHtml(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// renderMarkdown converts the CommonMark subset the strategy agent
// emits (fenced + inline code, headers, lists, blockquotes, bold,
// italic, links, paragraphs) to safe HTML. Fenced and inline code are
// extracted before escaping so their contents are never reinterpreted.
// No external dependency: the chat UI is a zero-dep static client.
function renderMarkdown(src) {
  if (!src) return '';

  // Fenced code blocks ```lang\n...\n``` -> escaped <pre><code>.
  const blocks = [];
  src = src.replace(/```([^\n`]*)\n?([\s\S]*?)```/g, (_, lang, code) => {
    blocks.push('<pre><code>' + escapeHtml(code.replace(/\n$/, '')) + '</code></pre>');
    return '\u0000B' + (blocks.length - 1) + '\u0000';
  });

  // Inline code spans -> escaped <code>, extracted before HTML escape.
  const spans = [];
  src = src.replace(/`([^`]+)`/g, (_, c) => {
    spans.push('<code>' + escapeHtml(c) + '</code>');
    return '\u0000I' + (spans.length - 1) + '\u0000';
  });

  // Escape everything that remains.
  src = escapeHtml(src);

  // Inline transforms on already-escaped text, then restore code spans.
  const inlineArt = (text) => {
    text = text.replace(
      /\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g,
      '<a href="$2" target="_blank" rel="noopener">$1</a>');
    text = text.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    text = text.replace(/__([^_]+)__/g, '<strong>$1</strong>');
    text = text.replace(/(^|[^*])\*([^*\n]+)\*(?!\*)/g, '$1<em>$2</em>');
    text = text.replace(/(^|[^_])_([^_\n]+)_(?!_)/g, '$1<em>$2</em>');
    return text.replace(/\u0000I(\d+)\u0000/g, (_, n) => spans[Number(n)]);
  };

  // Block-level: scan lines, group into blocks.
  const lines = src.split('\n');
  let out = '', i = 0, para = [];
  const flush = () => {
    if (para.length) { out += '<p>' + para.join('\n') + '</p>\n'; para = []; }
  };
  while (i < lines.length) {
    const t = lines[i].trim();

    // Standalone fenced-code placeholder: emit raw, never wrapped in <p>.
    if (/^\u0000B\d+\u0000$/.test(t)) {
      flush();
      out += t.replace(/\u0000B(\d+)\u0000/g, (_, n) => blocks[Number(n)]) + '\n';
      i++; continue;
    }
    const h = t.match(/^(#{1,6})\s+(.*)$/);
    if (h) {
      flush();
      const lvl = h[1].length;
      out += '<h' + lvl + '>' + inlineArt(h[2]) + '</h' + lvl + '>\n';
      i++; continue;
    }
    if (t.indexOf('&gt; ') === 0) {
      flush();
      out += '<blockquote>' + inlineArt(t.slice(5)) + '</blockquote>\n';
      i++; continue;
    }
    if (/^[-*]\s+/.test(t)) {
      flush();
      out += '<ul>\n';
      while (i < lines.length && /^[-*]\s+/.test(lines[i].trim())) {
        out += '<li>' + inlineArt(lines[i].trim().replace(/^[-*]\s+/, '')) + '</li>';
        i++;
      }
      out += '</ul>\n';
      continue;
    }
    if (/^\d+\.\s+/.test(t)) {
      flush();
      out += '<ol>\n';
      while (i < lines.length && /^\d+\.\s+/.test(lines[i].trim())) {
        out += '<li>' + inlineArt(lines[i].trim().replace(/^\d+\.\s+/, '')) + '</li>';
        i++;
      }
      out += '</ol>\n';
      continue;
    }
    if (t === '') { flush(); i++; continue; }

    // Paragraph line. Block-level fences sit on their own line and
    // never reach this branch in practice; the final sweep below
    // restores any stray placeholders.
    para.push(inlineArt(t));
    i++;
  }
  flush();
  out = out.replace(/\u0000B(\d+)\u0000/g, (_, n) => blocks[Number(n)]);
  out = out.replace(/\u0000I(\d+)\u0000/g, (_, n) => spans[Number(n)]);
  return out;
}

// --- Wire up ----------------------------------------------------------

loadAuth();
$('#pageOrigin').textContent = 'page origin: ' + window.location.origin;

// loadModelCatalog fetches the per-provider enums from the embedded
// /v1/openapi endpoint, then populates both model dropdowns once
// available. populateModels called before the catalog loads is a
// no-op (empty list), so it must come after the fetch resolves.
(async () => {
  await loadModelCatalog();
  populateModels($('#newModel'), $('#newProvider').value);
  populateModels($('#cfgDefaultModel'), $('#cfgProvider').value);
})();
$('#newProvider').onchange = (e) => populateModels($('#newModel'), e.target.value);
$('#cfgProvider').onchange = (e) => populateModels($('#cfgDefaultModel'), e.target.value);
refreshSessions();

$('#saveAuth').onclick = saveAuth;
$('#healthz').onclick = async () => {
  const url = state.baseUrl + '/healthz';
  try {
    const r = await fetch(url);
    flash(`healthz ${r.status} ${r.status === 200 ? 'ok' : 'FAIL'} (${url})`);
  } catch (e) {
    flashError(`healthz unreachable (${url})`, e);
  }
};

$('#newSessionForm').onsubmit = async (e) => {
  e.preventDefault();
  const provider = $('#newProvider').value;
  const model = $('#newModel').value.trim();
  const title = $('#newTitle').value.trim();
  if (!model) return alert('model required');
  try {
    await newSession(provider, model, title);
  } catch (err) { flashError('create session', err); }
};

$('#setProviderForm').onsubmit = async (e) => {
  e.preventDefault();
  try { await setProvider(e.target); } catch (err) { flashError('save provider', err); }
};
$('#listProviders').onclick = listProviders;
$('#deleteProvider').onclick = () => {
  deleteCurrentProvider().catch((e) => flashError('delete provider', e));
};

$('#deleteSession').onclick = () => {
  deleteSession().catch((e) => flashError('delete session', e));
};

$('#messageForm').onsubmit = async (e) => {
  e.preventDefault();
  const prompt = $('#promptInput').value.trim();
  if (!prompt) return;
  $('#promptInput').value = '';
  $('#sendBtn').disabled = true;
  try { await sendMessage(prompt); }
  catch (err) {
    if (err.name !== 'AbortError') flashError('send', err);
  } finally { $('#sendBtn').disabled = false; }
};

// --- Deployment panel event handlers -----------------------------------

$('#refreshDeployments').onclick = () => {
  if (state.activeStrategyId) {
    loadDeploymentPanel([{
      id: state.activeStrategyId,
      name: state.activeStrategyName,
      head_revision: state.activeStrategyHeadRevision,
    }]);
  }
};
$('#deploymentSelect').onchange = (e) => {
  state.activeDeploymentId = e.target.value;
  if (state.activeDeploymentId) {
    // Sync the deploy form's revision to the selected deployment so the
    // manifest inputs (order books/resolution/params) refresh for that
    // revision, not the previously selected row's.
    const selected = state.deployments.find((d) => d.deployment_id === state.activeDeploymentId);
    if (selected && state.strategyVersions.some((v) => v.revision === selected.revision)) {
      state.selectedRevisionId = selected.revision;
    }
    loadDeploymentDetail(state.activeDeploymentId, state.deploymentRequestVersion);
    if (state.selectedRevisionId) {
      loadRevisionManifest(state.selectedRevisionId, state.deploymentRequestVersion);
    }
  } else {
    renderDeploymentPanel();
  }
};
$('#deployRevision').onchange = (e) => {
  state.selectedRevisionId = e.target.value;
  loadRevisionManifest(state.selectedRevisionId, state.deploymentRequestVersion);
};
$('#stopDeployment').onclick = async () => {
  if (!state.activeDeploymentId || !confirm('Stop this deployment?')) return;
  await runDeploymentAction('stop');
};
$('#startDeployment').onclick = async () => {
  if (!state.activeDeploymentId) return;
  await runDeploymentAction('start');
};
$('#deployForm').onsubmit = submitDeployment;
