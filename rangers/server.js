'use strict';
// G² Rangers — a separate Ranger group (G2-RANGER-01..N) running from the G² repository as its own service.
// Additive (Oct 31, 2026, Joshua L. Lopez — DCGP.AI LLC).
//   1. Binds its own G² session on the existing G² service (Aura116 Express /api/g2-session, JetBase 3100 seat).
//      No message is sent there, so G² is never called directly from this service.
//   2. Takes ready bounty rows from AAROW (/api/aarow/dispatch-payloads).
//   3. Wraps each row in an AREX-P3Q dispatcher envelope sealed by the live AREX-P3Q pipeline
//      (/api/atlasaurus-rex-p3q, mode arex_p3q -> pipeline_seal_sha256 + master_seal).
//   4. Sends the row through AURA's answer chain (/api/aura-llm-router, station lane:
//      AURA -> Rush -> RNN -> 9D Brain Builder -> AURA compose, G² as AURA's inner provider).
//   5. Seals the envelope (sha256 over envelope + answer hash) and admits it onto the P3Q seals lane
//      (/api/p3q-seals) only when the lane has room, so no earlier admit is pushed out.
// Never posts an answer anywhere. Drafts stay here (GET /rangers/runs) for Joshua's OK.
// Keys are read from the environment and never returned.

const http = require('http');
const crypto = require('crypto');

const env = (k, d) => String(process.env[k] || d || '').trim();
const BASE = env('AURA116_HTTP_BASE', 'https://aura116-production.up.railway.app').replace(/\/$/, '');
const OP = () => env('AURA_OPERATOR_KEY');
const ENABLED = () => env('G2_RANGERS_ENABLED', '1') === '1';
const INTERVAL = () => Math.max(60000, Number(env('G2_RANGERS_INTERVAL_MS', '1800000')));
const BATCH = () => Math.max(1, Math.min(5, Number(env('G2_RANGERS_BATCH', '1'))));
// 'operator' (default): only operator-triggered runs admit onto the seal lane; 'always'; '0' never.
const SEAL_MODE = () => env('G2_RANGERS_SEAL_LANE_ADMIT', 'operator').toLowerCase();
const GROUP = 'G2-RANGERS';
const SEAL_LANE_CAP = 50; // /api/p3q-seals keeps the newest 50 admits

const sha = (t) => crypto.createHash('sha256').update(typeof t === 'string' ? t : JSON.stringify(t)).digest('hex');
const state = { cursor: 0, started_utc: new Date().toISOString(), runs: [], running: false, last_error: null, g2_session: null, next_run_utc: null };

async function call(method, path, body, timeoutMs, withOp) {
  const headers = { Accept: 'application/json', 'x-aura-source': 'g2-rangers' };
  if (body) headers['Content-Type'] = 'application/json';
  if (withOp && OP()) headers['x-aura-operator-key'] = OP();
  const t0 = Date.now();
  try {
    const r = await fetch(BASE + path, { method, headers, body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(timeoutMs || 30000) });
    const j = await r.json().catch(() => ({}));
    return { http: r.status, ok: r.ok, ms: Date.now() - t0, j };
  } catch (e) {
    return { http: 0, ok: false, ms: Date.now() - t0, j: {}, error: e && e.name === 'TimeoutError' ? 'timeout' : String((e && e.message) || e).slice(0, 160) };
  }
}

const STATION_RULES = [
  'You draft one reply to a Railway Central Station bounty question for the Station user docgrok1.',
  'A human will review your draft before anything is posted. You never post.',
  'Rules:',
  '- Answer only what the thread shows. Do not invent logs, error messages, settings or results.',
  '- If you are not sure, say what to check and how, instead of guessing.',
  '- Only link to docs.railway.com, railway.com, station.railway.com, github.com/railwayapp. No other links.',
  '- At most 300 words in total, including code.',
  '- Use only real railway CLI subcommands and real Railway settings.',
  '- Never include secrets, tokens, passwords or connection strings with credentials.',
  '- Do not add any AI disclosure, attribution, signature or sign-off line.'
].join('\n');

async function bindG2Session() {
  const day = new Date().toISOString().slice(0, 10);
  const r = await call('POST', '/api/g2-session', { userId: 'g2-rangers', surface: 'g2-rangers', displayName: 'G² Rangers', sessionId: 'g2-rangers-' + day }, 20000, false);
  const s = { http: r.http, ok: r.ok && r.j && r.j.ok === true, service: r.j && r.j.service, via: r.j && r.j.via,
    tenant_id: r.j && r.j.tenant && r.j.tenant.tenantId, session_id: r.j && r.j.session && r.j.session.sessionId, pg_ready: r.j && r.j.pg && r.j.pg.ready, error: r.error || null };
  state.g2_session = s;
  return s;
}

async function sealLaneRoom() {
  const r = await call('GET', '/api/p3q-seals', null, 20000, false);
  let list = r.j && r.j.last_admit;
  try { if (typeof list === 'string') list = JSON.parse(list); } catch (_) { list = null; }
  const count = Array.isArray(list) ? list.length : null;
  return { http: r.http, count, room: count !== null && count < SEAL_LANE_CAP - 1 };
}

async function rangeOne(i, row, g2, trigger) {
  const ranger_id = 'G2-RANGER-' + String(i + 1).padStart(2, '0');
  const question = [
    'Thread: ' + row.thread_url,
    'Bounty: $' + row.reward_usd + ' (' + row.column_id + ')',
    'Issue class: ' + row.failure_class,
    'Current draft answer on file (review it, keep what is correct, fix what is not, and write the final reply):',
    '-----', String(row.markdown_body || '').slice(0, 4000), '-----',
    'Write the reply now in Markdown.'
  ].join('\n');

  // AREX-P3Q dispatcher envelope, sealed by the live AREX-P3Q pipeline.
  const arex = await call('POST', '/api/atlasaurus-rex-p3q', { mode: 'arex_p3q' }, 30000, true);
  const envelope = {
    envelope_id: 'G2RX-' + Date.now().toString(36).toUpperCase() + '-' + (i + 1),
    group: GROUP, ranger_id, coin: 'CHARLIE-021', dispatcher: 'AREX-P3Q',
    row: { column_id: row.column_id, bounty_id: row.bounty_id, thread_url: row.thread_url, reward_usd: row.reward_usd, body_sha256: row.body_sha256 },
    g2_session: g2 && g2.session_id || null,
    question_sha256: sha(question),
    arex: { http: arex.http, ok: !!(arex.j && arex.j.ok), runtime: arex.j && arex.j.runtime, pipeline_seal_sha256: arex.j && arex.j.pipeline_seal_sha256 || null, master_seal: arex.j && arex.j.master_seal || null, collision_free: arex.j && arex.j.collision_free, lopez_noether_ql: arex.j && arex.j.lopez_noether_ql, error: arex.error || (arex.j && arex.j.error) || null },
    created_utc: new Date().toISOString()
  };

  // Answer through AURA's chain (G² is AURA's inner provider there).
  const aura = await call('POST', '/api/aura-llm-router', {
    message: question, systemPrompt: STATION_RULES, rootContextPrompt: STATION_RULES,
    station_drafter: true, source: 'station-drafter', lane: 'g2-rangers', preferred_provider: 'xai',
    session_id: envelope.envelope_id, max_tokens: 700, temperature: 0.2
  }, 150000, true);
  const reply = String((aura.j && aura.j.reply) || '').trim();
  const provider = String((aura.j && aura.j.provider) || '');
  const viaChain = /^aura-station-chain:/.test(provider);
  envelope.aura = { http: aura.http, ms: aura.ms, provider, model: aura.j && aura.j.model || null, via_chain: viaChain, chain_path: aura.j && aura.j.chain && aura.j.chain.path || null, chain: aura.j && aura.j.chain || null, error: aura.error || null };
  envelope.answer_ok = viaChain && reply.length > 0;
  envelope.answer_markdown = envelope.answer_ok ? reply : null;
  envelope.answer_sha256 = envelope.answer_ok ? sha(reply) : null;
  envelope.seal = sha({ envelope_id: envelope.envelope_id, row: envelope.row, question_sha256: envelope.question_sha256, arex_seal: envelope.arex.pipeline_seal_sha256, answer_sha256: envelope.answer_sha256 });

  // Seal lane admission — only when there is room, so nothing earlier is pushed out.
  envelope.seal_lane = { lane: 'P3Q_SEALS_LANE', route: '/api/p3q-seals', admitted: false };
  const admitThisRun = SEAL_MODE() === 'always' || (SEAL_MODE() === 'operator' && trigger === 'operator');
  envelope.seal_lane.mode = SEAL_MODE();
  if (admitThisRun && envelope.answer_ok && envelope.arex.ok) {
    const room = await sealLaneRoom();
    envelope.seal_lane.count_before = room.count;
    if (room.room) {
      const adm = await call('POST', '/api/p3q-seals', { planets: ['saturn'], packages: [{ group: GROUP, envelope_id: envelope.envelope_id, ranger_id, seal: envelope.seal, arex_seal: envelope.arex.pipeline_seal_sha256, answer_sha256: envelope.answer_sha256 }] }, 20000, false);
      envelope.seal_lane.http = adm.http;
      envelope.seal_lane.admitted = !!(adm.j && adm.j.admitted);
      envelope.seal_lane.wrote = adm.j && adm.j.wrote;
    } else {
      envelope.seal_lane.skipped = room.count === null ? 'lane_count_unknown' : 'lane_full_no_eviction';
    }
  }
  return envelope;
}

async function runOnce(trigger) {
  if (state.running) return { ok: false, error: 'already_running' };
  state.running = true;
  const run = { run_id: 'G2RUN-' + Date.now().toString(36).toUpperCase(), trigger, started_utc: new Date().toISOString(), envelopes: [] };
  try {
    const g2 = await bindG2Session();
    run.g2_session = g2;
    const p = await call('GET', '/api/aarow/dispatch-payloads', null, 30000, false);
    const rows = (p.j && Array.isArray(p.j.payloads) ? p.j.payloads : []).filter(r => r && r.thread_url && r.markdown_body);
    run.rows_available = rows.length;
    const pick = [];
    for (let k = 0; k < Math.min(BATCH(), rows.length); k++) pick.push(rows[(state.cursor + k) % rows.length]);
    state.cursor = rows.length ? (state.cursor + pick.length) % rows.length : 0;
    for (let i = 0; i < pick.length; i++) run.envelopes.push(await rangeOne(i, pick[i], g2, trigger));
    run.ok = run.envelopes.length > 0 && run.envelopes.every(e => e.answer_ok);
  } catch (e) {
    run.ok = false; run.error = String((e && e.message) || e).slice(0, 200); state.last_error = run.error;
  } finally {
    run.finished_utc = new Date().toISOString();
    state.runs.unshift(run); state.runs = state.runs.slice(0, 20); state.running = false;
  }
  return run;
}

function summary(run) {
  if (!run) return null;
  return { run_id: run.run_id, trigger: run.trigger, ok: run.ok, started_utc: run.started_utc, finished_utc: run.finished_utc, rows_available: run.rows_available,
    g2_session: run.g2_session && { ok: run.g2_session.ok, session_id: run.g2_session.session_id },
    envelopes: run.envelopes.map(e => ({ envelope_id: e.envelope_id, ranger_id: e.ranger_id, column_id: e.row.column_id, arex_ok: e.arex.ok, arex_seal: e.arex.pipeline_seal_sha256, provider: e.aura.provider, chain_path: e.aura.chain_path, answer_ok: e.answer_ok, answer_sha256: e.answer_sha256, seal: e.seal, seal_lane: e.seal_lane })) };
}

function send(res, code, obj) { res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(obj, null, 2)); }
function authed(req) { const k = String(req.headers['x-aura-operator-key'] || req.headers['x-operator-key'] || '').trim().toLowerCase(); const o = OP().toLowerCase(); return !!o && k.length === o.length && crypto.timingSafeEqual(Buffer.from(k), Buffer.from(o)); }

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/health' || url.pathname === '/rangers/status')) {
    return send(res, 200, { ok: true, service: 'g2-rangers', group: GROUP, repo: 'DocGrok1/g2-aura116', aura_base: BASE, enabled: ENABLED(), interval_ms: INTERVAL(), batch: BATCH(), seal_lane_admit: SEAL_MODE(),
      operator_key_present: !!OP(), running: state.running, started_utc: state.started_utc, next_run_utc: state.next_run_utc, g2_session: state.g2_session, last_run: summary(state.runs[0]), posts_anywhere: false });
  }
  if (req.method === 'GET' && url.pathname === '/rangers/runs') {
    if (!authed(req)) return send(res, 200, { ok: true, runs: state.runs.map(summary), note: 'full drafts need x-aura-operator-key' });
    return send(res, 200, { ok: true, runs: state.runs });
  }
  if (req.method === 'POST' && url.pathname === '/rangers/run') {
    if (!authed(req)) return send(res, 403, { ok: false, error: 'operator_key_required' });
    const run = await runOnce('operator');
    return send(res, 200, { ok: run.ok, run });
  }
  send(res, 404, { ok: false, error: 'not_found' });
});

const port = Number(process.env.PORT || 8080);
server.listen(port, () => {
  console.log('[g2-rangers] listening on ' + port + ' · aura ' + BASE + ' · enabled ' + ENABLED());
  const tick = async () => {
    state.next_run_utc = new Date(Date.now() + INTERVAL()).toISOString();
    if (ENABLED()) { try { const r = await runOnce('schedule'); console.log('[g2-rangers] run', r.run_id, 'ok', r.ok); } catch (e) { console.warn('[g2-rangers] run failed', e && e.message); } }
  };
  setTimeout(tick, 15000);
  setInterval(tick, INTERVAL());
});
