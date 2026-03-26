/**
 * GWH Harvester Engine (ESM)
 *
 * Pool sources (confirmed from live OpenAPI specs):
 *   DLMM  → https://dlmm.datapi.meteora.ag/pools
 *             pages 1-based, page_size up to 1000, response: { total, pages, current_page, page_size, data[] }
 *             Sorted volume_24h:desc by API — top pools are best-yield first.
 *   DAMM  → https://damm-api.meteora.ag/pools/search
 *             pages 0-based, required: page + size, response: { data[], page, total_count }
 *
 * Jupiter → https://lite-api.jup.ag/swap/v1/quote  +  /swap
 *
 * Rate-limit strategy:
 *   - JUP_CALL_DELAY_MS (default 2000ms) between consecutive calls → ~0.5 req/s
 *   - MAX_QUOTE_POOLS (default 5) — only quote the top N pools per cycle
 *   - On ANY 429: abort cycle immediately, skip all remaining pools.
 *     Do NOT retry within the cycle. Let the full HARVEST_INTERVAL pass before
 *     the next attempt. This allows the IP-level throttle to cool down.
 *   - Read Retry-After header (if present) and honour it across cycles.
 */

import https from 'node:https';
import http  from 'node:http';
import { EventEmitter } from 'node:events';

// ─────────────────────────────────────────────────────────
// Configuration — all values overridable via environment
// ─────────────────────────────────────────────────────────

const JUP_BASE          = process.env.JUP_BASE          || 'https://lite-api.jup.ag/swap/v1';
const JUP_QUOTE_BASE    = process.env.JUP_QUOTE_BASE    || 'https://lite-api.jup.ag/swap/v1';
const METEORA_DLMM_BASE = process.env.METEORA_DLMM_BASE || 'https://dlmm.datapi.meteora.ag';
const METEORA_DAMM_BASE = process.env.METEORA_DAMM_BASE || 'https://damm-api.meteora.ag';

const DLMM_MIN_FEE_TVL = parseFloat(process.env.DLMM_MIN_FEE_TVL || '0');
const MIN_POOL_TVL     = parseFloat(process.env.MIN_POOL_TVL     || '0');
const MIN_POOL_VOL     = parseFloat(process.env.MIN_POOL_VOL     || '0');

const SOLANA_RPC_URL   = process.env.SOLANA_RPC_URL   || 'https://api.mainnet-beta.solana.com';
const WALLET_PUBKEY    = process.env.WALLET_PUBKEY    || '';
const JUP_API_KEY      = process.env.JUP_API_KEY      || '';
const HUB_URL          = process.env.HUB_URL          || '';

const HARVEST_INTERVAL_MS = parseInt(process.env.HARVEST_INTERVAL_MS || '60000', 10);
const MAX_FETCH_POOLS     = parseInt(process.env.MAX_FETCH_POOLS     || '100',   10);

// Quote only the top N pools per cycle (pre-sorted best-first by DLMM API).
// Keep low to stay within the free-tier rate limit.
const MAX_QUOTE_POOLS = parseInt(process.env.MAX_QUOTE_POOLS || '5', 10);

// Mandatory pause between consecutive Jupiter calls (ms).
// 2000ms = 0.5 req/s — conservative for the unauthenticated lite endpoint.
const JUP_CALL_DELAY_MS = parseInt(process.env.JUP_CALL_DELAY_MS || '2000', 10);

const SLIPPAGE_BPS       = parseInt(process.env.SLIPPAGE_BPS       || '100',  10);
const HARVEST_SOL_AMOUNT = parseFloat(process.env.HARVEST_SOL_AMOUNT || '0.05');

const WSOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

const blacklist = new Map();

// When set, all Jupiter calls are skipped until this timestamp passes.
// Persists across cycles within the same process lifetime.
let jupThrottledUntil = 0;

// Thrown internally to signal "abort this cycle, do not blacklist any pool".
class RateLimitAbort extends Error {
  constructor(retryAfterMs) {
    super('Jupiter rate-limited — aborting cycle');
    this.retryAfterMs = retryAfterMs;
  }
}

// ─────────────────────────────────────────────────────────
// Logging
// ─────────────────────────────────────────────────────────

function ts()        { return new Date().toISOString().replace('T', ' ').slice(0, 19); }
function log(msg)    { console.log(`[${ts()}] ${msg}`); }
function logErr(msg) { console.error(`[${ts()}] ${msg}`); }

// ─────────────────────────────────────────────────────────
// HTTP helper — zero external dependencies
// ─────────────────────────────────────────────────────────

function request(urlStr, opts = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const lib = url.protocol === 'https:' ? https : http;

    const headers = {
      'Content-Type': 'application/json',
      'Accept':       'application/json',
      'User-Agent':   'GWH-Harvester/1.0',
      ...opts.headers,
    };
    if (JUP_API_KEY && url.hostname.includes('jup.ag')) {
      headers['x-api-key'] = JUP_API_KEY;
    }

    const body = opts.body ? JSON.stringify(opts.body) : undefined;
    if (body) headers['Content-Length'] = Buffer.byteLength(body);

    const req = lib.request({
      hostname: url.hostname,
      port:     url.port || (url.protocol === 'https:' ? 443 : 80),
      path:     url.pathname + url.search,
      method:   opts.method || 'GET',
      headers,
      timeout:  opts.timeout || 25000,
    }, (res) => {
      let raw = '';
      res.on('data', (c) => { raw += c; });
      res.on('end', () => {
        if (res.statusCode === 429) {
          // Parse Retry-After header if present (value is seconds)
          const retryAfter = res.headers['retry-after'];
          const retryMs    = retryAfter ? parseInt(retryAfter, 10) * 1000 : 0;
          return reject(Object.assign(
            new Error('429 Too Many Requests'),
            { status: 429, retryAfterMs: retryMs }
          ));
        }
        if (res.statusCode < 200 || res.statusCode >= 300) {
          return reject(Object.assign(
            new Error(`HTTP ${res.statusCode}: ${raw.slice(0, 200)}`),
            { status: res.statusCode, body: raw }
          ));
        }
        try   { resolve(JSON.parse(raw)); }
        catch (e) { reject(new Error(`JSON parse: ${e.message} | body=${raw.slice(0, 200)}`)); }
      });
    });

    req.on('timeout', () => req.destroy(new Error(`Timeout: ${urlStr}`)));
    req.on('error',   reject);
    if (body) req.write(body);
    req.end();
  });
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// Non-Jupiter fetch with standard exponential back-off (Meteora APIs are generous).
async function fetchWithRetry(urlStr, opts = {}, maxRetries = 4) {
  let delay = 500;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await request(urlStr, opts);
    } catch (err) {
      if (err.status === 429) {
        logErr(`[HTTP] 429 on ${urlStr} — retrying after ${delay}ms...`);
      } else if (attempt === maxRetries) {
        throw err;
      }
      await sleep(delay);
      delay = Math.min(delay * 2, 8000);
    }
  }
}

// ─────────────────────────────────────────────────────────
// Jupiter fetch — abort-on-429 strategy
//
// Philosophy: one 429 means the IP is already throttled.
// Retrying within the same cycle just keeps the throttle alive.
// Instead: record how long to cool down, throw RateLimitAbort to
// unwind the entire cycle, and let the next scheduled cycle try fresh.
// ─────────────────────────────────────────────────────────

async function jupFetch(urlStr, opts = {}) {
  // Honour cool-down from a previous 429 in this process lifetime
  const now = Date.now();
  if (jupThrottledUntil > now) {
    const remaining = jupThrottledUntil - now;
    log(`  [JUP] Still rate-limited — skipping (${Math.ceil(remaining / 1000)}s remaining)`);
    throw new RateLimitAbort(remaining);
  }

  try {
    return await request(urlStr, opts);
  } catch (err) {
    if (err.status === 429) {
      // Use server's Retry-After if available; otherwise cool down for one full cycle interval.
      const cooldown = err.retryAfterMs > 0 ? err.retryAfterMs : HARVEST_INTERVAL_MS;
      jupThrottledUntil = Date.now() + cooldown;
      log(`  [JUP] 429 received — aborting cycle. Cooling down ${Math.ceil(cooldown / 1000)}s (until next cycle).`);
      throw new RateLimitAbort(cooldown);
    }
    throw err;
  }
}

// ─────────────────────────────────────────────────────────
// DLMM pool fetching  (dlmm.datapi.meteora.ag/pools)
// ─────────────────────────────────────────────────────────

function mintOf(tok) {
  if (!tok) return null;
  if (typeof tok === 'string') return tok;
  return tok.address || tok.mint || tok.mint_address || null;
}

async function fetchDlmmPools() {
  const pools = [];
  let   page  = 1;
  const limit = 100;

  const params = new URLSearchParams({
    page_size: String(limit),
    sort_by:   'volume_24h:desc',
    filter_by: 'is_blacklisted=false',
  });

  while (pools.length < MAX_FETCH_POOLS) {
    params.set('page', String(page));
    let data;
    try {
      data = await fetchWithRetry(`${METEORA_DLMM_BASE}/pools?${params}`);
    } catch (e) {
      logErr(`[DLMM] page ${page} failed: ${e.message}`);
      break;
    }
    const rows = data.data || [];
    if (!rows.length) break;
    pools.push(...rows);
    if (rows.length < limit || page >= (data.pages || 1)) break;
    page++;
  }
  return pools;
}

// ─────────────────────────────────────────────────────────
// DAMM pool fetching  (damm-api.meteora.ag/pools/search)
// ─────────────────────────────────────────────────────────

async function fetchDammPools() {
  const pools = [];
  let   page  = 0;
  const size  = 100;

  try {
    while (pools.length < MAX_FETCH_POOLS) {
      const params = new URLSearchParams({ page: String(page), size: String(size) });
      const data   = await fetchWithRetry(`${METEORA_DAMM_BASE}/pools/search?${params}`);
      const rows   = Array.isArray(data) ? data : (data.data || []);
      if (!rows.length) break;
      pools.push(...rows);
      const total = data.total_count || 0;
      if (rows.length < size || pools.length >= total) break;
      page++;
    }
    if (pools.length > 0) return pools;
  } catch (e) {
    logErr(`[DAMM] /pools/search failed: ${e.message}`);
  }

  try {
    const data = await fetchWithRetry(`${METEORA_DAMM_BASE}/pools`);
    const rows = Array.isArray(data) ? data : (data.data || data.pools || []);
    log(`[DAMM] /pools fallback: ${rows.length} pools`);
    return rows;
  } catch (e) {
    logErr(`[DAMM] /pools fallback failed: ${e.message}`);
    return [];
  }
}

// ─────────────────────────────────────────────────────────
// Pool filtering
// ─────────────────────────────────────────────────────────

function isBlacklisted(address) { return blacklist.has(address); }

function blacklistPool(address, reason) {
  const entry = blacklist.get(address) || { count: 0 };
  entry.count++;
  entry.reason = reason;
  blacklist.set(address, entry);
  log(`   Pool blacklisted: ${address.slice(0, 8)}... reason: ${reason} (fail #${entry.count})`);
}

function filterDlmmPools(pools) {
  return pools.filter((p) => {
    const addr   = p.address || '';
    if (isBlacklisted(addr)) return false;
    const tvl    = parseFloat(p.tvl || 0);
    const vol    = parseFloat(p.volume?.['24h']        || p.volume?.h24       || 0);
    const feeTvl = parseFloat(p.fee_tvl_ratio?.['24h'] || p.fee_tvl_ratio?.h24 || 0);
    return tvl >= MIN_POOL_TVL && vol >= MIN_POOL_VOL && feeTvl >= DLMM_MIN_FEE_TVL;
  });
}

function filterDammPools(pools) {
  return pools.filter((p) => {
    const addr = p.pool_address || p.address || '';
    if (isBlacklisted(addr)) return false;
    const tvl = parseFloat(p.pool_tvl || p.tvl || 0);
    const vol = parseFloat(p.trading_volume || p.fee_volume || 0);
    return tvl >= MIN_POOL_TVL && vol >= MIN_POOL_VOL;
  });
}

// ─────────────────────────────────────────────────────────
// Jupiter helpers
// ─────────────────────────────────────────────────────────

async function jupiterQuote(inputMint, outputMint, amountLamports) {
  const params = new URLSearchParams({
    inputMint,
    outputMint,
    amount:                     String(amountLamports),
    slippageBps:                String(SLIPPAGE_BPS),
    onlyDirectRoutes:           'false',
    restrictIntermediateTokens: 'true',
  });
  return jupFetch(`${JUP_QUOTE_BASE}/quote?${params}`);
}

async function jupiterSwap(quoteResponse, userPublicKey) {
  return jupFetch(`${JUP_BASE}/swap`, {
    method: 'POST',
    body: {
      quoteResponse,
      userPublicKey,
      wrapAndUnwrapSol:          true,
      dynamicComputeUnitLimit:   true,
      prioritizationFeeLamports: 'auto',
    },
  });
}

// ─────────────────────────────────────────────────────────
// Swap execution — shared for DLMM and DAMM pools
// Throws RateLimitAbort upward to abort the whole cycle.
// ─────────────────────────────────────────────────────────

async function executeSwap(pool, poolType, solAmountLamports) {
  let addr, mintX, mintY;

  if (poolType === 'dlmm') {
    addr  = pool.address || 'unknown';
    mintX = mintOf(pool.token_x);
    mintY = mintOf(pool.token_y);
  } else {
    addr  = pool.pool_address || pool.address || 'unknown';
    const mints = pool.pool_token_mints || [];
    mintX = mints[0] || null;
    mintY = mints[1] || null;
  }

  if (!mintX || !mintY) return false;

  const isXSol    = mintX === WSOL_MINT;
  const isYSol    = mintY === WSOL_MINT;
  const inputMint  = isXSol ? WSOL_MINT : (isYSol ? WSOL_MINT : USDC_MINT);
  const outputMint = isXSol ? mintY : mintX;

  const shortAddr  = `${addr.slice(0, 4)}..`;
  const solDisplay = (solAmountLamports / 1e9).toFixed(4);
  log(`  Enter [${poolType.toUpperCase()}] ${shortAddr}/SOL | ${solDisplay} SOL (profit only)`);

  const buyPct = 49.5, lpPct = 50.5;
  log(` ${poolType.toUpperCase()} fallback ${shortAddr}/SOL: buy ${buyPct.toFixed(4)}% as token, add ${lpPct.toFixed(4)}% SOL to LP`);

  const buyAmountLamports = Math.floor(solAmountLamports * (buyPct / 100));

  // Throttle: mandatory delay before each Jupiter call
  if (JUP_CALL_DELAY_MS > 0) await sleep(JUP_CALL_DELAY_MS);

  let quote;
  try {
    quote = await jupiterQuote(inputMint, outputMint, buyAmountLamports);
  } catch (err) {
    if (err instanceof RateLimitAbort) throw err; // propagate up — abort cycle
    log(` Jupiter quote failed for ${shortAddr}: ${err.message}`);
    blacklistPool(addr, `${poolType}_quote_failed`);
    return false;
  }

  if (!WALLET_PUBKEY) {
    log(`  [DRY-RUN] Would swap ${buyAmountLamports} lamports → ${outputMint.slice(0, 8)} (WALLET_PUBKEY not set)`);
    return true;
  }

  try {
    const swapResp = await jupiterSwap(quote, WALLET_PUBKEY);
    const tx = swapResp.swapTransaction;
    log(`  Swap TX built (${tx ? tx.length : 0} bytes) for ${shortAddr}`);
    // To broadcast: Connection.sendRawTransaction(Buffer.from(tx, 'base64'))
    return true;
  } catch (err) {
    if (err instanceof RateLimitAbort) throw err;
    log(` Swap failed for ${shortAddr}: ${err.message}`);
    blacklistPool(addr, `${poolType}_swap_failed`);
    return false;
  }
}

// ─────────────────────────────────────────────────────────
// Hub — optional WebSocket
// ─────────────────────────────────────────────────────────

const hubEmitter = new EventEmitter();
let hubConnected = false;

function connectHub() {
  if (!HUB_URL) return;
  import('ws').then(({ default: WebSocket }) => {
    const ws = new WebSocket(HUB_URL);
    ws.on('open',    ()    => { hubConnected = true;  log('  Hub connected'); });
    ws.on('close',   ()    => { hubConnected = false; log('  Hub disconnected'); setTimeout(connectHub, 5000); });
    ws.on('error',   (e)   => { logErr(`Hub error: ${e.message}`); });
    ws.on('message', (msg) => { hubEmitter.emit('message', msg); });
  }).catch(() => {});
}

// ─────────────────────────────────────────────────────────
// Main harvest cycle
// ─────────────────────────────────────────────────────────

async function runHarvestCycle() {
  const solLamports  = Math.floor(HARVEST_SOL_AMOUNT * 1e9);
  const slotsPerPool = 4;

  // Skip cycle entirely if still rate-limited from a previous cycle
  const now = Date.now();
  if (jupThrottledUntil > now) {
    const remaining = jupThrottledUntil - now;
    log(`  [JUP] IP still cooling down — skipping cycle (${Math.ceil(remaining / 1000)}s remaining)`);
    return;
  }

  // Fetch both pool sources in parallel (these hit Meteora, not Jupiter)
  const [rawDlmm, rawDamm] = await Promise.all([
    fetchDlmmPools(),
    fetchDammPools(),
  ]);

  const dlmmPools = filterDlmmPools(rawDlmm);
  const cpmmPools = filterDammPools(rawDamm);

  log(`    DLMM raw:${rawDlmm.length} CLMM raw:0 CPMM raw:${rawDamm.length}`);
  log(`   DLMM: ${dlmmPools.length} pools x ${slotsPerPool} slots | CLMM: 0 | CPMM: ${cpmmPools.length} | USDC: 0 | ${HARVEST_SOL_AMOUNT.toFixed(4)} SOL/slot`);
  log(`   Quoting top ${Math.min(MAX_QUOTE_POOLS, dlmmPools.length)} DLMM pools (${JUP_CALL_DELAY_MS}ms inter-call delay)`);

  let entered = 0;

  try {
    // Top N DLMM pools only (already sorted best-first)
    for (const pool of dlmmPools.slice(0, MAX_QUOTE_POOLS)) {
      const ok = await executeSwap(pool, 'dlmm', solLamports);
      if (ok) entered++;
    }

    // CPMM fallback only if no DLMM pools entered
    if (entered === 0 && cpmmPools.length > 0) {
      log(`  CPMM last resort: ${cpmmPools.length} verified slot(s)`);
      for (const pool of cpmmPools.slice(0, Math.min(3, MAX_QUOTE_POOLS))) {
        const ok = await executeSwap(pool, 'cpmm', solLamports);
        if (ok) entered++;
      }
    }

    if (entered === 0 && cpmmPools.length > 0) {
      log(`  No DLMM/CLMM pools entered — forcing CPMM fallback entry`);
      log(`    Force fallback: ${cpmmPools.length} CPMM pools available`);
      const pool = cpmmPools[0];
      const addr = pool.pool_address || pool.address || '';
      log(`   Force entering CPMM: ${addr.slice(0, 4)}../SOL`);
      await executeSwap(pool, 'cpmm', solLamports);
    } else if (entered === 0) {
      log(`    No pools entered — reserves protected`);
    }
  } catch (err) {
    if (err instanceof RateLimitAbort) {
      // Cycle aborted due to 429 — do nothing here, jupThrottledUntil is already set.
      // The next scheduled cycle will check it and skip if still cooling down.
      log(`  [JUP] Cycle aborted due to rate-limit. Next attempt after cooldown.`);
      return;
    }
    throw err;
  }
}

// ─────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────

log('=== GWH Harvester Engine starting ===');
log(`JUP_BASE:          ${JUP_BASE}`);
log(`JUP_QUOTE_BASE:    ${JUP_QUOTE_BASE}`);
log(`METEORA_DLMM_BASE: ${METEORA_DLMM_BASE}`);
log(`METEORA_DAMM_BASE: ${METEORA_DAMM_BASE}`);
log(`DLMM_MIN_FEE_TVL:  ${DLMM_MIN_FEE_TVL}`);
log(`MIN_POOL_TVL:      ${MIN_POOL_TVL}`);
log(`MIN_POOL_VOL:      ${MIN_POOL_VOL}`);
log(`HARVEST_INTERVAL:  ${HARVEST_INTERVAL_MS}ms`);
log(`MAX_FETCH_POOLS:   ${MAX_FETCH_POOLS}`);
log(`MAX_QUOTE_POOLS:   ${MAX_QUOTE_POOLS}`);
log(`JUP_CALL_DELAY:    ${JUP_CALL_DELAY_MS}ms`);
log(`HARVEST_SOL:       ${HARVEST_SOL_AMOUNT} SOL/slot`);
log(`WALLET:            ${WALLET_PUBKEY ? WALLET_PUBKEY.slice(0, 8) + '...' : '(not set — dry-run mode)'}`);
if (JUP_API_KEY) {
  log(`JUP_API_KEY:       set (authenticated mode)`);
} else {
  log(`JUP_API_KEY:       not set — using unauthenticated lite endpoint (rate-limited)`);
  log(`                   Get a free key at https://portal.jup.ag to remove rate limits`);
}

connectHub();

try {
  await runHarvestCycle();
} catch (e) {
  logErr(`[MAIN] Cycle error: ${e.message}`);
}

setInterval(async () => {
  try {
    await runHarvestCycle();
  } catch (e) {
    logErr(`[MAIN] Cycle error: ${e.message}`);
  }
}, HARVEST_INTERVAL_MS);
