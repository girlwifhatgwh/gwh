/**
 * GWH Harvester Engine (ESM)
 *
 * Pool sources (confirmed from live OpenAPI specs):
 *   DLMM  → https://dlmm.datapi.meteora.ag/pools
 *             pages 1-based, page_size up to 1000, response: { total, pages, current_page, page_size, data[] }
 *             Already sorted volume_24h:desc — top pools are best-yield first.
 *   DAMM  → https://damm-api.meteora.ag/pools/search
 *             pages 0-based, required: page + size, response: { data[], page, total_count }
 *
 * Jupiter → https://lite-api.jup.ag/swap/v1/quote  +  /swap
 *   Rate limit: lite endpoint is free-tier, ~10 req/s sustained.
 *   We cap MAX_QUOTE_POOLS and add JUP_CALL_DELAY_MS between calls to stay within limits.
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

// MAX_FETCH_POOLS: how many pools to pull from each API per cycle (for pool selection)
const MAX_FETCH_POOLS = parseInt(process.env.MAX_FETCH_POOLS || '100', 10);

// MAX_QUOTE_POOLS: how many pools to actually call Jupiter quote for per cycle.
// DLMM is already sorted volume_24h:desc so these are the highest-yield pools.
// Keep this LOW to respect the free-tier rate limit (~10 req/s).
const MAX_QUOTE_POOLS = parseInt(process.env.MAX_QUOTE_POOLS || '10', 10);

// Delay between Jupiter quote calls in milliseconds.
// At 300ms we stay well under 10 req/s even with retries.
const JUP_CALL_DELAY_MS = parseInt(process.env.JUP_CALL_DELAY_MS || '300', 10);

const SLIPPAGE_BPS       = parseInt(process.env.SLIPPAGE_BPS       || '100',  10);
const HARVEST_SOL_AMOUNT = parseFloat(process.env.HARVEST_SOL_AMOUNT || '0.05');

const WSOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

// Runtime blacklist — clears on restart
const blacklist = new Map();

// Global Jupiter rate-limit state: if we hit a 429, back off across all subsequent calls.
let jupGlobalBackoffUntil = 0;

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
          return reject(Object.assign(
            new Error('429 Too Many Requests'),
            { status: 429 }
          ));
        }
        if (res.statusCode < 200 || res.statusCode >= 300) {
          return reject(Object.assign(
            new Error(`HTTP ${res.statusCode}: ${raw.slice(0, 200)}`),
            { status: res.statusCode, body: raw }
          ));
        }
        try   { resolve(JSON.parse(raw)); }
        catch (e) { reject(new Error(`JSON parse error: ${e.message} | body=${raw.slice(0, 200)}`)); }
      });
    });

    req.on('timeout', () => req.destroy(new Error(`Timeout: ${urlStr}`)));
    req.on('error',   reject);
    if (body) req.write(body);
    req.end();
  });
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// Generic retry with exponential back-off for non-Jupiter endpoints (Meteora etc.)
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
// Jupiter-specific fetch with global rate-limit awareness
//
// When Jupiter returns 429:
//   • Sets jupGlobalBackoffUntil = now + backoff duration
//   • All subsequent Jupiter calls wait out the backoff before sending
// This prevents the cascade of per-pool 429 retries visible in the logs.
// ─────────────────────────────────────────────────────────

async function jupFetch(urlStr, opts = {}) {
  const maxRetries = 5;
  let backoff = 2000; // first 429 wait 2s, then 4s, 8s …

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    // Honour global backoff from a previous call in this cycle
    const now = Date.now();
    if (jupGlobalBackoffUntil > now) {
      const wait = jupGlobalBackoffUntil - now;
      log(`  [JUP] Global rate-limit cooldown ${Math.ceil(wait / 1000)}s…`);
      await sleep(wait);
    }

    try {
      return await request(urlStr, opts);
    } catch (err) {
      if (err.status === 429) {
        jupGlobalBackoffUntil = Date.now() + backoff;
        log(`  [JUP] 429 — global backoff ${backoff}ms (attempt ${attempt}/${maxRetries})`);
        await sleep(backoff);
        backoff = Math.min(backoff * 2, 30000);
      } else if (attempt === maxRetries) {
        throw err;
      } else {
        // Non-429 error: short retry
        await sleep(500);
      }
    }
  }
}

// ─────────────────────────────────────────────────────────
// DLMM pool fetching  (dlmm.datapi.meteora.ag/pools)
// Pagination 1-based. Sorted volume_24h:desc by API.
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
// Pagination 0-based. page + size both required.
// Fallback to bare /pools (no params) if search fails.
// ─────────────────────────────────────────────────────────

async function fetchDammPools() {
  const pools = [];
  let   page  = 0;
  const size  = 100;

  // Primary: /pools/search
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

  // Fallback: bare /pools (no query params)
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

  // Inter-call delay — throttle Jupiter quote rate
  if (JUP_CALL_DELAY_MS > 0) await sleep(JUP_CALL_DELAY_MS);

  let quote;
  try {
    quote = await jupiterQuote(inputMint, outputMint, buyAmountLamports);
  } catch (err) {
    log(` Jupiter quote failed for ${shortAddr}: ${err.message}`);
    if (err.message.includes('429')) {
      // Do not blacklist on rate-limit — try again next cycle
      return false;
    }
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
    log(` Swap failed for ${shortAddr}: ${err.message}`);
    if (!err.message.includes('429')) {
      blacklistPool(addr, `${poolType}_swap_failed`);
    }
    return false;
  }
}

// ─────────────────────────────────────────────────────────
// Hub — optional WebSocket (lazy ws import)
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

  // Fetch both sources in parallel — pool discovery is fast, Jupiter quotes are slow
  const [rawDlmm, rawDamm] = await Promise.all([
    fetchDlmmPools(),
    fetchDammPools(),
  ]);

  const dlmmPools = filterDlmmPools(rawDlmm);
  const cpmmPools = filterDammPools(rawDamm);

  log(`    DLMM raw:${rawDlmm.length} CLMM raw:0 CPMM raw:${rawDamm.length}`);
  log(`   DLMM: ${dlmmPools.length} pools x ${slotsPerPool} slots | CLMM: 0 | CPMM: ${cpmmPools.length} | USDC: 0 | ${HARVEST_SOL_AMOUNT.toFixed(4)} SOL/slot`);
  log(`   Quoting top ${MAX_QUOTE_POOLS} DLMM pools (${JUP_CALL_DELAY_MS}ms inter-call delay)`);

  let entered = 0;

  // Quote only the top MAX_QUOTE_POOLS (already sorted best-first by DLMM API)
  for (const pool of dlmmPools.slice(0, MAX_QUOTE_POOLS)) {
    const ok = await executeSwap(pool, 'dlmm', solLamports);
    if (ok) entered++;
  }

  // CPMM fallback only if zero DLMM pools entered
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
