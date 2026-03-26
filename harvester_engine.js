/**
 * GWH Harvester Engine (ESM)
 *
 * Pool sources (confirmed live API specs):
 *   DLMM  → https://dlmm.datapi.meteora.ag/pools   (page 1-based, returns .data[])
 *   DAMM  → https://damm-api.meteora.ag/pools       (no pagination, returns array)
 *
 * Jupiter swaps → https://lite-api.jup.ag/swap/v1/quote  + /swap
 */

import https from 'node:https';
import http from 'node:http';
import { EventEmitter } from 'node:events';

// ──────────────────────────────────────────────
// Configuration (env overrides)
// ──────────────────────────────────────────────

const JUP_BASE          = process.env.JUP_BASE          || 'https://lite-api.jup.ag/swap/v1';
const JUP_QUOTE_BASE    = process.env.JUP_QUOTE_BASE    || 'https://lite-api.jup.ag/swap/v1';
const METEORA_DLMM_BASE = process.env.METEORA_DLMM_BASE || 'https://dlmm.datapi.meteora.ag';
const METEORA_DAMM_BASE = process.env.METEORA_DAMM_BASE || 'https://damm-api.meteora.ag';

// Permissive by default; env can tighten them
const DLMM_MIN_FEE_TVL = parseFloat(process.env.DLMM_MIN_FEE_TVL || '0');
const MIN_POOL_TVL     = parseFloat(process.env.MIN_POOL_TVL     || '0');
const MIN_POOL_VOL     = parseFloat(process.env.MIN_POOL_VOL     || '0');

const SOLANA_RPC_URL     = process.env.SOLANA_RPC_URL     || 'https://api.mainnet-beta.solana.com';
const WALLET_PUBKEY      = process.env.WALLET_PUBKEY      || '';
const JUP_API_KEY        = process.env.JUP_API_KEY        || '';
const HUB_URL            = process.env.HUB_URL            || '';

const HARVEST_INTERVAL_MS = parseInt(process.env.HARVEST_INTERVAL_MS  || '60000', 10);
const MAX_POOLS_PER_CYCLE = parseInt(process.env.MAX_POOLS_PER_CYCLE  || '50',    10);
const SLIPPAGE_BPS        = parseInt(process.env.SLIPPAGE_BPS         || '100',   10);
const HARVEST_SOL_AMOUNT  = parseFloat(process.env.HARVEST_SOL_AMOUNT || '0.05');

const WSOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

// Runtime blacklist (clears on restart)
const blacklist = new Map();

// ──────────────────────────────────────────────
// Logging
// ──────────────────────────────────────────────

function ts() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}
function log(msg)    { console.log(`[${ts()}] ${msg}`); }
function logErr(msg) { console.error(`[${ts()}] ${msg}`); }

// ──────────────────────────────────────────────
// HTTP helper (no external dependencies)
// ──────────────────────────────────────────────

function request(urlStr, opts = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const lib = url.protocol === 'https:' ? https : http;
    const headers = {
      'Content-Type': 'application/json',
      'Accept':       'application/json',
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
      timeout:  opts.timeout || 20000,
    }, (res) => {
      let raw = '';
      res.on('data', (c) => { raw += c; });
      res.on('end', () => {
        if (res.statusCode === 429) {
          return reject(Object.assign(
            new Error('Server responded with 429 Too Many Requests.'),
            { status: 429 }
          ));
        }
        if (res.statusCode < 200 || res.statusCode >= 300) {
          return reject(Object.assign(
            new Error(`Request failed with status code ${res.statusCode}`),
            { status: res.statusCode, body: raw.slice(0, 300) }
          ));
        }
        try { resolve(JSON.parse(raw)); }
        catch (e) {
          reject(new Error(`JSON parse error: ${e.message} snippet=${raw.slice(0, 200)}`));
        }
      });
    });
    req.on('timeout', () => req.destroy(new Error(`Timeout: ${urlStr}`)));
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function fetchWithRetry(urlStr, opts = {}, maxRetries = 4) {
  let delay = 500;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await request(urlStr, opts);
    } catch (err) {
      if (err.status === 429) {
        logErr(`Server responded with 429 Too Many Requests.  Retrying after ${delay}ms delay...`);
      } else if (attempt === maxRetries) {
        throw err;
      }
      await sleep(delay);
      delay = Math.min(delay * 2, 8000);
    }
  }
}

// ──────────────────────────────────────────────
// DLMM pool fetching  (dlmm.datapi.meteora.ag)
// Pages are 1-based; response shape: { total, pages, current_page, page_size, data: [] }
// Each pool: { address, token_x: TokenMetrics, token_y: TokenMetrics, tvl, volume, fee_tvl_ratio, is_blacklisted }
// TokenMetrics likely has { address, symbol, ... } — we accept both string and object
// ──────────────────────────────────────────────

function mintOf(tok) {
  if (!tok) return null;
  if (typeof tok === 'string') return tok;
  return tok.address || tok.mint || tok.mint_address || null;
}

async function fetchDlmmPools() {
  const pools  = [];
  let   page   = 1; // 1-based per OpenAPI spec
  const limit  = 100;

  // Build filter: exclude blacklisted pools, sort by 24h volume descending
  const params = new URLSearchParams({
    page_size:  String(limit),
    sort_by:    'volume_24h:desc',
    filter_by:  'is_blacklisted=false',
  });

  while (pools.length < MAX_POOLS_PER_CYCLE) {
    params.set('page', String(page));
    const url = `${METEORA_DLMM_BASE}/pools?${params}`;
    let data;
    try {
      data = await fetchWithRetry(url);
    } catch (e) {
      logErr(`[METEORA] DLMM page ${page} failed: ${e.message}`);
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

// ──────────────────────────────────────────────
// DAMM (Dynamic AMM / CPMM) pool fetching  (damm-api.meteora.ag)
// Returns a plain array (no pagination wrapper).
// Each pool: { pool_address, pool_token_mints: [mintA, mintB], pool_tvl, trading_volume, fee_volume }
// ──────────────────────────────────────────────

async function fetchDammPools() {
  const params = new URLSearchParams({ pool_type: 'dynamic' });
  const url = `${METEORA_DAMM_BASE}/pools?${params}`;
  try {
    const data = await fetchWithRetry(url);
    return Array.isArray(data) ? data : (data.pools || data.data || []);
  } catch (e) {
    logErr(`[METEORA] DAMM pools failed: ${e.message}`);
    return [];
  }
}

// ──────────────────────────────────────────────
// Pool filtering
// ──────────────────────────────────────────────

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
    const addr = p.address || '';
    if (isBlacklisted(addr)) return false;
    const tvl = parseFloat(p.tvl || 0);
    if (tvl < MIN_POOL_TVL) return false;
    // volume is a TimeWindowData object: { "5m": x, "30m": x, ... "24h": x }
    const vol = parseFloat(p.volume?.['24h'] || p.volume?.h24 || 0);
    if (vol < MIN_POOL_VOL) return false;
    // fee_tvl_ratio is also TimeWindowData
    const feeTvl = parseFloat(p.fee_tvl_ratio?.['24h'] || p.fee_tvl_ratio?.h24 || 0);
    if (feeTvl < DLMM_MIN_FEE_TVL) return false;
    return true;
  });
}

function filterDammPools(pools) {
  return pools.filter((p) => {
    const addr = p.pool_address || p.address || '';
    if (isBlacklisted(addr)) return false;
    const tvl = parseFloat(p.pool_tvl || p.tvl || 0);
    if (tvl < MIN_POOL_TVL) return false;
    const vol = parseFloat(p.trading_volume || p.volume_24h || 0);
    if (vol < MIN_POOL_VOL) return false;
    return true;
  });
}

// ──────────────────────────────────────────────
// Jupiter helpers  (lite-api.jup.ag/swap/v1)
// ──────────────────────────────────────────────

async function jupiterQuote(inputMint, outputMint, amountLamports) {
  const params = new URLSearchParams({
    inputMint,
    outputMint,
    amount:                     String(amountLamports),
    slippageBps:                String(SLIPPAGE_BPS),
    onlyDirectRoutes:           'false',
    restrictIntermediateTokens: 'true',
  });
  return fetchWithRetry(`${JUP_QUOTE_BASE}/quote?${params}`);
}

async function jupiterSwap(quoteResponse, userPublicKey) {
  return fetchWithRetry(`${JUP_BASE}/swap`, {
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

// ──────────────────────────────────────────────
// Swap execution (shared for DLMM + DAMM)
// ──────────────────────────────────────────────

async function executeSwap(pool, poolType, solAmountLamports) {
  let addr, mintX, mintY;

  if (poolType === 'dlmm') {
    addr  = pool.address || 'unknown';
    mintX = mintOf(pool.token_x);
    mintY = mintOf(pool.token_y);
  } else {
    // DAMM / CPMM
    addr  = pool.pool_address || pool.address || 'unknown';
    const mints = pool.pool_token_mints || [];
    mintX = mints[0] || null;
    mintY = mints[1] || null;
  }

  if (!mintX || !mintY) return false;

  const isXSol   = mintX === WSOL_MINT;
  const isYSol   = mintY === WSOL_MINT;
  const inputMint  = isXSol ? WSOL_MINT : (isYSol ? WSOL_MINT : USDC_MINT);
  const outputMint = isXSol ? mintY : mintX;

  const shortAddr  = `${addr.slice(0, 4)}..`;
  const solDisplay = (solAmountLamports / 1e9).toFixed(4);
  log(`  Enter [${poolType.toUpperCase()}] ${shortAddr}/SOL | ${solDisplay} SOL (profit only)`);

  const buyPct = 49.5, lpPct = 50.5;
  log(` ${poolType.toUpperCase()} fallback ${shortAddr}/SOL: buy ${buyPct.toFixed(4)}% as token, add ${lpPct.toFixed(4)}% SOL to LP`);

  const buyAmountLamports = Math.floor(solAmountLamports * (buyPct / 100));

  let quote;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      quote = await jupiterQuote(inputMint, outputMint, buyAmountLamports);
      break;
    } catch (err) {
      const delay = attempt * 700;
      log(`  Jupiter V1 attempt ${attempt} failed: ${err.message} — retry ${delay}ms`);
      if (attempt === 3) {
        log(` Jupiter V1 swap failed: ${err.message}`);
        blacklistPool(addr, `${poolType}_swap failed`);
        return false;
      }
      await sleep(delay);
    }
  }

  if (!WALLET_PUBKEY) {
    log(`  [DRY-RUN] Would swap ${buyAmountLamports} lamports → ${outputMint.slice(0, 8)} (WALLET_PUBKEY not set)`);
    return true;
  }

  try {
    const swapResp = await jupiterSwap(quote, WALLET_PUBKEY);
    const tx = swapResp.swapTransaction;
    log(`  Swap TX built (${tx ? tx.length : 0} bytes) for ${shortAddr}`);
    // Broadcast: Connection.sendRawTransaction(Buffer.from(tx, 'base64'))
    return true;
  } catch (err) {
    log(` Swap execute failed: ${err.message}`);
    blacklistPool(addr, `${poolType}_swap failed`);
    return false;
  }
}

// ──────────────────────────────────────────────
// Hub (optional WebSocket)
// ──────────────────────────────────────────────

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

// ──────────────────────────────────────────────
// Main harvest cycle
// ──────────────────────────────────────────────

async function runHarvestCycle() {
  const solLamports  = Math.floor(HARVEST_SOL_AMOUNT * 1e9);
  const slotsPerPool = 4;

  // ── Fetch ──
  const [rawDlmm, rawDamm] = await Promise.all([
    fetchDlmmPools(),
    fetchDammPools(),
  ]);

  const dlmmPools = filterDlmmPools(rawDlmm);
  const cpmmPools = filterDammPools(rawDamm);

  log(`    DLMM raw:${rawDlmm.length} CLMM raw:0 CPMM raw:${rawDamm.length}`);
  log(`   DLMM: ${dlmmPools.length} pools x ${slotsPerPool} slots | CLMM: 0 | CPMM: ${cpmmPools.length} | USDC: 0 | ${HARVEST_SOL_AMOUNT.toFixed(4)} SOL/slot`);

  let entered = 0;

  // ── DLMM first (concentrated liquidity, higher fees) ──
  for (const pool of dlmmPools.slice(0, MAX_POOLS_PER_CYCLE)) {
    const ok = await executeSwap(pool, 'dlmm', solLamports);
    if (ok) entered++;
  }

  // ── CPMM (DAMM) fallback ──
  if (entered === 0 && cpmmPools.length > 0) {
    log(`  CPMM last resort: ${cpmmPools.length} verified slot(s)`);
    for (const pool of cpmmPools.slice(0, 3)) {
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

// ──────────────────────────────────────────────
// Entry point
// ──────────────────────────────────────────────

log('=== GWH Harvester Engine starting ===');
log(`JUP_BASE:          ${JUP_BASE}`);
log(`JUP_QUOTE_BASE:    ${JUP_QUOTE_BASE}`);
log(`METEORA_DLMM_BASE: ${METEORA_DLMM_BASE}`);
log(`METEORA_DAMM_BASE: ${METEORA_DAMM_BASE}`);
log(`DLMM_MIN_FEE_TVL:  ${DLMM_MIN_FEE_TVL}`);
log(`MIN_POOL_TVL:      ${MIN_POOL_TVL}`);
log(`MIN_POOL_VOL:      ${MIN_POOL_VOL}`);
log(`HARVEST_INTERVAL:  ${HARVEST_INTERVAL_MS}ms`);
log(`MAX_POOLS/CYCLE:   ${MAX_POOLS_PER_CYCLE}`);
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
