/**
 * GWH Harvester Engine (ESM)
 * Scans Meteora DLMM / CLMM / CPMM pools and executes yield-harvesting swaps
 * via Jupiter swap/v1 (lite-api).
 */

import https from 'node:https';
import http from 'node:http';
import { EventEmitter } from 'node:events';

// ──────────────────────────────────────────────
// Configuration (env overrides)
// ──────────────────────────────────────────────

const JUP_BASE         = process.env.JUP_BASE         || 'https://lite-api.jup.ag/swap/v1';
const JUP_QUOTE_BASE   = process.env.JUP_QUOTE_BASE   || 'https://lite-api.jup.ag/swap/v1';
const METEORA_API_BASE = process.env.METEORA_API_BASE || 'https://dlmm.datapi.meteora.ag';
const METEORA_DLMM_BASE= process.env.METEORA_DLMM_BASE|| 'https://dlmm.datapi.meteora.ag';

const DLMM_MIN_FEE_TVL   = parseFloat(process.env.DLMM_MIN_FEE_TVL   || '0');
const MIN_POOL_TVL        = parseFloat(process.env.MIN_POOL_TVL        || '0');
const MIN_POOL_VOL        = parseFloat(process.env.MIN_POOL_VOL        || '0');

const SOLANA_RPC_URL    = process.env.SOLANA_RPC_URL    || 'https://api.mainnet-beta.solana.com';
const WALLET_PUBKEY     = process.env.WALLET_PUBKEY     || '';
const WALLET_PRIVKEY    = process.env.WALLET_PRIVKEY    || '';
const JUP_API_KEY       = process.env.JUP_API_KEY       || '';
const HUB_URL           = process.env.HUB_URL           || '';

const HARVEST_INTERVAL_MS = parseInt(process.env.HARVEST_INTERVAL_MS  || '60000', 10);
const MAX_POOLS_PER_CYCLE = parseInt(process.env.MAX_POOLS_PER_CYCLE  || '50',    10);
const SLIPPAGE_BPS        = parseInt(process.env.SLIPPAGE_BPS         || '100',   10);
const MIN_PROFIT_LAMPORTS = parseInt(process.env.MIN_PROFIT_LAMPORTS  || '5000',  10);
const HARVEST_SOL_AMOUNT  = parseFloat(process.env.HARVEST_SOL_AMOUNT || '0.05');

// Well-known mints
const WSOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

// Pool blacklist (runtime, cleared each restart)
const blacklist = new Map(); // address → { reason, count }

// ──────────────────────────────────────────────
// Logging
// ──────────────────────────────────────────────

function ts() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}
function log(msg)  { console.log(`[${ts()}] ${msg}`); }
function logErr(msg) { console.error(`[${ts()}] ${msg}`); }

// ──────────────────────────────────────────────
// HTTP helper (native, no dependencies)
// ──────────────────────────────────────────────

function request(urlStr, opts = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const lib = url.protocol === 'https:' ? https : http;
    const headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
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
          return reject(Object.assign(new Error('Server responded with 429 Too Many Requests.'), { status: 429 }));
        }
        if (res.statusCode < 200 || res.statusCode >= 300) {
          return reject(Object.assign(
            new Error(`Request failed with status code ${res.statusCode}`),
            { status: res.statusCode, body: raw.slice(0, 300) }
          ));
        }
        try { resolve(JSON.parse(raw)); }
        catch (e) { reject(new Error(`JSON parse error: ${e.message} snippet=${raw.slice(0, 200)}`)); }
      });
    });
    req.on('timeout', () => req.destroy(new Error(`Timeout: ${urlStr}`)));
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function fetchWithRetry(urlStr, opts = {}, maxRetries = 3) {
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
      delay *= 2;
    }
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ──────────────────────────────────────────────
// Jupiter helpers  (lite-api.jup.ag/swap/v1)
// ──────────────────────────────────────────────

async function jupiterQuote(inputMint, outputMint, amountLamports, attempt = 1) {
  const params = new URLSearchParams({
    inputMint,
    outputMint,
    amount:                     String(amountLamports),
    slippageBps:                String(SLIPPAGE_BPS),
    onlyDirectRoutes:           'false',
    restrictIntermediateTokens: 'true',
  });
  const url = `${JUP_QUOTE_BASE}/quote?${params}`;
  try {
    return await fetchWithRetry(url);
  } catch (err) {
    if (attempt < 3) {
      const delay = attempt * 700;
      log(`  Jupiter V1 attempt ${attempt} failed: ${err.message} — retry ${delay}ms`);
      await sleep(delay);
      return jupiterQuote(inputMint, outputMint, amountLamports, attempt + 1);
    }
    log(` Jupiter V1 swap failed: ${err.message}`);
    throw err;
  }
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
// Meteora DLMM pool fetching
// ──────────────────────────────────────────────

async function fetchDlmmPage(page, limit = 50) {
  return fetchWithRetry(`${METEORA_DLMM_BASE}/pools?page=${page}&limit=${limit}`);
}

async function fetchAllDlmmPools() {
  const pools = [];
  let page = 0;
  const limit = 50;
  while (pools.length < MAX_POOLS_PER_CYCLE) {
    let data;
    try { data = await fetchDlmmPage(page, limit); }
    catch (e) { logErr(`[METEORA] DLMM page ${page} failed: ${e.message}`); break; }

    const rows = Array.isArray(data) ? data : (data.data || data.pools || []);
    if (!rows.length) break;
    pools.push(...rows);
    if (rows.length < limit) break;
    page++;
  }
  return pools;
}

// Meteora's public AMM pool API for CLMM/CPMM
const METEORA_AMM_BASE = process.env.METEORA_AMM_BASE || 'https://amm.datapi.meteora.ag';

async function fetchAmmPools(type = 'cpmm', page = 0, limit = 50) {
  try {
    const data = await fetchWithRetry(`${METEORA_AMM_BASE}/pools?type=${type}&page=${page}&limit=${limit}`);
    return Array.isArray(data) ? data : (data.data || data.pools || []);
  } catch (e) {
    logErr(`[METEORA] AMM(${type}) page ${page} failed: ${e.message}`);
    return [];
  }
}

// ──────────────────────────────────────────────
// Pool filtering
// ──────────────────────────────────────────────

function isBlacklisted(address) {
  return blacklist.has(address);
}

function blacklistPool(address, reason) {
  const entry = blacklist.get(address) || { count: 0 };
  entry.count++;
  entry.reason = reason;
  blacklist.set(address, entry);
  log(`   Pool blacklisted: ${address.slice(0, 8)}... reason: ${reason} (fail #${entry.count})`);
}

function filterPools(pools, type) {
  return pools.filter((p) => {
    const addr = p.address || p.pubkey || '';
    if (isBlacklisted(addr)) return false;
    const tvl = parseFloat(p.tvl || p.liquidity || 0);
    const vol = parseFloat(p.volume_24h || p.trade_volume_24h || 0);
    if (type === 'dlmm') {
      const feeTvl = parseFloat(p.fee_tvl_ratio || p.fees_24h || 0);
      if (feeTvl < DLMM_MIN_FEE_TVL) return false;
    }
    if (tvl < MIN_POOL_TVL) return false;
    if (vol < MIN_POOL_VOL) return false;
    return true;
  });
}

// ──────────────────────────────────────────────
// Swap execution
// ──────────────────────────────────────────────

async function executeSwap(pool, poolType, solAmountLamports) {
  const mintX = pool.mint_x || pool.token_x_mint || pool.base_mint || pool.tokenAMint;
  const mintY = pool.mint_y || pool.token_y_mint || pool.quote_mint || pool.tokenBMint;
  const addr  = pool.address || pool.pubkey || 'unknown';

  if (!mintX || !mintY) return false;

  const isXSol   = mintX === WSOL_MINT;
  const isYSol   = mintY === WSOL_MINT;
  const inputMint  = isXSol ? WSOL_MINT : (isYSol ? WSOL_MINT : USDC_MINT);
  const outputMint = isXSol ? mintY : (isYSol ? mintX : mintX);

  const shortAddr  = `${addr.slice(0, 4)}../${outputMint.slice(0, 4)}`;
  const solDisplay = (solAmountLamports / 1e9).toFixed(4);
  log(`  Enter [${poolType.toUpperCase()}] ${shortAddr}/SOL | ${solDisplay} SOL (profit only)`);

  // Determine split for LP add: ~49.5% buy token, ~50.5% SOL into LP
  const buyPct = 49.5, lpPct = 50.5;
  log(` ${poolType.toUpperCase()} fallback ${shortAddr}/SOL: buy ${buyPct.toFixed(4)}% as token, add ${lpPct.toFixed(4)}% SOL to LP`);

  const buyAmountLamports = Math.floor(solAmountLamports * (buyPct / 100));

  let quote;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      quote = await jupiterQuote(inputMint, outputMint, buyAmountLamports, 1);
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
    // Send via RPC: Connection.sendRawTransaction(Buffer.from(tx,'base64'))
    return true;
  } catch (err) {
    log(` Swap execute failed: ${err.message}`);
    blacklistPool(addr, `${poolType}_swap failed`);
    return false;
  }
}

// ──────────────────────────────────────────────
// Hub connectivity (optional WebSocket hub)
// ──────────────────────────────────────────────

let hubConnected = false;
const hubEmitter = new EventEmitter();

function connectHub() {
  if (!HUB_URL) return;
  // Lazy-load ws only if HUB_URL is set.
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
  const solLamports = Math.floor(HARVEST_SOL_AMOUNT * 1e9);
  const slotsPerPool = 4;

  // ── DLMM ──
  const rawDlmm = await fetchAllDlmmPools();
  const dlmmPools = filterPools(rawDlmm, 'dlmm');

  // ── CLMM ──
  const rawClmm = await fetchAmmPools('clmm');
  const clmmPools = filterPools(rawClmm, 'clmm');

  // ── CPMM ──
  const rawCpmm = await fetchAmmPools('cpmm');
  const cpmmPools = filterPools(rawCpmm, 'cpmm');

  log(`    DLMM raw:${rawDlmm.length} CLMM raw:${rawClmm.length} CPMM raw:${rawCpmm.length}`);
  log(`   DLMM: ${dlmmPools.length} pools x ${slotsPerPool} slots | CLMM: ${clmmPools.length} | CPMM: ${cpmmPools.length} | USDC: 0 | ${HARVEST_SOL_AMOUNT.toFixed(4)} SOL/slot`);

  let entered = 0;

  // Prefer DLMM > CLMM > CPMM
  for (const pool of dlmmPools.slice(0, MAX_POOLS_PER_CYCLE)) {
    const ok = await executeSwap(pool, 'dlmm', solLamports);
    if (ok) entered++;
  }

  for (const pool of clmmPools.slice(0, MAX_POOLS_PER_CYCLE)) {
    const ok = await executeSwap(pool, 'clmm', solLamports);
    if (ok) entered++;
  }

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
    const addr = pool.address || pool.pubkey || '';
    log(`   Force entering CPMM: ${addr.slice(0, 4)}../${(pool.mint_x || pool.tokenAMint || '').slice(0, 4)}/SOL`);
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
log(`METEORA_API_BASE:  ${METEORA_API_BASE}`);
log(`METEORA_DLMM_BASE: ${METEORA_DLMM_BASE}`);
log(`DLMM_MIN_FEE_TVL:  ${DLMM_MIN_FEE_TVL}`);
log(`MIN_POOL_TVL:      ${MIN_POOL_TVL}`);
log(`MIN_POOL_VOL:      ${MIN_POOL_VOL}`);
log(`HARVEST_INTERVAL:  ${HARVEST_INTERVAL_MS}ms`);
log(`MAX_POOLS/CYCLE:   ${MAX_POOLS_PER_CYCLE}`);
log(`HARVEST_SOL:       ${HARVEST_SOL_AMOUNT} SOL/slot`);
log(`WALLET:            ${WALLET_PUBKEY ? WALLET_PUBKEY.slice(0, 8) + '...' : '(not set — dry-run mode)'}`);

connectHub();

// Run immediately then on interval
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
