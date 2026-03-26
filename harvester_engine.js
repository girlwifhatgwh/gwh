'use strict';

/**
 * GWH Harvester Engine
 * Scans Meteora DLMM pools and executes yield-harvesting swaps via Jupiter.
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

// ──────────────────────────────────────────────
// Configuration (overridden by env)
// ──────────────────────────────────────────────

const JUP_BASE        = process.env.JUP_BASE        || 'https://lite-api.jup.ag/swap/v1';
const JUP_QUOTE_BASE  = process.env.JUP_QUOTE_BASE  || 'https://lite-api.jup.ag/swap/v1';
const METEORA_API_BASE= process.env.METEORA_API_BASE || 'https://dlmm.datapi.meteora.ag';
const METEORA_DLMM_BASE= process.env.METEORA_DLMM_BASE || 'https://dlmm.datapi.meteora.ag';

const DLMM_MIN_FEE_TVL = parseFloat(process.env.DLMM_MIN_FEE_TVL || '0');
const MIN_POOL_TVL     = parseFloat(process.env.MIN_POOL_TVL     || '0');
const MIN_POOL_VOL     = parseFloat(process.env.MIN_POOL_VOL     || '0');

const SOLANA_RPC_URL   = process.env.SOLANA_RPC_URL  || 'https://api.mainnet-beta.solana.com';
const WALLET_PUBKEY    = process.env.WALLET_PUBKEY   || '';
const WALLET_PRIVKEY   = process.env.WALLET_PRIVKEY  || '';
const JUP_API_KEY      = process.env.JUP_API_KEY     || '';

const HARVEST_INTERVAL_MS  = parseInt(process.env.HARVEST_INTERVAL_MS  || '60000', 10);
const MAX_POOLS_PER_CYCLE  = parseInt(process.env.MAX_POOLS_PER_CYCLE  || '50',    10);
const SLIPPAGE_BPS         = parseInt(process.env.SLIPPAGE_BPS         || '100',   10);
const MIN_PROFIT_LAMPORTS  = parseInt(process.env.MIN_PROFIT_LAMPORTS  || '5000',  10);

// Well-known mints
const WSOL_MINT  = 'So11111111111111111111111111111111111111112';
const USDC_MINT  = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

// ──────────────────────────────────────────────
// HTTP helpers
// ──────────────────────────────────────────────

function fetchJson(urlStr, opts) {
  opts = opts || {};
  return new Promise(function (resolve, reject) {
    const url = new URL(urlStr);
    const lib = url.protocol === 'https:' ? https : http;
    const reqOpts = {
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname + url.search,
      method: opts.method || 'GET',
      headers: Object.assign({
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      }, opts.headers || {}),
      timeout: opts.timeout || 15000,
    };
    if (JUP_API_KEY && (url.hostname.includes('jup.ag') || url.hostname.includes('lite-api'))) {
      reqOpts.headers['x-api-key'] = JUP_API_KEY;
    }
    const req = lib.request(reqOpts, function (res) {
      let body = '';
      res.on('data', function (c) { body += c; });
      res.on('end', function () {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try { resolve(JSON.parse(body)); }
          catch (e) { reject(new Error('JSON parse error: ' + e.message + ' body=' + body.slice(0, 200))); }
        } else {
          reject(new Error('HTTP ' + res.statusCode + ' ' + urlStr + ' body=' + body.slice(0, 300)));
        }
      });
    });
    req.on('timeout', function () { req.destroy(new Error('Request timeout: ' + urlStr)); });
    req.on('error', reject);
    if (opts.body) req.write(typeof opts.body === 'string' ? opts.body : JSON.stringify(opts.body));
    req.end();
  });
}

// ──────────────────────────────────────────────
// Meteora DLMM helpers
// ──────────────────────────────────────────────

async function fetchDlmmPools(page, limit) {
  page  = page  || 0;
  limit = limit || 50;
  const url = METEORA_DLMM_BASE + '/pools?page=' + page + '&limit=' + limit;
  return fetchJson(url);
}

async function fetchAllDlmmPools() {
  const pools = [];
  let page = 0;
  const limit = 50;
  while (true) {
    let data;
    try { data = await fetchDlmmPools(page, limit); }
    catch (e) {
      console.error('[METEORA] Failed to fetch pools page', page, e.message);
      break;
    }
    const rows = data.data || data.pools || data || [];
    if (!Array.isArray(rows) || rows.length === 0) break;
    for (const pool of rows) pools.push(pool);
    if (rows.length < limit) break;
    page++;
    if (pools.length >= MAX_POOLS_PER_CYCLE) break;
  }
  return pools;
}

function filterPools(pools) {
  return pools.filter(function (p) {
    const tvl = parseFloat(p.tvl || p.liquidity || 0);
    const vol = parseFloat(p.volume_24h || p.trade_volume_24h || 0);
    const fee = parseFloat(p.fee_tvl_ratio || p.fees_24h || 0);
    if (tvl < MIN_POOL_TVL) return false;
    if (vol < MIN_POOL_VOL) return false;
    if (fee < DLMM_MIN_FEE_TVL) return false;
    return true;
  });
}

async function fetchPoolStats(address) {
  const url = METEORA_API_BASE + '/pools/' + address;
  return fetchJson(url);
}

// ──────────────────────────────────────────────
// Jupiter helpers
// ──────────────────────────────────────────────

async function jupiterQuote(inputMint, outputMint, amountLamports) {
  const params = new URLSearchParams({
    inputMint:  inputMint,
    outputMint: outputMint,
    amount:     String(amountLamports),
    slippageBps: String(SLIPPAGE_BPS),
    onlyDirectRoutes: 'false',
    restrictIntermediateTokens: 'true',
  });
  const url = JUP_QUOTE_BASE + '/quote?' + params.toString();
  return fetchJson(url);
}

async function jupiterSwap(quoteResponse, userPublicKey) {
  const url = JUP_BASE + '/swap';
  return fetchJson(url, {
    method: 'POST',
    body: JSON.stringify({
      quoteResponse: quoteResponse,
      userPublicKey: userPublicKey,
      wrapAndUnwrapSol: true,
      dynamicComputeUnitLimit: true,
      prioritizationFeeLamports: 'auto',
    }),
  });
}

// ──────────────────────────────────────────────
// Harvest logic
// ──────────────────────────────────────────────

async function evaluatePool(pool) {
  const mintX = pool.mint_x || pool.token_x_mint || pool.base_mint;
  const mintY = pool.mint_y || pool.token_y_mint || pool.quote_mint;
  if (!mintX || !mintY) return null;

  const targetMint = mintX === WSOL_MINT ? mintY : mintX;
  const sourceMint = mintX === WSOL_MINT ? WSOL_MINT : (mintY === WSOL_MINT ? WSOL_MINT : USDC_MINT);

  const amountIn = 1_000_000_000; // 1 SOL in lamports
  let quote;
  try {
    quote = await jupiterQuote(sourceMint, targetMint, amountIn);
  } catch (e) {
    return null;
  }

  const outAmount = parseInt(quote.outAmount || 0, 10);
  if (outAmount <= 0) return null;

  return { pool, quote, sourceMint, targetMint, amountIn, outAmount };
}

async function executeHarvest(candidate) {
  if (!WALLET_PUBKEY) {
    console.log('[HARVEST] WALLET_PUBKEY not set — dry-run only.');
    return;
  }
  try {
    const swapResp = await jupiterSwap(candidate.quote, WALLET_PUBKEY);
    const swapTx  = swapResp.swapTransaction;
    console.log('[HARVEST] Swap transaction built, length:', swapTx ? swapTx.length : 0);
    // Signing and sending requires @solana/web3.js — left as integration point.
    // Add: Connection.sendRawTransaction(Buffer.from(swapTx, 'base64'))
  } catch (e) {
    console.error('[HARVEST] Execute failed:', e.message);
  }
}

// ──────────────────────────────────────────────
// Main harvest cycle
// ──────────────────────────────────────────────

async function runHarvestCycle() {
  console.log('[CYCLE] Starting harvest cycle at', new Date().toISOString());
  console.log('[CYCLE] Endpoints: JUP=' + JUP_BASE + ' METEORA=' + METEORA_API_BASE);
  console.log('[CYCLE] Filters: MIN_TVL=' + MIN_POOL_TVL + ' MIN_VOL=' + MIN_POOL_VOL + ' MIN_FEE_TVL=' + DLMM_MIN_FEE_TVL);

  let pools;
  try {
    pools = await fetchAllDlmmPools();
  } catch (e) {
    console.error('[CYCLE] Failed to fetch pools:', e.message);
    return;
  }

  console.log('[CYCLE] Fetched', pools.length, 'pools from Meteora DLMM');
  const eligible = filterPools(pools);
  console.log('[CYCLE] Eligible after filtering:', eligible.length);

  const candidates = [];
  for (const pool of eligible.slice(0, MAX_POOLS_PER_CYCLE)) {
    const result = await evaluatePool(pool);
    if (result) candidates.push(result);
  }

  console.log('[CYCLE] Quote candidates:', candidates.length);

  candidates.sort(function (a, b) { return b.outAmount - a.outAmount; });

  for (const c of candidates.slice(0, 3)) {
    const poolAddr = c.pool.address || c.pool.pubkey || 'unknown';
    console.log('[HARVEST] Pool:', poolAddr, '| In:', c.amountIn, c.sourceMint.slice(0, 8), '| Out:', c.outAmount, c.targetMint.slice(0, 8));
    if (c.outAmount >= MIN_PROFIT_LAMPORTS) {
      await executeHarvest(c);
    } else {
      console.log('[HARVEST] Skipped — below MIN_PROFIT_LAMPORTS threshold');
    }
  }

  console.log('[CYCLE] Cycle complete at', new Date().toISOString());
}

// ──────────────────────────────────────────────
// Entry point
// ──────────────────────────────────────────────

async function main() {
  console.log('=== GWH Harvester Engine starting ===');
  console.log('JUP_BASE:         ', JUP_BASE);
  console.log('JUP_QUOTE_BASE:   ', JUP_QUOTE_BASE);
  console.log('METEORA_API_BASE: ', METEORA_API_BASE);
  console.log('METEORA_DLMM_BASE:', METEORA_DLMM_BASE);
  console.log('DLMM_MIN_FEE_TVL: ', DLMM_MIN_FEE_TVL);
  console.log('MIN_POOL_TVL:     ', MIN_POOL_TVL);
  console.log('MIN_POOL_VOL:     ', MIN_POOL_VOL);
  console.log('HARVEST_INTERVAL: ', HARVEST_INTERVAL_MS, 'ms');
  console.log('MAX_POOLS/CYCLE:  ', MAX_POOLS_PER_CYCLE);

  // Run immediately, then on interval
  await runHarvestCycle();
  setInterval(function () {
    runHarvestCycle().catch(function (e) {
      console.error('[MAIN] Unhandled cycle error:', e.message);
    });
  }, HARVEST_INTERVAL_MS);
}

main().catch(function (e) {
  console.error('[MAIN] Fatal:', e.message);
  process.exit(1);
});
