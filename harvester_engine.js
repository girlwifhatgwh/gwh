/**
 * GWH Harvester Engine (ESM)
 *
 * Pool sources:
 *   DLMM  → https://dlmm.datapi.meteora.ag/pools      (1-based pages, sorted volume_24h:desc)
 *   DAMM  → https://damm-api.meteora.ag/pools/search   (0-based pages)
 *
 * Jupiter → https://lite-api.jup.ag/swap/v1/quote  +  /swap
 *
 * Key loading order:
 *   1. key-manager.js → loadKeys() → injects HARVESTER_PRIVATE_KEY into process.env
 *   2. Windows Credential Manager direct read (target=HARVESTER_PRIVATE_KEY) via PowerShell
 *   3. HARVESTER_PRIVATE_KEY env var (fallback only — not in safe.env per security policy)
 *   4. dry-run mode
 */

import https from 'node:https';
import http  from 'node:http';
import { EventEmitter } from 'node:events';
import { existsSync, writeFileSync, unlinkSync } from 'node:fs';
import { resolve, join }   from 'node:path';
import { pathToFileURL }   from 'node:url';
import { execSync }        from 'node:child_process';
import { tmpdir }          from 'node:os';
import { Connection, Keypair, VersionedTransaction } from '@solana/web3.js';
import bs58 from 'bs58';

// ─────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────

const JUP_BASE          = process.env.JUP_BASE          || 'https://lite-api.jup.ag/swap/v1';
const JUP_QUOTE_BASE    = process.env.JUP_QUOTE_BASE    || 'https://lite-api.jup.ag/swap/v1';
const METEORA_DLMM_BASE = process.env.METEORA_DLMM_BASE || 'https://dlmm.datapi.meteora.ag';
const METEORA_DAMM_BASE = process.env.METEORA_DAMM_BASE || 'https://damm-api.meteora.ag';
const DLMM_MIN_FEE_TVL  = parseFloat(process.env.DLMM_MIN_FEE_TVL || '0');
const MIN_POOL_TVL      = parseFloat(process.env.MIN_POOL_TVL      || '0');
const MIN_POOL_VOL      = parseFloat(process.env.MIN_POOL_VOL      || '0');
const SOLANA_RPC_URL    = process.env.SOLANA_RPC_URL    || 'https://api.mainnet-beta.solana.com';
const JUP_API_KEY       = process.env.JUP_API_KEY       || '';
const HUB_URL           = process.env.HUB_URL           || '';
const HARVEST_INTERVAL_MS = parseInt(process.env.HARVEST_INTERVAL_MS || '60000', 10);
const MAX_FETCH_POOLS     = parseInt(process.env.MAX_FETCH_POOLS     || '100',   10);
const MAX_QUOTE_POOLS     = parseInt(process.env.MAX_QUOTE_POOLS     || '10',    10);
const JUP_CALL_DELAY_MS   = parseInt(process.env.JUP_CALL_DELAY_MS   || '300',   10);
const SLIPPAGE_BPS        = parseInt(process.env.SLIPPAGE_BPS        || '100',   10);
const HARVEST_SOL_AMOUNT  = parseFloat(process.env.HARVEST_SOL_AMOUNT || '0.05');
const CONFIRM_TX          = process.env.CONFIRM_TX === 'true';

const WSOL_MINT = 'So11111111111111111111111111111111111111112';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const blacklist = new Map();
let jupThrottledUntil = 0;

class RateLimitAbort extends Error {
  constructor(ms) { super('Jupiter rate-limited — aborting cycle'); this.retryAfterMs = ms; }
}

// ─────────────────────────────────────────────────────────
// Logging
// ─────────────────────────────────────────────────────────

function ts()        { return new Date().toISOString().replace('T', ' ').slice(0, 19); }
function log(msg)    { console.log(`[${ts()}] ${msg}`); }
function logErr(msg) { console.error(`[${ts()}] ${msg}`); }

// ─────────────────────────────────────────────────────────
// Key loading
//
// Tries three sources in order:
//   1. key-manager.js loadKeys() — reads from Windows Credential Manager,
//      injects into process.env. Uses file:// URL (fixes Windows path bug).
//   2. Windows Credential Manager direct read via PowerShell CredRead Win32 API.
//      Reads target=HARVESTER_PRIVATE_KEY, stored by cmdkey as Domain credential.
//   3. HARVESTER_PRIVATE_KEY env var (must not be in safe.env per security policy).
// ─────────────────────────────────────────────────────────

function keypairFromRaw(raw) {
  if (!raw) return null;
  raw = String(raw).trim();
  if (!raw || raw.includes('<') || raw.length < 32) return null;
  try {
    if (raw.startsWith('[')) return Keypair.fromSecretKey(Uint8Array.from(JSON.parse(raw)));
    return Keypair.fromSecretKey(bs58.decode(raw));
  } catch (e) {
    logErr(`[WALLET] Invalid key format: ${e.message}`);
    return null;
  }
}

async function loadPrivateKeyRaw() {
  // ── 1. key-manager.js ──────────────────────────────────
  const kmPath = resolve(process.cwd(), 'key-manager.js');
  if (existsSync(kmPath)) {
    try {
      // pathToFileURL fixes "Only URLs with a scheme..." error on Windows
      const kmUrl = pathToFileURL(kmPath).href;
      const km = await import(kmUrl);
      if (typeof km.loadKeys === 'function') {
        log('[WALLET] Calling key-manager.loadKeys() ...');
        await km.loadKeys();
        const injected = process.env.HARVESTER_PRIVATE_KEY;
        if (injected && injected.trim() && !injected.includes('<')) {
          log('[WALLET] Key loaded via key-manager.loadKeys()');
          return injected.trim();
        }
        log('[WALLET] loadKeys() ran but key not injected — trying direct WCM read');
      }
    } catch (e) {
      log(`[WALLET] key-manager.loadKeys() failed: ${e.message}`);
    }
  }

  // ── 2. Windows Credential Manager direct read via PowerShell ──
  // Write the PS1 script to a temp file (avoids all inline quoting/escaping issues).
  // Reads credential stored by: cmdkey /add:HARVESTER_PRIVATE_KEY /user:GWH_BOT
  // Tries all three Windows credential types: Generic(1), Domain(2), Certificate(3).
  const ps1Path = join(tmpdir(), `gwh_wcm_${process.pid}.ps1`);
  try {
    const psScript = [
      '$ErrorActionPreference = "Stop"',
      'Add-Type -TypeDefinition @"',
      'using System;',
      'using System.Runtime.InteropServices;',
      'using System.Text;',
      'public class WinCred {',
      '  [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)]',
      '  public struct CREDENTIAL {',
      '    public uint Flags; public uint Type; public string TargetName;',
      '    public string Comment; public long LastWritten;',
      '    public uint CredentialBlobSize; public IntPtr CredentialBlob;',
      '    public uint Persist; public uint AttributeCount; public IntPtr Attributes;',
      '    public string TargetAlias; public string UserName;',
      '  }',
      '  [DllImport("advapi32.dll", SetLastError=true, CharSet=CharSet.Unicode)]',
      '  public static extern bool CredReadW(string target, uint type, uint flags, out IntPtr cred);',
      '  [DllImport("advapi32.dll")]',
      '  public static extern void CredFree(IntPtr cred);',
      '  public static string Read(string target) {',
      '    IntPtr ptr = IntPtr.Zero;',
      '    foreach (uint t in new uint[]{1,2,3}) {',
      '      if (CredReadW(target, t, 0, out ptr)) {',
      '        var c = (CREDENTIAL)Marshal.PtrToStructure(ptr, typeof(CREDENTIAL));',
      '        byte[] b = new byte[c.CredentialBlobSize];',
      '        Marshal.Copy(c.CredentialBlob, b, 0, b.Length);',
      '        CredFree(ptr);',
      '        return Encoding.Unicode.GetString(b);',
      '      }',
      '    }',
      '    return "";',
      '  }',
      '}',
      '"@ -Language CSharp',
      '[WinCred]::Read("HARVESTER_PRIVATE_KEY")',
    ].join('\r\n');

    writeFileSync(ps1Path, psScript, 'utf8');

    const raw = execSync(
      `powershell -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "${ps1Path}"`,
      { encoding: 'utf8', timeout: 15000, windowsHide: true }
    ).trim();

    try { unlinkSync(ps1Path); } catch (_) {}

    if (raw && raw.length > 30 && !raw.includes('\n') && !raw.startsWith('Add-Type')) {
      log('[WALLET] Key loaded from Windows Credential Manager (target=HARVESTER_PRIVATE_KEY)');
      return raw;
    }
    if (raw) log(`[WALLET] WCM read returned unexpected value (len=${raw.length}), skipping`);
  } catch (e) {
    try { unlinkSync(ps1Path); } catch (_) {}
    log(`[WALLET] Windows Credential Manager read failed: ${e.message.split('\n')[0]}`);
  }

  // ── 3. Env var fallback ────────────────────────────────
  const envKey = process.env.HARVESTER_PRIVATE_KEY;
  if (envKey && envKey.trim() && !envKey.includes('<')) {
    log('[WALLET] Key loaded from HARVESTER_PRIVATE_KEY env var');
    return envKey.trim();
  }

  return null;
}

// ─────────────────────────────────────────────────────────
// HTTP helper
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
    if (JUP_API_KEY && url.hostname.includes('jup.ag')) headers['x-api-key'] = JUP_API_KEY;
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
          const retryMs = res.headers['retry-after'] ? parseInt(res.headers['retry-after'], 10) * 1000 : 0;
          return reject(Object.assign(new Error('429 Too Many Requests'), { status: 429, retryAfterMs: retryMs }));
        }
        if (res.statusCode < 200 || res.statusCode >= 300)
          return reject(Object.assign(new Error(`HTTP ${res.statusCode}: ${raw.slice(0, 200)}`), { status: res.statusCode }));
        try { resolve(JSON.parse(raw)); }
        catch (e) { reject(new Error(`JSON parse: ${e.message} | body=${raw.slice(0, 200)}`)); }
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
    try { return await request(urlStr, opts); }
    catch (err) {
      if (err.status === 429) logErr(`[HTTP] 429 — retrying after ${delay}ms...`);
      else if (attempt === maxRetries) throw err;
      await sleep(delay);
      delay = Math.min(delay * 2, 8000);
    }
  }
}

async function jupFetch(urlStr, opts = {}) {
  const now = Date.now();
  if (jupThrottledUntil > now) {
    const rem = jupThrottledUntil - now;
    log(`  [JUP] Rate-limit cooldown ${Math.ceil(rem / 1000)}s — skipping`);
    throw new RateLimitAbort(rem);
  }
  try { return await request(urlStr, opts); }
  catch (err) {
    if (err.status === 429) {
      const cooldown = err.retryAfterMs > 0 ? err.retryAfterMs : HARVEST_INTERVAL_MS;
      jupThrottledUntil = Date.now() + cooldown;
      log(`  [JUP] 429 — aborting cycle, cooling down ${Math.ceil(cooldown / 1000)}s`);
      throw new RateLimitAbort(cooldown);
    }
    throw err;
  }
}

// ─────────────────────────────────────────────────────────
// DLMM pool fetching
// ─────────────────────────────────────────────────────────

function mintOf(tok) {
  if (!tok) return null;
  if (typeof tok === 'string') return tok;
  return tok.address || tok.mint || tok.mint_address || null;
}

async function fetchDlmmPools() {
  const pools = []; let page = 1; const limit = 100;
  const params = new URLSearchParams({ page_size: String(limit), sort_by: 'volume_24h:desc', filter_by: 'is_blacklisted=false' });
  while (pools.length < MAX_FETCH_POOLS) {
    params.set('page', String(page));
    let data;
    try { data = await fetchWithRetry(`${METEORA_DLMM_BASE}/pools?${params}`); }
    catch (e) { logErr(`[DLMM] page ${page} failed: ${e.message}`); break; }
    const rows = data.data || [];
    if (!rows.length) break;
    pools.push(...rows);
    if (rows.length < limit || page >= (data.pages || 1)) break;
    page++;
  }
  return pools;
}

// ─────────────────────────────────────────────────────────
// DAMM pool fetching
// ─────────────────────────────────────────────────────────

async function fetchDammPools() {
  const pools = []; let page = 0; const size = 100;
  try {
    while (pools.length < MAX_FETCH_POOLS) {
      const params = new URLSearchParams({ page: String(page), size: String(size) });
      const data = await fetchWithRetry(`${METEORA_DAMM_BASE}/pools/search?${params}`);
      const rows = Array.isArray(data) ? data : (data.data || []);
      if (!rows.length) break;
      pools.push(...rows);
      const total = data.total_count || 0;
      if (rows.length < size || pools.length >= total) break;
      page++;
    }
    if (pools.length > 0) return pools;
  } catch (e) { logErr(`[DAMM] /pools/search failed: ${e.message}`); }
  try {
    const data = await fetchWithRetry(`${METEORA_DAMM_BASE}/pools`);
    return Array.isArray(data) ? data : (data.data || data.pools || []);
  } catch (e) { logErr(`[DAMM] /pools fallback failed: ${e.message}`); return []; }
}

// ─────────────────────────────────────────────────────────
// Pool filtering
// ─────────────────────────────────────────────────────────

function isBlacklisted(a) { return blacklist.has(a); }

function blacklistPool(address, reason) {
  const e = blacklist.get(address) || { count: 0 };
  e.count++; e.reason = reason; blacklist.set(address, e);
  log(`   Pool blacklisted: ${address.slice(0, 8)}... reason: ${reason} (fail #${e.count})`);
}

function filterDlmmPools(pools) {
  return pools.filter((p) => {
    if (isBlacklisted(p.address || '')) return false;
    return parseFloat(p.tvl || 0) >= MIN_POOL_TVL
      && parseFloat(p.volume?.['24h'] || p.volume?.h24 || 0) >= MIN_POOL_VOL
      && parseFloat(p.fee_tvl_ratio?.['24h'] || p.fee_tvl_ratio?.h24 || 0) >= DLMM_MIN_FEE_TVL;
  });
}

function filterDammPools(pools) {
  return pools.filter((p) => {
    if (isBlacklisted(p.pool_address || p.address || '')) return false;
    return parseFloat(p.pool_tvl || p.tvl || 0) >= MIN_POOL_TVL
      && parseFloat(p.trading_volume || p.fee_volume || 0) >= MIN_POOL_VOL;
  });
}

// ─────────────────────────────────────────────────────────
// Jupiter helpers
// ─────────────────────────────────────────────────────────

async function jupiterQuote(inputMint, outputMint, amountLamports) {
  const params = new URLSearchParams({
    inputMint, outputMint,
    amount:                     String(amountLamports),
    slippageBps:                String(SLIPPAGE_BPS),
    onlyDirectRoutes:           'false',
    restrictIntermediateTokens: 'true',
  });
  return jupFetch(`${JUP_QUOTE_BASE}/quote?${params}`);
}

async function jupiterSwapTx(quoteResponse, userPublicKey) {
  return jupFetch(`${JUP_BASE}/swap`, {
    method: 'POST',
    body: { quoteResponse, userPublicKey, wrapAndUnwrapSol: true, dynamicComputeUnitLimit: true, prioritizationFeeLamports: 'auto' },
  });
}

// ─────────────────────────────────────────────────────────
// Sign and send
// ─────────────────────────────────────────────────────────

async function signAndSend(swapTransaction, keypair, connection) {
  const tx = VersionedTransaction.deserialize(Buffer.from(swapTransaction, 'base64'));
  tx.sign([keypair]);
  const sig = await connection.sendRawTransaction(tx.serialize(), {
    skipPreflight: true, preflightCommitment: 'processed', maxRetries: 3,
  });
  log(`  TX sent: https://solscan.io/tx/${sig}`);
  if (CONFIRM_TX) {
    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash('finalized');
    const result = await connection.confirmTransaction({ signature: sig, blockhash, lastValidBlockHeight }, 'confirmed');
    if (result.value.err) throw new Error(`TX failed on-chain: ${JSON.stringify(result.value.err)}`);
    log(`  TX confirmed: ${sig}`);
  }
  return sig;
}

// ─────────────────────────────────────────────────────────
// Swap execution
// ─────────────────────────────────────────────────────────

async function executeSwap(pool, poolType, solAmountLamports, keypair, connection) {
  let addr, mintX, mintY;
  if (poolType === 'dlmm') {
    addr = pool.address || 'unknown'; mintX = mintOf(pool.token_x); mintY = mintOf(pool.token_y);
  } else {
    addr = pool.pool_address || pool.address || 'unknown';
    mintX = (pool.pool_token_mints || [])[0] || null;
    mintY = (pool.pool_token_mints || [])[1] || null;
  }
  if (!mintX || !mintY) return false;

  const isXSol    = mintX === WSOL_MINT, isYSol = mintY === WSOL_MINT;
  const inputMint  = isXSol ? WSOL_MINT : (isYSol ? WSOL_MINT : USDC_MINT);
  const outputMint = isXSol ? mintY : mintX;
  const shortAddr  = `${addr.slice(0, 4)}..`;
  const buyPct = 49.5, lpPct = 50.5;
  const buyAmountLamports = Math.floor(solAmountLamports * (buyPct / 100));

  log(`  Enter [${poolType.toUpperCase()}] ${shortAddr}/SOL | ${(solAmountLamports / 1e9).toFixed(4)} SOL (profit only)`);
  log(` ${poolType.toUpperCase()} fallback ${shortAddr}/SOL: buy ${buyPct.toFixed(4)}% as token, add ${lpPct.toFixed(4)}% SOL to LP`);

  if (JUP_CALL_DELAY_MS > 0) await sleep(JUP_CALL_DELAY_MS);

  let quote;
  try { quote = await jupiterQuote(inputMint, outputMint, buyAmountLamports); }
  catch (err) {
    if (err instanceof RateLimitAbort) throw err;
    log(` Jupiter quote failed for ${shortAddr}: ${err.message}`);
    blacklistPool(addr, `${poolType}_quote_failed`);
    return false;
  }

  if (!keypair) {
    log(`  [DRY-RUN] Would swap ${buyAmountLamports} lamports → ${outputMint.slice(0, 8)} (no signing key)`);
    return true;
  }

  let swapResp;
  try { swapResp = await jupiterSwapTx(quote, keypair.publicKey.toBase58()); }
  catch (err) {
    if (err instanceof RateLimitAbort) throw err;
    log(` Jupiter /swap failed for ${shortAddr}: ${err.message}`);
    blacklistPool(addr, `${poolType}_swap_failed`);
    return false;
  }

  try { await signAndSend(swapResp.swapTransaction, keypair, connection); return true; }
  catch (err) {
    log(` TX send failed for ${shortAddr}: ${err.message}`);
    blacklistPool(addr, `${poolType}_tx_failed`);
    return false;
  }
}

// ─────────────────────────────────────────────────────────
// Hub
// ─────────────────────────────────────────────────────────

const hubEmitter = new EventEmitter();
function connectHub() {
  if (!HUB_URL) return;
  import('ws').then(({ default: WebSocket }) => {
    const ws = new WebSocket(HUB_URL);
    ws.on('open',    ()    => { log('  Hub connected'); });
    ws.on('close',   ()    => { log('  Hub disconnected'); setTimeout(connectHub, 5000); });
    ws.on('error',   (e)   => { logErr(`Hub error: ${e.message}`); });
    ws.on('message', (msg) => { hubEmitter.emit('message', msg); });
  }).catch(() => {});
}

// ─────────────────────────────────────────────────────────
// Main harvest cycle
// ─────────────────────────────────────────────────────────

async function runHarvestCycle(keypair, connection) {
  const solLamports = Math.floor(HARVEST_SOL_AMOUNT * 1e9);
  if (jupThrottledUntil > Date.now()) {
    log(`  [JUP] Still cooling down — skipping cycle (${Math.ceil((jupThrottledUntil - Date.now()) / 1000)}s remaining)`);
    return;
  }

  const [rawDlmm, rawDamm] = await Promise.all([fetchDlmmPools(), fetchDammPools()]);
  const dlmmPools = filterDlmmPools(rawDlmm);
  const cpmmPools = filterDammPools(rawDamm);

  log(`    DLMM raw:${rawDlmm.length} CLMM raw:0 CPMM raw:${rawDamm.length}`);
  log(`   DLMM: ${dlmmPools.length} pools x 4 slots | CLMM: 0 | CPMM: ${cpmmPools.length} | USDC: 0 | ${HARVEST_SOL_AMOUNT.toFixed(4)} SOL/slot`);
  log(`   Quoting top ${Math.min(MAX_QUOTE_POOLS, dlmmPools.length)} DLMM pools (${JUP_CALL_DELAY_MS}ms inter-call delay)`);

  let entered = 0;
  try {
    for (const pool of dlmmPools.slice(0, MAX_QUOTE_POOLS)) {
      if (await executeSwap(pool, 'dlmm', solLamports, keypair, connection)) entered++;
    }
    if (entered === 0 && cpmmPools.length > 0) {
      log(`  CPMM last resort: ${cpmmPools.length} verified slot(s)`);
      for (const pool of cpmmPools.slice(0, Math.min(3, MAX_QUOTE_POOLS))) {
        if (await executeSwap(pool, 'cpmm', solLamports, keypair, connection)) entered++;
      }
    }
    if (entered === 0 && cpmmPools.length > 0) {
      log(`  No DLMM/CLMM pools entered — forcing CPMM fallback entry`);
      log(`    Force fallback: ${cpmmPools.length} CPMM pools available`);
      const pool = cpmmPools[0];
      log(`   Force entering CPMM: ${(pool.pool_address || pool.address || '').slice(0, 4)}../SOL`);
      await executeSwap(pool, 'cpmm', solLamports, keypair, connection);
    } else if (entered === 0) {
      log(`    No pools entered — reserves protected`);
    }
  } catch (err) {
    if (err instanceof RateLimitAbort) { log(`  [JUP] Cycle aborted — cooling down.`); return; }
    throw err;
  }
}

// ─────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────

const rawKey     = await loadPrivateKeyRaw();
const keypair    = keypairFromRaw(rawKey);
const connection = new Connection(SOLANA_RPC_URL, 'confirmed');

log('=== GWH Harvester Engine starting ===');
log(`JUP_BASE:          ${JUP_BASE}`);
log(`JUP_QUOTE_BASE:    ${JUP_QUOTE_BASE}`);
log(`METEORA_DLMM_BASE: ${METEORA_DLMM_BASE}`);
log(`METEORA_DAMM_BASE: ${METEORA_DAMM_BASE}`);
log(`SOLANA_RPC_URL:    ${SOLANA_RPC_URL}`);
log(`DLMM_MIN_FEE_TVL:  ${DLMM_MIN_FEE_TVL}`);
log(`MIN_POOL_TVL:      ${MIN_POOL_TVL}`);
log(`MIN_POOL_VOL:      ${MIN_POOL_VOL}`);
log(`HARVEST_INTERVAL:  ${HARVEST_INTERVAL_MS}ms`);
log(`MAX_FETCH_POOLS:   ${MAX_FETCH_POOLS}`);
log(`MAX_QUOTE_POOLS:   ${MAX_QUOTE_POOLS}`);
log(`JUP_CALL_DELAY:    ${JUP_CALL_DELAY_MS}ms`);
log(`HARVEST_SOL:       ${HARVEST_SOL_AMOUNT} SOL/slot`);
log(`CONFIRM_TX:        ${CONFIRM_TX}`);
if (keypair) {
  log(`WALLET:            ${keypair.publicKey.toBase58()} (LIVE — transactions will be signed and sent)`);
} else {
  log(`WALLET:            (no key — dry-run mode)`);
  log(`                   Run: cmdkey /add:HARVESTER_PRIVATE_KEY /user:GWH_BOT /pass:<your-key>`);
}
if (JUP_API_KEY) log(`JUP_API_KEY:       set`);

connectHub();
try { await runHarvestCycle(keypair, connection); } catch (e) { logErr(`[MAIN] ${e.message}`); }
setInterval(async () => {
  try { await runHarvestCycle(keypair, connection); } catch (e) { logErr(`[MAIN] ${e.message}`); }
}, HARVEST_INTERVAL_MS);
