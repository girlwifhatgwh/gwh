/**
 * GWH HARVESTER v14.0
 * DLMM + CPMM + CLMM + Copy-LP + Dual-Asset 6-Way Distribution
 *
 * Merged from:
 *   - v13.0 full architecture (DLMM/CPMM/CLMM engines, distribution, copy-LP)
 *   - Working key-injection (HARVESTER_PRIVATE_KEY via PM2 env / key-manager.js)
 *   - Confirmed working API endpoints (lite-api.jup.ag, dlmm.datapi.meteora.ag)
 *   - Jupiter VersionedTransaction signing (not legacy Transaction)
 *   - Abort-on-429 Jupiter rate-limit strategy
 *   - PQueue rate limiter (3 calls/sec)
 *
 * Bugs fixed from v13.0:
 *   - __normDlmmPool used `const 'string literal'` (invalid JS) -- removed dead code
 *   - `dlmmOnly` referenced before declaration -- fixed
 *   - `vault` used before assignment in getSolBalRaw -- reordered
 *   - Key loading now uses PM2 env / key-manager.js pattern (no throw at module level)
 *   - pathToFileURL used for Windows-safe ESM import of key-manager.js
 */

// ── DNS IPv4-first (Windows VPS fix) ──────────────────────────────────────
import dns   from 'node:dns';
import https from 'node:https';
import http  from 'node:http';
dns.setDefaultResultOrder('ipv4first');

const ipv4Agent = new https.Agent({
  family: 4,
  lookup: (hostname, options, callback) => dns.lookup(hostname, { ...options, family: 4 }, callback),
});

// ── Core imports ──────────────────────────────────────────────────────────
import dotenv        from 'dotenv';
import path          from 'path';
import { fileURLToPath } from 'url';
import { pathToFileURL } from 'node:url';
import { existsSync }    from 'node:fs';
import { execSync }      from 'node:child_process';
import { resolve as pathResolve } from 'node:path';
import { join as pathJoin }       from 'node:path';
import { writeFileSync, unlinkSync } from 'node:fs';
import { tmpdir }    from 'node:os';
import { io as ioClient } from 'socket.io-client';
import fs            from 'fs';
import {
  Connection, Keypair, LAMPORTS_PER_SOL, PublicKey,
  SystemProgram, Transaction, TransactionInstruction,
  ComputeBudgetProgram, VersionedTransaction,
} from '@solana/web3.js';
import {
  getAssociatedTokenAddressSync, getAccount,
  createAssociatedTokenAccountInstruction,
  createSyncNativeInstruction, createCloseAccountInstruction,
  createTransferCheckedInstruction, getMint,
  TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import bs58   from 'bs58';
import axios  from 'axios';
import BN     from 'bn.js';
import PQueue from 'p-queue';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, 'safe.env'), override: true });

// ── Global rate limiter: max 3 external calls/sec ─────────────────────────
const apiQueue = new PQueue({ interval: 1000, intervalCap: 3 });
const JSON_HEADERS = { 'Content-Type': 'application/json' };
const axiosGet  = (url, cfg={}) => apiQueue.add(() => axios.get(url, {
  ...cfg, httpsAgent: ipv4Agent,
  headers: { ...JSON_HEADERS, ...(cfg.headers || {}) },
}));
const axiosPost = (url, data, cfg={}) => apiQueue.add(() => axios.post(url, data, {
  ...cfg, httpsAgent: ipv4Agent,
  headers: { ...JSON_HEADERS, ...(cfg.headers || {}) },
}));

// ── Program IDs ───────────────────────────────────────────────────────────
const CPMM_PROGRAM  = new PublicKey('CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C');
const DLMM_PROGRAM  = new PublicKey('LBUZKhRxPF3XUpBCjp4YzTKgLLjgLmEFXFHBGe6mJJu');
const WSOL_MINT     = new PublicKey('So11111111111111111111111111111111111111112');
const SOL_MINT_STR  = 'So11111111111111111111111111111111111111112';
const USDC_MINT_STR = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const USDT_MINT_STR = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB';
const GWH_POOL_ID   = 'EYsX6vFYPbjWucsnEUVwqupdAozjv9BAw3thWJRXw9sm';
const GWH_MINT_STR  = '5cpDwB3LJJYaf5XjghHRXji7TKw9NFpfJJdDWw8Pxpct';
const USDC_MINT_PK  = new PublicKey(USDC_MINT_STR);
const MEMO_PID      = new PublicKey('MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr');

// ── CPMM layout offsets ───────────────────────────────────────────────────
const CPMM_OFF = {
  ammConfig:8, token0Vault:72, token1Vault:104, lpMint:136,
  token0Mint:168, token1Mint:200, token0Program:232,
  token1Program:264, observationKey:296,
};
const readPk = (d, o) => new PublicKey(d.slice(o, o + 32));

// ── Config ────────────────────────────────────────────────────────────────
const CFG = {
  PRINCIPAL:         parseFloat(process.env.PRINCIPAL_LOCK    || '0.05'),
  MIN_VAULT_SOL:     parseFloat(process.env.MIN_VAULT_SOL     || '0.05'),
  GAS_RESERVE:       parseFloat(process.env.GAS_RESERVE       || '0.01'),
  DEPLOY_PCT:        parseFloat(process.env.DEPLOY_PCT        || '0.80'),
  LIQUID_RESERVE:    parseFloat(process.env.LIQUID_RESERVE    || '0.10'),
  DLMM_RESERVE:      parseFloat(process.env.DLMM_RESERVE      || '0.02'),
  CPMM_RESERVE:      parseFloat(process.env.CPMM_RESERVE      || '0.02'),
  MIN_PER_SLOT:      parseFloat(process.env.MIN_PER_SLOT       || '0.05'),
  MAX_POSITIONS:     parseInt  (process.env.MAX_POSITIONS      || '5'),
  SLIPPAGE:          parseFloat(process.env.SLIPPAGE           || '0.03'),
  CHECK_MS:          parseInt  (process.env.CHECK_MS           || String(20*60*1000)),
  RESCAN_MS:         parseInt  (process.env.RESCAN_MS          || String(4*3600*1000)),
  HARVEST_COOL_MS:   parseInt  (process.env.HARVEST_COOL_MS    || String(2*3600*1000)),
  HARVEST_PCT:       parseFloat(process.env.HARVEST_PCT        || '0.10'),
  MIN_HARVEST_SOL:   parseFloat(process.env.MIN_HARVEST_SOL    || '0.005'),
  DLMM_BIN_RANGE:    parseInt  (process.env.DLMM_BIN_RANGE     || '20'),
  DLMM_BIN_MIN:      parseInt  (process.env.DLMM_BIN_MIN       || '8'),
  DLMM_BIN_MAX:      parseInt  (process.env.DLMM_BIN_MAX       || '50'),
  DLMM_VOL_LOW:      parseFloat(process.env.DLMM_VOL_LOW       || '0.03'),
  DLMM_VOL_HIGH:     parseFloat(process.env.DLMM_VOL_HIGH      || '0.15'),
  DLMM_MIN_FEE_TVL:  parseFloat(process.env.DLMM_MIN_FEE_TVL   || '0.0002'),
  DLMM_REBAL_PCT:    parseFloat(process.env.DLMM_REBAL_PCT     || '0.80'),
  DIST_VAULT_PCT:    parseFloat(process.env.DIST_VAULT_PCT     || '0.40'),
  DIST_DEV_PCT:      parseFloat(process.env.DIST_DEV_PCT       || '0.15'),
  DIST_APEX_PCT:     parseFloat(process.env.DIST_APEX_PCT      || '0.12'),
  DIST_LP_PCT:       parseFloat(process.env.DIST_LP_PCT        || '0.10'),
  DIST_BURN_PCT:     parseFloat(process.env.DIST_BURN_PCT      || '0.10'),
  DIST_COMMUNITY_PCT:parseFloat(process.env.DIST_COMMUNITY_PCT || '0.10'),
  DIST_SELL_PCT:     parseFloat(process.env.DIST_SELL_PCT      || '0.03'),
  GWH_LP_WALLET:     process.env.GWH_LP_WALLET      || 'ChGXTzXdHw6ZocYLP3NV1iN2bbisfau4Kzkf5fmATDGh',
  GWH_BURN_WALLET:   process.env.GWH_BURN_WALLET    || 'ChGXTzXdHw6ZocYLP3NV1iN2bbisfau4Kzkf5fmATDGh',
  COMMUNITY_WALLET:  process.env.COMMUNITY_WALLET   || '9Xqw5JNobAXxuLin3yjEAcNys8frgBo7g8mYBSgf6UxT',
  DEV_WALLET:        process.env.DEV_WALLET         || '6NUgiMWCAU1HNhwa3ui1iBgZXofEAVt5kdG2hhLknSab',
  MIN_DIST_SOL:      parseFloat(process.env.MIN_DIST_SOL       || '0.05'),
  DIST_TIMES:        process.env.DIST_TIMES         || '06:00,18:00',
  TP_PCT:            parseFloat(process.env.TP_PCT             || '0.25'),
  SL_PCT:            parseFloat(process.env.SL_PCT             || '0.15'),
  IL_EXIT_PCT:       parseFloat(process.env.IL_EXIT_PCT        || '0.30'),
  CLMM_STABLE_PAIRS: process.env.CLMM_STABLE_PAIRS  || 'USDC/USDT,SOL/jitoSOL,SOL/mSOL,USDC/SOL',
  CLMM_TICK_SPACING: parseInt  (process.env.CLMM_TICK_SPACING  || '1'),
  CLMM_RANGE_PCT:    parseFloat(process.env.CLMM_RANGE_PCT    || '0.005'),
  SOL_TRACK_PCT:     parseFloat(process.env.SOL_TRACK_PCT      || '0.80'),
  USDC_TRACK_ENABLED:process.env.USDC_TRACK !== 'false',
  MIN_POOL_TVL:      parseFloat(process.env.MIN_POOL_TVL       || '1000'),
  MIN_POOL_VOL:      parseFloat(process.env.MIN_POOL_VOL       || '10000'),
  COPY_LP_WALLETS:   (process.env.COPY_LP_WALLETS || '').split(',').filter(Boolean),
  COPY_LP_MIN_WR:    parseFloat(process.env.COPY_LP_MIN_WR     || '0.75'),
  COPY_LP_DELAY_MS:  parseInt  (process.env.COPY_LP_DELAY_MS   || '30000'),
  DAILY_DEPLOY_CAP:  parseFloat(process.env.DAILY_DEPLOY_CAP   || '0.20'),
  TP_LEVELS:         process.env.TP_LEVELS          || '0.03,0.05,0.10,0.25',
};

const DIST_WALLETS = {
  DEV:       process.env.DEV_WALLET        || '6NUgiMWCAU1HNhwa3ui1iBgZXofEAVt5kdG2hhLknSab',
  APEX:      process.env.APEX_WALLET       || 'ChGXTzXdHw6ZocYLP3NV1iN2bbisfau4Kzkf5fmATDGh',
  COMMUNITY: process.env.COMMUNITY_WALLET  || '9Xqw5JNobAXxuLin3yjEAcNys8frgBo7g8mYBSgf6UxT',
  LP_GROWTH: process.env.LP_WALLET         || 'ChGXTzXdHw6ZocYLP3NV1iN2bbisfau4Kzkf5fmATDGh',
  BURN:      '11111111111111111111111111111111',
};

const USDC_FLOOR        = parseInt(process.env.MIN_VAULT_USDC || '1000000');
const POSITIONS_FILE    = './harvester_positions.json';
const BASELINE_FILE     = './harvester_baseline.json';
const DIST_LEDGER_FILE  = './harvester_distributions.json';

// ── Logging (must be defined before anything that calls log()) ────────────
const ts    = () => new Date().toISOString().replace('T', ' ').slice(0, 19);
const f4    = n  => Number(n).toFixed(4);
const f2    = n  => Number(n).toFixed(2);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const bnToNum = (bn) => bn.gt(new BN(Number.MAX_SAFE_INTEGER.toString()))
  ? Number.MAX_SAFE_INTEGER : bn.toNumber();

let _hub = null;
const emit = (ev, d) => { try { _hub?.emit(ev, d); } catch {} };

const log = (msg, type = 'system') => {
  const icons = {
    system:'  ', success:'✅', error:'❌', warning:'⚠️ ',
    profit:'💰', lp:'🌊', scan:'🔍', dlmm:'🎯', rebal:'🔄',
  };
  console.log(`[${ts()}] ${icons[type] ?? '*'} ${msg}`);
  emit('terminal_log',  { message: `[HARV] ${msg}`, type, timestamp: ts(), _src: 'HARV' });
  emit('harvester_log', { message: msg, type, timestamp: ts() });
};

// ── Key loading ───────────────────────────────────────────────────────────
// Order: key-manager.js → HARVESTER_PRIVATE_KEY env → dry-run

function keypairFromRaw(raw) {
  if (!raw) return null;
  raw = String(raw).trim();
  if (!raw || raw.includes('<') || raw.length < 32) return null;
  try {
    if (raw.startsWith('[')) return Keypair.fromSecretKey(Uint8Array.from(JSON.parse(raw)));
    return Keypair.fromSecretKey(bs58.decode(raw));
  } catch (e) {
    log(`[WALLET] Invalid key format: ${e.message}`, 'error');
    return null;
  }
}

async function loadPrivateKeyRaw() {
  // 1. key-manager.js (pathToFileURL fixes Windows ESM c: protocol bug)
  const kmPath = pathResolve(process.cwd(), 'key-manager.js');
  if (existsSync(kmPath)) {
    try {
      const km = await import(pathToFileURL(kmPath).href);
      if (typeof km.loadKeys === 'function') {
        log('[WALLET] Calling key-manager.loadKeys() ...');
        await km.loadKeys();
        const injected = process.env.HARVESTER_PRIVATE_KEY;
        if (injected && injected.trim() && !injected.includes('<')) {
          log('[WALLET] Key loaded via key-manager.loadKeys()');
          return injected.trim();
        }
        log('[WALLET] loadKeys() ran but key not injected — trying WCM fallback');
      }
    } catch (e) {
      log(`[WALLET] key-manager.loadKeys() failed: ${e.message}`);
    }
  }

  // 2. Windows Credential Manager via PS1 temp file
  const ps1Path = pathJoin(tmpdir(), `gwh_wcm_${process.pid}.ps1`);
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
      log('[WALLET] Key loaded from Windows Credential Manager');
      return raw;
    }
  } catch (e) {
    try { unlinkSync(ps1Path); } catch (_) {}
    log(`[WALLET] WCM read failed: ${e.message.split('\n')[0]}`);
  }

  // 3. Env var (injected via pm2 restart --update-env)
  const envKey = process.env.HARVESTER_PRIVATE_KEY;
  if (envKey && envKey.trim() && !envKey.includes('<')) {
    log('[WALLET] Key loaded from HARVESTER_PRIVATE_KEY env var');
    return envKey.trim();
  }
  return null;
}

// Load keypair at startup (top-level await, ESM)
const _rawKey = await loadPrivateKeyRaw();
const vault   = keypairFromRaw(_rawKey);
if (!vault) {
  log('[WALLET] No valid key found — running in DRY-RUN mode (no transactions will be sent)', 'warning');
}

// ── RPC pool ──────────────────────────────────────────────────────────────
const HARV_RPC_ENDPOINTS = [
  process.env.HELIUS_RPC3,
  process.env.HELIUS_RPC2,
  process.env.HELIUS_RPC,
  process.env.RPC_URL,
  'https://api.mainnet-beta.solana.com',
].filter(Boolean);

let _rpcIndex = 0;
const getConn = () => new Connection(
  HARV_RPC_ENDPOINTS[_rpcIndex % HARV_RPC_ENDPOINTS.length],
  { commitment: 'confirmed', confirmTransactionInitialTimeout: 60_000 }
);
let conn = getConn();

const rotateRpc = (reason = '') => {
  conn = getConn();
  _rpcIndex++;
  const next = HARV_RPC_ENDPOINTS[(_rpcIndex - 1) % HARV_RPC_ENDPOINTS.length];
  log(`RPC rotated [${reason}] -> ${next?.slice(0, 40) || 'fallback'}`, 'warning');
};

// ── Sol balance helpers ───────────────────────────────────────────────────
const getSolBal = async (pk = vault?.publicKey) => {
  if (!pk) return 0;
  try { return (await conn.getBalance(pk)) / LAMPORTS_PER_SOL; }
  catch (e) {
    if ((e.message || '').includes('429') || (e.message || '').includes('Too Many')) {
      rotateRpc('getSolBal-429');
      try { return (await conn.getBalance(pk)) / LAMPORTS_PER_SOL; } catch { return 0; }
    }
    return 0;
  }
};

const getTokenBal = async (mint, owner = vault?.publicKey, prog = TOKEN_PROGRAM_ID) => {
  if (!owner) return 0n;
  try {
    const ata = getAssociatedTokenAddressSync(new PublicKey(mint), owner, false, prog);
    const a   = await getAccount(conn, ata, 'confirmed', prog);
    return BigInt(a.amount);
  } catch { return 0n; }
};

const getUSDCBal = async () => {
  if (!vault) return 0;
  try {
    const ata = getAssociatedTokenAddressSync(USDC_MINT_PK, vault.publicKey);
    const acc = await getAccount(conn, ata);
    return Number(acc.amount);
  } catch { return 0; }
};

// ── Hub socket ────────────────────────────────────────────────────────────
function connectHub() {
  _hub = ioClient('https://localhost:3001', {
    transports: ['websocket'], rejectUnauthorized: false,
    reconnection: true, reconnectionDelay: 5000,
  });
  _hub.on('connect',      () => log('Hub connected', 'success'));
  _hub.on('disconnect',   () => log('Hub disconnected', 'warning'));
  _hub.on('connect_error', () => {});
}

// ── State ─────────────────────────────────────────────────────────────────
let positions = {};
try { positions = JSON.parse(fs.readFileSync(POSITIONS_FILE, 'utf8')); } catch {}
const savePositions = () => {
  try { fs.writeFileSync(POSITIONS_FILE, JSON.stringify(positions, null, 2)); } catch {}
};

let totalHarvest   = 0;
let farmingProfit  = 0;
let compoundedBal  = 0;
let baselineUSDC   = 0;

let dailyDeployedSol = 0;
let dailyDeployDate  = new Date().toDateString();
function checkDailyCap(amount) {
  const today = new Date().toDateString();
  if (today !== dailyDeployDate) { dailyDeployedSol = 0; dailyDeployDate = today; }
  if (dailyDeployedSol + amount > CFG.DAILY_DEPLOY_CAP) {
    log(`Daily deploy cap reached: ${f4(dailyDeployedSol)}/${f4(CFG.DAILY_DEPLOY_CAP)} SOL — skipping`, 'warning');
    return false;
  }
  return true;
}
function recordDeploy(amount) {
  dailyDeployedSol += amount;
  log(`Daily deploy: ${f4(dailyDeployedSol)}/${f4(CFG.DAILY_DEPLOY_CAP)} SOL used today`);
}

const TP_LEVELS = CFG.TP_LEVELS.split(',').map(Number).filter(n => n > 0).sort((a, b) => a - b);

// ── Baseline / profit guard ───────────────────────────────────────────────
let baseline    = 0;
let baselineSet = false;

const setBaseline = async () => {
  if (baselineSet) return;
  try {
    const saved = JSON.parse(fs.readFileSync(BASELINE_FILE, 'utf8'));
    if (saved.baseline > 0) {
      baseline      = saved.baseline;
      baselineUSDC  = saved.baselineUSDC || 0;
      farmingProfit = saved.farmingProfit || 0;
      compoundedBal = saved.compoundedBal || 0;
      baselineSet   = true;
      log(`Baseline restored: ${f4(baseline)} SOL (seed capital locked)`, 'system');
      return;
    }
  } catch {}
  baseline      = await getSolBal();
  baselineUSDC  = await getUSDCBal();
  baselineSet   = true;
  farmingProfit = 0;
  log(`Baseline locked: ${f4(baseline)} SOL — NEVER distributed`, 'system');
  try {
    fs.writeFileSync(BASELINE_FILE, JSON.stringify({
      baseline, baselineUSDC, farmingProfit: 0, compoundedBal: 0,
      lockedAt: new Date().toISOString(),
    }));
  } catch {}
};

const getDeployable = async () => {
  const bal       = await getSolBal();
  const vaultFloor = Math.max(baseline, CFG.MIN_VAULT_SOL);
  const floor     = vaultFloor + CFG.GAS_RESERVE + CFG.DLMM_RESERVE + CFG.CPMM_RESERVE + (bal * CFG.LIQUID_RESERVE);
  return Math.max(0, bal - floor);
};

const canSpend = async (amount) => {
  const bal        = await getSolBal();
  const vaultFloor = Math.max(baseline, CFG.MIN_VAULT_SOL);
  const floor      = vaultFloor + CFG.GAS_RESERVE + CFG.DLMM_RESERVE + CFG.CPMM_RESERVE;
  const afterSpend = bal - amount;
  if (afterSpend < floor + afterSpend * CFG.LIQUID_RESERVE) {
    log(`RESERVE GUARD: ${f4(amount)} SOL blocked — vault floor protection`, 'warning');
    return false;
  }
  return true;
};

// ── TX helpers ────────────────────────────────────────────────────────────
async function pollConfirm(txid, _bh, lv, ms = 60000) {
  const t = Date.now();
  while (Date.now() - t < ms) {
    try {
      const h = await conn.getBlockHeight('confirmed');
      if (Number.isFinite(lv) && h > lv) throw new Error('Transaction expired');
    } catch {}
    const stRes = await conn.getSignatureStatuses([txid]).catch(() => ({ value: [null] }));
    const st = stRes?.value?.[0];
    if (st) {
      if (st.err) throw new Error('TX failed: ' + JSON.stringify(st.err));
      if (st.confirmationStatus === 'confirmed' || st.confirmationStatus === 'finalized') return txid;
    }
    await sleep(1200);
  }
  throw new Error('Timeout: ' + txid);
}

async function simulateAndSend(tx, signers = [], label = 'tx') {
  if (!vault) {
    log(`[DRY-RUN] Would send ${label} (no wallet loaded)`, 'warning');
    return null;
  }
  const { blockhash, lastValidBlockHeight } = await conn.getLatestBlockhash('confirmed');
  tx.recentBlockhash = blockhash;
  tx.feePayer        = vault.publicKey;
  if (signers.length > 0) tx.sign(...signers);
  else tx.sign(vault);

  try {
    const sim = await conn.simulateTransaction(tx, { commitment: 'confirmed' });
    if (sim.value.err) {
      const errStr = JSON.stringify(sim.value.err);
      log(`Sim FAILED [${label}]: ${errStr.slice(0, 120)} — TX ABORTED`, 'warning');
      if (sim.value.logs) {
        sim.value.logs.filter(l => l.includes('Error') || l.includes('failed')).slice(0, 3)
          .forEach(l => log(`   ${l}`, 'warning'));
      }
      return null;
    }
    const cuUsed  = sim.value.unitsConsumed || 200_000;
    const cuLimit = Math.min(Math.ceil(cuUsed * 1.2), 600_000);

    const ixs = tx.instructions.filter(ix =>
      !ix.programId.equals(new PublicKey('ComputeBudget111111111111111111111111111111'))
    );
    const optimized = new Transaction();
    optimized.add(ComputeBudgetProgram.setComputeUnitLimit({ units: cuLimit }));
    optimized.add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 100_000 }));
    ixs.forEach(ix => optimized.add(ix));
    optimized.recentBlockhash = blockhash;
    optimized.feePayer        = vault.publicKey;
    if (signers.length > 0) optimized.sign(...signers);
    else optimized.sign(vault);

    const txid = await conn.sendRawTransaction(optimized.serialize(), {
      skipPreflight: true, maxRetries: 3,
    });
    await pollConfirm(txid, blockhash, lastValidBlockHeight);
    log(`TX confirmed [${label}]: ${txid.slice(0, 8)}...`, 'success');
    return txid;
  } catch (e) {
    const msg = e.message || '';
    if (msg.includes('429') || msg.includes('Too Many') || msg.includes('rate limit')) {
      rotateRpc('simulateAndSend-429');
    }
    log(`TX error [${label}]: ${msg.slice(0, 100)}`, 'error');
    return null;
  }
}

// ── Jupiter swap (VersionedTransaction, lite endpoint) ────────────────────
const JUP_BASE = process.env.JUP_BASE || 'https://lite-api.jup.ag/swap/v1';
let jupThrottledUntil = 0;

class RateLimitAbort extends Error {
  constructor(ms) { super('Jupiter rate-limited — aborting'); this.retryAfterMs = ms; }
}

async function jupiterSwap(inputMint, outputMint, amountLamports, slippageBps = 50) {
  if (!vault) { log('[DRY-RUN] Jupiter swap skipped (no wallet)', 'warning'); return null; }
  if (jupThrottledUntil > Date.now()) {
    const rem = jupThrottledUntil - Date.now();
    log(`  [JUP] Rate-limit cooldown ${Math.ceil(rem / 1000)}s — skipping`, 'warning');
    return null;
  }
  let delay = 700;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const quoteRes = await axiosGet(JUP_BASE + '/quote', {
        params: { inputMint, outputMint, amount: String(amountLamports), slippageBps: String(slippageBps) },
        timeout: 12000,
      });
      const quote = quoteRes.data;
      if (!quote?.outAmount) throw new Error('No Jupiter quote');

      const swapRes = await axiosPost(JUP_BASE + '/swap', {
        quoteResponse: quote,
        userPublicKey: vault.publicKey.toBase58(),
        wrapAndUnwrapSol: true,
        dynamicComputeUnitLimit: true,
        prioritizationFeeLamports: 100000,
      }, { timeout: 18000 });

      const swapTransaction = swapRes?.data?.swapTransaction;
      if (!swapTransaction) throw new Error('No swapTransaction in response');

      const vtx = VersionedTransaction.deserialize(Buffer.from(swapTransaction, 'base64'));
      vtx.sign([vault]);
      const txid = await conn.sendRawTransaction(vtx.serialize(), { skipPreflight: true, maxRetries: 3 });
      await pollConfirm(txid, null, Number.MAX_SAFE_INTEGER, 60000);
      log(`Jupiter swap OK ${inputMint.slice(0, 4)}→${outputMint.slice(0, 4)} | ${txid.slice(0, 8)}`, 'success');
      return { txid, outAmount: Number(quote.outAmount) };
    } catch (e) {
      const msg = e?.message || '';
      if (msg.includes('429') || msg.includes('Too Many')) {
        const cooldown = JUP_BASE.includes('lite-api') ? 60000 : 30000;
        jupThrottledUntil = Date.now() + cooldown;
        log(`  [JUP] 429 — cooling down ${Math.ceil(cooldown / 1000)}s`, 'warning');
        return null;
      }
      if (attempt < 3) {
        log(`Jupiter attempt ${attempt} failed: ${msg.slice(0, 80)} — retry ${delay}ms`, 'warning');
        await sleep(delay); delay *= 2;
      } else {
        log(`Jupiter swap failed: ${msg.slice(0, 120)}`, 'error');
      }
    }
  }
  return null;
}

// ── Rug detection ─────────────────────────────────────────────────────────
async function isRugRisk(pool) {
  try {
    const tvl = parseFloat(pool.tvl || 0);
    const vol = parseFloat(pool.vol || pool.volume || 0);
    if (tvl < 3000 && vol > 30000) {
      log(`RUG FILTER: ${pool.name} TVL:$${tvl.toFixed(0)} vs Vol:$${vol.toFixed(0)} — skipping`, 'warning');
      return true;
    }
    if (pool.lpMint) {
      try {
        const mintInfo = await getMint(conn, new PublicKey(pool.lpMint));
        if (Number(mintInfo.supply) < 1000) {
          log(`RUG FILTER: ${pool.name} LP supply ${mintInfo.supply} too low — skipping`, 'warning');
          return true;
        }
      } catch {}
    }
    return false;
  } catch { return true; }
}

// ── CPMM engine (Raydium) ─────────────────────────────────────────────────
async function cpmm_loadKeys(poolId) {
  const info = await conn.getAccountInfo(new PublicKey(poolId), 'confirmed');
  if (!info) throw new Error(`Pool not found: ${poolId}`);
  if (!info.owner.equals(CPMM_PROGRAM)) throw new Error(`Not CPMM`);
  const d = info.data;
  const [auth] = PublicKey.findProgramAddressSync(
    [Buffer.from('vault_and_lp_mint_auth_seed')], CPMM_PROGRAM
  );
  return {
    type: 'cpmm', id: new PublicKey(poolId),
    ammConfig:      readPk(d, CPMM_OFF.ammConfig),
    authority:      auth,
    token0Vault:    readPk(d, CPMM_OFF.token0Vault),
    token1Vault:    readPk(d, CPMM_OFF.token1Vault),
    lpMint:         readPk(d, CPMM_OFF.lpMint),
    token0Mint:     readPk(d, CPMM_OFF.token0Mint),
    token1Mint:     readPk(d, CPMM_OFF.token1Mint),
    token0Program:  readPk(d, CPMM_OFF.token0Program),
    token1Program:  readPk(d, CPMM_OFF.token1Program),
    observationKey: readPk(d, CPMM_OFF.observationKey),
  };
}

const _resCache = new Map();
async function cpmm_getReserves(pk) {
  const k = pk.id.toBase58(), c = _resCache.get(k);
  if (c && Date.now() - c.t < 15000) return { r0: c.r0, r1: c.r1 };
  const [b0, b1] = await Promise.all([
    conn.getTokenAccountBalance(pk.token0Vault),
    conn.getTokenAccountBalance(pk.token1Vault),
  ]);
  const r0 = new BN(b0.value.amount), r1 = new BN(b1.value.amount);
  _resCache.set(k, { r0, r1, t: Date.now() });
  return { r0, r1 };
}

async function cpmm_swap(pk, inputMintStr, amtIn, slip = 0.03) {
  if (amtIn < 5000) return null;
  const isIn0   = inputMintStr === pk.token0Mint.toBase58();
  const outMint = isIn0 ? pk.token1Mint : pk.token0Mint;
  const result  = await jupiterSwap(inputMintStr, outMint.toBase58(), amtIn, Math.round(slip * 10000));
  if (!result) return null;
  return result.txid;
}

async function cpmm_addLiquidity(pk, solAmount) {
  const lamports = Math.floor(solAmount * LAMPORTS_PER_SOL);
  const sol0     = pk.token0Mint.equals(WSOL_MINT);
  const tokMint  = sol0 ? pk.token1Mint : pk.token0Mint;
  const tokProg  = sol0 ? pk.token1Program : pk.token0Program;
  const wsolProg = sol0 ? pk.token0Program : pk.token1Program;
  const { r0, r1 } = await cpmm_getReserves(pk);
  const rSOL = sol0 ? r0 : r1, rTok = sol0 ? r1 : r0;
  const solBN    = new BN(lamports);
  const tokNeeded = solBN.mul(rTok).div(rSOL);
  const tokATA   = getAssociatedTokenAddressSync(tokMint, vault.publicKey, false, tokProg);
  let tokBal = new BN(0);
  try {
    const ta = await getAccount(conn, tokATA, 'confirmed', tokProg);
    tokBal = new BN(ta.amount.toString());
  } catch {}
  if (tokBal.isZero()) { log(`No token balance — skipping addLiquidity`, 'warning'); return null; }
  const maxTok  = BN.min(tokNeeded.muln(103).divn(100), tokBal);
  const wsolATA = getAssociatedTokenAddressSync(WSOL_MINT, vault.publicKey, false, wsolProg);
  const lpATA   = getAssociatedTokenAddressSync(pk.lpMint, vault.publicKey);
  const tx = new Transaction();
  tx.add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 200_000 }));
  tx.add(ComputeBudgetProgram.setComputeUnitLimit({ units: 500_000 }));
  for (const [ata, mint, prog] of [[wsolATA, WSOL_MINT, wsolProg], [tokATA, tokMint, tokProg], [lpATA, pk.lpMint, TOKEN_PROGRAM_ID]]) {
    try { await getAccount(conn, ata, 'confirmed', prog); }
    catch { tx.add(createAssociatedTokenAccountInstruction(vault.publicKey, ata, vault.publicKey, mint, prog)); }
  }
  tx.add(SystemProgram.transfer({ fromPubkey: vault.publicKey, toPubkey: wsolATA, lamports }));
  tx.add({ keys: [{ pubkey: wsolATA, isSigner: false, isWritable: true }, { pubkey: vault.publicKey, isSigner: true, isWritable: false }], programId: wsolProg, data: Buffer.from([17]) });
  const disc = Buffer.from([242, 35, 198, 137, 82, 225, 242, 182]);
  const data  = Buffer.concat([disc,
    sol0 ? solBN.toArrayLike(Buffer, 'le', 8) : maxTok.toArrayLike(Buffer, 'le', 8),
    sol0 ? maxTok.toArrayLike(Buffer, 'le', 8) : solBN.toArrayLike(Buffer, 'le', 8),
    new BN(1).toArrayLike(Buffer, 'le', 8),
  ]);
  tx.add({ programId: CPMM_PROGRAM, data, keys: [
    { pubkey: vault.publicKey, isSigner: true,  isWritable: true  },
    { pubkey: pk.authority,    isSigner: false, isWritable: false },
    { pubkey: pk.ammConfig,    isSigner: false, isWritable: false },
    { pubkey: pk.id,           isSigner: false, isWritable: true  },
    { pubkey: sol0 ? wsolATA : tokATA, isSigner: false, isWritable: true },
    { pubkey: sol0 ? tokATA : wsolATA, isSigner: false, isWritable: true },
    { pubkey: pk.token0Vault,  isSigner: false, isWritable: true  },
    { pubkey: pk.token1Vault,  isSigner: false, isWritable: true  },
    { pubkey: pk.lpMint,       isSigner: false, isWritable: true  },
    { pubkey: lpATA,           isSigner: false, isWritable: true  },
    { pubkey: sol0 ? wsolProg : tokProg,  isSigner: false, isWritable: false },
    { pubkey: sol0 ? tokProg  : wsolProg, isSigner: false, isWritable: false },
    { pubkey: TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
    { pubkey: pk.observationKey, isSigner: false, isWritable: true },
  ]});
  const txid = await simulateAndSend(tx, [vault], 'cpmm_addLiq');
  if (txid) log(`CPMM addLiquidity ✅ ${txid.slice(0, 8)}`, 'success');
  return txid;
}

async function cpmm_removeLiquidity(pk, lpAmount) {
  if (lpAmount <= 0n) return null;
  const sol0    = pk.token0Mint.equals(WSOL_MINT);
  const tokMint = sol0 ? pk.token1Mint : pk.token0Mint;
  const tokProg = sol0 ? pk.token1Program : pk.token0Program;
  const wsolProg= sol0 ? pk.token0Program : pk.token1Program;
  const wsolATA = getAssociatedTokenAddressSync(WSOL_MINT, vault.publicKey, false, wsolProg);
  const tokATA  = getAssociatedTokenAddressSync(tokMint,   vault.publicKey, false, tokProg);
  const lpATA   = getAssociatedTokenAddressSync(pk.lpMint, vault.publicKey);
  const disc    = Buffer.from([80, 85, 209, 72, 24, 206, 177, 116]);
  const lpBN    = new BN(lpAmount.toString());
  const data    = Buffer.concat([disc, lpBN.toArrayLike(Buffer, 'le', 8),
    new BN(0).toArrayLike(Buffer, 'le', 8), new BN(0).toArrayLike(Buffer, 'le', 8)]);
  const tx = new Transaction();
  tx.add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 200_000 }));
  tx.add(ComputeBudgetProgram.setComputeUnitLimit({ units: 400_000 }));
  tx.add({ programId: CPMM_PROGRAM, data, keys: [
    { pubkey: vault.publicKey, isSigner: true,  isWritable: true  },
    { pubkey: pk.authority,    isSigner: false, isWritable: false },
    { pubkey: pk.ammConfig,    isSigner: false, isWritable: false },
    { pubkey: pk.id,           isSigner: false, isWritable: true  },
    { pubkey: lpATA,           isSigner: false, isWritable: true  },
    { pubkey: sol0 ? wsolATA : tokATA, isSigner: false, isWritable: true },
    { pubkey: sol0 ? tokATA : wsolATA, isSigner: false, isWritable: true },
    { pubkey: pk.token0Vault,  isSigner: false, isWritable: true  },
    { pubkey: pk.token1Vault,  isSigner: false, isWritable: true  },
    { pubkey: pk.lpMint,       isSigner: false, isWritable: true  },
    { pubkey: sol0 ? wsolProg : tokProg,  isSigner: false, isWritable: false },
    { pubkey: sol0 ? tokProg  : wsolProg, isSigner: false, isWritable: false },
    { pubkey: TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
    { pubkey: pk.observationKey, isSigner: false, isWritable: true },
  ]});
  tx.add({ keys: [
    { pubkey: wsolATA, isSigner: false, isWritable: true },
    { pubkey: vault.publicKey, isSigner: false, isWritable: true },
    { pubkey: vault.publicKey, isSigner: true,  isWritable: false },
  ], programId: wsolProg, data: Buffer.from([9]) });
  const txid = await simulateAndSend(tx, [vault], 'cpmm_removeLiq');
  if (txid) log(`CPMM removeLiquidity ✅ ${txid.slice(0, 8)}`, 'success');
  return txid;
}

async function cpmm_sellToken(pk, label) {
  const sol0    = pk.token0Mint.equals(WSOL_MINT);
  const tokMint = (sol0 ? pk.token1Mint : pk.token0Mint).toBase58();
  const tokProg = sol0 ? pk.token1Program : pk.token0Program;
  const tokBal  = await getTokenBal(tokMint, vault.publicKey, tokProg);
  if (tokBal < 100n) return;
  log(`CPMM sell ${label} → SOL...`, 'lp');
  try {
    await jupiterSwap(tokMint, SOL_MINT_STR, Number(tokBal), Math.round(CFG.SLIPPAGE * 10000));
  } catch (e) { log(`${label} sell failed: ${e.message?.slice(0, 60)}`, 'error'); }
}

// ── DLMM engine (Meteora) ─────────────────────────────────────────────────
const METEORA_API = process.env.METEORA_DLMM_BASE || 'https://dlmm.datapi.meteora.ag';

function tw24(v) {
  if (v == null) return 0;
  if (typeof v === 'number') return Number(v) || 0;
  if (typeof v === 'object') return Number(v['24h'] ?? v.h24 ?? v.day ?? 0) || 0;
  return Number(v) || 0;
}

function normDlmmPool(p) {
  const mint_x = p?.mint_x || p?.token_x?.address || p?.tokenXMint || '';
  const mint_y = p?.mint_y || p?.token_y?.address || p?.tokenYMint || '';
  const tvl    = Number(p?.tvl ?? 0) || 0;
  const volume24h   = Number(p?.volume24h ?? p?.volume_24h ?? tw24(p?.volume) ?? 0) || 0;
  const fees24h     = Number(p?.fees24h   ?? p?.fees_24h   ?? tw24(p?.fees)   ?? 0) || 0;
  const fee_tvl24h  = Number(p?.fee_tvl_ratio24h ?? p?.fee_tvl_ratio_24h ?? tw24(p?.fee_tvl_ratio) ?? 0)
    || (tvl > 0 ? fees24h / tvl : 0);
  const name = p?.name || `${p?.token_x?.symbol || mint_x.slice(0, 4)}/${p?.token_y?.symbol || mint_y.slice(0, 4)}`;
  const score = (fee_tvl24h * 100000) + Math.log10(volume24h + 1) * 20 + Math.log10(tvl + 1) * 10;
  return {
    ...p, address: p?.address || p?.poolAddress || p?.id || '',
    mint_x, mint_y, tvl, volume24h, fees24h, fee_tvl_ratio24h: fee_tvl24h,
    apr: Number(p?.apr ?? fee_tvl24h ?? 0) || 0,
    score, name, is_blacklisted: Boolean(p?.is_blacklisted),
  };
}

async function dlmm_fetchPools(filterSol = true, pageSize = 200) {
  try {
    const res = await axiosGet(`${METEORA_API}/pools`, {
      params: { page: 1, page_size: pageSize, sort_by: 'fee_tvl_ratio_24h:desc', filter_by: 'is_blacklisted=false' },
      timeout: 15000,
    });
    const rows = Array.isArray(res?.data?.data) ? res.data.data : (Array.isArray(res?.data) ? res.data : []);
    const normalized = rows.map(normDlmmPool).filter(p => p.address && p.mint_x && p.mint_y);
    if (!filterSol) return normalized;
    return normalized.filter(p => !p.is_blacklisted && (p.mint_x === SOL_MINT_STR || p.mint_y === SOL_MINT_STR));
  } catch (e) {
    log(`Meteora DLMM scan failed: ${e?.response?.status || e?.message}`, 'warning');
    return [];
  }
}

async function dlmm_loadPool(poolAddress) {
  try {
    const res = await axiosGet(`${METEORA_API}/pools/${poolAddress}`, { timeout: 8000 });
    return normDlmmPool(res.data || {});
  } catch { return null; }
}

async function dlmm_getActiveBin(poolAddress) {
  try {
    const p = await dlmm_loadPool(poolAddress);
    if (p && Number.isFinite(p.active_id ?? p.active_bin_id)) {
      return { bin_id: p.active_id ?? p.active_bin_id, price: p.current_price || 0 };
    }
  } catch {}
  try {
    const acctInfo = await conn.getAccountInfo(new PublicKey(poolAddress));
    if (acctInfo && acctInfo.data.length > 76) {
      return { bin_id: acctInfo.data.readInt32LE(72), price: 0 };
    }
  } catch {}
  return null;
}

// ── LP Strategies ─────────────────────────────────────────────────────────
const STRATEGIES = {
  QUANTUM_CURVE: { name: 'Quantum Curve', bins: 69, risk: 'LOW',    tpPct: 0.25, rebalThreshold: 0.90 },
  SWING_FLIP:    { name: 'Swing Flip',    bins: 69, risk: 'MEDIUM', tpPct: 0.10, rebalThreshold: 0.70 },
  HFL_REGULAR:   { name: 'HFL Regular',   bins: 30, risk: 'MEDIUM', tpPct: 0.05, rebalThreshold: 0.60 },
  HFL_TIGHT:     { name: 'HFL Tight',     bins: 10, risk: 'HIGH',   tpPct: 0.03, rebalThreshold: 0.50 },
};

function pickStrategy(pool) {
  const volTvl = pool.tvl > 0 ? (pool.vol || pool.volume24h || 0) / pool.tvl : 0;
  const feePct  = pool.feeTvl || pool.fee_tvl_ratio24h || 0;
  if (pool.isCopyEntry) return STRATEGIES.HFL_TIGHT;
  if (volTvl >= 10)     return STRATEGIES.HFL_TIGHT;
  if (feePct >= 0.05 && volTvl >= 2) return STRATEGIES.HFL_REGULAR;
  if (volTvl >= 1 && feePct >= 0.01) return STRATEGIES.SWING_FLIP;
  return STRATEGIES.QUANTUM_CURVE;
}

async function getDynamicBinRange(pool) {
  try {
    let priceChangePct = 0;
    try {
      const res = await axiosGet(
        `https://api.dexscreener.com/latest/dex/pairs/solana/${pool.address}`,
        { timeout: 5000 }
      );
      const pair = res.data?.pairs?.[0];
      if (pair?.priceChange?.h24 !== undefined)
        priceChangePct = Math.abs(parseFloat(pair.priceChange.h24) / 100);
    } catch {}
    const volTvlRatio = pool.tvl > 0 ? (pool.vol || pool.volume24h || 0) / pool.tvl : 0;
    const volScore    = Math.min(
      (priceChangePct / CFG.DLMM_VOL_HIGH) * 0.7 + (volTvlRatio / 2.0) * 0.3, 1.0
    );
    const snapped = volScore < 0.2 ? 8 : volScore < 0.4 ? 12 : volScore < 0.6 ? 20 : volScore < 0.8 ? 35 : 50;
    return snapped;
  } catch { return CFG.DLMM_BIN_RANGE; }
}

async function dlmm_addLiquidity(pool, solAmountLamports) {
  try {
    const binRange    = await getDynamicBinRange(pool);
    const strategy    = pickStrategy(pool);
    log(`DLMM [${strategy.name}] ${pool.name} | ${f4(solAmountLamports / LAMPORTS_PER_SOL)} SOL | ${binRange} bins`, 'dlmm');

    const activeBin = await dlmm_getActiveBin(pool.address);
    if (!activeBin) throw new Error('Cannot get active bin');
    const activeBinId = activeBin.bin_id;
    const lowerBinId  = activeBinId - binRange;
    const upperBinId  = activeBinId + binRange;

    const mintX = pool.mint_x ?? pool.raw?.mint_x;
    const mintY = pool.mint_y ?? pool.raw?.mint_y;
    if (!mintX || !mintY) throw new Error('DLMM pool missing mint_x/mint_y');
    const tokMint = mintX === SOL_MINT_STR ? mintY : mintX;
    const solIsX  = mintX === SOL_MINT_STR;

    const halfLam = Math.floor(solAmountLamports * 0.5);
    const swapRes = await jupiterSwap(SOL_MINT_STR, tokMint, halfLam);
    if (!swapRes) throw new Error('Pre-swap failed');
    await sleep(3000);

    const tokBal = await getTokenBal(tokMint);
    if (tokBal < 100n) throw new Error('No token balance after swap');

    const { createRequire } = await import('module');
    const _req    = createRequire(import.meta.url);
    const _dlmmMod = _req('@meteora-ag/dlmm');
    const DLMM    = _dlmmMod.default ?? _dlmmMod.DLMM ?? _dlmmMod;
    const dlmm    = await DLMM.create(conn, new PublicKey(pool.address));
    const activeBinSdk = await dlmm.getActiveBin();
    const realActiveBinId = activeBinSdk?.binId ?? activeBinId;

    const xAmt = new BN(solIsX ? halfLam : Number(tokBal));
    const yAmt = new BN(solIsX ? Number(tokBal) : halfLam);
    const newPosition = Keypair.generate();

    const stratTypeMap = { 'Quantum Curve': 1, 'Swing Flip': 2, 'HFL Regular': 0, 'HFL Tight': 0 };
    const strategyType = stratTypeMap[strategy.name] ?? 0;

    const { transactions } = await dlmm.addLiquidityByStrategy({
      positionPubKey: newPosition.publicKey,
      user:           vault.publicKey,
      totalXAmount:   xAmt,
      totalYAmount:   yAmt,
      strategy:       { maxBinId: upperBinId, minBinId: lowerBinId, strategyType },
    });

    let txid;
    for (const tx of transactions) {
      const bh = await conn.getLatestBlockhash();
      tx.recentBlockhash = bh.blockhash;
      tx.feePayer = vault.publicKey;
      txid = await simulateAndSend(tx, [vault, newPosition], `dlmm_${strategy.name}`);
      if (txid) await sleep(2000);
    }
    if (!txid) throw new Error('All LP transactions failed');
    log(`DLMM [${strategy.name}] ✅ bins[${lowerBinId}..${upperBinId}] | ${txid.slice(0, 8)}`, 'success');
    return {
      txid, activeBinId: realActiveBinId, lowerBinId, upperBinId,
      positionAddress: newPosition.publicKey.toBase58(),
      strategy: strategy.name, bins: binRange,
    };
  } catch (e) {
    log(`DLMM addLiquidity failed: ${e.message?.slice(0, 80)}`, 'error');
    return null;
  }
}

async function dlmm_claimFees(pool, positionAddress) {
  try {
    const res = await axiosPost(`${METEORA_API}/transaction/claim_fee`, {
      lb_pair: pool.address, user: vault.publicKey.toBase58(), position: positionAddress,
    }, { timeout: 10000 });
    if (!res.data?.transaction) return null;
    const tx   = Transaction.from(Buffer.from(res.data.transaction, 'base64'));
    return simulateAndSend(tx, [vault], 'dlmm_claimFees');
  } catch (e) { log(`DLMM claimFees failed: ${e.message?.slice(0, 60)}`, 'error'); return null; }
}

async function dlmm_removeLiquidity(pool, positionAddress) {
  try {
    const res = await axiosPost(`${METEORA_API}/transaction/remove_liquidity`, {
      lb_pair: pool.address, user: vault.publicKey.toBase58(),
      position: positionAddress, bps_to_remove: 10000, should_claim_and_close: true,
    }, { timeout: 10000 });
    if (!res.data?.transaction) throw new Error('No TX from API');
    const tx = Transaction.from(Buffer.from(res.data.transaction, 'base64'));
    return simulateAndSend(tx, [vault], 'dlmm_removeLiq');
  } catch (e) { log(`DLMM removeLiquidity failed: ${e.message?.slice(0, 60)}`, 'error'); return null; }
}

async function dlmm_checkInRange(pos) {
  try {
    const activeBin = await dlmm_getActiveBin(pos.address);
    if (!activeBin) return 1.0;
    const cur = activeBin.bin_id;
    if (cur >= pos.lowerBinId && cur <= pos.upperBinId) return 1.0;
    const totalRange = pos.upperBinId - pos.lowerBinId;
    const outBy = cur < pos.lowerBinId ? pos.lowerBinId - cur : cur - pos.upperBinId;
    return Math.max(0, 1 - outBy / (totalRange * 0.5));
  } catch { return 1.0; }
}

// ── CLMM engine (Orca Whirlpool) ──────────────────────────────────────────
const ORCA_POSITION_PROGRAM = new PublicKey('whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc');
const METADATA_PROGRAM      = new PublicKey('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s');
const Q64                   = BigInt('18446744073709551616');

const WP_OFF = {
  tokenMintA: 101, tokenMintB: 133, tokenVaultA: 165, tokenVaultB: 197,
  tickSpacing: 229, sqrtPrice: 269, currentTickIndex: 285, feeRate: 289,
};

const STABLE_WHIRLPOOLS = [
  { address: 'HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ', name: 'USDC/USDT',    type: 'clmm', stable: true,  tickSpacing: 1  },
  { address: '7qbRF6YsyGuLUVs6Y1q64bdVrfe4ZcUUz1JRdoVNUJnm', name: 'SOL/jitoSOL', type: 'clmm', stable: false, tickSpacing: 8  },
  { address: 'BqnpCdDLPV2pFdAaLnVidmn3G93RP2p5oRdGEY2sJGez', name: 'USDC/SOL',    type: 'clmm', stable: false, tickSpacing: 64 },
  { address: '83v8iPyZihDEjDdY8RdZddyZNyUtXngz69Lgo9Kt5d6d', name: 'USDT/SOL',    type: 'clmm', stable: false, tickSpacing: 64 },
];

function tickArrayPDA(whirlpool, startTickIndex) {
  const startBuf = Buffer.alloc(4);
  startBuf.writeInt32LE(startTickIndex);
  const [pda] = PublicKey.findProgramAddressSync(
    [Buffer.from('tick_array'), whirlpool.toBuffer(), startBuf],
    ORCA_POSITION_PROGRAM
  );
  return pda;
}

function positionPDA(positionMint) {
  const [pda] = PublicKey.findProgramAddressSync(
    [Buffer.from('position'), positionMint.toBuffer()],
    ORCA_POSITION_PROGRAM
  );
  return pda;
}

function getTickArrayStart(tick, tickSpacing) {
  const ticksPerArray = tickSpacing * 88;
  return Math.floor(tick / ticksPerArray) * ticksPerArray;
}

async function clmm_loadPool(address) {
  const info = await conn.getAccountInfo(new PublicKey(address), 'confirmed');
  if (!info) throw new Error(`Whirlpool not found: ${address}`);
  const d = info.data;
  return {
    address:         new PublicKey(address),
    tickSpacing:     d.readUInt16LE(WP_OFF.tickSpacing),
    sqrtPriceX64:    d.readBigUInt64LE(WP_OFF.sqrtPrice),
    currentTick:     d.readInt32LE(WP_OFF.currentTickIndex),
    feeRate:         d.readUInt16LE(WP_OFF.feeRate),
    tokenMintA:      new PublicKey(d.slice(WP_OFF.tokenMintA,  WP_OFF.tokenMintA  + 32)),
    tokenMintB:      new PublicKey(d.slice(WP_OFF.tokenMintB,  WP_OFF.tokenMintB  + 32)),
    tokenVaultA:     new PublicKey(d.slice(WP_OFF.tokenVaultA, WP_OFF.tokenVaultA + 32)),
    tokenVaultB:     new PublicKey(d.slice(WP_OFF.tokenVaultB, WP_OFF.tokenVaultB + 32)),
  };
}

async function clmm_enterPosition(pool, solAmountSol) {
  log(`CLMM enter: ${pool.name} | ${f4(solAmountSol)} SOL`, 'lp');
  try {
    const preCheck = await conn.getAccountInfo(new PublicKey(pool.address), 'confirmed');
    if (!preCheck) { log(`CLMM ${pool.name}: Whirlpool not found — skipping`, 'warning'); return false; }
  } catch (e) { log(`CLMM ${pool.name}: pre-validation failed — skipping`, 'warning'); return false; }
  try {
    const wp = await clmm_loadPool(pool.address);
    const isSOLA = wp.tokenMintA.equals(WSOL_MINT);
    const isSOLB = wp.tokenMintB.equals(WSOL_MINT);
    if (!isSOLA && !isSOLB) { log(`CLMM ${pool.name}: no SOL side — skip`, 'warning'); return false; }

    const otherMint  = isSOLA ? wp.tokenMintB : wp.tokenMintA;
    const widthTicks = (pool.stable ? 120 : 220) * wp.tickSpacing;
    const lowerTick  = Math.floor((wp.currentTick - widthTicks) / wp.tickSpacing) * wp.tickSpacing;
    const upperTick  = Math.floor((wp.currentTick + widthTicks) / wp.tickSpacing) * wp.tickSpacing;

    const buyLam = Math.floor(solAmountSol * 0.495 * LAMPORTS_PER_SOL);
    const swapResult = await jupiterSwap(WSOL_MINT.toBase58(), otherMint.toBase58(), buyLam, 30);
    if (!swapResult) { log(`CLMM ${pool.name}: swap failed — abort`, 'error'); return false; }
    await sleep(3000);

    const positionMintKP = Keypair.generate();
    const positionMint   = positionMintKP.publicKey;
    const positionPubkey = positionPDA(positionMint);
    const positionATA    = getAssociatedTokenAddressSync(positionMint, vault.publicKey);
    const wsolATA        = getAssociatedTokenAddressSync(WSOL_MINT,    vault.publicKey);
    const otherATA       = getAssociatedTokenAddressSync(otherMint,    vault.publicKey);

    const currentStart = getTickArrayStart(wp.currentTick, wp.tickSpacing);
    const lowerStart   = getTickArrayStart(lowerTick,       wp.tickSpacing);
    const upperStart   = getTickArrayStart(upperTick,       wp.tickSpacing);

    const [metadataPDA] = PublicKey.findProgramAddressSync([
      Buffer.from('metadata'), METADATA_PROGRAM.toBuffer(), positionMint.toBuffer(),
    ], METADATA_PROGRAM);

    const openDiscriminator = Buffer.from([77, 184, 74, 214, 112, 86, 241, 199]);
    const tickLowerBuf = Buffer.alloc(4); tickLowerBuf.writeInt32LE(lowerTick);
    const tickUpperBuf = Buffer.alloc(4); tickUpperBuf.writeInt32LE(upperTick);
    const openData = Buffer.concat([openDiscriminator, tickLowerBuf, tickUpperBuf]);

    const tx = new Transaction();
    tx.add(ComputeBudgetProgram.setComputeUnitLimit({ units: 400_000 }));
    tx.add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 100_000 }));
    tx.add(createAssociatedTokenAccountInstruction(vault.publicKey, wsolATA, vault.publicKey, WSOL_MINT));

    const wrapLam = Math.floor(solAmountSol * 0.505 * LAMPORTS_PER_SOL);
    tx.add(SystemProgram.transfer({ fromPubkey: vault.publicKey, toPubkey: wsolATA, lamports: wrapLam }));
    tx.add(createSyncNativeInstruction(wsolATA));

    tx.add(new TransactionInstruction({
      programId: ORCA_POSITION_PROGRAM, data: openData,
      keys: [
        { pubkey: vault.publicKey, isSigner: true,  isWritable: true  },
        { pubkey: vault.publicKey, isSigner: true,  isWritable: true  },
        { pubkey: positionMint,    isSigner: true,  isWritable: true  },
        { pubkey: positionPubkey,  isSigner: false, isWritable: true  },
        { pubkey: positionATA,     isSigner: false, isWritable: true  },
        { pubkey: metadataPDA,     isSigner: false, isWritable: true  },
        { pubkey: wp.address,      isSigner: false, isWritable: true  },
        { pubkey: TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
        { pubkey: METADATA_PROGRAM, isSigner: false, isWritable: false },
        { pubkey: SystemProgram.programId, isSigner: false, isWritable: false },
        { pubkey: new PublicKey('SysvarRent111111111111111111111111111111111'), isSigner: false, isWritable: false },
      ],
    }));

    const otherBal  = await getTokenBal(otherMint.toBase58());
    const wsolBal   = BigInt(wrapLam);
    const tokenAMax = isSOLA ? wsolBal : otherBal;
    const tokenBMax = isSOLA ? otherBal : wsolBal;
    const liquidityEst = BigInt(Math.floor(Math.min(Number(tokenAMax), Number(tokenBMax)) * 10));

    const incrDiscriminator = Buffer.from([46, 156, 243, 118, 13, 205, 251, 178]);
    const liqBuf  = Buffer.alloc(16);
    liqBuf.writeBigUInt64LE(liquidityEst & BigInt('0xFFFFFFFFFFFFFFFF'), 0);
    liqBuf.writeBigUInt64LE(liquidityEst >> BigInt(64), 8);
    const maxABuf = Buffer.alloc(8); maxABuf.writeBigUInt64LE(tokenAMax, 0);
    const maxBBuf = Buffer.alloc(8); maxBBuf.writeBigUInt64LE(tokenBMax, 0);
    const incrData = Buffer.concat([incrDiscriminator, liqBuf, maxABuf, maxBBuf]);

    tx.add(new TransactionInstruction({
      programId: ORCA_POSITION_PROGRAM, data: incrData,
      keys: [
        { pubkey: vault.publicKey,   isSigner: true,  isWritable: true  },
        { pubkey: wp.address,        isSigner: false, isWritable: true  },
        { pubkey: positionPubkey,    isSigner: false, isWritable: true  },
        { pubkey: positionATA,       isSigner: false, isWritable: false },
        { pubkey: isSOLA ? wsolATA  : otherATA, isSigner: false, isWritable: true },
        { pubkey: isSOLA ? otherATA : wsolATA,  isSigner: false, isWritable: true },
        { pubkey: wp.tokenVaultA,    isSigner: false, isWritable: true  },
        { pubkey: wp.tokenVaultB,    isSigner: false, isWritable: true  },
        { pubkey: tickArrayPDA(wp.address, lowerStart),   isSigner: false, isWritable: true },
        { pubkey: tickArrayPDA(wp.address, upperStart),   isSigner: false, isWritable: true },
        { pubkey: tickArrayPDA(wp.address, currentStart), isSigner: false, isWritable: true },
        { pubkey: TOKEN_PROGRAM_ID,  isSigner: false, isWritable: false },
      ],
    }));

    tx.add(createCloseAccountInstruction(wsolATA, vault.publicKey, vault.publicKey));

    const txid = await simulateAndSend(tx, [vault, positionMintKP], `clmm_open_${pool.name}`);
    if (!txid) { log(`CLMM ${pool.name}: open position failed sim — 0 gas wasted`, 'warning'); return false; }
    log(`CLMM position open ✅ ${pool.name} | NFT:${positionMint.toBase58().slice(0, 8)} | ${txid.slice(0, 8)}`, 'success');
    return { txid, positionMint: positionMint.toBase58(), positionAddress: positionPubkey.toBase58(), lowerTick, upperTick, currentTick: wp.currentTick };
  } catch (e) {
    log(`CLMM enter failed [${pool.name}]: ${e.message?.slice(0, 100)}`, 'error');
    return false;
  }
}

async function clmm_collectFees(pos) {
  try {
    const wp         = await clmm_loadPool(pos.address);
    const posMint    = new PublicKey(pos.positionMint);
    const posAddr    = positionPDA(posMint);
    const posATA     = getAssociatedTokenAddressSync(posMint, vault.publicKey);
    const isSOLA     = wp.tokenMintA.equals(WSOL_MINT);
    const otherMint  = isSOLA ? wp.tokenMintB : wp.tokenMintA;
    const wsolATA    = getAssociatedTokenAddressSync(WSOL_MINT,  vault.publicKey);
    const otherATA   = getAssociatedTokenAddressSync(otherMint,  vault.publicKey);

    const currentStart = getTickArrayStart(wp.currentTick, wp.tickSpacing);
    const lowerStart   = getTickArrayStart(pos.lowerTick || wp.currentTick - wp.tickSpacing * 20, wp.tickSpacing);
    const upperStart   = getTickArrayStart(pos.upperTick || wp.currentTick + wp.tickSpacing * 20, wp.tickSpacing);

    const disc = Buffer.from([164, 152, 207, 99, 214, 49, 24, 1]);
    const tx   = new Transaction();
    tx.add(ComputeBudgetProgram.setComputeUnitLimit({ units: 200_000 }));
    tx.add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 80_000 }));
    tx.add(new TransactionInstruction({
      programId: ORCA_POSITION_PROGRAM, data: disc,
      keys: [
        { pubkey: vault.publicKey,   isSigner: true,  isWritable: false },
        { pubkey: posAddr,           isSigner: false, isWritable: true  },
        { pubkey: posATA,            isSigner: false, isWritable: false },
        { pubkey: wp.address,        isSigner: false, isWritable: true  },
        { pubkey: isSOLA ? wsolATA  : otherATA, isSigner: false, isWritable: true },
        { pubkey: isSOLA ? otherATA : wsolATA,  isSigner: false, isWritable: true },
        { pubkey: wp.tokenVaultA,    isSigner: false, isWritable: true  },
        { pubkey: wp.tokenVaultB,    isSigner: false, isWritable: true  },
        { pubkey: tickArrayPDA(wp.address, lowerStart),   isSigner: false, isWritable: true },
        { pubkey: tickArrayPDA(wp.address, upperStart),   isSigner: false, isWritable: true },
        { pubkey: tickArrayPDA(wp.address, currentStart), isSigner: false, isWritable: true },
        { pubkey: TOKEN_PROGRAM_ID,  isSigner: false, isWritable: false },
      ],
    }));
    tx.add(createCloseAccountInstruction(wsolATA, vault.publicKey, vault.publicKey));
    const txid = await simulateAndSend(tx, [vault], `clmm_fees_${pos.name}`);
    if (txid) log(`CLMM fees collected ✅ ${pos.name} | ${txid.slice(0, 8)}`, 'success');
    return txid;
  } catch (e) { log(`CLMM collectFees failed [${pos.name}]: ${e.message?.slice(0, 80)}`, 'error'); return null; }
}

async function clmm_exitPosition(pos) {
  try {
    const wp        = await clmm_loadPool(pos.address);
    const posMint   = new PublicKey(pos.positionMint);
    const posAddr   = positionPDA(posMint);
    const posATA    = getAssociatedTokenAddressSync(posMint, vault.publicKey);
    const isSOLA    = wp.tokenMintA.equals(WSOL_MINT);
    const otherMint = isSOLA ? wp.tokenMintB : wp.tokenMintA;
    const wsolATA   = getAssociatedTokenAddressSync(WSOL_MINT,  vault.publicKey);
    const otherATA  = getAssociatedTokenAddressSync(otherMint,  vault.publicKey);

    const currentStart = getTickArrayStart(wp.currentTick, wp.tickSpacing);
    const lowerStart   = getTickArrayStart(pos.lowerTick || wp.currentTick - wp.tickSpacing * 20, wp.tickSpacing);
    const upperStart   = getTickArrayStart(pos.upperTick || wp.currentTick + wp.tickSpacing * 20, wp.tickSpacing);

    const decrDisc = Buffer.from([160, 38, 208, 111, 104, 91, 202, 8]);
    const liqBuf   = Buffer.alloc(16, 0);
    const decrData = Buffer.concat([decrDisc, liqBuf, Buffer.alloc(8, 0), Buffer.alloc(8, 0)]);
    const closeDisc = Buffer.from([123, 134, 81, 0, 49, 68, 98, 98]);

    const tx = new Transaction();
    tx.add(ComputeBudgetProgram.setComputeUnitLimit({ units: 350_000 }));
    tx.add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 100_000 }));
    tx.add(createAssociatedTokenAccountInstruction(vault.publicKey, wsolATA, vault.publicKey, WSOL_MINT));

    tx.add(new TransactionInstruction({
      programId: ORCA_POSITION_PROGRAM, data: decrData,
      keys: [
        { pubkey: vault.publicKey,   isSigner: true,  isWritable: false },
        { pubkey: posAddr,           isSigner: false, isWritable: true  },
        { pubkey: posATA,            isSigner: false, isWritable: false },
        { pubkey: wp.address,        isSigner: false, isWritable: true  },
        { pubkey: isSOLA ? wsolATA : otherATA, isSigner: false, isWritable: true },
        { pubkey: isSOLA ? otherATA : wsolATA, isSigner: false, isWritable: true },
        { pubkey: wp.tokenVaultA,    isSigner: false, isWritable: true  },
        { pubkey: wp.tokenVaultB,    isSigner: false, isWritable: true  },
        { pubkey: tickArrayPDA(wp.address, lowerStart),   isSigner: false, isWritable: true },
        { pubkey: tickArrayPDA(wp.address, upperStart),   isSigner: false, isWritable: true },
        { pubkey: tickArrayPDA(wp.address, currentStart), isSigner: false, isWritable: true },
        { pubkey: TOKEN_PROGRAM_ID,  isSigner: false, isWritable: false },
      ],
    }));

    tx.add(new TransactionInstruction({
      programId: ORCA_POSITION_PROGRAM, data: closeDisc,
      keys: [
        { pubkey: vault.publicKey,  isSigner: true,  isWritable: true  },
        { pubkey: posAddr,          isSigner: false, isWritable: true  },
        { pubkey: posATA,           isSigner: false, isWritable: true  },
        { pubkey: posMint,          isSigner: false, isWritable: true  },
        { pubkey: vault.publicKey,  isSigner: true,  isWritable: false },
        { pubkey: TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
      ],
    }));

    const otherBal = await getTokenBal(otherMint.toBase58());
    if (otherBal > 1000n) {
      await sleep(2000);
      await jupiterSwap(otherMint.toBase58(), SOL_MINT_STR, Number(otherBal), 50);
    }

    tx.add(createCloseAccountInstruction(wsolATA, vault.publicKey, vault.publicKey));
    const txid = await simulateAndSend(tx, [vault], `clmm_exit_${pos.name}`);
    if (txid) log(`CLMM exit ✅ ${pos.name} | ${txid.slice(0, 8)}`, 'success');
    return txid;
  } catch (e) { log(`CLMM exit failed [${pos.name}]: ${e.message?.slice(0, 80)}`, 'error'); return null; }
}

async function clmm_isInRange(pos) {
  try {
    const wp      = await clmm_loadPool(pos.address);
    const inRange = wp.currentTick >= pos.lowerTick && wp.currentTick <= pos.upperTick;
    return { inRange, pctInRange: inRange ? 1.0 : 0.0, currentTick: wp.currentTick };
  } catch { return { inRange: true, pctInRange: 1.0 }; }
}

// ── Copy-LP engine ────────────────────────────────────────────────────────
const copyLpQueue       = new Map();
const copyLpSeen        = new Set();
const METEORA_PROGRAM_STR = 'LBUZKhRxPF3XUpBCjp4YzTKgLLjgLmEFXFHBGe6mJJu';
let autoTrackedWallets  = [];
let lastWalletScan      = 0;
const WALLET_SCAN_MS    = 6 * 3600 * 1000;

async function discoverTopLpers() {
  try {
    log('Copy-LP: Auto-discovering top Meteora LPers...');
    await axiosGet(`${METEORA_API}/pools?page=1&page_size=10`, { timeout: 12000 });
    return [];
  } catch (e) {
    log(`Copy-LP Meteora scan failed: ${e?.response?.status || e?.message}`, 'warning');
    return [];
  }
}

async function scanCopyLpWallets() {
  if (Date.now() - lastWalletScan > WALLET_SCAN_MS || autoTrackedWallets.length === 0) {
    await discoverTopLpers();
    lastWalletScan = Date.now();
  }
  if (!autoTrackedWallets.length) return [];

  const newEntries = [];
  for (const w of autoTrackedWallets) {
    try {
      const pk   = new PublicKey(w.address);
      const sigs = await conn.getSignaturesForAddress(pk, { limit: 5 }, 'confirmed');
      for (const sig of sigs.filter(s => !s.err)) {
        try {
          const tx = await conn.getParsedTransaction(sig.signature, {
            commitment: 'confirmed', maxSupportedTransactionVersion: 0,
          });
          if (!tx) continue;
          const hasMeteora = (tx.transaction?.message?.instructions || []).some(ix =>
            (ix.programId?.toBase58?.() || String(ix.programId)) === METEORA_PROGRAM_STR
          );
          if (!hasMeteora) continue;
          const accts    = tx.transaction?.message?.accountKeys || [];
          const poolAddr = accts[1]?.pubkey?.toBase58?.() || accts[1]?.toBase58?.();
          if (!poolAddr || copyLpSeen.has(poolAddr) || copyLpQueue.has(poolAddr)) continue;
          if (poolAddr === GWH_POOL_ID) continue;
          log(`Copy-LP DETECTED: ${w.address.slice(0, 8)} entered ${poolAddr.slice(0, 8)} — queuing`, 'lp');
          copyLpQueue.set(poolAddr, { detectedAt: Date.now(), wallet: w.address, poolAddress: poolAddr });
        } catch { continue; }
        await sleep(150);
      }
    } catch (e) { log(`Copy-LP watch ${w.address.slice(0, 8)}: ${e.message?.slice(0, 40)}`, 'warning'); }
  }

  for (const [poolAddr, entry] of copyLpQueue.entries()) {
    if (Date.now() - entry.detectedAt < CFG.COPY_LP_DELAY_MS) continue;
    if (copyLpSeen.has(poolAddr)) { copyLpQueue.delete(poolAddr); continue; }
    try {
      const poolInfo = await dlmm_loadPool(poolAddr);
      if (poolInfo) {
        const hasSol = poolInfo.mint_x === SOL_MINT_STR || poolInfo.mint_y === SOL_MINT_STR;
        if (hasSol) {
          newEntries.push({
            type: 'dlmm', name: `COPY:${poolInfo.name || poolAddr.slice(0, 6)}/SOL`,
            address: poolAddr, mint: poolInfo.mint_x === SOL_MINT_STR ? poolInfo.mint_y : poolInfo.mint_x,
            tvl: poolInfo.tvl || 0, vol: poolInfo.volume24h || 0, fees24h: poolInfo.fees24h || 0,
            feeTvl: poolInfo.fee_tvl_ratio24h || 0, apr: poolInfo.apr || 0,
            binStep: poolInfo.bin_step || 10, isCopyEntry: true, copyWallet: entry.wallet.slice(0, 8) + '...',
          });
          log(`Copy-LP READY: ${poolInfo.name || poolAddr.slice(0, 8)}`, 'success');
        }
        copyLpSeen.add(poolAddr);
        copyLpQueue.delete(poolAddr);
      }
    } catch { copyLpQueue.delete(poolAddr); }
  }
  return newEntries;
}

// ── Pool discovery ────────────────────────────────────────────────────────
function scorePool(pool) {
  const base = pool.type === 'dlmm' ? 1000 : pool.type === 'clmm' ? 800 : 0;
  return base
    + Math.min((pool.feeTvl || pool.fee_tvl_ratio24h || 0) * 10000, 50)
    + Math.min((pool.vol || pool.volume24h || 0) / 1000000 * 10, 30)
    + Math.min((pool.tvl || 0) / 500000 * 10, 20)
    + (pool.verified ? 10 : 0);
}

const poolBlacklist = new Map();
const BLACKLIST_TTL = 2 * 60 * 60 * 1000;

function blacklistPool(address, reason) {
  const existing = poolBlacklist.get(address) || { failures: 0 };
  poolBlacklist.set(address, { reason, bannedAt: Date.now(), failures: existing.failures + 1 });
  log(`⛔ Pool blacklisted: ${address.slice(0, 8)}... reason: ${reason} (fail #${existing.failures + 1})`, 'warning');
}

function isBlacklisted(address) {
  const entry = poolBlacklist.get(address);
  if (!entry) return false;
  if (Date.now() - entry.bannedAt > BLACKLIST_TTL) { poolBlacklist.delete(address); return false; }
  return true;
}

async function discoverDLMM() {
  log('Scanning Meteora DLMM pools...', 'scan');
  const minTvl    = CFG.MIN_POOL_TVL;
  const minVol    = CFG.MIN_POOL_VOL;
  const minFeeTvl = CFG.DLMM_MIN_FEE_TVL;

  const solPairs = await dlmm_fetchPools(true, 200);
  let qualified  = solPairs.filter(p => p.tvl >= minTvl && p.volume24h >= minVol && p.fee_tvl_ratio24h >= minFeeTvl);
  if (!qualified.length) qualified = solPairs;
  qualified.sort((a, b) => b.score - a.score);
  const top = qualified.slice(0, 50);
  log(`DLMM: ${top.length} qualifying pools`, 'scan');
  return top;
}

async function discoverUsdcPools() {
  log('USDC Track: scanning Meteora DLMM for USDC pairs...', 'scan');
  try {
    const rows     = await dlmm_fetchPools(false, 200);
    const usdcPairs = rows.filter(p => !p.is_blacklisted && (p.mint_x === USDC_MINT_STR || p.mint_y === USDC_MINT_STR));
    let qualified  = usdcPairs.filter(p => p.tvl >= CFG.MIN_POOL_TVL && p.volume24h >= CFG.MIN_POOL_VOL && p.fee_tvl_ratio24h >= CFG.DLMM_MIN_FEE_TVL);
    if (!qualified.length) qualified = usdcPairs;
    qualified.sort((a, b) => b.score - a.score);
    const top = qualified.slice(0, 50);
    top.forEach(p => { p.track = 'usdc'; });
    log(`USDC Track: ${top.length} qualifying pools`, 'scan');
    return top;
  } catch (e) { log(`USDC Track scan failed: ${e.message}`, 'warning'); return []; }
}

async function discoverCLMM() {
  if (process.env.CLMM_ENABLED === 'false') return [];
  log('CLMM scan: Orca Whirlpool stable pairs...', 'scan');
  const results = [];
  for (const p of STABLE_WHIRLPOOLS) {
    try {
      const info = await conn.getAccountInfo(new PublicKey(p.address), 'confirmed');
      if (!info) continue;
      results.push({ ...p, tvl: 100000, vol: 50000, fees24h: 5, feeTvl: 0.00005, apr: 18, verified: true });
    } catch {}
  }
  log(`CLMM: ${results.length}/${STABLE_WHIRLPOOLS.length} pools passed validation`, 'scan');
  return results;
}

async function discoverCPMM() {
  log('Scanning Raydium CPMM pools...', 'scan');
  try {
    const res = await axiosGet(
      'https://api-v3.raydium.io/pools/info/list?poolType=Standard&poolSortField=volume24h&sortType=desc&pageSize=100',
      { timeout: 8000 }
    );
    const list = res.data?.data?.data ?? res.data?.data ?? [];
    const results = [];
    for (const p of list) {
      if (p.id === GWH_POOL_ID) continue;
      if (p.mintA?.address === GWH_MINT_STR || p.mintB?.address === GWH_MINT_STR) continue;
      if (p.programId !== CPMM_PROGRAM.toBase58()) continue;
      const hasSol = p.mintA?.address === SOL_MINT_STR || p.mintB?.address === SOL_MINT_STR;
      if (!hasSol) continue;
      const tvl = parseFloat(p.tvl || 0), vol = parseFloat(p.day?.volume || 0);
      if (tvl < 50000 || vol < 10000) continue;
      const other   = p.mintA?.address === SOL_MINT_STR ? p.mintB : p.mintA;
      const fees24h = vol * 0.0025;
      results.push({
        type: 'cpmm', name: `${other?.symbol || other?.address?.slice(0, 6)}/SOL`,
        address: p.id, mint: other?.address, tvl, vol, fees24h,
        feeTvl: tvl > 0 ? fees24h / tvl : 0,
        apr: parseFloat(p.day?.apr || 0) * 100,
      });
    }
    results.sort((a, b) => b.feeTvl - a.feeTvl);
    log(`CPMM: ${results.length} pools`, 'scan');
    return results;
  } catch (e) { log(`CPMM scan failed: ${e.message?.slice(0, 60)}`, 'error'); return []; }
}

async function discoverAllPools() {
  let copyEntries = [];
  try { copyEntries = await scanCopyLpWallets(); } catch (e) { log(`Copy-LP scan error: ${e.message?.slice(0, 50)}`, 'warning'); }

  log('🔍 DLMM-FIRST SCAN: DLMM → CLMM → CPMM', 'scan');
  const [dlmmR, usdcR, clmmR, cpmmR] = await Promise.allSettled([
    discoverDLMM(), discoverUsdcPools(), discoverCLMM(), discoverCPMM(),
  ]);
  const dlmmPools = dlmmR.status === 'fulfilled' ? dlmmR.value : [];
  const usdcPools = usdcR.status === 'fulfilled' ? usdcR.value : [];
  const clmmPools = clmmR.status === 'fulfilled' ? clmmR.value : [];
  const cpmmPools = cpmmR.status === 'fulfilled' ? cpmmR.value : [];

  const seen = new Set();
  const deduped = [
    ...dlmmPools.sort((a, b) => scorePool(b) - scorePool(a)),
    ...usdcPools.sort((a, b) => scorePool(b) - scorePool(a)),
    ...clmmPools.sort((a, b) => scorePool(b) - scorePool(a)),
    ...cpmmPools.sort((a, b) => scorePool(b) - scorePool(a)),
  ].filter(p => {
    const key = p.mint || p.address;
    if (seen.has(key)) return false;
    seen.add(key); return true;
  });

  const copyPools  = (Array.isArray(copyEntries) ? copyEntries : []).filter(p => !isBlacklisted(p.address));
  const dlmmOnly   = deduped.filter(p => p.type === 'dlmm');
  const cpmmFb     = deduped.filter(p => p.type === 'cpmm');
  const basePools  = dlmmOnly.length > 0 ? dlmmOnly : cpmmFb;
  const finalPools = [...copyPools, ...basePools];

  const cleaned = finalPools.filter(p => {
    if (isBlacklisted(p.address)) {
      const entry    = poolBlacklist.get(p.address);
      const minsLeft = Math.ceil((BLACKLIST_TTL - (Date.now() - entry.bannedAt)) / 60000);
      log(`Skipping blacklisted pool ${p.name} — ${entry.reason} (${minsLeft}min left)`, 'warning');
      return false;
    }
    return true;
  });

  const dlmmCount = cleaned.filter(p => p.type === 'dlmm').length;
  const clmmCount = cleaned.filter(p => p.type === 'clmm').length;
  const cpmmCount = cleaned.filter(p => p.type === 'cpmm').length;
  log(`DLMM: ${dlmmCount} | CLMM: ${clmmCount} | CPMM: ${cpmmCount} | Blacklisted: ${poolBlacklist.size}`, 'scan');
  if (dlmmOnly.length > 0) log('DLMM-ONLY mode active — max fee concentration', 'scan');
  else log('No DLMM pools — using fallback engines', 'warning');

  cleaned.slice(0, 8).forEach(p =>
    log(`  [${p.type.toUpperCase()}] ${(p.name || '').padEnd(16)} score:${scorePool(p).toFixed(0)} apr:${p.apr?.toFixed(0) || '?'}%`, 'scan')
  );
  return cleaned.slice(0, CFG.MAX_POSITIONS * 3);
}

// ── Position manager ──────────────────────────────────────────────────────
async function enterPosition(pool, solAmount) {
  const alloc = solAmount ?? CFG.MIN_PER_SLOT;
  if (!await canSpend(alloc)) return false;
  if (!checkDailyCap(alloc)) return false;
  if (await isRugRisk(pool)) return false;

  const solBal = await getSolBal();
  if (solBal < baseline + CFG.GAS_RESERVE + alloc) {
    log(`Insufficient profit to enter ${pool.name} (need ${f4(alloc)} SOL above baseline)`, 'warning');
    return false;
  }
  log(`🌊 Enter [${pool.type.toUpperCase()}] ${pool.name} | ${f4(alloc)} SOL (profit only)`, 'lp');

  if (pool.type === 'dlmm') {
    const lamports = Math.floor(alloc * LAMPORTS_PER_SOL);
    const result   = await dlmm_addLiquidity(pool, lamports);
    if (!result) { blacklistPool(pool.address, 'dlmm_addLiquidity failed'); return false; }
    positions[pool.address] = {
      type: 'dlmm', name: pool.name, address: pool.address, mint: pool.mint,
      enteredAt: Date.now(), entryPrice: pool.entryPrice || 0, solIn: alloc,
      solDeployed: alloc, lastHarvest: 0, harvested: 0,
      activeBinId: result.activeBinId, lowerBinId: result.lowerBinId, upperBinId: result.upperBinId,
      positionAddress: result.positionAddress || null,
      apr: pool.apr, strategy: result.strategy || pickStrategy(pool).name, bins: result.bins || 20,
      isCopyEntry: pool.isCopyEntry || false, copyWallet: pool.copyWallet || null,
    };
    savePositions(); recordDeploy(alloc);
    log(`DLMM [${result.strategy}] position open: ${pool.name} | bins[${result.lowerBinId}..${result.upperBinId}]${pool.isCopyEntry ? ' [COPY-LP]' : ''}`, 'success');
    return true;

  } else if (pool.type === 'clmm') {
    const result = await clmm_enterPosition(pool, alloc);
    if (!result) { blacklistPool(pool.address, 'clmm_enterPosition failed'); return false; }
    positions[pool.address] = {
      type: 'clmm', name: `🌀 ${pool.name}`, address: pool.address, mint: pool.mint || pool.address,
      enteredAt: Date.now(), entryPrice: 0, solIn: alloc, solDeployed: alloc,
      lastHarvest: 0, harvested: 0,
      positionMint: result.positionMint, positionAddress: result.positionAddress,
      lowerTick: result.lowerTick, upperTick: result.upperTick, currentTick: result.currentTick,
      apr: pool.apr, stable: pool.stable,
    };
    savePositions();
    log(`CLMM position open: ${pool.name} | ticks[${result.lowerTick}..${result.upperTick}]`, 'success');
    return true;

  } else {
    let pk;
    try { pk = await cpmm_loadKeys(pool.address); }
    catch (e) { log(`CPMM loadKeys failed: ${e.message}`, 'error'); return false; }

    const sol0    = pk.token0Mint.equals(WSOL_MINT);
    const tokMint = (sol0 ? pk.token1Mint : pk.token0Mint).toBase58();
    const buyFraction = 0.495;
    const buyLam      = Math.floor(alloc * buyFraction * LAMPORTS_PER_SOL);
    const lpSol       = alloc * (1 - buyFraction);

    log(`CPMM fallback ${pool.name}: buy ${f4(buyFraction * 100)}% as token, add ${f4((1 - buyFraction) * 100)}% SOL to LP`, 'lp');

    const swapOk = await cpmm_swap(pk, SOL_MINT_STR, buyLam, CFG.SLIPPAGE);
    if (!swapOk) { blacklistPool(pool.address, 'cpmm_swap failed'); return false; }
    await sleep(4000);

    const addOk = await cpmm_addLiquidity(pk, lpSol);
    if (!addOk) { blacklistPool(pool.address, 'cpmm_addLiquidity failed'); await cpmm_sellToken(pk, pool.name); return false; }

    await sleep(2000);
    await cpmm_sellToken(pk, `${pool.name}_dust`);

    const lpBal = await getTokenBal(pk.lpMint.toBase58());
    const { r0: r0b, r1: r1b } = await cpmm_getReserves(pk);
    const entryPrice = sol0 ? bnToNum(r0b) / bnToNum(r1b) : bnToNum(r1b) / bnToNum(r0b);

    positions[pool.address] = {
      type: 'cpmm', name: `📉 ${pool.name}`, address: pool.address, mint: tokMint,
      enteredAt: Date.now(), entryPrice, solIn: alloc, solDeployed: alloc,
      lastHarvest: 0, harvested: 0, lpAtEntry: lpBal.toString(), apr: pool.apr,
    };
    savePositions();
    log(`CPMM fallback position open: ${pool.name} | ${f4(alloc)} SOL deployed`, 'success');
    return true;
  }
}

async function harvestPosition(posAddr) {
  const pos = positions[posAddr];
  if (!pos) return 0;
  if (Date.now() - pos.lastHarvest < CFG.HARVEST_COOL_MS) return 0;

  if (pos.type === 'dlmm') {
    const solBefore = await getSolBal();
    if (pos.positionAddress) {
      const pool = { address: pos.address, mint_x: pos.mint, mint_y: SOL_MINT_STR };
      await dlmm_claimFees(pool, pos.positionAddress);
      await sleep(3000);
    }
    const inRangeRatio = await dlmm_checkInRange(pos);
    if (inRangeRatio < CFG.DLMM_REBAL_PCT) {
      log(`DLMM ${pos.name}: ${(inRangeRatio * 100).toFixed(0)}% in range — rebalancing...`, 'rebal');
      await rebalanceDLMM(pos);
    }
    try {
      const tokBal = await getTokenBal(pos.mint);
      if (tokBal > 1000n) await jupiterSwap(pos.mint, SOL_MINT_STR, Number(tokBal));
    } catch {}
    const solAfter  = await getSolBal();
    const harvested = Math.max(0, solAfter - solBefore);
    if (harvested > CFG.MIN_HARVEST_SOL) {
      pos.harvested = (pos.harvested || 0) + harvested;
      pos.lastHarvest = Date.now();
      savePositions();
      log(`DLMM ${pos.name} fees: +${f4(harvested)} SOL 💰`, 'profit');
    }
    return harvested;

  } else if (pos.type === 'clmm') {
    if (!pos.positionMint) { log(`CLMM ${pos.name}: no positionMint — skip`, 'warning'); return 0; }
    const solBefore   = await getSolBal();
    const rangeCheck  = await clmm_isInRange(pos);
    if (!rangeCheck.inRange) {
      log(`CLMM ${pos.name}: out of range — rebalancing`, 'rebal');
      await clmm_exitPosition(pos);
      await sleep(3000);
      const solBal      = await getSolBal();
      const redeployAmt = Math.min(pos.solIn, solBal - baseline - CFG.GAS_RESERVE);
      if (redeployAmt >= CFG.MIN_PER_SLOT && await canSpend(redeployAmt)) {
        const pool = { address: pos.address, name: pos.name, type: 'clmm', stable: pos.stable, apr: pos.apr, mint: pos.mint, verified: true };
        await clmm_enterPosition(pool, redeployAmt);
      }
    } else {
      await clmm_collectFees(pos);
    }
    const solAfter  = await getSolBal();
    const harvested = Math.max(0, solAfter - solBefore);
    if (harvested > CFG.MIN_HARVEST_SOL) {
      pos.harvested   = (pos.harvested || 0) + harvested;
      pos.lastHarvest = Date.now();
      savePositions();
      log(`CLMM ${pos.name} fees: +${f4(harvested)} SOL 💰`, 'profit');
    }
    return harvested;

  } else {
    let pk;
    try { pk = await cpmm_loadKeys(posAddr); }
    catch (e) { log(`Harvest loadKeys failed: ${e.message}`, 'error'); return 0; }
    const lpBal = await getTokenBal(pk.lpMint.toBase58());
    if (lpBal < 1000n) { log(`${pos.name}: LP gone — closing position`, 'warning'); delete positions[posAddr]; savePositions(); return 0; }
    const toRemove = lpBal * BigInt(Math.floor(CFG.HARVEST_PCT * 10000)) / 10000n;
    if (toRemove < 100n) return 0;
    const solBefore = await getSolBal();
    log(`CPMM harvest ${pos.name}: remove ${(CFG.HARVEST_PCT * 100).toFixed(0)}% LP`, 'lp');
    const ok = await cpmm_removeLiquidity(pk, toRemove);
    if (!ok) return 0;
    await sleep(3000);
    await cpmm_sellToken(pk, pos.name);
    await sleep(2000);
    const solAfter  = await getSolBal();
    const harvested = Math.max(0, solAfter - solBefore);
    pos.lastHarvest = Date.now();
    pos.harvested   = (pos.harvested || 0) + harvested;
    savePositions();
    if (harvested > 0) log(`CPMM ${pos.name}: +${f4(harvested)} SOL 💰`, 'profit');
    return harvested;
  }
}

async function rebalanceDLMM(pos) {
  try {
    const pool = { address: pos.address, mint_x: SOL_MINT_STR, mint_y: pos.mint };
    if (pos.positionAddress) await dlmm_removeLiquidity(pool, pos.positionAddress);
    await sleep(4000);
    const tokBal = await getTokenBal(pos.mint);
    if (tokBal > 1000n) await jupiterSwap(pos.mint, SOL_MINT_STR, Number(tokBal));
    await sleep(3000);
    const solBal      = await getSolBal();
    const redeployAmt = Math.min(pos.solIn, solBal - baseline - CFG.GAS_RESERVE);
    if (redeployAmt < 0.02 || !await canSpend(redeployAmt)) { delete positions[pos.address]; savePositions(); return; }
    const result = await dlmm_addLiquidity(pool, Math.floor(redeployAmt * LAMPORTS_PER_SOL));
    if (result) {
      pos.activeBinId = result.activeBinId;
      pos.lowerBinId  = result.lowerBinId;
      pos.upperBinId  = result.upperBinId;
      pos.lastHarvest = Date.now();
      savePositions();
      log(`🔄 DLMM rebalance ✅ ${pos.name} bins[${result.lowerBinId}..${result.upperBinId}]`, 'success');
    }
  } catch (e) { log(`DLMM rebalance failed: ${e.message?.slice(0, 60)}`, 'error'); }
}

async function exitPosition(posAddr, reason = 'exit') {
  const pos = positions[posAddr];
  if (!pos) return;
  log(`⚠️  EXIT [${pos.type}] ${pos.name} | reason:${reason}`, 'warning');
  if (pos.type === 'dlmm') {
    const pool = { address: pos.address, mint_x: SOL_MINT_STR, mint_y: pos.mint };
    if (pos.positionAddress) await dlmm_removeLiquidity(pool, pos.positionAddress);
    await sleep(4000);
    const tokBal = await getTokenBal(pos.mint);
    if (tokBal > 1000n) await jupiterSwap(pos.mint, SOL_MINT_STR, Number(tokBal));
  } else if (pos.type === 'clmm') {
    await clmm_exitPosition(pos);
  } else {
    try {
      const pk    = await cpmm_loadKeys(posAddr);
      const lpBal = await getTokenBal(pk.lpMint.toBase58());
      if (lpBal > 0n) await cpmm_removeLiquidity(pk, lpBal);
      await sleep(3000);
      await cpmm_sellToken(pk, pos.name);
    } catch (e) { log(`Exit failed: ${e.message?.slice(0, 60)}`, 'error'); }
  }
  delete positions[posAddr];
  savePositions();
}

async function checkRiskGuards(posAddr) {
  const pos = positions[posAddr];
  if (!pos || !pos.entryPrice || pos.entryPrice === 0) return false;
  let currentPrice = 0;
  try {
    if (pos.type === 'dlmm') {
      const bin = await dlmm_getActiveBin(posAddr);
      currentPrice = parseFloat(bin?.price || 0);
    } else if (pos.type === 'clmm') {
      const rangeCheck = await clmm_isInRange(pos);
      currentPrice = pos.entryPrice || 1;
      if (!rangeCheck.inRange) { await exitPosition(posAddr, 'OOR'); return true; }
    } else {
      const pk = await cpmm_loadKeys(posAddr);
      const { r0, r1 } = await cpmm_getReserves(pk);
      const sol0 = pk.token0Mint.equals(WSOL_MINT);
      currentPrice = sol0 ? bnToNum(r0) / bnToNum(r1) : bnToNum(r1) / bnToNum(r0);
    }
  } catch { return false; }
  if (currentPrice === 0) return false;
  const pnlPct = (currentPrice - pos.entryPrice) / pos.entryPrice;
  const levels  = pos.isCopyEntry ? [0.03, ...TP_LEVELS] : TP_LEVELS;
  const tpHit   = levels.find(lvl => pnlPct >= lvl);
  if (tpHit) { log(`💰 ${pos.name} TP hit: +${(pnlPct * 100).toFixed(1)}% — exiting`, 'profit'); await exitPosition(posAddr, `TP+${Math.round(tpHit * 100)}%`); return true; }
  if (pnlPct <= -CFG.SL_PCT) { log(`${pos.name} SL hit: ${(pnlPct * 100).toFixed(1)}% — exiting`, 'warning'); await exitPosition(posAddr, 'SL'); return true; }
  if (Math.abs(pnlPct) >= CFG.IL_EXIT_PCT) { log(`${pos.name} IL guard: ${(Math.abs(pnlPct) * 100).toFixed(1)}% price move — exiting`, 'warning'); await exitPosition(posAddr, 'IL'); return true; }
  return false;
}

async function autoCompound(harvestedSol, activePools) {
  if (harvestedSol < 0.005) return;
  const deployable = await getDeployable();
  if (deployable < CFG.MIN_PER_SLOT) { log(`Auto-compound: deployable ${f4(deployable)} SOL below min — reserves protected`); return; }
  const addAmt = Math.min(harvestedSol * CFG.DEPLOY_PCT, deployable);
  if (!await canSpend(addAmt)) return;
  const bestExisting = Object.values(positions).filter(p => p.apr > 0).sort((a, b) => b.apr - a.apr)[0];
  const targetPool   = bestExisting ? activePools.find(p => p.address === bestExisting.address) : activePools[0];
  if (!targetPool) return;
  log(`Auto-compound +${f4(addAmt)} SOL → ${targetPool.name} [${targetPool.type}]`, 'lp');
  if (targetPool.type === 'dlmm') {
    await dlmm_addLiquidity(targetPool, Math.floor(addAmt * LAMPORTS_PER_SOL));
  } else {
    try {
      const pk      = await cpmm_loadKeys(targetPool.address);
      const sol0    = pk.token0Mint.equals(WSOL_MINT);
      const tokMint = (sol0 ? pk.token1Mint : pk.token0Mint).toBase58();
      await cpmm_swap(pk, SOL_MINT_STR, Math.floor(addAmt * 0.5 * LAMPORTS_PER_SOL), CFG.SLIPPAGE);
      await sleep(3000);
      await cpmm_addLiquidity(pk, addAmt * 0.5);
    } catch (e) { log(`Compound failed: ${e.message?.slice(0, 60)}`, 'error'); }
  }
}

async function emitStatus() {
  const bal           = await getSolBal();
  const totalDeployed = Object.values(positions).reduce((s, p) => s + (p.solIn || 0), 0);
  const slots = Object.values(positions).map((p, i) => ({
    slot:   ['ALPHA', 'BETA', 'GAMMA', 'DELTA', 'EPSILON'][i] || `SLOT${i + 1}`,
    name:   `[${p.type?.toUpperCase() || '?'}] ${p.name}`,
    solIn:  p.solIn || 0, sol: p.solIn || 0, pnl: p.harvested || 0,
    status: (p.solIn || 0) > 0 ? 'FARMING' : 'IDLE',
    apr:    p.apr || 0, type: p.type,
  }));
  emit('lp_positions', { total: totalDeployed, totalSol: totalDeployed, count: slots.length, slots, positions: slots });
  emit('harvester_health', {
    vault: bal, vaultBal: bal, baseline, profit: farmingProfit,
    farmingProfit, compoundedBal,
    stats: { totalDeployed, totalFeesEarned: totalHarvest, farmingProfit, compoundedBal, profit: farmingProfit, baseline, positions: Object.keys(positions).length },
  });
}

// ── Distribution engine ───────────────────────────────────────────────────
let distLedger = { lastDist: 0, totalDistributed: 0, totalPoolGrowth: 0, totalBurned: 0, totalApex: 0, totalDev: 0, totalCommunity: 0, history: [] };
try { distLedger = { ...distLedger, ...JSON.parse(fs.readFileSync(DIST_LEDGER_FILE, 'utf8')) }; } catch {}
const saveLedger = () => { try { fs.writeFileSync(DIST_LEDGER_FILE, JSON.stringify(distLedger, null, 2)); } catch {} };

let apexMasterPubkey = null;
try {
  const apexWallet = process.env.APEX_MASTER_WALLET;
  if (apexWallet) {
    apexMasterPubkey = new PublicKey(apexWallet);
    log(`APEX master: ${apexMasterPubkey.toBase58().slice(0, 8)}... (pubkey only)`);
  }
} catch (e) { log(`APEX_MASTER_WALLET invalid: ${e.message?.slice(0, 50)}`, 'warning'); }

function isDistributionTime() {
  const now    = new Date();
  const nowMin = now.getUTCHours() * 60 + now.getUTCMinutes();
  const times  = CFG.DIST_TIMES.split(',').map(t => {
    const [h, m] = t.trim().split(':').map(Number);
    return h * 60 + (m || 0);
  });
  return times.some(t => Math.abs(nowMin - t) <= 10) && (Date.now() - distLedger.lastDist > 6 * 3600 * 1000);
}

async function sendSolTo(destPubkey, lamports, label) {
  if (!destPubkey || lamports < 5000) return false;
  try {
    const tx = new Transaction();
    tx.add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 100_000 }));
    tx.add(ComputeBudgetProgram.setComputeUnitLimit({ units: 50_000 }));
    tx.add(SystemProgram.transfer({ fromPubkey: vault.publicKey, toPubkey: destPubkey, lamports }));
    const txid = await simulateAndSend(tx, [vault], `dist_${label}`);
    emit('terminal_log', { message: `[DIST] ${label}: ${f4(lamports / LAMPORTS_PER_SOL)} SOL sent`, type: 'profit', _src: 'HARV' });
    return txid;
  } catch (e) { log(`❌ ${label} failed: ${e.message?.slice(0, 60)}`, 'error'); return false; }
}

async function runDistribution() {
  log('', 'system');
  log('💰💰💰 DISTRIBUTION EVENT — TWICE DAILY 💰💰💰', 'profit');
  const solBal = await getSolBal();
  if (farmingProfit <= 0) { log('No farming profit yet — keep farming!', 'warning'); return; }
  const VAULT_PCT = parseFloat(process.env.COMPOUND_RATIO     || '40') / 100;
  const DIST_PCT  = parseFloat(process.env.DISTRIBUTION_SPLIT || '60') / 100;
  const wouldRemain = solBal - (farmingProfit * DIST_PCT);
  if (wouldRemain < CFG.MIN_VAULT_SOL) { log(`DIST BLOCKED — vault would drop to ${f4(wouldRemain)} SOL`, 'warning'); return; }
  if (farmingProfit < CFG.MIN_DIST_SOL) { log(`Below minimum ${f4(CFG.MIN_DIST_SOL)} SOL — accumulating`, 'warning'); return; }

  const distPool   = farmingProfit;
  const vaultSlice = distPool * VAULT_PCT;
  const distSlice  = distPool * DIST_PCT;

  const devWallet  = process.env.DEV_WALLET       || CFG.DEV_WALLET;
  const apexWallet = process.env.APEX_WALLET;
  const commWallet = process.env.COMMUNITY_WALLET  || CFG.COMMUNITY_WALLET;
  const lpWallet   = process.env.LP_WALLET         || CFG.GWH_LP_WALLET;
  const burnWallet = '11111111111111111111111111111111';
  const distWallet = process.env.DIST_WALLET;
  const useMulti   = apexWallet && devWallet && commWallet;

  log(`6-WAY PROFIT SPLIT — pool: ${f4(distPool)} SOL`, 'profit');
  log(`  40% Vault +${f4(vaultSlice)} SOL — compounds into LP`, 'profit');
  log(`  60% Dist   ${f4(distSlice)} SOL — ${useMulti ? '6-way split' : 'DIST_WALLET'}`, 'profit');

  compoundedBal += vaultSlice;

  let results = {};
  if (useMulti) {
    const splits = [
      { label: 'DEV (15%)',       addr: devWallet,  pct: 0.15 },
      { label: 'APEX (12%)',      addr: apexWallet, pct: 0.12 },
      { label: 'COMMUNITY (11%)', addr: commWallet, pct: 0.11 },
      { label: 'LP_GROWTH (10%)', addr: lpWallet,   pct: 0.10 },
      { label: 'BURN (10%)',      addr: burnWallet, pct: 0.10 },
    ];
    let totalSent = 0;
    for (const split of splits) {
      const amount = distSlice * (split.pct / 0.58);
      const sent   = await sendSolTo(new PublicKey(split.addr), Math.floor(amount * LAMPORTS_PER_SOL), split.label);
      if (sent) { totalSent += amount; log(`  ✅ ${split.label}: ${f4(amount)} SOL → ${split.addr.slice(0, 8)}...`, 'profit'); }
      await sleep(800);
    }
    results.totalSent = totalSent;
    results.ok = totalSent > 0;

    // USDC dual-asset distribution
    try {
      const usdcAcct = await getAccount(conn, getAssociatedTokenAddressSync(USDC_MINT_PK, vault.publicKey)).catch(() => null);
      if (usdcAcct && Number(usdcAcct.amount) > USDC_FLOOR + 500_000) {
        const usdcProfit = Number(usdcAcct.amount) - USDC_FLOOR;
        log(`USDC Profit: $${(usdcProfit / 1e6).toFixed(2)} — distributing`, 'profit');
        const usdcSplits = [
          { label: 'DEV (15%)', addr: DIST_WALLETS.DEV, pct: 0.15 },
          { label: 'APEX (12%)', addr: DIST_WALLETS.APEX, pct: 0.12 },
          { label: 'COMMUNITY (11%)', addr: DIST_WALLETS.COMMUNITY, pct: 0.11 },
          { label: 'LP_GROWTH (10%)', addr: DIST_WALLETS.LP_GROWTH, pct: 0.10 },
        ];
        for (const split of usdcSplits) {
          const usdcAmount = Math.floor(usdcProfit * split.pct);
          if (usdcAmount < 50000) continue;
          const destPk  = new PublicKey(split.addr);
          const srcAta  = getAssociatedTokenAddressSync(USDC_MINT_PK, vault.publicKey);
          const destAta = getAssociatedTokenAddressSync(USDC_MINT_PK, destPk);
          try {
            const usdcTx = new Transaction();
            usdcTx.add(ComputeBudgetProgram.setComputeUnitLimit({ units: 80_000 }));
            usdcTx.add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 100_000 }));
            try { await getAccount(conn, destAta); }
            catch { usdcTx.add(createAssociatedTokenAccountInstruction(vault.publicKey, destAta, destPk, USDC_MINT_PK)); }
            usdcTx.add(createTransferCheckedInstruction(srcAta, USDC_MINT_PK, destAta, vault.publicKey, usdcAmount, 6));
            await simulateAndSend(usdcTx, [vault], `USDC-dist-${split.label}`);
            log(`  ✅ USDC ${split.label}: $${(usdcAmount / 1e6).toFixed(2)}`, 'profit');
          } catch (e) { log(`  USDC dist failed [${split.label}]: ${e.message?.slice(0, 50)}`, 'warning'); }
          await sleep(500);
        }
      }
    } catch (e) { log(`USDC distribution check failed: ${e.message?.slice(0, 60)}`, 'warning'); }

  } else if (distWallet) {
    const ok = await sendSolTo(new PublicKey(distWallet), Math.floor(distSlice * LAMPORTS_PER_SOL), 'Distribution (60%)');
    results = { ok, totalSent: ok ? distSlice : 0 };
  } else {
    log('No distribution wallet set — skipping', 'error');
    compoundedBal -= vaultSlice;
    return;
  }

  if (!results.ok) { log('Distribution transfer failed — profit preserved', 'error'); compoundedBal -= vaultSlice; return; }

  const entry = {
    timestamp: new Date().toISOString(), distPool,
    vault: vaultSlice, dist: results.totalSent, totalSent: results.totalSent,
    vaultAfter: await getSolBal(), baseline, mode: useMulti ? '6-way' : 'single-wallet',
  };
  distLedger.lastDist          = Date.now();
  distLedger.totalDistributed += results.totalSent;
  distLedger.history           = [...(distLedger.history || []).slice(-96), entry];
  saveLedger();

  farmingProfit = 0;
  try { fs.writeFileSync(BASELINE_FILE, JSON.stringify({ baseline, baselineUSDC, farmingProfit: 0, compoundedBal, lockedAt: new Date().toISOString() })); } catch {}

  log('', 'system');
  log('✅ DISTRIBUTION COMPLETE', 'profit');
  log(`   Vault kept: +${f4(vaultSlice)} SOL (40% — compounds next pulse)`, 'profit');
  log(`   Dist sent:   ${f4(results.totalSent)} SOL (${useMulti ? '6-way split' : 'DIST_WALLET'})`, 'profit');
  log(`   All-time distributed: ${f4(distLedger.totalDistributed)} SOL`, 'profit');

  emit('distribution_event', {
    timestamp: entry.timestamp, distPool, vaultKept: vaultSlice,
    distSent: results.totalSent, totalDistributed: distLedger.totalDistributed,
    nextDist: CFG.DIST_TIMES, mode: entry.mode,
  });
}

// ── Main loop ─────────────────────────────────────────────────────────────
async function main() {
  connectHub();
  await sleep(2000);

  log('', 'system');
  log('🚀 GWH HARVESTER v14.0 — DLMM + CPMM + CLMM + Copy-LP + Dual-Asset 6-Way Distribution', 'system');
  log(`RPC pool: ${HARV_RPC_ENDPOINTS.length} endpoint(s)`, 'system');
  log(`Vault: ${vault ? vault.publicKey.toBase58() : '(DRY-RUN — no wallet)'}`, 'system');

  await setBaseline();

  log(`TP levels: ${TP_LEVELS.map(t => '+' + Math.round(t * 100) + '%').join('/')}`, 'system');
  log(`Daily deploy cap: ${f4(CFG.DAILY_DEPLOY_CAP)} SOL/day`, 'system');
  log(`Distribution: ${CFG.DIST_TIMES} UTC`, 'system');
  if (distLedger.totalDistributed > 0)
    log(`History: ${f4(distLedger.totalDistributed)} SOL distributed total`, 'profit');

  let activePools = [];
  let lastScan    = 0;

  const pulse = async () => {
    try {
      const solBal   = await getSolBal();
      const profit   = Math.max(0, solBal - baseline - CFG.GAS_RESERVE);
      const posCount = Object.keys(positions).length;
      log(`💊 Pulse | SOL:${f4(solBal)} | Base:${f4(baseline)} | Profit:${f4(profit)} | Pos:${posCount} | Earned:${f4(totalHarvest)}`, 'system');

      if (solBal < baseline) {
        log(`Vault below baseline (${f4(baseline)} SOL) — principal guard active`, 'warning');
        await emitStatus(); return;
      }

      if (Date.now() - lastScan > CFG.RESCAN_MS) {
        activePools = await discoverAllPools();
        lastScan    = Date.now();
      }

      // Risk checks
      for (const addr of [...Object.keys(positions)]) { await checkRiskGuards(addr); await sleep(1000); }

      // Harvest
      let pulseHarvest = 0;
      for (const addr of [...Object.keys(positions)]) {
        const earned = await harvestPosition(addr);
        pulseHarvest  += earned;
        totalHarvest  += earned;
        farmingProfit += earned;
        await sleep(2000);
      }
      if (pulseHarvest > 0) {
        try { fs.writeFileSync(BASELINE_FILE, JSON.stringify({ baseline, farmingProfit, compoundedBal, lockedAt: new Date().toISOString() })); } catch {}
        log(`💰 Farming fees this pulse: +${f4(pulseHarvest)} SOL | Total: ${f4(farmingProfit)} SOL`, 'profit');
      }

      if (pulseHarvest >= 0.005) await autoCompound(pulseHarvest, activePools);
      if (isDistributionTime()) await runDistribution();

      // Capital allocator
      const solBalNow  = await getSolBal();
      const deployable = await getDeployable();
      const openSlots  = CFG.MAX_POSITIONS - Object.keys(positions).length;

      log('🌊 CAPITAL ALLOCATOR', 'lp');
      log(`  Balance: ${f4(solBalNow)} SOL | Principal: ${f4(baseline)} SOL (locked)`, 'lp');
      log(`  Deployable: ${f4(deployable)} SOL | Open slots: ${openSlots}`, 'lp');

      if (openSlots > 0 && deployable >= CFG.MIN_PER_SLOT) {
        const maxPerSlot = deployable * 0.40;
        const perSlot    = Math.min(maxPerSlot, Math.max(CFG.MIN_PER_SLOT, deployable / openSlots));

        const dlmmSol  = activePools.filter(p => p.type === 'dlmm' && p.track !== 'usdc' && !positions[p.address]);
        const dlmmUsdc = CFG.USDC_TRACK_ENABLED ? activePools.filter(p => p.type === 'dlmm' && p.track === 'usdc' && !positions[p.address]) : [];
        const cpmmSol  = activePools.filter(p => p.type === 'cpmm' && !positions[p.address]);

        const solSlots  = Math.ceil(openSlots * 0.80);
        const usdcSlots = Math.floor(openSlots * 0.20);

        log(`  Strategy: DLMM-SOL:${dlmmSol.length}/${solSlots}slots | DLMM-USDC:${dlmmUsdc.length}/${usdcSlots}slots | CPMM-fallback:${cpmmSol.length} | ${f4(perSlot)} SOL/slot`, 'dlmm');

        let slotsRemaining = openSlots;

        // 1. DLMM SOL (80%)
        for (const pool of dlmmSol) {
          if (slotsRemaining <= 0 || (openSlots - slotsRemaining) >= solSlots) break;
          if (!await canSpend(perSlot)) break;
          if (await enterPosition(pool, perSlot)) slotsRemaining--;
          await sleep(4000);
        }

        // 2. DLMM USDC (20%)
        if (slotsRemaining > 0 && dlmmUsdc.length > 0) {
          for (const pool of dlmmUsdc) {
            if (slotsRemaining <= 0) break;
            if (!await canSpend(perSlot)) break;
            if (await enterPosition(pool, perSlot)) slotsRemaining--;
            await sleep(4000);
          }
        }

        // 3. CPMM fallback (only when DLMM = 0)
        if (slotsRemaining > 0 && dlmmSol.length === 0 && dlmmUsdc.length === 0) {
          log(`No DLMM pools — CPMM SOL fallback: ${cpmmSol.length} pools`, 'warning');
          for (const pool of cpmmSol.slice(0, slotsRemaining)) {
            if (!await canSpend(perSlot)) break;
            if (await enterPosition(pool, perSlot)) slotsRemaining--;
            await sleep(4000);
          }
        }

        const filled = openSlots - slotsRemaining;
        if (filled > 0) log(`Entered ${filled} position(s)`, 'success');
        else             log(`No pools entered — waiting for qualifying Meteora pools`, 'system');

      } else if (openSlots > 0) {
        log(`Deployable ${f4(deployable)} SOL below min ${f4(CFG.MIN_PER_SLOT)} SOL — accumulating...`, 'system');
      } else {
        log(`All ${CFG.MAX_POSITIONS} slots filled — farming 💰`, 'success');
      }

      await emitStatus();
    } catch (e) { log(`Pulse error: ${e.message}`, 'error'); }
  };

  activePools = await discoverAllPools();
  lastScan    = Date.now();
  await pulse();
  setInterval(pulse, CFG.CHECK_MS);

  // Health log every 10 min
  setInterval(async () => {
    const sol    = await getSolBal();
    const profit = Math.max(0, sol - baseline - CFG.GAS_RESERVE);
    log(`HEALTH v14 | SOL:${f4(sol)} | Base:${f4(baseline)} | Profit:${f4(profit)} | Pos:${Object.keys(positions).length} | Earned:${f4(totalHarvest)} | Daily:${f4(dailyDeployedSol)}/${f4(CFG.DAILY_DEPLOY_CAP)}`, 'system');
    for (const p of Object.values(positions)) {
      const age = Math.floor((Date.now() - p.enteredAt) / 60000);
      log(`  [${p.type.toUpperCase()}] ${p.name.padEnd(14)} in:${f4(p.solIn)} SOL | age:${age}m | earned:${f4(p.harvested || 0)} SOL | apr:${f2(p.apr || 0)}%`, 'lp');
    }
    await emitStatus();
  }, 10 * 60 * 1000);
}

main().catch(e => { log(`FATAL: ${e.message}`, 'error'); process.exit(1); });
