"""
=============================================================================
 TELEGRAM -> MT5 MASTER/SLAVE TRADE COPIER | ARCHITECT v36.0
=============================================================================
Main upgrades over v35:
  1) Faster trading path:
     - lower slave poll latency (1s) + event wakeup on new master trades
     - symbol resolution cache to reduce repeated MT5 lookups
     - retry logic for transient broker errors (requote/prices changed/no quotes)
  2) Smarter auto risk controls:
     - auto SL/TP for any trade missing one or both values
     - derives missing SL from TP (and vice-versa) using RR profile
     - broker stop-level validation and directional correction for SL/TP
  3) Correct slave deduplication:
     - per-slave copy tracking via copied_by[slave_account]
     - avoids global copied=true bug across multiple slaves
  4) Better bridge payload:
     - stores final executed entry/sl/tps (not raw signal values)
=============================================================================
"""

import asyncio
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime
from typing import Any

import MetaTrader5 as mt5
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("copier_v36.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("ARCHITECT-v36")


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
TG_SESSION = os.getenv("TG_SESSION", "")
TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")


# ---------------------------------------------------------------------------
# Runtime tuning
# ---------------------------------------------------------------------------
POLL_SECONDS = float(os.getenv("SLAVE_POLL_SECONDS", "1"))
TRANSIENT_ORDER_RETCODES = {
    10004,  # requote
    10012,  # timeout
    10020,  # prices changed
    10021,  # no quotes
    10024,  # too many requests
    10028,  # locked/processing
}

# A practical RR profile that still works across FX/Metals/Indices.
RR_CONFIG: dict[str, Any] = {
    "auto_rr1": 1.0,
    "auto_rr2": 1.5,
    "auto_rr3": 2.0,
    "auto_sl_pips": 40,
    "draw_on_chart": False,
    "symbol_sl_pips": {
        "XAUUSD": 120,
        "XAGUSD": 180,
        "NASDAQ": 250,
        "US30": 380,
        "SP500": 140,
        "GER40": 220,
        "UK100": 170,
        "BTCUSD": 1200,
        "ETHUSD": 650,
    },
}


# ---------------------------------------------------------------------------
# Accounts (replace with your live configs from v35)
# ---------------------------------------------------------------------------
MASTER_ACCOUNTS = {
    "Master1": {
        "enabled": True,
        "account": int(os.getenv("MASTER1_ACCOUNT", "0")),
        "password": os.getenv("MASTER1_PASSWORD", ""),
        "server": os.getenv("MASTER1_SERVER", ""),
        "mt5_path": os.getenv("MASTER1_PATH", r"C:\MT5\Master1\terminal64.exe"),
        "lot_size": float(os.getenv("MASTER1_LOT", "0.01")),
        "magic_number": int(os.getenv("MASTER1_MAGIC", "123456")),
        "deviation": int(os.getenv("MASTER1_DEV", "300")),
        "max_lot": float(os.getenv("MASTER1_MAX_LOT", "10.0")),
        "channels": [
            int(x)
            for x in os.getenv("MASTER1_CHANNELS", "").split(",")
            if x.strip()
        ],
        "json_file": os.getenv(
            "MASTER1_JSON_FILE", r"C:\AI_Signal\master_trades_master1.json"
        ),
    },
}

SLAVE_ACCOUNTS = {
    "Slave1": {
        "enabled": bool(int(os.getenv("SLAVE1_ENABLED", "0"))),
        "account": int(os.getenv("SLAVE1_ACCOUNT", "0")),
        "password": os.getenv("SLAVE1_PASSWORD", ""),
        "server": os.getenv("SLAVE1_SERVER", ""),
        "mt5_path": os.getenv("SLAVE1_PATH", r"C:\MT5\Slave1\terminal64.exe"),
        "lot_multiplier": float(os.getenv("SLAVE1_LOT_MULT", "1.0")),
        "magic_number": int(os.getenv("SLAVE1_MAGIC", "999998")),
        "deviation": int(os.getenv("SLAVE1_DEV", "300")),
        "max_lot": float(os.getenv("SLAVE1_MAX_LOT", "10.0")),
        "copy_from_master": int(os.getenv("SLAVE1_COPY_FROM", "0")),
    },
}


# ---------------------------------------------------------------------------
# Symbol mapping
# ---------------------------------------------------------------------------
SYMBOL_MAP = {
    "GOLD": "XAUUSD",
    "XAU": "XAUUSD",
    "XAUUSD": "XAUUSD",
    "SILVER": "XAGUSD",
    "XAG": "XAGUSD",
    "XAGUSD": "XAGUSD",
    "EURUSD": "EURUSD",
    "EUR/USD": "EURUSD",
    "GBPUSD": "GBPUSD",
    "GBP/USD": "GBPUSD",
    "USDJPY": "USDJPY",
    "USD/JPY": "USDJPY",
    "USDCHF": "USDCHF",
    "USDCAD": "USDCAD",
    "AUDUSD": "AUDUSD",
    "NZDUSD": "NZDUSD",
    "NAS": "NASDAQ",
    "NASDAQ": "NASDAQ",
    "NAS100": "NASDAQ",
    "US100": "NASDAQ",
    "US30": "US30",
    "SP500": "SP500",
    "US500": "SP500",
    "DAX": "GER40",
    "GER40": "GER40",
    "FTSE": "UK100",
    "UK100": "UK100",
    "BTC": "BTCUSD",
    "BTCUSD": "BTCUSD",
    "ETH": "ETHUSD",
    "ETHUSD": "ETHUSD",
}
SUFFIXES = ["", ".crp", "+", ".pro", ".v", ".a", "m", ".r", ".c", "_micro", "micro"]


# ---------------------------------------------------------------------------
# MT5 state
# ---------------------------------------------------------------------------
_mt5_lock = asyncio.Lock()
_mt5_active_account: int | None = None
_symbol_cache: dict[tuple[int, str], str | None] = {}
_seen_signals: dict[str, float] = {}
_new_trade_event = asyncio.Event()


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------
def _cleanup_seen_signals(max_items: int = 1200, ttl_seconds: int = 7200) -> None:
    now = time.time()
    stale = [k for k, ts in _seen_signals.items() if now - ts > ttl_seconds]
    for k in stale:
        _seen_signals.pop(k, None)
    while len(_seen_signals) > max_items:
        oldest = min(_seen_signals, key=_seen_signals.get)
        _seen_signals.pop(oldest, None)


def get_filling_mode(symbol: str) -> int:
    info = mt5.symbol_info(symbol)
    if not info:
        return mt5.ORDER_FILLING_FOK
    fm = info.filling_mode
    if fm & 2:
        return mt5.ORDER_FILLING_IOC
    if fm & 1:
        return mt5.ORDER_FILLING_FOK
    return mt5.ORDER_FILLING_RETURN


def normalize_volume(symbol: str, volume: float, max_lot: float) -> float:
    info = mt5.symbol_info(symbol)
    if not info:
        return round(min(max(volume, 0.01), max_lot), 2)
    min_lot = info.volume_min or 0.01
    step = info.volume_step or 0.01
    v = min(max(volume, min_lot), max_lot)
    steps = round(v / step)
    return round(steps * step, 2)


def symbol_point_and_digits(symbol: str) -> tuple[float, int]:
    info = mt5.symbol_info(symbol)
    if not info:
        return 0.0001, 5
    return info.point, info.digits


def get_pip_size(symbol: str) -> float:
    info = mt5.symbol_info(symbol)
    if not info:
        return 0.0001
    if "JPY" in symbol or symbol in {"NASDAQ", "US30", "SP500", "GER40", "UK100"}:
        return info.point
    if "XAU" in symbol or "XAG" in symbol:
        return info.point * 10
    return info.point * 10


def min_stop_distance(symbol: str) -> float:
    info = mt5.symbol_info(symbol)
    if not info:
        return 0.0
    stops_level = max(int(info.trade_stops_level or 0), int(info.trade_freeze_level or 0))
    return stops_level * info.point


def safe_round(symbol: str, value: float) -> float:
    _, digits = symbol_point_and_digits(symbol)
    return round(value, digits)


def enforce_sl_tp(
    symbol: str,
    action: int,
    ref_price: float,
    sl: float,
    tp1: float,
    tp2: float = 0.0,
    tp3: float = 0.0,
) -> tuple[float, float, float, float]:
    """
    Ensure SL/TP direction is valid and respects broker minimum stop distance.
    """
    min_dist = min_stop_distance(symbol)
    pip = get_pip_size(symbol)
    safety = max(min_dist, pip * 2)

    def _clip_tp(tp: float) -> float:
        if tp <= 0:
            return 0.0
        if action == mt5.ORDER_TYPE_BUY and tp <= ref_price + safety:
            tp = ref_price + safety
        if action == mt5.ORDER_TYPE_SELL and tp >= ref_price - safety:
            tp = ref_price - safety
        return safe_round(symbol, tp)

    if sl > 0:
        if action == mt5.ORDER_TYPE_BUY and sl >= ref_price - safety:
            sl = ref_price - safety
        elif action == mt5.ORDER_TYPE_SELL and sl <= ref_price + safety:
            sl = ref_price + safety
        sl = safe_round(symbol, sl)

    return safe_round(symbol, sl), _clip_tp(tp1), _clip_tp(tp2), _clip_tp(tp3)


def smart_auto_levels(
    symbol: str,
    action: int,
    entry_price: float,
    sl: float,
    tp1: float,
    tp2: float,
    tp3: float,
) -> tuple[float, float, float, float]:
    """
    Smart fill of missing levels:
      - none provided => generate SL + TP1/TP2/TP3
      - SL only       => derive TPs by RR
      - TP only       => back-calc SL from TP1 and RR1
    """
    if entry_price <= 0:
        return 0.0, 0.0, 0.0, 0.0

    pip = get_pip_size(symbol)
    base_sl_pips = RR_CONFIG["symbol_sl_pips"].get(symbol, RR_CONFIG["auto_sl_pips"])
    default_risk = base_sl_pips * pip
    rr1 = float(RR_CONFIG["auto_rr1"])
    rr2 = float(RR_CONFIG["auto_rr2"])
    rr3 = float(RR_CONFIG["auto_rr3"])

    # Build missing risk from available data.
    risk = 0.0
    if sl > 0:
        risk = abs(entry_price - sl)
    elif tp1 > 0:
        risk = abs(tp1 - entry_price) / max(rr1, 0.1)
    elif tp2 > 0:
        risk = abs(tp2 - entry_price) / max(rr2, 0.1)
    elif tp3 > 0:
        risk = abs(tp3 - entry_price) / max(rr3, 0.1)
    if risk <= 0:
        risk = default_risk

    # Fill SL if missing.
    if sl <= 0:
        sl = entry_price - risk if action == mt5.ORDER_TYPE_BUY else entry_price + risk

    # Fill TPs if missing.
    if tp1 <= 0:
        tp1 = entry_price + (risk * rr1) if action == mt5.ORDER_TYPE_BUY else entry_price - (risk * rr1)
    if tp2 <= 0:
        tp2 = entry_price + (risk * rr2) if action == mt5.ORDER_TYPE_BUY else entry_price - (risk * rr2)
    if tp3 <= 0:
        tp3 = entry_price + (risk * rr3) if action == mt5.ORDER_TYPE_BUY else entry_price - (risk * rr3)

    sl, tp1, tp2, tp3 = enforce_sl_tp(symbol, action, entry_price, sl, tp1, tp2, tp3)

    log.info(
        "  📐 Smart SL/TP | entry:%s sl:%s tp1:%s tp2:%s tp3:%s",
        safe_round(symbol, entry_price),
        sl,
        tp1,
        tp2,
        tp3,
    )
    return sl, tp1, tp2, tp3


def load_log(path: str) -> dict:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({}, f)
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_log(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# MT5 connection management
# ---------------------------------------------------------------------------
async def _ensure_mt5_account(cfg: dict) -> bool:
    global _mt5_active_account
    account = cfg["account"]
    if not account:
        log.error("MT5 account missing in config")
        return False

    if _mt5_active_account == account:
        acc = mt5.account_info()
        if acc and acc.login == account:
            return True
        mt5.shutdown()
        _mt5_active_account = None

    if _mt5_active_account is not None:
        mt5.shutdown()
        _mt5_active_account = None
        await asyncio.sleep(0.25)

    ok = mt5.initialize(
        path=cfg["mt5_path"],
        login=account,
        password=cfg["password"],
        server=cfg["server"],
        timeout=15000,
    )
    if not ok:
        log.error("MT5 login failed for #%s: %s", account, mt5.last_error())
        return False

    acc = mt5.account_info()
    if not acc:
        log.error("MT5 account_info() unavailable after login #%s", account)
        return False

    _mt5_active_account = account
    log.info("✅ MT5 #%s ready (%s)", acc.login, acc.server)
    return True


def resolve_symbol(base: str, account: int) -> str | None:
    key = (account, base)
    if key in _symbol_cache:
        return _symbol_cache[key]

    for sfx in SUFFIXES:
        sym = f"{base}{sfx}"
        if mt5.symbol_select(sym, True):
            info = mt5.symbol_info(sym)
            if info and info.visible:
                _symbol_cache[key] = sym
                return sym

    _symbol_cache[key] = None
    return None


def infer_entry_price(symbol: str, action: int, requested_entry: float) -> float:
    if requested_entry and requested_entry > 0:
        return requested_entry
    tick = mt5.symbol_info_tick(symbol)
    if not tick:
        return 0.0
    return tick.ask if action == mt5.ORDER_TYPE_BUY else tick.bid


def _log_order_error(code: int) -> str:
    hints = {
        10014: "invalid lot volume",
        10015: "invalid price",
        10016: "invalid SL/TP distance",
        10019: "insufficient funds",
        10020: "prices changed",
        10021: "no quotes",
        10024: "too many requests",
    }
    return hints.get(code, "unknown error")


def send_order(
    cfg: dict,
    symbol: str,
    action: int,
    lot: float,
    sl: float,
    tp: float,
    entry: float,
    is_limit: bool,
    comment: str = "v36",
) -> int | None:
    lot = normalize_volume(symbol, lot, cfg["max_lot"])
    tick = mt5.symbol_info_tick(symbol)
    info = mt5.symbol_info(symbol)
    if not tick or not info:
        log.warning("No tick/symbol info for %s", symbol)
        return None

    market_price = tick.ask if action == mt5.ORDER_TYPE_BUY else tick.bid
    entry = entry if entry > 0 else market_price
    sl, tp, _, _ = enforce_sl_tp(symbol, action, entry, sl, tp, 0.0, 0.0)
    point = info.point

    trade_action = mt5.TRADE_ACTION_DEAL
    order_type = action
    exec_price = market_price
    if entry > 0:
        distance = abs(market_price - entry)
        if distance > point * 20 or is_limit:
            trade_action = mt5.TRADE_ACTION_PENDING
            exec_price = safe_round(symbol, entry)
            if action == mt5.ORDER_TYPE_BUY:
                order_type = (
                    mt5.ORDER_TYPE_BUY_LIMIT
                    if exec_price < market_price
                    else mt5.ORDER_TYPE_BUY_STOP
                )
            else:
                order_type = (
                    mt5.ORDER_TYPE_SELL_LIMIT
                    if exec_price > market_price
                    else mt5.ORDER_TYPE_SELL_STOP
                )

    request = {
        "action": trade_action,
        "symbol": symbol,
        "volume": lot,
        "type": order_type,
        "price": exec_price,
        "deviation": cfg["deviation"],
        "magic": cfg["magic_number"],
        "comment": comment[:31],
        "type_filling": get_filling_mode(symbol),
        "type_time": mt5.ORDER_TIME_GTC,
    }
    if sl > 0:
        request["sl"] = sl
    if tp > 0:
        request["tp"] = tp

    for attempt in range(1, 4):
        res = mt5.order_send(request)
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            log.info(
                "✅ Order #%s %s lot:%s sl:%s tp:%s attempt:%s",
                res.order,
                symbol,
                lot,
                request.get("sl", 0),
                request.get("tp", 0),
                attempt,
            )
            return res.order

        code = res.retcode if res else -1
        hint = _log_order_error(code)
        log.warning("❌ Order failed (%s): %s | attempt:%s", code, hint, attempt)

        if code == 10016:
            # Broker rejected SL/TP. Keep the order flow alive by widening and retrying.
            sl2, tp2, _, _ = enforce_sl_tp(symbol, action, exec_price, sl, tp, 0.0, 0.0)
            if sl2 > 0:
                request["sl"] = sl2
            if tp2 > 0:
                request["tp"] = tp2

        if code not in TRANSIENT_ORDER_RETCODES and code != 10016:
            break
        time.sleep(0.2 * attempt)

    return None


def close_positions(symbol: str, magic: int) -> None:
    positions = mt5.positions_get(symbol=symbol)
    if not positions:
        return
    for pos in positions:
        if pos.magic != magic:
            continue
        close_type = (
            mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        )
        tick = mt5.symbol_info_tick(symbol)
        if not tick:
            continue
        price = tick.bid if close_type == mt5.ORDER_TYPE_SELL else tick.ask
        mt5.order_send(
            {
                "action": mt5.TRADE_ACTION_DEAL,
                "position": pos.ticket,
                "symbol": symbol,
                "volume": pos.volume,
                "type": close_type,
                "price": price,
                "deviation": 300,
                "magic": magic,
                "comment": "CLOSE_v36",
                "type_filling": get_filling_mode(symbol),
            }
        )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
async def ai_parse_signal(text: str) -> dict | None:
    if not ANTHROPIC_API_KEY:
        return None
    try:
        import aiohttp

        prompt = f"""Extract only JSON from this trade message:\n{text}\n
Required keys: is_signal,type,symbol,action,entry,sl,tp1,tp2,tp3,is_limit,partial_percent,new_sl,confidence
Types: trade|close|breakeven|partial_close|modify_sl|news|other
If unclear set is_signal=false."""

        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 350,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=aiohttp.ClientTimeout(total=8),
            )
            payload = await resp.json()

        raw = payload["content"][0]["text"].strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        p = json.loads(raw)
        if not p.get("is_signal") or p.get("confidence", 0) < 40:
            return None
        action = {"BUY": mt5.ORDER_TYPE_BUY, "SELL": mt5.ORDER_TYPE_SELL}.get(
            p.get("action")
        )
        return {
            "type": p.get("type", "trade"),
            "symbol": p.get("symbol"),
            "action": action,
            "entry": p.get("entry") or 0.0,
            "sl": p.get("sl") or 0.0,
            "tp1": p.get("tp1") or 0.0,
            "tp2": p.get("tp2") or 0.0,
            "tp3": p.get("tp3") or 0.0,
            "is_limit": bool(p.get("is_limit", False)),
            "partial_percent": p.get("partial_percent"),
            "new_sl": p.get("new_sl"),
        }
    except Exception:
        return None


def regex_parse_signal(text: str) -> dict | None:
    t = text.upper().strip()
    if not any(x in t for x in ("BUY", "SELL", "CLOSE", "BREAKEVEN", "SL TO", "PARTIAL")):
        return None

    symbol = None
    for key in sorted(SYMBOL_MAP, key=len, reverse=True):
        if re.search(rf"(?<![A-Z]){re.escape(key)}(?![A-Z])", t):
            symbol = SYMBOL_MAP[key]
            break
    if not symbol:
        return None

    if any(x in t for x in ("BREAKEVEN", "MOVE SL TO BE", "SL TO ENTRY", "SL TO BE")):
        return {"type": "breakeven", "symbol": symbol, "action": None}

    pct = 50
    m_pct = re.search(r"(\d{1,3})\s*%", t)
    if m_pct:
        pct = max(1, min(100, int(m_pct.group(1))))
    if "PARTIAL" in t and ("CLOSE" in t or "EXIT" in t):
        return {
            "type": "partial_close",
            "symbol": symbol,
            "action": None,
            "partial_percent": pct,
        }

    if any(x in t for x in ("FULL CLOSE", "CLOSE ALL", "EXIT ALL", "CLOSE NOW")):
        return {"type": "close", "symbol": symbol, "action": None}

    action = None
    if "BUY" in t or "LONG" in t:
        action = mt5.ORDER_TYPE_BUY
    elif "SELL" in t or "SHORT" in t:
        action = mt5.ORDER_TYPE_SELL
    if action is None:
        return None

    def _num(pattern: str) -> float:
        m = re.search(pattern, t)
        return float(m.group(1)) if m else 0.0

    entry = _num(r"(?:ENTRY|ENTER|@)\s*[:\-=@]?\s*(\d{1,6}\.?\d{0,5})")
    sl = _num(r"(?:S\.?L\.?|STOP\s*LOSS)\s*[:\-=@]?\s*(\d{1,6}\.?\d{0,5})")

    tps: list[float] = []
    for m_tp in re.finditer(r"T\.?P\.?\s*[1-9]?\s*[:\-=@]?\s*(\d{1,6}\.?\d{0,5})", t):
        v = float(m_tp.group(1))
        if v > 0 and v not in tps:
            tps.append(v)
    if not tps:
        m = re.search(
            r"T\.?P\.?\s*[:\-]?\s*(\d{1,6}\.?\d{0,5})(?:\s*[\/,]\s*(\d{1,6}\.?\d{0,5}))?(?:\s*[\/,]\s*(\d{1,6}\.?\d{0,5}))?",
            t,
        )
        if m:
            for g in m.groups():
                if g:
                    tps.append(float(g))
    tps = tps[:3]
    tp1 = tps[0] if len(tps) > 0 else 0.0
    tp2 = tps[1] if len(tps) > 1 else 0.0
    tp3 = tps[2] if len(tps) > 2 else 0.0

    is_limit = any(x in t for x in ("LIMIT", "PENDING", "BUY STOP", "SELL STOP", "BUY LIMIT", "SELL LIMIT"))
    return {
        "type": "trade",
        "symbol": symbol,
        "action": action,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "is_limit": is_limit,
    }


async def parse_signal(text: str) -> dict | None:
    if ANTHROPIC_API_KEY:
        sig = await ai_parse_signal(text)
        if sig:
            return sig
    return regex_parse_signal(text)


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
async def execute_on_master(master_name: str, cfg: dict, sig: dict) -> None:
    async with _mt5_lock:
        if not await _ensure_mt5_account(cfg):
            return

        sym = resolve_symbol(sig["symbol"], cfg["account"])
        if not sym:
            log.warning("[%s] Symbol %s not found", master_name, sig["symbol"])
            return

        kind = sig.get("type", "trade")
        magic = cfg["magic_number"]

        if kind == "close":
            close_positions(sym, magic)
            return

        if kind != "trade":
            # Keep this version focused on trade + close speed path.
            return

        if sig.get("action") not in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL):
            return

        action = sig["action"]
        requested_entry = float(sig.get("entry", 0) or 0)
        entry = infer_entry_price(sym, action, requested_entry)
        if entry <= 0:
            log.warning("[%s] Unable to infer entry for %s", master_name, sym)
            return

        sl = float(sig.get("sl", 0) or 0)
        tp1 = float(sig.get("tp1", 0) or 0)
        tp2 = float(sig.get("tp2", 0) or 0)
        tp3 = float(sig.get("tp3", 0) or 0)
        sl, tp1, tp2, tp3 = smart_auto_levels(sym, action, entry, sl, tp1, tp2, tp3)

        active_tps = [v for v in (tp1, tp2, tp3) if v > 0]
        tickets: list[int] = []
        lot = cfg["lot_size"]
        is_limit = bool(sig.get("is_limit", False))

        # Faster split execution with less sleeping than v35.
        if len(active_tps) <= 1:
            t = send_order(cfg, sym, action, lot, sl, active_tps[0] if active_tps else 0.0, entry, is_limit, "MSTR_v36")
            if t:
                tickets.append(t)
        else:
            split = max(round(lot / len(active_tps), 2), 0.01)
            for tp in active_tps:
                t = send_order(cfg, sym, action, split, sl, tp, entry, is_limit, "MSTR_v36")
                if t:
                    tickets.append(t)
                await asyncio.sleep(0.1)

        if not tickets:
            return

        bridge = load_log(cfg["json_file"])
        key = str(uuid.uuid4())
        bridge[key] = {
            "symbol": sym,
            "action": action,
            "entry": entry,   # final executable entry
            "sl": sl,         # final SL after smart/broker checks
            "tps": active_tps,
            "is_limit": is_limit,
            "lot": lot,
            "tickets": tickets,
            "copied_by": {},  # per-slave tracking
            "timestamp": datetime.utcnow().isoformat(),
        }
        save_log(cfg["json_file"], bridge)
        _new_trade_event.set()
        log.info("[%s] Saved trade %s for slaves", master_name, key)


async def slave_copy_loop() -> None:
    log.info("🔁 Slave loop running | poll %.1fs", POLL_SECONDS)
    while True:
        for slave_name, scfg in SLAVE_ACCOUNTS.items():
            if not scfg.get("enabled"):
                continue
            slave_account = int(scfg.get("account", 0))
            if slave_account <= 0:
                continue

            # Find source master.
            source_master = None
            source_name = ""
            for mn, mc in MASTER_ACCOUNTS.items():
                if int(mc.get("account", 0)) == int(scfg.get("copy_from_master", 0)):
                    source_master = mc
                    source_name = mn
                    break
            if not source_master:
                continue

            bridge = load_log(source_master["json_file"])
            changed = False
            for key, trade in bridge.items():
                copied_by = trade.setdefault("copied_by", {})
                if copied_by.get(str(slave_account)):
                    continue

                async with _mt5_lock:
                    if not await _ensure_mt5_account(scfg):
                        break
                    sym = resolve_symbol(trade["symbol"], scfg["account"])
                    if not sym:
                        copied_by[str(slave_account)] = {"status": "symbol_not_found", "ts": datetime.utcnow().isoformat()}
                        changed = True
                        continue

                    lot = normalize_volume(
                        sym,
                        float(trade["lot"]) * float(scfg["lot_multiplier"]),
                        float(scfg["max_lot"]),
                    )
                    tps = [float(v) for v in trade.get("tps", []) if float(v) > 0]
                    action = int(trade["action"])
                    entry = float(trade.get("entry", 0))
                    sl = float(trade.get("sl", 0))
                    is_limit = bool(trade.get("is_limit", False))

                    slave_tickets = []
                    if len(tps) <= 1:
                        t = send_order(
                            scfg,
                            sym,
                            action,
                            lot,
                            sl,
                            tps[0] if tps else 0.0,
                            entry,
                            is_limit,
                            comment=f"SLV_{source_name}",
                        )
                        if t:
                            slave_tickets.append(t)
                    else:
                        split = max(round(lot / len(tps), 2), 0.01)
                        for tp in tps:
                            t = send_order(
                                scfg,
                                sym,
                                action,
                                split,
                                sl,
                                tp,
                                entry,
                                is_limit,
                                comment=f"SLV_{source_name}",
                            )
                            if t:
                                slave_tickets.append(t)
                            await asyncio.sleep(0.1)

                    copied_by[str(slave_account)] = {
                        "status": "ok" if slave_tickets else "failed",
                        "tickets": slave_tickets,
                        "ts": datetime.utcnow().isoformat(),
                    }
                    changed = True
                    log.info("[%s] copied %s from %s", slave_name, key, source_name)

            if changed:
                save_log(source_master["json_file"], bridge)

        # Event-based wakeup + polling fallback.
        try:
            await asyncio.wait_for(_new_trade_event.wait(), timeout=POLL_SECONDS)
            _new_trade_event.clear()
        except asyncio.TimeoutError:
            pass


async def mt5_watchdog() -> None:
    await asyncio.sleep(20)
    while True:
        async with _mt5_lock:
            global _mt5_active_account
            if _mt5_active_account is not None and mt5.account_info() is None:
                log.warning("🔌 MT5 disconnected; resetting session")
                mt5.shutdown()
                _mt5_active_account = None
        await asyncio.sleep(30)


# ---------------------------------------------------------------------------
# Telegram routing
# ---------------------------------------------------------------------------
CHANNEL_TO_MASTERS: dict[int, list] = {}
client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)


def build_channel_map() -> None:
    CHANNEL_TO_MASTERS.clear()
    for name, cfg in MASTER_ACCOUNTS.items():
        if not cfg.get("enabled"):
            continue
        for ch in cfg.get("channels", []):
            CHANNEL_TO_MASTERS.setdefault(int(ch), []).append((name, cfg))


@client.on(events.NewMessage())
async def handler(event) -> None:
    text = event.raw_text or ""
    if not text.strip():
        return

    cid = int(event.chat_id or 0)
    masters = CHANNEL_TO_MASTERS.get(cid, [])
    if not masters:
        # Try fallback form -100{peer_id} if config uses raw id.
        if cid < 0 and str(abs(cid)).startswith("100"):
            reduced = -int(str(abs(cid))[3:])
            masters = CHANNEL_TO_MASTERS.get(reduced, [])
    if not masters:
        return

    sig_hash = f"{cid}:{text[:200]}"
    if sig_hash in _seen_signals:
        return
    _seen_signals[sig_hash] = time.time()
    _cleanup_seen_signals()

    sig = await parse_signal(text)
    if not sig:
        return

    for master_name, master_cfg in masters:
        await execute_on_master(master_name, master_cfg, sig)


async def main() -> None:
    log.info("═" * 65)
    log.info("ARCHITECT v36.0 | Fast path + Smart SL/TP")
    log.info("═" * 65)

    if not all([TG_SESSION, TG_API_ID, TG_API_HASH]):
        log.error("Missing Telegram env keys: TG_SESSION, TG_API_ID, TG_API_HASH")
        return

    # Filter out incomplete account stubs so runtime stays safe.
    invalid_masters = [
        name
        for name, cfg in MASTER_ACCOUNTS.items()
        if cfg.get("enabled")
        and (not cfg.get("account") or not cfg.get("password") or not cfg.get("server"))
    ]
    for name in invalid_masters:
        log.warning("Disabling incomplete master config: %s", name)
        MASTER_ACCOUNTS[name]["enabled"] = False

    for cfg in MASTER_ACCOUNTS.values():
        if cfg.get("enabled"):
            load_log(cfg["json_file"])

    build_channel_map()
    asyncio.create_task(slave_copy_loop())
    asyncio.create_task(mt5_watchdog())

    while True:
        try:
            await client.start()
            log.info(
                "Telegram connected | masters:%s slaves:%s channels:%s",
                sum(1 for c in MASTER_ACCOUNTS.values() if c.get("enabled")),
                sum(1 for c in SLAVE_ACCOUNTS.values() if c.get("enabled")),
                len(CHANNEL_TO_MASTERS),
            )
            await client.run_until_disconnected()
            log.warning("Telegram disconnected; reconnecting...")
        except Exception as e:
            log.error("Telegram loop error: %s", e)
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
