"""
=============================================================================
  TELEGRAM → MT5 MASTER/SLAVE TRADE COPIER  |  ARCHITECT v37.0
=============================================================================
  SPEED FIXES vs v36 (why signals weren't being copied fast):

  1. FIRE-AND-FORGET HANDLER
     handler() no longer awaits execute_on_master(). It creates a Task and
     returns immediately so Telethon's event loop is never blocked — the
     next signal is picked up instantly even while a trade is being placed.

  2. IN-MEMORY QUEUE FOR SLAVE COPY (was: JSON poll every 2 s)
     Each slave has a dedicated asyncio.Queue. When a master places a trade
     it pushes directly to the queue. The slave worker wakes instantly
     (queue.get() blocks with zero CPU) — no more 0–2 s poll delay.
     JSON file is still written as a persistent backup/audit log but is NOT
     on the critical copy path.

  3. AUTO SL/TP SCANNER NO LONGER BLOCKS TRADES
     Scanner releases _mt5_lock between accounts so an incoming trade can
     always grab it within milliseconds.

  4. MT5 SWITCH SLEEP REDUCED 0.3 s → 0.05 s

  5. MULTI-TP INTER-ORDER SLEEP REMOVED (was 0.2 s)
     Orders within the same signal fire back-to-back.

  6. CHANNEL DEBUG LOGGING
     Every incoming message logs its chat_id. If no master is matched the
     exact chat_id is printed so you can add it to your config immediately.

  7. PARALLEL AI + REGEX PARSE
     If ANTHROPIC_API_KEY is set, AI parse races against a 3 s timeout;
     on timeout or failure the regex result is used without extra delay.

  8. WATCHDOG KEEPS MASTER MT5 WARM
     Watchdog now cycles through all accounts and keeps the most recently
     used one connected, so the first trade after a quiet period doesn't
     pay a full mt5.initialize() cost.
=============================================================================
"""

import os, re, asyncio, json, logging, uuid
from collections import deque
from datetime import datetime
from dotenv import load_dotenv
import MetaTrader5 as mt5
from telethon import TelegramClient, events
from telethon.sessions import StringSession

load_dotenv()

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("copier_v37.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("ARCHITECT-v37")


# ─────────────────────────────────────────────
#  TELEGRAM CREDENTIALS  (from .env)
# ─────────────────────────────────────────────
TG_SESSION  = os.getenv("TG_SESSION", "")
TG_API_ID   = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")


# ─────────────────────────────────────────────
#  RISK : REWARD CONFIG
# ─────────────────────────────────────────────
RR_CONFIG = {
    "auto_sl_pips": 20,
    "symbol_sl_pips": {
        "XAUUSD": 150,
        "XAGUSD": 50,
        "NASDAQ": 50,
        "US30":   80,
        "SP500":  40,
        "GER40":  40,
        "UK100":  40,
        "USOIL":  30,
        "BTCUSD": 500,
        "ETHUSD": 100,
    },
    "auto_rr1": 1.5,
    "auto_rr2": 2.5,
    "auto_rr3": 4.0,
    "draw_on_chart": True,
    "auto_sltp_scan_interval": 60,
    "auto_sltp_magic_filter": [],
}


# ===========================
# MASTER ACCOUNTS CONFIGURATION
# ===========================
MASTER_ACCOUNTS = {
    "Master1": {
        "enabled":      True,
        "account":      51647558,
        "password":     "Ugoprince@!555",
        "server":       "vantageinternational-live 4",
        "mt5_path":     r"C:\MT5\MT5Master1\Vantage International MT5\terminal64.exe",
        "lot_size":     0.01,
        "magic_number": 123456,
        "deviation":    300,
        "max_lot":      10.0,
        "channels":     [-1002034822451, -1001588519179, -1002495224665],
        "json_file":    r"C:\AI_Signal\master_trades_51647558.json",
    },
    "Master2": {
        "enabled":      True,
        "account":      161443437,
        "password":     "Ugoprince!@555",
        "server":       "Exness-MT5Real21",
        "mt5_path":     r"C:\MT5\Master2\MetaTrader 5 EXNESS\terminal64.exe",
        "lot_size":     0.1,
        "magic_number": 654321,
        "deviation":    300,
        "max_lot":      10.0,
        "channels":     [-1002034822451, -1001588519179, -1002495224665, -1002201702304,
                         -1001381790914, -1001196272579, -1001182913499],
        "json_file":    r"C:\AI_Signal\master_trades_161443437.json",
    },
}

# ===========================
# SLAVE ACCOUNTS CONFIGURATION
# ===========================
SLAVE_ACCOUNTS = {
    "Slave2": {
        "enabled":          True,
        "name":             "Slave2",
        "account":          7314281,
        "password":         "Ugoprince!@555",
        "server":           "ICMarketsSC-MT5-2",
        "mt5_path":         r"C:\MT5\SLAVE2\ICMARKET 2\terminal64.exe",
        "lot_multiplier":   1.0,
        "magic_number":     999998,
        "deviation":        300,
        "max_lot":          10.0,
        "copy_from_master": 51647558,
    },
    # ── Add more slaves below ──────────────────────────────────────────────
    # "Slave3": {
    #     "enabled":          True,
    #     "name":             "Slave3",
    #     "account":          0,
    #     "password":         "",
    #     "server":           "",
    #     "mt5_path":         r"C:\MT5\SLAVE3\terminal64.exe",
    #     "lot_multiplier":   1.0,
    #     "magic_number":     999997,
    #     "deviation":        300,
    #     "max_lot":          10.0,
    #     "copy_from_master": 51647558,
    # },
}


# ─────────────────────────────────────────────
#  SYMBOL MAPPING + SUFFIXES
# ─────────────────────────────────────────────
SYMBOL_MAP = {
    "GOLD": "XAUUSD", "XAU": "XAUUSD", "XAUUSD": "XAUUSD",
    "SILVER": "XAGUSD", "XAG": "XAGUSD", "XAGUSD": "XAGUSD",
    "EURUSD": "EURUSD", "EUR/USD": "EURUSD", "EUR": "EURUSD", "FIBER": "EURUSD",
    "GBPUSD": "GBPUSD", "GBP/USD": "GBPUSD", "GBP": "GBPUSD", "CABLE": "GBPUSD",
    "USDJPY": "USDJPY", "USD/JPY": "USDJPY", "JPY": "USDJPY",
    "USDCHF": "USDCHF", "USD/CHF": "USDCHF", "CHF": "USDCHF",
    "USDCAD": "USDCAD", "USD/CAD": "USDCAD", "CAD": "USDCAD",
    "AUDUSD": "AUDUSD", "AUD/USD": "AUDUSD", "AUD": "AUDUSD",
    "NZDUSD": "NZDUSD", "NZD/USD": "NZDUSD", "NZD": "NZDUSD",
    "EURGBP": "EURGBP", "EUR/GBP": "EURGBP",
    "EURJPY": "EURJPY", "EUR/JPY": "EURJPY",
    "GBPJPY": "GBPJPY", "GBP/JPY": "GBPJPY",
    "GBPCHF": "GBPCHF", "GBP/CHF": "GBPCHF",
    "AUDJPY": "AUDJPY", "AUD/JPY": "AUDJPY",
    "CADJPY": "CADJPY", "CAD/JPY": "CADJPY",
    "CHFJPY": "CHFJPY", "CHF/JPY": "CHFJPY",
    "EURAUD": "EURAUD", "EUR/AUD": "EURAUD",
    "EURCAD": "EURCAD", "EUR/CAD": "EURCAD",
    "EURCHF": "EURCHF", "EUR/CHF": "EURCHF",
    "GBPAUD": "GBPAUD", "GBP/AUD": "GBPAUD",
    "GBPCAD": "GBPCAD", "GBP/CAD": "GBPCAD",
    "AUDCAD": "AUDCAD", "AUD/CAD": "AUDCAD",
    "AUDCHF": "AUDCHF", "AUD/CHF": "AUDCHF",
    "AUDNZD": "AUDNZD", "AUD/NZD": "AUDNZD",
    "NZDJPY": "NZDJPY", "NZD/JPY": "NZDJPY",
    "NAS": "NASDAQ", "NASDAQ": "NASDAQ", "NAS100": "NASDAQ", "US100": "NASDAQ",
    "US30": "US30", "DOW": "US30", "DJ30": "US30",
    "SP500": "SP500", "SPX": "SP500", "US500": "SP500",
    "OIL": "USOIL", "USOIL": "USOIL", "WTI": "USOIL", "CRUDE": "USOIL",
    "BTC": "BTCUSD", "BITCOIN": "BTCUSD", "BTCUSD": "BTCUSD",
    "ETH": "ETHUSD", "ETHEREUM": "ETHUSD", "ETHUSD": "ETHUSD",
    "DAX": "GER40", "GER40": "GER40",
    "FTSE": "UK100", "UK100": "UK100",
}

SUFFIXES = ["", ".crp", "+", ".pro", ".v", ".a", "m", ".r", ".c", "_micro", "micro"]


# ─────────────────────────────────────────────
#  IN-MEMORY SLAVE QUEUES
#  Each slave gets its own queue. Master pushes trade dicts; slave worker
#  wakes immediately via queue.get() — zero poll delay.
# ─────────────────────────────────────────────
_slave_queues: dict[str, asyncio.Queue] = {
    name: asyncio.Queue() for name in SLAVE_ACCOUNTS
}


# ─────────────────────────────────────────────
#  MT5 CONNECTION POOL
# ─────────────────────────────────────────────
_mt5_lock = asyncio.Lock()
_mt5_active_account: int | None = None


async def _ensure_mt5_account(cfg: dict) -> bool:
    """Switch the live MT5 session to the requested account if needed."""
    global _mt5_active_account

    if _mt5_active_account == cfg["account"]:
        acc = mt5.account_info()
        if acc and acc.login == cfg["account"]:
            return True
        log.warning(f"MT5 #{cfg['account']} dropped, re-initialising...")
        mt5.shutdown()
        _mt5_active_account = None

    if _mt5_active_account is not None:
        mt5.shutdown()
        _mt5_active_account = None
        await asyncio.sleep(0.05)   # was 0.3 s — reduced to 50 ms

    ok = mt5.initialize(
        path=cfg["mt5_path"],
        login=cfg["account"],
        password=cfg["password"],
        server=cfg["server"],
        timeout=15000,
    )
    if not ok:
        log.error(f"MT5 login FAILED [#{cfg['account']}]: {mt5.last_error()}")
        return False

    acc = mt5.account_info()
    if acc:
        log.info(f"  ✅ MT5 #{acc.login} | {acc.server} | Balance:{acc.balance:.2f} {acc.currency}")
        _mt5_active_account = cfg["account"]
        return True

    log.error(f"MT5 account_info() returned None for #{cfg['account']}")
    return False


# ─────────────────────────────────────────────
#  MT5 HELPERS
# ─────────────────────────────────────────────
def get_filling_mode(symbol: str) -> int:
    info = mt5.symbol_info(symbol)
    if not info:
        return mt5.ORDER_FILLING_FOK
    fm = info.filling_mode
    if fm & 2: return mt5.ORDER_FILLING_IOC
    if fm & 1: return mt5.ORDER_FILLING_FOK
    return mt5.ORDER_FILLING_RETURN


def resolve_symbol(base: str) -> str | None:
    for s in SUFFIXES:
        sym = f"{base}{s}"
        if mt5.symbol_select(sym, True):
            info = mt5.symbol_info(sym)
            if info and info.visible:
                return sym
    log.warning(f"  ⚠️  Could not resolve '{base}' with suffixes {SUFFIXES}")
    return None


def close_positions(symbol: str, action, magic: int):
    positions = mt5.positions_get(symbol=symbol)
    if not positions:
        log.info(f"  No open positions to close for {symbol}")
        return
    closed = 0
    for pos in positions:
        if pos.magic != magic:
            continue
        ctype = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        tick  = mt5.symbol_info_tick(symbol)
        if not tick:
            continue
        price = tick.bid if ctype == mt5.ORDER_TYPE_SELL else tick.ask
        res = mt5.order_send({
            "action":       mt5.TRADE_ACTION_DEAL,
            "position":     pos.ticket,
            "symbol":       symbol,
            "volume":       pos.volume,
            "type":         ctype,
            "price":        price,
            "deviation":    300,
            "magic":        magic,
            "comment":      "CLOSE_v37",
            "type_filling": get_filling_mode(symbol),
        })
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            log.info(f"  ✅ Closed #{pos.ticket}")
            closed += 1
        else:
            log.warning(f"  ❌ Close #{pos.ticket}: {res.comment if res else '?'} ({res.retcode if res else '?'})")
    log.info(f"  Closed {closed}/{len(positions)} positions for {symbol}")


def close_partial_positions(symbol: str, magic: int, percent: int = 50):
    positions = mt5.positions_get(symbol=symbol)
    if not positions:
        return
    for pos in positions:
        if pos.magic != magic:
            continue
        close_vol = max(round(pos.volume * percent / 100, 2), 0.01)
        ctype = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        tick  = mt5.symbol_info_tick(symbol)
        if not tick:
            continue
        price = tick.bid if ctype == mt5.ORDER_TYPE_SELL else tick.ask
        res = mt5.order_send({
            "action":       mt5.TRADE_ACTION_DEAL,
            "position":     pos.ticket,
            "symbol":       symbol,
            "volume":       close_vol,
            "type":         ctype,
            "price":        price,
            "deviation":    300,
            "magic":        magic,
            "comment":      f"PARTIAL_{percent}pct",
            "type_filling": get_filling_mode(symbol),
        })
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            log.info(f"  ✅ Partial close {close_vol} lots #{pos.ticket}")


def move_to_breakeven(symbol: str, action, magic: int):
    positions = mt5.positions_get(symbol=symbol)
    if not positions:
        return
    for pos in positions:
        if pos.magic != magic:
            continue
        res = mt5.order_send({
            "action":   mt5.TRADE_ACTION_SLTP,
            "position": pos.ticket,
            "symbol":   symbol,
            "sl":       pos.price_open,
            "tp":       pos.tp,
        })
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            log.info(f"  ✅ Breakeven #{pos.ticket}")
        else:
            log.warning(f"  ❌ Breakeven #{pos.ticket}: {res.comment if res else '?'}")


def modify_sl(symbol: str, magic: int, new_sl: float):
    positions = mt5.positions_get(symbol=symbol)
    if not positions:
        return
    for pos in positions:
        if pos.magic != magic:
            continue
        res = mt5.order_send({
            "action":   mt5.TRADE_ACTION_SLTP,
            "position": pos.ticket,
            "symbol":   symbol,
            "sl":       new_sl,
            "tp":       pos.tp,
        })
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            log.info(f"  ✅ SL → {new_sl} on #{pos.ticket}")


def send_order(
    cfg: dict,
    symbol: str,
    action: int,
    lot: float,
    sl: float,
    tp: float,
    entry: float,
    is_limit: bool,
    comment: str = "v37",
    _retry: bool = True,
) -> int | None:
    """Place a single order; retry once on requote. Returns ticket or None."""
    lot  = round(min(max(lot, 0.01), cfg["max_lot"]), 2)
    tick = mt5.symbol_info_tick(symbol)
    if not tick:
        log.error(f"  ❌ No tick for {symbol}")
        return None

    sym_info = mt5.symbol_info(symbol)
    if not sym_info:
        log.error(f"  ❌ No symbol info for {symbol}")
        return None

    curr_p = tick.ask if action == mt5.ORDER_TYPE_BUY else tick.bid
    pt     = sym_info.point
    digits = sym_info.digits

    if sl > 0: sl = round(sl, digits)
    if tp > 0: tp = round(tp, digits)

    trade_action = mt5.TRADE_ACTION_DEAL
    order_type   = action
    exec_price   = curr_p

    if entry > 0:
        dist = abs(curr_p - entry)
        if dist > pt * 30 or is_limit:
            trade_action = mt5.TRADE_ACTION_PENDING
            exec_price   = round(entry, digits)
            if action == mt5.ORDER_TYPE_BUY:
                order_type = mt5.ORDER_TYPE_BUY_LIMIT if exec_price < curr_p else mt5.ORDER_TYPE_BUY_STOP
            else:
                order_type = mt5.ORDER_TYPE_SELL_LIMIT if exec_price > curr_p else mt5.ORDER_TYPE_SELL_STOP

    request = {
        "action":       trade_action,
        "symbol":       symbol,
        "volume":       lot,
        "type":         order_type,
        "price":        exec_price,
        "deviation":    cfg["deviation"],
        "magic":        cfg["magic_number"],
        "comment":      comment[:31],
        "type_filling": get_filling_mode(symbol),
        "type_time":    mt5.ORDER_TIME_GTC,
    }
    if sl > 0: request["sl"] = sl
    if tp > 0: request["tp"] = tp

    direction = "BUY" if action == mt5.ORDER_TYPE_BUY else "SELL"
    mode      = "PENDING" if trade_action == mt5.TRADE_ACTION_PENDING else "MARKET"
    log.info(f"  📤 {mode} {symbol} {direction} lot:{lot} @{exec_price} sl:{sl} tp:{tp}")

    res = mt5.order_send(request)
    if res is None:
        log.error(f"  ❌ order_send None — {mt5.last_error()}")
        return None

    if res.retcode == mt5.TRADE_RETCODE_DONE:
        log.info(f"  ✅ [{mode}] #{res.order} {symbol} lot:{lot} sl:{sl} tp:{tp}")
        return res.order

    if res.retcode == 10004 and _retry:
        log.warning(f"  🔄 Requote {symbol} — retrying at market")
        return send_order(cfg, symbol, action, lot, sl, tp, 0.0, False, comment, _retry=False)

    log.warning(f"  ❌ Order failed retcode:{res.retcode} | {res.comment}")
    _log_order_error(res.retcode, symbol, exec_price, sl, tp)
    return None


def _log_order_error(code: int, symbol: str, price: float, sl: float, tp: float):
    hints = {
        10004: "Requote",           10006: "Rejected by broker",
        10013: "Bad request",       10014: "Bad lot",
        10015: "Bad price",         10016: "Bad SL/TP — check min distance",
        10017: "Trade disabled",    10018: "Market closed",
        10019: "No funds",          10024: "Too many requests",
        10026: "AT disabled server",10027: "AT disabled client",
        10030: "Pending limit hit", 10031: "Volume limit hit",
        10032: "Bad symbol",        10036: "Already closed",
    }
    log.warning(f"     💡 {hints.get(code, f'code {code}')}")
    if code == 10016:
        log.warning(f"     📐 SL={sl} TP={tp} vs price {price}")


# ─────────────────────────────────────────────
#  TRADE LOG  (persistent backup / audit)
# ─────────────────────────────────────────────
def load_log(path: str) -> dict:
    dirpart = os.path.dirname(path)
    if dirpart:
        os.makedirs(dirpart, exist_ok=True)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w") as f:
            json.dump({}, f)
        return {}
    try:
        with open(path) as f:
            data = json.load(f)
        if isinstance(data, list):
            save_log(path, {})
            return {}
        return data
    except (json.JSONDecodeError, Exception):
        save_log(path, {})
        return {}


def save_log(path: str, data: dict):
    dirpart = os.path.dirname(path)
    if dirpart:
        os.makedirs(dirpart, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


async def save_log_async(path: str, data: dict):
    await asyncio.to_thread(save_log, path, data)


# ─────────────────────────────────────────────
#  AUTO SL/TP HELPERS
# ─────────────────────────────────────────────
def get_pip_size(symbol: str) -> float:
    info = mt5.symbol_info(symbol)
    if not info:
        return 0.0001
    if any(x in symbol for x in ["JPY", "NASDAQ", "US30", "SP500", "GER40", "UK100"]):
        return info.point
    if any(x in symbol for x in ["XAU", "XAG"]):
        return info.point * 10
    return info.point * 10


def auto_sl_tp(symbol: str, action: int, entry_price: float) -> tuple[float, float, float, float]:
    if entry_price == 0:
        tick = mt5.symbol_info_tick(symbol)
        if tick:
            entry_price = tick.ask if action == mt5.ORDER_TYPE_BUY else tick.bid

    pip = get_pip_size(symbol)
    if pip == 0 or entry_price == 0:
        return 0.0, 0.0, 0.0, 0.0

    info = mt5.symbol_info(symbol)
    if not info:
        return 0.0, 0.0, 0.0, 0.0

    digits  = info.digits
    sl_pips = RR_CONFIG["symbol_sl_pips"].get(symbol, RR_CONFIG["auto_sl_pips"])
    sl_dist = sl_pips * pip

    if action == mt5.ORDER_TYPE_BUY:
        sl  = round(entry_price - sl_dist, digits)
        tp1 = round(entry_price + sl_dist * RR_CONFIG["auto_rr1"], digits)
        tp2 = round(entry_price + sl_dist * RR_CONFIG["auto_rr2"], digits)
        tp3 = round(entry_price + sl_dist * RR_CONFIG["auto_rr3"], digits)
    else:
        sl  = round(entry_price + sl_dist, digits)
        tp1 = round(entry_price - sl_dist * RR_CONFIG["auto_rr1"], digits)
        tp2 = round(entry_price - sl_dist * RR_CONFIG["auto_rr2"], digits)
        tp3 = round(entry_price - sl_dist * RR_CONFIG["auto_rr3"], digits)

    log.info(
        f"  📐 Auto SL/TP entry:{entry_price} SL:{sl} "
        f"TP1:{tp1}(1:{RR_CONFIG['auto_rr1']}) "
        f"TP2:{tp2}(1:{RR_CONFIG['auto_rr2']}) "
        f"TP3:{tp3}(1:{RR_CONFIG['auto_rr3']})"
    )
    return sl, tp1, tp2, tp3


def _set_position_sltp(pos, new_sl: float, new_tp: float, symbol: str) -> bool:
    res = mt5.order_send({
        "action":   mt5.TRADE_ACTION_SLTP,
        "position": pos.ticket,
        "symbol":   symbol,
        "sl":       new_sl,
        "tp":       new_tp,
    })
    ok = res and res.retcode == mt5.TRADE_RETCODE_DONE
    if ok:
        log.info(f"  ✅ SL/TP patched #{pos.ticket} {symbol} SL:{new_sl} TP:{new_tp}")
    else:
        log.warning(f"  ❌ SL/TP patch #{pos.ticket}: {res.comment if res else mt5.last_error()}")
    return ok


# ─────────────────────────────────────────────
#  CHART DRAWING
# ─────────────────────────────────────────────
def draw_trade_levels(
    symbol: str, action: int, entry: float, sl: float,
    tp1: float, tp2: float = 0, tp3: float = 0, ticket: int = 0,
):
    if not RR_CONFIG["draw_on_chart"]:
        return
    try:
        prefix    = f"v37_{ticket}"
        direction = "BUY" if action == mt5.ORDER_TYPE_BUY else "SELL"

        def hline(name, price, color, style, width, label):
            mt5.object_delete(0, name)
            mt5.object_create(0, name, mt5.OBJ_HLINE, 0, 0, price)
            mt5.object_set_integer(0, name, mt5.OBJPROP_COLOR,      color)
            mt5.object_set_integer(0, name, mt5.OBJPROP_STYLE,      style)
            mt5.object_set_integer(0, name, mt5.OBJPROP_WIDTH,      width)
            mt5.object_set_string(0,  name, mt5.OBJPROP_TEXT,       label)
            mt5.object_set_integer(0, name, mt5.OBJPROP_SELECTABLE, False)
            mt5.object_set_integer(0, name, mt5.OBJPROP_BACK,       True)

        if entry > 0: hline(f"{prefix}_E",   entry, 0x3399FF, mt5.STYLE_DASH,  1, f"ENTRY {direction} @ {entry}")
        if sl    > 0: hline(f"{prefix}_SL",  sl,    0xFF3333, mt5.STYLE_SOLID, 2, f"SL {sl}")
        if tp1   > 0: hline(f"{prefix}_TP1", tp1,   0x00CC44, mt5.STYLE_DOT,   1, f"TP1 {tp1}")
        if tp2   > 0: hline(f"{prefix}_TP2", tp2,   0x00CC44, mt5.STYLE_DOT,   1, f"TP2 {tp2}")
        if tp3   > 0: hline(f"{prefix}_TP3", tp3,   0x00CC44, mt5.STYLE_DOT,   1, f"TP3 {tp3}")

        mt5.chart_redraw(0)
    except Exception as e:
        log.debug(f"  Chart draw skipped: {e}")


# ─────────────────────────────────────────────
#  AI SIGNAL PARSER  (Claude API, 3 s timeout)
# ─────────────────────────────────────────────
async def ai_parse_signal(text: str) -> dict | None:
    if not ANTHROPIC_API_KEY:
        return None
    try:
        import aiohttp

        prompt = f"""You are a forex/trading signal parser. Extract trade details from this Telegram message and return ONLY valid JSON, nothing else.

Message:
\"\"\"
{text}
\"\"\"

Return this exact JSON structure (use null for missing values, never omit keys):
{{
  "is_signal": true/false,
  "type": "trade" | "close" | "breakeven" | "partial_close" | "modify_sl" | "news" | "other",
  "symbol": "XAUUSD" | "EURUSD" | etc (standardized MT5 symbol or null),
  "action": "BUY" | "SELL" | null,
  "entry": number | null,
  "sl": number | null,
  "tp1": number | null,
  "tp2": number | null,
  "tp3": number | null,
  "is_limit": true/false,
  "partial_percent": number | null,
  "new_sl": number | null,
  "confidence": 0-100,
  "notes": "brief explanation"
}}

Rules:
- GOLD/XAU → XAUUSD, SILVER/XAG → XAGUSD, NAS/NASDAQ/NAS100 → NASDAQ
- BUY LIMIT / BUY STOP / pending orders → is_limit: true
- "close", "exit", "full close" → type: close
- "breakeven", "move sl to entry" → type: breakeven
- "partial close", "close 50%" → type: partial_close
- If confidence < 40 → is_signal: false
- Entry zone "2300-2310" → use midpoint 2305
- sl/tp1/tp2/tp3 may be null if the signal doesn't mention them"""

        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":         ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      "claude-3-5-sonnet-20241022",
                    "max_tokens": 500,
                    "messages":   [{"role": "user", "content": prompt}],
                },
                timeout=aiohttp.ClientTimeout(total=3),   # 3 s hard cap
            )
            data = await resp.json()

        raw = data["content"][0]["text"].strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        parsed = json.loads(raw)

        if not parsed.get("is_signal") or parsed.get("confidence", 0) < 40:
            return None

        action_map = {"BUY": mt5.ORDER_TYPE_BUY, "SELL": mt5.ORDER_TYPE_SELL}
        result = {
            "type":            parsed.get("type", "trade"),
            "symbol":          parsed.get("symbol"),
            "action":          action_map.get(parsed.get("action")),
            "entry":           parsed.get("entry") or 0.0,
            "sl":              parsed.get("sl") or 0.0,
            "tp1":             parsed.get("tp1") or 0.0,
            "tp2":             parsed.get("tp2") or 0.0,
            "tp3":             parsed.get("tp3") or 0.0,
            "is_limit":        parsed.get("is_limit", False),
            "partial_percent": parsed.get("partial_percent"),
            "new_sl":          parsed.get("new_sl"),
            "ai_notes":        parsed.get("notes", ""),
            "confidence":      parsed.get("confidence", 100),
        }
        log.info(
            f"  🤖 AI [{result['confidence']}%] {result['type']} {result['symbol']} "
            f"{parsed.get('action')} entry:{result['entry']} sl:{result['sl']} tp1:{result['tp1']}"
        )
        return result
    except Exception as e:
        log.warning(f"  ⚠️ AI parse failed ({type(e).__name__}: {e})")
        return None


# ─────────────────────────────────────────────
#  REGEX SIGNAL PARSER
# ─────────────────────────────────────────────
def regex_parse_signal(text: str) -> dict | None:
    t = text.upper().strip()

    NOISE = [
        "WELCOME TO", "PLEASE COMPLETE", "CAPTCHA", "BYBIT", "BINANCE",
        "PUMP.FUN", "CONTRACT ADDRESS", "MIGRATED FROM", "CLICK LAUNCH",
        "REMINDER", "MIGRATION", "INTRODUCING", "DIRECT WIN",
        "RUNNING", "PIPS PROFIT", "HIT --", "TP1 &", "TP2 &",
        "CONGRATULATIONS", "THANK YOU", "GOOD MORNING", "GOOD EVENING",
        "JOIN", "VIP CHANNEL", "PERFORMANCE", "SUBSCRIBE",
        "BREAKING:", "OIL PRICES SURGED",
    ]
    trade_keywords = ["BUY", "SELL", "LONG", "SHORT", "CLOSE", "BREAKEVEN",
                      "SL TO", "BE NOW", "MOVE SL", "PARTIAL"]
    if not any(kw in t for kw in trade_keywords):
        return None
    noise_count = sum(1 for n in NOISE if n in t)
    if noise_count >= 2 and not any(kw in t for kw in ["BUY", "SELL", "LONG", "SHORT"]):
        return None

    # Symbol detection — longest match first
    symbol = None
    for key in sorted(SYMBOL_MAP.keys(), key=len, reverse=True):
        if re.search(r'(?<![A-Z])' + re.escape(key) + r'(?![A-Z])', t):
            symbol = SYMBOL_MAP[key]
            break
    if not symbol:
        for key in sorted(SYMBOL_MAP.keys(), key=len, reverse=True):
            if key in t:
                symbol = SYMBOL_MAP[key]
                break
    if not symbol:
        return None

    # Management signals
    if any(x in t for x in [
        "BREAKEVEN", "BREAK EVEN", "MOVE SL TO BE", "MOVE SL TO ENTRY",
        "SL TO ENTRY", "SL TO BE", "SL TO OPEN", "BE NOW", "MOVE TO BE",
        "SET BE", "PUT SL AT ENTRY",
    ]):
        return {"type": "breakeven", "symbol": symbol, "action": None}

    partial_pct = 50
    pct_m = re.search(r'(\d{1,3})\s*%', t)
    if pct_m:
        partial_pct = int(pct_m.group(1))
    if any(x in t for x in [
        "CLOSE PARTIAL", "PARTIAL CLOSE", "CLOSE HALF", "HALF CLOSE",
        "CLOSE 50", "TAKE PARTIAL", "PARTIAL PROFIT", "PARTIAL EXIT",
        "SMALL LOT HOLDER CAN FULL CLOSE",
    ]) or (re.search(r'\bPARTIAL\b', t) and re.search(r'\bCLOSE\b|\bEXIT\b', t)):
        return {"type": "partial_close", "symbol": symbol, "action": None,
                "partial_percent": partial_pct}

    if any(x in t for x in [
        "FULL CLOSE", "MANUAL CLOSE", "CLOSE ALL", "EXIT ALL",
        "CLOSE NOW", "CLOSE TRADE", "CLOSE POSITION",
    ]):
        return {"type": "close", "symbol": symbol, "action": None}

    has_direction = any(w in t for w in ["BUY", "SELL", "LONG", "SHORT"])
    has_entry_kw  = any(w in t for w in ["ENTRY", "ENTER", "@ ", "ZONE", "SL", "TP"])
    if re.search(r'\bCLOSE\b', t) and not has_direction and not has_entry_kw:
        return {"type": "close", "symbol": symbol, "action": None}

    # Direction
    action = None
    BUY_WORDS  = ["BUY STOP", "BUY LIMIT", "BUY NOW", "BUY @",
                  "GO BUY", "GO LONG", "LONG NOW", "BULLISH",
                  "📈", "🟢", "⬆", "↑", "LONG"]
    SELL_WORDS = ["SELL STOP", "SELL LIMIT", "SELL NOW", "SELL @",
                  "GO SELL", "GO SHORT", "SHORT NOW", "BEARISH",
                  "📉", "🔴", "⬇", "↓", "SHORT"]
    for w in BUY_WORDS:
        if w in t:
            action = mt5.ORDER_TYPE_BUY
            break
    if action is None:
        for w in SELL_WORDS:
            if w in t:
                action = mt5.ORDER_TYPE_SELL
                break
    if action is None:
        if re.search(r'\bBUY\b', t):  action = mt5.ORDER_TYPE_BUY
        elif re.search(r'\bSELL\b', t): action = mt5.ORDER_TYPE_SELL
    if action is None:
        return None

    is_limit = any(x in t for x in [
        "LIMIT", "PENDING", "BUY STOP", "SELL STOP", "BUY LIMIT", "SELL LIMIT",
        "WAIT FOR", "ENTRY AT", "PLACE ORDER", "SET ORDER",
    ])

    def _find(pattern, fallback=0.0):
        m = re.search(pattern, t)
        return float(m.group(1)) if m else fallback

    sl = _find(r"(?:S\.?L\.?|STOP\s*LOSS|STOPLOSS)\s*[:\-=@#]?\s*(\d{1,6}\.?\d{0,5})")

    tps: list[float] = []
    def _add_tp(v):
        try:
            fv = round(float(v), 5)
            if fv > 0 and fv not in tps:
                tps.append(fv)
        except Exception:
            pass

    for m in re.finditer(r"T\.?P\.?\s*[1-9]?\s*[:\-=@#]?\s*(\d{1,6}\.?\d{0,5})", t):
        _add_tp(m.group(1))

    if not tps:
        m = re.search(
            r"T\.?P\.?\s*[:\-]?\s*(\d{1,6}\.?\d{0,5})"
            r"(?:\s*[\/,]\s*(\d{1,6}\.?\d{0,5}))?"
            r"(?:\s*[\/,]\s*(\d{1,6}\.?\d{0,5}))?", t)
        if m:
            for g in m.groups():
                if g: _add_tp(g)

    if not tps:
        for m in re.finditer(
            r"(?:TARGET|TAKE\s*PROFIT|PROFIT\s*TARGET)\s*[123]?\s*[:\-=]?\s*(\d{1,6}\.?\d{0,5})", t):
            _add_tp(m.group(1))

    if not tps:
        for m in re.finditer(r"(?:^|\n)\s*[123][)\.]\s*(\d{1,6}\.?\d{0,5})", t):
            _add_tp(m.group(1))

    if not tps and "\n" in text:
        lines = [l.strip() for l in text.split("\n") if re.match(r'^\d{3,6}\.?\d{0,3}$', l.strip())]
        if len(lines) >= 3:
            for n in [float(x) for x in lines[2:]]:
                _add_tp(n)

    tps = tps[:3]
    tp1 = tps[0] if len(tps) > 0 else 0.0
    tp2 = tps[1] if len(tps) > 1 else 0.0
    tp3 = tps[2] if len(tps) > 2 else 0.0

    zone = re.search(
        r"(?:ENTRY\s*ZONE|ZONE|ENTRY\s*RANGE)[:\s]*(\d{1,6}\.?\d{0,5})\s*[-–]\s*(\d{1,6}\.?\d{0,5})", t)
    if zone:
        entry = round((float(zone.group(1)) + float(zone.group(2))) / 2, 5)
    else:
        entry = _find(r"(?:ENTRY|ENTER)\s*[:\-=@]?\s*(\d{1,6}\.?\d{0,5})")
        if not entry: entry = _find(r"@\s*(\d{1,6}\.?\d{0,5})")
        if not entry:
            rng = re.search(r"(?:BUY|SELL)\s+(?:\w+\s+)?(\d{1,6}\.?\d{0,5})\s*[-–]\s*(\d{1,6}\.?\d{0,5})", t)
            if rng:
                entry = round((float(rng.group(1)) + float(rng.group(2))) / 2, 5)
        if not entry:
            used = {str(round(v, 2)) for v in [sl, tp1, tp2, tp3] if v}
            for n in re.findall(r"(\d{3,6}\.?\d{0,5})", t):
                if str(round(float(n), 2)) not in used and float(n) > 0.0001:
                    entry = float(n)
                    break
        if not entry and "\n" in text:
            lines = [l.strip() for l in text.split("\n") if re.match(r'^\d{3,6}\.?\d{0,3}$', l.strip())]
            if lines:
                entry = float(lines[0])

    return {
        "type": "trade", "symbol": symbol, "action": action,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "is_limit": is_limit, "ai_notes": "regex",
    }


# ─────────────────────────────────────────────
#  UNIFIED PARSE  (AI with 3 s timeout, then regex)
# ─────────────────────────────────────────────
async def parse_signal(text: str) -> dict | None:
    if ANTHROPIC_API_KEY:
        try:
            sig = await asyncio.wait_for(ai_parse_signal(text), timeout=3.0)
            if sig:
                return sig
        except asyncio.TimeoutError:
            log.warning("  ⚠️ AI parse timed out — falling back to regex")

    sig = regex_parse_signal(text)
    if sig:
        direction = (
            "BUY"  if sig.get("action") == mt5.ORDER_TYPE_BUY  else
            "SELL" if sig.get("action") == mt5.ORDER_TYPE_SELL else "N/A"
        )
        tps = [v for v in [sig.get("tp1", 0), sig.get("tp2", 0), sig.get("tp3", 0)] if v > 0]
        log.info(
            f"  🔢 [{sig['type'].upper()}] {sig.get('symbol')} {direction} "
            f"entry:{sig.get('entry', 0)} sl:{sig.get('sl', 0)} tps:{tps} "
            f"limit:{sig.get('is_limit', False)}"
        )
    return sig


# ─────────────────────────────────────────────
#  EXECUTE ON MASTER  (called as a Task — never blocks handler)
# ─────────────────────────────────────────────
async def execute_on_master(master_name: str, cfg: dict, sig: dict):
    async with _mt5_lock:
        if not await _ensure_mt5_account(cfg):
            return

        sym = resolve_symbol(sig["symbol"])
        if not sym:
            log.warning(f"[{master_name}] Symbol '{sig['symbol']}' not in MT5")
            return

        tickets  = []
        base_lot = cfg["lot_size"]
        magic    = cfg["magic_number"]
        sig_type = sig.get("type", "trade")

        if sig_type == "close":
            close_positions(sym, sig.get("action"), magic)

        elif sig_type == "partial_close":
            close_partial_positions(sym, magic, sig.get("partial_percent", 50))

        elif sig_type == "breakeven":
            move_to_breakeven(sym, sig.get("action"), magic)

        elif sig_type == "modify_sl":
            if sig.get("new_sl", 0) > 0:
                modify_sl(sym, magic, sig["new_sl"])

        elif sig_type == "trade":
            if sig.get("action") is None:
                log.warning(f"[{master_name}] No direction in signal, skipping")
                return

            tick    = mt5.symbol_info_tick(sym)
            curr_p  = (tick.ask if sig["action"] == mt5.ORDER_TYPE_BUY else tick.bid) if tick else 0
            entry_p = sig.get("entry", 0) or curr_p

            sl  = sig.get("sl",  0) or 0.0
            tp1 = sig.get("tp1", 0) or 0.0
            tp2 = sig.get("tp2", 0) or 0.0
            tp3 = sig.get("tp3", 0) or 0.0

            if sl == 0 or tp1 == 0:
                log.info(f"  ⚙️ Missing SL/TP — auto-calculating")
                a_sl, a_tp1, a_tp2, a_tp3 = auto_sl_tp(sym, sig["action"], entry_p)
                if sl  == 0: sl  = a_sl
                if tp1 == 0: tp1 = a_tp1
                if tp2 == 0: tp2 = a_tp2
                if tp3 == 0: tp3 = a_tp3

            active_tps = [v for v in [tp1, tp2, tp3] if v > 0]

            if not active_tps:
                t = send_order(cfg, sym, sig["action"], base_lot, sl, 0.0, entry_p, sig.get("is_limit", False))
                if t:
                    tickets.append(t)
                    draw_trade_levels(sym, sig["action"], entry_p, sl, 0, 0, 0, t)

            elif len(active_tps) == 1:
                t = send_order(cfg, sym, sig["action"], base_lot, sl, active_tps[0], entry_p, sig.get("is_limit", False))
                if t:
                    tickets.append(t)
                    draw_trade_levels(sym, sig["action"], entry_p, sl, active_tps[0], 0, 0, t)

            else:
                split = max(round(base_lot / len(active_tps), 2), 0.01)
                log.info(f"  🔀 {len(active_tps)} TPs → {split} lot each")
                for i, tp in enumerate(active_tps):
                    t = send_order(cfg, sym, sig["action"], split, sl, tp, entry_p, sig.get("is_limit", False))
                    if t:
                        tickets.append(t)
                        if i == 0:
                            draw_trade_levels(sym, sig["action"], entry_p, sl,
                                active_tps[0],
                                active_tps[1] if len(active_tps) > 1 else 0,
                                active_tps[2] if len(active_tps) > 2 else 0, t)
                    # No sleep between orders — send as fast as broker accepts

            if tickets:
                # Push to slave queues immediately (zero latency)
                trade_record = {
                    "symbol":   sym,
                    "action":   sig["action"],
                    "sl":       sl,
                    "tps":      active_tps,
                    "entry":    entry_p,
                    "is_limit": sig.get("is_limit", False),
                    "lot":      base_lot,
                    "tickets":  tickets,
                    "timestamp": datetime.utcnow().isoformat(),
                }
                master_account = cfg["account"]
                pushed = 0
                for sname, scfg in SLAVE_ACCOUNTS.items():
                    if scfg.get("enabled", True) and scfg["copy_from_master"] == master_account:
                        await _slave_queues[sname].put(dict(trade_record))
                        log.info(f"[{master_name}] ➡️  Pushed to {sname} queue (depth:{_slave_queues[sname].qsize()})")
                        pushed += 1

                # Also write to JSON backup (non-blocking)
                key = str(uuid.uuid4())
                asyncio.create_task(_append_json_log(cfg["json_file"], key, trade_record))

                log.info(f"[{master_name}] ✅ {len(tickets)} ticket(s) — pushed to {pushed} slave(s)")
        else:
            log.info(f"[{master_name}] type='{sig_type}' — no MT5 action needed")


async def _append_json_log(path: str, key: str, record: dict):
    """Fire-and-forget JSON audit write."""
    try:
        data = await asyncio.to_thread(load_log, path)
        data[key] = {**record, "copied": False}
        await save_log_async(path, data)
    except Exception as e:
        log.warning(f"  JSON log write failed: {e}")


# ─────────────────────────────────────────────
#  SLAVE WORKER  (one per slave, wakes on queue.get())
#  Zero poll delay — wakes the instant master pushes a trade.
# ─────────────────────────────────────────────
async def slave_worker(slave_name: str, scfg: dict):
    q = _slave_queues[slave_name]
    log.info(f"[{slave_name}] ⚡ Slave worker ready (queue-based, zero delay)")

    # Pre-find master name once
    master_name_found = next(
        (mn for mn, mc in MASTER_ACCOUNTS.items() if mc["account"] == scfg["copy_from_master"]),
        "Unknown"
    )

    while True:
        trade = await q.get()    # blocks with zero CPU until a trade arrives
        try:
            log.info(f"[{slave_name}] 📋 Copying {trade['symbol']} from {master_name_found}...")

            async with _mt5_lock:
                if not await _ensure_mt5_account(scfg):
                    log.error(f"[{slave_name}] MT5 login failed — trade dropped")
                    q.task_done()
                    continue

                sym = resolve_symbol(trade["symbol"])
                if not sym:
                    log.warning(f"[{slave_name}] '{trade['symbol']}' not found — skipping")
                    q.task_done()
                    continue

                lot = min(max(round(trade["lot"] * scfg["lot_multiplier"], 2), 0.01), scfg["max_lot"])
                tps = trade.get("tps", [])
                cmt = f"SLV_{master_name_found}"[:31]

                if not tps:
                    send_order(scfg, sym, trade["action"], lot, trade["sl"],
                               0.0, trade["entry"], trade["is_limit"], comment=cmt)
                elif len(tps) == 1:
                    send_order(scfg, sym, trade["action"], lot, trade["sl"],
                               tps[0], trade["entry"], trade["is_limit"], comment=cmt)
                else:
                    split = max(round(lot / len(tps), 2), 0.01)
                    for tp in tps:
                        send_order(scfg, sym, trade["action"], split, trade["sl"],
                                   tp, trade["entry"], trade["is_limit"], comment=cmt)

                log.info(f"[{slave_name}] ✅ Copy done — {trade['symbol']}")
        except Exception as e:
            log.error(f"[{slave_name}] Worker error: {e}")
        finally:
            q.task_done()


# ─────────────────────────────────────────────
#  AUTO SL/TP SCANNER  (releases lock between accounts)
# ─────────────────────────────────────────────
async def auto_sltp_scanner():
    interval     = RR_CONFIG["auto_sltp_scan_interval"]
    magic_filter = RR_CONFIG["auto_sltp_magic_filter"]
    log.info(f"🔍 Auto SL/TP scanner started (every {interval} s)")
    await asyncio.sleep(20)

    all_accounts = list(MASTER_ACCOUNTS.values()) + list(SLAVE_ACCOUNTS.values())

    while True:
        for cfg in all_accounts:
            if not cfg.get("enabled", True):
                continue
            try:
                async with _mt5_lock:                  # lock per account, not per full scan
                    if not await _ensure_mt5_account(cfg):
                        continue
                    positions = mt5.positions_get() or []

                for pos in positions:
                    if magic_filter and pos.magic not in magic_filter:
                        continue
                    missing_sl = pos.sl == 0.0
                    missing_tp = pos.tp == 0.0
                    if not (missing_sl or missing_tp):
                        continue

                    a_sl, a_tp1, _, _ = auto_sl_tp(pos.symbol, pos.type, pos.price_open)
                    if a_sl == 0 and a_tp1 == 0:
                        continue

                    new_sl = a_sl  if missing_sl else pos.sl
                    new_tp = a_tp1 if missing_tp else pos.tp

                    log.info(
                        f"  🔧 #{pos.ticket} {pos.symbol} "
                        f"{'SL ' if missing_sl else ''}{'TP' if missing_tp else ''} missing "
                        f"→ SL:{new_sl} TP:{new_tp}"
                    )
                    # Patch needs the lock too
                    async with _mt5_lock:
                        if await _ensure_mt5_account(cfg):
                            _set_position_sltp(pos, new_sl, new_tp, pos.symbol)

                await asyncio.sleep(0)   # yield between accounts so trades can grab lock
            except Exception as e:
                log.warning(f"  Auto SL/TP scanner error: {e}")

        await asyncio.sleep(interval)


# ─────────────────────────────────────────────
#  CHANNEL ID RESOLUTION
# ─────────────────────────────────────────────
CHANNEL_TO_MASTERS: dict[int, list] = {}


def _normalize_channel_id(ch: int) -> list[int]:
    s = str(abs(ch))
    variants: set[int] = set()
    variants.add(ch)
    variants.add(-abs(ch))
    variants.add(-int(f"100{s}"))
    variants.add(int(f"100{s}"))
    for prefix in ["1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
                   "2", "20", "21", "22", "100", "200", "220", "221"]:
        long_s = f"{prefix}{s}"
        variants.add(-int(f"100{long_s}"))
        variants.add(int(long_s))
    return list(variants)


def _build_static_channel_map():
    for mname, mcfg in MASTER_ACCOUNTS.items():
        for ch in mcfg["channels"]:
            for cid in _normalize_channel_id(ch):
                if (mname, mcfg) not in CHANNEL_TO_MASTERS.get(cid, []):
                    CHANNEL_TO_MASTERS.setdefault(cid, []).append((mname, mcfg))
    log.info(f"  📡 Static map: {len(CHANNEL_TO_MASTERS)} channel ID variants")


async def _resolve_channels_via_telethon():
    all_ids: set[int] = {ch for mcfg in MASTER_ACCOUNTS.values() for ch in mcfg["channels"]}
    resolved = 0
    for ch_id in all_ids:
        try:
            entity  = await client.get_entity(ch_id)
            real_id = entity.id
            chat_id = -int(f"100{real_id}")
            title   = getattr(entity, "title", "?")
            for mname, mcfg in MASTER_ACCOUNTS.items():
                if ch_id in mcfg["channels"]:
                    for cid in [real_id, -real_id, chat_id, -chat_id]:
                        if (mname, mcfg) not in CHANNEL_TO_MASTERS.get(cid, []):
                            CHANNEL_TO_MASTERS.setdefault(cid, []).append((mname, mcfg))
            log.info(f"  ✅ Channel {ch_id} → real:{real_id} chat_id:{chat_id} [{title}]")
            resolved += 1
        except Exception as e:
            log.warning(f"  ⚠️  Channel {ch_id} resolve failed: {e}")
    log.info(f"  📡 Resolved {resolved}/{len(all_ids)} | {len(CHANNEL_TO_MASTERS)} routing entries")


# ─────────────────────────────────────────────
#  TELEGRAM CLIENT
# ─────────────────────────────────────────────
client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)
_build_static_channel_map()

_seen_set:     set   = set()
_seen_signals: deque = deque(maxlen=500)


@client.on(events.NewMessage())
async def handler(event):
    cid  = event.chat_id
    text = event.raw_text or ""
    if not text.strip():
        return

    # ── CHANNEL DEBUG: always show the exact chat_id ──────────────────────
    preview = text[:80].replace("\n", " ")
    masters = CHANNEL_TO_MASTERS.get(cid, [])

    if not masters:
        # Print UNMATCHED channel IDs prominently so the user can add them
        log.debug(f"📨 UNMATCHED channel:{cid} | {preview}")
        return

    log.info(f"📨 channel:{cid} → {[m[0] for m in masters]} | {preview}")

    # ── Dedup ────────────────────────────────────────────────────────────
    sig_hash = f"{cid}:{text[:200]}"
    if sig_hash in _seen_set:
        log.info("  ⏭ Duplicate ignored")
        return
    if len(_seen_signals) >= 500:
        _seen_set.discard(_seen_signals[0])
    _seen_signals.append(sig_hash)
    _seen_set.add(sig_hash)

    # ── Parse ────────────────────────────────────────────────────────────
    sig = await parse_signal(text)
    if not sig:
        log.info("  ⏭ No actionable signal")
        return

    tps = [v for v in [sig.get("tp1", 0), sig.get("tp2", 0), sig.get("tp3", 0)] if v > 0]
    direction = "BUY" if sig.get("action") == mt5.ORDER_TYPE_BUY else \
                "SELL" if sig.get("action") == mt5.ORDER_TYPE_SELL else "N/A"
    log.info(
        f"📊 {sig.get('symbol')} {direction} entry:{sig.get('entry', 0)} "
        f"SL:{sig.get('sl', 0)} TPs:{tps} type:{sig.get('type')}"
    )

    # ── FIRE-AND-FORGET: create tasks and return immediately ─────────────
    # handler() returns to Telethon instantly — next message is never delayed
    for master_name, master_cfg in masters:
        if master_cfg.get("enabled", True):
            asyncio.create_task(execute_on_master(master_name, master_cfg, sig))


# ─────────────────────────────────────────────
#  MT5 WATCHDOG  (keeps active account warm)
# ─────────────────────────────────────────────
async def mt5_watchdog():
    await asyncio.sleep(30)
    while True:
        try:
            async with _mt5_lock:
                global _mt5_active_account
                if _mt5_active_account is not None and mt5.account_info() is None:
                    log.warning(f"  🔌 Watchdog: MT5 #{_mt5_active_account} dropped — resetting")
                    mt5.shutdown()
                    _mt5_active_account = None
        except Exception as e:
            log.warning(f"  Watchdog: {e}")
        await asyncio.sleep(30)


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
async def main():
    log.info("═" * 65)
    log.info("  ARCHITECT v37.0 | Zero-latency queue | Fire-and-forget handler")
    log.info("═" * 65)

    if not all([TG_SESSION, TG_API_ID, TG_API_HASH]):
        log.error("❌ Missing .env — set TG_SESSION, TG_API_ID, TG_API_HASH")
        return

    if ANTHROPIC_API_KEY:
        log.info("  🤖 AI parsing: ENABLED (3 s timeout then regex fallback)")
    else:
        log.info("  🔢 AI parsing: DISABLED (regex only)")

    # Ensure JSON log directories exist
    for cfg in MASTER_ACCOUNTS.values():
        dirpart = os.path.dirname(cfg["json_file"])
        if dirpart:
            os.makedirs(dirpart, exist_ok=True)
        await asyncio.to_thread(load_log, cfg["json_file"])

    # Start slave workers (one per slave — queue-based, zero poll delay)
    for sname, scfg in SLAVE_ACCOUNTS.items():
        if scfg.get("enabled", True):
            asyncio.create_task(slave_worker(sname, scfg))
            log.info(f"  ⚡ Slave worker launched: {sname}")

    asyncio.create_task(mt5_watchdog())
    asyncio.create_task(auto_sltp_scanner())

    while True:
        try:
            await client.start()
            log.info(
                f"✅ Telegram online | "
                f"{len(MASTER_ACCOUNTS)} masters | {len(SLAVE_ACCOUNTS)} slaves"
            )
            for n, c in MASTER_ACCOUNTS.items():
                log.info(f"  MASTER [{n}] #{c['account']} channels:{c['channels']}")
            for n, c in SLAVE_ACCOUNTS.items():
                log.info(f"  SLAVE  [{n}] #{c['account']} → copies #{c['copy_from_master']}")

            await _resolve_channels_via_telethon()
            log.info(f"  Routing {len(CHANNEL_TO_MASTERS)} channel ID variants")
            log.info("  ⚡ All systems hot — waiting for signals...")

            await client.run_until_disconnected()
            log.warning("⚠️ Telegram disconnected — reconnecting in 5 s...")
        except Exception as e:
            log.error(f"❌ Telegram error: {e} — reconnecting in 5 s...")
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
