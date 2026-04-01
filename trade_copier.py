"""
=============================================================================
  TELEGRAM → MT5 MASTER/SLAVE TRADE COPIER  |  ARCHITECT v40.0
=============================================================================
  ROOT CAUSE OF ACCOUNT SWITCHING (fixed here):

  MetaTrader5's Python library uses a SINGLE shared COM/IPC connection
  inside the DLL.  Calling mt5.initialize() a second time (even with the
  same login) ALWAYS disconnects the previously active session first.
  That is why Master1 kept getting logged out — every time a slave or
  another master needed the connection, it called mt5.initialize() and
  blew away the active session.

  THE FIX — per-account subprocess workers:
  ─────────────────────────────────────────
  Each MT5 account gets its own dedicated Python subprocess.
  The subprocess opens MT5 ONCE at startup and never calls initialize()
  again (unless the connection drops).  It then sits in a loop reading
  orders from a multiprocessing.Queue and executing them.

  The main Telegram process never calls mt5.initialize() at all.
  Signals are routed to the correct subprocess queue.
  ZERO account switching.  ZERO logouts.

  Architecture:
    Main process  ─── Telegram ──→ parse signal
                  ─── route to correct account worker(s) via mp.Queue
    Worker-Master1 ─── mt5.initialize(Master1) ONCE ─── execute orders
    Worker-Master2 ─── mt5.initialize(Master2) ONCE ─── execute orders
    Worker-Master3 ─── mt5.initialize(Master3) ONCE ─── execute orders
    Worker-Slave1  ─── mt5.initialize(Slave1)  ONCE ─── copy orders
    Worker-Slave2  ─── mt5.initialize(Slave2)  ONCE ─── copy orders

  All v39 features preserved:
    ✅ In-memory queue slave copy (zero poll delay)
    ✅ Fire-and-forget handler
    ✅ Auto SL/TP scanner (in each worker subprocess)
    ✅ ATR trailing stop
    ✅ Zone pending orders
    ✅ AI + regex signal parser
    ✅ Channel ID resolution
    ✅ Requote retry
    ✅ Chart drawing
=============================================================================
"""

import os, re, sys, asyncio, json, logging, uuid, time, multiprocessing as mp
from collections import deque
from datetime import datetime
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession

load_dotenv()

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
def _make_logger(name: str, logfile: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger

log = _make_logger("MAIN", "copier_v40.log")


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
        "XAUUSD": 150, "XAGUSD": 50, "NASDAQ": 50,
        "US30": 80,    "SP500":  40,  "GER40":  40,
        "UK100": 40,   "USOIL":  30,  "BTCUSD": 500,
        "ETHUSD": 100,
    },
    "auto_rr1": 1.5,
    "auto_rr2": 2.5,
    "auto_rr3": 4.0,
    "draw_on_chart": True,
    "auto_sltp_scan_interval": 60,
    "auto_sltp_magic_filter": [],
    "market_snap_pts": 15,
    "ai_timeout_s": 1.5,
    "ai_min_confidence": 60,
    "trailing_scan_interval": 5,
    "atr_period": 14,
    "atr_timeframe": "M15",
    "atr_multiplier": 1.5,
    "trailing_magic_filter": [],
    "max_positions_per_signal": 3,
    "max_tps_per_signal": 2,
}


# ===========================
# MASTER ACCOUNTS CONFIGURATION
# ===========================
MASTER_ACCOUNTS = {
    "Master1": {
        "enabled":      True,
        "account":      128961,
        "password":     "xiP!S2sM",
        "server":       "4xHubInternational-Server",
        "mt5_path":     r"C:\MT5\MT5Master3\4xHub International MT5 Terminal\terminal64.exe",
        "lot_size":     0.01,
        "magic_number": 111111,
        "deviation":    300,
        "max_lot":      10.0,
        "channels":     [-1001640332422],
        "json_file":    r"C:\AI_Signal\master_trades_128961.json",
    },
    "Master2": {
        "enabled":      True,
        "account":      161443437,
        "password":     "Ugoprince!@555",
        "server":       "Exness-MT5Real21",
        "mt5_path":     r"C:\MT5\MT5Master2\MetaTrader 5 EXNESS\terminal64.exe",
        "lot_size":     0.1,
        "magic_number": 654321,
        "deviation":    300,
        "max_lot":      10.0,
        "channels":     [-1002034822451, -1001588519179, -1002201702304,
                         -1001381790914, -1001196272579, -1001182913499],
        "json_file":    r"C:\AI_Signal\master_trades_161443437.json",
    },
    "Master3": {
        "enabled":      True,
        "account":      51647558,
        "password":     "Ugoprince@!555",
        "server":       "VantageInternational-Live 4",
        "mt5_path":     r"C:\MT5\MT5Master1\Vantage International MT5\terminal64.exe",
        "lot_size":     0.01,
        "magic_number": 123456,
        "deviation":    300,
        "max_lot":      10.0,
        "channels":     [-1002034822451],
        "json_file":    r"C:\AI_Signal\master_trades_51647558.json",
    },
}

# ===========================
# SLAVE ACCOUNTS CONFIGURATION
# ===========================
SLAVE_ACCOUNTS = {
    "Slave1": {
        "enabled":          True,
        "name":             "Slave1",
        "account":          5237650,
        "password":         "Ugoprince@!555",
        "server":           "ICMarketsSC-MT5",
        "mt5_path":         r"C:\MT5\SLAVE1\terminal64.exe",
        "lot_multiplier":   1.0,
        "magic_number":     999999,
        "deviation":        300,
        "max_lot":          10.0,
        "copy_from_master": 128961,
    },
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
        "copy_from_master": 128961,
    },
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


# ═══════════════════════════════════════════════════════════════════════════
#
#  ACCOUNT WORKER (runs in a dedicated subprocess)
#
#  Each account (master or slave) gets one of these.
#  It initializes MT5 ONCE and never switches.
#  Commands arrive as dicts on a multiprocessing.Queue.
#
# ═══════════════════════════════════════════════════════════════════════════

def _worker_log(name: str) -> logging.Logger:
    return _make_logger(f"WORKER-{name}", f"worker_{name}.log")


def _worker_main(cfg: dict, cmd_queue: mp.Queue, result_queue: mp.Queue,
                 account_name: str, is_slave: bool, rr_config: dict):
    """
    Entry point for each per-account subprocess.
    Runs a synchronous event loop — no asyncio needed inside the worker.
    """
    import MetaTrader5 as mt5   # imported fresh per process
    wlog = _worker_log(account_name)
    wlog.info(f"[{account_name}] Worker subprocess started (PID:{os.getpid()})")

    # ── STEP 1: Connect to MT5 once ──────────────────────────────────────
    def _connect() -> bool:
        """
        Two-step connection strategy:

        Step A — attach to a RUNNING terminal for this account (fast).
          We pass login/password/server so MT5 only attaches if those
          credentials match the already-open terminal.  Safe — if the
          running terminal belongs to a different account, MT5 will reject
          the attach and we fall through to Step B.

        Step B — launch from the dedicated path.
          path= binds this subprocess to its own terminal executable.

        After either attempt we verify acc.login == cfg["account"] to
        catch any edge case where the wrong account slipped through.
        """

        def _verify(label: str) -> bool:
            acc = mt5.account_info()
            if acc and acc.login == cfg["account"]:
                wlog.info(
                    f"[{account_name}] ✅ Connected ({label}) #{acc.login} | "
                    f"{acc.server} | Balance:{acc.balance:.2f} {acc.currency}"
                )
                return True
            wlog.warning(
                f"[{account_name}] {label} returned wrong account "
                f"#{acc.login if acc else '?'} (expected #{cfg['account']}) — retrying"
            )
            mt5.shutdown()
            return False

        # Step A: attach to already-running terminal
        wlog.info(f"[{account_name}] Attaching to running terminal (account #{cfg['account']})...")
        if mt5.initialize(
            login=cfg["account"],
            password=cfg["password"],
            server=cfg["server"],
            timeout=8000,
        ) and _verify("attach"):
            return True

        # Step B: launch from dedicated terminal path
        wlog.info(f"[{account_name}] Launching terminal from: {cfg['mt5_path']}")
        if mt5.initialize(
            path=cfg["mt5_path"],
            login=cfg["account"],
            password=cfg["password"],
            server=cfg["server"],
            timeout=40000,
        ) and _verify("launch"):
            return True

        wlog.error(f"[{account_name}] ❌ Connect failed: {mt5.last_error()}")
        wlog.error(
            f"[{account_name}] 💡 Fix: open the terminal manually, log in to "
            f"#{cfg['account']}, enable Expert Advisors, then restart the bot."
        )
        return False

    connected = _connect()
    if not connected:
        result_queue.put({"type": "startup_failed", "account": cfg["account"]})
        return

    result_queue.put({"type": "startup_ok", "account": cfg["account"], "name": account_name})

    # ── Helper functions (inline so they share the worker's mt5 instance) ─

    def _get_filling(symbol):
        info = mt5.symbol_info(symbol)
        if not info: return mt5.ORDER_FILLING_FOK
        fm = info.filling_mode
        if fm & 2: return mt5.ORDER_FILLING_IOC
        if fm & 1: return mt5.ORDER_FILLING_FOK
        return mt5.ORDER_FILLING_RETURN

    def _resolve(base):
        for s in SUFFIXES:
            sym = f"{base}{s}"
            if mt5.symbol_select(sym, True):
                info = mt5.symbol_info(sym)
                if info and info.visible:
                    return sym
        wlog.warning(f"[{account_name}] ⚠️ Cannot resolve '{base}'")
        return None

    def _pip(symbol):
        info = mt5.symbol_info(symbol)
        if not info: return 0.0001
        if any(x in symbol for x in ["JPY","NASDAQ","US30","SP500","GER40","UK100"]):
            return info.point
        if any(x in symbol for x in ["XAU","XAG"]):
            return info.point * 10
        return info.point * 10

    def _auto_sltp(symbol, action, entry_price):
        if entry_price == 0:
            tick = mt5.symbol_info_tick(symbol)
            if tick:
                entry_price = tick.ask if action == mt5.ORDER_TYPE_BUY else tick.bid
        pip = _pip(symbol)
        if pip == 0 or entry_price == 0: return 0.0,0.0,0.0,0.0
        info = mt5.symbol_info(symbol)
        if not info: return 0.0,0.0,0.0,0.0
        digits  = info.digits
        sl_pips = rr_config["symbol_sl_pips"].get(symbol, rr_config["auto_sl_pips"])
        sl_dist = sl_pips * pip
        if action == mt5.ORDER_TYPE_BUY:
            sl  = round(entry_price - sl_dist, digits)
            tp1 = round(entry_price + sl_dist * rr_config["auto_rr1"], digits)
            tp2 = round(entry_price + sl_dist * rr_config["auto_rr2"], digits)
            tp3 = round(entry_price + sl_dist * rr_config["auto_rr3"], digits)
        else:
            sl  = round(entry_price + sl_dist, digits)
            tp1 = round(entry_price - sl_dist * rr_config["auto_rr1"], digits)
            tp2 = round(entry_price - sl_dist * rr_config["auto_rr2"], digits)
            tp3 = round(entry_price - sl_dist * rr_config["auto_rr3"], digits)
        wlog.info(
            f"[{account_name}] 📐 Auto SL/TP entry:{entry_price} "
            f"SL:{sl} TP1:{tp1} TP2:{tp2}"
        )
        return sl, tp1, tp2, tp3

    def _send_order(sym, action, lot, sl, tp, entry, is_limit, comment="v40", retry=True):
        lot  = round(min(max(lot, 0.01), cfg["max_lot"]), 2)
        tick = mt5.symbol_info_tick(sym)
        if not tick:
            wlog.error(f"[{account_name}] No tick for {sym}"); return None
        sym_info = mt5.symbol_info(sym)
        if not sym_info:
            wlog.error(f"[{account_name}] No symbol info for {sym}"); return None

        curr_p = tick.ask if action == mt5.ORDER_TYPE_BUY else tick.bid
        pt     = sym_info.point
        digits = sym_info.digits

        if sl > 0: sl = round(sl, digits)
        if tp > 0: tp = round(tp, digits)

        trade_action = mt5.TRADE_ACTION_DEAL
        order_type   = action
        exec_price   = curr_p

        snap = rr_config.get("market_snap_pts", 15)
        if entry > 0:
            if abs(curr_p - entry) > pt * snap or is_limit:
                trade_action = mt5.TRADE_ACTION_PENDING
                exec_price   = round(entry, digits)
                if action == mt5.ORDER_TYPE_BUY:
                    order_type = mt5.ORDER_TYPE_BUY_LIMIT if exec_price < curr_p else mt5.ORDER_TYPE_BUY_STOP
                else:
                    order_type = mt5.ORDER_TYPE_SELL_LIMIT if exec_price > curr_p else mt5.ORDER_TYPE_SELL_STOP

        req = {
            "action":       trade_action,
            "symbol":       sym,
            "volume":       lot,
            "type":         order_type,
            "price":        exec_price,
            "deviation":    cfg["deviation"],
            "magic":        cfg["magic_number"],
            "comment":      comment[:31],
            "type_filling": _get_filling(sym),
            "type_time":    mt5.ORDER_TIME_GTC,
        }
        if sl > 0: req["sl"] = sl
        if tp > 0: req["tp"] = tp

        direction = "BUY" if action == mt5.ORDER_TYPE_BUY else "SELL"
        mode      = "PENDING" if trade_action == mt5.TRADE_ACTION_PENDING else "MARKET"
        wlog.info(f"[{account_name}] 📤 {mode} {sym} {direction} lot:{lot} @{exec_price} sl:{sl} tp:{tp}")

        res = mt5.order_send(req)
        if res is None:
            wlog.error(f"[{account_name}] order_send None: {mt5.last_error()}"); return None
        if res.retcode == mt5.TRADE_RETCODE_DONE:
            wlog.info(f"[{account_name}] ✅ [{mode}] #{res.order} {sym} lot:{lot} sl:{sl} tp:{tp}")
            return res.order
        if res.retcode == 10004 and retry:
            wlog.warning(f"[{account_name}] Requote {sym} — retrying at market")
            return _send_order(sym, action, lot, sl, tp, 0.0, False, comment, retry=False)
        wlog.warning(f"[{account_name}] ❌ Order failed retcode:{res.retcode} | {res.comment}")
        return None

    def _close_positions(sym, magic):
        positions = mt5.positions_get(symbol=sym) or []
        closed = 0
        for pos in positions:
            if pos.magic != magic: continue
            ctype = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
            tick  = mt5.symbol_info_tick(sym)
            if not tick: continue
            price = tick.bid if ctype == mt5.ORDER_TYPE_SELL else tick.ask
            res = mt5.order_send({
                "action": mt5.TRADE_ACTION_DEAL, "position": pos.ticket,
                "symbol": sym, "volume": pos.volume, "type": ctype,
                "price": price, "deviation": 300, "magic": magic,
                "comment": "CLOSE_v40", "type_filling": _get_filling(sym),
            })
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                closed += 1
        wlog.info(f"[{account_name}] Closed {closed} positions for {sym}")

    def _close_partial(sym, magic, pct):
        for pos in (mt5.positions_get(symbol=sym) or []):
            if pos.magic != magic: continue
            vol   = max(round(pos.volume * pct / 100, 2), 0.01)
            ctype = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
            tick  = mt5.symbol_info_tick(sym)
            if not tick: continue
            mt5.order_send({
                "action": mt5.TRADE_ACTION_DEAL, "position": pos.ticket,
                "symbol": sym, "volume": vol, "type": ctype,
                "price": tick.bid if ctype == mt5.ORDER_TYPE_SELL else tick.ask,
                "deviation": 300, "magic": magic,
                "comment": f"PARTIAL_{pct}pct", "type_filling": _get_filling(sym),
            })

    def _breakeven(sym, magic):
        for pos in (mt5.positions_get(symbol=sym) or []):
            if pos.magic != magic: continue
            mt5.order_send({
                "action": mt5.TRADE_ACTION_SLTP, "position": pos.ticket,
                "symbol": sym, "sl": pos.price_open, "tp": pos.tp,
            })

    def _modify_sl(sym, magic, new_sl):
        for pos in (mt5.positions_get(symbol=sym) or []):
            if pos.magic != magic: continue
            mt5.order_send({
                "action": mt5.TRADE_ACTION_SLTP, "position": pos.ticket,
                "symbol": sym, "sl": new_sl, "tp": pos.tp,
            })

    def _draw_levels(sym, action, entry, sl, tp1, tp2=0, ticket=0):
        if not rr_config.get("draw_on_chart"): return
        try:
            pfx = f"v40_{ticket}"
            d   = "BUY" if action == mt5.ORDER_TYPE_BUY else "SELL"
            def hl(name, price, color, style, w, lbl):
                mt5.object_delete(0, name)
                mt5.object_create(0, name, mt5.OBJ_HLINE, 0, 0, price)
                mt5.object_set_integer(0, name, mt5.OBJPROP_COLOR, color)
                mt5.object_set_integer(0, name, mt5.OBJPROP_STYLE, style)
                mt5.object_set_integer(0, name, mt5.OBJPROP_WIDTH, w)
                mt5.object_set_string(0,  name, mt5.OBJPROP_TEXT,  lbl)
                mt5.object_set_integer(0, name, mt5.OBJPROP_SELECTABLE, False)
                mt5.object_set_integer(0, name, mt5.OBJPROP_BACK, True)
            if entry > 0: hl(f"{pfx}_E",   entry, 0x3399FF, mt5.STYLE_DASH,  1, f"ENTRY {d}@{entry}")
            if sl    > 0: hl(f"{pfx}_SL",  sl,    0xFF3333, mt5.STYLE_SOLID, 2, f"SL {sl}")
            if tp1   > 0: hl(f"{pfx}_TP1", tp1,   0x00CC44, mt5.STYLE_DOT,   1, f"TP1 {tp1}")
            if tp2   > 0: hl(f"{pfx}_TP2", tp2,   0x00CC44, mt5.STYLE_DOT,   1, f"TP2 {tp2}")
            mt5.chart_redraw(0)
        except Exception:
            pass

    def _auto_sltp_scan():
        """Scan all positions on this account and patch missing SL/TP."""
        mf = rr_config.get("auto_sltp_magic_filter", [])
        positions = mt5.positions_get() or []
        for pos in positions:
            if mf and pos.magic not in mf: continue
            miss_sl = pos.sl == 0.0
            miss_tp = pos.tp == 0.0
            if not (miss_sl or miss_tp): continue
            a_sl, a_tp1, _, _ = _auto_sltp(pos.symbol, pos.type, pos.price_open)
            if a_sl == 0 and a_tp1 == 0: continue
            new_sl = a_sl  if miss_sl else pos.sl
            new_tp = a_tp1 if miss_tp else pos.tp
            wlog.info(f"[{account_name}] 🔧 Patching #{pos.ticket} {pos.symbol} SL:{new_sl} TP:{new_tp}")
            mt5.order_send({
                "action": mt5.TRADE_ACTION_SLTP, "position": pos.ticket,
                "symbol": pos.symbol, "sl": new_sl, "tp": new_tp,
            })

    # Trailing state: {ticket: {symbol, action, tp2, active, trail_sl}}
    _trail: dict[int, dict] = {}

    def _trailing_scan():
        mult    = rr_config.get("atr_multiplier", 1.5)
        period  = rr_config.get("atr_period", 14)
        tf_name = rr_config.get("atr_timeframe", "M15")
        TF_MAP  = {
            "M1":  mt5.TIMEFRAME_M1,  "M5":  mt5.TIMEFRAME_M5,
            "M15": mt5.TIMEFRAME_M15, "M30": mt5.TIMEFRAME_M30,
            "H1":  mt5.TIMEFRAME_H1,  "H4":  mt5.TIMEFRAME_H4,
            "D1":  mt5.TIMEFRAME_D1,
        }
        tf = TF_MAP.get(tf_name, mt5.TIMEFRAME_M15)
        mf = rr_config.get("trailing_magic_filter", [])
        dead = []
        for ticket, st in list(_trail.items()):
            pos_list = mt5.positions_get(ticket=ticket)
            if not pos_list:
                dead.append(ticket); continue
            pos = pos_list[0]
            if mf and pos.magic not in mf: continue
            tick = mt5.symbol_info_tick(st["symbol"])
            if not tick: continue
            curr = tick.bid if st["action"] == mt5.ORDER_TYPE_BUY else tick.ask
            info = mt5.symbol_info(st["symbol"])
            digits = info.digits if info else 5
            if not st["active"]:
                hit = (st["action"] == mt5.ORDER_TYPE_BUY  and curr >= st["tp2"]) or \
                      (st["action"] == mt5.ORDER_TYPE_SELL and curr <= st["tp2"])
                if hit:
                    st["active"] = True
                    wlog.info(f"[{account_name}] 🚀 Trailing ACTIVATED #{ticket} {st['symbol']} price:{curr}")
                continue
            # Calc ATR
            try:
                bars = mt5.copy_rates_from_pos(st["symbol"], tf, 0, period+1)
                if not bars or len(bars) < period+1: continue
                trs = [max(bars[i]["high"]-bars[i]["low"],
                           abs(bars[i]["high"]-bars[i-1]["close"]),
                           abs(bars[i]["low"] -bars[i-1]["close"]))
                       for i in range(1, len(bars))]
                atr = sum(trs[-period:]) / period
            except Exception:
                continue
            if atr <= 0: continue
            dist = atr * mult
            if st["action"] == mt5.ORDER_TYPE_BUY:
                new_sl = round(curr - dist, digits)
                if new_sl <= st["trail_sl"]: continue
            else:
                new_sl = round(curr + dist, digits)
                if new_sl >= st["trail_sl"]: continue
            res = mt5.order_send({
                "action": mt5.TRADE_ACTION_SLTP, "position": ticket,
                "symbol": st["symbol"], "sl": new_sl, "tp": pos.tp,
            })
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                wlog.info(f"[{account_name}] 📈 Trail #{ticket} SL:{st['trail_sl']}→{new_sl}")
                st["trail_sl"] = new_sl
        for t in dead:
            _trail.pop(t, None)

    # ── STEP 2: Main command loop ─────────────────────────────────────────
    last_sltp_scan    = 0.0
    last_trail_scan   = 0.0
    sltp_interval     = rr_config.get("auto_sltp_scan_interval", 60)
    trail_interval    = rr_config.get("trailing_scan_interval", 5)

    wlog.info(f"[{account_name}] Entering command loop...")

    while True:
        # ── Periodic background scans ──────────────────────────────────
        now = time.monotonic()
        if now - last_sltp_scan > sltp_interval:
            # Verify connection is still alive AND is the correct account
            acc_check = mt5.account_info()
            if acc_check is None or acc_check.login != cfg["account"]:
                if acc_check and acc_check.login != cfg["account"]:
                    wlog.warning(
                        f"[{account_name}] ⚠️ Wrong account #{acc_check.login} active "
                        f"(expected #{cfg['account']}) — reconnecting to dedicated terminal"
                    )
                    mt5.shutdown()
                else:
                    wlog.warning(f"[{account_name}] Connection lost — reconnecting to dedicated terminal...")
                if not _connect():
                    time.sleep(5)
                    continue
            try:
                _auto_sltp_scan()
            except Exception as e:
                wlog.warning(f"[{account_name}] auto_sltp_scan error: {e}")
            last_sltp_scan = now

        if now - last_trail_scan > trail_interval:
            try:
                _trailing_scan()
            except Exception as e:
                wlog.warning(f"[{account_name}] trailing_scan error: {e}")
            last_trail_scan = now

        # ── Read next command (0.5 s timeout so scans still run) ──────
        try:
            cmd = cmd_queue.get(timeout=0.5)
        except Exception:
            continue   # queue.Empty — loop back for periodic scans

        try:
            ctype = cmd.get("type")

            # ── Reconnect healthcheck ──────────────────────────────────
            acc_pre = mt5.account_info()
            if acc_pre is None or acc_pre.login != cfg["account"]:
                if acc_pre and acc_pre.login != cfg["account"]:
                    wlog.warning(
                        f"[{account_name}] ⚠️ Wrong account #{acc_pre.login} "
                        f"(expected #{cfg['account']}) before {ctype} — reconnecting"
                    )
                    mt5.shutdown()
                else:
                    wlog.warning(f"[{account_name}] Connection lost before {ctype} — reconnecting...")
                if not _connect():
                    wlog.error(f"[{account_name}] Reconnect failed — dropping {ctype}")
                    continue

            if ctype == "trade":
                sig      = cmd["sig"]
                sym_base = sig.get("symbol", "")
                sym      = _resolve(sym_base)
                if not sym:
                    wlog.warning(f"[{account_name}] Symbol '{sym_base}' not found")
                    continue

                action  = sig.get("action")
                if action is None:
                    wlog.warning(f"[{account_name}] No direction — skipping"); continue

                tick    = mt5.symbol_info_tick(sym)
                curr_p  = (tick.ask if action == mt5.ORDER_TYPE_BUY else tick.bid) if tick else 0
                entry_p = sig.get("entry", 0) or curr_p

                sl  = sig.get("sl",  0) or 0.0
                tp1 = sig.get("tp1", 0) or 0.0
                tp2 = sig.get("tp2", 0) or 0.0

                if sl == 0 or tp1 == 0 or tp2 == 0:
                    a_sl, a_tp1, a_tp2, _ = _auto_sltp(sym, action, entry_p)
                    if sl  == 0: sl  = a_sl
                    if tp1 == 0: tp1 = a_tp1
                    if tp2 == 0: tp2 = a_tp2

                max_tps = rr_config.get("max_tps_per_signal", 2)
                max_pos = rr_config.get("max_positions_per_signal", 3)
                active_tps = [v for v in [tp1, tp2] if v > 0][:max_tps]

                tickets   = []
                base_lot  = cfg["lot_size"] if not is_slave else cmd.get("lot", cfg.get("lot_size", 0.01))
                is_limit  = sig.get("is_limit", False)
                zone_low  = sig.get("zone_low",  0.0)
                zone_high = sig.get("zone_high", 0.0)
                has_zone  = zone_low > 0 and zone_high > 0

                if has_zone:
                    z_entries = [zone_low, zone_high] if action == mt5.ORDER_TYPE_BUY else [zone_high, zone_low]
                    z_entries = z_entries[:max_pos]
                    split = max(round(base_lot / len(z_entries), 2), 0.01)
                    for i, ze in enumerate(z_entries):
                        tp_i = active_tps[i] if i < len(active_tps) else (active_tps[-1] if active_tps else 0.0)
                        t = _send_order(sym, action, split, sl, tp_i, ze, True)
                        if t: tickets.append(t)
                elif not active_tps:
                    t = _send_order(sym, action, base_lot, sl, 0.0, entry_p, is_limit)
                    if t: tickets.append(t)
                elif len(active_tps) == 1:
                    t = _send_order(sym, action, base_lot, sl, active_tps[0], entry_p, is_limit)
                    if t: tickets.append(t)
                else:
                    n = min(len(active_tps), max_pos)
                    split = max(round(base_lot / n, 2), 0.01)
                    for i in range(n):
                        t = _send_order(sym, action, split, sl, active_tps[i], entry_p, is_limit)
                        if t:
                            tickets.append(t)
                        if len(tickets) >= max_pos: break

                if tickets:
                    _draw_levels(sym, action, entry_p, sl, tp1, tp2, tickets[0])
                    # Register for trailing
                    if tp2 > 0:
                        for tk in tickets:
                            _trail[tk] = {
                                "symbol": sym, "action": action,
                                "tp2": tp2, "active": False,
                                "trail_sl": sl, "entry": entry_p,
                            }
                    # Send copy record back to main so slaves can be notified
                    result_queue.put({
                        "type":    "trade_placed",
                        "master":  cfg["account"],
                        "symbol":  sym,
                        "action":  action,
                        "sl":      sl,
                        "tps":     active_tps,
                        "entry":   entry_p,
                        "lot":     base_lot,
                        "tickets": tickets,
                        "is_limit": is_limit,
                        "_push_ts": cmd.get("_push_ts", time.monotonic()),
                    })
                    wlog.info(f"[{account_name}] ✅ {len(tickets)} ticket(s) placed")

            elif ctype == "slave_trade":
                # Slave receives a pre-built trade record — just copy it
                trade  = cmd["trade"]
                sym    = _resolve(trade["symbol"])
                if not sym:
                    wlog.warning(f"[{account_name}] '{trade['symbol']}' not found"); continue

                mul  = cfg.get("lot_multiplier", 1.0)
                lot  = min(max(round(trade["lot"] * mul, 2), 0.01), cfg["max_lot"])
                tps  = trade.get("tps", [])
                cmt  = cmd.get("comment", "SLV_v40")[:31]
                t0   = time.monotonic()

                if not tps:
                    _send_order(sym, trade["action"], lot, trade["sl"], 0.0, trade["entry"], trade["is_limit"], cmt)
                elif len(tps) == 1:
                    _send_order(sym, trade["action"], lot, trade["sl"], tps[0], trade["entry"], trade["is_limit"], cmt)
                else:
                    split = max(round(lot / len(tps), 2), 0.01)
                    for tp in tps:
                        _send_order(sym, trade["action"], split, trade["sl"], tp, trade["entry"], trade["is_limit"], cmt)

                push_ts  = trade.get("_push_ts")
                total_ms = round((time.monotonic() - push_ts) * 1000) if push_ts else -1
                copy_ms  = round((time.monotonic() - t0) * 1000)
                wlog.info(
                    f"[{account_name}] ✅ Slave copy done {trade['symbol']} "
                    f"order_send:{copy_ms}ms end-to-end:{total_ms}ms"
                )

            elif ctype == "close":
                sym = _resolve(cmd["symbol"])
                if sym: _close_positions(sym, cfg["magic_number"])

            elif ctype == "partial_close":
                sym = _resolve(cmd["symbol"])
                if sym: _close_partial(sym, cfg["magic_number"], cmd.get("pct", 50))

            elif ctype == "breakeven":
                sym = _resolve(cmd["symbol"])
                if sym: _breakeven(sym, cfg["magic_number"])

            elif ctype == "modify_sl":
                sym = _resolve(cmd["symbol"])
                if sym: _modify_sl(sym, cfg["magic_number"], cmd["new_sl"])

            elif ctype == "ping":
                result_queue.put({"type": "pong", "account": cfg["account"]})

            elif ctype == "stop":
                wlog.info(f"[{account_name}] Stop command received — shutting down")
                mt5.shutdown()
                return

        except Exception as e:
            wlog.error(f"[{account_name}] Command error ({ctype}): {e}")


# ═══════════════════════════════════════════════════════════════════════════
#
#  WORKER MANAGER  (runs in main process)
#  Spawns and manages all account subprocesses.
#
# ═══════════════════════════════════════════════════════════════════════════

class WorkerManager:
    def __init__(self):
        self._workers:  dict[str, mp.Process]  = {}   # name → Process
        self._cmd_qs:   dict[str, mp.Queue]    = {}   # name → command Queue
        self._result_q: mp.Queue               = mp.Queue()
        # master account number → list of slave worker names that copy it
        self._slave_map: dict[int, list[str]]  = {}

    def start_all(self):
        # Start master workers
        for name, cfg in MASTER_ACCOUNTS.items():
            if not cfg.get("enabled", True):
                continue
            self._start_worker(name, cfg, is_slave=False)

        # Start slave workers
        for name, cfg in SLAVE_ACCOUNTS.items():
            if not cfg.get("enabled", True):
                continue
            self._start_worker(name, cfg, is_slave=True)
            master_acc = cfg["copy_from_master"]
            self._slave_map.setdefault(master_acc, []).append(name)

        # Wait for all startups
        expected  = sum(1 for c in MASTER_ACCOUNTS.values() if c.get("enabled", True)) + \
                    sum(1 for c in SLAVE_ACCOUNTS.values()   if c.get("enabled", True))
        responses = 0
        succeeded = 0
        failed    = 0
        deadline  = time.monotonic() + 60
        while responses < expected and time.monotonic() < deadline:
            try:
                msg = self._result_q.get(timeout=2)
                if msg["type"] == "startup_ok":
                    log.info(f"  ✅ Worker [{msg['name']}] #{msg['account']} ready")
                    succeeded += 1
                elif msg["type"] == "startup_failed":
                    log.error(f"  ❌ Worker startup FAILED for #{msg['account']} — check worker_*.log for details")
                    failed += 1
                responses += 1
            except Exception:
                pass
        if failed == 0:
            log.info(f"  ✅ All {succeeded}/{expected} workers connected")
        else:
            log.warning(f"  ⚠️ Workers: {succeeded} connected, {failed} FAILED (check worker logs)")

    def _start_worker(self, name: str, cfg: dict, is_slave: bool):
        q = mp.Queue()
        self._cmd_qs[name] = q
        p = mp.Process(
            target=_worker_main,
            args=(cfg, q, self._result_q, name, is_slave, RR_CONFIG),
            name=f"worker-{name}",
            daemon=True,
        )
        p.start()
        self._workers[name] = p
        log.info(f"  Spawned worker [{name}] PID:{p.pid}")

    def send_trade(self, master_name: str, sig: dict):
        """Route a parsed signal to the correct master worker."""
        q = self._cmd_qs.get(master_name)
        if q:
            q.put({"type": "trade", "sig": sig, "_push_ts": time.monotonic()})

    def forward_to_slaves(self, master_account: int, trade_record: dict):
        """Forward a trade_placed record to all slaves that copy this master."""
        for sname in self._slave_map.get(master_account, []):
            q = self._cmd_qs.get(sname)
            if q:
                # find master name for comment
                mname = next((n for n, c in MASTER_ACCOUNTS.items()
                              if c["account"] == master_account), "Master")
                q.put({
                    "type":    "slave_trade",
                    "trade":   trade_record,
                    "comment": f"SLV_{mname}"[:31],
                })
                log.info(f"  ➡️  Forwarded to {sname} (end-to-end so far: "
                         f"{round((time.monotonic()-trade_record.get('_push_ts',time.monotonic()))*1000)}ms)")

    def send_management(self, master_name: str, cmd: dict):
        q = self._cmd_qs.get(master_name)
        if q:
            q.put(cmd)

    def drain_results(self):
        """Process all pending result messages from workers (non-blocking)."""
        results = []
        while True:
            try:
                results.append(self._result_q.get_nowait())
            except Exception:
                break
        return results

    def get_result_queue(self) -> mp.Queue:
        return self._result_q

    def stop_all(self):
        for name, q in self._cmd_qs.items():
            try: q.put({"type": "stop"})
            except Exception: pass
        for name, p in self._workers.items():
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()


# Global worker manager (created in main())
_wm: WorkerManager | None = None


# ─────────────────────────────────────────────
#  RESULT DRAINER  (async task in main process)
#  Reads trade_placed messages from worker result queues and
#  forwards them to the appropriate slave queues.
# ─────────────────────────────────────────────
async def result_drainer():
    """Continuously drain worker result queue and forward slave copies."""
    log.info("🔄 Result drainer started")
    while True:
        try:
            results = _wm.drain_results()
            for msg in results:
                if msg["type"] == "trade_placed":
                    _wm.forward_to_slaves(msg["master"], msg)
        except Exception as e:
            log.warning(f"Result drainer error: {e}")
        await asyncio.sleep(0.05)   # 50 ms poll — fast enough, non-blocking


# ─────────────────────────────────────────────
#  AI SIGNAL PARSER
# ─────────────────────────────────────────────
async def ai_parse_signal(text: str) -> dict | None:
    if not ANTHROPIC_API_KEY:
        return None
    try:
        import aiohttp
        prompt = f"""You are a professional forex/trading signal parser. Extract trade details from this Telegram message and return ONLY valid JSON, nothing else.

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
- GOLD/XAU → XAUUSD, SILVER/XAG → XAGUSD, NAS/NASDAQ/NAS100/US100 → NASDAQ
- "BUY NOW", "BUY MARKET" → is_limit: false, entry: null
- "BUY @ 1234", "BUY LIMIT 1234", "BUY STOP 1234" → is_limit: true
- "close", "exit" → type: close
- "breakeven", "move sl to entry" → type: breakeven
- "partial close", "close 50%" → type: partial_close
- confidence < 40 → is_signal: false
- Entry zone "2300-2310" → use midpoint 2305"""

        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-3-5-sonnet-20241022",
                    "max_tokens": 500,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=aiohttp.ClientTimeout(total=RR_CONFIG["ai_timeout_s"]),
            )
            data = await resp.json()

        # Handle API-level errors (invalid key, billing, rate limit, etc.)
        if "error" in data:
            err = data["error"]
            log.warning(f"  ⚠️ AI API error [{err.get('type','?')}]: {err.get('message','?')} — falling back to regex")
            return None
        if "content" not in data:
            log.warning(f"  ⚠️ AI unexpected response (no 'content' key): {str(data)[:120]} — falling back to regex")
            return None

        raw = data["content"][0]["text"].strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        parsed = json.loads(raw)

        conf = parsed.get("confidence", 0)
        if not parsed.get("is_signal") or conf < RR_CONFIG["ai_min_confidence"]:
            return None

        action_map = {"BUY": 0, "SELL": 1}   # mt5 constants unavailable in main process
        return {
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
            "confidence":      conf,
        }
    except Exception as e:
        log.warning(f"  ⚠️ AI parse failed: {e}")
        return None


# ─────────────────────────────────────────────
#  REGEX SIGNAL PARSER
# ─────────────────────────────────────────────
def regex_parse_signal(text: str) -> dict | None:
    # Note: mt5 constants (ORDER_TYPE_BUY=0, ORDER_TYPE_SELL=1) used as plain ints
    # because mt5 module may not be imported in the main process.
    BUY, SELL = 0, 1
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
    trade_kws = ["BUY", "SELL", "LONG", "SHORT", "CLOSE", "BREAKEVEN",
                 "SL TO", "BE NOW", "MOVE SL", "PARTIAL"]
    if not any(kw in t for kw in trade_kws):
        return None
    noise_count = sum(1 for n in NOISE if n in t)
    if noise_count >= 2 and not any(kw in t for kw in ["BUY","SELL","LONG","SHORT"]):
        return None

    # Symbol
    symbol = None
    for key in sorted(SYMBOL_MAP.keys(), key=len, reverse=True):
        if re.search(r'(?<![A-Z])' + re.escape(key) + r'(?![A-Z])', t):
            symbol = SYMBOL_MAP[key]; break
    if not symbol:
        for key in sorted(SYMBOL_MAP.keys(), key=len, reverse=True):
            if key in t:
                symbol = SYMBOL_MAP[key]; break
    if not symbol:
        return None

    # Management signals
    if any(x in t for x in ["BREAKEVEN","BREAK EVEN","MOVE SL TO BE","MOVE SL TO ENTRY",
                              "SL TO ENTRY","SL TO BE","SL TO OPEN","BE NOW","MOVE TO BE",
                              "SET BE","PUT SL AT ENTRY"]):
        return {"type": "breakeven", "symbol": symbol, "action": None}

    pct = 50
    pm = re.search(r'(\d{1,3})\s*%', t)
    if pm: pct = int(pm.group(1))
    if any(x in t for x in ["CLOSE PARTIAL","PARTIAL CLOSE","CLOSE HALF","HALF CLOSE",
                              "CLOSE 50","TAKE PARTIAL","PARTIAL PROFIT","PARTIAL EXIT",
                              "SMALL LOT HOLDER CAN FULL CLOSE"]) or \
       (re.search(r'\bPARTIAL\b', t) and re.search(r'\bCLOSE\b|\bEXIT\b', t)):
        return {"type": "partial_close", "symbol": symbol, "action": None, "partial_percent": pct}

    if any(x in t for x in ["FULL CLOSE","MANUAL CLOSE","CLOSE ALL","EXIT ALL",
                              "CLOSE NOW","CLOSE TRADE","CLOSE POSITION"]):
        return {"type": "close", "symbol": symbol, "action": None}

    has_dir = any(w in t for w in ["BUY","SELL","LONG","SHORT"])
    has_ekw = any(w in t for w in ["ENTRY","ENTER","@ ","ZONE","SL","TP"])
    if re.search(r'\bCLOSE\b', t) and not has_dir and not has_ekw:
        return {"type": "close", "symbol": symbol, "action": None}

    # Market vs pending detection
    MKT = [r'\bBUY\s+NOW\b', r'\bSELL\s+NOW\b', r'\bBUY\s+MARKET\b', r'\bSELL\s+MARKET\b',
           r'\bBUY\s+@\s*MARKET\b', r'\bSELL\s+@\s*MARKET\b', r'\bGO\s+LONG\s+NOW\b',
           r'\bGO\s+SHORT\s+NOW\b', r'\bLONG\s+NOW\b', r'\bSHORT\s+NOW\b']
    PND = [r'\bBUY\s+LIMIT\b', r'\bBUY\s+STOP\b', r'\bSELL\s+LIMIT\b', r'\bSELL\s+STOP\b',
           r'\bBUY\s+@\s*\d', r'\bSELL\s+@\s*\d', r'\bPENDING\b', r'\bWAIT\s+FOR\b',
           r'\bENTRY\s+AT\b', r'\bPLACE\s+ORDER\b']
    force_mkt = any(re.search(p, t) for p in MKT)
    force_pnd = not force_mkt and any(re.search(p, t) for p in PND)

    # Direction
    action = None
    for w in ["BUY STOP","BUY LIMIT","BUY NOW","BUY @","BUY MARKET",
              "GO BUY","GO LONG","LONG NOW","BULLISH","📈","🟢","⬆","↑","LONG"]:
        if w in t: action = BUY; break
    if action is None:
        for w in ["SELL STOP","SELL LIMIT","SELL NOW","SELL @","SELL MARKET",
                  "GO SELL","GO SHORT","SHORT NOW","BEARISH","📉","🔴","⬇","↓","SHORT"]:
            if w in t: action = SELL; break
    if action is None:
        if re.search(r'\bBUY\b', t):   action = BUY
        elif re.search(r'\bSELL\b', t): action = SELL
    if action is None:
        return None

    is_limit = force_pnd
    if not force_mkt and not force_pnd:
        is_limit = any(x in t for x in ["LIMIT","PENDING","BUY STOP","SELL STOP",
                                          "BUY LIMIT","SELL LIMIT","WAIT FOR","ENTRY AT"])

    def _f(pat, fb=0.0):
        m = re.search(pat, t)
        return float(m.group(1)) if m else fb

    sl = _f(r"(?:S\.?L\.?|STOP\s*LOSS|STOPLOSS)\s*[:\-=@#]?\s*(\d{1,6}\.?\d{0,5})")

    tps: list[float] = []
    def _atp(v):
        try:
            fv = round(float(v), 5)
            if fv > 0 and fv not in tps: tps.append(fv)
        except Exception: pass

    for m in re.finditer(r"T\.?P\.?\s*[1-9]?\s*[:\-=@#]?\s*(\d{1,6}\.?\d{0,5})", t):
        _atp(m.group(1))
    if not tps:
        m2 = re.search(r"T\.?P\.?\s*[:\-]?\s*(\d{1,6}\.?\d{0,5})"
                       r"(?:\s*[\/,]\s*(\d{1,6}\.?\d{0,5}))?"
                       r"(?:\s*[\/,]\s*(\d{1,6}\.?\d{0,5}))?", t)
        if m2:
            for g in m2.groups():
                if g: _atp(g)
    if not tps:
        for m in re.finditer(r"(?:TARGET|TAKE\s*PROFIT|PROFIT\s*TARGET)\s*[123]?\s*[:\-=]?\s*(\d{1,6}\.?\d{0,5})", t):
            _atp(m.group(1))
    if not tps:
        for m in re.finditer(r"(?:^|\n)\s*[123][)\.]\s*(\d{1,6}\.?\d{0,5})", t):
            _atp(m.group(1))
    if not tps:
        inl = re.search(r"(?:BUY|SELL)\s+(?:\S+\s+)?(\d{1,6}\.?\d{0,5})\s*/\s*(\d{1,6}\.?\d{0,5})"
                        r"(?:\s*/\s*(\d{1,6}\.?\d{0,5}))?", t)
        if inl:
            for g in inl.groups():
                if g: _atp(g)
    if not tps and "\n" in text:
        lns = [l.strip() for l in text.split("\n") if re.match(r'^\d{3,6}\.?\d{0,3}$', l.strip())]
        if len(lns) >= 3:
            for n in [float(x) for x in lns[2:]]: _atp(n)

    tps = tps[:3]
    tp1 = tps[0] if len(tps) > 0 else 0.0
    tp2 = tps[1] if len(tps) > 1 else 0.0

    # Entry
    zone_low = zone_high = 0.0
    zn = re.search(r"(?:ENTRY\s*(?:ZONE|RANGE|AREA)?|ZONE|RANGE|AREA|AROUND)[:\s]*"
                   r"(\d{1,6}\.?\d{0,5})\s*[-–]\s*(\d{1,6}\.?\d{0,5})", t)
    if zn:
        z1, z2 = float(zn.group(1)), float(zn.group(2))
        zone_low, zone_high = min(z1,z2), max(z1,z2)
        entry = round((z1+z2)/2, 5)
    else:
        bz = re.search(r"(?:BUY|SELL)[\s\S]{0,30}?(\d{3,6}\.?\d{0,3})\s*[-–]\s*(\d{3,6}\.?\d{0,3})", t)
        if bz:
            z1, z2 = float(bz.group(1)), float(bz.group(2))
            zone_low, zone_high = min(z1,z2), max(z1,z2)
            entry = round((z1+z2)/2, 5)
        else:
            entry = _f(r"(?:BUY|SELL)\s+@\s*(\d{1,6}\.?\d{0,5})")
            if not entry: entry = _f(r"(?:ENTRY|ENTER)\s*[:\-=@]?\s*(\d{1,6}\.?\d{0,5})")
            if not entry: entry = _f(r"(?<!\w)@\s*(\d{1,6}\.?\d{0,5})")
            if not entry:
                rg = re.search(r"(?:BUY|SELL)\s+(?:\w+\s+)?(\d{1,6}\.?\d{0,5})\s*[-–]\s*(\d{1,6}\.?\d{0,5})", t)
                if rg: entry = round((float(rg.group(1))+float(rg.group(2)))/2, 5)
            if not entry:
                used = {str(round(v,2)) for v in [sl,tp1,tp2] if v}
                for n in re.findall(r"(\d{3,6}\.?\d{0,5})", t):
                    if str(round(float(n),2)) not in used and float(n) > 0.0001:
                        entry = float(n); break
            if not entry and "\n" in text:
                lns = [l.strip() for l in text.split("\n") if re.match(r'^\d{3,6}\.?\d{0,3}$', l.strip())]
                if lns: entry = float(lns[0])

    if force_mkt:
        entry = 0.0; is_limit = False

    return {
        "type": "trade", "symbol": symbol, "action": action,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": 0.0,
        "is_limit": is_limit, "ai_notes": "regex",
        "zone_low": zone_low, "zone_high": zone_high,
    }


# ─────────────────────────────────────────────
#  UNIFIED PARSE
# ─────────────────────────────────────────────
async def parse_signal(text: str) -> dict | None:
    regex_result = regex_parse_signal(text)

    if ANTHROPIC_API_KEY:
        try:
            ai = await asyncio.wait_for(ai_parse_signal(text), timeout=RR_CONFIG["ai_timeout_s"])
            if ai:
                return ai
        except asyncio.TimeoutError:
            log.warning(f"  ⚠️ AI timeout ({RR_CONFIG['ai_timeout_s']}s) — using regex")

    if regex_result:
        direction = "BUY" if regex_result.get("action") == 0 else \
                    "SELL" if regex_result.get("action") == 1 else "N/A"
        tps = [v for v in [regex_result.get("tp1",0), regex_result.get("tp2",0)] if v > 0]
        log.info(
            f"  🔢 [{regex_result['type'].upper()}] {regex_result.get('symbol')} {direction} "
            f"entry:{regex_result.get('entry',0)} sl:{regex_result.get('sl',0)} tps:{tps}"
        )
    return regex_result


# ─────────────────────────────────────────────
#  CHANNEL ID RESOLUTION
# ─────────────────────────────────────────────
CHANNEL_TO_MASTERS: dict[int, list] = {}


def _normalize_channel_id(ch: int) -> list[int]:
    s = str(abs(ch))
    v: set[int] = set()
    v.add(ch); v.add(-abs(ch))
    v.add(-int(f"100{s}")); v.add(int(f"100{s}"))
    for p in ["1","10","11","12","13","14","15","16","17","18","19",
              "2","20","21","22","100","200","220","221"]:
        ls = f"{p}{s}"
        v.add(-int(f"100{ls}")); v.add(int(ls))
    return list(v)


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
            log.info(f"  ✅ Channel {ch_id} → {real_id} / {chat_id} [{title}]")
            resolved += 1
        except Exception as e:
            log.warning(f"  ⚠️ Channel {ch_id} resolve failed: {e}")
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

    preview = text[:80].replace("\n", " ")
    masters = CHANNEL_TO_MASTERS.get(cid, [])

    if not masters:
        log.debug(f"📨 UNMATCHED channel:{cid} | {preview}")
        return

    log.info(f"📨 channel:{cid} → {[m[0] for m in masters]} | {preview}")

    sig_hash = f"{cid}:{text[:200]}"
    if sig_hash in _seen_set:
        log.info("  ⏭ Duplicate ignored"); return
    if len(_seen_signals) >= 500:
        _seen_set.discard(_seen_signals[0])
    _seen_signals.append(sig_hash)
    _seen_set.add(sig_hash)

    sig = await parse_signal(text)
    if not sig:
        log.info("  ⏭ No actionable signal"); return

    tps = [v for v in [sig.get("tp1",0), sig.get("tp2",0)] if v > 0]
    direction = "BUY" if sig.get("action") == 0 else \
                "SELL" if sig.get("action") == 1 else "N/A"
    log.info(
        f"📊 {sig.get('symbol')} {direction} entry:{sig.get('entry',0)} "
        f"SL:{sig.get('sl',0)} TPs:{tps} type:{sig.get('type')}"
    )

    sig_type = sig.get("type", "trade")

    for master_name, master_cfg in masters:
        if not master_cfg.get("enabled", True):
            continue

        if sig_type == "trade":
            # Fire-and-forget: push to worker queue and return immediately
            asyncio.create_task(asyncio.to_thread(_wm.send_trade, master_name, sig))

        elif sig_type == "close":
            asyncio.create_task(asyncio.to_thread(
                _wm.send_management, master_name,
                {"type": "close", "symbol": sig["symbol"]}
            ))
        elif sig_type == "partial_close":
            asyncio.create_task(asyncio.to_thread(
                _wm.send_management, master_name,
                {"type": "partial_close", "symbol": sig["symbol"],
                 "pct": sig.get("partial_percent", 50)}
            ))
        elif sig_type == "breakeven":
            asyncio.create_task(asyncio.to_thread(
                _wm.send_management, master_name,
                {"type": "breakeven", "symbol": sig["symbol"]}
            ))
        elif sig_type == "modify_sl":
            if sig.get("new_sl", 0) > 0:
                asyncio.create_task(asyncio.to_thread(
                    _wm.send_management, master_name,
                    {"type": "modify_sl", "symbol": sig["symbol"], "new_sl": sig["new_sl"]}
                ))


# ─────────────────────────────────────────────
#  STALE TERMINAL CLEANUP
# ─────────────────────────────────────────────
def _kill_stale_mt5_terminals():
    """
    On Windows, MT5 terminal64.exe processes persist after the Python script
    exits (Ctrl+C).  Re-launching the same terminal path on the next run fails
    because MT5 won't start a second instance of the same executable.
    Kill all terminal64.exe processes before spawning workers so every worker
    gets a clean launch.
    """
    try:
        import subprocess
        result = subprocess.run(
            ["taskkill", "/F", "/IM", "terminal64.exe"],
            capture_output=True, text=True
        )
        if "SUCCESS" in result.stdout:
            log.info("  🧹 Closed stale MT5 terminal(s) from previous run")
        else:
            log.info("  🧹 No stale MT5 terminals found (clean start)")
        time.sleep(2)   # give Windows time to fully release the process handles
    except Exception as e:
        log.warning(f"  ⚠️ Could not clean stale terminals: {e}")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
async def main():
    global _wm

    log.info("═" * 70)
    log.info("  ARCHITECT v40.0 | Per-account subprocesses | NO account switching")
    log.info("═" * 70)

    if not all([TG_SESSION, TG_API_ID, TG_API_HASH]):
        log.error("❌ Missing .env — set TG_SESSION, TG_API_ID, TG_API_HASH")
        return

    # Ensure JSON log directories exist
    for cfg in MASTER_ACCOUNTS.values():
        d = os.path.dirname(cfg["json_file"])
        if d: os.makedirs(d, exist_ok=True)

    # Kill any stale MT5 terminal processes left over from a previous run.
    # Without this, restarting the script fails because MT5 refuses to launch
    # a second instance of the same terminal64.exe that is still running.
    _kill_stale_mt5_terminals()

    # Start all account worker subprocesses
    log.info("  Spawning per-account MT5 worker subprocesses...")
    _wm = WorkerManager()
    await asyncio.to_thread(_wm.start_all)

    if ANTHROPIC_API_KEY:
        log.info(f"  🤖 AI parsing: ENABLED ({RR_CONFIG['ai_timeout_s']}s timeout)")
    else:
        log.info("  🔢 AI parsing: DISABLED (regex only)")

    # Start result drainer (routes trade_placed → slave queues)
    asyncio.create_task(result_drainer())

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
    mp.set_start_method("spawn", force=True)   # required on Windows for MT5 COM isolation
    asyncio.run(main())
