"""
=============================================================================
  MAGIC TRADER → IQ OPTION BOT v1.0
=============================================================================
  - Listens DIRECTLY to Magic Trader Signals channel
  - Parses his exact signal format (asset, direction, expiry, gales)
  - Places trades automatically on IQ Option
  - Full Martingale/Gale support (2 gales)
  - Brazil timezone support
  - Dynamic stake sizing based on balance
  - Auto reconnect on disconnect
  - No tunnel needed - runs entirely on USA VPS
=============================================================================
"""

import os
import re
import asyncio
import logging
import json
import time
import ssl
import threading

from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

load_dotenv()

# ── ANALYTICS DB ──────────────────────────────────────────────────────────────
TRADES_FILE = os.environ.get(
    "TRADES_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "trades_db.json"),
)


def _load_trades():
    try:
        if os.path.exists(TRADES_FILE):
            with open(TRADES_FILE, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return []


def _save_trades(trades):
    try:
        tmp = TRADES_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(trades, f, indent=2)
        os.replace(tmp, TRADES_FILE)
    except Exception as e:
        log.error(f"Save trades error: {e}")


def record_trade(sig, gale_num, stake, placed, demo):
    trades = _load_trades()
    trade = {
        "id":         f"{int(time.time())}_{sig['asset_key']}",
        "timestamp":  datetime.now().isoformat(),
        "date":       datetime.now().strftime("%Y-%m-%d"),
        "brt_entry":  sig["entry_time"],
        "asset":      sig["asset_key"],
        "direction":  sig["direction"],
        "expiry_min": sig["expiry_min"],
        "stake":      stake,
        "gale_num":   gale_num,
        "placed":     placed,
        "result":     None,
        "pnl":        None,
        "mode":       "DEMO" if demo else "REAL",
    }
    trades.append(trade)
    _save_trades(trades)


def update_trade_result(asset_key, entry_time, result, stake):
    trades = _load_trades()
    for trade in reversed(trades):
        if (
            trade["asset"] == asset_key
            and trade["brt_entry"] == entry_time
            and trade["result"] is None
        ):
            trade["result"] = result
            trade["pnl"] = round(stake * 0.8 if result == "GAIN" else -stake, 2)
            _save_trades(trades)
            return True
    return False


def get_stats(days=7):
    trades = _load_trades()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    recent = [t for t in trades if t["timestamp"] >= cutoff]
    placed = [t for t in recent if t["placed"]]
    results = [t for t in placed if t["result"]]
    wins = [t for t in results if t["result"] == "GAIN"]
    losses = [t for t in results if t["result"] == "LOSS"]
    pnl = sum(t["pnl"] for t in results if t["pnl"])
    wr = len(wins) / len(results) * 100 if results else 0

    # Asset breakdown
    assets = {}
    for t in results:
        a = t["asset"]
        assets.setdefault(a, {"w": 0, "l": 0, "pnl": 0})
        if t["result"] == "GAIN":
            assets[a]["w"] += 1
        else:
            assets[a]["l"] += 1
        assets[a]["pnl"] += t.get("pnl", 0)

    # Gale breakdown
    gales = {0: [0, 0], 1: [0, 0], 2: [0, 0]}
    for t in results:
        g = min(t.get("gale_num", 0), 2)
        if t["result"] == "GAIN":
            gales[g][0] += 1
        else:
            gales[g][1] += 1

    lines = [
        f"Performance ({days}d)",
        f"Signals: {len(recent)} | Placed: {len(placed)}",
        f"Wins: {len(wins)} | Losses: {len(losses)} | Pending: {len(placed) - len(results)}",
        f"Win Rate: {wr:.1f}%",
        f"P&L: ${pnl:.2f}",
        "",
        "Gale breakdown:",
        f"Entry:  {gales[0][0]}W / {gales[0][1]}L",
        f"Gale 1: {gales[1][0]}W / {gales[1][1]}L",
        f"Gale 2: {gales[2][0]}W / {gales[2][1]}L",
    ]
    if assets:
        lines += ["", "Top assets:"]
        for a, s in sorted(
            assets.items(),
            key=lambda x: x[1]["w"] / (x[1]["w"] + x[1]["l"] + 0.001),
            reverse=True,
        )[:5]:
            tot = s["w"] + s["l"]
            lines.append(
                f"{a}: {s['w']/tot*100:.0f}% ({s['w']}W/{s['l']}L) ${s['pnl']:.2f}"
            )
    return "\n".join(lines)


# ── SSL FIX ───────────────────────────────────────────────────────────────────
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("magic_iq.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("MAGIC-IQ")

# ── CONFIG ────────────────────────────────────────────────────────────────────
TG_SESSION      = os.getenv("TG_SESSION", "")
TG_API_ID       = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH     = os.getenv("TG_API_HASH", "")
ALERT_BOT_TOKEN = os.getenv("ALERT_BOT_TOKEN", "")
ALERT_CHAT_ID   = int(os.getenv("ALERT_CHAT_ID", "0"))

IQ_EMAIL    = os.getenv("IQ_EMAIL", "")
IQ_PASSWORD = os.getenv("IQ_PASSWORD", "")

MAGIC_TRADER_CHANNEL = int(os.getenv("MAGIC_TRADER_CHANNEL", "-1001940077808"))

try:
    from zoneinfo import ZoneInfo
    BRT = ZoneInfo("America/Sao_Paulo")

    def brt_now():
        return datetime.now(BRT).strftime("%H:%M:%S")

    def now_brt():
        return datetime.now(BRT)

except Exception:
    def brt_now():
        return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%H:%M:%S")

    def now_brt():
        return datetime.now(timezone.utc) - timedelta(hours=3)

# ── RISK SETTINGS ─────────────────────────────────────────────────────────────
RISK_PERCENT    = float(os.getenv("RISK_PERCENT", "5.0"))
MIN_STAKE       = float(os.getenv("MIN_STAKE", "1.0"))
MAX_STAKE       = float(os.getenv("MAX_STAKE", "500.0"))
MAX_GALE_STAKE  = float(os.getenv("MAX_GALE_STAKE", "500.0"))
MAX_GALES       = 2
GALE_MULTIPLIER = 2.0
COUNTDOWN_SECS  = 30

# ── GLOBAL STATE ──────────────────────────────────────────────────────────────
account_balance = 0.0
is_demo         = True
iq_api          = None
alert_bot       = None
_seen_signals   = set()
_cancelled      = set()

# Trade result tracking: gale_key -> dict
trade_log: dict = {}


def log_trade(sig: dict, gale_num: int, stake: float, placed: bool):
    """Log a placed trade for result matching (tracks each gale separately)."""
    base_key  = f"{sig['entry_time']}_{sig['asset_key']}"
    gale_key  = f"{base_key}_g{gale_num}"
    gale_label = "Entry" if gale_num == 0 else f"Gale {gale_num}"

    trade_log[gale_key] = {
        "base_key":   base_key,
        "display":    sig["display"],
        "direction":  sig["direction"],
        "stake":      stake,
        "placed":     placed,
        "gale_num":   gale_num,
        "gale_label": gale_label,
        "result":     None,
        "time":       time.time(),
    }
    # Keep only last 100 entries
    if len(trade_log) > 100:
        oldest = min(trade_log, key=lambda k: trade_log[k]["time"])
        del trade_log[oldest]


# ── RESULT PARSER ─────────────────────────────────────────────────────────────
def parse_result(text: str):
    """
    Parse Magic Trader result messages, e.g.:
      ✅ EUR/AUD;09:05;PUT->GAIN
      ❌ USD/BRL;10:35;PUT->LOSS
      ✅ EUR/AUD;09:05;PUT->DIRECT WIN
      ✅ EUR/AUD;09:26|CALL->GAIN ✅  (1st GALE)
      ❌ EUR/AUD;09:26|CALL->LOSS (2nd GALE)
    """
    t = text.strip()
    upper = t.upper()
    gain = "GAIN" in upper or "DIRECT WIN" in upper or (
        "WIN" in upper and "GALE" not in upper
    )
    loss = "LOSS" in upper

    if not gain and not loss:
        return None

    # Detect gale number
    gale_num = 0
    if "1ST GALE" in upper or "1 GALE" in upper or "GALE 1" in upper:
        gale_num = 1
    elif "2ND GALE" in upper or "2 GALE" in upper or "GALE 2" in upper:
        gale_num = 2

    # Extract asset (longest match wins)
    asset_found = None
    for key in sorted(ASSET_MAP.keys(), key=len, reverse=True):
        if key.upper() in upper:
            asset_found = key
            break
    if not asset_found:
        return None

    # Extract time
    time_found = None
    m = re.search(r"(\d{2}:\d{2})", t)
    if m:
        time_found = m.group(1)

    return {
        "asset_key": asset_found,
        "time":      time_found,
        "result":    "GAIN" if gain else "LOSS",
        "gale_num":  gale_num,
    }


# ── ASSET MAPS ────────────────────────────────────────────────────────────────
ASSET_MAP = {
    "EUR/AUD": "EURAUD-OTC", "EUR/USD": "EURUSD-OTC", "EUR/GBP": "EURGBP-OTC",
    "EUR/JPY": "EURJPY-OTC", "EUR/CHF": "EURCHF-OTC", "EUR/CAD": "EURCAD-OTC",
    "GBP/USD": "GBPUSD-OTC", "GBP/JPY": "GBPJPY-OTC", "GBP/CHF": "GBPCHF-OTC",
    "GBP/AUD": "GBPAUD-OTC", "GBP/CAD": "GBPCAD-OTC",
    "USD/JPY": "USDJPY-OTC", "USD/CHF": "USDCHF-OTC", "USD/CAD": "USDCAD-OTC",
    "AUD/USD": "AUDUSD-OTC", "AUD/JPY": "AUDJPY-OTC",
    "NZD/USD": "NZDUSD-OTC", "USD/BRL": "USDBRL-OTC",
    "NZD/JPY": "NZDJPY-OTC", "CAD/JPY": "CADJPY-OTC",
    "AUD/CAD": "AUDCAD-OTC", "AUD/NZD": "AUDNZD-OTC",
    "XAUUSD":  "XAUUSD-OTC", "GOLD":    "XAUUSD-OTC", "XAU/USD": "XAUUSD-OTC",
}

LIVE_ASSET_MAP = {
    "EUR/AUD": "EURAUD", "EUR/USD": "EURUSD", "EUR/GBP": "EURGBP",
    "EUR/JPY": "EURJPY", "EUR/CHF": "EURCHF", "EUR/CAD": "EURCAD",
    "GBP/USD": "GBPUSD", "GBP/JPY": "GBPJPY", "GBP/CHF": "GBPCHF",
    "GBP/AUD": "GBPAUD", "GBP/CAD": "GBPCAD",
    "USD/JPY": "USDJPY", "USD/CHF": "USDCHF", "USD/CAD": "USDCAD",
    "AUD/USD": "AUDUSD", "AUD/JPY": "AUDJPY",
    "NZD/USD": "NZDUSD", "USD/BRL": "USDBRL",
    "NZD/JPY": "NZDJPY", "CAD/JPY": "CADJPY",
    "AUD/CAD": "AUDCAD", "AUD/NZD": "AUDNZD",
    "XAUUSD":  "XAUUSD", "GOLD":    "XAUUSD", "XAU/USD": "XAUUSD",
}

# ── HELPERS ───────────────────────────────────────────────────────────────────
def calculate_stake(balance: float, gale_num: int = 0) -> float:
    base = max(balance * RISK_PERCENT / 100, MIN_STAKE)
    base = min(base, MAX_STAKE)
    gale_stake = round(base * (GALE_MULTIPLIER ** gale_num), 2)
    return min(gale_stake, MAX_GALE_STAKE)


def mode():
    return "DEMO" if is_demo else "REAL"


# ── IQ OPTION ─────────────────────────────────────────────────────────────────
def iq_connect() -> bool:
    global iq_api, account_balance, is_demo
    try:
        from iqoptionapi.stable_api import IQ_Option
        log.info("Connecting to IQ Option...")
        iq_api = IQ_Option(IQ_EMAIL, IQ_PASSWORD)
        check, reason = iq_api.connect()
        if not check:
            log.error(f"IQ connect failed: {reason}")
            return False
        time.sleep(2)
        iq_api.change_balance("PRACTICE" if is_demo else "REAL")
        time.sleep(1)
        bal = iq_api.get_balance()
        if bal is not None:
            account_balance = float(bal)
        log.info(f"IQ Option connected! [{mode()}] Balance: ${account_balance:.2f}")
        return True
    except Exception as e:
        log.error(f"IQ connect error: {e}")
        return False


def iq_place_trade(asset: str, direction: str, amount: float, expiry_min: int) -> bool:
    global iq_api, account_balance
    try:
        if iq_api is None:
            if not iq_connect():
                return False

        if is_demo:
            iq_asset = ASSET_MAP.get(asset, asset)
        else:
            iq_asset = LIVE_ASSET_MAP.get(asset, ASSET_MAP.get(asset, asset))

        action = "call" if direction == "call" else "put"
        log.info(f"Placing: {iq_asset} {action} ${amount} {expiry_min}min [{mode()}]")
        check, order_id = iq_api.buy(amount, iq_asset, action, expiry_min)

        if check:
            log.info(f"TRADE PLACED! Order: {order_id}")
            time.sleep(1)
            bal = iq_api.get_balance()
            if bal:
                account_balance = float(bal)
            return True

        # Fallback: try non-OTC variant
        iq_asset2 = iq_asset.replace("-OTC", "")
        log.info(f"Retrying non-OTC: {iq_asset2}")
        check2, order_id2 = iq_api.buy(amount, iq_asset2, action, expiry_min)
        if check2:
            log.info(f"TRADE PLACED (non-OTC)! Order: {order_id2}")
            bal = iq_api.get_balance()
            if bal:
                account_balance = float(bal)
            return True

        log.error(f"Trade failed: {order_id}")
        return False

    except Exception as e:
        log.error(f"Trade error: {e}")
        iq_connect()
        return False


def iq_keepalive():
    """Keep IQ Option connection alive in a background thread."""
    while True:
        time.sleep(60)
        try:
            if iq_api is None:
                iq_connect()
                continue
            bal = iq_api.get_balance()
            if bal is not None:
                global account_balance
                account_balance = float(bal)
            else:
                log.warning("Keepalive: reconnecting...")
                iq_connect()
        except Exception as e:
            log.warning(f"Keepalive error: {e}")
            iq_connect()


# ── SIGNAL PARSER ─────────────────────────────────────────────────────────────
def parse_magic_trader(text: str):
    t = text.strip()
    upper = t.upper()

    if any(x in upper for x in ["GAIN", "LOSS", "WIN", "DIRECT WIN"]):
        return None
    if "expiration" not in t.lower() and "expiry" not in t.lower():
        return None

    exp = re.search(r"(\d+)[- ]minute", t, re.IGNORECASE)
    if not exp:
        return None
    expiry_min = int(exp.group(1))

    asset_key = None
    for key in sorted(ASSET_MAP, key=len, reverse=True):
        if key.upper() in upper:
            asset_key = key
            break
    if not asset_key:
        return None

    direction = None
    if "PUT" in upper:
        direction = "put"
    elif "CALL" in upper:
        direction = "call"
    elif any(x in t for x in ["↑", "🟢"]):
        direction = "call"
    elif any(x in t for x in ["↓", "🟥"]):
        direction = "put"
    if not direction:
        return None

    times = re.findall(r"\b(\d{2}:\d{2})\b", t)
    if not times:
        return None
    entry_time = times[0]

    gale_times = re.findall(r"TIME TO (\d{2}:\d{2})", t, re.IGNORECASE)[:MAX_GALES]
    if not gale_times:
        gale_times = re.findall(
            r"(?:1st|2nd) GALE.*?(\d{2}:\d{2})", t, re.IGNORECASE
        )[:MAX_GALES]

    return {
        "asset_key":  asset_key,
        "asset":      ASSET_MAP[asset_key],
        "display":    asset_key,
        "direction":  direction,
        "expiry_min": expiry_min,
        "entry_time": entry_time,
        "gale_times": gale_times,
        "id":         f"{entry_time}_{asset_key}_{direction}",
    }


# ── TIME HELPERS ──────────────────────────────────────────────────────────────
def brt_to_local(time_str: str) -> datetime:
    try:
        nb = now_brt()
        t = datetime.strptime(time_str, "%H:%M")
        if hasattr(nb, "tzinfo") and nb.tzinfo:
            from zoneinfo import ZoneInfo as _ZI
            BRT_zone = _ZI("America/Sao_Paulo")
            dt_brt = nb.replace(
                hour=t.hour, minute=t.minute, second=0, microsecond=0
            )
            if dt_brt < nb:
                dt_brt += timedelta(days=1)
            return dt_brt.astimezone().replace(tzinfo=None)
        else:
            dt = datetime.now().replace(
                hour=t.hour, minute=t.minute, second=0, microsecond=0
            )
            if dt < datetime.now():
                dt += timedelta(days=1)
            return dt
    except Exception as e:
        log.error(f"Time parse error: {e}")
        return datetime.now() + timedelta(minutes=1)


# ── ALERT HELPERS ─────────────────────────────────────────────────────────────
async def send_alert(sig: dict, gale_num: int = 0):
    global alert_bot
    if not alert_bot:
        return None

    stake = calculate_stake(account_balance, gale_num)
    arrow = "UP (CALL)" if sig["direction"] == "call" else "DOWN (PUT)"
    sig_id = sig["id"] + (f"_g{gale_num}" if gale_num > 0 else "")
    labels = [
        "SIGNAL - FIRES IN 30s",
        "GALE 1 - FIRES IN 30s",
        "GALE 2 FINAL - FIRES IN 30s",
    ]

    msg = (
        f"--- {labels[gale_num]} ---\n"
        f"Asset:     {sig['display']}\n"
        f"Direction: {arrow}\n"
        f"Expiry:    {sig['expiry_min']} minutes\n"
        f"Entry:     {sig['entry_time']} BRT\n"
        f"Stake:     ${stake}\n"
        f"Balance:   ${account_balance:.2f}\n"
        f"Mode:      {mode()}\n"
        f"BRT Now:   {brt_now()}\n\n"
        f"Trade fires in 30s! Tap CANCEL to skip."
    )
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("CANCEL TRADE", callback_data=f"cancel_{sig_id}")]]
    )
    try:
        msg_obj = await alert_bot.send_message(
            chat_id=ALERT_CHAT_ID, text=msg, reply_markup=keyboard
        )
        return msg_obj.message_id
    except Exception as e:
        log.error(f"Alert error: {e}")
        return None


# ── TRADE EXECUTOR ────────────────────────────────────────────────────────────
async def execute_with_countdown(sig: dict, gale_num: int = 0):
    sig_id = sig["id"] + (f"_g{gale_num}" if gale_num > 0 else "")
    stake = calculate_stake(account_balance, gale_num)

    msg_id = await send_alert(sig, gale_num)
    await asyncio.sleep(COUNTDOWN_SECS)

    if sig_id in _cancelled:
        _cancelled.discard(sig_id)
        log.info(f"Cancelled: {sig['display']}")
        if msg_id:
            try:
                await alert_bot.edit_message_text(
                    chat_id=ALERT_CHAT_ID,
                    message_id=msg_id,
                    text=f"CANCELLED\n{sig['display']} {sig['direction'].upper()} ${stake}",
                )
            except Exception:
                pass
        return

    log.info(f"FIRING: {sig['display']} {sig['direction']} ${stake}")

    loop = asyncio.get_event_loop()
    success = await loop.run_in_executor(
        None, iq_place_trade, sig["asset"], sig["direction"], stake, sig["expiry_min"]
    )

    log_trade(sig, gale_num, stake, success)
    record_trade(sig, gale_num, stake, success, is_demo)

    arrow = "UP" if sig["direction"] == "call" else "DOWN"
    result = (
        f"{'TRADE PLACED!' if success else 'TRADE FAILED!'}\n"
        f"{sig['display']} {arrow} ${stake} [{mode()}]\n"
        f"Balance: ${account_balance:.2f}"
    )
    if msg_id:
        try:
            await alert_bot.edit_message_text(
                chat_id=ALERT_CHAT_ID, message_id=msg_id, text=result
            )
        except Exception:
            pass
    else:
        try:
            await alert_bot.send_message(chat_id=ALERT_CHAT_ID, text=result)
        except Exception:
            pass


# ── SCHEDULERS ────────────────────────────────────────────────────────────────
async def schedule_gales(sig: dict):
    for i, gale_time in enumerate(sig["gale_times"][:MAX_GALES]):
        try:
            gale_local = brt_to_local(gale_time)
            alert_at = gale_local - timedelta(seconds=COUNTDOWN_SECS)
            wait_secs = (alert_at - datetime.now()).total_seconds()
            if wait_secs > 0:
                log.info(f"Gale {i+1} alert in {wait_secs:.0f}s (BRT: {gale_time})")
                await asyncio.sleep(wait_secs)
            elif (gale_local - datetime.now()).total_seconds() < -30:
                log.warning(f"Gale {i+1} already passed, skipping")
                continue
            await execute_with_countdown(sig, gale_num=i + 1)
        except Exception as e:
            log.error(f"Gale {i+1} error: {e}")


async def schedule_entry(sig: dict):
    try:
        entry_local = brt_to_local(sig["entry_time"])
        now = datetime.now()

        alert_at = entry_local - timedelta(seconds=COUNTDOWN_SECS)
        wait_secs = (alert_at - now).total_seconds()

        log.info(f"Entry: {sig['entry_time']} BRT | Alert in: {wait_secs:.0f}s")

        if wait_secs > 3600:
            log.warning(f"Entry too far ahead ({wait_secs:.0f}s), skipping")
            return
        if (entry_local - now).total_seconds() < -30:
            log.warning("Entry already passed, skipping")
            return
        if wait_secs > 0:
            await asyncio.sleep(wait_secs)

        await execute_with_countdown(sig, gale_num=0)
        if sig.get("gale_times"):
            asyncio.create_task(schedule_gales(sig))
    except Exception as e:
        log.error(f"schedule_entry error: {e}")


# ── BOT COMMANDS ──────────────────────────────────────────────────────────────
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Magic → IQ Option Bot v1.0\n"
        f"IQ Option: {'Connected' if iq_api else 'Disconnected'}\n"
        f"Mode:      {mode()}\n"
        f"Balance:   ${account_balance:.2f}\n"
        f"Stake:     ${calculate_stake(account_balance):.2f}\n"
        f"Gale 1:    ${calculate_stake(account_balance, 1):.2f}\n"
        f"Gale 2:    ${calculate_stake(account_balance, 2):.2f}\n"
        f"Risk:      {RISK_PERCENT}%\n"
        f"BRT:       {brt_now()}"
    )


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global account_balance
    if context.args:
        try:
            account_balance = float(context.args[0])
            await update.message.reply_text(
                f"Balance: ${account_balance:.2f}\n"
                f"Stake:  ${calculate_stake(account_balance):.2f}\n"
                f"Gale 1: ${calculate_stake(account_balance, 1):.2f}\n"
                f"Gale 2: ${calculate_stake(account_balance, 2):.2f}"
            )
        except ValueError:
            await update.message.reply_text("Usage: /balance 100")
    else:
        loop = asyncio.get_event_loop()
        bal = await loop.run_in_executor(
            None, lambda: iq_api.get_balance() if iq_api else None
        )
        if bal is not None:
            account_balance = float(bal)
        await update.message.reply_text(f"Balance: ${account_balance:.2f}")


async def cmd_risk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global RISK_PERCENT
    if context.args:
        try:
            RISK_PERCENT = float(context.args[0])
            await update.message.reply_text(
                f"Risk: {RISK_PERCENT}%\n"
                f"Stake: ${calculate_stake(account_balance):.2f}"
            )
        except ValueError:
            await update.message.reply_text("Usage: /risk 5")
    else:
        await update.message.reply_text(f"Current risk: {RISK_PERCENT}%")


async def cmd_demo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global is_demo
    is_demo = True
    if iq_api:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: iq_api.change_balance("PRACTICE"))
        bal = await loop.run_in_executor(None, iq_api.get_balance)
        if bal is not None:
            global account_balance
            account_balance = float(bal)
    await update.message.reply_text(f"Switched to DEMO!\nBalance: ${account_balance:.2f}")


async def cmd_real(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global is_demo
    is_demo = False
    if iq_api:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: iq_api.change_balance("REAL"))
        bal = await loop.run_in_executor(None, iq_api.get_balance)
        if bal is not None:
            global account_balance
            account_balance = float(bal)
    await update.message.reply_text(f"Switched to REAL!\nBalance: ${account_balance:.2f}")


async def cmd_connect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Reconnecting to IQ Option...")
    loop = asyncio.get_event_loop()
    success = await loop.run_in_executor(None, iq_connect)
    await update.message.reply_text(
        f"{'Connected!' if success else 'Failed — check credentials'}\n"
        f"Balance: ${account_balance:.2f}"
    )


async def cmd_brt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Brazil time: {brt_now()}")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    days = 7
    if context.args:
        try:
            days = int(context.args[0])
        except ValueError:
            pass
    await update.message.reply_text(get_stats(days))


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(get_stats(30))


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not trade_log:
        await update.message.reply_text("No trades yet.")
        return

    wins   = sum(1 for t in trade_log.values() if t["result"] == "GAIN")
    losses = sum(1 for t in trade_log.values() if t["result"] == "LOSS")
    placed = sum(1 for t in trade_log.values() if t["placed"])
    total  = len(trade_log)
    win_rate = f"{wins/(wins+losses)*100:.0f}%" if (wins + losses) > 0 else "N/A"

    lines = [
        f"Recent Trades ({total} total)",
        f"Placed: {placed} | Win: {wins} | Loss: {losses}",
        f"Win Rate: {win_rate}",
        "---",
    ]
    for t in list(trade_log.values())[-10:]:
        arrow  = "UP" if t["direction"] == "call" else "DOWN"
        result = t["result"] or "Pending"
        emoji  = "✅" if result == "GAIN" else "❌" if result == "LOSS" else "⏳"
        placed_str = "placed" if t["placed"] else "FAILED"
        lines.append(
            f"{emoji} {t['display']} {arrow} ${t['stake']:.2f} - {result} ({placed_str})"
        )

    await update.message.reply_text("\n".join(lines))


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Cancelled!")
    if query.data.startswith("cancel_"):
        _cancelled.add(query.data.replace("cancel_", ""))
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text("Trade cancelled!")


# ── MAGIC TRADER LISTENER ─────────────────────────────────────────────────────
tg_listener = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)


@tg_listener.on(events.NewMessage(chats=[MAGIC_TRADER_CHANNEL]))
async def on_magic_trader(event):
    text = event.raw_text or ""
    if not text.strip():
        return
    log.info(f"MAGIC TRADER: {text[:80].replace(chr(10), ' ')}")

    sig_hash = text[:150]
    if sig_hash in _seen_signals:
        return
    _seen_signals.add(sig_hash)
    if len(_seen_signals) > 200:
        _seen_signals.discard(next(iter(_seen_signals)))

    # Check for result message first
    result = parse_result(text)
    if result:
        gale_num = result.get("gale_num", 0)
        key = (
            f"{result['time']}_{result['asset_key']}_g{gale_num}"
            if result["time"]
            else None
        )

        # Find the matching trade entry; fall back across gale levels
        trade = trade_log.get(key) if key else None
        if not trade and result["time"]:
            for g in range(MAX_GALES + 1):
                k2 = f"{result['time']}_{result['asset_key']}_g{g}"
                if k2 in trade_log and trade_log[k2]["result"] is None:
                    trade = trade_log[k2]
                    key   = k2
                    break

        emoji = "✅" if result["result"] == "GAIN" else "❌"
        label = "ENTRY" if gale_num == 0 else f"GALE {gale_num}"

        result_text = (
            f"{emoji} Magic Trader: {result['result']}\n"
            f"Asset:  {result['asset_key']}\n"
            f"Level:  {label}"
        )
        if trade:
            arrow     = "UP" if trade["direction"] == "call" else "DOWN"
            our_label = trade["gale_label"]
            result_text += (
                f"\nOur {our_label}: {arrow} ${trade['stake']:.2f}\n"
                f"Placed: {'Yes' if trade['placed'] else 'FAILED'}\n"
                f"Result: {result['result']}"
            )
            trade["result"] = result["result"]
            try:
                loop = asyncio.get_event_loop()
                bal = await loop.run_in_executor(
                    None, lambda: iq_api.get_balance() if iq_api else None
                )
                if bal:
                    account_balance = float(bal)
                    result_text += f"\nBalance: ${account_balance:.2f}"
            except Exception:
                pass
        else:
            result_text += "\n(No matching trade found)"

        try:
            await alert_bot.send_message(chat_id=ALERT_CHAT_ID, text=result_text)
        except Exception:
            pass
        log.info(f"Result: {result['asset_key']} {label} {result['result']}")
        return

    sig = parse_magic_trader(text)
    if not sig:
        log.info("Not a trade signal")
        return

    log.info(
        f"SIGNAL: {sig['display']} {sig['direction']} {sig['expiry_min']}min "
        f"BRT:{sig['entry_time']} gales:{sig['gale_times']}"
    )
    asyncio.create_task(schedule_entry(sig))


# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    global alert_bot

    log.info("=" * 55)
    log.info("  MAGIC TRADER → IQ OPTION BOT v1.0")
    log.info(f"  Risk: {RISK_PERCENT}% | Gales: {MAX_GALES} | Countdown: {COUNTDOWN_SECS}s")
    log.info("=" * 55)

    if not all([TG_SESSION, TG_API_ID, TG_API_HASH]):
        log.error("Missing TG credentials in .env")
        return
    if not ALERT_BOT_TOKEN or not ALERT_CHAT_ID:
        log.error("Missing ALERT_BOT_TOKEN or ALERT_CHAT_ID in .env")
        return
    if not IQ_EMAIL or not IQ_PASSWORD:
        log.error("Missing IQ_EMAIL or IQ_PASSWORD in .env")
        return

    loop = asyncio.get_event_loop()
    connected = await loop.run_in_executor(None, iq_connect)

    threading.Thread(target=iq_keepalive, daemon=True).start()

    # Build the PTB Application first; reuse its internal bot as alert_bot.
    # Creating a separate Bot() instance with the same token would cause a
    # Telegram 409 Conflict because both would open competing getUpdates
    # long-poll connections.
    ptb_app = Application.builder().token(ALERT_BOT_TOKEN).build()
    ptb_app.add_handler(CommandHandler("status",  cmd_status))
    ptb_app.add_handler(CommandHandler("balance", cmd_balance))
    ptb_app.add_handler(CommandHandler("risk",    cmd_risk))
    ptb_app.add_handler(CommandHandler("demo",    cmd_demo))
    ptb_app.add_handler(CommandHandler("real",    cmd_real))
    ptb_app.add_handler(CommandHandler("connect", cmd_connect))
    ptb_app.add_handler(CommandHandler("brt",     cmd_brt))
    ptb_app.add_handler(CommandHandler("trades",  cmd_trades))
    ptb_app.add_handler(CommandHandler("stats",   cmd_stats))
    ptb_app.add_handler(CommandHandler("report",  cmd_report))
    ptb_app.add_handler(CallbackQueryHandler(button_callback))

    async with ptb_app:
        # ptb_app.__aenter__ calls initialize(), which sets up the HTTP client.
        # Point alert_bot at the same Bot object — no second connection.
        alert_bot = ptb_app.bot

        me = await alert_bot.get_me()
        log.info(f"Alert bot: @{me.username}")

        await alert_bot.send_message(
            chat_id=ALERT_CHAT_ID,
            text=(
                "Magic Trader → IQ Option Bot v1.0 Started!\n\n"
                f"IQ Option: {'Connected' if connected else 'Use /connect'}\n"
                f"Mode:      {mode()}\n"
                f"Balance:   ${account_balance:.2f}\n"
                f"Stake:     ${calculate_stake(account_balance):.2f}\n"
                f"Gale 1:    ${calculate_stake(account_balance, 1):.2f}\n"
                f"Gale 2:    ${calculate_stake(account_balance, 2):.2f}\n"
                f"BRT:       {brt_now()}\n\n"
                "Commands:\n"
                "/demo      - use demo account\n"
                "/real      - use real account\n"
                "/connect   - reconnect IQ Option\n"
                "/balance   - check balance\n"
                "/risk 5    - change risk %\n"
                "/status    - full status\n"
                "/trades    - recent trade results\n"
                "/stats     - 7-day performance\n"
                "/report    - 30-day performance\n"
                "/brt       - Brazil time\n\n"
                "Watching Magic Trader... Trade fires 30s before entry!"
            ),
        )

        await ptb_app.start()
        await ptb_app.updater.start_polling(drop_pending_updates=True)

        while True:
            try:
                await tg_listener.start()
                log.info("Telegram connected | Watching Magic Trader...")
                await tg_listener.run_until_disconnected()
            except Exception as e:
                log.error(f"Listener error: {e}")
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
