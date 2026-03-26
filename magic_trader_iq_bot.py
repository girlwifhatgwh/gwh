"""
=============================================================================
  MAGIC TRADER -> IQ OPTION BOT v1.0
=============================================================================
  - Listens DIRECTLY to Magic Trader Signals channel
  - Parses his exact signal format (asset, direction, expiry, gales)
  - Places trades automatically on IQ Option
  - Full Martingale/Gale support (2 gales)
  - Brazil timezone support
  - Dynamic stake sizing based on balance
  - Auto reconnect on disconnect
  - No tunnel needed - runs entirely on VPS
=============================================================================
"""

import asyncio
import json as _json
import logging
import os
import re
import ssl
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)
from telethon import TelegramClient, events
from telethon.sessions import StringSession

load_dotenv()


# -----------------------------------------------------------------------------
# Logging / SSL
# -----------------------------------------------------------------------------
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("magic_iq.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("MAGIC-IQ")


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
TG_SESSION = os.getenv("TG_SESSION", "")
TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")
ALERT_BOT_TOKEN = os.getenv("ALERT_BOT_TOKEN", "")
ALERT_CHAT_ID = int(os.getenv("ALERT_CHAT_ID", "0"))

IQ_EMAIL = os.getenv("IQ_EMAIL", "")
IQ_PASSWORD = os.getenv("IQ_PASSWORD", "")

MAGIC_TRADER_CHANNEL = int(os.getenv("MAGIC_TRADER_CHANNEL", "-1001940077808"))

# Risk
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "5.0"))
MIN_STAKE = float(os.getenv("MIN_STAKE", "1.0"))
MAX_STAKE = float(os.getenv("MAX_STAKE", "500.0"))  # max base stake
MAX_GALE_STAKE = float(os.getenv("MAX_GALE_STAKE", "500.0"))
MAX_GALES = int(os.getenv("MAX_GALES", "2"))
GALE_MULTIPLIER = float(os.getenv("GALE_MULTIPLIER", "2.0"))
COUNTDOWN_SECS = int(os.getenv("COUNTDOWN_SECS", "30"))

TRADES_FILE = Path(os.getenv("TRADES_FILE", "trades_db.json"))

try:
    from zoneinfo import ZoneInfo

    BRT = ZoneInfo("America/Sao_Paulo")

    def brt_now() -> str:
        return datetime.now(BRT).strftime("%H:%M:%S")

    def now_brt() -> datetime:
        return datetime.now(BRT)

except Exception:

    def brt_now() -> str:
        return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%H:%M:%S")

    def now_brt() -> datetime:
        return datetime.now(timezone.utc) - timedelta(hours=3)


# -----------------------------------------------------------------------------
# State
# -----------------------------------------------------------------------------
account_balance = 0.0
is_demo = True
iq_api = None
alert_bot: Bot | None = None
_seen_signals: set[str] = set()
_cancelled: set[str] = set()

# Trade result tracking:
# key = f"{entry_time}_{asset_key}_g{gale_num}"
trade_log: dict[str, dict[str, Any]] = {}


# -----------------------------------------------------------------------------
# Asset map (Magic Trader -> IQ Option)
# -----------------------------------------------------------------------------
ASSET_MAP = {
    # Forex OTC (demo & weekends)
    "EUR/AUD": "EURAUD-OTC",
    "EUR/USD": "EURUSD-OTC",
    "EUR/GBP": "EURGBP-OTC",
    "EUR/JPY": "EURJPY-OTC",
    "EUR/CHF": "EURCHF-OTC",
    "EUR/CAD": "EURCAD-OTC",
    "GBP/USD": "GBPUSD-OTC",
    "GBP/JPY": "GBPJPY-OTC",
    "GBP/CHF": "GBPCHF-OTC",
    "GBP/AUD": "GBPAUD-OTC",
    "GBP/CAD": "GBPCAD-OTC",
    "USD/JPY": "USDJPY-OTC",
    "USD/CHF": "USDCHF-OTC",
    "USD/CAD": "USDCAD-OTC",
    "AUD/USD": "AUDUSD-OTC",
    "AUD/JPY": "AUDJPY-OTC",
    "NZD/USD": "NZDUSD-OTC",
    "USD/BRL": "USDBRL-OTC",
    "NZD/JPY": "NZDJPY-OTC",
    "CAD/JPY": "CADJPY-OTC",
    "AUD/CAD": "AUDCAD-OTC",
    "AUD/NZD": "AUDNZD-OTC",
    "XAUUSD": "XAUUSD-OTC",
    "GOLD": "XAUUSD-OTC",
    "XAU/USD": "XAUUSD-OTC",
}

LIVE_ASSET_MAP = {
    "EUR/AUD": "EURAUD",
    "EUR/USD": "EURUSD",
    "EUR/GBP": "EURGBP",
    "EUR/JPY": "EURJPY",
    "EUR/CHF": "EURCHF",
    "EUR/CAD": "EURCAD",
    "GBP/USD": "GBPUSD",
    "GBP/JPY": "GBPJPY",
    "GBP/CHF": "GBPCHF",
    "GBP/AUD": "GBPAUD",
    "GBP/CAD": "GBPCAD",
    "USD/JPY": "USDJPY",
    "USD/CHF": "USDCHF",
    "USD/CAD": "USDCAD",
    "AUD/USD": "AUDUSD",
    "AUD/JPY": "AUDJPY",
    "NZD/USD": "NZDUSD",
    "USD/BRL": "USDBRL",
    "NZD/JPY": "NZDJPY",
    "CAD/JPY": "CADJPY",
    "AUD/CAD": "AUDCAD",
    "AUD/NZD": "AUDNZD",
    "XAUUSD": "XAUUSD",
    "GOLD": "XAUUSD",
    "XAU/USD": "XAUUSD",
}


# -----------------------------------------------------------------------------
# Persistence / analytics helpers
# -----------------------------------------------------------------------------
def _load_trades() -> list[dict[str, Any]]:
    try:
        if TRADES_FILE.exists():
            with TRADES_FILE.open(encoding="utf-8") as f:
                data = _json.load(f)
                if isinstance(data, list):
                    return data
    except Exception as exc:
        log.warning(f"Load trades error: {exc}")
    return []


def _save_trades(trades: list[dict[str, Any]]) -> None:
    try:
        TRADES_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = TRADES_FILE.with_suffix(TRADES_FILE.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            _json.dump(trades, f, indent=2)
        os.replace(tmp, TRADES_FILE)
    except Exception as exc:
        log.error(f"Save trades error: {exc}")


def record_trade(sig: dict[str, Any], gale_num: int, stake: float, placed: bool, demo: bool) -> None:
    trades = _load_trades()
    trades.append(
        {
            "id": f"{int(time.time())}_{sig['asset_key']}_g{gale_num}",
            "timestamp": datetime.now().isoformat(),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "brt_entry": sig["entry_time"],
            "asset": sig["asset_key"],
            "direction": sig["direction"],
            "expiry_min": sig["expiry_min"],
            "stake": stake,
            "gale_num": gale_num,
            "placed": placed,
            "result": None,
            "pnl": None,
            "mode": "DEMO" if demo else "REAL",
        }
    )
    _save_trades(trades)


def update_trade_result(
    asset_key: str,
    entry_time: str,
    gale_num: int,
    result: str,
    stake: float,
) -> bool:
    trades = _load_trades()
    for trade in reversed(trades):
        if (
            trade.get("asset") == asset_key
            and trade.get("brt_entry") == entry_time
            and int(trade.get("gale_num", 0)) == int(gale_num)
            and trade.get("result") is None
        ):
            trade["result"] = result
            trade["pnl"] = round(stake * 0.8 if result == "GAIN" else -stake, 2)
            _save_trades(trades)
            return True
    return False


def get_stats(days: int = 7) -> str:
    trades = _load_trades()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    recent = [t for t in trades if t.get("timestamp", "") >= cutoff]
    placed = [t for t in recent if t.get("placed")]
    results = [t for t in placed if t.get("result")]
    wins = [t for t in results if t.get("result") == "GAIN"]
    losses = [t for t in results if t.get("result") == "LOSS"]
    pnl = sum(float(t.get("pnl", 0) or 0) for t in results)
    wr = (len(wins) / len(results) * 100) if results else 0.0

    assets: dict[str, dict[str, float]] = {}
    for t in results:
        asset = str(t.get("asset", "UNKNOWN"))
        assets.setdefault(asset, {"w": 0, "l": 0, "pnl": 0.0})
        if t.get("result") == "GAIN":
            assets[asset]["w"] += 1
        else:
            assets[asset]["l"] += 1
        assets[asset]["pnl"] += float(t.get("pnl", 0) or 0.0)

    gales = {0: [0, 0], 1: [0, 0], 2: [0, 0]}
    for t in results:
        gale_num = min(int(t.get("gale_num", 0)), 2)
        if t.get("result") == "GAIN":
            gales[gale_num][0] += 1
        else:
            gales[gale_num][1] += 1

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
        sorted_assets = sorted(
            assets.items(),
            key=lambda x: x[1]["w"] / (x[1]["w"] + x[1]["l"] + 0.001),
            reverse=True,
        )
        for asset, stats in sorted_assets[:5]:
            total = stats["w"] + stats["l"]
            win_rate = (stats["w"] / total * 100) if total else 0
            lines.append(
                f"{asset}: {win_rate:.0f}% ({int(stats['w'])}W/{int(stats['l'])}L) ${stats['pnl']:.2f}"
            )
    return "\n".join(lines)


# -----------------------------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------------------------
def calculate_stake(balance: float, gale_num: int = 0) -> float:
    base = max(balance * RISK_PERCENT / 100, MIN_STAKE)
    base = min(base, MAX_STAKE)
    gale_stake = round(base * (GALE_MULTIPLIER ** gale_num), 2)
    return min(gale_stake, MAX_GALE_STAKE)


def mode() -> str:
    return "DEMO" if is_demo else "REAL"


def _trade_key(entry_time: str, asset_key: str, gale_num: int) -> str:
    return f"{entry_time}_{asset_key}_g{gale_num}"


def _base_key(sig: dict[str, Any]) -> str:
    return f"{sig['entry_time']}_{sig['asset_key']}"


def log_trade(sig: dict[str, Any], gale_num: int, stake: float, placed: bool) -> None:
    """Track each trade/gale for later result matching."""
    key = _trade_key(sig["entry_time"], sig["asset_key"], gale_num)
    gale_label = "Entry" if gale_num == 0 else f"Gale {gale_num}"
    trade_log[key] = {
        "base_key": _base_key(sig),
        "asset_key": sig["asset_key"],
        "entry_time": sig["entry_time"],
        "display": sig["display"],
        "direction": sig["direction"],
        "stake": stake,
        "placed": placed,
        "gale_num": gale_num,
        "gale_label": gale_label,
        "result": None,
        "time": time.time(),
    }
    if len(trade_log) > 200:
        oldest = min(trade_log, key=lambda k: trade_log[k]["time"])
        del trade_log[oldest]


def _normalized_symbol(symbol: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", symbol.upper())


def _build_symbol_candidates(asset: str) -> list[str]:
    """
    Build a robust list of IQ Option symbol candidates.
    Supports inputs like EUR/GBP (asset key), EURGBP, EURGBP-OTC.
    """
    candidates: list[str] = []

    def add(sym: str | None) -> None:
        if not sym:
            return
        if sym not in candidates:
            candidates.append(sym)

    if asset in ASSET_MAP:
        # asset key style: EUR/GBP
        add(ASSET_MAP.get(asset))
        add(LIVE_ASSET_MAP.get(asset))
        compact = asset.replace("/", "")
        add(compact)
        add(f"{compact}-OTC")
    else:
        # already symbol style
        add(asset)
        compact = asset.replace("/", "")
        add(compact)

    for base in list(candidates):
        if base.endswith("-OTC"):
            add(base.replace("-OTC", ""))
        else:
            add(f"{base}-OTC")

    return candidates


def _resolve_open_symbol_variants() -> dict[str, str]:
    """
    Map normalized symbols to open symbols returned by IQ Option.
    """
    if iq_api is None:
        return {}
    normalized_to_open: dict[str, str] = {}
    try:
        open_times = iq_api.get_all_open_time() or {}
        for market in ("turbo", "binary", "digital"):
            market_data = open_times.get(market, {})
            if not isinstance(market_data, dict):
                continue
            for symbol, data in market_data.items():
                if isinstance(data, dict) and data.get("open"):
                    normalized_to_open[_normalized_symbol(symbol)] = symbol
    except Exception as exc:
        log.warning(f"Could not fetch open symbols: {exc}")
    return normalized_to_open


def _match_trade_for_result(result: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    """
    Match a result message to our most likely pending trade.
    - Prefer exact (time + asset + gale).
    - Fallback to same time + asset any gale.
    - Fallback to latest pending by asset (+ gale when available).
    """
    result_time = result.get("time")
    result_asset = result.get("asset_key")
    result_gale = int(result.get("gale_num", 0))

    if result_time and result_asset:
        exact_key = _trade_key(result_time, result_asset, result_gale)
        trade = trade_log.get(exact_key)
        if trade and trade.get("result") is None:
            return exact_key, trade

        for g in range(MAX_GALES + 1):
            key = _trade_key(result_time, result_asset, g)
            t = trade_log.get(key)
            if t and t.get("result") is None:
                return key, t

    candidates: list[tuple[str, dict[str, Any]]] = []
    for key, trade in trade_log.items():
        if trade.get("result") is not None:
            continue
        if trade.get("asset_key") != result_asset:
            continue
        if result_gale > 0 and int(trade.get("gale_num", 0)) != result_gale:
            continue
        candidates.append((key, trade))

    if not candidates and result_asset:
        # Last fallback: ignore gale, still keep asset constraint.
        for key, trade in trade_log.items():
            if trade.get("result") is not None:
                continue
            if trade.get("asset_key") == result_asset:
                candidates.append((key, trade))

    if not candidates:
        return None, None

    best_key, best_trade = max(candidates, key=lambda pair: float(pair[1].get("time", 0.0)))
    return best_key, best_trade


# -----------------------------------------------------------------------------
# IQ Option integration
# -----------------------------------------------------------------------------
def iq_connect() -> bool:
    global iq_api, account_balance, is_demo
    try:
        if not IQ_EMAIL or not IQ_PASSWORD:
            log.error("IQ_EMAIL or IQ_PASSWORD missing in .env")
            return False
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
    except Exception as exc:
        log.error(f"IQ connect error: {exc}")
        return False


def iq_place_trade(asset: str, direction: str, amount: float, expiry_min: int) -> bool:
    global iq_api, account_balance
    try:
        if iq_api is None and not iq_connect():
            return False
        action = "call" if direction == "call" else "put"

        # Optional reconnect guard when session drops silently.
        try:
            if hasattr(iq_api, "check_connect") and not iq_api.check_connect():
                log.warning("IQ session disconnected, reconnecting before trade...")
                if not iq_connect():
                    return False
        except Exception:
            pass

        candidates = _build_symbol_candidates(asset)
        open_symbol_variants = _resolve_open_symbol_variants()
        prioritized: list[str] = []

        # First try candidates that map to currently open symbols.
        for symbol in candidates:
            normalized = _normalized_symbol(symbol)
            open_symbol = open_symbol_variants.get(normalized)
            if open_symbol and open_symbol not in prioritized:
                prioritized.append(open_symbol)

        # Then try raw candidates as fallback.
        for symbol in candidates:
            if symbol not in prioritized:
                prioritized.append(symbol)

        last_reason = None
        for iq_asset in prioritized:
            log.info(f"Placing: {iq_asset} {action} ${amount} {expiry_min}min [{mode()}]")
            check, order_id = iq_api.buy(amount, iq_asset, action, expiry_min)
            if check:
                log.info(f"TRADE PLACED! Symbol: {iq_asset} | Order: {order_id}")
                time.sleep(1)
                bal = iq_api.get_balance()
                if bal is not None:
                    account_balance = float(bal)
                return True

            last_reason = order_id
            log.warning(f"Trade attempt failed for {iq_asset}: {order_id}")

        log.error(
            f"Trade failed for asset {asset} after {len(prioritized)} symbol attempts. "
            f"Last reason: {last_reason}"
        )
        return False
    except Exception as exc:
        log.error(f"Trade error: {exc}")
        iq_connect()
        return False


def iq_keepalive() -> None:
    """Keep IQ Option connection alive."""
    global account_balance
    while True:
        time.sleep(60)
        try:
            if iq_api is None:
                iq_connect()
                continue
            bal = iq_api.get_balance()
            if bal is not None:
                account_balance = float(bal)
            else:
                log.warning("Keepalive: reconnecting...")
                iq_connect()
        except Exception as exc:
            log.warning(f"Keepalive error: {exc}")
            iq_connect()


# -----------------------------------------------------------------------------
# Parser helpers
# -----------------------------------------------------------------------------
def parse_result(text: str) -> dict[str, Any] | None:
    """
    Parse Magic Trader result messages:
    ✅ EUR/AUD;09:05;PUT->GAIN
    ❌ USD/BRL;10:35;PUT->LOSS
    ✅ EUR/AUD;09:05;PUT->DIRECT WIN
    ✅ EUR/AUD;09:26|CALL->GAIN ✅  (1st GALE)
    ❌ EUR/AUD;09:26|CALL->LOSS (2nd GALE)
    """
    t = text.strip()
    upper = t.upper()
    gain = (
        "GAIN" in upper
        or "DIRECT WIN" in upper
        or ("WIN" in upper and "GALE" not in upper)
    )
    loss = "LOSS" in upper
    if not gain and not loss:
        return None

    gale_num = 0
    if "1ST GALE" in upper or "1 GALE" in upper or "GALE 1" in upper:
        gale_num = 1
    elif "2ND GALE" in upper or "2 GALE" in upper or "GALE 2" in upper:
        gale_num = 2

    asset_found = None
    for key in sorted(ASSET_MAP.keys(), key=len, reverse=True):
        if key.upper() in upper:
            asset_found = key
            break
    if not asset_found:
        return None

    time_found = None
    match = re.search(r"\b(\d{2}:\d{2})\b", t)
    if match:
        time_found = match.group(1)

    return {
        "asset_key": asset_found,
        "time": time_found,
        "result": "GAIN" if gain else "LOSS",
        "gale_num": gale_num,
    }


def parse_magic_trader(text: str) -> dict[str, Any] | None:
    t = text.strip()

    # Ignore result-like messages here.
    if any(x in t.upper() for x in ["GAIN", "LOSS", "WIN", "DIRECT WIN"]):
        return None
    if "expiration" not in t.lower() and "expiry" not in t.lower():
        return None

    exp = re.search(r"(\d+)[- ]minute", t, re.IGNORECASE)
    if not exp:
        return None
    expiry_min = int(exp.group(1))

    asset_key = None
    for key in sorted(ASSET_MAP, key=len, reverse=True):
        if key.upper() in t.upper():
            asset_key = key
            break
    if not asset_key:
        return None

    direction = None
    if "PUT" in t.upper():
        direction = "put"
    elif "CALL" in t.upper():
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
            r"(?:1st|2nd)\s*GALE.*?(\d{2}:\d{2})",
            t,
            re.IGNORECASE,
        )[:MAX_GALES]

    return {
        "asset_key": asset_key,
        "asset": ASSET_MAP[asset_key],
        "display": asset_key,
        "direction": direction,
        "expiry_min": expiry_min,
        "entry_time": entry_time,
        "gale_times": gale_times,
        "id": f"{entry_time}_{asset_key}_{direction}",
    }


# -----------------------------------------------------------------------------
# Time helpers
# -----------------------------------------------------------------------------
def brt_to_local(time_str: str) -> datetime:
    try:
        nb = now_brt()
        parsed = datetime.strptime(time_str, "%H:%M")
        if nb.tzinfo:
            dt_brt = nb.replace(
                hour=parsed.hour,
                minute=parsed.minute,
                second=0,
                microsecond=0,
            )
            if dt_brt < nb:
                dt_brt += timedelta(days=1)
            return dt_brt.astimezone().replace(tzinfo=None)

        dt = datetime.now().replace(
            hour=parsed.hour,
            minute=parsed.minute,
            second=0,
            microsecond=0,
        )
        if dt < datetime.now():
            dt += timedelta(days=1)
        return dt
    except Exception as exc:
        log.error(f"Time parse error: {exc}")
        return datetime.now() + timedelta(minutes=1)


def _next_gale_allowed(sig: dict[str, Any], gale_num: int) -> bool:
    """
    Only allow Gale N if Gale N-1 was placed and resulted in LOSS.
    If no prior result is known by fire time, skip for safety.
    """
    if gale_num <= 0:
        return True

    prev_key = _trade_key(sig["entry_time"], sig["asset_key"], gale_num - 1)
    prev_trade = trade_log.get(prev_key)
    if not prev_trade:
        log.info(f"Skipping Gale {gale_num}: previous trade not found")
        return False
    if not prev_trade.get("placed"):
        log.info(f"Skipping Gale {gale_num}: previous trade failed to place")
        return False
    if prev_trade.get("result") != "LOSS":
        log.info(f"Skipping Gale {gale_num}: previous result is {prev_trade.get('result')}")
        return False
    return True


# -----------------------------------------------------------------------------
# Alert + execution
# -----------------------------------------------------------------------------
async def send_alert(sig: dict[str, Any], gale_num: int = 0) -> int | None:
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
    label = labels[gale_num] if gale_num < len(labels) else f"GALE {gale_num}"

    msg = (
        f"--- {label} ---\n"
        f"Asset:     {sig['display']}\n"
        f"Direction: {arrow}\n"
        f"Expiry:    {sig['expiry_min']} minutes\n"
        f"Entry:     {sig['entry_time']} BRT\n"
        f"Stake:     ${stake:.2f}\n"
        f"Balance:   ${account_balance:.2f}\n"
        f"Mode:      {mode()}\n"
        f"BRT Now:   {brt_now()}\n\n"
        "Trade fires in 30s! Tap CANCEL to skip."
    )
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("CANCEL TRADE", callback_data=f"cancel_{sig_id}")]]
    )
    try:
        sent = await alert_bot.send_message(
            chat_id=ALERT_CHAT_ID,
            text=msg,
            reply_markup=keyboard,
        )
        return sent.message_id
    except Exception as exc:
        log.error(f"Alert error: {exc}")
        return None


async def execute_with_countdown(sig: dict[str, Any], gale_num: int = 0) -> None:
    sig_id = sig["id"] + (f"_g{gale_num}" if gale_num > 0 else "")
    stake = calculate_stake(account_balance, gale_num)

    msg_id = await send_alert(sig, gale_num)
    await asyncio.sleep(COUNTDOWN_SECS)

    if sig_id in _cancelled:
        _cancelled.discard(sig_id)
        log.info(f"Cancelled: {sig['display']} g{gale_num}")
        if msg_id and alert_bot:
            try:
                await alert_bot.edit_message_text(
                    chat_id=ALERT_CHAT_ID,
                    message_id=msg_id,
                    text=f"CANCELLED\n{sig['display']} {sig['direction'].upper()} ${stake:.2f}",
                )
            except Exception:
                pass
        log_trade(sig, gale_num, stake, False)
        record_trade(sig, gale_num, stake, placed=False, demo=is_demo)
        return

    log.info(f"FIRING: {sig['display']} {sig['direction']} ${stake:.2f} g{gale_num}")
    loop = asyncio.get_event_loop()
    success = await loop.run_in_executor(
        None,
        iq_place_trade,
        sig["asset_key"],
        sig["direction"],
        stake,
        sig["expiry_min"],
    )

    log_trade(sig, gale_num, stake, success)
    record_trade(sig, gale_num, stake, placed=success, demo=is_demo)

    arrow = "UP" if sig["direction"] == "call" else "DOWN"
    result_text = (
        f"{'TRADE PLACED!' if success else 'TRADE FAILED!'}\n"
        f"{sig['display']} {arrow} ${stake:.2f} [{mode()}]\n"
        f"Balance: ${account_balance:.2f}"
    )
    if alert_bot:
        try:
            if msg_id:
                await alert_bot.edit_message_text(
                    chat_id=ALERT_CHAT_ID,
                    message_id=msg_id,
                    text=result_text,
                )
            else:
                await alert_bot.send_message(chat_id=ALERT_CHAT_ID, text=result_text)
        except Exception:
            pass


# -----------------------------------------------------------------------------
# Schedulers
# -----------------------------------------------------------------------------
async def schedule_gales(sig: dict[str, Any]) -> None:
    for i, gale_time in enumerate(sig["gale_times"][:MAX_GALES]):
        gale_num = i + 1
        try:
            gale_local = brt_to_local(gale_time)
            alert_at = gale_local - timedelta(seconds=COUNTDOWN_SECS)
            wait_secs = (alert_at - datetime.now()).total_seconds()
            if wait_secs > 0:
                log.info(f"Gale {gale_num} alert in {wait_secs:.0f}s (BRT: {gale_time})")
                await asyncio.sleep(wait_secs)
            elif (gale_local - datetime.now()).total_seconds() < -30:
                log.warning(f"Gale {gale_num} already passed, skipping")
                continue

            if not _next_gale_allowed(sig, gale_num):
                continue
            await execute_with_countdown(sig, gale_num=gale_num)
        except Exception as exc:
            log.error(f"Gale {gale_num} error: {exc}")


async def schedule_entry(sig: dict[str, Any]) -> None:
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
    except Exception as exc:
        log.error(f"schedule_entry error: {exc}")


# -----------------------------------------------------------------------------
# Bot commands
# -----------------------------------------------------------------------------
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Magic -> IQ Option Bot v1.0\n"
        f"IQ Option: {'Connected' if iq_api else 'Disconnected'}\n"
        f"Mode:      {mode()}\n"
        f"Balance:   ${account_balance:.2f}\n"
        f"Stake:     ${calculate_stake(account_balance):.2f}\n"
        f"Gale 1:    ${calculate_stake(account_balance, 1):.2f}\n"
        f"Gale 2:    ${calculate_stake(account_balance, 2):.2f}\n"
        f"Risk:      {RISK_PERCENT}%\n"
        f"BRT:       {brt_now()}"
    )


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global account_balance
    if context.args:
        try:
            account_balance = float(context.args[0])
            await update.message.reply_text(
                f"Balance: ${account_balance:.2f}\n"
                f"Stake: ${calculate_stake(account_balance):.2f}\n"
                f"Gale 1: ${calculate_stake(account_balance, 1):.2f}\n"
                f"Gale 2: ${calculate_stake(account_balance, 2):.2f}"
            )
        except Exception:
            await update.message.reply_text("Usage: /balance 100")
        return

    loop = asyncio.get_event_loop()
    bal = await loop.run_in_executor(None, lambda: iq_api.get_balance() if iq_api else None)
    if bal is not None:
        account_balance = float(bal)
    await update.message.reply_text(f"Balance: ${account_balance:.2f}")


async def cmd_risk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global RISK_PERCENT
    if context.args:
        try:
            RISK_PERCENT = float(context.args[0])
            await update.message.reply_text(
                f"Risk: {RISK_PERCENT}%\n"
                f"Stake: ${calculate_stake(account_balance):.2f}"
            )
        except Exception:
            await update.message.reply_text("Usage: /risk 5")


async def cmd_demo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global is_demo, account_balance
    is_demo = True
    if iq_api:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: iq_api.change_balance("PRACTICE"))
        bal = await loop.run_in_executor(None, lambda: iq_api.get_balance())
        if bal is not None:
            account_balance = float(bal)
    await update.message.reply_text(f"Switched to DEMO!\nBalance: ${account_balance:.2f}")


async def cmd_real(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global is_demo, account_balance
    is_demo = False
    if iq_api:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: iq_api.change_balance("REAL"))
        bal = await loop.run_in_executor(None, lambda: iq_api.get_balance())
        if bal is not None:
            account_balance = float(bal)
    await update.message.reply_text(f"Switched to REAL!\nBalance: ${account_balance:.2f}")


async def cmd_connect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Reconnecting to IQ Option...")
    loop = asyncio.get_event_loop()
    success = await loop.run_in_executor(None, iq_connect)
    await update.message.reply_text(
        f"{'Connected!' if success else 'Failed - check credentials'}\n"
        f"Balance: ${account_balance:.2f}"
    )


async def cmd_brt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"Brazil time: {brt_now()}")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    days = 7
    if context.args:
        try:
            days = int(context.args[0])
        except Exception:
            pass
    await update.message.reply_text(get_stats(days))


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(get_stats(30))


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not trade_log:
        await update.message.reply_text("No trades yet.")
        return

    wins = sum(1 for t in trade_log.values() if t.get("result") == "GAIN")
    losses = sum(1 for t in trade_log.values() if t.get("result") == "LOSS")
    placed = sum(1 for t in trade_log.values() if t.get("placed"))
    total = len(trade_log)
    win_rate = f"{wins / (wins + losses) * 100:.0f}%" if (wins + losses) > 0 else "N/A"

    lines = [
        f"Recent Trades ({total} total)",
        f"Placed: {placed} | Win: {wins} | Loss: {losses}",
        f"Win Rate: {win_rate}",
        "---",
    ]
    for _, trade in list(trade_log.items())[-10:]:
        arrow = "UP" if trade["direction"] == "call" else "DOWN"
        result = trade["result"] or "Pending"
        emoji = "✅" if result == "GAIN" else "❌" if result == "LOSS" else "⏳"
        placed_str = "placed" if trade["placed"] else "FAILED"
        lines.append(
            f"{emoji} {trade['display']} {arrow} ${trade['stake']:.2f} - {result} ({placed_str})"
        )

    await update.message.reply_text("\n".join(lines))


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("Cancelled!")
    if query.data.startswith("cancel_"):
        _cancelled.add(query.data.replace("cancel_", ""))
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text("Trade cancelled!")


# -----------------------------------------------------------------------------
# Magic Trader listener
# -----------------------------------------------------------------------------
tg_listener = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)


@tg_listener.on(events.NewMessage(chats=[MAGIC_TRADER_CHANNEL]))
async def on_magic_trader(event: events.NewMessage.Event) -> None:
    global account_balance
    text = event.raw_text or ""
    if not text.strip():
        return

    log.info(f"MAGIC TRADER: {text[:120].replace(chr(10), ' ')}")

    sig_hash = text[:150]
    if sig_hash in _seen_signals:
        return
    _seen_signals.add(sig_hash)
    if len(_seen_signals) > 300:
        _seen_signals.pop()

    # Result message first
    result = parse_result(text)
    if result:
        gale_num = result.get("gale_num", 0)
        key, trade = _match_trade_for_result(result)

        emoji = "✅" if result["result"] == "GAIN" else "❌"
        label = "ENTRY" if gale_num == 0 else f"GALE {gale_num}"
        result_text = (
            f"{emoji} Magic Trader: {result['result']}\n"
            f"Asset:  {result['asset_key']}\n"
            f"Level:  {label}"
        )

        if trade:
            arrow = "UP" if trade["direction"] == "call" else "DOWN"
            our_label = trade["gale_label"]
            result_text += (
                f"\nOur {our_label}: {arrow} ${trade['stake']:.2f}\n"
                f"Placed: {'Yes' if trade['placed'] else 'FAILED'}\n"
                f"Result: {result['result']}"
            )
            trade["result"] = result["result"]
            trade_entry_time = result.get("time") or trade.get("entry_time")
            if trade_entry_time:
                update_trade_result(
                    asset_key=result["asset_key"],
                    entry_time=str(trade_entry_time),
                    gale_num=int(trade["gale_num"]),
                    result=result["result"],
                    stake=float(trade["stake"]),
                )
            try:
                loop = asyncio.get_event_loop()
                bal = await loop.run_in_executor(
                    None,
                    lambda: iq_api.get_balance() if iq_api else None,
                )
                if bal is not None:
                    account_balance = float(bal)
                    result_text += f"\nBalance: ${account_balance:.2f}"
            except Exception:
                pass
        else:
            result_text += "\n(No matching trade found)"

        if alert_bot:
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


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
async def main() -> None:
    global alert_bot
    log.info("=" * 55)
    log.info("  MAGIC TRADER -> IQ OPTION BOT v1.0")
    log.info(f"  Risk: {RISK_PERCENT}% | Gales: {MAX_GALES} | Countdown: {COUNTDOWN_SECS}s")
    log.info("=" * 55)

    if not all([TG_SESSION, TG_API_ID, TG_API_HASH]):
        log.error("Missing TG credentials in .env")
        return
    if not ALERT_BOT_TOKEN or not ALERT_CHAT_ID:
        log.error("Missing ALERT_BOT_TOKEN or ALERT_CHAT_ID in .env")
        return

    # connect IQ Option in executor
    loop = asyncio.get_event_loop()
    connected = await loop.run_in_executor(None, iq_connect)

    # keepalive thread
    threading.Thread(target=iq_keepalive, daemon=True).start()

    # init alert bot
    alert_bot = Bot(token=ALERT_BOT_TOKEN)
    me = await alert_bot.get_me()
    log.info(f"Alert bot: @{me.username}")

    await alert_bot.send_message(
        chat_id=ALERT_CHAT_ID,
        text=(
            "Magic Trader -> IQ Option Bot v1.0 Started!\n\n"
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
            "/balance   - check/set balance\n"
            "/risk 5    - change risk %\n"
            "/status    - full status\n"
            "/brt       - Brazil time\n"
            "/trades    - recent trades\n"
            "/stats 7   - performance summary\n"
            "/report    - 30d performance\n\n"
            "Watching Magic Trader... Trade fires 30s before entry!"
        ),
    )

    ptb_app = Application.builder().token(ALERT_BOT_TOKEN).build()
    ptb_app.add_handler(CommandHandler("status", cmd_status))
    ptb_app.add_handler(CommandHandler("balance", cmd_balance))
    ptb_app.add_handler(CommandHandler("risk", cmd_risk))
    ptb_app.add_handler(CommandHandler("demo", cmd_demo))
    ptb_app.add_handler(CommandHandler("real", cmd_real))
    ptb_app.add_handler(CommandHandler("connect", cmd_connect))
    ptb_app.add_handler(CommandHandler("brt", cmd_brt))
    ptb_app.add_handler(CommandHandler("trades", cmd_trades))
    ptb_app.add_handler(CommandHandler("stats", cmd_stats))
    ptb_app.add_handler(CommandHandler("report", cmd_report))
    ptb_app.add_handler(CallbackQueryHandler(button_callback))

    async with ptb_app:
        await ptb_app.start()
        await ptb_app.updater.start_polling(drop_pending_updates=True)

        while True:
            try:
                await tg_listener.start()
                log.info("Telegram connected | Watching Magic Trader...")
                await tg_listener.run_until_disconnected()
            except Exception as exc:
                log.error(f"Telegram listener error: {exc}")
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
