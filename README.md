# Magic Trader → IQ Option Bot v1.0

Automatically listens to the **Magic Trader Signals** Telegram channel and places binary-option trades on **IQ Option** — with full Martingale/Gale support, Brazil-timezone awareness, dynamic stake sizing, and a live alert bot.

---

## Features

- Listens directly to the Magic Trader channel via a Telethon user session (no forwarding tunnel needed)
- Parses asset, direction, expiry, and gale times from Magic Trader's exact signal format
- Places trades on IQ Option (demo or real) via `iqoptionapi`
- 2-level Martingale (Gale) — doubles the base stake on each level
- 30-second countdown with a Telegram inline **CANCEL** button before every trade fires
- Balance refresh after every trade and keepalive ping every 60 s
- Persistent trade history (`trades_db.json`) with win-rate and P&L analytics
- Configurable risk percentage, min/max stake via environment variables
- Auto-reconnect on IQ Option disconnects

---

## Quick Start

### 1. Requirements

- Python 3.10+
- A USA VPS (or any server that can reach Telegram and IQ Option)

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
```

Fill in all values in `.env`:

| Variable | Description |
|---|---|
| `TG_SESSION` | Telethon StringSession (see below) |
| `TG_API_ID` | From [my.telegram.org/apps](https://my.telegram.org/apps) |
| `TG_API_HASH` | From [my.telegram.org/apps](https://my.telegram.org/apps) |
| `ALERT_BOT_TOKEN` | BotFather token for the alert bot |
| `ALERT_CHAT_ID` | Your Telegram user/chat ID |
| `IQ_EMAIL` | IQ Option account email |
| `IQ_PASSWORD` | IQ Option account password |

Optional overrides (with defaults) are documented in `.env.example`.

#### Generating a Telethon StringSession

```bash
python - <<'EOF'
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
import os

api_id   = int(input("API ID: "))
api_hash = input("API Hash: ")

with TelegramClient(StringSession(), api_id, api_hash) as c:
    print("\nSession string:")
    print(c.session.save())
EOF
```

Paste the output as `TG_SESSION` in your `.env`.

### 4. Run

```bash
python magic_iq_bot.py
```

---

## Telegram Commands

| Command | Description |
|---|---|
| `/status` | Connection status, balance, stake sizes |
| `/demo` | Switch to demo (practice) account |
| `/real` | Switch to real account |
| `/connect` | Reconnect to IQ Option |
| `/balance [amount]` | Show or manually set balance |
| `/risk <pct>` | Set risk % (e.g. `/risk 3`) |
| `/trades` | Last 10 trade results |
| `/stats [days]` | Win-rate and P&L (default: 7 days) |
| `/report` | 30-day performance report |
| `/brt` | Current Brazil time |

---

## Signal Format Recognised

```
EUR/AUD - PUT
Expiration: 5-minute
Entry: 09:05
TIME TO 09:10 (1st GALE)
TIME TO 09:15 (2nd GALE)
```

Result messages are also parsed to update trade outcomes:

```
✅ EUR/AUD;09:05;PUT->GAIN
❌ USD/BRL;10:35;PUT->LOSS
✅ EUR/AUD;09:26|CALL->GAIN ✅  (1st GALE)
```

---

## Risk Management

- **Base stake** = `max(balance × RISK_PERCENT / 100, MIN_STAKE)`, capped at `MAX_STAKE`
- **Gale stake** = `base × 2^gale_num`, capped at `MAX_GALE_STAKE`
- Default: 5 % risk, $1 minimum, $500 maximum

---

## File Layout

```
magic_iq_bot.py   — main bot
requirements.txt  — Python dependencies
.env.example      — environment template
trades_db.json    — trade history (auto-created)
magic_iq.log      — rotating log file (auto-created)
```
