# BTC 5-Minute Polymarket Observer (Telegram + SQLite)

An **observation-only** Python bot for Polymarket's BTC 5-minute Up/Down markets. It tracks public market prices, stores event summaries and raw ticks in SQLite, can message a Telegram chat, and exports the market summary table to CSV. It does **not** place orders or need a wallet/private key.

## What it records

The summary table (and `/export` CSV) includes:

1. Market identifier (`btc-updown-5m-<window_start_unix>`)
2. First Sweet Spot direction (UP or DOWN)
3. First observed Sweet Spot price
4. BTC price difference in USD (signed)
5. Sweet Spot time remaining (`[minutes, seconds]`)
6. Timing category (`3 min+` if the event was more than 180 seconds before expiry; otherwise the actual time label)
7. Initial qualifying direction
8. Pullback occurrence (YES/NO)
9. First observed Pullback price
10. Pullback time remaining
11. Switch Up occurrence (YES/NO after resolution is known)
12. Pullback direction
13. Final resolution (UP/DOWN)
14. Observation status (`observing`, `complete`, `incomplete`, or `data_missing`)

The separate **opening reference price** and **BTC price at Sweet Spot** are not stored as summary fields. The bot reads/holds the reference in memory to calculate the signed difference, then stores only that difference.

Raw Polymarket observations are stored in `raw_ticks` by default. Those rows contain the market identifier, outcome, timestamps, top-of-book quotes, last-trade price, observed price and the price source. They do not store the BTC opening reference or the BTC spot price at Sweet Spot.

## Event rules implemented

- **Sweet Spot:** first observed market price **strictly greater than $0.80** on either outcome. Price means the best-bid/ask midpoint where a two-sided quote exists; otherwise the latest trade is used as a fallback. The first event is not overwritten by later crossings.
- **Time label:** the bot records actual time remaining in `[minutes, seconds]`. If more than 180 seconds remain, the separate timing category is `3 min+`. The bot continues tracking Pullbacks and reversals.
- **Pullback:** after the initial Sweet Spot, the **initial direction's** observed price first reaches `<= $0.55`. The bot records the observed price, not an assumed `$0.55` value.
- **Switch Up:** the **opposite direction** subsequently reaches `>= $0.80` and then wins at resolution. This is separate from Pullback. The opposite-side threshold event is retained internally until the market resolves.
- **Final resolution:** retrieved from the public Gamma market record only when the market is marked closed and an outcome is identified.
- **Data quality:** starting after a market has begun or experiencing a WebSocket interruption marks that market incomplete. No usable prices marks it `data_missing`.

## Important pricing limitation

Polymarket's market prices are the event triggers, but the BTC difference currently uses **Binance BTCUSDT** as a price proxy: the 5-minute candle opening price at the market's start and the latest available Binance trade/spot price when Sweet Spot is detected. This is not guaranteed to match the Chainlink reference used by Polymarket for settlement. The bot stores only the computed difference, not either BTC price itself. If you later require Chainlink-aligned differences, swap this proxy out for a verified Chainlink feed.

The market share event price is not guaranteed to be an executable fill price. Midpoint prices can differ from the best ask/bid and the last trade. This bot is for observation and research, not execution.

## 1. Create the Telegram bot

1. Open Telegram and message **@BotFather**.
2. Run `/newbot` and follow its prompts.
3. Copy the token BotFather gives you. Treat it like a password; don't post it publicly.

This project cannot connect to your Telegram account until you add that token to the local `.env` file.

## 2. Install on Windows

1. Download and extract the ZIP.
2. Open the extracted `btc_5m_observer_bot` folder.
3. Double-click `start_bot.bat`. It creates a virtual environment and installs dependencies. On the first run, it creates `.env` and stops.
4. Open `.env` in Notepad and paste the BotFather token after `TELEGRAM_BOT_TOKEN=`.
5. Run `start_bot.bat` again.
6. Open your bot in Telegram and send `/start`.

You need Python 3.11 or newer installed. If `py` is not recognized, install Python from python.org and select the option to add Python to PATH.

### Restrict access (recommended)

The first run leaves `TELEGRAM_ALLOWED_CHAT_IDS` blank, which means anyone who finds the bot can subscribe and receive alerts. To make it private:

1. Send `/myid` to your bot and copy the numeric chat ID.
2. Stop the script with Ctrl+C.
3. In `.env`, set `TELEGRAM_ALLOWED_CHAT_IDS=123456789` using your actual ID.
4. Restart the bot.

For a group, use the group chat ID. Multiple IDs can be comma-separated. Don't put the example ID in as-is.

## 3. Install manually (Windows, macOS or Linux)

In a terminal inside the project folder:

```bash
python -m venv .venv
```

Activate it:

```powershell
# Windows PowerShell
.venv\Scripts\Activate.ps1
```

```bash
# macOS / Linux
source .venv/bin/activate
```

Then install and run:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env  # Windows: copy .env.example .env
# Edit .env and add TELEGRAM_BOT_TOKEN
python bot.py
```

Keep the terminal open while observing. If you need it running 24/7, run it on a reliable VPS or use a process supervisor; closing the terminal stops the bot. A persistent hosted service is not included or automatically configured by this ZIP.

## Telegram commands

- `/start` — subscribe this chat to alerts
- `/status` — WebSocket/feed status, active market and latest observed prices
- `/latest` — most recent market summary
- `/stats` — aggregate observation counts
- `/export` — send the summary table as CSV
- `/stopalerts` — stop alerts to this chat; observation continues
- `/myid` — show the current chat ID
- `/help` — command reference

Alerts include Sweet Spot, Pullback, the opposite side reaching $0.80+, and the final market resolution. Set `ALERTS_ENABLED=false` in `.env` to silence alerts while leaving collection enabled.

## SQLite files and data

The database is created automatically as `btc_observer.sqlite3` in the project folder. The `markets` table stores summaries, while `raw_ticks` stores price observations when `STORE_RAW_TICKS=true`.

To disable raw tick storage and retain only summary records, set:

```env
STORE_RAW_TICKS=false
```

Raw tick data can accumulate. Back up the SQLite database periodically, and don't delete the `.sqlite3-wal` / `.sqlite3-shm` files while the bot is running.

## Tests

The event-rule tests are dependency-free:

```bash
python tests_rules.py
```

## Reliability notes

- A market already in progress when the bot starts cannot be reconstructed from live WebSocket messages alone; the record is marked incomplete if the initial observation starts late.
- On disconnect, the bot attempts to reconnect and marks the affected market incomplete rather than pretending it saw every threshold crossing.
- Resolution can take a little time after the 5-minute window ends; the bot checks in the background while continuing to observe the next market.
- Public endpoints, feed formats, regional network availability and market schemas can change. The code has defensive parsing and logging but should be monitored before using its dataset for analysis.

## Public endpoints used

- Gamma market metadata: `https://gamma-api.polymarket.com/markets/slug/btc-updown-5m-<window_start_unix>`
- Polymarket CLOB market WebSocket: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- Binance BTCUSDT price proxy REST: `https://api.binance.com/api/v3/klines` and `/api/v3/ticker/price`
- Binance BTCUSDT public trade stream: `wss://stream.binance.com:9443/ws/btcusdt@trade`
