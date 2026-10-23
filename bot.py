"""Observation-only Polymarket BTC 5-minute Up/Down Telegram bot.

This program places no trades and never requests wallet/private-key credentials.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import sqlite3
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp
from dotenv import load_dotenv
from telegram import BotCommand, Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

from rules import (
    is_opposite_80,
    is_pullback,
    is_sweet_spot,
    opposite_direction,
    remaining_label,
    sweet_spot_category,
    switch_up_result,
)

load_dotenv()

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_REST = os.getenv("BINANCE_REST_URL", "https://api.binance.com")
BINANCE_WS_URL = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws/btcusdt@trade")
WINDOW_SECONDS = 300

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "btc_observer.sqlite3"))
STORE_RAW_TICKS = os.getenv("STORE_RAW_TICKS", "true").strip().lower() in {"1", "true", "yes", "on"}
ALERTS_ENABLED = os.getenv("ALERTS_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
ALLOWED_CHAT_IDS = {
    int(part.strip()) for part in os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", "").split(",")
    if part.strip().lstrip("-").isdigit()
}
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("btc-observer")

SUMMARY_FIELDS = [
    ("market_identifier", "slug"),
    ("sweet_spot_direction", "sweet_spot_direction"),
    ("sweet_spot_price", "sweet_spot_price"),
    ("btc_price_difference_usd", "btc_price_diff_usd"),
    ("sweet_spot_time", "sweet_spot_time"),
    ("sweet_spot_timing_category", "sweet_spot_timing_category"),
    ("initial_qualifying_direction", "initial_qualifying_direction"),
    ("pullback_occurrence", "pullback_occurrence"),
    ("pullback_price", "pullback_price"),
    ("pullback_time", "pullback_time"),
    ("switch_up_occurrence", "switch_up_occurrence"),
    ("pullback_direction", "pullback_direction"),
    ("final_resolution", "final_resolution"),
    ("observation_status", "observation_status"),
]


def now_utc_iso(ts: Optional[float] = None) -> str:
    return datetime.fromtimestamp(ts or time.time(), timezone.utc).isoformat(timespec="milliseconds")


def parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
            return loaded if isinstance(loaded, list) else []
        except json.JSONDecodeError:
            return []
    return []


def bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def event_timestamp(message: dict[str, Any], fallback: float) -> float:
    value = message.get("timestamp") or message.get("ts") or message.get("time")
    if value is None:
        return fallback
    try:
        number = float(value)
        # Polymarket timestamps are commonly milliseconds.
        if number > 100_000_000_000:
            number /= 1000.0
        return number
    except (TypeError, ValueError):
        return fallback


def parse_market_tokens(record: dict[str, Any]) -> tuple[str, str]:
    token_ids = parse_json_list(record.get("clobTokenIds") or record.get("clob_token_ids"))
    outcomes = parse_json_list(record.get("outcomes"))
    if not token_ids or len(token_ids) < 2:
        tokens = record.get("tokens")
        if isinstance(tokens, list):
            mapped: dict[str, str] = {}
            for token in tokens:
                if isinstance(token, dict):
                    outcome = str(token.get("outcome", "")).strip().upper()
                    token_id = token.get("token_id") or token.get("tokenId") or token.get("id")
                    if outcome in {"UP", "DOWN"} and token_id:
                        mapped[outcome] = str(token_id)
            if mapped.get("UP") and mapped.get("DOWN"):
                return mapped["UP"], mapped["DOWN"]
        raise ValueError("Gamma market record did not contain two outcome token IDs")
    outcome_names = [str(item).strip().upper() for item in outcomes]
    if len(outcome_names) == len(token_ids):
        mapping = {outcome_names[i]: str(token_ids[i]) for i in range(len(token_ids))}
        if mapping.get("UP") and mapping.get("DOWN"):
            return mapping["UP"], mapping["DOWN"]
    # Current BTC 5m market schema has Up first and Down second.
    return str(token_ids[0]), str(token_ids[1])


@dataclass
class MarketInfo:
    slug: str
    title: str
    start_ts: int
    end_ts: int
    token_up: str
    token_down: str
    gamma_record: dict[str, Any] = field(default_factory=dict)

    def token_map(self) -> dict[str, str]:
        return {self.token_up: "UP", self.token_down: "DOWN"}


@dataclass
class PriceState:
    direction: str
    asset_id: str
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    last_trade: Optional[float] = None
    observed_price: Optional[float] = None
    price_source: Optional[str] = None
    feed_ts: Optional[float] = None


@dataclass
class RuntimeState:
    started_ts: float = field(default_factory=time.time)
    active_slug: Optional[str] = None
    active_start_ts: Optional[int] = None
    active_end_ts: Optional[int] = None
    clob_connected: bool = False
    binance_connected: bool = False
    last_polymarket_event_ts: Optional[float] = None
    last_binance_event_ts: Optional[float] = None
    last_error: Optional[str] = None
    ticks_received: int = 0
    up_price: Optional[float] = None
    down_price: Optional[float] = None
    btc_price: Optional[float] = None
    btc_price_ts: Optional[float] = None
    stopping: asyncio.Event = field(default_factory=asyncio.Event)
    observer_task: Optional[asyncio.Task] = None
    btc_task: Optional[asyncio.Task] = None
    settlement_tasks: set[asyncio.Task] = field(default_factory=set)
    open_references: dict[str, Optional[float]] = field(default_factory=dict)


class Database:
    def __init__(self, path: Path):
        if str(path) != ":memory:":
            path.parent.mkdir(parents=True, exist_ok=True)
        self.path = str(path)
        self.conn = sqlite3.connect(self.path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        # Anything left mid-observation by a previous process may have missed data.
        self.conn.execute(
            "UPDATE markets SET observation_status='incomplete', data_gap=1, "
            "status_note=COALESCE(status_note, 'Process restarted; observation gap possible') "
            "WHERE observation_status='observing'"
        )
        self.conn.commit()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS markets (
                slug TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL,
                token_up TEXT NOT NULL,
                token_down TEXT NOT NULL,
                first_connected_ts REAL,
                data_gap INTEGER NOT NULL DEFAULT 0,
                has_price_data INTEGER NOT NULL DEFAULT 0,
                sweet_spot_direction TEXT,
                sweet_spot_price REAL,
                btc_price_diff_usd REAL,
                sweet_spot_time TEXT,
                sweet_spot_elapsed_seconds INTEGER,
                sweet_spot_event_ts REAL,
                sweet_spot_timing_category TEXT,
                initial_qualifying_direction TEXT,
                pullback_occurrence TEXT,
                pullback_price REAL,
                pullback_time TEXT,
                pullback_elapsed_seconds INTEGER,
                pullback_event_ts REAL,
                pullback_direction TEXT,
                opposite_reached_80 INTEGER NOT NULL DEFAULT 0,
                opposite_80_price REAL,
                opposite_80_event_ts REAL,
                switch_up_occurrence TEXT,
                final_resolution TEXT,
                observation_status TEXT NOT NULL DEFAULT 'observing',
                status_note TEXT,
                last_event_ts REAL,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS raw_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT NOT NULL,
                asset_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                feed_ts REAL NOT NULL,
                received_ts REAL NOT NULL,
                best_bid REAL,
                best_ask REAL,
                last_trade REAL,
                observed_price REAL,
                price_source TEXT,
                event_type TEXT,
                FOREIGN KEY(slug) REFERENCES markets(slug)
            );
            CREATE INDEX IF NOT EXISTS idx_raw_ticks_slug_ts ON raw_ticks(slug, feed_ts);
            CREATE INDEX IF NOT EXISTS idx_markets_start_ts ON markets(start_ts DESC);
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id INTEGER PRIMARY KEY,
                created_at REAL NOT NULL
            );
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def ensure_market(self, market: MarketInfo) -> None:
        existing = self.conn.execute("SELECT slug FROM markets WHERE slug=?", (market.slug,)).fetchone()
        if existing:
            self.conn.execute(
                "UPDATE markets SET title=?, token_up=?, token_down=? WHERE slug=?",
                (market.title, market.token_up, market.token_down, market.slug),
            )
        else:
            self.conn.execute(
                "INSERT INTO markets (slug,title,start_ts,end_ts,token_up,token_down,created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (market.slug, market.title, market.start_ts, market.end_ts,
                 market.token_up, market.token_down, time.time()),
            )
        self.conn.commit()

    def set_connected(self, slug: str, connected_ts: float, start_ts: int) -> None:
        late = connected_ts > start_ts + 4
        self.conn.execute(
            "UPDATE markets SET first_connected_ts=COALESCE(first_connected_ts, ?), "
            "observation_status=CASE WHEN ? THEN 'incomplete' ELSE observation_status END, "
            "status_note=CASE WHEN ? THEN COALESCE(status_note, 'Connected after market start; earlier events may be missed') ELSE status_note END "
            "WHERE slug=?",
            (connected_ts, int(late), int(late), slug),
        )
        self.conn.commit()

    def mark_gap(self, slug: str, reason: str) -> None:
        self.conn.execute(
            "UPDATE markets SET data_gap=1, observation_status='incomplete', status_note=? WHERE slug=?",
            (reason[:500], slug),
        )
        self.conn.commit()

    def save_tick(self, slug: str, state: PriceState, event_type: str, received_ts: float) -> None:
        self.conn.execute(
            "UPDATE markets SET has_price_data=1, last_event_ts=? WHERE slug=?",
            (state.feed_ts or received_ts, slug),
        )
        if STORE_RAW_TICKS:
            self.conn.execute(
                "INSERT INTO raw_ticks (slug,asset_id,direction,feed_ts,received_ts,best_bid,best_ask,last_trade,observed_price,price_source,event_type) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (slug, state.asset_id, state.direction, state.feed_ts or received_ts,
                 received_ts, state.best_bid, state.best_ask, state.last_trade,
                 state.observed_price, state.price_source, event_type),
            )
        self.conn.commit()

    def record_sweet_spot(self, slug: str, direction: str, price: float,
                          btc_diff: Optional[float], event_ts: float,
                          end_ts: float) -> bool:
        remaining = max(0, int(round(end_ts - event_ts)))
        elapsed = max(0, int(round(event_ts - (end_ts - WINDOW_SECONDS))))
        cursor = self.conn.execute(
            "UPDATE markets SET sweet_spot_direction=?, sweet_spot_price=?, btc_price_diff_usd=?, "
            "sweet_spot_time=?, sweet_spot_elapsed_seconds=?, sweet_spot_event_ts=?, "
            "sweet_spot_timing_category=?, initial_qualifying_direction=? "
            "WHERE slug=? AND sweet_spot_direction IS NULL",
            (direction, price, btc_diff, remaining_label(end_ts, event_ts), elapsed,
             event_ts, sweet_spot_category(end_ts, event_ts), direction, slug),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def record_pullback(self, slug: str, direction: str, price: float,
                        event_ts: float, end_ts: float) -> bool:
        elapsed = max(0, int(round(event_ts - (end_ts - WINDOW_SECONDS))))
        cursor = self.conn.execute(
            "UPDATE markets SET pullback_occurrence='YES', pullback_price=?, pullback_time=?, "
            "pullback_elapsed_seconds=?, pullback_event_ts=?, pullback_direction=? "
            "WHERE slug=? AND initial_qualifying_direction=? AND pullback_occurrence IS NULL",
            (price, remaining_label(end_ts, event_ts), elapsed, event_ts, direction,
             slug, direction),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def record_opposite_80(self, slug: str, price: float, event_ts: float) -> bool:
        cursor = self.conn.execute(
            "UPDATE markets SET opposite_reached_80=1, opposite_80_price=?, opposite_80_event_ts=? "
            "WHERE slug=? AND opposite_reached_80=0",
            (price, event_ts, slug),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def finish_observation(self, slug: str) -> None:
        row = self.get_market(slug)
        if not row:
            return
        if not row["has_price_data"]:
            status = "data_missing"
            note = row["status_note"] or "No usable Polymarket price observations were received"
        elif row["data_gap"] or row["observation_status"] == "incomplete":
            status = "incomplete"
            note = row["status_note"] or "One or more observation gaps may have occurred"
        else:
            status = "complete"
            note = row["status_note"]
        self.conn.execute(
            "UPDATE markets SET observation_status=?, status_note=?, "
            "pullback_occurrence=COALESCE(pullback_occurrence,'NO') WHERE slug=?",
            (status, note, slug),
        )
        self.conn.commit()

    def set_resolution(self, slug: str, resolution: str) -> None:
        row = self.get_market(slug)
        if not row:
            return
        switch_result = switch_up_result(
            row["initial_qualifying_direction"], bool(row["opposite_reached_80"]), resolution
        )
        pullback_occurrence = row["pullback_occurrence"] or "NO"
        self.conn.execute(
            "UPDATE markets SET final_resolution=?, switch_up_occurrence=?, pullback_occurrence=? WHERE slug=?",
            (resolution, switch_result, pullback_occurrence, slug),
        )
        self.conn.commit()

    def set_resolution_missing(self, slug: str, note: str) -> None:
        self.conn.execute(
            "UPDATE markets SET observation_status='incomplete', status_note=? WHERE slug=?",
            (note[:500], slug),
        )
        self.conn.commit()

    def get_market(self, slug: str) -> Optional[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM markets WHERE slug=?", (slug,)).fetchone()

    def latest_market(self) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM markets ORDER BY start_ts DESC LIMIT 1"
        ).fetchone()

    def stats(self) -> dict[str, int]:
        row = self.conn.execute(
            "SELECT COUNT(*) AS markets, "
            "SUM(CASE WHEN observation_status='complete' THEN 1 ELSE 0 END) AS complete, "
            "SUM(CASE WHEN observation_status IN ('incomplete','data_missing') THEN 1 ELSE 0 END) AS incomplete, "
            "SUM(CASE WHEN sweet_spot_direction IS NOT NULL THEN 1 ELSE 0 END) AS sweet_spots, "
            "SUM(CASE WHEN pullback_occurrence='YES' THEN 1 ELSE 0 END) AS pullbacks, "
            "SUM(CASE WHEN switch_up_occurrence='YES' THEN 1 ELSE 0 END) AS switchups, "
            "SUM(CASE WHEN pullback_occurrence='YES' AND final_resolution IS NOT NULL THEN 1 ELSE 0 END) AS resolved_pullbacks, "
            "SUM(CASE WHEN pullback_occurrence='YES' AND switch_up_occurrence='YES' AND final_resolution IS NOT NULL THEN 1 ELSE 0 END) AS pullback_switchups, "
            "SUM(CASE WHEN final_resolution IS NOT NULL THEN 1 ELSE 0 END) AS resolved "
            "FROM markets"
        ).fetchone()
        return {key: int(row[key] or 0) for key in row.keys()}

    def add_subscriber(self, chat_id: int) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO subscribers(chat_id,created_at) VALUES(?,?)",
            (chat_id, time.time()),
        )
        self.conn.commit()

    def remove_subscriber(self, chat_id: int) -> None:
        self.conn.execute("DELETE FROM subscribers WHERE chat_id=?", (chat_id,))
        self.conn.commit()

    def subscribers(self) -> list[int]:
        return [int(row[0]) for row in self.conn.execute("SELECT chat_id FROM subscribers")]

    def export_csv(self, output_path: Path) -> int:
        rows = self.conn.execute("SELECT * FROM markets ORDER BY start_ts ASC").fetchall()
        with output_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=[field for field, _ in SUMMARY_FIELDS])
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row[column] for field, column in SUMMARY_FIELDS})
        return len(rows)


class ObserverService:
    def __init__(self, db: Database, runtime: RuntimeState):
        self.db = db
        self.runtime = runtime
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self, application: Application) -> None:
        timeout = aiohttp.ClientTimeout(total=12, connect=8, sock_read=10)
        self.session = aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "BTC-5m-Observer/1.0"})
        self.runtime.btc_task = asyncio.create_task(self._binance_price_loop(), name="binance-price-feed")
        self.runtime.observer_task = asyncio.create_task(self._market_loop(application), name="polymarket-observer")
        log.info("Observer service started. Database: %s", DATABASE_PATH.resolve())

    async def stop(self) -> None:
        self.runtime.stopping.set()
        tasks = [task for task in [self.runtime.observer_task, self.runtime.btc_task] if task]
        tasks += list(self.runtime.settlement_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self.session:
            await self.session.close()

    async def _get_json(self, url: str, params: Optional[dict[str, Any]] = None) -> Any:
        assert self.session is not None
        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json(content_type=None)

    async def fetch_market(self, start_ts: int) -> MarketInfo:
        slug = f"btc-updown-5m-{start_ts}"
        record = await self._get_json(f"{GAMMA_BASE}/markets/slug/{slug}")
        if isinstance(record, list):
            if not record:
                raise ValueError(f"Gamma returned no market for {slug}")
            record = record[0]
        if not isinstance(record, dict):
            raise ValueError(f"Unexpected Gamma market response for {slug}")
        token_up, token_down = parse_market_tokens(record)
        title = str(record.get("question") or record.get("title") or slug)
        return MarketInfo(slug, title, start_ts, start_ts + WINDOW_SECONDS,
                          token_up, token_down, record)

    async def fetch_open_reference(self, start_ts: int) -> Optional[float]:
        """Use Binance's 5-minute BTCUSDT candle open as a documented BTC proxy."""
        try:
            data = await self._get_json(
                f"{BINANCE_REST}/api/v3/klines",
                {"symbol": "BTCUSDT", "interval": "5m", "startTime": start_ts * 1000, "limit": 1},
            )
            if data and int(data[0][0]) == start_ts * 1000:
                return to_float(data[0][1])
        except Exception as exc:
            log.warning("Could not retrieve Binance opening reference for %s: %s", start_ts, exc)
        return None

    async def fetch_current_btc(self) -> Optional[float]:
        try:
            payload = await self._get_json(f"{BINANCE_REST}/api/v3/ticker/price", {"symbol": "BTCUSDT"})
            return to_float(payload.get("price")) if isinstance(payload, dict) else None
        except Exception as exc:
            log.warning("Could not retrieve Binance BTC spot fallback: %s", exc)
            return None

    async def _binance_price_loop(self) -> None:
        assert self.session is not None
        while not self.runtime.stopping.is_set():
            ws = None
            try:
                ws = await self.session.ws_connect(BINANCE_WS_URL, heartbeat=20, autoping=True)
                self.runtime.binance_connected = True
                log.info("Connected to Binance BTCUSDT trade stream")
                async for message in ws:
                    if self.runtime.stopping.is_set():
                        break
                    if message.type == aiohttp.WSMsgType.TEXT:
                        try:
                            event = json.loads(message.data)
                        except json.JSONDecodeError:
                            continue
                        price = to_float(event.get("p"))
                        if price is not None:
                            event_ts = to_float(event.get("T")) or time.time() * 1000
                            if event_ts > 100_000_000_000:
                                event_ts /= 1000.0
                            self.runtime.btc_price = price
                            self.runtime.btc_price_ts = event_ts
                            self.runtime.last_binance_event_ts = time.time()
                    elif message.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                        break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.runtime.last_error = f"Binance feed: {exc}"
                log.warning("Binance stream disconnected: %s", exc)
            finally:
                self.runtime.binance_connected = False
                if ws and not ws.closed:
                    await ws.close()
            if not self.runtime.stopping.is_set():
                await asyncio.sleep(2)

    async def _market_loop(self, application: Application) -> None:
        while not self.runtime.stopping.is_set():
            now = time.time()
            start_ts = int(now // WINDOW_SECONDS) * WINDOW_SECONDS
            slug = f"btc-updown-5m-{start_ts}"
            try:
                market = await self.fetch_market(start_ts)
                self.db.ensure_market(market)
                self.runtime.open_references[slug] = await self.fetch_open_reference(start_ts)
                self.runtime.active_slug = slug
                self.runtime.active_start_ts = market.start_ts
                self.runtime.active_end_ts = market.end_ts
                await self._observe_market(application, market)
                self.db.finish_observation(slug)
                task = asyncio.create_task(self._settle_market(application, market), name=f"settle-{slug}")
                self.runtime.settlement_tasks.add(task)
                task.add_done_callback(self.runtime.settlement_tasks.discard)
                self.runtime.active_slug = None
                self.runtime.active_start_ts = None
                self.runtime.active_end_ts = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.runtime.last_error = f"Market loop: {exc}"
                log.exception("Market observation failed: %s", exc)
                await asyncio.sleep(3)

    async def _observe_market(self, application: Application, market: MarketInfo) -> None:
        token_map = market.token_map()
        price_states = {
            market.token_up: PriceState("UP", market.token_up),
            market.token_down: PriceState("DOWN", market.token_down),
        }
        self.runtime.up_price = None
        self.runtime.down_price = None
        while time.time() < market.end_ts and not self.runtime.stopping.is_set():
            ws = None
            try:
                assert self.session is not None
                ws = await self.session.ws_connect(CLOB_WS_URL, heartbeat=15, autoping=True)
                connected_ts = time.time()
                self.runtime.clob_connected = True
                self.db.set_connected(market.slug, connected_ts, market.start_ts)
                await ws.send_json({"type": "market", "assets_ids": [market.token_up, market.token_down]})
                log.info("Subscribed to %s", market.slug)
                while time.time() < market.end_ts and not self.runtime.stopping.is_set():
                    timeout = max(1.0, min(20.0, market.end_ts - time.time()))
                    try:
                        message = await ws.receive(timeout=timeout)
                    except asyncio.TimeoutError:
                        # Idle period alone isn't proof of data loss; keep the socket alive.
                        continue
                    if message.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(message.data)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(payload, list):
                            for item in payload:
                                if isinstance(item, dict):
                                    await self._handle_clob_payload(application, market, item, token_map, price_states)
                        elif isinstance(payload, dict):
                            await self._handle_clob_payload(application, market, payload, token_map, price_states)
                    elif message.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                        raise ConnectionError(f"CLOB WebSocket closed ({message.type})")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if time.time() < market.end_ts:
                    self.db.mark_gap(market.slug, f"Polymarket WebSocket interruption: {exc}")
                    self.runtime.last_error = f"Polymarket feed: {exc}"
                    log.warning("Market WebSocket interruption for %s: %s", market.slug, exc)
            finally:
                self.runtime.clob_connected = False
                if ws and not ws.closed:
                    await ws.close()
            if time.time() < market.end_ts and not self.runtime.stopping.is_set():
                await asyncio.sleep(1)

    async def _handle_clob_payload(self, application: Application, market: MarketInfo,
                                   payload: dict[str, Any], token_map: dict[str, str],
                                   price_states: dict[str, PriceState]) -> None:
        event_type = str(payload.get("event_type") or payload.get("type") or "").lower()
        received_ts = time.time()
        top_ts = event_timestamp(payload, received_ts)
        changes: list[tuple[str, dict[str, Any], str, float]] = []

        if event_type == "price_change" or isinstance(payload.get("price_changes"), list):
            for item in payload.get("price_changes", []):
                if not isinstance(item, dict):
                    continue
                asset_id = str(item.get("asset_id") or "")
                if asset_id in token_map:
                    changes.append((asset_id, item, event_type or "price_change", event_timestamp(item, top_ts)))
        else:
            asset_id = str(payload.get("asset_id") or "")
            if asset_id in token_map and event_type in {
                "book", "last_trade_price", "best_bid_ask", "price_change"
            }:
                changes.append((asset_id, payload, event_type, top_ts))

        for asset_id, item, kind, feed_ts in changes:
            # Ignore delayed events that belong to a previous/next five-minute window.
            if feed_ts < market.start_ts or feed_ts >= market.end_ts:
                continue
            state = price_states[asset_id]
            if kind == "book":
                bids = item.get("bids") or []
                asks = item.get("asks") or []
                bid_prices = [to_float(level.get("price")) for level in bids if isinstance(level, dict)]
                ask_prices = [to_float(level.get("price")) for level in asks if isinstance(level, dict)]
                valid_bids = [value for value in bid_prices if value is not None]
                valid_asks = [value for value in ask_prices if value is not None]
                state.best_bid = max(valid_bids) if valid_bids else None
                state.best_ask = min(valid_asks) if valid_asks else None
            else:
                if "best_bid" in item:
                    state.best_bid = to_float(item.get("best_bid"))
                if "best_ask" in item:
                    state.best_ask = to_float(item.get("best_ask"))
                if kind == "last_trade_price":
                    state.last_trade = to_float(item.get("price"))

            if (state.best_bid is not None and state.best_ask is not None
                    and 0 <= state.best_bid <= 1 and 0 <= state.best_ask <= 1
                    and state.best_bid <= state.best_ask):
                state.observed_price = (state.best_bid + state.best_ask) / 2
                state.price_source = "midpoint"
            elif state.last_trade is not None and 0 <= state.last_trade <= 1:
                state.observed_price = state.last_trade
                state.price_source = "last_trade_fallback"
            else:
                state.observed_price = None
                state.price_source = None
            state.feed_ts = feed_ts
            self.runtime.last_polymarket_event_ts = received_ts
            self.runtime.ticks_received += 1
            if state.direction == "UP":
                self.runtime.up_price = state.observed_price
            else:
                self.runtime.down_price = state.observed_price
            if state.observed_price is None:
                continue
            self.db.save_tick(market.slug, state, kind, received_ts)
            await self._process_observation(application, market, state, price_states, feed_ts)

    async def _btc_difference(self, market: MarketInfo, event_ts: float) -> Optional[float]:
        reference = self.runtime.open_references.get(market.slug)
        if reference is None:
            reference = await self.fetch_open_reference(market.start_ts)
            self.runtime.open_references[market.slug] = reference
        current = self.runtime.btc_price
        btc_ts = self.runtime.btc_price_ts
        if current is None or btc_ts is None or abs(time.time() - btc_ts) > 5:
            current = await self.fetch_current_btc()
        if reference is None or current is None:
            return None
        # Store only the difference; neither reference nor event BTC price is stored separately.
        return round(current - reference, 2)

    async def _process_observation(self, application: Application, market: MarketInfo,
                                   state: PriceState,
                                   price_states: dict[str, PriceState], event_ts: float) -> None:
        price = state.observed_price
        if price is None:
            return
        row = self.db.get_market(market.slug)
        if not row:
            return

        if row["sweet_spot_direction"] is None and is_sweet_spot(price):
            btc_diff = await self._btc_difference(market, event_ts)
            created = self.db.record_sweet_spot(
                market.slug, state.direction, price, btc_diff, event_ts, market.end_ts
            )
            if created:
                remaining = max(0, int(round(market.end_ts - event_ts)))
                diff_text = f"{btc_diff:+.2f} USD" if btc_diff is not None else "unavailable"
                text = (
                    f"🎯 *Sweet Spot*\nMarket: `{market.slug}`\n"
                    f"Direction: *{state.direction}*\nShare price: *${price:.4f}*\n"
                    f"BTC difference: *{diff_text}*\n"
                    f"Time remaining: *{remaining_label(market.end_ts, event_ts)}*\n"
                    f"Timing category: *{sweet_spot_category(market.end_ts, event_ts)}*"
                )
                await self.notify(application, text)

        row = self.db.get_market(market.slug)
        if not row or not row["initial_qualifying_direction"]:
            return
        # Price events can arrive slightly out of order; later event rules must
        # not use a quote timestamp older than the first Sweet Spot timestamp.
        sweet_spot_ts = row["sweet_spot_event_ts"]
        if sweet_spot_ts is not None and event_ts < sweet_spot_ts:
            return
        initial = row["initial_qualifying_direction"]
        # The first price at or below 0.55 on the initial Sweet Spot side is the pullback price.
        if is_pullback(initial, state.direction, price):
            if self.db.record_pullback(market.slug, state.direction, price, event_ts, market.end_ts):
                remaining = remaining_label(market.end_ts, event_ts)
                await self.notify(
                    application,
                    f"↩️ *Pullback detected*\nMarket: `{market.slug}`\n"
                    f"Initial direction: *{initial}*\nFirst observed pullback price: *${price:.4f}*\n"
                    f"Time remaining: *{remaining}*",
                )
        if is_opposite_80(state.direction, initial, price):
            if self.db.record_opposite_80(market.slug, price, event_ts):
                await self.notify(
                    application,
                    f"🔄 *Opposite side reached $0.80+*\nMarket: `{market.slug}`\n"
                    f"Initial direction: *{initial}*\nOpposite direction: *{state.direction}*\n"
                    f"Observed price: *${price:.4f}*\nSwitch Up will be decided after resolution.",
                )

    async def _settle_market(self, application: Application, market: MarketInfo) -> None:
        # Resolution may lag the five-minute trading window; don't block observing the next one.
        deadline = time.time() + 600
        while time.time() < deadline and not self.runtime.stopping.is_set():
            try:
                record = await self._get_json(f"{GAMMA_BASE}/markets/slug/{market.slug}")
                if isinstance(record, list):
                    record = record[0] if record else {}
                if isinstance(record, dict):
                    resolution = self._resolution_from_gamma(record)
                    if resolution:
                        self.db.set_resolution(market.slug, resolution)
                        row = self.db.get_market(market.slug)
                        if row:
                            await self.notify(
                                application,
                                f"🏁 *Market resolved*\nMarket: `{market.slug}`\n"
                                f"Resolution: *{resolution}*\n"
                                f"Pullback: *{row['pullback_occurrence'] or 'NO'}*\n"
                                f"Switch Up: *{row['switch_up_occurrence'] or 'NO'}*\n"
                                f"Observation status: *{row['observation_status']}*",
                            )
                        return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.debug("Resolution lookup pending for %s: %s", market.slug, exc)
            await asyncio.sleep(4)
        self.db.set_resolution_missing(market.slug, "Gamma resolution was not confirmed within 10 minutes")
        log.warning("No confirmed resolution found for %s", market.slug)

    @staticmethod
    def _resolution_from_gamma(record: dict[str, Any]) -> Optional[str]:
        if not bool_value(record.get("closed")):
            return None
        outcomes = parse_json_list(record.get("outcomes"))
        prices = parse_json_list(record.get("outcomePrices") or record.get("outcome_prices"))
        for outcome, price in zip(outcomes, prices):
            numeric = to_float(price)
            if numeric is not None and numeric >= 0.99:
                normalized = str(outcome).strip().upper()
                if normalized in {"UP", "DOWN"}:
                    return normalized
        winner = record.get("winner")
        if isinstance(winner, str) and winner.strip().upper() in {"UP", "DOWN"}:
            return winner.strip().upper()
        for token in record.get("tokens", []) if isinstance(record.get("tokens"), list) else []:
            if isinstance(token, dict) and bool_value(token.get("winner")):
                normalized = str(token.get("outcome", "")).strip().upper()
                if normalized in {"UP", "DOWN"}:
                    return normalized
        return None

    async def notify(self, application: Application, text: str) -> None:
        if not ALERTS_ENABLED:
            return
        for chat_id in self.db.subscribers():
            try:
                await application.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
            except Exception as exc:
                log.warning("Telegram notification failed for chat %s: %s", chat_id, exc)


DB = Database(DATABASE_PATH)
RUNTIME = RuntimeState()
SERVICE = ObserverService(DB, RUNTIME)


def chat_is_allowed(update: Update) -> bool:
    if not ALLOWED_CHAT_IDS:
        return True
    chat = update.effective_chat
    return chat is not None and chat.id in ALLOWED_CHAT_IDS


async def guard(update: Update) -> bool:
    if not chat_is_allowed(update):
        if update.effective_message:
            await update.effective_message.reply_text("This bot is private. Add this Telegram chat ID to TELEGRAM_ALLOWED_CHAT_IDS in your .env file.")
        return False
    return True


async def cmd_myid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat and update.effective_message:
        await update.effective_message.reply_text(f"This chat ID is: `{update.effective_chat.id}`", parse_mode="Markdown")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard(update):
        return
    chat = update.effective_chat
    message = update.effective_message
    if chat and message:
        DB.add_subscriber(chat.id)
        await message.reply_text(
            "✅ *BTC 5-Minute Observer connected.*\n\n"
            "This bot only observes prices and records events. It never places trades.\n\n"
            "Commands: /status /latest /stats /export /stopalerts /help",
            parse_mode="Markdown",
        )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard(update):
        return
    if update.effective_message:
        await update.effective_message.reply_text(
            "*BTC Observer commands*\n"
            "/start - subscribe this chat to event alerts\n"
            "/status - feed and active-market status\n"
            "/latest - latest stored market summary\n"
            "/stats - aggregate observation counts\n"
            "/export - send market summaries as CSV\n"
            "/stopalerts - stop alerts in this chat\n"
            "/myid - show this chat ID\n\n"
            "Share prices use the best-bid/ask midpoint where possible, with last-trade fallback. BTC difference uses Binance BTCUSDT as a proxy.",
            parse_mode="Markdown",
        )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard(update):
        return
    now = time.time()
    slug = RUNTIME.active_slug or "None"
    remaining = "—"
    if RUNTIME.active_end_ts:
        left = max(0, int(RUNTIME.active_end_ts - now))
        remaining = f"{left // 60}:{left % 60:02d}"
    latest_poly = now_utc_iso(RUNTIME.last_polymarket_event_ts) if RUNTIME.last_polymarket_event_ts else "Never"
    latest_btc = now_utc_iso(RUNTIME.last_binance_event_ts) if RUNTIME.last_binance_event_ts else "Never"
    text = (
        "*BTC Observer status*\n"
        f"Polymarket WebSocket: *{'Connected' if RUNTIME.clob_connected else 'Disconnected/rotating'}*\n"
        f"Binance BTC feed: *{'Connected' if RUNTIME.binance_connected else 'Disconnected/retrying'}*\n"
        f"Active market: `{slug}`\n"
        f"Time remaining: *{remaining}*\n"
        f"Latest UP midpoint/price: *{format_price(RUNTIME.up_price)}*\n"
        f"Latest DOWN midpoint/price: *{format_price(RUNTIME.down_price)}*\n"
        f"Latest BTCUSDT: *{format_price(RUNTIME.btc_price)}*\n"
        f"Polymarket event received: `{latest_poly}`\n"
        f"Binance trade received: `{latest_btc}`\n"
        f"Raw/event messages handled: *{RUNTIME.ticks_received}*\n"
        f"Subscribers: *{len(DB.subscribers())}*\n"
        f"Database: `{DATABASE_PATH}`"
    )
    if RUNTIME.last_error:
        text += f"\nLast warning: `{RUNTIME.last_error[:200]}`"
    if update.effective_message:
        await update.effective_message.reply_text(text, parse_mode="Markdown")


def format_price(price: Optional[float]) -> str:
    return "—" if price is None else f"${price:.4f}"


async def cmd_latest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard(update):
        return
    row = DB.latest_market()
    if not row:
        text = "No markets have been recorded yet."
    else:
        diff = f"{row['btc_price_diff_usd']:+.2f} USD" if row["btc_price_diff_usd"] is not None else "unavailable"
        text = (
            "*Latest market record*\n"
            f"Market: `{row['slug']}`\n"
            f"Status: *{row['observation_status']}*\n"
            f"Sweet Spot: *{row['sweet_spot_direction'] or 'None'}* at *{format_price(row['sweet_spot_price'])}*\n"
            f"BTC difference: *{diff}*\n"
            f"Sweet Spot time: `{row['sweet_spot_time'] or '—'}`\n"
            f"Timing category: *{row['sweet_spot_timing_category'] or '—'}*\n"
            f"Pullback: *{row['pullback_occurrence'] or 'Pending'}* at *{format_price(row['pullback_price'])}*\n"
            f"Pullback time: `{row['pullback_time'] or '—'}`\n"
            f"Switch Up: *{row['switch_up_occurrence'] or 'Pending resolution'}*\n"
            f"Final resolution: *{row['final_resolution'] or 'Pending'}*"
        )
    if update.effective_message:
        await update.effective_message.reply_text(text, parse_mode="Markdown")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard(update):
        return
    stats = DB.stats()
    resolved = stats["resolved"]
    pullback_pct = 100 * stats["pullbacks"] / stats["sweet_spots"] if stats["sweet_spots"] else 0
    resolved_pullbacks = stats["resolved_pullbacks"]
    pullback_switchups = stats["pullback_switchups"]
    switch_pct = 100 * pullback_switchups / resolved_pullbacks if resolved_pullbacks else 0
    text = (
        "*Observation statistics*\n"
        f"Markets seen: *{stats['markets']}*\n"
        f"Complete observations: *{stats['complete']}*\n"
        f"Incomplete/data missing: *{stats['incomplete']}*\n"
        f"Sweet Spots: *{stats['sweet_spots']}*\n"
        f"Pullbacks: *{stats['pullbacks']}* ({pullback_pct:.1f}% of Sweet Spots)\n"
        f"Switch Up events: *{stats['switchups']}*\n"
        f"Pullbacks that became Switch Ups: *{pullback_switchups}/{resolved_pullbacks}* ({switch_pct:.1f}% of resolved Pullbacks)\n"
        f"Resolved markets: *{resolved}*\n\n"
        "These are descriptive counts, not evidence that a trading strategy is profitable."
    )
    if update.effective_message:
        await update.effective_message.reply_text(text, parse_mode="Markdown")


async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard(update):
        return
    if not update.effective_message:
        return
    temp_path: Optional[Path] = None
    try:
        fd, path_string = tempfile.mkstemp(prefix="btc_observations_", suffix=".csv")
        os.close(fd)
        temp_path = Path(path_string)
        count = DB.export_csv(temp_path)
        with temp_path.open("rb") as handle:
            await update.effective_message.reply_document(
                document=handle,
                filename="btc_5m_observations.csv",
                caption=f"Exported {count} BTC 5-minute market summary rows.",
            )
    except Exception as exc:
        await update.effective_message.reply_text(f"CSV export failed: {exc}")
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink(missing_ok=True)


async def cmd_stopalerts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard(update):
        return
    if update.effective_chat and update.effective_message:
        DB.remove_subscriber(update.effective_chat.id)
        await update.effective_message.reply_text("Alerts stopped for this chat. The observer will continue collecting data.")


async def post_init(application: Application) -> None:
    await SERVICE.start(application)
    try:
        await application.bot.set_my_commands([
            BotCommand("start", "Subscribe to observation alerts"),
            BotCommand("status", "Show feed and market status"),
            BotCommand("latest", "Show latest stored market"),
            BotCommand("stats", "Show aggregate counts"),
            BotCommand("export", "Export market summaries as CSV"),
            BotCommand("stopalerts", "Stop alerts for this chat"),
            BotCommand("myid", "Show this chat ID"),
            BotCommand("help", "Show all commands"),
        ])
    except Exception as exc:
        log.warning("Could not set Telegram command menu: %s", exc)


async def post_shutdown(application: Application) -> None:
    await SERVICE.stop()
    DB.close()


def main() -> None:
    if not TELEGRAM_TOKEN:
        raise SystemExit(
            "Missing TELEGRAM_BOT_TOKEN. Copy .env.example to .env and add the token from @BotFather."
        )
    application = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    application.add_handler(CommandHandler("myid", cmd_myid))
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("latest", cmd_latest))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CommandHandler("export", cmd_export))
    application.add_handler(CommandHandler("stopalerts", cmd_stopalerts))
    log.info("Starting Telegram polling. Observation only; no trading orders are submitted.")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        try:
            DB.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
