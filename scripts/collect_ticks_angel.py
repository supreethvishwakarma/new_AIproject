#!/usr/bin/env python3
"""
Angel One NIFTY Tick Collector
──────────────────────────────
Live tick feed for the NIFTY index future via Angel One SmartWebSocket V2.

This is the Angel One counterpart to `scripts/collect_ticks.py` (which is
TrueData-only). It:

  1. Logs into Angel One SmartAPI (API key + client id + PIN + TOTP)
  2. Resolves the near-month NIFTY FUTIDX contract token from Angel One's
     public instrument master (NFO segment)
  3. Subscribes to that token on SmartWebSocket V2 (NFO exchangeType = 2)
  4. Aggregates 1-min candles into `minute_candles` under "NIFTY-I" — the
     symbol the dashboard scanner / feature pipeline expects
  5. Writes a live-price cache file every second for the Flask backend

Usage:
  python scripts/collect_ticks_angel.py                       # stream until stopped
  python scripts/collect_ticks_angel.py --test                # login + 15s of ticks, exit
  python scripts/collect_ticks_angel.py --backfill-days 60    # pull 60d history first
  python scripts/collect_ticks_angel.py --backfill-only --backfill-days 60

Notes:
  - Angel One sends NSE/NFO prices ×100 (paise). The first tick is logged
    raw + scaled so you can eyeball it; override with ANGEL_PRICE_SCALE.
  - Only the underlying future is subscribed (enough for price + regime +
    signal scanning). Option-leg pricing for open paper positions still
    falls back to the DB.
"""

from __future__ import annotations

import os
import sys
import json
import time
import signal
import argparse
import threading
from datetime import datetime, date, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import requests

from config.settings import BASE_DIR
from database.db import upsert_candles, get_engine
from utils.helpers import now_ist
from utils.logger import get_logger

logger = get_logger("collect_ticks_angel")

# ── Config ───────────────────────────────────────────────────────────────────
SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
NFO_EXCHANGE_TYPE = 2         # SmartWebSocket exchangeType for NFO (futures/options)
SNAP_QUOTE_MODE = 3          # 1=LTP, 2=Quote, 3=SnapQuote
PRICE_SCALE = float(os.getenv("ANGEL_PRICE_SCALE", "100"))  # paise -> rupees

PRIMARY_UNDERLYING = os.getenv("PRIMARY_UNDERLYING", "NIFTY")

# The scanner in backend/app.py reads `minute_candles WHERE symbol = 'NIFTY-I'`
SCAN_SYMBOL = f"{PRIMARY_UNDERLYING}-I"

LIVE_CACHE_FILE = Path(
    os.getenv("LIVE_CACHE_FILE") or (BASE_DIR / "logs" / "live_prices.json")
)

# ── State ────────────────────────────────────────────────────────────────────
running = True
_first_tick_logged = False
candle_buffer: dict[str, list] = {}     # symbol -> ticks for the current minute
last_minute: dict[str, datetime] = {}   # symbol -> minute bucket last seen
live_price_cache: dict[str, dict] = {}  # symbol -> {price, bid, ask, ts}
_last_tick_wall = time.time()


def _stop(*_):
    global running
    running = False
    logger.info("Shutdown requested; stopping collector...")


signal.signal(signal.SIGINT, _stop)
try:
    signal.signal(signal.SIGTERM, _stop)
except (ValueError, AttributeError):
    pass


# ── Instrument master ────────────────────────────────────────────────────────

def resolve_future_token(underlying: str, override_token: str = "") -> dict:
    """Return {token, symbol, lotsize, expiry} for the nearest NIFTY FUTIDX contract.

    If override_token is given (env ANGEL_FUTURE_TOKEN or --future-token), skip
    the ~40MB instrument-master download entirely and trust that token.
    """
    override_token = override_token or os.getenv("ANGEL_FUTURE_TOKEN", "")
    if override_token:
        logger.info(f"Using override future token {override_token} (skipping master download)")
        return {"token": str(override_token), "symbol": f"{underlying}-FUT",
                "lotsize": 0, "expiry": "unknown"}

    logger.info(f"Downloading Angel instrument master to resolve {underlying} future...")
    resp = requests.get(SCRIP_MASTER_URL, timeout=60)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())

    fut = df[
        (df["exch_seg"] == "NFO")
        & (df["instrumenttype"] == "FUTIDX")
        & (df["name"] == underlying)
    ].copy()
    if fut.empty:
        raise RuntimeError(
            f"No NFO FUTIDX rows for {underlying} in Angel's master - "
            "check the underlying name or whether it's currently listed."
        )

    def _exp(s):
        try:
            return datetime.strptime(s, "%d%b%Y").date()
        except (ValueError, TypeError):
            return None

    fut["expiry_date"] = fut["expiry"].apply(_exp)
    fut = fut.dropna(subset=["expiry_date"])
    today = date.today()
    fut = fut[fut["expiry_date"] >= today].sort_values("expiry_date")
    if fut.empty:
        raise RuntimeError(f"All {underlying} FUTIDX contracts are expired in the master.")

    row = fut.iloc[0]
    info = {
        "token": str(row["token"]),
        "symbol": str(row["symbol"]),
        "lotsize": int(row.get("lotsize", 0) or 0),
        "expiry": row["expiry_date"].isoformat(),
    }
    logger.info(
        f"Resolved {underlying} future: {info['symbol']} "
        f"token={info['token']} expiry={info['expiry']} lot={info['lotsize']}"
    )
    return info


# ── Auth ─────────────────────────────────────────────────────────────────────

def angel_login():
    """Authenticate with Angel One; return (tokens dict, authenticated adapter)."""
    from broker.angelone_adapter import AngelOneAdapter

    adapter = AngelOneAdapter()
    if not adapter.has_credentials:
        raise RuntimeError(
            "Angel One credentials missing - set ANGEL_ONE_API_KEY / "
            "ANGEL_ONE_CLIENT_ID / ANGEL_ONE_PASSWORD (and ANGEL_ONE_TOTP_SECRET) in .env"
        )
    if not adapter.authenticate():
        raise RuntimeError("Angel One authentication failed - see log above.")

    auth_token = (adapter._access_token or "").replace("Bearer ", "").strip()
    tokens = {
        "auth_token": auth_token,
        "feed_token": adapter.feed_token,
        "api_key": adapter._api_key,
        "client_id": adapter._client_id,
    }
    if not tokens["feed_token"]:
        raise RuntimeError("Login succeeded but no feedToken returned - cannot open the stream.")
    logger.info(f"Angel One login OK for {tokens['client_id']}; feed token acquired.")
    return tokens, adapter


def backfill_candles(adapter, token: str, days: int) -> int:
    """
    Pull historical 1-min candles for the future token from Angel One's
    getCandleData and upsert them into minute_candles under SCAN_SYMBOL,
    so the backend scanner has enough history (it needs ~250 bars) without
    waiting hours for the live stream to accumulate them.
    """
    smart = adapter._smart_api
    if smart is None:
        raise RuntimeError("adapter not authenticated - no SmartConnect instance")

    total = 0
    now = now_ist()
    # ONE_MINUTE is capped at ~30 days/request; page day-by-day to stay well under.
    for d in range(days, 0, -1):
        day = (now - timedelta(days=d))
        if day.weekday() >= 5:      # skip Sat/Sun - NSE is closed
            continue
        start = day.replace(hour=9, minute=15, second=0, microsecond=0)
        end = day.replace(hour=15, minute=30, second=0, microsecond=0)
        if end > now:
            end = now
        params = {
            "exchange": "NFO",
            "symboltoken": token,
            "interval": "ONE_MINUTE",
            "fromdate": start.strftime("%Y-%m-%d %H:%M"),
            "todate": end.strftime("%Y-%m-%d %H:%M"),
        }
        try:
            resp = smart.getCandleData(params)
        except Exception as e:
            logger.warning(f"backfill {start.date()}: request failed: {e}")
            time.sleep(1)
            continue
        rows = (resp or {}).get("data") or []
        if not rows:
            logger.info(f"backfill {start.date()}: no data")
            time.sleep(0.4)
            continue
        recs = []
        for r in rows:
            # r = [timestamp, open, high, low, close, volume]
            ts = pd.to_datetime(r[0]).tz_localize(None)
            recs.append({
                "timestamp": ts.replace(second=0, microsecond=0),
                "symbol": SCAN_SYMBOL,
                "open": float(r[1]), "high": float(r[2]),
                "low": float(r[3]), "close": float(r[4]),
                "volume": int(r[5] or 0),
                "vwap": (float(r[1]) + float(r[2]) + float(r[3]) + float(r[4])) / 4,
                "oi": 0,
            })
        try:
            ins = upsert_candles(pd.DataFrame(recs))
            total += ins
            logger.info(f"backfill {start.date()}: {len(recs)} bars fetched, {ins} new")
        except Exception as e:
            logger.error(f"backfill {start.date()}: upsert failed: {e}")
        time.sleep(0.5)  # respect rate limit

    logger.info(f"Backfill complete: {total} new candles for {SCAN_SYMBOL}")
    return total


# ── Tick handling ────────────────────────────────────────────────────────────

def _to_dt(exchange_ts) -> datetime:
    """SmartWebSocket exchange_timestamp is epoch milliseconds."""
    try:
        ts = float(exchange_ts)
        if ts > 1e12:      # ms
            ts /= 1000.0
        if ts > 1e9:       # plausible epoch seconds
            return datetime.fromtimestamp(ts)
    except (TypeError, ValueError):
        pass
    return now_ist()


def normalize_tick(raw: dict) -> dict | None:
    global _first_tick_logged
    ltp = raw.get("last_traded_price")
    if ltp in (None, 0):
        return None
    price = float(ltp) / PRICE_SCALE

    buy = raw.get("best_5_buy_data") or []
    sell = raw.get("best_5_sell_data") or []
    bid = float(buy[0]["price"]) / PRICE_SCALE if buy and buy[0].get("price") else price
    ask = float(sell[0]["price"]) / PRICE_SCALE if sell and sell[0].get("price") else price

    if not _first_tick_logged:
        logger.info(
            f"FIRST TICK  raw last_traded_price={ltp}  ->  scaled Rs {price:.2f}  "
            f"(scale={PRICE_SCALE}; if this price looks wrong by a power of 10, "
            f"set ANGEL_PRICE_SCALE in .env)"
        )
        _first_tick_logged = True

    return {
        "timestamp": _to_dt(raw.get("exchange_timestamp")),
        "symbol": SCAN_SYMBOL,
        "price": price,
        "volume": int(raw.get("volume_trade_for_the_day", 0) or 0),
        "bid_price": bid,
        "ask_price": ask,
        "oi": int(raw.get("open_interest", 0) or 0),
    }


def _aggregate(symbol: str, ticks: list) -> dict | None:
    prices = [t["price"] for t in ticks if t.get("price", 0) > 0]
    if not prices:
        return None
    vols = [t.get("volume", 0) for t in ticks]
    ois = [t.get("oi", 0) for t in ticks]
    minute_ts = ticks[0]["timestamp"].replace(second=0, microsecond=0)
    # volume_trade_for_the_day is cumulative; per-minute volume is the delta
    minute_vol = max(vols) - min(vols) if len(vols) > 1 else 0
    return {
        "timestamp": minute_ts,
        "symbol": symbol,
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": int(minute_vol),
        "vwap": sum(prices) / len(prices),
        "oi": ois[-1] if ois else 0,
    }


def _write_candle(symbol: str, ticks: list):
    candle = _aggregate(symbol, ticks)
    if not candle:
        return
    try:
        upsert_candles(pd.DataFrame([candle]))
        logger.info(
            f"candle {symbol} {candle['timestamp']:%H:%M}  "
            f"O={candle['open']:.1f} H={candle['high']:.1f} "
            f"L={candle['low']:.1f} C={candle['close']:.1f} v={candle['volume']}"
        )
    except Exception as e:
        logger.error(f"Failed to write candle for {symbol}: {e}")


def on_tick(tick: dict):
    global _last_tick_wall
    _last_tick_wall = time.time()
    symbol = tick["symbol"]
    minute_ts = tick["timestamp"].replace(second=0, microsecond=0)

    if tick["price"] > 0:
        live_price_cache[symbol] = {
            "price": tick["price"],
            "bid": tick["bid_price"] or tick["price"],
            "ask": tick["ask_price"] or tick["price"],
            "ts": datetime.now().isoformat(),
        }

    if symbol not in candle_buffer:
        candle_buffer[symbol] = []
        last_minute[symbol] = minute_ts

    if minute_ts > last_minute.get(symbol, minute_ts):
        prev = candle_buffer[symbol]
        if prev:
            _write_candle(symbol, prev)
        candle_buffer[symbol] = []
        last_minute[symbol] = minute_ts

    candle_buffer[symbol].append(tick)


# ── Background flushers ──────────────────────────────────────────────────────

def _cache_flusher():
    LIVE_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    while running:
        try:
            tmp = LIVE_CACHE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(live_price_cache))
            tmp.replace(LIVE_CACHE_FILE)
        except Exception as e:
            logger.debug(f"cache flush error: {e}")
        time.sleep(1)


def _candle_timer():
    """Close a minute bucket even if the next tick is late (illiquid stretch)."""
    while running:
        time.sleep(5)
        cur = now_ist().replace(second=0, microsecond=0)
        for symbol in list(candle_buffer.keys()):
            if candle_buffer[symbol] and cur > last_minute.get(symbol, cur):
                _write_candle(symbol, candle_buffer[symbol])
                candle_buffer[symbol] = []
                last_minute[symbol] = cur


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Angel One NIFTY tick collector")
    parser.add_argument("--test", action="store_true",
                        help="Login, resolve token, stream ~15s, then exit")
    parser.add_argument("--backfill-days", type=int, default=0, metavar="N",
                        help="Before streaming, pull N days of historical 1-min "
                             "candles from Angel One so the scanner has history now")
    parser.add_argument("--backfill-only", action="store_true",
                        help="Do the backfill and exit (no live stream)")
    parser.add_argument("--future-token", default="", metavar="TOKEN",
                        help="Skip the instrument-master download; use this NFO "
                             "FUTIDX token directly (also env ANGEL_FUTURE_TOKEN)")
    args = parser.parse_args()

    logger.info("=" * 64)
    logger.info(f"  ANGEL ONE NIFTY TICK COLLECTOR - {PRIMARY_UNDERLYING} ({SCAN_SYMBOL})")
    logger.info(f"  cache file: {LIVE_CACHE_FILE}")
    logger.info("=" * 64)

    # DB sanity
    try:
        import sqlalchemy
        with get_engine().connect() as c:
            c.execute(sqlalchemy.text("SELECT 1"))
        logger.info("Database connection OK.")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return 1

    fut = resolve_future_token(PRIMARY_UNDERLYING, args.future_token)
    tokens, adapter = angel_login()

    if args.backfill_days > 0 or args.backfill_only:
        days = args.backfill_days or 5
        try:
            backfill_candles(adapter, fut["token"], days)
        except Exception as e:
            logger.error(f"Backfill failed: {e}")
        if args.backfill_only:
            return 0

    from SmartApi.smartWebSocketV2 import SmartWebSocketV2

    sws = SmartWebSocketV2(
        tokens["auth_token"], tokens["api_key"],
        tokens["client_id"], tokens["feed_token"],
        max_retry_attempt=50, retry_strategy=1, retry_delay=5,
    )

    token_list = [{"exchangeType": NFO_EXCHANGE_TYPE, "tokens": [fut["token"]]}]

    def _on_open(_wsapp):
        logger.info(f"SmartWebSocket open; subscribing to {fut['symbol']} ({fut['token']}) on NFO...")
        sws.subscribe("nifty_feed", SNAP_QUOTE_MODE, token_list)

    def _on_data(_wsapp, message):
        try:
            if isinstance(message, (bytes, bytearray)):
                return  # library also delivers parsed dicts; ignore raw frames
            tick = normalize_tick(message)
            if tick:
                on_tick(tick)
        except Exception as e:
            logger.error(f"tick handling error: {e}")

    def _on_error(_wsapp, error):
        logger.error(f"SmartWebSocket error: {error}")

    def _on_close(_wsapp):
        logger.warning("SmartWebSocket closed.")

    sws.on_open = _on_open
    sws.on_data = _on_data
    sws.on_error = _on_error
    sws.on_close = _on_close

    threading.Thread(target=_cache_flusher, daemon=True).start()
    threading.Thread(target=_candle_timer, daemon=True).start()

    if args.test:
        threading.Thread(target=_test_watchdog, args=(sws,), daemon=True).start()

    logger.info("Connecting to SmartWebSocket (blocking)...")
    # SmartWebSocketV2.connect() returns when the socket dies past its internal
    # retry budget. Supervise it here so a dropped feed doesn't end the process
    # (the Flask _ensure_collector fallback only checks every 30s).
    backoff = 5
    while running and not args.test:
        try:
            sws.connect()
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"SmartWebSocket connect loop ended: {e}")
        if not running:
            break
        logger.warning(f"Feed dropped; reconnecting in {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)
    if args.test:
        try:
            sws.connect()
        except Exception as e:
            logger.error(f"SmartWebSocket connect loop ended: {e}")

    # flush leftovers
    for symbol, ticks in candle_buffer.items():
        if ticks:
            _write_candle(symbol, ticks)
    logger.info("Collector stopped.")
    return 0


def _test_watchdog(sws):
    time.sleep(15)
    n = len(candle_buffer.get(SCAN_SYMBOL, []))
    logger.info(f"--test: {n} ticks buffered for {SCAN_SYMBOL}; cache={live_price_cache}")
    global running
    running = False
    try:
        sws.close_connection()
    except Exception:
        pass


if __name__ == "__main__":
    raise SystemExit(main())
