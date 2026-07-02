"""
The Portfolio Plug — Alpaca WebSocket Real-Time Data Stream
Replaces Yahoo Finance with Alpaca's real-time SIP feed.
Streams live 1-minute bars for all tickers and stores them in memory.
All other modules (volume_profile, pattern scanner, watchlist) read from this store.
"""

import os
import json
import logging
import threading
import time
import websocket
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import deque

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

ALPACA_API_KEY    = os.environ.get("ALPACA_API_KEY", "")
ALPACA_API_SECRET = os.environ.get("ALPACA_API_SECRET", "")
ALPACA_WS_URL     = "wss://stream.data.alpaca.markets/v2/sip"
ALPACA_REST_URL   = "https://data.alpaca.markets/v2"

# In-memory candle store — {ticker: deque of candle dicts, max 500 bars each}
_candle_store = {}
_store_lock   = threading.Lock()

# Keep ~90 trading days of 1-min bars (90 days × 390 bars/day ≈ 35,100 — rounded up)
MAX_BARS = 40000

TICKERS = ["SPY", "QQQ", "NVDA", "TSLA", "AMZN", "MSFT", "META", "GOOG"]


def _headers():
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET,
    }


# ── REST: backfill historical bars on startup ──────────────────────────────────
def backfill_historical(ticker: str, days: int = 90):
    """Pull last N days of 1-minute bars via REST (paginated) to seed the candle store."""
    try:
        end   = datetime.now(ET).strftime("%Y-%m-%dT%H:%M:%SZ")
        start = (datetime.now(ET) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        url   = f"{ALPACA_REST_URL}/stocks/{ticker}/bars"

        all_bars   = []
        page_token = None
        for _ in range(10):  # max 10 pages (100K bars) as a safety cap
            params = {
                "timeframe": "1Min",
                "start": start,
                "end": end,
                "limit": 10000,
                "feed": "sip",
                "sort": "asc",
            }
            if page_token:
                params["page_token"] = page_token

            resp = requests.get(url, headers=_headers(), params=params, timeout=20)
            if resp.status_code != 200:
                log.error(f"Alpaca REST backfill failed for {ticker}: {resp.status_code} {resp.text}")
                break

            data = resp.json()
            all_bars.extend(data.get("bars", []))
            page_token = data.get("next_page_token")
            if not page_token:
                break

        with _store_lock:
            if ticker not in _candle_store:
                _candle_store[ticker] = deque(maxlen=MAX_BARS)
            for bar in all_bars:
                _candle_store[ticker].append({
                    "t": bar["t"],
                    "o": bar["o"],
                    "h": bar["h"],
                    "l": bar["l"],
                    "c": bar["c"],
                    "v": bar["v"],
                })
        log.info(f"✅ Backfilled {len(all_bars)} bars for {ticker}")

    except Exception as e:
        log.error(f"Backfill error for {ticker}: {e}", exc_info=True)


def backfill_all():
    """Backfill historical data for all tickers on startup."""
    log.info("🔄 Backfilling historical data from Alpaca...")
    for ticker in TICKERS:
        backfill_historical(ticker)
    log.info("✅ Historical backfill complete for all tickers")


# ── WebSocket: stream live 1-minute bars ──────────────────────────────────────
_ws_app = None
_ws_connected = False

def _on_open(ws):
    global _ws_connected
    log.info("🔌 Alpaca WebSocket connected — authenticating...")
    ws.send(json.dumps({
        "action": "auth",
        "key": ALPACA_API_KEY,
        "secret": ALPACA_API_SECRET,
    }))

def _on_message(ws, message):
    global _ws_connected
    try:
        data = json.loads(message)
        for msg in data:
            msg_type = msg.get("T")

            if msg_type == "success" and msg.get("msg") == "authenticated":
                log.info("✅ Alpaca WebSocket authenticated — subscribing to bars...")
                ws.send(json.dumps({
                    "action": "subscribe",
                    "bars": TICKERS,
                }))
                _ws_connected = True

            elif msg_type == "subscription":
                log.info(f"✅ Alpaca WebSocket subscribed: {msg}")

            elif msg_type == "b":  # 1-minute bar
                ticker = msg.get("S")
                if ticker in TICKERS:
                    bar = {
                        "t": msg.get("t"),
                        "o": msg.get("o"),
                        "h": msg.get("h"),
                        "l": msg.get("l"),
                        "c": msg.get("c"),
                        "v": msg.get("v"),
                    }
                    with _store_lock:
                        if ticker not in _candle_store:
                            _candle_store[ticker] = deque(maxlen=MAX_BARS)
                        _candle_store[ticker].append(bar)
                    log.debug(f"📊 New bar: {ticker} c={bar['c']} v={bar['v']}")

            elif msg_type == "error":
                log.error(f"Alpaca WebSocket error msg: {msg}")

    except Exception as e:
        log.error(f"WebSocket message error: {e}", exc_info=True)

def _on_error(ws, error):
    log.error(f"Alpaca WebSocket error: {error}")

def _on_close(ws, close_status_code, close_msg):
    global _ws_connected
    _ws_connected = False
    log.warning(f"Alpaca WebSocket closed: {close_status_code} {close_msg}")

def _run_websocket():
    """Run WebSocket with auto-reconnect."""
    global _ws_app
    backoff = 5
    while True:
        try:
            log.info("🔌 Connecting to Alpaca WebSocket...")
            _ws_app = websocket.WebSocketApp(
                ALPACA_WS_URL,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )
            _ws_app.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            log.error(f"WebSocket run error: {e}")
        log.info(f"🔄 Reconnecting Alpaca WebSocket in {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)  # exponential backoff, max 60s


# ── Public API ─────────────────────────────────────────────────────────────────
def get_candles(ticker: str, limit: int = 30) -> list:
    """Get the most recent N candles for a ticker from the live store."""
    with _store_lock:
        store = _candle_store.get(ticker)
        if not store:
            return []
        bars = list(store)
        return bars[-limit:] if len(bars) >= limit else bars


def get_latest_price(ticker: str) -> float | None:
    """Get the most recent close price for a ticker."""
    candles = get_candles(ticker, limit=1)
    return candles[-1]["c"] if candles else None


def get_ohlcv_for_zones(ticker: str) -> dict | None:
    """
    Return OHLCV data in the format volume_profile.py expects
    for zone calculation — using all stored bars.
    """
    with _store_lock:
        store = _candle_store.get(ticker)
        if not store or len(store) < 10:
            return None
        bars = list(store)

    return {
        "high":   [b["h"] for b in bars],
        "low":    [b["l"] for b in bars],
        "close":  [b["c"] for b in bars],
        "volume": [b["v"] for b in bars],
    }


def get_premarket_data(ticker: str) -> dict:
    """Get pre-market price, gap, and volume ratio for watchlist generation."""
    try:
        candles = get_candles(ticker, limit=100)
        if len(candles) < 2:
            return {}
        closes  = [b["c"] for b in candles]
        volumes = [b["v"] for b in candles]
        current    = closes[-1]
        prev_close = closes[-2]
        gap_pct    = round((current - prev_close) / prev_close * 100, 2)
        avg_vol    = sum(volumes[-20:]) / max(len(volumes[-20:]), 1)
        vol_ratio  = round(volumes[-1] / avg_vol, 1) if avg_vol else 0
        return {
            "ticker": ticker,
            "price": round(current, 2),
            "prev_close": round(prev_close, 2),
            "gap_pct": gap_pct,
            "volume_ratio": vol_ratio,
        }
    except Exception as e:
        log.error(f"Pre-market data error for {ticker}: {e}")
        return {}


def is_connected() -> bool:
    return _ws_connected


def start():
    """Start the Alpaca data stream — backfill history then open WebSocket."""
    log.info("🚀 Starting Alpaca real-time data stream...")
    # Backfill historical data first (blocking, so zones are ready immediately)
    backfill_all()
    # Then start WebSocket in background thread
    ws_thread = threading.Thread(target=_run_websocket, daemon=True)
    ws_thread.start()
    log.info("✅ Alpaca stream started — live bars incoming")
