"""
The Portfolio Plug — Volume Profile Zone Calculator
v2: fetches its own history via Alpaca REST (restart-proof) with the live
stream store as fallback. No longer depends on websocket backfill.
"""

import json
import logging
import os
import requests
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")
ZONE_FILE = "/tmp/tpp_zones.json"

ALPACA_KEY    = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET = (os.environ.get("ALPACA_API_SECRET")
                 or os.environ.get("ALPACA_SECRET_KEY", ""))

def _fetch_hourly_bars(ticker: str, limit: int = 500) -> dict | None:
    """~1 month of 1-hour bars straight from Alpaca REST — instant, no backfill needed."""
    try:
        resp = requests.get(
            f"https://data.alpaca.markets/v2/stocks/{ticker}/bars",
            headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
            params={"timeframe": "1Hour", "limit": limit, "adjustment": "raw", "feed": "sip"},
            timeout=15,
        )
        if resp.status_code == 403:   # account without SIP entitlement → retry IEX
            resp = requests.get(
                f"https://data.alpaca.markets/v2/stocks/{ticker}/bars",
                headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
                params={"timeframe": "1Hour", "limit": limit, "adjustment": "raw", "feed": "iex"},
                timeout=15,
            )
        if resp.status_code != 200:
            log.error(f"Alpaca REST bars failed for {ticker}: {resp.status_code} {resp.text[:120]}")
            return None
        bars = resp.json().get("bars", [])
        if len(bars) < 10:
            return None
        return {
            "high":   [b["h"] for b in bars],
            "low":    [b["l"] for b in bars],
            "close":  [b["c"] for b in bars],
            "volume": [b["v"] for b in bars],
        }
    except Exception as e:
        log.error(f"REST bars error for {ticker}: {e}")
        return None

def _save_zones(zones: dict):
    try:
        with open(ZONE_FILE, "w") as f:
            json.dump(zones, f)
    except Exception as e:
        log.error(f"Failed to save zones: {e}")

def _load_zones() -> dict:
    try:
        if os.path.exists(ZONE_FILE):
            with open(ZONE_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        log.error(f"Failed to load zones: {e}")
    return {}

def calculate_volume_nodes(ohlcv: dict, num_bins: int = 40) -> dict | None:
    try:
        highs   = np.array(ohlcv["high"])
        lows    = np.array(ohlcv["low"])
        volumes = np.array(ohlcv["volume"])
        current_price = ohlcv["close"][-1]
        price_min, price_max = lows.min(), highs.max()
        if price_max <= price_min:
            return None
        bins       = np.linspace(price_min, price_max, num_bins + 1)
        bin_volume = np.zeros(num_bins)
        bin_width  = (price_max - price_min) / num_bins
        for h, l, v in zip(highs, lows, volumes):
            bar_bins = np.where((bins[:-1] < h) & (bins[1:] > l))[0]
            if len(bar_bins) > 0:
                vol_per_bin = v / len(bar_bins)
                for b in bar_bins:
                    bin_volume[b] += vol_per_bin
        bin_centers = (bins[:-1] + bins[1:]) / 2
        poc_price   = float(bin_centers[int(np.argmax(bin_volume))])

        below_mask, demand_zone = bin_centers < current_price, None
        if below_mask.any():
            dc = float(bin_centers[int(np.argmax(np.where(below_mask, bin_volume, -1)))])
            demand_zone = [round(dc - bin_width, 2), round(dc + bin_width, 2)]
        above_mask, supply_zone = bin_centers > current_price, None
        if above_mask.any():
            sc = float(bin_centers[int(np.argmax(np.where(above_mask, bin_volume, -1)))])
            supply_zone = [round(sc - bin_width, 2), round(sc + bin_width, 2)]
        return {
            "poc": round(poc_price, 2),
            "demand": demand_zone,
            "supply": supply_zone,
            "current_price": round(float(current_price), 2),
        }
    except Exception as e:
        log.error(f"Volume node calculation error: {e}")
        return None

def update_all_zones(tickers: list) -> dict:
    """Zones from Alpaca REST history (primary) or live stream store (fallback)."""
    import alpaca_stream
    all_zones, results = _load_zones(), {}
    for ticker in tickers:
        try:
            ohlcv = _fetch_hourly_bars(ticker)                      # primary: REST
            source = "REST"
            if not ohlcv or len(ohlcv["close"]) < 10:               # fallback: stream store
                ohlcv, source = alpaca_stream.get_ohlcv_for_zones(ticker), "stream"
            if not ohlcv or len(ohlcv["close"]) < 10:
                log.warning(f"⚠️ Not enough Alpaca data for {ticker} zones (REST + stream both thin)")
                results[ticker] = False
                continue
            nodes = calculate_volume_nodes(ohlcv)
            if nodes:
                all_zones[ticker] = {"1H": nodes, "updated_at": datetime.now(ET).isoformat()}
                log.info(f"✅ Zones updated for {ticker} via {source}: {nodes}")
                results[ticker] = True
            else:
                results[ticker] = False
        except Exception as e:
            log.error(f"Zone update error for {ticker}: {e}")
            results[ticker] = False
    _save_zones(all_zones)
    log.info(f"🔄 Zone file saved with {sum(results.values())}/{len(tickers)} tickers")
    return results

def get_zones(ticker: str) -> dict | None:
    return _load_zones().get(ticker)

def get_all_zones() -> dict:
    return _load_zones()

def is_price_in_zone(price: float, zone: list | None, tolerance_pct: float = 0.3) -> bool:
    if not zone:
        return False
    low, high = zone
    margin = (high - low) * tolerance_pct
    return (low - margin) <= price <= (high + margin)
