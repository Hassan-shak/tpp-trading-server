"""
The Portfolio Plug — Automated Volume Profile Zone Calculator
Runs daily without any manual input. Pulls free historical OHLCV data from
Yahoo Finance's public chart API, builds a volume-by-price histogram for
1H and 4H timeframes, and identifies the highest-volume price nodes —
these become the demand/supply zones Claude checks against for Rules 1, 2, 5, 6.

No API key required. No manual screenshot. No human in the loop.

Zones are persisted to /tmp/tpp_zones.json so all processes can read them.
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

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
ZONE_FILE = "/tmp/tpp_zones.json"


def _save_zones(zones: dict):
    """Persist zone data to disk so all processes share it."""
    try:
        with open(ZONE_FILE, "w") as f:
            json.dump(zones, f)
    except Exception as e:
        log.error(f"Failed to save zones to disk: {e}")


def _load_zones() -> dict:
    """Load zone data from disk."""
    try:
        if os.path.exists(ZONE_FILE):
            with open(ZONE_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        log.error(f"Failed to load zones from disk: {e}")
    return {}


def fetch_ohlcv(ticker: str, interval: str = "60m", range_: str = "1mo") -> dict | None:
    """Fetch OHLCV bars from Yahoo Finance's free chart endpoint."""
    try:
        url = YAHOO_CHART_URL.format(symbol=ticker)
        resp = requests.get(
            url,
            params={"range": range_, "interval": interval},
            headers=HEADERS,
            timeout=10
        )
        if resp.status_code != 200:
            log.error(f"Yahoo fetch failed for {ticker} ({interval}): {resp.status_code}")
            return None

        data = resp.json()
        result = data.get("chart", {}).get("result")
        if not result:
            log.error(f"No chart data for {ticker}")
            return None

        result = result[0]
        quote = result["indicators"]["quote"][0]
        highs   = quote.get("high", [])
        lows    = quote.get("low", [])
        closes  = quote.get("close", [])
        volumes = quote.get("volume", [])

        clean = [
            (h, l, c, v) for h, l, c, v in zip(highs, lows, closes, volumes)
            if h is not None and l is not None and c is not None and v is not None
        ]
        if not clean:
            return None

        return {
            "high":   [x[0] for x in clean],
            "low":    [x[1] for x in clean],
            "close":  [x[2] for x in clean],
            "volume": [x[3] for x in clean],
        }

    except Exception as e:
        log.error(f"OHLCV fetch error for {ticker}: {e}")
        return None


def calculate_volume_nodes(ohlcv: dict, num_bins: int = 40) -> dict | None:
    """Build a volume-by-price histogram and return POC, demand, and supply zones."""
    try:
        highs   = np.array(ohlcv["high"])
        lows    = np.array(ohlcv["low"])
        volumes = np.array(ohlcv["volume"])
        current_price = ohlcv["close"][-1]

        price_min = lows.min()
        price_max = highs.max()
        if price_max <= price_min:
            return None

        bins       = np.linspace(price_min, price_max, num_bins + 1)
        bin_volume = np.zeros(num_bins)

        for h, l, v in zip(highs, lows, volumes):
            bar_bins = np.where((bins[:-1] < h) & (bins[1:] > l))[0]
            if len(bar_bins) > 0:
                vol_per_bin = v / len(bar_bins)
                for b in bar_bins:
                    bin_volume[b] += vol_per_bin

        bin_centers = (bins[:-1] + bins[1:]) / 2
        bin_width   = (price_max - price_min) / num_bins

        poc_idx   = int(np.argmax(bin_volume))
        poc_price = float(bin_centers[poc_idx])

        below_mask = bin_centers < current_price
        demand_zone = None
        if below_mask.any():
            below_vols = np.where(below_mask, bin_volume, -1)
            di = int(np.argmax(below_vols))
            dc = float(bin_centers[di])
            demand_zone = [round(dc - bin_width, 2), round(dc + bin_width, 2)]

        above_mask = bin_centers > current_price
        supply_zone = None
        if above_mask.any():
            above_vols = np.where(above_mask, bin_volume, -1)
            si = int(np.argmax(above_vols))
            sc = float(bin_centers[si])
            supply_zone = [round(sc - bin_width, 2), round(sc + bin_width, 2)]

        return {
            "poc":           round(poc_price, 2),
            "demand":        demand_zone,
            "supply":        supply_zone,
            "current_price": round(float(current_price), 2),
        }

    except Exception as e:
        log.error(f"Volume node calculation error: {e}")
        return None


def update_zones_for_ticker(ticker: str, all_zones: dict) -> bool:
    """Calculate and store 1H and 4H volume profile zones for a ticker into all_zones dict."""
    success = False
    zones   = {"updated_at": datetime.now(ET).isoformat()}

    # 1-Hour zones
    ohlcv_1h = fetch_ohlcv(ticker, interval="60m", range_="1mo")
    if ohlcv_1h:
        nodes_1h = calculate_volume_nodes(ohlcv_1h)
        if nodes_1h:
            zones["1H"] = nodes_1h
            success = True

    # 4-Hour zones (resample 1H into 4H)
    ohlcv_raw = fetch_ohlcv(ticker, interval="60m", range_="3mo")
    if ohlcv_raw and len(ohlcv_raw["close"]) >= 4:
        resampled = {"high": [], "low": [], "close": [], "volume": []}
        h, l, c, v = (ohlcv_raw["high"], ohlcv_raw["low"],
                      ohlcv_raw["close"], ohlcv_raw["volume"])
        for i in range(0, len(h) - 3, 4):
            resampled["high"].append(max(h[i:i+4]))
            resampled["low"].append(min(l[i:i+4]))
            resampled["close"].append(c[i+3])
            resampled["volume"].append(sum(v[i:i+4]))
        if resampled["close"]:
            nodes_4h = calculate_volume_nodes(resampled)
            if nodes_4h:
                zones["4H"] = nodes_4h
                success = True

    if success:
        all_zones[ticker] = zones
        log.info(f"✅ Volume Profile zones updated for {ticker}: {zones}")
    else:
        log.warning(f"⚠️ Could not calculate Volume Profile zones for {ticker}")

    return success


def update_all_zones(tickers: list) -> dict:
    """Run the daily zone calculation for all tickers and persist to disk."""
    all_zones = _load_zones()  # start from existing so partial failures don't wipe good data
    results = {}
    for ticker in tickers:
        results[ticker] = update_zones_for_ticker(ticker, all_zones)
    _save_zones(all_zones)
    log.info(f"🔄 Zone file saved to {ZONE_FILE} with {len(all_zones)} tickers")
    return results


def get_zones(ticker: str) -> dict | None:
    """Retrieve the most recently calculated zones for a ticker (reads from disk)."""
    return _load_zones().get(ticker)


def get_all_zones() -> dict:
    """Return the full zone store from disk — used for the /zones debug endpoint."""
    return _load_zones()


def is_price_in_zone(price: float, zone: list | None, tolerance_pct: float = 0.3) -> bool:
    """Check if a price falls within (or near) a zone range."""
    if not zone:
        return False
    low, high = zone
    margin = (high - low) * tolerance_pct
    return (low - margin) <= price <= (high + margin)
