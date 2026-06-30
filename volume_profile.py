"""
The Portfolio Plug — Automated Volume Profile Zone Calculator
Runs daily without any manual input. Pulls free historical OHLCV data from
Yahoo Finance's public chart API, builds a volume-by-price histogram for
1H and 4H timeframes, and identifies the highest-volume price nodes —
these become the demand/supply zones Claude checks against for Rules 1, 2, 5, 6.

No API key required. No manual screenshot. No human in the loop.
"""

import logging
import requests
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# In-memory zone storage — refreshed daily, read by main.py on every webhook
_zone_store = {}  # {ticker: {"1H": {"demand": [low, high], "supply": [low, high]}, "4H": {...}, "updated_at": iso_string}}


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

        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])

        # Filter out None bars (Yahoo sometimes returns nulls for illiquid periods)
        clean = [
            (h, l, c, v) for h, l, c, v in zip(highs, lows, closes, volumes)
            if h is not None and l is not None and c is not None and v is not None
        ]

        if not clean:
            return None

        return {
            "high": [x[0] for x in clean],
            "low": [x[1] for x in clean],
            "close": [x[2] for x in clean],
            "volume": [x[3] for x in clean],
        }

    except Exception as e:
        log.error(f"OHLCV fetch error for {ticker}: {e}")
        return None


def calculate_volume_nodes(ohlcv: dict, num_bins: int = 40) -> dict | None:
    """
    Build a volume-by-price histogram (the core math behind Volume Profile)
    and identify the highest-volume price node (Point of Control) plus
    the demand zone (high volume below current price) and supply zone
    (high volume above current price).
    """
    try:
        highs = np.array(ohlcv["high"])
        lows = np.array(ohlcv["low"])
        volumes = np.array(ohlcv["volume"])
        current_price = ohlcv["close"][-1]

        price_min = lows.min()
        price_max = highs.max()

        if price_max <= price_min:
            return None

        bins = np.linspace(price_min, price_max, num_bins + 1)
        bin_volume = np.zeros(num_bins)

        # Distribute each bar's volume across the price bins it spans
        for h, l, v in zip(highs, lows, volumes):
            bar_bins = np.where((bins[:-1] < h) & (bins[1:] > l))[0]
            if len(bar_bins) > 0:
                vol_per_bin = v / len(bar_bins)
                for b in bar_bins:
                    bin_volume[b] += vol_per_bin

        bin_centers = (bins[:-1] + bins[1:]) / 2

        # Point of Control — the single highest-volume price node
        poc_idx = np.argmax(bin_volume)
        poc_price = bin_centers[poc_idx]

        # Demand zone: highest-volume node BELOW current price
        below_mask = bin_centers < current_price
        if below_mask.any():
            below_volumes = np.where(below_mask, bin_volume, -1)
            demand_idx = np.argmax(below_volumes)
            demand_center = bin_centers[demand_idx]
            bin_width = (price_max - price_min) / num_bins
            demand_zone = [round(demand_center - bin_width, 2), round(demand_center + bin_width, 2)]
        else:
            demand_zone = None

        # Supply zone: highest-volume node ABOVE current price
        above_mask = bin_centers > current_price
        if above_mask.any():
            above_volumes = np.where(above_mask, bin_volume, -1)
            supply_idx = np.argmax(above_volumes)
            supply_center = bin_centers[supply_idx]
            bin_width = (price_max - price_min) / num_bins
            supply_zone = [round(supply_center - bin_width, 2), round(supply_center + bin_width, 2)]
        else:
            supply_zone = None

        return {
            "poc": round(float(poc_price), 2),
            "demand": demand_zone,
            "supply": supply_zone,
            "current_price": round(float(current_price), 2),
        }

    except Exception as e:
        log.error(f"Volume node calculation error: {e}")
        return None


def update_zones_for_ticker(ticker: str) -> bool:
    """Calculate and store both 1H and 4H volume profile zones for a ticker."""
    success = False
    zones = {"updated_at": datetime.now(ET).isoformat()}

    # 1-Hour zones — use 60m interval, last 1 month of data
    ohlcv_1h = fetch_ohlcv(ticker, interval="60m", range_="1mo")
    if ohlcv_1h:
        nodes_1h = calculate_volume_nodes(ohlcv_1h)
        if nodes_1h:
            zones["1H"] = nodes_1h
            success = True

    # 4-Hour zones — Yahoo doesn't offer native 4H, so fetch 1H and resample
    ohlcv_4h_raw = fetch_ohlcv(ticker, interval="60m", range_="3mo")
    if ohlcv_4h_raw and len(ohlcv_4h_raw["close"]) >= 4:
        # Resample 1H bars into 4H bars by grouping every 4 bars
        resampled = {"high": [], "low": [], "close": [], "volume": []}
        h, l, c, v = ohlcv_4h_raw["high"], ohlcv_4h_raw["low"], ohlcv_4h_raw["close"], ohlcv_4h_raw["volume"]
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
        _zone_store[ticker] = zones
        log.info(f"✅ Volume Profile zones updated for {ticker}: {zones}")
    else:
        log.warning(f"⚠️ Could not calculate Volume Profile zones for {ticker}")

    return success


def update_all_zones(tickers: list) -> dict:
    """Run the daily zone calculation for every ticker in the watchlist."""
    results = {}
    for ticker in tickers:
        results[ticker] = update_zones_for_ticker(ticker)
    return results


def get_zones(ticker: str) -> dict | None:
    """Retrieve the most recently calculated zones for a ticker."""
    return _zone_store.get(ticker)


def get_all_zones() -> dict:
    """Return the full zone store — used for the /zones debug endpoint."""
    return _zone_store


def is_price_in_zone(price: float, zone: list | None, tolerance_pct: float = 0.3) -> bool:
    """Check if a price falls within (or near, within tolerance) a zone range."""
    if not zone:
        return False
    low, high = zone
    margin = (high - low) * tolerance_pct
    return (low - margin) <= price <= (high + margin)
