"""
The Portfolio Plug — Volume Profile Zone Calculator
Now powered by Alpaca real-time data instead of Yahoo Finance.
Reads from the live alpaca_stream candle store — zero delay, fully real-time.
"""

import json
import logging
import os
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")
ZONE_FILE = "/tmp/tpp_zones.json"


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

        price_min = lows.min()
        price_max = highs.max()
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

        poc_idx   = int(np.argmax(bin_volume))
        poc_price = float(bin_centers[poc_idx])

        below_mask  = bin_centers < current_price
        demand_zone = None
        if below_mask.any():
            di = int(np.argmax(np.where(below_mask, bin_volume, -1)))
            dc = float(bin_centers[di])
            demand_zone = [round(dc - bin_width, 2), round(dc + bin_width, 2)]

        above_mask  = bin_centers > current_price
        supply_zone = None
        if above_mask.any():
            si = int(np.argmax(np.where(above_mask, bin_volume, -1)))
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


def update_all_zones(tickers: list) -> dict:
    """Calculate zones from Alpaca real-time candle store and persist to disk."""
    import alpaca_stream
    all_zones = _load_zones()
    results   = {}

    for ticker in tickers:
        try:
            ohlcv = alpaca_stream.get_ohlcv_for_zones(ticker)
            if not ohlcv or len(ohlcv["close"]) < 10:
                log.warning(f"⚠️ Not enough Alpaca data for {ticker} zones yet")
                results[ticker] = False
                continue

            nodes = calculate_volume_nodes(ohlcv)
            if nodes:
                all_zones[ticker] = {
                    "1H": nodes,
                    "updated_at": datetime.now(ET).isoformat(),
                }
                log.info(f"✅ Volume Profile zones updated for {ticker}: {nodes}")
                results[ticker] = True
            else:
                results[ticker] = False

        except Exception as e:
            log.error(f"Zone update error for {ticker}: {e}")
            results[ticker] = False

    _save_zones(all_zones)
    log.info(f"🔄 Zone file saved to {ZONE_FILE} with {len(all_zones)} tickers")
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
