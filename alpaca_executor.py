"""
The Portfolio Plug — Alpaca Paper Trading Executor
Executes paper trades via Alpaca's REST API using the same API keys
already configured for market data. Falls back to this when Tastytrade
paper trading account has no funds.
"""

import os
import logging
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

ALPACA_API_KEY    = os.environ.get("ALPACA_API_KEY", "")
ALPACA_API_SECRET = os.environ.get("ALPACA_API_SECRET", "")
ALPACA_PAPER_URL  = "https://paper-api.alpaca.markets/v2"

def _headers():
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET,
        "Content-Type": "application/json",
    }

def get_account() -> dict:
    """Get Alpaca paper account balance and buying power."""
    try:
        resp = requests.get(f"{ALPACA_PAPER_URL}/account", headers=_headers(), timeout=10)
        if resp.status_code == 200:
            return resp.json()
        log.error(f"Alpaca account fetch failed: {resp.status_code} {resp.text}")
        return {}
    except Exception as e:
        log.error(f"Alpaca account error: {e}")
        return {}

def find_option_contract(ticker: str, direction: str, current_price: float) -> dict | None:
    """
    Find the best options contract matching playbook rules:
    - Weekly expiration
    - OTM but liquid
    - Price $0.75-$1.50
    - Spread < 5%
    """
    try:
        # Get next Friday expiration
        today = datetime.now(ET)
        days_until_friday = (4 - today.weekday()) % 7
        if days_until_friday == 0:
            days_until_friday = 7
        expiry = (today + timedelta(days=days_until_friday)).strftime("%Y-%m-%d")

        option_type = "call" if direction == "CALL" else "put"

        # Get options chain
        resp = requests.get(
            f"{ALPACA_PAPER_URL}/options/contracts",
            headers=_headers(),
            params={
                "underlying_symbols": ticker,
                "expiration_date": expiry,
                "type": option_type,
                "limit": 50,
            },
            timeout=10
        )

        if resp.status_code != 200:
            log.error(f"Alpaca options chain failed: {resp.status_code} {resp.text}")
            return None

        contracts = resp.json().get("option_contracts", [])
        if not contracts:
            log.warning(f"No Alpaca options contracts found for {ticker} {expiry} {option_type}")
            return None

        # Filter for OTM contracts in price range
        best = None
        for contract in contracts:
            strike = float(contract.get("strike_price", 0))
            # OTM check
            if option_type == "call" and strike <= current_price:
                continue
            if option_type == "put" and strike >= current_price:
                continue

            # Check bid/ask via snapshot
            symbol = contract.get("symbol")
            snap_resp = requests.get(
                f"https://data.alpaca.markets/v1beta1/options/snapshots/{symbol}",
                headers=_headers(),
                timeout=5
            )
            if snap_resp.status_code != 200:
                continue

            snap = snap_resp.json().get("snapshots", {}).get(symbol, {})
            greeks = snap.get("greeks", {})
            quote = snap.get("latestQuote", {})

            bid = float(quote.get("bp", 0))
            ask = float(quote.get("ap", 0))
            mid = (bid + ask) / 2

            if mid < 0.75 or mid > 1.50:
                continue

            spread_pct = (ask - bid) / ask * 100 if ask > 0 else 100
            if spread_pct > 5:
                continue

            if best is None or abs(mid - 1.10) < abs(float(best.get("mid", 0)) - 1.10):
                best = {
                    "symbol": symbol,
                    "strike": strike,
                    "expiry": expiry,
                    "type": option_type,
                    "bid": bid,
                    "ask": ask,
                    "mid": round(mid, 2),
                    "spread_pct": round(spread_pct, 2),
                }

        if best:
            log.info(f"✅ Alpaca contract found: {best}")
        else:
            log.warning(f"No suitable Alpaca contract found for {ticker}")

        return best

    except Exception as e:
        log.error(f"Alpaca find_option_contract error: {e}", exc_info=True)
        return None

def place_order(contract: dict, quantity: int = 1) -> dict | None:
    """Place a paper options order on Alpaca."""
    try:
        order_data = {
            "symbol": contract["symbol"],
            "qty": quantity,
            "side": "buy",
            "type": "limit",
            "limit_price": str(contract["ask"]),
            "time_in_force": "day",
        }

        resp = requests.post(
            f"{ALPACA_PAPER_URL}/orders",
            headers=_headers(),
            json=order_data,
            timeout=10
        )

        if resp.status_code in (200, 201):
            order = resp.json()
            log.info(f"✅ Alpaca paper order placed: {order.get('id')} {contract['symbol']}")
            return order
        else:
            log.error(f"Alpaca order failed: {resp.status_code} {resp.text}")
            return None

    except Exception as e:
        log.error(f"Alpaca place_order error: {e}", exc_info=True)
        return None

def get_positions() -> list:
    """Get current open paper positions from Alpaca."""
    try:
        resp = requests.get(f"{ALPACA_PAPER_URL}/positions", headers=_headers(), timeout=10)
        if resp.status_code == 200:
            return resp.json()
        return []
    except Exception as e:
        log.error(f"Alpaca get_positions error: {e}")
        return []

def is_available() -> bool:
    """Check if Alpaca paper trading is accessible."""
    try:
        account = get_account()
        buying_power = float(account.get("buying_power", 0))
        return buying_power > 0
    except Exception:
        return False
