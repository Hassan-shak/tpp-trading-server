"""
The Portfolio Plug — Tastytrade Execution Layer
Handles authentication, option chain lookup, order placement, and position monitoring.
Paper trading mode by default — set TASTYTRADE_PAPER_TRADING=false to go live.
"""

import os
import logging
import requests
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal

log = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

# ── Config ────────────────────────────────────────────────────────────────────
PAPER_TRADING     = os.environ.get("TASTYTRADE_PAPER_TRADING", "true").lower() == "true"
CLIENT_ID         = os.environ.get("TASTYTRADE_CLIENT_ID", "")
CLIENT_SECRET     = os.environ.get("TASTYTRADE_CLIENT_SECRET", "")
ACCOUNT_NUMBER    = os.environ.get("TASTYTRADE_ACCOUNT_NUMBER", "")
REDIRECT_URI      = os.environ.get("TASTYTRADE_REDIRECT_URI", "https://tpp-trading-server.onrender.com/oauth/callback")

# API base URLs
PAPER_BASE  = "https://api.cert.tastyworks.com"   # sandbox/paper trading
LIVE_BASE   = "https://api.tastytrade.com"          # live trading
BASE_URL    = PAPER_BASE if PAPER_TRADING else LIVE_BASE

# Token storage (in-memory — resets on server restart)
_token_store = {
    "access_token": None,
    "refresh_token": None,
    "expires_at": None,
}

# ── Authentication ─────────────────────────────────────────────────────────────

def get_headers() -> dict:
    """Get auth headers, refreshing token if needed."""
    token = _token_store.get("access_token")
    if not token:
        raise RuntimeError("Tastytrade not authenticated. Visit /oauth/start to authenticate.")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def save_tokens(access_token: str, refresh_token: str, expires_in: int):
    """Store tokens in memory."""
    _token_store["access_token"] = access_token
    _token_store["refresh_token"] = refresh_token
    _token_store["expires_at"] = datetime.now(ET).timestamp() + expires_in
    log.info(f"Tastytrade tokens saved. Mode: {'PAPER' if PAPER_TRADING else 'LIVE'}")

def refresh_access_token() -> bool:
    """Refresh the access token using the refresh token."""
    refresh_token = _token_store.get("refresh_token")
    if not refresh_token:
        return False
    try:
        resp = requests.post(
            f"{BASE_URL}/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            }
        )
        if resp.status_code == 200:
            data = resp.json()
            save_tokens(data["access_token"], data.get("refresh_token", refresh_token), data.get("expires_in", 3600))
            return True
    except Exception as e:
        log.error(f"Token refresh failed: {e}")
    return False

def is_authenticated() -> bool:
    """Check if we have a valid token."""
    if not _token_store.get("access_token"):
        return False
    expires_at = _token_store.get("expires_at", 0)
    # Refresh if expiring in next 5 minutes
    if datetime.now(ET).timestamp() > expires_at - 300:
        return refresh_access_token()
    return True

# ── Option Chain Lookup ────────────────────────────────────────────────────────

def get_expiry_date(trade_type: str = "DAY") -> str:
    """
    Calculate the correct expiry date based on Junior's playbook rules.
    DAY trades: current week Friday (or next week on Fri/Thu-after-11)
    SWING trades: ~30-90 days out
    """
    now = datetime.now(ET)
    weekday = now.weekday()  # 0=Mon, 4=Fri

    if trade_type == "DAY":
        # Find this week's Friday
        days_to_friday = (4 - weekday) % 7
        if days_to_friday == 0:
            # Today is Friday — use next week's Friday (no 0DTE)
            days_to_friday = 7
        elif weekday == 3:
            # Thursday — use next week if after 11 AM
            if now.hour >= 11:
                days_to_friday += 7
        target = now + timedelta(days=days_to_friday)
    else:
        # Swing: ~45 days out
        target = now + timedelta(days=45)

    return target.strftime("%Y-%m-%d")

def find_option_contract(ticker: str, direction: str, trade_type: str = "DAY") -> dict | None:
    """
    Find the best option contract matching Junior's playbook rules:
    - Day trades: $0.75-$1.50 premium, slightly OTM, tight spread
    - Returns contract details or None if no suitable contract found
    """
    if not is_authenticated():
        log.error("Cannot find contract — not authenticated")
        return None

    expiry = get_expiry_date(trade_type)
    option_type = "C" if direction == "CALL" else "P"

    try:
        # Get option chain
        url = f"{BASE_URL}/option-chains/{ticker}/nested"
        resp = requests.get(url, headers=get_headers())

        if resp.status_code != 200:
            log.error(f"Option chain fetch failed: {resp.status_code}")
            return None

        chain_data = resp.json().get("data", {}).get("items", [])
        if not chain_data:
            return None

        # Find the correct expiration
        target_expiry = None
        for item in chain_data:
            for exp in item.get("expirations", []):
                if exp.get("expiration-date") == expiry:
                    target_expiry = exp
                    break

        if not target_expiry:
            log.warning(f"No expiry found for {ticker} on {expiry}")
            return None

        # Get current price to find OTM strikes
        price_url = f"{BASE_URL}/market-data/quotes"
        price_resp = requests.get(price_url, headers=get_headers(), params={"symbols[]": ticker})
        current_price = None
        if price_resp.status_code == 200:
            quotes = price_resp.json().get("data", {}).get("items", [])
            if quotes:
                current_price = float(quotes[0].get("last", 0))

        if not current_price:
            log.error(f"Could not get current price for {ticker}")
            return None

        # Find best contract in $0.75-$1.50 range
        best_contract = None
        for strike_data in target_expiry.get("strikes", []):
            strike_price = float(strike_data.get("strike-price", 0))
            contract_symbol = strike_data.get("call" if option_type == "C" else "put", "")

            if not contract_symbol:
                continue

            # For calls: want strike slightly above current price (OTM)
            # For puts: want strike slightly below current price (OTM)
            if option_type == "C" and strike_price <= current_price:
                continue
            if option_type == "P" and strike_price >= current_price:
                continue

            # Get quote for this contract
            quote_resp = requests.get(
                f"{BASE_URL}/market-data/quotes",
                headers=get_headers(),
                params={"symbols[]": contract_symbol}
            )

            if quote_resp.status_code != 200:
                continue

            quotes = quote_resp.json().get("data", {}).get("items", [])
            if not quotes:
                continue

            bid = float(quotes[0].get("bid", 0))
            ask = float(quotes[0].get("ask", 0))
            mid = (bid + ask) / 2

            if bid <= 0 or ask <= 0:
                continue

            # Check spread percentage
            spread_pct = (ask - bid) / ask * 100 if ask > 0 else 100
            if spread_pct > 5:
                continue  # Spread too wide

            # Check premium in range $0.75-$1.50 (day trade)
            if trade_type == "DAY":
                if 0.75 <= mid <= 1.50:
                    best_contract = {
                        "symbol": contract_symbol,
                        "strike": strike_price,
                        "expiry": expiry,
                        "direction": direction,
                        "bid": bid,
                        "ask": ask,
                        "mid": mid,
                        "spread_pct": round(spread_pct, 2),
                        "ticker": ticker,
                    }
                    break  # First valid OTM contract in range
            else:
                # Swing: $1.00-$3.50
                if 1.00 <= mid <= 3.50:
                    best_contract = {
                        "symbol": contract_symbol,
                        "strike": strike_price,
                        "expiry": expiry,
                        "direction": direction,
                        "bid": bid,
                        "ask": ask,
                        "mid": mid,
                        "spread_pct": round(spread_pct, 2),
                        "ticker": ticker,
                    }
                    break

        if best_contract:
            log.info(f"Found contract: {best_contract['symbol']} @ ${best_contract['mid']:.2f}")
        else:
            log.warning(f"No suitable contract found for {ticker} {direction} on {expiry}")

        return best_contract

    except Exception as e:
        log.error(f"Error finding contract: {e}")
        return None

# ── Order Placement ────────────────────────────────────────────────────────────

def place_order(contract: dict, quantity: int = 1) -> dict | None:
    """
    Place a market order for the specified contract.
    Uses limit order at ask price to ensure fill.
    """
    if not is_authenticated():
        log.error("Cannot place order — not authenticated")
        return None

    if PAPER_TRADING:
        log.info(f"PAPER TRADE: Would buy {quantity}x {contract['symbol']} @ ${contract['ask']:.2f}")

    try:
        order_payload = {
            "time-in-force": "Day",
            "order-type": "Limit",
            "price": str(round(contract["ask"], 2)),
            "price-effect": "Debit",
            "legs": [
                {
                    "instrument-type": "Equity Option",
                    "symbol": contract["symbol"],
                    "quantity": quantity,
                    "action": "Buy to Open",
                }
            ]
        }

        url = f"{BASE_URL}/accounts/{ACCOUNT_NUMBER}/orders"
        if PAPER_TRADING:
            # Dry run first
            dry_resp = requests.post(
                f"{url}/dry-run",
                headers=get_headers(),
                json=order_payload
            )
            log.info(f"Dry run response: {dry_resp.status_code} — {dry_resp.text[:200]}")

            if dry_resp.status_code not in (200, 201):
                log.error(f"Dry run failed: {dry_resp.text}")
                return None

            # In paper trading mode — place the actual paper order
            resp = requests.post(url, headers=get_headers(), json=order_payload)
        else:
            # Live trading
            resp = requests.post(url, headers=get_headers(), json=order_payload)

        if resp.status_code in (200, 201):
            order_data = resp.json().get("data", {}).get("order", {})
            order_id = order_data.get("id", "unknown")
            log.info(f"Order placed: {order_id} — {contract['symbol']} x{quantity}")
            return {
                "order_id": order_id,
                "symbol": contract["symbol"],
                "quantity": quantity,
                "price": contract["ask"],
                "status": order_data.get("status", "submitted"),
                "paper": PAPER_TRADING,
            }
        else:
            log.error(f"Order failed: {resp.status_code} — {resp.text}")
            return None

    except Exception as e:
        log.error(f"Order placement error: {e}")
        return None

def close_position(symbol: str, quantity: int) -> dict | None:
    """
    Close an open position with a market sell.
    """
    if not is_authenticated():
        return None

    try:
        # Get current bid to sell at market
        quote_resp = requests.get(
            f"{BASE_URL}/market-data/quotes",
            headers=get_headers(),
            params={"symbols[]": symbol}
        )
        bid = 0
        if quote_resp.status_code == 200:
            quotes = quote_resp.json().get("data", {}).get("items", [])
            if quotes:
                bid = float(quotes[0].get("bid", 0))

        order_payload = {
            "time-in-force": "Day",
            "order-type": "Limit",
            "price": str(round(bid, 2)) if bid > 0 else "0.01",
            "price-effect": "Credit",
            "legs": [
                {
                    "instrument-type": "Equity Option",
                    "symbol": symbol,
                    "quantity": quantity,
                    "action": "Sell to Close",
                }
            ]
        }

        url = f"{BASE_URL}/accounts/{ACCOUNT_NUMBER}/orders"
        resp = requests.post(url, headers=get_headers(), json=order_payload)

        if resp.status_code in (200, 201):
            order_data = resp.json().get("data", {}).get("order", {})
            log.info(f"Position closed: {symbol} x{quantity}")
            return {"status": "closed", "symbol": symbol, "order_id": order_data.get("id")}
        else:
            log.error(f"Close order failed: {resp.status_code} — {resp.text}")
            return None

    except Exception as e:
        log.error(f"Close position error: {e}")
        return None

def get_positions() -> list:
    """Get all open positions."""
    if not is_authenticated():
        return []
    try:
        url = f"{BASE_URL}/accounts/{ACCOUNT_NUMBER}/positions"
        resp = requests.get(url, headers=get_headers())
        if resp.status_code == 200:
            return resp.json().get("data", {}).get("items", [])
        return []
    except Exception as e:
        log.error(f"Get positions error: {e}")
        return []

def get_account_balance() -> dict:
    """Get account balance and buying power."""
    if not is_authenticated():
        return {}
    try:
        url = f"{BASE_URL}/accounts/{ACCOUNT_NUMBER}/balances"
        resp = requests.get(url, headers=get_headers())
        if resp.status_code == 200:
            return resp.json().get("data", {})
        return {}
    except Exception as e:
        log.error(f"Get balance error: {e}")
        return {}
