"""
The Portfolio Plug — Tastytrade Execution Layer
Auto-authenticates on startup using username/password session auth.
Paper trading mode by default — set TASTYTRADE_PAPER_TRADING=false to go live.
"""

import os
import logging
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

# ── Config ────────────────────────────────────────────────────────────────────
PAPER_TRADING     = os.environ.get("TASTYTRADE_PAPER_TRADING", "true").lower() == "true"
ACCOUNT_NUMBER    = os.environ.get("TASTYTRADE_ACCOUNT_NUMBER", "")
TT_USERNAME       = os.environ.get("TASTYTRADE_USERNAME", "")
TT_PASSWORD       = os.environ.get("TASTYTRADE_PASSWORD", "")

# Tastytrade uses same base URL for both paper and live — paper is account-level
BASE_URL = "https://api.tastytrade.com"

# Token storage
_token_store = {
    "session_token": None,
    "expires_at": None,
}

# ── Authentication ─────────────────────────────────────────────────────────────

def authenticate() -> bool:
    """Login with username/password and get a session token."""
    if not TT_USERNAME or not TT_PASSWORD:
        log.warning("Tastytrade credentials not set — set TASTYTRADE_USERNAME and TASTYTRADE_PASSWORD")
        return False

    try:
        resp = requests.post(
            f"{BASE_URL}/sessions",
            json={
                "login": TT_USERNAME,
                "password": TT_PASSWORD,
                "remember-me": True,
            },
            headers={"Content-Type": "application/json"}
        )

        if resp.status_code == 201:
            data = resp.json().get("data", {})
            token = data.get("session-token")
            if token:
                _token_store["session_token"] = token
                # Sessions last 24 hours — refresh after 23
                _token_store["expires_at"] = datetime.now(ET).timestamp() + (23 * 3600)
                mode = "PAPER" if PAPER_TRADING else "LIVE"
                log.info(f"✅ Tastytrade authenticated — {mode} mode | Account: {ACCOUNT_NUMBER}")
                return True
            else:
                log.error("No session token in response")
                return False
        else:
            log.error(f"Tastytrade auth failed: {resp.status_code} — {resp.text[:200]}")
            return False

    except Exception as e:
        log.error(f"Tastytrade auth error: {e}")
        return False

def get_headers() -> dict:
    """Get auth headers, re-authenticating if token expired."""
    # Check if token needs refresh
    expires_at = _token_store.get("expires_at", 0)
    if not _token_store.get("session_token") or datetime.now(ET).timestamp() > expires_at:
        log.info("Session token expired or missing — re-authenticating...")
        authenticate()

    token = _token_store.get("session_token")
    if not token:
        raise RuntimeError("Tastytrade not authenticated")

    return {
        "Authorization": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def is_authenticated() -> bool:
    """Check if we have a valid session."""
    if not _token_store.get("session_token"):
        return False
    expires_at = _token_store.get("expires_at", 0)
    if datetime.now(ET).timestamp() > expires_at:
        return authenticate()
    return True

def save_tokens(access_token, refresh_token, expires_in):
    """Stub for OAuth compatibility — not used in session auth."""
    pass

# ── Auto-authenticate on import ───────────────────────────────────────────────
if TT_USERNAME and TT_PASSWORD:
    authenticate()

# ── Option Chain & Contract Lookup ────────────────────────────────────────────

def get_expiry_date(trade_type: str = "DAY") -> str:
    """Calculate correct expiry based on Junior's playbook rules."""
    now = datetime.now(ET)
    weekday = now.weekday()  # 0=Mon, 4=Fri

    if trade_type == "DAY":
        days_to_friday = (4 - weekday) % 7
        if days_to_friday == 0:
            days_to_friday = 7  # Never 0DTE on Fridays
        elif weekday == 3 and now.hour >= 11:
            days_to_friday += 7  # Thursday after 11 AM — use next week
        target = now + timedelta(days=days_to_friday)
    else:
        target = now + timedelta(days=45)

    return target.strftime("%Y-%m-%d")

def get_current_price(ticker: str) -> float | None:
    """Get current market price for a ticker."""
    try:
        resp = requests.get(
            f"{BASE_URL}/market-data/quotes",
            headers=get_headers(),
            params={"symbols[]": ticker}
        )
        if resp.status_code == 200:
            items = resp.json().get("data", {}).get("items", [])
            if items:
                last = items[0].get("last") or items[0].get("mark")
                return float(last) if last else None
    except Exception as e:
        log.error(f"Price fetch error for {ticker}: {e}")
    return None

def find_option_contract(ticker: str, direction: str, trade_type: str = "DAY") -> dict | None:
    """
    Find the best option contract matching Junior's playbook rules:
    - Day trades: $0.75-$1.50 premium, slightly OTM, spread < 5%
    - Swing trades: $1.00-$3.50 premium, ~45 DTE
    """
    if not is_authenticated():
        log.error("Cannot find contract — not authenticated")
        return None

    expiry = get_expiry_date(trade_type)
    option_type = "call" if direction == "CALL" else "put"

    try:
        # Get option chain
        resp = requests.get(
            f"{BASE_URL}/option-chains/{ticker}/nested",
            headers=get_headers()
        )

        if resp.status_code != 200:
            log.error(f"Option chain fetch failed for {ticker}: {resp.status_code}")
            return None

        chain_items = resp.json().get("data", {}).get("items", [])
        if not chain_items:
            log.warning(f"Empty option chain for {ticker}")
            return None

        # Find the target expiration
        target_expiry = None
        for item in chain_items:
            for exp in item.get("expirations", []):
                if exp.get("expiration-date") == expiry:
                    target_expiry = exp
                    break
            if target_expiry:
                break

        if not target_expiry:
            # Try nearest available expiry
            all_expiries = []
            for item in chain_items:
                for exp in item.get("expirations", []):
                    all_expiries.append(exp)
            if all_expiries:
                all_expiries.sort(key=lambda x: x.get("expiration-date", ""))
                target_expiry = all_expiries[0]
                expiry = target_expiry.get("expiration-date")
                log.info(f"Using nearest expiry for {ticker}: {expiry}")
            else:
                log.warning(f"No expirations found for {ticker}")
                return None

        # Get current price
        current_price = get_current_price(ticker)
        if not current_price:
            log.error(f"Cannot get price for {ticker}")
            return None

        log.info(f"Searching {ticker} {direction} contracts — current price: ${current_price:.2f}, expiry: {expiry}")

        # Find best contract in premium range
        best_contract = None
        min_premium = 0.75 if trade_type == "DAY" else 1.00
        max_premium = 1.50 if trade_type == "DAY" else 3.50

        strikes = target_expiry.get("strikes", [])
        # Sort strikes for directional scanning
        if direction == "CALL":
            strikes = sorted(strikes, key=lambda x: float(x.get("strike-price", 0)))
        else:
            strikes = sorted(strikes, key=lambda x: float(x.get("strike-price", 0)), reverse=True)

        for strike_data in strikes:
            strike_price = float(strike_data.get("strike-price", 0))
            contract_symbol = strike_data.get(option_type, "")

            if not contract_symbol:
                continue

            # Filter for OTM only
            if direction == "CALL" and strike_price <= current_price:
                continue
            if direction == "PUT" and strike_price >= current_price:
                continue

            # Get quote for this contract
            try:
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

                bid = float(quotes[0].get("bid", 0) or 0)
                ask = float(quotes[0].get("ask", 0) or 0)

                if bid <= 0 or ask <= 0:
                    continue

                mid = (bid + ask) / 2
                spread_pct = (ask - bid) / ask * 100

                # Check spread and premium
                if spread_pct > 5:
                    continue

                if min_premium <= mid <= max_premium:
                    best_contract = {
                        "symbol": contract_symbol,
                        "strike": strike_price,
                        "expiry": expiry,
                        "direction": direction,
                        "bid": round(bid, 2),
                        "ask": round(ask, 2),
                        "mid": round(mid, 2),
                        "spread_pct": round(spread_pct, 2),
                        "ticker": ticker,
                        "current_price": current_price,
                    }
                    log.info(f"Found contract: {contract_symbol} strike=${strike_price} mid=${mid:.2f} spread={spread_pct:.1f}%")
                    break

            except Exception as e:
                log.error(f"Quote fetch error for {contract_symbol}: {e}")
                continue

        if not best_contract:
            log.warning(f"No contract found for {ticker} {direction} in ${min_premium}-${max_premium} range on {expiry}")

        return best_contract

    except Exception as e:
        log.error(f"Contract search error: {e}")
        return None

# ── Order Placement ────────────────────────────────────────────────────────────

def place_order(contract: dict, quantity: int = 1) -> dict | None:
    """Place a limit buy order at the ask price."""
    if not is_authenticated():
        log.error("Cannot place order — not authenticated")
        return None

    mode = "PAPER" if PAPER_TRADING else "LIVE"
    log.info(f"[{mode}] Placing order: {quantity}x {contract['symbol']} @ ${contract['ask']:.2f}")

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

        # Always do a dry run first
        dry_resp = requests.post(
            f"{url}/dry-run",
            headers=get_headers(),
            json=order_payload
        )

        if dry_resp.status_code not in (200, 201):
            log.error(f"Dry run failed: {dry_resp.status_code} — {dry_resp.text[:300]}")
            return None

        log.info(f"Dry run passed for {contract['symbol']}")

        # Place the actual order
        resp = requests.post(url, headers=get_headers(), json=order_payload)

        if resp.status_code in (200, 201):
            order_data = resp.json().get("data", {}).get("order", {})
            order_id = order_data.get("id", "unknown")
            log.info(f"Order placed successfully: ID={order_id}")
            return {
                "order_id": order_id,
                "symbol": contract["symbol"],
                "quantity": quantity,
                "price": contract["ask"],
                "status": order_data.get("status", "submitted"),
                "paper": PAPER_TRADING,
            }
        else:
            log.error(f"Order failed: {resp.status_code} — {resp.text[:300]}")
            return None

    except Exception as e:
        log.error(f"Order placement error: {e}")
        return None

def close_position(symbol: str, quantity: int) -> dict | None:
    """Close an open position."""
    if not is_authenticated():
        return None

    try:
        quote_resp = requests.get(
            f"{BASE_URL}/market-data/quotes",
            headers=get_headers(),
            params={"symbols[]": symbol}
        )
        bid = 0.01
        if quote_resp.status_code == 200:
            items = quote_resp.json().get("data", {}).get("items", [])
            if items:
                bid = float(items[0].get("bid", 0.01) or 0.01)

        order_payload = {
            "time-in-force": "Day",
            "order-type": "Limit",
            "price": str(round(bid, 2)),
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

        resp = requests.post(
            f"{BASE_URL}/accounts/{ACCOUNT_NUMBER}/orders",
            headers=get_headers(),
            json=order_payload
        )

        if resp.status_code in (200, 201):
            order_data = resp.json().get("data", {}).get("order", {})
            log.info(f"Position closed: {symbol} x{quantity}")
            return {"status": "closed", "symbol": symbol, "order_id": order_data.get("id")}
        else:
            log.error(f"Close failed: {resp.status_code} — {resp.text[:200]}")
            return None

    except Exception as e:
        log.error(f"Close position error: {e}")
        return None

def get_positions() -> list:
    """Get all open positions."""
    if not is_authenticated():
        return []
    try:
        resp = requests.get(
            f"{BASE_URL}/accounts/{ACCOUNT_NUMBER}/positions",
            headers=get_headers()
        )
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
        resp = requests.get(
            f"{BASE_URL}/accounts/{ACCOUNT_NUMBER}/balances",
            headers=get_headers()
        )
        if resp.status_code == 200:
            return resp.json().get("data", {})
        return {}
    except Exception as e:
        log.error(f"Get balance error: {e}")
        return {}
