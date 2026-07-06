"""
The Portfolio Plug — Tastytrade Executor v2 (LIVE-READY)
Market-order entries with fill confirmation, broker-resting stop-limit exits,
limit-order profit taking, emergency market close, quotes, and cancel/replace.

Interface preserved for main.py:
  is_authenticated, find_option_contract, place_order, close_position,
  get_positions, get_account_balance, BASE_URL, PAPER_TRADING
New in v2:
  wait_for_fill, place_stop_limit_exit, place_limit_exit, market_close,
  cancel_order, get_option_quote, get_open_orders
"""

import os
import time
import logging
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

# ── Config ────────────────────────────────────────────────────────────────────
PAPER_TRADING = os.environ.get("TASTYTRADE_PAPER_TRADING", "true").lower() == "true"
BASE_URL = "https://api.cert.tastyworks.com" if PAPER_TRADING else "https://api.tastyworks.com"

CLIENT_ID     = os.environ.get("TASTYTRADE_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("TASTYTRADE_CLIENT_SECRET", "")
REFRESH_TOKEN = os.environ.get("TASTYTRADE_REFRESH_TOKEN", "")
ACCOUNT       = os.environ.get("TASTYTRADE_ACCOUNT", "5WI89808")

_access_token  = None
_token_expiry  = None

# ── Auth ──────────────────────────────────────────────────────────────────────
def _refresh_access_token() -> bool:
    global _access_token, _token_expiry
    try:
        resp = requests.post(
            f"{BASE_URL}/oauth/token",
            json={
                "grant_type":    "refresh_token",
                "refresh_token": REFRESH_TOKEN,
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            _access_token = data.get("access_token")
            expires_in    = int(data.get("expires_in", 900))
            _token_expiry = datetime.now(ET) + timedelta(seconds=expires_in - 60)
            mode = "PAPER" if PAPER_TRADING else "LIVE"
            log.info(f"✅ Tastytrade access token refreshed — {mode} mode | Account: {ACCOUNT}")
            return True
        log.error(f"Tastytrade token refresh failed: {resp.status_code} {resp.text}")
        return False
    except Exception as e:
        log.error(f"Tastytrade token refresh error: {e}")
        return False

def _headers() -> dict:
    global _access_token, _token_expiry
    if _access_token is None or _token_expiry is None or datetime.now(ET) >= _token_expiry:
        _refresh_access_token()
    return {"Authorization": f"Bearer {_access_token}", "Content-Type": "application/json"}

def is_authenticated() -> bool:
    try:
        resp = requests.get(f"{BASE_URL}/customers/me", headers=_headers(), timeout=10)
        return resp.status_code == 200
    except Exception:
        return False

# ── Account data ──────────────────────────────────────────────────────────────
def get_account_balance() -> dict:
    try:
        resp = requests.get(f"{BASE_URL}/accounts/{ACCOUNT}/balances", headers=_headers(), timeout=10)
        if resp.status_code == 200:
            d = resp.json().get("data", {})
            return {
                "cash": d.get("cash-balance"),
                "buying_power": d.get("derivative-buying-power"),
                "net_liq": d.get("net-liquidating-value"),
            }
        return {}
    except Exception as e:
        log.error(f"Tastytrade balance error: {e}")
        return {}

def get_positions() -> list:
    try:
        resp = requests.get(f"{BASE_URL}/accounts/{ACCOUNT}/positions", headers=_headers(), timeout=10)
        if resp.status_code == 200:
            return resp.json().get("data", {}).get("items", [])
        return []
    except Exception as e:
        log.error(f"Tastytrade positions error: {e}")
        return []

def get_open_orders() -> list:
    """All live (working) orders on the account."""
    try:
        resp = requests.get(
            f"{BASE_URL}/accounts/{ACCOUNT}/orders/live",
            headers=_headers(), timeout=10
        )
        if resp.status_code == 200:
            return resp.json().get("data", {}).get("items", [])
        return []
    except Exception as e:
        log.error(f"Tastytrade open orders error: {e}")
        return []

# ── Quotes ────────────────────────────────────────────────────────────────────
def get_option_quote(occ_symbol: str) -> dict | None:
    """Bid/ask/mid for an equity option. Returns None if unavailable."""
    try:
        resp = requests.get(
            f"{BASE_URL}/market-data/by-type",
            headers=_headers(),
            params={"equity-option": occ_symbol},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        items = resp.json().get("data", {}).get("items", [])
        if not items:
            return None
        q   = items[0]
        bid = float(q.get("bid") or 0)
        ask = float(q.get("ask") or 0)
        if ask <= 0:
            return None
        return {"bid": bid, "ask": ask, "mid": round((bid + ask) / 2, 2)}
    except Exception as e:
        log.error(f"Quote error for {occ_symbol}: {e}")
        return None

# ── Contract selection ────────────────────────────────────────────────────────
def _next_valid_expiry(trade_type: str) -> str:
    """Day trades: this week's Friday (Mon–Wed), next Friday (Thu after 11 / Fri).
    Never 0DTE. Swings handled by caller with wider windows."""
    now   = datetime.now(ET)
    today = now.date()
    dow   = today.weekday()  # Mon=0
    days_until_friday = (4 - dow) % 7
    if days_until_friday == 0:                      # today is Friday → next week
        days_until_friday = 7
    if dow == 3 and now.hour >= 11:                 # Thursday after 11 AM → next week
        days_until_friday += 7
    return (today + timedelta(days=days_until_friday)).strftime("%Y-%m-%d")

def find_option_contract(ticker: str, direction: str, trade_type: str = "DAY") -> dict | None:
    """
    Playbook rules: weekly expiry (no 0DTE, no LEAPs), OTM, premium $0.75–$1.50
    (day) / $1.00–$3.50 (swing), spread < 5%.
    """
    try:
        expiry      = _next_valid_expiry(trade_type)
        option_type = "C" if direction == "CALL" else "P"
        lo, hi      = (0.75, 1.50)   # ALL trades capped at $1.50 per Junior (Jul 6)

        log.info(f"🔍 Tastytrade chain search: {ticker} {direction} {trade_type} expiry={expiry}")

        resp = requests.get(f"{BASE_URL}/option-chains/{ticker}/nested", headers=_headers(), timeout=15)
        if resp.status_code != 200:
            log.error(f"Option chain fetch failed: {resp.status_code}")
            return None

        expirations = resp.json().get("data", {}).get("items", [{}])[0].get("expirations", [])
        target = next((e for e in expirations if e.get("expiration-date") == expiry), None)
        if not target:
            # fall back to nearest expiry AFTER target (never before → avoids 0DTE)
            future = [e for e in expirations if e.get("expiration-date", "") > datetime.now(ET).date().isoformat()]
            future.sort(key=lambda e: e.get("expiration-date", ""))
            target = future[0] if future else None
        if not target:
            log.warning("No valid expiration found")
            return None
        expiry = target.get("expiration-date")

        best = None
        for strike in target.get("strikes", []):
            occ = strike.get("call") if option_type == "C" else strike.get("put")
            if not occ:
                continue
            quote = get_option_quote(occ)
            if not quote:
                continue
            mid = quote["mid"]
            if mid < lo or mid > hi:
                continue
            spread_pct = (quote["ask"] - quote["bid"]) / quote["ask"] * 100 if quote["ask"] > 0 else 100
            if spread_pct > 5:
                continue
            sweet = (lo + hi) / 2
            if best is None or abs(mid - sweet) < abs(best["mid"] - sweet):
                best = {
                    "symbol":     occ,
                    "strike":     float(strike.get("strike-price", 0)),
                    "expiry":     expiry,
                    "type":       "call" if option_type == "C" else "put",
                    "bid":        quote["bid"],
                    "ask":        quote["ask"],
                    "mid":        mid,
                    "spread_pct": round(spread_pct, 2),
                }
        if best:
            log.info(f"✅ Contract selected: {best}")
        else:
            log.warning(f"No contract passed filters for {ticker} {direction}")
        return best
    except Exception as e:
        log.error(f"find_option_contract error: {e}", exc_info=True)
        return None

# ── Orders ────────────────────────────────────────────────────────────────────
def _submit_order(order_json: dict) -> dict | None:
    try:
        resp = requests.post(
            f"{BASE_URL}/accounts/{ACCOUNT}/orders",
            headers=_headers(), json=order_json, timeout=15
        )
        if resp.status_code in (200, 201):
            data = resp.json().get("data", {}).get("order", {})
            log.info(f"✅ Order submitted: id={data.get('id')} type={order_json.get('order-type')}")
            return data
        log.error(f"Order rejected: {resp.status_code} {resp.text}")
        return None
    except Exception as e:
        log.error(f"Order submit error: {e}", exc_info=True)
        return None

def place_order(contract: dict, quantity: int = 1) -> dict | None:
    """ENTRY — MARKET order, Buy to Open (per playbook: instant fill on entry)."""
    order = {
        "time-in-force": "Day",
        "order-type":    "Market",
        "legs": [{
            "instrument-type": "Equity Option",
            "symbol":          contract["symbol"],
            "quantity":        quantity,
            "action":          "Buy to Open",
        }],
    }
    result = _submit_order(order)
    if result:
        result["order_id"] = result.get("id")
    return result

def get_order(order_id) -> dict | None:
    try:
        resp = requests.get(f"{BASE_URL}/accounts/{ACCOUNT}/orders/{order_id}",
                            headers=_headers(), timeout=10)
        if resp.status_code == 200:
            return resp.json().get("data", {})
        return None
    except Exception:
        return None

def wait_for_fill(order_id, timeout_sec: int = 60) -> dict | None:
    """Poll until the order is Filled. Returns {'fill_price': X} or None.
    If not filled within timeout, attempts to cancel and returns None (fail closed)."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        o = get_order(order_id)
        if o:
            status = o.get("status", "")
            if status == "Filled":
                fills, qty_total, cost = [], 0, 0.0
                for leg in o.get("legs", []):
                    for f in leg.get("fills", []):
                        q = int(f.get("quantity", 0)); p = float(f.get("fill-price", 0))
                        qty_total += q; cost += q * p
                fill_price = round(cost / qty_total, 2) if qty_total else None
                log.info(f"✅ Order {order_id} FILLED @ {fill_price}")
                return {"fill_price": fill_price, "order": o}
            if status in ("Cancelled", "Rejected", "Expired"):
                log.warning(f"Order {order_id} ended without fill: {status}")
                return None
        time.sleep(2)
    log.warning(f"Order {order_id} not filled in {timeout_sec}s — cancelling (fail closed)")
    cancel_order(order_id)
    return None

def cancel_order(order_id) -> bool:
    try:
        resp = requests.delete(f"{BASE_URL}/accounts/{ACCOUNT}/orders/{order_id}",
                               headers=_headers(), timeout=10)
        ok = resp.status_code in (200, 202, 204)
        log.info(f"Cancel order {order_id}: {'ok' if ok else resp.status_code}")
        return ok
    except Exception as e:
        log.error(f"Cancel error: {e}")
        return False

def place_stop_limit_exit(occ_symbol: str, quantity: int, stop_trigger: float, limit_price: float) -> dict | None:
    """Protective stop resting AT THE BROKER — survives server crashes."""
    order = {
        "time-in-force": "Day",
        "order-type":    "Stop Limit",
        "stop-trigger":  f"{stop_trigger:.2f}",
        "price":         f"{limit_price:.2f}",
        "price-effect":  "Credit",
        "legs": [{
            "instrument-type": "Equity Option",
            "symbol":          occ_symbol,
            "quantity":        quantity,
            "action":          "Sell to Close",
        }],
    }
    result = _submit_order(order)
    if result:
        result["order_id"] = result.get("id")
    return result

def place_limit_exit(occ_symbol: str, quantity: int, limit_price: float) -> dict | None:
    """Profit-take / controlled exit — LIMIT order per playbook."""
    order = {
        "time-in-force": "Day",
        "order-type":    "Limit",
        "price":         f"{limit_price:.2f}",
        "price-effect":  "Credit",
        "legs": [{
            "instrument-type": "Equity Option",
            "symbol":          occ_symbol,
            "quantity":        quantity,
            "action":          "Sell to Close",
        }],
    }
    result = _submit_order(order)
    if result:
        result["order_id"] = result.get("id")
    return result

def market_close(occ_symbol: str, quantity: int) -> dict | None:
    """EMERGENCY exit only — used when a limit exit fails to fill and capital is at risk."""
    order = {
        "time-in-force": "Day",
        "order-type":    "Market",
        "legs": [{
            "instrument-type": "Equity Option",
            "symbol":          occ_symbol,
            "quantity":        quantity,
            "action":          "Sell to Close",
        }],
    }
    result = _submit_order(order)
    if result:
        result["order_id"] = result.get("id")
    return result

def close_position(occ_symbol: str, quantity: int = 1, limit_price: float | None = None) -> dict | None:
    """Backwards-compatible close: limit if price given, else market."""
    if limit_price:
        return place_limit_exit(occ_symbol, quantity, limit_price)
    return market_close(occ_symbol, quantity)
