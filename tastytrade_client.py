"""
tastytrade_client.py
TPP Trading Server v5.0

All Tastytrade API interactions:
  - Session auth (cached, auto-refresh at 23hrs)
  - Options chain fetch
  - Contract selection ($0.75–$1.50 ask/share, ATM → OTM walk)
  - Market order entry
  - Fill polling (60s timeout → cancel)
  - Stop-limit placement (-25% trigger / -30% limit)
  - Limit close with 45s escalation to market
  - Position monitor (profit target, trailing stop, chop circuit)

IMPORTANT: There is no forced flatten at 10:30 AM.
If a position is open at window close, it continues to be monitored
and managed by exit rules until it closes naturally. The recap posts
to Discord when the trade closes, regardless of time.
"""

import os
import time
import logging
import requests
from datetime import datetime, timedelta
import pytz

ET  = pytz.timezone("America/New_York")
log = logging.getLogger("tastytrade")

TT_BASE    = "https://api.tastytrade.com"
TT_USER    = os.environ["TASTYTRADE_USERNAME"]
TT_PASS    = os.environ["TASTYTRADE_PASSWORD"]
TT_ACCOUNT = os.environ["TASTYTRADE_ACCOUNT_NUMBER"]

MIN_PREMIUM  = 0.75   # per share → $75 / contract
MAX_PREMIUM  = 1.50   # per share → $150 / contract
MAX_SPREAD   = 0.05   # 5% bid-ask spread cap
FILL_TIMEOUT = 60     # seconds before cancel on entry
CLOSE_TIMEOUT = 45    # seconds before market escalation on exit


# ── session auth ──────────────────────────────────────────────────────────────
_session_token  = None
_session_expiry = None


def _get_token() -> str:
    global _session_token, _session_expiry
    now = datetime.now(ET)
    if _session_token and _session_expiry and now < _session_expiry:
        return _session_token

    resp = requests.post(
        f"{TT_BASE}/sessions",
        json={"login": TT_USER, "password": TT_PASS, "remember-me": True},
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    if resp.status_code != 201:
        raise RuntimeError(f"Tastytrade auth failed {resp.status_code}: {resp.text}")

    _session_token  = resp.json()["data"]["session-token"]
    _session_expiry = now + timedelta(hours=23)
    log.info("Tastytrade session refreshed")
    return _session_token


def _headers() -> dict:
    return {"Authorization": _get_token(), "Content-Type": "application/json"}


# ── expiry ────────────────────────────────────────────────────────────────────
def _next_friday():
    today = datetime.now(ET).date()
    now   = datetime.now(ET)
    days  = (4 - today.weekday()) % 7 or 7
    if today.weekday() == 3 and now.hour >= 14:
        days += 7
    return today + timedelta(days=days)


# ── spot price (Alpaca primary, Tastytrade fallback) ──────────────────────────
def _spot_price(ticker: str) -> float | None:
    try:
        from alpaca_data import get_latest_price
        price = get_latest_price(ticker)
        if price:
            return float(price)
    except Exception as e:
        log.warning(f"Alpaca spot price failed for {ticker}: {e}")

    try:
        resp = requests.get(
            f"{TT_BASE}/market-data/equities",
            headers=_headers(),
            params={"symbols[]": ticker},
            timeout=5,
        )
        if resp.status_code == 200:
            items = resp.json()["data"]["items"]
            if items:
                return float(items[0].get("last", 0)) or None
    except Exception as e:
        log.error(f"Tastytrade spot price fallback failed for {ticker}: {e}")

    return None


# ── option quote ──────────────────────────────────────────────────────────────
def _live_option_quote(occ_symbol: str) -> dict | None:
    try:
        resp = requests.get(
            f"{TT_BASE}/market-data/options",
            headers=_headers(),
            params={"symbols[]": occ_symbol},
            timeout=5,
        )
        if resp.status_code == 200:
            items = resp.json()["data"]["items"]
            return items[0] if items else None
    except Exception as e:
        log.error(f"Quote fetch failed for {occ_symbol}: {e}")
    return None


def _build_occ(ticker: str, expiry, strike: float, opt_type: str) -> str:
    return f"{ticker}{expiry.strftime('%y%m%d')}{opt_type}{int(strike * 1000):08d}"


# ── contract selection ────────────────────────────────────────────────────────
def select_contract(ticker: str, direction: str) -> tuple[str | None, float | None, float | None]:
    """
    Find best contract in $0.75–$1.50 ask/share range.
    Walks ATM → OTM. Stops if ask drops below MIN_PREMIUM.
    Returns (occ_symbol, strike, ask) or (None, None, None).
    """
    expiry   = _next_friday()
    opt_type = "C" if direction == "call" else "P"

    spot = _spot_price(ticker)
    if not spot:
        log.error(f"No spot price for {ticker} — cannot select contract")
        return None, None, None

    resp = requests.get(
        f"{TT_BASE}/option-chains/{ticker}/nested",
        headers=_headers(),
        params={"expiration-date": expiry.strftime("%Y-%m-%d")},
        timeout=10,
    )
    if resp.status_code != 200:
        log.error(f"Options chain fetch failed for {ticker}: {resp.text}")
        return None, None, None

    expirations = resp.json()["data"].get("expirations", [])
    target_exp  = next(
        (e for e in expirations if e["expiration-date"] == expiry.strftime("%Y-%m-%d")),
        None,
    )
    if not target_exp:
        log.error(f"No expiry {expiry} in chain for {ticker}")
        return None, None, None

    strikes = sorted(float(s["strike-price"]) for s in target_exp.get("strikes", []))
    if not strikes:
        log.error(f"Empty strikes for {ticker} {expiry}")
        return None, None, None

    atm     = min(strikes, key=lambda s: abs(s - spot))
    atm_idx = strikes.index(atm)
    ordered = strikes[atm_idx:] if direction == "call" else list(reversed(strikes[:atm_idx + 1]))

    for strike in ordered:
        occ   = _build_occ(ticker, expiry, strike, opt_type)
        quote = _live_option_quote(occ)
        if not quote:
            continue

        ask = float(quote.get("ask") or 0)
        bid = float(quote.get("bid") or 0)

        if ask <= 0:
            continue
        if ask < MIN_PREMIUM:
            log.info(f"OTM walk stopped at {strike} — ask ${ask:.2f} below min")
            break

        spread = (ask - bid) / ask
        if spread >= MAX_SPREAD:
            log.info(f"Strike {strike} skipped — spread {spread:.1%} too wide")
            continue

        if MIN_PREMIUM <= ask <= MAX_PREMIUM:
            log.info(f"Contract selected: {occ} | ask=${ask:.2f}/share (${ask*100:.0f}/contract)")
            return occ, strike, ask

        log.info(f"Strike {strike} ask=${ask:.2f} above max — moving OTM")

    log.warning(f"No valid contract for {ticker} {direction} in ${MIN_PREMIUM}–${MAX_PREMIUM} range")
    return None, None, None


# ── order helpers ─────────────────────────────────────────────────────────────
def _place_order(payload: dict) -> str | None:
    resp = requests.post(
        f"{TT_BASE}/accounts/{TT_ACCOUNT}/orders",
        headers=_headers(),
        json=payload,
        timeout=10,
    )
    if resp.status_code not in (200, 201):
        log.error(f"Order placement failed {resp.status_code}: {resp.text}")
        return None
    order_id = str(resp.json()["data"]["order"]["id"])
    log.info(f"Order placed → ID {order_id}")
    return order_id


def _poll_fill(order_id: str, timeout: int) -> float | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(
            f"{TT_BASE}/accounts/{TT_ACCOUNT}/orders/{order_id}",
            headers=_headers(),
            timeout=5,
        )
        if resp.status_code != 200:
            time.sleep(2)
            continue

        order  = resp.json()["data"]["order"]
        status = order.get("status", "")

        if status == "Filled":
            fill = float(order["legs"][0].get("average-fill-price", 0))
            log.info(f"Order {order_id} FILLED @ ${fill:.2f}/share (${fill*100:.0f}/contract)")
            return fill
        if status in ("Cancelled", "Rejected", "Expired"):
            log.warning(f"Order {order_id} ended: {status}")
            return None

        log.debug(f"Order {order_id}: {status} — waiting…")
        time.sleep(3)

    log.warning(f"Order {order_id} timed out after {timeout}s — cancelling")
    _cancel_order(order_id)
    return None


def _cancel_order(order_id: str):
    resp = requests.delete(
        f"{TT_BASE}/accounts/{TT_ACCOUNT}/orders/{order_id}",
        headers=_headers(),
        timeout=5,
    )
    log.info(f"Cancel {order_id}: {resp.status_code}")


# ── entry ─────────────────────────────────────────────────────────────────────
def enter_trade(occ_symbol: str) -> float | None:
    """Market Buy-to-Open, 1 contract. Returns fill price or None."""
    order_id = _place_order({
        "time-in-force": "Day",
        "order-type":    "Market",
        "legs": [{
            "instrument-type": "Equity Option",
            "symbol":          occ_symbol,
            "quantity":        1,
            "action":          "Buy to Open",
        }],
    })
    return _poll_fill(order_id, FILL_TIMEOUT) if order_id else None


# ── stop-loss ─────────────────────────────────────────────────────────────────
def place_stop_loss(occ_symbol: str, fill_price: float) -> str | None:
    """
    Resting stop-limit immediately after entry.
    Trigger: -25% | Limit: -30%
    """
    trigger  = round(fill_price * 0.75, 2)
    limit    = round(fill_price * 0.70, 2)
    order_id = _place_order({
        "time-in-force": "Day",
        "order-type":    "Stop Limit",
        "stop-trigger":  str(trigger),
        "price":         str(limit),
        "legs": [{
            "instrument-type": "Equity Option",
            "symbol":          occ_symbol,
            "quantity":        1,
            "action":          "Sell to Close",
        }],
    })
    if order_id:
        log.info(f"Stop-limit: trigger=${trigger} limit=${limit} → order {order_id}")
    else:
        log.critical(f"STOP LOSS FAILED for {occ_symbol} — MANUAL INTERVENTION NEEDED")
    return order_id


# ── exit ──────────────────────────────────────────────────────────────────────
def close_position(occ_symbol: str, reason: str, current_bid: float) -> float:
    """
    Limit Sell-to-Close at current bid.
    Escalates to market after 45s if not filled.
    Returns exit fill price.
    """
    log.info(f"Closing {occ_symbol} — {reason}")

    order_id = _place_order({
        "time-in-force": "Day",
        "order-type":    "Limit",
        "price":         str(round(current_bid, 2)),
        "legs": [{
            "instrument-type": "Equity Option",
            "symbol":          occ_symbol,
            "quantity":        1,
            "action":          "Sell to Close",
        }],
    })

    fill = _poll_fill(order_id, CLOSE_TIMEOUT) if order_id else None

    if not fill:
        log.warning(f"Limit close not filled in {CLOSE_TIMEOUT}s — escalating to market")
        if order_id:
            _cancel_order(order_id)
        market_id = _place_order({
            "time-in-force": "Day",
            "order-type":    "Market",
            "legs": [{
                "instrument-type": "Equity Option",
                "symbol":          occ_symbol,
                "quantity":        1,
                "action":          "Sell to Close",
            }],
        })
        fill = _poll_fill(market_id, FILL_TIMEOUT) if market_id else current_bid

    return fill or current_bid


# ── position monitor ──────────────────────────────────────────────────────────
def monitor_position(
    position:      dict,
    discord_fn,
    clear_fn,
    record_fn,
    cancel_stop_fn,
) -> dict | None:
    """
    Called every 1 minute while a position is open.
    Runs past 10:30 AM if needed — no forced window-close flatten.
    Position is managed until one of the exit rules fires naturally.
    Recap posts to Discord when trade closes, regardless of time.

    Exit rules:
      • Profit target +40%
      • Trailing stop (arms at +10%, trails 15% below peak)
      • 10-min chop circuit: -15% if <5% move in 10 min
      • Hard stop-limit at broker handles -25% / -30% (resting order)

    Returns updated position dict if still open, None if closed.
    """
    now        = datetime.now(ET)
    occ        = position["occ_symbol"]
    fill_price = float(position["fill_price"])
    entry_time = datetime.fromisoformat(position["entry_time"])
    peak_pnl   = float(position.get("peak_pnl", 0.0))

    quote = _live_option_quote(occ)
    if not quote:
        log.warning(f"No quote for {occ} — skipping monitor tick")
        return position

    current_bid = float(quote.get("bid") or 0)
    if current_bid <= 0:
        return position

    pnl_pct = (current_bid - fill_price) / fill_price

    # Update peak
    if pnl_pct > peak_pnl:
        peak_pnl           = pnl_pct
        position["peak_pnl"] = peak_pnl

    reason = None

    # 1. Profit target +40%
    if pnl_pct >= 0.40:
        reason = "PROFIT TARGET +40%"

    # 2. Trailing stop — arms at +10%, trails 15% below peak
    elif peak_pnl >= 0.10 and pnl_pct <= (peak_pnl - 0.15):
        reason = f"TRAILING STOP (peak {peak_pnl:+.1%})"

    # 3. 10-min chop circuit — tighten to -15% if flat for 10 min
    else:
        mins_in = (now - entry_time).total_seconds() / 60
        if mins_in >= 10 and abs(pnl_pct) < 0.05 and pnl_pct <= -0.15:
            reason = "CHOP CIRCUIT -15%"

    if reason:
        exit_price = close_position(occ, reason, current_bid)
        cancel_stop_fn(position.get("stop_order_id"))

        pnl_dollar    = (exit_price - fill_price) * 100
        pnl_pct_final = (exit_price - fill_price) / fill_price
        emoji         = "✅" if exit_price > fill_price else "❌"
        close_time    = datetime.now(ET).strftime("%H:%M ET")

        discord_fn(
            "profits-and-recaps",
            f"{emoji} **{occ} CLOSED** — {reason}\n"
            f"Entry: ${fill_price:.2f} → Exit: ${exit_price:.2f} ({close_time})\n"
            f"P&L: {pnl_pct_final:+.1%} "
            f"({'+' if pnl_dollar >= 0 else ''}${abs(pnl_dollar):.0f}/contract)",
        )

        clear_fn()
        record_fn(win=(exit_price > fill_price))
        log.info(f"Position closed — {occ} | {reason} | {pnl_pct_final:+.1%}")
        return None  # position cleared

    return position  # still open
