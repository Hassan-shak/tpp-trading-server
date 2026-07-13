"""
gate_checks.py
TPP Trading Server v5.0

All hard gates checked in order on every incoming webhook.
Every gate logs its decision — pass or block — so Render logs tell you
exactly why any signal was suppressed.

Gate order:
  1. Market day (weekends + holidays)
  2. Trading window (9:30–10:30 AM ET — new entries only)
  3. Ticker whitelist (NVDA, TSLA only)
  4. FOMC / blackout
  5. Circuit breaker
  6. Max trades (2/day)
  7. Open position (one at a time)
  8. Entry cooldown (entry signals: 0s — commentary: 5 min)

NOTE: _is_in_window() gates new entries only. It does NOT affect the
position monitor — open positions are managed until closed regardless
of window status.
"""

import os
import logging
from datetime import datetime, date, timedelta
import pytz

ET  = pytz.timezone("America/New_York")
log = logging.getLogger("gates")

TRADEABLE_TICKERS = {"NVDA", "TSLA"}

# ── market holidays 2026 ──────────────────────────────────────────────────────
MARKET_HOLIDAYS_2026 = {
    date(2026,  1,  1),  # New Year's Day
    date(2026,  1, 19),  # MLK Day
    date(2026,  2, 16),  # Presidents' Day
    date(2026,  4,  3),  # Good Friday
    date(2026,  5, 25),  # Memorial Day
    date(2026,  6, 19),  # Juneteenth
    date(2026,  7,  3),  # Independence Day (observed)
    date(2026,  9,  7),  # Labor Day
    date(2026, 11, 26),  # Thanksgiving
    date(2026, 12, 25),  # Christmas
}

# FOMC decision days 2026 — full day halt
FOMC_DECISION_DAYS_2026 = {
    date(2026,  1, 29),
    date(2026,  3, 19),
    date(2026,  5,  7),
    date(2026,  6, 18),
    date(2026,  7, 29),
    date(2026,  9, 16),
    date(2026, 10, 28),
    date(2026, 12,  9),
}

# ── cooldown tracking ─────────────────────────────────────────────────────────
_last_signal: dict[str, datetime]      = {}
_last_commentary: dict[str, datetime]  = {}

ENTRY_COOLDOWN_SECONDS      = 0    # entry signals bypass cooldown entirely
COMMENTARY_COOLDOWN_SECONDS = 300  # 5 minutes


# ── individual gates ──────────────────────────────────────────────────────────
def _is_market_day() -> tuple[bool, str]:
    today = datetime.now(ET).date()
    if today.weekday() >= 5:
        return False, f"weekend ({today.strftime('%A')})"
    if today in MARKET_HOLIDAYS_2026:
        return False, f"market holiday ({today})"
    return True, "ok"


def _is_in_window() -> tuple[bool, str]:
    """
    Gates NEW trade entries only.
    Open positions are managed past 10:30 AM until naturally closed.
    """
    now    = datetime.now(ET)
    open_  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_ = now.replace(hour=10, minute=30, second=0, microsecond=0)
    if open_ <= now <= close_:
        return True, "ok"
    return False, f"outside 9:30–10:30 AM window (current: {now.strftime('%H:%M ET')})"


def _is_valid_ticker(ticker: str) -> tuple[bool, str]:
    if ticker in TRADEABLE_TICKERS:
        return True, "ok"
    return False, f"ticker {ticker} not in whitelist {TRADEABLE_TICKERS}"


def _is_blackout() -> tuple[bool, str]:
    today = datetime.now(ET).date()
    if today in FOMC_DECISION_DAYS_2026:
        return True, "FOMC decision day — full day halt"
    if os.environ.get("MANUAL_BLACKOUT", "0").strip() == "1":
        return True, "manual blackout active (MANUAL_BLACKOUT=1)"
    return False, "ok"


def _circuit_breaker_active() -> tuple[bool, str]:
    from session_state import get_circuit_breaker
    if get_circuit_breaker():
        return True, "circuit breaker — 2 consecutive losses today"
    return False, "ok"


def _max_trades_reached() -> tuple[bool, str]:
    from session_state import is_max_trades_reached, get_trade_count
    if is_max_trades_reached():
        return True, f"max trades reached ({get_trade_count()}/2 today)"
    return False, "ok"


def _position_already_open() -> tuple[bool, str]:
    from session_state import get_open_position
    pos = get_open_position()
    if pos:
        return True, f"position already open: {pos.get('occ_symbol')}"
    return False, "ok"


def _cooldown_ok(ticker: str, signal_type: str) -> tuple[bool, str]:
    now = datetime.now(ET)
    if signal_type == "entry":
        _last_signal[ticker] = now
        return True, "entry signals bypass cooldown"
    last = _last_commentary.get(ticker)
    if last:
        elapsed = (now - last).total_seconds()
        if elapsed < COMMENTARY_COOLDOWN_SECONDS:
            remaining = int(COMMENTARY_COOLDOWN_SECONDS - elapsed)
            return False, f"commentary cooldown — {remaining}s remaining for {ticker}"
    _last_commentary[ticker] = now
    return True, "ok"


# ── master gate check ─────────────────────────────────────────────────────────
def all_gates_pass(ticker: str, signal_type: str = "entry") -> bool:
    """
    Run all gates in order. Logs every decision.
    Returns True only if every gate passes.
    signal_type: 'entry' | 'commentary'
    """
    checks = [
        ("market_day",      _is_market_day),
        ("window",          _is_in_window),
        ("ticker",          lambda: _is_valid_ticker(ticker)),
        ("blackout",        _is_blackout),
        ("circuit_breaker", _circuit_breaker_active),
        ("max_trades",      _max_trades_reached),
        ("open_position",   _position_already_open),
        ("cooldown",        lambda: _cooldown_ok(ticker, signal_type)),
    ]

    for name, check_fn in checks:
        passed, reason = check_fn()
        if not passed:
            log.info(f"GATE BLOCKED [{ticker}] [{signal_type}] — {name}: {reason}")
            return False

    log.info(f"GATE PASSED [{ticker}] [{signal_type}] — all checks clear → sending to Claude")
    return True


# ── commentary filter ─────────────────────────────────────────────────────────
def commentary_allowed(ticker: str) -> bool:
    """
    For pulse/watchlist commentary only.
    Blocks non-tradeable tickers entirely — zero SPY/QQQ output ever.
    """
    if ticker not in TRADEABLE_TICKERS:
        log.debug(f"Commentary blocked — {ticker} not tradeable")
        return False
    _, reason = _is_in_window()
    if reason != "ok":
        return False
    ok, _ = _cooldown_ok(ticker, "commentary")
    return ok
