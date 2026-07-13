"""
scheduler.py
TPP Trading Server v5.0

All scheduled jobs:

  08:00 AM  — Alpaca volume profile zone recalculation
  09:15 AM  — Daily watchlist post to #daily-watchlist (NVDA + TSLA only)
  09:25 AM  — 1-min scanner starts (runs every minute until 10:30 AM)
  09:45 AM  — Status update to #day-trade-signals (only if no trade yet)
  10:15 AM  — Status update to #day-trade-signals (only if no trade yet)
  Every 1min — Position monitor (runs past 10:30 AM if position still open)

Notes:
  - NO mandatory attempt rule. Trades only when valid setups exist.
  - Position monitor keeps running after 10:30 AM if a position is open.
    It goes idle only after the trade closes naturally and recap is posted.
  - Status updates at 9:45 and 10:15 are skipped automatically if a
    trade is open or has already fired that day.
  - Zero SPY/QQQ output anywhere.
"""

import logging
from datetime import datetime
import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from session_state import (
    load_state,
    get_open_position,
    set_open_position,
    clear_open_position,
    record_trade_result,
    get_trade_count,
)
from discord_bot      import post_to_discord
from tastytrade_client import monitor_position
from execution_engine  import cancel_resting_stop

ET  = pytz.timezone("America/New_York")
log = logging.getLogger("scheduler")

scheduler = BackgroundScheduler(timezone=ET)


# ── window helper ─────────────────────────────────────────────────────────────
def _in_window() -> bool:
    now = datetime.now(ET)
    return (now.hour == 9 and now.minute >= 30) or (now.hour == 10 and now.minute < 30)


# ── job 1: volume zones (8:00 AM) ─────────────────────────────────────────────
def job_volume_zones():
    log.info("JOB: volume zone recalculation")
    try:
        from alpaca_data import compute_volume_zones
        zones = compute_volume_zones(["NVDA", "TSLA"])
        log.info(f"Volume zones updated: {zones}")
    except Exception as e:
        log.error(f"Volume zone job failed: {e}")


# ── job 2: watchlist (9:15 AM) ────────────────────────────────────────────────
def job_watchlist():
    """
    Post daily watchlist for NVDA and TSLA only.
    No SPY/QQQ. No market commentary on non-tradeable tickers.
    """
    log.info("JOB: daily watchlist")
    try:
        from alpaca_data import get_daily_levels
        from claude_brain import call_claude

        for ticker in ["NVDA", "TSLA"]:
            levels = get_daily_levels(ticker)

            alert_data = {
                "ticker":     ticker,
                "alert_type": "WATCHLIST",
                "pmh":        levels.get("pmh"),
                "pml":        levels.get("pml"),
                "close":      levels.get("prev_close"),
                "volume":     levels.get("avg_volume"),
                "high":       levels.get("pmh"),
                "low":        levels.get("pml"),
                "open":       levels.get("prev_open"),
            }

            session  = load_state()
            decision = call_claude(alert_data, session)

            if decision and decision.get("decision") == "APPROVE":
                blurb = decision.get("setup_description", f"{ticker} levels loaded.")
            else:
                pmh = levels.get("pmh", "N/A")
                pml = levels.get("pml", "N/A")
                blurb = f"Watching {pmh} (PMH) and {pml} (PML) for direction."

            post_to_discord("daily-watchlist", f"**{ticker}** — {blurb}")

    except Exception as e:
        log.error(f"Watchlist job failed: {e}")


# ── job 3: 1-min scanner (9:25–10:30 AM) ─────────────────────────────────────
def job_scanner():
    """
    Runs every minute inside the trading window.
    Sends valid setups to Claude. Executes on APPROVE.
    Does not post commentary — only trade signals.
    """
    if not _in_window():
        return

    log.debug("JOB: scanner tick")

    try:
        from alpaca_data    import get_latest_1min_candle, get_key_levels
        from gate_checks    import all_gates_pass
        from claude_brain   import call_claude
        from execution_engine import execute_trade

        for ticker in ["NVDA", "TSLA"]:
            if not all_gates_pass(ticker, signal_type="entry"):
                continue

            candle = get_latest_1min_candle(ticker)
            levels = get_key_levels(ticker)

            if not candle:
                log.warning(f"No 1-min candle for {ticker}")
                continue

            alert_data = {
                "ticker":     ticker,
                "alert_type": "SCANNER_1MIN",
                "close":      candle.get("close"),
                "open":       candle.get("open"),
                "high":       candle.get("high"),
                "low":        candle.get("low"),
                "volume":     candle.get("volume"),
                "pmh":        levels.get("pmh"),
                "pml":        levels.get("pml"),
            }

            session  = load_state()
            decision = call_claude(alert_data, session)

            if decision and decision.get("decision") == "APPROVE":
                direction = decision.get("direction", "").lower()
                if direction in ("call", "put"):
                    execute_trade(ticker, direction, decision)
                    break  # one trade per scanner tick

    except Exception as e:
        log.error(f"Scanner job failed: {e}")


# ── job 4: 9:45 AM status update ─────────────────────────────────────────────
def job_status_update_945():
    """
    Post one status update at 9:45 AM.
    Skipped automatically if a trade is already open or has fired today.
    """
    s = load_state()
    if s["trade_count"] > 0 or get_open_position():
        log.info("9:45 status update skipped — trade already active")
        return

    post_to_discord(
        "day-trade-signals",
        "👀 Live and scanning — looking for a valid setup on NVDA and TSLA. "
        "Nothing worth the risk yet. Will alert when something lines up.",
    )
    log.info("9:45 status update posted")


# ── job 5: 10:15 AM status update ────────────────────────────────────────────
def job_status_update_1015():
    """
    Post one status update at 10:15 AM.
    Skipped automatically if a trade is already open or has fired today.
    """
    s = load_state()
    if s["trade_count"] > 0 or get_open_position():
        log.info("10:15 status update skipped — trade already active")
        return

    post_to_discord(
        "day-trade-signals",
        "🔍 Still watching — 15 minutes left in the window. "
        "No clean setup yet on NVDA or TSLA. "
        "If nothing sets up we sit out — no forced trades.",
    )
    log.info("10:15 status update posted")


# ── job 6: position monitor (every 1 min) ────────────────────────────────────
def job_position_monitor():
    """
    Runs every minute.

    Inside window (9:30–10:30): monitors and manages open position.
    Outside window: ONLY runs if a position is still open from the session.
      → Keeps managing past 10:30 AM until trade closes naturally.
      → Posts recap to Discord on close.
      → Goes idle once position is cleared.

    No forced flatten at 10:30 AM. Members get the close alert
    whenever the trade actually closes, even if that's past the window.
    """
    pos = get_open_position()

    # Outside window with no open position — nothing to do
    if not _in_window() and not pos:
        return

    if not pos:
        return

    log.debug(f"JOB: position monitor — {pos.get('occ_symbol')}")

    updated = monitor_position(
        position=pos,
        discord_fn=post_to_discord,
        clear_fn=clear_open_position,
        record_fn=record_trade_result,
        cancel_stop_fn=cancel_resting_stop,
    )

    if updated is not None:
        # Still open — save updated peak_pnl
        set_open_position(updated)
    # If None, monitor_position already called clear_fn and posted recap


# ── start ─────────────────────────────────────────────────────────────────────
def start_scheduler():
    # Volume zones: 8:00 AM
    scheduler.add_job(
        job_volume_zones, "cron",
        hour=8, minute=0,
        id="volume_zones", replace_existing=True,
    )

    # Watchlist: 9:15 AM
    scheduler.add_job(
        job_watchlist, "cron",
        hour=9, minute=15,
        id="watchlist", replace_existing=True,
    )

    # 1-min scanner: every minute starting 9:25 AM
    scheduler.add_job(
        job_scanner, "cron",
        hour="9-10", minute="*",
        id="scanner", replace_existing=True,
    )

    # 9:45 AM status update (only if no trade)
    scheduler.add_job(
        job_status_update_945, "cron",
        hour=9, minute=45,
        id="status_945", replace_existing=True,
    )

    # 10:15 AM status update (only if no trade)
    scheduler.add_job(
        job_status_update_1015, "cron",
        hour=10, minute=15,
        id="status_1015", replace_existing=True,
    )

    # Position monitor: every minute (runs past 10:30 if position open)
    scheduler.add_job(
        job_position_monitor, "interval",
        minutes=1,
        id="position_monitor", replace_existing=True,
    )

    scheduler.start()
    log.info("Scheduler started — all jobs registered")
    return scheduler
