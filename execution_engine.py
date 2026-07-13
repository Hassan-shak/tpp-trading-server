"""
execution_engine.py
TPP Trading Server v5.0

Master trade entry orchestrator. Called after Claude returns APPROVE.

Flow:
  1. Select contract (ATM → OTM walk, $0.75–$1.50 ask/share)
  2. Market order entry via Tastytrade
  3. Poll for fill (60s timeout)
  4. Place resting stop-limit (-25% trigger / -30% limit)
  5. Post signal to #day-trade-signals (@everyone)
  6. Write session state
"""

import logging
from datetime import datetime
import pytz

from tastytrade_client import (
    select_contract,
    enter_trade,
    place_stop_loss,
    _cancel_order,
)
from session_state import (
    get_open_position,
    set_open_position,
    clear_open_position,
    increment_trade_count,
    record_trade_result,
)
from discord_bot import post_to_discord, send_emergency_dm

ET  = pytz.timezone("America/New_York")
log = logging.getLogger("execution")


def cancel_resting_stop(stop_order_id: str | None):
    """Cancel the broker stop-limit when position is closed via monitor or manually."""
    if stop_order_id:
        _cancel_order(stop_order_id)
        log.info(f"Resting stop {stop_order_id} cancelled")


def _build_signal_message(
    ticker:            str,
    direction:         str,
    occ_symbol:        str,
    fill_price:        float,
    tier:              str,
    setup_description: str,
) -> str:
    arrow      = "🟢" if direction == "call" else "🔴"
    type_label = "CALL" if direction == "call" else "PUT"
    target     = round(fill_price * 1.40, 2)
    stop       = round(fill_price * 0.75, 2)
    cost       = round(fill_price * 100,  2)
    entry_time = datetime.now(ET).strftime("%H:%M ET")

    return (
        f"@everyone\n\n"
        f"{arrow} **{ticker} {type_label}** [{tier}]\n\n"
        f"**Contract:** `{occ_symbol}`\n"
        f"**Entry:** ${fill_price:.2f}/share (${cost:.0f}/contract) @ {entry_time}\n"
        f"**Target:** ${target:.2f} (+40%)\n"
        f"**Stop:** ${stop:.2f} (-25%)\n\n"
        f"{setup_description}"
    )


def execute_trade(ticker: str, direction: str, claude_decision: dict) -> bool:
    """
    Full entry flow. Returns True if trade entered and confirmed.
    """
    log.info(f"{'='*50}")
    log.info(f"EXECUTE: {ticker} {direction.upper()}")
    log.info(f"{'='*50}")

    tier  = claude_decision.get("tier", "TIER-1")
    setup = claude_decision.get("setup_description", "")

    # Step 1: select contract
    occ, strike, ask = select_contract(ticker, direction)

    if not occ:
        post_to_discord(
            "day-trade-signals",
            f"⚠️ Setup identified on **{ticker} {direction.upper()}** "
            f"but no contract available in the $75–$150 range. Passing on this one.",
        )
        log.warning(f"No valid contract — trade aborted for {ticker} {direction}")
        return False

    log.info(f"Selected: {occ} | ask=${ask:.2f} (${ask*100:.0f}/contract)")

    # Step 2: enter
    fill_price = enter_trade(occ)

    if not fill_price:
        post_to_discord(
            "day-trade-signals",
            f"⚠️ Order placed for **{ticker}** but fill not confirmed within 60s. "
            f"No position recorded — check broker.",
        )
        log.error(f"No fill confirmed for {occ}")
        return False

    log.info(f"FILLED: {occ} @ ${fill_price:.2f}/share")

    # Step 3: stop-loss
    stop_order_id = place_stop_loss(occ, fill_price)

    if not stop_order_id:
        send_emergency_dm(
            f"🚨 STOP LOSS FAILED — {occ} filled @ ${fill_price:.2f} — SET MANUALLY NOW"
        )

    # Step 4: post signal
    post_to_discord(
        "day-trade-signals",
        _build_signal_message(ticker, direction, occ, fill_price, tier, setup),
    )
    log.info("Signal posted to #day-trade-signals")

    # Step 5: write state
    set_open_position({
        "ticker":        ticker,
        "direction":     direction,
        "occ_symbol":    occ,
        "fill_price":    fill_price,
        "stop_order_id": stop_order_id,
        "target_price":  round(fill_price * 1.40, 2),
        "peak_pnl":      0.0,
        "entry_time":    datetime.now(ET).isoformat(),
    })
    increment_trade_count()

    log.info(f"Trade complete — {occ} live | stop order={stop_order_id}")
    return True
