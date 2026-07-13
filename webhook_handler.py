"""
webhook_handler.py
TPP Trading Server v5.0

Flask routes:
  POST /webhook   — TradingView alert receiver (HMAC authenticated)
  POST /flatten   — Remote kill: close open position immediately
  POST /kill      — Remote kill: close position + trip circuit breaker
  GET  /status    — Health check: session state snapshot
"""

import os
import hmac
import hashlib
import logging
from datetime import datetime
import pytz
from flask import Blueprint, request, jsonify

from gate_checks      import all_gates_pass
from claude_brain     import call_claude
from session_state    import load_state, get_open_position
from execution_engine import execute_trade
from discord_bot      import post_to_discord

ET  = pytz.timezone("America/New_York")
log = logging.getLogger("webhook")

bp             = Blueprint("webhook", __name__)
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")


# ── auth ──────────────────────────────────────────────────────────────────────
def _verify_signature(payload: bytes, signature: str) -> bool:
    if not WEBHOOK_SECRET:
        log.warning("WEBHOOK_SECRET not set — skipping signature check")
        return True
    expected = hmac.new(WEBHOOK_SECRET.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature or "")


# ── /webhook ──────────────────────────────────────────────────────────────────
@bp.route("/webhook", methods=["POST"])
def webhook():
    sig = request.headers.get("X-Signature", "")
    if not _verify_signature(request.data, sig):
        log.warning("Webhook rejected — invalid signature")
        return jsonify({"error": "unauthorized"}), 401

    data = request.json
    if not data:
        return jsonify({"error": "no data"}), 400

    ticker     = str(data.get("ticker", "")).upper()
    alert_type = str(data.get("alert_type", "UNKNOWN"))

    log.info(f"━━━ WEBHOOK: {ticker} | {alert_type} ━━━")

    # Gate check — logs exact block reason if rejected
    if not all_gates_pass(ticker, signal_type="entry"):
        return jsonify({"status": "blocked"}), 200

    # Claude decision
    session  = load_state()
    decision = call_claude(data, session)

    if not decision:
        log.error("Claude returned no decision")
        return jsonify({"status": "claude_error"}), 200

    if decision.get("decision") == "APPROVE":
        d_ticker    = decision.get("ticker", ticker).upper()
        d_direction = decision.get("direction", "").lower()

        if d_direction not in ("call", "put"):
            log.error(f"Invalid direction from Claude: {d_direction}")
            return jsonify({"status": "invalid_direction"}), 200

        success = execute_trade(d_ticker, d_direction, decision)
        return jsonify({"status": "traded" if success else "execution_failed"}), 200

    else:
        # NO_TRADE — log reason, post nothing to Discord
        reason = decision.get("reason", "No reason given")
        log.info(f"NO_TRADE: {reason}")
        return jsonify({"status": "no_trade", "reason": reason}), 200


# ── /flatten ──────────────────────────────────────────────────────────────────
@bp.route("/flatten", methods=["POST"])
def flatten():
    """Close any open position immediately. Remote kill switch."""
    pos = get_open_position()
    if not pos:
        log.info("/flatten — no open position")
        return jsonify({"status": "no_position"}), 200

    from tastytrade_client import _live_option_quote, close_position
    from session_state     import clear_open_position, record_trade_result
    from execution_engine  import cancel_resting_stop

    occ        = pos["occ_symbol"]
    fill_price = float(pos["fill_price"])

    quote = _live_option_quote(occ)
    bid   = float(quote.get("bid", fill_price * 0.90)) if quote else fill_price * 0.90

    exit_price = close_position(occ, "MANUAL FLATTEN /flatten", bid)
    cancel_resting_stop(pos.get("stop_order_id"))

    pnl_pct    = (exit_price - fill_price) / fill_price
    pnl_dollar = (exit_price - fill_price) * 100
    emoji      = "✅" if exit_price > fill_price else "❌"
    close_time = datetime.now(ET).strftime("%H:%M ET")

    post_to_discord(
        "profits-and-recaps",
        f"{emoji} **{occ} CLOSED** — MANUAL FLATTEN\n"
        f"Entry: ${fill_price:.2f} → Exit: ${exit_price:.2f} ({close_time})\n"
        f"P&L: {pnl_pct:+.1%} ({'+' if pnl_dollar >= 0 else ''}${abs(pnl_dollar):.0f}/contract)",
    )

    clear_open_position()
    record_trade_result(win=(exit_price > fill_price))
    log.info(f"/flatten complete — {occ} @ ${exit_price:.2f}")
    return jsonify({"status": "flattened", "exit_price": exit_price}), 200


# ── /kill ─────────────────────────────────────────────────────────────────────
@bp.route("/kill", methods=["POST"])
def kill():
    """Flatten position + trip circuit breaker. Full day halt."""
    flatten()

    s = load_state()
    s["circuit_breaker"] = True
    from session_state import _commit
    _commit(s)

    log.warning("/kill — circuit breaker activated, no more trades today")
    post_to_discord("day-trade-signals", "🛑 Kill switch activated — no more trades today.")
    return jsonify({"status": "killed"}), 200


# ── /status ───────────────────────────────────────────────────────────────────
@bp.route("/status", methods=["GET"])
def status():
    """Session state snapshot. Use this to verify the system is live."""
    s   = load_state()
    now = datetime.now(ET)
    pos = s.get("open_position")
    return jsonify({
        "time_et":            now.strftime("%Y-%m-%d %H:%M:%S ET"),
        "trade_count":        s["trade_count"],
        "circuit_breaker":    s["circuit_breaker"],
        "consecutive_losses": s["consecutive_losses"],
        "open_position":      pos is not None,
        "open_symbol":        pos["occ_symbol"] if pos else None,
        "last_reset_date":    s["last_reset_date"],
    }), 200
