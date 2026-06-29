"""
The Portfolio Plug — AI Trading Webhook Server
Receives TradingView alerts → calls Claude API → posts signals to Discord
Phase 2: Tastytrade API execution layer integrated
"""

import os
import json
import hmac
import logging
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import requests
from flask import Flask, request, jsonify, redirect
from anthropic import Anthropic
from tastytrade_executor import (
    save_tokens, is_authenticated, find_option_contract,
    place_order, close_position, get_positions, get_account_balance,
    CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, BASE_URL, PAPER_TRADING
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ── Clients ───────────────────────────────────────────────────────────────────
anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ── Config from environment ───────────────────────────────────────────────────
WEBHOOK_SECRET        = os.environ["WEBHOOK_SECRET"]
DISCORD_BOT_TOKEN     = os.environ["DISCORD_BOT_TOKEN"]
DISCORD_GUILD_ID      = os.environ["DISCORD_GUILD_ID"]
CHANNEL_WATCHLIST     = os.environ["DISCORD_CHANNEL_WATCHLIST"]
CHANNEL_DAY_SIGNALS   = os.environ["DISCORD_CHANNEL_DAY_SIGNALS"]
CHANNEL_SWING_SIGNALS = os.environ["DISCORD_CHANNEL_SWING_SIGNALS"]
CHANNEL_RECAPS        = os.environ["DISCORD_CHANNEL_RECAPS"]
CHANNEL_LONGTERM      = os.environ["DISCORD_CHANNEL_LONGTERM"]

ET = ZoneInfo("America/New_York")

# ── Approved tickers ──────────────────────────────────────────────────────────
DAY_TRADE_TICKERS = {"SPY", "QQQ", "TSLA", "NVDA", "AMZN", "MSFT"}
SWING_TICKERS     = {"SPY", "QQQ", "TSLA", "NVDA", "AMZN", "MSFT", "META", "GOOG"}

# ── Active positions tracking ─────────────────────────────────────────────────
active_positions = {}  # symbol -> {order_id, entry_price, quantity, ticker, direction}

# ── Session state (resets at midnight ET) ─────────────────────────────────────
session = {
    "date": None,
    "trade_count": 0,
    "consecutive_losses": 0,
    "circuit_breaker": False,
    "kill_switch_active": False,
    "kill_switch_reason": None,
}

def reset_session_if_new_day():
    today = datetime.now(ET).date()
    if session["date"] != today:
        session.update({
            "date": today,
            "trade_count": 0,
            "consecutive_losses": 0,
            "circuit_breaker": False,
            "kill_switch_active": False,
            "kill_switch_reason": None,
        })
        log.info(f"Session reset for {today}")

# ── Time window checks ────────────────────────────────────────────────────────
def in_day_trade_window() -> bool:
    now = datetime.now(ET).time()
    return dtime(9, 30) <= now <= dtime(11, 0)

def in_swing_window() -> bool:
    now = datetime.now(ET).time()
    return dtime(15, 0) <= now <= dtime(16, 0)

def in_dead_zone() -> bool:
    now = datetime.now(ET).time()
    return dtime(11, 0) < now < dtime(15, 0)

# ── Discord helper ────────────────────────────────────────────────────────────
DISCORD_API = "https://discord.com/api/v10"

def post_discord(channel_id: str, message: str) -> bool:
    url = f"{DISCORD_API}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "Content-Type": "application/json",
    }
    chunks = [message[i:i+1990] for i in range(0, len(message), 1990)]
    success = True
    for chunk in chunks:
        resp = requests.post(url, headers=headers, json={"content": chunk})
        if resp.status_code not in (200, 201):
            log.error(f"Discord post failed: {resp.status_code} — {resp.text}")
            success = False
    return success

# ── Load system prompt ────────────────────────────────────────────────────────
def get_system_prompt() -> str:
    path = os.path.join(os.path.dirname(__file__), "system_prompt.txt")
    with open(path, "r") as f:
        return f.read()

# ── Claude analysis ───────────────────────────────────────────────────────────
def analyze_with_claude(alert_data: dict, analysis_type: str) -> str:
    system_prompt = get_system_prompt()
    now_et = datetime.now(ET)
    tt_status = "CONNECTED (PAPER)" if (is_authenticated() and PAPER_TRADING) else \
                "CONNECTED (LIVE)" if (is_authenticated() and not PAPER_TRADING) else \
                "NOT CONNECTED"

    context = f"""
CURRENT TIME (ET): {now_et.strftime('%A, %B %d, %Y %I:%M %p ET')}
DAY OF WEEK: {now_et.strftime('%A')}
SESSION TRADE COUNT TODAY: {session['trade_count']}
CONSECUTIVE LOSSES TODAY: {session['consecutive_losses']}
CIRCUIT BREAKER ACTIVE: {session['circuit_breaker']}
TASTYTRADE STATUS: {tt_status}

INCOMING ALERT DATA:
{json.dumps(alert_data, indent=2)}

ANALYSIS TYPE REQUESTED: {analysis_type}

Based on my complete trading playbook and all rules in your system prompt:
1. Run the full 5-category pre-flight checklist against this alert
2. Determine if this is a valid trade signal, watchlist note, or no-trade
3. If valid: format the exact Discord message in Junior's voice
4. If not valid: explain briefly why

IMPORTANT: Start your response with exactly one of these tags on the first line:
- TRADE_VALID: (if this should be executed)
- NO_TRADE: (if this should be skipped)
- WATCHLIST: (if this is a watchlist update only)

Then on the next lines, write the Discord message exactly as Junior would post it.
"""

    response = anthropic.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        system=system_prompt,
        messages=[{"role": "user", "content": context}]
    )
    return response.content[0].text

# ── Route alert to correct Discord channel ────────────────────────────────────
def route_to_channel(alert_type: str, analysis_type: str) -> str:
    if analysis_type == "WATCHLIST":
        return CHANNEL_WATCHLIST
    elif analysis_type == "SWING_SIGNAL":
        return CHANNEL_SWING_SIGNALS
    elif analysis_type == "RECAP":
        return CHANNEL_RECAPS
    elif analysis_type == "LONGTERM":
        return CHANNEL_LONGTERM
    else:
        return CHANNEL_DAY_SIGNALS

# ── Determine analysis type from alert ───────────────────────────────────────
def determine_analysis_type(alert_data: dict) -> str:
    alert_type = alert_data.get("alert_type", "")
    if "WATCHLIST" in alert_type:
        return "WATCHLIST"
    elif in_swing_window():
        return "SWING_SIGNAL"
    elif "RECAP" in alert_type or "CLOSE" in alert_type:
        return "RECAP"
    elif in_day_trade_window():
        return "DAY_SIGNAL"
    else:
        return "NO_TRADE_WINDOW"

# ── Pre-flight gate ───────────────────────────────────────────────────────────
def pre_flight_gate(alert_data: dict) -> tuple[bool, str]:
    reset_session_if_new_day()
    ticker = alert_data.get("ticker", "").upper()

    if session["circuit_breaker"]:
        return False, "CIRCUIT_BREAKER: Two consecutive losses — system shut down"
    if session["kill_switch_active"]:
        return False, f"KILL_SWITCH: {session['kill_switch_reason']}"
    if session["trade_count"] >= 3:
        return False, "TRADE_CAP: Maximum 3 trades reached for today"
    if in_dead_zone():
        return False, "DEAD_ZONE: No trades between 11:00 AM – 3:00 PM ET"

    analysis_type = determine_analysis_type(alert_data)
    if analysis_type == "NO_TRADE_WINDOW":
        return False, "OUT_OF_WINDOW: Alert received outside trading windows"
    if analysis_type == "DAY_SIGNAL" and ticker not in DAY_TRADE_TICKERS:
        return False, f"INVALID_TICKER: {ticker} not on approved day-trade list"

    return True, "PASS"

# ── Execute trade via Tastytrade ──────────────────────────────────────────────
def execute_trade(alert_data: dict, direction: str, analysis_type: str) -> dict | None:
    """Find contract and place order via Tastytrade API."""
    if not is_authenticated():
        log.warning("Tastytrade not authenticated — signal posted to Discord only, no execution")
        return None

    ticker = alert_data.get("ticker", "").upper()
    trade_type = "DAY" if analysis_type == "DAY_SIGNAL" else "SWING"

    contract = find_option_contract(ticker, direction, trade_type)
    if not contract:
        log.warning(f"No suitable contract found for {ticker} {direction}")
        post_discord(CHANNEL_RECAPS,
            f"⚠️ Signal identified for {ticker} {direction} but no contract met our criteria "
            f"(budget or spread requirements). No order placed.")
        return None

    # Place the order — 1 contract default (members size to their own account)
    order = place_order(contract, quantity=1)
    if not order:
        log.error("Order placement failed")
        return None

    # Track the position
    active_positions[contract["symbol"]] = {
        "order_id": order["order_id"],
        "entry_price": contract["ask"],
        "quantity": 1,
        "ticker": ticker,
        "direction": direction,
        "opened_at": datetime.now(ET).isoformat(),
        "paper": PAPER_TRADING,
    }

    # Post fill confirmation to #profits-and-recaps
    mode_tag = "📋 PAPER TRADE" if PAPER_TRADING else "✅ ORDER FILLED"
    fill_msg = (
        f"{mode_tag} @everyone\n"
        f"Mine filled at {contract['ask']:.2f}\n"
        f"Contract: {contract['symbol']}\n"
        f"Spread: {contract['spread_pct']}%"
    )
    post_discord(CHANNEL_RECAPS, fill_msg)

    return order

# ── OAUTH ENDPOINTS ───────────────────────────────────────────────────────────

@app.route("/oauth/start")
def oauth_start():
    """Redirect to Tastytrade OAuth login page."""
    auth_url = (
        f"{BASE_URL}/oauth2/authorize"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&response_type=code"
        f"&scope=read+trade+openid"
    )
    return redirect(auth_url)

@app.route("/oauth/callback")
def oauth_callback():
    """Handle OAuth callback and exchange code for tokens."""
    code = request.args.get("code")
    error = request.args.get("error")

    if error:
        log.error(f"OAuth error: {error}")
        return jsonify({"error": error}), 400

    if not code:
        return jsonify({"error": "No authorization code received"}), 400

    # Exchange code for tokens
    try:
        resp = requests.post(
            f"{BASE_URL}/oauth2/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "redirect_uri": REDIRECT_URI,
            }
        )

        if resp.status_code == 200:
            data = resp.json()
            save_tokens(
                data["access_token"],
                data.get("refresh_token", ""),
                data.get("expires_in", 3600)
            )
            mode = "PAPER TRADING" if PAPER_TRADING else "LIVE TRADING"
            log.info(f"Tastytrade authenticated successfully — {mode}")
            return jsonify({
                "status": "authenticated",
                "mode": mode,
                "message": f"Tastytrade connected in {mode} mode. The system is now fully live."
            }), 200
        else:
            log.error(f"Token exchange failed: {resp.text}")
            return jsonify({"error": "Token exchange failed", "details": resp.text}), 400

    except Exception as e:
        log.error(f"OAuth callback error: {e}")
        return jsonify({"error": str(e)}), 500

# ── MAIN WEBHOOK ENDPOINT ─────────────────────────────────────────────────────
@app.route("/webhook", methods=["POST"])
def webhook():
    reset_session_if_new_day()

    try:
        data = request.get_json(force=True)
    except Exception as e:
        return jsonify({"error": "Invalid JSON"}), 400

    if not data:
        return jsonify({"error": "Empty payload"}), 400

    log.info(f"Webhook received: {json.dumps(data)}")

    # Verify secret
    incoming_secret = data.get("secret", "")
    if not hmac.compare_digest(incoming_secret, WEBHOOK_SECRET):
        return jsonify({"error": "Unauthorized"}), 401

    # Pre-flight gate
    can_proceed, gate_reason = pre_flight_gate(data)
    if not can_proceed:
        log.info(f"Pre-flight blocked: {gate_reason}")
        return jsonify({"status": "blocked", "reason": gate_reason}), 200

    analysis_type = determine_analysis_type(data)

    # Call Claude
    try:
        claude_response = analyze_with_claude(data, analysis_type)
        log.info(f"Claude response: {claude_response[:200]}...")
    except Exception as e:
        log.error(f"Claude API error: {e}")
        return jsonify({"error": "Claude API failed"}), 500

    # Parse Claude's decision
    first_line = claude_response.split("\n")[0].strip().upper()
    discord_message = "\n".join(claude_response.split("\n")[1:]).strip()

    if first_line.startswith("NO_TRADE"):
        log.info("Claude determined: no trade")
        return jsonify({"status": "no_trade"}), 200

    if first_line.startswith("WATCHLIST"):
        post_discord(CHANNEL_WATCHLIST, discord_message)
        return jsonify({"status": "watchlist_posted"}), 200

    if first_line.startswith("TRADE_VALID"):
        # Post signal to Discord first
        channel_id = route_to_channel(data.get("alert_type", ""), analysis_type)
        post_discord(channel_id, discord_message)

        # Execute via Tastytrade
        direction = data.get("direction", "CALL")
        if direction in ("CALL", "PUT"):
            order = execute_trade(data, direction, analysis_type)
            if order:
                session["trade_count"] += 1
                log.info(f"Trade executed: {order}")
                return jsonify({"status": "trade_executed", "order": order}), 200

        return jsonify({"status": "signal_posted", "execution": "skipped"}), 200

    # Fallback — post whatever Claude returned
    channel_id = route_to_channel(data.get("alert_type", ""), analysis_type)
    post_discord(channel_id, claude_response)
    return jsonify({"status": "posted"}), 200

# ── KILL SWITCH ───────────────────────────────────────────────────────────────
@app.route("/kill", methods=["POST"])
def kill_switch():
    data = request.get_json(force=True)
    if data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401
    action = data.get("action", "activate")
    reason = data.get("reason", "Manual override")
    if action == "activate":
        session["kill_switch_active"] = True
        session["kill_switch_reason"] = reason
        return jsonify({"status": "kill_switch_active", "reason": reason}), 200
    else:
        session["kill_switch_active"] = False
        session["kill_switch_reason"] = None
        return jsonify({"status": "kill_switch_deactivated"}), 200

# ── LOSS / WIN TRACKING ───────────────────────────────────────────────────────
@app.route("/loss", methods=["POST"])
def log_loss():
    data = request.get_json(force=True)
    if data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401
    reset_session_if_new_day()
    session["consecutive_losses"] += 1
    if session["consecutive_losses"] >= 2:
        session["circuit_breaker"] = True
        msg = "🚨 TWO CONSECUTIVE LOSSES — System shutting down for the day. Capital protection mode active. See you tomorrow. @everyone"
        post_discord(CHANNEL_RECAPS, msg)
        return jsonify({"status": "circuit_breaker_triggered"}), 200
    return jsonify({"status": "loss_logged", "consecutive": session["consecutive_losses"]}), 200

@app.route("/win", methods=["POST"])
def log_win():
    data = request.get_json(force=True)
    if data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401
    reset_session_if_new_day()
    session["consecutive_losses"] = 0
    return jsonify({"status": "win_logged"}), 200

# ── POSITIONS ─────────────────────────────────────────────────────────────────
@app.route("/positions", methods=["GET"])
def positions():
    return jsonify({
        "active_positions": active_positions,
        "tastytrade_positions": get_positions(),
        "account_balance": get_account_balance(),
        "paper_trading": PAPER_TRADING,
    }), 200

# ── HEALTH CHECK ──────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    reset_session_if_new_day()
    now_et = datetime.now(ET)
    return jsonify({
        "status": "online",
        "time_et": now_et.strftime("%Y-%m-%d %H:%M:%S ET"),
        "in_day_trade_window": in_day_trade_window(),
        "in_swing_window": in_swing_window(),
        "paper_trading": PAPER_TRADING,
        "tastytrade_authenticated": is_authenticated(),
        "session": {
            "date": str(session["date"]),
            "trade_count": session["trade_count"],
            "consecutive_losses": session["consecutive_losses"],
            "circuit_breaker": session["circuit_breaker"],
            "kill_switch_active": session["kill_switch_active"],
        }
    }), 200

@app.route("/status", methods=["GET"])
def status():
    return health()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
