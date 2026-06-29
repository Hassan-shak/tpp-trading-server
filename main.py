"""
The Portfolio Plug — AI Trading Webhook Server
Receives TradingView alerts → calls Claude API → posts signals to Discord
"""

import os
import json
import hmac
import logging
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import requests
from flask import Flask, request, jsonify
from anthropic import Anthropic

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ── Clients ───────────────────────────────────────────────────────────────────
anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ── Config from environment ───────────────────────────────────────────────────
WEBHOOK_SECRET      = os.environ["WEBHOOK_SECRET"]          # TPP_WEBHOOK_2024
DISCORD_BOT_TOKEN   = os.environ["DISCORD_BOT_TOKEN"]
DISCORD_GUILD_ID    = os.environ["DISCORD_GUILD_ID"]

# Discord channel IDs (set these after creating the bot)
CHANNEL_WATCHLIST   = os.environ["DISCORD_CHANNEL_WATCHLIST"]       # #daily-watchlist
CHANNEL_DAY_SIGNALS = os.environ["DISCORD_CHANNEL_DAY_SIGNALS"]     # #day-trade-signals
CHANNEL_SWING_SIGNALS = os.environ["DISCORD_CHANNEL_SWING_SIGNALS"] # #swing-trade-signals
CHANNEL_RECAPS      = os.environ["DISCORD_CHANNEL_RECAPS"]          # #profits-and-recaps
CHANNEL_LONGTERM    = os.environ["DISCORD_CHANNEL_LONGTERM"]        # #long-term-stock-investing

ET = ZoneInfo("America/New_York")

# ── Approved tickers ──────────────────────────────────────────────────────────
DAY_TRADE_TICKERS  = {"SPY", "QQQ", "TSLA", "NVDA", "AMZN", "MSFT"}
SWING_TICKERS      = {"SPY", "QQQ", "TSLA", "NVDA", "AMZN", "MSFT", "META", "GOOG"}

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
    """Post a message to a Discord channel via bot token."""
    url = f"{DISCORD_API}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "Content-Type": "application/json",
    }
    # Discord has a 2000 char limit — split if needed
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
    """
    Send alert data to Claude with the full system prompt.
    Returns Claude's formatted response for Discord.
    """
    system_prompt = get_system_prompt()

    now_et = datetime.now(ET)
    context = f"""
CURRENT TIME (ET): {now_et.strftime('%A, %B %d, %Y %I:%M %p ET')}
DAY OF WEEK: {now_et.strftime('%A')}
SESSION TRADE COUNT TODAY: {session['trade_count']}
CONSECUTIVE LOSSES TODAY: {session['consecutive_losses']}
CIRCUIT BREAKER ACTIVE: {session['circuit_breaker']}

INCOMING ALERT DATA:
{json.dumps(alert_data, indent=2)}

ANALYSIS TYPE REQUESTED: {analysis_type}

Based on my complete trading playbook and all rules in your system prompt:
1. Run the full 5-category pre-flight checklist against this alert
2. Determine if this is a valid trade signal, watchlist note, or no-trade
3. If valid: format the exact Discord message in Junior's voice
4. If not valid: explain briefly why (for internal logging — do NOT post this to Discord unless it adds value to members)

For a valid trade signal, format EXACTLY as Junior posts:
- Use the exact signal templates from the playbook
- Include @everyone on every signal
- Keep it clean, professional, educational
- No profanity ever
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
    now = datetime.now(ET).time()

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
    """
    Fast server-side checks before even calling Claude.
    Returns (can_proceed, reason_if_not).
    """
    reset_session_if_new_day()

    ticker = alert_data.get("ticker", "").upper()

    # Circuit breaker
    if session["circuit_breaker"]:
        return False, "CIRCUIT_BREAKER: Two consecutive losses — system shut down for the day"

    # Kill switch
    if session["kill_switch_active"]:
        return False, f"KILL_SWITCH: {session['kill_switch_reason']}"

    # Trade cap
    if session["trade_count"] >= 3:
        return False, "TRADE_CAP: Maximum 3 trades reached for today"

    # Dead zone
    if in_dead_zone():
        return False, "DEAD_ZONE: No trades between 11:00 AM – 3:00 PM ET"

    # Time window
    analysis_type = determine_analysis_type(alert_data)
    if analysis_type == "NO_TRADE_WINDOW":
        return False, f"OUT_OF_WINDOW: Alert received outside trading windows"

    # Ticker check
    if analysis_type in ("DAY_SIGNAL",) and ticker not in DAY_TRADE_TICKERS:
        return False, f"INVALID_TICKER: {ticker} not on approved day-trade list"

    return True, "PASS"

# ── Main webhook endpoint ─────────────────────────────────────────────────────
@app.route("/webhook", methods=["POST"])
def webhook():
    reset_session_if_new_day()

    # ── 1. Parse body ─────────────────────────────────────────────────────────
    try:
        data = request.get_json(force=True)
    except Exception as e:
        log.error(f"Failed to parse JSON: {e}")
        return jsonify({"error": "Invalid JSON"}), 400

    if not data:
        return jsonify({"error": "Empty payload"}), 400

    log.info(f"Webhook received: {json.dumps(data)}")

    # ── 2. Verify secret ──────────────────────────────────────────────────────
    incoming_secret = data.get("secret", "")
    if not hmac.compare_digest(incoming_secret, WEBHOOK_SECRET):
        log.warning("Invalid webhook secret — request rejected")
        return jsonify({"error": "Unauthorized"}), 401

    # ── 3. Pre-flight gate ────────────────────────────────────────────────────
    can_proceed, gate_reason = pre_flight_gate(data)
    if not can_proceed:
        log.info(f"Pre-flight blocked: {gate_reason}")
        return jsonify({"status": "blocked", "reason": gate_reason}), 200

    # ── 4. Determine analysis type ────────────────────────────────────────────
    analysis_type = determine_analysis_type(data)

    # ── 5. Call Claude ────────────────────────────────────────────────────────
    try:
        claude_response = analyze_with_claude(data, analysis_type)
        log.info(f"Claude response: {claude_response[:200]}...")
    except Exception as e:
        log.error(f"Claude API error: {e}")
        return jsonify({"error": "Claude API failed"}), 500

    # ── 6. Check if Claude says to trade or skip ──────────────────────────────
    # Claude will start its response with TRADE_VALID or NO_TRADE if we instruct it to
    # For now we post everything Claude returns that isn't a no-trade
    if "NO_TRADE" in claude_response.upper() and "SKIP" in claude_response.upper():
        log.info("Claude determined: no trade — not posting to Discord")
        return jsonify({"status": "no_trade", "reason": claude_response[:200]}), 200

    # ── 7. Route to Discord ───────────────────────────────────────────────────
    channel_id = route_to_channel(data.get("alert_type", ""), analysis_type)
    posted = post_discord(channel_id, claude_response)

    if posted:
        log.info(f"Posted to Discord channel {channel_id}")
        # Increment trade count on valid day trade signals
        if analysis_type == "DAY_SIGNAL":
            session["trade_count"] += 1
        return jsonify({"status": "success", "channel": channel_id}), 200
    else:
        return jsonify({"error": "Discord post failed"}), 500

# ── Kill switch endpoint (manual override) ────────────────────────────────────
@app.route("/kill", methods=["POST"])
def kill_switch():
    """Manually activate or deactivate the kill switch."""
    data = request.get_json(force=True)
    if data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    action = data.get("action", "activate")
    reason = data.get("reason", "Manual override")

    if action == "activate":
        session["kill_switch_active"] = True
        session["kill_switch_reason"] = reason
        log.warning(f"Kill switch ACTIVATED: {reason}")
        return jsonify({"status": "kill_switch_active", "reason": reason}), 200
    else:
        session["kill_switch_active"] = False
        session["kill_switch_reason"] = None
        log.info("Kill switch DEACTIVATED")
        return jsonify({"status": "kill_switch_deactivated"}), 200

# ── Circuit breaker endpoint ──────────────────────────────────────────────────
@app.route("/loss", methods=["POST"])
def log_loss():
    """Call this when a trade closes at a loss to track the circuit breaker."""
    data = request.get_json(force=True)
    if data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    reset_session_if_new_day()
    session["consecutive_losses"] += 1

    if session["consecutive_losses"] >= 2:
        session["circuit_breaker"] = True
        msg = "🚨 TWO CONSECUTIVE LOSSES — System shutting down for the day. Capital protection mode active. See you tomorrow. @everyone"
        post_discord(CHANNEL_RECAPS, msg)
        log.warning("CIRCUIT BREAKER TRIGGERED — system shut down")
        return jsonify({"status": "circuit_breaker_triggered"}), 200

    return jsonify({"status": "loss_logged", "consecutive": session["consecutive_losses"]}), 200

@app.route("/win", methods=["POST"])
def log_win():
    """Call this when a trade closes as a win — resets consecutive loss counter."""
    data = request.get_json(force=True)
    if data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    reset_session_if_new_day()
    session["consecutive_losses"] = 0
    return jsonify({"status": "win_logged", "consecutive_losses_reset": True}), 200

# ── Health check ──────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    reset_session_if_new_day()
    now_et = datetime.now(ET)
    return jsonify({
        "status": "online",
        "time_et": now_et.strftime("%Y-%m-%d %H:%M:%S ET"),
        "in_day_trade_window": in_day_trade_window(),
        "in_swing_window": in_swing_window(),
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
