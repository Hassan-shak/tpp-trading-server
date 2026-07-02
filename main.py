"""
The Portfolio Plug — AI Trading Webhook Server
Receives TradingView alerts → calls Claude API → posts signals to Discord
Phase 2: Tastytrade API execution layer integrated
"""

import os
import json
import hmac
import logging
import threading
import time as time_module
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import requests
from flask import Flask, request, jsonify
from anthropic import Anthropic
from tastytrade_executor import (
    is_authenticated, find_option_contract,
    place_order, close_position, get_positions, get_account_balance,
    BASE_URL, PAPER_TRADING
)
import volume_profile
import alpaca_stream
import alpaca_executor

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

app = Flask(__name__)
log.info(f"📦 Module main.py imported — process PID {os.getpid()}")

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

DISCORD_POSTING_ENABLED = os.environ.get("DISCORD_POSTING_ENABLED", "true").lower() == "true"

def post_discord(channel_id: str, message: str) -> bool:
    if not DISCORD_POSTING_ENABLED:
        log.info(f"Discord posting disabled — suppressed message to channel {channel_id}")
        return False
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

    ticker = alert_data.get("ticker", "").upper()
    zones = volume_profile.get_zones(ticker)
    if zones:
        zone_text = f"""
VOLUME PROFILE ZONES FOR {ticker} (auto-calculated, last updated {zones.get('updated_at', 'unknown')}):
1H — Point of Control: {zones.get('1H', {}).get('poc', 'N/A')} | Demand Zone: {zones.get('1H', {}).get('demand', 'N/A')} | Supply Zone: {zones.get('1H', {}).get('supply', 'N/A')}
4H — Point of Control: {zones.get('4H', {}).get('poc', 'N/A')} | Demand Zone: {zones.get('4H', {}).get('demand', 'N/A')} | Supply Zone: {zones.get('4H', {}).get('supply', 'N/A')}
"""
    else:
        zone_text = f"\nVOLUME PROFILE ZONES FOR {ticker}: NOT AVAILABLE this session — treat any zone-dependent confirmation (Rules 1, 2, 5, 6) as UNCONFIRMED.\n"

    context = f"""
CURRENT TIME (ET): {now_et.strftime('%A, %B %d, %Y %I:%M %p ET')}
DAY OF WEEK: {now_et.strftime('%A')}
SESSION TRADE COUNT TODAY: {session['trade_count']}
CONSECUTIVE LOSSES TODAY: {session['consecutive_losses']}
CIRCUIT BREAKER ACTIVE: {session['circuit_breaker']}
TASTYTRADE STATUS: {tt_status}
{zone_text}
INCOMING ALERT DATA:
{json.dumps(alert_data, indent=2)}

ANALYSIS TYPE REQUESTED: {analysis_type}

Based on my complete trading playbook and all rules in your system prompt:
1. Run the full 5-category pre-flight checklist against this alert
2. Check the alert against the 3-Screen Confluence System (Rules 1-6) — use the Volume Profile zone data above for any rule requiring zone alignment
3. Determine if this is a valid trade signal, watchlist note, or no-trade
4. If valid: format the exact Discord message in Junior's voice
5. If not valid: explain briefly why

IMPORTANT: Start your response with exactly one of these tags on the first line:
- TRADE_VALID: (if this should be executed)
- NO_TRADE: (if this should be skipped)
- WATCHLIST: (if this is a watchlist update only)

Then on the next lines, write the Discord message exactly as Junior would post it.
"""

    response = anthropic.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        system=[
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ],
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
    """Find contract and place paper order via Alpaca."""
    ticker        = alert_data.get("ticker", "").upper()
    current_price = alpaca_stream.get_latest_price(ticker) or float(alert_data.get("price", 0))

    if not alpaca_executor.is_available():
        log.warning("Alpaca paper trading unavailable — signal posted to Discord only")
        return None

    contract = alpaca_executor.find_option_contract(ticker, direction, current_price)
    if not contract:
        log.warning(f"No suitable Alpaca contract found for {ticker} {direction}")
        post_discord(CHANNEL_RECAPS,
            f"⚠️ Signal identified for {ticker} {direction} but no contract met criteria "
            f"($0.75-$1.50, spread <5%). No order placed.")
        return None

    order = alpaca_executor.place_order(contract, quantity=1)
    if not order:
        log.error("Alpaca order placement failed")
        return None

    active_positions[contract["symbol"]] = {
        "order_id": order.get("id"),
        "entry_price": contract["ask"],
        "quantity": 1,
        "ticker": ticker,
        "direction": direction,
        "opened_at": datetime.now(ET).isoformat(),
        "paper": True,
        "broker": "alpaca",
    }

    fill_msg = (
        f"📋 PAPER TRADE @everyone\n"
        f"Filled at ${contract['ask']:.2f}\n"
        f"Contract: {contract['symbol']}\n"
        f"Strike: {contract['strike']} | Expiry: {contract['expiry']}\n"
        f"Spread: {contract['spread_pct']}%"
    )
    post_discord(CHANNEL_RECAPS, fill_msg)
    log.info(f"✅ Alpaca paper trade executed: {contract['symbol']} @ {contract['ask']}")
    return order



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

# ── VOLUME PROFILE ZONES ───────────────────────────────────────────────────────
@app.route("/zones", methods=["GET"])
def zones():
    """Debug endpoint — view current auto-calculated Volume Profile zones for all tickers."""
    return jsonify({
        "zones": volume_profile.get_all_zones(),
        "tickers_tracked": list(SWING_TICKERS),
        "worker_pid": os.getpid(),
        "scheduler_started": _scheduler_started,
    }), 200

@app.route("/zones/refresh", methods=["POST"])
def refresh_zones():
    """Manually trigger an immediate zone recalculation (also runs automatically every morning)."""
    data = request.get_json(force=True, silent=True) or {}
    if data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401
    results = volume_profile.update_all_zones(list(SWING_TICKERS))
    return jsonify({"status": "refreshed", "results": results}), 200

# ── DAILY ZONE SCHEDULER (runs automatically, no manual input required) ───────
_scheduler_started = False
_scheduler_lock = threading.Lock()

# ── AUTOMATED JOB 1: Daily Watchlist ─────────────────────────────────────────
def fetch_premarket_data(ticker: str) -> dict:
    """Pull premarket price, gap, and volume data from Alpaca real-time stream."""
    return alpaca_stream.get_premarket_data(ticker)

def post_daily_watchlist():
    """Generate and post the morning watchlist to #daily-watchlist."""
    log.info("📋 Generating daily watchlist...")
    zones   = volume_profile.get_all_zones()
    tickers = list(DAY_TRADE_TICKERS)

    # Fetch premarket data for all tickers
    market_data = []
    for t in tickers:
        data = fetch_premarket_data(t)
        if data:
            z = zones.get(t, {})
            data["demand_1h"] = z.get("1H", {}).get("demand")
            data["supply_1h"] = z.get("1H", {}).get("supply")
            data["poc_1h"]    = z.get("1H", {}).get("poc")
            market_data.append(data)

    if not market_data:
        log.warning("No pre-market data available for watchlist")
        return

    # Ask Claude to write the watchlist in Junior's voice
    prompt = f"""It is {datetime.now(ET).strftime('%A, %B %d, %Y')} — pre-market. You are Junior from The Portfolio Plug.

Write the daily morning watchlist post for #daily-watchlist on Discord. Use your real voice — direct, confident, educational.

PRE-MARKET DATA:
{json.dumps(market_data, indent=2)}

VOLUME PROFILE ZONES (1H demand/supply for reference):
{json.dumps({t: zones.get(t, {}).get("1H") for t in tickers if zones.get(t)}, indent=2)}

Your watchlist post must include:
1. A short market context read (1-2 sentences on overall tone/direction today)
2. For each ticker: current price, gap %, what level to watch (demand/supply zone or PMH/PML), and ONE sentence on what you're looking for
3. Key time to watch: 9:30-10:00 AM window
4. A closing line reminding members of the rules (1 trade, volume confirmation, no chasing)

Keep it tight — members read this on their phone before market open. No fluff."""

    try:
        resp = anthropic.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        watchlist_msg = resp.content[0].text
        post_discord(CHANNEL_WATCHLIST, watchlist_msg)
        log.info("✅ Daily watchlist posted to Discord")
    except Exception as e:
        log.error(f"Watchlist generation error: {e}", exc_info=True)


# ── AUTOMATED JOB 2: Pattern Scanner (every 5 min during trade window) ────────
_pattern_scan_cooldown = {}  # ticker -> last signal time, prevents spam

def scan_for_visual_patterns():
    """
    Fetches recent candle data for all tickers and asks Claude to identify
    confluence setups forming — EMA approaches, pivot proximity, flag patterns,
    divergence conditions. Posts valid setups to day-trade-signals as WATCHLIST alerts.
    """
    now_et = datetime.now(ET)
    if not (dtime(9, 25) <= now_et.time() <= dtime(10, 30)):
        return  # Only scan during the trade window

    log.info("🔍 Running pattern scan...")
    zones = volume_profile.get_all_zones()

    for ticker in DAY_TRADE_TICKERS:
        # Cooldown: don't re-scan same ticker within 10 minutes
        last = _pattern_scan_cooldown.get(ticker)
        if last and (now_et - last).total_seconds() < 600:
            continue

        try:
            candles = alpaca_stream.get_candles(ticker, limit=30)
            if len(candles) < 10:
                continue

            zone_data = zones.get(ticker, {})
            prompt = f"""You are Junior's AI trading system. Analyze this 1-minute candle data for {ticker} and determine if any high-probability setup from the 3-Screen Confluence System is currently forming or has just triggered.

LAST 30 CANDLES (most recent last):
{json.dumps(candles[-20:], indent=2)}

VOLUME PROFILE ZONES:
1H: {zone_data.get('1H')}
4H: {zone_data.get('4H')}

Current time: {now_et.strftime('%I:%M %p ET')}

Check for:
1. Price approaching or bouncing off 8 EMA or 21 EMA (calculate approximate EMAs from the candle data)
2. Price near a Volume Profile demand or supply zone listed above
3. Flag/consolidation pattern forming after a strong move
4. RSI divergence conditions (price making lower lows while momentum improving, or vice versa)
5. PMH/PML proximity (highest high and lowest low of the first 15 candles = premarket levels)

Respond with EXACTLY one of:
- "NO_SETUP: [brief reason]" if nothing significant is forming
- "WATCHLIST: [ticker] [direction] — [1 sentence describing the setup and what to watch for]"

Be conservative. Only flag genuinely high-probability developing setups, not noise."""

            scan_resp = anthropic.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}]
            )
            result_text = scan_resp.content[0].text.strip()
            log.info(f"Pattern scan {ticker}: {result_text[:80]}")

            if result_text.startswith("WATCHLIST:"):
                msg = f"👀 **Pattern Alert** — {result_text.replace('WATCHLIST: ', '')}\n\n_Developing setup — confirm your confirmations before entering. Not a trade signal._"
                post_discord(CHANNEL_DAY_SIGNALS, msg)
                _pattern_scan_cooldown[ticker] = now_et
                log.info(f"✅ Pattern alert posted for {ticker}")

        except Exception as e:
            log.error(f"Pattern scan error for {ticker}: {e}")


# ── AUTOMATED JOB 3: End-of-Day Recap ────────────────────────────────────────
def post_eod_recap():
    """Generate and post an end-of-day recap to #profits-and-recaps."""
    log.info("📊 Generating end-of-day recap...")

    # Pull positions and balance from Tastytrade
    positions = []
    balance   = {}
    try:
        positions = get_positions()
        balance   = get_account_balance()
    except Exception as e:
        log.error(f"Could not fetch Tastytrade data for recap: {e}")

    # Pull today's session stats
    trade_count       = session.get("trade_count", 0)
    consecutive_loss  = session.get("consecutive_losses", 0)
    circuit_breaker   = session.get("circuit_breaker", False)

    prompt = f"""It is {datetime.now(ET).strftime('%A, %B %d, %Y')} — market just closed. You are Junior from The Portfolio Plug.

Write the end-of-day recap post for #profits-and-recaps. Keep it real, educational, and in your voice.

TODAY'S SESSION STATS:
- Trades taken: {trade_count}
- Consecutive losses at close: {consecutive_loss}
- Circuit breaker triggered today: {circuit_breaker}
- Open positions: {len(positions)}
- Account balance data: {json.dumps(balance, indent=2) if balance else 'Not available'}

OPEN POSITIONS:
{json.dumps(positions, indent=2) if positions else 'None — flat going into tomorrow'}

Your recap must include:
1. One honest sentence about how today went overall
2. If trades were taken: what the setup was, what worked or didn't
3. If no trades: why the system stayed in cash (dead zone, no confirmation, etc.) — frame it as discipline, not failure
4. Open positions if any: what you're holding and why
5. What to watch for tomorrow (1-2 things)
6. A closing line for the community

Keep it under 300 words. No fluff — members respect honesty over hype."""

    try:
        resp = anthropic.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        recap_msg = resp.content[0].text
        post_discord(CHANNEL_RECAPS, recap_msg)
        log.info("✅ End-of-day recap posted to Discord")
    except Exception as e:
        log.error(f"EOD recap generation error: {e}", exc_info=True)


# ── MASTER SCHEDULER LOOP ─────────────────────────────────────────────────────
def zone_scheduler_loop():
    """
    Master background thread handling ALL automated daily jobs:
    1. Volume Profile zone calculation (8:00 AM ET daily)
    2. Daily watchlist post (8:30 AM ET daily)
    3. Pattern scanner (every 5 min, 9:25-10:30 AM ET)
    4. End-of-day recap (4:01 PM ET daily)
    """
    SCHEDULER_STATE_FILE = "/tmp/tpp_scheduler_state.json"

    def load_scheduler_state() -> dict:
        try:
            if os.path.exists(SCHEDULER_STATE_FILE):
                with open(SCHEDULER_STATE_FILE, "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def save_scheduler_state(state: dict):
        try:
            with open(SCHEDULER_STATE_FILE, "w") as f:
                json.dump(state, f)
        except Exception as e:
            log.error(f"Failed to save scheduler state: {e}")

    # Load persisted state so restarts/redeploys don't re-fire jobs already done today
    _state = load_scheduler_state()
    today_str = datetime.now(ET).date().isoformat()

    last_zone_date      = datetime.fromisoformat(_state["zone_date"]).date() if _state.get("zone_date") else None
    last_watchlist_date = datetime.fromisoformat(_state["watchlist_date"]).date() if _state.get("watchlist_date") else None
    last_recap_date     = datetime.fromisoformat(_state["recap_date"]).date() if _state.get("recap_date") else None

    log.info(f"📅 Scheduler state loaded — zones: {last_zone_date}, watchlist: {last_watchlist_date}, recap: {last_recap_date}")

    # Start Alpaca real-time stream first — backfills history then opens WebSocket
    try:
        alpaca_stream.start()
        log.info("✅ Alpaca real-time stream started")
        # Give it a moment to backfill before calculating zones
        time_module.sleep(5)
    except Exception as e:
        log.error(f"Alpaca stream start failed: {e}", exc_info=True)

    # Run zone calculation immediately on startup
    try:
        log.info("🔄 Running initial Volume Profile zone calculation on startup...")
        results = volume_profile.update_all_zones(list(SWING_TICKERS))
        log.info(f"🔄 Initial zone calculation results: {results}")
        log.info(f"🔄 Zone store now contains: {list(volume_profile.get_all_zones().keys())}")
        last_zone_date = datetime.now(ET).date()
    except Exception as e:
        log.error(f"Initial zone calculation failed: {e}", exc_info=True)

    while True:
        try:
            now_et = datetime.now(ET)
            today  = now_et.date()
            t      = now_et.time()

            # Skip market-facing jobs on weekends (Sat=5, Sun=6)
            is_weekend = now_et.weekday() >= 5

            # JOB 1: Volume Profile zones — 8:00 AM ET (weekdays only)
            if not is_weekend and t >= dtime(8, 0) and today != last_zone_date:
                log.info("🔄 Running daily Volume Profile zone calculation...")
                volume_profile.update_all_zones(list(SWING_TICKERS))
                last_zone_date = today
                save_scheduler_state({"zone_date": today.isoformat(), "watchlist_date": last_watchlist_date.isoformat() if last_watchlist_date else None, "recap_date": last_recap_date.isoformat() if last_recap_date else None})
                log.info("✅ Daily Volume Profile zones updated automatically.")

            # JOB 2: Daily watchlist — 8:30 AM ET (weekdays only)
            if not is_weekend and t >= dtime(8, 30) and today != last_watchlist_date:
                post_daily_watchlist()
                last_watchlist_date = today
                save_scheduler_state({"zone_date": last_zone_date.isoformat() if last_zone_date else None, "watchlist_date": today.isoformat(), "recap_date": last_recap_date.isoformat() if last_recap_date else None})

            # JOB 3: Pattern scanner — every loop tick during trade window (weekdays only)
            if not is_weekend and dtime(9, 25) <= t <= dtime(10, 30):
                scan_for_visual_patterns()

            # JOB 4: End-of-day recap — 4:01 PM ET (weekdays only)
            if not is_weekend and t >= dtime(16, 1) and today != last_recap_date:
                post_eod_recap()
                last_recap_date = today
                save_scheduler_state({"zone_date": last_zone_date.isoformat() if last_zone_date else None, "watchlist_date": last_watchlist_date.isoformat() if last_watchlist_date else None, "recap_date": today.isoformat()})

        except Exception as e:
            log.error(f"Master scheduler error: {e}", exc_info=True)

        time_module.sleep(300)  # check every 5 minutes

def ensure_scheduler_started():
    """
    Idempotent scheduler starter — guaranteed to run exactly once, inside the
    actual gunicorn worker process that's serving requests (not at module import
    time, which can run in a transient pre-fork process gunicorn discards).
    """
    global _scheduler_started
    with _scheduler_lock:
        if not _scheduler_started:
            _scheduler_started = True
            log.info(f"🚀 Starting zone scheduler in worker PID {os.getpid()}")
            thread = threading.Thread(target=zone_scheduler_loop, daemon=True)
            thread.start()

@app.before_request
def _start_scheduler_on_first_request():
    """Guarantees the scheduler thread is running in the actual serving process."""
    if not _scheduler_started:
        ensure_scheduler_started()

# Also attempt immediate start for cases where gunicorn doesn't go through before_request
# (e.g. direct `python main.py` runs) — ensure_scheduler_started() is idempotent and safe to call twice
ensure_scheduler_started()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
