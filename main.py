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
    wait_for_fill, BASE_URL, PAPER_TRADING
)
import volume_profile
import alpaca_stream
import position_manager
# alpaca_executor kept for data streaming only (not execution)
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
WEBHOOK_SECRET      = os.environ["WEBHOOK_SECRET"]
DISCORD_BOT_TOKEN   = os.environ["DISCORD_BOT_TOKEN"]
DISCORD_GUILD_ID    = os.environ["DISCORD_GUILD_ID"]
CHANNEL_WATCHLIST   = os.environ["DISCORD_CHANNEL_WATCHLIST"]
CHANNEL_DAY_SIGNALS = os.environ["DISCORD_CHANNEL_DAY_SIGNALS"]
CHANNEL_SWING_SIGNALS = os.environ["DISCORD_CHANNEL_SWING_SIGNALS"]
CHANNEL_RECAPS      = os.environ["DISCORD_CHANNEL_RECAPS"]
CHANNEL_LONGTERM    = os.environ["DISCORD_CHANNEL_LONGTERM"]

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
        save_session_state()
        log.info(f"Session reset for {today}")

# ── Day-of-week checks ────────────────────────────────────────────────────────

def is_off_day() -> bool:
    """Friday (4), Saturday (5), and Sunday (6) are all off days — no trading, no Discord."""
    return datetime.now(ET).weekday() >= 4

# ── Time window checks ────────────────────────────────────────────────────────

def in_day_trade_window() -> bool:
    """
    Day-trade entries: 9:30 AM – 11:00 AM ET.
    The system prompt's '10 AM lockout' (Rule 6) is enforced by Claude via the
    session trade count — NOT by a hard time gate here.  Claude knows to stop
    scanning after 10 AM if no A+ setup has fired.
    """
    now = datetime.now(ET).time()
    return dtime(9, 30) <= now <= dtime(11, 0)

def in_swing_window() -> bool:
    now = datetime.now(ET).time()
    return dtime(15, 0) <= now <= dtime(16, 0)

def in_dead_zone() -> bool:
    now = datetime.now(ET).time()
    return dtime(11, 0) < now < dtime(15, 0)

# ── BUILT-IN 2026 ECONOMIC CALENDAR (verified against Fed official schedule) ──
# FOMC rate decisions: 2:00 PM ET — Kill Switch 2: FULL session halt
FOMC_DECISION_DATES = {"2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09"}
# FOMC minutes: 2:00 PM ET, 3 weeks after each decision — blocks the 3-4 PM swing window
FOMC_MINUTES_DATES  = {"2026-07-08", "2026-08-19", "2026-10-07", "2026-11-18", "2026-12-30"}
US_MARKET_HOLIDAYS  = {"2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25",
                       "2026-06-19", "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25"}

def _nth_business_day(year: int, month: int, n: int):
    """Nth business day of a month, skipping weekends and US market holidays."""
    from datetime import date, timedelta
    d, count = date(year, month, 1), 0
    while True:
        if d.weekday() < 5 and d.isoformat() not in US_MARKET_HOLIDAYS:
            count += 1
            if count == n:
                return d
        d += timedelta(days=1)

def todays_scheduled_events() -> list:
    """High-impact events scheduled for today, computed — never goes stale."""
    now = datetime.now(ET)
    today_iso = now.date().isoformat()
    events = []
    if today_iso in FOMC_DECISION_DATES:
        events.append(("FOMC_RATE_DECISION", "full-day"))
    if today_iso in FOMC_MINUTES_DATES:
        events.append(("FOMC_MINUTES_2PM", "13:45-16:00"))
    if now.date() == _nth_business_day(now.year, now.month, 1):
        events.append(("ISM_MANUFACTURING_10AM", "09:45-10:15"))
    if now.date() == _nth_business_day(now.year, now.month, 3):
        events.append(("ISM_SERVICES_10AM", "09:45-10:15"))
    return events

def is_fomc_decision_day() -> bool:
    return datetime.now(ET).date().isoformat() in FOMC_DECISION_DATES

def in_report_blackout() -> bool:
    """Code-enforced economic report blackouts (Kill Switch 1).
    Env var REPORT_BLACKOUTS, comma-separated windows:
      "2026-07-06:09:45-10:15"  -> applies only on that date
      "09:45-10:15"             -> applies every trading day
    """
    now = datetime.now(ET)
    # Built-in calendar first (automatic, every day, forever)
    for _name, window in todays_scheduled_events():
        if window == "full-day":
            return True
        s, e = window.split("-")
        sh, sm = map(int, s.split(":")); eh, em = map(int, e.split(":"))
        if dtime(sh, sm) <= now.time() <= dtime(eh, em):
            return True
    # Then manual env var overrides for one-off events (Fed testimony, addresses, etc.)
    raw = os.environ.get("REPORT_BLACKOUTS", "")
    if not raw:
        return False
    today_iso = now.date().isoformat()
    for win in raw.split(","):
        win = win.strip()
        if not win:
            continue
        try:
            if win.count(":") == 3:                 # date-scoped
                date_part, time_part = win.split(":", 1)
                if date_part != today_iso:
                    continue
            else:
                time_part = win
            start_s, end_s = time_part.split("-")
            sh, sm = map(int, start_s.split(":"))
            eh, em = map(int, end_s.split(":"))
            if dtime(sh, sm) <= now.time() <= dtime(eh, em):
                return True
        except Exception:
            continue
    return False

# ── Discord helper ────────────────────────────────────────────────────────────
DISCORD_API = "https://discord.com/api/v10"
DISCORD_POSTING_ENABLED = os.environ.get("DISCORD_POSTING_ENABLED", "true").lower() == "true"

def post_discord(channel_id: str, message: str) -> bool:
    if not DISCORD_POSTING_ENABLED:
        log.info(f"Discord posting disabled — suppressed message to channel {channel_id}")
        return False

    # ── FRIDAY BLOCK ──────────────────────────────────────────────────────────
    if is_off_day():
        log.info("Friday — Discord posting suppressed (no-trading day)")
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

    # Parse the alert's own timestamp so Claude reasons about ALERT time, not processing time
    alert_time_raw = alert_data.get("time", "")
    try:
        alert_dt_utc = datetime.fromisoformat(alert_time_raw.replace("Z", "+00:00"))
        alert_dt_et  = alert_dt_utc.astimezone(ET)
        alert_time_et_str = alert_dt_et.strftime("%I:%M %p ET")
    except Exception:
        alert_time_et_str = "unknown (parse error)"

    tt_status = "CONNECTED (PAPER)" if (is_authenticated() and PAPER_TRADING) else \
                "CONNECTED (LIVE)"  if (is_authenticated() and not PAPER_TRADING) else \
                "NOT CONNECTED"

    ticker = alert_data.get("ticker", "").upper()
    zones  = volume_profile.get_zones(ticker)

    if zones:
        zone_text = f"""
VOLUME PROFILE ZONES FOR {ticker} (auto-calculated, last updated {zones.get('updated_at', 'unknown')}):
1H — Point of Control: {zones.get('1H', {}).get('poc', 'N/A')} | Demand Zone: {zones.get('1H', {}).get('demand', 'N/A')} | Supply Zone: {zones.get('1H', {}).get('supply', 'N/A')}
4H — Point of Control: {zones.get('4H', {}).get('poc', 'N/A')} | Demand Zone: {zones.get('4H', {}).get('demand', 'N/A')} | Supply Zone: {zones.get('4H', {}).get('supply', 'N/A')}
"""
    else:
        zone_text = f"\nVOLUME PROFILE ZONES FOR {ticker}: NOT AVAILABLE this session — treat any zone-dependent confirmation (Rules 1, 2, 5, 6) as UNCONFIRMED.\n"

    # Include last 10 live candles from Alpaca for real-time price structure context
    recent_candles = alpaca_stream.get_candles(ticker, limit=10)
    if recent_candles:
        candle_text = f"\nRECENT 1-MIN CANDLES FOR {ticker} (last {len(recent_candles)}, most recent last):\n"
        for c in recent_candles:
            candle_text += f"  {c.get('t','')[:16]} | O:{c.get('o')} H:{c.get('h')} L:{c.get('l')} C:{c.get('c')} V:{c.get('v')}\n"
    else:
        candle_text = f"\nRECENT CANDLES FOR {ticker}: Not yet available (Alpaca backfill in progress).\n"

    context = f"""
CURRENT SERVER TIME (ET): {now_et.strftime('%A, %B %d, %Y %I:%M %p ET')}
ALERT TRIGGER TIME (ET):  {alert_time_et_str}   ← USE THIS for time-window checks, not server time
DAY OF WEEK: {now_et.strftime('%A')}

IMPORTANT TIME-WINDOW RULES (hard-coded — do NOT override):
- Day trade entries are valid between 9:30 AM ET and 11:00 AM ET ONLY.
  Use the ALERT TRIGGER TIME above for this check, not server processing time.
- Dead zone (NO trades of any kind): 11:00 AM ET – 3:00 PM ET
- Swing window: 3:00 PM ET – 4:00 PM ET
- Friday is a NO-TRADING day — respond NO_TRADE for all signals.
- Weekend (Sat/Sun) — respond NO_TRADE.
- ECONOMIC REPORT BLACKOUTS are enforced by code before you ever see an alert;
  if you receive an alert, no blackout is active. Do not invent report-based rejections.
- The 10 AM LOCKOUT in the playbook (Rule 6) means: if NO trade has been executed
  by 10:00 AM and no A+ setup is printing, STOP scanning. It does NOT mean all
  signals after 10 AM are automatically rejected — signals between 10:00 AM and
  11:00 AM are still valid if they meet all other criteria.

SESSION TRADE COUNT TODAY: {session['trade_count']}
CONSECUTIVE LOSSES TODAY: {session['consecutive_losses']}
CIRCUIT BREAKER ACTIVE: {session['circuit_breaker']}
TASTYTRADE STATUS: {tt_status}

{zone_text}{candle_text}

INCOMING ALERT DATA:
{json.dumps(alert_data, indent=2)}

ANALYSIS TYPE REQUESTED: {analysis_type}

Based on my complete trading playbook and all rules in your system prompt:
1. Run the full 5-category pre-flight checklist against this alert
2. Use the ALERT TRIGGER TIME (not server time) for all time-window checks
3. Check the alert against the 3-Screen Confluence System (Rules 1-6)
4. Determine if this is a valid trade signal, watchlist note, or no-trade
5. If valid: format the exact Discord message in Junior's voice
6. If not valid: explain briefly why (1-2 sentences max, no lengthy breakdowns)

IMPORTANT: Start your response with exactly one of these tags on the first line:
- TRADE_VALID: (if this should be executed)
- NO_TRADE: (if this should be skipped)
- WATCHLIST: (if this is a watchlist update only)

Then on the next lines, write the Discord message exactly as Junior would post it.
Keep NO_TRADE explanations to 1-2 sentences.
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

    # ── FRIDAY BLOCK ──────────────────────────────────────────────────────────
    if is_off_day():
        return False, "FRIDAY: No trading day — system offline"

    # ── WEEKEND BLOCK ─────────────────────────────────────────────────────────
    if is_off_day():
        return False, "WEEKEND: Market closed"

    ticker = alert_data.get("ticker", "").upper()

    if session["circuit_breaker"]:
        return False, "CIRCUIT_BREAKER: Two consecutive losses — system shut down"

    if session["kill_switch_active"]:
        return False, f"KILL_SWITCH: {session['kill_switch_reason']}"

    if session["trade_count"] >= 2:
        return False, "TRADE_CAP: Maximum 2 trades reached for today"

    if is_fomc_decision_day():
        return False, "KILL_SWITCH_2: FOMC rate decision day — 100% cash, full trading halt"

    if in_report_blackout():
        return False, "REPORT_BLACKOUT: Economic data release window — no entries (Kill Switch 1)"

    if in_dead_zone():
        return False, "DEAD_ZONE: No trades between 11:00 AM – 3:00 PM ET"

    analysis_type = determine_analysis_type(alert_data)
    if analysis_type == "NO_TRADE_WINDOW":
        return False, "OUT_OF_WINDOW: Alert received outside trading windows"

    if analysis_type == "DAY_SIGNAL" and ticker not in DAY_TRADE_TICKERS:
        return False, f"INVALID_TICKER: {ticker} not on approved day-trade list"

    return True, "PASS"

# ── Execute trade via Alpaca ──────────────────────────────────────────────────
def execute_trade(alert_data: dict, direction: str, analysis_type: str) -> dict | None:
    """LIVE execution: MARKET entry -> confirmed fill -> broker-resting stop ->
    hand off to position_manager exit engine. Discord signal ONLY after fill."""
    ticker = alert_data.get("ticker", "").upper()

    if not is_authenticated():
        log.warning("Tastytrade not authenticated — signal posted to Discord only")
        return None

    # One position at a time when unattended — never stack live risk
    if position_manager.open_position_count() >= 1:
        log.info("Position already open — skipping new entry (single-position rule)")
        return None

    trade_type = "DAY" if analysis_type == "DAY_SIGNAL" else "SWING"
    contract   = find_option_contract(ticker, direction, trade_type)
    if not contract:
        log.warning(f"No suitable contract found for {ticker} {direction}")
        return None

    # MARKET order entry (playbook rule: instant fill)
    order = place_order(contract, quantity=1)
    if not order:
        log.error("Entry order rejected by Tastytrade")
        return None

    fill = wait_for_fill(order["order_id"], timeout_sec=60)
    if not fill or not fill.get("fill_price"):
        log.error("Entry not filled within 60s — cancelled, no position (fail closed)")
        return None

    entry_price = fill["fill_price"]

    # Protective stop + monitoring BEFORE we announce anything
    position_manager.register(contract["symbol"], ticker, direction, entry_price, 1, trade_type)

    session["trade_count"] += 1
    save_session_state()

    # Exact playbook signal format, only after a confirmed fill
    expiry_dt = datetime.fromisoformat(contract["expiry"])
    signal = (f"Buying {ticker} {expiry_dt.strftime('%B %-d').upper()} "
              f"${contract['strike']:g} {direction} @ {entry_price:.2f} @everyone")
    channel = CHANNEL_DAY_SIGNALS if trade_type == "DAY" else CHANNEL_SWING_SIGNALS
    post_discord(channel, signal)

    mode = "PAPER" if PAPER_TRADING else "LIVE"
    log.info(f"✅ {mode} trade filled & protected: {contract['symbol']} @ {entry_price}")
    return {"order_id": order["order_id"], "fill_price": entry_price, "symbol": contract["symbol"]}

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
    first_line     = claude_response.split("\n")[0].strip().upper()
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

        # Execute via Alpaca
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

# ── SESSION PERSISTENCE (survives Render restarts) ───────────────────────────
SESSION_FILE = "/tmp/tpp_session.json"

def save_session_state():
    try:
        with open(SESSION_FILE, "w") as f:
            json.dump({**session, "date": str(session["date"])}, f)
    except Exception as e:
        log.error(f"Session save failed: {e}")

def load_session_state():
    try:
        if os.path.exists(SESSION_FILE):
            with open(SESSION_FILE) as f:
                data = json.load(f)
            if data.get("date") == str(datetime.now(ET).date()):
                session.update({k: v for k, v in data.items() if k != "date"})
                session["date"] = datetime.now(ET).date()
                log.info(f"📂 Session state restored: trades={session['trade_count']} losses={session['consecutive_losses']} breaker={session['circuit_breaker']}")
    except Exception as e:
        log.error(f"Session load failed: {e}")

# ── AUTO WIN/LOSS (fed by exit engine — no manual endpoints needed) ──────────
def record_win(symbol: str):
    reset_session_if_new_day()
    session["consecutive_losses"] = 0
    save_session_state()

def record_loss(symbol: str):
    reset_session_if_new_day()
    session["consecutive_losses"] += 1
    save_session_state()
    if session["consecutive_losses"] >= 2 and not session["circuit_breaker"]:
        session["circuit_breaker"] = True
        save_session_state()
        post_discord(CHANNEL_RECAPS,
            "Two stops hit back to back — that's the market telling us to sit out. "
            "Shutting it down for the day to protect capital. Hands in our pockets, back tomorrow. @everyone")

# ── EMERGENCY FLATTEN (hit this from your phone) ─────────────────────────────
@app.route("/flatten", methods=["POST"])
def flatten():
    data = request.get_json(force=True, silent=True) or {}
    if not hmac.compare_digest(data.get("secret", ""), WEBHOOK_SECRET):
        return jsonify({"error": "Unauthorized"}), 401
    session["kill_switch_active"] = True
    session["kill_switch_reason"] = "Manual flatten"
    save_session_state()
    results = position_manager.flatten_all()
    log.info(f"🧯 FLATTEN executed: {results}")
    return jsonify({"status": "flattened", **results, "kill_switch": True}), 200

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
        "managed_positions": position_manager.open_position_count(),
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
        "day_of_week": now_et.strftime("%A"),
        "is_off_day": is_off_day(),
        "in_day_trade_window": in_day_trade_window(),
        "report_blackout_active": in_report_blackout(),
        "todays_scheduled_events": [e[0] for e in todays_scheduled_events()],
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
    """Manually trigger an immediate zone recalculation."""
    data = request.get_json(force=True, silent=True) or {}
    if data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401
    results = volume_profile.update_all_zones(list(SWING_TICKERS))
    return jsonify({"status": "refreshed", "results": results}), 200

# ── DAILY ZONE SCHEDULER ───────────────────────────────────────────────────────
_scheduler_started = False
_scheduler_lock    = threading.Lock()

# ── AUTOMATED JOB 1: Daily Watchlist ─────────────────────────────────────────
def fetch_premarket_data(ticker: str) -> dict:
    """Pull premarket price, gap, and volume data from Alpaca real-time stream."""
    return alpaca_stream.get_premarket_data(ticker)

def post_daily_watchlist():
    """Generate and post the morning watchlist to #daily-watchlist."""
    # Never post on Friday or weekends
    if is_off_day():
        log.info("Friday/weekend — watchlist suppressed")
        return

    log.info("📋 Generating daily watchlist...")
    zones  = volume_profile.get_all_zones()
    tickers = list(DAY_TRADE_TICKERS)

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

    events_note = ", ".join(e[0].replace("_", " ").title() for e in todays_scheduled_events()) or "None"
    prompt = f"""It is {datetime.now(ET).strftime('%A, %B %d, %Y')} — pre-market. You are Junior from The Portfolio Plug.\nHIGH-IMPACT EVENTS SCHEDULED TODAY: {events_note} (entries are auto-blocked around these — mention them naturally in the watchlist if any).

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

# ── AUTOMATED JOB 2: Pattern Scanner ─────────────────────────────────────────
_pattern_scan_cooldown = {}

def scan_for_visual_patterns():
    """Scan for confluence setups — only runs on trading days (not Fri/weekend)."""
    if is_off_day() or in_report_blackout():
        return

    now_et = datetime.now(ET)
    if not (dtime(9, 25) <= now_et.time() <= dtime(10, 30)):
        return

    log.info("🔍 Running pattern scan...")
    zones = volume_profile.get_all_zones()

    for ticker in DAY_TRADE_TICKERS:
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
1. Price approaching or bouncing off 8 EMA or 21 EMA
2. Price near a Volume Profile demand or supply zone
3. Flag/consolidation pattern forming after a strong move
4. RSI divergence conditions
5. PMH/PML proximity

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
    """Generate and post an end-of-day recap. Suppressed on Friday/weekends."""
    if is_off_day():
        log.info("Friday/weekend — EOD recap suppressed")
        return

    log.info("📊 Generating end-of-day recap...")

    positions_data = []
    balance = {}
    try:
        positions_data = get_positions()
        balance        = get_account_balance()
    except Exception as e:
        log.error(f"Could not fetch Tastytrade data for recap: {e}")

    trade_count      = session.get("trade_count", 0)
    consecutive_loss = session.get("consecutive_losses", 0)
    circuit_breaker  = session.get("circuit_breaker", False)

    prompt = f"""It is {datetime.now(ET).strftime('%A, %B %d, %Y')} — market just closed. You are Junior from The Portfolio Plug.

Write the end-of-day recap post for #profits-and-recaps. Keep it real, educational, and in your voice.

TODAY'S SESSION STATS:
- Trades taken: {trade_count}
- Consecutive losses at close: {consecutive_loss}
- Circuit breaker triggered today: {circuit_breaker}
- Open positions: {len(positions_data)}
- Account balance data: {json.dumps(balance, indent=2) if balance else 'Not available'}

OPEN POSITIONS:
{json.dumps(positions_data, indent=2) if positions_data else 'None — flat going into tomorrow'}

Your recap must include:
1. One honest sentence about how today went overall
2. If trades were taken: what the setup was, what worked or didn't
3. If no trades: why the system stayed in cash — frame it as discipline, not failure
4. Open positions if any: what you're holding and why
5. What to watch for tomorrow (1-2 things)
6. A closing line for the community

Keep it under 300 words. No fluff."""

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
    2. Daily watchlist post (9:15 AM ET, weekdays excluding Friday)
    3. Pattern scanner (every 5 min, 9:25-10:30 AM ET, weekdays excluding Friday)
    4. End-of-day recap (4:01 PM ET, weekdays excluding Friday)
    """
    SCHEDULER_STATE_FILE = "/tmp/tpp_scheduler_state.json"
    ZONE_FILE            = "/tmp/tpp_zones.json"
    RENDER_API_KEY       = os.environ.get("RENDER_API_KEY", "")
    RENDER_SERVICE_ID    = "srv-d91fh10k1i2s73arkh20"

    def load_scheduler_state() -> dict:
        state = {}
        zone_date      = os.environ.get("SCHEDULER_ZONE_DATE")
        watchlist_date = os.environ.get("SCHEDULER_WATCHLIST_DATE")
        recap_date     = os.environ.get("SCHEDULER_RECAP_DATE")
        if zone_date:      state["zone_date"]      = zone_date
        if watchlist_date: state["watchlist_date"] = watchlist_date
        if recap_date:     state["recap_date"]     = recap_date
        if state:
            log.info(f"📅 Loaded scheduler state from env vars: {state}")
            return state
        try:
            if os.path.exists(SCHEDULER_STATE_FILE):
                with open(SCHEDULER_STATE_FILE, "r") as f:
                    data = json.load(f)
                if data:
                    log.info(f"📅 Loaded scheduler state from file: {data}")
                    return data
        except Exception:
            pass
        try:
            if os.path.exists(ZONE_FILE):
                with open(ZONE_FILE, "r") as f:
                    zones = json.load(f)
                meta = zones.get("__scheduler_state__", {})
                if meta:
                    return meta
                today_iso = datetime.now(ET).date().isoformat()
                for key, val in zones.items():
                    if key.startswith("__"):
                        continue
                    if today_iso in val.get("updated_at", ""):
                        return {"zone_date": today_iso, "watchlist_date": None, "recap_date": None}
        except Exception:
            pass
        return {}

    def save_scheduler_state(state: dict):
        try:
            with open(SCHEDULER_STATE_FILE, "w") as f:
                json.dump(state, f)
        except Exception as e:
            log.error(f"Failed to save scheduler state locally: {e}")

        if RENDER_API_KEY:
            try:
                env_vars = []
                if state.get("zone_date"):
                    env_vars.append({"key": "SCHEDULER_ZONE_DATE",      "value": state["zone_date"]})
                if state.get("watchlist_date"):
                    env_vars.append({"key": "SCHEDULER_WATCHLIST_DATE", "value": state["watchlist_date"]})
                if state.get("recap_date"):
                    env_vars.append({"key": "SCHEDULER_RECAP_DATE",     "value": state["recap_date"]})
                if env_vars:
                    resp = requests.put(
                        f"https://api.render.com/v1/services/{RENDER_SERVICE_ID}/env-vars",
                        headers={"Authorization": f"Bearer {RENDER_API_KEY}", "Content-Type": "application/json"},
                        json=env_vars,
                        timeout=10
                    )
                    if resp.status_code == 200:
                        log.info(f"✅ Scheduler state persisted to Render env vars: {state}")
                    else:
                        log.warning(f"⚠️ Render API state save failed: {resp.status_code}")
            except Exception as e:
                log.error(f"Render API state save error: {e}")

        try:
            if os.path.exists(ZONE_FILE):
                with open(ZONE_FILE, "r") as f:
                    zones = json.load(f)
                zones["__scheduler_state__"] = state
                with open(ZONE_FILE, "w") as f:
                    json.dump(zones, f)
        except Exception:
            pass

    _state = load_scheduler_state()
    last_zone_date      = datetime.fromisoformat(_state["zone_date"]).date()      if _state.get("zone_date")      else None
    last_watchlist_date = datetime.fromisoformat(_state["watchlist_date"]).date() if _state.get("watchlist_date") else None
    last_recap_date     = datetime.fromisoformat(_state["recap_date"]).date()     if _state.get("recap_date")     else None

    log.info(f"📅 Scheduler state loaded — zones: {last_zone_date}, watchlist: {last_watchlist_date}, recap: {last_recap_date}")

    try:
        alpaca_stream.start()
        log.info("✅ Alpaca real-time stream started — backfill running in background")
        time_module.sleep(3)
    except Exception as e:
        log.error(f"Alpaca stream start failed: {e}", exc_info=True)

    # Run zone calculation on startup only if no fresh zone data exists
    try:
        existing_zones = volume_profile.get_all_zones()
        now_et_boot    = datetime.now(ET)
        zones_are_fresh = False
        if existing_zones:
            for ticker_data in existing_zones.values():
                updated_at_str = ticker_data.get("updated_at", "")
                if updated_at_str:
                    try:
                        updated_at = datetime.fromisoformat(updated_at_str)
                        if updated_at.date() == now_et_boot.date():
                            zones_are_fresh = True
                            break
                    except Exception:
                        pass
        if zones_are_fresh:
            log.info("✅ Zone data from today already exists — skipping startup recalculation")
            last_zone_date = now_et_boot.date()
        else:
            log.info("🔄 No fresh zones found — will calculate at 8:00 AM ET")
    except Exception as e:
        log.error(f"Zone startup check failed: {e}", exc_info=True)

    while True:
        try:
            now_et = datetime.now(ET)
            today  = now_et.date()
            t      = now_et.time()

            # Skip market-facing jobs on weekends AND Fridays
            no_trade_day = is_off_day()

            # JOB 1: Volume Profile zones — 8:00 AM ET (weekdays only, including Friday for zone data)
            if not is_off_day() and dtime(8, 0) <= t <= dtime(8, 30) and today != last_zone_date:
                log.info("🔄 Running daily Volume Profile zone calculation...")
                volume_profile.update_all_zones(list(SWING_TICKERS))
                last_zone_date = today
                save_scheduler_state({
                    "zone_date":      today.isoformat(),
                    "watchlist_date": last_watchlist_date.isoformat() if last_watchlist_date else None,
                    "recap_date":     last_recap_date.isoformat()     if last_recap_date     else None,
                })
                log.info("✅ Daily Volume Profile zones updated automatically.")

            # JOB 2: Daily watchlist — 9:15 AM ET (weekdays, NOT Friday)
            if not no_trade_day and dtime(9, 15) <= t <= dtime(9, 45) and today != last_watchlist_date:
                post_daily_watchlist()
                last_watchlist_date = today
                save_scheduler_state({
                    "zone_date":      last_zone_date.isoformat()      if last_zone_date      else None,
                    "watchlist_date": today.isoformat(),
                    "recap_date":     last_recap_date.isoformat()     if last_recap_date     else None,
                })

            # JOB 3: Pattern scanner — every loop tick during trade window (weekdays, NOT Friday)
            if not no_trade_day and dtime(9, 25) <= t <= dtime(10, 30):
                candles_ready = sum(1 for tk in DAY_TRADE_TICKERS if len(alpaca_stream.get_candles(tk, limit=101)) > 100)
                if candles_ready >= len(DAY_TRADE_TICKERS) // 2:
                    scan_for_visual_patterns()
                else:
                    log.info(f"⏳ Pattern scanner skipped — only {candles_ready}/{len(DAY_TRADE_TICKERS)} tickers ready")

            # JOB 4: End-of-day recap — 4:01 PM ET (weekdays, NOT Friday)
            if not no_trade_day and dtime(16, 1) <= t <= dtime(16, 30) and today != last_recap_date:
                post_eod_recap()
                last_recap_date = today
                save_scheduler_state({
                    "zone_date":      last_zone_date.isoformat()      if last_zone_date      else None,
                    "watchlist_date": last_watchlist_date.isoformat() if last_watchlist_date else None,
                    "recap_date":     today.isoformat(),
                })

        except Exception as e:
            log.error(f"Master scheduler error: {e}", exc_info=True)

        time_module.sleep(300)  # check every 5 minutes

def ensure_scheduler_started():
    global _scheduler_started
    with _scheduler_lock:
        if not _scheduler_started:
            _scheduler_started = True
            log.info(f"🚀 Starting zone scheduler in worker PID {os.getpid()}")
            thread = threading.Thread(target=zone_scheduler_loop, daemon=True)
            thread.start()

@app.before_request
def _start_scheduler_on_first_request():
    if not _scheduler_started:
        ensure_scheduler_started()

position_manager.configure(
    post_discord=post_discord,
    channels={"day_signals": CHANNEL_DAY_SIGNALS, "swing_signals": CHANNEL_SWING_SIGNALS, "recaps": CHANNEL_RECAPS},
    on_win=record_win,
    on_loss=record_loss,
)
load_session_state()
position_manager.start()
ensure_scheduler_started()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
