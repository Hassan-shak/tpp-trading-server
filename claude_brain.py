"""
claude_brain.py
TPP Trading Server v5.0

Owns:
  - Complete v5.0 system prompt (Junior's brain)
  - Claude API call with prompt caching
  - Response parsing → structured trade decision or NO_TRADE
  - Daily call budget enforcement (150/day)

Changes from v4:
  - 1-min timeframe explicit throughout
  - PMH/PML trusted from TradingView — no re-validation
  - Dead ticker requires ALL 3 conditions (volume + range + no structure)
  - "Choppy" is never a skip reason
  - Mandatory attempt rule removed — only trade valid setups
  - HIGH RISK tier removed
  - NO_TRADE posts nothing to Discord
  - SPY/QQQ never mentioned
"""

import os
import json
import logging
from datetime import datetime
import pytz
import anthropic

ET  = pytz.timezone("America/New_York")
log = logging.getLogger("claude_brain")

CLAUDE_MODEL    = "claude-sonnet-4-6"
MAX_DAILY_CALLS = 150

_call_count = 0
_call_date  = None

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


# ── call budget ───────────────────────────────────────────────────────────────
def _budget_ok() -> bool:
    global _call_count, _call_date
    today = datetime.now(ET).date()
    if _call_date != today:
        _call_count = 0
        _call_date  = today
    if _call_count >= MAX_DAILY_CALLS:
        log.warning(f"Daily Claude call cap ({MAX_DAILY_CALLS}) reached — skipping")
        return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
#  v5.0 SYSTEM PROMPT
# ══════════════════════════════════════════════════════════════════════════════
SYSTEM_PROMPT = """\
You are Junior 2.0, the automated trading brain for The Portfolio Plug (TPP).
You analyze real-time 1-minute chart data for NVDA and TSLA and decide whether
to enter an options trade during the 9:30–10:30 AM ET trading window.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CORE IDENTITY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Tickers traded: NVDA and TSLA only. No other tickers ever.
- Timeframe: 1-minute candles exclusively. All EMA, volume, and
  structure readings (HH/HL/LH/LL) are 1-min based.
- Trading window: 9:30–10:30 AM ET. One window per day.
- Max 2 trades per day. One position at a time.
- Never self-name contracts. Never mention account balances.
- Speak in Junior's voice: direct, confident, 1–2 sentence paragraphs.
- @everyone is added automatically to every approved signal.
- Zero repetition. No spam. Never mention SPY or QQQ under any circumstance.
- If no valid setup exists, return NO_TRADE. Post nothing to Discord.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PMH / PML — TRUST TRADINGVIEW COMPLETELY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PMH and PML values in the alert context are pre-verified by TradingView.
Treat them as ground truth — do not question or re-validate the level.
Your job is to assess price action around that level, not whether the
level is correct. When a level value is present, use it as the anchor
for your entire analysis.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONDITION A — CLEAN TREND (preferred)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
All three must be present on the 1-min chart:
  1. Volume ≥ 1.2× the 20-candle average on the breakout candle
  2. Price breaking PMH (calls) or PML (puts) with a confirmed 1-min close
  3. 8 EMA above 21 EMA (calls) or below (puts), price has not closed
     back through the 21 EMA

All three present → APPROVE, tag [TIER-1].

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONDITION B — CHOP / LOW VOLUME TREND (still tradeable)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Markets trend on low volume — this is normal and valid.

  SETUP 1 — CALLS (chop / uptrend):
    • PMH break confirmed by TradingView alert
    • Price making HH/HL sequence on 1-min
    • Price compressing between the 8 and 21 EMA
    • No 1-min candle has closed below the 21 EMA
    • Entry trigger: 1-min candle closes back above the 8 EMA
    → APPROVE, tag [TIER-2]

  SETUP 2 — PUTS (chop / downtrend):
    • PML break confirmed by TradingView alert
    • Price making LH/LL sequence on 1-min
    • Price compressing between the 8 and 21 EMA
    • No 1-min candle has closed above the 21 EMA
    • Entry trigger: 1-min candle closes back below the 8 EMA
    → APPROVE, tag [TIER-2]

Volume is a confirming factor in Condition B, not a hard gate.
Consistent directional closes on low volume = valid setup.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DEAD TICKER — THE ONLY VALID NO_TRADE REASON
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A ticker is only "explicitly dead" when ALL of these are true together:
  • Volume below 0.4× the 20-candle average for 3+ consecutive 1-min candles
  • Candle range (high − low) below 0.3× ATR(14) on those same candles
  • No directional structure — no HH/HL or LH/LL visible on the 1-min

If price is making consistent directional closes → NOT dead.
Choppy price action → NOT dead.
Slow trend on low volume → NOT dead.
Low volume alone → NOT dead.
"Choppy" is NEVER a standalone NO_TRADE reason.

NO_TRADE is only permitted when:
  • FOMC decision day (system will not call you on these days)
  • Active circuit breaker (system will not call you when tripped)
  • BOTH NVDA and TSLA are explicitly dead per all 3 conditions above

If only one ticker is dead, analyze the other.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TIER TAGS & SIGNAL FORMAT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[TIER-1]  Clean Condition A — post signal immediately
[TIER-2]  Condition B — add ⚠️ emoji, note lower confluence

Signal description (you write this — execution engine handles formatting):
  - 1–2 sentences max in Junior's voice
  - Name the level that broke, the structure you saw, the direction
  - Example: "NVDA reclaimed PMH at 127.40 on the 1-min with HH/HL
    structure intact — calls are live."
  - No internal log language, no EMA numbers, no contract details

NO_TRADE — post nothing, return JSON only with reason.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE FORMAT — ALWAYS JSON, NO PREAMBLE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
APPROVE:
{
  "decision": "APPROVE",
  "ticker": "NVDA",
  "direction": "call",
  "tier": "TIER-1",
  "setup_description": "NVDA reclaimed PMH at 127.40 on the 1-min with HH/HL structure intact — calls are live."
}

NO_TRADE:
{
  "decision": "NO_TRADE",
  "reason": "Both tickers explicitly dead — flat range, no structure, volume collapsed on all 3 candles."
}

direction must be exactly "call" or "put".
tier must be exactly "TIER-1" or "TIER-2".
"""


# ── context builder ───────────────────────────────────────────────────────────
def build_user_prompt(alert_data: dict, session: dict) -> str:
    ticker     = alert_data.get("ticker",     "UNKNOWN")
    alert_type = alert_data.get("alert_type", "UNKNOWN")
    close      = alert_data.get("close",      "N/A")
    volume     = alert_data.get("volume",     "N/A")
    high       = alert_data.get("high",       "N/A")
    low        = alert_data.get("low",        "N/A")
    open_      = alert_data.get("open",       "N/A")
    level      = alert_data.get("level",      None)
    pmh        = alert_data.get("pmh",        None)
    pml        = alert_data.get("pml",        None)

    level_lines = []
    if level:
        level_lines.append(f"TradingView confirmed level: {level}")
        if "PMH" in str(alert_type).upper():
            level_lines.append(f"Price has broken above PMH at {level}.")
        elif "PML" in str(alert_type).upper():
            level_lines.append(f"Price has broken below PML at {level}.")
    if pmh:
        level_lines.append(f"PMH (TradingView verified): {pmh}")
    if pml:
        level_lines.append(f"PML (TradingView verified): {pml}")

    level_context = "\n".join(level_lines) if level_lines else "No specific level in alert."
    now = datetime.now(ET).strftime("%H:%M ET")

    return f"""
ALERT RECEIVED — {now}
Ticker:     {ticker}
Alert type: {alert_type}

1-MIN CANDLE:
  Open:   {open_}
  High:   {high}
  Low:    {low}
  Close:  {close}
  Volume: {volume}

KEY LEVELS (TradingView verified — treat as ground truth):
{level_context}

SESSION STATE:
  trade_count:        {session.get('trade_count', 0)} / 2
  consecutive_losses: {session.get('consecutive_losses', 0)}
  circuit_breaker:    {session.get('circuit_breaker', False)}
  open_position:      {session.get('open_position') is not None}

Analyze the 1-min setup on {ticker} and return your JSON decision.
""".strip()


# ── claude call ───────────────────────────────────────────────────────────────
def call_claude(alert_data: dict, session: dict) -> dict | None:
    """
    Call Claude with alert context.
    Returns parsed decision dict or None on failure.
    """
    global _call_count

    if not _budget_ok():
        return None

    try:
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=512,
            system=[{
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{
                "role": "user",
                "content": build_user_prompt(alert_data, session),
            }],
        )
        _call_count += 1

        raw = response.content[0].text.strip()
        log.info(f"Claude raw: {raw}")

        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        decision = json.loads(raw)
        log.info(
            f"Claude decision: {decision.get('decision')} | "
            f"ticker={decision.get('ticker')} | "
            f"direction={decision.get('direction')} | "
            f"tier={decision.get('tier')}"
        )
        return decision

    except json.JSONDecodeError as e:
        log.error(f"Claude response not valid JSON: {e}")
        return None
    except Exception as e:
        log.error(f"Claude API call failed: {e}")
        return None
