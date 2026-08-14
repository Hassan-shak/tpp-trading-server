"""
main.py
TPP Trading Server v5.0
The Portfolio Plug — AI Trading Webhook Server

Single-file deployment. Replaces v4.1 main.py entirely.

What changed from v4.1:
  - AM window only: 9:30–10:30 AM ET (PM window removed)
  - 1-min timeframe explicit throughout
  - PMH/PML trusted from TradingView as ground truth
  - Contract pricing: ask $0.75–$1.50/share ($75–$150/contract), ATM→OTM walk
  - Dead ticker requires ALL 3 conditions — "choppy" never a skip reason
  - Mandatory attempt rule removed — only valid setups traded
  - HIGH RISK tier removed — TIER-1 and TIER-2 only
  - Position monitor runs past 10:30 AM if trade is open — no forced flatten
  - Recap posts whenever trade closes, regardless of time
  - 9:45 AM + 10:15 AM status updates (skipped if trade already fired)
  - Zero SPY/QQQ output anywhere
  - Entry signals bypass cooldown (0s) — commentary still 5 min
  - Full gate audit log — every webhook logged with pass/block reason
  - Emergency DM to Junior if stop-loss placement fails
"""

import os
import json
import hmac
import hashlib
import logging
import time as time_module
import threading
from datetime import datetime, date, timedelta, timezone
import pytz
import re
import requests
import anthropic as anthropic_sdk
from flask import Flask, request, jsonify

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("main")

app = Flask(__name__)
log.info(f"TPP Trading Server v5.0 — PID {os.getpid()}")

ET = pytz.timezone("America/New_York")

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS & ENV
# ══════════════════════════════════════════════════════════════════════════════
TICKERS            = [t.strip().upper() for t in
                      os.environ.get("TRADEABLE_TICKERS", "NVDA,TSLA,SPY").split(",") if t.strip()]
TRADEABLE_TICKERS  = set(TICKERS)
WEBHOOK_SECRET     = os.environ.get("WEBHOOK_SECRET", "")
DISCORD_BOT_TOKEN  = os.environ["DISCORD_BOT_TOKEN"]
# Comma-separated list of Discord user IDs — every ID gets every alert/pre-flight DM
JUNIOR_USER_IDS    = [u.strip() for u in
                      os.environ.get("JUNIOR_DISCORD_USER_ID", "").split(",") if u.strip()]
ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
TT_BASE            = "https://api.tastytrade.com"
TT_USER            = os.environ["TASTYTRADE_USERNAME"]
TT_PASS            = os.environ["TASTYTRADE_PASSWORD"]
TT_ACCOUNT         = os.environ["TASTYTRADE_ACCOUNT_NUMBER"]
DISCORD_API        = "https://discord.com/api/v10"
CLAUDE_MODEL       = "claude-sonnet-4-6"
MAX_DAILY_CALLS    = 220
MIN_PREMIUM        = float(os.environ.get("PREMIUM_MIN", "0.75"))   # $75/contract floor
MAX_PREMIUM        = float(os.environ.get("PREMIUM_MAX", "1.50"))   # $150/contract cap — members can afford every trade
MAX_SPREAD         = 0.05   # 5% bid-ask spread cap
FILL_TIMEOUT       = 60     # seconds before cancel on entry
CLOSE_TIMEOUT      = 45     # seconds before market escalation on exit

CHANNEL_IDS = {
    "daily-watchlist":    os.environ.get("DISCORD_CHANNEL_WATCHLIST",  ""),
    "day-trade-signals":  os.environ.get("DISCORD_CHANNEL_DAY_SIGNALS") or os.environ.get("DISCORD_CHANNEL_SIGNALS",    ""),
    "profits-and-recaps": os.environ.get("DISCORD_CHANNEL_RECAPS",     ""),
}

MARKET_HOLIDAYS = {
    # 2026
    date(2026,  1,  1), date(2026,  1, 19), date(2026,  2, 16),
    date(2026,  4,  3), date(2026,  5, 25), date(2026,  6, 19),
    date(2026,  7,  3), date(2026,  9,  7), date(2026, 11, 26),
    date(2026, 12, 25),
    # 2027
    date(2027,  1,  1), date(2027,  1, 18), date(2027,  2, 15),
    date(2027,  3, 26), date(2027,  5, 31), date(2027,  6, 18),
    date(2027,  7,  5), date(2027,  9,  6), date(2027, 11, 25),
    date(2027, 12, 24),
}
MARKET_HOLIDAYS_2026 = MARKET_HOLIDAYS   # legacy alias

# Early closes — market shuts 1:00 PM ET (system flattens 12:45, stands down after)
EARLY_CLOSE_DATES = {
    date(2026, 11, 27), date(2026, 12, 24),   # Black Friday, Christmas Eve
    date(2027, 11, 26),                        # Black Friday 2027
}


def _flatten_time(d) -> tuple[int, int]:
    """(hour, minute) of the forced flatten: 12:45 on early-close days, else 15:45."""
    return (12, 45) if d in EARLY_CLOSE_DATES else (15, 45)

FOMC_DECISION_DAYS_2026 = {
    date(2026,  1, 29), date(2026,  3, 19), date(2026,  5,  7),
    date(2026,  6, 18), date(2026,  7, 29), date(2026,  9, 16),
    date(2026, 10, 28), date(2026, 12,  9),
}

# ══════════════════════════════════════════════════════════════════════════════
#  SESSION STATE
# ══════════════════════════════════════════════════════════════════════════════
_TMP_PATH = "/tmp/tpp_v5_session.json"

# ── v11: boot identity, single-scheduler election, heartbeat ──────────────────
import uuid as _uuid
import fcntl as _fcntl

_BOOT_ID        = _uuid.uuid4().hex[:6]
_IMPORT_PID     = os.getpid()
_BOOT_TIME_ET   = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET")
_SCHED_LOCKFILE = "/tmp/tpp_scheduler.lock"
_HEARTBEAT_FILE = "/tmp/tpp_heartbeat.json"
_sched_lock_fd  = None   # held forever by the elected scheduler process


def _try_acquire_scheduler_lock() -> bool:
    """Cross-process election: only ONE process in this container may run the
    scheduler, no matter how many gunicorn workers exist. The flock is held
    for the life of the winning process and dies with it."""
    global _sched_lock_fd
    try:
        fd = open(_SCHED_LOCKFILE, "w")
        _fcntl.flock(fd, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
        fd.write(f"{os.getpid()} {_BOOT_ID}\n")
        fd.flush()
        _sched_lock_fd = fd   # keep the fd (and the lock) alive forever
        return True
    except OSError:
        return False


def _beat_heartbeat():
    try:
        with open(_HEARTBEAT_FILE, "w") as f:
            json.dump({"ts": time_module.time(), "boot_id": _BOOT_ID,
                       "pid": os.getpid()}, f)
    except Exception:
        pass


def _read_heartbeat() -> dict | None:
    try:
        with open(_HEARTBEAT_FILE) as f:
            return json.load(f)
    except Exception:
        return None


# ── v11.1: POSITION SIZING + RISK MODES (was: quantity hardcoded to 1) ────────
# high_vol : 10% of net-liq per trade, 25% max stop  (~2.5% acct risk)
# normal   : 20% of net-liq per trade, 20% max stop  (~4.0% acct risk)
# v13: fixed RISK_MODES retired — allocation now follows the ACCOUNT TIERS
# (see SIZING_TIERS): <$10k -> 10%, $10k-<$20k -> 7.5%, >=$20k -> 5%, stop 20%.
NO_TRADE_DATES = {d.strip() for d in
                  os.environ.get("NO_TRADE_DATES", "2026-07-29").split(",") if d.strip()}
MAX_CONTRACTS  = int(os.environ.get("MAX_CONTRACTS", "10"))
MAX_OTM_PCT    = float(os.environ.get("MAX_OTM_PCT", "0.06"))


def _risk() -> dict:
    cash = _sizing_cash()
    if not cash:
        return {"alloc": 0.10, "stop": 0.20, "tier": "Tier 1 (balance unreadable)"}
    alloc, name = _tier_for(cash)
    return {"alloc": alloc, "stop": 0.20, "tier": name}


def _stop_pct() -> float:
    return _risk()["stop"]


def _no_trade_today() -> bool:
    return datetime.now(ET).date().isoformat() in NO_TRADE_DATES


_bal_cache: dict = {"ts": 0.0, "cash": None, "nl": None}


def _account_balances() -> tuple[float | None, float | None]:
    """(cash_balance, net_liq) — 60s cache so per-tick calls don't hammer TT."""
    global _bal_cache
    if time_module.time() - _bal_cache["ts"] < 60 and (_bal_cache["cash"] or _bal_cache["nl"]):
        return _bal_cache["cash"], _bal_cache["nl"]
    try:
        r = requests.get(f"{TT_BASE}/accounts/{TT_ACCOUNT}/balances",
                         headers=_tt_headers(), timeout=6)
        if r.status_code == 200:
            d = r.json().get("data", {})
            cash = float(d.get("cash-balance") or 0) or None
            nl   = float(d.get("net-liquidating-value") or 0) or None
            _bal_cache = {"ts": time_module.time(), "cash": cash, "nl": nl}
            return cash, nl
    except Exception as e:
        log.warning(f"balance fetch failed: {e}")
    return _bal_cache["cash"], _bal_cache["nl"]


def _account_net_liq() -> float | None:
    _, nl = _account_balances()
    return nl


def _sizing_cash() -> float | None:
    """Account cash for tier + sizing math (spec: 'available account cash');
    net-liq stands in when cash is unreadable."""
    cash, nl = _account_balances()
    return cash or nl


# v13: ACCOUNT ALLOCATION TIERS (Junior's Intraday Sizing & Risk Specification)
#   <$10k: 10% alloc | $10k–<$20k: 7.5% | >=$20k: 5% — stop fixed 20% at every
#   tier, so max portfolio risk per trade = alloc x stop = 2% / 1.5% / 1% by
#   construction.
SIZING_TIERS = [
    (10_000.0, 0.200, "Tier 1"),   # v13.1 Community-First spec: 20% <$10k
    (20_000.0, 0.100, "Tier 2"),   # 10% $10k-<$20k
    (float("inf"), 0.050, "Tier 3"),
]
# Optional breathing room under each profit-lock rung (0.0 = lock exactly AT
# the milestone per spec; e.g. 0.02 puts the +10% rung's stop at +8%)
LOCK_BUFFER = float(os.environ.get("LOCK_BUFFER_PCT", "0.0"))


def _tier_for(cash: float) -> tuple[float, str]:
    for _ceil, _alloc, _name in SIZING_TIERS:
        if cash < _ceil:
            return _alloc, _name
    return 0.050, "Tier 3"


def _position_size(premium: float) -> int:
    """v13 SPEC: qty = Floor(cash * tier_alloc / (ask * 100)), capped at
    MAX_CONTRACTS. Returns 0 = ABORT: the allocation rules are never
    overridden to squeeze in 1 contract."""
    if premium <= 0:
        return 0
    cash = _sizing_cash()
    if not cash:
        log.error("Sizing ABORT: account balance unreadable — refusing to size blind")
        return 0
    r = _risk()
    qty = int((cash * r["alloc"]) // (premium * 100))
    qty = min(qty, MAX_CONTRACTS)
    log.info(f"Sizing [{r['tier']}]: cash=${cash:.0f} alloc={r['alloc']:.1%} "
             f"premium=${premium:.2f} -> {qty} contract(s)"
             + (" — ABORT (exceeds tier allocation)" if qty == 0 else ""))
    return qty
_state: dict = {}


def _blank_state(today: str) -> dict:
    return {
        "trade_count":        0,
        "consecutive_losses": 0,
        "ticker_losses":      {},
        "today_results":      [],
        "circuit_breaker":    False,
        "open_position":      None,
        "last_reset_date":    today,
        "daily_trade_log":    [],
    }


def _save_state(s: dict):
    try:
        with open(_TMP_PATH, "w") as f:
            json.dump(s, f)
    except Exception as e:
        log.warning(f"State save failed: {e}")


def load_state() -> dict:
    global _state
    today = datetime.now(ET).strftime("%Y-%m-%d")
    if _state.get("last_reset_date") == today:
        return _state
    # Try disk
    try:
        with open(_TMP_PATH) as f:
            data = json.load(f)
        if data.get("last_reset_date") == today:
            _state = data
            return _state
    except Exception:
        pass
    # Try Render env
    raw = os.environ.get("SESSION_STATE_JSON", "")
    if raw:
        try:
            data = json.loads(raw)
            if data.get("last_reset_date") == today:
                _state = data
                _save_state(_state)
                return _state
        except Exception:
            pass
    log.info(f"New trading day {today} — resetting session state")
    _state = _blank_state(today)
    _save_state(_state)
    return _state


_state_lock = threading.RLock()


def _commit(s: dict):
    global _state
    with _state_lock:
        _state = s
        _save_state(s)


def _locked_update(mutator):
    """Atomically load -> mutate -> commit under the state lock."""
    with _state_lock:
        s = load_state()
        mutator(s)
        _state_locked_commit = s
        _commit(s)
        return s


def get_trade_count() -> int:
    return load_state()["trade_count"]

def get_circuit_breaker() -> bool:
    return load_state()["circuit_breaker"]

def get_open_position() -> dict | None:
    return load_state()["open_position"]

def is_max_trades_reached() -> bool:
    return load_state()["trade_count"] >= 2

def increment_trade_count():
    with _state_lock:
        s = load_state()
        s["trade_count"] += 1
        log.info(f"Trade count → {s['trade_count']}")
        _commit(s)

def set_open_position(position: dict):
    with _state_lock:
        s = load_state()
        s["open_position"] = position
        _commit(s)

def clear_open_position():
    with _state_lock:
        s = load_state()
        s["open_position"] = None
        _commit(s)

def record_trade_result(win: bool, ticker: str | None = None,
                        pnl_pct: float | None = None, pnl_dollar: float | None = None,
                        direction: str | None = None):
  with _state_lock:
    s = load_state()
    s.setdefault("today_results", []).append(
        {"ticker": ticker or "?", "win": bool(win),
         "pnl_pct": pnl_pct, "pnl_dollar": pnl_dollar})
    if not win and ticker:
        tl = s.setdefault("ticker_losses", {})
        tl[ticker] = int(tl.get(ticker, 0)) + 1
        if direction:
            ld = s.setdefault("ticker_loss_dirs", {})
            ld.setdefault(ticker, [])
            if direction not in ld[ticker]:
                ld[ticker].append(direction)
        log.info(f"Ticker loss recorded: {ticker} {direction or ''} -> {tl[ticker]} "
                 "(same direction locked for the day; opposite needs TIER-1)")
    if win:
        s["consecutive_losses"] = 0
        log.info("Win recorded — loss counter reset")
    else:
        s["consecutive_losses"] += 1
        log.info(f"Loss recorded — consecutive: {s['consecutive_losses']}")
        if s["consecutive_losses"] >= 2:
            s["circuit_breaker"] = True
            log.warning("CIRCUIT BREAKER — 2 consecutive losses, day over")
    s["daily_trade_log"].append({
        "result": "win" if win else "loss",
        "time":   datetime.now(ET).isoformat(),
    })
    _commit(s)


# ══════════════════════════════════════════════════════════════════════════════
#  DISCORD
# ══════════════════════════════════════════════════════════════════════════════
_last_discord_hash: dict[str, str] = {}


def _msg_hash(msg: str) -> str:
    return hashlib.md5(msg.encode()).hexdigest()


_LAST_CHART_ERR = ""


def _render_chart_png(ticker: str, highlight: str = "") -> bytes | None:
    """Render the session 1-min chart for a ticker from OUR OWN structure data:
    price line + volume bars + key levels (PMH/PML/OR) + highlight note.
    Lazy-imports matplotlib so a broken install can never take down trading.
    Returns PNG bytes, or None on any failure — callers always degrade to
    text-only posts."""
    try:
        import io, os as _os
        _os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib import dates as mdates

        st = _mkt_structure.get(ticker) or {}
        candles = st.get("candles") or []
        if len(candles) < 3:
            # Pre-market / fresh boot: structure is empty, but the chart can
            # still be real — pull the morning's 1-min bars straight from
            # Alpaca (pre-market action IS the right 9:15 watchlist chart).
            global _LAST_CHART_ERR
            _attempts = []
            try:
                _day = datetime.now(ET).strftime("%Y-%m-%d")
                for _feed in ("sip", "iex", None):
                    _p = {"timeframe": "1Min", "start": _day + "T08:00:00Z", "limit": 1000}
                    if _feed:
                        _p["feed"] = _feed
                    _r = requests.get(
                        f"https://data.alpaca.markets/v2/stocks/{ticker}/bars",
                        headers=_alpaca_headers(), params=_p, timeout=6)
                    _n = len(_r.json().get("bars") or []) if _r.status_code == 200 else 0
                    _attempts.append(f"{_feed or 'default'}:{_r.status_code}/{_n}bars"
                                     + ("" if _r.status_code == 200 else f" {_r.text[:80]}"))
                    if _r.status_code == 200 and _n:
                        candles = [{"t": b["t"], "close": b["c"], "volume": b["v"]}
                                   for b in _r.json()["bars"]]
                        break
            except Exception as _fe:
                _attempts.append(f"exception: {type(_fe).__name__}: {_fe}")
                log.warning(f"chart fallback bars {ticker}: {_fe}")
        if len(candles) < 3:
            _LAST_CHART_ERR = f"no bars: structure=0, fallback=[{'; '.join(_attempts) if '_attempts' in dir() else 'n/a'}]"
            log.error(f"CHART RENDER FAILED for {ticker}: {_LAST_CHART_ERR}")
            return None
        ts, closes, vols = [], [], []
        for c in candles:
            try:
                _t = datetime.fromisoformat(str(c.get("t")).replace("Z", "+00:00")).astimezone(ET)
                ts.append(_t); closes.append(float(c["close"])); vols.append(float(c.get("volume") or 0))
            except Exception:
                continue
        if len(closes) < 3:
            return None

        plt.style.use("dark_background")
        fig, (ax, axv) = plt.subplots(2, 1, figsize=(8, 4.6), dpi=110,
                                      gridspec_kw={"height_ratios": [3, 1]}, sharex=True)
        ax.plot(ts, closes, linewidth=1.6)
        _pmh, _pml = st.get("pm_high"), st.get("pm_low")
        if not (_pmh or _pml):
            try:
                _kl = get_key_levels(ticker)
                _pmh, _pml = _kl.get("pmh"), _kl.get("pml")
            except Exception:
                pass
        levels = [("PMH", _pmh), ("PML", _pml),
                  ("OR-H", st.get("or_high")), ("OR-L", st.get("or_low"))]
        for name, lv in levels:
            if lv:
                ax.axhline(float(lv), linewidth=0.8, linestyle="--", alpha=0.7)
                ax.annotate(f"{name} {float(lv):.2f}", xy=(ts[0], float(lv)),
                            fontsize=7, alpha=0.9, va="bottom")
        axv.bar(ts, vols, width=0.0006)
        axv.set_ylabel("Vol", fontsize=7)
        ax.set_title(f"{ticker} — 1-min session" + (f" | {highlight[:70]}" if highlight else ""),
                     fontsize=9)
        ax.tick_params(labelsize=7); axv.tick_params(labelsize=7)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=ET))
        # tight_layout() deepcopy-recurses on Python 3.14 + older matplotlib —
        # fixed margins avoid that code path entirely on any version
        fig.subplots_adjust(left=0.09, right=0.98, top=0.90, bottom=0.12, hspace=0.18)
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        plt.close(fig)
        return buf.getvalue()
    except Exception as e:
        import traceback as _tb
        _tail = _tb.format_exc().splitlines()
        _LAST_CHART_ERR = (f"{type(e).__name__}: {e} | trace: "
                           + " || ".join(_tail[1:3] + ["..."] + _tail[-8:]))[:900]
        log.error(f"CHART RENDER FAILED for {ticker}: {_LAST_CHART_ERR}")
        return None


def post_chart_to_discord(channel: str, ticker: str, highlight: str = "",
                          caption: str = "") -> bool:
    """Attach a rendered session chart beneath a text post. Best-effort:
    returns False (and posts nothing) if rendering or upload fails."""
    png = _render_chart_png(ticker, highlight)
    if not png:
        log.error(f"CHART: no PNG produced for {ticker} — see render error above")
        return False
    channel_id = CHANNEL_IDS.get(channel)
    if not channel_id:
        log.error(f"CHART: no channel id for '{channel}'")
        return False
    try:
        import json as _json
        resp = requests.post(
            f"{DISCORD_API}/channels/{channel_id}/messages",
            headers={"Authorization": f"Bot {DISCORD_BOT_TOKEN}"},
            data={"payload_json": _json.dumps(
                {"content": (caption or f"📈 {ticker} — chart for the setup above")
                            + f"\n-# ⚙️ {_BOOT_ID}"})},
            files={"files[0]": (f"{ticker.lower()}_session.png", png, "image/png")},
            timeout=15,
        )
        ok = resp.status_code in (200, 201)
        if ok:
            log.info(f"CHART posted: {ticker} → #{channel} ({len(png)} bytes)")
        else:
            log.error(f"CHART UPLOAD FAILED {resp.status_code} for {ticker} → #{channel}: {resp.text[:300]}"
                      + (" — bot role likely missing ATTACH FILES permission" if resp.status_code == 403 else ""))
        return ok
    except Exception as e:
        log.warning(f"Chart upload error: {e}")
        return False


def post_to_discord(channel: str, message: str) -> bool:
    channel_id = CHANNEL_IDS.get(channel)
    if not channel_id:
        log.error(f"No channel ID for '{channel}'")
        return False
    h = _msg_hash(message)
    if _last_discord_hash.get(channel) == h:
        log.info(f"Dedup skip — identical message to #{channel}")
        return True
    headers = {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "Content-Type":  "application/json",
    }
    stamped = f"{message}\n-# ⚙️ {_BOOT_ID}"
    resp = requests.post(
        f"{DISCORD_API}/channels/{channel_id}/messages",
        headers=headers,
        json={"content": stamped},
        timeout=10,
    )
    if resp.status_code in (200, 201):
        _last_discord_hash[channel] = h
        log.info(f"Discord → #{channel}: {message[:80]}…")
        return True
    log.error(f"Discord post failed #{channel}: {resp.status_code} {resp.text}")
    return False


def send_emergency_dm(message: str, prefix: str = "🚨 **TPP ALERT** 🚨"):
    if not JUNIOR_USER_IDS:
        log.warning("JUNIOR_DISCORD_USER_ID not set — cannot DM")
        try:
            post_to_discord("daily-watchlist",
                            "🚨 **SYSTEM ALERT (DM not configured)** — " + message)
        except Exception:
            pass
        return
    headers = {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "Content-Type":  "application/json",
    }
    delivered = 0
    for uid in JUNIOR_USER_IDS:
        try:
            dm = requests.post(
                f"{DISCORD_API}/users/@me/channels",
                headers=headers,
                json={"recipient_id": uid},
                timeout=10,
            )
            if dm.status_code not in (200, 201):
                log.error(f"DM channel creation failed for {uid}: {dm.text}")
                continue
            requests.post(
                f"{DISCORD_API}/channels/{dm.json()['id']}/messages",
                headers=headers,
                json={"content": f"{prefix}\n{message}"},
                timeout=10,
            )
            delivered += 1
        except Exception as e:
            log.error(f"DM to {uid} failed: {e}")
    if delivered == 0:
        try:
            post_to_discord("daily-watchlist",
                            "🚨 **SYSTEM ALERT (DM delivery failed)** — " + message)
        except Exception:
            pass
    log.warning(f"Emergency DM sent to {delivered}/{len(JUNIOR_USER_IDS)} recipient(s)")


# ══════════════════════════════════════════════════════════════════════════════
#  GATE CHECKS
# ══════════════════════════════════════════════════════════════════════════════
_last_entry_signal: dict[str, datetime]    = {}
_last_commentary:   dict[str, datetime]    = {}
COMMENTARY_COOLDOWN = 300  # 5 minutes


def _gate_market_day() -> tuple[bool, str]:
    today = datetime.now(ET).date()
    if today.weekday() >= 5:
        return False, f"weekend ({today.strftime('%A')})"
    if today in MARKET_HOLIDAYS_2026:
        return False, f"market holiday ({today})"
    return True, "ok"


def _gate_window() -> tuple[bool, str]:
    """Gates NEW entries only. Does not affect position monitoring."""
    now    = datetime.now(ET)
    open_  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_ = now.replace(hour=10, minute=30, second=0, microsecond=0)
    if open_ <= now <= close_:
        return True, "ok"
    return False, f"outside 9:30–10:30 AM window ({now.strftime('%H:%M ET')})"


def _gate_ticker(ticker: str) -> tuple[bool, str]:
    if ticker in TRADEABLE_TICKERS:
        return True, "ok"
    return False, f"{ticker} not in whitelist {TRADEABLE_TICKERS}"


def _gate_blackout() -> tuple[bool, str]:
    today = datetime.now(ET).date()
    if today in FOMC_DECISION_DAYS_2026:
        return False, "FOMC decision day — full day halt"
    if os.environ.get("MANUAL_BLACKOUT", "0").strip() == "1":
        return False, "manual blackout active"
    return True, "ok"


def _gate_circuit_breaker() -> tuple[bool, str]:
    if get_circuit_breaker():
        return False, "circuit breaker — 2 consecutive losses today"
    return True, "ok"


def _gate_max_trades() -> tuple[bool, str]:
    if is_max_trades_reached():
        return False, f"max trades reached ({get_trade_count()}/2)"
    return True, "ok"


def _gate_open_position() -> tuple[bool, str]:
    pos = get_open_position()
    if pos:
        return False, f"position already open: {pos.get('occ_symbol')}"
    return True, "ok"


def _gate_cooldown(ticker: str, signal_type: str) -> tuple[bool, str]:
    now = datetime.now(ET)
    if signal_type == "entry":
        _last_entry_signal[ticker] = now
        return True, "entry signals bypass cooldown"
    last = _last_commentary.get(ticker)
    if last:
        elapsed = (now - last).total_seconds()
        if elapsed < COMMENTARY_COOLDOWN:
            return False, f"commentary cooldown — {int(COMMENTARY_COOLDOWN - elapsed)}s left"
    _last_commentary[ticker] = now
    return True, "ok"


EARNINGS_BLACKOUT = os.environ.get("EARNINGS_BLACKOUT", "")
_earnings_set = {p.strip().upper() for p in EARNINGS_BLACKOUT.split(",") if p.strip()}


def _gate_earnings(ticker: str) -> tuple[bool, str]:
    """Blocks a ticker on its earnings-day session. Env EARNINGS_BLACKOUT holds
    comma-separated TICKER:YYYY-MM-DD entries maintained weekly from the
    earnings calendar. Post-earnings gap days are allowed by design."""
    key = f"{ticker.upper()}:{datetime.now(ET).strftime('%Y-%m-%d')}"
    if key in _earnings_set:
        return False, f"earnings blackout ({key})"
    return True, "ok"


_broker_pos_cache = (0.0, [])


def _tt_open_option_positions() -> list:
    """Live option positions at the broker — the ground truth the gates trust."""
    global _broker_pos_cache
    ts, cached = _broker_pos_cache
    if time_module.time() - ts < 20:
        return cached
    try:
        resp = requests.get(
            f"{TT_BASE}/accounts/{TT_ACCOUNT}/positions",
            headers=_tt_headers(), timeout=6,
        )
        if resp.status_code != 200:
            log.warning(f"broker positions check -> {resp.status_code}")
            return cached
        items = resp.json().get("data", {}).get("items", []) or []
        live = [i for i in items
                if "option" in str(i.get("instrument-type", "")).lower()
                and abs(float(i.get("quantity") or 0)) > 0]
        _broker_pos_cache = (time_module.time(), live)
        return live
    except Exception as e:
        log.warning(f"broker positions check failed: {e}")
        return cached


def _adopt_broker_positions():
    """v11 boot step: the broker is ground truth. If the account holds an open
    option position that this process's state doesn't know about (orphaned by a
    restart/redeploy), adopt it so the monitor, trailing stop, and 3:45 flatten
    reattach. Never again does a restart leave a live position unmanaged."""
    try:
        s = load_state()
        if s.get("open_position"):
            return
        live = _tt_open_option_positions()
        if not live:
            return
        if len(live) > 1:
            log.critical(f"[{_BOOT_ID}] ADOPTION SKIPPED — broker shows "
                         f"{len(live)} open option positions; manual review needed")
            post_to_discord("daily-watchlist",
                            "⚠️ System reattached after restart and found multiple "
                            "open positions at the broker — holding off on automation, "
                            "manual review in progress.")
            return
        item = live[0]
        occ  = str(item.get("symbol", "")).strip()
        qty  = float(item.get("quantity") or 0)
        if qty <= 0 or not occ:
            log.critical(f"[{_BOOT_ID}] ADOPTION SKIPPED — unsupported position "
                         f"(qty={qty}, symbol={occ!r})")
            return
        # underlying ticker = leading alpha chars of the OCC symbol
        und = re.match(r"[A-Z]+", occ.replace(" ", ""))
        ticker = und.group(0) if und else "?"
        basis = float(item.get("average-open-price") or 0)
        if basis <= 0:
            q = _live_option_quote(occ)
            basis = float((q or {}).get("bid") or 0) or 0.01
        direction = "short" if ("P" in occ.replace(" ", "")[-9:]) else "long"
        set_open_position({
            "ticker":        ticker,
            "direction":     direction,
            "occ_symbol":    occ,
            "qty":           int(qty),
            "fill_price":    basis,
            "oco_id":        (place_oco_bracket(occ, basis, int(qty))[0]),
            "stop_order_id": None,
            "floor_trigger": round(basis * (1 - _stop_pct()), 2),
            "target_price":  round(basis * 1.40, 2),
            "peak_pnl":      0.0,
            "entry_time":    datetime.now(ET).isoformat(),
            "adopted":       True,
        })
        log.critical(f"[{_BOOT_ID}] ADOPTED broker position {occ} basis ${basis:.2f} "
                     "— monitor/trailing/3:45 flatten reattached")
        post_to_discord("daily-watchlist",
                        f"♻️ Reattached to open position **{_fmt_occ(occ)}** after a "
                        f"restart — managing per playbook (target +40%, trailing stop, "
                        f"3:45 ET flatten).")
    except Exception as e:
        log.critical(f"[{_BOOT_ID}] Position adoption failed: {e}")


def _gate_whipsaw(ticker: str, signal_type: str) -> tuple[bool, str]:
    """Chop day detector: if any single anchor has been crossed 4+ times
    in-window, the level structure is saw-toothing — entries off this ticker
    are done for the day."""
    if signal_type == "entry":
        st = _mkt_structure.get(ticker) or {}
        cc = st.get("cross_counts") or {}
        if cc:
            worst_level, worst_n = max(cc.items(), key=lambda kv: kv[1])
            if worst_n >= 4:
                return False, (f"{ticker} chop-locked — {worst_level} crossed "
                               f"{worst_n}x in-window (whipsaw day)")
    return True, "clear"


def _gate_ticker_lockout(ticker: str, signal_type: str, direction: str | None = None) -> tuple[bool, str]:
    """v12: directional. A loss on calls locks CALLS on that ticker for the day;
    puts stay eligible (and vice versa) — but re-entry needs TIER-1 conviction,
    enforced post-decision in execute path."""
    if signal_type == "entry":
        s = load_state()
        loss_dirs = (s.get("ticker_loss_dirs") or {}).get(ticker, [])
        if direction and direction in loss_dirs:
            return False, (f"{ticker} {direction.upper()}S locked out — lost on this "
                           f"direction today (opposite direction still eligible at TIER-1)")
        if not direction and int((s.get("ticker_losses") or {}).get(ticker, 0)) >= 2:
            return False, f"{ticker} locked out — two losses today"
    return True, "clear"


def _gate_no_trade_date(signal_type: str) -> tuple[bool, str]:
    if signal_type == "entry" and _no_trade_today():
        return False, f"no-trade date (FOMC/high-risk day) — entries disabled"
    return True, "clear"


def _gate_broker_flat(signal_type: str) -> tuple[bool, str]:
    """Entries require the BROKER to show no open option positions — state files
    can lie (orphans), the broker cannot. Kills stacking permanently, including
    across overlapping instances during deploys."""
    if signal_type != "entry":
        return True, "n/a"
    live = _tt_open_option_positions()
    if live:
        syms = ", ".join(_fmt_occ(str(p.get("symbol", "?"))) for p in live[:3])
        return False, f"broker shows {len(live)} open position(s): {syms}"
    return True, "flat"


def all_gates_pass(ticker: str, signal_type: str = "entry", direction: str | None = None) -> bool:
    checks = [
        ("market_day",      _gate_market_day),
        ("window",          _gate_window),
        ("ticker",          lambda: _gate_ticker(ticker)),
        ("earnings",        lambda: _gate_earnings(ticker)),
        ("no_trade_date",   lambda: _gate_no_trade_date(signal_type)),
        ("ticker_lockout",  lambda: _gate_ticker_lockout(ticker, signal_type, direction)),
        ("whipsaw",         lambda: _gate_whipsaw(ticker, signal_type)),
        ("blackout",        _gate_blackout),
        ("broker_flat",     lambda: _gate_broker_flat(signal_type)),
        ("circuit_breaker", _gate_circuit_breaker),
        ("max_trades",      _gate_max_trades),
        ("open_position",   _gate_open_position),
        ("cooldown",        lambda: _gate_cooldown(ticker, signal_type)),
    ]
    for name, fn in checks:
        passed, reason = fn()
        if not passed:
            log.info(f"GATE BLOCKED [{ticker}] [{signal_type}] — {name}: {reason}")
            return False
    log.info(f"GATE PASSED [{ticker}] [{signal_type}] — all clear → Claude")
    return True


def _in_window() -> bool:
    now = datetime.now(ET)
    return (now.hour == 9 and now.minute >= 30) or (now.hour == 10 and now.minute < 30)


# ══════════════════════════════════════════════════════════════════════════════
#  TASTYTRADE CLIENT
# ══════════════════════════════════════════════════════════════════════════════
_tt_token        = None
_tt_token_expiry = None


def _tt_get_token() -> str:
    """OAuth2 refresh-token flow (v4.1 method). Password sessions fail on servers."""
    global _tt_token, _tt_token_expiry
    now = datetime.now(ET)
    if _tt_token and _tt_token_expiry and now < _tt_token_expiry:
        return _tt_token
    resp = requests.post(
        f"{TT_BASE}/oauth/token",
        json={
            "grant_type":    "refresh_token",
            "refresh_token": os.environ["TASTYTRADE_REFRESH_TOKEN"],
            "client_id":     os.environ["TASTYTRADE_CLIENT_ID"],
            "client_secret": os.environ["TASTYTRADE_CLIENT_SECRET"],
        },
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Tastytrade OAuth failed {resp.status_code}: {resp.text}")
    body             = resp.json()
    _tt_token        = body["access_token"]
    expires_in       = int(body.get("expires_in", 900))
    _tt_token_expiry = now + timedelta(seconds=max(expires_in - 120, 300))
    log.info("Tastytrade access token refreshed — LIVE mode | Account: " + TT_ACCOUNT)
    return _tt_token


def _tt_headers() -> dict:
    return {"Authorization": "Bearer " + _tt_get_token(), "Content-Type": "application/json"}


def _next_friday() -> date:
    today = datetime.now(ET).date()
    now   = datetime.now(ET)
    days  = (4 - today.weekday()) % 7 or 7
    if today.weekday() == 3 and now.hour >= 14:
        days += 7
    return today + timedelta(days=days)


def _spot_price(ticker: str) -> float | None:
    """Last trade from Alpaca with feed fallback; quote midpoint backup. (IEX quotes are sparse for some symbols; TT market-data is 403.)"""
    key = os.environ.get("ALPACA_API_KEY", "")
    sec = os.environ.get("ALPACA_API_SECRET") or os.environ.get("ALPACA_SECRET_KEY", "")
    hdr = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": sec}
    for feed in ("iex", "sip", None):
        try:
            params = {"feed": feed} if feed else {}
            r = requests.get(f"https://data.alpaca.markets/v2/stocks/{ticker}/trades/latest",
                             headers=hdr, params=params, timeout=5)
            if r.status_code == 200:
                px = float(r.json().get("trade", {}).get("p") or 0)
                if px:
                    return px
        except Exception as e:
            log.warning(f"spot trade {ticker} feed={feed}: {e}")
    for feed in ("iex", "sip", None):
        try:
            params = {"feed": feed} if feed else {}
            r = requests.get(f"https://data.alpaca.markets/v2/stocks/{ticker}/quotes/latest",
                             headers=hdr, params=params, timeout=5)
            if r.status_code == 200:
                q = r.json().get("quote", {})
                a = float(q.get("ap") or 0)
                b = float(q.get("bp") or 0)
                if a and b:
                    return (a + b) / 2
        except Exception as e:
            log.warning(f"spot quote {ticker} feed={feed}: {e}")
    log.error(f"No spot price for {ticker} from any source")
    return None

def _live_option_quote(occ_symbol: str) -> dict | None:
    """Option NBBO from Alpaca OPRA (included in Algo Trader Plus).
    Tastytrade REST market-data returns 403 for this OAuth grant, so quotes
    come from Alpaca; orders still go to Tastytrade with the padded symbol."""
    try:
        compact = occ_symbol.replace(" ", "")
        key = os.environ.get("ALPACA_API_KEY", "")
        sec = os.environ.get("ALPACA_API_SECRET") or os.environ.get("ALPACA_SECRET_KEY", "")
        resp = requests.get(
            "https://data.alpaca.markets/v1beta1/options/quotes/latest",
            headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": sec},
            params={"symbols": compact},
            timeout=5,
        )
        if resp.status_code == 200:
            q = resp.json().get("quotes", {}).get(compact)
            if q:
                return {"bid": q.get("bp"), "ask": q.get("ap")}
            log.warning("alpaca option quote empty for " + compact)
        else:
            log.warning("alpaca option quote http " + str(resp.status_code) + " for " + compact)
    except Exception as e:
        log.error(f"Quote fetch failed for {occ_symbol}: {e}")
    return None

def _build_occ(ticker: str, expiry: date, strike: float, opt_type: str) -> str:
    return f"{ticker}{expiry.strftime('%y%m%d')}{opt_type}{int(strike * 1000):08d}"


def select_contract(ticker: str, direction: str) -> tuple:
    """ATM->OTM walk. Parses nested chain data.items[].expirations[].strikes[], uses strike own OCC."""
    expiry   = _next_friday()
    exp_str  = expiry.strftime("%Y-%m-%d")
    side_key = "call" if direction == "call" else "put"
    spot = _spot_price(ticker)
    if not spot:
        log.error(f"No spot price for {ticker}")
        return None, None, None
    resp = requests.get(
        f"{TT_BASE}/option-chains/{ticker}/nested",
        headers=_tt_headers(),
        params={"expiration-date": exp_str},
        timeout=10,
    )
    if resp.status_code != 200:
        log.error(f"Chain fetch failed for {ticker}: {resp.status_code}")
        return None, None, None
    items = resp.json().get("data", {}).get("items", [])
    strike_map = {}
    for it in items:
        for exp in it.get("expirations", []):
            if exp.get("expiration-date") != exp_str:
                continue
            for stk in exp.get("strikes", []):
                sym = stk.get(side_key)
                try:
                    sp = float(stk.get("strike-price"))
                except (TypeError, ValueError):
                    continue
                if sym and sp:
                    strike_map[sp] = sym  # TT-native symbol (padded, keep spaces)
    if not strike_map:
        log.error(f"No strikes parsed for {ticker} {exp_str} items={len(items)}")
        return None, None, None
    strikes = sorted(strike_map.keys())
    atm     = min(strikes, key=lambda s: abs(s - spot))
    ai      = strikes.index(atm)
    ordered = strikes[ai:] if direction == "call" else list(reversed(strikes[:ai + 1]))

    # v13.2 COMMUNITY-FIRST SELECTION: the premium band IS the rule.
    # Only contracts asking $%.2f–$%.2f qualify (small-account friendly —
    # members can mirror every trade). Closest-to-money strike inside the
    # band wins; sizing then buys multiple contracts within the tier
    # allocation. Nothing in band within MAX_OTM_PCT of spot = NO TRADE.
    quotes = []
    for strike in ordered:
        occ   = strike_map[strike]
        quote = _live_option_quote(occ)
        if not quote:
            continue
        ask = float(quote.get("ask") or 0)
        bid = float(quote.get("bid") or 0)
        if ask <= 0:
            continue
        spread = (ask - bid) / ask if ask else 1
        quotes.append((strike, occ, ask, spread))
        if ask < MIN_PREMIUM:
            break   # premiums below the band floor — no point walking further

    for strike, occ, ask, spread in quotes:   # ordered = closest-to-ATM first
        if spread >= MAX_SPREAD:
            continue
        if not (MIN_PREMIUM <= ask <= MAX_PREMIUM):
            continue
        if abs(strike - spot) / spot > MAX_OTM_PCT:
            continue
        log.info(f"Contract {occ} ask {ask} (in-band ${MIN_PREMIUM}-${MAX_PREMIUM}, "
                 f"{abs(strike - spot) / spot:.1%} OTM)")
        return occ, strike, ask
    log.warning(f"No contract in the ${MIN_PREMIUM}-${MAX_PREMIUM} band for {ticker} "
                f"{direction} within {MAX_OTM_PCT:.0%} OTM — trade will be passed")
    return None, None, None

_last_order_error = None
_last_order_id    = None
_last_order_ask   = None
_boot_ts          = time_module.time()


def _tt_place_order(payload: dict) -> str | None:
    global _last_order_error, _last_order_id
    _last_order_error = None
    _last_order_id    = None
    resp = requests.post(
        f"{TT_BASE}/accounts/{TT_ACCOUNT}/orders",
        headers=_tt_headers(),
        json=payload,
        timeout=10,
    )
    if resp.status_code not in (200, 201):
        log.error(f"Order failed {resp.status_code}: {resp.text}")
        try:
            _err = resp.json().get("error", {})
            _msgs = [e.get("message", "") for e in _err.get("errors", []) if e.get("message")]
            _last_order_error = "; ".join(_msgs) or _err.get("message") or f"HTTP {resp.status_code}"
        except Exception:
            _last_order_error = f"HTTP {resp.status_code}"
        return None
    order_id = str(resp.json()["data"]["order"]["id"])
    log.info(f"Order placed → ID {order_id}")
    _last_order_id = order_id
    return order_id


def _extract_fill_price(order: dict) -> float | None:
    """Fill price from any of the places Tastytrade puts it: leg average,
    per-fill records (weighted), or order-level average. Today's live fill
    (486701499) proved status parses while the price field we read is empty."""
    def _f(v):
        try:
            x = float(v)
            return x if x > 0 else None
        except (TypeError, ValueError):
            return None
    for leg in (order.get("legs") or []):
        p = _f(leg.get("average-fill-price"))
        if p:
            return p
        tot_q = tot_pq = 0.0
        for fl in (leg.get("fills") or []):
            fp, fq = _f(fl.get("fill-price")), _f(fl.get("quantity"))
            if fp and fq:
                tot_pq += fp * fq
                tot_q  += fq
        if tot_q:
            return tot_pq / tot_q
    return _f(order.get("average-fill-price"))


_MONTHS = ["", "JAN", "FEB", "MARCH", "APRIL", "MAY", "JUNE",
           "JULY", "AUG", "SEPT", "OCT", "NOV", "DEC"]


def _fmt_occ(occ: str) -> str:
    """OCC symbol -> member-readable: 'NVDA  260724P00205000' -> 'NVDA JULY 24 $205 PUT'."""
    try:
        sym = occ.split()[0]
        tail = occ.replace(" ", "")[len(sym):]
        mo, dy = int(tail[2:4]), int(tail[4:6])
        cp = "CALL" if tail[6].upper() == "C" else "PUT"
        strike = int(tail[7:]) / 1000
        strike_s = f"${strike:.1f}".rstrip("0").rstrip(".") if strike % 1 else f"${int(strike)}"
        return f"{sym} {_MONTHS[mo]} {dy} {strike_s} {cp}"
    except Exception:
        return occ


def _tt_order_status(order_id: str) -> tuple[str, float | None]:
    """One robust status check. Handles both response shapes (order directly
    under data, or nested under data.order) and case-insensitive statuses.
    Returns (status_lower, fill_price_or_None)."""
    try:
        resp = requests.get(
            f"{TT_BASE}/accounts/{TT_ACCOUNT}/orders/{order_id}",
            headers=_tt_headers(), timeout=5,
        )
        if resp.status_code != 200:
            return f"http_{resp.status_code}", None
        d = resp.json().get("data", {}) or {}
        order = d.get("order", d)
        status = str(order.get("status", "")).strip().lower()
        return status, _extract_fill_price(order)
    except Exception as e:
        return f"error_{e.__class__.__name__}", None


def _resolve_filled_price(order_id: str, fill: float | None) -> float:
    """A confirmed-filled order MUST yield a price. Fall back to the ask we
    placed at (close enough for stop/target math) rather than ever orphaning."""
    if fill:
        log.info(f"FILLED {order_id} @ ${fill:.2f}/share (${fill*100:.0f}/contract)")
        return fill
    fb = _last_order_ask or 0.0
    log.warning(f"FILLED {order_id} but price unreadable — using placement ask ${fb:.2f} as fill estimate")
    return fb


def _tt_poll_fill(order_id: str, timeout: int) -> float | None:
    deadline = time_module.time() + timeout
    first = True
    while time_module.time() < deadline:
        status, fill = _tt_order_status(order_id)
        if first:
            log.info(f"Order {order_id} first poll status: {status}")
            first = False
        if status == "filled":
            return _resolve_filled_price(order_id, fill)
        if status in ("cancelled", "canceled", "rejected", "expired"):
            log.warning(f"Order {order_id}: {status}")
            return None
        time_module.sleep(2)
    # Timeout: one FINAL check. Cancel ONLY a still-working order — a filled
    # order is accepted (with fallback pricing), never cancelled, never orphaned.
    status, fill = _tt_order_status(order_id)
    if status == "filled":
        return _resolve_filled_price(order_id, fill)
    if status in ("live", "received", "routed", "in-flight", "contingent"):
        log.warning(f"Order {order_id} timed out working (status={status}) — cancelling")
        _tt_cancel_order(order_id)
    else:
        log.warning(f"Order {order_id} timed out in state '{status}' — NOT cancelling; check broker")
    return None


def _tt_cancel_order(order_id: str):
    requests.delete(
        f"{TT_BASE}/accounts/{TT_ACCOUNT}/orders/{order_id}",
        headers=_tt_headers(), timeout=5,
    )
    log.info(f"Cancelled order {order_id}")


def enter_trade(occ_symbol: str, qty: int = 1) -> float | None:
    order_id = _tt_place_order({
        "time-in-force": "Day",
        "order-type":    "Market",
        "legs": [{"instrument-type": "Equity Option", "symbol": occ_symbol,
                  "quantity": qty, "action": "Buy to Open"}],
    })
    return _tt_poll_fill(order_id, FILL_TIMEOUT) if order_id else None


def _tick(px: float, side: str = "nearest") -> float:
    """Snap a price to Tastytrade's valid increment grid: $0.05 at/above $3.00,
    $0.01 below. side='down' floors (use for sell limits so the price stays
    marketable); 'up' ceils; default rounds to nearest."""
    if px < 3.00:
        return round(px, 2)
    import math
    if side == "down":
        return round(math.floor(px / 0.05) * 0.05, 2)
    if side == "up":
        return round(math.ceil(px / 0.05) * 0.05, 2)
    return round(round(px / 0.05) * 0.05, 2)


def place_oco_bracket(occ_symbol: str, fill_price: float, qty: int = 1,
                      trigger_override: float | None = None) -> tuple[str | None, str | None]:
    """v11.7: rest BOTH exits at the broker as one OCO complex order —
    a limit sell at the +40% target and a stop-market underneath. Either leg
    filling cancels the other atomically at the broker. Returns
    (complex_order_id, None) on success, (None, None) on failure (caller
    falls back to plain stop + software target — the pre-v11.7 behavior)."""
    target  = _tick(fill_price * 1.40)
    trigger = _tick(trigger_override) if trigger_override else _tick(fill_price * (1 - _stop_pct()))
    payload = {
        "type": "OCO",
        "orders": [
            {
                "time-in-force": "Day",
                "order-type":    "Limit",
                "price":         str(target),
                "price-effect":  "Credit",
                "legs": [{"instrument-type": "Equity Option", "symbol": occ_symbol,
                          "quantity": qty, "action": "Sell to Close"}],
            },
            {
                "time-in-force": "Day",
                "order-type":    "Stop",
                "stop-trigger":  str(trigger),
                "legs": [{"instrument-type": "Equity Option", "symbol": occ_symbol,
                          "quantity": qty, "action": "Sell to Close"}],
            },
        ],
    }
    try:
        r = requests.post(f"{TT_BASE}/accounts/{TT_ACCOUNT}/complex-orders",
                          headers=_tt_headers(), json=payload, timeout=10)
        if r.status_code in (200, 201):
            cid = str(r.json().get("data", {}).get("complex-order", {}).get("id")
                      or r.json().get("data", {}).get("id") or "")
            if cid:
                log.info(f"OCO bracket placed: target ${target} / stop ${trigger} → complex {cid}")
                return cid, None
        log.error(f"OCO bracket rejected {r.status_code}: {r.text[:300]}")
    except Exception as e:
        log.error(f"OCO bracket failed: {e}")
    return None, None


def _cancel_complex_order(complex_id: str | None):
    if not complex_id:
        return
    try:
        requests.delete(f"{TT_BASE}/accounts/{TT_ACCOUNT}/complex-orders/{complex_id}",
                        headers=_tt_headers(), timeout=10)
        log.info(f"Complex order {complex_id} cancelled")
    except Exception as e:
        log.warning(f"Complex cancel {complex_id}: {e}")


def _complex_order_fill(complex_id: str) -> tuple[str | None, float | None]:
    """Inspect an OCO's legs. Returns ('target'|'stop', fill_price) if a leg
    filled, else (None, None)."""
    try:
        r = requests.get(f"{TT_BASE}/accounts/{TT_ACCOUNT}/complex-orders/{complex_id}",
                         headers=_tt_headers(), timeout=8)
        if r.status_code != 200:
            return None, None
        data = r.json().get("data", {})
        orders = (data.get("complex-order", data) or {}).get("orders", []) or []
        for o in orders:
            if str(o.get("status", "")).lower() == "filled":
                kind = "stop" if o.get("stop-trigger") else "target"
                fills = []
                for leg in o.get("legs", []) or []:
                    for f in leg.get("fills", []) or []:
                        try:
                            fills.append((float(f.get("fill-price") or 0),
                                          float(f.get("quantity") or 0)))
                        except Exception:
                            pass
                if fills:
                    tq = sum(q for _, q in fills) or 1
                    px = sum(p * q for p, q in fills) / tq
                else:
                    px = None
                return kind, px
    except Exception as e:
        log.warning(f"Complex status {complex_id}: {e}")
    return None, None


def place_stop_loss(occ_symbol: str, fill_price: float, qty: int = 1,
                    trigger_override: float | None = None) -> str | None:
    sp       = _stop_pct()
    trigger  = _tick(trigger_override) if trigger_override else _tick(fill_price * (1 - sp))
    order_id = _tt_place_order({
        "time-in-force": "Day",
        "order-type":    "Stop",
        "stop-trigger":  str(trigger),
        "legs": [{"instrument-type": "Equity Option", "symbol": occ_symbol,
                  "quantity": qty, "action": "Sell to Close"}],
    })
    if order_id:
        log.info(f"Stop-market: trigger=${trigger} → {order_id}")
    else:
        log.critical(f"STOP LOSS FAILED for {occ_symbol} — MANUAL INTERVENTION NEEDED")
    return order_id


def close_position_tt(occ_symbol: str, reason: str, current_bid: float, qty: int = 1) -> float:
    log.info(f"Closing {occ_symbol} — {reason}")
    order_id = _tt_place_order({
        "time-in-force": "Day",
        "order-type":    "Limit",
        "price":         str(_tick(current_bid, side="down")),
        "price-effect":  "Credit",
        "legs": [{"instrument-type": "Equity Option", "symbol": occ_symbol,
                  "quantity": qty, "action": "Sell to Close"}],
    })
    fill = _tt_poll_fill(order_id, CLOSE_TIMEOUT) if order_id else None
    if not fill:
        log.warning(f"Limit close not filled in {CLOSE_TIMEOUT}s — escalating to market")
        if order_id:
            _tt_cancel_order(order_id)
        mkt_id = _tt_place_order({
            "time-in-force": "Day",
            "order-type":    "Market",
            "legs": [{"instrument-type": "Equity Option", "symbol": occ_symbol,
                      "quantity": qty, "action": "Sell to Close"}],
        })
        fill = _tt_poll_fill(mkt_id, FILL_TIMEOUT) if mkt_id else None
    if fill:
        return fill
    # No confirmed fill. Ask the broker whether we are actually flat before
    # declaring ANY outcome — a fabricated exit price detaches state from
    # reality (the phantom-exit bug class).
    try:
        still_open = any(str(p.get("symbol", "")).replace(" ", "") == occ_symbol.replace(" ", "")
                         and float(p.get("quantity") or 0) > 0
                         for p in _tt_open_option_positions())
    except Exception:
        still_open = True   # cannot verify -> assume open, never fake an exit
    if still_open:
        log.critical(f"CLOSE FAILED for {occ_symbol} — position still open at broker")
        send_emergency_dm(f"CLOSE FAILED — {occ_symbol} still open at broker. Will retry next tick.")
        return None
    log.warning(f"Close fill unconfirmed but broker shows flat — using ${current_bid:.2f} approx")
    return current_bid


def cancel_resting_stop(stop_order_id: str | None):
    if stop_order_id:
        _tt_cancel_order(stop_order_id)
        log.info(f"Resting stop {stop_order_id} cancelled")


# ══════════════════════════════════════════════════════════════════════════════
#  POSITION MONITOR
#  Runs past 10:30 AM if position open — no forced flatten at window close
# ══════════════════════════════════════════════════════════════════════════════
def monitor_open_position():
    pos = get_open_position()
    if not pos:
        return

    now        = datetime.now(ET)
    occ        = pos["occ_symbol"]
    fill_price = float(pos["fill_price"])
    entry_time = datetime.fromisoformat(pos["entry_time"])
    peak_pnl   = float(pos.get("peak_pnl", 0.0))
    _q         = int(pos.get("qty") or 1)

    # v11.5: if the resting broker stop has FILLED, that IS the exit — tell
    # members immediately, reconcile state, record the loss. Never let a
    # stop fill pass silently.
    _oco = pos.get("oco_id")
    if _oco:
        _kind, _px = _complex_order_fill(_oco)
        if _kind:
            exit_px = float(_px or (fill_price * 1.40 if _kind == "target"
                                    else float(pos.get("floor_trigger") or fill_price * (1 - _stop_pct()))))
            pnl_d = (exit_px - fill_price) * 100 * _q
            pnl_p = (exit_px - fill_price) / fill_price
            if _kind == "target":
                head = f"🎯 **TARGET HIT — {_fmt_occ(occ)}**"
                body = (f"The +40% profit target filled at the broker — position CLOSED "
                        f"@ ${exit_px:.2f} ({pnl_p:+.1%}). If you followed this trade, "
                        f"take your profit NOW if you haven't already. 📈")
            elif exit_px > fill_price:
                head = f"🔒 **TRAILING FLOOR HIT — {_fmt_occ(occ)}**"
                body = (f"The ratcheted stop filled at the broker and LOCKED IN the gain — "
                        f"position CLOSED @ ${exit_px:.2f} ({pnl_p:+.1%}). If you followed "
                        f"this trade, close NOW if you haven't already.")
            else:
                head = f"🛑 **STOP HIT — {_fmt_occ(occ)}**"
                body = (f"The stop-loss triggered at the broker and the position is CLOSED "
                        f"@ ${exit_px:.2f} ({pnl_p:+.1%}). If you followed this trade, close "
                        f"your contracts NOW if you haven't already. Loss taken per the risk "
                        f"plan — that's the stop doing its job.")
            post_to_discord("day-trade-signals", f"{head}\n{body}")
            post_to_discord(
                "profits-and-recaps",
                f"{'✅' if exit_px > fill_price else '❌'} **{_fmt_occ(occ)} CLOSED** — "
                f"{'TARGET' if _kind == 'target' else ('TRAILING FLOOR' if exit_px > fill_price else 'STOP LOSS')} (broker OCO)\n"
                f"Entry: ${fill_price:.2f} → Exit: ${exit_px:.2f} "
                f"({datetime.now(ET).strftime('%H:%M ET')})\n"
                f"P&L: {pnl_p:+.1%} ({'+' if pnl_d >= 0 else ''}${abs(pnl_d):.0f} total on {_q} contract(s))",
            )
            clear_open_position()
            record_trade_result(win=(exit_px > fill_price), ticker=pos.get("ticker"),
                                pnl_pct=pnl_p, pnl_dollar=pnl_d,
                        direction=pos.get("direction"))
            log.info(f"OCO {_kind} filled — {occ} @ ${exit_px:.2f} ({pnl_p:+.1%})")
            return

    _sid = pos.get("stop_order_id")
    if _sid:
        try:
            _st, _sfill = _tt_order_status(_sid)
        except Exception:
            _st, _sfill = None, None
        if str(_st).lower() == "filled":
            exit_px = float(_sfill or fill_price * (1 - _stop_pct()))
            pnl_d   = (exit_px - fill_price) * 100 * _q
            pnl_p   = (exit_px - fill_price) / fill_price
            post_to_discord(
                "day-trade-signals",
                f"🛑 **STOP HIT — {_fmt_occ(occ)}**\n"
                f"The stop-loss triggered at the broker and the position is CLOSED "
                f"@ ${exit_px:.2f} ({pnl_p:+.1%}). If you followed this trade, close your "
                f"contracts NOW if you haven't already. Loss taken per the risk plan — "
                f"that's the stop doing its job.",
            )
            post_to_discord(
                "profits-and-recaps",
                f"❌ **{_fmt_occ(occ)} CLOSED** — STOP LOSS (broker)\n"
                f"Entry: ${fill_price:.2f} → Exit: ${exit_px:.2f} "
                f"({datetime.now(ET).strftime('%H:%M ET')})\n"
                f"P&L: {pnl_p:+.1%} ({'+' if pnl_d >= 0 else ''}${abs(pnl_d):.0f} total on {_q} contract(s))",
            )
            clear_open_position()
            record_trade_result(win=(exit_px > fill_price), ticker=pos.get("ticker"),
                                pnl_pct=pnl_p, pnl_dollar=pnl_d,
                        direction=pos.get("direction"))
            log.info(f"Broker stop filled — {occ} @ ${exit_px:.2f} ({pnl_p:+.1%})")
            return

    quote = _live_option_quote(occ)
    if not quote:
        log.warning(f"No quote for {occ} — skipping monitor tick")
        return

    current_bid = float(quote.get("bid") or 0)
    if current_bid <= 0:
        return

    pnl_pct = (current_bid - fill_price) / fill_price

    if pnl_pct > peak_pnl:
        peak_pnl        = pnl_pct
        pos["peak_pnl"] = peak_pnl
        set_open_position(pos)

    # v13.1 RECOVERY RULE: a trade that dipped <=-10% and clawed back above
    # flat gets its stop raised to -10% — a comeback never re-tests -20%.
    _trough = float(pos.get("trough_pnl") or 0.0)
    if pnl_pct < _trough:
        _trough = pnl_pct
        pos["trough_pnl"] = _trough
        set_open_position(pos)
    if (_trough <= -0.10 and pnl_pct > 0.0
            and int(pos.get("ratchet_stage") or 0) == 0
            and not pos.get("recovery_locked")):
        _rec_floor = _tick(fill_price * 0.90)
        _cur_floor = float(pos.get("floor_trigger") or fill_price * (1 - _stop_pct()))
        if _rec_floor > _cur_floor:
            _ok_r = False
            if pos.get("oco_id"):
                _cancel_complex_order(pos.get("oco_id"))
                _new_oco, _ = place_oco_bracket(occ, fill_price, _q, trigger_override=_rec_floor)
                if _new_oco:
                    pos["oco_id"] = _new_oco
                    _ok_r = True
                else:
                    _sid_r = place_stop_loss(occ, fill_price, _q, trigger_override=_rec_floor)
                    pos["oco_id"] = None
                    pos["stop_order_id"] = _sid_r
                    _ok_r = bool(_sid_r)
                    send_emergency_dm(f"RECOVERY: OCO re-place failed for {occ}; "
                                      f"plain stop {'set' if _sid_r else 'ALSO FAILED'} at ${_rec_floor:.2f}")
            elif pos.get("stop_order_id"):
                cancel_resting_stop(pos.get("stop_order_id"))
                _sid_r = place_stop_loss(occ, fill_price, _q, trigger_override=_rec_floor)
                pos["stop_order_id"] = _sid_r
                _ok_r = bool(_sid_r)
            if _ok_r:
                pos["recovery_locked"] = True
                pos["floor_trigger"] = _rec_floor
                post_to_discord(
                    "day-trade-signals",
                    f"🔄 **{_fmt_occ(occ)} — comeback protected.**\n"
                    f"This trade dipped {_trough:+.1%} and fought back green — stop raised to "
                    f"${_rec_floor:.2f} (-10%). A recovered trade never re-tests the full stop.",
                )
            set_open_position(pos)

    # v12.1 MULTI-TIER PROFIT PROTECTION (3 rungs, max 3 stop replacements):
    #   Stage 1: peak >= +10%  -> stop to BREAK-EVEN
    #   Stage 2: peak >= +20%  -> stop to ENTRY +10%   (was +25% — trades kept
    #            peaking ~+20% and scratching out at breakeven)
    #   Stage 3: peak >= +30%  -> stop to ENTRY +20%
    _LADDER = [(0.10, 0.10 - LOCK_BUFFER, "+10% locked — first green win guaranteed"),
               (0.20, 0.20 - LOCK_BUFFER, "+20% locked — solid base gain banked"),
               (0.30, 0.30 - LOCK_BUFFER, "+30% locked — the bulk of this move is protected")]
    _stage = int(pos.get("ratchet_stage") or 0)
    _want  = 0
    for _k, (_thr, _lock, _msg) in enumerate(_LADDER, start=1):
        if peak_pnl >= _thr:
            _want = _k
    if _want > _stage:
        _thr, _lock, _msg = _LADDER[_want - 1]
        _desired = _tick(fill_price * (1 + _lock))
        _ok = False
        if pos.get("oco_id"):
            _cancel_complex_order(pos.get("oco_id"))
            _new_oco, _ = place_oco_bracket(occ, fill_price, _q, trigger_override=_desired)
            if _new_oco:
                pos["oco_id"] = _new_oco
                _ok = True
            else:
                _sid2 = place_stop_loss(occ, fill_price, _q, trigger_override=_desired)
                pos["oco_id"] = None
                pos["stop_order_id"] = _sid2
                _ok = bool(_sid2)
                send_emergency_dm(f"TIER {_want}: OCO re-place failed for {occ}; "
                                  f"plain stop {'set' if _sid2 else 'ALSO FAILED'} at ${_desired:.2f}")
        elif pos.get("stop_order_id"):
            cancel_resting_stop(pos.get("stop_order_id"))
            _sid2 = place_stop_loss(occ, fill_price, _q, trigger_override=_desired)
            pos["stop_order_id"] = _sid2
            _ok = bool(_sid2)
        if _ok:
            pos["ratchet_stage"] = _want
            pos["floor_trigger"] = _desired
            _take = ("Green and comfortable? Take profits — don't wait for us."
                     if _want >= 2 else
                     "Feel free to add your own trail stop or start taking profits "
                     "whenever you're comfortable — you don't have to wait for the +40% target.")
            post_to_discord(
                "day-trade-signals",
                f"🔔 **{_fmt_occ(occ)} — {pnl_pct:+.1%}, protection raised (stage {_want}/{len(_LADDER)}).**\n"
                f"Stop moved to ${_desired:.2f} — {_msg}.\n{_take}",
            )
        set_open_position(pos)

    # v11.7: in-trade update every 30 minutes so members are never in the dark
    _lu = float(pos.get("last_update_ts") or 0)
    if time_module.time() - _lu >= int(os.environ.get("UPDATE_INTERVAL_MIN", "10")) * 60:
        pos["last_update_ts"] = time_module.time()
        set_open_position(pos)
        post_to_discord(
            "day-trade-signals",
            f"📊 **Open trade update — {_fmt_occ(occ)}**\n"
            f"Bid ${current_bid:.2f} | P&L {pnl_pct:+.1%} (entry ${fill_price:.2f}) | "
            f"peak {peak_pnl:+.1%} | protective floor ${float(pos.get('floor_trigger') or fill_price * (1 - _stop_pct())):.2f}\n"
            f"Target ${round(fill_price * 1.40, 2):.2f} — managed automatically; exit call posts the moment anything fires."
            + ("\n💰 Green — taking profits early is always allowed. Our target is +40%, but your gains are yours."
               if pnl_pct >= 0.10 else ""),
        )

    reason = None

    # 0. Forced flatten — 3:45 PM ET normally, 12:45 on early-close days.
    #    Nothing is ever held past the close.
    _fh, _fm = _flatten_time(now.date())
    if now.time() >= dtime(_fh, _fm):
        reason = "EOD FLATTEN 3:45 ET (no overnight holds)"

    # 1. HARD STOP — software enforcement of the max loss, independent of the
    #    resting broker stop (covers rejected stops and gaps through the trigger)
    elif pnl_pct <= -_stop_pct():
        reason = f"HARD STOP {-_stop_pct():+.0%} (software)"

    # 2. Profit target +40%
    elif pnl_pct >= 0.40:
        reason = "PROFIT TARGET +40%"

    # 2. Trailing stop — arms at +10%, trails 15% below peak
    elif peak_pnl >= 0.10 and pnl_pct <= (peak_pnl - 0.15):
        reason = f"TRAILING STOP (peak {peak_pnl:+.1%})"

    # 3. 10-min chop circuit
    else:
        mins_in = (now - entry_time).total_seconds() / 60
        if mins_in >= 10 and (abs(pnl_pct) < 0.05 or pnl_pct <= -0.15):
            reason = "CHOP CIRCUIT (stagnant or -15%)"

    if reason:
        _tag = ("🎯 TAKE PROFIT" if "PROFIT TARGET" in reason
                else "🔒 LOCK IT IN" if "TRAILING" in reason
                else "🛑 STOP" if "HARD STOP" in reason
                else "⏰ CLOSING" )
        post_to_discord(
            "day-trade-signals",
            f"{_tag} — **{_fmt_occ(occ)}**\n"
            f"Close your contracts NOW — {reason}. "
            f"Current bid ~${current_bid:.2f} ({pnl_pct:+.1%} from ${fill_price:.2f} entry). "
            f"Exact fill posts in #profits-and-recaps in a moment.",
        )
        _cancel_complex_order(pos.get("oco_id"))
        cancel_resting_stop(pos.get("stop_order_id"))
        exit_price    = close_position_tt(occ, reason, current_bid, _q)
        if exit_price is None:
            # Close did not go through — position stays tracked; re-arm a stop
            # so we are never unprotected while retrying, and try again next tick.
            if pos.get("stop_order_id"):
                pos["stop_order_id"] = place_stop_loss(occ, fill_price, _q)
                set_open_position(pos)
            return
        pnl_dollar    = (exit_price - fill_price) * 100 * _q
        pnl_pct_final = (exit_price - fill_price) / fill_price
        emoji         = "✅" if exit_price > fill_price else "❌"
        close_time    = datetime.now(ET).strftime("%H:%M ET")
        post_to_discord(
            "profits-and-recaps",
            f"{emoji} **{_fmt_occ(occ)} CLOSED** — {reason}\n"
            f"Entry: ${fill_price:.2f} → Exit: ${exit_price:.2f} ({close_time})\n"
            f"P&L: {pnl_pct_final:+.1%} "
            f"({'+' if pnl_dollar >= 0 else ''}${abs(pnl_dollar):.0f} total on "
            f"{int(pos.get('qty') or 1)} contract(s))",
        )
        clear_open_position()
        record_trade_result(win=(exit_price > fill_price), ticker=pos.get("ticker"),
                            pnl_pct=pnl_pct_final, pnl_dollar=pnl_dollar,
                        direction=pos.get("direction"))
        log.info(f"Position closed — {occ} | {reason} | {pnl_pct_final:+.1%}")


# ══════════════════════════════════════════════════════════════════════════════
#  CLAUDE BRAIN — v5.0 SYSTEM PROMPT
# ══════════════════════════════════════════════════════════════════════════════
_claude_client    = anthropic_sdk.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=60.0, max_retries=1)
_claude_calls     = 0
_claude_call_date = None

SYSTEM_PROMPT = """\
You are Junior 2.0, the automated trading brain for The Portfolio Plug (TPP).
You analyze real-time 1-minute chart data for the tracked tickers and decide whether
to enter an options trade during the 9:30–10:30 AM ET trading window.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CORE IDENTITY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Tickers traded: only the tickers presented in the data. No other tickers ever.
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
  1. Volume >= 1.2x the 20-candle average on the breakout candle
  2. Price breaking PMH (calls) or PML (puts) with a confirmed 1-min close
  3. 8 EMA above 21 EMA (calls) or below (puts), price has not closed
     back through the 21 EMA

All three present → APPROVE, tag [TIER-1].

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONDITION B — CHOP / LOW VOLUME TREND (still tradeable)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Markets trend on low volume — this is normal and valid.

  SETUP 1 — CALLS (chop / uptrend):
    - PMH break confirmed by TradingView alert
    - Price making HH/HL sequence on 1-min
    - Price compressing between the 8 and 21 EMA
    - No 1-min candle has closed below the 21 EMA
    - Entry trigger: 1-min candle closes back above the 8 EMA
    → APPROVE, tag [TIER-2]

  SETUP 2 — PUTS (chop / downtrend):
    - PML break confirmed by TradingView alert
    - Price making LH/LL sequence on 1-min
    - Price compressing between the 8 and 21 EMA
    - No 1-min candle has closed above the 21 EMA
    - Entry trigger: 1-min candle closes back below the 8 EMA
    → APPROVE, tag [TIER-2]

Volume is a confirming factor in Condition B, not a hard gate.
Consistent directional closes on low volume = valid setup.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONDITION C — GAP-DAY ANCHORS (pre-market levels + opening range)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
When price opens away from PMH/PML (gap day), the SESSION STRUCTURE
anchors are equally valid trigger levels with the SAME break-and-hold
standard as Conditions A/B:
  - PRE-MARKET HIGH / PRE-MARKET LOW (today 4:00-9:29 battle lines)
  - OPENING-RANGE HIGH / LOW (first 5 minutes, locked at 9:35)

A recorded break DOWN through any of these anchors with price holding
below it = PUTS. A recorded reclaim/break UP with price holding above
it = CALLS. Recorded crosses in SESSION STRUCTURE are authoritative.
Confirmation = current candle holding beyond the anchor; volume is
supportive but not a hard gate. Tag [TIER-2].

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONDITION D — OVERSOLD REVERSAL AT SUPPORT (and overbought mirror)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
After an extended flush well below the day open / opening-range low:
  - A clear reversal candle prints AT a support reference
    (pre-market low, session-low retest, prior-day low, round number)
  - The reversal candle closes in its upper third
  - The next candle holds the reclaim (does not close back below the
    reversal candle midpoint)
  → CALLS on that confirmation, tag [TIER-2].
Mirror logic for an extended rip rejecting at resistance → PUTS.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SETUP LIBRARY — RESTORED FROM THE FULL PLAYBOOK (evaluate alongside A-D)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Use the INDICATORS line and candle history in SESSION STRUCTURE.
If indicators show "warming up", rely on Conditions A-D only.

SETUP 1 — PMH/PML MOMENTUM BREAKOUT
  1-min candle CLOSES above PMH (calls) / below PML (puts).
  Trigger-candle volume >= 1.5x the 10-candle average.
  RSI14 < 68 for calls / > 32 for puts (ignore RSI if volume >= 3.0x).
  Price above both 8 & 21 EMA (calls) or below both (puts).
  Marubozu exception: volume > 3.0x with a full-body candle = enter
  immediately, cap target at +15%, stop to entry after 30 seconds.
  → APPROVE [TIER-1]

SETUP 2 — EMA BOUNCE / STRUCTURAL RETEST
  Bullish engulfing or pin-bar REJECTION off the 8 or 21 EMA (calls;
  mirror for puts), or a retest-and-hold of a previously broken
  PMH/PML/anchor. NEVER on a mere touch - require the confirmation
  candle. → APPROVE [TIER-2]

SETUP 3 — BULL FLAG / ASCENDING TRIANGLE (CALLS)
  Strong initial leg up, then downward-sloping consolidation.
  Early entry: flag bottom tests 8/21 EMA AND RSI14 dips below ~50.
  Breakout entry: 1-min candle breaks the flag upper trendline or
  flat-top resistance with expanding volume. → APPROVE [TIER-2]

SETUP 4 — BEAR FLAG / DESCENDING TRIANGLE (PUTS)
  Strong initial leg down, then upward-drifting consolidation.
  Early entry: flag top rejects off 8/21 EMA AND RSI14 recovers to ~50.
  Breakdown entry: 1-min candle breaks the flag lower support or
  flat-bottom with expanding volume. → APPROVE [TIER-2]

SETUP 5 — TREND CONTINUATION (EMA RIDE)
  Consistent HH/HL (calls) or LH/LL (puts) with clean 8 EMA bounces.
  REQUIRED: the 8/21 EMA spread must be WIDENING. Flat or tangling
  EMAs = chop = NO TRADE under this setup. Enter the minor
  consolidations along the 8 EMA. → APPROVE [TIER-2]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DEAD TICKER — THE ONLY VALID NO_TRADE REASON
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A ticker is only "explicitly dead" when ALL of these are true together:
  - Volume below 0.4x the 20-candle average for 3+ consecutive 1-min candles
  - Candle range (high minus low) below 0.3x ATR(14) on those same candles
  - No directional structure — no HH/HL or LH/LL visible on the 1-min

If price is making consistent directional closes → NOT dead.
Choppy price action → NOT dead.
Slow trend on low volume → NOT dead.
Low volume alone → NOT dead.
"Choppy" is NEVER a standalone NO_TRADE reason.

NO_TRADE is only permitted when:
  - FOMC decision day (system will not call you on these days)
  - Active circuit breaker (system will not call you when tripped)
  - ALL tracked tickers are explicitly dead per all 3 conditions above

If only one ticker is dead, analyze the other.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TIER TAGS & SIGNAL FORMAT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[TIER-1]  Clean Condition A — post signal immediately
[TIER-2]  Condition B — add warning emoji, note lower confluence

Signal description (you write this — execution engine handles the rest):
  - 1-2 sentences max in Junior's voice
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
  "setup_description": "NVDA reclaimed PMH at 127.40 on the 1-min with HH/HL structure intact — calls are live.",
  "chart_request": {"ticker": "NVDA", "timeframe": "1m", "indicators": "PMH,VOLUME", "highlight": "Reclaim of PMH 127.40 with rising volume"}
}

chart_request is REQUIRED on every APPROVE: it must reference the exact ticker,
the timeframe your claim is based on (1m|5m|15m|1h|1d), the indicators/levels
your setup_description cites (comma-separated), and a short note on what a
chart should highlight to PROVE the claim. Every observational claim you make
(a break, a reclaim, a volume spike, structure) must be verifiable from that
chart request — never cite a phenomenon the chart could not show.

NO_TRADE:
{
  "decision": "NO_TRADE",
  "reason": "Both tickers explicitly dead — flat range, no structure, volume collapsed."
}

direction must be exactly "call" or "put".
tier must be exactly "TIER-1" or "TIER-2".
"""


def _claude_budget_ok() -> bool:
    global _claude_calls, _claude_call_date
    today = datetime.now(ET).date()
    if _claude_call_date != today:
        _claude_calls     = 0
        _claude_call_date = today
    if _claude_calls >= MAX_DAILY_CALLS:
        log.warning(f"Daily Claude cap ({MAX_DAILY_CALLS}) reached")
        return False
    return True


# -- session market structure (in-window memory) --------------------------------
_mkt_structure: dict = {}


def _hydrate_structure():
    global _mkt_structure
    try:
        s = load_state()
        saved = s.get("mkt_structure") or {}
        today = datetime.now(ET).strftime("%Y-%m-%d")
        _mkt_structure = {k: v for k, v in saved.items() if isinstance(v, dict) and v.get("date") == today}
    except Exception as e:
        log.warning("structure hydrate failed: " + str(e))


def _persist_structure():
    try:
        s = load_state()
        s["mkt_structure"] = _mkt_structure
        _commit(s)
    except Exception as e:
        log.warning("structure persist failed: " + str(e))


def _premarket_levels(ticker: str, day: str | None = None) -> dict:
    """Pre-market high/low from 1-min bars 04:00-09:29 ET (EDT offsets)."""
    day = day or datetime.now(ET).strftime("%Y-%m-%d")
    start = day + "T08:00:00Z"
    end   = day + "T13:29:59Z"
    for feed in ("sip", "iex", None):
        try:
            params = {"timeframe": "1Min", "start": start, "end": end, "limit": 1000}
            if feed:
                params["feed"] = feed
            r = requests.get(
                f"https://data.alpaca.markets/v2/stocks/{ticker}/bars",
                headers=_alpaca_headers(),
                params=params,
                timeout=6,
            )
            if r.status_code == 200:
                bars = r.json().get("bars", [])
                if bars:
                    return {"high": max(b["h"] for b in bars), "low": min(b["l"] for b in bars)}
        except Exception as e:
            log.warning("premarket bars " + ticker + " feed=" + str(feed) + ": " + str(e))
    return {"high": None, "low": None}

def _update_structure(ticker: str, candle: dict, pmh, pml, day: str | None = None) -> dict:
    """Track day open, running H/L, candle history, and in-window level crosses."""
    today = day or datetime.now(ET).strftime("%Y-%m-%d")
    st = _mkt_structure.get(ticker)
    if not st or st.get("date") != today:
        st = {"date": today,
              "day_open": candle.get("open"),
              "day_high": candle.get("high"),
              "day_low":  candle.get("low"),
              "candles":  [],
              "crosses":  {}}
        _mkt_structure[ticker] = st
        try:
            _pmv = _premarket_levels(ticker, day=today)
            st["pm_high"] = _pmv.get("high")
            st["pm_low"]  = _pmv.get("low")
            if st["pm_high"]:
                log.info("STRUCTURE: " + ticker + " pre-market range " + str(st["pm_low"]) + "-" + str(st["pm_high"]))
        except Exception as _pe:
            log.warning("premarket levels failed " + ticker + ": " + str(_pe))
    try:
        if candle.get("high") is not None:
            st["day_high"] = max(st.get("day_high") or candle["high"], candle["high"])
        if candle.get("low") is not None:
            st["day_low"] = min(st.get("day_low") or candle["low"], candle["low"])
    except Exception:
        pass
    try:
        if not st.get("or_locked"):
            _bt = str(candle.get("t") or "")
            _bmin = None
            if _bt:
                _bdt = datetime.fromisoformat(_bt.replace("Z", "+00:00")).astimezone(ET)
                _bmin = _bdt.hour * 60 + _bdt.minute
            if _bmin is not None and 570 <= _bmin < 575:
                if candle.get("high") is not None:
                    st["or_high"] = max(st.get("or_high") or candle["high"], candle["high"])
                if candle.get("low") is not None:
                    st["or_low"] = min(st.get("or_low") or candle["low"], candle["low"])
            _nowm = _bmin if _bmin is not None else (datetime.now(ET).hour * 60 + datetime.now(ET).minute)
            if _nowm >= 575 and st.get("or_high") is not None and not st.get("or_locked"):
                st["or_locked"] = True
                log.info("STRUCTURE: " + ticker + " opening range locked " + str(st.get("or_low")) + "-" + str(st.get("or_high")))
    except Exception:
        pass
    ct = str(candle.get("t") or "")
    last = st["candles"][-1] if st.get("candles") else None
    is_new = (not last) or (not ct) or (str(last.get("t") or "") != ct)
    if is_new:
        prev_close = last.get("close") if last else candle.get("open")
        try:
            _ct2 = str(candle.get("t") or "")
            now_hm = (datetime.fromisoformat(_ct2.replace("Z", "+00:00"))
                      .astimezone(ET).strftime("%H:%M")) if _ct2 else datetime.now(ET).strftime("%H:%M")
        except Exception:
            now_hm = datetime.now(ET).strftime("%H:%M")
        cur_close = candle.get("close")
        def _cross(level, key_dn, key_up, dn_lbl, up_lbl):
            if not level or prev_close is None or cur_close is None:
                return
            _base = key_dn.rsplit("_", 2)[0]
            if (prev_close >= level > cur_close) or (prev_close <= level < cur_close):
                cc = st.setdefault("cross_counts", {})
                cc[_base] = int(cc.get(_base, 0)) + 1
            if prev_close >= level > cur_close and key_dn not in st["crosses"]:
                st["crosses"][key_dn] = now_hm
                log.info("STRUCTURE: " + ticker + " " + dn_lbl + " " + str(level) + " at " + now_hm + " ET")
            if prev_close <= level < cur_close and key_up not in st["crosses"]:
                st["crosses"][key_up] = now_hm
                log.info("STRUCTURE: " + ticker + " " + up_lbl + " " + str(level) + " at " + now_hm + " ET")
        _cross(pml, "pml_break_down", "pml_reclaim_up", "BROKE DOWN through PML", "RECLAIMED UP through PML")
        _cross(pmh, "pmh_reject_down", "pmh_break_up", "rejected back below PMH", "BROKE UP through PMH")
        _cross(st.get("pm_low"), "pmlow_break_down", "pmlow_reclaim_up", "BROKE DOWN through PRE-MARKET LOW", "RECLAIMED UP through PRE-MARKET LOW")
        _cross(st.get("pm_high"), "pmhigh_reject_down", "pmhigh_break_up", "rejected below PRE-MARKET HIGH", "BROKE UP through PRE-MARKET HIGH")
        if st.get("or_locked"):
            _cross(st.get("or_low"), "orl_break_down", "orl_reclaim_up", "BROKE DOWN through OPENING-RANGE LOW", "RECLAIMED UP through OPENING-RANGE LOW")
            _cross(st.get("or_high"), "orh_reject_down", "orh_break_up", "rejected below OPENING-RANGE HIGH", "BROKE UP through OPENING-RANGE HIGH")
        st["candles"] = (st.get("candles", []) + [candle])[-30:]
        _persist_structure()
    return st


def _indicators(candles: list) -> dict:
    """8/21 EMA, RSI(14), 10-candle volume avg, EMA-spread widening from 1-min candles."""
    out = {"ema8": None, "ema21": None, "rsi14": None, "spread_widening": None}
    try:
        closes = [c.get("close") for c in candles if c.get("close") is not None]
        vols = [c.get("volume") or 0 for c in candles]
        n = len(closes)
        def _ema_of(series, period):
            if len(series) < period:
                return None
            k = 2.0 / (period + 1)
            e = sum(series[:period]) / float(period)
            for x in series[period:]:
                e = x * k + e * (1 - k)
            return e
        e8 = _ema_of(closes, 8)
        e21 = _ema_of(closes, 21)
        out["ema8"] = round(e8, 2) if e8 is not None else None
        out["ema21"] = round(e21, 2) if e21 is not None else None
        if n >= 15:
            gains = 0.0
            losses = 0.0
            for i in range(n - 14, n):
                d = closes[i] - closes[i - 1]
                if d >= 0:
                    gains += d
                else:
                    losses -= d
            avg_l = losses / 14.0
            avg_g = gains / 14.0
            out["rsi14"] = 100.0 if avg_l == 0 else round(100.0 - 100.0 / (1.0 + avg_g / avg_l), 1)
        if len(vols) >= 10:
            out["vol_avg10"] = int(sum(vols[-10:]) / 10.0)
            out["last_vol"] = vols[-1]
        if e8 is not None and e21 is not None:
            out["spread"] = round(abs(e8 - e21), 3)
            p8 = _ema_of(closes[:-3], 8)
            p21 = _ema_of(closes[:-3], 21)
            if p8 is not None and p21 is not None:
                out["spread_widening"] = abs(e8 - e21) > abs(p8 - p21)
    except Exception as e:
        log.warning("indicators failed: " + str(e))
    return out

def _structure_context(ticker: str, pmh, pml) -> str:
    st = _mkt_structure.get(ticker)
    if not st:
        return ""
    L = ["SESSION STRUCTURE (authoritative in-window memory - a recorded cross DID occur in-window even if price has since moved; judge Condition A/B break-and-hold from these events, do NOT require witnessing the cross on the current candle):"]
    L.append("  Day open: " + str(st.get("day_open")) + " | Session high: " + str(st.get("day_high")) + " | Session low: " + str(st.get("day_low")))
    cr = st.get("crosses", {})
    cc = st.get("cross_counts", {})
    def _line(name, level, dn, up, dn_lbl, up_lbl):
        if not level:
            return
        ev = []
        if dn in cr:
            ev.append(dn_lbl + " at " + cr[dn] + " ET")
        if up in cr:
            ev.append(up_lbl + " at " + cr[up] + " ET")
        _n = int(cc.get(dn.rsplit("_", 2)[0], 0))
        cnt = (" [crossed " + str(_n) + "x in-window" + (" — CHOP ZONE, no entries off this anchor" if _n >= 3 else "") + "]") if _n else ""
        L.append("  " + name + " " + str(level) + ": " + ("; ".join(ev) if ev else "not crossed in-window yet") + cnt)
    _line("PML", pml, "pml_break_down", "pml_reclaim_up", "BROKEN DOWN", "RECLAIMED UP")
    _line("PMH", pmh, "pmh_reject_down", "pmh_break_up", "rejected back down", "BROKEN UP")
    _line("PRE-MARKET LOW", st.get("pm_low"), "pmlow_break_down", "pmlow_reclaim_up", "BROKEN DOWN", "RECLAIMED UP")
    _line("PRE-MARKET HIGH", st.get("pm_high"), "pmhigh_reject_down", "pmhigh_break_up", "rejected back down", "BROKEN UP")
    if st.get("or_locked"):
        _line("OPENING-RANGE LOW", st.get("or_low"), "orl_break_down", "orl_reclaim_up", "BROKEN DOWN", "RECLAIMED UP")
        _line("OPENING-RANGE HIGH", st.get("or_high"), "orh_reject_down", "orh_break_up", "rejected back down", "BROKEN UP")
    elif st.get("or_high") is not None:
        L.append("  Opening range: forming (locks 9:35) currently " + str(st.get("or_low")) + "-" + str(st.get("or_high")))
    cs = st.get("candles", [])
    try:
        ind = _indicators(cs)
        if ind.get("ema8") is not None:
            _sw = ind.get("spread_widening")
            _swtxt = " WIDENING" if _sw else (" narrowing/flat" if _sw is not None else "")
            L.append("  INDICATORS (1-min): 8EMA " + str(ind.get("ema8")) + " | 21EMA " + str(ind.get("ema21")) + " | RSI14 " + str(ind.get("rsi14")) + " | last vol " + str(ind.get("last_vol")) + " vs 10-avg " + str(ind.get("vol_avg10")) + " | EMA spread " + str(ind.get("spread")) + _swtxt)
        elif cs:
            L.append("  INDICATORS: warming up (" + str(len(cs)) + " candles so far - EMAs/RSI need 8/21/15)")
    except Exception:
        pass
    if cs:
        L.append("  Recent 1-min candles (O/H/L/C/V, oldest first):")
        for c in cs[-8:]:
            L.append("    " + str(c.get("open")) + "/" + str(c.get("high")) + "/" + str(c.get("low")) + "/" + str(c.get("close")) + "/" + str(c.get("volume")))
    return chr(10).join(L)

def _build_claude_prompt(alert_data: dict, session: dict, as_of: str | None = None) -> str:
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

    now = as_of or datetime.now(ET).strftime("%H:%M ET")
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
  today_ticker_losses: {session.get('ticker_losses', {})} (loss directions: {session.get('ticker_loss_dirs', {})})
  consecutive_losses: {session.get('consecutive_losses', 0)}
  circuit_breaker:    {session.get('circuit_breaker', False)}
  open_position:      {session.get('open_position') is not None}

{_structure_context(ticker, pmh, pml)}

Analyze the 1-min setup on {ticker} and return your JSON decision.

OUTPUT CONTRACT (mandatory):
- Respond with ONLY the JSON object, starting with {{ — no preamble, no prose, no code fences.
- The "reason" field is REQUIRED for BOTH APPROVE and NO_TRADE: 1-2 sentences in Junior's voice naming the condition/setup and the level. Never leave it empty.

ENTRY FRESHNESS RULE:
- A break-based entry (Condition A, Condition C, Setup 1) is only valid if the triggering cross occurred within the LAST 5 candles, or price is retesting the broken level right now.
- Do not chase an extended move: if the break happened earlier and price has already traveled far from the level and sits at/near session extremes, that is NO_TRADE unless a fresh Condition D reversal or a new break prints.

REJECTION-THEN-RECLAIM GUARD:
- If the SAME anchor recorded a rejection within the last 3 candles, do NOT approve an entry through that anchor off a single candle. Require the CURRENT candle AND the PRIOR candle to both close beyond the anchor before treating it as a confirmed break/reclaim. One-candle flips at a contested level are chop, not confirmation.

POST-LOSS RE-ENTRY RULE:
- SESSION STATE shows today's losses per ticker and direction. After a loss on a ticker: the SAME direction is dead for the day. The OPPOSITE direction is allowed ONLY as TIER-1 — a decisive break of VWAP or the day's high/low with clearly above-average volume. An ordinary Tier-2 setup on a ticker that already cost us money today is NO_TRADE.

WHIPSAW RULE:
- CROSS COUNTS in the structure show how many times each anchor has been crossed in-window today. An anchor crossed 3 or more times is a CHOP ZONE — break or reclaim entries off that anchor are NO_TRADE for the rest of the session, no matter how clean the current candle looks. A level that keeps changing hands is churning, not breaking.
""".strip()


def _extract_json_object(text: str) -> dict:
    """Parse the first complete JSON object found anywhere in the model's reply.
    The model sometimes prefixes prose ("Looking at this setup: ...") or wraps
    the JSON in ``` fences; the old strict parser raised on both and silently
    ate decisions on exactly the bars where a setup was forming."""
    t = text.strip()
    if t.startswith("```"):
        parts = t.split("```")
        t = parts[1] if len(parts) > 1 else t
        if t[:4].lower() == "json":
            t = t[4:]
        t = t.strip()
    i = t.find("{")
    if i == -1:
        raise json.JSONDecodeError("no JSON object found", t[:50] or " ", 0)
    depth, in_str, esc = 0, False, False
    for j in range(i, len(t)):
        ch = t[j]
        if esc:
            esc = False
            continue
        if ch == "\\":
            esc = in_str
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return json.loads(t[i:j + 1])
    raise json.JSONDecodeError("unbalanced JSON object", t[:50] or " ", 0)


def call_claude(alert_data: dict, session: dict, replay: bool = False, as_of: str | None = None) -> dict | None:
    global _claude_calls
    if not replay and not _claude_budget_ok():
        return None
    prompt = _build_claude_prompt(alert_data, session, as_of=as_of)
    last_err = None
    for attempt in (1, 2):
        try:
            response = _claude_client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1024,
                system=[{
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{
                    "role":    "user",
                    "content": prompt,
                }],
            )
            if not replay:
                _claude_calls += 1
            raw = response.content[0].text.strip()
            log.info(f"Claude raw: {raw[:200]}")
            decision = _extract_json_object(raw)
            log.info(
                f"Claude: {decision.get('decision')} | "
                f"ticker={decision.get('ticker')} | "
                f"direction={decision.get('direction')} | "
                f"tier={decision.get('tier')}"
            )
            return decision
        except json.JSONDecodeError as e:
            last_err = f"JSON parse failed: {e}"
            log.error(f"Claude {last_err} (attempt {attempt}/2)")
        except Exception as e:
            last_err = f"API call failed: {e}"
            log.error(f"Claude {last_err} (attempt {attempt}/2)")
        if attempt == 1:
            time_module.sleep(1.5)
    log.error(f"Claude decision unrecoverable after 2 attempts: {last_err}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  EXECUTION ENGINE
# ══════════════════════════════════════════════════════════════════════════════
def execute_trade(ticker: str, direction: str, claude_decision: dict) -> bool:
    log.info(f"{'='*50}")
    log.info(f"EXECUTE: {ticker} {direction.upper()}")
    log.info(f"{'='*50}")

    tier  = claude_decision.get("tier", "TIER-1")
    setup = claude_decision.get("setup_description", "")

    # v12: directional lockout + higher-bar re-entry, enforced with the decision in hand
    _ok, _why = _gate_ticker_lockout(ticker, "entry", direction)
    if not _ok:
        log.info(f"BLOCKED post-decision: {_why}")
        post_to_discord(
            "daily-watchlist",
            f"📚 **Setup we passed on — {ticker} {direction.upper()}** [{tier}]\n"
            f"{setup}\n**Passed:** {_why}.",
        )
        return False
    _s_now = load_state()
    if int((_s_now.get("ticker_losses") or {}).get(ticker, 0)) >= 1 and str(tier).upper() != "TIER-1":
        _why = (f"post-loss re-entry on {ticker} requires TIER-1 conviction "
                f"(VWAP / day high-low break with above-average volume) — this signal is {tier}")
        log.info(f"BLOCKED post-decision: {_why}")
        post_to_discord(
            "daily-watchlist",
            f"📚 **Setup we passed on — {ticker} {direction.upper()}** [{tier}]\n"
            f"{setup}\n**Passed:** {_why}.",
        )
        return False

    occ, strike, ask = select_contract(ticker, direction)
    global _last_order_ask
    _last_order_ask = ask
    if occ:
        post_to_discord(
            "day-trade-signals",
            f"📤 **Order placed — Buying {_fmt_occ(occ)} @ ~${ask:.2f}**\n"
            f"Waiting on fill confirmation from the broker…",
        )
    if not occ:
        post_to_discord(
            "day-trade-signals",
            f"⚠️ Setup identified on **{ticker} {direction.upper()}** "
            f"but no contract available in the $75–$150 range. Passing on this one.",
        )
        post_to_discord(
            "daily-watchlist",
            f"📚 **Setup we passed on — {ticker} {direction.upper()}** [{tier}]\n"
            + (setup if setup else "Level-break setup confirmed by the model.")
            + "\nPassed only because no contract fit the $0.75–$1.50 premium rule at selection time.",
        )
        return False

    _entry_q = _live_option_quote(occ)
    _entry_ask = float((_entry_q or {}).get("ask") or 0)
    qty = _position_size(_entry_ask)
    if qty == 0:
        _r = _risk()
        _cash = _sizing_cash() or 0
        log.info(f"ABORT: {occ} ask ${_entry_ask:.2f} exceeds {_r['tier']} allocation "
                 f"({_r['alloc']:.1%} of ${_cash:.0f})")
        post_to_discord(
            "daily-watchlist",
            f"📚 **Setup we passed on — {ticker} {direction.upper()}**\n"
            f"Valid signal, but the contract (${_entry_ask:.2f} ≈ ${_entry_ask * 100:.0f}) "
            f"exceeds our {_r['tier']} allocation cap ({_r['alloc']:.1%} of account cash). "
            f"Sizing rules are never overridden — no trade.",
        )
        return False
    fill_price = enter_trade(occ, qty)
    if not fill_price and _last_order_id and not _last_order_error:
        # Order reached the broker but confirmation failed — never walk away.
        _st, _fp = _tt_order_status(_last_order_id)
        if _st == "filled":
            fill_price = _resolve_filled_price(_last_order_id, _fp)
            log.warning(f"ADOPTED orphan fill {_last_order_id} @ ${fill_price:.2f}")
        else:
            send_emergency_dm(
                f"UNCONFIRMED ORDER — {_fmt_occ(occ)} ({occ}) "
                f"order ID {_last_order_id} status={_st}. CHECK BROKER NOW — "
                f"a fill here is NOT tracked and has NO stop loss."
            )
    if not fill_price:
        if _last_order_error:
            # Order REJECTED by broker — never reached the market. Keep the
            # signals channel clean; ops detail goes to daily-watchlist + DM.
            post_to_discord(
                "daily-watchlist",
                f"⚠️ Signal fired on **{ticker} {direction.upper()}** [{tier}] but the "
                f"broker rejected the order — {_last_order_error} No position opened.",
            )
            send_emergency_dm(
                f"ORDER REJECTED — {ticker} {direction.upper()} {occ}: {_last_order_error}"
            )
        else:
            post_to_discord(
                "day-trade-signals",
                f"⚠️ Order placed for **{_fmt_occ(occ)}** — fill NOT confirmed by the broker. "
                f"No position recorded. Do not chase this one until a fill confirmation posts.",
            )
        return False

    log.info(f"FILLED: {occ} @ ${fill_price:.2f}/share")

    oco_id, _ = place_oco_bracket(occ, fill_price, qty)
    stop_order_id = None if oco_id else place_stop_loss(occ, fill_price, qty)
    stop_verified = False
    if oco_id:
        stop_verified = True   # bracket accepted atomically by the broker
    if stop_order_id:
        try:
            _sst, _ = _tt_order_status(stop_order_id)
            stop_verified = str(_sst).lower() not in ("rejected", "cancelled", "canceled", "expired", "error", "unknown")
        except Exception:
            stop_verified = True   # order id exists; verification glitch is not a reason to flatten
    if not stop_verified:
        # No confirmed protection at the broker -> we do not hold the position. Period.
        send_emergency_dm(
            f"STOP FAILED — {occ} filled @ ${fill_price:.2f} x{qty} — AUTO-FLATTENING NOW "
            f"({_last_order_error or 'stop order not working at broker'})"
        )
        _q2 = _live_option_quote(occ) or {}
        _bid2 = float(_q2.get("bid") or 0) or round(fill_price * 0.98, 2)
        _exit = close_position_tt(occ, "STOP PLACEMENT FAILED — auto-flatten (no unprotected positions)", _bid2, qty)
        if _exit is None:
            send_emergency_dm(f"EMERGENCY: {occ} has NO STOP and auto-flatten FAILED — CLOSE MANUALLY NOW")
            post_to_discord("day-trade-signals",
                            f"⚠️ **{_fmt_occ(occ)}** entry filled but protective orders are failing at the broker — "
                            f"manual intervention in progress. Do not follow this entry.")
            set_open_position({"ticker": ticker, "direction": direction, "occ_symbol": occ,
                               "qty": qty, "fill_price": fill_price, "stop_order_id": None,
                               "target_price": round(fill_price * 1.40, 2), "peak_pnl": 0.0,
                               "entry_time": datetime.now(ET).isoformat()})
            return True
        _pnl_d = round((_exit - fill_price) * 100 * qty, 2)
        clear_open_position()
        post_to_discord(
            "day-trade-signals",
            f"⚠️ **{ticker} {('CALL' if direction == 'call' else 'PUT')} — entry filled @ ${fill_price:.2f} x{qty}, "
            f"but the stop-loss order was rejected by the broker.**\n"
            f"Safety rule: no position is ever held without a working stop — closed immediately @ ${_exit:.2f} "
            f"({'+' if _pnl_d >= 0 else ''}${abs(_pnl_d):.0f}). Investigating the rejection; standing down on this signal.",
        )
        log.critical(f"STOP-FAIL AUTO-FLATTEN complete for {occ}: entry ${fill_price:.2f} exit ${_exit:.2f}")
        return True

    arrow      = "🟢" if direction == "call" else "🔴"
    type_label = "CALL" if direction == "call" else "PUT"
    target     = round(fill_price * 1.40, 2)
    stop       = round(fill_price * (1 - _stop_pct()), 2)
    cost       = round(fill_price * 100 * qty,  2)
    stop_lbl   = f"-{int(_stop_pct()*100)}%"
    entry_time = datetime.now(ET).strftime("%H:%M ET")

    post_to_discord(
        "day-trade-signals",
        f"@everyone\n\n"
        f"{arrow} **{ticker} {type_label}** [{tier}]\n\n"
        f"**Contract:** {_fmt_occ(occ)}\n"
        f"✅ **Fill confirmed:** ${fill_price:.2f}/share × {qty} contract(s) (${cost:.0f} total) @ {entry_time}\n"
        f"**Target:** ${target:.2f} (+40%)\n"
        + (f"**Bracket resting at broker ✅** — target ${target:.2f} & stop ${stop:.2f} ({stop_lbl}) as one OCO: "
           f"either fills, the other cancels\n"
           if oco_id else
           f"**Stop:** ${stop:.2f} ({stop_lbl}) — resting at broker ✅\n")
        + "Protection: recovery rule (dip ≤-10% then back green → stop -10%) | locks AT +10% / +20% / +30% | target +40%. "
          "Take profits early whenever you're comfortable — the target is ours, the gains are yours\n\n"
        f"{setup}",
    )
    log.info("Signal posted to #day-trade-signals")
    _cr = (claude_decision.get("chart_request") or {}) if isinstance(claude_decision, dict) else {}
    try:
        post_chart_to_discord("day-trade-signals", ticker,
                              highlight=str(_cr.get("highlight", "")) if _cr else "",
                              caption=f"📈 **{ticker}** — the chart behind this setup")
    except Exception:
        pass
    _cr_line = ""
    if _cr:
        _cr_line = ("\n[CHART_REQUEST: TICKER=" + str(_cr.get("ticker", ticker))
                    + ", TIMEFRAME=" + str(_cr.get("timeframe", "1m"))
                    + ", INDICATORS=" + str(_cr.get("indicators", ""))
                    + ", HIGHLIGHT=\"" + str(_cr.get("highlight", ""))[:120] + "\"]")
    post_to_discord(
        "daily-watchlist",
        f"📚 **Setup breakdown — {ticker} {type_label}** [{tier}]\n"
        + (setup if setup else "Level-break setup confirmed.")
        + f"\nEntry ${fill_price:.2f} x{qty} | Target ${target:.2f} (+40%) | Stop ${stop:.2f} ({stop_lbl})"
        + _cr_line,
    )

    set_open_position({
        "ticker":        ticker,
        "direction":     direction,
        "occ_symbol":    occ,
        "qty":           qty,
        "fill_price":    fill_price,
        "stop_order_id": stop_order_id,
        "oco_id":        oco_id,
        "floor_trigger": round(fill_price * (1 - _stop_pct()), 2),
        "ratchet_stage": 0,
        "trough_pnl":    0.0,
        "recovery_locked": False,
        "last_update_ts": time_module.time(),
        "target_price":  round(fill_price * 1.40, 2),
        "peak_pnl":      0.0,
        "entry_time":    datetime.now(ET).isoformat(),
    })
    increment_trade_count()
    log.info(f"Trade complete — {occ} live | stop={stop_order_id}")
    return True


# ══════════════════════════════════════════════════════════════════════════════
#  ALPACA DATA HELPERS (REST only — no WebSocket)
# ══════════════════════════════════════════════════════════════════════════════
def _alpaca_headers() -> dict:
    return {
        "APCA-API-KEY-ID":     os.environ.get("ALPACA_API_KEY", ""),
        "APCA-API-SECRET-KEY": os.environ.get("ALPACA_API_SECRET")
                               or os.environ.get("ALPACA_SECRET_KEY", ""),
    }


def _fetch_1min_bars(ticker: str, start_utc_iso: str, end_utc_iso: str | None = None,
                     limit: int = 500) -> list:
    """1-min bars ascending from an EXPLICIT start. Alpaca defaults start=midnight
    + sort=asc, so any call without start returns the FIRST bars of the day —
    the bug that fed a frozen 4 AM candle to the scanner for a week."""
    for feed in ("iex", "sip", None):
        try:
            params = {"timeframe": "1Min", "start": start_utc_iso,
                      "limit": limit, "sort": "asc"}
            if end_utc_iso:
                params["end"] = end_utc_iso
            if feed:
                params["feed"] = feed
            resp = requests.get(
                f"https://data.alpaca.markets/v2/stocks/{ticker}/bars",
                headers=_alpaca_headers(),
                params=params,
                timeout=6,
            )
            if resp.status_code == 200:
                bars = resp.json().get("bars", [])
                if bars:
                    return bars
            else:
                log.warning(f"1min bars {ticker} feed={feed} -> {resp.status_code}")
        except Exception as e:
            log.warning(f"1min bars {ticker} feed={feed}: {e}")
    return []


def get_latest_1min_candle(ticker: str) -> dict | None:
    """TRUE latest completed 1-min bar (explicit start 30 min back, take last)."""
    start = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    bars = _fetch_1min_bars(ticker, start, limit=40)
    if bars:
        b = bars[-1]
        return {"open": b["o"], "high": b["h"], "low": b["l"],
                "close": b["c"], "volume": b["v"], "t": b.get("t")}
    return None


def _ingest_new_bars(ticker: str, pmh, pml) -> dict | None:
    """Fetch every 1-min bar since 9:30 ET (or since the last seen bar) and replay
    each NEW bar through _update_structure in order. This is the ONLY way the
    structure engine sees the market — one bar per tick loses bars whenever a
    tick is slow or skipped. Returns the newest candle."""
    st = _mkt_structure.get(ticker)
    day = datetime.now(ET).strftime("%Y-%m-%d")
    floor = day + "T13:30:00Z"  # 9:30 EDT — session bars only
    if st and st.get("candles"):
        # purge any pre-open seed candles (e.g. from /structure-test hits)
        st["candles"] = [c for c in st["candles"] if str(c.get("t") or "") >= floor]
    last_t = ""
    if st and st.get("candles"):
        last_t = str(st["candles"][-1].get("t") or "")
    start = last_t if (last_t and last_t >= floor) else floor
    bars = _fetch_1min_bars(ticker, start, limit=200)
    newest = None
    for b in bars:
        bt = str(b.get("t") or "")
        if last_t and bt <= last_t:
            continue
        c = {"open": b["o"], "high": b["h"], "low": b["l"],
             "close": b["c"], "volume": b["v"], "t": bt}
        _update_structure(ticker, c, pmh, pml)
        newest = c
    if newest:
        return newest
    st = _mkt_structure.get(ticker)
    return st["candles"][-1] if st and st.get("candles") else None

def get_key_levels(ticker: str) -> dict:
    """PMH/PML/prev_close from last COMPLETED daily bar (explicit start: Alpaca defaults start=today -> empty pre-open)."""
    start = (datetime.now(ET).date() - timedelta(days=7)).isoformat()
    today_str = datetime.now(ET).strftime("%Y-%m-%d")
    for feed in ("iex", "sip", None):
        try:
            params = {"timeframe": "1Day", "limit": 10, "start": start}
            if feed:
                params["feed"] = feed
            resp = requests.get(
                f"https://data.alpaca.markets/v2/stocks/{ticker}/bars",
                headers=_alpaca_headers(),
                params=params,
                timeout=5,
            )
            if resp.status_code == 200:
                bars = resp.json().get("bars", [])
                prev = None
                for b in reversed(bars):
                    if not str(b.get("t", "")).startswith(today_str):
                        prev = b
                        break
                if prev:
                    return {"pmh": prev["h"], "pml": prev["l"], "prev_close": prev["c"]}
            else:
                log.warning(f"key levels {ticker} feed={feed} -> {resp.status_code}")
        except Exception as e:
            log.warning(f"key levels {ticker} feed={feed}: {e}")
    log.error(f"Key levels failed on all feeds for {ticker}")
    return {"pmh": None, "pml": None, "prev_close": None}

def get_daily_levels(ticker: str) -> dict:
    levels = get_key_levels(ticker)
    return {
        "pmh":        levels.get("pmh"),
        "pml":        levels.get("pml"),
        "prev_close": None,
        "prev_open":  None,
        "avg_volume": None,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  SCHEDULER — background thread
# ══════════════════════════════════════════════════════════════════════════════
_scheduler_started = False
_scheduler_lock    = threading.Lock()


def _post_scanning_update(label: str, closing: str):
    """Claude-written live scanning update for #daily-watchlist — educational, real levels."""
    try:
        rows = []
        for _tk in TICKERS:
            _lvl = get_key_levels(_tk)
            _cd  = get_latest_1min_candle(_tk)
            _px  = (_cd or {}).get("close") or _spot_price(_tk)
            if _px and _lvl.get("pmh") and _lvl.get("pml"):
                rows.append(_tk + ": price=$" + str(_px) + " PMH=$" + str(_lvl["pmh"]) + " PML=$" + str(_lvl["pml"]))
        if not rows:
            post_to_discord("daily-watchlist", "Scanning " + ", ".join(TICKERS) + " — waiting on confirmed level breaks. " + closing)
            return
        _pr = (
            "You are Junior from The Portfolio Plug posting a mid-window scanning update (" + label + ") in #daily-watchlist.\n"
            "RULES: " + ", ".join(TICKERS) + " only. Junior voice - direct, educational, zero fluff, no disclaimers. "
            "CHART TAGS: whenever you cite a specific technical phenomenon (a level break, volume spike, "
            "widening spreads, divergence), append on its own line at the END of that ticker's section: "
            "[CHART_REQUEST: TICKER=<symbol>, TIMEFRAME=<1m|5m|15m|1h|1d>, INDICATORS=<comma-separated>, "
            "HIGHLIGHT=\"<what the chart should prove>\"] — only for claims a chart can verify, max one per ticker. "
            "For each ticker, 1-2 sentences: where price trades versus PMH and PML right now, "
            "and exactly what has to happen for an entry (break and hold which level). "
            "This teaches the playbook - it is NOT a signal. At most one emoji total. "
            "End with exactly this line: " + closing + "\n"
            "LIVE DATA:\n" + "\n".join(rows)
        )
        _cl = anthropic_sdk.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=60.0, max_retries=1)
        _rp = _cl.messages.create(model=CLAUDE_MODEL, max_tokens=400, messages=[{"role": "user", "content": _pr}])
        post_to_discord("daily-watchlist", _rp.content[0].text.strip())
    except Exception as _e:
        log.error("Scanning update failed: " + str(_e))

def _scheduler_loop():
    log.info("Scheduler loop started")
    _s0 = load_state()
    last_watchlist_date   = _s0.get("last_watchlist_date")
    last_945_date         = _s0.get("last_945_date")
    last_1015_date        = _s0.get("last_1015_date")

    while True:
        try:
            now     = datetime.now(ET)
            today   = now.date()
            today_s = today.isoformat()
            t       = now.time()

            # v11.3 INGEST-FIRST: keep session structure fresh before anything
            # else in this tick can slow down or fail. Nothing may starve the
            # scanner's memory again.
            if (today.weekday() < 5 and today not in MARKET_HOLIDAYS
                    and (9, 25) <= (now.hour, now.minute)
                    <= ((12, 50) if today in EARLY_CLOSE_DATES else (15, 50))):
                for _tk in TICKERS:
                    try:
                        _lv = get_key_levels(_tk)
                        _ingest_new_bars(_tk, _lv.get("pmh"), _lv.get("pml"))
                    except Exception as _ie:
                        log.warning(f"ingest-first {_tk}: {_ie}")

            # Skip everything on weekends and holidays
            is_trading_day = (
                today.weekday() < 5
                and today not in MARKET_HOLIDAYS_2026
            )

            if is_trading_day:

                # ── 9:00 AM self pre-flight — DM the vitals before the day ──
                if ((8, 55) <= (now.hour, now.minute) <= (9, 14)
                        and load_state().get("last_preflight_date") != today_s):
                    with _state_lock:
                        _sp = load_state(); _sp["last_preflight_date"] = today_s; _commit(_sp)
                    log.info("JOB: 9:00 pre-flight self-check")
                    lines, bad = [], False
                    try:
                        nl = _account_net_liq()
                        if nl:
                            lines.append(f"✅ Broker auth OK — Net Liq ${nl:,.2f}")
                        else:
                            lines.append("❌ Broker balance UNREADABLE — sizing will fail-safe to 1 contract")
                            bad = True
                    except Exception as e:
                        lines.append(f"❌ Broker check failed: {e}"); bad = True
                    try:
                        _c = get_latest_1min_candle("TSLA")
                        lines.append("✅ Market data flowing" if _c else "❌ Market data: no candle returned")
                        bad = bad or not _c
                    except Exception as e:
                        lines.append(f"❌ Market data failed: {e}"); bad = True
                    try:
                        live_pos = _tt_open_option_positions()
                        if live_pos:
                            lines.append(f"⚠️ Broker shows {len(live_pos)} open position(s) pre-market — verify")
                            bad = True
                        else:
                            lines.append("✅ Broker flat")
                    except Exception as e:
                        lines.append(f"❌ Position check failed: {e}"); bad = True
                    _s0 = load_state()
                    lines.append(f"{'🛑 FOMC/no-trade day — entries OFF' if _no_trade_today() else '✅ Trading day'}"
                                 f" | {_risk()['tier']} ({_risk()['alloc']:.1%}/trade, {_stop_pct():.0%} stop, "
                                 f"max {_risk()['alloc'] * _stop_pct():.1%} account risk)")
                    lines.append(f"✅ v{'11.6'} boot {_BOOT_ID} | trades {_s0.get('trade_count', 0)}/2 | "
                                 f"circuit {'ON' if _s0.get('circuit_breaker') else 'off'}")
                    send_emergency_dm(
                        "\n".join(lines) + "\nWindow 9:30–10:30 ET. Watchlist posts 9:15.",
                        prefix=("🚨 **TPP PRE-FLIGHT — ATTENTION NEEDED** 🚨" if bad
                                else "✅ **TPP PRE-FLIGHT — ALL SYSTEMS GREEN**"),
                    )

                # ── Watchlist 9:15 AM ─────────────────────────────────────
                from datetime import time as dtime
                if (dtime(9, 15) <= t <= dtime(9, 44) and last_watchlist_date != today_s
                        and _no_trade_today()):
                    log.info("JOB: no-trade day notice (FOMC/high-risk)")
                    post_to_discord(
                        "daily-watchlist",
                        "@everyone 🛑 **No trades today — FOMC day.**\n"
                        "Fed rate decision at 2:00 PM ET with the press conference at 2:30 "
                        "means whipsaw risk all session, and our edge is the opening-range "
                        "playbook — not Fed roulette. Capital protection IS a position. "
                        "Back at it tomorrow with the same rules.",
                    )
                    last_watchlist_date = today_s
                    _sp = load_state(); _sp["last_watchlist_date"] = today_s; _commit(_sp)

                if (dtime(9, 15) <= t <= dtime(9, 44) and last_watchlist_date != today_s
                        and not _no_trade_today()):
                    log.info("JOB: daily watchlist")
                    try:
                        rows = []
                        for ticker in TICKERS:
                            lv  = get_key_levels(ticker)
                            pmh = lv.get("pmh")
                            pml = lv.get("pml")
                            pc  = lv.get("prev_close")
                            if pmh is None or pml is None:
                                log.warning("Watchlist: no levels for " + ticker)
                                continue
                            spot = _spot_price(ticker) or pc
                            gap  = round(((spot - pc) / pc) * 100, 2) if (spot and pc) else None
                            poc  = round((pmh + pml) / 2, 2)
                            rows.append({"ticker": ticker, "price": spot, "gap": gap,
                                         "pmh": pmh, "pml": pml, "poc": poc})
                        if rows:
                            data_lines = []
                            for r_ in rows:
                                g = r_["gap"]
                                gs = (("+" if g >= 0 else "") + str(g) + "%") if g is not None else "flat"
                                data_lines.append(
                                    r_["ticker"] + ": price=$" + str(r_["price"]) +
                                    " gap=" + gs + " PMH=$" + str(r_["pmh"]) +
                                    " PML=$" + str(r_["pml"]) + " POC=$" + str(r_["poc"]))
                            wl_prompt = (
                                "You are Junior from The Portfolio Plug. Write the morning "
                                "watchlist post for #daily-watchlist.\n"
                                "RULES: " + ", ".join(TICKERS) + " ONLY - never mention any other ticker. "
            "CHART TAGS: if you cite a verifiable technical phenomenon, append at the end of that ticker's line: "
            "[CHART_REQUEST: TICKER=<symbol>, TIMEFRAME=<1m|5m|15m|1h|1d>, INDICATORS=<comma-separated>, "
            "HIGHLIGHT=\"<what to prove>\"]. Max one per ticker, only when a chart could verify the claim. "
                                "Junior voice: direct, confident, educational, zero fluff, no disclaimers. "
                                "For each ticker in its own block: bold ticker name, price, gap %, "
                                "whether price sits near PMH (supply) or PML (demand), the POC level, "
                                "and ONE clear sentence on what you are watching for (bounce, break, rejection). "
                                "3-4 sentences per ticker max. End with exactly one line: "
                                "Window opens 9:30 AM ET - alerts fire on confirmed setups only.\n"
                                "DATA:\n" + "\n".join(data_lines)
                            )
                            try:
                                _wl_client = anthropic_sdk.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=60.0, max_retries=1)
                                _wl_resp = _wl_client.messages.create(
                                    model=CLAUDE_MODEL, max_tokens=600,
                                    messages=[{"role": "user", "content": wl_prompt}],
                                )
                                post_to_discord("daily-watchlist", _wl_resp.content[0].text.strip())
                                for _wtk in TICKERS:
                                    try:
                                        post_chart_to_discord("daily-watchlist", _wtk,
                                                              caption=f"📈 **{_wtk}** — levels on the chart")
                                    except Exception:
                                        pass
                            except Exception as we:
                                log.error("Watchlist Claude call failed: " + str(we))
                                for r_ in rows:
                                    post_to_discord(
                                        "daily-watchlist",
                                        "**" + r_["ticker"] + "** | $" + str(r_["price"]) +
                                        " | PMH $" + str(r_["pmh"]) + " / PML $" + str(r_["pml"]) +
                                        " / POC $" + str(r_["poc"]) +
                                        " - window opens 9:30 AM ET, alerts on confirmed setups only.")
                        else:
                            log.warning("Watchlist skipped - no level data for either ticker")
                        try:
                            _sb_lines = []
                            for _t in TICKERS:
                                _c_occ, _cs, _ca = select_contract(_t, "call")
                                _p_occ, _ps, _pa = select_contract(_t, "put")
                                if _c_occ or _p_occ:
                                    _parts = []
                                    if _c_occ:
                                        _parts.append(f"📈 breakout → **{_fmt_occ(_c_occ)}** (~${_ca:.2f})")
                                    if _p_occ:
                                        _parts.append(f"📉 breakdown → **{_fmt_occ(_p_occ)}** (~${_pa:.2f})")
                                    _sb_lines.append(f"**{_t}:** " + "  |  ".join(_parts))
                            if _sb_lines:
                                post_to_discord(
                                    "daily-watchlist",
                                    "🎯 **Contracts on standby — have these loaded, enter ONLY when the entry signal fires:**\n"
                                    + "\n".join(_sb_lines)
                                    + "\n_Premiums move by the open — the entry signal names the final contract and price._",
                                )
                        except Exception as _sb_e:
                            log.warning(f"standby contracts post failed: {_sb_e}")
                        last_watchlist_date = today_s
                        _sp = load_state(); _sp["last_watchlist_date"] = today_s; _commit(_sp)
                    except Exception as e:
                        log.error(f"Watchlist job error: {e}")

                # ── 9:45 AM status update ─────────────────────────────────
                if (dtime(9, 45) <= t <= dtime(9, 59) and last_945_date != today_s
                        and not _no_trade_today()):
                    s = load_state()
                    if s["trade_count"] == 0 and not get_open_position():
                        _post_scanning_update("9:45 AM", "No forced trades — entries hit #day-trade-signals only on confirmed setups.")
                        log.info("9:45 status update posted")
                    else:
                        log.info("9:45 status update skipped — trade already active")
                    last_945_date = today_s
                    _sp = load_state(); _sp["last_945_date"] = today_s; _commit(_sp)

                # ── 10:15 AM status update ────────────────────────────────
                if (dtime(10, 15) <= t <= dtime(10, 29) and last_1015_date != today_s
                        and not _no_trade_today()):
                    s = load_state()
                    if s["trade_count"] == 0 and not get_open_position():
                        _post_scanning_update("10:15 AM — final stretch", "Window closes 10:30 — if nothing sets up we sit out. No forced trades.")
                        log.info("10:15 status update posted")
                    else:
                        log.info("10:15 status update skipped — trade already active")
                    last_1015_date = today_s
                    _sp = load_state(); _sp["last_1015_date"] = today_s; _commit(_sp)

                # ── 1-min scanner 9:25–10:30 AM ──────────────────────────
                if dtime(9, 25) <= t <= dtime(10, 30) and _in_window():
                    try:
                        for ticker in TICKERS:
                            # Structure stays fresh every tick, gates or not
                            levels = get_key_levels(ticker)
                            candle = _ingest_new_bars(ticker, levels.get("pmh"), levels.get("pml"))
                            if not candle:
                                continue
                            if time_module.time() - _boot_ts < 120:
                                continue  # boot grace: ingest yes, entries no
                            if not all_gates_pass(ticker, signal_type="entry"):
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
                                    break
                    except Exception as e:
                        log.error(f"Scanner error: {e}")

            # ── Daily wrap 10:31+ — members get an official end to the day ──
            if (today.weekday() < 5 and today not in MARKET_HOLIDAYS
                    and dtime(10, 31) <= t <= dtime(15, 59)
                    and load_state().get("last_wrap_date") != today_s
                    and not get_open_position()
                    and not _no_trade_today()):
                with _state_lock:
                    _sw = load_state(); _sw["last_wrap_date"] = today_s; _commit(_sw)
                try:
                    _res = load_state().get("today_results", []) or []
                    if not _res:
                        _wrap = ("🏁 **Window closed — no trades today.**\n"
                                 "Nothing met the playbook's confirmation rules, so we sat on our "
                                 "hands. Not trading IS a position. Watchlist back tomorrow at 9:15.")
                    else:
                        _wins = sum(1 for r in _res if r.get("win"))
                        _tot  = sum(float(r.get("pnl_dollar") or 0) for r in _res)
                        _lines = []
                        for r in _res:
                            _pp = r.get("pnl_pct")
                            _pd = float(r.get("pnl_dollar") or 0)
                            _lines.append(f"{'✅' if r.get('win') else '❌'} {r.get('ticker')} "
                                          f"{(f'{_pp:+.1%}' if _pp is not None else '')} "
                                          f"({'+' if _pd >= 0 else ''}${abs(_pd):.0f})")
                        _locked = [tk for tk, n in (load_state().get("ticker_losses") or {}).items() if n >= 1]
                        _wrap = (f"🏁 **Window closed — day recap: {_wins}W / {len(_res) - _wins}L, "
                                 f"{'+' if _tot >= 0 else ''}${abs(_tot):.0f} net.**\n"
                                 + "\n".join(_lines)
                                 + (f"\n🚫 {', '.join(_locked)} benched after a loss — one loss per "
                                    f"ticker per day, that's the rule." if _locked else "")
                                 + "\nEvery entry, exit, and stop was posted live above. "
                                   "Watchlist back tomorrow at 9:15.")
                    post_to_discord("daily-watchlist", _wrap)
                    log.info("JOB: daily wrap posted")
                except Exception as e:
                    log.error(f"Daily wrap failed: {e}")

            # ── Position monitor — runs regardless of window/day ──────────
            # Keeps managing past 10:30 AM if trade is still open
            pos = get_open_position()
            if pos or _in_window():
                try:
                    monitor_open_position()
                except Exception as e:
                    log.error(f"Position monitor error: {e}")

            # v11.7: heartbeat at the END — a fresh beat now proves the entire
            # iteration completed, not just that the loop woke up (Jul 28 lesson)
            _beat_heartbeat()

        except Exception as e:
            log.error(f"Scheduler loop error: {e}")

        time_module.sleep(60)  # tick every 60 seconds


def _ensure_scheduler():
    global _scheduler_started
    with _scheduler_lock:
        if _scheduler_started:
            return
        if not _try_acquire_scheduler_lock():
            log.warning(f"[{_BOOT_ID}] Scheduler election LOST — another process "
                        "holds the lock; this worker serves HTTP only")
            return
        _scheduler_started = True
        t = threading.Thread(target=_scheduler_loop, daemon=True, name="tpp-scheduler")
        t.start()
        log.info(f"[{_BOOT_ID}] Scheduler election WON (pid {os.getpid()}) — loop started")


# ══════════════════════════════════════════════════════════════════════════════
#  FLASK ROUTES
# ══════════════════════════════════════════════════════════════════════════════

# ── /webhook ──────────────────────────────────────────────────────────────────
@app.route("/webhook", methods=["POST"])
def webhook():
    # Verify secret from JSON body (TradingView compatible)
    data = request.get_json(force=True, silent=True) or {}
    incoming_secret = data.get("secret", "")
    if WEBHOOK_SECRET and not hmac.compare_digest(incoming_secret, WEBHOOK_SECRET):
        log.warning("Webhook rejected — invalid signature")
        return jsonify({"error": "unauthorized"}), 401

    if not data:
        return jsonify({"error": "no data"}), 400

    ticker     = str(data.get("ticker", "")).upper()
    alert_type = str(data.get("alert_type", "UNKNOWN"))

    # -- sanitize TradingView template quirks + enrich levels server-side --
    if "{{" in alert_type or not alert_type.strip():
        alert_type = "PRICE_ALERT"
        data["alert_type"] = alert_type
    def _num(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    for _k in ("pmh", "pml", "level", "close", "open", "high", "low", "volume"):
        if _k in data:
            data[_k] = _num(data[_k])
    if ticker in TRADEABLE_TICKERS:
        _lv = get_key_levels(ticker)
        if _lv.get("pmh") and _lv.get("pml"):
            data["pmh"] = _lv.get("pmh")
            data["pml"] = _lv.get("pml")
        elif not data.get("pmh") or not data.get("pml"):
            data["pmh"] = None
            data["pml"] = None
        log.info(f"Webhook enriched {ticker}: PMH={data.get('pmh')} PML={data.get('pml')}")
    log.info(f"WEBHOOK: {ticker} | {alert_type}")

    if not all_gates_pass(ticker, signal_type="entry"):
        return jsonify({"status": "blocked"}), 200

    session  = load_state()
    decision = call_claude(data, session)

    if not decision:
        return jsonify({"status": "claude_error"}), 200

    if decision.get("decision") == "APPROVE":
        d_ticker    = decision.get("ticker", ticker).upper()
        d_direction = decision.get("direction", "").lower()
        if d_direction not in ("call", "put"):
            log.error(f"Invalid direction: {d_direction}")
            return jsonify({"status": "invalid_direction"}), 200
        success = execute_trade(d_ticker, d_direction, decision)
        return jsonify({"status": "traded" if success else "execution_failed"}), 200

    reason = decision.get("reason", "No reason given")
    log.info(f"NO_TRADE: {reason}")
    return jsonify({"status": "no_trade", "reason": reason}), 200


# ── /flatten ──────────────────────────────────────────────────────────────────
@app.route("/flatten", methods=["POST"])
def flatten():
    data = request.get_json(force=True, silent=True) or {}
    if WEBHOOK_SECRET and not hmac.compare_digest(
        data.get("secret", ""), WEBHOOK_SECRET
    ):
        return jsonify({"error": "unauthorized"}), 401

    pos = get_open_position()
    if not pos:
        return jsonify({"status": "no_position"}), 200

    occ        = pos["occ_symbol"]
    fill_price = float(pos["fill_price"])
    quote      = _live_option_quote(occ)
    bid        = float(quote.get("bid", fill_price * 0.90)) if quote else fill_price * 0.90

    _cancel_complex_order(pos.get("oco_id"))
    cancel_resting_stop(pos.get("stop_order_id"))
    exit_price = close_position_tt(occ, "MANUAL FLATTEN", bid, int(pos.get("qty") or 1))
    if exit_price is None:
        return jsonify({"status": "close_failed", "detail": "position still open at broker — retry"}), 502

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
    record_trade_result(win=(exit_price > fill_price), ticker=pos.get("ticker"),
                        pnl_pct=pnl_pct, pnl_dollar=pnl_dollar,
                        direction=pos.get("direction"))
    log.info(f"Flatten complete — {occ} @ ${exit_price:.2f}")
    return jsonify({"status": "flattened", "exit_price": exit_price}), 200


# ── /kill ─────────────────────────────────────────────────────────────────────
@app.route("/kill", methods=["POST"])
def kill():
    data = request.get_json(force=True, silent=True) or {}
    if WEBHOOK_SECRET and not hmac.compare_digest(
        data.get("secret", ""), WEBHOOK_SECRET
    ):
        return jsonify({"error": "unauthorized"}), 401

    flatten()
    s = load_state()
    s["circuit_breaker"] = True
    _commit(s)
    log.warning("/kill — circuit breaker on, day over")
    post_to_discord("day-trade-signals", "🛑 Kill switch activated — no more trades today.")
    return jsonify({"status": "killed"}), 200


# ── /status ───────────────────────────────────────────────────────────────────
@app.route("/chart-test")
def chart_test():
    """Render + upload one chart on demand and report exactly what happened.
    Safe to hit anytime; posts a test chart to daily-watchlist on success."""
    ticker = (request.args.get("ticker") or "TSLA").upper()
    out = {"ticker": ticker, "candles": len((_mkt_structure.get(ticker) or {}).get("candles") or [])}
    png = _render_chart_png(ticker, "chart-test diagnostic")
    out["render_ok"] = bool(png)
    out["png_bytes"] = len(png or b"")
    if not png:
        out["last_chart_error"] = _LAST_CHART_ERR
        return jsonify(out), 200
    try:
        import json as _json
        channel_id = CHANNEL_IDS.get("daily-watchlist")
        resp = requests.post(
            f"{DISCORD_API}/channels/{channel_id}/messages",
            headers={"Authorization": f"Bot {DISCORD_BOT_TOKEN}"},
            data={"payload_json": _json.dumps({"content": f"🧪 chart-test — {ticker}\n-# ⚙️ {_BOOT_ID}"})},
            files={"files[0]": (f"{ticker.lower()}_test.png", png, "image/png")},
            timeout=15,
        )
        out["upload_status"] = resp.status_code
        out["upload_ok"] = resp.status_code in (200, 201)
        if not out["upload_ok"]:
            out["discord_error"] = resp.text[:300]
            if resp.status_code == 403:
                out["hint"] = "Bot role is missing the ATTACH FILES permission in this channel"
    except Exception as e:
        out["upload_ok"] = False
        out["error"] = str(e)[:200]
    return jsonify(out), 200


@app.route("/oco-test")
def oco_test():
    """Validate the v11.7 OCO bracket payload against Tastytrade's dry-run
    endpoint. NO ORDER IS PLACED. Proves the complex-order shape is accepted
    before the first live trade uses it."""
    ticker = (request.args.get("ticker") or "TSLA").upper()
    occ, strike, ask = select_contract(ticker, "call")
    if not occ:
        return jsonify({"ok": False, "error": "no contract found"}), 200
    fill = float(ask or 1.00)
    target  = _tick(fill * 1.40)
    trigger = _tick(fill * (1 - _stop_pct()))
    payload = {
        "type": "OCO",
        "orders": [
            {"time-in-force": "Day", "order-type": "Limit", "price": str(target),
             "price-effect": "Credit",
             "legs": [{"instrument-type": "Equity Option", "symbol": occ,
                       "quantity": 1, "action": "Sell to Close"}]},
            {"time-in-force": "Day", "order-type": "Stop", "stop-trigger": str(trigger),
             "legs": [{"instrument-type": "Equity Option", "symbol": occ,
                       "quantity": 1, "action": "Sell to Close"}]},
        ],
    }
    try:
        r = requests.post(f"{TT_BASE}/accounts/{TT_ACCOUNT}/complex-orders/dry-run",
                          headers=_tt_headers(), json=payload, timeout=10)
        return jsonify({"ok": r.status_code in (200, 201), "http": r.status_code,
                        "contract": occ, "target": target, "stop": trigger,
                        "detail": (r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text[:400])}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200


@app.route("/status", methods=["GET"])
def status():
    s   = load_state()
    now = datetime.now(ET)
    pos = s.get("open_position")
    hb = _read_heartbeat()
    hb_age = round(time_module.time() - hb["ts"], 1) if hb and hb.get("ts") else None
    # v11.9 WATCHDOG: if this process claims the scheduler but the thread is
    # dead, revive it on the spot (any /status hit — e.g. an uptime monitor —
    # doubles as the resurrection trigger).
    global _scheduler_started
    _thread_alive = any(t.name == "tpp-scheduler" and t.is_alive() for t in threading.enumerate())
    if _sched_lock_fd is not None and not _thread_alive:
        log.critical(f"[{_BOOT_ID}] WATCHDOG: scheduler thread DEAD — reviving now")
        with _scheduler_lock:
            _scheduler_started = False
        _ensure_scheduler()
        try:
            send_emergency_dm(f"WATCHDOG revived a dead scheduler thread (boot {_BOOT_ID}). "
                              "Investigate logs, but trading continuity was preserved.")
        except Exception:
            pass
        _thread_alive = True
    return jsonify({
        "status":             "online",
        "version":            "v13.3",
        "boot_id":            _BOOT_ID,
        "boot_time_et":       _BOOT_TIME_ET,
        "serving_pid":        os.getpid(),
        "scheduler_in_this_process": _sched_lock_fd is not None,
        "scheduler_thread_alive":    _thread_alive,
        "heartbeat_boot_id":  hb.get("boot_id") if hb else None,
        "heartbeat_age_s":    hb_age,
        "risk_mode":          _risk().get("tier", "?"),
        "alloc_pct":          _risk()["alloc"],
        "stop_pct":           _stop_pct(),
        "no_trade_today":     _no_trade_today(),
        "time_et":            now.strftime("%Y-%m-%d %H:%M:%S ET"),
        "trade_count":        s["trade_count"],
        "circuit_breaker":    s["circuit_breaker"],
        "consecutive_losses": s["consecutive_losses"],
        "open_position":      pos is not None,
        "open_symbol":        pos["occ_symbol"] if pos else None,
        "last_reset_date":    s["last_reset_date"],
        "in_window":          _in_window(),
    }), 200


@app.route("/health", methods=["GET"])
def health():
    return status()


@app.route("/tt-test", methods=["GET"])
def tt_test():
    """Verify Tastytrade auth works without placing any order."""
    try:
        token = _tt_get_token()
        if token:
            return jsonify({"tastytrade_auth": "ok", "account": TT_ACCOUNT}), 200
        return jsonify({"tastytrade_auth": "failed", "reason": "no token returned"}), 500
    except Exception as e:
        return jsonify({"tastytrade_auth": "failed", "reason": str(e)}), 500


# ── / ─────────────────────────────────────────────────────────────────────────
@app.route("/wl-test", methods=["GET"])
def wl_test():
    """Dry-run: generate the watchlist with LIVE data right now. Nothing posts to Discord."""
    try:
        rows = []
        for _tk in TICKERS:
            lv  = get_key_levels(_tk)
            pmh = lv.get("pmh")
            pml = lv.get("pml")
            pc  = lv.get("prev_close")
            if pmh is None or pml is None:
                continue
            spot = _spot_price(_tk) or pc
            gap  = round(((spot - pc) / pc) * 100, 2) if (spot and pc) else None
            poc  = round((pmh + pml) / 2, 2)
            rows.append({"ticker": _tk, "price": spot, "gap": gap, "pmh": pmh, "pml": pml, "poc": poc})
        if not rows:
            return jsonify({"ok": False, "reason": "no level data"}), 500
        data_lines = [r["ticker"] + ": price=$" + str(r["price"]) + " gap=" + str(r["gap"]) + "% PMH=$" + str(r["pmh"]) + " PML=$" + str(r["pml"]) + " POC=$" + str(r["poc"]) for r in rows]
        _pr = (
            "You are Junior from The Portfolio Plug. Write the morning watchlist post for #daily-watchlist.\n"
            "RULES: NVDA and TSLA ONLY. Junior voice - direct, confident, educational, zero fluff, no disclaimers. "
            "For each ticker in its own block: bold ticker name, price, gap %, whether price sits near PMH (supply) or PML (demand), "
            "the POC level, and ONE clear sentence on what you are watching for (bounce, break, rejection). "
            "3-4 sentences per ticker max. End with exactly one line: Window opens 9:30 AM ET - alerts fire on confirmed setups only.\n"
            "DATA:\n" + "\n".join(data_lines)
        )
        _cl = anthropic_sdk.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=60.0, max_retries=1)
        _rp = _cl.messages.create(model=CLAUDE_MODEL, max_tokens=600, messages=[{"role": "user", "content": _pr}])
        return jsonify({"ok": True, "rows": rows, "watchlist": _rp.content[0].text.strip()}), 200
    except Exception as e:
        return jsonify({"ok": False, "reason": str(e)}), 500


@app.route("/exec-test", methods=["GET"])
def exec_test():
    """Dry-run: dumps live chain diagnostics + runs select_contract. NO ORDER PLACED."""
    try:
        tk = str(request.args.get("ticker", "NVDA")).upper()
        dr = str(request.args.get("direction", "call")).lower()
        if tk not in TRADEABLE_TICKERS or dr not in ("call", "put"):
            return jsonify({"ok": False, "reason": "invalid params"}), 400
        spot   = _spot_price(tk)
        expiry = _next_friday()
        opt_type = "C" if dr == "call" else "P"
        resp = requests.get(
            f"{TT_BASE}/option-chains/{tk}/nested",
            headers=_tt_headers(),
            params={"expiration-date": expiry.strftime("%Y-%m-%d")},
            timeout=10,
        )
        chain_status = resp.status_code
        strikes = []
        if chain_status == 200:
            items0 = resp.json().get("data", {}).get("items", [])
            for it0 in items0:
                for ex0 in it0.get("expirations", []):
                    if ex0.get("expiration-date") == expiry.strftime("%Y-%m-%d"):
                        strikes = sorted(float(s["strike-price"]) for s in ex0.get("strikes", []) if s.get("strike-price"))
        sample = []
        if strikes and spot:
            atm = min(strikes, key=lambda s: abs(s - spot))
            ai  = strikes.index(atm)
            walk = strikes[ai:ai+6] if dr == "call" else list(reversed(strikes[max(0,ai-5):ai+1]))
            for st in walk:
                occ = (tk + "      ")[:6] + expiry.strftime("%y%m%d") + opt_type + ("%08d" % int(round(st * 1000)))
                q   = _live_option_quote(occ) or {}
                entry = {"strike": st, "occ": occ,
                         "bid": q.get("bid"), "ask": q.get("ask")}
                if not sample and q:
                    entry["raw"] = {k: q.get(k) for k in list(q.keys())[:20]}
                sample.append(entry)
        occ, strike, ask = select_contract(tk, dr)
        return jsonify({
            "ok": bool(occ), "ticker": tk, "direction": dr,
            "spot": spot, "expiry": expiry.strftime("%Y-%m-%d"),
            "chain_http": chain_status, "num_strikes": len(strikes),
            "sample_quotes": sample,
            "selected": ({"occ": occ, "strike": strike, "ask": ask,
                          "cost_per_contract": round(ask*100,2) if ask else None} if occ else None),
            "note": "DRY RUN - no order placed; zero bid/ask = market closed",
        }), 200
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "reason": str(e), "trace": traceback.format_exc()[-400:]}), 500


@app.route("/quote-test", methods=["GET"])
def quote_test():
    """Probe 4 quote request variants to find the one Tastytrade answers. Read-only."""
    try:
        occ_padded = "NVDA  260717C00210000"
        compact = occ_padded.replace(" ", "")
        from urllib.parse import quote as _uq
        out = {}
        def probe(name, url, params=None):
            try:
                r = requests.get(url, headers=_tt_headers(), params=params, timeout=5)
                try:
                    d = r.json().get("data", {})
                    items = d.get("items", []) if isinstance(d, dict) else []
                    if not items and isinstance(d, dict) and d.get("symbol"):
                        items = [d]
                except Exception:
                    items = []
                first = items[0] if items else None
                out[name] = {"http": r.status_code, "n": len(items),
                             "keys": (sorted(list(first.keys()))[:14] if isinstance(first, dict) else None),
                             "bid": (first or {}).get("bid"), "ask": (first or {}).get("ask")}
            except Exception as e:
                out[name] = {"err": str(e)[:80]}
        probe("bytype_padded", f"{TT_BASE}/market-data/by-type?equity-option=" + _uq(occ_padded, safe=""))
        probe("bytype_compact", f"{TT_BASE}/market-data/by-type", {"equity-option": compact})
        probe("path_padded", f"{TT_BASE}/market-data/" + _uq(occ_padded, safe=""))
        probe("options_padded", f"{TT_BASE}/market-data/options", {"symbols[]": occ_padded})
        return jsonify(out), 200
    except Exception as e:
        return jsonify({"err": str(e)}), 500

@app.route("/order-test", methods=["GET"])
def order_test():
    """Validate the EXACT entry order payload against Tastytrade /orders/dry-run.
    NO ORDER IS PLACED - dry-run only. Proves symbology + buying power + payload."""
    try:
        tk = str(request.args.get("ticker", "NVDA")).upper()
        dr = str(request.args.get("direction", "call")).lower()
        if tk not in TRADEABLE_TICKERS or dr not in ("call", "put"):
            return jsonify({"ok": False, "reason": "invalid params"}), 400
        occ, strike, ask = select_contract(tk, dr)
        if not occ:
            return jsonify({"ok": False, "reason": "no contract selected"}), 200
        payload = {
            "time-in-force": "Day",
            "order-type":    "Market",
            "legs": [{"instrument-type": "Equity Option", "symbol": occ,
                      "quantity": 1, "action": "Buy to Open"}],
        }
        resp = requests.post(
            f"{TT_BASE}/accounts/{TT_ACCOUNT}/orders/dry-run",
            headers=_tt_headers(),
            json=payload,
            timeout=10,
        )
        body = {}
        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text[:300]}
        d = body.get("data", {}) if isinstance(body, dict) else {}
        bp = d.get("buying-power-effect", {})
        return jsonify({
            "ok": resp.status_code in (200, 201),
            "http": resp.status_code,
            "contract": {"occ": occ, "strike": strike, "ask": ask},
            "order_status": (d.get("order", {}) or {}).get("status"),
            "buying_power_change": bp.get("change-in-buying-power"),
            "warnings": body.get("warnings") or d.get("warnings"),
            "errors": body.get("error") or body.get("errors"),
            "note": "DRY RUN via Tastytrade /orders/dry-run - nothing placed",
        }), 200
    except Exception as e:
        return jsonify({"ok": False, "reason": str(e)}), 500

@app.route("/structure-test", methods=["GET"])
def structure_test():
    """Live proof of session-structure tracking. Read-only, no orders."""
    try:
        tk = str(request.args.get("ticker", "NVDA")).upper()
        lv = get_key_levels(tk)
        c = get_latest_1min_candle(tk)
        if c:
            _update_structure(tk, c, lv.get("pmh"), lv.get("pml"))
        return jsonify({"candle": c, "structure": _mkt_structure.get(tk),
                        "context": _structure_context(tk, lv.get("pmh"), lv.get("pml"))}), 200
    except Exception as e:
        return jsonify({"err": str(e)}), 500

@app.route("/fill-test", methods=["GET"])
def fill_test():
    """Runs the live fill reader against REAL past order IDs from this account.
    Default IDs = the nine July 23 fills. Read-only; places nothing."""
    default_ids = ("486657096,486664254,486671221,486671958,486701499,"
                   "486710469,486726217,486731376,486770060")
    ids = [i.strip() for i in request.args.get("ids", default_ids).split(",") if i.strip()]
    rows, ok = [], 0
    for oid in ids[:20]:
        status, fill = _tt_order_status(oid)
        good = (status == "filled" and fill is not None)
        ok += 1 if good else 0
        rows.append({"order_id": oid, "status": status,
                     "fill_price": fill, "pass": good})
    return jsonify({"tested": len(rows), "passed": ok,
                    "verdict": "CERTIFIED" if ok == len(rows) else "FAILING",
                    "rows": rows}), 200


@app.route("/replay-test", methods=["GET"])
def replay_test():
    """Replay any past session through the EXACT live decision path.
    /replay-test?ticker=TSLA&date=2026-07-20            -> structure/cross timeline only (free)
    /replay-test?ticker=TSLA&date=2026-07-20&claude=1   -> + a real Claude decision per bar (~66 calls)
    Never places orders. Never posts to Discord. Does not touch live session state."""
    ticker = str(request.args.get("ticker", "TSLA")).upper()
    day    = str(request.args.get("date", ""))
    use_claude = str(request.args.get("claude", "0")) == "1"
    # Chunking: Render's proxy caps requests near 100s, so a full-day Claude
    # replay (60+ sequential calls) can never return in one request. Evaluate
    # bars [claude_from, claude_to) per request (default 15-bar chunk).
    try:
        c_from = int(request.args.get("claude_from", "0"))
        c_to   = int(request.args.get("claude_to", str(c_from + 15)))
    except ValueError:
        return jsonify({"error": "claude_from/claude_to must be integers"}), 400
    if not day:
        return jsonify({"error": "date=YYYY-MM-DD required"}), 400
    # Never run replays inside the live scanning window: replay swaps live
    # session state and would race the scanner thread.
    _nw = datetime.now(ET)
    if _nw.strftime("%Y-%m-%d") != day and (9, 20) <= (_nw.hour, _nw.minute) <= (10, 35) and _nw.weekday() < 5:
        return jsonify({"error": "replay disabled during live window (9:20-10:35 ET)"}), 409

    # prior-day PMH/PML for that date
    p_start = (date.fromisoformat(day) - timedelta(days=7)).isoformat()
    pmh = pml = None
    for feed in ("iex", "sip", None):
        try:
            params = {"timeframe": "1Day", "limit": 10, "start": p_start, "end": day + "T00:00:00Z"}
            if feed:
                params["feed"] = feed
            r = requests.get(f"https://data.alpaca.markets/v2/stocks/{ticker}/bars",
                             headers=_alpaca_headers(), params=params, timeout=6)
            if r.status_code == 200 and r.json().get("bars"):
                b = r.json()["bars"][-1]
                pmh, pml = b["h"], b["l"]
                break
        except Exception:
            pass

    bars = _fetch_1min_bars(ticker, day + "T13:24:00Z", day + "T14:31:00Z", limit=200)
    if not bars:
        return jsonify({"error": "no bars for that date/ticker"}), 404

    saved = _mkt_structure.pop(ticker, None)   # protect live state
    timeline, approvals = [], []
    try:
        for _bi, b in enumerate(bars):
            c = {"open": b["o"], "high": b["h"], "low": b["l"],
                 "close": b["c"], "volume": b["v"], "t": b.get("t")}
            st = _update_structure(ticker, c, pmh, pml, day=day)
            row = {"t": b.get("t"), "close": b["c"], "volume": b["v"]}
            if use_claude and c_from <= _bi < c_to:
                alert = {"ticker": ticker, "alert_type": "REPLAY_1MIN",
                         "close": c["close"], "open": c["open"], "high": c["high"],
                         "low": c["low"], "volume": c["volume"], "pmh": pmh, "pml": pml}
                try:
                    _bt_et = (datetime.fromisoformat(str(b.get("t")).replace("Z", "+00:00"))
                              .astimezone(ET).strftime("%H:%M ET"))
                except Exception:
                    _bt_et = None
                d = call_claude(alert, {"trade_count": 0, "consecutive_losses": 0,
                                        "circuit_breaker": False, "open_position": None},
                                replay=True, as_of=_bt_et)
                if not d:
                    row["decision"] = "ERROR"
                    row["reason"] = "call_claude returned no decision (parse/API failure)"
                if d:
                    row["decision"] = d.get("decision")
                    row["reason"] = d.get("reason", "")[:160]
                    if d.get("decision") == "APPROVE":
                        approvals.append({"t": b.get("t"), "direction": d.get("direction"),
                                          "tier": d.get("tier"), "reason": d.get("reason", "")})
            timeline.append(row)
        final = _mkt_structure.get(ticker, {})
        result = {"ticker": ticker, "date": day, "pmh": pmh, "pml": pml,
                  "pm_high": final.get("pm_high"), "pm_low": final.get("pm_low"),
                  "or_low": final.get("or_low"), "or_high": final.get("or_high"),
                  "day_high": final.get("day_high"), "day_low": final.get("day_low"),
                  "crosses": final.get("crosses", {}), "bars": len(bars),
                  "claude_from": c_from if use_claude else None,
                  "claude_to": min(c_to, len(bars)) if use_claude else None,
                  "next_chunk": (day and use_claude and c_to < len(bars)) and (
                      "/replay-test?ticker=" + ticker + "&date=" + day
                      + "&claude=1&claude_from=" + str(c_to)
                      + "&claude_to=" + str(min(c_to + 15, len(bars)))) or None,
                  "approvals": approvals, "timeline": timeline}
    finally:
        if saved is not None:
            _mkt_structure[ticker] = saved
        else:
            _mkt_structure.pop(ticker, None)
    return jsonify(result), 200


@app.route("/", methods=["GET"])
def root():
    return jsonify({"status": "TPP Trading Server v5.0 — live"}), 200


# ══════════════════════════════════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════════════════════════════════
_boot_done = False
_boot_guard = threading.Lock()


def _resync_day_state_from_discord():
    """A fresh instance starts with blank /tmp state. Before the scheduler
    runs, read today's OWN posts back from Discord so the day's facts survive
    a redeploy: (1) a wrap already posted today -> stamp last_wrap_date so we
    never post a second, contradictory wrap; (2) recap posts from today ->
    rebuild today_results so a later wrap reports the real tally. Best-effort:
    any failure leaves state blank, which is no worse than before."""
    import re as _re
    today_et = datetime.now(ET).date()

    def _msgs(channel):
        cid = CHANNEL_IDS.get(channel)
        if not cid:
            return []
        try:
            r = requests.get(f"{DISCORD_API}/channels/{cid}/messages",
                             headers={"Authorization": f"Bot {DISCORD_BOT_TOKEN}"},
                             params={"limit": 30}, timeout=10)
            return r.json() if r.status_code == 200 else []
        except Exception as e:
            log.warning(f"day-state resync: fetch {channel} failed: {e}")
            return []

    def _is_today(m):
        try:
            ts = datetime.fromisoformat(str(m.get("timestamp")).replace("Z", "+00:00"))
            return ts.astimezone(ET).date() == today_et
        except Exception:
            return False

    try:
        # (1) wrap dedup
        if any(_is_today(m) and "Window closed" in (m.get("content") or "")
               for m in _msgs("daily-watchlist")):
            with _state_lock:
                s = load_state()
                s["last_wrap_date"] = today_et.isoformat()
                _commit(s)
            log.info("day-state resync: wrap already posted today — stamped")

        # (2) rebuild today's results from our own recap posts
        results = []
        pat = _re.compile(
            r"(✅|❌) \*\*(\S+) .*?CLOSED\*\*.*?P&L: ([+-][\d.]+)% \(([+-]?)\$([\d.]+) total",
            _re.S)
        for m in reversed(_msgs("profits-and-recaps")):   # oldest first
            if not _is_today(m):
                continue
            g = pat.search(m.get("content") or "")
            if g:
                _sign = -1.0 if (g.group(1) == "❌" or g.group(4) == "-") else 1.0
                results.append({"ticker": g.group(2), "win": g.group(1) == "✅",
                                "pnl_pct": float(g.group(3)) / 100.0,
                                "pnl_dollar": _sign * abs(float(g.group(5)))})
        if results:
            with _state_lock:
                s = load_state()
                if not s.get("today_results"):
                    s["today_results"] = results
                    _commit(s)
            log.info(f"day-state resync: rebuilt {len(results)} trade result(s) from recaps")
    except Exception as e:
        log.warning(f"day-state resync failed (non-fatal): {e}")


def _boot_in_worker():
    """v11.2: ALL boot work happens here, in the process that serves HTTP —
    never at module import. This makes the app immune to gunicorn preload /
    fork topology: master imports nothing but definitions; the worker that
    answers requests is the same process that runs the scheduler, owns the
    structure, and holds the state. Called from gunicorn.conf.py
    post_worker_init, from Flask's first request as a fallback, and from
    __main__ for direct runs."""
    global _boot_done, _BOOT_ID
    with _boot_guard:
        if _boot_done:
            return
        _boot_done = True
    if os.getpid() != _IMPORT_PID:
        # We are a forked child of a preloading master — mint our own identity
        _BOOT_ID = _uuid.uuid4().hex[:6]
    log.info(f"[{_BOOT_ID}] worker boot (pid {os.getpid()}, import pid {_IMPORT_PID}) — "
             "state/structure/adoption/scheduler starting IN THIS PROCESS")
    load_state()
    _hydrate_structure()
    _adopt_broker_positions()
    _resync_day_state_from_discord()
    _ensure_scheduler()


@app.before_request
def _boot_fallback():
    if not _boot_done:
        _boot_in_worker()


if __name__ == "__main__":
    _boot_in_worker()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
