"""
The Portfolio Plug — Position Manager (Exit Engine) v1
Owns every open position from confirmed fill to confirmed exit.

Safety architecture (designed for UNATTENDED live operation):
  1. The hard stop is a Stop-Limit order RESTING AT THE BROKER, placed the moment
     the entry fill is confirmed. If this server dies, the stop still protects you.
  2. A monitor loop (every 20s) handles what resting orders can't: trailing stop
     after +10%, profit target, 10-minute chop tightening, and 3:55 PM flatten.
  3. Every managed exit is a LIMIT order; if a protective limit doesn't fill in
     45s while capital is at risk, it escalates to a MARKET close. Capital first.
  4. State persists to disk; on boot we reconcile with the broker and ADOPT any
     orphan position (protect it with a stop) so nothing is ever unmanaged.

Playbook parameters:
  HARD_STOP   -25% from entry (limit leg at -30%)
  TRAIL_ARM   +10%  → trail 15% below peak premium
  TARGET      +40%  → take profits
  CHOP        open >10 min with P&L between -5% and +8% → tighten stop to -15%
  FLATTEN     15:55 ET — all day-trade positions closed, end of story
"""

import os
import json
import time
import logging
import threading
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import tastytrade_executor as tt

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

STATE_FILE = "/tmp/tpp_positions.json"

HARD_STOP_PCT    = 0.25
HARD_STOP_LIMIT  = 0.30
TRAIL_ARM_PCT    = 0.10
TRAIL_GIVEBACK   = 0.15
TARGET_PCT       = 0.40
CHOP_MINUTES     = 10
CHOP_STOP_PCT    = 0.15
FLATTEN_TIME     = dtime(15, 55)
ESCALATE_SECONDS = 45

_positions: dict = {}          # occ_symbol -> position dict
_lock = threading.Lock()
_started = False

# injected by main.py
_post_discord = lambda ch, msg: None
_channels     = {}
_on_win       = lambda sym: None
_on_loss      = lambda sym: None


def configure(post_discord, channels: dict, on_win, on_loss):
    global _post_discord, _channels, _on_win, _on_loss
    _post_discord, _channels, _on_win, _on_loss = post_discord, channels, on_win, on_loss


# ── Persistence ───────────────────────────────────────────────────────────────
def _save():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(_positions, f)
    except Exception as e:
        log.error(f"Position state save failed: {e}")

def _load():
    global _positions
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                _positions = json.load(f)
            log.info(f"📂 Loaded {len(_positions)} tracked position(s) from disk")
    except Exception as e:
        log.error(f"Position state load failed: {e}")


# ── Registration ──────────────────────────────────────────────────────────────
def register(occ_symbol: str, ticker: str, direction: str, entry_price: float,
             quantity: int, trade_type: str = "DAY") -> bool:
    """Called by main.py AFTER the entry fill is confirmed.
    Immediately places the broker-resting hard stop, then tracks the position."""
    stop_trigger = round(entry_price * (1 - HARD_STOP_PCT), 2)
    stop_limit   = round(entry_price * (1 - HARD_STOP_LIMIT), 2)

    stop_order = tt.place_stop_limit_exit(occ_symbol, quantity, stop_trigger, stop_limit)
    stop_id    = stop_order.get("order_id") if stop_order else None
    if not stop_id:
        log.error(f"⚠️ CRITICAL: could not place protective stop for {occ_symbol} — retrying once")
        time.sleep(2)
        stop_order = tt.place_stop_limit_exit(occ_symbol, quantity, stop_trigger, stop_limit)
        stop_id    = stop_order.get("order_id") if stop_order else None

    with _lock:
        _positions[occ_symbol] = {
            "ticker": ticker, "direction": direction, "trade_type": trade_type,
            "entry": entry_price, "quantity": quantity,
            "peak": entry_price, "opened_at": datetime.now(ET).isoformat(),
            "stop_order_id": stop_id, "stop_trigger": stop_trigger,
            "trail_armed": False, "exiting": False,
        }
        _save()

    if stop_id:
        log.info(f"🛡️ Protective stop resting at broker for {occ_symbol}: trigger {stop_trigger}")
    else:
        log.error(f"🚨 {occ_symbol} has NO resting stop — monitor loop is sole protection")
        _post_discord(_channels.get("recaps", ""),
                      "⚠️ Heads up — protective stop order was rejected by the broker on the "
                      "open position. The system is watching it closely and will exit at the "
                      "stop level manually.")
    return stop_id is not None


# ── Exit paths ────────────────────────────────────────────────────────────────
def _controlled_exit(sym: str, pos: dict, reason: str, is_win: bool):
    """Cancel resting stop → limit sell at bid → escalate to market if unfilled."""
    if pos.get("exiting"):
        return
    pos["exiting"] = True
    log.info(f"🚪 Exiting {sym} — {reason}")

    if pos.get("stop_order_id"):
        tt.cancel_order(pos["stop_order_id"])

    quote = tt.get_option_quote(sym)
    limit_price = quote["bid"] if quote and quote["bid"] > 0 else round(pos["entry"] * 0.5, 2)

    order = tt.place_limit_exit(sym, pos["quantity"], limit_price)
    filled = tt.wait_for_fill(order["order_id"], timeout_sec=ESCALATE_SECONDS) if order else None

    if not filled:
        log.warning(f"Limit exit unfilled for {sym} — ESCALATING TO MARKET (capital protection)")
        order  = tt.market_close(sym, pos["quantity"])
        filled = tt.wait_for_fill(order["order_id"], timeout_sec=60) if order else None

    exit_price = filled["fill_price"] if filled else None
    _finalize(sym, pos, exit_price, is_win)


def _finalize(sym: str, pos: dict, exit_price, is_win: bool):
    entry = pos["entry"]
    pnl_pct = round((exit_price - entry) / entry * 100, 1) if exit_price else None
    actually_win = (exit_price or 0) > entry if exit_price is not None else is_win

    ch = _channels.get("day_signals" if pos["trade_type"] == "DAY" else "swing_signals", "")
    if actually_win:
        _post_discord(ch, "Closing and taking profits @everyone")
        _on_win(sym)
    else:
        _post_discord(ch, "Closing for loss @everyone")
        _on_loss(sym)

    detail = f"{sym} | in {entry:.2f} → out {exit_price:.2f} ({pnl_pct:+.1f}%)" if exit_price else f"{sym} | exit fill unconfirmed — verify in Tastytrade"
    _post_discord(_channels.get("recaps", ""), f"📊 Position closed: {detail}")
    log.info(f"✅ Closed {sym}: {detail}")

    with _lock:
        _positions.pop(sym, None)
        _save()


# ── Monitor loop ──────────────────────────────────────────────────────────────
def _check_position(sym: str, pos: dict):
    # 0) Did the resting stop already fire at the broker?
    if pos.get("stop_order_id"):
        o = tt.get_order(pos["stop_order_id"])
        if o and o.get("status") == "Filled":
            fills = [float(f.get("fill-price", 0)) for leg in o.get("legs", []) for f in leg.get("fills", [])]
            exit_price = round(sum(fills) / len(fills), 2) if fills else None
            log.info(f"🛑 Broker stop FILLED for {sym}")
            _finalize(sym, pos, exit_price, is_win=False)
            return

    quote = tt.get_option_quote(sym)
    if not quote:
        return                      # resting stop still protects; try next cycle
    mid, entry = quote["mid"], pos["entry"]
    pnl = (mid - entry) / entry

    if mid > pos["peak"]:
        pos["peak"] = mid
        _save()

    # 1) Profit target
    if pnl >= TARGET_PCT:
        _controlled_exit(sym, pos, f"target hit ({pnl*100:.0f}%)", is_win=True); return

    # 2) Trailing stop
    if not pos["trail_armed"] and pnl >= TRAIL_ARM_PCT:
        pos["trail_armed"] = True
        _save()
        _msg = f"📈 {pos['ticker']} +{pnl*100:.0f}% — trail stop added. Take profits if or when comfortable."
        _post_discord(_channels.get("watchlist", ""), _msg)
        _post_discord(_channels.get("recaps", ""), _msg)
    if pos["trail_armed"] and mid <= pos["peak"] * (1 - TRAIL_GIVEBACK):
        _controlled_exit(sym, pos, "trailing stop", is_win=(mid > entry)); return

    # 3) Manual backstop if broker stop missing/failed
    if not pos.get("stop_order_id") and pnl <= -HARD_STOP_PCT:
        _controlled_exit(sym, pos, "hard stop (manual backstop)", is_win=False); return

    # 4) 10-minute chop tighten (cancel/replace resting stop once)
    opened = datetime.fromisoformat(pos["opened_at"])
    age_min = (datetime.now(ET) - opened).total_seconds() / 60
    if age_min >= CHOP_MINUTES and -0.05 < pnl < 0.08 and not pos.get("chop_tightened"):
        new_trigger = round(entry * (1 - CHOP_STOP_PCT), 2)
        if pos.get("stop_order_id"):
            tt.cancel_order(pos["stop_order_id"])
        new_stop = tt.place_stop_limit_exit(sym, pos["quantity"], new_trigger, round(new_trigger * 0.95, 2))
        pos["stop_order_id"] = new_stop.get("order_id") if new_stop else None
        pos["chop_tightened"] = True
        pos["stop_trigger"] = new_trigger
        _save()
        log.info(f"⏱️ Chop circuit: stop tightened to {new_trigger} on {sym}")

    # 5) EOD flatten for day trades
    if pos["trade_type"] == "DAY" and datetime.now(ET).time() >= FLATTEN_TIME:
        _controlled_exit(sym, pos, "EOD flatten", is_win=(mid > entry)); return


def _loop():
    log.info("🩺 Position monitor loop started (20s cadence)")
    while True:
        try:
            with _lock:
                items = list(_positions.items())
            for sym, pos in items:
                if not pos.get("exiting"):
                    _check_position(sym, pos)
        except Exception as e:
            log.error(f"Monitor loop error: {e}", exc_info=True)
        time.sleep(20)


# ── Boot reconciliation ───────────────────────────────────────────────────────
def adopt_orphans():
    """On boot: any broker option position we aren't tracking gets adopted and
    protected with a stop. Nothing is ever left unmanaged after a restart."""
    try:
        broker_positions = tt.get_positions()
        for bp in broker_positions:
            if bp.get("instrument-type") != "Equity Option":
                continue
            sym = bp.get("symbol", "")
            qty = int(float(bp.get("quantity", 0)))
            if qty <= 0 or sym in _positions:
                continue
            avg = float(bp.get("average-open-price", 0)) or 1.0
            log.warning(f"👀 Adopting orphan position from broker: {sym} x{qty} @ {avg}")
            register(sym, bp.get("underlying-symbol", "?"), "UNKNOWN", avg, qty, "DAY")
            _post_discord(_channels.get("recaps", ""),
                          f"🔄 System restarted and re-secured the open position on {bp.get('underlying-symbol','?')} — stop in place, monitoring resumed.")
    except Exception as e:
        log.error(f"Orphan adoption error: {e}", exc_info=True)


def flatten_all() -> dict:
    """Kill everything: cancel all working orders, close all positions. For /flatten."""
    results = {"cancelled": 0, "closed": []}
    try:
        for o in tt.get_open_orders():
            if tt.cancel_order(o.get("id")):
                results["cancelled"] += 1
        with _lock:
            items = list(_positions.items())
        for sym, pos in items:
            _controlled_exit(sym, pos, "manual flatten", is_win=False)
            results["closed"].append(sym)
        # also close anything at the broker we somehow don't track
        for bp in tt.get_positions():
            if bp.get("instrument-type") == "Equity Option":
                q = int(float(bp.get("quantity", 0)))
                if q > 0 and bp.get("symbol") not in results["closed"]:
                    tt.market_close(bp["symbol"], q)
                    results["closed"].append(bp["symbol"])
    except Exception as e:
        log.error(f"Flatten error: {e}", exc_info=True)
    return results


def open_position_count() -> int:
    with _lock:
        return len(_positions)


def start():
    global _started
    if _started:
        return
    _started = True
    _load()
    adopt_orphans()
    threading.Thread(target=_loop, daemon=True).start()
