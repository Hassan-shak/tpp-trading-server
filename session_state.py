"""
session_state.py
TPP Trading Server v5.0

Single trading window: 9:30–10:30 AM ET only.
State resets once per market day.

Persistence: three-layer
  1. In-memory dict       (fast, lost on restart)
  2. /tmp/session_state.json  (survives redeploy within same instance)
  3. Render env var SESSION_STATE_JSON (survives full restarts)

Keys:
  trade_count        : int   (0–2, max 2 trades/day)
  consecutive_losses : int   (circuit breaker trips at 2)
  circuit_breaker    : bool
  open_position      : dict | None
  last_reset_date    : str   YYYY-MM-DD
  daily_trade_log    : list
"""

import os
import json
import logging
from datetime import datetime
import pytz

ET  = pytz.timezone("America/New_York")
log = logging.getLogger("session_state")

_TMP_PATH = "/tmp/session_state.json"
_state: dict = {}


# ── persistence ───────────────────────────────────────────────────────────────
def _load_from_disk() -> dict:
    try:
        with open(_TMP_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _load_from_render_env() -> dict:
    raw = os.environ.get("SESSION_STATE_JSON", "")
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    return {}


def _save(state: dict):
    try:
        with open(_TMP_PATH, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"Could not save state to /tmp: {e}")


def _blank(today: str) -> dict:
    return {
        "trade_count":        0,
        "consecutive_losses": 0,
        "circuit_breaker":    False,
        "open_position":      None,
        "last_reset_date":    today,
        "daily_trade_log":    [],
    }


# ── load / reset ──────────────────────────────────────────────────────────────
def load_state() -> dict:
    global _state
    today = datetime.now(ET).strftime("%Y-%m-%d")

    if _state.get("last_reset_date") == today:
        return _state

    for source in [_load_from_disk, _load_from_render_env]:
        data = source()
        if data.get("last_reset_date") == today:
            _state = data
            _save(_state)
            return _state

    log.info(f"New trading day {today} — resetting session state")
    _state = _blank(today)
    _save(_state)
    return _state


def _commit(state: dict):
    global _state
    _state = state
    _save(state)


# ── public accessors ──────────────────────────────────────────────────────────
def get_trade_count() -> int:
    return load_state()["trade_count"]

def get_circuit_breaker() -> bool:
    return load_state()["circuit_breaker"]

def get_open_position() -> dict | None:
    return load_state()["open_position"]

def is_max_trades_reached() -> bool:
    return load_state()["trade_count"] >= 2

def increment_trade_count():
    s = load_state()
    s["trade_count"] += 1
    log.info(f"Trade count → {s['trade_count']}")
    _commit(s)

def set_open_position(position: dict):
    s = load_state()
    s["open_position"] = position
    _commit(s)

def clear_open_position():
    s = load_state()
    s["open_position"] = None
    _commit(s)

def record_trade_result(win: bool):
    s = load_state()
    if win:
        s["consecutive_losses"] = 0
        log.info("Win recorded — consecutive loss counter reset")
    else:
        s["consecutive_losses"] += 1
        log.info(f"Loss recorded — consecutive losses: {s['consecutive_losses']}")
        if s["consecutive_losses"] >= 2:
            s["circuit_breaker"] = True
            log.warning("CIRCUIT BREAKER — 2 consecutive losses, day over")
    s["daily_trade_log"].append({
        "result": "win" if win else "loss",
        "time":   datetime.now(ET).isoformat(),
    })
    _commit(s)
