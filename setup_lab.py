"""TPP SETUP LAB (v14.22)
Pure, dependency-free setup detectors + a trade simulator that runs the SAME
rules on a full day of 1-minute bars. Used two ways:
  * /setup-lab replays the last N sessions and scores every setup;
  * (next build) the live engine calls lab_signal() on today's bars, so live
    decisions match the replay exactly.

Bars are dicts {"t": ISO-UTC, "o","h","l","c","v"} in ascending order.
Times are handled as ET minutes-since-midnight via the caller-supplied
converter so this file has no timezone dependency.

Setups (Junior, Nov 13): scan from the 9:30 open.
  BREAKOUT     - a 5-min candle CLOSES beyond a key level (pre-market or prior
                 day high/low) decisively; 1-min + VWAP agree; not chasing.
                 Stop = back through the level.
  REVERSAL     - price tags a key level and a 5-min candle REJECTS it (long
                 wick, closes back inside); the next 1-min bar confirms the turn.
                 Stop = beyond the rejection candle's extreme.
  CONTINUATION - the VWAP pullback, only once a real trend exists (10:00+),
                 with the last completed 15-min and 5-min bars agreeing.
                 Stop = beyond the pullback extreme.
  BASELINE_9_45 - the CURRENT live logic (VWAP pullback from 9:45, flat stop
                 ~= the -20% option stop) so every result has a comparison.

Management (all setups): risk R = |entry - stop|. Half off at +1R, stop to
break-even, rest exits at +2R, stop, or the 11:30 window end. Results are in
R (risk units) and an estimated $ per contract (delta ~0.5 -> $50 per $1 of
stock move).
"""

from __future__ import annotations

LEVEL_BUF = 0.0005        # 0.05% beyond a level counts as "beyond"
CHASE_MAX = 0.005         # skip breakout entries already >0.5% past the level
STOP_BUF = 0.0005         # stop placed 0.05% beyond the invalidation point
MIN_RISK_PCT = 0.0010     # stops tighter than 0.10% are noise -> widen to this
MAX_RISK_PCT = 0.0080     # setups needing >0.80% risk are skipped
WICK_MIN = 0.40           # reversal: rejection wick >= 40% of the 5-min range
WINDOW_START = 9 * 60 + 30
WINDOW_END = 11 * 60 + 30
CONT_START = 10 * 60
BASE_START = 9 * 60 + 45
BASE_STOP_USD = 1.12      # -20% on a ~$2.80, ~0.5-delta contract
DELTA_USD = 50.0          # est. $ per contract per $1 stock move


def _ema(xs, p):
    out, e, k = [], None, 2.0 / (p + 1)
    for x in xs:
        e = x if e is None else x * k + e * (1 - k)
        out.append(e)
    return out


def _agg(bars, n):
    """Aggregate RTH 1-min bars (with 'm' = ET minute) into n-min bars aligned
    to 9:30. Returns list of dicts with 'end' = last 1-min bar index."""
    out, cur = [], None
    for i, b in enumerate(bars):
        slot = (b["m"] - WINDOW_START) // n
        if cur is None or cur["slot"] != slot:
            if cur:
                out.append(cur)
            cur = {"slot": slot, "o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"],
                   "v": b["v"], "end": i, "n": 1}
        else:
            cur["h"] = max(cur["h"], b["h"])
            cur["l"] = min(cur["l"], b["l"])
            cur["c"] = b["c"]
            cur["v"] += b["v"]
            cur["end"] = i
            cur["n"] += 1
    if cur:
        out.append(cur)
    return out


def prepare_day(bars, to_et_min, prior_day=None):
    """Split a day's bars into pre-market + RTH, compute VWAP, levels.
    prior_day: (high, low) of the previous regular session, or None."""
    pre, rth = [], []
    for b in bars:
        m = to_et_min(b["t"])
        bb = dict(b, m=m)
        if 4 * 60 <= m < WINDOW_START:
            pre.append(bb)
        elif WINDOW_START <= m < 16 * 60:
            rth.append(bb)
    pv = vv = 0.0
    for b in rth:
        tp = (b["h"] + b["l"] + b["c"]) / 3.0
        pv += tp * b["v"]
        vv += b["v"]
        b["vwap"] = pv / vv if vv else b["c"]
    levels = []
    if pre:
        levels.append(("pre-market high", max(b["h"] for b in pre), "res"))
        levels.append(("pre-market low", min(b["l"] for b in pre), "sup"))
    if prior_day and prior_day[0] and prior_day[1]:
        levels.append(("prior-day high", float(prior_day[0]), "res"))
        levels.append(("prior-day low", float(prior_day[1]), "sup"))
    # merge levels within 0.15% (they are one zone, not two confirmations)
    merged = []
    for name, px, kind in sorted(levels, key=lambda x: x[1]):
        if merged and abs(px - merged[-1][1]) / px < 0.0015:
            merged[-1] = (merged[-1][0] + " / " + name, (merged[-1][1] + px) / 2, merged[-1][2])
        else:
            merged.append((name, px, kind))
    return {"rth": rth, "levels": merged}


def _clamp_risk(entry, stop, direction):
    risk = abs(entry - stop)
    if risk < entry * MIN_RISK_PCT:
        risk = entry * MIN_RISK_PCT
        stop = entry - risk if direction == "call" else entry + risk
    if risk > entry * MAX_RISK_PCT:
        return None
    return stop


def _signals_at(day, i, done, setups=None):
    """All setup signals that trigger on RTH bar index i (bar i just closed).
    done: set of (setup) already traded today (one per setup per day)."""
    rth, levels = day["rth"], day["levels"]
    b = rth[i]
    m = b["m"]
    if m < WINDOW_START or m >= WINDOW_END:
        return []
    want = lambda s: (setups is None or s in setups) and s not in done
    out = []
    bars = rth[: i + 1]
    five = [x for x in _agg(bars, 5) if x["n"] == 5]       # completed 5-min bars
    fifteen = [x for x in _agg(bars, 15) if x["n"] == 15]  # completed 15-min bars
    px = b["c"]
    vw = b["vwap"]

    # ---------- BREAKOUT: evaluated on the bar right after a 5-min close ----------
    if want("BREAKOUT") and five and five[-1]["end"] == i - 1:
        f = five[-1]
        rng = max(f["h"] - f["l"], 1e-9)
        for name, L, _k in levels:
            for direction in ("call", "put"):
                if direction == "call":
                    broke = f["c"] > L * (1 + LEVEL_BUF) and f["o"] < f["c"] and (f["c"] - f["l"]) / rng >= 0.7
                    first = all(x["c"] <= L * (1 + LEVEL_BUF) for x in five[:-1])
                    agree = px > vw and px > L
                    chase = (px - L) / L > CHASE_MAX
                    stop = L * (1 - STOP_BUF)
                else:
                    broke = f["c"] < L * (1 - LEVEL_BUF) and f["o"] > f["c"] and (f["h"] - f["c"]) / rng >= 0.7
                    first = all(x["c"] >= L * (1 - LEVEL_BUF) for x in five[:-1])
                    agree = px < vw and px < L
                    chase = (L - px) / L > CHASE_MAX
                    stop = L * (1 + STOP_BUF)
                if broke and first and agree and not chase:
                    st = _clamp_risk(px, stop, direction)
                    if st is not None:
                        out.append({"setup": "BREAKOUT", "dir": direction, "entry": px, "stop": st,
                                    "i": i, "why": f"5-min close {'above' if direction == 'call' else 'below'} {name} ${L:.2f}"})
                        break
            if out and out[-1]["setup"] == "BREAKOUT":
                break

    # ---------- REVERSAL: 5-min rejection of a level, 1-min confirmation ----------
    if want("REVERSAL") and len(five) >= 1 and five[-1]["end"] == i - 1 and i >= 1:
        f = five[-1]
        rng = max(f["h"] - f["l"], 1e-9)
        prev1 = rth[i - 1]
        for name, L, _k in levels:
            # resistance rejection -> puts
            if f["h"] >= L * (1 - LEVEL_BUF) and f["c"] < L and (f["h"] - max(f["o"], f["c"])) / rng >= WICK_MIN \
                    and f["c"] < f["o"] and b["c"] < prev1["l"]:
                st = _clamp_risk(px, f["h"] * (1 + STOP_BUF), "put")
                if st is not None:
                    out.append({"setup": "REVERSAL", "dir": "put", "entry": px, "stop": st, "i": i,
                                "why": f"5-min rejection at {name} ${L:.2f}, 1-min confirmed down"})
                    break
            # support rejection -> calls
            if f["l"] <= L * (1 + LEVEL_BUF) and f["c"] > L and (min(f["o"], f["c"]) - f["l"]) / rng >= WICK_MIN \
                    and f["c"] > f["o"] and b["c"] > prev1["h"]:
                st = _clamp_risk(px, f["l"] * (1 - STOP_BUF), "call")
                if st is not None:
                    out.append({"setup": "REVERSAL", "dir": "call", "entry": px, "stop": st, "i": i,
                                "why": f"5-min rejection at {name} ${L:.2f}, 1-min confirmed up"})
                    break

    # ---------- CONTINUATION + BASELINE (VWAP pullback) ----------
    for setup, start in (("CONTINUATION", CONT_START), ("BASELINE_9_45", BASE_START)):
        if not want(setup) or m < start or i < 14:
            continue
        C = [x["c"] for x in bars]
        VW = [x["vwap"] for x in bars]
        e8, e21 = _ema(C, 8), _ema(C, 21)
        above = sum(1 for k in range(i - 12, i) if C[k] > VW[k])
        d = None
        if above >= 10 and e8[i] > e21[i] and min(C[i - 2:i]) <= VW[i - 1] * 1.0015 and C[i] > C[i - 1] and C[i] > VW[i]:
            d = "call"
        elif (12 - above) >= 10 and e8[i] < e21[i] and max(C[i - 2:i]) >= VW[i - 1] * 0.9985 and C[i] < C[i - 1] and C[i] < VW[i]:
            d = "put"
        if not d:
            continue
        if setup == "BASELINE_9_45":
            stop = px - BASE_STOP_USD if d == "call" else px + BASE_STOP_USD
            out.append({"setup": setup, "dir": d, "entry": px, "stop": stop, "i": i,
                        "why": "current live VWAP pullback"})
            continue
        # multi-timeframe agreement: last completed 15-min AND 5-min on the trend side of VWAP
        if not fifteen or not five:
            continue
        f15, f5 = fifteen[-1], five[-1]
        vw15, vw5 = bars[f15["end"]]["vwap"], bars[f5["end"]]["vwap"]
        ok = (f15["c"] > vw15 and f5["c"] > vw5 and f15["c"] > f15["o"]) if d == "call" else \
             (f15["c"] < vw15 and f5["c"] < vw5 and f15["c"] < f15["o"])
        if not ok:
            continue
        ext = min(x["l"] for x in bars[i - 3:i + 1]) if d == "call" else max(x["h"] for x in bars[i - 3:i + 1])
        st = _clamp_risk(px, ext * (1 - STOP_BUF) if d == "call" else ext * (1 + STOP_BUF), d)
        if st is not None:
            out.append({"setup": setup, "dir": d, "entry": px, "stop": st, "i": i,
                        "why": "VWAP pullback with 15-min + 5-min trend agreeing"})
    return out


def _manage(day, sig):
    """Simulate one trade from bar sig['i']+1. Returns result dict."""
    rth = day["rth"]
    d, entry, stop = sig["dir"], sig["entry"], sig["stop"]
    risk = abs(entry - stop)
    sgn = 1 if d == "call" else -1
    t1, t2 = entry + sgn * risk, entry + sgn * 2 * risk
    half_done, realized, cur_stop = False, 0.0, stop
    peak = trough = 0.0
    exit_px, reason, j = None, "window end", sig["i"]
    for j in range(sig["i"] + 1, len(rth)):
        b = rth[j]
        fav = (b["h"] - entry) if d == "call" else (entry - b["l"])
        adv = (b["l"] - entry) if d == "call" else (entry - b["h"])
        peak, trough = max(peak, fav / risk), min(trough, adv / risk)
        hit_stop = (b["l"] <= cur_stop) if d == "call" else (b["h"] >= cur_stop)
        if hit_stop:   # conservative: stop checked before targets inside a bar
            exit_px, reason = cur_stop, ("break-even stop" if half_done else "stop")
            break
        if not half_done and ((b["h"] >= t1) if d == "call" else (b["l"] <= t1)):
            half_done, realized, cur_stop = True, 0.5 * 1.0, entry
        if (b["h"] >= t2) if d == "call" else (b["l"] <= t2):
            exit_px, reason = t2, "target +2R"
            break
        if b["m"] >= WINDOW_END:
            exit_px, reason = b["c"], "window end"
            break
    if exit_px is None:
        exit_px = rth[j]["c"] if rth else entry
    rest_r = sgn * (exit_px - entry) / risk
    r = realized + (0.5 if half_done else 1.0) * rest_r
    usd = r * risk * DELTA_USD
    return {"setup": sig["setup"], "dir": d, "entry": round(entry, 2), "stop": round(stop, 2),
            "exit": round(exit_px, 2), "r": round(r, 2), "usd": round(usd, 0), "win": r > 0,
            "reason": reason, "why": sig["why"], "peak_r": round(peak, 2), "trough_r": round(trough, 2),
            "mins": rth[j]["m"] - rth[sig["i"]]["m"], "risk_pct": round(risk / entry * 100, 2),
            "end_i": j}


def run_day(day, setups=None):
    """Every setup independently: first signal of each setup per day, simulated."""
    trades, done = [], set()
    for i in range(len(day["rth"])):
        for sig in _signals_at(day, i, done, setups):
            if sig["setup"] in done:
                continue
            done.add(sig["setup"])
            tr = _manage(day, sig)
            tr["time"] = "%d:%02d" % divmod(day["rth"][i]["m"], 60)
            trades.append(tr)
    return trades


def lab_signal(day, setups):
    """Live hook: signals on the LAST bar of today's data (next build)."""
    if not day["rth"]:
        return []
    return _signals_at(day, len(day["rth"]) - 1, set(), setups)


def scorecard(rows):
    """rows: trades with 'setup','ticker','r','usd','win'."""
    by = {}
    for r in rows:
        by.setdefault(r["setup"], []).append(r)
    out = {}
    for s, rs in by.items():
        n = len(rs)
        w = sum(1 for r in rs if r["win"])
        streak = cur = 0
        for r in rs:
            cur = cur + 1 if not r["win"] else 0
            streak = max(streak, cur)
        tick = {}
        for r in rs:
            t = tick.setdefault(r.get("ticker"), [0, 0, 0.0])
            t[0 if r["win"] else 1] += 1
            t[2] += r["r"]
        out[s] = {"n": n, "w": w, "l": n - w, "win_rate": round(w / n, 2) if n else None,
                  "total_r": round(sum(r["r"] for r in rs), 2),
                  "avg_r": round(sum(r["r"] for r in rs) / n, 2) if n else None,
                  "est_usd_per_contract": round(sum(r["usd"] for r in rs)),
                  "worst_losing_streak": streak,
                  "by_ticker": {k: f"{v[0]}W/{v[1]}L {v[2]:+.1f}R" for k, v in tick.items()}}
    return out


def report_text(card, days, tickers):
    order = ["BASELINE_9_45", "BREAKOUT", "REVERSAL", "CONTINUATION"]
    label = {"BASELINE_9_45": "CURRENT LIVE (VWAP pullback from 9:45)", "BREAKOUT": "BREAKOUT (5-min close through key level)",
             "REVERSAL": "KEY-LEVEL REVERSAL (5-min rejection + 1-min confirm)",
             "CONTINUATION": "TREND CONTINUATION (10:00+, 15m/5m agree)"}
    L = [f"🧪 **SETUP LAB — last {days} sessions · {', '.join(tickers)}**",
         "R = units of risk (1R = what the stop would lose). Positive avg R = setup makes money over time.", ""]
    base = card.get("BASELINE_9_45")
    for s in order:
        c = card.get(s)
        if not c:
            L.append(f"**{label[s]}** — no trades")
            continue
        verdict = ""
        if s != "BASELINE_9_45":
            beats = c["avg_r"] is not None and c["avg_r"] > 0 and (not base or c["avg_r"] > (base["avg_r"] or 0))
            verdict = " → ✅ candidate to go live" if (beats and c["n"] >= 10) else (
                " → ⏳ too few trades to judge" if c["n"] < 10 else " → ❌ stays off")
        L.append(f"**{label[s]}**{verdict}")
        L.append(f"  {c['w']}W/{c['l']}L ({c['win_rate']:.0%}) · avg {c['avg_r']:+.2f}R · total {c['total_r']:+.1f}R "
                 f"· est ${c['est_usd_per_contract']:+,} per contract · worst streak {c['worst_losing_streak']}L")
        L.append("  " + " | ".join(f"{k} {v}" for k, v in c["by_ticker"].items()))
    return "\n".join(L)
