# TPP v5.0 — Deploy Checklist

Deploy outside 9 AM – 4 PM ET. Complete steps in order.

---

## Step 1 — Replace Files in GitHub
Push these files into `Hassan-shak/tpp-trading-server`, replacing existing versions:

- `app.py`
- `webhook_handler.py`
- `gate_checks.py`
- `claude_brain.py`
- `session_state.py`
- `execution_engine.py`
- `tastytrade_client.py`
- `scheduler.py`
- `discord_bot.py`

Keep your existing `alpaca_data.py` unchanged — v5 imports from it as-is.

---

## Step 2 — Add New Env Vars in Render

**New in v5 — add these:**
| Variable | Where to find it |
|---|---|
| `TASTYTRADE_USERNAME` | Your Tastytrade login email |
| `TASTYTRADE_PASSWORD` | Your Tastytrade password |
| `TASTYTRADE_ACCOUNT_NUMBER` | Tastytrade account page |
| `DISCORD_CHANNEL_WATCHLIST` | Right-click #daily-watchlist → Copy ID |
| `DISCORD_CHANNEL_SIGNALS` | Right-click #day-trade-signals → Copy ID |
| `DISCORD_CHANNEL_RECAPS` | Right-click #profits-and-recaps → Copy ID |
| `JUNIOR_DISCORD_USER_ID` | Your own Discord user ID |
| `MANUAL_BLACKOUT` | Set to `0` |

**Already set — verify these exist:**
- `ALPACA_API_KEY` / `ALPACA_API_SECRET`
- `ANTHROPIC_API_KEY`
- `DISCORD_BOT_TOKEN`
- `WEBHOOK_SECRET`

---

## Step 3 — Update TradingView Alerts
For every NVDA and TSLA alert:
1. Edit the alert **Message** to match the JSON template in `env_vars_reference.md`
2. Confirm `plot_0` = PMH line, `plot_1` = PML line
3. Set alert comment to `PMH_BREAK` or `PML_BREAK`

SPY/QQQ alerts: leave them alone. They're blocked at the gate automatically.

---

## Step 4 — Deploy
Push to GitHub → Render auto-deploys.

---

## Step 5 — Smoke Test
Hit `/status` before 9:30 AM:
```
GET https://tpp-trading-server.onrender.com/status
```
Expected:
```json
{
  "time_et": "...",
  "trade_count": 0,
  "circuit_breaker": false,
  "consecutive_losses": 0,
  "open_position": false,
  "open_symbol": null,
  "last_reset_date": "today"
}
```

---

## Step 6 — Watch Render Logs at 9:30 AM
Every webhook logs one of:
- `GATE PASSED [NVDA] [entry] — all checks clear → sending to Claude`
- `GATE BLOCKED [NVDA] [entry] — window: outside 9:30–10:30 AM window`
- `EXECUTE: NVDA CALL`
- `FILLED: NVDAXXXXXX @ $X.XX/share`
- `Signal posted to #day-trade-signals`

---

## Kill Switches
```
POST /flatten  → closes open position, posts recap
POST /kill     → closes position + halts trading for the day
MANUAL_BLACKOUT=1 → blocks all new signals (flip back to 0 to resume)
```

---

## What Changed v4 → v5

| Area | v4 | v5 |
|---|---|---|
| Trading windows | AM + PM | AM only (9:30–10:30) |
| Timeframe | Unspecified | 1-min explicit |
| PMH/PML | Alert name only | Level value in webhook payload, trusted as ground truth |
| Contract pricing | Ambiguous | Ask price $0.75–$1.50/share ($75–$150/contract), explicit |
| OTM walk | None | ATM → OTM until in range or stopped |
| Dead ticker rule | Claude's judgment | Requires all 3 conditions simultaneously |
| "Choppy" skip | Allowed | Never a valid skip reason |
| Mandatory attempt | Yes (10:00 AM) | Removed — only valid setups traded |
| HIGH RISK tier | Present | Removed |
| 10:30 AM behavior | Force flatten | Window closes to new entries only; open position runs until natural exit |
| Post-window monitoring | Stops at 10:30 | Continues until trade closes, recap posts whenever it closes |
| Status updates | Pulse every 15 min | 9:45 AM + 10:15 AM only, skipped if trade already fired |
| SPY/QQQ commentary | Leaking through | Hard blocked — zero output |
| Entry cooldown | 5 min (shared) | 0s — entry signals bypass cooldown entirely |
| Stop-loss failure | Silent | Emergency DM to Junior |
| Signal audit log | None | Every webhook logged with pass/block reason |
