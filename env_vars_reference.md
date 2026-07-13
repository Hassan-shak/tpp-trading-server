# TPP v5.0 — Environment Variables Reference

Set all of these in your Render dashboard under **Environment**.

---

## Tastytrade
| Variable | Value |
|---|---|
| `TASTYTRADE_USERNAME` | Your Tastytrade login email |
| `TASTYTRADE_PASSWORD` | Your Tastytrade password |
| `TASTYTRADE_ACCOUNT_NUMBER` | Your account number (e.g. `5WX12345`) |

## Alpaca
| Variable | Value |
|---|---|
| `ALPACA_API_KEY` | Your Alpaca API key |
| `ALPACA_API_SECRET` | Your Alpaca API secret |
| `ALPACA_BASE_URL` | `https://data.alpaca.markets` |

## Anthropic
| Variable | Value |
|---|---|
| `ANTHROPIC_API_KEY` | Your Anthropic API key |

## Discord
| Variable | Value |
|---|---|
| `DISCORD_BOT_TOKEN` | Your Discord bot token |
| `DISCORD_CHANNEL_WATCHLIST` | Channel ID for #daily-watchlist |
| `DISCORD_CHANNEL_SIGNALS` | Channel ID for #day-trade-signals |
| `DISCORD_CHANNEL_RECAPS` | Channel ID for #profits-and-recaps |
| `JUNIOR_DISCORD_USER_ID` | Your Discord user ID (for emergency DMs to you) |

## Webhook Security
| Variable | Value |
|---|---|
| `WEBHOOK_SECRET` | Secret string — set same value in TradingView alert URL header |

## Trading Controls
| Variable | Default | Notes |
|---|---|---|
| `MANUAL_BLACKOUT` | `0` | Set to `1` to pause all trading without redeploying |
| `SESSION_STATE_JSON` | (auto) | Do not set manually — server manages this |

---

## TradingView Alert Message Template

Use this JSON in the **Message** field of every NVDA and TSLA alert:

```json
{
  "ticker": "{{ticker}}",
  "alert_type": "{{strategy.order.comment}}",
  "close": {{close}},
  "open": {{open}},
  "high": {{high}},
  "low": {{low}},
  "volume": {{volume}},
  "level": {{plot_0}},
  "pmh": {{plot_0}},
  "pml": {{plot_1}},
  "time": "{{time}}"
}
```

**Setup:**
- `plot_0` → your PMH indicator plot
- `plot_1` → your PML indicator plot
- Alert comment field → set to `PMH_BREAK` or `PML_BREAK`
- Webhook URL: `https://tpp-trading-server.onrender.com/webhook`
- Header: `X-Signature: <your WEBHOOK_SECRET>`

You do not need to touch or delete SPY/QQQ alerts —
they are blocked at the gate and produce zero output.

---

## Discord Channel IDs
Right-click a channel in Discord → **Copy Channel ID**
(requires Developer Mode: User Settings → Advanced → Developer Mode)
