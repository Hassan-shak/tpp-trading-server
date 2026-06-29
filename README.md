# The Portfolio Plug — Trading Webhook Server

## What this does
Receives TradingView alerts → runs through Claude AI with Junior's system prompt → posts signals to Discord in Junior's exact voice.

## Files
- `main.py` — the server
- `system_prompt.txt` — Junior's full trading playbook (place in same folder)
- `requirements.txt` — Python dependencies
- `render.yaml` — Render deployment config

---

## Step 1 — Deploy to Render

1. Go to https://render.com and create a free account
2. Click **New → Web Service**
3. Connect your GitHub account and create a new repo called `tpp-trading-server`
4. Upload all files from this folder to that repo
5. Render auto-detects `render.yaml` and configures everything
6. Click **Deploy**

Your server URL will be: `https://tpp-trading-server.onrender.com`

---

## Step 2 — Set environment variables in Render

In your Render dashboard → your service → **Environment**:

| Variable | Value |
|---|---|
| `ANTHROPIC_API_KEY` | Your key from console.anthropic.com |
| `WEBHOOK_SECRET` | `TPP_WEBHOOK_2024` (or change it) |
| `DISCORD_BOT_TOKEN` | From Discord Developer Portal |
| `DISCORD_GUILD_ID` | Your Discord server ID |
| `DISCORD_CHANNEL_WATCHLIST` | Channel ID for #daily-watchlist |
| `DISCORD_CHANNEL_DAY_SIGNALS` | Channel ID for #day-trade-signals |
| `DISCORD_CHANNEL_SWING_SIGNALS` | Channel ID for #swing-trade-signals |
| `DISCORD_CHANNEL_RECAPS` | Channel ID for #profits-and-recaps |
| `DISCORD_CHANNEL_LONGTERM` | Channel ID for #long-term-stock-investing |

---

## Step 3 — Get Discord channel IDs

In Discord:
1. Go to Settings → Advanced → Enable Developer Mode
2. Right-click any channel → **Copy Channel ID**
3. Paste into Render environment variables above

---

## Step 4 — Create a Discord bot

1. Go to https://discord.com/developers/applications
2. Click **New Application** → name it "Portfolio Plug Bot"
3. Go to **Bot** tab → **Reset Token** → copy the token
4. Paste token as `DISCORD_BOT_TOKEN` in Render
5. Go to **OAuth2 → URL Generator**:
   - Scopes: `bot`
   - Bot Permissions: `Send Messages`, `Read Message History`
6. Copy the generated URL → open it → add the bot to your Discord server

---

## Step 5 — Update TradingView alerts with your Render URL

Go back to each TradingView alert you created and paste this as the Webhook URL:
```
https://tpp-trading-server.onrender.com/webhook
```

---

## Step 6 — Test the connection

Open your browser and visit:
```
https://tpp-trading-server.onrender.com/health
```

You should see a JSON response with server status, current ET time, and session state.

Then send a test webhook manually:
```bash
curl -X POST https://tpp-trading-server.onrender.com/webhook \
  -H "Content-Type: application/json" \
  -d '{"ticker":"NVDA","price":200.00,"volume":1500000,"high":201.00,"low":199.00,"time":"2024-01-01T09:35:00","interval":"1","alert_type":"PMH_BREAKOUT","direction":"CALL","secret":"TPP_WEBHOOK_2024"}'
```

Check your #day-trade-signals Discord channel — you should see Claude's formatted signal appear.

---

## API Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/webhook` | POST | Receives TradingView alerts |
| `/health` | GET | Server status + session state |
| `/kill` | POST | Manually activate/deactivate kill switch |
| `/loss` | POST | Log a losing trade (tracks circuit breaker) |
| `/win` | POST | Log a winning trade (resets loss counter) |

---

## Important notes

- The server resets session state (trade count, circuit breaker) automatically at midnight ET each day
- Free Render plan sleeps after 15 minutes of inactivity — upgrade to the $7/mo paid plan to keep it always awake (critical for trading)
- The `system_prompt.txt` file must be in the same directory as `main.py`
- Never commit your API keys to GitHub — always use Render's environment variables
