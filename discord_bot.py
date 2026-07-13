"""
discord_bot.py
TPP Trading Server v5.0

All Discord output. Three channels:
  daily-watchlist    — 9:15 AM watchlist (NVDA + TSLA only)
  day-trade-signals  — signals, status updates, no-contract warnings
  profits-and-recaps — trade closes + P&L recaps

Rules:
  - Deduplication: identical back-to-back messages are dropped
  - Zero SPY/QQQ output — filtered at gate level before reaching here
  - Emergency DM to Junior if stop-loss placement fails
"""

import os
import hashlib
import logging
import requests

log = logging.getLogger("discord")

DISCORD_TOKEN  = os.environ["DISCORD_BOT_TOKEN"]
JUNIOR_USER_ID = os.environ.get("JUNIOR_DISCORD_USER_ID", "")

CHANNEL_IDS = {
    "daily-watchlist":    os.environ.get("DISCORD_CHANNEL_WATCHLIST",  ""),
    "day-trade-signals":  os.environ.get("DISCORD_CHANNEL_SIGNALS",    ""),
    "profits-and-recaps": os.environ.get("DISCORD_CHANNEL_RECAPS",     ""),
}

DISCORD_API = "https://discord.com/api/v10"
_last_hash: dict[str, str] = {}


def _msg_hash(message: str) -> str:
    return hashlib.md5(message.encode()).hexdigest()


def post_to_discord(channel: str, message: str) -> bool:
    """
    Post to a Discord channel.
    Deduplicates: identical consecutive messages are skipped silently.
    """
    channel_id = CHANNEL_IDS.get(channel)
    if not channel_id:
        log.error(f"No channel ID configured for '{channel}'")
        return False

    h = _msg_hash(message)
    if _last_hash.get(channel) == h:
        log.info(f"Dedup skip — identical message already posted to #{channel}")
        return True

    headers = {
        "Authorization": f"Bot {DISCORD_TOKEN}",
        "Content-Type":  "application/json",
    }

    resp = requests.post(
        f"{DISCORD_API}/channels/{channel_id}/messages",
        headers=headers,
        json={"content": message},
        timeout=10,
    )

    if resp.status_code in (200, 201):
        _last_hash[channel] = h
        log.info(f"Posted to #{channel}: {message[:80]}…")
        return True

    log.error(f"Discord post failed #{channel}: {resp.status_code} {resp.text}")
    return False


def send_emergency_dm(message: str):
    """DM Junior directly for critical alerts (stop-loss failure, etc.)."""
    if not JUNIOR_USER_ID:
        log.warning("JUNIOR_DISCORD_USER_ID not set — cannot send emergency DM")
        return

    headers = {
        "Authorization": f"Bot {DISCORD_TOKEN}",
        "Content-Type":  "application/json",
    }

    dm_resp = requests.post(
        f"{DISCORD_API}/users/@me/channels",
        headers=headers,
        json={"recipient_id": JUNIOR_USER_ID},
        timeout=10,
    )

    if dm_resp.status_code not in (200, 201):
        log.error(f"Failed to create DM channel: {dm_resp.text}")
        return

    dm_channel_id = dm_resp.json()["id"]

    requests.post(
        f"{DISCORD_API}/channels/{dm_channel_id}/messages",
        headers=headers,
        json={"content": f"🚨 **TPP ALERT** 🚨\n{message}"},
        timeout=10,
    )
    log.warning(f"Emergency DM sent to Junior: {message}")
