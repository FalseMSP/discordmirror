"""
chat_mirror.py  (v2 — low-latency edition)
===========================================
Mirrors Twitch and YouTube Live chat to Discord with near-zero latency.

Architecture:
  • Twitch   → persistent WebSocket IRC via twitchio (~10–50 ms)
  • YouTube  → WebSub/PubSubHubbub push callbacks (sub-second)
              falls back to API polling if no public URL is configured
  • Discord  → async queue + background worker (never blocks ingest)

Dependencies:
    pip install -r requirements.txt

Quick start:
    1. cp .env.example .env  — fill in credentials
    2. If you want WebSub (recommended):
         expose port 8080 publicly, e.g. with ngrok:
           ngrok http 8080
         then set WEBSUB_PUBLIC_URL=https://<your-ngrok-id>.ngrok-free.app
    3. python chat_mirror.py
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import time
import xml.etree.ElementTree as ET
from collections import deque
from typing import Optional

import aiohttp
from aiohttp import web
from dotenv import load_dotenv

# ── Optional: twitchio ────────────────────────────────────────────────────
try:
    from twitchio.ext import commands as twitch_commands
    TWITCH_AVAILABLE = True
except ImportError:
    TWITCH_AVAILABLE = False
    print("[WARN] twitchio not installed — Twitch mirroring disabled.")

# ── Optional: Google API client (used for chat message fetching) ──────────
try:
    from googleapiclient.discovery import build as yt_build
    YT_GAPI_AVAILABLE = True
except ImportError:
    YT_GAPI_AVAILABLE = False

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("chat_mirror")

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DISCORD_WEBHOOK_URL   = os.getenv("DISCORD_WEBHOOK_URL", "")
DISCORD_TWITCH_PREFIX = os.getenv("DISCORD_TWITCH_PREFIX", "🟣")
DISCORD_YT_PREFIX     = os.getenv("DISCORD_YT_PREFIX", "🔴")
DISCORD_RATE_LIMIT    = float(os.getenv("DISCORD_RATE_LIMIT", "2"))   # msgs/sec max

# Twitch
TWITCH_TOKEN          = os.getenv("TWITCH_TOKEN", "")      # oauth:xxxx
TWITCH_CLIENT_ID      = os.getenv("TWITCH_CLIENT_ID", "")
TWITCH_BOT_NICK       = os.getenv("TWITCH_BOT_NICK", "")
TWITCH_CHANNELS       = [c.strip() for c in os.getenv("TWITCH_CHANNELS", "").split(",") if c.strip()]

# YouTube
YT_API_KEY            = os.getenv("YT_API_KEY", "")
YT_VIDEO_ID           = os.getenv("YT_VIDEO_ID", "")
YT_CHANNEL_ID         = os.getenv("YT_CHANNEL_ID", "")     # needed for WebSub
YT_POLL_INTERVAL      = int(os.getenv("YT_POLL_INTERVAL", "8"))

# WebSub  (leave WEBSUB_PUBLIC_URL blank to fall back to polling)
WEBSUB_PUBLIC_URL     = os.getenv("WEBSUB_PUBLIC_URL", "").rstrip("/")
WEBSUB_SECRET         = os.getenv("WEBSUB_SECRET", "change-me-please")
WEBSUB_PORT           = int(os.getenv("WEBSUB_PORT", "8080"))
WEBSUB_HUB            = "https://pubsubhubbub.appspot.com/"
WEBSUB_LEASE_SECONDS  = 86400   # re-subscribe every 24 h


# ─────────────────────────────────────────────────────────────────────────────
#  ASYNC DISCORD QUEUE
#  All platforms drop (username, message, platform) tuples here.
#  A single background worker drains the queue while respecting Discord's
#  rate limit — nothing in the ingest path ever blocks on an HTTP call.
# ─────────────────────────────────────────────────────────────────────────────

_discord_queue: asyncio.Queue[tuple[str, str, str]] = asyncio.Queue(maxsize=500)


async def discord_worker(session: aiohttp.ClientSession) -> None:
    """Drains _discord_queue and POSTs to Discord, handling 429 retries."""
    min_interval = 1.0 / DISCORD_RATE_LIMIT
    last_send    = 0.0

    while True:
        username, message, platform = await _discord_queue.get()
        try:
            wait = min_interval - (time.monotonic() - last_send)
            if wait > 0:
                await asyncio.sleep(wait)

            prefix  = DISCORD_TWITCH_PREFIX if platform == "twitch" else DISCORD_YT_PREFIX
            content = f"{prefix} **{username}**: {message}"

            async with session.post(
                DISCORD_WEBHOOK_URL,
                json={"content": content},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 429:
                    data  = await resp.json()
                    retry = float(data.get("retry_after", 1))
                    log.warning("Discord rate-limited — retrying in %.1fs", retry)
                    await asyncio.sleep(retry)
                    await _discord_queue.put((username, message, platform))   # re-queue
                elif resp.status >= 400:
                    log.error("Discord webhook HTTP %d", resp.status)
                else:
                    last_send = time.monotonic()
                    log.debug("→ Discord [%s] %s: %s", platform, username, message)

        except Exception as exc:
            log.error("Discord worker error: %s", exc)
        finally:
            _discord_queue.task_done()


def enqueue(username: str, message: str, platform: str) -> None:
    """Non-blocking drop into the Discord queue (backpressure: drops if full)."""
    try:
        _discord_queue.put_nowait((username, message, platform))
    except asyncio.QueueFull:
        log.warning("Discord queue full — dropping message from %s", username)


# ─────────────────────────────────────────────────────────────────────────────
#  TWITCH  (already push-based via WebSocket IRC)
# ─────────────────────────────────────────────────────────────────────────────

def build_twitch_bot() -> Optional[object]:
    if not TWITCH_AVAILABLE:
        return None
    if not all([TWITCH_TOKEN, TWITCH_BOT_NICK, TWITCH_CHANNELS]):
        log.warning("Twitch credentials incomplete — Twitch mirroring disabled.")
        return None

    class TwitchMirrorBot(twitch_commands.Bot):
        def __init__(self):
            super().__init__(
                token=TWITCH_TOKEN,
                client_id=TWITCH_CLIENT_ID or None,
                nick=TWITCH_BOT_NICK,
                prefix="!",
                initial_channels=TWITCH_CHANNELS,
            )

        async def event_ready(self):
            log.info("Twitch ready | nick=%s | channels=%s", self.nick, TWITCH_CHANNELS)

        async def event_message(self, msg):
            if msg.echo:
                return
            author = msg.author.name if msg.author else "unknown"
            enqueue(author, msg.content, "twitch")

        async def event_error(self, error, data=None):
            log.error("Twitch error: %s | data: %s", error, data)

    return TwitchMirrorBot()


# ─────────────────────────────────────────────────────────────────────────────
#  YOUTUBE — WebSub (PubSubHubbub)
#
#  How it works:
#   1. We spin up a tiny HTTP server on WEBSUB_PORT.
#   2. On startup we POST a subscription to Google's hub for the channel feed.
#   3. Google verifies us with a GET challenge — we echo it back.
#   4. When a live stream starts, Google POSTs an Atom entry containing the
#      video ID.  We extract it and start a chat session for that video.
#   5. We re-subscribe every 23 h (lease is 24 h).
#
#  WebSub fires on new video/live *events*, not per chat message.
#  Chat messages still use the YouTube Data API — but we get the chatId
#  immediately via push rather than discovering it through polling.
# ─────────────────────────────────────────────────────────────────────────────

_active_chat_ids: set[str] = set()


def _verify_websub_signature(body: bytes, header: str) -> bool:
    expected = hmac.new(
        WEBSUB_SECRET.encode(), body, hashlib.sha1
    ).hexdigest()
    return hmac.compare_digest(expected, header.removeprefix("sha1="))


async def websub_callback(request: web.Request) -> web.Response:
    """Handles both the hub challenge (GET) and feed notifications (POST)."""
    if request.method == "GET":
        challenge = request.rel_url.query.get("hub.challenge", "")
        mode      = request.rel_url.query.get("hub.mode", "")
        log.info("WebSub %s verified", mode)
        return web.Response(text=challenge)

    body = await request.read()
    sig  = request.headers.get("X-Hub-Signature", "")

    if WEBSUB_SECRET and not _verify_websub_signature(body, sig):
        log.warning("WebSub signature mismatch — ignoring")
        return web.Response(status=403)

    try:
        root = ET.fromstring(body)
        ns   = {
            "atom": "http://www.w3.org/2005/Atom",
            "yt":   "http://www.youtube.com/xml/schemas/2015",
        }
        for entry in root.findall("atom:entry", ns):
            vid_el = entry.find("yt:videoId", ns)
            if vid_el is not None:
                log.info("WebSub push: new video/live event vid=%s", vid_el.text)
                asyncio.create_task(youtube_chat_session(vid_el.text.strip()))
    except ET.ParseError as exc:
        log.error("WebSub Atom parse error: %s", exc)

    return web.Response(status=204)


async def websub_subscribe(session: aiohttp.ClientSession, channel_id: str) -> None:
    topic    = f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={channel_id}"
    callback = f"{WEBSUB_PUBLIC_URL}/websub"
    data = {
        "hub.mode":          "subscribe",
        "hub.topic":         topic,
        "hub.callback":      callback,
        "hub.lease_seconds": str(WEBSUB_LEASE_SECONDS),
        "hub.secret":        WEBSUB_SECRET,
    }
    try:
        async with session.post(WEBSUB_HUB, data=data) as resp:
            if resp.status in (202, 204):
                log.info("WebSub subscription accepted | channel=%s", channel_id)
            else:
                log.error("WebSub subscription failed %d: %s", resp.status, await resp.text())
    except Exception as exc:
        log.error("WebSub subscribe error: %s", exc)


async def websub_renewer(session: aiohttp.ClientSession, channel_id: str) -> None:
    while True:
        await asyncio.sleep(WEBSUB_LEASE_SECONDS - 3600)
        log.info("Renewing WebSub subscription...")
        await websub_subscribe(session, channel_id)


# ─────────────────────────────────────────────────────────────────────────────
#  YOUTUBE LIVE CHAT SESSION
#  Once we have a video ID (from WebSub push or direct config), this fetches
#  the liveChatId then streams messages using the API's pollingIntervalMillis.
# ─────────────────────────────────────────────────────────────────────────────

async def youtube_chat_session(video_id: str) -> None:
    if not YT_GAPI_AVAILABLE or not YT_API_KEY:
        log.error("YouTube API unavailable — install google-api-python-client and set YT_API_KEY")
        return

    loop = asyncio.get_event_loop()

    def _get_chat_id():
        yt    = yt_build("youtube", "v3", developerKey=YT_API_KEY)
        resp  = yt.videos().list(part="liveStreamingDetails", id=video_id).execute()
        items = resp.get("items", [])
        return items[0].get("liveStreamingDetails", {}).get("activeLiveChatId") if items else None

    live_chat_id = await loop.run_in_executor(None, _get_chat_id)
    if not live_chat_id:
        log.warning("No active live chat for video %s", video_id)
        return

    if live_chat_id in _active_chat_ids:
        log.info("Chat session already active for chatId=%s", live_chat_id)
        return

    _active_chat_ids.add(live_chat_id)
    log.info("YouTube chat session started | chatId=%s", live_chat_id)

    seen_set:   set[str]       = set()
    seen_ids:   deque[str]     = deque(maxlen=4000)   # bounded memory
    page_token: Optional[str]  = None

    def _fetch(token):
        yt     = yt_build("youtube", "v3", developerKey=YT_API_KEY)
        params = dict(liveChatId=live_chat_id, part="snippet,authorDetails", maxResults=200)
        if token:
            params["pageToken"] = token
        return yt.liveChatMessages().list(**params).execute()

    try:
        while True:
            try:
                data = await loop.run_in_executor(None, _fetch, page_token)
            except Exception as exc:
                log.error("YT chat fetch error: %s", exc)
                await asyncio.sleep(15)
                continue

            for item in data.get("items", []):
                msg_id = item["id"]
                if msg_id in seen_set:
                    continue
                seen_set.add(msg_id)
                seen_ids.append(msg_id)

                author = item.get("authorDetails", {}).get("displayName", "unknown")
                text   = item.get("snippet", {}).get("displayMessage", "")
                if text:
                    enqueue(author, text, "youtube")

            page_token = data.get("nextPageToken")
            interval   = data.get("pollingIntervalMillis", YT_POLL_INTERVAL * 1000)
            await asyncio.sleep(interval / 1000)

    finally:
        _active_chat_ids.discard(live_chat_id)
        log.info("YouTube chat session ended | chatId=%s", live_chat_id)


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    if not DISCORD_WEBHOOK_URL:
        log.error("DISCORD_WEBHOOK_URL is not set.")
        return

    async with aiohttp.ClientSession() as session:
        tasks: list[asyncio.Task] = []

        # Discord background worker (always running)
        tasks.append(asyncio.create_task(discord_worker(session)))

        # Twitch
        bot = build_twitch_bot()
        if bot:
            tasks.append(asyncio.create_task(bot.start()))

        # YouTube — WebSub or polling
        if WEBSUB_PUBLIC_URL and YT_CHANNEL_ID:
            app    = web.Application()
            app.router.add_route("GET",  "/websub", websub_callback)
            app.router.add_route("POST", "/websub", websub_callback)
            runner = web.AppRunner(app)
            await runner.setup()
            await web.TCPSite(runner, "0.0.0.0", WEBSUB_PORT).start()
            log.info("WebSub server on port %d | callback=%s/websub", WEBSUB_PORT, WEBSUB_PUBLIC_URL)

            await websub_subscribe(session, YT_CHANNEL_ID)
            tasks.append(asyncio.create_task(websub_renewer(session, YT_CHANNEL_ID)))

            # Immediately start a session for any currently-live video
            if YT_VIDEO_ID:
                tasks.append(asyncio.create_task(youtube_chat_session(YT_VIDEO_ID)))
        elif YT_VIDEO_ID:
            log.info("YouTube: WebSub not configured — using API polling fallback")
            tasks.append(asyncio.create_task(youtube_chat_session(YT_VIDEO_ID)))
        else:
            log.warning("YouTube: no YT_VIDEO_ID or WebSub config — YouTube disabled")

        if len(tasks) <= 1:
            log.error("No platforms configured. Check your .env file.")
            return

        log.info("Chat mirror v2 running. Ctrl+C to stop.")
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down.")
