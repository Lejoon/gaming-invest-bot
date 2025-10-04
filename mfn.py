import websockets
import bs4
import discord
from discord import TextChannel, DMChannel, Thread
from bs4 import BeautifulSoup
import asyncio
import random
import contextlib
from datetime import datetime, timedelta
from general_utils import log_message, error_message

from websockets.exceptions import ConnectionClosed, InvalidHandshake, WebSocketException
from websockets.client import connect

# --- Configuration Constants ---
# Discord Channel ID where messages should be sent
PRESS_RELEASES_CHANNEL = 1163373835886805013
# MFN Websocket URL with filters for English language and specific industry
WEBSOCKET_URL = 'wss://mfn.se/all/?filter=(and(or(.properties.lang="en"))(or(a.industry_id=36)))'
# Keep-alive settings for the websocket connection
PING_INTERVAL_SECONDS = 30  # Send a ping every 30 seconds to keep connection alive (handled by lib)
PING_TIMEOUT_SECONDS = 10   # If no pong reply received within 10 seconds, assume connection is lost
INACTIVITY_FRAME_TIMEOUT_SECONDS = 120  # If no frame (any message) seen within this, force a reconnect (watchdog)
MANUAL_PING_GRACE_SECONDS = 15  # After watchdog triggers, allow this grace after a manual ping before forcing close
# Reconnection backoff settings
MAX_RECONNECT_WAIT_SECONDS = 60 # Maximum time to wait between reconnection attempts for general errors
BASE_BACKOFF_SECONDS = 1.5      # Base backoff multiplier
# Log delay message only if wait time is longer than this (to reduce noise for quick retries)
LOG_DELAY_THRESHOLD_SECONDS = 4
# Specific delay for the server-indicated "fast reconnect"
FAST_RECONNECT_DELAY_SECONDS = 1

# --- Core Websocket Function ---

def _parse_item(container, soup):
    """Parse a potential item container; return dict or None."""
    try:
        item_link = container.find("a", class_=lambda c: c and "title-link" in c.split() and "item-link" in c.split())
        if not item_link:
            return None
        author_link = container.find("a", class_=lambda c: c and "title-link" in c.split() and "author-link" in c.split())
        date_span = container.find("span", class_="compressed-date") or soup.find("span", class_="compressed-date")
        time_span = container.find("span", class_="compressed-time") or soup.find("span", class_="compressed-time")
        if not (date_span and time_span):
            return None
        date_str = date_span.get_text(strip=True)
        raw_time = time_span.get_text(strip=True)
        time_parts = raw_time.split(":")
        time_str = ":".join(time_parts[:2]) if len(time_parts) >= 2 else raw_time
        item_href = item_link.get('href')
        if not item_href:
            return None
        author_name = author_link.get_text(strip=True) if author_link else "MFN"
        item_title = item_link.get_text(strip=True)
        try:
            timestamp = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            timestamp = datetime.now()
        return {"author": author_name, "title": item_title, "href": item_href, "timestamp": timestamp}
    except Exception:
        return None


async def fetch_mfn_updates(bot: discord.Client):
    """
    Connects to the MFN websocket, listens for messages, parses them,
    and sends updates to a Discord channel. Handles keep-alive automatically.
    Raises exceptions on connection errors or processing failures.
    """
    websocket_url = WEBSOCKET_URL
    # Connect with automatic ping/pong handling for keep-alive
    # Add common browser-ish headers; some websocket servers prune non-browser clients after a while
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0 Safari/537.36",
        "Origin": "https://mfn.se",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    connect_started_at = datetime.utcnow()
    async with connect(
        websocket_url,
        ping_interval=PING_INTERVAL_SECONDS,
        ping_timeout=PING_TIMEOUT_SECONDS,
        max_size=None,              # Allow arbitrarily large frames; prevent unexpected 1006 if message bigger than default
        extra_headers=headers,
        close_timeout=5,
    ) as ws:
        conn_id = f"mfn-{int(connect_started_at.timestamp())}-{random.randint(1000,9999)}"
        log_message(
            f"[MFN][{conn_id}] Connected (interval={PING_INTERVAL_SECONDS}s timeout={PING_TIMEOUT_SECONDS}s inactivity={INACTIVITY_FRAME_TIMEOUT_SECONDS}s)."
        )

        last_frame_at = datetime.utcnow()
        manual_ping_out_at = None
        frame_counter = 0
        while True:
            try:
                # Use a timeout around recv to implement inactivity watchdog
                timeout_for_frame = INACTIVITY_FRAME_TIMEOUT_SECONDS
                raw_message = await asyncio.wait_for(ws.recv(), timeout=timeout_for_frame)
                now = datetime.utcnow()
                last_frame_at = now
                manual_ping_out_at = None  # Reset manual ping tracking once we get any frame
                frame_counter += 1
                message_text = (
                    raw_message.decode('utf-8', 'replace') if isinstance(raw_message, (bytes, bytearray)) else str(raw_message)
                )
                soup = BeautifulSoup(message_text, 'html.parser')

                candidate_containers = []
                for tag_name in ("li", "article", "div"):
                    candidate_containers.extend(soup.find_all(tag_name))

                parsed = []
                seen_in_frame = set()
                for c in candidate_containers:
                    item = _parse_item(c, soup)
                    if item:
                        href = item["href"]
                        if href in seen_in_frame:
                            continue
                        seen_in_frame.add(href)
                        parsed.append(item)

                if not parsed:
                    fallback = _parse_item(soup, soup)
                    if fallback:
                        href = fallback["href"]
                        if href not in seen_in_frame:
                            parsed.append(fallback)
                            seen_in_frame.add(href)

                if not parsed:
                    head_snippet = message_text[:100].replace("\n", " ")
                    if frame_counter % 50 == 0:  # avoid spamming logs
                        log_message(
                            f"[MFN][{conn_id}] Frame#{frame_counter}: no parsable items (containers={len(candidate_containers)}) head='{head_snippet}...'"
                        )
                    continue

                channel = bot.get_channel(PRESS_RELEASES_CHANNEL)
                if not channel:
                    await error_message(message=f"[MFN][{conn_id}] Discord channel ID {PRESS_RELEASES_CHANNEL} not found", bot=bot)
                    continue

                sent_count = 0
                for item in parsed:
                    try:
                        embed = discord.Embed(
                            title=item["author"],
                            url=f"http://www.mfn.se{item['href']}",
                            description=item["title"],
                            color=0x00ff00,
                            timestamp=item["timestamp"],
                        )
                        if isinstance(channel, (TextChannel, DMChannel, Thread)):
                            await channel.send(embed=embed)
                            sent_count += 1
                        else:
                            await error_message(
                                message=f"[MFN][{conn_id}] Channel object not messageable (type={type(channel)})", bot=bot
                            )
                            break
                    except discord.errors.Forbidden:
                        await error_message(
                            message=f"[MFN][{conn_id}] Missing permissions for channel {PRESS_RELEASES_CHANNEL}", bot=bot
                        )
                        break
                    except Exception as send_e:
                        await error_message(
                            message=f"[MFN][{conn_id}] Failed to send item: {send_e}", bot=bot
                        )

                log_message(
                    f"[MFN][{conn_id}] Frame#{frame_counter}: containers={len(candidate_containers)} parsed={len(parsed)} sent={sent_count}"
                )

            except asyncio.TimeoutError:
                # No frame within inactivity window -> send a manual ping then enforce grace
                now = datetime.utcnow()
                since_last = (now - last_frame_at).total_seconds()
                if manual_ping_out_at is None:
                    # Send manual ping
                    with contextlib.suppress(Exception):
                        await ws.ping()
                    manual_ping_out_at = now
                    log_message(
                        f"[MFN][{conn_id}] Inactivity: {since_last:.1f}s without frames. Sent manual ping; waiting for up to {MANUAL_PING_GRACE_SECONDS}s."
                    )
                    continue
                else:
                    grace_elapsed = (now - manual_ping_out_at).total_seconds()
                    if grace_elapsed < MANUAL_PING_GRACE_SECONDS:
                        # Continue waiting within grace period
                        continue
                    log_message(
                        f"[MFN][{conn_id}] Inactivity persists (>{since_last:.1f}s, grace {grace_elapsed:.1f}s). Forcing reconnect by closing."
                    )
                    # Force-close to bubble up and reconnect
                    await ws.close(code=4000, reason="inactivity watchdog")
                    # Raise a generic WebSocketException subclass instance for outer logic.
                    class InactivityWatchdog(Exception):
                        pass
                    raise InactivityWatchdog("inactivity watchdog forced reconnect")

            except ConnectionClosed as e:
                log_message(
                    f"[MFN][{conn_id}] Websocket closed: Code={e.code} Reason='{e.reason}' after {(datetime.utcnow()-connect_started_at).total_seconds():.1f}s. Raising."
                )
                raise e
            except Exception as e:
                await error_message(
                    message=f"[MFN][{conn_id}] Error processing frame: {type(e).__name__}: {e}", bot=bot
                )
                raise e

# --- Background Task for Connection Management ---

async def websocket_background_task(bot: discord.Client): # Added type hint for bot
    """
    Manages the websocket connection lifecycle, including reconnections
    with exponential backoff for most errors, and handles specific
    close codes like 'fast reconnect' differently.
    """
    attempt_count = 0 # Counter for backoff calculation for general errors
    consecutive_1006 = 0  # Track consecutive abnormal closures to differentiate behavior
    while True:
        wait_time = 0
        try:
            log_message(f"Attempting MFN websocket connection (try #{attempt_count + 1} for general errors)...")
            await fetch_mfn_updates(bot)
            log_message("WARNING: fetch_mfn_updates exited loop unexpectedly. Resetting connection.")
            attempt_count = 0
            consecutive_1006 = 0
            wait_time = 1
        except ConnectionClosed as e:
            if e.code == 3000:
                log_message("Received 'fast reconnect' signal (Code=3000). Reconnecting shortly.")
                wait_time = FAST_RECONNECT_DELAY_SECONDS
                consecutive_1006 = 0
            else:
                if e.code == 1006:
                    consecutive_1006 += 1
                else:
                    consecutive_1006 = 0
                await error_message(
                    message=(
                        f"Connection closed (Code={e.code}, Reason='{e.reason}'). 1006_seq={consecutive_1006} Attempt {attempt_count + 1}."
                    ),
                    bot=bot,
                )
                attempt_count += 1
                # Use BASE_BACKOFF_SECONDS and add jitter; if many 1006 in a row, cap early to avoid long downtime
                exponent = min(attempt_count, 10)
                backoff_core = BASE_BACKOFF_SECONDS * (2 ** (exponent - 1))
                if e.code == 1006 and consecutive_1006 <= 3:
                    backoff_core = min(backoff_core, 8)  # keep relatively quick tries for transient network resets
                jitter = random.uniform(0, 0.4 * backoff_core)
                wait_time = min(backoff_core + jitter, MAX_RECONNECT_WAIT_SECONDS)
        except Exception as e:
            # Intercept our inactivity watchdog custom exception by message text
            if str(e).startswith("inactivity watchdog"):
                log_message("Watchdog-triggered reconnect (pseudo code=4000). Soft restart.")
                attempt_count = 0
                consecutive_1006 = 0
                wait_time = 2
            else:
                await error_message(message=f"Unhandled error in websocket task: {e}. Incrementing backoff counter. Attempt {attempt_count + 1}.", bot=bot)
                attempt_count += 1
                exponent = min(attempt_count, 10)
                backoff_core = BASE_BACKOFF_SECONDS * (2 ** (exponent - 1))
                jitter = random.uniform(0, 0.4 * backoff_core)
                wait_time = min(backoff_core + jitter, MAX_RECONNECT_WAIT_SECONDS)
        # Note: (InvalidHandshake, WebSocketException) are already subclasses of Exception and handled above.

        if wait_time > LOG_DELAY_THRESHOLD_SECONDS:
            log_message(f"Delaying MFN websocket reconnect attempt by {wait_time} seconds (backoff active)...")
        await asyncio.sleep(wait_time)
