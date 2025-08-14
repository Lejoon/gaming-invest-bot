import websockets
import bs4
import discord
from discord import TextChannel, DMChannel, Thread
from bs4 import BeautifulSoup
import asyncio
from datetime import datetime
from general_utils import log_message, error_message

from websockets.exceptions import ConnectionClosed, InvalidHandshake, WebSocketException
from websockets.client import connect

# --- Configuration Constants ---
# Discord Channel ID where messages should be sent
PRESS_RELEASES_CHANNEL = 1163373835886805013
# MFN Websocket URL with filters for English language and specific industry
WEBSOCKET_URL = 'wss://mfn.se/all/?filter=(and(or(.properties.lang="en"))(or(a.industry_id=36)))'
# Keep-alive settings for the websocket connection
PING_INTERVAL_SECONDS = 30  # Send a ping every 30 seconds to keep connection alive
PING_TIMEOUT_SECONDS = 10   # If no pong reply received within 10 seconds, assume connection is lost
# Reconnection backoff settings
MAX_RECONNECT_WAIT_SECONDS = 60 # Maximum time to wait between reconnection attempts for general errors
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
    async with connect(
        websocket_url,
        ping_interval=PING_INTERVAL_SECONDS,
        ping_timeout=PING_TIMEOUT_SECONDS
    ) as ws:
        log_message(f'Successfully connected/reconnected to websocket for MFN (Keep-alive enabled: interval={PING_INTERVAL_SECONDS}s, timeout={PING_TIMEOUT_SECONDS}s).')

        while True:
            try:
                raw_message = await ws.recv()
                message_text = raw_message.decode('utf-8', 'replace') if isinstance(raw_message, (bytes, bytearray)) else str(raw_message)
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
                        if href in seen_in_frame:  # Avoid duplicate sends within same frame only
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
                    # Treat as debug/info rather than error to avoid noise
                    head_snippet = message_text[:120].replace("\n", " ")
                    log_message(f"MFN frame: containers={len(candidate_containers)} parsed=0 sent=0 (head='{head_snippet}...')")
                    continue

                channel = bot.get_channel(PRESS_RELEASES_CHANNEL)
                if not channel:
                    await error_message(message=f"Could not find Discord channel with ID: {PRESS_RELEASES_CHANNEL}", bot=bot)
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
                            await error_message(message=f"Resolved channel object not messageable (type={type(channel)}).", bot=bot)
                            break
                    except discord.errors.Forbidden:
                        await error_message(message=f"Bot lacks permissions to send messages in channel {PRESS_RELEASES_CHANNEL}.", bot=bot)
                        break
                    except Exception as send_e:
                        await error_message(message=f"Failed to send MFN item to channel {PRESS_RELEASES_CHANNEL}: {send_e}", bot=bot)
                        # Continue

                # Metrics log per frame
                log_message(f"MFN frame: containers={len(candidate_containers)} parsed={len(parsed)} sent={sent_count}")

            except ConnectionClosed as e:
                log_message(f"Websocket connection closed inside fetch_mfn_updates: Code={e.code}, Reason='{e.reason}'. Raising exception.")
                raise e
            except Exception as e:
                await error_message(message=f"Error processing MFN websocket frame: {e}", bot=bot)
                raise e

# --- Background Task for Connection Management ---

async def websocket_background_task(bot: discord.Client): # Added type hint for bot
    """
    Manages the websocket connection lifecycle, including reconnections
    with exponential backoff for most errors, and handles specific
    close codes like 'fast reconnect' differently.
    """
    attempt_count = 0 # Counter for backoff calculation for general errors
    while True:
        wait_time = 0
        try:
            log_message(f"Attempting MFN websocket connection (try #{attempt_count + 1} for general errors)...")
            await fetch_mfn_updates(bot)
            log_message("WARNING: fetch_mfn_updates exited loop unexpectedly. Resetting connection.")
            attempt_count = 0
            wait_time = 1
        except ConnectionClosed as e:
            if e.code == 3000:
                log_message("Received 'fast reconnect' signal (Code=3000). Reconnecting shortly.")
                wait_time = FAST_RECONNECT_DELAY_SECONDS
            else:
                await error_message(message=f"Connection closed (Code={e.code}, Reason='{e.reason}'). Incrementing backoff counter. Attempt {attempt_count + 1}.", bot=bot)
                attempt_count += 1
                wait_time = min(2 ** attempt_count, MAX_RECONNECT_WAIT_SECONDS)
        except (InvalidHandshake, WebSocketException) as e:
            await error_message(message=f"Websocket connection error: {type(e).__name__}. Incrementing backoff counter. Attempt {attempt_count + 1}.", bot=bot)
            attempt_count += 1
            wait_time = min(2 ** attempt_count, MAX_RECONNECT_WAIT_SECONDS)
        except Exception as e:
            await error_message(message=f"Unhandled error in websocket task: {e}. Incrementing backoff counter. Attempt {attempt_count + 1}.", bot=bot)
            attempt_count += 1
            wait_time = min(2 ** attempt_count, MAX_RECONNECT_WAIT_SECONDS)

        if wait_time > LOG_DELAY_THRESHOLD_SECONDS:
            log_message(f"Delaying MFN websocket reconnect attempt by {wait_time} seconds (backoff active)...")
        await asyncio.sleep(wait_time)
