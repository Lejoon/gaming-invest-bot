import websockets
import bs4
import discord
from bs4 import BeautifulSoup
import asyncio
from datetime import datetime
# Assuming these utility functions exist for logging
from general_utils import log_message, error_message

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

async def fetch_mfn_updates(bot):
    """
    Connects to the MFN websocket, listens for messages, parses them,
    and sends updates to a Discord channel. Handles keep-alive automatically.
    Raises exceptions on connection errors or processing failures.
    """
    websocket_url = WEBSOCKET_URL
    # Connect with automatic ping/pong handling for keep-alive
    async with websockets.connect(
        websocket_url,
        ping_interval=PING_INTERVAL_SECONDS,
        ping_timeout=PING_TIMEOUT_SECONDS
    ) as ws:
        # Log successful connection/reconnection
        log_message(f'Successfully connected/reconnected to websocket for MFN (Keep-alive enabled: interval={PING_INTERVAL_SECONDS}s, timeout={PING_TIMEOUT_SECONDS}s).')

        # Main loop to receive messages
        while True:
            try:
                # Wait for a message from the websocket.
                # websockets library handles ping/pong in the background.
                # If a ping timeout occurs, this will raise ConnectionClosedError.
                message = await ws.recv()

                # --- Parse the HTML message ---
                soup = BeautifulSoup(message, 'html.parser')

                # Extract data - Added checks in case elements are missing
                date_span = soup.find("span", class_="compressed-date")
                time_span = soup.find("span", class_="compressed-time")
                author_link = soup.find("a", class_="title-link author-link author-preview")
                item_link = soup.find("a", class_="title-link item-link")

                if not all([date_span, time_span, author_link, item_link]):
                    error_message(f"Could not parse all required elements from MFN message: {message[:200]}...") # Log snippet
                    continue # Skip this message if essential parts are missing

                date_str = date_span.text
                time_str = time_span.text[:-3] # Remove milliseconds
                author_name = author_link.text
                # author_url = author_link.get('href') # .get() is safer than ['href']
                item_title = item_link.text
                item_href = item_link.get('href')

                if not item_href:
                     error_message(f"Missing href in item link for message: {message[:200]}...")
                     continue # Skip if the link URL is missing

                title_url = f"http://www.mfn.se{item_href}" # Construct full URL

                # Removed the per-message log to reduce spam:
                # log_message(f'Fetched news {item_title} from MFN')

                # --- Prepare and Send Discord Embed ---
                try:
                    timestamp = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
                except ValueError as time_e:
                    error_message(f"Error parsing date/time '{date_str} {time_str}': {time_e}")
                    timestamp = datetime.now() # Fallback to current time

                embed = discord.Embed(
                    title=author_name,
                    url=title_url,
                    description=item_title,
                    color=0x00ff00, # Green color
                    timestamp=timestamp
                )

                channel = bot.get_channel(PRESS_RELEASES_CHANNEL)
                if channel:
                    await channel.send(embed=embed)
                else:
                    # Log if the channel isn't found (might happen during startup or if ID is wrong)
                    error_message(f"Could not find Discord channel with ID: {PRESS_RELEASES_CHANNEL}")
                # --- End Discord Send ---

            except websockets.exceptions.ConnectionClosed as e:
                # Log specific closure reason if available (includes ping timeouts)
                # This log message is now more informative before raising
                log_message(f"Websocket connection closed inside fetch_mfn_updates: Code={e.code}, Reason='{e.reason}'. Raising exception.")
                raise e # Re-raise to signal the outer loop to reconnect, passing the exception object

            except Exception as e:
                # Catch unexpected errors during message processing (parsing, Discord API issues, etc.)
                error_message(f"Error processing MFN message or sending to Discord: {e}")
                # Depending on the error, you might want to add more specific handling.
                # For robustness, re-raising to trigger a reconnect is often the safest default.
                raise e # Re-raise to trigger reconnect by the outer loop, passing the exception object

# --- Background Task for Connection Management ---

async def websocket_background_task(bot):
    """
    Manages the websocket connection lifecycle, including reconnections
    with exponential backoff for most errors, and handles specific
    close codes like 'fast reconnect' differently.
    """
    attempt_count = 0 # Counter for backoff calculation for general errors
    while True:
        wait_time = 0 # Initialize wait time for this loop iteration
        try:
            # Log connection attempt
            log_message(f"Attempting MFN websocket connection (try #{attempt_count + 1} for general errors)...")
            # Start the main fetch loop. This runs until an exception occurs.
            await fetch_mfn_updates(bot)

            # If fetch_mfn_updates exits without exception (which it shouldn't due to inner while True),
            # it indicates an unexpected state. Log it and reset attempts.
            log_message("WARNING: fetch_mfn_updates exited loop unexpectedly. Resetting connection.")
            attempt_count = 0 # Reset attempts as it might have been a temporary issue resolved?
            wait_time = 1 # Wait a short time before trying again even in this weird case

        except websockets.exceptions.ConnectionClosed as e:
            # Check for the specific "fast reconnect" code
            if e.code == 3000:
                log_message(f"Received 'fast reconnect' signal (Code=3000). Reconnecting shortly.")
                # Don't increment attempt_count for fast reconnects
                wait_time = FAST_RECONNECT_DELAY_SECONDS # Use the short, fixed delay
            else:
                # Handle all other connection closures with backoff
                error_message(f"Connection closed (Code={e.code}, Reason='{e.reason}'). Incrementing backoff counter. Attempt {attempt_count + 1}.")
                attempt_count += 1
                # Calculate wait time using exponential backoff
                wait_time = min(2 ** attempt_count, MAX_RECONNECT_WAIT_SECONDS)

        except (websockets.exceptions.InvalidHandshake, websockets.exceptions.WebSocketException) as e:
            # Handle specific websocket connection errors with backoff
            error_message(f"Websocket connection error: {type(e).__name__}. Incrementing backoff counter. Attempt {attempt_count + 1}.")
            attempt_count += 1
            wait_time = min(2 ** attempt_count, MAX_RECONNECT_WAIT_SECONDS) # Calculate wait time

        except Exception as e:
            # Catch any other unexpected errors from connect() or fetch_mfn_updates() with backoff
            error_message(f"Unhandled error in websocket task: {e}. Incrementing backoff counter. Attempt {attempt_count + 1}.")
            attempt_count += 1
            wait_time = min(2 ** attempt_count, MAX_RECONNECT_WAIT_SECONDS) # Calculate wait time

        # --- Reconnection Delay ---
        # Log the delay only if it's longer than the threshold (and not the fast reconnect)
        if wait_time > LOG_DELAY_THRESHOLD_SECONDS:
             log_message(f"Delaying MFN websocket reconnect attempt by {wait_time} seconds (backoff active)...")
        elif wait_time == FAST_RECONNECT_DELAY_SECONDS:
             # Optional: Log the fast reconnect delay if you want confirmation
             # log_message(f"Waiting {wait_time}s before fast reconnect...")
             pass # Default: Don't log the very short fast reconnect delay
        else:
             # Optional: Log other short delays if needed
             pass # Default: Don't log other very short delays

        # Wait before the next connection attempt
        await asyncio.sleep(wait_time)
