import asyncio
import aiohttp
import pickle
import re
from bs4 import BeautifulSoup
from collections import deque
import discord
from datetime import datetime, timezone
import random

# Assuming general_utils provides these functions as in the original script
from general_utils import log_message, error_message

TELEGRAM_CHANNEL = 1167391973825593424
PRESS_RELEASES_CHANNEL = 1163373835886805013

icon_dict = {
    'Finwire': 'https://finwire.com/wp-content/uploads/2021/03/1.5-FINWIRE-Logotype-Bird-Icon-2020-PMS021-300x300.png',
    'Nyhetsbyrån Direkt': 'https://media.licdn.com/dms/image/v2/D4D0BAQGQgNOpVKGypg/company-logo_200_200/company-logo_200_200/0/1732630386396/nyhetsbyr_n_direkt_logo?e=1758153600&v=beta&t=_anf5gLW_Q0ACK_G49tqRM1SUMiXke-hik5H-uyzrf8',
    'MFN': None,  # MFN handled through another proxy
}

def get_source_icon(src_text):
    """Strip 'Källa:' and lookup icon."""
    m = re.match(r'Källa:\s*(.+)', src_text)
    if not m:
        return None, None
    src = m.group(1)
    return src, icon_dict.get(src)

# --- Configuration ---
max_queue_size = 1000
seen_file = 'seen_articles.pkl'
companies_to_track = [
    'Embracer', 'Paradox', 'Ubisoft', 'Starbreeze',
    'EG7', 'Flexion', 'Enad Global 7', 'Take Two',
    'Capcom', 'Maximum Entertainment', 'MAG Interactive', 'MAGI', 'G5 Entertainment', # (comma fixed before 'G5')
    'G5', 'Remedy', 'MTG', 'Modern Times Group',
    'Rovio', 'Thunderful', 'MGI', 'Electronic Arts',
    'Take-Two', 'Stillfront', 'Asmodee', 'ASMODEE'
]
TAB_HREF_PATTERNS = {
    'telegram': re.compile(r'^/telegram/'),
    'pressmeddelande': re.compile(r'^/pressmeddelanden/'),
    'extern-analys': re.compile(r'^/externa-analyser/')
}
REQUEST_TIMEOUT = 15  # seconds
MAX_FETCH_RETRIES = 3
RETRY_DELAY_BASE = 5  # seconds
INTER_TAB_DELAY = 2 # seconds
# Make requests look like a common browser
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
}
# --- End Configuration ---

def load_seen():
    try:
        loaded = pickle.load(open(seen_file,'rb'))
        # enforce maxlen so old entries get pushed out when new ones arrive
        return deque(loaded, maxlen=max_queue_size)
    except FileNotFoundError:
        return deque(maxlen=max_queue_size)
    except Exception:
        # Silently start fresh on load error, as per user request (no new logs)
        return deque(maxlen=max_queue_size)


def save_seen(q):
    try:
        with open(seen_file, 'wb') as f:
            pickle.dump(q, f)
    except Exception:
        # Silently ignore save errors, as per user request (no new logs)
        pass # Or use error_message if critical, but avoiding new logs

seen_articles = load_seen()

async def send_to_discord(title, raw_date, url, company, source, icon_url, bot, tab=None):
    # Skip MFN sources as they're handled through another proxy
    if source == 'MFN':
        log_message(f'Skipping MFN source: "{title}" ({raw_date})')
        return

    # Determine channel based on tab
    if tab == 'pressmeddelande':
        channel_id = PRESS_RELEASES_CHANNEL
    else:
        channel_id = TELEGRAM_CHANNEL

    if bot is None:
        # Handle the case where bot is None (e.g., during local testing)
        print(f"[INFO] Local test: Would send Discord message:")
        print(f"  Title: {company or 'Placera'}")
        print(f"  Description: {title}")
        print(f"  URL: {url}")
        print(f"  Channel: {channel_id}")
        print(f"  Timestamp: {datetime.now(timezone.utc)}")
        if source:
            print(f"  Footer: {source} (Icon: {icon_url or 'N/A'})")
        # log_message is assumed to handle console/file logging independently of a live bot
        log_message(f'(Local Test) Sent "{title}" ({raw_date}) to Discord.')
        return

    chan = bot.get_channel(channel_id)
    if not chan:
        # Using original error_message for critical failure
        await error_message(f"Could not find Discord channel {channel_id}", bot)
        return

    embed = discord.Embed(
        title=company or 'Placera',
        description=title,
        url=url,
        timestamp=datetime.now(timezone.utc) # Use timezone-aware datetime
    )
    if source and icon_url:
        embed.set_footer(text=source, icon_url=icon_url)

    try:
        await chan.send(embed=embed)
        # Keep original log message for successful send
        log_message(f'Sent "{title}" ({raw_date}) to Discord channel {channel_id}.')
    except discord.errors.Forbidden:
        # Using original error_message for critical failure
        await error_message(f"Missing permissions to send message in channel {channel_id}", bot)
    except Exception as e:
        # Using original error_message for critical failure
        await error_message(f"Failed to send message to Discord: {e}", bot)


async def fetch(session, url, bot, retries=MAX_FETCH_RETRIES, delay_base=RETRY_DELAY_BASE, timeout=REQUEST_TIMEOUT, headers=DEFAULT_HEADERS):
    """Fetches URL with retries, timeout, headers, and exponential backoff with jitter."""
    for attempt in range(retries + 1):
        try:
            async with session.get(url, timeout=timeout, headers=headers) as resp:
                resp.raise_for_status()  # Raises ClientResponseError for 4xx/5xx
                return await resp.text()
        except (aiohttp.ClientResponseError, aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
            # Retry logic for specific errors
            if attempt < retries:
                # Exponential backoff with jitter
                delay = (delay_base * (2 ** attempt)) + random.uniform(0, 1)
                await asyncio.sleep(delay)
            else:
                # Log final failure after all retries using original error_message
                await error_message(f'Fetch error for {url} after {retries + 1} attempts: {e}', bot)
                return None # Failed after retries
        except Exception as e:
            # Catch any other unexpected errors during fetch using original error_message
            await error_message(f'Unexpected fetch error for {url}: {e}', bot)
            return None # Don't retry unknown errors

    return None # Should not be reached if retries are configured > 0, but acts as a safeguards


async def check_placera(bot, verbose=False):
    tabs = ['telegram','pressmeddelande','extern-analys']
    base = 'https://www.placera.se/telegram?tab={}&limit=200'
    loop_delay, max_loop_delay = 60, 600
    current_loop_delay = loop_delay

    async with aiohttp.ClientSession() as session:
        while True:
            log_message("[Placera] Starting new Placera scan cycle.") # Removed await
            if not bot:
                print("[INFO] Starting new Placera scan cycle (local test).")
            success_occurred = False
            try:
                for tab in tabs:
                    current_href_pattern = TAB_HREF_PATTERNS.get(tab)
                    if not current_href_pattern:
                        if verbose: print(f"[verbose] No href pattern defined for tab '{tab}'. Skipping article search for this tab.")
                        await asyncio.sleep(INTER_TAB_DELAY) # Still respect inter-tab delay
                        continue

                    fetch_url = base.format(tab)
                    if verbose:
                        print(f"[verbose] Fetching tab='{tab}' → {fetch_url} (expecting links matching: {current_href_pattern.pattern})")
                    html = await fetch(session, fetch_url, bot)

                    if not html:
                        continue  # Skip tab on fetch failure

                    soup = BeautifulSoup(html, 'html.parser')
                    
                    list_items = [] # This will store the final <a> tags to be processed

                    # Directly perform global search for <a> tags matching the pattern
                    if verbose: print(f"[verbose] Performing global search for <a> tags with pattern '{current_href_pattern.pattern}' for tab '{tab}'.")
                    candidate_a_tags = soup.find_all('a', href=current_href_pattern)
                    if verbose: print(f"[verbose] Global search found {len(candidate_a_tags)} candidate <a> tags for tab '{tab}'.")
                    
                    # Relaxed filtering: require (title h5, date p, source p); company span optional.
                    # This allows articles without an explicit company span but whose title contains a tracked company to be considered hits.
                    for item_a in candidate_a_tags:
                        title_tag = item_a.find('h5')
                        date_p = item_a.find('p', class_=lambda c: c and 'text-sm' in c.split())
                        source_p = None
                        for p in item_a.find_all('p'):
                            if p.get_text(strip=True).startswith('Källa:'):
                                source_p = p
                        if title_tag and date_p and source_p:
                            list_items.append(item_a)
                        elif verbose:
                            missing = []
                            if not title_tag: missing.append('title')
                            if not date_p: missing.append('date')
                            if not source_p: missing.append('source')
                            print(f"[verbose] Skip href={item_a.get('href')} missing={','.join(missing)}")
                    if verbose: print(f"[verbose] After relaxed filtering for tab '{tab}', {len(list_items)} items remain.")

                    if not list_items:
                        if verbose: print(f"[verbose] No articles found for tab {tab} after global search and filtering.")
                        await asyncio.sleep(INTER_TAB_DELAY) # Wait before next tab
                        continue # Try next tab

                    # Now, list_items contains only <a> tags matching the current_href_pattern and (if global) filters
                    for a_tag in list_items:  # Only anchors that passed strict filter
                        href = a_tag.get('href')
                        if not href:
                            continue
                        full_url = 'https://www.placera.se' + href

                        # Re-acquire elements
                        company_span = a_tag.find('span', class_=lambda c: c and 'text-brand' in c.split())
                        title_tag = a_tag.find('h5')
                        date_p = a_tag.find('p', class_=lambda c: c and 'text-sm' in c.split())
                        source_p = None
                        for p in a_tag.find_all('p'):
                            if p.get_text(strip=True).startswith('Källa:'):
                                source_p = p
                        # Defensive: ensure core elements exist (title/date/source); company span optional
                        if not (title_tag and date_p and source_p):
                            if verbose:
                                print(f"[verbose] Unexpected missing core element for {full_url}")
                            continue

                        title = title_tag.get_text(strip=True)
                        raw_date = date_p.get_text(strip=True)
                        source, icon_url = get_source_icon(source_p.get_text(strip=True))

                        # Helper: find first tracked company within given text (case-insensitive)
                        def find_tracked_company(text: str):
                            lower_text = text.lower()
                            for tc in companies_to_track:
                                if tc.lower() in lower_text:
                                    return tc
                            return None

                        matched_company = None
                        span_company_text = company_span.get_text(strip=True) if company_span else None
                        if span_company_text:
                            matched_company = find_tracked_company(span_company_text)
                        # If no company span match, attempt in title
                        if not matched_company:
                            matched_company = find_tracked_company(title)

                        if verbose:
                            print(f"[verbose] Article parsed: span_company={span_company_text!r} matched_company={matched_company!r} raw_date={raw_date!r} source={source!r} url={full_url}")

                        key = full_url
                        if key in seen_articles:
                            continue
                        if not title or not raw_date:
                            continue

                        # If we matched only in title (company span missing or not matching), we send with embed title set to 'Placera'
                        if matched_company:
                            embed_company_title = span_company_text if span_company_text and matched_company and matched_company.lower() in span_company_text.lower() else None
                            await send_to_discord(title, raw_date, full_url, embed_company_title, source, icon_url, bot, tab)
                            success_occurred = True

                        seen_articles.append(key)
                        if len(seen_articles) % 10 == 0:
                            save_seen(seen_articles)

                    # Add delay between tabs
                    await asyncio.sleep(INTER_TAB_DELAY)

                # Adjust main loop delay based on success
                if success_occurred:
                    current_loop_delay = loop_delay
                else:
                    # Increase delay if no tabs were successfully processed
                    current_loop_delay = min(current_loop_delay * 2, max_loop_delay)

                # Save seen articles at the end of a cycle where at least one tab worked
                if success_occurred:
                    save_seen(seen_articles)

            except Exception as e:
                # Use original error_message for main loop errors
                try:
                    await error_message(f'Main loop parser/processing error: {e}', bot)
                except Exception as em_exception:
                    # If error_message itself fails, log to console to avoid breaking the loop
                    # and to ensure the original error is also visible.
                    timestamp = datetime.now(timezone.utc).isoformat()
                    print(f"[{timestamp}] [Placera] CRITICAL: The error_message utility failed: {em_exception}")
                    print(f"[{timestamp}] [Placera] Original error that triggered error_message: {e}")
                current_loop_delay = min(current_loop_delay * 2, max_loop_delay)
            finally:
                # Wait before next full cycle
                await asyncio.sleep(current_loop_delay)

async def placera_updates(bot):
    # No starting log message as per request
    await check_placera(bot)

if __name__ == "__main__":
    import asyncio
    # Run in verbose mode without a real Discord bot for local testing
    asyncio.run(check_placera(bot=None, verbose=True))