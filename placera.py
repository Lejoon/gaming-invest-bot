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

icon_dict = {
    'Finwire': 'https://finwire.com/wp-content/uploads/2021/03/1.5-FINWIRE-Logotype-Bird-Icon-2020-PMS021-300x300.png',
    'Nyhetsbyrån Direkt': 'https://media.licdn.com/dms/image/C560BAQFerUMPTdDrAA/company-logo_200_200/0/1569249859285/nyhetsbyr_n_direkt_logo?e=1706745600&v=beta&t=YUjFmqgCdSjIebxklnaYep7RfaKL9vLhfJdJNBA594Q',
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
    'Capcom', 'Maximum Entertainment', 'MAG Interactive',
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

async def send_to_discord(title, raw_date, url, company, source, icon_url, bot):
    if bot is None:
        # Handle the case where bot is None (e.g., during local testing)
        print(f"[INFO] Local test: Would send Discord message:")
        print(f"  Title: {company or 'Placera'}")
        print(f"  Description: {title}")
        print(f"  URL: {url}")
        print(f"  Timestamp: {datetime.now(timezone.utc)}")
        if source:
            print(f"  Footer: {source} (Icon: {icon_url or 'N/A'})")
        # log_message is assumed to handle console/file logging independently of a live bot
        log_message(f'(Local Test) Sent "{title}" ({raw_date}) to Discord.')
        return

    chan = bot.get_channel(TELEGRAM_CHANNEL)
    if not chan:
        # Using original error_message for critical failure
        await error_message(f"Could not find Discord channel {TELEGRAM_CHANNEL}", bot)
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
        log_message(f'Sent "{title}" ({raw_date}) to Discord.')
    except discord.errors.Forbidden:
        # Using original error_message for critical failure
        await error_message(f"Missing permissions to send message in channel {TELEGRAM_CHANNEL}", bot)
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
    base = 'https://www.placera.se/telegram?tab={}'
    loop_delay, max_loop_delay = 60, 600
    current_loop_delay = loop_delay

    async with aiohttp.ClientSession() as session:
        while True:
            await log_message("[Placera] Starting new Placera scan cycle.")
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
                    
                    # Filter these candidates to ensure they are likely articles
                    for item_a in candidate_a_tags:
                        company_span_present = item_a.find('span', class_=re.compile(r'text-\[#')) 
                        title_h5_present = item_a.find('h5')
                        
                        if company_span_present and title_h5_present:
                            list_items.append(item_a)
                        elif verbose:
                            # Log why an item was filtered out if it's useful for debugging
                            # print(f"[verbose] Filtered out candidate item for tab '{tab}': company_span={bool(company_span_present)}, title_h5={bool(title_h5_present)}, href={item_a.get('href')}")
                            pass
                    
                    if verbose: print(f"[verbose] After filtering global candidates for tab '{tab}', {len(list_items)} items remain.")

                    if not list_items:
                        if verbose: print(f"[verbose] No articles found for tab {tab} after global search and filtering.")
                        await asyncio.sleep(INTER_TAB_DELAY) # Wait before next tab
                        continue # Try next tab

                    # Now, list_items contains only <a> tags matching the current_href_pattern and (if global) filters
                    for a_tag in list_items: # Iterate directly over the <a> tags
                        href = a_tag.get('href')
                        # This check should ideally not be needed if current_href_pattern ensures href exists,
                        # but it's a safe guard.
                        if not href: 
                            if verbose: print(f"[verbose] Skipping a_tag with no href: {a_tag.prettify()}")
                            continue

                        full_url = 'https://www.placera.se' + href

                        # Extract info - Selectors might need adjustment
                        company_span = a_tag.select_one('span.button__label.truncate') or \
                                       a_tag.select_one('span.text-brand, span.font-bold') or \
                                       a_tag.find('span', class_=re.compile(r'text-\[#')) # Matches example
                        company = company_span.text.strip() if company_span else None

                        title_tag = a_tag.select_one('h3.heading--small') or \
                                    a_tag.find('h5') or \
                                    a_tag.select_one('p.news_feed__paragraph') # h5 matches example
                        title = title_tag.text.strip() if title_tag else ''

                        # Date tag refinement
                        date_tag = a_tag.select_one('time.feed__timestamp') # Original selector for <time>
                        if not date_tag:
                            # Try specific P tag for date based on example structure's classes
                            # Example: <p class="font-sans text-sm text-text-main ...">
                            date_tag = a_tag.select_one('p.text-sm.text-text-main')
                        if not date_tag:
                             # Fallback to the original broader search if the specific one fails
                             date_tag = a_tag.find('p', string=re.compile(r'.+')) # General <p> tag
                        
                        raw_date = date_tag.text.strip() if date_tag else ''
                        if date_tag and date_tag.name == 'time' and date_tag.has_attr('datetime'):
                             raw_date = date_tag['datetime']

                        source_p = a_tag.select_one('p.feed__meta') or \
                                   (a_tag.find_all('p')[-1] if len(a_tag.find_all('p')) > (1 if date_tag and date_tag.name == 'p' else 0) else None) # Avoid picking date as source
                        if source_p and date_tag and source_p == date_tag : # Ensure source_p is not the same as date_tag if date_tag was a <p>s
                            all_p = a_tag.find_all('p')
                            if len(all_p) > 1: # If there are multiple p tags, try to find one that is not the date
                                for p_tag_candidate in reversed(all_p): # Check from last
                                    if p_tag_candidate != date_tag:
                                        source_p = p_tag_candidate
                                        break
                                else: # All p tags were the date tag? Unlikely.
                                    source_p = None 
                            else: # Only one p tag, and it was used for date
                                source_p = None

                        source, icon_url = get_source_icon(source_p.text.strip()) if source_p else (None, None)

                        if verbose:
                            print(f"[verbose] Scanning article: url={full_url}, title={title!r}, date={raw_date!r}, company={company!r}")

                        key = full_url  # Use URL as the primary key
                        if not key: # Fallback if URL somehow fails
                            key = f'{tab}|{raw_date}|{title}'

                        if not title or not raw_date:
                            continue # Skip item if essential info missing

                        if key in seen_articles:
                            continue

                        # Track only specified companies
                        company_match = company and any(tc.lower() in company.lower() for tc in companies_to_track)
                        title_match = title and any(tc.lower() in title.lower() for tc in companies_to_track)

                        if company_match or title_match:
                            # Call send_to_discord which contains the original log message
                            await send_to_discord(title, raw_date, full_url, company, source, icon_url, bot)

                        success_occurred = True # Mark success if we reach here

                        seen_articles.append(key)
                        # Save less frequently to reduce I/O
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
                await error_message(f'Main loop parser/processing error: {e}', bot)
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