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
        # Ensure deque has maxlen when loading
        loaded_deque = pickle.load(open(seen_file,'rb'))
        # Re-create deque with maxlen constraint if it wasn't saved with one or if size changed
        resized_deque = deque(loaded_deque, maxlen=max_queue_size)
        return resized_deque
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


async def fetch(session, url, retries=MAX_FETCH_RETRIES, delay_base=RETRY_DELAY_BASE, timeout=REQUEST_TIMEOUT, headers=DEFAULT_HEADERS):
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
                await error_message(f'Fetch error for {url} after {retries + 1} attempts: {e}')
                return None # Failed after retries
        except Exception as e:
            # Catch any other unexpected errors during fetch using original error_message
            await error_message(f'Unexpected fetch error for {url}: {e}')
            return None # Don't retry unknown errors

    return None # Should not be reached if retries are configured > 0, but acts as a safeguard


async def check_placera(bot):
    tabs = ['telegram','pressmeddelande','extern-analys']
    base = 'https://www.placera.se/telegram?tab={}'
    loop_delay, max_loop_delay = 60, 600
    current_loop_delay = loop_delay

    async with aiohttp.ClientSession() as session:
        while True:
            success_occurred = False
            try:
                for tab in tabs:
                    fetch_url = base.format(tab)
                    html = await fetch(session, fetch_url)
                    if not html:
                        continue # Skip tab on fetch failure

                    soup = BeautifulSoup(html, 'html.parser')
                    # Adjusted selectors (keep checking/updating these if parsing fails)
                    container = soup.select_one('ul.list.list--links') or soup.select_one('div.feed.list.list--links')
                    if not container:
                        container = soup.select_one('div.w-full.bg-surf-tertiary div.flex.flex-col')

                    if not container:
                        # Use original error_message
                        await error_message(f'Could not find article container in {tab} ({fetch_url}). Structure might have changed.', bot)
                        continue # Try next tab

                    # Find articles within the container
                    list_items = container.select('li.feed__list-item') or container.find_all('a', href=re.compile(r'^/telegram/'))

                    for item in list_items:
                        a_tag = item if item.name == 'a' else item.find('a', href=re.compile(r'^/telegram/'))
                        if not a_tag: continue

                        href = a_tag.get('href')
                        if not href: continue

                        full_url = 'https://www.placera.se' + href

                        # Extract info - Selectors might need adjustment
                        company_span = a_tag.select_one('span.button__label.truncate') or a_tag.select_one('span.text-brand, span.font-bold') or a_tag.find('span', class_=re.compile(r'text-\[#'))
                        company = company_span.text.strip() if company_span else None

                        title_tag = a_tag.select_one('h3.heading--small') or a_tag.find('h5') or a_tag.select_one('p.news_feed__paragraph')
                        title = title_tag.text.strip() if title_tag else ''

                        date_tag = a_tag.select_one('time.feed__timestamp') or a_tag.find('p', string=re.compile(r'.+'))
                        raw_date = date_tag.text.strip() if date_tag else ''
                        if date_tag and date_tag.name == 'time' and date_tag.has_attr('datetime'):
                             raw_date = date_tag['datetime']

                        source_p = a_tag.select_one('p.feed__meta') or (a_tag.find_all('p')[-1] if a_tag.find_all('p') else None)
                        source, icon_url = get_source_icon(source_p.text.strip()) if source_p else (None, None)

                        key = full_url # Use URL as the primary key
                        if not key: # Fallback if URL somehow fails
                            key = f'{tab}|{raw_date}|{title}'

                        if not title or not raw_date:
                            continue # Skip item if essential info missing

                        if key in seen_articles:
                            continue

                        success_occurred = True # Mark success if we reach here

                        # Track only specified companies
                        if company and any(tc.lower() in company.lower() for tc in companies_to_track):
                            # Call send_to_discord which contains the original log message
                            await send_to_discord(title, raw_date, full_url, company, source, icon_url, bot)

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
