from bs4 import BeautifulSoup
from collections import deque
import discord
from datetime import datetime
import asyncio
import aiohttp
import pickle
import re
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

# persistence
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

def load_seen():
    try:
        return pickle.load(open(seen_file,'rb'))
    except FileNotFoundError:
        return deque(maxlen=max_queue_size)

def save_seen(q):
    pickle.dump(q, open(seen_file,'wb'))

seen_articles = load_seen()

async def send_to_discord(title, raw_date, url, company, source, icon_url, bot):
    chan = bot.get_channel(TELEGRAM_CHANNEL)
    embed = discord.Embed(
        title=company or 'Placera',
        description=title,
        url=url,
        timestamp=datetime.utcnow()
    )
    if source and icon_url:
        embed.set_footer(text=source, icon_url=icon_url)
    if chan:
        await chan.send(embed=embed)
        log_message(f'Sent "{title}" ({raw_date}) to Discord.')

async def fetch(session, url):
    try:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.text()
    except Exception as e:
        await error_message(f'Fetch error for {url}: {e}')
        return None

async def check_placera(bot):
    tabs = ['telegram','pressmeddelande','extern-analys']
    base = 'https://www.placera.se/telegram?tab={}'
    delay, max_delay = 60, 600

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                for tab in tabs:
                    html = await fetch(session, base.format(tab))
                    if not html:
                        continue

                    soup = BeautifulSoup(html, 'html.parser')
                    container = soup.select_one('div.w-full.bg-surf-tertiary div.flex.flex-col')
                    if not container:
                        await error_message(f'No container in {tab}', bot)
                        continue

                    for a in container.find_all('a', href=re.compile(r'^/telegram/')):
                        href = a['href']
                        full_url = 'https://www.placera.se' + href
                        # company
                        span = a.find('span', class_=re.compile(r'text-\[#'))
                        company = span.text.strip() if span else None
                        # raw date text e.g. "Idag, 12:35"
                        date_p = a.find('p', string=re.compile(r'.+'))
                        raw_date = date_p.text.strip() if date_p else ''
                        # title
                        h5 = a.find('h5')
                        title = h5.text.strip() if h5 else ''
                        # source
                        src_p = a.find_all('p')[-1]
                        source, icon_url = get_source_icon(src_p.text.strip()) if src_p else (None,None)

                        # dedupe
                        key = f'{tab}|{raw_date}|{title}'
                        if key in seen_articles:
                            continue

                        # track only your companies
                        if company and any(tc.lower() in company.lower() for tc in companies_to_track):
                            await send_to_discord(title, raw_date, full_url, company, source, icon_url, bot)

                        seen_articles.append(key)
                        save_seen(seen_articles)

                delay = 60
            except Exception as e:
                await error_message(f'Parser error: {e}', bot)
                delay = min(delay * 2, max_delay)
            finally:
                await asyncio.sleep(delay)

async def placera_updates(bot):
    await check_placera(bot)
