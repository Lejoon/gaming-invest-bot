from bs4 import BeautifulSoup
from collections import deque
import discord
from datetime import datetime
import asyncio
import aiohttp
import pickle
import re

TELEGRAM_CHANNEL = 1167391973825593424
icon_dict = {
    'Finwire': 'https://finwire.com/wp-content/uploads/2021/03/1.5-FINWIRE-Logotype-Bird-Icon-2020-PMS021-300x300.png',
    'Direkt': 'https://media.licdn.com/dms/image/C560BAQFerUMPTdDrAA/company-logo_200_200/0/1569249859285/nyhetsbyr_n_direkt_logo?e=1706745600&v=beta&t=YUjFmqgCdSjIebxklnaYep7RfaKL9vLhfJdJNBA594Q',
}

def get_icon_from_description(description):
    for key in icon_dict:
        if key in description:
            description = re.sub(rf'\({key}\)', '', description).strip()
            print(icon_dict[key])
            return key, description, icon_dict[key]
    return key, description, None

# List of companies to track (case insensitive)
companies_to_track = ['Embracer', 'Paradox', 'Ubisoft', 'Starbreeze', 'EG7', 'Flexion', 'Enad Global 7', 'Take Two', 'Capcom', 'Maximum Entertainment', 'MAG Interactive', 'G5', 'Remedy', 'MTG', 'Modern Times Group', 'Rovio', 'Thunderful', 'MGI', 'Electronic Arts', 'Take-Two', 'Stillfront', 'Take-Two']

# Create a deque with a maximum size to store the recently seen articles
max_queue_size = 1000

def save_seen_articles():
    with open('seen_articles.pkl', 'wb') as f:
        pickle.dump(seen_articles, f)

def load_seen_articles():
    try:
        with open('seen_articles.pkl', 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return deque(maxlen=max_queue_size)

seen_articles = load_seen_articles()

async def send_to_discord(title, date, url, company, bot):
    channel = bot.get_channel(TELEGRAM_CHANNEL)
    
    if company:
        title = title.replace(f"{company}:", "").strip()
    
    key, description, icon_url = get_icon_from_description(title)
        
    timestamp=datetime.strptime(date, "%Y-%m-%d %H:%M")
    print(timestamp)
    
    embed = discord.Embed(title=company, description=description, url=url, timestamp=timestamp)
    
    if icon_url:
        embed.set_footer(text=key, icon_url=icon_url)

    if channel:
        await channel.send(embed=embed)
        print('Sent telegram item')

async def fetch_page(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()  # Raise an error for bad responses like 404 or 500
                return await response.text()
            
    except aiohttp.ClientError as e:
        print(f"[ERR] Placera error occurred: {e}")
    except aiohttp.http_exceptions.HttpProcessingError as e:
        print(f"[ERR] Placera error occurred: {e}")
    except Exception as e:
        print(f"[ERR] Placera, an unexpected error occurred: {e}")
    return None  


async def check_for_placera_updates(bot):
    delay = 60  # Initial delay in seconds
    max_delay = 600  # Maximum delay in seconds (10 minutes)

    while True:
        try:
            url = 'https://www.placera.se/placera/telegram.html'
            page_content = await fetch_page(url)
            
            if page_content is None:
                print("[ERR] Failed to retrieve Placera page content.")
                raise Exception("Failed to retrieve content")

        
            soup = BeautifulSoup(page_content, 'html.parser')

            ul_list = soup.find('ul', {'class': 'feedArticleList XSText'})

            if ul_list is None:
                raise Exception("Could not find the required ul element. The Placera page structure might have changed.")

            for li in ul_list.find_all('li', {'class': 'item'}):
                a_tag = li.find('a')
                relative_url = a_tag['href']
                full_url = f'http://www.placera.se{relative_url}'
                
                intro_div = a_tag.find('div', {'class': 'intro'})
                company_span = intro_div.find('span', {'class': 'bold'})
                
                company = company_span.text.strip().rstrip(":") if company_span else None
                title = intro_div.text.strip()
                date = li.find('span', {'class': 'date'}).text.strip()

                article_id = date + title

                if article_id not in seen_articles:
                    for tracked_company in companies_to_track:
                        if company and tracked_company.lower() in company.lower():
                            print(f'Found news item regarding {company}')
                            await send_to_discord(title, date, full_url, company, bot)
                            break
                    
                    seen_articles.append(article_id)
                    save_seen_articles()
                
                delay = 60  # Reset delay on success
    
        except Exception as e:
            print(f"An error occurred while parsing Placera: {e}")
            delay = min(delay * 2, max_delay)  # Double the delay, up to a maximum

        finally:
            await asyncio.sleep(delay)  # Sleep for the current del

async def placera_updates(bot):
    await check_for_placera_updates(bot)
