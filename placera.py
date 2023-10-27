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
            return description, icon_dict[key]
    return description, None

# List of companies to track (case insensitive)
companies_to_track = ['Embracer', 'Paradox', 'Ubisoft', 'Starbreeze', 'EG7', 'Enad Global 7', 'Take Two', 'Capcom', 'Maximum Entertainment', 'MAG Interactive', 'G5', 'Remedy', 'MTG', 'Modern Times Group', 'Rovio', 'Thunderful', 'MGI', 'Electronic Arts', 'Take-Two', 'Stillfront']

# Create a deque with a maximum size to store the recently seen articles
max_queue_size = 100

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
    channel = bot.get_channel(TELEGRAM_CHANNEL)  # Replace with your channel ID
    if company:
        title = title.replace(f"{company}:", "").strip()
    timestamp=datetime.strptime(date, "%Y-%m-%d %H:%M")
    description, icon_url = get_icon_from_description(title)
    embed = discord.Embed(title=company, description=description, timestamp=timestamp)
    if icon_url:
        embed.set_thumbnail(url=icon_url)

    embed = discord.Embed(title=company, description=title, url=url, )
    if channel:
        await channel.send(embed=embed)
        print('Sent telegram item')

async def fetch_page(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

async def check_for_placera_updates(bot):
    while True:
        await asyncio.sleep(30)
        url = 'https://www.placera.se/placera/telegram.html'
        page_content = await fetch_page(url)
        soup = BeautifulSoup(page_content, 'html.parser')

        ul_list = soup.find('ul', {'class': 'feedArticleList XSText'})

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

async def placera_updates(bot):
    await check_for_placera_updates(bot)
