from bs4 import BeautifulSoup
from collections import deque
import discord
from datetime import datetime
import asyncio
import aiohttp

TELEGRAM_CHANNEL = 1167391973825593424

# List of companies to track (case insensitive)
companies_to_track = ['Embracer', 'Paradox', 'Ubisoft', 'Starbreeze', 'EG7', 'Enad Global 7', 'Take Two', 'Capcom', 'Maximum Entertainment', 'MAG Interactive', 'G5', 'Remedy', 'MTG', 'Modern Times Group', 'Rovio', 'Thunderful', 'MGI', 'Electronic Arts', 'Take-Two', 'Stillfront']

# Create a deque with a maximum size to store the recently seen articles
max_queue_size = 100
seen_articles = deque(maxlen=max_queue_size)

async def send_to_discord(title, date, url, company, bot):
    channel = bot.get_channel(TELEGRAM_CHANNEL)  # Replace with your channel ID
    embed = discord.Embed(title=company, description=title, url=url, timestamp=datetime.strptime(date, "%Y-%m-%d %H:%M"))
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

async def placera_updates(bot):
    await check_for_placera_updates(bot)
