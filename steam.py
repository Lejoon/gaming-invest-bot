import sqlite3
from datetime import datetime, timedelta
import aiohttp
from bs4 import BeautifulSoup
import os
from database import Database
import asyncio
STEAM_API_KEY = os.getenv('STEAM_API_KEY')

def get_seconds_until(time_hour, time_minute):
    now = datetime.now()
    target_time = datetime(now.year, now.month, now.day, time_hour, time_minute)
    
    # If target time is in the past, calculate for the next day
    if now > target_time:
        target_time += timedelta(days=1)
        
    return int((target_time - now).total_seconds())

async def fetch_ccu(appid):
    url = f"http://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?key={STEAM_API_KEY}&appid={appid}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
    if data['response']['result'] == 1:
        ccu = data['response']['player_count']
    else:
        ccu = 0
    return ccu

async def update_steam_top_sellers(db: Database) -> dict:
    url = "https://store.steampowered.com/search/?filter=globaltopsellers"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()
    soup = BeautifulSoup(html, 'html.parser')
    
    games = []
    count = 0
    
    for game_div in soup.select('.search_result_row')[:50]:
        appid = game_div['data-ds-appid']
        title_elements = game_div.select('.title')
        price_elements = game_div.select('.discount_final_price, .search_price')
        discount_elements = game_div.select('.discount_pct, .search_price')

        title = title_elements[0].text if title_elements else "Unknown title"
        discount = discount_elements[0].text.strip() if discount_elements else ""
        price = price_elements[0].text.strip() if price_elements else ""
        
        if price == "Free":
            discount = "Free"
        
        count += 1
        
        # Check if the appid already exists in the translation table. If the appid doesn't exist, insert it into the translation table
        db.update_appid(appid, title)

        timestamp = datetime.now().strftime('%Y-%m-%d %H')
        
        # Fetch CCU using Steam API
        ccu = await fetch_ccu(appid)
        
        # Store the data in a dictionary and add it to the games list
        game_data = {
            'timestamp': timestamp,
            'count': count,
            'appid': appid,
            'title': title,
            'discount': discount,
            'ccu': ccu
        }
        games.append(game_data)
        
        # Fetch the latest timestamp from the database
    latest_timestamp = db.get_latest_timestamp()
    
    if latest_timestamp is not None:
        latest_timestamp = datetime.strptime(latest_timestamp, '%Y-%m-%d %H')

    # Get the current time (up to the hour)
    current_time = datetime.now().replace(minute=0, second=0, microsecond=0)

    # If there was an update within the last hour, don't update the database
    if latest_timestamp is not None and current_time - latest_timestamp < timedelta(hours=1):
        return games

    db.insert_bulk_data(games)
    
    return games

async def gts_command(ctx, db: Database):
    top_games = await update_steam_top_sellers(db)
    
    latest_timestamp = db.get_latest_timestamp()
    yesterday_games = db.get_yesterday_top_games(latest_timestamp)
    
    top_games = top_games[:15]
    
    response = []
    
    for game in top_games:
        # Construct the line for each game
        place = game['count']
        title = game['title']
        discount = game['discount']
        ccu = game['ccu']
        
        # Convert place_yesterday to an integer
        place_yesterday = int(place_yesterday) if place_yesterday != 'N/A' else None
        
        # Add + or - depending on sign of place_yesterday - place
        place_delta = place_yesterday - place if place_yesterday is not None else None
        place_delta_str = "+" + str(place_delta) if place_delta is not None and place_delta > 0 else str(place_delta)
            
        if place_yesterday:
            line = f"{place}. ({place_delta_str}) {title}"
        else:
            line = f"{place}. {title}"
        
        if ccu:
            line += f" (CCU: {ccu})"
              
        response.append(line)
    
    await ctx.send(f"**Top 15 Global Sellers on Steam:**\n{response}")

async def daily_steam_database_refresh(db: Database):
    while True:
        # Calculate the number of seconds until 21:00
        await asyncio.sleep(get_seconds_until(21, 0))

        # Perform the database update
        await update_steam_top_sellers(db)
        print('Database updated with Steam top sellers.')
        
