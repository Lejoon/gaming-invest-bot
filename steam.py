import sqlite3
from datetime import datetime, timedelta
from general_utils import get_seconds_until
import aiohttp
from bs4 import BeautifulSoup
import os
from database import Database
import asyncio
from general_utils import log_message, error_message, aiohttp_retry
STEAM_API_KEY = os.getenv('STEAM_API_KEY')

@aiohttp_retry()
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
    latest_timestamp = db.get_latest_timestamp('SteamTopGames')
    
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
    def format_ccu(ccu):
        if ccu >= 1000:
            return f"{ccu // 1000}k"  # Integer division by 1000
        else:
            return str(ccu)  # Keep the original number if it's less than 1000

    top_games = await update_steam_top_sellers(db)
    
    latest_timestamp = db.get_latest_timestamp('SteamTopGames')
    yesterday_games = db.get_yesterday_top_games(latest_timestamp)
    
    top_games = top_games[:15]
    
    response = []
    
    for game in top_games:
        # Construct the line for each game
        place = game['count']
        title = game['title']
        discount = game['discount']
        ccu = game['ccu']
        place_yesterday = yesterday_games.get(game['appid'], 'N/A')

        # Convert place_yesterday to an integer
        place_yesterday = int(place_yesterday) if place_yesterday != 'N/A' else None
        
        # Add + or - depending on sign of place_yesterday - place
        place_delta = place_yesterday - place if place_yesterday is not None else None
        
        if place_delta is not None and place_delta > 0:
            place_delta_str = "+" + str(place_delta)
        elif place_delta is not None and place_delta == 0:
            place_delta_str = "-"
        else:
            place_delta_str = str(place_delta)

        if place_yesterday:
            line = f"{place}. ({place_delta_str}) {title}"
        else:
            line = f"{place}. {title}"
        
        if ccu:
            formated_ccu = format_ccu(ccu)
            line += f" (CCU: {formated_ccu})"
              
        response.append(line)
    
    joined_response = '\n'.join(response)
    await ctx.send(f"**Top 15 Global Sellers on Steam:**\n{joined_response}")


async def gts_weekly_command(ctx, db: Database):
    top_games = await update_steam_top_sellers(db)
    latest_timestamp = db.get_latest_timestamp('SteamTopGames')
    last_week_ranks = db.get_last_week_ranks(latest_timestamp, [game['appid'] for game in top_games[:25]])

    number_of_days = {}
    # Calculate the 7-day average rank for each game
    game_avg_ranks_7d = {}
    for appid, ranks in last_week_ranks.items():
        game_avg_ranks_7d[appid] = sum(ranks) / len(ranks)
        number_of_days[appid] = len(ranks)

    # Calculate the 3-day average rank for each game
    game_avg_ranks_3d = {}
    for appid, ranks in last_week_ranks.items():
        game_avg_ranks_3d[appid] = sum(ranks[-3:]) / min(len(ranks), 3)

    # Sort the games based on their 7-day average rank
    sorted_games = sorted(top_games, key=lambda game: game_avg_ranks_7d.get(game['appid'], float('inf')))

    response = []

    for game in sorted_games[:25]:
        # Construct the line for each game
        place = sorted_games.index(game) + 1
        title = game['title']
        discount = game['discount']
        ccu = game['ccu']

        avg_rank_7d = game_avg_ranks_7d.get(game['appid'], None)
        avg_rank_3d = game_avg_ranks_3d.get(game['appid'], None)

        if avg_rank_7d is not None and avg_rank_3d is not None:
            # Determine the trend symbol based on the comparison of 3-day and 7-day average ranks
            if abs(avg_rank_3d - avg_rank_7d) <= 1:
                trend_symbol = ':small_orange_diamond:'
            elif avg_rank_3d < avg_rank_7d:
                trend_symbol = ':small_red_triangle:'
            else:
                trend_symbol = ':small_red_triangle_down:'
                
            if number_of_days[game['appid']] < 7:
                trend_symbol = ':new:'

            line = f"{place}. {trend_symbol} {title}"
        else:
            line = f"{place}. {title}"


        response.append(line)

    joined_response = '\n'.join(response)
    await ctx.send(f"**Top 25 Global Sellers on Steam, last week average:**\n{joined_response}")
async def daily_steam_database_refresh(db: Database):
    while True:
        log_time = datetime.now()
        log_time += timedelta(seconds=get_seconds_until(21,0))
        log_message(f'Waiting until {log_time.strftime("%Y-%m-%d %H:%M")} to update Steam database.')
        await asyncio.sleep(get_seconds_until(21, 0))
        

        # Perform the database update
        await update_steam_top_sellers(db)
        print('Database updated with Steam top sellers.')
        
