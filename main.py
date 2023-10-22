from datetime import datetime, timedelta
import re
import discord
from discord.ext import commands
from datetime import datetime
from bs4 import BeautifulSoup
import aiohttp
import os
import websockets
import time
import asyncio
import json
import aiomysql
import sqlite3

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Create an instance of the Database class
from database import Database
db = Database('steam_top_games.db')

# Import earning dates
from earning_dates import date_to_company

# Initialize the bot
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents = intents)

# ------ WebSocket ------

from mfn import websocket_background_task

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user.name} ({bot.user.id})")
    bot.loop.create_task(websocket_background_task())  # Start the background task

# Create tables
loop = asyncio.get_event_loop()
loop.run_until_complete(db.create_tables())

async def fetch_ccu(appid):
    STEAM_API_KEY = os.getenv('STEAM_API_KEY')
    url = f"http://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?key={STEAM_API_KEY}&appid={appid}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
    if data['response']['result'] == 1:
        ccu = data['response']['player_count']
    else:
        ccu = 0
    return ccu

async def fetch_steam_top_sellers():
    # Initialize SQLite Database
    conn = sqlite3.connect('steam_top_games.db')
    cursor = conn.cursor()
    

    # Fetch the latest timestamp from the database
    cursor.execute("SELECT MAX(timestamp) FROM SteamTopGames")
    latest_timestamp = cursor.fetchone()[0]
    if latest_timestamp is not None:
        latest_timestamp = datetime.strptime(latest_timestamp, '%Y-%m-%d %H')

    # Get the current time (up to the hour)
    current_time = datetime.now().replace(minute=0, second=0, microsecond=0)

    # If there was an update within the last hour, don't update the database
    if latest_timestamp is not None and current_time - latest_timestamp < timedelta(hours=1):
        return
    
    
    url = "https://store.steampowered.com/search/?filter=globaltopsellers"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()
    soup = BeautifulSoup(html, 'html.parser')
    
    games = []
    count = 1
    
    for game_div in soup.select('.search_result_row')[:250]:
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
        
        # Check if the appid already exists in the translation table
        cursor.execute("SELECT game_name FROM GameTranslation WHERE appid = ?", (appid,))
        result = cursor.fetchone()
        
        # If the appid doesn't exist, insert it into the translation table
        if result is None:
            cursor.execute("INSERT INTO GameTranslation (appid, game_name) VALUES (?, ?)", (appid, title))

        timestamp = datetime.now().strftime('%Y-%m-%d %H')
        
        cursor.execute('''
        INSERT INTO SteamTopGames (timestamp, place, appid, discount, ccu)
        VALUES (?, ?, ?, ?, ?)
        ''', (timestamp, count, appid, discount, ccu))
        
        # Fetch CCU using Steam API
        ccu = fetch_ccu(appid)

    return games

# Global Top Sellers command
@bot.command()
async def gts(ctx):
    top_games = await fetch_steam_top_sellers()
    response = "\n".join(top_games)
    await ctx.send(f"**Top 250 Global Sellers on Steam:**\n{response}")

# Earnings command
@bot.command()
async def earnings(ctx, *args):
    valid_formats = [
        'YYYY-MM-DD',
        'MM/DD/YYYY',
        'YYYY/MM/DD',
        'YYYYMM (for a whole month)',
    ]
    if args:
        date_str = args[0]
        # YYYY-MM-DD format
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            query_date = datetime.strptime(date_str, '%Y-%m-%d')
        # MM/DD/YYYY format
        elif re.match(r'\d{2}/\d{2}/\d{4}', date_str):
            query_date = datetime.strptime(date_str, '%m/%d/%Y')
        # YYYY/MM/DD format
        elif re.match(r'\d{4}/\d{2}/\d{2}', date_str):
            query_date = datetime.strptime(date_str, '%Y/%m/%d')
        # YYYYMM format for a whole month
        elif re.match(r'\d{4}\d{2}', date_str):
            month_companies = {date: company for date, company in date_to_company.items() if date.strftime('%Y%m') == date_str}
            if month_companies:
                await ctx.send('\n'.join(f"{date.strftime('%Y-%m-%d')}: {company}" for date, company in month_companies.items()))
                return
            else:
                await ctx.send('No earnings in this month.')
                return
        else:
            await ctx.send(f'Invalid date format. Valid formats are: {", ".join(valid_formats)}.')
            return

        earnings_info = date_to_company.get(query_date, 'No earnings on this date.')
        await ctx.send(earnings_info)

    else:
        # No argument, show next week's companies including today
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) # Set the time to 00:00:00
        next_week = today + timedelta(days=7)
        next_week_companies = {date: company for date, company in date_to_company.items() if today <= date <= next_week}
        
        if next_week_companies:
            await ctx.send('**Next 7 days earnings and dates**:\n'+'\n'.join(f"{date.strftime('%Y-%m-%d, %A')}: {company}" for date, company in next_week_companies.items()))
        else:
            await ctx.send('No earnings in the 7 days.')



# Run the bot
bot_token = os.getenv('BOT_TOKEN')

bot.run(bot_token)

# Close the connection when the bot is stopped
@bot.event
async def on_close():
    db.close()