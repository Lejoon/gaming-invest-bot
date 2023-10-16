from datetime import datetime, timedelta
import re
import discord
from discord.ext import commands
from datetime import datetime
from bs4 import BeautifulSoup
import aiohttp
from dotenv import load_dotenv
import os
import websockets
import time
import asyncio

date_to_company = {
    datetime(2023, 9, 13): "Frontier Developments",
    datetime(2023, 9, 16): "Gamestop",
    datetime(2023, 9, 19): "Team17",
    datetime(2023, 9, 21): "People Can Fly",
    datetime(2023, 9, 25): "Devolver Digital",
    datetime(2023, 9, 26): "tinyBuild",
    datetime(2023, 9, 27): "Digital Bros/505 Games",
    datetime(2023, 9, 29): "CI Games",
    datetime(2023, 10, 16): "DON'T NOD",
    datetime(2023, 10, 18): "Netflix",
    datetime(2023, 10, 19): "Focus Entertainment",
    datetime(2023, 10, 24): "Microsoft",
    datetime(2023, 10, 26): ["Capcom", "Ubisoft", "Paradox Interactive"],
    datetime(2023, 10, 27): "Activision Blizzard*",
    datetime(2023, 10, 30): ["NACON", "KOEI Tecmo"],
    datetime(2023, 10, 31): "Remedy",
    datetime(2023, 11, 1): "EA",
    datetime(2023, 11, 2): ["Kadokawa", "Paramount", "Konami"],
    datetime(2023, 11, 7): ["Bandai Namco", "Nintendo"],
    datetime(2023, 11, 8): ["Take-Two*", "SEGA Sammy", "Disney", "Roblox", "Warner Discovery"],
    datetime(2023, 11, 9): ["NEXON", "Square Enix*", "Krafton*", "Sony"],
    datetime(2023, 11, 14): "Bloober Team*",
    datetime(2023, 11, 15): ["Tencent", "NetEase*", "Maximum Entertainment", "Thunderful Group"],
    datetime(2023, 11, 16): ["Embracer Group", "Starbreeze"],
    datetime(2023, 11, 23): "11bit Studios",
    datetime(2023, 11, 28): "CD Projekt Group",
}

# Initialize the bot
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents = intents)

# ------ WebSocket ------

async def fetch_mfn_updates():
    websocket_url = 'wss://www.mfn.se/all/s?filter=(and(or(.properties.lang%3D%22sv%22))(or(a.list_id%3D35207)(a.list_id%3D35208)(a.list_id%3D35209)(a.list_id%3D919325)(a.list_id%3D35198)(a.list_id%3D29934)(a.list_id%3D5700306)(a.list_id%3D4680265)))'
    try:
        async with websockets.connect(websocket_url) as ws:
            print("WebSocket connection established.")
            while True:
                message = await ws.recv()

                # Parse the HTML content
                soup = BeautifulSoup(message, 'html.parser')
                
                # Extract the required information
                date = soup.find("span", class_="compressed-date").text
                time = soup.find("span", class_="compressed-time").text
                author = soup.find("a", class_="title-link author-link author-preview").text
                author_url = soup.find("a", class_="title-link author-link author-preview")['href']
                title = soup.find("a", class_="title-link item-link").text
                title_url = "http://www.mfn.se/"+soup.find("a", class_="title-link item-link")['href']

                # Create an embedded message
                embed = discord.Embed(title=author, url=title_url, description=title, color=0x00ff00)
                #embed = discord.Embed(title=title, url=title_url, description=f"Author: [{author}]({author_url})\nDate: {date}\nTime: {time}", color=0x00ff00)

                # Fetch a Discord channel by its ID (replace 'your_channel_id_here' with the actual channel ID)
                channel = bot.get_channel(1163373835886805013)
                if channel:
                    await channel.send(embed=embed)
                    
    except Exception as e:
        print(f"WebSocket Error: {e}")
        return  # Connection closed or other error, return to allow reconnection attempt

async def websocket_background_task():
    attempt_count = 0
    while True:
        try:
            await fetch_mfn_updates()
            print("WebSocket connection closed.")
            attempt_count = 0  # Reset the attempt count if successfully connected
        except Exception as e:
            print(f"WebSocket Error: {e}")

        # Calculate the wait time using exponential backoff
        attempt_count += 1
        wait_time = min(2 ** attempt_count, 60)  # Exponential backoff, capped at 60 seconds
        print(f"Reconnecting in {wait_time} seconds...")
        
        await asyncio.sleep(wait_time)  # Wait before retrying

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user.name} ({bot.user.id})")
    bot.loop.create_task(websocket_background_task())  # Start the background task


#------ Commands ------


async def fetch_steam_top_sellers():
    url = "https://store.steampowered.com/search/?filter=globaltopsellers"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()
    soup = BeautifulSoup(html, 'html.parser')
    games = []
    count = 1
    for game_div in soup.select('.search_result_row')[:10]:
        title_elements = game_div.select('.title')
        price_elements = game_div.select('.discount_pct')

        title = title_elements[0].text if title_elements else "Unknown title"
        price = price_elements[0].text.strip() if price_elements else ""

        if price_elements:
            games.append(f"{count}. {title} **({price})**")
        else:
            games.append(f"{count}. {title}")
        
        count += 1
    return games

# Global Top Sellers command
@bot.command()
async def gts(ctx):
    top_games = await fetch_steam_top_sellers()
    response = "\n".join(top_games)
    await ctx.send(f"**Top 10 Global Sellers on Steam:**\n{response}")

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
        # No argument, show next week's companies
        today = datetime.now()
        next_week = today + timedelta(days=7)
        next_week_companies = {date: company for date, company in date_to_company.items() if today <= date <= next_week}
        
        if next_week_companies:
            await ctx.send('**Next 7 days earnings and dates**:\n'+'\n'.join(f"{date.strftime('%Y-%m-%d, %A')}: {company}" for date, company in next_week_companies.items()))
        else:
            await ctx.send('No earnings in the 7 days.')

# Run the bot
load_dotenv()
bot_token = os.getenv('BOT_TOKEN')

bot.run(bot_token)



## 1163373835886805013