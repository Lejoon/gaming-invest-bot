
import websockets
import bs4
import discord
from bs4 import BeautifulSoup
import asyncio
from datetime import datetime, timedelta

PRESS_RELEASES_CHANNEL = 1163373835886805013
WEBSOCKET_URL = 'wss://mfn.se/all/s?filter=(and(or(.properties.lang="en"))(or(a.list_id=35207)(a.list_id=35208)(a.list_id=35209)(a.list_id=919325)(a.list_id=35198)(a.list_id=29934)(a.list_id=5700306)(a.list_id=4680265))(or(a.industry_id=36)))'

async def fetch_mfn_updates(bot):
    websocket_url = WEBSOCKET_URL
    last_disconnect_time = None
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
                time = time[:-3]
                author = soup.find("a", class_="title-link author-link author-preview").text
                author_url = soup.find("a", class_="title-link author-link author-preview")['href']
                title = soup.find("a", class_="title-link item-link").text
                title_url = "http://www.mfn.se"+soup.find("a", class_="title-link item-link")['href']
                print(f'Fetched news {title} from MFN')
                # Create an embedded message
    
                embed = discord.Embed(title=author, url=title_url, description=title, color=0x00ff00, timestamp=datetime.strptime(date+" "+time, "%Y-%m-%d %H:%M"))
                #embed = discord.Embed(title=title, url=title_url, description=f"Author: [{author}]({author_url})\nDate: {date}\nTime: {time}", color=0x00ff00)

                # Fetch a Discord channel by its ID (replace 'your_channel_id_here' with the actual channel ID)

                channel = bot.get_channel(PRESS_RELEASES_CHANNEL)
                if channel:
                    await channel.send(embed=embed)
                    
    except Exception as e:
        current_time = datetime.now()

        # The websocket automatically closes after 5 minutes of inactivity
        if last_disconnect_time is None or (current_time - last_disconnect_time).total_seconds() > 330:
            print(f"[ERR] WebSocket Error: {e}")
        return 

async def websocket_background_task(bot):
    attempt_count = 0
    while True:
        try:
            await fetch_mfn_updates(bot)
            print("WebSocket connection closed.")
            attempt_count = 0  # Reset the attempt count if successfully connected
        except Exception as e:
            print(f"WebSocket Error: {e}")

        # Calculate the wait time using exponential backoff
        attempt_count += 1
        wait_time = min(2 ** attempt_count, 60)  # Exponential backoff, capped at 60 seconds
        
        if wait_time > 10:  # Only log if the wait time is more than 8 seconds to be less verbose
            print(f"[LOG] Reconnecting websocket in {wait_time} seconds...")
        
        await asyncio.sleep(wait_time)  # Wait before retrying