
import websockets
import bs4
import discord
from bs4 import BeautifulSoup
import asyncio

PRESS_RELEASES_CHANNEL = 1163373835886805013

async def fetch_mfn_updates(bot):
    websocket_url = 'wss://mfn.se/all/s?filter=(and(or(.properties.lang="sv"))(or(a.list_id=35207)(a.list_id=35208)(a.list_id=35209)(a.list_id=919325)(a.list_id=35198)(a.list_id=29934)(a.list_id=5700306)(a.list_id=4680265))(or(a.industry_id=36)))'
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
                title_url = "http://www.mfn.se"+soup.find("a", class_="title-link item-link")['href']
                print(f'Fetched news {title} from MFN')
                # Create an embedded message
                embed = discord.Embed(title=author, url=title_url, description=title, color=0x00ff00)
                #embed = discord.Embed(title=title, url=title_url, description=f"Author: [{author}]({author_url})\nDate: {date}\nTime: {time}", color=0x00ff00)

                # Fetch a Discord channel by its ID (replace 'your_channel_id_here' with the actual channel ID)

                channel = bot.get_channel(PRESS_RELEASES_CHANNEL)
                if channel:
                    await channel.send(embed=embed)
                    
    except Exception as e:
        print(f"WebSocket Error: {e}")
        return  # Connection closed or other error, return to allow reconnection attempt

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
        print(f"Reconnecting in {wait_time} seconds...")
        
        await asyncio.sleep(wait_time)  # Wait before retrying