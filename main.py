import discord
from discord.ext import commands
import os
import asyncio
import pytz


# Load environment variables
from dotenv import load_dotenv
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Create an instance of the Database class
from database import Database
db = Database('steam_top_games.db')

# Initialize the bot
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents = intents)

# Create tables
#loop = asyncio.get_event_loop()
#loop.run_until_complete(db.create_tables())
db.create_tables()

# Global Top Sellers command
from steam import gts_command
@bot.command()
async def gts(ctx):
    await gts_command(ctx, db)

# Short seller command
from fi_blankning import short_command    
@bot.command()
async def short(ctx, *, company_name):
    await short_command(ctx, db, company_name)
    
# Earnings command
from earnings import earnings_command
@bot.command()
async def earnings(ctx, *args):
    await earnings_command(ctx, *args)

# WebSocket background task scanning MFN.se
from mfn import websocket_background_task
from ig import daily_message_morning, daily_message_evening, current_index
from placera import placera_updates
from steam import daily_steam_database_refresh
from fi_blankning import update_fi_from_web
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user.name} ({bot.user.id})")
    print('Starting background websocket task.')
    bot.loop.create_task(websocket_background_task(bot))
    print('Starting daily morning task.') 
    bot.loop.create_task(daily_message_morning(bot))
    print('Starting daily evening task.') 
    bot.loop.create_task(daily_message_evening(bot))
    print('Starting Placera telegram loop')
    bot.loop.create_task(placera_updates(bot))
    print('Start Steam Daily loop')
    bot.loop.create_task(daily_steam_database_refresh(db))
    print('Start FI Blankning loop')
    bot.loop.create_task(update_fi_from_web(db, bot))
    
@bot.command()
async def index(ctx):
    await current_index(ctx)
    
# Close the database connection when the bot is stopped
@bot.event
async def on_close():
    db.close()
    
# Run the bot, connect to Discord
bot.run(BOT_TOKEN)