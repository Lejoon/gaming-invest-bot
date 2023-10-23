import discord
from discord.ext import commands
import os
import asyncio


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
    
# Earnings command
from earnings import earnings_command
@bot.command()
async def earnings(ctx, *args):
    await earnings_command(ctx, *args)

# WebSocket background task scanning MFN.se
from mfn import websocket_background_task
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user.name} ({bot.user.id})")
    bot.loop.create_task(websocket_background_task())  # Start the background task
    
# Close the database connection when the bot is stopped
@bot.event
async def on_close():
    db.close()
    
# Run the bot, connect to Discord
bot.run(BOT_TOKEN)