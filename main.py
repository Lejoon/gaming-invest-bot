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
from steam import gts_command, gts_weekly_command
@bot.command()
async def gts(ctx, *, game_name: str = None):
    await gts_command(ctx, db, game_name)
    
@bot.command()
async def gtsweekly(ctx):
    await gts_weekly_command(ctx, db)
    
# Chart command
from chart import chart_command, parse_period_str # Updated import
@bot.command()
async def chart(ctx, company_name: str, period_str: str = None):
    period_enum = parse_period_str(period_str) # Use the new parsing function
    if period_str and period_enum is None:
        await ctx.send(f"Invalid period: {period_str}. Valid periods are: 1d, 1w, 1m, 3m, ytd, 1y, 3y, 5y, all.")
        return
    await chart_command(ctx, company_name=company_name, period=period_enum)

# Chart command
from chart import report_command
@bot.command()
async def reports(ctx, *, company_name):
    await report_command(ctx, company_name=company_name)

# Steam command
from steam_chart import steam_command
@bot.command()
async def steam(ctx, *, game_name):
    await steam_command(ctx, game_name=game_name)

# PS Store command
from psstore import gtsps_command
@bot.command()
async def ps(ctx):
    await gtsps_command(ctx, db)

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
from steam import SteamPipeline
from pipeline import schedule_pipeline
from psstore import daily_ps_database_refresh
from fi_blankning import update_fi_from_web

# Import and initialize Avanza session
from avanzaauth import get_avanza_session

# Initialize the task variables to None
websocket_task = None
daily_morning_task = None
daily_evening_task = None
placera_task = None
steam_task = None
fi_task = None
ps_task = None

@bot.event
async def on_ready():
    global websocket_task, daily_morning_task, daily_evening_task, placera_task, steam_task, fi_task, ps_task

    print(f"Logged in as {bot.user.name} ({bot.user.id})")

    # Attempt to authenticate with Avanza on startup
    print("Initializing Avanza session...")
    session, push_id = get_avanza_session()
    if session:
        print("Avanza session initialized successfully.")
    else:
        print("Failed to initialize Avanza session on startup. Will retry on first command.")

    if websocket_task is None or websocket_task.done():
        print('Starting background websocket task.')
        websocket_task = bot.loop.create_task(websocket_background_task(bot))
    else:
        print('Background websocket task is already running.')

    if daily_morning_task is None or daily_morning_task.done():
        print('Starting daily morning task.') 
        daily_morning_task = bot.loop.create_task(daily_message_morning(bot))
    else:
        print('Daily morning task is already running.')

    if daily_evening_task is None or daily_evening_task.done():
        print('Starting daily evening task.') 
        daily_evening_task = bot.loop.create_task(daily_message_evening(bot))
    else:
        print('Daily evening task is already running.')

    if placera_task is None or placera_task.done():
        print('Starting Placera telegram loop')
        placera_task = bot.loop.create_task(placera_updates(bot))
    else:
        print('Placera telegram loop is already running.')

    if steam_task is None or steam_task.done():
        print('Starting Steam pipeline')
        steam_pipeline = SteamPipeline(db)
        steam_task = bot.loop.create_task(schedule_pipeline(steam_pipeline))
    else:
        print('Steam pipeline is already running.')

    if ps_task is None or ps_task.done():
        print('Start PS Daily loop')
        ps_task = bot.loop.create_task(daily_ps_database_refresh(db))
    else:
        print('PS Daily loop is already running.')

    if fi_task is None or fi_task.done():
        print('Start FI Blankning loop')
        fi_task = bot.loop.create_task(update_fi_from_web(db, bot))
    else:
        print('FI Blankning loop is already running.')
    
@bot.command()
async def index(ctx):
    await current_index(ctx)
    
# Close the database connection when the bot is stopped
@bot.event
async def on_close():
    db.close()
    
# Run the bot, connect to Discord
bot.run(BOT_TOKEN)