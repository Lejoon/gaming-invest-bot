# commands.py
from discord.ext import commands

class BotCommands(commands.Cog):
    def __init__(self, bot, db):
        self.bot = bot
        self.db = db

    @commands.command()
    async def gts(self, ctx):
        top_games = await fetch_steam_top_sellers()
        response = "\n".join(top_games)
        await ctx.send(f"**Top 250 Global Sellers on Steam:**\n{response}")

    # Add other commands here