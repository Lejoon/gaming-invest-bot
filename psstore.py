import asyncio
import aiohttp
import json
import html  # to unescape HTML entities
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# Assume these come from your project’s modules.
from database import Database
from general_utils import log_message, error_message, aiohttp_retry, get_seconds_until

# --------------------------
# PS Top Sellers Scraper
# --------------------------

@aiohttp_retry()
async def fetch_page_content(url: str) -> str:
    """Fetch HTML content from the given URL using aiohttp."""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

async def update_ps_top_sellers(db: Database) -> list:
    """
    Scrapes the PlayStation Store top sellers from 5 pages,
    updates the translation table, and (if an update is due)
    inserts the new data into the PSTopGames table.
    
    Returns a list of game dictionaries with keys:
    'timestamp', 'place', 'ps_id', 'game_name', and 'discount'.
    """
    BASE_URL = "https://store.playstation.com/en-us/pages/browse"
    TOTAL_PAGES = 5
    games = []
    placement_counter = 0
    # Use current time rounded down to the hour
    timestamp = datetime.now().strftime('%Y-%m-%d %H')
    
    for page in range(1, TOTAL_PAGES + 1):
        if page == 1:
            url = BASE_URL
        else:
            url = f"{BASE_URL}/{page}"
        print(f"Fetching PS page {page}: {url}")
        html_content = await fetch_page_content(url)
        if not html_content:
            print(f"Failed to fetch page {page}")
            continue

        soup = BeautifulSoup(html_content, 'html.parser')
        # Look for all <a> tags that have telemetry metadata
        a_tags = soup.find_all("a", attrs={"data-telemetry-meta": True})
        for a_tag in a_tags:
            meta_raw = a_tag.get("data-telemetry-meta")
            if meta_raw:
                meta_str = html.unescape(meta_raw)
                try:
                    meta = json.loads(meta_str)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    continue

                ps_id = meta.get("id", "N/A")
                game_name = meta.get("name", "Unknown")
                # If discount info were available, you could extract it here.
                discount = ""  # Default to empty string for now.
                placement_counter += 1
                # Update or insert the translation mapping for this PS game.
                db.update_ps_appid(ps_id, game_name)
                game_data = {
                    'timestamp': timestamp,
                    'place': placement_counter,
                    'ps_id': ps_id,
                    'game_name': game_name,
                    'discount': discount
                }
                games.append(game_data)
        # Sleep briefly to avoid overwhelming the server.
        await asyncio.sleep(0.1)

    # Check if a recent update was already saved (within the last hour)
    latest_timestamp = db.get_latest_timestamp('PSTopGames')
    if latest_timestamp is not None:
        latest_timestamp = datetime.strptime(latest_timestamp, '%Y-%m-%d %H')
    current_time = datetime.now().replace(minute=0, second=0, microsecond=0)
    if latest_timestamp is not None and current_time - latest_timestamp < timedelta(hours=1):
        return games

    # Insert the scraped data into the PSTopGames table
    db.insert_bulk_data(games, 'PSTopGames')
    return games

# --------------------------
# Command Function for PS Top Sellers
# --------------------------

async def gtsps_command(ctx, db: Database):
    """
    This command fetches the top PS games, compares each game's
    current placement (assigned sequentially) with yesterday's placement,
    and sends a message showing any change.
    """
    top_games = await update_ps_top_sellers(db)
    latest_timestamp = db.get_latest_timestamp('PSTopGames')
    # Assume db.get_yesterday_top_games returns a dict: { ps_id: yesterday_placement, ... }
    yesterday_games = db.get_yesterday_top_games(latest_timestamp, table='PSTopGames')

    # Limit to the top 15 games
    top_games = top_games[:15]
    response_lines = []

    for game in top_games:
        current_place = game['place']
        game_name = game['game_name']
        ps_id = game['ps_id']
        # Look up yesterday's placement (if available)
        place_yesterday = yesterday_games.get(ps_id, None)
        if place_yesterday is not None:
            delta = place_yesterday - current_place
            if delta > 0:
                delta_str = f"+{delta}"
            elif delta == 0:
                delta_str = "-"
            else:
                delta_str = f"{delta}"
            line = f"{current_place}. ({delta_str}) {game_name}"
        else:
            line = f"{current_place}. {game_name}"
        response_lines.append(line)

    joined_response = '\n'.join(response_lines)
    await ctx.send(f"**Top 15 PS Games:**\n{joined_response}")

# --------------------------
# (Optional) Daily PS Database Refresh
# --------------------------

async def daily_ps_database_refresh(db: Database):
    while True:
        next_run = datetime.now()
        next_run += timedelta(seconds=get_seconds_until(21, 0))
        log_message(f'Waiting until {next_run.strftime("%Y-%m-%d %H:%M")} to update PS database.')
        await asyncio.sleep(get_seconds_until(21, 0))

        # Update the PS top sellers data in the database.
        await update_ps_top_sellers(db)
        print('Database updated with PS top sellers.')

# --------------------------
# Example Main (for testing purposes)
# --------------------------

if __name__ == "__main__":
    # For testing outside of a bot context:
    # Create an instance of your Database class.
    db = Database("steam_top_games.db")
    # Create an asyncio event loop and run the gtsps_command with a dummy context.
    class DummyContext:
        async def send(self, message):
            print(message)
    dummy_ctx = DummyContext()
    asyncio.run(gtsps_command(dummy_ctx, db))
