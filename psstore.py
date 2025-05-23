import asyncio
import aiohttp
import json
import html  # to unescape HTML entities
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import discord  # Added import

# Assume these come from your project’s modules.
from database import Database
from general_utils import log_message, error_message, aiohttp_retry, get_seconds_until, normalize_game_name_for_search, generate_gts_placements_plot # Updated import

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
    TOTAL_PAGES = 10
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

def get_best_ps_game_match(user_query, db: Database):
    """Finds the best match for a user's game query against PS game names."""
    cursor = db.conn.execute("SELECT DISTINCT game_name FROM PSTranslation") # Assuming PSTranslation table
    original_game_names = [row[0] for row in cursor.fetchall()]
    if not original_game_names:
        return None

    q = normalize_game_name_for_search(user_query)
    if not q:
        return None

    pairs = []
    for orig in original_game_names:
        norm = normalize_game_name_for_search(orig)
        if norm:
            pairs.append((norm, orig))

    # 1) word‐level match: all tokens must match whole words
    query_tokens = q.split()
    word_matches = [
        (norm, orig) for norm, orig in pairs
        if all(token in norm.split() for token in query_tokens)
    ]
    if word_matches:
        # Prefer shorter matches if multiple word-level matches exist
        return min(word_matches, key=lambda x: len(x[0]))[1]

    # 2) prefix match
    prefix = [p for p in pairs if p[0].startswith(q)]
    if prefix:
        return min(prefix, key=lambda x: len(x[0]))[1]

    # 3) substring match
    substr = [p for p in pairs if q in p[0]]
    if substr:
        return min(substr, key=lambda x: len(x[0]))[1]

    # 4) difflib fallback (ensure difflib is imported in general_utils or here)
    # For simplicity, assuming difflib is available via general_utils or globally
    # import difflib # Would be needed if not imported elsewhere
    # names = [p[0] for p in pairs]
    # close = difflib.get_close_matches(q, names, n=1, cutoff=0.75)
    # if close:
    #     # Find the original name corresponding to the normalized close match
    #     for norm_name, orig_name in pairs:
    #         if norm_name == close[0]:
    #             return orig_name
    # For now, skipping difflib for PS Store to keep it simpler unless specified

    return None

async def gtsps_command(ctx, db: Database, game_name: str = None):
    """
    If a game name is provided, generates a graph of its PS Store placements.
    Otherwise, displays the top 15 PS sellers.
    """
    if game_name is not None:
        matched_game_name = get_best_ps_game_match(game_name, db)
        if matched_game_name:
            # You will need a method in your Database class to fetch placement data for a PS game
            # Example: aggregated_data = db.get_ps_gts_placements_for_game(matched_game_name)
            aggregated_data = db.get_last_month_ps_placements(matched_game_name) # Placeholder, changed method name
            if aggregated_data and aggregated_data.get("positions") and aggregated_data.get("placements"):
                image_stream, discord_file = generate_gts_placements_plot(aggregated_data, matched_game_name)
                await ctx.send(file=discord_file)
                return
            else:
                await ctx.send(f"Could not find enough data to generate a plot for '{matched_game_name}' on PS Store.")
                return
        else:
            await ctx.send(f"Could not find a match for game: '{game_name}' on PS Store.")
            return

    # No game name provided; display the standard top sellers list.
    top_games = await update_ps_top_sellers(db)
    latest_timestamp = db.get_latest_timestamp('PSTopGames')
    
    # Calculate yesterday's timestamp at hour 21 for comparison
    if latest_timestamp:
        current_dt = datetime.strptime(latest_timestamp, '%Y-%m-%d %H')
        yesterday_dt = current_dt - timedelta(days=1)
        # The timestamp passed to get_yesterday_top_games will be used to derive yesterday's date.
        # The method itself will handle finding the correct data for PSTopGames (latest for that date)
        # or use hour 21 for SteamTopGames.
        yesterday_query_timestamp = current_dt.strftime('%Y-%m-%d %H') 
    else:
        yesterday_query_timestamp = None
    
    # Get yesterday's games using the calculated yesterday timestamp
    yesterday_games = db.get_yesterday_top_games(yesterday_query_timestamp, table='PSTopGames')

    # --- Debug print statement ---
    if hasattr(ctx, 'is_dummy_context'): # Check if it's the dummy context from __main__
        print("\n--- Debug: yesterday_games ---")
        print(yesterday_games)
        print("--- End Debug ---\n")
    # --- End Debug print statement ---

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
                delta_str = f"(+{delta})"
            elif delta == 0:
                delta_str = "(-)"
            else:
                delta_str = f"({delta})"
            line = f"{current_place}. {delta_str} {game_name}"
        else:
            line = f"{current_place}. {game_name}"
        response_lines.append(line)

    joined_response = '\n'.join(response_lines)
    await ctx.send(f"**Top 15 PS Games:**\n{joined_response}\n")

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
        
        def __init__(self):
            self.is_dummy_context = True # Add a flag to identify dummy context
            
    dummy_ctx = DummyContext()
    asyncio.run(gtsps_command(dummy_ctx, db)) # Test without game name
    # To test with a game name:
    # asyncio.run(gtsps_command(dummy_ctx, db, game_name="Spider-Man"))
