from datetime import datetime, timedelta
from general_utils import get_seconds_until, normalize_game_name_for_search, generate_gts_placements_plot
import aiohttp
from bs4 import BeautifulSoup
from database import Database
from general_utils import log_message, error_message, aiohttp_retry
from matplotlib import rcParams
from pipeline import BasePipeline
import os
import asyncio
import numpy as np
import matplotlib.pyplot as plt
import io
import discord
import re
import difflib

STEAM_API_KEY = os.getenv('STEAM_API_KEY')

@aiohttp_retry()
async def fetch_ccu(appid):
    url = f"http://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?key={STEAM_API_KEY}&appid={appid}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
    if data['response']['result'] == 1:
        ccu = data['response']['player_count']
    else:
        ccu = 0
    return ccu

def get_best_game_match(user_query, db):
    cursor = db.conn.execute("SELECT game_name FROM GameTranslation")
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
        word_matches.sort(key=lambda x: len(x[0]))
        return word_matches[0][1]

    # 2) prefix match
    prefix = [p for p in pairs if p[0].startswith(q)]
    if prefix:
        return min(prefix, key=lambda x: len(x[0]))[1]

    # 3) substring match
    substr = [p for p in pairs if q in p[0]]
    if substr:
        return min(substr, key=lambda x: len(x[0]))[1]

    # 4) difflib fallback
    names = [p[0] for p in pairs]
    close = difflib.get_close_matches(q, names, n=1, cutoff=0.75)
    if close:
        return next(orig for norm, orig in pairs if norm == close[0])

    return None


async def update_steam_top_sellers(db: Database, write_db: bool = True) -> list: # Changed dict to list
    # Phase 1: paginate and collect metadata (no CCU or DB writes)
    preliminary = []
    current_rank = 0
    latest_ts = db.get_latest_timestamp('SteamTopGames')
    latest_dt = None
    if latest_ts:
        latest_dt = datetime.strptime(latest_ts, '%Y-%m-%d %H')
    now_hour = datetime.now().replace(minute=0, second=0, microsecond=0)

    async with aiohttp.ClientSession() as session:
        for page in range(5):
            start = page * 100
            url = (
                "https://store.steampowered.com/search/results/"
                f"?query&start={start}&count=100&dynamic_data=&sort_by=_ASC"
                "&supportedlang=english&snr=1_7_7_globaltopsellers_7"
                "&filter=globaltopsellers&infinite=1"
            )
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    html = (await resp.json()).get("results_html", "")
            except Exception as e:
                error_message(f"Steam page {page+1} fetch error: {e}")
                if page < 4: await asyncio.sleep(0.1)
                continue

            soup = BeautifulSoup(html, 'html.parser')
            for d in soup.select('.search_result_row'):
                appid = d.get('data-ds-appid')
                if not appid:
                    continue
                title = d.select_one('.title').text.strip() if d.select_one('.title') else "Unknown"
                disc = d.select_one('.discount_pct')
                price = d.select_one('.discount_final_price, .search_price')
                discount = disc.text.strip() if disc else ("Free" if price and "free" in price.text.lower() else "")
                current_rank += 1
                preliminary.append({
                    'rank': current_rank,
                    'appid': appid,
                    'title': title,
                    'discount': discount
                })
            if page < 4:
                await asyncio.sleep(0.1)

    if not preliminary:
        log_message("No top-seller metadata fetched.")
        return []

    # Phase 2: update translations once per unique appid/title
    for appid, title in {(g['appid'], g['title']) for g in preliminary}:
        db.update_appid(appid, title)

    # Phase 3: fetch CCU concurrently
    appids = list({g['appid'] for g in preliminary})
    tasks = [fetch_ccu(a) for a in appids]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    ccu_map = {
        appid: (res if not isinstance(res, Exception) else 0)
        for appid, res in zip(appids, results)
    }

    # Phase 4: assemble final games list
    ts = datetime.now().strftime('%Y-%m-%d %H')
    games = [
        {
            'timestamp': ts,
            'count': g['rank'],
            'appid': g['appid'],
            'title': g['title'],
            'discount': g['discount'],
            'ccu': ccu_map.get(g['appid'], 0)
        }
        for g in preliminary
    ]

    # Phase 5: conditional DB insert
    if latest_dt and (now_hour - latest_dt) < timedelta(hours=1):
        log_message(f"Recent update at {latest_ts}, skipping DB write.")
        return games

    if games:
        if write_db:
            db.insert_bulk_data(games)
            log_message(f"Inserted {len(games)} SteamTopGames records.")
        else:
            log_message(f"Fetched {len(games)} SteamTopGames records (no DB write).")
    else:
        log_message("No games to insert after processing.")
    return games

async def gts_command(ctx, db, game_name: str = None):
    """
    If a game name is provided, the command will generate a graph
    showing the last month's GTS placements for that specific game.
    Otherwise, it displays the top 15 global sellers on Steam.
    """
    # If a game name is provided, try to generate a placements graph.
    if game_name is not None:
        matched_game_name = get_best_game_match(game_name, db)
        if matched_game_name:
            # Fetch data for the plot
            aggregated_data = db.get_last_month_placements(matched_game_name) # Changed method name
            if aggregated_data and aggregated_data.get("positions") and aggregated_data.get("placements"):
                image_stream, discord_file = generate_gts_placements_plot(aggregated_data, matched_game_name)
                await ctx.send(file=discord_file)
                return
            else:
                await ctx.send(f"Could not find enough data to generate a plot for '{matched_game_name}'.")
                return
        else:
            await ctx.send(f"Could not find a match for game: '{game_name}'.")
            return

    # No game name provided; display the standard top sellers list.
    def format_ccu(ccu):
        if ccu >= 1000:
            return f"{ccu // 1000}k"  # Integer division by 1000.
        else:
            return str(ccu)

    top_games = await update_steam_top_sellers(db, write_db=False)
    latest_timestamp = db.get_latest_timestamp('SteamTopGames')
    yesterday_games = db.get_yesterday_top_games(latest_timestamp)
    
    # Limit to the top 15 games.
    top_games = top_games[:15]
    response_lines = []
    
    for game in top_games:
        place = game['count']
        title = game['title']
        discount = game['discount']
        ccu = game['ccu']
        # Retrieve yesterday's placement (if available)
        place_yesterday = yesterday_games.get(game['appid'], 'N/A')
        place_yesterday = int(place_yesterday) if place_yesterday != 'N/A' else None
        
        # Calculate the placement change.
        place_delta = place_yesterday - place if place_yesterday is not None else None
        if place_delta is not None and place_delta > 0:
            place_delta_str = "+" + str(place_delta)
        elif place_delta is not None and place_delta == 0:
            place_delta_str = "-"
        else:
            place_delta_str = str(place_delta)
        
        # Build the line for the game.
        if place_yesterday is not None:
            line = f"{place}. ({place_delta_str}) {title}"
        else:
            line = f"{place}. {title}"
        
        if ccu:
            formated_ccu = format_ccu(ccu)
            line += f" (CCU: {formated_ccu})"
        
        response_lines.append(line)
    
    joined_response = '\n'.join(response_lines)
    
    await ctx.send(f"**Top 15 Global Sellers on Steam:**\n{joined_response}")
    
async def gts_weekly_command(ctx, db: Database):
    top_games = await update_steam_top_sellers(db, write_db=False)
    latest_timestamp = db.get_latest_timestamp('SteamTopGames')
    last_week_ranks = db.get_last_week_ranks(latest_timestamp, [game['appid'] for game in top_games[:25]])

    number_of_days = {}
    # Calculate the 7-day average rank for each game
    game_avg_ranks_7d = {}
    for appid, ranks in last_week_ranks.items():
        game_avg_ranks_7d[appid] = sum(ranks) / len(ranks)
        number_of_days[appid] = len(ranks)

    # Calculate the 3-day average rank for each game
    game_avg_ranks_3d = {}
    for appid, ranks in last_week_ranks.items():
        game_avg_ranks_3d[appid] = sum(ranks[-3:]) / min(len(ranks), 3)

    # Sort the games based on their 7-day average rank
    sorted_games = sorted(top_games, key=lambda game: game_avg_ranks_7d.get(game['appid'], float('inf')))

    response = []

    for game in sorted_games[:25]:
        # Construct the line for each game
        place = sorted_games.index(game) + 1
        title = game['title']
        discount = game['discount']
        ccu = game['ccu']

        avg_rank_7d = game_avg_ranks_7d.get(game['appid'], None)
        avg_rank_3d = game_avg_ranks_3d.get(game['appid'], None)

        if avg_rank_7d is not None and avg_rank_3d is not None:
            # Determine the trend symbol based on the comparison of 3-day and 7-day average ranks
            if abs(avg_rank_3d - avg_rank_7d) <= 1:
                trend_symbol = ':small_orange_diamond:'
            elif avg_rank_3d < avg_rank_7d:
                trend_symbol = ':small_red_triangle:'
            else:
                trend_symbol = ':small_red_triangle_down:'
                
            if number_of_days[game['appid']] < 7:
                trend_symbol = ':new:'

            line = f"{place}. {trend_symbol} {title}"
        else:
            line = f"{place}. {title}"


        response.append(line)

    joined_response = '\n'.join(response)
    await ctx.send(f"**Top 25 Global Sellers on Steam, last week average:**\n{joined_response}")
async def daily_steam_database_refresh(db: Database):
    while True:
        log_time = datetime.now()
        log_time += timedelta(seconds=get_seconds_until(21,0))
        log_message(f'Waiting until {log_time.strftime("%Y-%m-%d %H:%M")} to update Steam database.')
        await asyncio.sleep(get_seconds_until(21, 0))
        

        # Perform the database update
        await update_steam_top_sellers(db)
        print('Database updated with Steam top sellers.')
        

class SteamPipeline(BasePipeline):
    """Pipeline for scraping and storing Steam top sellers."""
    def __init__(self, db):
        # uses existing update_steam_top_sellers which handles DB writes
        super().__init__(name="steam", db=db)

    async def fetch(self):
        # fetch and store inside update_steam_top_sellers
        return await update_steam_top_sellers(self.db)

    async def store(self, items):
        # skip BasePipeline.store since update_steam_top_sellers already wrote to DB
        return items

if __name__ == "__main__":
    # Example usage (optional, for testing)
    # db = Database("steam_top_games.db")
    # class DummyContext:
    #     async def send(self, message=None, file=None):
    #         if message:
    #             print(message)
    #         if file:
    #             print(f"Sent file: {file.filename}")
    # dummy_ctx = DummyContext()
    # asyncio.run(gts_command(dummy_ctx, db, game_name="Some Game Name")) # Test with a game name
    # asyncio.run(gts_command(dummy_ctx, db)) # Test without a game name
    pass