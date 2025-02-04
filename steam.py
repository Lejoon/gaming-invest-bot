from datetime import datetime, timedelta
from general_utils import get_seconds_until
import aiohttp
from bs4 import BeautifulSoup
import os
from database import Database
import asyncio
from general_utils import log_message, error_message, aiohttp_retry
import matplotlib.pyplot as plt
from matplotlib import rcParams
import discord
import io
import difflib
import numpy as np



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

def generate_gts_placements_plot(aggregated_data, game_name):
    """
    Generates a plot showing the last month's GTS placements for a specific game.
    The aggregated_data dict is expected to contain:
      - "positions": a list or numpy array of numeric positions (e.g. day indices)
      - "aggregated_labels": a list of labels corresponding to each position (e.g. dates in "YYYY-MM-DD" format)
      - "placements": a list or numpy array of placement values (e.g. rank position per day)
    
    The plot uses styling similar to generate_sales_plot.
    
    Returns:
        A tuple: (image_stream, discord_file) where discord_file is a discord.File
        ready for sending.
    """
    positions = aggregated_data["positions"]
    aggregated_labels = aggregated_data["aggregated_labels"]
    placements = aggregated_data["placements"]
    
    # Set up plotting parameters.
    rcParams.update({'font.size': 7})
    plt.rcParams['font.family'] = ['sans-serif']
    plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']
    
    # Create a figure and a single axis.
    fig, ax = plt.subplots(figsize=(8, 4))
    
    # Plot the placements as a line plot with markers.
    ax.plot(positions, placements, marker='o', linestyle='-', color='#7289DA', markersize=3)
    ax.set_title(f"{game_name.upper()}, LAST MONTH GTS PLACEMENTS (log)", fontsize=6, weight='bold', loc='left')
    
    # Process x-axis labels so that every tick is on two lines:
    # The first line shows "Year Month" and the second line shows the day.
    new_labels = []
    prev_year = None
    prev_month = None
    for label in aggregated_labels:
        try:
            dt = datetime.strptime(label, "%Y-%m-%d")
        except ValueError:
            new_labels.append(label)
            continue
        year = dt.strftime("%Y")
        month_abbr = dt.strftime("%b")
        day = str(dt.day)  # Remove any leading zero
        if prev_year is None or prev_month is None or year != prev_year or month_abbr != prev_month:
            new_label = f"{year} {month_abbr}\n{day}"
        else:
            new_label = f"\n{day}"
        new_labels.append(new_label)
        prev_year, prev_month = year, month_abbr

    ax.set_xticks(positions)
    ax.set_xticklabels(new_labels, fontsize=6)
    
    # Set the y-axis to a logarithmic scale and invert it so that lower numbers appear higher.
    ax.set_yscale('log')
    ax.invert_yaxis()
    
    # Remove the y-axis completely:
    ax.yaxis.set_visible(False)
    # Hide the left, top, and right spines (leave the bottom spine visible for the x-axis)
    ax.spines['left'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    
    # Annotate each data point with its (rounded) placement value.
    for x, y in zip(positions, placements):
        if np.isfinite(y):
            rounded_y = int(round(y))
            # With the y-axis inverted and on a log scale, subtract a small offset to place text above the marker.
            ax.text(x, y - 0.3, f"{rounded_y}", fontsize=6, ha='center', va='bottom')
    
    plt.tight_layout()
    
    # Save the plot to a BytesIO stream and create a discord.File.
    image_stream = io.BytesIO()
    fig.savefig(image_stream, format='png')
    image_stream.seek(0)
    plt.close(fig)
    
    discord_file = discord.File(fp=image_stream, filename="placements_plot.png")
    return image_stream, discord_file

def get_best_game_match(user_query, db):
    # Fetch all game names from the database.
    cursor = db.conn.execute("SELECT game_name FROM GameTranslation")

    game_names = [row[0] for row in cursor.fetchall()]

    # Use difflib to find the closest match.
    # n=1 means we only get the best match; cutoff=0.6 means only return matches with a score >= 0.6.
    matches = difflib.get_close_matches(user_query, game_names, n=1, cutoff=0.9)
    if matches:
        return matches[0]
    return None


async def update_steam_top_sellers(db: Database) -> dict:
    url = "https://store.steampowered.com/search/?filter=globaltopsellers"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()
    soup = BeautifulSoup(html, 'html.parser')
    
    games = []
    count = 0
    
    for game_div in soup.select('.search_result_row')[:50]:
        appid = game_div['data-ds-appid']
        title_elements = game_div.select('.title')
        price_elements = game_div.select('.discount_final_price, .search_price')
        discount_elements = game_div.select('.discount_pct, .search_price')

        title = title_elements[0].text if title_elements else "Unknown title"
        discount = discount_elements[0].text.strip() if discount_elements else ""
        price = price_elements[0].text.strip() if price_elements else ""
        
        if price == "Free":
            discount = "Free"
        
        count += 1
        
        # Check if the appid already exists in the translation table. If the appid doesn't exist, insert it into the translation table
        db.update_appid(appid, title)

        timestamp = datetime.now().strftime('%Y-%m-%d %H')
        
        # Fetch CCU using Steam API
        ccu = await fetch_ccu(appid)
        
        # Store the data in a dictionary and add it to the games list
        game_data = {
            'timestamp': timestamp,
            'count': count,
            'appid': appid,
            'title': title,
            'discount': discount,
            'ccu': ccu
        }
        games.append(game_data)
        
        # Fetch the latest timestamp from the database
    latest_timestamp = db.get_latest_timestamp('SteamTopGames')
    
    if latest_timestamp is not None:
        latest_timestamp = datetime.strptime(latest_timestamp, '%Y-%m-%d %H')

    # Get the current time (up to the hour)
    current_time = datetime.now().replace(minute=0, second=0, microsecond=0)

    # If there was an update within the last hour, don't update the database
    if latest_timestamp is not None and current_time - latest_timestamp < timedelta(hours=1):
        return games

    db.insert_bulk_data(games)
    
    return games

async def gts_command(ctx, db, game_name: str = None):
    """
    If a game name is provided, the command will generate a graph
    showing the last month's GTS placements for that specific game.
    Otherwise, it displays the top 15 global sellers on Steam.
    """
    # If a game name is provided, try to generate a placements graph.
    if game_name is not None:
        # Try to find the best match for the game name.
        best_match = get_best_game_match(game_name, db)
        if best_match is None:
            await ctx.send(f"No matching game found for '{game_name}'.")
            return

        aggregated_data = db.get_last_month_placements(best_match)
        if aggregated_data is None:
            await ctx.send(f"No placement data found for '{best_match}'.")
            return

        # Generate the placements plot using your plotting function.
        image_stream, discord_file = generate_gts_placements_plot(aggregated_data, best_match)
        await ctx.send(file=discord_file)
        return

    # No game name provided; display the standard top sellers list.
    def format_ccu(ccu):
        if ccu >= 1000:
            return f"{ccu // 1000}k"  # Integer division by 1000.
        else:
            return str(ccu)

    top_games = await update_steam_top_sellers(db)
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
    top_games = await update_steam_top_sellers(db)
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
        
