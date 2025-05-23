from datetime import datetime, timedelta
import asyncio
import random
import aiohttp
import discord
import re
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams
import io

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f'[LOG] {timestamp} - {message}')
    
ERROR_ID = 1162053416290361516 # Define your error channel ID

async def error_message(message: str, bot: discord.Client | None = None):
    """
    Logs an error message to the console and, if a bot object is provided,
    attempts to send it to a specific Discord channel.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[ERR] {timestamp} - {message}"
    print(log_entry) # Always print to console

    # Only attempt to send to Discord if bot object is provided
    if bot:
        try:
            # Attempt to get the channel and send the message
            error_channel = bot.get_channel(ERROR_ID)
            if error_channel and isinstance(error_channel, discord.TextChannel):
                 # Send the same message that was printed (or customize if needed)
                 # Ensure message length is within Discord limits (2000 chars)
                await error_channel.send(log_entry[:2000])
            elif error_channel:
                # Log locally if channel is wrong type
                print(f"[ERR] {timestamp} - Channel {ERROR_ID} is not a valid text channel.")
            else:
                # Log locally if channel not found
                print(f"[ERR] {timestamp} - Could not find error channel with ID: {ERROR_ID}")

        except discord.errors.Forbidden:
            print(f"[ERR] {timestamp} - Bot lacks permissions to send messages in channel {ERROR_ID}.")
        except Exception as e:
            # Catch any other exceptions during the Discord send process
            print(f"[ERR] {timestamp} - Failed to report error to Discord channel {ERROR_ID}: {type(e).__name__}: {e}")

def get_seconds_until(time_hour, time_minute):
    now = datetime.now()
    target_time = datetime(now.year, now.month, now.day, time_hour, time_minute)
    
    # If target time is in the past, calculate for the next day
    if now > target_time:
        target_time += timedelta(days=1)
        
    return int((target_time - now).total_seconds())

def aiohttp_retry(retries=5, base_delay=15.0, max_delay=120.0):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            nonlocal retries, base_delay, max_delay
            for attempt in range(retries):
                try:
                    return await func(*args, **kwargs)
                except (aiohttp.ClientError, aiohttp.ClientConnectorError, aiohttp.ClientConnectionError) as e:
                    if attempt == retries - 1:  # If this was the last retry
                        raise  # Re-raise the last exception
                    else:
                        # Calculate delay: base_delay * 2 ^ attempt, but with a random factor to avoid thundering herd problem
                        delay = min(max_delay, base_delay * 2 ** attempt) * (0.5 + random.random())
                        log_message(f'Awaiting {delay:.2f} seconds before retrying...')
                        await asyncio.sleep(delay)
            return await func(*args, **kwargs)  # Try one last time
        return wrapper
    return decorator

def normalize_game_name_for_search(text: str) -> str:
    text = text.lower()
    # Roman numerals → Arabic
    text = re.sub(r'\\bx\\b', '10', text)
    text = re.sub(r'\\bix\\b', '9', text)
    text = re.sub(r'\\bviii\\b', '8', text)
    text = re.sub(r'\\bvii\\b', '7', text)
    text = re.sub(r'\\bvi\\b', '6', text)
    text = re.sub(r'\\bv\\b', '5', text)
    text = re.sub(r'\\biv\\b', '4', text)
    text = re.sub(r'\\biii\\b', '3', text)
    text = re.sub(r'\\bii\\b', '2', text)
    # Hyphens → spaces
    text = text.replace('-', ' ')
    # Remove punctuation
    text = re.sub(r"[:!?'®™©]", "", text)
    # Collapse spaces
    return re.sub(r'\\s+', ' ', text).strip()

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
    placements = np.round(aggregated_data["placements"]).astype(int)
    
    # Set up plotting parameters.
    rcParams.update({'font.size': 7})
    plt.rcParams['font.family'] = ['sans-serif']
    plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']
    
    # Create a figure and a single axis.
    fig, ax = plt.subplots(figsize=(8, 4))
    
    # Plot the placements as a line plot with markers.
    ax.plot(positions, placements, marker='o', linestyle='-', color='#7289DA', markersize=3)
    ax.set_title(f"{game_name.upper()}, LAST QUARTER GTS PLACEMENTS (log)", fontsize=6, weight='bold', loc='left')
    
    # Process x-axis labels so that every tick is on two lines:
    # The first line shows "Year Month" and the second line shows the day.
    new_labels = []
    prev_year = None
    prev_month = None
    for label in aggregated_labels:
        try:
            dt = datetime.strptime(label, "%Y-%m-%d")
        except ValueError:
            new_labels.append(label) # Keep original label if parsing fails
            continue
        year = dt.strftime("%Y")
        month_abbr = dt.strftime("%b")
        day = str(dt.day)  # Remove any leading zero
        if prev_year is None or prev_month is None or year != prev_year or month_abbr != prev_month:
            new_label = f"{year} {month_abbr}\\n{day}"
        else:
            new_label = f"\\n{day}"
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
        if np.isfinite(y): # Check for finite values before annotating
            ax.text(x, y - 0.3, f"{y}", fontsize=6, ha='center', va='bottom')
    
    plt.tight_layout()
    
    # Save the plot to a BytesIO stream and create a discord.File.
    image_stream = io.BytesIO()
    fig.savefig(image_stream, format='png')
    image_stream.seek(0)
    plt.close(fig)
    
    discord_file = discord.File(fp=image_stream, filename="placements_plot.png")
    return image_stream, discord_file
