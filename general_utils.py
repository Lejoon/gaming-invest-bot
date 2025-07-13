from datetime import datetime, timedelta
import asyncio
import random
import aiohttp
import discord
import re
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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

    # Define Roman numeral replacements in order to avoid conflicts (e.g., IX before X, VIII before V)
    # Using r'\broman\b' for word boundaries.
    roman_map = [
        (r'\bviii\b', '8'), # VIII before V, VII, II, I
        (r'\bvii\b', '7'),  # VII before V, II, I
        (r'\bvi\b', '6'),   # VI before V, I
        (r'\bix\b', '9'),   # IX before X, I
        (r'\biv\b', '4'),   # IV before V, I
        (r'\bx\b', '10'),   # X after IX
        (r'\bv\b', '5'),    # V after VIII, VII, VI, IV
        (r'\biii\b', '3'), # III before II, I
        (r'\bii\b', '2'),  # II before I
    ]

    for pattern, replacement in roman_map:
        text = re.sub(pattern, replacement, text) # Corrected: No longer need to replace \\b

    # Hyphens → spaces
    text = text.replace('-', ' ')
    # Remove specified punctuation
    text = re.sub(r"[:!?'®™©]", "", text)
    # Collapse multiple spaces to a single space and strip leading/trailing spaces
    text = re.sub(r'\\s+', ' ', text).strip() # Correcting \\s+ to \s+
    return text

def generate_gts_placements_plot(aggregated_data, game_name, is_steam=True):
    """
    Generates a plot showing the last month's GTS placements for a specific game.
    The aggregated_data dict is expected to contain:
      - "positions": a list or numpy array of numeric positions (e.g. day indices)
      - "aggregated_labels": a list of labels corresponding to each position (e.g. dates in "YYYY-MM-DD" format)
      - "placements": a list or numpy array of placement values (e.g. rank position per day)
    
    Args:
        aggregated_data: Dictionary containing the placement data
        game_name: Name of the game
        is_steam: True for Steam, False for PS Store (default: True)
    
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
    
    # Set title based on platform
    platform_name = "Steam" if is_steam else "PS Store"
    ax.set_title(f"{game_name.upper()}, LAST QUARTER {platform_name} PLACEMENTS (log)", fontsize=6, weight='bold', loc='left')
    
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

def generate_gts_placements_plot_with_minmax(aggregated_data, game_name):
    """
    Generates a plot showing min, max, and average game placements over time.
    Uses the same style as the original GTS plotting function.
    """
    if not all(k in aggregated_data for k in ["aggregated_labels", "avg_placements", "min_placements", "max_placements"]):
        return None

    aggregated_labels = aggregated_data["aggregated_labels"]
    avg_placements = np.round(aggregated_data["avg_placements"]).astype(int)
    min_placements = np.round(aggregated_data["min_placements"]).astype(int)
    max_placements = np.round(aggregated_data["max_placements"]).astype(int)
    
    positions = list(range(len(aggregated_labels)))

    # Set up plotting parameters - match original style
    rcParams.update({'font.size': 7})
    plt.rcParams['font.family'] = ['sans-serif']
    plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']
    
    # Create a figure and a single axis.
    fig, ax = plt.subplots(figsize=(8, 4))
    
    # Add the min/max fill area
    ax.fill_between(positions, min_placements, max_placements, color='#7289DA', alpha=0.3)
    
    # Plot the average placements as a line plot with markers - match original style
    ax.plot(positions, avg_placements, marker='o', linestyle='-', color='#7289DA', markersize=3)
    
    # Set title to match original style
    ax.set_title(f"{game_name.upper()}, LAST QUARTER STEAM PLACEMENTS (log scale)", fontsize=6, weight='bold', loc='left')
    
    # Process x-axis labels so that every tick is on two lines - match original
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
        day = str(dt.day)
        if prev_year is None or prev_month is None or year != prev_year or month_abbr != prev_month:
            new_label = f"{year} {month_abbr}\n{day}"
        else:
            new_label = f"\n{day}"
        new_labels.append(new_label)
        prev_year, prev_month = year, month_abbr

    ax.set_xticks(positions)
    ax.set_xticklabels(new_labels, fontsize=6)
    
    # Set the y-axis to a logarithmic scale and invert it
    ax.set_yscale('log')
    ax.invert_yaxis()
    
    # Format y-axis to show integers instead of scientific notation
    from matplotlib.ticker import ScalarFormatter, LogLocator
    formatter = ScalarFormatter()
    formatter.set_scientific(False)
    ax.yaxis.set_major_formatter(formatter)
    
    # Add more tick marks on y-axis - show more intermediate values
    ax.yaxis.set_major_locator(LogLocator(base=10, subs=[1, 2, 3, 4, 5, 6, 7, 8, 9]))
    ax.yaxis.set_minor_locator(LogLocator(base=10, subs=[1, 2, 3, 4, 5, 6, 7, 8, 9], numticks=50))
    
    # Add light grey dotted grid lines in background
    # Y-axis grid on all major ticks (light grey dotted lines)
    ax.grid(True, which='major', axis='y', linestyle=':', linewidth=0.5, color='lightgrey', alpha=0.6)
    
    # X-axis grid on 1st of each month (light grey dotted lines)
    # First, let's set up monthly grid for x-axis
    month_positions = []
    for i, label in enumerate(aggregated_labels):
        try:
            dt = datetime.strptime(label, "%Y-%m-%d")
            if dt.day == 1:  # First day of month
                month_positions.append(positions[i])
        except ValueError:
            continue
    
    # Add vertical grid lines at month boundaries
    for pos in month_positions:
        ax.axvline(x=pos, linestyle=':', linewidth=0.5, color='lightgrey', alpha=0.6)
    
    # Set grid lines to be in background
    ax.set_axisbelow(True)
    
    # Keep the y-axis visible and add label
    #ax.set_ylabel('Placement (log scale)', fontsize=8)
    
    # Hide only the top and right spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Annotate each data point with its (rounded) placement value - match original
    for x, y in zip(positions, avg_placements):
        if np.isfinite(y):
            ax.text(x, y - 0.3, f"{y}", fontsize=6, ha='center', va='bottom')
    
    plt.tight_layout()

    # Save to buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    
    return buf

def generate_comparison_placements_plot_delta_days(games_data, primary_game, comparison_game, days_before_release):
    """
    Generates a comparison plot showing average game placements over delta days to release.
    Uses the same style as generate_gts_placements_plot_with_minmax but without min/max bands.
    """
    if primary_game not in games_data or comparison_game not in games_data:
        print(f"Debug: Missing game data. Available: {list(games_data.keys())}")
        return None

    # Set up plotting parameters - match original style exactly
    rcParams.update({'font.size': 7})
    plt.rcParams['font.family'] = ['sans-serif']
    plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']

    # Create a figure and a single axis - match original size
    fig, ax = plt.subplots(figsize=(8, 4))
    
    # Plot primary game (Wuchang) in blue - match original style
    primary_data = games_data[primary_game]
    primary_delta_days = primary_data["delta_days"]
    primary_avg_placements = np.round(primary_data["avg_placements"]).astype(int)
    
    ax.plot(primary_delta_days, primary_avg_placements, marker='o', linestyle='-', 
            color='#7289DA', markersize=3)
    
    # Plot comparison game in red
    comparison_data = games_data[comparison_game]
    comparison_delta_days = comparison_data["delta_days"]
    comparison_avg_placements = np.round(comparison_data["avg_placements"]).astype(int)
    
    ax.plot(comparison_delta_days, comparison_avg_placements, marker='o', linestyle='-', 
            color='#DC143C', markersize=3)

    # Set title to match original style exactly
    ax.set_title(f"STEAM PLACEMENTS COMPARISON (log scale)\nDays to Release: -{days_before_release} to 0", 
                fontsize=6, weight='bold', loc='left')
    
    # X-axis: delta days - match original label style
    all_delta_days = sorted(set(primary_delta_days + comparison_delta_days))
    ax.set_xticks(all_delta_days)
    ax.set_xticklabels([str(d) for d in all_delta_days], fontsize=6)
    
    # Set the y-axis to a logarithmic scale and invert it - match original
    ax.set_yscale('log')
    ax.invert_yaxis()
    
    # Format y-axis to show integers instead of scientific notation - match original
    from matplotlib.ticker import ScalarFormatter, LogLocator
    formatter = ScalarFormatter()
    formatter.set_scientific(False)
    ax.yaxis.set_major_formatter(formatter)
    
    # Add more tick marks on y-axis - match original exactly
    ax.yaxis.set_major_locator(LogLocator(base=10, subs=[1, 2, 3, 4, 5, 6, 7, 8, 9]))
    ax.yaxis.set_minor_locator(LogLocator(base=10, subs=[1, 2, 3, 4, 5, 6, 7, 8, 9], numticks=50))
    
    # Add light grey dotted grid lines in background - match original
    ax.grid(True, which='major', axis='y', linestyle=':', linewidth=0.5, color='lightgrey', alpha=0.6)
    
    # Set grid lines to be in background - match original
    ax.set_axisbelow(True)
    
    # Hide only the top and right spines - match original
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Add Summer sale annotations (hardcoded as requested)
    ax.annotate('Summer sale start', 
                xy=(-27, 252), xytext=(-15, 400),
                arrowprops=dict(arrowstyle='->', color='black', lw=1),
                fontsize=6, color='black')
    
    ax.annotate('Summer sale end', 
                xy=(-14, 93), xytext=(-5, 150),
                arrowprops=dict(arrowstyle='->', color='black', lw=1),
                fontsize=6, color='black')
    
    # Annotate each data point with its placement value - match original style exactly
    for x, y in zip(primary_delta_days, primary_avg_placements):
        if np.isfinite(y):
            ax.text(x, y - 0.3, f"{y}", fontsize=6, ha='center', va='bottom')
    
    for x, y in zip(comparison_delta_days, comparison_avg_placements):
        if np.isfinite(y):
            ax.text(x, y - 0.3, f"{y}", fontsize=6, ha='center', va='bottom')
    
    plt.tight_layout()

    # Save to buffer - match original
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    
    return buf
