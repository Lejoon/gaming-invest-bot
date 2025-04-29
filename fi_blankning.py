from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import asyncio
import os
import pandas as pd
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from discord import Embed
from database import Database  # Assuming Database class is already defined
from general_utils import aiohttp_retry, log_message, error_message
import matplotlib.pyplot as plt
import io
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from matplotlib import rcParams
import discord # Added missing import potentially needed for delete_error_message

# Constants
URLS = {
    'DATA_AGG': 'https://www.fi.se/sv/vara-register/blankningsregistret/GetBlankningsregisterAggregat/',
    'DATA_ACT': 'https://www.fi.se/sv/vara-register/blankningsregistret/GetAktuellFile/',
    'TIMESTAMP': 'https://www.fi.se/sv/vara-register/blankningsregistret/'
}

FILE_PATHS = {'DATA_AGG': 'Blankningsregisteraggregat.ods',
              'DATA_ACT': 'AktuellaPositioner.ods',
              'TIMESTAMP': 'last_known_timestamp.txt'}
DELAY_TIME = 15*60
CHANNEL_ID = 1175019650963222599
ERROR_ID = 1162053416290361516
TRACKED_COMPANIES = set([
    'Embracer Group AB', 'Paradox Interactive AB (publ)', 'Starbreeze AB',
    'EG7', 'Enad Global 7', 'Maximum Entertainment', 'MAG Interactive',
    'G5 Entertainment AB (publ)', 'Modern Times Group MTG AB', 'Thunderful',
    'MGI - Media and Games Invest SE', 'Stillfront Group AB (publ)', 'Asmodee Group AB'
])

@asynccontextmanager
async def aiohttp_session():
    async with ClientSession() as session:
        yield session

@aiohttp_retry(retries=5, base_delay=5.0, max_delay=120.0)
async def fetch_url(session, url):
    async with session.get(url) as response:  # type: ClientResponse
        response.raise_for_status() # Raise an error for bad responses like 404 or 500
        content_type = response.headers.get('Content-Type', '')
        if 'text' in content_type or 'json' in content_type or 'xml' in content_type:
            return await response.text()
        else:
            return await response.read()

def read_last_known_timestamp(file_path):
    try:
        with open(file_path, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        return None

def write_last_known_timestamp(file_path, timestamp):
    with open(file_path, 'w') as f:
        f.write(timestamp)

@aiohttp_retry(retries=5, base_delay=5.0, max_delay=120.0)
async def fetch_last_update_time(session):
    content = await fetch_url(session, URLS['TIMESTAMP'])
    soup = BeautifulSoup(content, 'html.parser')
    timestamp_text = soup.find('p', string=lambda text: 'Listan uppdaterades:' in text if text else False)
    return timestamp_text.string.split(": ")[1] if timestamp_text else None

async def download_file(session, url, path):
    content = await fetch_url(session, url)
    with open(path, 'wb') as f:
        f.write(content)

async def read_aggregate_data(path, bot):
    try:
        df = pd.read_excel(path, sheet_name='Blad1', skiprows=5, engine="odf")
        os.remove(path)

        new_column_names = {
            df.columns[0]: 'company_name',
            df.columns[1]: 'lei',
            df.columns[2]: 'position_percent',
            df.columns[3]: 'latest_position_date'
        }
        df.rename(columns=new_column_names, inplace=True)

        if 'company_name' in df.columns:
            df['company_name'] = df['company_name'].str.strip()

        return df
    except Exception as e:
        await report_error_to_channel(bot, e)
        return pd.DataFrame() # Return empty DataFrame on error

async def read_current_data(path, bot): # Added bot parameter for consistency
    try:
        df = pd.read_excel(path, sheet_name='Blad1', skiprows=5, engine="odf")
        os.remove(path)

        new_column_names = {
            df.columns[0]: 'entity_name',
            df.columns[1]: 'issuer_name',
            df.columns[2]: 'isin',
            df.columns[3]: 'position_percent',
            df.columns[4]: 'position_date',
            df.columns[5]: 'comment'
        }
        df.rename(columns=new_column_names, inplace=True)

        df['issuer_name'] = df['issuer_name'].str.strip()
        df['entity_name'] = df['entity_name'].str.strip()

        return df
    except Exception as e:
        await report_error_to_channel(bot, e)
        return pd.DataFrame() # Return empty DataFrame on error


async def report_error_to_channel(bot, exception):
    error_channel = bot.get_channel(ERROR_ID)

    if error_channel:
        # Assuming error_message is defined in general_utils and sends to the error channel
        await error_message(f"An error occurred in fi_blankning: {type(exception).__name__}: {exception}", bot)

async def delete_error_message(message):
    if message:
        try:
            await message.delete()
        except discord.NotFound:
            pass  # Message already deleted

async def send_embed(old_agg_data, new_agg_data, old_act_data, new_act_data, db, fetched_timestamp, bot=None):
    if bot is not None:
        channel = bot.get_channel(CHANNEL_ID)
    else:
        channel = None # Ensure channel is None if bot is None

    agg_new_rows = await update_database_diff(old_agg_data, new_agg_data, db, fetched_timestamp)
    act_new_rows = await update_position_holders(old_act_data, new_act_data, db, fetched_timestamp)

    # Ensure dataframes are not None before iterating
    if agg_new_rows is None or act_new_rows is None:
         log_message("Could not generate embeds due to error in data update functions.")
         return

    for _, row in agg_new_rows.iterrows():
        company_name = row['company_name']
        new_position_percent = row['position_percent']
        lei = row['lei']
        timestamp = row['timestamp']

        if company_name in TRACKED_COMPANIES:
            old_position_data = old_agg_data[old_agg_data['company_name'] == company_name] # Use boolean indexing
            # Get the "last" known position percentage before this update
            old_position_percent = old_position_data['position_percent'].iloc[-1] if not old_position_data.empty else 0.0 # Default to 0.0
            change = new_position_percent - old_position_percent

            description = f"Ändrad aggregerad blankning: **{new_position_percent:.2f}%**" # Use .2f for formatting
            description += f" (Förändring: {change:+.2f}%)" # Use .2f for formatting

            # Filter act_new_rows for the current company *and* the current timestamp to get relevant changes
            issuer_data = act_new_rows[(act_new_rows['issuer_name'] == company_name) & (act_new_rows['timestamp'] == timestamp)]

            if not issuer_data.empty:
                holder_changes = []
                for _, holder_row in issuer_data.iterrows():
                    entity_name = holder_row['entity_name']
                    new_holder_percent = holder_row['position_percent']
                    time_holder_position = holder_row['position_date'] # Already formatted as string/date by pandas? Ensure consistency

                    # Find the previous position for this specific holder
                    old_holder_data = old_act_data[(old_act_data['entity_name'] == entity_name) & (old_act_data['issuer_name'] == company_name)]
                    old_holder_percent = old_holder_data.sort_values('timestamp')['position_percent'].iloc[-1] if not old_holder_data.empty else 0.0

                    holder_change = new_holder_percent - old_holder_percent

                    # Format position date if it's a Timestamp object
                    if isinstance(time_holder_position, pd.Timestamp):
                        time_holder_position_str = time_holder_position.strftime('%Y-%m-%d')
                    else:
                         time_holder_position_str = str(time_holder_position) # Assume it's already a string

                    if new_holder_percent < 0.5 and old_holder_percent >= 0.5: # Position dropped below threshold
                         holder_changes.append(f"*{entity_name}*: < 0.5% (var {old_holder_percent:.2f}%)")
                    elif new_holder_percent >= 0.5: # Position updated or newly above threshold
                         holder_changes.append(f"*{entity_name}*: {new_holder_percent:.2f}% ({holder_change:+.2f}%) den {time_holder_position_str}")
                    # Else: change happened below 0.5% threshold, not usually reported unless it crossed the line.

                if holder_changes:
                     description += "\n\n**Ändrade Positioner (>0.5%):**\n" + "\n".join(holder_changes)

            if channel: # Check if channel exists
                embed = Embed(
                    title=company_name,
                    description=description,
                    url=f"https://www.fi.se/sv/vara-register/blankningsregistret/emittent/?id={lei}",
                    timestamp=datetime.strptime(timestamp, "%Y-%m-%d %H:%M") # Ensure timestamp is string
                )
                embed.set_footer(text="FI Blankningsregister", icon_url="https://upload.wikimedia.org/wikipedia/en/thumb/a/aa/Financial_Supervisory_Authority_%28Sweden%29_logo.svg/320px-Financial_Supervisory_Authority_%28Sweden%29_logo.svg.png")

                await channel.send(embed=embed)
            else:
                print('--- Test Embedding ---')
                print(f"Title: {company_name}")
                print(description)
                print(f"URL: https://www.fi.se/sv/vara-register/blankningsregistret/emittent/?id={lei}")
                print(f"Timestamp: {timestamp}")
                print('----------------------')


async def update_position_holders(old_data, new_data, db, fetched_timestamp):
    """Compares old and new individual position data, updates database, and returns changes."""
    if new_data is None or new_data.empty:
        log_message("No new position holder data fetched or data is empty.")
        # Decide if we need to mark existing positions as potentially dropped (0.0)
        # For now, just return an empty dataframe if no new data.
        return pd.DataFrame() # Return empty DataFrame if new data is missing

    # Ensure necessary columns exist and handle potential type issues from SQL read
    required_cols = ['entity_name', 'issuer_name', 'isin', 'position_percent', 'timestamp', 'position_date']
    for col in required_cols:
        if col not in old_data.columns:
            old_data[col] = None # Add missing columns if needed
        if col not in new_data.columns:
             # This shouldn't happen if read_current_data worked, but as safeguard:
            log_message(f"Warning: Column {col} missing in new_data for position holders.")
            return pd.DataFrame()

    # Convert timestamp to datetime if needed (can be string from SQL)
    old_data['timestamp'] = pd.to_datetime(old_data['timestamp'], errors='coerce')
    # fetched_timestamp is already a string, format position_date if needed
    new_data['position_date'] = pd.to_datetime(new_data['position_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    old_data['position_date'] = pd.to_datetime(old_data['position_date'], errors='coerce').dt.strftime('%Y-%m-%d')

    # Add the current update timestamp to the new data
    new_data['timestamp'] = fetched_timestamp

    # Get the latest record for each position in the old data
    old_data = old_data.sort_values('timestamp').drop_duplicates(['entity_name', 'issuer_name', 'isin'], keep='last')
    # New data should already be the current state, but drop duplicates just in case
    new_data = new_data.drop_duplicates(['entity_name', 'issuer_name', 'isin'], keep='last') # No sort needed here

    # Identify positions present in new data but not in old data
    merge_cols = ['entity_name', 'issuer_name', 'isin']
    merged_new = pd.merge(new_data, old_data[merge_cols], on=merge_cols, how='left', indicator=True)
    # MODIFICATION: Added .copy()
    new_positions = merged_new[merged_new['_merge'] == 'left_only'][new_data.columns].copy()

    # Identify positions present in both, for checking changes
    merged_common = pd.merge(new_data, old_data, on=merge_cols, suffixes=('_new', '_old'))

    # Find common positions where the percentage actually changed
    # MODIFICATION: Added .copy() after filtering/column selection
    changed_positions_merged = merged_common[merged_common['position_percent_new'] != merged_common['position_percent_old']].copy()

    # Select and rename columns for the changed positions dataframe
    changed_positions = changed_positions_merged[['entity_name', 'issuer_name', 'isin', 'position_percent_new', 'position_date_new', 'timestamp_new']].copy() # Ensure we copy the final selection
    changed_positions.columns = ['entity_name', 'issuer_name', 'isin', 'position_percent', 'position_date', 'timestamp']


    # Identify positions present in old data but not in new data (dropped)
    merged_dropped = pd.merge(old_data, new_data[merge_cols], on=merge_cols, how='left', indicator=True)
    potential_dropped_positions = merged_dropped[merged_dropped['_merge'] == 'left_only'][old_data.columns].copy() # MODIFICATION: Added .copy()

    # Filter to only include positions that were actually above 0% before disappearing
    # And assign 0% and the current timestamp
    dropped_positions = potential_dropped_positions[potential_dropped_positions['position_percent'] > 0.0].copy() # MODIFICATION: Added .copy()

    if not dropped_positions.empty:
        # These assignments are now safe due to .copy() above
        dropped_positions['position_percent'] = 0.0
        dropped_positions['timestamp'] = fetched_timestamp
        # Keep original position_date as the last known date it existed? Or update? Using fetched_timestamp for consistency.
        # dropped_positions['position_date'] = pd.to_datetime(fetched_timestamp, format="%Y-%m-%d %H:%M").strftime('%Y-%m-%d') # Update date too? Or keep old? Let's keep old for now.
        # Select only necessary columns for insertion
        dropped_positions = dropped_positions[['entity_name', 'issuer_name', 'isin', 'position_percent', 'position_date', 'timestamp']]

    # Combine new, changed, and dropped positions
    # Ensure all parts have the same columns before concat
    cols_to_keep = ['entity_name', 'issuer_name', 'isin', 'position_percent', 'position_date', 'timestamp']
    new_rows = pd.concat([
        new_positions[cols_to_keep],
        changed_positions[cols_to_keep],
        dropped_positions[cols_to_keep] # Already filtered to cols_to_keep
    ], ignore_index=True)

    # Insert into database if there are changes
    if not new_rows.empty:
        try:
            db.insert_bulk_data(input=new_rows, table='PositionHolders')
            log_message(f"Inserted/Updated {len(new_rows)} rows in PositionHolders.")
        except Exception as e:
            await report_error_to_channel(bot, f"Database insert error (PositionHolders): {e}")
            return pd.DataFrame() # Return empty on DB error

    return new_rows


async def update_database_diff(old_data, new_data, db, fetched_timestamp):
    """Compares old and new aggregated position data, updates database, and returns changes."""
    if new_data is None or new_data.empty:
        log_message("No new aggregate data fetched or data is empty.")
        return pd.DataFrame()

    # Ensure necessary columns exist
    required_cols = ['company_name', 'lei', 'position_percent', 'timestamp', 'latest_position_date']
    for col in required_cols:
        if col not in old_data.columns:
            old_data[col] = None
        if col not in new_data.columns:
            log_message(f"Warning: Column {col} missing in new_data for aggregate positions.")
            return pd.DataFrame()

    # Convert timestamp to datetime if needed
    old_data['timestamp'] = pd.to_datetime(old_data['timestamp'], errors='coerce')
    # Format dates in new data
    new_data['latest_position_date'] = pd.to_datetime(new_data['latest_position_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    old_data['latest_position_date'] = pd.to_datetime(old_data['latest_position_date'], errors='coerce').dt.strftime('%Y-%m-%d')


    # Add the current update timestamp to the new data
    new_data['timestamp'] = fetched_timestamp

    # Get the latest record for each company in the old data
    old_data = old_data.sort_values('timestamp').drop_duplicates(['lei', 'company_name'], keep='last')
    # New data is already current state, drop duplicates just in case
    new_data = new_data.drop_duplicates(['lei', 'company_name'], keep='last')

    # Identify new LEIs (companies)
    merge_cols = ['lei', 'company_name']
    merged_new = pd.merge(new_data, old_data[merge_cols], on=merge_cols, how='left', indicator=True)
    # MODIFICATION: Added .copy()
    new_leis = merged_new[merged_new['_merge'] == 'left_only'][new_data.columns].copy()

    # Identify common LEIs to check for changes
    merged_common = pd.merge(new_data, old_data, on=merge_cols, suffixes=('_new', '_old'))

    # Find common companies where the position percentage changed
    # MODIFICATION: Added .copy() after filtering/column selection
    changed_positions_merged = merged_common[merged_common['position_percent_new'] != merged_common['position_percent_old']].copy()

    # Select and rename columns for the changed positions dataframe
    changed_positions = changed_positions_merged[['company_name', 'lei', 'position_percent_new', 'latest_position_date_new', 'timestamp_new']].copy() # Ensure we copy the final selection
    changed_positions.columns = ['company_name', 'lei', 'position_percent', 'latest_position_date', 'timestamp']

    # Combine new companies and changed positions
    # Ensure columns match before concat
    cols_to_keep = ['company_name', 'lei', 'position_percent', 'latest_position_date', 'timestamp']
    new_rows = pd.concat([
        new_leis[cols_to_keep],
        changed_positions[cols_to_keep]
        ], ignore_index=True)

    # Insert new and updated records if any exist
    if not new_rows.empty:
        try:
            db.insert_bulk_data(input=new_rows, table='ShortPositions')
            log_message(f"Inserted/Updated {len(new_rows)} rows in ShortPositions.")
        except Exception as e:
             await report_error_to_channel(bot, f"Database insert error (ShortPositions): {e}")
             return pd.DataFrame() # Return empty on DB error

    return new_rows

async def is_timestamp_updated(session):
    last_known_timestamp = read_last_known_timestamp(FILE_PATHS['TIMESTAMP'])
    web_timestamp = await fetch_last_update_time(session)

    # Handle case where timestamp couldn't be fetched
    if web_timestamp is None:
        log_message('Could not fetch web timestamp. Retrying later.')
        await asyncio.sleep(60) # Wait a minute before retrying fetch
        return None # Indicate no valid timestamp obtained

    # Retry logic if timestamp is default/invalid value
    while web_timestamp == "0001-01-01 00:00":
        next_update_time = datetime.now() + timedelta(seconds=30)
        log_message(f'Web timestamp unavailable (0001-01-01 00:00). Waiting until {next_update_time.strftime("%Y-%m-%d %H:%M")} to retry.')
        await asyncio.sleep(30)
        web_timestamp = await fetch_last_update_time(session)
        if web_timestamp is None:
            log_message('Could not fetch web timestamp after retry. Waiting for next cycle.')
            await asyncio.sleep(DELAY_TIME)
            return None # Indicate failure after retry

    next_update_time_log = datetime.now() + timedelta(seconds=DELAY_TIME)

    if web_timestamp == last_known_timestamp:
        log_message(f'Web timestamp unchanged ({web_timestamp}). Waiting until {next_update_time_log.strftime("%Y-%m-%d %H:%M")}.')
        await asyncio.sleep(DELAY_TIME)
        return None # Indicate no update

    # If timestamps differ, update the known timestamp and proceed
    write_last_known_timestamp(FILE_PATHS['TIMESTAMP'], web_timestamp)
    log_message(f'New web timestamp detected ({web_timestamp}). Checking for updates. Next check around {next_update_time_log.strftime("%Y-%m-%d %H:%M")}.')
    return web_timestamp # Return the new timestamp


async def plot_timeseries(daily_data, company_name):
    if daily_data.empty:
        return None # Cannot plot empty data

    # Ensure index is datetime
    daily_data.index = pd.to_datetime(daily_data.index)

    # Filter for the last 3 months from the latest data point available
    latest_date = daily_data.index.max()
    three_months_ago = latest_date - pd.DateOffset(months=3)
    plot_data = daily_data[daily_data.index >= three_months_ago].copy() # Use .copy()

    if plot_data.empty:
        return None # No data in the last 3 months

    plot_data.loc[:,'position_percent'] = plot_data['position_percent'] / 100 # Scaling down by 100

    # Adjusting figure size and setting a professional font
    plt.figure(figsize=(4, 2), dpi=150) # Increased DPI for clarity
    rcParams.update({'font.size': 7})  # Adjust font size
    plt.rcParams['font.family'] = ['sans-serif']
    plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']

    # Formatting the plot
    plt.plot(plot_data.index, plot_data['position_percent'], marker='.', linestyle='-', color='#7289DA', markersize=4, linewidth=1) # Adjusted style

    plt.title(f'{company_name} - Aggregerad Blankning (%) Senaste 3M'.upper(), fontsize=6, weight='bold', loc='left')
    plt.xlabel('')
    plt.ylabel('')  # Y-axis label removed

    # Set y-axis to display percentage
    plt.gca().yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=1))
    plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(nbins=5, prune='lower')) # Limit number of Y ticks

    # Improve date formatting on x-axis
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1)) # Tick every month
    plt.gca().xaxis.set_minor_locator(mdates.DayLocator(interval=7)) # Minor ticks weekly

    # Thin and transparent grid lines
    plt.grid(True, which='major', linestyle='-', linewidth=0.4, color='gray', alpha=0.3)

    # Remove plot outline (spines)
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['bottom'].set_visible(True) # Keep bottom spine
    plt.gca().spines['left'].set_visible(False)
    plt.gca().spines['bottom'].set_linewidth(0.5)
    plt.gca().spines['bottom'].set_color('gray')


    # Adjust tick size and padding
    plt.tick_params(axis='x', labelsize=6, pad=2, length=2, color='gray')
    plt.tick_params(axis='y', labelsize=6, pad=2, length=0) # No length for y ticks
    plt.tick_params(axis='x', which='minor', length=1, color='lightgray')


    # Set Y-axis limits dynamically, ensuring 0 is included unless all values are high
    min_val = plot_data['position_percent'].min()
    max_val = plot_data['position_percent'].max()
    plt.ylim(bottom=max(0, min_val - 0.005), top=max_val + 0.005) # Start at 0 or slightly below min, add padding top


    plt.tight_layout(pad=0.5) # Adjust padding

    # Generate image stream
    image_stream = io.BytesIO()
    plt.savefig(image_stream, format='png', bbox_inches='tight')
    image_stream.seek(0)
    plt.close() # Close the plot to free memory
    return image_stream

# Main asynchronous loop to update the database at intervals
@aiohttp_retry(retries=5, base_delay=15.0, max_delay=300.0) # Increased retry delays
async def update_fi_from_web(db: Database, bot): # Added type hints
    """Main loop to check FI website for updates and process data."""
    while True:
        try:
            async with aiohttp_session() as session:
                web_timestamp = await is_timestamp_updated(session)

                if not web_timestamp:
                    # is_timestamp_updated handles logging and delay if no update
                    continue # Skip to next iteration

                # Timestamp has changed, proceed with download and processing
                log_message("Downloading updated files...")
                await download_file(session, URLS['DATA_AGG'], FILE_PATHS['DATA_AGG'])
                await download_file(session, URLS['DATA_ACT'], FILE_PATHS['DATA_ACT'])
                log_message("Downloads complete.")

                # Read data
                new_data_agg = await read_aggregate_data(FILE_PATHS['DATA_AGG'], bot)
                new_data_act = await read_current_data(FILE_PATHS['DATA_ACT'], bot)

                if new_data_agg.empty or new_data_act.empty:
                     log_message("Failed to read one or both data files. Skipping update cycle.")
                     await asyncio.sleep(DELAY_TIME) # Wait before next check
                     continue

                # Fetch old data from DB
                log_message("Fetching old data from database...")
                # Use try-except for robustness during DB read
                try:
                    old_data_agg = pd.read_sql('SELECT * FROM ShortPositions ORDER BY timestamp', db.conn)
                    old_data_act = pd.read_sql('SELECT * FROM PositionHolders ORDER BY timestamp', db.conn)
                    log_message("Old data fetched.")
                except Exception as e:
                    await report_error_to_channel(bot, f"Failed to read from database: {e}")
                    await asyncio.sleep(DELAY_TIME)
                    continue

                # Process differences, update DB, and send embeds
                log_message("Processing data and sending updates...")
                await send_embed(old_data_agg, new_data_agg, old_data_act, new_data_act, db, web_timestamp, bot)

                log_message('Update cycle complete. Waiting for next check.')
                await asyncio.sleep(DELAY_TIME) # Wait for the next check interval

        except asyncio.CancelledError:
             log_message("Update loop cancelled.")
             break # Exit the loop if task is cancelled
        except Exception as e:
            # Catch broad exceptions in the main loop for resilience
            await report_error_to_channel(bot, f"Unhandled error in main update loop: {e}")
            log_message(f"Unhandled error occurred: {e}. Retrying after delay.")
            await asyncio.sleep(DELAY_TIME) # Wait before retrying the loop


async def manual_update(db, bot):
    """Performs a one-off manual update check and process."""
    log_message("--- Starting Manual Update ---")
    async with aiohttp_session() as session:
        try:
            # Fetch current timestamp to use if updated
            web_timestamp = await fetch_last_update_time(session)
            if not web_timestamp or web_timestamp == "0001-01-01 00:00":
                log_message("Manual update failed: Could not fetch valid web timestamp.")
                return

            log_message(f"Current web timestamp: {web_timestamp}")
            log_message("Downloading files for manual update...")
            # Use separate file paths for manual update to avoid conflicts? For now, overwrite.
            await download_file(session, URLS['DATA_AGG'], FILE_PATHS['DATA_AGG'])
            await download_file(session, URLS['DATA_ACT'], FILE_PATHS['DATA_ACT'])
            log_message("Downloads complete.")

            new_data_agg = await read_aggregate_data(FILE_PATHS['DATA_AGG'], bot)
            new_data_act = await read_current_data(FILE_PATHS['DATA_ACT'], bot)

            if new_data_agg.empty or new_data_act.empty:
                 log_message("Manual update failed: Error reading data files.")
                 return

            log_message("Fetching old data for manual update...")
            old_data_agg = pd.read_sql('SELECT * FROM ShortPositions ORDER BY timestamp', db.conn)
            old_data_act = pd.read_sql('SELECT * FROM PositionHolders ORDER BY timestamp', db.conn)
            log_message("Old data fetched.")

            # Update timestamp file only if the web timestamp is newer than the stored one
            last_known = read_last_known_timestamp(FILE_PATHS['TIMESTAMP'])
            if web_timestamp != last_known:
                 write_last_known_timestamp(FILE_PATHS['TIMESTAMP'], web_timestamp)
                 log_message(f"Updated last known timestamp to {web_timestamp}")


            log_message("Processing data and sending updates (manual)...")
            # Use the fetched web_timestamp for the update
            await send_embed(old_data_agg, new_data_agg, old_data_act, new_data_act, db, web_timestamp, bot)

            log_message('--- Manual update of database completed. ---')

        except Exception as e:
            await report_error_to_channel(bot, f"Error during manual update: {e}")
            log_message(f"Error during manual update: {e}")
    log_message("--- Finished Manual Update ---")


async def execute_query(db, query):
    # Consider adding error handling here
    try:
        # Assuming db.conn is a sqlite3 connection or similar
        cursor = db.conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()
    except Exception as e:
        # Log the error appropriately
        print(f"Database query error: {e}") # Simple print, replace with logging/reporting
        return None


def create_query_for_company_check(company_name_like):
    """Creates SQL query to check if a company exists (case-insensitive)."""
    # Use parameterization to prevent SQL injection if company_name_like comes from user input elsewhere
    # For this specific internal use, direct formatting might be okay, but parameterization is best practice.
    # Example with parameterization (assuming DB API supports it):
    # query = "SELECT company_name FROM ShortPositions WHERE LOWER(company_name) LIKE ? LIMIT 1"
    # params = ('%' + company_name_like.lower() + '%',)
    # return query, params
    # --- Using string formatting as in original ---
    return f"""
        SELECT company_name
        FROM ShortPositions
        WHERE LOWER(company_name) LIKE '%{company_name_like.lower()}%' COLLATE NOCASE
        ORDER BY timestamp DESC
        LIMIT 1
        """ # Added COLLATE NOCASE for robustness, ordering by timestamp to get latest match


async def create_timeseries(db, company_name):
    """Creates a daily time series of short positions for the last 3 months."""
    # Get the latest timestamp available in the database
    latest_db_timestamp_query = "SELECT MAX(timestamp) FROM ShortPositions"
    latest_db_timestamp_result = await execute_query(db, latest_db_timestamp_query)

    if not latest_db_timestamp_result or not latest_db_timestamp_result[0]:
        log_message(f"No timestamp data found for {company_name} in DB.")
        return pd.DataFrame() # Return empty DataFrame if no data

    latest_db_ts = pd.to_datetime(latest_db_timestamp_result[0])
    three_months_ago = latest_db_ts - pd.DateOffset(months=3)

    # Query data for the specific company within the date range
    # Use parameterization for safety if company_name could originate from user input
    query = f"""
        SELECT timestamp, position_percent
        FROM ShortPositions
        WHERE company_name = ?
        AND date(timestamp) >= date(?)
        AND date(timestamp) <= date(?)
        ORDER BY timestamp ASC
        """
    params = (company_name, three_months_ago.strftime('%Y-%m-%d'), latest_db_ts.strftime('%Y-%m-%d'))

    try:
        # Use pandas read_sql_query with parameters
        data = pd.read_sql_query(query, db.conn, params=params)
    except Exception as e:
        # Log error - replace print with proper logging/reporting
        print(f"Error querying timeseries data for {company_name}: {e}")
        return pd.DataFrame()


    if data.empty:
        log_message(f"No short position data found for {company_name} in the last 3 months.")
        return pd.DataFrame()

    # Convert the timestamp column to datetime
    data['timestamp'] = pd.to_datetime(data['timestamp'])

    # Set the timestamp column as the index
    data.set_index('timestamp', inplace=True)

    # Ensure data is sorted by index before resampling/filling
    data.sort_index(inplace=True)

    # Find the first date in the data to create the full range
    start_date = data.index.min()
    # Create a full date range from the start date to the latest db timestamp
    # Use latest_db_ts as end date to ensure plot extends to present if data exists
    date_range = pd.date_range(start=start_date, end=latest_db_ts, freq='D')


    # Reindex the data to the full daily range, then forward fill
    daily_data = data.reindex(date_range).ffill()

    # Filter out any rows that might be NaNs if the first day had no data originally
    daily_data.dropna(subset=['position_percent'], inplace=True)

    return daily_data


async def short_command(ctx, db, company_name_input):
    """Handles the Discord command to show short interest for a company."""
    company_name_like = company_name_input.strip() # Clean input
    if not company_name_like:
        await ctx.send("Ange ett företagsnamn.")
        return

    # Check if the company exists using LIKE and get the exact name
    check_query = create_query_for_company_check(company_name_like)
    result = await execute_query(db, check_query)

    if not result:
        await ctx.send(f"Kunde inte hitta data för ett företag som matchar '{company_name_input}'. Försök igen med ett exaktare namn.")
        return

    company_name_exact = result[0] # Get the correctly cased name from DB

    await ctx.send(f"Hämtar blankningsdata för **{company_name_exact}**...", delete_after=5.0) # Inform user

    # Create the time series data
    daily_data = await create_timeseries(db, company_name_exact)

    if daily_data.empty:
        # Try fetching the absolute latest value as fallback
        latest_query = f"SELECT position_percent, timestamp FROM ShortPositions WHERE company_name = ? ORDER BY timestamp DESC LIMIT 1"
        latest_result = await execute_query(db, latest_query)
        if latest_result:
             latest_perc, latest_ts = latest_result
             await ctx.send(f"**{company_name_exact}**: Senaste rapporterade aggregerade blankning är **{latest_perc:.2f}%** ({pd.to_datetime(latest_ts).strftime('%Y-%m-%d')}). Ingen historik för graf de senaste 3 månaderna.")
        else:
             await ctx.send(f"Ingen blankningsdata (aggregerad > 0.5%) hittades för **{company_name_exact}**.")
        return

    # Generate the plot
    image_stream = await plot_timeseries(daily_data, company_name_exact)

    if image_stream:
        latest_percentage = daily_data['position_percent'].iloc[-1]
        latest_date_str = daily_data.index[-1].strftime('%Y-%m-%d')

        message = (f"**{company_name_exact}**: Senaste aggregerade blankning är **{latest_percentage:.2f}%** "
                   f"(per {latest_date_str}). Se graf för de senaste 3 månaderna.")

        await ctx.send(message, file=discord.File(image_stream, filename=f'{company_name_exact}_shorts.png'))
    else:
        # Fallback if plot generation failed but data exists
        latest_percentage = daily_data['position_percent'].iloc[-1]
        latest_date_str = daily_data.index[-1].strftime('%Y-%m-%d')
        await ctx.send(f"**{company_name_exact}**: Senaste aggregerade blankning är **{latest_percentage:.2f}%** (per {latest_date_str}). Kunde inte generera graf.")
