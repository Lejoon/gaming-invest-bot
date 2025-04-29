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
    'MGI - Media and Games Invest SE', 'Stillfront Group AB (publ)'
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
    # Use current time as fallback? Or handle None more explicitly later?
    # For now, returning None if not found.
    return timestamp_text.string.split(": ")[1].strip() if timestamp_text and ": " in timestamp_text.string else None


async def download_file(session, url, path):
    content = await fetch_url(session, url)
    with open(path, 'wb') as f:
        f.write(content)

async def read_aggregate_data(path, bot):
    try:
        df = pd.read_excel(path, sheet_name='Blad1', skiprows=5, engine="odf")
        try: # Try removing file, but don't fail if it's already gone
             os.remove(path)
        except OSError:
             pass # Ignore error if file cannot be removed


        new_column_names = {
            df.columns[0]: 'company_name',
            df.columns[1]: 'lei',
            df.columns[2]: 'position_percent',
            df.columns[3]: 'latest_position_date'
        }
        df.rename(columns=new_column_names, inplace=True)

        # Convert percentage column to numeric, coercing errors
        df['position_percent'] = pd.to_numeric(df['position_percent'], errors='coerce')
        # Convert date column, coercing errors
        df['latest_position_date'] = pd.to_datetime(df['latest_position_date'], errors='coerce')


        if 'company_name' in df.columns:
            df['company_name'] = df['company_name'].str.strip()

        # Drop rows where essential info might be missing after coercion
        df.dropna(subset=['company_name', 'lei', 'position_percent', 'latest_position_date'], inplace=True)

        return df
    except Exception as e:
        await report_error_to_channel(bot, f"Error reading aggregate data file {path}: {e}")
        return pd.DataFrame() # Return empty DataFrame on error

async def read_current_data(path, bot): # Added bot parameter for consistency
    try:
        df = pd.read_excel(path, sheet_name='Blad1', skiprows=5, engine="odf")
        try: # Try removing file
             os.remove(path)
        except OSError:
             pass

        new_column_names = {
            df.columns[0]: 'entity_name',
            df.columns[1]: 'issuer_name',
            df.columns[2]: 'isin',
            df.columns[3]: 'position_percent',
            df.columns[4]: 'position_date',
            # Check if column 5 exists before assigning name
            df.columns[5] if len(df.columns) > 5 else 'comment_placeholder': 'comment'
        }
        # Only keep names for columns that actually exist
        valid_new_names = {k: v for k, v in new_column_names.items() if k in df.columns}
        df.rename(columns=valid_new_names, inplace=True)

        # Convert percentage column to numeric, coercing errors
        df['position_percent'] = pd.to_numeric(df['position_percent'], errors='coerce')
        # Convert date column, coercing errors
        df['position_date'] = pd.to_datetime(df['position_date'], errors='coerce')

        # Clean string columns if they exist
        if 'issuer_name' in df.columns:
             df['issuer_name'] = df['issuer_name'].str.strip()
        if 'entity_name' in df.columns:
             df['entity_name'] = df['entity_name'].str.strip()

        # Drop rows where essential info might be missing after coercion
        df.dropna(subset=['entity_name', 'issuer_name', 'isin', 'position_percent', 'position_date'], inplace=True)

        return df
    except Exception as e:
        await report_error_to_channel(bot, f"Error reading current data file {path}: {e}")
        return pd.DataFrame() # Return empty DataFrame on error


async def report_error_to_channel(bot, exception_message): # Pass message directly
    error_channel = bot.get_channel(ERROR_ID)

    if error_channel:
        # Assuming error_message is defined in general_utils and sends to the error channel
        # Ensure the message doesn't exceed Discord limits
        full_message = f"An error occurred in fi_blankning: {exception_message}"
        await error_message(full_message[:1990], bot) # Truncate if too long

async def delete_error_message(message):
    if message:
        try:
            await message.delete()
        except discord.NotFound:
            pass  # Message already deleted
        except discord.Forbidden:
             log_message("Bot lacks permissions to delete messages.") # Log permission issue

async def send_embed(old_agg_data, new_agg_data, old_act_data, new_act_data, db, fetched_timestamp, bot=None):
    if bot is not None:
        channel = bot.get_channel(CHANNEL_ID)
    else:
        channel = None # Ensure channel is None if bot is None

    agg_new_rows = await update_database_diff(old_agg_data, new_agg_data, db, fetched_timestamp, bot) # Pass bot for error reporting
    act_new_rows = await update_position_holders(old_act_data, new_act_data, db, fetched_timestamp, bot) # Pass bot for error reporting

    # Ensure dataframes are not None before iterating
    if agg_new_rows is None or act_new_rows is None:
         log_message("Could not generate embeds due to error in data update functions.")
         return

    for _, row in agg_new_rows.iterrows():
        company_name = row['company_name']
        new_position_percent = row['position_percent']
        lei = row['lei']
        timestamp_str = row['timestamp'] # Already a string "YYYY-MM-DD HH:MM"

        if company_name in TRACKED_COMPANIES:
            old_position_data = old_agg_data[old_agg_data['company_name'] == company_name] # Use boolean indexing
            # Get the "last" known position percentage before this update
            old_position_percent = old_position_data['position_percent'].iloc[-1] if not old_position_data.empty else 0.0 # Default to 0.0
            change = new_position_percent - old_position_percent

            description = f"Ändrad aggregerad blankning: **{new_position_percent:.2f}%**" # Use .2f for formatting
            description += f" (Förändring: {change:+.2f}%)" # Use .2f for formatting

            # Filter act_new_rows for the current company *and* the current timestamp to get relevant changes for this update cycle
            issuer_data = act_new_rows[(act_new_rows['issuer_name'] == company_name) & (act_new_rows['timestamp'] == timestamp_str)]

            if not issuer_data.empty:
                holder_changes = []
                for _, holder_row in issuer_data.iterrows():
                    entity_name = holder_row['entity_name']
                    new_holder_percent = holder_row['position_percent']
                    # Ensure position_date is a string 'YYYY-MM-DD'
                    time_holder_position_obj = pd.to_datetime(holder_row['position_date'], errors='coerce')
                    time_holder_position_str = time_holder_position_obj.strftime('%Y-%m-%d') if pd.notna(time_holder_position_obj) else "Okänt datum"


                    # Find the previous position for this specific holder in the old data
                    old_holder_data = old_act_data[(old_act_data['entity_name'] == entity_name) & (old_act_data['issuer_name'] == company_name)]
                    # Get the latest position percent before this update cycle
                    old_holder_percent = old_holder_data.sort_values('timestamp')['position_percent'].iloc[-1] if not old_holder_data.empty else 0.0

                    holder_change = new_holder_percent - old_holder_percent

                    if new_holder_percent < 0.5 and old_holder_percent >= 0.5: # Position dropped below threshold
                         holder_changes.append(f"*{entity_name}*: < 0.5% (var {old_holder_percent:.2f}%)")
                    elif new_holder_percent >= 0.5: # Position updated or newly above threshold
                         # Only report if change is significant (e.g., > 0.01) or crossing threshold? For now, report all changes >= 0.5
                         # if abs(holder_change) > 0.001: # Add threshold if desired
                         holder_changes.append(f"*{entity_name}*: {new_holder_percent:.2f}% ({holder_change:+.2f}%) den {time_holder_position_str}")
                    # Else: change happened below 0.5% threshold, not usually reported unless it crossed the line.

                if holder_changes:
                     description += "\n\n**Ändrade Positioner (>0.5%):**\n" + "\n".join(holder_changes)

            if channel: # Check if channel exists
                try:
                    # Ensure timestamp string can be parsed; use current time as fallback
                    try:
                        embed_timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")
                    except (ValueError, TypeError):
                         log_message(f"Could not parse fetched timestamp '{timestamp_str}', using current time for embed.")
                         embed_timestamp = datetime.now()

                    embed = Embed(
                        title=company_name,
                        description=description,
                        url=f"https://www.fi.se/sv/vara-register/blankningsregistret/emittent/?id={lei}",
                        timestamp=embed_timestamp
                    )
                    embed.set_footer(text="FI Blankningsregister", icon_url="https://upload.wikimedia.org/wikipedia/en/thumb/a/aa/Financial_Supervisory_Authority_%28Sweden%29_logo.svg/320px-Financial_Supervisory_Authority_%28Sweden%29_logo.svg.png")

                    await channel.send(embed=embed)
                except discord.Forbidden:
                     log_message(f"Bot lacks permissions to send messages in channel {CHANNEL_ID}")
                except Exception as e:
                     log_message(f"Error sending embed for {company_name}: {e}")
            else:
                # Fallback print for testing when bot/channel is None
                print('--- Test Embedding ---')
                print(f"Title: {company_name}")
                print(description)
                print(f"URL: https://www.fi.se/sv/vara-register/blankningsregistret/emittent/?id={lei}")
                print(f"Timestamp: {timestamp_str}")
                print('----------------------')


# ** CORRECTED update_position_holders **
async def update_position_holders(old_data, new_data, db, fetched_timestamp, bot): # Added bot
    """Compares old and new individual position data, updates database, and returns changes."""
    if new_data is None or new_data.empty:
        log_message("No new position holder data fetched or data is empty.")
        return pd.DataFrame() # Return empty DataFrame if new data is missing

    # Define columns expected AFTER reading the file (excluding timestamp)
    required_cols_from_file = ['entity_name', 'issuer_name', 'isin', 'position_percent', 'position_date']
    # Define columns needed overall, including those added later or potentially missing from old DB data
    all_expected_cols = ['entity_name', 'issuer_name', 'isin', 'position_percent', 'position_date', 'timestamp'] # Added timestamp here

    # --- Validate and Prepare new_data ---
    for col in required_cols_from_file:
        if col not in new_data.columns:
            log_message(f"CRITICAL: Column {col} missing in new_data from file read. Aborting update for PositionHolders.")
            await report_error_to_channel(bot, f"Column {col} missing in new position holder data file.")
            return pd.DataFrame()
    # Ensure correct types from file read
    new_data['position_date'] = pd.to_datetime(new_data['position_date'], errors='coerce')
    new_data['position_percent'] = pd.to_numeric(new_data['position_percent'], errors='coerce')
    # Add the timestamp from this update cycle
    new_data['timestamp'] = fetched_timestamp # This is a string "YYYY-MM-DD HH:MM"

    # --- Validate and Prepare old_data ---
    for col in all_expected_cols:
        if col not in old_data.columns:
            log_message(f"Warning: Column {col} missing in old_data for position holders. Adding default.")
            # Add potentially missing columns to old_data to prevent errors
            if 'date' in col or 'timestamp' in col: # Handle both date and timestamp
                old_data[col] = pd.NaT
            elif 'percent' in col:
                old_data[col] = 0.0
            else:
                old_data[col] = '' # Add as empty string or appropriate default
    # Ensure correct types from DB read
    old_data['timestamp'] = pd.to_datetime(old_data['timestamp'], errors='coerce') # Convert timestamp to datetime for comparison/sorting
    old_data['position_date'] = pd.to_datetime(old_data['position_date'], errors='coerce')
    old_data['position_percent'] = pd.to_numeric(old_data['position_percent'], errors='coerce')


    # Convert dates to strings for consistent comparison after ensuring datetime type
    new_data['position_date_str'] = new_data['position_date'].dt.strftime('%Y-%m-%d')
    old_data['position_date_str'] = old_data['position_date'].dt.strftime('%Y-%m-%d')


    # Get the latest record for each position in the old data based on datetime timestamp
    old_data = old_data.sort_values('timestamp').drop_duplicates(['entity_name', 'issuer_name', 'isin'], keep='last')
    # New data should already be the current state, but drop duplicates just in case
    new_data = new_data.drop_duplicates(['entity_name', 'issuer_name', 'isin'], keep='last') # No sort needed here


    # --- Identify Changes ---
    merge_cols = ['entity_name', 'issuer_name', 'isin']

    # Identify positions present in new data but not in old data
    merged_new = pd.merge(new_data, old_data[merge_cols + ['position_percent']], on=merge_cols, how='left', indicator=True, suffixes=('_new', '_old'))
    # MODIFICATION: Added .copy()
    new_positions = merged_new[merged_new['_merge'] == 'left_only'][new_data.columns].copy()


    # Identify positions present in both, for checking changes
    # Use the string date for comparison here if needed, or compare numeric percentage directly
    merged_common = pd.merge(new_data, old_data, on=merge_cols, suffixes=('_new', '_old'))


    # Find common positions where the percentage actually changed (handle potential float precision)
    merged_common['perc_diff'] = (merged_common['position_percent_new'] - merged_common['position_percent_old']).abs()
    # Consider a position changed if the difference is > tiny tolerance OR if the date changed
    changed_positions_merged = merged_common[
         (merged_common['perc_diff'] > 0.0001) | # Check for percentage change
         (merged_common['position_date_str_new'] != merged_common['position_date_str_old']) # Check for date change
        ].copy() # MODIFICATION: Added .copy()

    # Select and rename columns for the changed positions dataframe
    # Use the _new columns as they represent the latest state
    changed_positions = changed_positions_merged[
        ['entity_name', 'issuer_name', 'isin', 'position_percent_new', 'position_date_new', 'timestamp_new']
        ].copy() # Ensure we copy the final selection
    changed_positions.columns = ['entity_name', 'issuer_name', 'isin', 'position_percent', 'position_date', 'timestamp']


    # Identify positions present in old data but not in new data (dropped)
    merged_dropped = pd.merge(old_data, new_data[merge_cols], on=merge_cols, how='left', indicator=True)
    potential_dropped_positions = merged_dropped[merged_dropped['_merge'] == 'left_only'][old_data.columns].copy() # MODIFICATION: Added .copy()

    # Filter to only include positions that were actually >= 0.5% threshold before disappearing? Or just > 0? Let's stick to > 0 for now.
    # And assign 0% and the current timestamp
    dropped_positions = potential_dropped_positions[potential_dropped_positions['position_percent'] > 0.0].copy() # MODIFICATION: Added .copy()


    if not dropped_positions.empty:
        # These assignments are now safe due to .copy() above
        dropped_positions['position_percent'] = 0.0
        dropped_positions['timestamp'] = fetched_timestamp # Use the current update cycle timestamp (string)
        # Keep original position_date? Convert to string here for consistency if needed.
        dropped_positions['position_date'] = dropped_positions['position_date'].dt.strftime('%Y-%m-%d')

        # Select only necessary columns for insertion
        dropped_positions = dropped_positions[['entity_name', 'issuer_name', 'isin', 'position_percent', 'position_date', 'timestamp']]


    # --- Combine and Insert ---
    # Convert position_date back to string for final combined dataframe consistency before DB insert
    new_positions['position_date'] = new_positions['position_date'].dt.strftime('%Y-%m-%d')
    changed_positions['position_date'] = changed_positions['position_date'].dt.strftime('%Y-%m-%d')
    # dropped_positions['position_date'] is already string


    cols_to_insert = ['entity_name', 'issuer_name', 'isin', 'position_percent', 'position_date', 'timestamp']
    # Ensure all parts have the required columns before concat
    new_rows = pd.concat([
        new_positions[cols_to_insert],
        changed_positions[cols_to_insert],
        dropped_positions[cols_to_insert] # Already filtered to cols_to_insert? Double check.
    ], ignore_index=True).round({'position_percent': 5}) # Round percentage for consistency


    # Insert into database if there are changes
    if not new_rows.empty:
        try:
            # Ensure position_date is string before insert if DB expects it
            # It should be string from strftime above
            db.insert_bulk_data(input=new_rows, table='PositionHolders')
            log_message(f"Inserted/Updated {len(new_rows)} rows in PositionHolders.")
        except Exception as e:
            await report_error_to_channel(bot, f"Database insert error (PositionHolders): {e}")
            return pd.DataFrame() # Return empty on DB error

    return new_rows


# ** CORRECTED update_database_diff **
async def update_database_diff(old_data, new_data, db, fetched_timestamp, bot): # Added bot
    """Compares old and new aggregated position data, updates database, and returns changes."""
    if new_data is None or new_data.empty:
        log_message("No new aggregate data fetched or data is empty.")
        return pd.DataFrame()

    # Define columns expected AFTER reading the file (excluding timestamp)
    required_cols_from_file = ['company_name', 'lei', 'position_percent', 'latest_position_date']
    # Define columns needed overall
    all_expected_cols = ['company_name', 'lei', 'position_percent', 'latest_position_date', 'timestamp']

    # --- Validate and Prepare new_data ---
    for col in required_cols_from_file:
        if col not in new_data.columns:
            log_message(f"CRITICAL: Column {col} missing in new_data from aggregate file read. Aborting update for ShortPositions.")
            await report_error_to_channel(bot, f"Column {col} missing in new aggregate data file.")
            return pd.DataFrame()
    # Ensure correct types from file read
    new_data['latest_position_date'] = pd.to_datetime(new_data['latest_position_date'], errors='coerce')
    new_data['position_percent'] = pd.to_numeric(new_data['position_percent'], errors='coerce')
    # Add the timestamp from this update cycle
    new_data['timestamp'] = fetched_timestamp # String "YYYY-MM-DD HH:MM"

    # --- Validate and Prepare old_data ---
    for col in all_expected_cols:
        if col not in old_data.columns:
            log_message(f"Warning: Column {col} missing in old_data for aggregate positions. Adding default.")
            if 'date' in col or 'timestamp' in col:
                old_data[col] = pd.NaT
            elif 'percent' in col:
                old_data[col] = 0.0
            else:
                old_data[col] = ''
    # Ensure correct types from DB read
    old_data['timestamp'] = pd.to_datetime(old_data['timestamp'], errors='coerce') # Convert to datetime for comparison/sorting
    old_data['latest_position_date'] = pd.to_datetime(old_data['latest_position_date'], errors='coerce')
    old_data['position_percent'] = pd.to_numeric(old_data['position_percent'], errors='coerce')

    # Convert dates to strings for consistent comparison after ensuring datetime type
    new_data['latest_position_date_str'] = new_data['latest_position_date'].dt.strftime('%Y-%m-%d')
    old_data['latest_position_date_str'] = old_data['latest_position_date'].dt.strftime('%Y-%m-%d')


    # Get the latest record for each company in the old data based on datetime timestamp
    old_data = old_data.sort_values('timestamp').drop_duplicates(['lei', 'company_name'], keep='last')
    # New data is already current state, drop duplicates just in case
    new_data = new_data.drop_duplicates(['lei', 'company_name'], keep='last')

    # --- Identify Changes ---
    merge_cols = ['lei', 'company_name']

    # Identify new LEIs (companies)
    merged_new = pd.merge(new_data, old_data[merge_cols + ['position_percent']], on=merge_cols, how='left', indicator=True, suffixes=('_new', '_old'))
    # MODIFICATION: Added .copy()
    new_leis = merged_new[merged_new['_merge'] == 'left_only'][new_data.columns].copy()

    # Identify common LEIs to check for changes
    merged_common = pd.merge(new_data, old_data, on=merge_cols, suffixes=('_new', '_old'))

    # Find common companies where the position percentage changed (handle potential float precision)
    merged_common['perc_diff'] = (merged_common['position_percent_new'] - merged_common['position_percent_old']).abs()
    # Consider changed if percentage difference > tolerance OR date changed
    changed_positions_merged = merged_common[
        (merged_common['perc_diff'] > 0.0001) |
        (merged_common['latest_position_date_str_new'] != merged_common['latest_position_date_str_old'])
        ].copy() # MODIFICATION: Added .copy()


    # Select and rename columns for the changed positions dataframe
    # Use _new columns as they represent the latest state
    changed_positions = changed_positions_merged[
        ['company_name', 'lei', 'position_percent_new', 'latest_position_date_new', 'timestamp_new']
        ].copy() # Ensure we copy the final selection
    changed_positions.columns = ['company_name', 'lei', 'position_percent', 'latest_position_date', 'timestamp']

    # --- Combine and Insert ---
    # Convert date back to string before concat/insert
    new_leis['latest_position_date'] = new_leis['latest_position_date'].dt.strftime('%Y-%m-%d')
    changed_positions['latest_position_date'] = changed_positions['latest_position_date'].dt.strftime('%Y-%m-%d')


    cols_to_insert = ['company_name', 'lei', 'position_percent', 'latest_position_date', 'timestamp']
    new_rows = pd.concat([
        new_leis[cols_to_insert],
        changed_positions[cols_to_insert]
        ], ignore_index=True).round({'position_percent': 5}) # Round percentage

    # Insert new and updated records if any exist
    if not new_rows.empty:
        try:
            # Ensure latest_position_date is string
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
            # Don't sleep for DELAY_TIME here, let the main loop handle the delay
            return None # Indicate failure after retry

    next_update_time_log = datetime.now() + timedelta(seconds=DELAY_TIME)

    if web_timestamp == last_known_timestamp:
        # Only log verbosely occasionally, e.g., every hour or few checks
        # This avoids flooding logs when no updates happen for a long time.
        # Simple check: log only if minutes % 60 == 0 (roughly hourly)
        if datetime.now().minute % 60 == 0:
             log_message(f'Web timestamp unchanged ({web_timestamp}). Next check around {next_update_time_log.strftime("%Y-%m-%d %H:%M")}.')
        # No need to await asyncio.sleep here, the caller loop handles the main delay
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
    # Ensure max_val > min_val before setting limits
    if max_val <= min_val:
         max_val = min_val + 0.01 # Add small buffer if only one value

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
        next_check_time = datetime.now() + timedelta(seconds=DELAY_TIME)
        try:
            async with aiohttp_session() as session:
                web_timestamp = await is_timestamp_updated(session)

                if not web_timestamp:
                    # is_timestamp_updated handles logging if needed
                    # Wait for the calculated delay time before the next check
                    await asyncio.sleep(DELAY_TIME)
                    continue # Skip to next iteration

                # Timestamp has changed, proceed with download and processing
                log_message("Downloading updated files...")
                await download_file(session, URLS['DATA_AGG'], FILE_PATHS['DATA_AGG'])
                await download_file(session, URLS['DATA_ACT'], FILE_PATHS['DATA_ACT'])
                log_message("Downloads complete.")

                # Read data
                new_data_agg = await read_aggregate_data(FILE_PATHS['DATA_AGG'], bot)
                new_data_act = await read_current_data(FILE_PATHS['DATA_ACT'], bot)

                # Check if reading failed (returned empty DataFrame)
                if new_data_agg.empty or new_data_act.empty:
                     log_message("Failed to read one or both data files. Skipping update cycle.")
                     # Wait before next check even on read failure
                     await asyncio.sleep(max(0, (next_check_time - datetime.now()).total_seconds()))
                     continue

                # Fetch old data from DB
                log_message("Fetching old data from database...")
                # Use try-except for robustness during DB read
                try:
                    # Order by timestamp helps in update logic if needed, but primary key constraint should handle uniqueness
                    old_data_agg = pd.read_sql('SELECT * FROM ShortPositions', db.conn)
                    old_data_act = pd.read_sql('SELECT * FROM PositionHolders', db.conn)
                    log_message("Old data fetched.")
                except Exception as e:
                    await report_error_to_channel(bot, f"Failed to read from database: {e}")
                    await asyncio.sleep(max(0, (next_check_time - datetime.now()).total_seconds()))
                    continue

                # Process differences, update DB, and send embeds
                log_message("Processing data and sending updates...")
                await send_embed(old_data_agg, new_data_agg, old_data_act, new_data_act, db, web_timestamp, bot)

                log_message(f'Update cycle complete. Waiting until approx {next_check_time.strftime("%Y-%m-%d %H:%M:%S")} for next check.')
                # Wait until the calculated next check time
                await asyncio.sleep(max(0, (next_check_time - datetime.now()).total_seconds()))


        except asyncio.CancelledError:
             log_message("Update loop cancelled.")
             break # Exit the loop if task is cancelled
        except Exception as e:
            # Catch broad exceptions in the main loop for resilience
            await report_error_to_channel(bot, f"Unhandled error in main update loop: {e}")
            log_message(f"Unhandled error occurred: {e}. Waiting until {next_check_time.strftime('%Y-%m-%d %H:%M:%S')} to retry.")
            # Wait before retrying the loop
            await asyncio.sleep(max(0, (next_check_time - datetime.now()).total_seconds()))


async def manual_update(db, bot):
    """Performs a one-off manual update check and process."""
    log_message("--- Starting Manual Update ---")
    async with aiohttp_session() as session:
        try:
            # Fetch current timestamp to use if updated
            web_timestamp = await fetch_last_update_time(session)
            if not web_timestamp or web_timestamp == "0001-01-01 00:00":
                log_message("Manual update failed: Could not fetch valid web timestamp.")
                await ctx.send("Manual update failed: Could not fetch valid web timestamp.") # Inform user if called from ctx
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
                 await ctx.send("Manual update failed: Error reading data files.") # Inform user if called from ctx
                 return

            log_message("Fetching old data for manual update...")
            old_data_agg = pd.read_sql('SELECT * FROM ShortPositions', db.conn)
            old_data_act = pd.read_sql('SELECT * FROM PositionHolders', db.conn)
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
            # Maybe send confirmation to Discord? Depends on how it's triggered.

        except Exception as e:
            await report_error_to_channel(bot, f"Error during manual update: {e}")
            log_message(f"Error during manual update: {e}")
            # Maybe send error to Discord? Depends on context.
    log_message("--- Finished Manual Update ---")


async def execute_query(db, query, params=None): # Added params
    # Consider adding error handling here
    cursor = None # Initialize cursor to None
    try:
        # Assuming db.conn is a sqlite3 connection or similar that uses '?' for parameters
        cursor = db.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.fetchone()
    except Exception as e:
        # Log the error appropriately
        log_message(f"Database query error: {e} | Query: {query} | Params: {params}") # Log query details
        # Maybe report to error channel?
        # await report_error_to_channel(bot, f"Database query error: {e}") # Needs bot instance
        return None
    # finally:
        # Closing cursor might be handled by db connection context manager?
        # if cursor:
        #    cursor.close()


def create_query_for_company_check(company_name_like):
    """Creates SQL query to check if a company exists (case-insensitive)."""
    # Use parameterization placeholder '?' for sqlite
    query = """
        SELECT company_name
        FROM ShortPositions
        WHERE LOWER(company_name) LIKE LOWER(?) COLLATE NOCASE
        ORDER BY timestamp DESC
        LIMIT 1
        """
    # Need to add wildcards for LIKE search with parameters
    params = ('%' + company_name_like + '%',)
    return query, params


async def create_timeseries(db, company_name):
    """Creates a daily time series of short positions for the last 3 months."""
    # Get the latest timestamp available in the database FOR THIS COMPANY
    latest_db_timestamp_query = "SELECT MAX(timestamp) FROM ShortPositions WHERE company_name = ?"
    latest_db_timestamp_result = await execute_query(db, latest_db_timestamp_query, (company_name,))


    if not latest_db_timestamp_result or not latest_db_timestamp_result[0]:
        log_message(f"No timestamp data found for {company_name} in DB.")
        return pd.DataFrame() # Return empty DataFrame if no data

    try:
         latest_db_ts = pd.to_datetime(latest_db_timestamp_result[0])
    except (ValueError, TypeError):
         log_message(f"Could not parse latest timestamp '{latest_db_timestamp_result[0]}' for {company_name}.")
         return pd.DataFrame()

    three_months_ago = latest_db_ts - pd.DateOffset(months=3)

    # Query data for the specific company within the date range
    # Use parameterization for safety
    query = f"""
        SELECT timestamp, position_percent
        FROM ShortPositions
        WHERE company_name = ?
        AND timestamp >= ? /* Compare directly using timestamps */
        AND timestamp <= ?
        ORDER BY timestamp ASC
        """
    # Use string format compatible with typical DB timestamp comparison
    params = (company_name, three_months_ago.strftime('%Y-%m-%d %H:%M:%S'), latest_db_ts.strftime('%Y-%m-%d %H:%M:%S'))


    try:
        # Use pandas read_sql_query with parameters
        data = pd.read_sql_query(query, db.conn, params=params)
    except Exception as e:
        # Log error - replace print with proper logging/reporting
        log_message(f"Error querying timeseries data for {company_name}: {e}")
        return pd.DataFrame()


    if data.empty:
        log_message(f"No short position data found for {company_name} in the last 3 months ({three_months_ago.date()} to {latest_db_ts.date()}).")
        return pd.DataFrame()

    # Convert the timestamp column to datetime
    data['timestamp'] = pd.to_datetime(data['timestamp'])

    # Set the timestamp column as the index
    data.set_index('timestamp', inplace=True)

    # Ensure data is sorted by index before resampling/filling
    data.sort_index(inplace=True)

    # Find the first date in the data to create the full range
    start_date = data.index.min().normalize() # Start from beginning of the day
    # Use latest_db_ts as end date, normalized to include the whole day
    end_date = latest_db_ts.normalize()
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')


    # Reindex the data to the full daily range, then forward fill
    # Use tolerance on reindex if needed, but ffill should handle gaps
    daily_data = data.reindex(date_range).ffill()


    # Filter out any rows that might be NaNs if the first day had no data originally
    daily_data.dropna(subset=['position_percent'], inplace=True)


    # Filter again for the required 3-month plot window relative to latest data point
    plot_start_date = (daily_data.index.max() - pd.DateOffset(months=3)).normalize()
    daily_data = daily_data[daily_data.index >= plot_start_date]


    return daily_data


async def short_command(ctx, db, company_name_input):
    """Handles the Discord command to show short interest for a company."""
    company_name_like = company_name_input.strip() # Clean input
    if not company_name_like:
        await ctx.send("Ange ett företagsnamn.")
        return

    # Check if the company exists using LIKE and get the exact name
    check_query, check_params = create_query_for_company_check(company_name_like)
    result = await execute_query(db, check_query, check_params)


    if not result:
        await ctx.send(f"Kunde inte hitta data för ett företag som matchar '{company_name_input}'. Försök igen med ett exaktare namn eller kolla stavning.")
        return

    company_name_exact = result[0] # Get the correctly cased name from DB

    # Acknowledge command and start processing
    processing_msg = await ctx.send(f"Hämtar blankningsdata för **{company_name_exact}**...")

    # Create the time series data
    daily_data = await create_timeseries(db, company_name_exact)

    # Delete the "processing" message
    try:
        await processing_msg.delete()
    except (discord.NotFound, discord.Forbidden):
        pass # Ignore if message already deleted or permissions missing


    if daily_data.empty:
        # Try fetching the absolute latest value as fallback
        latest_query = f"SELECT position_percent, timestamp FROM ShortPositions WHERE company_name = ? ORDER BY timestamp DESC LIMIT 1"
        latest_result = await execute_query(db, latest_query, (company_name_exact,))
        if latest_result:
             latest_perc, latest_ts = latest_result
             # Attempt to parse timestamp robustly
             try:
                 latest_dt = pd.to_datetime(latest_ts)
                 latest_date_str = latest_dt.strftime('%Y-%m-%d')
             except (ValueError, TypeError):
                 latest_date_str = str(latest_ts) # Fallback to raw string if parsing fails

             await ctx.send(f"**{company_name_exact}**: Senaste rapporterade aggregerade blankning är **{latest_perc:.2f}%** ({latest_date_str}). Ingen sammanhängande historik för graf de senaste 3 månaderna.")
        else:
             await ctx.send(f"Ingen blankningsdata (aggregerad > 0.5%) hittades för **{company_name_exact}**.")
        return

    # Generate the plot
    image_stream = await plot_timeseries(daily_data, company_name_exact)

    if image_stream:
        latest_percentage = daily_data['position_percent'].iloc[-1]
        latest_date_str = daily_data.index[-1].strftime('%Y-%m-%d')

        message = (f"**{company_name_exact}**: Senaste aggregerade blankning är **{latest_percentage:.2f}%** "
                   f"(per {latest_date_str}). Graf för senaste 3 månader:")

        await ctx.send(message, file=discord.File(image_stream, filename=f'{company_name_exact}_shorts.png'))
    else:
        # Fallback if plot generation failed but data exists
        latest_percentage = daily_data['position_percent'].iloc[-1]
        latest_date_str = daily_data.index[-1].strftime('%Y-%m-%d')
        await ctx.send(f"**{company_name_exact}**: Senaste aggregerade blankning är **{latest_percentage:.2f}%** (per {latest_date_str}). Kunde inte generera graf.")
