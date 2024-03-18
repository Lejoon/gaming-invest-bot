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

async def read_current_data(path):
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
    
    
    return df

async def report_error_to_channel(bot, exception):
    """
    Sends an error message to a specific Discord channel.

    Parameters:
    - bot: The Discord bot instance.
    - channel_id (int): The ID of the channel where the message should be sent.
    - exception (Exception): The exception object to report.
    """
    channel = bot.get_channel(ERROR_ID)
    if channel:
        # Format the error message
        error_message = f"An error occurred: {type(exception).__name__}: {exception}"
        # Send the message to the channel
        await channel.send(error_message)
    else:
        print(f"Could not find a channel with ID {ERROR_ID}")

async def send_embed(old_agg_data, new_agg_data, old_act_data, new_act_data, db, fetched_timestamp, bot=None):
    if bot is not None:
        channel = bot.get_channel(CHANNEL_ID)

    agg_new_rows = await update_database_diff(old_agg_data, new_agg_data, db, fetched_timestamp)
    act_new_rows = await update_position_holders(old_act_data, new_act_data, db, fetched_timestamp)

    for _, row in agg_new_rows.iterrows():
        company_name = row['company_name']
        new_position_percent = row['position_percent']
        lei = row['lei']
        timestamp = row['timestamp']

        if company_name in TRACKED_COMPANIES:
            old_position_data = old_agg_data.loc[old_agg_data['company_name'] == company_name]
            old_position_percent = old_position_data['position_percent'].iloc[0] if not old_position_data.empty else None
            time_new_position = agg_new_rows.loc[agg_new_rows['company_name'] == company_name, 'timestamp'].iloc[0]

            change = None
            if old_position_percent is not None:
                change = new_position_percent - old_position_percent

            description = f"Ändrad blankning: {new_position_percent}%, {time_new_position}"
            if change is not None:
                description += f" ({change:+.2f})" if change > 0 else f" ({change:-.2f})"

            issuer_data = act_new_rows[act_new_rows['issuer_name'] == company_name]

            if not issuer_data.empty:
                holder_description = "\n"
                for _, holder_row in issuer_data.iterrows():
                    entity_name = holder_row['entity_name']
                    new_holder_percent = holder_row['position_percent']
                    old_holder_data = old_act_data[(old_act_data['entity_name'] == entity_name) & (old_act_data['issuer_name'] == company_name)]
                    old_holder_percent = old_holder_data['position_percent'].iloc[0] if not old_holder_data.empty else 0
                    time_holder_position = holder_row['position_date']
                    holder_change = new_holder_percent - old_holder_percent

                    holder_description += f"{entity_name}: {new_holder_percent}% ({holder_change:+.2f}), senast uppdaterad {time_holder_position}\n"

                description += holder_description
            if bot is not None:
                embed = Embed(
                    title=company_name,
                    description=description,
                    url=f"https://www.fi.se/sv/vara-register/blankningsregistret/emittent/?id={lei}",
                    timestamp=datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
                )
                embed.set_footer(text="FI", icon_url="https://upload.wikimedia.org/wikipedia/en/thumb/a/aa/Financial_Supervisory_Authority_%28Sweden%29_logo.svg/320px-Financial_Supervisory_Authority_%28Sweden%29_logo.svg.png")

                await channel.send(embed=embed)
            else:
                print('Test embedding')
                print(description)

async def update_position_holders(old_data, new_data, db, fetched_timestamp):
    if old_data.empty:
        new_data['timestamp'] = fetched_timestamp
        db.insert_bulk_data(input=new_data, table='PositionHolders')
        return
    
    if not new_data.empty:
        new_data['timestamp'] = fetched_timestamp
        
    old_data = old_data.sort_values('timestamp').drop_duplicates(['entity_name', 'issuer_name', 'isin'], keep='last')
    new_data = new_data.sort_values('timestamp').drop_duplicates(['entity_name', 'issuer_name', 'isin'], keep='last')
    
    new_positions = new_data.loc[~new_data[['entity_name', 'issuer_name', 'isin']].apply(tuple, 1).isin(old_data[['entity_name', 'issuer_name', 'isin']].apply(tuple, 1))]
    common_positions = new_data.loc[new_data[['entity_name', 'issuer_name', 'isin']].apply(tuple, 1).isin(old_data[['entity_name', 'issuer_name', 'isin']].apply(tuple, 1))]

    changed_positions = pd.merge(common_positions, old_data, on=['entity_name', 'issuer_name', 'isin'])
    changed_positions = changed_positions[changed_positions['position_percent_x'] != changed_positions['position_percent_y']]
    changed_positions = changed_positions[['entity_name', 'issuer_name', 'isin', 'position_percent_x', 'position_date_x']]
    changed_positions.columns = ['entity_name', 'issuer_name', 'isin', 'position_percent', 'position_date']
    
    new_positions['timestamp'] = fetched_timestamp
    changed_positions['timestamp'] = fetched_timestamp
    new_rows = pd.concat([new_positions, changed_positions])

    db.insert_bulk_data(input=new_rows, table='PositionHolders')
    return new_rows

async def update_database_diff(old_data, new_data, db, fetched_timestamp):

    if old_data.empty:
        new_data['timestamp'] = fetched_timestamp
        db.insert_bulk_data(input=new_data, table='ShortPositions')
        return
    
    if not new_data.empty:
        new_data['timestamp'] = fetched_timestamp
        
    old_data = old_data.sort_values('timestamp').drop_duplicates(['lei', 'company_name'], keep='last')
    new_data = new_data.sort_values('timestamp').drop_duplicates(['lei', 'company_name'], keep='last')
    
    new_leis = new_data.loc[~new_data['lei'].isin(old_data['lei'])]
    common_leis = new_data.loc[new_data['lei'].isin(old_data['lei'])]

    changed_positions = pd.merge(common_leis, old_data, on=['lei','company_name'])
    changed_positions = changed_positions[changed_positions['position_percent_x'] != changed_positions['position_percent_y']]
    changed_positions = changed_positions[['company_name', 'lei', 'position_percent_x', 'latest_position_date_x']]
    changed_positions.columns = ['company_name', 'lei', 'position_percent', 'latest_position_date']
    
    # Should probably be changed to a more efficient way of doing this
    if not new_leis.empty:
        new_leis.loc[:, 'timestamp'] = fetched_timestamp
    changed_positions['timestamp'] = fetched_timestamp
    new_rows = pd.concat([new_leis, changed_positions])

    # Insert new and updated records
    db.insert_bulk_data(input=new_rows, table='ShortPositions')
    return new_rows

async def is_timestamp_updated(session):
    last_known_timestamp = read_last_known_timestamp(FILE_PATHS['TIMESTAMP'])
    web_timestamp = await fetch_last_update_time(session)
    
    # If it's of the form "0001-01-01 00:00" it means that the web timestamp is not available, fetch again with delay
    while web_timestamp == "0001-01-01 00:00":
        next_update_time = datetime.now() + timedelta(seconds=30)
        log_message(f'Web timestamp unavailable. Waiting until {next_update_time.strftime("%Y-%m-%d %H:%M")} to retry.')
        await asyncio.sleep(30)
        web_timestamp = await fetch_last_update_time(session)
        
    next_update_time = datetime.now() + timedelta(seconds=DELAY_TIME)

    if web_timestamp == last_known_timestamp:
        log_message(f'Web timestamp unchanged ({web_timestamp}). Waiting until {next_update_time.strftime("%Y-%m-%d %H:%M")}.')
        await asyncio.sleep(DELAY_TIME)
        return False

    last_known_timestamp = web_timestamp
    write_last_known_timestamp(FILE_PATHS['TIMESTAMP'], web_timestamp)
    log_message(f'New web timestamp detected ({web_timestamp}). Updating database at {next_update_time.strftime("%Y-%m-%d %H:%M")}.')
    return web_timestamp

async def plot_timeseries(daily_data, company_name):
    three_months_ago = pd.Timestamp.now() - pd.DateOffset(months=3)
    daily_data = daily_data[daily_data.index >= three_months_ago]

    daily_data.loc[:,'position_percent'] = daily_data['position_percent'] / 100  # Scaling down by 100

    # Adjusting figure size and setting a professional font
    plt.figure(figsize=(4, 2))
    rcParams.update({'font.size': 7})  # Adjust font size
    plt.rcParams['font.family'] = ['sans-serif']
    plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']

    # Formatting the plot
    plt.plot(daily_data.index, daily_data['position_percent'], marker='o', linestyle='-', color='#7289DA', markersize=3)
    
    plt.title(f'{company_name}, Shorts Percentage Last 3m'.upper(), fontsize=6, weight='bold', loc='left')
    plt.xlabel('')
    plt.ylabel('')  # Y-axis label removed as per request

    # Set y-axis to display percentage
    plt.gca().yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=1))

    # Improve date formatting on x-axis
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())

    # Thin and transparent grid lines
    plt.grid(True, which='both', linestyle='-', linewidth=0.5, color='gray', alpha=0.3)

    # Remove plot outline
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['bottom'].set_visible(False)
    plt.gca().spines['left'].set_visible(False)

    # Adjust tick size
    plt.tick_params(axis='x', labelsize=6)
    plt.tick_params(axis='y', labelsize=6)

    # Display the plot
    plt.tight_layout()

    # Generate image stream
    image_stream = io.BytesIO()
    plt.savefig(image_stream, format='png')
    image_stream.seek(0)
    plt.close()
    return image_stream

# Main asynchronous loop to update the database at intervals
@aiohttp_retry(retries=5, base_delay=5.0, max_delay=120.0)
async def update_fi_from_web(db, bot):
    while True:
        async with aiohttp_session() as session:
            web_timestamp = await is_timestamp_updated(session)
            
            if not web_timestamp:
                continue
            
            await download_file(session, URLS['DATA_AGG'], FILE_PATHS['DATA_AGG'])
            await download_file(session, URLS['DATA_ACT'], FILE_PATHS['DATA_ACT'])
            try: 
                new_data_agg = await read_aggregate_data(FILE_PATHS['DATA_AGG'], bot)
                new_data_act = await read_current_data(FILE_PATHS['DATA_ACT'])

                old_data_agg = pd.read_sql('SELECT * FROM ShortPositions', db.conn)
                old_data_act = pd.read_sql('SELECT * FROM PositionHolders', db.conn)
                
                await send_embed(old_data_agg, new_data_agg, old_data_act, new_data_act, db, web_timestamp, bot)
                
                log_message('Database updated with new shorts if any.')
                await asyncio.sleep(DELAY_TIME)
            except Exception as e:
                await report_error_to_channel(bot, e)

async def manual_update(db, bot):
    async with aiohttp_session() as session:
        await download_file(session,URLS['DATA_AGG'], FILE_PATHS['DATA'])
        try:
            new_data = await read_aggregate_data(FILE_PATHS['DATA_AGG'],bot)
            old_data = pd.read_sql('SELECT * FROM ShortPositions', db.conn)

            update_database_diff(old_data, new_data, db)

            log_message('Manual update of database completed.')
        except Exception as e:
            await report_error_to_channel(e)


async def execute_query(db, query):
    return db.cursor.execute(query).fetchone()

def create_query(company_name, date, is_exact_date=True):
    if is_exact_date:
        date_condition = f"date(timestamp) = date('{date}')"
    else:
        date_condition = f"date(timestamp) <= date('{date}')"

    return f"""
        SELECT company_name, position_percent, timestamp
        FROM ShortPositions
        WHERE LOWER(company_name) LIKE '%{company_name}%'
        AND {date_condition}
        ORDER BY timestamp DESC
        LIMIT 1
        """
        
async def create_timeseries(db, company_name):
    # Get the current date
    now = datetime.now()

    # Calculate the date 3 months ago
    three_months_ago = pd.Timestamp.now() - pd.DateOffset(months=3)

    # Query the database to get the data for the last 3 months
    query = f"""
        SELECT timestamp, position_percent
        FROM ShortPositions
        WHERE company_name LIKE '{company_name}'
        AND timestamp >= (
            SELECT MAX(timestamp) 
            FROM ShortPositions 
            WHERE timestamp <= '{three_months_ago.strftime("%Y-%m-%d %H:%M")}'
        )
        AND timestamp <= '{now.strftime("%Y-%m-%d %H:%M")}'
        ORDER BY timestamp
        """
    data = pd.read_sql_query(query, db.conn)

    # Convert the timestamp column to datetime
    data['timestamp'] = pd.to_datetime(data['timestamp'])

    # Set the timestamp column as the index
    data.set_index('timestamp', inplace=True)

    # Resample the data to daily frequency, taking the last value of each day
    daily_data = data.resample('D').last()

    # Forward fill the missing values
    daily_data.ffill(inplace=True)

    return daily_data

import discord

async def short_command(ctx, db, company_name):
    company_name = company_name.lower()
    now = datetime.now()
    
    query = f"""
        SELECT company_name
        FROM ShortPositions
        WHERE company_name LIKE '%{company_name}%'
        LIMIT 1
        """
    
    # If the company name is not found in the database, return None to indicate that the company is not tracked
    if not await execute_query(db, query):
        await ctx.send(f'Kan inte hitta någon blankning för {company_name}.')
        return None
    else:
        company_name = (await execute_query(db, query))[0]


    daily_data = await create_timeseries(db, company_name)
    image_stream = await plot_timeseries(daily_data, company_name)

    await ctx.send(f'Company: {company_name}, {daily_data.iloc[-1, 0]}% total shorted above with smallest individual position > 0.1%')
    await ctx.send(file=discord.File(image_stream, filename='plot.png'))

