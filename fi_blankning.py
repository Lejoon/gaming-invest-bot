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

async def fetch_last_update_time(session):
    content = await fetch_url(session, URLS['TIMESTAMP'])
    soup = BeautifulSoup(content, 'html.parser')
    timestamp_text = soup.find('p', string=lambda text: 'Listan uppdaterades:' in text if text else False)
    return timestamp_text.string.split(": ")[1] if timestamp_text else None

async def download_file(session, url, path):
    content = await fetch_url(session, url)
    with open(path, 'wb') as f:
        f.write(content)
        
async def read_aggregate_data(path):
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

async def read_current_data(path):
    df = pd.read_excel(path, sheet_name='Blad1', skiprows=5, engine="odf")
    os.remove(path)
    
    new_column_names = {
        df.columns[0]: 'entity_name',
        df.columns[1]: 'issuer_name',
        df.columns[2]: 'isin',
        df.columns[3]: 'position_percent',
        df.columns[4]: 'latest_position_date'
    }
    df.rename(columns=new_column_names, inplace=True)
    
    df['company_name'] = df['company_name'].str.strip()
    df['issuer_name'] = df['issuer_name'].str.strip()
        
    return df

async def update_database_diff(old_data, new_data, db, fetched_timestamp, bot):

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
    
    if not new_rows.empty:
        channel = bot.get_channel(CHANNEL_ID)

        for _, row in new_rows.iterrows():
            company_name = row['company_name']
            new_position_percent = row['position_percent']
            lei = row['lei']
            timestamp = row['timestamp']

            # Check for exact matches in companies_to_track
            if company_name in TRACKED_COMPANIES:
                # Find the old position for this company if available
                old_position_data = old_data.loc[old_data['company_name'] == company_name]
                old_position_percent = old_position_data['position_percent'].iloc[0] if not old_position_data.empty else None

                change = None
                if old_position_percent is not None:
                    change = new_position_percent - old_position_percent

                description = f"Ändrad blankning: {new_position_percent}%"
                if change is not None:
                    description += f" ({change:+.2f})" if change > 0 else f" ({change:-.2f})"
                    
                embed = Embed(
                    title=company_name, 
                    description=description,
                    url=f"https://www.fi.se/sv/vara-register/blankningsregistret/emittent/?id={lei}",
                    timestamp=datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
                )
                embed.set_footer(text="FI", icon_url="https://upload.wikimedia.org/wikipedia/en/thumb/a/aa/Financial_Supervisory_Authority_%28Sweden%29_logo.svg/320px-Financial_Supervisory_Authority_%28Sweden%29_logo.svg.png")

                if channel:
                    await channel.send(embed=embed)

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
   # Load the Roboto font
    #roboto_font = fm.FontProperties(fname='/System/Library/Fonts/Supplemental/Arial.ttf')

    fig, ax = plt.subplots(figsize=(2.5, 1.5))  # Adjust figure size to 50%
    
    # Set figure background color
    fig.patch.set_facecolor('#36393F')  # Discord dark mode background color

    ax.plot(daily_data.index, daily_data['position_percent'], linewidth=1, color='#1DA1F2')
    # Remove axes
    ax.axis('off')

    # Add title with company name
    ax.text(daily_data.index[0], daily_data['position_percent'].max() + 0.2, company_name, fontsize=8, ha='left',color='white')

    # Calculate the change over 1 day and 1 week
    change_1d = daily_data['position_percent'].iloc[-1] - daily_data['position_percent'].iloc[-2]
    if len(daily_data) >= 7:
        change_1w = daily_data['position_percent'].iloc[-1] - daily_data['position_percent'].iloc[-7]
    else: 
        change_1w = "N/A"
    if len(daily_data) >= 30: 
        change_1m = daily_data['position_percent'].iloc[-1] - daily_data['position_percent'].iloc[-30]
    else:
        change_1m = "N/A"
    # Add the change text under the title
    change_1w_str = str(change_1w) if isinstance(change_1w, str) else f"{change_1w:.2f}"
    change_1m_str = str(change_1m) if isinstance(change_1m, str) else f"{change_1m:.2f}"

    ax.text(daily_data.index[0], daily_data['position_percent'].max() +0.05, f'1D ({change_1d:.2f}) 1W ({change_1w_str}) 1M ({change_1m_str})', fontsize=6, ha='left',color='white')
    # Label the first and last timestamp with the position percent
    first_timestamp, last_timestamp = daily_data.index[0], daily_data.index[-1]
    first_value, last_value = daily_data.iloc[0, 0], daily_data.iloc[-1, 0]
    ax.text(first_timestamp, first_value, f'{first_value:.2f}', ha='right',  fontsize=6, bbox=dict(facecolor='#36393F', edgecolor='none', pad=1), color='white')
    ax.text(last_timestamp, last_value, f'{last_value:.2f}', ha='left',  fontsize=6, bbox=dict(facecolor='#36393F', edgecolor='none', pad=1), color='white')

    plt.tight_layout()

    # Save the figure to a BytesIO object
    image_stream = io.BytesIO()
    plt.savefig(image_stream, format='png', dpi=120, facecolor=fig.get_facecolor(), edgecolor='none')  # Save the figure with a resolution that fits a 200x150 image
    image_stream.seek(0)  # Go back to the start of the BytesIO object

    plt.close(fig)  # Close the figure to free up memory

    return image_stream
          
# Main asynchronous loop to update the database at intervals
async def update_fi_from_web(db, bot):
    while True:
        async with aiohttp_session() as session:
            web_timestamp = await is_timestamp_updated(session)
            
            if not web_timestamp:
                continue
            
            await download_file(session, URLS['DATA_AGG'], FILE_PATHS['DATA_AGG'])
            new_data = await read_aggregate_data(FILE_PATHS['DATA_AGG'])

            old_data = pd.read_sql('SELECT * FROM ShortPositions', db.conn)
            await update_database_diff(old_data, new_data, db, fetched_timestamp=web_timestamp, bot=bot)
            
            log_message('Database updated with new shorts if any.')
            await asyncio.sleep(DELAY_TIME)


async def manual_update(db):
    async with aiohttp_session() as session:
        await download_file(session,URLS['DATA_AGG'], FILE_PATHS['DATA'])
        new_data = await read_aggregate_data(FILE_PATHS['DATA_AGG'])
        old_data = pd.read_sql('SELECT * FROM ShortPositions', db.conn)

        update_database_diff(old_data, new_data, db)

        log_message('Manual update of database completed.')


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

    # Calculate the date 30 days ago
    thirty_days_ago = now - timedelta(days=30)

    # Query the database to get the data for the last 30 days
    query = f"""
        SELECT timestamp, position_percent
        FROM ShortPositions
        WHERE company_name LIKE '{company_name}'
        AND timestamp >= (
            SELECT MAX(timestamp) 
            FROM ShortPositions 
            WHERE timestamp <= '{thirty_days_ago.strftime("%Y-%m-%d %H:%M")}'
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

    await ctx.send(file=discord.File(image_stream, filename='plot.png'))

        
# Entry point
if __name__ == "__main__":
    db = Database('steam_top_games.db')  # Replace with your actual DB name

    #drop shortpositions
    #db.cursor.execute('DROP TABLE ShortPositions')
    db.create_tables()
    asyncio.run(update_fi_from_web(db))
