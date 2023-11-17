from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import asyncio
import os
import pandas as pd
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from discord import Embed
from database import Database  # Assuming Database class is already defined
from general_utils import retry_with_backoff

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
CHANNEL_ID = 1167391973825593424
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

@retry_with_backoff(retries=5, base_delay=5.0, max_delay=120.0)
async def fetch_last_update_time(session):
    content = await fetch_url(session, URLS['TIMESTAMP'])
    soup = BeautifulSoup(content, 'html.parser')
    timestamp_text = soup.find('p', string=lambda text: 'Listan uppdaterades:' in text if text else False)
    return timestamp_text.string.split(": ")[1] if timestamp_text else None

async def download_file(session, url, path):
    content = await fetch_url(session, url)
    with open(path, 'wb') as f:
        f.write(content)
        
def read_aggregate_data(path):
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

def read_current_data(path):
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
    next_update_time = datetime.now() + timedelta(seconds=DELAY_TIME)

    if web_timestamp == last_known_timestamp:
        print(f'[LOG] Web timestamp unchanged ({web_timestamp}). Waiting until {next_update_time.strftime("%Y-%m-%d %H:%M:%S")}.')
        await asyncio.sleep(DELAY_TIME)
        return False

    last_known_timestamp = web_timestamp
    write_last_known_timestamp(FILE_PATHS['TIMESTAMP'], web_timestamp)
    
    print(f'[LOG] New web timestamp detected ({web_timestamp}). Updating database at {next_update_time.strftime("%Y-%m-%d %H:%M:%S")}.')
    return web_timestamp
                    
# Main asynchronous loop to update the database at intervals
async def update_fi_from_web(db, bot):
    while True:
        async with aiohttp_session() as session:
            web_timestamp = await is_timestamp_updated(session)
            
            if not web_timestamp:
                continue
            
            await download_file(session, URLS['DATA_AGG'], FILE_PATHS['DATA_AGG'])
            new_data = read_aggregate_data(FILE_PATHS['DATA_AGG'])

            old_data = pd.read_sql('SELECT * FROM ShortPositions', db.conn)
            await update_database_diff(old_data, new_data, db, fetched_timestamp=web_timestamp, bot=bot)
            
            print('Database updated with new shorts if any.')
            await asyncio.sleep(DELAY_TIME)


async def manual_update(db):
    async with aiohttp_session() as session:
        await download_file(session,URLS['DATA_AGG'], FILE_PATHS['DATA'])
        new_data = read_aggregate_data(FILE_PATHS['DATA_AGG'])
        old_data = pd.read_sql('SELECT * FROM ShortPositions', db.conn)

        update_database_diff(old_data, new_data, db)

        print('Database updated with new shorts if any.')



# Command that returns the current short position for a given company name. It tries to match the company name at the best possible level, could be just partly or in another case. 
async def short_command(ctx, db, company_name):
    company_name = company_name.lower()
    
    now = datetime.now()
    one_day_ago = (now - timedelta(days=1))
    one_week_ago = (now - timedelta(weeks=1))

    query = f"""
    SELECT company_name, position_percent, timestamp
    FROM ShortPositions
    WHERE LOWER(company_name) LIKE '%{company_name}%'
    AND timestamp >= '{one_week_ago.strftime("%Y-%m-%d %H:%M")}'
    ORDER BY timestamp DESC
    """
    results = db.cursor.execute(query).fetchall()

    if results:
        current_data = results[0]
        one_day_change = None
        one_week_change = None

        for data in results:
            data_timestamp = datetime.strptime(data[2], "%Y-%m-%d %H:%M")

            # Checking for one day change
            if data_timestamp <= one_day_ago:
                if one_day_change is None or data_timestamp > datetime.strptime(one_day_change[2], "%Y-%m-%d %H:%M"):
                    one_day_change = data

            # Checking for one week change
            if data_timestamp <= one_week_ago:
                if one_week_change is None or data_timestamp > datetime.strptime(one_week_change[2], "%Y-%m-%d %H:%M"):
                    one_week_change = data

        # Calculate the changes
        if one_day_change:
            one_day_change_value = current_data[1] - one_day_change[1]
        else:
            one_day_change_value = None

        if one_week_change:
            one_week_change_value = current_data[1] - one_week_change[1]
        else:
            one_week_change_value = None

        response = f"The latest short position for {current_data[0]} is {current_data[1]}% at {current_data[2]}."
        if one_day_change_value is not None:
            response += f"\n1-day change: {one_day_change_value:+.2f}%."
        if one_week_change_value is not None:
            response += f"\n1-week change: {one_week_change_value:+.2f}%."

        await ctx.send(response)
    else:
        await ctx.send(f"No short position found for {company_name}.")

        
# Entry point
if __name__ == "__main__":
    db = Database('steam_top_games.db')  # Replace with your actual DB name

    #drop shortpositions
    #db.cursor.execute('DROP TABLE ShortPositions')
    db.create_tables()
    asyncio.run(update_fi_from_web(db))
