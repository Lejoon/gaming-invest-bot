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
    'DATA': 'https://www.fi.se/sv/vara-register/blankningsregistret/GetBlankningsregisterAggregat/',
    'TIMESTAMP': 'https://www.fi.se/sv/vara-register/blankningsregistret/'

}
FILE_PATHS = {'DATA': 'Blankningsregisteraggregat.ods', 'TIMESTAMP': 'last_known_timestamp.txt'}
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
        response.raise_for_status()  # Always good to check for HTTP errors
        
        # Check the content type header
        content_type = response.headers.get('Content-Type', '')
        
        # If the content is HTML, XML, etc., decode it as text
        if 'text' in content_type or 'json' in content_type or 'xml' in content_type:
            # Here you might use response.charset or response.get_encoding()
            # if you expect different encodings
            return await response.text()
            
        # If the content is a binary file or anything else, return as bytes
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
        
def read_data(path):
    df = pd.read_excel(path, sheet_name='Blad1', skiprows=5, engine="odf")
    os.remove(path)
    
    # Rename columns by position: [0] -> 'company_name', [1] -> 'lei', etc.
    new_column_names = {
        df.columns[0]: 'company_name',
        df.columns[1]: 'lei',
        df.columns[2]: 'position_percent',
        df.columns[3]: 'latest_position_date'
    }
    df.rename(columns=new_column_names, inplace=True)

    # Only rename columns that exist in the DataFrame
    new_column_names = {k: v for k, v in new_column_names.items() if k in df.columns}
    
    if 'company_name' in df.columns:
        df['company_name'] = df['company_name'].str.strip()
        
    return df


# Function to update the database based on the differences between old and new data
async def update_database_diff(old_data, new_data, db, fetched_timestamp, bot):

    if old_data.empty:
        new_data['timestamp'] = fetched_timestamp  # Add the fetched timestamp
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
                    
# Main asynchronous loop to update the database at intervals
async def update_fi_from_web(db, bot):
    while True:
        async with aiohttp_session() as session:
            last_known_timestamp = read_last_known_timestamp(FILE_PATHS['TIMESTAMP'])
            web_timestamp = await fetch_last_update_time(session)
            next_update_time = datetime.now() + timedelta(seconds=DELAY_TIME)

            if web_timestamp == last_known_timestamp:
                print(f'[LOG] Web timestamp unchanged ({web_timestamp}). Waiting until {next_update_time.strftime("%Y-%m-%d %H:%M:%S")}.')
                await asyncio.sleep(DELAY_TIME)
                continue

            last_known_timestamp = web_timestamp
            write_last_known_timestamp(FILE_PATHS['TIMESTAMP'], web_timestamp)
            
            print(f'[LOG] New web timestamp detected ({web_timestamp}). Updating database at {next_update_time.strftime("%Y-%m-%d %H:%M:%S")}.')
            
            await download_file(session, URLS['DATA'], FILE_PATHS['DATA'])
            new_data = read_data(FILE_PATHS['DATA'])

            old_data = pd.read_sql('SELECT * FROM ShortPositions', db.conn)
            await update_database_diff(old_data, new_data, db, fetched_timestamp=web_timestamp, bot=bot)
            
            print('Database updated with new shorts if any.')
            await asyncio.sleep(DELAY_TIME)


async def manual_update(db):
    async with aiohttp_session() as session:
        await download_file(session,URLS['DATA'], FILE_PATHS['DATA'])
        new_data = read_data(FILE_PATHS['DATA'])
        old_data = pd.read_sql('SELECT * FROM ShortPositions', db.conn)

        update_database_diff(old_data, new_data, db)

        print('Database updated with new shorts if any.')



# Command that returns the current short position for a given company name. It tries to match the company name at the best possible level, could be just partly or in another case. 
async def short_command(ctx, db, company_name):
    company_name = company_name.lower()
    
    query = f"""
    SELECT company_name, position_percent, timestamp
    FROM ShortPositions
    WHERE LOWER(company_name) LIKE '%{company_name}%'
    ORDER BY timestamp DESC
    LIMIT 1
    """
    result = db.cursor.execute(query).fetchone()

    if result:
        db_company_name, position_percent, timestamp = result
        await ctx.send(f"The latest short position for {db_company_name} is {position_percent}% at {timestamp}.")
    else:
        await ctx.send(f"No short position found for {company_name}.")
        
# Entry point
if __name__ == "__main__":
    db = Database('steam_top_games.db')  # Replace with your actual DB name

    #drop shortpositions
    #db.cursor.execute('DROP TABLE ShortPositions')
    db.create_tables()
    asyncio.run(update_fi_from_web(db))
