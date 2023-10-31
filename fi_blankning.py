import pandas as pd
from datetime import datetime, timedelta
import asyncio
from aiohttp import ClientSession
from database import Database  # Assuming you've already defined this class
from bs4 import BeautifulSoup
import os

# Constants
URL = 'https://www.fi.se/sv/vara-register/blankningsregistret/GetBlankningsregisterAggregat/'
TIMESTAMP_URL = 'https://www.fi.se/sv/vara-register/blankningsregistret/'
ODS_FILE_PATH = 'Blankningsregisteraggregat.ods'
DELAY_TIME = 15 * 60  # 15 minutes
TIMESTAMP_FILE = 'last_known_timestamp.txt'

def read_last_known_timestamp(file_path):
    try:
        with open(file_path, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        return None
    
def write_last_known_timestamp(file_path, timestamp):
    with open(file_path, 'w') as f:
        f.write(timestamp)

async def fetch_last_update_time():
    global last_known_timestamp
    async with ClientSession() as session:
        async with session.get(TIMESTAMP_URL) as response:
            content = await response.text()
            soup = BeautifulSoup(content, 'html.parser')
            timestamp_text = soup.find('p', string=lambda text: 'Listan uppdaterades:' in text if text else False)
            if timestamp_text:
                last_known_timestamp = timestamp_text.string.split(": ")[1]
                return last_known_timestamp
            else:
                return None


# Asynchronous function to download the ODS file
async def download_ods_file(url, save_path):
    async with ClientSession() as session:
        async with session.get(url) as response:
            with open(save_path, 'wb') as f:
                f.write(await response.read())

# Function to read the new data from the ODS file into a DataFrame
def read_new_data(file_path):
    df = pd.read_excel(file_path, sheet_name='Blad1', skiprows=5, engine="odf")
    df.columns = ['company_name', 'lei', 'position_percent', 'latest_position_date']
    df['company_name'] = df['company_name'].str.strip()  # Remove leading and trailing whitespaces

    #remove the ods file
    os.remove(file_path)
    return df

# Function to update the database based on the differences between old and new data
def update_database_diff(old_data, new_data, db, fetched_timestamp):

    if old_data.empty:
        new_data['timestamp'] = fetched_timestamp  # Add the fetched timestamp
        db.insert_bulk_data(input=new_data, table='ShortPositions')
        return

    new_leis = new_data.loc[~new_data['lei'].isin(old_data['lei'])]
    common_leis = new_data.loc[new_data['lei'].isin(old_data['lei'])]

    changed_positions = pd.merge(common_leis, old_data, on=['lei','company_name'])

    changed_positions = changed_positions[changed_positions['position_percent_x'] != changed_positions['position_percent_y']]
    changed_positions = changed_positions[['company_name', 'lei', 'position_percent_x', 'latest_position_date_x']]
    changed_positions.columns = ['company_name', 'lei', 'position_percent', 'latest_position_date']
    
    new_leis['timestamp'] = fetched_timestamp
    changed_positions['timestamp'] = fetched_timestamp
    new_rows = pd.concat([new_leis, changed_positions])

    # Insert new and updated records
    db.insert_bulk_data(input=new_rows, table='ShortPositions')


# Main asynchronous loop to update the database at intervals
async def update_fi_from_web(db):
    last_known_timestamp = read_last_known_timestamp(TIMESTAMP_FILE)
    
    while True:
        web_timestamp = await fetch_last_update_time()
        next_update_time = datetime.now() + timedelta(seconds=DELAY_TIME)

        if web_timestamp == last_known_timestamp:
            print(f'[LOG] Web timestamp unchanged ({web_timestamp}). Waiting until {next_update_time.strftime("%Y-%m-%d %H:%M:%S")}.')
            await asyncio.sleep(DELAY_TIME)
            continue

        last_known_timestamp = web_timestamp
        write_last_known_timestamp(TIMESTAMP_FILE, web_timestamp)
        
        print(f'[LOG] New web timestamp detected ({web_timestamp}). Updating database at {next_update_time.strftime("%Y-%m-%d %H:%M:%S")}.')
        
        await download_ods_file(URL, ODS_FILE_PATH)
        new_data = read_new_data(ODS_FILE_PATH)

        old_data = pd.read_sql('SELECT * FROM ShortPositions', db.conn)
        update_database_diff(old_data, new_data, db, fetched_timestamp=web_timestamp)
        
        print('Database updated with new shorts if any.')
        await asyncio.sleep(DELAY_TIME)


async def manual_update(db):
    await download_ods_file(URL, ODS_FILE_PATH)
    new_data = read_new_data(ODS_FILE_PATH)
    old_data = pd.read_sql('SELECT * FROM ShortPositions', db.conn)

    update_database_diff(old_data, new_data, db)

    print('Database updated with new shorts if any.')

# Command that returns the current short position for a given company name. It tries to match the company name at the best possible level, could be just partly or in another case. 
async def short_command(ctx, db, company_name):
    company_name = company_name.lower()
    
    query = f"""
    SELECT position_percent, timestamp
    FROM ShortPositions
    WHERE LOWER(company_name) LIKE '%{company_name}%'
    ORDER BY timestamp DESC
    LIMIT 1
    """
    
    result = db.cursor.execute(query).fetchone()

    if result:
        position_percent, timestamp = result
        await ctx.send(f"The latest short position for {company_name} is {position_percent}% at {timestamp}.")
    else:
        await ctx.send(f"No short position found for {company_name}.")
        
# Entry point
if __name__ == "__main__":
    db = Database('steam_top_games.db')  # Replace with your actual DB name

    #drop shortpositions
    #db.cursor.execute('DROP TABLE ShortPositions')
    db.create_tables()
    asyncio.run(update_fi_from_web(db))
