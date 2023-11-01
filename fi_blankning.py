from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import asyncio
import os
import pandas as pd
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from discord import Embed
from database import Database  # Assuming Database class is already defined

# Constants
URLS = {
    'DATA': 'https://www.fi.se/sv/vara-register/blankningsregistret/GetBlankningsregisterAggregat/',
    'TIMESTAMP': 'https://www.fi.se/sv/vara-register/blankningsregistret/'
}
FILE_PATHS = {'DATA': 'Blankningsregisteraggregat.ods', 'TIMESTAMP': 'last_known_timestamp.txt'}
DELAY_TIME = timedelta(minutes=15)
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
    async with session.get(url) as response:
        return await response.text()


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
    return df.rename(columns={
        'Bolagsnamn': 'company_name',
        'LEI': 'lei',
        'Position i procent': 'position_percent',
        'Senast rapporterade positionens dag': 'latest_position_date'
    }).assign(company_name=lambda x: x['company_name'].str.strip())


async def update_database_diff(db, old_data, new_data, timestamp, bot_channel):
    new_data['timestamp'] = timestamp
    if old_data.empty:
        db.insert_bulk_data(new_data, table='ShortPositions')
        return

    merge_on = ['lei', 'company_name']
    
    # Last observation carried forward
    old_data = old_data.sort_values('latest_position_date').drop_duplicates(['lei', 'company_name'], keep='last')
    new_data = new_data.sort_values('latest_position_date').drop_duplicates(['lei', 'company_name'], keep='last')

    common = old_data.merge(new_data, on=merge_on, suffixes=('', '_new'))
    changed = common.loc[common['position_percent'] != common['position_percent_new']]
    
    new_entries = new_data.loc[~new_data['lei'].isin(old_data['lei'])]
    updated_entries = changed.loc[:, merge_on + ['position_percent_new', 'latest_position_date_new']]

    db_entries = pd.concat([new_entries, updated_entries])
    db.insert_bulk_data(db_entries, table='ShortPositions')

    if not db_entries.empty:
        for _, row in db_entries.iterrows():
            if row['company_name'] in TRACKED_COMPANIES:
                await send_update(bot_channel, row)


async def send_update(channel, row):
    embed = Embed(
        title=row['company_name'],
        description=f"Ändrad blankning: {row['position_percent']}%",
        url=f"https://www.fi.se/sv/vara-register/blankningsregistret/emittent/?id={row['lei']}",
        timestamp=datetime.strptime(row['timestamp'], "%Y-%m-%d %H:%M")
    )
    await channel.send(embed=embed)


async def main(bot):
    db = Database('steam_top_games.db')
    db.create_tables()
    bot_channel = bot.get_channel(CHANNEL_ID)
    old_timestamp = None

    while True:
        async with aiohttp_session() as session:
            new_timestamp = await fetch_last_update_time(session)
            if old_timestamp != new_timestamp:
                await download_file(session, URLS['DATA'], FILE_PATHS['DATA'])
                new_data = read_data(FILE_PATHS['DATA'])
                old_data = pd.read_sql('SELECT * FROM ShortPositions', db.conn)
                await update_database_diff(db, old_data, new_data, new_timestamp, bot_channel)
                old_timestamp = new_timestamp
            await asyncio.sleep(DELAY_TIME.total_seconds())


if __name__ == '__main__':
    asyncio.run(main())
