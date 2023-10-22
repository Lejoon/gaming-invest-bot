import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
import cloudscraper

print('Running teststeamdb.py')

# Step 1 & 2: Create SQLite Database and Table
conn = sqlite3.connect('top_games.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS TopGames (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    place TEXT,
    appid TEXT,
    game_name TEXT,
    followers INTEGER,
    peak_24h INTEGER
);
''')

from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537'
}

# Initialize a session
scraper = cloudscraper.create_scraper()

# Fetch the webpage
url = "https://steamdb.info/stats/globaltopsellers/"
response = scraper.get(url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    html_content = response.text
    soup = BeautifulSoup(html_content, 'html.parser')
    # Your scraping logic here
    
    # Step 4: Insert Data into Database
    soup = BeautifulSoup(html_content, 'html.parser')
    rows = soup.select('tr.app')  # Assuming each row has a class 'app'

    for row in rows:
        place = row.select_one('td[data-sort]').text.strip()
        appid = row['data-appid']
        game_name = row.select_one('a[href*="/charts/"]').text.strip()
        followers = int(row.select_one('td[data-sort]').get('data-sort').replace(',', ''))
        peak_24h = int(row.select_one('td[data-sort]').get('data-sort').replace(',', ''))
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute('''
        INSERT INTO TopGames (timestamp, place, appid, game_name, followers, peak_24h)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (timestamp, place, appid, game_name, followers, peak_24h))

else:
    print('Status code:', response.text)
    print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
    quit()


conn.commit()
conn.close()

print('Finished running teststeamdb.py')