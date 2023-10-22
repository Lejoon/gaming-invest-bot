import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import dotenv
import os

dotenv.load_dotenv()

def fetch_ccu(appid):
    # Replace YOUR_STEAM_API_KEY with your actual Steam API key
    STEAM_API_KEY = os.getenv('STEAM_API_KEY')
    url = f"http://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?key={STEAM_API_KEY}&appid={appid}"
    response = requests.get(url)
    data = json.loads(response.text)
    if data['response']['result'] == 1:
        ccu = data['response']['player_count']
    else:
        ccu = 0
    
    return ccu

def create_tables(cursor):
    # Create the translation table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS GameTranslation (
        appid TEXT PRIMARY KEY,
        game_name TEXT
    );
    ''')

    # Create the SteamTopGames table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS SteamTopGames (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        place INTEGER,
        appid TEXT,
        discount TEXT,
        ccu INTEGER
    );
    ''')

def fetch_steam_top_sellers():
    url = "https://store.steampowered.com/search/?filter=globaltopsellers"
    response = requests.get(url)
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    
    # Initialize SQLite Database
    conn = sqlite3.connect('steam_top_games.db')
    cursor = conn.cursor()
    
    # Create the tables if they don't exist
    create_tables(cursor)
    
    # Fetch the latest timestamp from the database
    cursor.execute("SELECT MAX(timestamp) FROM SteamTopGames")
    latest_timestamp = cursor.fetchone()[0]
    if latest_timestamp is not None:
        latest_timestamp = datetime.strptime(latest_timestamp, '%Y-%m-%d %H')

    # Get the current time (up to the hour)
    current_time = datetime.now().replace(minute=0, second=0, microsecond=0)

    # If there was an update within the last hour, don't update the database
    if latest_timestamp is not None and current_time - latest_timestamp < timedelta(hours=1):
        return

    
    count = 1
    for game_div in soup.select('.search_result_row')[:250]:
        appid = game_div['data-ds-appid']
        title_elements = game_div.select('.title')
        price_elements = game_div.select('.discount_final_price, .search_price')
        discount_elements = game_div.select('.discount_pct, .search_price')

        title = title_elements[0].text if title_elements else "Unknown title"
        discount = discount_elements[0].text.strip() if discount_elements else ""
        price = price_elements[0].text.strip() if price_elements else ""
        
        if price == "Free":
            discount = "Free"
        

        # Fetch CCU using Steam API
        ccu = fetch_ccu(appid)

        # Fetch wishlisted copies from Steam API (mocked here)
        wishlisted = 0  # Replace with actual API call
        
        # Check if the appid already exists in the translation table
        cursor.execute("SELECT game_name FROM GameTranslation WHERE appid = ?", (appid,))
        result = cursor.fetchone()
        
        # If the appid doesn't exist, insert it into the translation table
        if result is None:
            cursor.execute("INSERT INTO GameTranslation (appid, game_name) VALUES (?, ?)", (appid, title))

        timestamp = datetime.now().strftime('%Y-%m-%d %H')
        
        cursor.execute('''
        INSERT INTO SteamTopGames (timestamp, place, appid, discount, ccu)
        VALUES (?, ?, ?, ?, ?)
        ''', (timestamp, count, appid, discount, ccu))
        
        count += 1

    conn.commit()
    conn.close()

# Run the function
fetch_steam_top_sellers()

with sqlite3.connect('steam_top_games.db') as conn:
    cursor = conn.cursor()

    cursor.execute('''
    SELECT SteamTopGames.timestamp, SteamTopGames.place, GameTranslation.game_name, SteamTopGames.discount, SteamTopGames.ccu
    FROM SteamTopGames
    JOIN GameTranslation ON SteamTopGames.appid = GameTranslation.appid
    ''')
    rows = cursor.fetchall()

    for row in rows:
        print(row)