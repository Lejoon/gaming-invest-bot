# database.py
import sqlite3
from datetime import datetime, timedelta

class Database:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        
    def create_tables(self):
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS GameTranslation (
            appid TEXT PRIMARY KEY,
            game_name TEXT
        );
        ''')
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS SteamTopGames (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            place INTEGER,
            appid TEXT,
            discount TEXT,
            ccu INTEGER
        );
        ''')
        self.conn.commit()

    def insert_data(self, timestamp, place, appid, discount, ccu):
        self.cursor.execute('''
        INSERT INTO SteamTopGames (timestamp, place, appid, discount, ccu)
        VALUES (?, ?, ?, ?, ?)
        ''', (timestamp, place, appid, discount, ccu))
        self.conn.commit()
        
    def get_latest_timestamp(self):
        self.cursor.execute("SELECT MAX(timestamp) FROM SteamTopGames")
        return self.cursor.fetchone()[0]
    
    def get_yesterday_top_games(self, timestamp):
        # Calculate the date for yesterday and set the hour to 21
        yesterday_date = (datetime.strptime(timestamp, '%Y-%m-%d %H') - timedelta(days=1)).strftime('%Y-%m-%d')
        yesterday_timestamp_21 = f"{yesterday_date} 21"
        
        self.cursor.execute('''
        SELECT place, appid FROM SteamTopGames
        WHERE timestamp = ?
        ''', (yesterday_timestamp_21,))
        
        rows = self.cursor.fetchall()
        return {appid: place for place, appid in rows}

    def update_appid(self, appid, title):
        self.cursor.execute("SELECT game_name FROM GameTranslation WHERE appid = ?", (appid,))
        if self.cursor.fetchone() is None:
            self.cursor.execute("INSERT INTO GameTranslation (appid, game_name) VALUES (?, ?)", (appid, title))
            self.conn.commit()
            
    def insert_bulk_data(self, games):
        ''' 
        Insert multiple rows in a single transaction
        '''
        query = '''
        INSERT INTO SteamTopGames (timestamp, place, appid, discount, ccu)
        VALUES (?, ?, ?, ?, ?)
        '''
        # Prepare data in the format required for executemany
        data = [(game['timestamp'], game['count'], game['appid'], game['discount'], game['ccu']) for game in games]
        self.cursor.executemany(query, data)
        self.conn.commit()
    
    def close(self):
        self.conn.close()