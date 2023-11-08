# database.py
import sqlite3
from datetime import datetime, timedelta

GAME_TRANSLATION_SCHEMA = '''
        CREATE TABLE IF NOT EXISTS GameTranslation (
            appid TEXT PRIMARY KEY,
            game_name TEXT
        );
        '''
STEAM_TOP_GAMES_SCHEMA = '''
        CREATE TABLE IF NOT EXISTS SteamTopGames (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            place INTEGER,
            appid TEXT,
            discount TEXT,
            ccu INTEGER
        );
        '''
SHORT_POSITIONS_SCHEMA = '''    
        CREATE TABLE IF NOT EXISTS ShortPositions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            company_name TEXT NOT NULL,
            lei TEXT,
            position_percent REAL,
            latest_position_date TEXT
        );
        '''
REPORTED_ENTITIES_SCHEMA = '''
        CREATE TABLE IF NOT EXISTS ReportedEntities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_name TEXT NOT NULL,
            issuer_name TEXT NOT NULL,
            isin TEXT,
            position_percent REAL,
            position_date TEXT
        );
        '''
class Database:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        
    def create_tables(self):
        self.cursor.execute(GAME_TRANSLATION_SCHEMA)
        self.cursor.execute(STEAM_TOP_GAMES_SCHEMA)
        self.cursor.execute(SHORT_POSITIONS_SCHEMA)
        self.cursor.execute(REPORTED_ENTITIES_SCHEMA)
        self.conn.commit()
        
    def get_latest_timestamp(self, table):
        if not table.isidentifier():
            raise ValueError(f"Invalid table name: {table}")

        query = f"SELECT MAX(timestamp) FROM {table}"
        self.cursor.execute(query)
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
            
    def insert_bulk_data(self, input, table='SteamTopGames'):
        ''' 
        Insert multiple rows in a single transaction
        '''
        
        if table == 'SteamTopGames':
            query = '''
            INSERT INTO SteamTopGames (timestamp, place, appid, discount, ccu)
            VALUES (?, ?, ?, ?, ?)
            '''
            data = [(game['timestamp'], game['count'], game['appid'], game['discount'], game['ccu']) for game in input]

        if table == 'ShortPositions':
            query = '''
            INSERT INTO ShortPositions (timestamp, company_name, lei, position_percent, latest_position_date)
            VALUES (?, ?, ?, ?, ?);
            '''
            data = [(row['timestamp'],row['company_name'], row['lei'], row['position_percent'], row['latest_position_date']) for _, row in input.iterrows()]

        self.cursor.executemany(query, data)
        self.conn.commit()
    
    # TODO: Not used currently
    def fetch_current_short_position(self, company_name):
        query = '''
                SELECT * FROM ShortPositions
                WHERE TRIM(company_name) = ?
                ORDER BY timestamp DESC
                LIMIT 1;
                '''
        self.cursor.execute(query, (company_name.strip(),))
        result = self.cursor.fetchone()
        
        if result:
            return {
                'id': result[0],
                'timestamp': result[1],
                'company_name': result[2].strip(),
                'lei': result[3],
                'position_percent': result[4],
                'latest_position_date': result[5]
            }
        else:
            return None   
    
    # TODO: Not used currently    
    def fetch_historical_short_positions(self, company_name):
        query = '''
                SELECT * FROM ShortPositions
                WHERE TRIM(company_name) = ?
                ORDER BY timestamp DESC;
                '''
        self.cursor.execute(query, (company_name.strip(),))
        results = self.cursor.fetchall()
        
        if results:
            return [{
                'id': row[0],
                'timestamp': row[1],
                'company_name': row[2].strip(),
                'lei': row[3],
                'position_percent': row[4],
                'latest_position_date': row[5]
            } for row in results]
        else:
            return None
    
    def close(self):
        self.conn.close()