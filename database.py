# database.py
import sqlite3

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

    def close(self):
        self.conn.close()