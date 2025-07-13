# database.py
import sqlite3
from datetime import datetime, timedelta

POSITION_HOLDERS_SCHEMA = '''
        CREATE TABLE IF NOT EXISTS PositionHolders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_name TEXT,
            issuer_name TEXT,
            isin TEXT,
            position_percent REAL,
            position_date TEXT,
            timestamp TEXT
        );
    '''

GAME_TRANSLATION_SCHEMA = '''
        CREATE TABLE IF NOT EXISTS GameTranslation (
            appid TEXT PRIMARY KEY,
            game_name TEXT
        );
        '''
        
PS_GAME_TRANSLATION_SCHEMA = '''
    CREATE TABLE IF NOT EXISTS PSGameTranslation (
        ps_id INTEGER PRIMARY KEY,
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
        
PS_TOP_GAMES_SCHEMA = '''
    CREATE TABLE IF NOT EXISTS PSTopGames (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        place INTEGER,
        ps_id TEXT,
        discount TEXT
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
        self.cursor.execute(PS_TOP_GAMES_SCHEMA)
        self.cursor.execute(PS_GAME_TRANSLATION_SCHEMA)
        self.cursor.execute(SHORT_POSITIONS_SCHEMA)
        self.cursor.execute(REPORTED_ENTITIES_SCHEMA)
        self.cursor.execute(POSITION_HOLDERS_SCHEMA)
        self.conn.commit()
        
    def get_latest_timestamp(self, table):
        if not table.isidentifier():
            raise ValueError(f"Invalid table name: {table}")

        query = f"SELECT MAX(timestamp) FROM {table}"
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
    def get_gts_placements(self, game_name):
        """
        Retrieves aggregated GTS placement data for the given game over the last 90 days.
        
        The function:
         1. Finds the appid for the given game name from the GameTranslation table.
         2. Queries SteamTopGames for all records with that appid and a timestamp within the last 90 days.
         3. Aggregates the data by day (using the date portion of the timestamp) and calculates the harmonic mean placement.
         4. Returns a dictionary with keys:
            - "positions": a list of numeric positions for plotting.
            - "aggregated_labels": a list of date strings corresponding to each position.
            - "placements": a list of harmonic mean placement values (as floats) per day.
        If no data is found for the given game name, returns None.
        
        Note: Harmonic mean is used instead of arithmetic mean as it gives more weight to better (lower) ranks,
        which is more appropriate for ranking data.
        """
        # Look up the appid using an exact (case-insensitive) match.
        self.cursor.execute("""
            SELECT appid FROM GameTranslation
            WHERE LOWER(game_name) = LOWER(?)
        """, (game_name,))
        row = self.cursor.fetchone()
        if row is None:
            return None  # No such game found.
        
        appid = row[0]
        
        # Calculate the threshold timestamp: 90 days ago.
        from datetime import datetime, timedelta
        threshold_dt = datetime.now() - timedelta(days=90)
        threshold_str = threshold_dt.strftime('%Y-%m-%d %H')
        
        # Query SteamTopGames for records with this appid from the last 90 days.
        # We extract the date part (YYYY-MM-DD) from the timestamp (which is stored as "YYYY-MM-DD HH").
        # Use harmonic mean for better ranking aggregation (gives more weight to better/lower ranks)
        query = """
            SELECT substr(timestamp, 1, 10) AS date, 
                   COUNT(place) / SUM(1.0 / place) AS harmonic_mean_place
            FROM SteamTopGames
            WHERE appid = ? AND timestamp >= ?
            GROUP BY date
            ORDER BY date ASC
        """
        self.cursor.execute(query, (appid, threshold_str))
        rows = self.cursor.fetchall()
        
        if not rows:
            return None  # No placement data available for the last 30 days.
        
        aggregated_labels = []
        placements = []
        positions = []
        
        for index, (date_label, harmonic_mean_place) in enumerate(rows):
            aggregated_labels.append(date_label)
            placements.append(harmonic_mean_place)
            positions.append(index)
        
        aggregated_data = {
            "positions": positions,
            "aggregated_labels": aggregated_labels,
            "placements": placements
        }
        
        return aggregated_data
    
    def get_gts_placements_with_minmax(self, game_name):
        """
        Retrieves aggregated GTS placement data for the given game over the last 90 days,
        including min, max, and harmonic mean placements per day.
        """
        self.cursor.execute("""
            SELECT appid FROM GameTranslation
            WHERE LOWER(game_name) = LOWER(?)
        """, (game_name,))
        row = self.cursor.fetchone()
        if row is None:
            return None
        
        appid = row[0]
        
        from datetime import datetime, timedelta
        threshold_dt = datetime.now() - timedelta(days=90)
        threshold_str = threshold_dt.strftime('%Y-%m-%d %H')
        
        query = """
            SELECT
                substr(timestamp, 1, 10) AS date,
                COUNT(place) / SUM(1.0 / place) AS harmonic_mean_place,
                MIN(place) AS min_place,
                MAX(place) AS max_place
            FROM SteamTopGames
            WHERE appid = ? AND timestamp >= ?
            GROUP BY date
            ORDER BY date ASC
        """
        self.cursor.execute(query, (appid, threshold_str))
        rows = self.cursor.fetchall()
        
        if not rows:
            return None
        
        aggregated_labels = []
        harmonic_mean_placements = []
        min_placements = []
        max_placements = []
        
        for date_label, harmonic_mean_place, min_place, max_place in rows:
            aggregated_labels.append(date_label)
            harmonic_mean_placements.append(harmonic_mean_place)
            min_placements.append(min_place)
            max_placements.append(max_place)
        
        aggregated_data = {
            "aggregated_labels": aggregated_labels,
            "avg_placements": harmonic_mean_placements,
            "min_placements": min_placements,
            "max_placements": max_placements
        }
        
        return aggregated_data

    def get_last_month_ps_placements(self, game_name):
        """
        Retrieves aggregated GTS placement data for the given game over the last 30 days from PS Store.
        
        The function:
         1. Finds the ps_id for the given game name from the PSGameTranslation table.
         2. Queries PSTopGames for all records with that ps_id and a timestamp within the last 30 days.
         3. Aggregates the data by day (using the date portion of the timestamp) and calculates the average placement.
         4. Returns a dictionary with keys:
            - "positions": a list of numeric positions for plotting.
            - "aggregated_labels": a list of date strings corresponding to each position.
            - "placements": a list of average placement values (as floats) per day.
        If no data is found for the given game name, returns None.
        """
        # Look up the ps_id using an exact (case-insensitive) match.
        self.cursor.execute("""
            SELECT ps_id FROM PSGameTranslation
            WHERE LOWER(game_name) = LOWER(?)
        """, (game_name,))
        row = self.cursor.fetchone()
        if row is None:
            return None  # No such game found.
        
        ps_id = row[0]
        
        # Calculate the threshold timestamp: 30 days ago.
        from datetime import datetime, timedelta
        threshold_dt = datetime.now() - timedelta(days=90) # Changed to 90 days for consistency with steam
        threshold_str = threshold_dt.strftime('%Y-%m-%d %H')
        
        # Query PSTopGames for records with this ps_id from the last 30 days.
        # We extract the date part (YYYY-MM-DD) from the timestamp (which is stored as "YYYY-MM-DD HH").
        query = """
            SELECT substr(timestamp, 1, 10) AS date, AVG(place) AS avg_place
            FROM PSTopGames
            WHERE ps_id = ? AND timestamp >= ?
            GROUP BY date
            ORDER BY date ASC
        """
        self.cursor.execute(query, (ps_id, threshold_str))
        rows = self.cursor.fetchall()
        
        if not rows:
            return None  # No placement data available for the last 30 days.
        
        aggregated_labels = []
        placements = []
        positions = []
        
        for index, (date_label, avg_place) in enumerate(rows):
            aggregated_labels.append(date_label)
            placements.append(avg_place)
            positions.append(index)
        
        aggregated_data = {
            "positions": positions,
            "aggregated_labels": aggregated_labels,
            "placements": placements
        }
        
        return aggregated_data

    def get_yesterday_top_games(self, timestamp, table='SteamTopGames'):
        if not timestamp:
            return {}
            
        try:
            current_dt = datetime.strptime(timestamp, '%Y-%m-%d %H')
        except ValueError:
            return {}

        if table == 'SteamTopGames':
            # Calculate the date for yesterday and always set the hour to 21
            yesterday_date = (current_dt - timedelta(days=1)).strftime('%Y-%m-%d')
            yesterday_timestamp_21 = f"{yesterday_date} 21"
            
            self.cursor.execute('''
            SELECT place, appid FROM SteamTopGames
            WHERE timestamp = ?
            ''', (yesterday_timestamp_21,))
            
            rows = self.cursor.fetchall()
            return {appid: place for place, appid in rows}
    
        elif table == 'PSTopGames':
            yesterday_date_str = (current_dt - timedelta(days=1)).strftime('%Y-%m-%d')
            
            # Find the latest timestamp for yesterday's date in PSTopGames
            query_latest_yesterday_ts = """
                SELECT MAX(timestamp) 
                FROM PSTopGames
                WHERE SUBSTR(timestamp, 1, 10) = ?
            """
            self.cursor.execute(query_latest_yesterday_ts, (yesterday_date_str,))
            latest_yesterday_timestamp_row = self.cursor.fetchone()

            if not latest_yesterday_timestamp_row or not latest_yesterday_timestamp_row[0]:
                return {} # No data found for yesterday

            latest_yesterday_timestamp = latest_yesterday_timestamp_row[0]

            # Fetch all games for that specific latest timestamp
            self.cursor.execute('''
                SELECT place, ps_id FROM PSTopGames
                WHERE timestamp = ?
            ''', (latest_yesterday_timestamp,))
            
            rows = self.cursor.fetchall()
            return {ps_id: place for place, ps_id in rows}
        
        return {}
    
    def get_last_week_ranks(self, timestamp, current_top_appids):
        # Calculate the date for 7 days ago and set the hour to 21
        last_week_date = (datetime.strptime(timestamp, '%Y-%m-%d %H') - timedelta(days=7)).strftime('%Y-%m-%d')
        last_week_timestamp_21 = f"{last_week_date} 21"

        placeholders = ','.join(['?'] * len(current_top_appids))
        query = f'''
            SELECT appid, GROUP_CONCAT(place) AS ranks
            FROM SteamTopGames
            WHERE timestamp BETWEEN ? AND ? AND appid IN ({placeholders})
            GROUP BY appid
        '''

        self.cursor.execute(query, (last_week_timestamp_21, timestamp, *current_top_appids))

        rows = self.cursor.fetchall()

        last_week_ranks = {}
        for appid, ranks_str in rows:
            ranks = [int(rank) for rank in ranks_str.split(',')]
            last_week_ranks[appid] = ranks

        return last_week_ranks

    def update_appid(self, appid, title):
        self.cursor.execute("SELECT game_name FROM GameTranslation WHERE appid = ?", (appid,))
        if self.cursor.fetchone() is None:
            self.cursor.execute("INSERT INTO GameTranslation (appid, game_name) VALUES (?, ?)", (appid, title))
            self.conn.commit()
            
    def update_ps_appid(self, ps_id, game_name):
        self.cursor.execute("SELECT game_name FROM PsGameTranslation WHERE ps_id = ?", (ps_id,))
        result = self.cursor.fetchone()
        if result is None:
            self.cursor.execute("INSERT INTO PsGameTranslation (ps_id, game_name) VALUES (?, ?)",
                              (ps_id, game_name))
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

        elif table == 'PSTopGames':
            query = '''
            INSERT INTO PSTopGames (timestamp, place, ps_id, discount)
            VALUES (?, ?, ?, ?)
            '''
            data = [(game['timestamp'], game['place'], game['ps_id'], game['discount']) for game in input]


        elif table == 'ShortPositions':
            query = '''
            INSERT INTO ShortPositions (timestamp, company_name, lei, position_percent, latest_position_date)
            VALUES (?, ?, ?, ?, ?);
            '''
            data = [(row['timestamp'],row['company_name'], row['lei'], row['position_percent'], row['latest_position_date']) for _, row in input.iterrows()]
            
        elif table == 'PositionHolders':
            query = '''
            INSERT INTO PositionHolders (entity_name, issuer_name, isin, position_percent, position_date, timestamp)
            VALUES (?, ?, ?, ?, ?, ?);
            '''
            data = [(row['entity_name'], row['issuer_name'], row['isin'], row['position_percent'], row['position_date'], row['timestamp']) for _, row in input.iterrows()]

        else:
            raise ValueError(f"Invalid table name: {table}")

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

    def get_gts_placements_with_minmax_delta_days(self, game_name, release_date_str):
        """
        Retrieves aggregated GTS placement data for the given game over the last 90 days,
        including min, max, and harmonic mean placements per day, and computes delta days to release.
        release_date_str: string in 'YYYY-MM-DD' format (e.g. '2025-07-24')
        Returns a dict with keys:
            - delta_days: list of ints (negative, days to release)
            - avg_placements: list of floats
            - min_placements: list of ints
            - max_placements: list of ints
        """
        self.cursor.execute("""
            SELECT appid FROM GameTranslation
            WHERE LOWER(game_name) = LOWER(?)
        """, (game_name,))
        row = self.cursor.fetchone()
        if row is None:
            return None
        appid = row[0]
        from datetime import datetime, timedelta
        threshold_dt = datetime.now() - timedelta(days=90)
        threshold_str = threshold_dt.strftime('%Y-%m-%d %H')
        query = """
            SELECT
                substr(timestamp, 1, 10) AS date,
                COUNT(place) / SUM(1.0 / place) AS harmonic_mean_place,
                MIN(place) AS min_place,
                MAX(place) AS max_place
            FROM SteamTopGames
            WHERE appid = ? AND timestamp >= ?
            GROUP BY date
            ORDER BY date ASC
        """
        self.cursor.execute(query, (appid, threshold_str))
        rows = self.cursor.fetchall()
        if not rows:
            return None
        release_date = datetime.strptime(release_date_str, '%Y-%m-%d')
        delta_days = []
        avg_placements = []
        min_placements = []
        max_placements = []
        for date_label, harmonic_mean_place, min_place, max_place in rows:
            date_obj = datetime.strptime(date_label, '%Y-%m-%d')
            delta = (date_obj - release_date).days
            delta_days.append(delta)
            avg_placements.append(harmonic_mean_place)
            min_placements.append(min_place)
            max_placements.append(max_place)
        aggregated_data = {
            "delta_days": delta_days,
            "avg_placements": avg_placements,
            "min_placements": min_placements,
            "max_placements": max_placements
        }
        return aggregated_data
    
    def get_multiple_games_placements_delta_days(self, games_info, days_before_release=90):
        """
        Retrieves aggregated GTS placement data for multiple games with delta days to release.
        For each game, gets data from specified days before release to release day.
        games_info: list of dicts with keys 'game_name' and 'release_date_str'
        days_before_release: number of days before release to look back (default: 90)
        Returns a dict with game names as keys and placement data as values.
        """
        results = {}
        
        for game_info in games_info:
            game_name = game_info['game_name']
            release_date_str = game_info['release_date_str']
            
            # Use the new reusable function
            game_data = self.get_game_placements_delta_days(game_name, release_date_str, days_before_release)
            if game_data:
                results[game_name] = game_data
        
        return results
    
    def get_game_placements_delta_days(self, game_name, release_date_str, days_before_release=90):
        """
        Retrieves aggregated GTS placement data for a single game with delta days to release.
        
        Args:
            game_name: Name of the game to look up
            release_date_str: Release date in 'YYYY-MM-DD' format
            days_before_release: Number of days before release to look back (default: 90)
            
        Returns:
            Dict with keys:
                - delta_days: list of ints (negative, days to release)
                - avg_placements: list of floats (harmonic mean placements)
            Returns None if no data found.
        """
        # Look up the appid
        self.cursor.execute("""
            SELECT appid FROM GameTranslation
            WHERE LOWER(game_name) = LOWER(?)
        """, (game_name,))
        row = self.cursor.fetchone()
        if row is None:
            return None
            
        appid = row[0]
        release_date = datetime.strptime(release_date_str, '%Y-%m-%d')
        
        # Calculate date range: specified days before release to release day
        start_date = release_date - timedelta(days=days_before_release)
        end_date = release_date
        
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        query = """
            SELECT
                substr(timestamp, 1, 10) AS date,
                COUNT(place) / SUM(1.0 / place) AS harmonic_mean_place
            FROM SteamTopGames
            WHERE appid = ? AND substr(timestamp, 1, 10) >= ? AND substr(timestamp, 1, 10) <= ?
            GROUP BY date
            ORDER BY date ASC
        """
        self.cursor.execute(query, (appid, start_date_str, end_date_str))
        rows = self.cursor.fetchall()
        
        if not rows:
            return None
            
        delta_days = []
        avg_placements = []
        
        for date_label, harmonic_mean_place in rows:
            date_obj = datetime.strptime(date_label, '%Y-%m-%d')
            delta = (date_obj - release_date).days
            delta_days.append(delta)
            avg_placements.append(harmonic_mean_place)
        
        return {
            "delta_days": delta_days,
            "avg_placements": avg_placements
        }