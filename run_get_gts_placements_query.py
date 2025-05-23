\
import sqlite3
from datetime import datetime, timedelta

# Database name
DB_NAME = "steam_top_games.db"

# Parameters for the query (matching the SQL file's example)
GAME_NAME_TO_QUERY = "Kingdom Come: Deliverance II"
# The reference date used in the SQL query to calculate 90 days prior
REFERENCE_DATE_STR_SQL = "2025-05-23 00:00:00"

# The SQL query, adapted to use Python parameters
# Note: In Python, we'll calculate the threshold date string directly
# or pass the reference date for SQL's strftime to handle.
# For closer replication of the SQL file's logic where strftime does the date math:
SQL_QUERY = """
WITH GameAppID AS (
    SELECT appid
    FROM GameTranslation
    WHERE LOWER(game_name) = LOWER(?) -- Parameter 1: game_name
),
DateFilteredPlacements AS (
    SELECT
        substr(timestamp, 1, 10) AS game_date,
        AVG(place) AS avg_daily_placement
    FROM SteamTopGames
    WHERE appid = (SELECT appid FROM GameAppID)
      AND timestamp >= strftime('%Y-%m-%d %H', ?, '-90 days') -- Parameter 2: reference_date_str
    GROUP BY game_date
)
SELECT
    game_date,
    avg_daily_placement
FROM DateFilteredPlacements
ORDER BY game_date ASC;
"""

def execute_gts_query():
    """
    Connects to the database, executes the GTS placements query,
    and prints the results.
    """
    conn = None  # Initialize conn to None
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        print(f"Querying placements for: {GAME_NAME_TO_QUERY}")
        print(f"Reference date for -90 days calculation: {REFERENCE_DATE_STR_SQL}")
        
        params = (GAME_NAME_TO_QUERY, REFERENCE_DATE_STR_SQL)
        cursor.execute(SQL_QUERY, params)
        
        rows = cursor.fetchall()
        
        if rows:
            print("\\nResults:")
            print("Date       | Avg. Placement")
            print("-----------|----------------")
            for row in rows:
                print(f"{row[0]} | {row[1]:.2f}")
        else:
            print("No data found for the given game and date range.")
            
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    execute_gts_query()
