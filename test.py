import pandas as pd
import sqlite3
from datetime import datetime, timedelta

# --- Configuration ---
# Use the destination database file
DB_FILENAME = 'steam_top_games.db' 
COMPANY_NAME = 'Embracer Group AB' # Example company

# --- Time window for the query ---
# Based on the current time you provided
now = datetime(2025, 6, 13, 12, 23) 
three_months_ago = now - timedelta(days=91) # Roughly 3 months

# --- Database Connection ---
try:
    conn = sqlite3.connect(DB_FILENAME)

    # The query, adapted for the short_positions_history table
    query = f"""
        SELECT
            event_timestamp,
            position_percent
        FROM
            short_positions_history
        WHERE
            company_name LIKE '{COMPANY_NAME}'
            AND event_timestamp >= (
                SELECT MAX(event_timestamp) 
                FROM short_positions_history 
                WHERE company_name LIKE '{COMPANY_NAME}' AND event_timestamp <= '{three_months_ago.strftime("%Y-%m-%d %H:%M:%S")}'
            )
            AND event_timestamp <= '{now.strftime("%Y-%m-%d %H:%M:%S")}'
        ORDER BY event_timestamp
    """

    print("--- Executing Query ---")
    print(query)
    
    query = f"""
        SELECT
            *
        FROM
            positionHolders
    """
    
    print("--- Executing Query ---")
    print(query)
    
    # Execute the query and load data into a DataFrame
    data = pd.read_sql_query(query, conn)

    print("\n--- Query Results ---")
    print(data)

except sqlite3.Error as e:
    print(f"An error occurred: {e}")
finally:
    if 'conn' in locals() and conn:
        conn.close()