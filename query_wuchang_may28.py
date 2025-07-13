#!/usr/bin/env python3
"""
Query script to find all daily placements for Wuchang: Fallen Feathers on May 28th
"""

import sqlite3
from database import Database

def query_wuchang_may28():
    """Query for Wuchang: Fallen Feathers placements and show entire top list for 2025-05-28 16"""
    
    target_timestamp = '2025-05-28 16'
    
    with Database('steam_top_games.db') as db:
        print(f"=== Complete Top Games List for {target_timestamp} ===")
        
        # Get the complete top list for the specific timestamp
        db.cursor.execute("""
            SELECT stg.place, stg.appid, gt.game_name, stg.discount, stg.ccu
            FROM SteamTopGames stg
            LEFT JOIN GameTranslation gt ON stg.appid = gt.appid
            WHERE stg.timestamp = ?
            ORDER BY stg.place ASC
        """, (target_timestamp,))
        
        top_games = db.cursor.fetchall()
        
        if top_games:
            print(f"Found {len(top_games)} games in the top list for {target_timestamp}:")
            print("=" * 80)
            print(f"{'Rank':<4} {'AppID':<10} {'Game Name':<40} {'Discount':<10} {'CCU':<10}")
            print("=" * 80)
            
            wuchang_found = False
            for place, appid, game_name, discount, ccu in top_games:
                # Highlight Wuchang if found
                if game_name and 'wuchang' in game_name.lower():
                    print(f">>> {place:<4} {appid:<10} {game_name or 'Unknown':<40} {discount or 'N/A':<10} {ccu or 'N/A':<10} <<<")
                    wuchang_found = True
                else:
                    print(f"{place:<4} {appid:<10} {game_name or 'Unknown':<40} {discount or 'N/A':<10} {ccu or 'N/A':<10}")
            
            print("=" * 80)
            if wuchang_found:
                print(">>> Wuchang: Fallen Feathers entries are highlighted with >>> <<<")
            else:
                print("Note: Wuchang: Fallen Feathers not found in this timestamp")
                
        else:
            print(f"No games found for timestamp {target_timestamp}")
            
            # Check what timestamps are available around that date
            print(f"\n=== Available timestamps around 2025-05-28 ===")
            db.cursor.execute("""
                SELECT DISTINCT timestamp
                FROM SteamTopGames 
                WHERE substr(timestamp, 1, 10) = '2025-05-28'
                ORDER BY timestamp
            """)
            
            available_timestamps = db.cursor.fetchall()
            if available_timestamps:
                print("Available timestamps for 2025-05-28:")
                for (ts,) in available_timestamps:
                    print(f"  {ts}")
            else:
                print("No data found for 2025-05-28")
        
        # Also search for Wuchang specifically to show its placements on that date
        print(f"\n=== Wuchang: Fallen Feathers on 2025-05-28 (all hours) ===")
        
        # First find Wuchang's appid
        db.cursor.execute("""
            SELECT appid, game_name FROM GameTranslation 
            WHERE LOWER(game_name) LIKE LOWER('%wuchang%')
        """)
        
        wuchang_games = db.cursor.fetchall()
        if wuchang_games:
            for appid, game_name in wuchang_games:
                print(f"\nGame: {game_name} (AppID: {appid})")
                
                # Get all placements for this game on 2025-05-28
                db.cursor.execute("""
                    SELECT timestamp, place, discount, ccu
                    FROM SteamTopGames 
                    WHERE appid = ? AND substr(timestamp, 1, 10) = '2025-05-28'
                    ORDER BY timestamp
                """, (appid,))
                
                placements = db.cursor.fetchall()
                if placements:
                    print(f"  Placements throughout the day:")
                    for timestamp, place, discount, ccu in placements:
                        marker = " *** TARGET ***" if timestamp == target_timestamp else ""
                        print(f"    {timestamp}: Rank {place}, Discount: {discount or 'N/A'}, CCU: {ccu or 'N/A'}{marker}")
                else:
                    print(f"  No placements found on 2025-05-28")
        else:
            print("No games found matching 'wuchang' in GameTranslation table")

if __name__ == "__main__":
    query_wuchang_may28()
