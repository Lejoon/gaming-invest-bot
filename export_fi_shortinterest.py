import sqlite3
import csv
from pathlib import Path
from datetime import datetime

DB_PATH = 'steam_top_games.db'
START_DATE = '2025-07-25'
END_DATE = '2025-08-25'

OUTPUT_SHORT = f'ShortPositions_{START_DATE}_{END_DATE}.csv'
OUTPUT_HOLDERS = f'PositionHolders_{START_DATE}_{END_DATE}.csv'

def export_table(conn, table, output_file, date_column='timestamp'):
    query = f"""
        SELECT *
        FROM {table}
        WHERE date({date_column}) >= date(?) AND date({date_column}) <= date(?)
        ORDER BY {date_column} ASC, id ASC
    """
    cur = conn.cursor()
    cur.execute(query, (START_DATE, END_DATE))
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(col_names)
        writer.writerows(rows)
    return len(rows)

def main():
    if not Path(DB_PATH).exists():
        raise SystemExit(f'Database file not found: {DB_PATH}')

    conn = sqlite3.connect(DB_PATH)
    try:
        short_count = export_table(conn, 'ShortPositions', OUTPUT_SHORT)
        holders_count = export_table(conn, 'PositionHolders', OUTPUT_HOLDERS)
    finally:
        conn.close()

    print(f'Exported {short_count} rows to {OUTPUT_SHORT}')
    print(f'Exported {holders_count} rows to {OUTPUT_HOLDERS}')

if __name__ == '__main__':
    main()
