import sqlite3
import pandas as pd
from database.db import get_connection

def check_games():
    conn = get_connection()
    
    # Check games specifically for Feb 1
    # Note: timestamps might be '2026-02-01T...' or '2026-02-01 ...'
    query = """
        SELECT id, start_time, home_team_id, away_team_id 
        FROM games 
        WHERE start_time LIKE '2026-02-01%'
        ORDER BY start_time
    """
    df = pd.read_sql(query, conn)
    
    if df.empty:
        print("No games found specifically matching '2026-02-01%'")
        # Try finding games that might fall on this day UTC wise
        print("Checking surrounding days...")
        query_wide = "SELECT id, start_time FROM games WHERE start_time BETWEEN '2026-01-31' AND '2026-02-03'"
        df_wide = pd.read_sql(query_wide, conn)
        print(df_wide)
    else:
        print(f"Found {len(df)} games for Feb 1:")
        for _, row in df.iterrows():
            print(f"ID: {row['id']}, Start: {row['start_time']}")
    
    conn.close()

if __name__ == "__main__":
    check_games()
