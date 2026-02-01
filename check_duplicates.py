"""
Check for duplicate games in nba_betting.db
"""
import sqlite3
import pandas as pd

DB_PATH = "nba_betting.db"

def check_dupes():
    conn = sqlite3.connect(DB_PATH)
    
    # Check for exact duplicates of (date, home_team, away_team)
    # Note: start_time might differ slightly, so we check date(start_time)
    
    query = """
        SELECT date(start_time) as game_date, home_team_id, away_team_id, COUNT(*) as count, GROUP_CONCAT(id) as ids
        FROM games
        GROUP BY date(start_time), home_team_id, away_team_id
        HAVING count > 1
        ORDER BY count DESC
    """
    
    df = pd.read_sql(query, conn)
    
    if not df.empty:
        print(f"Found {len(df)} duplicate groups!")
        print(f"Total duplicate extra records: {df['count'].sum() - len(df)}")
        print("\nSample Duplicates:")
        print(df.head(10))
        
        # Check if they have different data (scores?)
        first_group = df.iloc[0]
        ids = first_group['ids'].split(',')
        print(f"\nInspecting IDs for first group: {ids}")
        
        q2 = f"SELECT * FROM games WHERE id IN ({','.join(ids)})"
        details = pd.read_sql(q2, conn)
        print(details)
    else:
        print("No duplicates found based on (Date, Home, Away).")
        
    conn.close()

if __name__ == "__main__":
    check_dupes()
