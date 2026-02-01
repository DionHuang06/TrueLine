"""
Deep duplicate analysis
"""
import sqlite3
import pandas as pd

def analyze():
    conn = sqlite3.connect("nba_betting.db")
    
    # Check total games per team
    print("Games per Team (Top 10):")
    q1 = """
        SELECT t.name, COUNT(*) as games
        FROM games g
        JOIN teams t ON g.home_team_id = t.id OR g.away_team_id = t.id
        GROUP BY t.name
        ORDER BY games DESC
        LIMIT 10
    """
    print(pd.read_sql(q1, conn))
    
    # Check for same matchup within 24h window
    # Self-join is expensive, do in pandas
    print("\nfetching all games...")
    df = pd.read_sql("SELECT id, start_time, home_team_id, away_team_id FROM games", conn)
    # Fix datetime parsing
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    
    # Inspect Lakers (assuming ID 1 or find it)
    print("\nLakers Games:")
    lakers_id = pd.read_sql("SELECT id FROM teams WHERE name LIKE '%Lakers%'", conn).iloc[0,0]
    lakers_games = df[(df['home_team_id'] == lakers_id) | (df['away_team_id'] == lakers_id)].sort_values('start_time')
    print(lakers_games.head(20))
    print(lakers_games.tail(20))
    
    conn.close()

if __name__ == "__main__":
    analyze()
