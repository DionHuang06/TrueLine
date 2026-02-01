"""
Generate summary of isportsapi.db contents.
"""
import sqlite3
import pandas as pd
from database.isportsapi_schema import get_connection

def summary():
    with get_connection() as conn:
        cursor = conn.cursor()
        
        print("=== iSportsAPI Database Summary ===\n")
        
        # 1. Games Count
        cursor.execute("SELECT COUNT(*) FROM games")
        total_games = cursor.fetchone()[0]
        print(f"Total Games (NBA): {total_games}")
        
        # 2. Date Range
        cursor.execute("SELECT MIN(game_date), MAX(game_date) FROM games")
        start, end = cursor.fetchone()
        print(f"Date Range: {start} to {end}")
        
        # 3. Odds Count
        cursor.execute("SELECT COUNT(*) FROM moneyline_odds")
        total_odds = cursor.fetchone()[0]
        print(f"Total Odds Entries: {total_odds}")
        
        # 4. Coverage
        cursor.execute("SELECT COUNT(DISTINCT match_id) FROM moneyline_odds")
        games_with_odds = cursor.fetchone()[0]
        print(f"Games with Odds: {games_with_odds} ({games_with_odds/total_games:.1%})")
        
        # 5. Team Check (Sample)
        print("\nSample Games (Last 5):")
        df = pd.read_sql("""
            SELECT game_date, home_team, away_team, home_score, away_score 
            FROM games 
            ORDER BY game_date DESC 
            LIMIT 5
        """, conn)
        print(df.to_string(index=False))
        
        print("\nSample Odds (Last 5):")
        df_odds = pd.read_sql("""
            SELECT g.game_date, g.home_team, g.away_team, m.opening_home, m.opening_away
            FROM moneyline_odds m
            JOIN games g ON m.match_id = g.match_id
            ORDER BY g.game_date DESC
            LIMIT 5
        """, conn)
        print(df_odds.to_string(index=False))

if __name__ == "__main__":
    summary()
