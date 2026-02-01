"""
Analyze retrieved iSportsAPI data for quality and consistency.
Checks for:
1. Valid odds ranges (e.g. no negative odds, reasonable values)
2. Odds movement (opening vs closing)
3. Data volume per day
4. Bookmaker coverage
"""

import sqlite3
import pandas as pd
from pathlib import Path
from tabulate import tabulate

DB_PATH = Path("isportsapi.db")

def analyze_data():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Volume Stats
    print("\n=== DATA VOLUME CHECK ===")
    query_volume = """
        SELECT 
            g.game_date, 
            COUNT(DISTINCT g.match_id) as games,
            COUNT(m.id) as odds_entries
        FROM games g
        JOIN moneyline_odds m ON g.match_id = m.match_id
        GROUP BY g.game_date
        ORDER BY g.game_date
    """
    df_volume = pd.read_sql_query(query_volume, conn)
    print(tabulate(df_volume, headers='keys', tablefmt='psql', showindex=False))
    
    # 2. Odds Value Check
    print("\n=== ODDS VALUE INTEGRITY CHECK ===")
    query_integrity = """
        SELECT 
            MIN(opening_home) as min_open_home, MAX(opening_home) as max_open_home,
            MIN(closing_home) as min_close_home, MAX(closing_home) as max_close_home,
            COUNT(*) as total_records,
            SUM(CASE WHEN opening_home < 1.01 THEN 1 ELSE 0 END) as suspicious_low,
            SUM(CASE WHEN opening_home > 100 THEN 1 ELSE 0 END) as suspicious_high
        FROM moneyline_odds
    """
    df_integrity = pd.read_sql_query(query_integrity, conn)
    print(tabulate(df_integrity, headers='keys', tablefmt='psql', showindex=False))
    
    # 3. Sample Games with Odds Movement
    print("\n=== SAMPLE GAMES WITH ODDS MOVEMENT ===")
    query_movement = """
        SELECT 
            g.game_date,
            g.match_id,
            b.name as bookmaker,
            m.opening_home, m.opening_away,
            m.closing_home, m.closing_away,
            ROUND(ABS(m.closing_home - m.opening_home), 3) as movement_diff
        FROM moneyline_odds m
        JOIN games g ON m.match_id = g.match_id
        JOIN bookmakers b ON m.bookmaker_id = b.id
        WHERE movement_diff > 0.05
        ORDER BY g.game_date DESC, movement_diff DESC
        LIMIT 10
    """
    df_movement = pd.read_sql_query(query_movement, conn)
    print(tabulate(df_movement, headers='keys', tablefmt='psql', showindex=False))

    conn.close()

if __name__ == "__main__":
    analyze_data()
