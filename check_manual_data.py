"""
Check for manual data in nba_betting.db
"""
import sqlite3
import pandas as pd

def check():
    conn = sqlite3.connect("nba_betting.db")
    
    # 1. Manual Odds
    print("Manual Odds:")
    df_manual = pd.read_sql("SELECT * FROM odds_snapshots WHERE book LIKE '%Manual%'", conn)
    print(df_manual)
    
    # 2. Paper Bets
    print("\nPaper Bets:")
    df_bets = pd.read_sql("SELECT * FROM paper_bets", conn)
    print(df_bets)
    
    conn.close()

if __name__ == "__main__":
    check()
