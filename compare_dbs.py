"""
Compare contents of isportsapi.db vs nba_betting.db
"""
import sqlite3
import pandas as pd
from pathlib import Path

ISPORTS_DB = "isportsapi.db"
MAIN_DB = "nba_betting.db"

def compare():
    print("=== Database Comparison ===\n")
    
    # 1. Check iSportsAPI DB
    if not Path(ISPORTS_DB).exists():
        print(f"ERROR: {ISPORTS_DB} not found!")
        return
        
    conn_iso = sqlite3.connect(ISPORTS_DB)
    c_iso = conn_iso.cursor()
    c_iso.execute("SELECT COUNT(*) FROM games")
    iso_count = c_iso.fetchone()[0]
    c_iso.execute("SELECT MIN(game_date), MAX(game_date) FROM games")
    iso_range = c_iso.fetchone()
    conn_iso.close()
    
    print(f"[iSportsAPI DB] (Historical Source)")
    print(f"  Games: {iso_count}")
    print(f"  Range: {iso_range[0]} to {iso_range[1]}")
    
    # 2. Check Main Betting DB
    if not Path(MAIN_DB).exists():
        print(f"\n[Main DB] {MAIN_DB} does not exist yet.")
        return

    conn_main = sqlite3.connect(MAIN_DB)
    c_main = conn_main.cursor()
    
    # Check tables existence
    c_main.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in c_main.fetchall()]
    print(f"\n[Main DB] Tables: {', '.join(tables)}")
    
    if 'games' in tables:
        c_main.execute("SELECT COUNT(*) FROM games")
        main_count = c_main.fetchone()[0]
        c_main.execute("SELECT MIN(start_time), MAX(start_time) FROM games")
        main_range = c_main.fetchone()
        
        print(f"  Games: {main_count}")
        print(f"  Range: {main_range[0]} to {main_range[1]}")
        
        diff = iso_count - main_count
        print(f"\nDifference: Main DB has {abs(diff)} {'fewer' if diff > 0 else 'more'} games than iSports Source.")
    else:
        print("  'games' table not found in Main DB.")
        
    conn_main.close()

if __name__ == "__main__":
    compare()
