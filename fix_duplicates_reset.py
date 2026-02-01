"""
Complete Reset and Re-Migration to fix duplicates.
"""
import sqlite3
import os
from database.db import init_db
from migrate_data import migrate

DB_PATH = "nba_betting.db"

def reset_and_fix():
    print("WARNING: This will wipe games/odds in nba_betting.db and re-sync.")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    tables = ['odds_snapshots', 'edges', 'paper_bets', 'bankroll_history', 'predictions', 'games']
    
    print("Dropping tables...")
    for t in tables:
        c.execute(f"DROP TABLE IF EXISTS {t}")
        
    conn.commit()
    conn.close()
    
    print("Re-initializing Schema...")
    # Open new conn
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    from database.schema import init_schema
    init_schema(c)
    conn.commit()
    
    # Verify
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    print(f"Tables after init: {[r[0] for r in c.fetchall()]}")
    conn.close()
    
    print("starting Migration...")
    migrate()
    
    print("\nDONE. Database should be clean.")

if __name__ == "__main__":
    reset_and_fix()
