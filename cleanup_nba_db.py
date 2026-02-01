"""
Cleanup database to keep ONLY verified NBA games.
Safeguards Nov/Dec data by default (since we couldn't filter it).
"""
import sqlite3
from pathlib import Path

DB_PATH = Path("isportsapi.db")
NBA_LEAGUE_ID = 111

def cleanup_db(delete_raw_months=False):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Analyze before cleanup:")
    cursor.execute("SELECT COUNT(*) as c FROM games")
    total = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT COUNT(*) as c FROM games WHERE league_id = {NBA_LEAGUE_ID}")
    nba = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) as c FROM games WHERE league_id IS NULL")
    raw = cursor.fetchone()[0]
    
    print(f"  Total Games: {total}")
    print(f"  Verified NBA: {nba}")
    print(f"  Unfiltered/Raw: {raw}")
    print("-" * 30)
    
    if delete_raw_months:
        print("WARNING: Deleting ALL non-NBA data (including Nov/Dec raw data)...")
        # Delete games that are NOT NBA (including NULLs)
        cursor.execute(f"DELETE FROM moneyline_odds WHERE match_id IN (SELECT match_id FROM games WHERE league_id != {NBA_LEAGUE_ID} OR league_id IS NULL)")
        cursor.execute(f"DELETE FROM games WHERE league_id != {NBA_LEAGUE_ID} OR league_id IS NULL")
    else:
        print("Cleaning up verified non-NBA games only (January)...")
        print("Keeping Nov/Dec raw data safe.")
        # Only delete games we KNOW are not NBA (league_id is not NULL and not 111)
        cursor.execute(f"DELETE FROM moneyline_odds WHERE match_id IN (SELECT match_id FROM games WHERE league_id != {NBA_LEAGUE_ID} AND league_id IS NOT NULL)")
        cursor.execute(f"DELETE FROM games WHERE league_id != {NBA_LEAGUE_ID} AND league_id IS NOT NULL")
        
    conn.commit()
    deleted = conn.total_changes
    print(f"\nCleanup complete. Deleted {deleted} records.")
    
    # Final stats
    cursor.execute("SELECT COUNT(*) as c FROM games")
    new_total = cursor.fetchone()[0]
    print(f"Remaining Games: {new_total}")
    
    conn.close()

if __name__ == "__main__":
    import sys
    # Usage: python cleanup_nba_db.py [--delete-all]
    delete_all = "--delete-all" in sys.argv
    cleanup_db(delete_all)
