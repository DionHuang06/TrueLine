"""
Check info of nba_betting.db tables
"""
import sqlite3

def check():
    conn = sqlite3.connect("nba_betting.db")
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cursor.fetchall()]
    print(f"Tables: {tables}")
    
    for t in tables:
        if "odds" in t:
            print(f"\nSchema for {t}:")
            cursor.execute(f"PRAGMA table_info({t})")
            for c in cursor.fetchall():
                print(c)
    conn.close()

if __name__ == "__main__":
    check()
