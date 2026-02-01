import sqlite3
import pandas as pd
from database.db import get_connection

def check_game_status():
    conn = get_connection()
    query = """
        SELECT id, status
        FROM games 
        WHERE start_time LIKE '2026-02-01%'
    """
    df = pd.read_sql(query, conn)
    print(df)
    conn.close()

if __name__ == "__main__":
    check_game_status()
