from database.db import get_connection
import pandas as pd

conn = get_connection()
q = """
    SELECT g.id, g.start_time, h.name as home, a.name as away, 
           g.home_score, g.away_score, g.status,
           AVG(CASE WHEN o.snapshot_type = '10h' THEN o.home_odds END) as home_open,
           AVG(CASE WHEN o.snapshot_type = '10h' THEN o.away_odds END) as away_open,
           AVG(CASE WHEN o.snapshot_type = 'closing' THEN o.home_odds END) as home_close,
           AVG(CASE WHEN o.snapshot_type = 'closing' THEN o.away_odds END) as away_close
    FROM games g
    JOIN teams h ON g.home_team_id = h.id
    JOIN teams a ON g.away_team_id = a.id
    LEFT JOIN odds o ON g.id = o.game_id
    GROUP BY g.id, g.start_time, h.name, a.name, g.home_score, g.away_score, g.status
    ORDER BY g.start_time DESC
    LIMIT 5
"""

try:
    df = pd.read_sql(q, conn)
    print("Query successful!")
    print(df)
except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
