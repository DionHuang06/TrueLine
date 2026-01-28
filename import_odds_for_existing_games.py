"""Fetch odds for games already in the database."""
from fetch_historical_odds import HistoricalOddsFetcher
from database.new_schema import get_connection
from datetime import datetime, timedelta
import time

ODDS_API_KEY = "6f74ae3112ba76e41120a0ce23042d36a1cc29b6d3a2a1e4a8ffa5f3635d3a96"


def fetch_odds_for_existing_games(limit=None):
    """Fetch odds for games already in database."""
    fetcher = HistoricalOddsFetcher(ODDS_API_KEY)
    conn = get_connection()
    conn.row_factory = lambda cursor, row: {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
    cursor = conn.cursor()
    
    # Get games (prioritize FINAL games, then SCHEDULED)
    query = """
        SELECT g.id, g.start_time, g.external_id,
               ht.name as home_team, at.name as away_team, g.status
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.id
        JOIN teams at ON g.away_team_id = at.id
        WHERE g.status IN ('FINAL', 'SCHEDULED')
        ORDER BY 
            CASE WHEN g.status = 'FINAL' THEN 0 ELSE 1 END,
            g.start_time
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    games = cursor.fetchall()
    conn.close()
    
    print(f"Found {len(games)} games to process")
    print("=" * 80)
    
    total_10h = 0
    total_closing = 0
    skipped = 0
    
    for i, game in enumerate(games, 1):
        print(f"\n[{i}/{len(games)}] {game['away_team']} @ {game['home_team']}")
        print(f"  Status: {game['status']}, Date: {game['start_time'][:10]}")
        
        try:
            game_start_str = game['start_time']
            if 'T' in game_start_str:
                game_start = datetime.fromisoformat(game_start_str.split('+')[0].split('Z')[0])
            elif ' ' in game_start_str:
                game_start = datetime.strptime(game_start_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
            else:
                game_start = datetime.fromisoformat(game_start_str)
        except Exception as e:
            print(f"  Error parsing time: {e}")
            skipped += 1
            continue
        
        # Check if we already have odds for this game
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM odds 
            WHERE game_id = ? AND snapshot_type IN ('10h', 'closing')
        """, (game['id'],))
        existing_odds = cursor.fetchone()['count']
        conn.close()
        
        if existing_odds >= 2:
            print(f"  Already has {existing_odds} odds snapshots, skipping...")
            continue
        
        # 10h before
        ten_hours_before = game_start - timedelta(hours=10)
        print(f"  Fetching 10h before odds ({ten_hours_before.strftime('%Y-%m-%d %H:%M')})...")
        stored_10h = fetcher.process_game_odds(game, ten_hours_before, "10h")
        total_10h += stored_10h
        print(f"    Stored {stored_10h} snapshots")
        time.sleep(2)
        
        # Closing line
        closing_time = game_start - timedelta(minutes=5)
        print(f"  Fetching closing line odds ({closing_time.strftime('%Y-%m-%d %H:%M')})...")
        stored_closing = fetcher.process_game_odds(game, closing_time, "closing")
        total_closing += stored_closing
        print(f"    Stored {stored_closing} snapshots")
        time.sleep(2)
    
    print(f"\n{'='*80}")
    print(f"SUMMARY:")
    print(f"  Games processed: {len(games)}")
    print(f"  Games skipped: {skipped}")
    print(f"  10h-before odds: {total_10h} snapshots")
    print(f"  Closing line odds: {total_closing} snapshots")
    print(f"  Total stored: {total_10h + total_closing} snapshots")
    print(f"{'='*80}")


if __name__ == "__main__":
    import sys
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if limit:
        print(f"Processing first {limit} games...")
    fetch_odds_for_existing_games(limit=limit)
