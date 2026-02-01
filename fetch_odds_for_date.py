import sys
import time
import argparse
import json
from datetime import datetime, timedelta
from database.db import get_connection
from fetch_historical_odds import HistoricalOddsFetcher, ODDS_API_KEY as EXISTING_KEY

ODDS_API_KEY = EXISTING_KEY

def fetch_specific_date(target_date_str, dry_run=False):
    print(f"Fetch odds for date: {target_date_str}")
    
    fetcher = HistoricalOddsFetcher(ODDS_API_KEY)
    conn = get_connection()
    conn.row_factory = lambda cursor, row: {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
    cursor = conn.cursor()
    
    # Get games
    search_pattern = f"{target_date_str}%"
    cursor.execute("""
        SELECT g.id, g.start_time, ht.name as home_team, at.name as away_team
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.id
        JOIN teams at ON g.away_team_id = at.id
        WHERE g.start_time LIKE ?
        ORDER BY g.start_time
    """, (search_pattern,))
    games = cursor.fetchall()
    conn.close()
    
    print(f"Fetching full event list for {target_date_str}...")
    daily_events = fetcher.fetch_events(sport="basketball", date=target_date_str)
    
    now = datetime.now()
    total_stored = 0
    
    for i, game in enumerate(games, 1):
        print(f"\n[{i}/{len(games)}] Processing {game['away_team']} @ {game['home_team']}")
        
        # Match Game
        matching_event = None
        home_team = game['home_team']
        away_team = game['away_team']
        
        for event in daily_events:
            event_home = event.get('home', '')
            event_away = event.get('away', '')
            home_match = (home_team.lower() in event_home.lower() or event_home.lower() in home_team.lower() or fetcher._team_names_match(home_team, event_home))
            away_match = (away_team.lower() in event_away.lower() or event_away.lower() in away_team.lower() or fetcher._team_names_match(away_team, event_away))
            
            if home_match and away_match:
                matching_event = event
                break
        
        if not matching_event:
            print(f"  Warning: Could not match event in API list")
            continue
            
        event_id = matching_event.get('id')
        
        # Parse Time
        try:
            game_start_str = game['start_time']
            if 'T' in game_start_str:
                game_start = datetime.fromisoformat(game_start_str.split('+')[0].split('Z')[0])
            elif ' ' in game_start_str:
                game_start = datetime.strptime(game_start_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
            else:
                game_start = datetime.fromisoformat(game_start_str)
        except Exception:
            continue

        ten_hours_before = game_start - timedelta(hours=10)
        closing_time = game_start - timedelta(minutes=5)
        
        fetch_open = ten_hours_before < now
        fetch_close = closing_time < now
        
        # Helper Parser
        def parse_and_store(odds_payload, snapshot_type, timestamp_iso):
            count = 0
            b_data = odds_payload.get('bookmakers', {})
            
            if not b_data: return 0
            
            # Handle user's mysterious legacy format
            iterator = []
            if isinstance(b_data, dict):
                 iterator = b_data.items()
            else:
                 # Standard API often returns list, maybe we can adapt?
                 print(f"  [Error] Expected dict for bookmakers, got {type(b_data)}")
                 return 0

            for book_name, markets in iterator:
                if not isinstance(markets, list): continue
                for market in markets:
                    if market.get('name') != 'ML': continue
                    odds_arr = market.get('odds', [])
                    if not odds_arr: continue
                    entry = odds_arr[0]
                    
                    try:
                        ho = float(entry.get('home', 0))
                        ao = float(entry.get('away', 0))
                        if ho > 0 and ao > 0:
                            fetcher.store_odds_snapshot(game['id'], book_name, ho, ao, timestamp_iso, snapshot_type)
                            count += 1
                    except: continue
            return count

        # Fetches
        if fetch_open:
            print(f"  Fetching 10h before...")
            ts = int(ten_hours_before.timestamp())
            payload = fetcher.fetch_event_odds(event_id, timestamp=ts)
            if payload:
                cnt = parse_and_store(payload, "10h", ten_hours_before.isoformat())
                print(f"    Stored {cnt} odds snapshots")
                total_stored += cnt
            time.sleep(1)

        if fetch_close:
            print(f"  Fetching closing...")
            ts = int(closing_time.timestamp())
            payload = fetcher.fetch_event_odds(event_id, timestamp=ts)
            if payload:
                cnt = parse_and_store(payload, "closing", closing_time.isoformat())
                print(f"    Stored {cnt} odds snapshots")
                total_stored += cnt
            time.sleep(1)
            
    print(f"Total stored: {total_stored}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, required=True)
    parser.add_argument('--key', type=str)
    args = parser.parse_args()
    if args.key: ODDS_API_KEY = args.key
    fetch_specific_date(args.date)
