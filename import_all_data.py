"""Import all games and odds, then export to text file."""
from fetch_historical_odds import HistoricalOddsFetcher
from database.new_schema import get_connection
from datetime import datetime, timedelta
from config import TEAM_MAPPINGS
import time

ODDS_API_KEY = "6f74ae3112ba76e41120a0ce23042d36a1cc29b6d3a2a1e4a8ffa5f3635d3a96"


def import_games_from_odds_api():
    """Import games from odds API events."""
    fetcher = HistoricalOddsFetcher(ODDS_API_KEY)
    conn = get_connection()
    cursor = conn.cursor()
    
    print("Importing games from odds API...")
    
    # Get events for date range
    start_date = datetime(2025, 10, 1)
    end_date = datetime(2026, 6, 30)
    current_date = start_date
    
    games_imported = 0
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"Fetching events for {date_str}...")
        
        events = fetcher.fetch_events(sport="basketball", date=date_str)
        
        # Filter for NBA events
        nba_events = []
        for event in events:
            league = event.get('league', {})
            if isinstance(league, dict):
                league_name = league.get('name', '')
                if 'nba' in league_name.lower():
                    nba_events.append(event)
        
        # Import NBA games
        for event in nba_events:
            try:
                external_id = str(event.get('id', ''))
                home_team_name = event.get('home', '')
                away_team_name = event.get('away', '')
                event_date = event.get('date', '')
                
                if not home_team_name or not away_team_name or not event_date:
                    continue
                
                # Normalize team names
                home_normalized = TEAM_MAPPINGS.get(home_team_name, home_team_name)
                away_normalized = TEAM_MAPPINGS.get(away_team_name, away_team_name)
                
                # Get team IDs
                cursor.execute("SELECT id FROM teams WHERE name = ?", (home_normalized,))
                home_row = cursor.fetchone()
                if not home_row:
                    # Create team if doesn't exist
                    cursor.execute("INSERT INTO teams (name) VALUES (?)", (home_normalized,))
                    home_team_id = cursor.lastrowid
                else:
                    home_team_id = home_row['id']
                
                cursor.execute("SELECT id FROM teams WHERE name = ?", (away_normalized,))
                away_row = cursor.fetchone()
                if not away_row:
                    cursor.execute("INSERT INTO teams (name) VALUES (?)", (away_normalized,))
                    away_team_id = cursor.lastrowid
                else:
                    away_team_id = away_row['id']
                
                # Check if game exists
                cursor.execute("SELECT id FROM games WHERE external_id = ?", (external_id,))
                if cursor.fetchone():
                    continue
                
                # Determine season
                try:
                    if 'T' in event_date:
                        game_dt = datetime.fromisoformat(event_date.split('+')[0].split('Z')[0])
                    else:
                        game_dt = datetime.strptime(event_date, '%Y-%m-%d')
                    
                    if game_dt.month >= 10:
                        season = f"{game_dt.year}-{game_dt.year + 1}"
                    else:
                        season = f"{game_dt.year - 1}-{game_dt.year}"
                except:
                    season = "2025-2026"
                
                # Insert game
                cursor.execute("""
                    INSERT INTO games 
                    (external_id, start_time, home_team_id, away_team_id, status, season)
                    VALUES (?, ?, ?, ?, 'SCHEDULED', ?)
                """, (external_id, event_date, home_team_id, away_team_id, season))
                
                games_imported += 1
                
            except Exception as e:
                print(f"  Error importing game: {e}")
                continue
        
        conn.commit()
        current_date += timedelta(days=1)
        time.sleep(0.5)  # Rate limiting
    
    conn.close()
    print(f"\nImported {games_imported} games")
    return games_imported


def fetch_all_odds():
    """Fetch odds for all games in database."""
    fetcher = HistoricalOddsFetcher(ODDS_API_KEY)
    conn = get_connection()
    conn.row_factory = lambda cursor, row: {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
    cursor = conn.cursor()
    
    # Get all FINAL games
    cursor.execute("""
        SELECT g.id, g.start_time, g.external_id,
               ht.name as home_team, at.name as away_team
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.id
        JOIN teams at ON g.away_team_id = at.id
        WHERE g.status = 'FINAL'
        ORDER BY g.start_time
    """)
    
    games = cursor.fetchall()
    conn.close()
    
    if not games:
        print("No FINAL games found. Fetching odds for SCHEDULED games...")
        conn = get_connection()
        conn.row_factory = lambda cursor, row: {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
        cursor = conn.cursor()
        cursor.execute("""
            SELECT g.id, g.start_time, g.external_id,
                   ht.name as home_team, at.name as away_team
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE g.status = 'SCHEDULED'
            ORDER BY g.start_time
            LIMIT 100
        """)
        games = cursor.fetchall()
        conn.close()
    
    print(f"\nFetching odds for {len(games)} games...")
    
    total_10h = 0
    total_closing = 0
    
    for i, game in enumerate(games, 1):
        print(f"\n[{i}/{len(games)}] {game['away_team']} @ {game['home_team']}")
        
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
            continue
        
        # 10h before
        ten_hours_before = game_start - timedelta(hours=10)
        stored_10h = fetcher.process_game_odds(game, ten_hours_before, "10h")
        total_10h += stored_10h
        print(f"  10h odds: {stored_10h} snapshots")
        time.sleep(2)
        
        # Closing line
        closing_time = game_start - timedelta(minutes=5)
        stored_closing = fetcher.process_game_odds(game, closing_time, "closing")
        total_closing += stored_closing
        print(f"  Closing odds: {stored_closing} snapshots")
        time.sleep(2)
    
    print(f"\n{'='*80}")
    print(f"ODDS FETCH SUMMARY:")
    print(f"  10h-before odds: {total_10h} snapshots")
    print(f"  Closing line odds: {total_closing} snapshots")
    print(f"  Total: {total_10h + total_closing} snapshots")
    print(f"{'='*80}")


if __name__ == "__main__":
    print("=" * 80)
    print("IMPORTING ALL DATA")
    print("=" * 80)
    
    # Step 1: Import games
    games_count = import_games_from_odds_api()
    
    # Step 2: Fetch odds
    if games_count > 0:
        fetch_all_odds()
    else:
        print("\nNo games imported. Cannot fetch odds.")
    
    print("\n" + "=" * 80)
    print("IMPORT COMPLETE")
    print("=" * 80)
    print("\nRun 'python database_info.py' to view database information")
