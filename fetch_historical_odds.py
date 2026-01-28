"""Fetch historical odds from Odds API (odds-api.io) for 2025-2026 NBA season.
Gets odds at two times: 10 hours before tipoff and closing line."""
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from database.db import get_connection
from config import TEAM_MAPPINGS
import time


# API Key provided by user
ODDS_API_KEY = "6f74ae3112ba76e41120a0ce23042d36a1cc29b6d3a2a1e4a8ffa5f3635d3a96"

# Odds API.io base URL (v3)
ODDS_API_BASE = "https://api.odds-api.io/v3"


class HistoricalOddsFetcher:
    """Fetch historical odds from Odds API (odds-api.io)."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = ODDS_API_BASE
    
    def _get_game_id_by_teams(self, conn, home_team: str, away_team: str, 
                               start_time: str) -> Optional[int]:
        """Find game ID by team names and approximate start time."""
        cursor = conn.cursor()
        
        # Normalize team names
        home_normalized = TEAM_MAPPINGS.get(home_team, home_team)
        away_normalized = TEAM_MAPPINGS.get(away_team, away_team)
        
        # Look up team IDs
        cursor.execute("SELECT id FROM teams WHERE name = ?", (home_normalized,))
        home_row = cursor.fetchone()
        
        cursor.execute("SELECT id FROM teams WHERE name = ?", (away_normalized,))
        away_row = cursor.fetchone()
        
        if not home_row or not away_row:
            return None
        
        # Find matching game (within same day)
        game_date = start_time[:10]  # YYYY-MM-DD
        cursor.execute("""
            SELECT id FROM games 
            WHERE home_team_id = ? AND away_team_id = ?
            AND date(start_time) = date(?)
        """, (home_row['id'], away_row['id'], game_date))
        
        row = cursor.fetchone()
        return row['id'] if row else None
    
    def fetch_bookmakers(self) -> List[Dict]:
        """Fetch list of available bookmakers from the API."""
        if not self.api_key:
            print("Error: API key not set.")
            return []
        
        endpoint = f"{self.base_url}/bookmakers"
        params = {"apiKey": self.api_key}
        
        try:
            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'bookmakers' in data:
                return data['bookmakers']
            elif isinstance(data, dict) and 'data' in data:
                return data['data']
            else:
                return []
            
        except requests.RequestException as e:
            print(f"Error fetching bookmakers: {e}")
            return []
    
    def fetch_events(self, sport: str = "basketball", date: Optional[str] = None) -> List[Dict]:
        """
        Fetch events (games) from Odds API.
        
        Args:
            sport: Sport key (default: basketball)
            date: Date in format YYYY-MM-DD (optional, for historical)
        
        Returns:
            List of event dictionaries
        """
        if not self.api_key:
            print("Error: API key not set.")
            return []
        
        params = {
            "apiKey": self.api_key,
            "sport": sport
        }
        
        if date:
            params["date"] = date
        
        endpoint = f"{self.base_url}/events"
        
        try:
            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            # API might return events in different formats
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'events' in data:
                return data['events']
            elif isinstance(data, dict) and 'data' in data:
                return data['data']
            else:
                return []
            
        except requests.RequestException as e:
            print(f"Error fetching events: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response: {e.response.text[:200]}")
            return []
    
    def fetch_event_odds(self, event_id: str, bookmakers: Optional[str] = None, 
                        timestamp: Optional[int] = None) -> Optional[Dict]:
        """
        Fetch odds for a specific event from Odds API.
        
        Args:
            event_id: Event ID from the API
            bookmakers: Comma-separated list of bookmaker names (required, max 30)
            timestamp: Unix timestamp for historical odds (optional)
        
        Returns:
            Event dictionary with odds, or None if error
        """
        if not self.api_key:
            print("Error: API key not set.")
            return None
        
        # Default bookmakers if not provided
        # API plan may limit number of bookmakers - use only allowed ones
        if not bookmakers:
            bookmakers = "FanDuel"  # Start with just FanDuel (API allows max 2)
        
        params = {
            "apiKey": self.api_key,
            "eventId": event_id,
            "bookmakers": bookmakers
        }
        
        if timestamp:
            params["timestamp"] = timestamp
        
        endpoint = f"{self.base_url}/odds"
        
        try:
            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            print(f"Error fetching event odds: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response: {e.response.text[:200]}")
            return None
    
    def fetch_historical_odds(self, sport: str = "basketball", 
                               date: Optional[str] = None,
                               timestamp: Optional[int] = None) -> List[Dict]:
        """
        Fetch historical odds by getting events for a date and then fetching odds.
        
        Args:
            sport: Sport key (default: basketball)
            date: Date in format YYYY-MM-DD
            timestamp: Unix timestamp for specific historical time
        
        Returns:
            List of event dictionaries with odds
        """
        if not date and not timestamp:
            print("Error: Either date or timestamp required.")
            return []
        
        # Convert timestamp to date if needed
        if timestamp and not date:
            date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
        
        # First, get events for this date
        events = self.fetch_events(sport=sport, date=date)
        
        if not events:
            return []
        
        # For each event, fetch odds at the specific timestamp
        events_with_odds = []
        for event in events:
            event_id = event.get('id') or event.get('eventId')
            if not event_id:
                continue
            
            # Fetch odds for this event at the timestamp
            odds_data = self.fetch_event_odds(event_id, timestamp=timestamp)
            if odds_data:
                events_with_odds.append(odds_data)
            
            # Rate limiting
            time.sleep(0.5)
        
        return events_with_odds
    
    def fetch_current_odds(self, sport: str = "basketball") -> List[Dict]:
        """
        Fetch current (live) odds from Odds API.
        Useful for testing API connectivity.
        """
        return self.fetch_events(sport=sport)
    
    def store_odds_snapshot(self, game_id: int, book: str, home_odds: float, 
                           away_odds: float, snapshot_time: str, snapshot_type: str):
        """
        Store odds snapshot in database using new schema.
        
        Args:
            game_id: Game ID
            book: Bookmaker name
            home_odds: Home team decimal odds
            away_odds: Away team decimal odds
            snapshot_time: When the odds were captured (ISO format)
            snapshot_type: Either '10h' or 'closing'
        """
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO odds 
            (game_id, book, snapshot_type, snapshot_time, home_odds, away_odds)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (game_id, book, snapshot_type, snapshot_time, home_odds, away_odds))
        
        conn.commit()
        conn.close()
    
    def process_game_odds(self, game: Dict, target_time: datetime, 
                         snapshot_type: str) -> int:
        """
        Process odds for a single game at a specific time.
        
        Args:
            game: Game dict from database
            target_time: When we want the odds (10h before or closing)
            snapshot_type: "10h" or "closing"
        
        Returns:
            Number of odds snapshots stored
        """
        stored = 0
        
        # Convert target_time to Unix timestamp
        target_timestamp = int(target_time.timestamp())
        
        # Fetch events for the target date
        target_date = target_time.strftime("%Y-%m-%d")
        events = self.fetch_events(sport="basketball", date=target_date)
        
        if not events:
            print(f"    No events found for date {target_date}")
            return 0
        
        # Find matching event
        home_team = game['home_team']
        away_team = game['away_team']
        
        matching_event = None
        for event in events:
            # Filter for NBA events only
            league = event.get('league', {})
            if isinstance(league, dict):
                league_name = league.get('name', '')
                if 'nba' not in league_name.lower():
                    continue
            
            # Odds API.io uses 'home'/'away'
            event_home = event.get('home', '')
            event_away = event.get('away', '')
            
            # Try to match teams (handle variations in team names)
            home_match = (home_team.lower() in event_home.lower() or 
                         event_home.lower() in home_team.lower() or
                         self._team_names_match(home_team, event_home))
            away_match = (away_team.lower() in event_away.lower() or 
                         event_away.lower() in away_team.lower() or
                         self._team_names_match(away_team, event_away))
            
            if home_match and away_match:
                matching_event = event
                break
        
        if not matching_event:
            print(f"    No matching event found for {away_team} @ {home_team}")
            return 0
        
        # Fetch odds for this specific event at the timestamp
        event_id = matching_event.get('id')
        if not event_id:
            print(f"    Event has no ID")
            return 0
        
        odds_data = self.fetch_event_odds(event_id, timestamp=target_timestamp)
        if not odds_data:
            print(f"    Could not fetch odds for event {event_id}")
            return 0
        
        # Use the odds data as the matching event
        matching_event = odds_data
        
        # Process bookmakers - Odds API.io structure
        # bookmakers is a dict: { "FanDuel": [markets], "DraftKings": [markets], ... }
        bookmakers_data = matching_event.get('bookmakers', {})
        if not bookmakers_data:
            print(f"    No bookmakers found for this event")
            return 0
        
        # Iterate through each bookmaker
        for book_name, markets in bookmakers_data.items():
            if not isinstance(markets, list):
                continue
            
            # Find Moneyline market (name: "ML")
            for market in markets:
                market_name = market.get('name', '')
                if market_name != 'ML':  # Moneyline
                    continue
                
                # Get odds array - should have one entry with "home" and "away" decimal odds
                odds_array = market.get('odds', [])
                if not odds_array or len(odds_array) == 0:
                    continue
                
                # Get first odds entry (should be the main moneyline)
                odds_entry = odds_array[0]
                home_odds_str = odds_entry.get('home', '')
                away_odds_str = odds_entry.get('away', '')
                
                try:
                    home_odds = float(home_odds_str)
                    away_odds = float(away_odds_str)
                    
                    if home_odds > 0 and away_odds > 0:
                        self.store_odds_snapshot(
                            game['id'], book_name, home_odds, away_odds,
                            target_time.isoformat(), snapshot_type
                        )
                        stored += 1
                except (ValueError, TypeError):
                    continue
        
        return stored
    
    def _team_names_match(self, team1: str, team2: str) -> bool:
        """Check if two team names likely refer to the same team."""
        # Normalize team names
        t1 = team1.lower().replace('los angeles', 'la').replace(' ', '')
        t2 = team2.lower().replace('los angeles', 'la').replace(' ', '')
        
        # Check if key words match
        t1_words = set(t1.split())
        t2_words = set(t2.split())
        
        # If they share significant words, likely the same team
        if len(t1_words) > 0 and len(t2_words) > 0:
            overlap = len(t1_words & t2_words) / max(len(t1_words), len(t2_words))
            return overlap > 0.5
        
        return False
    
    def fetch_season_odds(self, season_start: str = "2025-10-01", 
                         season_end: str = "2026-06-30"):
        """
        Fetch historical odds for entire season.
        Gets odds at 10 hours before tipoff and closing line for each game.
        """
        conn = get_connection()
        conn.row_factory = lambda cursor, row: {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
        cursor = conn.cursor()
        
        # Get all games in the season
        cursor.execute("""
            SELECT g.id, g.start_time, ht.name as home_team, at.name as away_team
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE g.start_time >= ? AND g.start_time <= ?
            AND g.status = 'FINAL'
            ORDER BY g.start_time
        """, (season_start, season_end))
        
        games = cursor.fetchall()
        conn.close()
        
        print(f"Found {len(games)} games for season {season_start} to {season_end}")
        
        total_stored_10h = 0
        total_stored_closing = 0
        
        for i, game in enumerate(games, 1):
            print(f"\n[{i}/{len(games)}] Processing {game['away_team']} @ {game['home_team']}")
            
            try:
                game_start_str = game['start_time']
                if 'T' in game_start_str:
                    game_start = datetime.fromisoformat(game_start_str.split('+')[0].split('Z')[0])
                elif ' ' in game_start_str:
                    game_start = datetime.strptime(game_start_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
                else:
                    game_start = datetime.fromisoformat(game_start_str)
            except Exception as e:
                print(f"  Error parsing game time: {e}")
                continue
            
            # Calculate 10 hours before tipoff
            ten_hours_before = game_start - timedelta(hours=10)
            
            # For closing line, use game start time (or slightly before)
            closing_time = game_start - timedelta(minutes=5)  # 5 minutes before tipoff
            
            # Fetch 10h before odds
            print(f"  Fetching 10h before odds (target: {ten_hours_before.strftime('%Y-%m-%d %H:%M')})...")
            stored_10h = self.process_game_odds(game, ten_hours_before, "10h")
            total_stored_10h += stored_10h
            print(f"    Stored {stored_10h} odds snapshots")
            
            # Rate limiting - historical API costs 10 credits per request
            time.sleep(2)  # 2 seconds between requests
            
            # Fetch closing line odds
            print(f"  Fetching closing line odds (target: {closing_time.strftime('%Y-%m-%d %H:%M')})...")
            stored_closing = self.process_game_odds(game, closing_time, "closing")
            total_stored_closing += stored_closing
            print(f"    Stored {stored_closing} odds snapshots")
            
            # Rate limiting between games
            time.sleep(2)  # 2 seconds between games
        
        print(f"\n{'='*80}")
        print(f"SUMMARY:")
        print(f"  Total 10h-before odds snapshots: {total_stored_10h}")
        print(f"  Total closing line odds snapshots: {total_stored_closing}")
        print(f"  Total stored: {total_stored_10h + total_stored_closing}")
        print(f"{'='*80}")


def test_api_connection():
    """Test API connection with current odds."""
    print("Testing API connection...")
    fetcher = HistoricalOddsFetcher(ODDS_API_KEY)
    
    current_odds = fetcher.fetch_current_odds()
    if current_odds:
        print(f"[OK] API connection successful! Found {len(current_odds)} current events.")
        return True
    else:
        print("[ERROR] API connection failed. Check your API key.")
        print("The API key provided may be invalid or expired.")
        return False


if __name__ == "__main__":
    import sys
    
    # Test API connection first
    if not test_api_connection():
        print("\nExiting. Please check your API key.")
        sys.exit(1)
    
    print("\n" + "="*80)
    print("Starting historical odds fetch for 2025-2026 NBA season")
    print("="*80)
    print("\nNote: Historical odds requests cost 10 credits each.")
    print("This will make 2 requests per game (10h before + closing line).")
    print("="*80 + "\n")
    
    fetcher = HistoricalOddsFetcher(ODDS_API_KEY)
    
    # Fetch odds for 2025-2026 NBA season
    # Season typically runs from October to June
    fetcher.fetch_season_odds(
        season_start="2025-10-01",
        season_end="2026-06-30"
    )
