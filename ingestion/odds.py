"""Odds ingestion from The Odds API."""
import requests
from datetime import datetime
from typing import List, Dict, Optional
from database.db import get_connection
from config import ODDS_API_KEY, ODDS_API_BASE, TEAM_MAPPINGS


class OddsIngester:
    """Ingest odds from The Odds API."""
    
    def __init__(self):
        self.api_key = ODDS_API_KEY
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
    
    def _create_game_from_odds(self, conn, home_team: str, away_team: str,
                                start_time: str) -> int:
        """Create a game entry from odds data."""
        cursor = conn.cursor()
        
        # Normalize and ensure teams exist
        home_normalized = TEAM_MAPPINGS.get(home_team, home_team)
        away_normalized = TEAM_MAPPINGS.get(away_team, away_team)
        
        # Ensure home team exists
        cursor.execute("SELECT id FROM teams WHERE name = ?", (home_normalized,))
        home_row = cursor.fetchone()
        if not home_row:
            abbr = ''.join([w[0] for w in home_normalized.split()])[:3].upper()
            cursor.execute(
                "INSERT INTO teams (name, abbreviation) VALUES (?, ?)",
                (home_normalized, abbr)
            )
            home_team_id = cursor.lastrowid
        else:
            home_team_id = home_row['id']
        
        # Ensure away team exists
        cursor.execute("SELECT id FROM teams WHERE name = ?", (away_normalized,))
        away_row = cursor.fetchone()
        if not away_row:
            abbr = ''.join([w[0] for w in away_normalized.split()])[:3].upper()
            cursor.execute(
                "INSERT INTO teams (name, abbreviation) VALUES (?, ?)",
                (away_normalized, abbr)
            )
            away_team_id = cursor.lastrowid
        else:
            away_team_id = away_row['id']
        
        # Create game with odds API ID as external_id
        external_id = f"odds_{home_normalized}_{away_normalized}_{start_time[:10]}"
        
        cursor.execute("""
            INSERT OR IGNORE INTO games 
            (external_id, start_time, home_team_id, away_team_id, status)
            VALUES (?, ?, ?, ?, 'SCHEDULED')
        """, (external_id, start_time, home_team_id, away_team_id))
        
        conn.commit()
        
        # Get the game ID
        cursor.execute("SELECT id FROM games WHERE external_id = ?", (external_id,))
        row = cursor.fetchone()
        return row['id'] if row else None
    
    def fetch_odds(self) -> List[Dict]:
        """Fetch current odds from The Odds API."""
        if not self.api_key:
            print("Error: ODDS_API_KEY not set. Please set it in .env file.")
            return []
        
        params = {
            "apiKey": self.api_key,
            "regions": "us",
            "markets": "h2h",
            "oddsFormat": "decimal",
            "dateFormat": "iso"
        }
        
        try:
            response = requests.get(
                f"{self.base_url}/sports/basketball_nba/odds",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            # Check remaining requests
            remaining = response.headers.get('x-requests-remaining', 'unknown')
            print(f"API requests remaining: {remaining}")
            
            return response.json()
            
        except requests.RequestException as e:
            print(f"Error fetching odds: {e}")
            return []
    
    def ingest_odds(self) -> int:
        """
        Fetch and store odds snapshots.
        
        Returns number of snapshots stored.
        """
        events = self.fetch_odds()
        
        if not events:
            print("No odds data available.")
            return 0
        
        conn = get_connection()
        cursor = conn.cursor()
        pulled_at = datetime.utcnow().isoformat()
        stored = 0
        
        for event in events:
            try:
                home_team = event.get('home_team', '')
                away_team = event.get('away_team', '')
                start_time = event.get('commence_time', '')
                
                # Find or create game
                game_id = self._get_game_id_by_teams(
                    conn, home_team, away_team, start_time
                )
                
                if not game_id:
                    game_id = self._create_game_from_odds(
                        conn, home_team, away_team, start_time
                    )
                
                if not game_id:
                    continue
                
                # Process each bookmaker
                bookmakers = event.get('bookmakers', [])
                for book in bookmakers:
                    book_key = book.get('key', '')
                    markets = book.get('markets', [])
                    
                    for market in markets:
                        if market.get('key') != 'h2h':
                            continue
                        
                        outcomes = market.get('outcomes', [])
                        home_odds = None
                        away_odds = None
                        
                        for outcome in outcomes:
                            team_name = outcome.get('name', '')
                            price = outcome.get('price', 0)
                            
                            if team_name == home_team:
                                home_odds = price
                            elif team_name == away_team:
                                away_odds = price
                        
                        if home_odds and away_odds:
                            cursor.execute("""
                                INSERT INTO odds_snapshots 
                                (game_id, book, pulled_at, home_dec, away_dec)
                                VALUES (?, ?, ?, ?, ?)
                            """, (game_id, book_key, pulled_at, home_odds, away_odds))
                            stored += 1
                
            except Exception as e:
                print(f"Error processing event: {e}")
                continue
        
        conn.commit()
        conn.close()
        
        print(f"Stored {stored} odds snapshots.")
        return stored
