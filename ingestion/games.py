"""Games ingestion from balldontlie API."""
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from database.db import get_connection
from config import BALLDONTLIE_API_BASE, BALLDONTLIE_API_KEY, TEAM_MAPPINGS_REVERSE


class GamesIngester:
    """Ingest NBA games from balldontlie API."""
    
    def __init__(self):
        self.base_url = BALLDONTLIE_API_BASE
        self.api_key = BALLDONTLIE_API_KEY
        self.headers = {"Authorization": self.api_key} if self.api_key else {}
    
    def _ensure_team_exists(self, conn, team_name: str) -> int:
        """Ensure team exists in database and return its ID."""
        cursor = conn.cursor()
        
        # Check if team exists
        cursor.execute("SELECT id FROM teams WHERE name = ?", (team_name,))
        row = cursor.fetchone()
        
        if row:
            return row['id']
        
        # Create team
        abbreviation = ''.join([word[0] for word in team_name.split()])[:3].upper()
        cursor.execute(
            "INSERT INTO teams (name, abbreviation) VALUES (?, ?)",
            (team_name, abbreviation)
        )
        conn.commit()
        return cursor.lastrowid
    
    def _normalize_team_name(self, team_data: Dict) -> str:
        """Normalize team name from balldontlie format."""
        full_name = team_data.get('full_name', '')
        if full_name in TEAM_MAPPINGS_REVERSE:
            return TEAM_MAPPINGS_REVERSE[full_name]
        return full_name
    
    def fetch_games(self, start_date: str, end_date: Optional[str] = None) -> List[Dict]:
        """
        Fetch games from balldontlie API.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (optional)
        """
        params = {
            "dates[]": [start_date],
        }
        
        if end_date:
            # Build list of dates
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            dates = []
            current = start
            while current <= end:
                dates.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
            params["dates[]"] = dates
        
        all_games = []
        cursor = None
        
        while True:
            if cursor:
                params["cursor"] = cursor
            
            try:
                response = requests.get(
                    f"{self.base_url}/games",
                    params=params,
                    headers=self.headers,
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                games = data.get("data", [])
                all_games.extend(games)
                
                # Handle pagination
                meta = data.get("meta", {})
                cursor = meta.get("next_cursor")
                
                if not cursor:
                    break
                    
            except requests.RequestException as e:
                print(f"Error fetching games: {e}")
                break
        
        return all_games
    
    def ingest_games(self, start_date: str, end_date: Optional[str] = None) -> int:
        """
        Fetch and store games in database.
        
        Returns number of games ingested.
        """
        games = self.fetch_games(start_date, end_date)
        
        if not games:
            print("No games found for the specified date range.")
            return 0
        
        conn = get_connection()
        cursor = conn.cursor()
        ingested = 0
        
        for game in games:
            try:
                external_id = str(game['id'])
                
                # Check if game already exists
                cursor.execute(
                    "SELECT id FROM games WHERE external_id = ?", 
                    (external_id,)
                )
                if cursor.fetchone():
                    continue
                
                # Get team names
                home_team = self._normalize_team_name(game['home_team'])
                away_team = self._normalize_team_name(game['visitor_team'])
                
                # Ensure teams exist
                home_team_id = self._ensure_team_exists(conn, home_team)
                away_team_id = self._ensure_team_exists(conn, away_team)
                
                # Parse game time
                game_date = game.get('date', '')
                if game_date:
                    # balldontlie returns date in ISO format
                    start_time = game_date
                else:
                    start_time = datetime.now().isoformat()
                
                # Determine status
                status = game.get('status', 'SCHEDULED')
                if status == 'Final':
                    status = 'FINAL'
                elif status in ('1st Qtr', '2nd Qtr', '3rd Qtr', '4th Qtr', 'Halftime'):
                    status = 'LIVE'
                else:
                    status = 'SCHEDULED'
                
                # Get scores if available
                home_score = game.get('home_team_score')
                away_score = game.get('visitor_team_score')
                
                # Determine season from date (NBA season typically Oct-June)
                game_date = datetime.fromisoformat(start_time.split('T')[0] if 'T' in start_time else start_time)
                if game_date.month >= 10:
                    season = f"{game_date.year}-{game_date.year + 1}"
                else:
                    season = f"{game_date.year - 1}-{game_date.year}"
                
                cursor.execute("""
                    INSERT INTO games 
                    (external_id, start_time, home_team_id, away_team_id, 
                     home_score, away_score, status, season)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    external_id, start_time, home_team_id, away_team_id,
                    home_score, away_score, status, season
                ))
                
                ingested += 1
                
            except Exception as e:
                print(f"Error ingesting game {game.get('id')}: {e}")
                continue
        
        conn.commit()
        conn.close()
        
        print(f"Ingested {ingested} new games.")
        return ingested
    
    def pull_upcoming(self, days: int = 7) -> int:
        """Pull upcoming games for the next N days."""
        today = datetime.now().strftime("%Y-%m-%d")
        end_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
        return self.ingest_games(today, end_date)
