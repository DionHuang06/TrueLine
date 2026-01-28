"""Results ingestion and game finalization."""
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from database.db import get_connection
from config import BALLDONTLIE_API_BASE, BALLDONTLIE_API_KEY


class ResultsIngester:
    """Ingest game results and finalize games."""
    
    def __init__(self):
        self.base_url = BALLDONTLIE_API_BASE
        self.api_key = BALLDONTLIE_API_KEY
        self.headers = {"Authorization": self.api_key} if self.api_key else {}
    
    def fetch_results(self, date: str) -> List[Dict]:
        """Fetch game results for a specific date."""
        params = {
            "dates[]": [date]
        }
        
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
                
                meta = data.get("meta", {})
                cursor = meta.get("next_cursor")
                
                if not cursor:
                    break
                    
            except requests.RequestException as e:
                print(f"Error fetching results: {e}")
                break
        
        return all_games
    
    def update_results(self, days_back: int = 3) -> int:
        """
        Update game results for recent games.
        
        Args:
            days_back: How many days back to check for results
            
        Returns:
            Number of games updated
        """
        conn = get_connection()
        cursor = conn.cursor()
        updated = 0
        
        # Get games that need results
        cursor.execute("""
            SELECT g.id, g.external_id, g.start_time, 
                   ht.name as home_team, at.name as away_team
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE g.status != 'FINAL'
            AND datetime(g.start_time) < datetime('now')
        """)
        
        pending_games = cursor.fetchall()
        
        if not pending_games:
            print("No pending games to update.")
            conn.close()
            return 0
        
        # Fetch results for each day
        dates_checked = set()
        for game in pending_games:
            game_date = game['start_time'][:10]
            if game_date not in dates_checked:
                dates_checked.add(game_date)
        
        # Fetch all results
        all_results = {}
        for date in dates_checked:
            results = self.fetch_results(date)
            for result in results:
                all_results[str(result['id'])] = result
        
        # Update games
        for game in pending_games:
            external_id = game['external_id']
            
            # Check if this is a balldontlie ID
            if external_id.startswith('odds_'):
                # Need to match by teams and date
                game_date = game['start_time'][:10]
                home_team = game['home_team']
                away_team = game['away_team']
                
                # Find matching result
                for result in all_results.values():
                    result_date = result.get('date', '')[:10]
                    result_home = result.get('home_team', {}).get('full_name', '')
                    result_away = result.get('visitor_team', {}).get('full_name', '')
                    
                    # Normalize for comparison
                    if (result_date == game_date and 
                        (result_home == home_team or result_home in home_team or home_team in result_home) and
                        (result_away == away_team or result_away in away_team or away_team in result_away)):
                        
                        if result.get('status') == 'Final':
                            home_score = result.get('home_team_score', 0)
                            away_score = result.get('visitor_team_score', 0)
                            
                            cursor.execute("""
                                UPDATE games 
                                SET home_score = ?, away_score = ?, status = 'FINAL'
                                WHERE id = ?
                            """, (home_score, away_score, game['id']))
                            updated += 1
                        break
            else:
                # Direct ID match
                if external_id in all_results:
                    result = all_results[external_id]
                    if result.get('status') == 'Final':
                        home_score = result.get('home_team_score', 0)
                        away_score = result.get('visitor_team_score', 0)
                        
                        cursor.execute("""
                            UPDATE games 
                            SET home_score = ?, away_score = ?, status = 'FINAL'
                            WHERE id = ?
                        """, (home_score, away_score, game['id']))
                        updated += 1
        
        conn.commit()
        conn.close()
        
        print(f"Updated {updated} game results.")
        return updated
    
    def get_pending_games(self) -> List[Dict]:
        """Get games that are pending results."""
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT g.id, g.external_id, g.start_time,
                   ht.name as home_team, at.name as away_team
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE g.status != 'FINAL'
            AND datetime(g.start_time) < datetime('now')
            ORDER BY g.start_time
        """)
        
        games = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return games
