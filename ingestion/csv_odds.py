"""Load and parse historical odds from CSV file."""
import csv
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict


class CSVOddsLoader:
    """Load historical odds from nba_detailed_odds.csv."""
    
    def __init__(self, csv_path: str = "nba_detailed_odds.csv"):
        self.csv_path = csv_path
        self.odds_cache: Dict[str, Dict] = {}  # matchup -> odds data
        self._loaded = False
    
    def load(self) -> int:
        """Load all moneyline odds from CSV. Returns count of games loaded."""
        if self._loaded:
            return len(self.odds_cache)
        
        games_by_date = defaultdict(dict)
        
        with open(self.csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                market = row.get('Market', '')
                
                # Only process Money Line - Game markets (handle encoding issues)
                if 'Money Line' not in market or 'Game' not in market:
                    continue
                
                # Skip quarter/half markets
                if 'Quarter' in market or 'Half' in market:
                    continue
                
                matchup = row.get('matchup', '')
                selection = row.get('Selection', '')
                timestamp = row.get('timestamp', '')
                
                try:
                    odds = float(row.get('Odds', 0))
                except (ValueError, TypeError):
                    continue
                
                if not matchup or not selection or odds <= 0:
                    continue
                
                # Extract date from timestamp
                date = timestamp[:10] if timestamp else ''
                
                # Create unique key: date + matchup
                key = f"{date}|{matchup}"
                
                if key not in games_by_date:
                    games_by_date[key] = {
                        'date': date,
                        'matchup': matchup,
                        'timestamp': timestamp,
                        'odds': {}
                    }
                
                games_by_date[key]['odds'][selection] = odds
        
        # Filter to only games with 2 teams
        for key, data in games_by_date.items():
            if len(data['odds']) == 2:
                self.odds_cache[key] = data
        
        self._loaded = True
        return len(self.odds_cache)
    
    def get_odds_for_date(self, date: str) -> List[Dict]:
        """Get all games with odds for a specific date (YYYY-MM-DD)."""
        if not self._loaded:
            self.load()
        
        games = []
        for key, data in self.odds_cache.items():
            if data['date'] == date:
                games.append(self._format_game(data))
        
        return games
    
    def get_odds_for_game(self, date: str, home_team: str, away_team: str) -> Optional[Dict]:
        """Get odds for a specific game by date and teams."""
        if not self._loaded:
            self.load()
        
        # Try different matchup formats
        matchup_formats = [
            f"{home_team} vs {away_team}",
            f"{away_team} vs {home_team}",
            f"{home_team} at {away_team}",
            f"{away_team} at {home_team}",
        ]
        
        for matchup in matchup_formats:
            key = f"{date}|{matchup}"
            if key in self.odds_cache:
                return self._format_game(self.odds_cache[key])
        
        # Try partial matching
        for key, data in self.odds_cache.items():
            if data['date'] != date:
                continue
            
            matchup_lower = data['matchup'].lower()
            home_lower = home_team.lower()
            away_lower = away_team.lower()
            
            # Check if both teams are in the matchup
            if (home_lower in matchup_lower or self._team_abbrev(home_team) in matchup_lower) and \
               (away_lower in matchup_lower or self._team_abbrev(away_team) in matchup_lower):
                return self._format_game(data)
        
        return None
    
    def _team_abbrev(self, team: str) -> str:
        """Get common abbreviation for team."""
        abbrevs = {
            'los angeles lakers': 'lakers',
            'los angeles clippers': 'clippers',
            'golden state warriors': 'warriors',
            'san antonio spurs': 'spurs',
            'oklahoma city thunder': 'thunder',
            'minnesota timberwolves': 'wolves',
            'portland trail blazers': 'blazers',
            'new orleans pelicans': 'pelicans',
            'new york knicks': 'knicks',
        }
        return abbrevs.get(team.lower(), team.lower().split()[-1])
    
    def _format_game(self, data: Dict) -> Dict:
        """Format game data for use in betting engine."""
        odds = data['odds']
        teams = list(odds.keys())
        
        if len(teams) != 2:
            return None
        
        team_a, team_b = teams[0], teams[1]
        odds_a, odds_b = odds[team_a], odds[team_b]
        
        # Determine home/away from matchup format
        # Format is typically "Away vs Home" or "Away at Home"
        matchup = data['matchup']
        parts = matchup.replace(' at ', ' vs ').split(' vs ')
        
        if len(parts) == 2:
            first_team = parts[0].strip()
            second_team = parts[1].strip()
            
            # Match to our teams
            if first_team == team_a:
                away_team, home_team = team_a, team_b
                away_odds, home_odds = odds_a, odds_b
            else:
                away_team, home_team = team_b, team_a
                away_odds, home_odds = odds_b, odds_a
        else:
            # Default: assume first team in odds dict is away
            away_team, home_team = team_a, team_b
            away_odds, home_odds = odds_a, odds_b
        
        # Determine favorite
        if home_odds < away_odds:
            favorite = home_team
            favorite_odds = home_odds
        else:
            favorite = away_team
            favorite_odds = away_odds
        
        return {
            'date': data['date'],
            'timestamp': data['timestamp'],
            'matchup': matchup,
            'home_team': home_team,
            'away_team': away_team,
            'home_odds': home_odds,
            'away_odds': away_odds,
            'favorite': favorite,
            'favorite_odds': favorite_odds,
            'implied_prob_home': 1 / home_odds,
            'implied_prob_away': 1 / away_odds,
        }
    
    def get_date_range(self) -> Tuple[str, str]:
        """Get the date range of available odds."""
        if not self._loaded:
            self.load()
        
        dates = [data['date'] for data in self.odds_cache.values()]
        if not dates:
            return ('', '')
        
        return (min(dates), max(dates))
    
    def get_all_dates(self) -> List[str]:
        """Get all unique dates with odds data."""
        if not self._loaded:
            self.load()
        
        dates = set(data['date'] for data in self.odds_cache.values())
        return sorted(dates)


# Quick test
if __name__ == "__main__":
    loader = CSVOddsLoader()
    count = loader.load()
    print(f"Loaded {count} games with odds")
    
    date_range = loader.get_date_range()
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    # Test Jan 20
    jan20 = loader.get_odds_for_date("2026-01-20")
    print(f"\nJan 20, 2026: {len(jan20)} games")
    
    for game in jan20:
        print(f"  {game['away_team']} @ {game['home_team']}")
        print(f"    Away: {game['away_odds']:.2f}, Home: {game['home_odds']:.2f}")
        print(f"    Favorite: {game['favorite']} ({game['favorite_odds']:.2f})")

