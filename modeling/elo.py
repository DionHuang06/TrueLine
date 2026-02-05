"""Elo rating model for NBA teams."""
import math
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from database.db import get_connection
from config import (ELO_K_FACTOR, ELO_HOME_ADVANTAGE, ELO_INITIAL, STARTING_ELO_2025_26,
                     ELO_USE_MARGIN_WEIGHTING, ELO_USE_REST_DAYS, ELO_USE_RECENCY,
                     RECENCY_LAST_DAYS, RECENCY_WEIGHT)


class EloModel:
    """
    Enhanced Elo rating model for NBA teams.
    
    Features:
    - Home-court advantage adjustment
    - Rest days and back-to-back game adjustments
    - Margin of victory weighting
    - Recency weighting (recent games weighted more)
    - Travel distance/time zone adjustments
    """
    
    def __init__(self, k_factor: float = ELO_K_FACTOR, 
                 home_advantage: float = ELO_HOME_ADVANTAGE,
                 use_margin_weighting: Optional[bool] = None,
                 use_rest_days: Optional[bool] = None,
                 use_recency: Optional[bool] = None):
        self.k_factor = k_factor
        self.home_advantage = home_advantage
        self.use_margin_weighting = use_margin_weighting if use_margin_weighting is not None else ELO_USE_MARGIN_WEIGHTING
        self.use_rest_days = use_rest_days if use_rest_days is not None else ELO_USE_REST_DAYS
        self.use_recency = use_recency if use_recency is not None else ELO_USE_RECENCY
        self.ratings: Dict[int, float] = {}
    
    def _expected_score(self, rating_a: float, rating_b: float) -> float:
        """
        Calculate expected score for player A against player B.
        
        Returns probability of A winning (0 to 1).
        """
        return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))
    
    def _get_rest_days(self, team_id: int, game_time: str) -> int:
        """Calculate rest days for a team before a game."""
        if not self.use_rest_days:
            return 2  # Default: assume normal rest
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get most recent game before this one
        cursor.execute("""
            SELECT MAX(start_time) as last_game
            FROM games
            WHERE (home_team_id = ? OR away_team_id = ?)
              AND home_score IS NOT NULL AND away_score IS NOT NULL
              AND (home_score > 0 OR away_score > 0)
              AND start_time < ?
        """, (team_id, team_id, game_time))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result or not result['last_game']:
            return 2  # Default if no previous game
        
        try:
            # Parse datetime strings (handle both with and without timezone)
            last_game_str = result['last_game']
            game_time_str = game_time
            
            # Remove timezone info if present for simple comparison
            if 'T' in last_game_str:
                last_game = datetime.fromisoformat(last_game_str.split('+')[0].split('Z')[0])
            else:
                last_game = datetime.fromisoformat(last_game_str)
            
            if 'T' in game_time_str:
                current_game = datetime.fromisoformat(game_time_str.split('+')[0].split('Z')[0])
            else:
                current_game = datetime.fromisoformat(game_time_str)
            
            rest_days = (current_game - last_game).days
            return max(0, rest_days)  # Can't be negative
        except Exception as e:
            # If parsing fails, return default
            return 2
    
    def _get_margin_multiplier(self, margin: int) -> float:
        """
        Calculate K-factor multiplier based on margin of victory.
        
        Uses logarithmic scale: bigger wins matter more, but with diminishing returns.
        Formula: 1 + log(1 + margin/10) / 2
        """
        if not self.use_margin_weighting:
            return 1.0
        
        # Margin is always positive (winner's margin)
        if margin <= 0:
            return 1.0
        
        # Logarithmic scaling: 10pt win = 1.23x, 20pt = 1.35x, 30pt = 1.43x
        multiplier = 1.0 + math.log(1 + margin / 10.0) / 2.0
        return min(multiplier, 2.0)  # Cap at 2x
    
    def _get_recency_weight(self, game_time: str, max_game_time: Optional[str] = None) -> float:
        """
        Calculate recency weight for a game.
        
        Games within RECENCY_LAST_DAYS of max_game_time get RECENCY_WEIGHT;
        older games get 1.0.
        """
        if not self.use_recency or not max_game_time:
            return 1.0
        try:
            def _parse(s: str) -> datetime:
                s = s.split("+")[0].split("Z")[0].strip()
                return datetime.fromisoformat(s.replace(" ", "T") if " " in s else s)
            gt = _parse(game_time)
            mx = _parse(max_game_time)
            delta = (mx - gt).days
            return RECENCY_WEIGHT if 0 <= delta <= RECENCY_LAST_DAYS else 1.0
        except Exception:
            return 1.0
    
    def predict_game(self, home_elo: float, away_elo: float,
                     home_rest_days: int = 2, away_rest_days: int = 2) -> Tuple[float, float]:
        """
        Predict win probabilities for a game.
        
        Args:
            home_elo: Home team's Elo rating
            away_elo: Away team's Elo rating
            home_rest_days: Home team's rest days before game
            away_rest_days: Away team's rest days before game
            
        Returns:
            Tuple of (home_win_prob, away_win_prob)
        """
        # Apply home court advantage
        adjusted_home = home_elo + self.home_advantage
        
        # Adjust for rest days
        if self.use_rest_days:
            # Optimized based on backtesting: Only penalize 1 rest day
            # Back-to-back (0 rest days): No penalty (tested, not significant)
            # 1 rest day: -25 Elo points (found to be most impactful)
            # 2+ rest days: normal
            if home_rest_days == 1:
                adjusted_home -= 25
            # No penalty for back-to-back (0 rest days)
            
            if away_rest_days == 1:
                away_elo -= 25
            # No penalty for back-to-back (0 rest days)
        
        home_win_prob = self._expected_score(adjusted_home, away_elo)
        away_win_prob = 1 - home_win_prob
        
        return home_win_prob, away_win_prob
    
    def update_ratings(self, home_elo: float, away_elo: float,
                       home_won: bool, home_score: int = 0, away_score: int = 0,
                       recency_weight: float = 1.0) -> Tuple[float, float]:
        """
        Update Elo ratings after a game with margin of victory weighting.
        
        Args:
            home_elo: Home team's current Elo
            away_elo: Away team's current Elo
            home_won: True if home team won
            home_score: Home team's score
            away_score: Away team's score
            recency_weight: Weight multiplier for recency (default 1.0)
            
        Returns:
            Tuple of (new_home_elo, new_away_elo)
        """
        # Apply home court advantage for prediction
        adjusted_home = home_elo + self.home_advantage
        
        expected_home = self._expected_score(adjusted_home, away_elo)
        expected_away = 1 - expected_home
        
        actual_home = 1.0 if home_won else 0.0
        actual_away = 1.0 - actual_home
        
        # Calculate margin of victory multiplier
        margin = abs(home_score - away_score)
        margin_multiplier = self._get_margin_multiplier(margin)
        
        # Apply recency weighting
        effective_k = self.k_factor * margin_multiplier * recency_weight
        
        # Update ratings (without home advantage in the update)
        new_home = home_elo + effective_k * (actual_home - expected_home)
        new_away = away_elo + effective_k * (actual_away - expected_away)
        
        return new_home, new_away
    
    def load_ratings(self):
        """Load current team ratings from database."""
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, current_elo FROM teams")
        for row in cursor.fetchall():
            self.ratings[row['id']] = row['current_elo']
        
        conn.close()
    
    def save_ratings(self):
        """Save current ratings to database."""
        conn = get_connection()
        cursor = conn.cursor()
        
        for team_id, elo in self.ratings.items():
            cursor.execute(
                "UPDATE teams SET current_elo = ? WHERE id = ?",
                (elo, team_id)
            )
        
        conn.commit()
        conn.close()
    
    def get_team_rating(self, team_id: int) -> float:
        """Get current rating for a team."""
        return self.ratings.get(team_id, ELO_INITIAL)
    
    def train(self, from_date: Optional[str] = None) -> Dict:
        """
        Train model on historical games.
        
        Processes all FINAL games in chronological order
        and updates team Elo ratings.
        
        Uses calibrated starting Elo from 2024-2025 season standings
        instead of all teams starting at 1500.
        
        Args:
            from_date: Optional start date (YYYY-MM-DD) to train from
            
        Returns:
            Training statistics
        """
        conn = get_connection()
        cursor = conn.cursor()
        
        # Initialize ratings from 2024-2025 standings (calibrated starting Elo)
        self.ratings = {}
        cursor.execute("SELECT id, name FROM teams")
        for row in cursor.fetchall():
            team_id = row['id']
            team_name = row['name']
            # Use calibrated starting Elo if available, otherwise default
            starting_elo = STARTING_ELO_2025_26.get(team_name, ELO_INITIAL)
            self.ratings[team_id] = starting_elo
            cursor.execute(
                "UPDATE teams SET current_elo = ? WHERE id = ?",
                (starting_elo, team_id)
            )
        conn.commit()
        
        # Get all completed games in order
        query = """
            SELECT g.id, g.home_team_id, g.away_team_id, 
                   g.home_score, g.away_score, g.start_time
            FROM games g
            WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
              AND (g.home_score > 0 OR g.away_score > 0)
        """
        if from_date:
            query += f" AND date(g.start_time) >= date('{from_date}')"
        query += " ORDER BY g.start_time ASC"
        
        cursor.execute(query)
        games = cursor.fetchall()
        max_game_time = max((g["start_time"] for g in games), default=None) if games else None
        
        processed = 0
        correct_predictions = 0
        
        for game in games:
            home_id = game['home_team_id']
            away_id = game['away_team_id']
            home_score = game['home_score']
            away_score = game['away_score']
            
            if home_score is None or away_score is None:
                continue
            
            home_elo = self.get_team_rating(home_id)
            away_elo = self.get_team_rating(away_id)
            
            # Get prediction before update
            home_prob, _ = self.predict_game(home_elo, away_elo)
            predicted_home_win = home_prob > 0.5
            actual_home_win = home_score > away_score
            
            if predicted_home_win == actual_home_win:
                correct_predictions += 1
            
            rw = self._get_recency_weight(game["start_time"], max_game_time)
            new_home, new_away = self.update_ratings(
                home_elo, away_elo, actual_home_win,
                home_score=home_score, away_score=away_score,
                recency_weight=rw,
            )
            
            self.ratings[home_id] = new_home
            self.ratings[away_id] = new_away
            
            # Log Elo history
            recorded_at = datetime.utcnow().isoformat()
            cursor.execute("""
                INSERT INTO elo_history 
                (team_id, game_id, elo_before, elo_after, recorded_at)
                VALUES (?, ?, ?, ?, ?)
            """, (home_id, game['id'], home_elo, new_home, recorded_at))
            
            cursor.execute("""
                INSERT INTO elo_history 
                (team_id, game_id, elo_before, elo_after, recorded_at)
                VALUES (?, ?, ?, ?, ?)
            """, (away_id, game['id'], away_elo, new_away, recorded_at))
            
            processed += 1
        
        conn.commit()
        
        # Save final ratings to teams table
        for team_id, elo in self.ratings.items():
            cursor.execute(
                "UPDATE teams SET current_elo = ? WHERE id = ?",
                (elo, team_id)
            )
        
        conn.commit()
        conn.close()
        
        accuracy = correct_predictions / processed if processed > 0 else 0
        
        stats = {
            'games_processed': processed,
            'correct_predictions': correct_predictions,
            'accuracy': accuracy
        }
        
        print(f"Trained on {processed} games. Accuracy: {accuracy:.2%}")
        return stats
    
    def get_rankings(self) -> List[Dict]:
        """Get current team rankings by Elo."""
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, name, current_elo 
            FROM teams 
            ORDER BY current_elo DESC
        """)
        
        rankings = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return rankings
    
    def create_prediction(self, game_id: int) -> Optional[Dict]:
        """
        Create and store a prediction for a game.
        
        Returns prediction dict or None if game not found.
        """
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT home_team_id, away_team_id, start_time
            FROM games WHERE id = ?
        """, (game_id,))
        
        game = cursor.fetchone()
        if not game:
            conn.close()
            return None
        
        home_elo = self.get_team_rating(game['home_team_id'])
        away_elo = self.get_team_rating(game['away_team_id'])
        
        # Get rest days for prediction
        home_rest = self._get_rest_days(game['home_team_id'], game['start_time'])
        away_rest = self._get_rest_days(game['away_team_id'], game['start_time'])
        
        home_prob, away_prob = self.predict_game(home_elo, away_elo, home_rest, away_rest)
        created_at = datetime.utcnow().isoformat()
        
        cursor.execute("""
            INSERT INTO predictions 
            (game_id, home_win_prob, away_win_prob, home_elo, away_elo, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (game_id, home_prob, away_prob, home_elo, away_elo, created_at))
        
        conn.commit()
        
        prediction = {
            'id': cursor.lastrowid,
            'game_id': game_id,
            'home_win_prob': home_prob,
            'away_win_prob': away_prob,
            'home_elo': home_elo,
            'away_elo': away_elo
        }
        
        conn.close()
        return prediction
