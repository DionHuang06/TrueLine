"""Edge detection and EV calculation."""
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from database.db import get_connection
from modeling.elo import EloModel
from config import MIN_EDGE, MAX_EDGE, MIN_GAMES_FOR_BETTING


class EdgeDetector:
    """
    Detect +EV betting opportunities.
    
    Compares model probabilities with de-vigged market odds
    to find edges.
    """
    
    def __init__(self, model: Optional[EloModel] = None):
        self.model = model or EloModel()
        self.model.load_ratings()
    
    def _decimal_to_implied_prob(self, decimal_odds: float) -> float:
        """Convert decimal odds to implied probability."""
        return 1 / decimal_odds if decimal_odds > 0 else 0
    
    def _devig_odds(self, home_dec: float, away_dec: float) -> Tuple[float, float]:
        """
        Remove vig from odds to get true implied probabilities.
        
        Uses multiplicative method (divide by total probability).
        """
        home_implied = self._decimal_to_implied_prob(home_dec)
        away_implied = self._decimal_to_implied_prob(away_dec)
        
        total = home_implied + away_implied
        
        if total == 0:
            return 0.5, 0.5
        
        home_fair = home_implied / total
        away_fair = away_implied / total
        
        return home_fair, away_fair
    
    def _get_best_odds(self, game_id: int, 
                        before_time: Optional[str] = None) -> Optional[Dict]:
        """
        Get best available odds for each side across all books.
        
        Args:
            game_id: Game ID to get odds for
            before_time: Optional cutoff time (for backtesting)
            
        Returns:
            Dict with best odds info or None
        """
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get latest odds snapshot for each book
        # CRITICAL: CSV odds have incorrect timestamps (set at import time, not when odds available)
        # CSV odds are opening odds, so we treat them as available before game start
        if before_time:
            # Normalize before_time format for SQLite comparison
            # SQLite can compare ISO strings directly, but ensure consistent format
            before_time_normalized = before_time.replace(' ', 'T') if ' ' in before_time else before_time
            
            query = """
                SELECT book, home_dec, away_dec, pulled_at
                FROM odds_snapshots
                WHERE game_id = ? 
                  AND (
                    (book = 'CSV') OR  -- CSV odds are opening odds, treat as available before game
                    (pulled_at < ?)   -- Other books must be pulled before game start
                  )
                ORDER BY pulled_at DESC
            """
            cursor.execute(query, (game_id, before_time_normalized))
        else:
            query = """
                SELECT book, home_dec, away_dec, pulled_at
                FROM odds_snapshots
                WHERE game_id = ?
                ORDER BY pulled_at DESC
            """
            cursor.execute(query, (game_id,))
        
        snapshots = cursor.fetchall()
        conn.close()
        
        if not snapshots:
            return None
        
        # Get best odds for each side (most recent per book)
        seen_books = set()
        best_home_odds = 0
        best_home_book = ""
        best_away_odds = 0
        best_away_book = ""
        
        all_home_odds = []
        all_away_odds = []
        
        for snap in snapshots:
            book = snap['book']
            if book in seen_books:
                continue
            seen_books.add(book)
            
            if snap['home_dec'] > best_home_odds:
                best_home_odds = snap['home_dec']
                best_home_book = book
            
            if snap['away_dec'] > best_away_odds:
                best_away_odds = snap['away_dec']
                best_away_book = book
            
            all_home_odds.append(snap['home_dec'])
            all_away_odds.append(snap['away_dec'])
        
        if not all_home_odds:
            return None
        
        # Calculate average odds for de-vigging (consensus line)
        avg_home = sum(all_home_odds) / len(all_home_odds)
        avg_away = sum(all_away_odds) / len(all_away_odds)
        
        # De-vig using average/consensus odds
        home_fair, away_fair = self._devig_odds(avg_home, avg_away)
        
        return {
            'best_home_odds': best_home_odds,
            'best_home_book': best_home_book,
            'best_away_odds': best_away_odds,
            'best_away_book': best_away_book,
            'home_implied_fair': home_fair,
            'away_implied_fair': away_fair
        }
    
    def calculate_edge(self, model_prob: float, implied_prob: float) -> float:
        """Calculate edge as difference between model and market probability."""
        return model_prob - implied_prob
    
    def calculate_ev(self, model_prob: float, decimal_odds: float) -> float:
        """
        Calculate expected value of a bet.
        
        EV = (prob_win * payout) - (prob_lose * stake)
        Normalized to per-unit stake.
        """
        payout = decimal_odds - 1  # Profit if win
        prob_lose = 1 - model_prob
        
        ev = (model_prob * payout) - (prob_lose * 1)
        return ev
    
    def _adjust_confidence(self, prob: float, games_played: int) -> float:
        """
        Adjust probability confidence based on sample size.
        
        Early in season (few games), reduce confidence to avoid overconfidence.
        Moves probabilities toward 50% when sample size is small.
        
        More aggressive adjustment to combat extreme starting Elo values.
        """
        if games_played >= 60:
            # Well-calibrated after 60 games, use as-is
            return prob
        
        # Confidence factor: 0.2 (no confidence) to 1.0 (full confidence)
        # Very aggressive: starts at 20% confidence, reaches 100% at 60 games
        # Linear interpolation: confidence = 0.2 + (games_played / 60) * 0.8
        confidence = 0.2 + (games_played / 60) * 0.8
        confidence = min(1.0, confidence)
        
        # Pull toward 50% based on confidence
        # At 25 games: 47% confidence (very conservative)
        # At 30 games: 60% confidence
        # At 40 games: 73% confidence
        # At 50 games: 87% confidence
        adjusted = 0.5 + (prob - 0.5) * confidence
        return adjusted
    
    def _get_games_played(self, team_id: int, before_time: Optional[str] = None) -> int:
        """Get number of games a team has played."""
        conn = get_connection()
        cursor = conn.cursor()
        
        if before_time:
            query = """
                SELECT COUNT(*) as count
                FROM games
                WHERE (home_team_id = ? OR away_team_id = ?)
                  AND status = 'FINAL'
                  AND start_time < ?
            """
            cursor.execute(query, (team_id, team_id, before_time))
        else:
            query = """
                SELECT COUNT(*) as count
                FROM games
                WHERE (home_team_id = ? OR away_team_id = ?)
                  AND status = 'FINAL'
            """
            cursor.execute(query, (team_id, team_id))
        
        result = cursor.fetchone()
        conn.close()
        return result['count'] if result else 0
    
    def find_edges(self, game_id: int, 
                   before_time: Optional[str] = None) -> List[Dict]:
        """
        Find betting edges for a game.
        
        Args:
            game_id: Game ID to analyze
            before_time: Optional cutoff for odds (backtesting)
            
        Returns:
            List of edge opportunities (can be 0, 1, or 2 per game)
        """
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get game info
        cursor.execute("""
            SELECT g.id, g.start_time, g.home_team_id, g.away_team_id,
                   ht.name as home_team, at.name as away_team
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE g.id = ?
        """, (game_id,))
        
        game = cursor.fetchone()
        conn.close()
        
        if not game:
            return []
        
        # Get best odds
        odds_info = self._get_best_odds(game_id, before_time)
        if not odds_info:
            return []
        
        # Get model prediction with rest days
        home_elo = self.model.get_team_rating(game['home_team_id'])
        away_elo = self.model.get_team_rating(game['away_team_id'])
        
        # Calculate rest days
        home_rest = self.model._get_rest_days(game['home_team_id'], game['start_time'])
        away_rest = self.model._get_rest_days(game['away_team_id'], game['start_time'])
        
        home_prob_raw, away_prob_raw = self.model.predict_game(
            home_elo, away_elo, home_rest, away_rest
        )
        
        # Adjust confidence based on games played (reduce early-season overconfidence)
        home_games = self._get_games_played(game['home_team_id'], before_time)
        away_games = self._get_games_played(game['away_team_id'], before_time)
        min_games = min(home_games, away_games)
        
        # No minimum games restriction - allow betting on all games
        # Confidence adjustment will handle early-season uncertainty
        
        home_prob = self._adjust_confidence(home_prob_raw, min_games)
        away_prob = 1 - home_prob  # Re-normalize
        
        edges = []
        
        # Check home side
        home_edge = self.calculate_edge(home_prob, odds_info['home_implied_fair'])
        home_ev = self.calculate_ev(home_prob, odds_info['best_home_odds'])
        
        # Check edge (no upper limit - trust calibrated model)
        if home_edge >= MIN_EDGE and home_ev > 0:
            edges.append({
                'game_id': game_id,
                'side': 'home',
                'team': game['home_team'],
                'opponent': game['away_team'],
                'start_time': game['start_time'],
                'best_book': odds_info['best_home_book'],
                'best_odds': odds_info['best_home_odds'],
                'implied_prob': odds_info['home_implied_fair'],
                'model_prob': home_prob,
                'edge': home_edge,
                'ev': home_ev
            })
        
        # Check away side
        away_edge = self.calculate_edge(away_prob, odds_info['away_implied_fair'])
        away_ev = self.calculate_ev(away_prob, odds_info['best_away_odds'])
        
        # Check edge (no upper limit - trust calibrated model)
        if away_edge >= MIN_EDGE and away_ev > 0:
            edges.append({
                'game_id': game_id,
                'side': 'away',
                'team': game['away_team'],
                'opponent': game['home_team'],
                'start_time': game['start_time'],
                'best_book': odds_info['best_away_book'],
                'best_odds': odds_info['best_away_odds'],
                'implied_prob': odds_info['away_implied_fair'],
                'model_prob': away_prob,
                'edge': away_edge,
                'ev': away_ev
            })
        
        return edges
    
    def find_today_edges(self) -> List[Dict]:
        """Find all edges for today's games."""
        conn = get_connection()
        cursor = conn.cursor()
        
        today = datetime.now().strftime("%Y-%m-%d")
        
        cursor.execute("""
            SELECT id FROM games 
            WHERE date(start_time) = date(?)
            AND status = 'SCHEDULED'
            ORDER BY start_time
        """, (today,))
        
        games = cursor.fetchall()
        conn.close()
        
        all_edges = []
        for game in games:
            edges = self.find_edges(game['id'])
            all_edges.extend(edges)
        
        return all_edges
    
    def store_edge(self, edge: Dict) -> int:
        """Store an edge in the database."""
        conn = get_connection()
        cursor = conn.cursor()
        
        created_at = datetime.utcnow().isoformat()
        
        cursor.execute("""
            INSERT INTO edges 
            (game_id, side, best_book, best_odds, implied_prob, 
             model_prob, edge, ev, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            edge['game_id'], edge['side'], edge['best_book'],
            edge['best_odds'], edge['implied_prob'], edge['model_prob'],
            edge['edge'], edge['ev'], created_at
        ))
        
        edge_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return edge_id

