"""Paper trading system for simulated betting."""
from datetime import datetime
from typing import Dict, List, Optional
from database.db import get_connection
from config import INITIAL_BANKROLL, FLAT_STAKE_PCT, MAX_STAKE_PCT, USE_KELLY_CRITERION, KELLY_FRACTION


class PaperTrader:
    """
    Paper trading system for bet simulation.
    
    Manages virtual bankroll and tracks bet performance.
    """
    
    def __init__(self):
        self.bankroll = self._get_current_bankroll()
    
    def _get_current_bankroll(self) -> float:
        """Get current bankroll from history or initialize."""
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT balance FROM bankroll_history 
            ORDER BY recorded_at DESC LIMIT 1
        """)
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return row['balance']
        
        # Initialize bankroll
        self._record_bankroll_change(INITIAL_BANKROLL, INITIAL_BANKROLL, "Initial bankroll")
        return INITIAL_BANKROLL
    
    def _record_bankroll_change(self, new_balance: float, change: float, reason: str):
        """Record a bankroll change."""
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO bankroll_history (balance, change, reason, recorded_at)
            VALUES (?, ?, ?, ?)
        """, (new_balance, change, reason, datetime.utcnow().isoformat()))
        
        conn.commit()
        conn.close()
    
    def calculate_stake(self, edge: Optional[Dict] = None, use_kelly: Optional[bool] = None) -> float:
        """
        Calculate stake size using Kelly Criterion or flat staking.
        
        Kelly Criterion: f = (p * b - q) / b
        where:
        - f = fraction of bankroll to bet
        - p = probability of winning (model prob)
        - q = probability of losing (1 - p)
        - b = odds - 1 (net odds)
        
        Uses fractional Kelly (50% of full Kelly) for safety.
        Falls back to flat staking if Kelly is negative or edge too small.
        """
        # Use config setting if not explicitly provided
        if use_kelly is None:
            use_kelly = USE_KELLY_CRITERION
        
        if not use_kelly or not edge:
            # Flat staking fallback
            stake_pct = FLAT_STAKE_PCT
            stake_pct = min(stake_pct, MAX_STAKE_PCT)
            stake = self.bankroll * stake_pct
            return round(stake, 2)
        
        model_prob = edge.get('model_prob', 0.5)
        decimal_odds = edge.get('best_odds', 2.0)
        edge_value = edge.get('edge', 0.0)
        
        # Only use Kelly if we have positive edge
        if edge_value <= 0 or model_prob <= 0 or model_prob >= 1:
            stake_pct = FLAT_STAKE_PCT
            stake_pct = min(stake_pct, MAX_STAKE_PCT)
            stake = self.bankroll * stake_pct
            return round(stake, 2)
        
        # Kelly Criterion calculation
        p = model_prob  # Probability of winning
        q = 1 - p       # Probability of losing
        b = decimal_odds - 1  # Net odds (profit if win)
        
        # Full Kelly: f = (p * b - q) / b
        # Simplified: f = p - q / b = p - (1-p) / b
        full_kelly = (p * b - q) / b
        
        # Use fractional Kelly (configurable fraction for safety, reduces variance)
        fractional_kelly = full_kelly * KELLY_FRACTION
        
        # Cap Kelly at reasonable maximum (5% of bankroll)
        kelly_pct = max(0, min(fractional_kelly, 0.05))
        
        # If Kelly suggests very small bet (< 0.1%), use flat staking
        if kelly_pct < 0.001:
            stake_pct = FLAT_STAKE_PCT
        else:
            stake_pct = kelly_pct
        
        # Always respect maximum cap
        stake_pct = min(stake_pct, MAX_STAKE_PCT)
        
        stake = self.bankroll * stake_pct
        return round(stake, 2)
    
    def place_bet(self, edge: Dict) -> Optional[Dict]:
        """
        Place a paper bet on an edge.
        
        Args:
            edge: Edge dictionary with bet details
            
        Returns:
            Paper bet dictionary or None if failed
        """
        conn = get_connection()
        cursor = conn.cursor()
        
        # Check if bet already exists for this game/side
        cursor.execute("""
            SELECT id FROM paper_bets 
            WHERE game_id = ? AND side = ? AND result IS NULL
        """, (edge['game_id'], edge['side']))
        
        if cursor.fetchone():
            print(f"Bet already exists for game {edge['game_id']} {edge['side']}")
            conn.close()
            return None
        
        # Calculate stake using Kelly Criterion
        stake = self.calculate_stake(edge, use_kelly=True)
        odds = edge['best_odds']
        potential_payout = stake * odds
        
        placed_at = datetime.utcnow().isoformat()
        
        # Get or create edge record
        edge_id = edge.get('edge_id')
        if not edge_id:
            cursor.execute("""
                INSERT INTO edges 
                (game_id, side, best_book, best_odds, implied_prob, 
                 model_prob, edge, ev, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                edge['game_id'], edge['side'], edge['best_book'],
                edge['best_odds'], edge['implied_prob'], edge['model_prob'],
                edge['edge'], edge['ev'], placed_at
            ))
            edge_id = cursor.lastrowid
        
        # Create paper bet
        cursor.execute("""
            INSERT INTO paper_bets 
            (game_id, edge_id, side, odds, stake, potential_payout, placed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            edge['game_id'], edge_id, edge['side'], 
            odds, stake, potential_payout, placed_at
        ))
        
        bet_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        bet = {
            'id': bet_id,
            'game_id': edge['game_id'],
            'side': edge['side'],
            'odds': odds,
            'stake': stake,
            'potential_payout': potential_payout,
            'team': edge.get('team', ''),
            'opponent': edge.get('opponent', ''),
            'edge': edge['edge'],
            'ev': edge['ev']
        }
        
        print(f"Placed bet: {bet['team']} @ {odds:.3f} | Stake: ${stake:.2f} | Edge: {edge['edge']:.2%}")
        return bet
    
    def settle_bets(self) -> Dict:
        """
        Settle all pending bets for completed games.
        
        Returns settlement summary.
        """
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get pending bets for completed games
        cursor.execute("""
            SELECT pb.id, pb.game_id, pb.side, pb.odds, pb.stake,
                   g.home_score, g.away_score, g.home_team_id, g.away_team_id,
                   ht.name as home_team, at.name as away_team
            FROM paper_bets pb
            JOIN games g ON pb.game_id = g.id
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE pb.result IS NULL AND g.status = 'FINAL'
        """)
        
        pending = cursor.fetchall()
        
        settled = 0
        total_pnl = 0
        wins = 0
        losses = 0
        
        for bet in pending:
            home_won = bet['home_score'] > bet['away_score']
            
            if bet['side'] == 'home':
                bet_won = home_won
                team = bet['home_team']
            else:
                bet_won = not home_won
                team = bet['away_team']
            
            if bet_won:
                result = 'win'
                pnl = (bet['odds'] - 1) * bet['stake']
                wins += 1
            else:
                result = 'loss'
                pnl = -bet['stake']
                losses += 1
            
            settled_at = datetime.utcnow().isoformat()
            
            cursor.execute("""
                UPDATE paper_bets 
                SET result = ?, pnl = ?, settled_at = ?
                WHERE id = ?
            """, (result, pnl, settled_at, bet['id']))
            
            # Update bankroll
            self.bankroll += pnl
            self._record_bankroll_change(
                self.bankroll, pnl, 
                f"Bet settled: {team} ({result})"
            )
            
            total_pnl += pnl
            settled += 1
            
            print(f"Settled: {team} - {result.upper()} | PnL: ${pnl:+.2f}")
        
        conn.commit()
        conn.close()
        
        summary = {
            'settled': settled,
            'wins': wins,
            'losses': losses,
            'pnl': total_pnl,
            'new_bankroll': self.bankroll
        }
        
        if settled > 0:
            print(f"\nSettled {settled} bets | W: {wins} L: {losses} | PnL: ${total_pnl:+.2f}")
            print(f"Bankroll: ${self.bankroll:.2f}")
        
        return summary
    
    def get_pending_bets(self) -> List[Dict]:
        """Get all pending (unsettled) bets."""
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT pb.*, g.start_time, 
                   ht.name as home_team, at.name as away_team
            FROM paper_bets pb
            JOIN games g ON pb.game_id = g.id
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE pb.result IS NULL
            ORDER BY g.start_time
        """)
        
        bets = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return bets
    
    def get_bet_history(self, limit: int = 50) -> List[Dict]:
        """Get recent bet history."""
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT pb.*, g.start_time,
                   ht.name as home_team, at.name as away_team
            FROM paper_bets pb
            JOIN games g ON pb.game_id = g.id
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            ORDER BY pb.placed_at DESC
            LIMIT ?
        """, (limit,))
        
        bets = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return bets
    
    def get_performance_stats(self) -> Dict:
        """Get overall betting performance statistics."""
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_bets,
                SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) as losses,
                SUM(stake) as total_staked,
                SUM(pnl) as total_pnl
            FROM paper_bets
            WHERE result IS NOT NULL
        """)
        
        row = cursor.fetchone()
        conn.close()
        
        if not row or row['total_bets'] == 0:
            return {
                'total_bets': 0,
                'wins': 0,
                'losses': 0,
                'win_rate': 0,
                'total_staked': 0,
                'total_pnl': 0,
                'roi': 0,
                'current_bankroll': self.bankroll
            }
        
        total_bets = row['total_bets']
        wins = row['wins'] or 0
        total_staked = row['total_staked'] or 0
        total_pnl = row['total_pnl'] or 0
        
        return {
            'total_bets': total_bets,
            'wins': wins,
            'losses': row['losses'] or 0,
            'win_rate': wins / total_bets if total_bets > 0 else 0,
            'total_staked': total_staked,
            'total_pnl': total_pnl,
            'roi': total_pnl / total_staked if total_staked > 0 else 0,
            'current_bankroll': self.bankroll
        }

