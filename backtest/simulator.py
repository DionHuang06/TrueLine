"""Backtesting simulator with strict time-ordering."""
import csv
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from database.db import get_connection
from modeling.elo import EloModel
from edge.detector import EdgeDetector
from config import INITIAL_BANKROLL, FLAT_STAKE_PCT, MAX_STAKE_PCT, MIN_EDGE, ELO_INITIAL


@dataclass
class BacktestBet:
    """A single backtest bet."""
    game_id: int
    game_time: str
    home_team: str
    away_team: str
    side: str
    odds: float
    stake: float
    model_prob: float
    implied_prob: float
    edge: float
    ev: float
    result: Optional[str] = None
    pnl: Optional[float] = None
    closing_odds: Optional[float] = None


@dataclass
class BacktestResult:
    """Results from a backtest run."""
    start_date: str
    end_date: str
    initial_bankroll: float
    final_bankroll: float
    total_bets: int
    wins: int
    losses: int
    win_rate: float
    total_pnl: float
    roi: float
    max_drawdown: float
    max_drawdown_pct: float
    avg_edge: float
    beat_closing_pct: float
    mean_clv: float
    bets: List[BacktestBet] = field(default_factory=list)
    bankroll_history: List[Tuple[str, float]] = field(default_factory=list)


class Backtester:
    """
    Time-ordered backtesting simulator.
    
    Key features:
    - Strict chronological processing (no lookahead)
    - Uses odds snapshot closest before game start
    - Elo ratings updated in real-time as games complete
    - Tracks drawdown and bankroll history
    """
    
    def __init__(self):
        self.elo_model = EloModel()
        self.team_ratings: Dict[int, float] = {}
        self.bankroll = INITIAL_BANKROLL
        self.peak_bankroll = INITIAL_BANKROLL
        self.max_drawdown = 0.0
        self.bets: List[BacktestBet] = []
        self.bankroll_history: List[Tuple[str, float]] = []
    
    def _reset(self):
        """Reset backtester state."""
        self.team_ratings = {}
        self.bankroll = INITIAL_BANKROLL
        self.peak_bankroll = INITIAL_BANKROLL
        self.max_drawdown = 0.0
        self.bets = []
        self.bankroll_history = [(datetime.now().isoformat(), INITIAL_BANKROLL)]
    
    def _get_team_elo(self, team_id: int) -> float:
        """Get current Elo for a team."""
        return self.team_ratings.get(team_id, ELO_INITIAL)
    
    def _update_elos(self, home_team_id: int, away_team_id: int, home_won: bool,
                     home_score: int = 0, away_score: int = 0,
                     recency_weight: float = 1.0):
        """Update Elo ratings after a game (MOV + recency weighting)."""
        home_elo = self._get_team_elo(home_team_id)
        away_elo = self._get_team_elo(away_team_id)
        
        new_home, new_away = self.elo_model.update_ratings(
            home_elo, away_elo, home_won,
            home_score=home_score, away_score=away_score,
            recency_weight=recency_weight,
        )
        
        self.team_ratings[home_team_id] = new_home
        self.team_ratings[away_team_id] = new_away
    
    def _table_exists(self, cursor, name: str) -> bool:
        cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
        return cursor.fetchone() is not None

    def _get_odds_before_game(self, conn, game_id: int, game_time: str,
                              use_snapshots: bool, closing_only: bool = False) -> Optional[Dict]:
        """Get best odds before game start. closing_only: use closing line (odds table)."""
        cur = conn.cursor()
        gt = game_time.replace(" ", "T") if " " in game_time else game_time

        if use_snapshots:
            cur.execute("""
                SELECT book, home_dec, away_dec, pulled_at
                FROM odds_snapshots
                WHERE game_id = ? AND ((book = 'CSV') OR (pulled_at < ?))
                ORDER BY pulled_at DESC
            """, (game_id, gt))
        else:
            q = """
                SELECT book, snapshot_time, home_odds AS home_dec, away_odds AS away_dec
                FROM odds WHERE game_id = ? AND snapshot_time < ?
            """
            if closing_only:
                q += " AND snapshot_type = 'closing'"
            q += " ORDER BY snapshot_time DESC"
            cur.execute(q, (game_id, gt))

        rows = list(cur.fetchall())
        if not rows:
            return None

        seen = set()
        best_home = 0.0
        best_away = 0.0
        all_home: List[float] = []
        all_away: List[float] = []
        for r in rows:
            b = r["book"] if "book" in r.keys() else "?"
            if b in seen:
                continue
            seen.add(b)
            h, a = float(r["home_dec"]), float(r["away_dec"])
            if h > 0 and a > 0:
                if h > best_home:
                    best_home = h
                if a > best_away:
                    best_away = a
                all_home.append(h)
                all_away.append(a)
        if not all_home:
            return None

        ah = sum(all_home) / len(all_home)
        aa = sum(all_away) / len(all_away)
        hi, ai = 1 / ah, 1 / aa
        tot = hi + ai
        return {
            "best_home_odds": best_home,
            "best_away_odds": best_away,
            "best_home_book": "",
            "best_away_book": "",
            "home_fair": hi / tot,
            "away_fair": ai / tot,
        }

    def _get_closing_odds(self, conn, game_id: int, game_time: str,
                         use_snapshots: bool) -> Optional[Dict]:
        """Get closing-line odds (for CLV). Uses closing when odds table; else same as open."""
        return self._get_odds_before_game(
            conn, game_id, game_time, use_snapshots, closing_only=not use_snapshots
        )
    
    def _calculate_stake(self) -> float:
        """Calculate stake using flat betting."""
        stake_pct = min(FLAT_STAKE_PCT, MAX_STAKE_PCT)
        return round(self.bankroll * stake_pct, 2)
    
    def _update_drawdown(self, game_time: str):
        """Update max drawdown tracking."""
        if self.bankroll > self.peak_bankroll:
            self.peak_bankroll = self.bankroll
        
        drawdown = self.peak_bankroll - self.bankroll
        if drawdown > self.max_drawdown:
            self.max_drawdown = drawdown
        
        self.bankroll_history.append((game_time, self.bankroll))
    
    def run(self, from_date: str, to_date: str) -> BacktestResult:
        """
        Run backtest simulation.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            BacktestResult with all metrics
        """
        self._reset()
        
        conn = get_connection()
        cursor = conn.cursor()
        use_snapshots = self._table_exists(cursor, "odds_snapshots")
        if not use_snapshots and not self._table_exists(cursor, "odds"):
            conn.close()
            raise RuntimeError("No odds_snapshots or odds table found.")
        
        # Initialize team ratings
        cursor.execute("SELECT id FROM teams")
        for row in cursor.fetchall():
            self.team_ratings[row['id']] = ELO_INITIAL
        
        # Get all final games in date range, ordered by time
        cursor.execute("""
            SELECT g.id, g.start_time, g.home_team_id, g.away_team_id,
                   g.home_score, g.away_score,
                   ht.name as home_team, at.name as away_team
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE g.status = 'FINAL'
            AND date(g.start_time) >= date(?)
            AND date(g.start_time) <= date(?)
            ORDER BY g.start_time ASC
        """, (from_date, to_date))
        
        games = cursor.fetchall()
        max_game_time = max((g["start_time"] for g in games), default=None) if games else None
        
        wins = 0
        losses = 0
        total_edge = 0.0
        
        for game in games:
            game_id = game['id']
            game_time = game['start_time']
            home_team_id = game['home_team_id']
            away_team_id = game['away_team_id']
            home_score = game['home_score']
            away_score = game['away_score']
            if home_score is None or away_score is None:
                continue
            home_won = home_score > away_score
            
            odds_info = self._get_odds_before_game(
                conn, game_id, game_time, use_snapshots, closing_only=False
            )
            
            if odds_info:
                # Model prediction with rest days (matches production)
                home_elo = self._get_team_elo(home_team_id)
                away_elo = self._get_team_elo(away_team_id)
                home_rest = self.elo_model._get_rest_days(home_team_id, game_time)
                away_rest = self.elo_model._get_rest_days(away_team_id, game_time)
                home_prob, away_prob = self.elo_model.predict_game(
                    home_elo, away_elo, home_rest, away_rest
                )
                
                stake = self._calculate_stake()
                home_edge = home_prob - odds_info['home_fair']
                home_ev = (home_prob * (odds_info['best_home_odds'] - 1)) - (1 - home_prob)
                away_edge = away_prob - odds_info['away_fair']
                away_ev = (away_prob * (odds_info['best_away_odds'] - 1)) - (1 - away_prob)
                
                # At most one bet per game: take the best edge that clears MIN_EDGE and EV > 0
                home_ok = home_edge >= MIN_EDGE and home_ev > 0
                away_ok = away_edge >= MIN_EDGE and away_ev > 0
                if home_ok and away_ok:
                    if home_edge >= away_edge:
                        away_ok = False
                    else:
                        home_ok = False
                
                if home_ok:
                    closing = self._get_closing_odds(conn, game_id, game_time, use_snapshots)
                    co = closing["best_home_odds"] if closing else None
                    bet = BacktestBet(
                        game_id=game_id,
                        game_time=game_time,
                        home_team=game['home_team'],
                        away_team=game['away_team'],
                        side='home',
                        odds=odds_info['best_home_odds'],
                        stake=stake,
                        model_prob=home_prob,
                        implied_prob=odds_info['home_fair'],
                        edge=home_edge,
                        ev=home_ev,
                        closing_odds=co,
                    )
                    if home_won:
                        bet.result = 'win'
                        bet.pnl = stake * (odds_info['best_home_odds'] - 1)
                        wins += 1
                    else:
                        bet.result = 'loss'
                        bet.pnl = -stake
                        losses += 1
                    self.bankroll += bet.pnl
                    self.bets.append(bet)
                    total_edge += home_edge
                elif away_ok:
                    closing = self._get_closing_odds(conn, game_id, game_time, use_snapshots)
                    co = closing["best_away_odds"] if closing else None
                    bet = BacktestBet(
                        game_id=game_id,
                        game_time=game_time,
                        home_team=game['home_team'],
                        away_team=game['away_team'],
                        side='away',
                        odds=odds_info['best_away_odds'],
                        stake=stake,
                        model_prob=away_prob,
                        implied_prob=odds_info['away_fair'],
                        edge=away_edge,
                        ev=away_ev,
                        closing_odds=co,
                    )
                    if not home_won:
                        bet.result = 'win'
                        bet.pnl = stake * (odds_info['best_away_odds'] - 1)
                        wins += 1
                    else:
                        bet.result = 'loss'
                        bet.pnl = -stake
                        losses += 1
                    self.bankroll += bet.pnl
                    self.bets.append(bet)
                    total_edge += away_edge
            
            rw = self.elo_model._get_recency_weight(game_time, max_game_time)
            self._update_elos(
                home_team_id, away_team_id, home_won,
                home_score=int(home_score), away_score=int(away_score),
                recency_weight=rw,
            )
            self._update_drawdown(game_time)
        
        conn.close()
        
        # Calculate final metrics
        total_bets = len(self.bets)
        total_staked = sum(b.stake for b in self.bets)
        total_pnl = self.bankroll - INITIAL_BANKROLL
        with_closing = [b for b in self.bets if b.closing_odds is not None]
        beat_closing = sum(1 for b in with_closing if b.odds > b.closing_odds)
        beat_closing_pct = beat_closing / len(with_closing) if with_closing else 0.0
        mean_clv = (sum(b.odds - b.closing_odds for b in with_closing) / len(with_closing)) if with_closing else 0.0
        
        result = BacktestResult(
            start_date=from_date,
            end_date=to_date,
            initial_bankroll=INITIAL_BANKROLL,
            final_bankroll=self.bankroll,
            total_bets=total_bets,
            wins=wins,
            losses=losses,
            win_rate=wins / total_bets if total_bets > 0 else 0,
            total_pnl=total_pnl,
            roi=total_pnl / total_staked if total_staked > 0 else 0,
            max_drawdown=self.max_drawdown,
            max_drawdown_pct=self.max_drawdown / self.peak_bankroll if self.peak_bankroll > 0 else 0,
            avg_edge=total_edge / total_bets if total_bets > 0 else 0,
            beat_closing_pct=beat_closing_pct,
            mean_clv=mean_clv,
            bets=self.bets,
            bankroll_history=self.bankroll_history
        )
        
        return result
    
    def print_results(self, result: BacktestResult):
        """Print backtest results to console."""
        print("\n" + "=" * 60)
        print("BACKTEST RESULTS")
        print("=" * 60)
        print(f"Period: {result.start_date} to {result.end_date}")
        print("-" * 60)
        print(f"Initial Bankroll:  ${result.initial_bankroll:,.2f}")
        print(f"Final Bankroll:    ${result.final_bankroll:,.2f}")
        print(f"Total P&L:         ${result.total_pnl:+,.2f}")
        print("-" * 60)
        print(f"Total Bets:        {result.total_bets}")
        print(f"Wins:              {result.wins}")
        print(f"Losses:            {result.losses}")
        print(f"Win Rate:          {result.win_rate:.2%}")
        print("-" * 60)
        print(f"ROI:               {result.roi:.2%}")
        print(f"Average Edge:      {result.avg_edge:.2%}")
        print(f"Max Drawdown:      ${result.max_drawdown:,.2f} ({result.max_drawdown_pct:.2%})")
        print(f"Beat closing:      {result.beat_closing_pct:.2%}")
        print(f"Mean CLV:          {result.mean_clv:+.3f}")
        print("=" * 60)
    
    def export_to_csv(self, result: BacktestResult, filename: str = "backtest_results.csv"):
        """Export backtest results to CSV."""
        # Export bets
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Game Time', 'Home Team', 'Away Team', 'Side', 
                'Odds', 'Closing Odds', 'Stake', 'Model Prob', 'Implied Prob',
                'Edge', 'EV', 'Result', 'PnL'
            ])
            
            for bet in result.bets:
                co = f"{bet.closing_odds:.3f}" if bet.closing_odds is not None else ""
                writer.writerow([
                    bet.game_time, bet.home_team, bet.away_team, bet.side,
                    f"{bet.odds:.3f}", co, f"{bet.stake:.2f}", 
                    f"{bet.model_prob:.3f}", f"{bet.implied_prob:.3f}",
                    f"{bet.edge:.4f}", f"{bet.ev:.4f}",
                    bet.result, f"{bet.pnl:.2f}"
                ])
        
        print(f"Bets exported to {filename}")
        
        # Export summary
        summary_file = filename.replace('.csv', '_summary.csv')
        with open(summary_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Metric', 'Value'])
            writer.writerow(['Period', f"{result.start_date} to {result.end_date}"])
            writer.writerow(['Initial Bankroll', f"${result.initial_bankroll:,.2f}"])
            writer.writerow(['Final Bankroll', f"${result.final_bankroll:,.2f}"])
            writer.writerow(['Total PnL', f"${result.total_pnl:+,.2f}"])
            writer.writerow(['Total Bets', result.total_bets])
            writer.writerow(['Wins', result.wins])
            writer.writerow(['Losses', result.losses])
            writer.writerow(['Win Rate', f"{result.win_rate:.2%}"])
            writer.writerow(['ROI', f"{result.roi:.2%}"])
            writer.writerow(['Average Edge', f"{result.avg_edge:.2%}"])
            writer.writerow(['Max Drawdown', f"${result.max_drawdown:,.2f}"])
            writer.writerow(['Max Drawdown %', f"{result.max_drawdown_pct:.2%}"])
            writer.writerow(['Beat closing %', f"{result.beat_closing_pct:.2%}"])
            writer.writerow(['Mean CLV', f"{result.mean_clv:+.3f}"])
        
        print(f"Summary exported to {summary_file}")
        
        # Export bankroll history
        history_file = filename.replace('.csv', '_bankroll.csv')
        with open(history_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Timestamp', 'Bankroll'])
            for timestamp, balance in result.bankroll_history:
                writer.writerow([timestamp, f"{balance:.2f}"])
        
        print(f"Bankroll history exported to {history_file}")

