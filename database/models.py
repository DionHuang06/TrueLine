"""Data models for the NBA betting engine."""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Team:
    id: int
    name: str
    abbreviation: str
    current_elo: float = 1500.0


@dataclass
class Game:
    id: int
    external_id: str  # From balldontlie or odds API
    start_time: datetime
    home_team_id: int
    away_team_id: int
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    status: str = "SCHEDULED"  # SCHEDULED, LIVE, FINAL


@dataclass
class OddsSnapshot:
    id: int
    game_id: int
    book: str
    pulled_at: datetime
    home_dec: float  # Decimal odds for home team
    away_dec: float  # Decimal odds for away team


@dataclass
class EloHistory:
    id: int
    team_id: int
    game_id: int
    elo_before: float
    elo_after: float
    recorded_at: datetime


@dataclass
class Prediction:
    id: int
    game_id: int
    home_win_prob: float
    away_win_prob: float
    home_elo: float
    away_elo: float
    created_at: datetime


@dataclass
class Edge:
    id: int
    game_id: int
    side: str  # 'home' or 'away'
    best_book: str
    best_odds: float
    implied_prob: float  # De-vigged
    model_prob: float
    edge: float
    ev: float
    created_at: datetime


@dataclass
class PaperBet:
    id: int
    game_id: int
    edge_id: int
    side: str
    odds: float
    stake: float
    potential_payout: float
    result: Optional[str] = None  # 'win', 'loss', None (pending)
    pnl: Optional[float] = None
    placed_at: Optional[datetime] = None
    settled_at: Optional[datetime] = None


@dataclass
class BankrollHistory:
    id: int
    balance: float
    change: float
    reason: str
    recorded_at: datetime
