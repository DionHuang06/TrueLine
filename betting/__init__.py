"""Betting module for paper trading and staking."""
from betting.paper import PaperTrader
from betting.staking import calculate_stake, get_current_bankroll

__all__ = ['PaperTrader', 'calculate_stake', 'get_current_bankroll']
