#!/usr/bin/env python3
"""
NBA Moneyline +EV Betting Engine

A paper trading system for finding positive expected value bets
on NBA moneyline markets using Elo ratings.

Usage:
    python main.py init              # Initialize database
    python main.py games pull        # Pull NBA schedule
    python main.py odds pull         # Pull current odds
    python main.py results pull      # Update game results
    python main.py model train       # Train Elo model
    python main.py edges today       # Find today's edges
    python main.py backtest --from 2024-01-01 --to 2024-12-31
"""

from cli import cli

if __name__ == '__main__':
    cli()

