"""Ingestion module for API data."""
from ingestion.odds import OddsIngester
from ingestion.games import GamesIngester
from ingestion.results import ResultsIngester

__all__ = ['OddsIngester', 'GamesIngester', 'ResultsIngester']
