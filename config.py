"""Configuration for the NBA betting engine."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Paths
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "nba_betting.db"

# API Keys
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
BALLDONTLIE_API_KEY = os.getenv("BALLDONTLIE_API_KEY", "")
ISPORTSAPI_KEY = os.getenv("ISPORTSAPI_KEY", "")

# API Endpoints
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
BALLDONTLIE_API_BASE = "https://api.balldontlie.io/v1"

# Betting parameters
INITIAL_BANKROLL = 10000.0
FLAT_STAKE_PCT = 0.005  # 0.5% of bankroll
MAX_STAKE_PCT = 0.01    # 1% cap
MIN_EDGE = 0.15         # 15% minimum edge (based on user preference)
MAX_EDGE = 1.0          # No maximum filter - if model finds high edge, trust it
                        # With calibrated starting Elo, unrealistic edges should be rare
                        # If 15%+ edges exist, they should be real (not filtered)
MIN_EV = 0.0            # Minimum EV (positive)

# Legacy aliases for compatibility
STAKE_FRACTION = FLAT_STAKE_PCT
STAKE_CAP = MAX_STAKE_PCT
BANKROLL_INITIAL = INITIAL_BANKROLL

# Elo parameters
ELO_K_FACTOR = 20
ELO_HOME_ADVANTAGE = 100  # ~3.5 point spread equivalent
ELO_INITIAL = 1500  # Default for teams not in standings
MIN_GAMES_FOR_BETTING = 0  # No minimum games restriction - removed to allow more betting opportunities
                            # Confidence adjustment in _adjust_confidence() handles early-season uncertainty

# Enhanced Elo features (can be enabled/disabled)
ELO_USE_MARGIN_WEIGHTING = True  # Weight Elo updates by margin of victory
ELO_USE_REST_DAYS = True  # Adjust predictions for rest days and back-to-back games
ELO_USE_RECENCY = True  # Weight recent games more heavily
# Recency: games within last N days of max game time get RECENCY_WEIGHT multiplier
RECENCY_LAST_DAYS = 14
RECENCY_WEIGHT = 1.5

# Betting strategy
USE_KELLY_CRITERION = True  # Use Kelly Criterion for optimal bet sizing (fractional Kelly at 50%)
KELLY_FRACTION = 0.5  # Fraction of full Kelly to use (0.5 = half Kelly, safer)

# Starting Elo ratings based on 2024-2025 season standings
# Formula: Elo = 1500 + (win_pct - 0.500) * 1000 (reduced from 2000 to compress range)
# Capped at 1650 max and 1350 min to prevent extreme differences (300 point range)
# This prevents all teams starting at 1500, but keeps range reasonable
STARTING_ELO_2025_26 = {
    "Cleveland Cavaliers": 1650,      # 78.0% win rate (capped)
    "Boston Celtics": 1650,            # 74.4% win rate (capped)
    "Oklahoma City Thunder": 1650,    # 82.9% win rate (capped)
    "Houston Rockets": 1634,           # 63.4% win rate
    "New York Knicks": 1622,          # 62.2% win rate
    "Indiana Pacers": 1610,           # 61.0% win rate
    "Los Angeles Lakers": 1610,       # 61.0% win rate
    "Denver Nuggets": 1610,           # 61.0% win rate
    "LA Clippers": 1610,              # 61.0% win rate
    "Minnesota Timberwolves": 1598,   # 59.8% win rate
    "Milwaukee Bucks": 1585,          # 58.5% win rate
    "Golden State Warriors": 1585,    # 58.5% win rate
    "Memphis Grizzlies": 1585,        # 58.5% win rate
    "Detroit Pistons": 1537,          # 53.7% win rate
    "Orlando Magic": 1500,            # 50.0% win rate
    "Atlanta Hawks": 1488,            # 48.8% win rate
    "Sacramento Kings": 1488,         # 48.8% win rate
    "Chicago Bulls": 1476,            # 47.6% win rate
    "Dallas Mavericks": 1476,         # 47.6% win rate
    "Miami Heat": 1451,               # 45.1% win rate
    "Phoenix Suns": 1439,             # 43.9% win rate
    "Portland Trail Blazers": 1439,    # 43.9% win rate
    "San Antonio Spurs": 1415,        # 41.5% win rate
    "Toronto Raptors": 1366,           # 36.6% win rate
    "Brooklyn Nets": 1350,             # 31.7% win rate (capped)
    "Philadelphia 76ers": 1350,       # 29.3% win rate (capped)
    "Charlotte Hornets": 1350,         # 23.2% win rate (capped)
    "Washington Wizards": 1350,       # 22.0% win rate (capped)
    "New Orleans Pelicans": 1350,     # 25.6% win rate (capped)
    "Utah Jazz": 1350,                # 20.7% win rate (capped)
}

# Team name mappings (The Odds API -> balldontlie)
TEAM_MAPPINGS = {
    "Atlanta Hawks": "Atlanta Hawks",
    "Boston Celtics": "Boston Celtics",
    "Brooklyn Nets": "Brooklyn Nets",
    "Charlotte Hornets": "Charlotte Hornets",
    "Chicago Bulls": "Chicago Bulls",
    "Cleveland Cavaliers": "Cleveland Cavaliers",
    "Dallas Mavericks": "Dallas Mavericks",
    "Denver Nuggets": "Denver Nuggets",
    "Detroit Pistons": "Detroit Pistons",
    "Golden State Warriors": "Golden State Warriors",
    "Houston Rockets": "Houston Rockets",
    "Indiana Pacers": "Indiana Pacers",
    "Los Angeles Clippers": "LA Clippers",
    "Los Angeles Lakers": "Los Angeles Lakers",
    "Memphis Grizzlies": "Memphis Grizzlies",
    "Miami Heat": "Miami Heat",
    "Milwaukee Bucks": "Milwaukee Bucks",
    "Minnesota Timberwolves": "Minnesota Timberwolves",
    "New Orleans Pelicans": "New Orleans Pelicans",
    "New York Knicks": "New York Knicks",
    "Oklahoma City Thunder": "Oklahoma City Thunder",
    "Orlando Magic": "Orlando Magic",
    "Philadelphia 76ers": "Philadelphia 76ers",
    "Phoenix Suns": "Phoenix Suns",
    "Portland Trail Blazers": "Portland Trail Blazers",
    "Sacramento Kings": "Sacramento Kings",
    "San Antonio Spurs": "San Antonio Spurs",
    "Toronto Raptors": "Toronto Raptors",
    "Utah Jazz": "Utah Jazz",
    "Washington Wizards": "Washington Wizards",
}

# Reverse mapping
TEAM_MAPPINGS_REVERSE = {v: k for k, v in TEAM_MAPPINGS.items()}
