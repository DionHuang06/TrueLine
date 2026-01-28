"""SQLite database schema and initialization."""
import sqlite3
from contextlib import contextmanager
from config import DB_PATH, STARTING_ELO_2025_26, ELO_INITIAL

# Re-export get_connection from db module for compatibility
from database.db import get_connection


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Initialize the database with all required tables."""
    conn = get_connection()
    cursor = conn.cursor()

    # Teams table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            abbreviation TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Games table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT UNIQUE,
            start_time TIMESTAMP NOT NULL,
            home_team_id INTEGER NOT NULL,
            away_team_id INTEGER NOT NULL,
            home_score INTEGER,
            away_score INTEGER,
            status TEXT DEFAULT 'SCHEDULED',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (home_team_id) REFERENCES teams(id),
            FOREIGN KEY (away_team_id) REFERENCES teams(id)
        )
    """)

    # Odds snapshots table (preserves all historical odds)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS odds_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            book TEXT NOT NULL,
            pulled_at TIMESTAMP NOT NULL,
            home_odds REAL NOT NULL,
            away_odds REAL NOT NULL,
            FOREIGN KEY (game_id) REFERENCES games(id)
        )
    """)

    # Elo history table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS elo_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER NOT NULL,
            game_id INTEGER,
            elo_before REAL NOT NULL,
            elo_after REAL NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (team_id) REFERENCES teams(id),
            FOREIGN KEY (game_id) REFERENCES games(id)
        )
    """)

    # Predictions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            home_win_prob REAL NOT NULL,
            away_win_prob REAL NOT NULL,
            home_elo REAL NOT NULL,
            away_elo REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES games(id)
        )
    """)

    # Edges table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            side TEXT NOT NULL,
            best_odds REAL NOT NULL,
            best_book TEXT NOT NULL,
            implied_prob REAL NOT NULL,
            model_prob REAL NOT NULL,
            edge REAL NOT NULL,
            ev REAL NOT NULL,
            is_bet_worthy INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES games(id)
        )
    """)

    # Paper bets table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS paper_bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            side TEXT NOT NULL,
            odds REAL NOT NULL,
            book TEXT NOT NULL,
            stake REAL NOT NULL,
            potential_payout REAL NOT NULL,
            edge REAL NOT NULL,
            ev REAL NOT NULL,
            result TEXT,
            pnl REAL,
            placed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            settled_at TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES games(id)
        )
    """)

    # Bankroll history table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bankroll_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            balance REAL NOT NULL,
            change REAL NOT NULL,
            reason TEXT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Add current_elo column if it doesn't exist (for existing databases)
    try:
        cursor.execute("ALTER TABLE teams ADD COLUMN current_elo REAL DEFAULT 1500")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    # Initialize teams with calibrated starting Elo from 2024-2025 season
    # Only update if current_elo is still at default (1500) - preserves trained ratings
    cursor.execute("SELECT id, name FROM teams")
    for row in cursor.fetchall():
        team_name = row['name']
        starting_elo = STARTING_ELO_2025_26.get(team_name, ELO_INITIAL)
        # Only update if still at default (preserves trained ratings)
        cursor.execute(
            "UPDATE teams SET current_elo = ? WHERE id = ? AND (current_elo = 1500 OR current_elo IS NULL)",
            (starting_elo, row['id'])
        )

    # Create indexes for common queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_games_start_time ON games(start_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_games_status ON games(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_odds_game_pulled ON odds_snapshots(game_id, pulled_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_elo_team ON elo_history(team_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_paper_bets_game ON paper_bets(game_id)")

    conn.commit()
    conn.close()
    print("Database initialized successfully.")


if __name__ == "__main__":
    init_db()

