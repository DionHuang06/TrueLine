"""Database connection and initialization."""
import sqlite3
from config import DB_PATH


def get_connection():
    """Get a database connection with row factory."""
    # Use Postgres if DB_URL is configured
    from config import DB_URL
    if DB_URL and "postgres" in DB_URL:
        from database.compat import get_postgres_connection_with_retry
        return get_postgres_connection_with_retry()
    
    # Fallback to SQLite
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    """Initialize the database with all required tables."""
    # Use new schema
    from database.new_schema import init_new_db
    return init_new_db()
    
    # Teams table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            abbreviation TEXT,
            current_elo REAL DEFAULT 1500.0
        )
    """)
    
    # Games table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT UNIQUE,
            start_time TEXT NOT NULL,
            home_team_id INTEGER NOT NULL,
            away_team_id INTEGER NOT NULL,
            home_score INTEGER,
            away_score INTEGER,
            status TEXT DEFAULT 'SCHEDULED',
            FOREIGN KEY (home_team_id) REFERENCES teams(id),
            FOREIGN KEY (away_team_id) REFERENCES teams(id)
        )
    """)
    
    # Odds snapshots table - stores every snapshot, never overwrites
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS odds_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            book TEXT NOT NULL,
            pulled_at TEXT NOT NULL,
            home_dec REAL NOT NULL,
            away_dec REAL NOT NULL,
            FOREIGN KEY (game_id) REFERENCES games(id)
        )
    """)
    
    # Create indexes for efficient queries
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_odds_game_time 
        ON odds_snapshots(game_id, pulled_at)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_games_start_time 
        ON games(start_time)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_games_status 
        ON games(status)
    """)
    
    # Elo history table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS elo_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER NOT NULL,
            game_id INTEGER NOT NULL,
            elo_before REAL NOT NULL,
            elo_after REAL NOT NULL,
            recorded_at TEXT NOT NULL,
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
            created_at TEXT NOT NULL,
            FOREIGN KEY (game_id) REFERENCES games(id)
        )
    """)
    
    # Edges table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            side TEXT NOT NULL,
            best_book TEXT NOT NULL,
            best_odds REAL NOT NULL,
            implied_prob REAL NOT NULL,
            model_prob REAL NOT NULL,
            edge REAL NOT NULL,
            ev REAL NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (game_id) REFERENCES games(id)
        )
    """)
    
    # Paper bets table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS paper_bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            edge_id INTEGER,
            side TEXT NOT NULL,
            odds REAL NOT NULL,
            stake REAL NOT NULL,
            potential_payout REAL NOT NULL,
            result TEXT,
            pnl REAL,
            placed_at TEXT NOT NULL,
            settled_at TEXT,
            FOREIGN KEY (game_id) REFERENCES games(id),
            FOREIGN KEY (edge_id) REFERENCES edges(id)
        )
    """)
    
    # Bankroll history table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bankroll_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            balance REAL NOT NULL,
            change REAL NOT NULL,
            reason TEXT,
            recorded_at TEXT NOT NULL
        )
    """)
    
    conn.commit()
    conn.close()
    print("Database initialized successfully.")


def reset_db():
    """Reset the database by deleting and reinitializing."""
    import os
    if DB_PATH.exists():
        DB_PATH.unlink()
    init_db()

