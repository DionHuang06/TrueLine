"""New database schema for games and odds (10h before and closing line)."""
import sqlite3
from pathlib import Path
from config import DB_PATH


def get_connection() -> sqlite3.Connection:
    """Get a database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_new_db():
    """Initialize new database schema with games and odds."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Drop old tables if they exist (children before parents; odds -> games -> teams)
    old_tables = [
        'predictions', 'edges', 'paper_bets', 'bankroll_history',
        'elo_history', 'odds_snapshots', 'odds', 'games', 'teams'
    ]
    
    print("Dropping old tables...")
    for table in old_tables:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        except sqlite3.OperationalError:
            pass
    
    # Teams table - stores NBA teams
    cursor.execute("""
        CREATE TABLE teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            abbreviation TEXT,
            current_elo REAL DEFAULT 1500.0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Games table - stores all game information
    cursor.execute("""
        CREATE TABLE games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT UNIQUE,
            start_time TEXT NOT NULL,
            home_team_id INTEGER NOT NULL,
            away_team_id INTEGER NOT NULL,
            home_score INTEGER,
            away_score INTEGER,
            status TEXT DEFAULT 'SCHEDULED',
            season TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (home_team_id) REFERENCES teams(id),
            FOREIGN KEY (away_team_id) REFERENCES teams(id)
        )
    """)
    
    # Odds table - stores odds at specific times (10h before and closing line)
    cursor.execute("""
        CREATE TABLE odds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            book TEXT NOT NULL,
            snapshot_type TEXT NOT NULL,
            snapshot_time TEXT NOT NULL,
            home_odds REAL NOT NULL,
            away_odds REAL NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES games(id),
            CHECK (snapshot_type IN ('10h', 'closing'))
        )
    """)
    
    # Create indexes for efficient queries
    cursor.execute("CREATE INDEX idx_games_start_time ON games(start_time)")
    cursor.execute("CREATE INDEX idx_games_status ON games(status)")
    cursor.execute("CREATE INDEX idx_games_season ON games(season)")
    cursor.execute("CREATE INDEX idx_odds_game ON odds(game_id)")
    cursor.execute("CREATE INDEX idx_odds_type ON odds(snapshot_type)")
    cursor.execute("CREATE INDEX idx_odds_game_type ON odds(game_id, snapshot_type)")
    
    conn.commit()
    conn.close()
    print("New database schema initialized successfully.")
    print("Tables created: teams, games, odds")


def reset_to_new_schema():
    """Reset database to new schema (drops all old data)."""
    import os
    if DB_PATH.exists():
        print(f"Removing old database: {DB_PATH}")
        os.remove(DB_PATH)
    init_new_db()


if __name__ == "__main__":
    reset_to_new_schema()
