"""iSportsAPI database schema for historical NBA odds."""
import sqlite3
from pathlib import Path
from contextlib import contextmanager

# Database path
DB_PATH = Path(__file__).parent.parent / "isportsapi.db"


@contextmanager
def get_connection():
    """Context manager for database connections."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    """Initialize the iSportsAPI database with all required tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Bookmakers table - maps company IDs to names
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bookmakers (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Games table - stores match information
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS games (
            match_id TEXT PRIMARY KEY,
            game_date DATE NOT NULL,
            home_team TEXT,
            away_team TEXT,
            home_score INTEGER,
            away_score INTEGER,
            status TEXT DEFAULT 'SCHEDULED',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Moneyline odds table - stores opening and closing odds
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS moneyline_odds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id TEXT NOT NULL,
            bookmaker_id INTEGER NOT NULL,
            opening_home REAL,
            opening_away REAL,
            closing_home REAL,
            closing_away REAL,
            eight_hour_home REAL,
            eight_hour_away REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES games(match_id),
            FOREIGN KEY (bookmaker_id) REFERENCES bookmakers(id),
            UNIQUE(match_id, bookmaker_id)
        )
    """)
    
    # Create indexes for common queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_games_date ON games(game_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_odds_match ON moneyline_odds(match_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_odds_bookmaker ON moneyline_odds(bookmaker_id)")
    
    # Insert bookmaker mappings (from iSportsAPI documentation)
    bookmakers = [
        (1, 'Macauslot'),
        (2, 'Easybets'),
        (3, 'Crown'),
        (8, 'Bet365'),
        (9, 'Vcbet'),
        (10, 'William Hill'),
        (19, 'Interwetten'),
        (20, 'Ladbrokes'),
        (31, 'Sbobet'),
        (24, '12bet'),
        (30, 'China Sports Lottery'),
        (49, 'BWin')
    ]
    
    cursor.executemany(
        "INSERT OR IGNORE INTO bookmakers (id, name) VALUES (?, ?)",
        bookmakers
    )
    
    conn.commit()
    conn.close()
    print(f"iSportsAPI database initialized at {DB_PATH}")


def reset_db():
    """Reset the database (delete all data)."""
    if DB_PATH.exists():
        DB_PATH.unlink()
    init_db()
    print("iSportsAPI database reset complete.")


if __name__ == "__main__":
    init_db()
