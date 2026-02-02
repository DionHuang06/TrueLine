import sqlite3
import psycopg2
import psycopg2.extras
from config import DB_PATH, DB_URL

def migrate():
    print(f"Connecting to SQLite: {DB_PATH}")
    sl_conn = sqlite3.connect(DB_PATH)
    sl_conn.row_factory = sqlite3.Row
    
    print(f"Connecting to Postgres...")
    pg_conn = psycopg2.connect(DB_URL)
    pg_c = pg_conn.cursor()
    
    # --- 1. TEAMS ---
    print("\nMigrating Teams...")
    pg_c.execute("DROP TABLE IF EXISTS teams CASCADE")
    pg_c.execute("""
        CREATE TABLE teams (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            abbreviation TEXT,
            current_elo DOUBLE PRECISION DEFAULT 1500.0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    rows = sl_conn.execute("SELECT * FROM teams").fetchall()
    if rows:
        data = [tuple(r) for r in rows]
        # Inspect columns to map correctly
        # Assuming id, name, abbrev, elo, created_at order in SQLite matches?
        # Safe to explicit map
        data_mapped = []
        for r in rows:
            abbrev = r['abbreviation'] if 'abbreviation' in r.keys() else None
            created = r['created_at'] if 'created_at' in r.keys() else None
            data_mapped.append((r['id'], r['name'], abbrev, r['current_elo'], created))
        
        psycopg2.extras.execute_values(
             pg_c,
             "INSERT INTO teams (id, name, abbreviation, current_elo, created_at) VALUES %s",
             data_mapped
        )
        pg_c.execute("SELECT setval(pg_get_serial_sequence('teams', 'id'), (SELECT MAX(id) FROM teams))")

    # --- 2. GAMES ---
    print("Migrating Games...")
    pg_c.execute("DROP TABLE IF EXISTS games CASCADE")
    pg_c.execute("""
        CREATE TABLE games (
            id SERIAL PRIMARY KEY,
            external_id TEXT UNIQUE,
            start_time TEXT NOT NULL, 
            home_team_id INTEGER REFERENCES teams(id),
            away_team_id INTEGER REFERENCES teams(id),
            home_score INTEGER,
            away_score INTEGER,
            status TEXT DEFAULT 'SCHEDULED',
            season TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    rows = sl_conn.execute("SELECT * FROM games").fetchall()
    if rows:
        data_mapped = []
        for r in rows:
            # Handle potential missing columns in old sqlite schema?
            # 'season' might be missing in older `games` table versions?
            # We use .get()
            # Also keys are accessible by name thanks to row_factory
            vals = (
                r['id'], r['external_id'], r['start_time'], 
                r['home_team_id'], r['away_team_id'],
                r['home_score'], r['away_score'], r['status'],
                r.keys().__contains__('season') and r['season'] or None,
                r.keys().__contains__('created_at') and r['created_at'] or None,
                r.keys().__contains__('updated_at') and r['updated_at'] or None
            )
            data_mapped.append(vals)
            
        psycopg2.extras.execute_values(
             pg_c,
             """INSERT INTO games (id, external_id, start_time, home_team_id, away_team_id, 
                home_score, away_score, status, season, created_at, updated_at) VALUES %s""",
             data_mapped
        )
        pg_c.execute("SELECT setval(pg_get_serial_sequence('games', 'id'), (SELECT MAX(id) FROM games))")

    # --- 3. ODDS ---
    print("Migrating Odds...")
    pg_c.execute("DROP TABLE IF EXISTS odds CASCADE")
    pg_c.execute("""
        CREATE TABLE odds (
            id SERIAL PRIMARY KEY,
            game_id INTEGER REFERENCES games(id),
            book TEXT NOT NULL,
            snapshot_type TEXT NOT NULL,
            snapshot_time TEXT NOT NULL,
            home_odds DOUBLE PRECISION NOT NULL,
            away_odds DOUBLE PRECISION NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    rows = sl_conn.execute("SELECT * FROM odds").fetchall()
    if rows:
        data_mapped = []
        for r in rows:
            data_mapped.append((
                r['id'], r['game_id'], r['book'], r['snapshot_type'], r['snapshot_time'],
                r['home_odds'], r['away_odds'], r['created_at']
            ))
        psycopg2.extras.execute_values(
            pg_c, 
            "INSERT INTO odds (id, game_id, book, snapshot_type, snapshot_time, home_odds, away_odds, created_at) VALUES %s",
            data_mapped
        )
        pg_c.execute("SELECT setval(pg_get_serial_sequence('odds', 'id'), (SELECT MAX(id) FROM odds))")

    # --- 4. PAPER BETS ---
    print("Migrating Paper Bets...")
    pg_c.execute("DROP TABLE IF EXISTS paper_bets CASCADE")
    
    # Inspect columns dynamically to match SQLite
    c = sl_conn.execute("PRAGMA table_info(paper_bets)")
    cols = [col['name'] for col in c.fetchall()]
    print(f"  SQLite Cols: {cols}")
    
    # Construct Create Table based on known fields + extras
    # Common fields: id, game_id, side, odds, stake, potential_payout, result, pnl, placed_at
    # Extras: edge, ev, book, edge_id?
    
    col_defs = [
        "id SERIAL PRIMARY KEY",
        "game_id INTEGER REFERENCES games(id)",
        "side TEXT",
        "odds DOUBLE PRECISION",
        "stake DOUBLE PRECISION",
        "potential_payout DOUBLE PRECISION",
        "result TEXT",
        "pnl DOUBLE PRECISION",
        "placed_at TEXT",
        "settled_at TEXT"
    ]
    
    # Add optional cols if they exist in source
    optionals = ['edge', 'ev', 'book', 'edge_id']
    for opt in optionals:
        if opt in cols:
            col_type = "DOUBLE PRECISION" if opt in ['edge', 'ev'] else "TEXT"
            if opt == 'edge_id': col_type = "INTEGER"
            col_defs.append(f"{opt} {col_type}")
            
    create_sql = f"CREATE TABLE paper_bets ({', '.join(col_defs)})"
    pg_c.execute(create_sql)
    
    rows = sl_conn.execute("SELECT * FROM paper_bets").fetchall()
    if rows:
        # Build insert dynamically
        target_cols = ['id', 'game_id', 'side', 'odds', 'stake', 'potential_payout', 'result', 'pnl', 'placed_at', 'settled_at']
        for opt in optionals:
            if opt in cols: target_cols.append(opt)
            
        data_mapped = []
        for r in rows:
            vals = []
            for col in target_cols:
                # Handle missing key in row if schema mismatch (unlikely if cols list derived from PRAGMA)
                val = r[col] if col in r.keys() else None
                vals.append(val)
            data_mapped.append(tuple(vals))
            
        sql = f"INSERT INTO paper_bets ({', '.join(target_cols)}) VALUES %s"
        psycopg2.extras.execute_values(pg_c, sql, data_mapped)
        pg_c.execute("SELECT setval(pg_get_serial_sequence('paper_bets', 'id'), (SELECT MAX(id) FROM paper_bets))")

    pg_conn.commit()
    print("\nMigration Successful!")
    
    pg_conn.close()
    sl_conn.close()

if __name__ == "__main__":
    migrate()
