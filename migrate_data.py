"""
Migrate clean data from isportsapi.db to nba_betting.db
"""
import sqlite3
import pandas as pd
from datetime import datetime
import uuid

SOURCE_DB = "isportsapi.db"
TARGET_DB = "nba_betting.db"

def migrate():
    print("Beginning migration...")
    
    # Connect
    conn_src = sqlite3.connect(SOURCE_DB)
    conn_tgt = sqlite3.connect(TARGET_DB)
    
    c_src = conn_src.cursor()
    c_tgt = conn_tgt.cursor()
    
    # 1. Load Bookmaker Map
    c_src.execute("SELECT id, name FROM bookmakers")
    book_map = {row[0]: row[1] for row in c_src.fetchall()}
    
    # 2. Load Team Map (Target)
    c_tgt.execute("SELECT id, name FROM teams")
    tgt_teams = {row[1]: row[0] for row in c_tgt.fetchall()}
    
    # 3. Load Games from Source
    c_src.execute("""
        SELECT match_id, game_date, home_team, away_team, home_score, away_score, status 
        FROM games 
        WHERE league_id = 111
    """)
    src_games = c_src.fetchall()
    
    print(f"Found {len(src_games)} games in Source.")
    
    stats = {"games_new": 0, "games_updated": 0, "odds_added": 0, "teams_added": 0}
    
    for g in src_games:
        match_id, date, home, away, h_score, a_score, status = g
        
        # Ensure teams exist
        for team in [home, away]:
            if team not in tgt_teams:
                print(f"Adding new team: {team}")
                c_tgt.execute("INSERT INTO teams (name) VALUES (?)", (team,))
                tgt_teams[team] = c_tgt.lastrowid
                stats["teams_added"] += 1
        
        h_id = tgt_teams[home]
        a_id = tgt_teams[away]
        
        # Check if game exists (by Date and Teams)
        # Note: Time in isports db is YYYY-MM-DD usually, or TS?
        # In schema it's DATE. In main DB it's TIMESTAMP.
        # We'll match by date(start_time)
        
        c_tgt.execute("""
            SELECT id FROM games 
            WHERE date(start_time) = ? AND home_team_id = ? AND away_team_id = ?
        """, (date, h_id, a_id))
        
        existing = c_tgt.fetchone()
        
        if existing:
            game_id = existing[0]
            # Update scores if missing
            c_tgt.execute("""
                UPDATE games SET home_score = ?, away_score = ?, status = 'FINAL'
                WHERE id = ? AND (home_score IS NULL OR status != 'FINAL')
            """, (h_score, a_score, game_id))
            if c_tgt.rowcount > 0:
                stats["games_updated"] += 1
        else:
            # Insert new
            ext_id = str(uuid.uuid4())
            c_tgt.execute("""
                INSERT INTO games (external_id, start_time, home_team_id, away_team_id, home_score, away_score, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (ext_id, date, h_id, a_id, h_score, a_score, 'FINAL' if h_score else 'SCHEDULED'))
            game_id = c_tgt.lastrowid
            stats["games_new"] += 1

        # Migrate Odds
        c_src.execute("SELECT bookmaker_id, opening_home, opening_away, closing_home, closing_away FROM moneyline_odds WHERE match_id = ?", (match_id,))
        odds_rows = c_src.fetchall()
        
        for o_row in odds_rows:
            bid, oh, oa, ch, ca = o_row
            book_name = book_map.get(bid, f"Book_{bid}")
            
            # Insert Opening
            if oh and oa:
                c_tgt.execute("""
                    INSERT OR IGNORE INTO odds_snapshots (game_id, book, home_dec, away_dec, pulled_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (game_id, f"{book_name} (Open)", oh, oa, f"{date} 00:00:00"))
                if c_tgt.rowcount > 0: stats["odds_added"] += 1
                
            # Insert Closing
            if ch and ca:
                c_tgt.execute("""
                    INSERT OR IGNORE INTO odds_snapshots (game_id, book, home_dec, away_dec, pulled_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (game_id, f"{book_name} (Close)", ch, ca, f"{date} 23:59:59"))
                if c_tgt.rowcount > 0: stats["odds_added"] += 1
    
    conn_tgt.commit()
    conn_src.close()
    conn_tgt.close()
    
    print("\nMigration Complete!")
    print(f"  New Games: {stats['games_new']}")
    print(f"  Updated Games: {stats['games_updated']}")
    print(f"  Odds Entries: {stats['odds_added']}")
    print(f"  Teams Added: {stats['teams_added']}")

if __name__ == "__main__":
    migrate()
