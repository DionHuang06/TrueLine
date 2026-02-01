import sqlite3
import pandas as pd
from datetime import datetime
from database.db import get_connection

# Config
SOURCE_DB = 'isportsapi.db'  # Root directory based on previous findings
TARGET_DB = 'nba_betting.db'

def matching_score(team1, team2):
    t1 = team1.lower().replace(' ', '').replace('laclippers','clippers').replace('lakers', 'lakers')
    t2 = team2.lower().replace(' ', '').replace('laclippers','clippers').replace('lakers', 'lakers')
    if t1 in t2 or t2 in t1: return True
    return False

def migrate_isports_to_main():
    print(f"Migrating NBA odds from {SOURCE_DB} to {TARGET_DB}...")
    
    # 1. Connect to Source
    try:
        src_conn = sqlite3.connect(SOURCE_DB)
    except Exception as e:
        print(f"Could not connect to source DB: {e}")
        return

    # 2. Fetch NBA Games from Source
    # We need to see the schema of 'games' in source to know how to filter for NBA
    # Inspect schema first
    c = src_conn.cursor()
    c.execute("PRAGMA table_info(games)")
    cols = [r[1] for r in c.fetchall()]
    print(f"Source Games Cols: {cols}")
    
    # Assuming there is a 'league' or 'competition' column. If not, we rely on team matching.
    # Let's check league column if exists
    has_league = 'league_name' in cols or 'league' in cols
    
    query = "SELECT * FROM games"
    if has_league:
        col_name = 'league_name' if 'league_name' in cols else 'league'
        query += f" WHERE {col_name} LIKE '%NBA%'"
    
    src_games = pd.read_sql(query, src_conn)
    print(f"Found {len(src_games)} potential NBA games in source.")
    
    # 3. Fetch Source Odds
    src_odds = pd.read_sql("SELECT * FROM moneyline_odds", src_conn)
    
    # 4. Connect to Target
    tgt_conn = get_connection()
    tgt_games = pd.read_sql("SELECT id, start_time, home_team_id, away_team_id FROM games", tgt_conn)
    
    # Get Team Names for Target
    teams = pd.read_sql("SELECT id, name FROM teams", tgt_conn)
    team_map = teams.set_index('id')['name'].to_dict()
    
    tgt_games['home_name'] = tgt_games['home_team_id'].map(team_map)
    tgt_games['away_name'] = tgt_games['away_team_id'].map(team_map)
    
    count_matched = 0
    count_odds = 0
    
    # 5. Matching Logic
    # We iterate target games and find match in source
    # Because target games are the "Master" list.
    
    for idx, t_game in tgt_games.iterrows():
        t_id = t_game['id']
        t_start = t_game['start_time'] # String YYYY-MM-DD...
        t_home = t_game['home_name']
        t_away = t_game['away_name']
        
        # Simple date match (YYYY-MM-DD)
        date_str = t_start[:10]
        
        # Filter source games by date
        # Source date col needs to be identified. Usually 'start_time' or 'date'
        # Let's try to identify date col from cols list printed earlier or guess
        # We will inspect row keys in loop
        
        # Optimization: Filter src_games by date string match
        # Assuming source 'start_time' or similar exists
        # We'll do a loose match on the fly
        
        # Find Matches
        # We look for source game on same DATE with similar TEAMS
        
        # Helper to find match in src_games
        # We assume src_games has 'home_team', 'away_team', 'start_time'
        # based on standard Isports schema usually seen
        
        # Normalize column names based on observed schema
        # Observed: ['match_id', 'game_date', 'home_team', 'away...']
        s_home_col = 'home_team'
        s_away_col = 'away_team'
        s_date_col = 'game_date'
        s_id_col = 'match_id'
        
        # Filter by date
        # Check if column exists
        if s_date_col not in src_games.columns:
            print(f"Error: {s_date_col} not found in source columns: {src_games.columns}")
            break
            
        day_matches = src_games[src_games[s_date_col].astype(str).str.startswith(date_str)]
        
        match = None
        for _, s_game in day_matches.iterrows():
            s_home = s_game[s_home_col]
            s_away = s_game[s_away_col]
            
            if matching_score(t_home, s_home) and matching_score(t_away, s_away):
                match = s_game
                break
        
        if match is not None:
            count_matched += 1
            s_id = match[s_id_col] # Source Game ID (`match_id`)
            
            # Find Odds for this Source Game
            # Foreign key is `match_id` as verified by schema check
            game_odds = src_odds[src_odds['match_id'] == s_id]
            
            if not game_odds.empty:
                # We usually take average or best if multiple books
                # The schema showed 'bookmaker_id', 'opening_home', 'closing_home'...
                # We want to insert into Target 'odds' table:
                # (game_id, book, snapshot_type, snapshot_time, home_odds, away_odds)
                
                # Check valid cols in source odds
                # 'opening_home', 'opening_away', 'closing_home', 'closing_away'
                # 'eight_hour_home', etc.

                # We will process each bookmaker row
                for _, row in game_odds.iterrows():
                    book_id = row.get('bookmaker_id', 'Unknown')
                    
                    # Store Opening
                    oh = row.get('opening_home')
                    oa = row.get('opening_away')
                    if oh and oa and oh > 1 and oa > 1:
                        # Insert OPEN (10h)
                        tgt_conn.execute("""
                            INSERT INTO odds (game_id, book, snapshot_type, snapshot_time, home_odds, away_odds)
                            VALUES (?, ?, '10h', ?, ?, ?)
                        """, (t_id, f"Book_{book_id}", date_str, oh, oa))
                        count_odds += 1

                    # Store Closing
                    ch = row.get('closing_home')
                    ca = row.get('closing_away')
                    if ch and ca and ch > 1 and ca > 1:
                        # Insert CLOSE
                        tgt_conn.execute("""
                            INSERT INTO odds (game_id, book, snapshot_type, snapshot_time, home_odds, away_odds)
                            VALUES (?, ?, 'closing', ?, ?, ?)
                        """, (t_id, f"Book_{book_id}", date_str, ch, ca))
                        count_odds += 1
                        
    tgt_conn.commit()
    src_conn.close()
    tgt_conn.close()
    
    print(f"Migration Complete.")
    print(f"Matched Games: {count_matched}")
    print(f"Inserted Odds Rows: {count_odds}")

if __name__ == "__main__":
    migrate_isports_to_main()
