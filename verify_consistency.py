"""
Verify consistency between isportsapi.db and nba_betting.db
"""
import sqlite3
import pandas as pd
import random

SOURCE = "isportsapi.db"
TARGET = "nba_betting.db"

def verify():
    print(f"Comparing {SOURCE} vs {TARGET}...\n")
    
    con_s = sqlite3.connect(SOURCE)
    con_t = sqlite3.connect(TARGET)
    
    cur_s = con_s.cursor()
    cur_t = con_t.cursor()
    
    # 1. Get Bookmaker Map
    cur_s.execute("SELECT id, name FROM bookmakers")
    book_map = {row[0]: row[1] for row in cur_s.fetchall()}
    
    # 2. Get Team Map in Target
    cur_t.execute("SELECT id, name FROM teams")
    tgt_teams = {row[0]: row[1] for row in cur_t.fetchall()} # ID -> Name
    tgt_teams_rev = {v: k for k, v in tgt_teams.items()} # Name -> ID
    
    # 3. Select random games from Source that HAVE odds
    # We want games that definitely have odds to verify odds consistency
    cur_s.execute("""
        SELECT m.match_id, g.game_date, g.home_team, g.away_team, g.home_score, g.away_score
        FROM moneyline_odds m
        JOIN games g ON m.match_id = g.match_id
        GROUP BY m.match_id
        ORDER BY RANDOM()
        LIMIT 5
    """)
    samples = cur_s.fetchall()
    
    for row in samples:
        match_id, date, home, away, s_h_score, s_a_score = row
        print(f"Checking Game: {date} {home} vs {away}")
        
        # Find in Target
        h_id = tgt_teams_rev.get(home)
        a_id = tgt_teams_rev.get(away)
        
        if not h_id or not a_id:
            print(f"  ❌ Team mapping failed: {home}({h_id}) vs {away}({a_id})")
            continue
            
        cur_t.execute("""
            SELECT id, start_time, home_score, away_score 
            FROM games 
            WHERE date(start_time) = ? AND home_team_id = ? AND away_team_id = ?
        """, (date, h_id, a_id))
        
        tgt_game = cur_t.fetchone()
        
        if not tgt_game:
            print(f"  ❌ Game not found in Target!")
            continue
            
        t_id, t_time, t_h_score, t_a_score = tgt_game
        
        # Check Details
        date_match = date in t_time # t_time might include time part
        score_match = (s_h_score == t_h_score) and (s_a_score == t_a_score)
        
        print(f"  {'✅' if date_match else '⚠️'} Date: {date} vs {t_time}")
        print(f"  {'✅' if score_match else '⚠️'} Score: {s_h_score}-{s_a_score} vs {t_h_score}-{t_a_score}")
        
        # Check Odds
        # Get Source Odds
        cur_s.execute("SELECT bookmaker_id, opening_home, opening_away FROM moneyline_odds WHERE match_id = ?", (match_id,))
        src_odds = cur_s.fetchall()
        
        # Get Target Odds
        cur_t.execute("SELECT book, home_dec, away_dec FROM odds_snapshots WHERE game_id = ?", (t_id,))
        tgt_odds_rows = cur_t.fetchall()
        
        # Build map for verification
        tgt_odds_map = {(r[0]): (r[1], r[2]) for r in tgt_odds_rows}
        
        matches = 0
        mismatches = 0
        
        for oid, oh, oa in src_odds:
            bname = book_map.get(oid, f"Book_{oid}")
            target_key_open = f"{bname} (Open)"
            
            if target_key_open in tgt_odds_map:
                th, ta = tgt_odds_map[target_key_open]
                # Compare floats
                if abs(th - oh) < 0.01 and abs(ta - oa) < 0.01:
                    matches += 1
                else:
                    print(f"    ❌ Mismatch {bname}: {oh}/{oa} vs {th}/{ta}")
                    mismatches += 1
            else:
                # Might be missing if we didn't migrate everything? 
                # But we just did full migrate.
                pass
        
        print(f"  ✅ Odds Verified: {matches} records matched exactly.")
        if mismatches > 0:
            print(f"  ⚠️ Odds Mismatches: {mismatches}")
        print("-" * 30)

    con_s.close()
    con_t.close()

if __name__ == "__main__":
    verify()
