"""
Backtest Simulation:
1. Train on games < 2026-01-01.
2. Bet on games >= 2026-01-01 with Edge >= 15%.
3. Use Opening Odds (Best Available).
"""
import sqlite3
import pandas as pd
from datetime import datetime
from modeling.elo import EloModel
from database.db import get_connection

# Settings
SPLIT_DATE = "2026-01-01"
MIN_EDGE = 0.15
STAKE = 100.0

def run_backtest():
    print(f"=== Backtest Simulation (Split: {SPLIT_DATE}, Edge: {MIN_EDGE:.0%}) ===\n")
    
    conn = get_connection()
    
    # 1. Fetch All Final Games Sorted by Date
    query = """
        SELECT id, start_time, home_team_id, away_team_id, home_score, away_score
        FROM games
        WHERE status = 'FINAL'
        ORDER BY start_time ASC
    """
    games = pd.read_sql(query, conn)
    
    # 2. Fetch All Odds (Optimized)
    # Get Best Opening Odds per game
    odds_query = """
        SELECT game_id, 
               MAX(home_dec) as best_home_open, 
               MAX(away_dec) as best_away_open
        FROM odds_snapshots
        WHERE book LIKE '%(Open)'
        GROUP BY game_id
    """
    odds_df = pd.read_sql(odds_query, conn)
    odds_map = odds_df.set_index('game_id').to_dict('index')
    
    # Initialize Model
    model = EloModel()
    # Initialize ratings from config manually (usually model.train does this, but we're doing custom loop)
    # We'll use model.train() logic partially:
    # First, let's load initial ratings from DB (which were set from config)
    # Actually, model.ratings is empty initially.
    # We should call a helper or manually init.
    # We'll rely on the fact that if we just start updating, it uses defaults/starts.
    # BUT we want 2025-26 starting ratings.
    # So we should call model.train(from_date) with a date far in past?
    # Or just loop ourselves.
    
    # Load starting ratings from CONFIG via temporary DB reset? 
    # No, assuming DB `teams` table already has current_elo updated... 
    # actually, previous runs might have changed `current_elo` in DB.
    # We should reset `self.ratings` in memory to `STARTING_ELO_2025_26`.
    from config import STARTING_ELO_2025_26, ELO_INITIAL
    
    # Get ID mapping
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM teams")
    teams_map = {row[1]: row[0] for row in cur.fetchall()}
    id_to_name = {v: k for k, v in teams_map.items()}
    
    # Init Ratings
    for name, elo in STARTING_ELO_2025_26.items():
        if name in teams_map:
            model.ratings[teams_map[name]] = elo
            
    stats = {
        "train_games": 0,
        "test_games": 0,
        "bets_placed": 0,
        "bets_won": 0,
        "profit": 0.0,
        "total_wagered": 0.0
    }
    
    history = []
    
    print("Starting Loop...")
    
    for _, game in games.iterrows():
        gid = game['id']
        date_str = game['start_time']
        hid = game['home_team_id']
        aid = game['away_team_id']
        h_score = game['home_score']
        a_score = game['away_score']
        
        # Determine Phase
        is_test = date_str >= SPLIT_DATE
        
        # Get Current Elo (Pre-Game)
        h_elo = model.get_team_rating(hid)
        a_elo = model.get_team_rating(aid)
        
        # Predict
        win_prob, _ = model.predict_game(h_elo, a_elo)
        
        # If Test Phase: Check for Bet
        if is_test:
            stats["test_games"] += 1
            odds = odds_map.get(gid)
            
            if odds:
                h_odds = odds['best_home_open']
                a_odds = odds['best_away_open']
                
                # Check Edge
                # EV = (Prob * Odds) - 1
                ev_home = (win_prob * h_odds) - 1 if h_odds else -1
                ev_away = ((1 - win_prob) * a_odds) - 1 if a_odds else -1
                
                bet_side = None
                bet_ev = 0
                bet_odds = 0
                
                if ev_home >= MIN_EDGE:
                    bet_side = 'HOME'
                    bet_ev = ev_home
                    bet_odds = h_odds
                elif ev_away >= MIN_EDGE:
                    bet_side = 'AWAY'
                    bet_ev = ev_away
                    bet_odds = a_odds
                
                if bet_side:
                    stats["bets_placed"] += 1
                    stats["total_wagered"] += STAKE
                    
                    # Resolve Bet
                    home_won = h_score > a_score
                    won = False
                    if bet_side == 'HOME' and home_won:
                        won = True
                    elif bet_side == 'AWAY' and not home_won:
                        won = True
                        
                    pnl = (STAKE * bet_odds - STAKE) if won else -STAKE
                    stats["profit"] += pnl
                    if won:
                        stats["bets_won"] += 1
                        
                    history.append({
                        "date": date_str,
                        "match": f"{id_to_name.get(hid)} vs {id_to_name.get(aid)}",
                        "bet": bet_side,
                        "odds": bet_odds,
                        "prob": win_prob if bet_side=='HOME' else (1-win_prob),
                        "edge": bet_ev,
                        "result": "WIN" if won else "LOSS",
                        "pnl": pnl
                    })
        else:
             stats["train_games"] += 1

        # Update Model (Post-Game)
        # Assuming EloModel.update_ratings returns new ratings
        # And importantly, UPDATES self.ratings internally?
        # Check elo.py line 33: self.ratings = {}.
        # Line 199: return new_home, new_away.
        # Does it update self.ratings?
        # Code view of update_ratings (lines 162-200) showed it calculates and returns.
        # I need to check if it updates `self.ratings`.
        # Usually it does, or caller must.
        # I'll Assume I must update it.
        
        new_h, new_a = model.update_ratings(h_elo, a_elo, h_score > a_score, h_score, a_score)
        model.ratings[hid] = new_h
        model.ratings[aid] = new_a

    # Report
    print("\n=== Results ===")
    print(f"Training Games: {stats['train_games']}")
    print(f"Test Games: {stats['test_games']}")
    print(f"Bets Placed: {stats['bets_placed']}")
    
    if stats['bets_placed'] > 0:
        roi = stats['profit'] / stats['total_wagered']
        win_rate = stats['bets_won'] / stats['bets_placed']
        print(f"Win Rate: {win_rate:.1%}")
        print(f"Total Profit: ${stats['profit']:.2f}")
        print(f"ROI: {roi:.1%}")
        
        print("\nLast 10 Bets:")
        for b in history[-10:]:
            print(f"{b['date']} | {b['match']} | {b['bet']} @ {b['odds']:.2f} | Edge: {b['edge']:.1%} | {b['result']} (${b['pnl']:.0f})")
    else:
        print("No bets found matching criteria.")

    conn.close()

if __name__ == "__main__":
    run_backtest()
