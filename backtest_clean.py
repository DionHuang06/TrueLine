"""
Backtest Simulation (Strict Unbiased)
1. Train on games < 2026-01-01.
2. Start ALL teams at Elo 1500 (No priors).
3. Bet on games >= 2026-01-01 with Edge >= 15%.
4. Stake $1.00.
"""
import sqlite3
import pandas as pd
from modeling.elo import EloModel
from database.db import get_connection

# Settings
SPLIT_DATE = "2026-01-01"
MODEL_INIT_DATE = "2025-11-01"
MIN_EDGE = 0.15
STAKE = 1.0

def run_backtest():
    print(f"=== Backtest Simulation (Unbiased 1500 Baseline, Stake ${STAKE:.2f}) ===\n")
    
    conn = get_connection()
    
    # 1. Fetch All Final Games Sorted by Date
    query = f"""
        SELECT id, start_time, home_team_id, away_team_id, home_score, away_score
        FROM games
        WHERE status = 'FINAL' AND start_time >= '{MODEL_INIT_DATE}'
        ORDER BY start_time ASC
    """
    games = pd.read_sql(query, conn)
    
    # 2. Fetch All Odds
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
    
    # Init Ratings - STRICT 1500
    cur = conn.cursor()
    cur.execute("SELECT id FROM teams")
    for row in cur.fetchall():
        model.ratings[row[0]] = 1500.0
            
    stats = {
        "train_games": 0,
        "test_games": 0,
        "bets_placed": 0,
        "bets_won": 0,
        "profit": 0.0,
        "total_wagered": 0.0
    }
    
    print("Starting Loop...")
    
    # Reset Paper Bets
    conn.execute("DELETE FROM paper_bets WHERE book = 'Backtest'")
    conn.commit()
    
    for _, game in games.iterrows():
        gid = game['id']
        date_str = game['start_time']
        hid = game['home_team_id']
        aid = game['away_team_id']
        h_score = game['home_score']
        a_score = game['away_score']
        
        is_test = date_str >= SPLIT_DATE
        
        # Get Current Elo (Pre-Game)
        h_elo = model.get_team_rating(hid)
        a_elo = model.get_team_rating(aid)
        
        # Predict
        win_prob, _ = model.predict_game(h_elo, a_elo)
        
        if is_test:
            stats["test_games"] += 1
            odds = odds_map.get(gid)
            
            if odds:
                h_odds = odds['best_home_open']
                a_odds = odds['best_away_open']
                
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
                        
                    # INSERT INTO DB
                    # We use 'Backtest' as book name to distinguish
                    # Check if already exists? (Assuming empty DB or unique constraint? No unique on bets)
                    # We'll just insert.
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO paper_bets (game_id, side, odds, book, stake, potential_payout, edge, ev, result, pnl, placed_at)
                        VALUES (?, ?, ?, 'Backtest', ?, ?, ?, ?, ?, ?, ?)
                    """, (gid, bet_side, bet_odds, STAKE, STAKE*bet_odds, bet_ev, bet_ev, 'WIN' if won else 'LOSS', pnl, date_str))
                    # Note: placed_at using game date for backtest sorting
                    conn.commit()
        else:
             stats["train_games"] += 1

        # Update Model
        new_h, new_a = model.update_ratings(h_elo, a_elo, h_score > a_score, h_score, a_score)
        model.ratings[hid] = new_h
        model.ratings[aid] = new_a

    # Report
    conn.close()
    
    print("\n=== Results ===")
    print(f"Training Games (Pre-Jan 1): {stats['train_games']}")
    print(f"Test Games (Jan 1+): {stats['test_games']}")
    print(f"Bets Placed: {stats['bets_placed']}")
    
    if stats['bets_placed'] > 0:
        roi = stats['profit'] / stats['total_wagered']
        win_rate = stats['bets_won'] / stats['bets_placed']
        print(f"Win Rate: {win_rate:.1%}")
        print(f"Total Profit: ${stats['profit']:.2f}")
        print(f"ROI: {roi:.1%}")
    else:
        print("No bets found.")

if __name__ == "__main__":
    run_backtest()
