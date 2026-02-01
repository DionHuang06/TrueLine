"""
Analyze Closing Line Value (CLV)
CLV = (Bet Odds / Closing Odds) - 1
We compare our "Backtest" bets (Opening Lines) against the Average Closing Odds.
"""
import sqlite3
import pandas as pd

def analyze_clv():
    conn = sqlite3.connect("nba_betting.db")
    
    # 1. Get Bets
    bets = pd.read_sql("""
        SELECT id, game_id, side, odds as bet_odds
        FROM paper_bets
        WHERE book = 'Backtest'
    """, conn)
    
    if bets.empty:
        print("No bets found.")
        return

    clv_list = []
    beat_closing = 0
    total_bets = 0
    
    print(f"Analyzing {len(bets)} bets for CLV...")
    
    for _, row in bets.iterrows():
        gid = row['game_id']
        side = row['side']
        bet_price = row['bet_odds']
        
        # 2. Get Closing Odds for this game
        # We average all closing lines for the specific side
        col = "home_dec" if side == 'HOME' else "away_dec"
        
        q = f"""
            SELECT AVG({col}) 
            FROM odds_snapshots 
            WHERE game_id = ? AND book LIKE '%(Close)'
        """
        cursor = conn.cursor()
        cursor.execute(q, (gid,))
        res = cursor.fetchone()
        
        avg_close = res[0] if res and res[0] else None
        
        if avg_close:
            clv = (bet_price / avg_close) - 1
            clv_list.append(clv)
            total_bets += 1
            if bet_price > avg_close:
                beat_closing += 1
        
    conn.close()
    
    if total_bets > 0:
        avg_clv = sum(clv_list) / total_bets
        beat_rate = beat_closing / total_bets
        
        print("\n=== CLV Analysis ===")
        print(f"Bets Analyzed: {total_bets}")
        print(f"Average CLV: {avg_clv:.2%}")
        print(f"Beat Closing Line: {beat_rate:.1%} of the time")
        
        if avg_clv > 0:
            print("✅ Positive CLV: You are beating the market moves.")
        else:
            print("⚠️ Negative CLV: The market is moving against you.")
            
    else:
        print("Could not find closing odds for bets.")

if __name__ == "__main__":
    analyze_clv()
