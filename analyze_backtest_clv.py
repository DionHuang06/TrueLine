import sqlite3
import pandas as pd
import numpy as np
from database.db import get_connection

def analyze_clv():
    conn = get_connection()
    
    # 1. Fetch Bets
    print("Fetching backtest bets...")
    bets_query = """
        SELECT game_id, side, odds as placed_odds, result
        FROM paper_bets
        WHERE book = 'Backtest'
    """
    bets = pd.read_sql(bets_query, conn)
    
    if bets.empty:
        print("No bets found.")
        conn.close()
        return

    # 2. Fetch Closing Odds
    # We will use the Average Closing Odds across all books for a robust market price.
    print("Fetching closing odds...")
    
    clv_data = []
    
    for _, row in bets.iterrows():
        gid = row['game_id']
        side = row['side']
        placed = row['placed_odds']
        
        # Get Avg Closing Odds for this game
        # We filter by LIKE '%(Close)'
        q = """
            SELECT AVG(home_dec) as avg_h, AVG(away_dec) as avg_a, COUNT(*) as cnt
            FROM odds_snapshots
            WHERE game_id = ? AND book LIKE '%(Close)'
        """
        cursor = conn.cursor()
        cursor.execute(q, (gid,))
        res = cursor.fetchone()
        
        if res and res[2] > 0:
            avg_h, avg_a = res[0], res[1]
            closing_price = avg_h if side == 'HOME' else avg_a
            
            if closing_price:
                # CLV Calculation: (Placed / Close) - 1
                clv_pct = (placed / closing_price) - 1
                clv_data.append({
                    "game_id": gid,
                    "side": side,
                    "placed": placed,
                    "close": closing_price,
                    "clv": clv_pct,
                    "result": row['result']
                })
    
    conn.close()
    
    # 3. Analyze
    if not clv_data:
        print("Could not find closing odds for bets.")
        return
        
    df = pd.DataFrame(clv_data)
    
    avg_clv = df['clv'].mean()
    beating_line_pct = len(df[df['clv'] > 0]) / len(df)
    
    print("\n=== Closing Line Value (CLV) Analysis ===")
    print(f"Bets Analyzed: {len(df)}")
    print(f"Average CLV: {avg_clv:.2%}")
    print(f"Beating Closing Line: {beating_line_pct:.1%} of the time")
    
    print("\n--- Breakdown by Result ---")
    wins = df[df['result'] == 'WIN']
    losses = df[df['result'] == 'LOSS']
    print(f"Avg CLV on Wins: {wins['clv'].mean():.2%}")
    print(f"Avg CLV on Losses: {losses['clv'].mean():.2%}")
    
    print("\n--- Interpretation ---")
    if avg_clv > 0.02:
        print("POSITIVE SIGNAL: You are consistently beating the market. Your edge is likely real.")
    elif avg_clv < -0.02:
        print("NEGATIVE SIGNAL: The market is moving against you. Your profits are likely luck/variance.")
    else:
        print("NEUTRAL SIGNAL: You are trading roughly at market value.")

if __name__ == "__main__":
    analyze_clv()
