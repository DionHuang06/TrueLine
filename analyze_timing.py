import sqlite3
import pandas as pd
from database.db import get_connection

def analyze_line_movement():
    conn = get_connection()
    
    # Get winning bets from backtest
    query = """
        SELECT game_id, side, odds as open_odds, result
        FROM paper_bets
        WHERE book = 'Backtest'
    """
    bets = pd.read_sql(query, conn)
    
    if bets.empty:
        print("No bets.")
        return
        
    print(f"Analyzing line movement for {len(bets)} bets...")
    
    moves = []
    
    for _, row in bets.iterrows():
        gid = row['game_id']
        side = row['side']
        open_val = row['open_odds']
        
        # Get Close
        cursor = conn.cursor()
        cursor.execute("""
            SELECT AVG(home_dec), AVG(away_dec)
            FROM odds_snapshots
            WHERE game_id = ? AND book LIKE '%(Close)'
        """, (gid,))
        res = cursor.fetchone()
        
        if res:
            close_val = res[0] if side == 'HOME' else res[1]
            if close_val:
                # Did line get worse (Shorten) or Better (Drift)?
                # We want Open > Close (Shortening) -> Betting Early was Good
                diff = open_val - close_val
                moves.append({
                    "open": open_val,
                    "close": close_val,
                    "diff": diff,
                    "result": row['result'],
                    "favorable_move": diff > 0 # True if Odds went down (we got higher odds)
                })
    
    conn.close()
    
    df = pd.DataFrame(moves)
    
    favorable = len(df[df['favorable_move']])
    total = len(df)
    
    print(f"\n--- Line Movement Analysis ---")
    print(f"Total Bets: {total}")
    print(f"Line Moved in our Favor (Shortened): {favorable} ({favorable/total:.1%})")
    print(f"Line Moved Against us (Drifted): {total - favorable} ({(total - favorable)/total:.1%})")
    
    print("\n--- Conclusion ---")
    if favorable > total / 2:
        print("MOST lines got WORSE (lower odds) by closing.")
        print("Recommendation: Bet EARLY. Waiting loses value.")
    else:
        print("Most lines got BETTER (higher odds) by closing.")
        print("Recommendation: You could wait.")

if __name__ == "__main__":
    analyze_line_movement()
