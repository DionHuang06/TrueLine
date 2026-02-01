import sqlite3
import pandas as pd
from database.db import get_connection

def analyze_profit_source():
    conn = get_connection()
    
    query = """
        SELECT side, odds, result, pnl, edge
        FROM paper_bets
        WHERE book = 'Backtest'
    """
    bets = pd.read_sql(query, conn)
    conn.close()
    
    if bets.empty:
        print("No backtest bets found.")
        return

    print(f"Total Bets: {len(bets)}")
    print(f"Total Profit: ${bets['pnl'].sum():.2f}")
    
    # Segment by Odds Type
    # Favorites: Odds < 2.0
    # Underdogs: Odds >= 2.0
    
    favorites = bets[bets['odds'] < 2.0]
    underdogs = bets[bets['odds'] >= 2.0]
    
    print("\n--- Favorites (Odds < 2.0) ---")
    print(f"Count: {len(favorites)}")
    print(f"Win Rate: {len(favorites[favorites['result']=='WIN']) / len(favorites) if len(favorites) > 0 else 0:.1%}")
    print(f"Profit: ${favorites['pnl'].sum():.2f}")
    
    print("\n--- Underdogs (Odds >= 2.0) ---")
    print(f"Count: {len(underdogs)}")
    print(f"Win Rate: {len(underdogs[underdogs['result']=='WIN']) / len(underdogs) if len(underdogs) > 0 else 0:.1%}")
    print(f"Profit: ${underdogs['pnl'].sum():.2f}")
    
    # Are there massive winners?
    print("\n--- Top 5 Profitable Bets ---")
    print(bets.sort_values(by='pnl', ascending=False).head(5)[['side', 'odds', 'pnl']])
    
    # Check "Massive Edge" performance (Edge > 50%)
    massive_edge = bets[bets['edge'] > 0.50]
    print(f"\n--- Massive Edge Bets (Edge > 50%) ---")
    print(f"Count: {len(massive_edge)}")
    print(f"Profit: ${massive_edge['pnl'].sum():.2f}")

if __name__ == "__main__":
    analyze_profit_source()
