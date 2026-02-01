import pandas as pd
import numpy as np
from scipy import stats
from database.db import get_connection

def calculate_detailed_stats():
    conn = get_connection()
    
    # Fetch Data
    query = """
        SELECT game_id, side, odds as placed_odds 
        FROM paper_bets 
        WHERE book = 'Backtest'
    """
    bets = pd.read_sql(query, conn)
    
    clv_values = []
    
    for _, row in bets.iterrows():
        gid = row['game_id']
        side = row['side']
        placed = row['placed_odds']
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT AVG(home_dec), AVG(away_dec)
            FROM odds_snapshots
            WHERE game_id = ? AND book LIKE '%(Close)'
        """, (gid,))
        res = cursor.fetchone()
        
        if res and (res[0] or res[1]):
            close_val = res[0] if side == 'HOME' else res[1]
            if close_val:
                clv = (placed / close_val) - 1
                clv_values.append(clv)
    
    conn.close()

    if not clv_values:
        print("No CLV data found.")
        return

    # CLV Stats
    clv_series = pd.Series(clv_values)
    n = len(clv_series)
    mean = clv_series.mean()
    std_dev = clv_series.std(ddof=1)
    se = std_dev / np.sqrt(n)
    
    # 1. Critical Value (t-score)
    # For 95% confidence (two-tailed), alpha = 0.05 -> 0.975 percentile
    t_crit_95 = stats.t.ppf(0.975, df=n-1)
    
    # 2. Probability CLV <= 0
    # Calculations based on t-distribution of the mean
    # t_score = (Target - Mean) / SE
    # We want P(Mean_True <= 0), so Target = 0
    t_score_zero = (0 - mean) / se
    prob_negative = stats.t.cdf(t_score_zero, df=n-1)
    
    print(f"Sample Size (N): {n}")
    print(f"Mean CLV: {mean:.4f}")
    print(f"Standard Error (SE): {se:.4f}")
    print(f"\n1. Critical Value (95% CI): {t_crit_95:.4f}")
    print(f"   (This is the multiplier used: Mean ± {t_crit_95:.2f} * SE)")
    
    print(f"\n2. Probability True CLV is Negative: {prob_negative:.8f}")
    print(f"   (Percentage: {prob_negative*100:.6f}%)")

if __name__ == "__main__":
    calculate_detailed_stats()
