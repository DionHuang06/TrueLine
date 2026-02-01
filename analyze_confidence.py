import sqlite3
import pandas as pd
import numpy as np
from scipy import stats
from database.db import get_connection

def calculate_confidence_intervals():
    conn = get_connection()
    
    # 1. Fetch Backtest Bets
    print("Fetching bets...")
    bets_query = """
        SELECT game_id, side, odds as placed_odds, result, pnl, stake
        FROM paper_bets
        WHERE book = 'Backtest'
    """
    bets = pd.read_sql(bets_query, conn)
    
    if bets.empty:
        print("No bets found.")
        conn.close()
        return

    # 2. Fetch Closing Odds for CLV
    print("Fetching closing odds...")
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
                # CLV% = (Placed / Close) - 1
                clv = (placed / close_val) - 1
                clv_values.append(clv)
    
    conn.close()

    # 3. Calculate ROI Stats
    # ROI per bet = PnL / Stake
    bets['roi_pct'] = bets['pnl'] / bets['stake']
    roi_series = bets['roi_pct']
    
    n_bets = len(roi_series)
    mean_roi = roi_series.mean()
    std_roi = roi_series.std(ddof=1) # Sample standard deviation
    se_roi = std_roi / np.sqrt(n_bets) # Standard Error
    
    # t-statistic for 95% CI
    t_crit = stats.t.ppf(0.975, df=n_bets-1)
    
    roi_lower = mean_roi - t_crit * se_roi
    roi_upper = mean_roi + t_crit * se_roi
    
    print(f"\n=== ROI Analysis (95% Confidence) ===")
    print(f"Sample Size (N): {n_bets}")
    print(f"Mean ROI: {mean_roi:.2%}")
    print(f"Standard Deviation: {std_roi:.2%}")
    print(f"Standard Error: {se_roi:.2%}")
    print(f"95% CI Range: [{roi_lower:.2%}, {roi_upper:.2%}]")
    
    if roi_lower > 0:
        print("✅ Statistically Significant Profit (Lower bound > 0)")
    else:
        print("⚠️ ROI not statistically distinguishable from zero yet.")

    # 4. Calculate CLV Stats
    if clv_values:
        clv_series = pd.Series(clv_values)
        n_clv = len(clv_series)
        mean_clv = clv_series.mean()
        std_clv = clv_series.std(ddof=1)
        se_clv = std_clv / np.sqrt(n_clv)
        
        t_crit_clv = stats.t.ppf(0.975, df=n_clv-1)
        
        clv_lower = mean_clv - t_crit_clv * se_clv
        clv_upper = mean_clv + t_crit_clv * se_clv
        
        print(f"\n=== CLV Analysis (95% Confidence) ===")
        print(f"Sample Size (N): {n_clv}")
        print(f"Mean CLV: {mean_clv:.2%}")
        print(f"Standard Error: {se_clv:.2%}")
        print(f"95% CI Range: [{clv_lower:.2%}, {clv_upper:.2%}]")
        
        if clv_lower > 0:
            print("✅ Statistically Significant Edge (Lower bound > 0)")
        else:
            print("⚠️ Edge not statistically proven.")
    else:
        print("\nCould not calculate CLV stats (missing closing odds).")

if __name__ == "__main__":
    calculate_confidence_intervals()
