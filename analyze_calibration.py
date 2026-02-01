"""
Analyze Calibration of Closing Lines.
Metric: Brier Score and Expected Calibration Error (ECE).
"""
import sqlite3
import pandas as pd
import numpy as np

def analyze_calibration():
    conn = sqlite3.connect("nba_betting.db")
    
    # Get Games + Closing Odds
    # We average closing odds across books for a "Market Consensus"
    query = """
        SELECT g.id, 
               g.home_score, g.away_score,
               AVG(o.home_dec) as avg_home_close,
               AVG(o.away_dec) as avg_away_close
        FROM games g
        JOIN odds_snapshots o ON g.id = o.game_id
        WHERE g.status = 'FINAL' 
          AND o.book LIKE '%(Close)'
        GROUP BY g.id
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    if df.empty:
        print("No closing odds found for final games.")
        return

    # Calculate Outcome
    df['home_win'] = (df['home_score'] > df['away_score']).astype(int)
    
    # Calculate Implied Probabilities (Vig-Free)
    # Raw Implied
    df['impl_h'] = 1 / df['avg_home_close']
    df['impl_a'] = 1 / df['avg_away_close']
    
    # Normalize (Remove Vig)
    df['total_prob'] = df['impl_h'] + df['impl_a']
    df['prob_h_true'] = df['impl_h'] / df['total_prob']
    
    # 1. Brier Score
    # Mean Squared Error between Probability and Outcome
    brier_score = ((df['prob_h_true'] - df['home_win']) ** 2).mean()
    
    print(f"=== Closing Line Calibration Analysis ({len(df)} games) ===")
    print(f"Brier Score: {brier_score:.4f} (Lower is better. 0.25 is random guessing)")
    
    # 2. Calibration Bins (ECE)
    bins = np.linspace(0, 1, 11) # 10 bins
    df['bin'] = pd.cut(df['prob_h_true'], bins)
    
    grouped = df.groupby('bin', observed=False).agg(
        avg_prob=('prob_h_true', 'mean'),
        actual_rate=('home_win', 'mean'),
        count=('home_win', 'count')
    )
    
    # Filter empty bins
    grouped = grouped[grouped['count'] > 0]
    
    print("\nCalibration by Probability Bin:")
    print(grouped)
    
    # ECE Calculation
    # Weighted average of absolute difference |avg_prob - actual_rate|
    n_total = df.shape[0]
    ece = 0
    for _, row in grouped.iterrows():
        diff = abs(row['avg_prob'] - row['actual_rate'])
        weight = row['count'] / n_total
        ece += weight * diff
        
    print(f"\nExpected Calibration Error (ECE): {ece:.4f} ({ece:.1%})")
    
    if ece < 0.05:
        print("✅ Excellent Calibration (<5% error)")
    elif ece < 0.10:
        print("⚠️ Moderate Calibration (<10% error)")
    else:
        print("❌ Poor Calibration (>10% error)")

if __name__ == "__main__":
    analyze_calibration()
