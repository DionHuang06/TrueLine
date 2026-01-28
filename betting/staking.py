"""Staking strategy for bet sizing."""
from config import FLAT_STAKE_PCT, MAX_STAKE_PCT, INITIAL_BANKROLL
from database.db import get_connection


def get_current_bankroll() -> float:
    """Get the current paper bankroll balance."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT balance FROM bankroll_history ORDER BY id DESC LIMIT 1"
    )
    row = cursor.fetchone()
    conn.close()
    return row["balance"] if row else INITIAL_BANKROLL


def calculate_stake(bankroll: float = None) -> float:
    """
    Calculate stake using flat staking strategy.
    
    Uses 0.5% of bankroll per bet, capped at 1%.
    
    Args:
        bankroll: Current bankroll. If None, fetches from database.
        
    Returns: Stake amount
    """
    if bankroll is None:
        bankroll = get_current_bankroll()
    
    stake = bankroll * FLAT_STAKE_PCT
    max_stake = bankroll * MAX_STAKE_PCT
    
    return min(stake, max_stake)


def initialize_bankroll(initial_balance: float = INITIAL_BANKROLL):
    """Initialize bankroll with starting balance."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Check if bankroll exists
    cursor.execute("SELECT 1 FROM bankroll_history LIMIT 1")
    if cursor.fetchone():
        print("Bankroll already initialized.")
        conn.close()
        return
    
    cursor.execute("""
        INSERT INTO bankroll_history (balance, change, reason, recorded_at)
        VALUES (?, ?, 'Initial bankroll', datetime('now'))
    """, (initial_balance, initial_balance))
    
    conn.commit()
    conn.close()
    print(f"Initialized bankroll with ${initial_balance:,.2f}")

