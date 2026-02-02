from database.db import get_connection

conn = get_connection()
c = conn.cursor()

# Count total bets
c.execute('SELECT COUNT(*) FROM paper_bets')
total = c.fetchone()[0]
print(f"Total bets in cloud DB: {total}")

# Show last 5 bets
c.execute('SELECT id, placed_at, side, odds, stake FROM paper_bets ORDER BY id DESC LIMIT 5')
print("\nLast 5 bets:")
for row in c.fetchall():
    print(f"  ID: {row[0]}, Placed: {row[1]}, Side: {row[2]}, Odds: {row[3]}, Stake: {row[4]}")

conn.close()
