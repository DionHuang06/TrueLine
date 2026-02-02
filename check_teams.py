from database.db import get_connection

conn = get_connection()
c = conn.cursor()

# Check if these teams have played games
teams = ['Dallas Mavericks', 'Charlotte Hornets', 'Golden State Warriors', 'Atlanta Hawks']

for team in teams:
    c.execute("""
        SELECT COUNT(*) 
        FROM games 
        WHERE status='FINAL' 
        AND (
            home_team_id = (SELECT id FROM teams WHERE name = ?) 
            OR away_team_id = (SELECT id FROM teams WHERE name = ?)
        )
    """, (team, team))
    count = c.fetchone()[0]
    print(f"{team}: {count} FINAL games")

# Also check their current Elo
print("\nCurrent Elo ratings:")
for team in teams:
    c.execute("SELECT current_elo FROM teams WHERE name = ?", (team,))
    elo = c.fetchone()[0]
    print(f"{team}: {elo:.1f}")

conn.close()
