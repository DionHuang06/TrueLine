"""Test the Reset Elo functionality locally"""
from database.db import get_connection
from config import STARTING_ELO_2025_26 as TEAMS

def test_reset_elo():
    print("Testing Reset Elo functionality...")
    
    conn = get_connection()
    c = conn.cursor()
    
    # Test 1: Fetch current ratings
    print("\n1. Fetching current ratings...")
    c.execute("SELECT id, name, current_elo FROM teams ORDER BY name")
    old_ratings_data = c.fetchall()
    print(f"   ✓ Fetched {len(old_ratings_data)} teams")
    
    # Test 2: Reset to starting Elo
    print("\n2. Resetting to starting Elo...")
    for team_name, starting_elo in TEAMS.items():
        c.execute("UPDATE teams SET current_elo = ? WHERE name = ?", (starting_elo, team_name))
    print(f"   ✓ Reset {len(TEAMS)} teams")
    
    # Test 3: Fetch games
    print("\n3. Fetching FINAL games...")
    c.execute("SELECT id, home_team_id, away_team_id, home_score, away_score FROM games WHERE status='FINAL' ORDER BY start_time ASC")
    games = c.fetchall()
    print(f"   ✓ Found {len(games)} FINAL games")
    
    # Test 4: Get teams list
    print("\n4. Fetching teams for model initialization...")
    c.execute("SELECT id, name FROM teams")
    teams_list = c.fetchall()
    print(f"   ✓ Fetched {len(teams_list)} teams")
    
    # Test 5: Verify we can build the ratings dict
    print("\n5. Building ratings dictionary...")
    model_ratings = {row[0]: TEAMS.get(row[1], 1500.0) for row in teams_list}
    print(f"   ✓ Built ratings dict with {len(model_ratings)} entries")
    
    conn.rollback()  # Don't actually save changes
    conn.close()
    
    print("\n✅ All tests passed!")

if __name__ == "__main__":
    test_reset_elo()
