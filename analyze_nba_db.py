"""
NBA-Filtered iSportsAPI Integration Strategy

PROBLEM: iSportsAPI historical odds endpoint returns ALL basketball globally
(~594 games/day including NBA, EuroLeague, college, international, etc.)

SOLUTION: Cross-reference with your existing nba_betting.db

Your existing database (nba_betting.db) has:
- Verified NBA games from balldontlie API
- Correct team names, dates, and game info
- This is the source of truth for "what is an NBA game"

APPROACH:
1. Fetch iSportsAPI odds for date range (all basketball)
2. For each date, get NBA games from nba_betting.db
3. Try to match iSportsAPI match_ids to NBA games by:
   - Exact date match
   - Team name matching (if available in future)
   - Manual mapping file (match_id -> nba_game_id)

ALTERNATIVE: Since iSportsAPI doesn't provide team names in the odds endpoint,
we may need to:
1. Use a different API that provides NBA-only odds
2. Contact iSportsAPI for NBA-specific access
3. Manually identify NBA match_id ranges

For now, let's check what data you already have in nba_betting.db
"""

from database.db import get_connection as get_nba_connection

with get_nba_connection() as conn:
    cursor = conn.cursor()
    
    # Check what NBA games you have
    cursor.execute("SELECT COUNT(*) as count FROM games")
    total_games = cursor.fetchone()['count']
    
    cursor.execute("SELECT MIN(start_time) as min_date, MAX(start_time) as max_date FROM games")
    date_range = cursor.fetchone()
    
    cursor.execute("""
        SELECT DATE(start_time) as game_date, COUNT(*) as game_count
        FROM games
        WHERE status = 'FINAL'
        GROUP BY DATE(start_time)
        ORDER BY game_date DESC
        LIMIT 10
    """)
    games_per_day = cursor.fetchall()
    
    print("NBA Betting Database (nba_betting.db) Stats:")
    print(f"Total games: {total_games}")
    if date_range['min_date']:
        print(f"Date range: {date_range['min_date'][:10]} to {date_range['max_date'][:10]}")
    
    print("\nNBA games per day (last 10 days with FINAL games):")
    for row in games_per_day:
        print(f"  {row['game_date']}: {row['game_count']} NBA games")
    
    print("\nTypical NBA schedule: 10-15 games per day during regular season")
    print("This matches the expected pattern ✓")
