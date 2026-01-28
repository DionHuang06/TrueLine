"""Setup new database schema and migrate/import data."""
from database.new_schema import reset_to_new_schema, get_connection
from config import DB_PATH, STARTING_ELO_2025_26, ELO_INITIAL
import sys


def setup_new_database(confirm=True):
    """Initialize new database schema."""
    print("=" * 80)
    print("SETTING UP NEW DATABASE SCHEMA")
    print("=" * 80)
    
    if confirm:
        try:
            response = input("\nThis will DELETE all existing data. Continue? (yes/no): ")
            if response.lower() != 'yes':
                print("Cancelled.")
                return
        except EOFError:
            # Non-interactive mode
            print("\nNon-interactive mode: Proceeding with reset...")
    
    print("\nResetting database to new schema...")
    reset_to_new_schema()
    
    # Initialize teams with starting Elo
    print("\nInitializing teams...")
    conn = get_connection()
    cursor = conn.cursor()
    
    # Add all NBA teams with starting Elo
    for team_name, starting_elo in STARTING_ELO_2025_26.items():
        cursor.execute("""
            INSERT OR IGNORE INTO teams (name, current_elo)
            VALUES (?, ?)
        """, (team_name, starting_elo))
    
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) as count FROM teams")
    team_count = cursor.fetchone()['count']
    print(f"  Initialized {team_count} teams")
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("NEW DATABASE SETUP COMPLETE")
    print("=" * 80)
    print("\nNext steps:")
    print("1. Import games: python main.py games pull --from 2025-10-01 --to 2026-06-30")
    print("2. Fetch odds: python fetch_historical_odds.py")
    print("3. View database info: python database_info.py")


if __name__ == "__main__":
    import sys
    # If --yes flag is provided, skip confirmation
    confirm = '--yes' not in sys.argv
    setup_new_database(confirm=confirm)
