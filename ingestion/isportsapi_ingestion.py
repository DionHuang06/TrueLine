"""Data ingestion for iSportsAPI historical odds."""
from datetime import datetime
from typing import Dict, List
from database.isportsapi_schema import get_connection
from ingestion.isportsapi_client import iSportsAPIClient


class iSportsAPIIngester:
    """Ingest historical odds data from iSportsAPI into database."""
    
    def __init__(self, use_recommended_delay: bool = True):
        self.client = iSportsAPIClient(use_recommended_delay=use_recommended_delay)
    
    def store_moneyline_odds(self, date: str, odds_data: List[Dict]) -> int:
        """
        Store moneyline odds in database.
        
        Args:
            date: Game date in YYYY-MM-DD format
            odds_data: List of parsed moneyline odds
            
        Returns:
            Number of records inserted/updated
        """
        if not odds_data:
            return 0
        
        with get_connection() as conn:
            cursor = conn.cursor()
            count = 0
            
            for odds in odds_data:
                # Ensure game exists (we'll update with team names later if needed)
                cursor.execute("""
                    INSERT OR IGNORE INTO games (match_id, game_date)
                    VALUES (?, ?)
                """, (odds['match_id'], date))
                
                # Insert or update moneyline odds
                cursor.execute("""
                    INSERT INTO moneyline_odds 
                    (match_id, bookmaker_id, opening_home, opening_away, 
                     closing_home, closing_away)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(match_id, bookmaker_id) DO UPDATE SET
                        opening_home = excluded.opening_home,
                        opening_away = excluded.opening_away,
                        closing_home = excluded.closing_home,
                        closing_away = excluded.closing_away
                """, (
                    odds['match_id'],
                    odds['bookmaker_id'],
                    odds['opening_home'],
                    odds['opening_away'],
                    odds['closing_home'],
                    odds['closing_away']
                ))
                count += 1
            
            conn.commit()
            return count
    
    def ingest_date(self, date: str) -> Dict:
        """
        Ingest all odds data for a specific date.
        
        Args:
            date: Date in YYYY-MM-DD format
            
        Returns:
            Dict with ingestion statistics
        """
        print(f"\nIngesting data for {date}...")
        
        data = self.client.fetch_historical_odds(date)
        if not data:
            return {'date': date, 'success': False, 'moneyline_count': 0}
        
        # Parse and store moneyline odds
        moneyline_raw = data.get('moneyLine', [])
        moneyline_parsed = self.client.parse_moneyline_odds(moneyline_raw)
        moneyline_count = self.store_moneyline_odds(date, moneyline_parsed)
        
        print(f"  ✓ Stored {moneyline_count} moneyline odds entries")
        
        return {
            'date': date,
            'success': True,
            'moneyline_count': moneyline_count
        }
    
    def ingest_date_range(self, start_date: str, end_date: str) -> Dict:
        """
        Ingest odds data for a date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Dict with overall statistics
        """
        print(f"\n{'='*60}")
        print(f"iSportsAPI Data Ingestion")
        print(f"Date Range: {start_date} to {end_date}")
        print(f"{'='*60}")
        
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        total_days = (end - start).days + 1
        
        # Estimate time
        delay = self.client.delay
        estimated_hours = (total_days * delay) / 3600
        print(f"\nEstimated time: {estimated_hours:.1f} hours")
        print(f"Rate limit: {delay} seconds between calls")
        
        # Fetch all data
        all_data = self.client.fetch_date_range(start_date, end_date)
        
        # Process and store
        total_moneyline = 0
        successful_dates = 0
        
        for date_str, data in all_data.items():
            moneyline_raw = data.get('moneyLine', [])
            moneyline_parsed = self.client.parse_moneyline_odds(moneyline_raw)
            count = self.store_moneyline_odds(date_str, moneyline_parsed)
            total_moneyline += count
            successful_dates += 1
        
        print(f"\n{'='*60}")
        print(f"Ingestion Complete")
        print(f"{'='*60}")
        print(f"Dates processed: {successful_dates}/{total_days}")
        print(f"Total moneyline odds: {total_moneyline}")
        print(f"{'='*60}\n")
        
        return {
            'start_date': start_date,
            'end_date': end_date,
            'total_days': total_days,
            'successful_dates': successful_dates,
            'total_moneyline': total_moneyline
        }
    
    def get_database_stats(self) -> Dict:
        """Get statistics about stored data."""
        with get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) as count FROM games")
            games_count = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM moneyline_odds")
            odds_count = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(DISTINCT bookmaker_id) as count FROM moneyline_odds")
            bookmakers_count = cursor.fetchone()['count']
            
            cursor.execute("SELECT MIN(game_date) as min_date, MAX(game_date) as max_date FROM games")
            date_range = cursor.fetchone()
            
            return {
                'games': games_count,
                'moneyline_odds': odds_count,
                'bookmakers': bookmakers_count,
                'date_range': (date_range['min_date'], date_range['max_date'])
            }


if __name__ == "__main__":
    # Test ingestion with a single date
    from database.isportsapi_schema import init_db
    
    init_db()
    ingester = iSportsAPIIngester(use_recommended_delay=False)
    
    # Test with recent date
    result = ingester.ingest_date("2026-01-25")
    print(f"\nTest ingestion result: {result}")
    
    # Show stats
    stats = ingester.get_database_stats()
    print(f"\nDatabase stats: {stats}")
