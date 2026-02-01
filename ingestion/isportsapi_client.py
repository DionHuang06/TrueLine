"""iSportsAPI client for fetching historical basketball odds."""
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
API_KEY = os.getenv("ISPORTSAPI_KEY", "")
BASE_URL = "http://api.isportsapi.com/sport/basketball/odds/history"
RATE_LIMIT_SECONDS = 60  # Minimum 60 seconds between calls
RECOMMENDED_DELAY = 900  # 15 minutes recommended


class iSportsAPIClient:
    """Client for iSportsAPI historical odds endpoint."""
    
    def __init__(self, api_key: str = API_KEY, use_recommended_delay: bool = True):
        self.api_key = api_key
        self.delay = RECOMMENDED_DELAY if use_recommended_delay else RATE_LIMIT_SECONDS
        self.last_call_time = 0
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def _wait_for_rate_limit(self):
        """Enforce rate limiting between API calls."""
        elapsed = time.time() - self.last_call_time
        if elapsed < self.delay:
            wait_time = self.delay - elapsed
            print(f"Rate limiting: waiting {wait_time:.1f} seconds...")
            time.sleep(wait_time)
        self.last_call_time = time.time()
    
    def fetch_historical_odds(self, date: str) -> Optional[Dict]:
        """
        Fetch historical odds for a specific date.
        
        Args:
            date: Date in YYYY-MM-DD format
            
        Returns:
            Dict with 'spread', 'moneyLine', and 'total' data, or None on error
        """
        self._wait_for_rate_limit()
        
        url = f"{BASE_URL}?api_key={self.api_key}&date={date}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') != 0:
                print(f"API error for {date}: {data.get('message', 'Unknown error')}")
                return None
            
            return data.get('data', {})
            
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {date}: {e}")
            return None
        except ValueError as e:
            print(f"JSON parsing failed for {date}: {e}")
            return None
    
    def parse_moneyline_odds(self, moneyline_data: List[str]) -> List[Dict]:
        """
        Parse moneyline odds from CSV-like format.
        
        Format: "matchId,companyId,initialHome,initialAway,lastHome,lastAway"
        
        Returns:
            List of dicts with parsed odds data
        """
        parsed = []
        
        for entry in moneyline_data:
            parts = entry.split(',')
            if len(parts) != 6:
                print(f"Invalid moneyline format: {entry}")
                continue
            
            try:
                parsed.append({
                    'match_id': parts[0],
                    'bookmaker_id': int(parts[1]),
                    'opening_home': float(parts[2]),
                    'opening_away': float(parts[3]),
                    'closing_home': float(parts[4]),
                    'closing_away': float(parts[5])
                })
            except (ValueError, IndexError) as e:
                print(f"Error parsing moneyline entry {entry}: {e}")
                continue
        
        return parsed
    
    def fetch_date_range(self, start_date: str, end_date: str) -> Dict[str, Dict]:
        """
        Fetch historical odds for a date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Dict mapping dates to their odds data
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        results = {}
        current = start
        
        total_days = (end - start).days + 1
        print(f"Fetching {total_days} days of data...")
        print(f"Estimated time: {(total_days * self.delay) / 3600:.1f} hours")
        
        day_count = 0
        while current <= end:
            day_count += 1
            date_str = current.strftime("%Y-%m-%d")
            print(f"\n[{day_count}/{total_days}] Fetching {date_str}...")
            
            data = self.fetch_historical_odds(date_str)
            if data:
                results[date_str] = data
                moneyline_count = len(data.get('moneyLine', []))
                print(f"  ✓ Retrieved {moneyline_count} moneyline entries")
            else:
                print(f"  ✗ No data for {date_str}")
            
            current += timedelta(days=1)
        
        return results


if __name__ == "__main__":
    # Test the client
    client = iSportsAPIClient(use_recommended_delay=False)
    
    # Test with a recent date
    test_date = "2026-01-25"
    print(f"Testing API with date: {test_date}")
    
    data = client.fetch_historical_odds(test_date)
    if data:
        print(f"\nSuccess! Retrieved data:")
        print(f"  Spread entries: {len(data.get('spread', []))}")
        print(f"  MoneyLine entries: {len(data.get('moneyLine', []))}")
        print(f"  Total entries: {len(data.get('total', []))}")
        
        # Parse and show sample moneyline
        if data.get('moneyLine'):
            parsed = client.parse_moneyline_odds(data['moneyLine'][:3])
            print(f"\nSample parsed moneyline odds:")
            for odds in parsed:
                print(f"  Match {odds['match_id']}: {odds}")
