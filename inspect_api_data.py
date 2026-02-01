"""Test to see what basketball data we're getting."""
from ingestion.isportsapi_client import iSportsAPIClient

client = iSportsAPIClient(use_recommended_delay=False)
data = client.fetch_historical_odds("2025-11-02")

if data and data.get('moneyLine'):
    print(f"Total moneyline entries: {len(data['moneyLine'])}")
    print("\nFirst 10 entries:")
    for i, entry in enumerate(data['moneyLine'][:10]):
        print(f"{i+1}. {entry}")
    
    # Check if there's any way to identify NBA games
    print(f"\nTotal unique match IDs: {len(set(e.split(',')[0] for e in data['moneyLine']))}")
