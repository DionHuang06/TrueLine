"""Test iSportsAPI client connection."""
from ingestion.isportsapi_client import iSportsAPIClient

# Create client with fast mode for testing
client = iSportsAPIClient(use_recommended_delay=False)

# Test with a recent date
test_date = "2026-01-25"
print(f"Testing API with date: {test_date}")

data = client.fetch_historical_odds(test_date)

if data:
    print(f"\n✓ Success! Retrieved data:")
    print(f"  Spread entries: {len(data.get('spread', []))}")
    print(f"  MoneyLine entries: {len(data.get('moneyLine', []))}")
    print(f"  Total entries: {len(data.get('total', []))}")
    
    # Parse and show sample moneyline
    if data.get('moneyLine'):
        parsed = client.parse_moneyline_odds(data['moneyLine'][:3])
        print(f"\nSample parsed moneyline odds:")
        for odds in parsed:
            print(f"  Match {odds['match_id']}: Opening {odds['opening_home']}/{odds['opening_away']}, Closing {odds['closing_home']}/{odds['closing_away']}")
else:
    print("\n✗ Failed to fetch data")
