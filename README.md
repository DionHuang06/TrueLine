# NBA Moneyline +EV Betting Engine

A paper trading system for finding positive expected value (+EV) bets on NBA moneyline markets using Elo ratings and odds from multiple sportsbooks.

## Features

- **Elo Rating Model**: Team ratings with home-court advantage adjustment
- **Multi-Book Odds**: Best odds selection across multiple sportsbooks
- **De-Vigging**: True probability estimation by removing bookmaker margin
- **Edge Detection**: Automated +EV opportunity identification (4%+ edge threshold)
- **Paper Trading**: Virtual bankroll with flat staking (0.5% per bet)
- **Backtesting**: Time-ordered simulation with no data leakage
- **Performance Tracking**: ROI, win rate, max drawdown metrics

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Get API Keys

Get free API keys from:
- [The Odds API](https://the-odds-api.com/) - for odds data
- [balldontlie](https://www.balldontlie.io/) - for NBA schedule and scores

Create a `.env` file:

```
ODDS_API_KEY=your_odds_api_key_here
BALLDONTLIE_API_KEY=your_balldontlie_key_here
```

### 3. Initialize Database

```bash
python main.py init
```

## CLI Commands

### Database

```bash
python main.py init          # Initialize database
python main.py reset         # Reset database (deletes all data)
python main.py status        # Show system status
```

### Data Ingestion

```bash
# Pull NBA games schedule
python main.py games pull --days 7
python main.py games pull --from 2024-01-01 --to 2024-06-30
python main.py games list --status FINAL --limit 50

# Pull odds snapshots (stores every pull, no overwrites)
python main.py odds pull

# Update game results
python main.py results pull --days 3
```

### Model

```bash
# Train Elo model on historical games
python main.py model train

# Show team rankings
python main.py model rankings --limit 30
```

### Edge Detection

```bash
# Find +EV edges for today
python main.py edges today

# Find edges and place paper bets
python main.py edges today --bet

# Scan specific date
python main.py edges scan --date 2024-12-25
```

### Paper Betting

```bash
# Show pending bets
python main.py bets pending

# Settle completed bets
python main.py bets settle

# View bet history
python main.py bets history --limit 50

# Performance statistics
python main.py bets stats
```

### Backtesting

```bash
# Run backtest
python main.py backtest --from 2024-01-01 --to 2024-06-30

# Export results to CSV
python main.py backtest --from 2024-01-01 --to 2024-06-30 --export --output results.csv
```

## How It Works

### 1. Data Flow

```
The Odds API ──────► odds_snapshots
balldontlie API ───► games + results
                          │
                          ▼
                    Elo Model Training
                          │
                          ▼
              Edge Detection (model vs market)
                          │
                          ▼
              Paper Betting (if edge >= 4%)
```

### 2. Elo Model

- Initial rating: 1500
- K-factor: 20
- Home-court advantage: +100 points (~3.5 point spread)
- Win probability: `1 / (1 + 10^((opponent - team) / 400))`

### 3. De-Vigging

Uses multiplicative method to remove vig:
```
true_prob = implied_prob / sum(all_implied_probs)
```

### 4. Edge Calculation

```
edge = model_prob - de_vigged_market_prob
EV = (model_prob × (odds - 1)) - (1 - model_prob)
```

Bet placed when:
- Edge >= 4%
- EV > 0

### 5. Staking

Flat betting: 0.5% of bankroll per bet (capped at 1%)

## Database Schema

| Table | Description |
|-------|-------------|
| `teams` | Team info and current Elo |
| `games` | Game schedule and results |
| `odds_snapshots` | Historical odds (never overwritten) |
| `elo_history` | Elo rating changes per game |
| `predictions` | Model predictions |
| `edges` | Detected betting edges |
| `paper_bets` | Paper trading records |
| `bankroll_history` | Bankroll changes over time |

## Backtesting Methodology

- **Strict time-ordering**: Games processed chronologically
- **No lookahead bias**: Only uses odds available before game start
- **Real-time Elo updates**: Ratings update as games complete
- **Drawdown tracking**: Maximum peak-to-trough decline

## Metrics Tracked

- Total bets
- Win/Loss count and rate
- Total P&L
- ROI (Return on Investment)
- Average edge
- Maximum drawdown ($ and %)

## Limitations

- NBA moneyline only (no spreads, totals, or props)
- Paper trading only (no real bet placement)
- Free API tier limits
- No player-level features

## API Rate Limits

The Odds API free tier: 500 requests/month

Each `odds pull` uses 1 request.

## License

MIT

