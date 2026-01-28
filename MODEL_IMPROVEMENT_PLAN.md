# Model Improvement Plan

Prioritized suggestions to improve your NBA Elo + edge-detection betting model. Based on your current codebase: Elo (margin, HCA +100, rest days), edge detector (de-vig, confidence adjustment), and backtest pipeline.

---

## 1. Quick wins (low effort, high impact)

**Done:** Simulator fixes (MOV, rest days, one-bet-per-game) in `backtest/simulator.py`.  
**Done:** Calibration script `calibration_metrics.py` (Brier, log loss, reliability table). Run: `python calibration_metrics.py` or `python -m cli calibration`. Uses `odds_snapshots` or `odds`; supports `--from` / `--to` / `--no-starting-elo` / `-o`.

### 1.1 Fix backtest simulator inconsistencies

The `backtest/simulator.py` path diverges from your real stack in several ways:

| Issue | Current behavior | Fix |
|-------|------------------|-----|
| **No margin in Elo updates** | `_update_elos` calls `update_ratings(..., home_won)` only; no scores | Pass `home_score`/`away_score` from the game row into `update_ratings` |
| **No rest days in predictions** | `predict_game(home_elo, away_elo)` — rest not passed | Compute rest days per team (reuse `_get_rest_days` or equivalent), pass into `predict_game` |
| **Elo bootstrap** | All teams start at 1500 in backtest | Option A: Load `current_elo` from DB at backtest start (warm Elo). Option B: Use `STARTING_ELO_2025_26` and train only on games before `from_date` |

These changes make backtest Elo evolution match production (margin + rest) and avoid understating model accuracy.

### 1.2 At most one bet per game

Right now the simulator can place **both** a home and an away bet on the same game if both clear `MIN_EDGE`. You’re effectively betting both sides. Typical design: **at most one bet per game** (e.g. only the larger edge, or first edge found). Enforce that so backtest ROI isn’t inflated by “betting both sides.”

### 1.3 Add calibration metrics

You don’t yet measure whether model probabilities are well calibrated (e.g. when you say 70%, do you win ~70%?).

- **Brier score**: `(p - outcome)^2` per prediction; average over games. Lower is better.
- **Log loss**: `-[y*log(p) + (1-y)*log(1-p)]`. Lower is better.
- **Reliability diagram**: Bin predictions (e.g. 50–55%, 55–60%, …), plot mean predicted prob vs actual win rate. Ideally points lie on the diagonal.

Implement a small script that, over all games with odds + result:

1. Gets model prob (and optional implied prob).
2. Computes Brier, log loss.
3. Optionally outputs a simple reliability table (bin → avg predicted, actual win rate, count).

Run it on historical data to see if you’re overconfident (e.g. favorites) or underconfident (underdogs), then tune.

---

## 2. Medium effort

### 2.1 Actually use recency weighting

**Done.** Time-based recency: games within `RECENCY_LAST_DAYS` (default 14) of the latest game get `RECENCY_WEIGHT` (default 1.5). Config: `RECENCY_LAST_DAYS`, `RECENCY_WEIGHT`. Used in `train`, calibration, backtest.

### 2.2 Benchmark vs closing line

You have (or can store) **closing line** odds. Compare:

- **Model vs closing**: same calibration/Brier setup, but use closing implied probs instead of “odds before game.”
- **CLV**: When you bet, did you get better than closing? Track “odds you got” vs “closing” per bet.

That tells you whether your edge is vs open, vs close, or neither—and whether CLV is positive.

### 2.3 Tune home-court advantage

**Done.** `tune_hca.py` (or `python -m cli tune-hca`): grid over 60, 80, 100, 120; runs calibration per HCA, reports Brier / log loss / accuracy. Use `--from` / `--to` to restrict dates.

### 2.4 De‑vig method

**Done.** `devig.py`: `multiplicative`, `power` (z=2), `shin`. Calibration accepts `--devig multiplicative|power|shin`. `compare_devig.py` (or `python -m cli compare-devig`) runs calibration for each method and reports Brier (implied) / log loss / accuracy.

---

## 3. Larger projects

### 3.1 Injury / availability

Biggest gap vs books: no injury or roster info. Even a simple **binary** “star out” (e.g. top 1–2 players per team) can help. Options:

- Manually tag a few key games.
- Use a free source (e.g. ESPN, NBA.com) and scrape “out” status; apply a fixed Elo deduction when a star is out.

Start small (one team, one star) and measure impact on calibration and edge.

### 3.2 Simple ensemble or regression layer

Keep Elo as the main strength model, but add a **second layer**:

- **Inputs**: Elo-based win prob, rest days, maybe H2H or “last 5” win% (derived from your existing data).
- **Output**: Adjusted win probability.

Use a small logistic regression or tiny neural net, trained on historical outcomes. Regularize heavily. This can improve calibration without replacing Elo.

### 3.3 Track and avoid weak markets

If you have book-level odds, track performance by **book** and by **game** (e.g. slate size, rest differential). If certain books or situations are consistently bad, reduce stake or skip them.

---

## 4. Implementation order

1. **Simulator fixes** (MOV, rest, one-bet-per-game, Elo bootstrap) so backtest matches production and is interpretable.
2. **Calibration script** (Brier, log loss, optional reliability) to establish a baseline.
3. **Recency weighting** and **HCA tune**; re-run calibration and backtest.
4. **Closing-line benchmark** and CLV.
5. **De‑vig experiment** and **injury/availability** when you’re ready for more complexity.

---

## 5. Config / code touchpoints

- **Elo**: `modeling/elo.py` — `update_ratings`, `predict_game`, `_get_rest_days`, `_get_recency_weight`.
- **Edge detection**: `edge/detector.py` — `_devig_odds`, `_adjust_confidence`, `find_edges`.
- **Backtest**: `backtest/simulator.py` — `_update_elos`, `run` (odds, rest, one-bet-per-game), Elo init; `backtest_100_recent.py` uses `EdgeDetector` + DB Elo.
- **Config**: `config.py` — `ELO_HOME_ADVANTAGE`, `ELO_K_FACTOR`, `MIN_EDGE`, `ELO_USE_*` flags.

---

---

## 6. Backtest CLI caveat

The `cli backtest` command runs `init_db()` before the backtest. `init_db` calls `init_new_db`, which **drops** existing tables and recreates the new schema. If you use a populated DB (e.g. `nba_betting copy.db`), that will **wipe** your data. Use a backup or a dedicated backtest DB, or change the CLI to skip `init_db` when running backtests.

---

*Generated from codebase review. Re-run calibration and backtests after each change to measure impact.*
