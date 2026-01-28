"""
Calibration metrics: Brier score, log loss, and reliability table.

Processes FINAL games with odds in chronological order. For each game:
- Predict home win prob (Elo + rest); get implied prob (de-vigged odds).
- Update Elo with MOV after.
- Compute Brier, log loss over model probs vs actual home wins.
- Bin predictions for reliability (mean predicted vs actual win rate).
"""
from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config import DB_PATH, ELO_INITIAL
from database.db import get_connection
from modeling.elo import EloModel

try:
    from config import STARTING_ELO_2025_26
except ImportError:
    STARTING_ELO_2025_26 = {}

# Clip probs to avoid log(0)
_EPS = 1e-6

# Reliability bins: [0.5, 0.55), [0.55, 0.6), ..., [0.9, 0.95), [0.95, 1.0]
RELIABILITY_BINS = [
    (0.50, 0.55),
    (0.55, 0.60),
    (0.60, 0.65),
    (0.65, 0.70),
    (0.70, 0.75),
    (0.75, 0.80),
    (0.80, 0.85),
    (0.85, 0.90),
    (0.90, 0.95),
    (0.95, 1.00),
]


def _table_exists(cursor, name: str) -> bool:
    cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cursor.fetchone() is not None


def _get_odds_before_game(
    conn: sqlite3.Connection,
    game_id: int,
    game_time: str,
    use_odds_snapshots: bool,
    closing_only: bool = False,
    devig_method: str = "multiplicative",
) -> Optional[Tuple[float, float]]:
    """
    Get de-vigged (home_fair, away_fair) for game before game_time.
    closing_only: use only closing snapshots (odds table); no-op for odds_snapshots.
    Returns None if no odds.
    """
    cur = conn.cursor()
    game_time_norm = game_time.replace(" ", "T") if " " in game_time else game_time

    if use_odds_snapshots:
        cur.execute(
            """
            SELECT book, home_dec, away_dec, pulled_at
            FROM odds_snapshots
            WHERE game_id = ?
              AND ((book = 'CSV') OR (pulled_at < ?))
            ORDER BY pulled_at DESC
            """,
            (game_id, game_time_norm),
        )
    else:
        q = """
            SELECT book, snapshot_time, home_odds AS home_dec, away_odds AS away_dec
            FROM odds
            WHERE game_id = ? AND snapshot_time < ?
        """
        if closing_only:
            q += " AND snapshot_type = 'closing'"
        q += " ORDER BY snapshot_time DESC"
        cur.execute(q, (game_id, game_time_norm))

    rows = list(cur.fetchall())
    if not rows:
        return None

    seen_books = set()
    all_home: List[float] = []
    all_away: List[float] = []
    for r in rows:
        book = r["book"] if "book" in r.keys() else "unknown"
        if book in seen_books:
            continue
        seen_books.add(book)
        h = float(r["home_dec"])
        a = float(r["away_dec"])
        if h > 0 and a > 0:
            all_home.append(h)
            all_away.append(a)

    if not all_home:
        return None

    avg_h = sum(all_home) / len(all_home)
    avg_a = sum(all_away) / len(all_away)
    from devig import devig
    home_fair, away_fair = devig(avg_h, avg_a, devig_method)
    return (home_fair, away_fair)


@dataclass
class CalibrationResult:
    n_games: int
    n_with_odds: int
    brier_model: float
    brier_implied: float
    logloss_model: float
    logloss_implied: float
    accuracy_model: float
    accuracy_implied: float
    reliability: List[Tuple[str, float, float, int]] = field(default_factory=list)
    used_closing_odds: bool = False


def run(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    use_starting_elo: bool = True,
    home_advantage: Optional[float] = None,
    use_closing_odds: bool = False,
    devig_method: str = "multiplicative",
) -> CalibrationResult:
    """
    Run calibration over FINAL games (optionally in [from_date, to_date]).
    Uses STARTING_ELO_2025_26 if use_starting_elo else 1500 for all.
    home_advantage: override HCA (default from config).
    use_closing_odds: use only closing-line odds (odds table); ignored for odds_snapshots.
    devig_method: multiplicative | power | shin.
    """
    conn = get_connection()
    cur = conn.cursor()

    use_snapshots = _table_exists(cur, "odds_snapshots")
    use_odds = _table_exists(cur, "odds")
    if not use_snapshots and not use_odds:
        conn.close()
        raise RuntimeError("No odds_snapshots or odds table found.")

    kwargs = {}
    if home_advantage is not None:
        kwargs["home_advantage"] = home_advantage
    model = EloModel(**kwargs)
    cur.execute("SELECT id, name FROM teams")
    teams = {row["id"]: row["name"] for row in cur.fetchall()}
    ratings: Dict[int, float] = {}
    for tid, tname in teams.items():
        ratings[tid] = float(STARTING_ELO_2025_26.get(tname, ELO_INITIAL)) if use_starting_elo else float(ELO_INITIAL)
    model.ratings = ratings

    # FINAL games with scores, chronological
    query = """
        SELECT g.id, g.start_time, g.home_team_id, g.away_team_id,
               g.home_score, g.away_score
        FROM games g
        WHERE g.status = 'FINAL'
          AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL
    """
    params: List[object] = []
    if from_date:
        query += " AND date(g.start_time) >= date(?)"
        params.append(from_date)
    if to_date:
        query += " AND date(g.start_time) <= date(?)"
        params.append(to_date)
    query += " ORDER BY g.start_time ASC"
    cur.execute(query, tuple(params))
    games = list(cur.fetchall())
    n_games = len(games)
    max_game_time = max((g["start_time"] for g in games), default=None) if games else None

    # Collect (model_prob, implied_prob, outcome) for games with odds
    pairs: List[Tuple[float, float, int]] = []
    correct_model = 0
    correct_implied = 0

    for g in games:
        gid = g["id"]
        start = g["start_time"]
        hid = g["home_team_id"]
        aid = g["away_team_id"]
        hs = g["home_score"]
        aws = g["away_score"]
        home_won = 1 if hs > aws else 0

        closing_only = use_closing_odds and not use_snapshots
        odds_fair = _get_odds_before_game(
            conn, gid, start, use_odds_snapshots=use_snapshots,
            closing_only=closing_only, devig_method=devig_method,
        )
        home_rest = model._get_rest_days(hid, start)
        away_rest = model._get_rest_days(aid, start)
        home_elo = model.get_team_rating(hid)
        away_elo = model.get_team_rating(aid)
        home_prob, _ = model.predict_game(home_elo, away_elo, home_rest, away_rest)

        if odds_fair is not None:
            home_fair, _ = odds_fair
            pairs.append((home_prob, home_fair, home_won))
            if (home_prob > 0.5) == (home_won == 1):
                correct_model += 1
            if (home_fair > 0.5) == (home_won == 1):
                correct_implied += 1

        rw = model._get_recency_weight(start, max_game_time)
        new_h, new_a = model.update_ratings(
            home_elo, away_elo, bool(home_won),
            home_score=int(hs), away_score=int(aws),
            recency_weight=rw,
        )
        model.ratings[hid] = new_h
        model.ratings[aid] = new_a

    conn.close()

    n_with = len(pairs)
    if n_with == 0:
        return CalibrationResult(
            n_games=n_games,
            n_with_odds=0,
            brier_model=float("nan"),
            brier_implied=float("nan"),
            logloss_model=float("nan"),
            logloss_implied=float("nan"),
            accuracy_model=float("nan"),
            accuracy_implied=float("nan"),
            reliability=[],
            used_closing_odds=use_closing_odds,
        )

    brier_m = sum((p - y) ** 2 for p, i, y in pairs) / n_with
    brier_i = sum((i - y) ** 2 for p, i, y in pairs) / n_with
    logloss_m = -sum(
        y * math.log(max(_EPS, min(1 - _EPS, p))) + (1 - y) * math.log(max(_EPS, min(1 - _EPS, 1 - p)))
        for p, i, y in pairs
    ) / n_with
    logloss_i = -sum(
        y * math.log(max(_EPS, min(1 - _EPS, i))) + (1 - y) * math.log(max(_EPS, min(1 - _EPS, 1 - i)))
        for p, i, y in pairs
    ) / n_with

    # Reliability bins (model only)
    bin_data: List[Tuple[str, float, float, int]] = []
    for lo, hi in RELIABILITY_BINS:
        subset = [(p, y) for p, _, y in pairs if lo <= p < hi]
        if not subset:
            bin_data.append((f"[{lo:.2f}, {hi:.2f})", float("nan"), float("nan"), 0))
            continue
        avg_p = sum(x[0] for x in subset) / len(subset)
        actual = sum(x[1] for x in subset) / len(subset)
        bin_data.append((f"[{lo:.2f}, {hi:.2f})", avg_p, actual, len(subset)))

    return CalibrationResult(
        n_games=n_games,
        n_with_odds=n_with,
        brier_model=brier_m,
        brier_implied=brier_i,
        logloss_model=logloss_m,
        logloss_implied=logloss_i,
        accuracy_model=correct_model / n_with,
        accuracy_implied=correct_implied / n_with,
        reliability=bin_data,
        used_closing_odds=use_closing_odds,
    )


def print_and_save(result: CalibrationResult, out_path: Optional[Path] = None) -> None:
    """Print calibration metrics and optionally write to file."""
    lines: List[str] = []
    lines.append("=" * 60)
    lines.append("CALIBRATION METRICS")
    lines.append("=" * 60)
    lines.append(f"FINAL games total:     {result.n_games}")
    lines.append(f"Games with odds:      {result.n_with_odds}")
    lines.append("")
    if result.n_with_odds == 0:
        if result.n_games == 0:
            lines.append("No FINAL games in DB. Populate DB with games + odds and re-run.")
        else:
            lines.append("No games with odds; skipping metrics. Add odds_snapshots or odds.")
    else:
        impl_label = "Implied (closing)" if result.used_closing_odds else "Implied (market)"
        lines.append(f"Metric              Model    {impl_label}")
        lines.append("-" * 50)
        lines.append(f"Brier score          {result.brier_model:.4f}   {result.brier_implied:.4f}")
        lines.append(f"Log loss             {result.logloss_model:.4f}   {result.logloss_implied:.4f}")
        lines.append(f"Accuracy (pick)      {result.accuracy_model:.2%}   {result.accuracy_implied:.2%}")
        lines.append("")
        lines.append("Reliability (model): bin         avg_pred  actual   n")
        lines.append("-" * 50)
        for label, avg_p, actual, n in result.reliability:
            ap = f"{avg_p:.3f}" if not math.isnan(avg_p) else " -"
            ac = f"{actual:.3f}" if not math.isnan(actual) else " -"
            lines.append(f"  {label:12}     {ap:>6}   {ac:>6}   {n:4}")
    lines.append("=" * 60)

    text = "\n".join(lines)
    print(text)
    if out_path is not None:
        out_path.write_text(text, encoding="utf-8")
        print(f"Written to {out_path}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Compute calibration metrics (Brier, log loss, reliability).")
    parser.add_argument("--from", dest="from_date", default=None, help="From date YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", default=None, help="To date YYYY-MM-DD")
    parser.add_argument("--no-starting-elo", action="store_true", help="Use 1500 for all teams")
    parser.add_argument("--closing", action="store_true", help="Use closing-line odds only (odds table)")
    parser.add_argument("--hca", type=float, default=None, help="Override home-court advantage")
    parser.add_argument("--devig", choices=["multiplicative", "power", "shin"], default="multiplicative")
    parser.add_argument("--output", "-o", type=Path, default=None, help="Output file path")
    args = parser.parse_args()
    res = run(
        from_date=args.from_date,
        to_date=args.to_date,
        use_starting_elo=not args.no_starting_elo,
        home_advantage=args.hca,
        use_closing_odds=args.closing,
        devig_method=args.devig,
    )
    out = Path(args.output) if args.output else (Path(__file__).parent / "calibration_report.txt")
    print_and_save(res, out)
