"""
HCA tuning: grid search over home-court advantage values.

Runs calibration for each HCA in [60, 80, 100, 120], reports Brier, log loss,
accuracy. Use --from/--to to restrict date range.
"""
from typing import Optional

from calibration_metrics import run

HCA_GRID = [60, 80, 100, 120]


def main(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    use_starting_elo: bool = True,
) -> None:
    print("HCA tuning (Brier, log loss, accuracy)")
    print("=" * 60)
    if from_date:
        print(f"From: {from_date}")
    if to_date:
        print(f"To:   {to_date}")
    print()

    rows = []
    for hca in HCA_GRID:
        res = run(
            from_date=from_date,
            to_date=to_date,
            use_starting_elo=use_starting_elo,
            home_advantage=float(hca),
        )
        if res.n_with_odds == 0:
            rows.append((hca, float("nan"), float("nan"), float("nan")))
            continue
        rows.append((hca, res.brier_model, res.logloss_model, res.accuracy_model))

    print(f"{'HCA':>6}  {'Brier':>8}  {'LogLoss':>8}  {'Accuracy':>8}")
    print("-" * 36)
    for hca, brier, ll, acc in rows:
        b = f"{brier:.4f}" if brier == brier else "  -"
        l = f"{ll:.4f}" if ll == ll else "  -"
        a = f"{acc:.2%}" if acc == acc else "  -"
        print(f"{hca:>6}  {b:>8}  {l:>8}  {a:>8}")

    valid = [(hca, b) for hca, b, _, _ in rows if b == b]
    if valid:
        best = min(valid, key=lambda x: x[1])
        print()
        print(f"Best Brier at HCA = {best[0]}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Tune home-court advantage via calibration.")
    ap.add_argument("--from", dest="from_date", default=None, help="From date YYYY-MM-DD")
    ap.add_argument("--to", dest="to_date", default=None, help="To date YYYY-MM-DD")
    ap.add_argument("--no-starting-elo", action="store_true", help="Use 1500 for all teams")
    args = ap.parse_args()
    main(
        from_date=args.from_date,
        to_date=args.to_date,
        use_starting_elo=not args.no_starting_elo,
    )
