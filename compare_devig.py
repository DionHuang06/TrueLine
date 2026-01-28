"""
Compare de-vig methods: run calibration with multiplicative, power, shin.

Reports Brier (implied), log loss (implied), accuracy (implied) per method.
"""
from typing import Optional

from calibration_metrics import run

METHODS = ["multiplicative", "power", "shin"]


def main(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    use_starting_elo: bool = True,
) -> None:
    print("De-vig comparison (Brier implied, log loss implied, accuracy implied)")
    print("=" * 70)
    if from_date:
        print(f"From: {from_date}")
    if to_date:
        print(f"To:   {to_date}")
    print()

    rows = []
    for method in METHODS:
        res = run(
            from_date=from_date,
            to_date=to_date,
            use_starting_elo=use_starting_elo,
            devig_method=method,
        )
        if res.n_with_odds == 0:
            rows.append((method, float("nan"), float("nan"), float("nan")))
            continue
        rows.append((method, res.brier_implied, res.logloss_implied, res.accuracy_implied))

    print(f"{'Method':<16}  {'Brier (impl)':>12}  {'LogLoss (impl)':>14}  {'Accuracy':>10}")
    print("-" * 58)
    for method, brier, ll, acc in rows:
        b = f"{brier:.4f}" if brier == brier else "  -"
        l = f"{ll:.4f}" if ll == ll else "  -"
        a = f"{acc:.2%}" if acc == acc else "  -"
        print(f"{method:<16}  {b:>12}  {l:>14}  {a:>10}")

    valid = [(m, b) for m, b, _, _ in rows if b == b]
    if valid:
        best = min(valid, key=lambda x: x[1])
        print()
        print(f"Best Brier (implied) at devig = {best[0]}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Compare de-vig methods via calibration.")
    ap.add_argument("--from", dest="from_date", default=None, help="From date YYYY-MM-DD")
    ap.add_argument("--to", dest="to_date", default=None, help="To date YYYY-MM-DD")
    ap.add_argument("--no-starting-elo", action="store_true", help="Use 1500 for all teams")
    args = ap.parse_args()
    main(
        from_date=args.from_date,
        to_date=args.to_date,
        use_starting_elo=not args.no_starting_elo,
    )
