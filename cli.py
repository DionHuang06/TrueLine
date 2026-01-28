"""CLI for NBA betting engine."""
import click
from datetime import datetime, timedelta
from tabulate import tabulate

from database.db import init_db, reset_db
from ingestion.odds import OddsIngester
from ingestion.games import GamesIngester
from ingestion.results import ResultsIngester
from modeling.elo import EloModel
from edge.detector import EdgeDetector
from betting.paper import PaperTrader
from backtest.simulator import Backtester


@click.group()
def cli():
    """NBA Moneyline +EV Betting Engine"""
    pass


@cli.command('init')
def init_command():
    """Initialize the database."""
    init_db()
    click.echo("Database initialized.")


@cli.command('reset')
@click.confirmation_option(prompt='Are you sure you want to reset the database?')
def reset_command():
    """Reset the database (deletes all data)."""
    reset_db()
    click.echo("Database reset complete.")


@cli.group('odds')
def odds_group():
    """Odds management commands."""
    pass


@odds_group.command('pull')
def odds_pull():
    """Pull current odds from The Odds API."""
    init_db()
    ingester = OddsIngester()
    count = ingester.ingest_odds()
    click.echo(f"Pulled {count} odds snapshots.")


@cli.group('games')
def games_group():
    """Games management commands."""
    pass


@games_group.command('pull')
@click.option('--days', default=7, help='Number of days ahead to pull')
@click.option('--from', 'from_date', default=None, help='Start date (YYYY-MM-DD)')
@click.option('--to', 'to_date', default=None, help='End date (YYYY-MM-DD)')
def games_pull(days, from_date, to_date):
    """Pull NBA games schedule."""
    init_db()
    ingester = GamesIngester()
    
    if from_date and to_date:
        count = ingester.ingest_games(from_date, to_date)
    else:
        count = ingester.pull_upcoming(days)
    
    click.echo(f"Pulled {count} games.")


@games_group.command('list')
@click.option('--status', default=None, help='Filter by status (SCHEDULED, FINAL)')
@click.option('--limit', default=20, help='Number of games to show')
def games_list(status, limit):
    """List games in the database."""
    from database.db import get_connection
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT g.id, g.start_time, g.status,
               ht.name as home_team, at.name as away_team,
               g.home_score, g.away_score
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.id
        JOIN teams at ON g.away_team_id = at.id
    """
    
    if status:
        query += f" WHERE g.status = '{status}'"
    
    query += " ORDER BY g.start_time DESC LIMIT ?"
    
    cursor.execute(query, (limit,))
    games = cursor.fetchall()
    conn.close()
    
    if not games:
        click.echo("No games found.")
        return
    
    table = []
    for g in games:
        score = ""
        if g['home_score'] is not None:
            score = f"{g['home_score']}-{g['away_score']}"
        table.append([
            g['id'],
            g['start_time'][:16],
            g['away_team'],
            '@',
            g['home_team'],
            score,
            g['status']
        ])
    
    click.echo(tabulate(table, headers=['ID', 'Time', 'Away', '', 'Home', 'Score', 'Status']))


@cli.group('results')
def results_group():
    """Results management commands."""
    pass


@results_group.command('pull')
@click.option('--days', default=3, help='Days back to check for results')
def results_pull(days):
    """Pull and update game results."""
    init_db()
    ingester = ResultsIngester()
    count = ingester.update_results(days)
    click.echo(f"Updated {count} game results.")


@cli.group('model')
def model_group():
    """Model management commands."""
    pass


@model_group.command('train')
@click.option('--from', 'from_date', default=None, help='Start date for training')
def model_train(from_date):
    """Train Elo model on historical games."""
    init_db()
    model = EloModel()
    stats = model.train(from_date)
    
    click.echo(f"\nTraining complete:")
    click.echo(f"  Games processed: {stats['games_processed']}")
    click.echo(f"  Correct predictions: {stats['correct_predictions']}")
    click.echo(f"  Accuracy: {stats['accuracy']:.2%}")


@model_group.command('rankings')
@click.option('--limit', default=30, help='Number of teams to show')
def model_rankings(limit):
    """Show current team Elo rankings."""
    model = EloModel()
    model.load_ratings()
    rankings = model.get_rankings()[:limit]
    
    if not rankings:
        click.echo("No rankings available. Run 'model train' first.")
        return
    
    table = []
    for i, team in enumerate(rankings, 1):
        table.append([i, team['name'], f"{team['current_elo']:.0f}"])
    
    click.echo(tabulate(table, headers=['Rank', 'Team', 'Elo']))


@cli.group('edges')
def edges_group():
    """Edge detection commands."""
    pass


@edges_group.command('today')
@click.option('--bet', is_flag=True, help='Place paper bets on found edges')
def edges_today(bet):
    """Find +EV edges for today's games."""
    init_db()
    
    detector = EdgeDetector()
    edges = detector.find_today_edges()
    
    if not edges:
        click.echo("No edges found for today's games.")
        return
    
    table = []
    for e in edges:
        table.append([
            e['team'],
            'vs',
            e['opponent'],
            e['start_time'][:16],
            e['best_book'],
            f"{e['best_odds']:.3f}",
            f"{e['model_prob']:.1%}",
            f"{e['implied_prob']:.1%}",
            f"{e['edge']:.2%}",
            f"{e['ev']:.3f}"
        ])
    
    click.echo("\n+EV EDGES FOR TODAY")
    click.echo("=" * 100)
    click.echo(tabulate(table, headers=[
        'Team', '', 'Opponent', 'Time', 'Book', 
        'Odds', 'Model', 'Market', 'Edge', 'EV'
    ]))
    
    if bet:
        click.echo("\nPlacing paper bets...")
        trader = PaperTrader()
        for edge in edges:
            trader.place_bet(edge)


@edges_group.command('scan')
@click.option('--date', default=None, help='Date to scan (YYYY-MM-DD)')
def edges_scan(date):
    """Scan for edges on a specific date."""
    from database.db import get_connection
    
    init_db()
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id FROM games 
        WHERE date(start_time) = date(?)
        ORDER BY start_time
    """, (target_date,))
    
    games = cursor.fetchall()
    conn.close()
    
    if not games:
        click.echo(f"No games found for {target_date}")
        return
    
    detector = EdgeDetector()
    all_edges = []
    
    for game in games:
        edges = detector.find_edges(game['id'])
        all_edges.extend(edges)
    
    if not all_edges:
        click.echo(f"No edges found for {target_date}")
        return
    
    click.echo(f"\nFound {len(all_edges)} edge(s) for {target_date}")
    for e in all_edges:
        click.echo(f"  {e['team']} @ {e['best_odds']:.3f} (Edge: {e['edge']:.2%}, EV: {e['ev']:.3f})")


@cli.command('backtest')
@click.option('--from', 'from_date', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--to', 'to_date', required=True, help='End date (YYYY-MM-DD)')
@click.option('--export', is_flag=True, help='Export results to CSV')
@click.option('--output', default='backtest_results.csv', help='Output CSV filename')
@click.option('--no-init', is_flag=True, help='Skip init_db (use existing DB, e.g. nba_betting copy.db)')
def backtest_command(from_date, to_date, export, output, no_init):
    """Run backtest simulation."""
    if not no_init:
        init_db()
    
    click.echo(f"Running backtest from {from_date} to {to_date}...")
    
    backtester = Backtester()
    result = backtester.run(from_date, to_date)
    
    backtester.print_results(result)
    
    if export:
        backtester.export_to_csv(result, output)


@cli.command('calibration')
@click.option('--from', 'from_date', default=None, help='From date (YYYY-MM-DD)')
@click.option('--to', 'to_date', default=None, help='To date (YYYY-MM-DD)')
@click.option('--no-starting-elo', is_flag=True, help='Use 1500 for all teams')
@click.option('--closing', is_flag=True, help='Use closing-line odds only (odds table)')
@click.option('--hca', type=float, default=None, help='Override home-court advantage')
@click.option('--devig', type=click.Choice(['multiplicative', 'power', 'shin']), default='multiplicative')
@click.option('--output', '-o', default='calibration_report.txt', help='Output file')
def calibration_command(from_date, to_date, no_starting_elo, closing, hca, devig, output):
    """Compute calibration metrics (Brier, log loss, reliability). Does not init_db."""
    from pathlib import Path
    from calibration_metrics import run, print_and_save
    result = run(
        from_date=from_date,
        to_date=to_date,
        use_starting_elo=not no_starting_elo,
        home_advantage=hca,
        use_closing_odds=closing,
        devig_method=devig,
    )
    print_and_save(result, Path(output))


@cli.command('tune-hca')
@click.option('--from', 'from_date', default=None, help='From date (YYYY-MM-DD)')
@click.option('--to', 'to_date', default=None, help='To date (YYYY-MM-DD)')
@click.option('--no-starting-elo', is_flag=True, help='Use 1500 for all teams')
def tune_hca_command(from_date, to_date, no_starting_elo):
    """Grid-search HCA (60, 80, 100, 120) via calibration. Does not init_db."""
    from tune_hca import main as tune_main
    tune_main(
        from_date=from_date,
        to_date=to_date,
        use_starting_elo=not no_starting_elo,
    )


@cli.command('compare-devig')
@click.option('--from', 'from_date', default=None, help='From date (YYYY-MM-DD)')
@click.option('--to', 'to_date', default=None, help='To date (YYYY-MM-DD)')
@click.option('--no-starting-elo', is_flag=True, help='Use 1500 for all teams')
def compare_devig_command(from_date, to_date, no_starting_elo):
    """Compare multiplicative vs power vs shin de-vig via calibration. Does not init_db."""
    from compare_devig import main as compare_main
    compare_main(
        from_date=from_date,
        to_date=to_date,
        use_starting_elo=not no_starting_elo,
    )


@cli.group('bets')
def bets_group():
    """Paper betting commands."""
    pass


@bets_group.command('pending')
def bets_pending():
    """Show pending paper bets."""
    trader = PaperTrader()
    bets = trader.get_pending_bets()
    
    if not bets:
        click.echo("No pending bets.")
        return
    
    table = []
    for b in bets:
        team = b['home_team'] if b['side'] == 'home' else b['away_team']
        table.append([
            b['id'],
            b['start_time'][:16],
            team,
            f"{b['odds']:.3f}",
            f"${b['stake']:.2f}",
            f"${b['potential_payout']:.2f}"
        ])
    
    click.echo(tabulate(table, headers=['ID', 'Game Time', 'Team', 'Odds', 'Stake', 'Payout']))


@bets_group.command('settle')
def bets_settle():
    """Settle completed paper bets."""
    init_db()
    
    # First update results
    results_ingester = ResultsIngester()
    results_ingester.update_results()
    
    # Then settle bets
    trader = PaperTrader()
    summary = trader.settle_bets()
    
    if summary['settled'] == 0:
        click.echo("No bets to settle.")


@bets_group.command('history')
@click.option('--limit', default=20, help='Number of bets to show')
def bets_history(limit):
    """Show bet history."""
    trader = PaperTrader()
    bets = trader.get_bet_history(limit)
    
    if not bets:
        click.echo("No bet history.")
        return
    
    table = []
    for b in bets:
        team = b['home_team'] if b['side'] == 'home' else b['away_team']
        result = b['result'].upper() if b['result'] else 'PENDING'
        pnl = f"${b['pnl']:+.2f}" if b['pnl'] is not None else '-'
        table.append([
            b['placed_at'][:16],
            team,
            f"{b['odds']:.3f}",
            f"${b['stake']:.2f}",
            result,
            pnl
        ])
    
    click.echo(tabulate(table, headers=['Placed', 'Team', 'Odds', 'Stake', 'Result', 'PnL']))


@bets_group.command('stats')
def bets_stats():
    """Show betting performance statistics."""
    trader = PaperTrader()
    stats = trader.get_performance_stats()
    
    click.echo("\nBETTING PERFORMANCE")
    click.echo("=" * 40)
    click.echo(f"Total Bets:     {stats['total_bets']}")
    click.echo(f"Wins:           {stats['wins']}")
    click.echo(f"Losses:         {stats['losses']}")
    click.echo(f"Win Rate:       {stats['win_rate']:.2%}")
    click.echo("-" * 40)
    click.echo(f"Total Staked:   ${stats['total_staked']:,.2f}")
    click.echo(f"Total P&L:      ${stats['total_pnl']:+,.2f}")
    click.echo(f"ROI:            {stats['roi']:.2%}")
    click.echo("-" * 40)
    click.echo(f"Bankroll:       ${stats['current_bankroll']:,.2f}")
    click.echo("=" * 40)


@cli.command('status')
def status_command():
    """Show system status overview."""
    from database.db import get_connection
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Count records
    cursor.execute("SELECT COUNT(*) as c FROM teams")
    teams = cursor.fetchone()['c']
    
    cursor.execute("SELECT COUNT(*) as c FROM games")
    games = cursor.fetchone()['c']
    
    cursor.execute("SELECT COUNT(*) as c FROM games WHERE status = 'FINAL'")
    final_games = cursor.fetchone()['c']
    
    cursor.execute("SELECT COUNT(*) as c FROM odds_snapshots")
    odds = cursor.fetchone()['c']
    
    cursor.execute("SELECT COUNT(*) as c FROM paper_bets")
    bets = cursor.fetchone()['c']
    
    cursor.execute("SELECT COUNT(*) as c FROM paper_bets WHERE result IS NULL")
    pending = cursor.fetchone()['c']
    
    conn.close()
    
    trader = PaperTrader()
    
    click.echo("\nSYSTEM STATUS")
    click.echo("=" * 40)
    click.echo(f"Teams:           {teams}")
    click.echo(f"Games:           {games} ({final_games} final)")
    click.echo(f"Odds Snapshots:  {odds}")
    click.echo(f"Paper Bets:      {bets} ({pending} pending)")
    click.echo(f"Bankroll:        ${trader.bankroll:,.2f}")
    click.echo("=" * 40)


if __name__ == '__main__':
    cli()

