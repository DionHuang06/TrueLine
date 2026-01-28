"""Remove duplicate games (same matchup + date, different external_id).

Keeps nba_com_* rows when both nba_com_* and manual_* exist; otherwise keeps
lowest id. Deletes child rows (elo_history, odds/odds_snapshots, etc.) first.
"""
import sqlite3
from pathlib import Path

from config import DB_PATH


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _table_exists(cursor, name: str) -> bool:
    cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cursor.fetchone() is not None


def run(dry_run: bool = False):
    conn = _get_conn()
    cur = conn.cursor()

    # Detect duplicates: same home, away, date
    cur.execute("""
        SELECT
            home_team_id,
            away_team_id,
            date(start_time) AS d
        FROM games
        GROUP BY home_team_id, away_team_id, date(start_time)
        HAVING COUNT(*) > 1
    """)
    dup_groups = cur.fetchall()
    if not dup_groups:
        print("No duplicate games found.")
        conn.close()
        return

    # Tables that reference games
    child_tables = ["elo_history", "predictions", "edges", "paper_bets"]
    if _table_exists(cur, "odds"):
        child_tables.append("odds")
    if _table_exists(cur, "odds_snapshots"):
        child_tables.append("odds_snapshots")

    to_delete = []  # list of game ids to remove
    to_keep = []    # list of (game_id, external_id) we keep

    for g in dup_groups:
        cur.execute("""
            SELECT id, external_id, start_time
            FROM games
            WHERE home_team_id = ? AND away_team_id = ? AND date(start_time) = ?
            ORDER BY
                CASE WHEN external_id LIKE 'nba_com_%' THEN 0 ELSE 1 END,
                id
        """, (g["home_team_id"], g["away_team_id"], g["d"]))
        rows = list(cur.fetchall())
        keeper = rows[0]
        to_keep.append((keeper["id"], keeper["external_id"] or ""))
        for r in rows[1:]:
            to_delete.append(r["id"])

    print(f"Found {len(dup_groups)} duplicate matchup(s), {len(to_delete)} game(s) to remove.")
    print()
    for (keep_id, keep_ext), gid in zip(to_keep, to_delete):
        cur.execute("SELECT external_id FROM games WHERE id = ?", (gid,))
        row = cur.fetchone()
        ext = (row["external_id"] if row else None) or ""
        print(f"  Keep game {keep_id} ({keep_ext})")
        print(f"  Delete game {gid} ({ext})")
        print()

    if dry_run:
        print("Dry run — no changes made.")
        conn.close()
        return

    try:
        for gid in to_delete:
            for tbl in child_tables:
                try:
                    cur.execute(f"DELETE FROM [{tbl}] WHERE game_id = ?", (gid,))
                    n = cur.rowcount
                    if n:
                        print(f"  Deleted {n} row(s) from {tbl} for game_id={gid}")
                except sqlite3.OperationalError as e:
                    if "no such table" not in str(e).lower():
                        raise
            cur.execute("DELETE FROM games WHERE id = ?", (gid,))
            assert cur.rowcount == 1, f"Expected 1 game deleted for id={gid}"
        conn.commit()
        print(f"\nRemoved {len(to_delete)} duplicate game(s).")
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Deduplication failed: {e}") from e
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    if dry:
        print("Dry run (use without --dry-run to apply changes):\n")
    run(dry_run=dry)
