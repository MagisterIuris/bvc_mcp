"""
Migration script: add the ``owner`` column to an existing watchlists table.

Safe to run multiple times (idempotent). Does nothing if the column already
exists. Migrates all existing watchlists to owner='default'.

Usage:
    python scripts/migrate_db.py
    python scripts/migrate_db.py --db path/to/custom.db
"""

from __future__ import annotations

import argparse
import contextlib
import sqlite3
import sys
from pathlib import Path

# Make sure the package is importable when running from the project root
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bvc_mcp.config import DB_PATH  # noqa: E402


def migrate(db_path: str) -> None:
    if not Path(db_path).exists():
        print(f"Database not found at {db_path!r} — nothing to migrate.")
        return

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        # Check existing columns
        cols = {row[1] for row in conn.execute("PRAGMA table_info(watchlists)")}

        if "owner" in cols:
            count = conn.execute("SELECT COUNT(*) FROM watchlists").fetchone()[0]
            print(f"Column 'owner' already exists — {count} watchlist(s) unchanged.")
            return

        print("Adding 'owner' column to watchlists table …")
        conn.execute(
            "ALTER TABLE watchlists ADD COLUMN owner TEXT NOT NULL DEFAULT 'default'"
        )
        conn.commit()

        # Create the index if absent
        existing_indexes = {
            row[1]
            for row in conn.execute("SELECT * FROM sqlite_master WHERE type='index'")
        }
        if "idx_watchlists_owner" not in existing_indexes:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_watchlists_owner ON watchlists(owner)"
            )
            conn.commit()

        count = conn.execute("SELECT COUNT(*) FROM watchlists").fetchone()[0]
        print(f"Migration complete — {count} watchlist(s) set to owner='default'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate BVC MCP database schema.")
    parser.add_argument(
        "--db",
        default=DB_PATH,
        help=f"Path to the SQLite database (default: {DB_PATH})",
    )
    args = parser.parse_args()
    migrate(args.db)


if __name__ == "__main__":
    main()
