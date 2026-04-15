"""
Watchlist management for the BVC MCP server.

Stores named watchlists in the same SQLite database as market history.
The required tables (watchlists, watchlist_stocks) are created by
database.init_db() — call that before using any function here.

All connections use contextlib.closing() so file handles are released
immediately on Windows.

Each public function accepts an ``owner`` parameter that scopes the
operation to a single user. The default is ``"default"``, which preserves
the single-user behaviour of Claude Desktop deployments.
"""

from __future__ import annotations

import contextlib
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from .config import DEFAULT_OWNER

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _utc_now() -> str:
    """Return current UTC time as a 'YYYY-MM-DD HH:MM:SS' string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _get_watchlist_id(
    conn: sqlite3.Connection, name: str, owner: str
) -> Optional[int]:
    """Return the id of a watchlist by (owner, name), or None if not found."""
    row = conn.execute(
        "SELECT id FROM watchlists WHERE owner = ? AND name = ?", (owner, name)
    ).fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_watchlist(
    name: str,
    symbols: list[str],
    db_path: str,
    owner: str = DEFAULT_OWNER,
) -> dict:
    """
    Create a new watchlist with an initial set of symbols.

    Args:
        name:    Unique name for the watchlist within the owner's namespace.
        symbols: List of BVC ticker symbols (uppercased and stripped).
        db_path: Path to the SQLite database.
        owner:   Owner identifier. Defaults to ``"default"``.

    Returns:
        Dict with keys: ``id``, ``name``, ``created_at``, ``symbols``, ``count``.

    Raises:
        ValueError: If a watchlist with the same (owner, name) already exists.
    """
    now = _utc_now()
    clean = [s.strip().upper() for s in symbols if s.strip()]

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        with conn:
            try:
                cursor = conn.execute(
                    "INSERT INTO watchlists (owner, name, created_at) VALUES (?, ?, ?)",
                    (owner, name, now),
                )
                wl_id: int = cursor.lastrowid  # type: ignore[assignment]
            except sqlite3.IntegrityError:
                raise ValueError(f"Watchlist '{name}' already exists.")

            for sym in clean:
                conn.execute(
                    """INSERT OR IGNORE INTO watchlist_stocks
                       (watchlist_id, symbol, added_at) VALUES (?, ?, ?)""",
                    (wl_id, sym, now),
                )

    logger.info("Created watchlist '%s' with %d symbols", name, len(clean))
    return {
        "id": wl_id,
        "name": name,
        "created_at": now,
        "symbols": clean,
        "count": len(clean),
    }


def get_watchlist(
    name: str,
    db_path: str,
    owner: str = DEFAULT_OWNER,
) -> Optional[dict]:
    """
    Return a watchlist by name, including its symbols and metadata.

    Args:
        name:    Watchlist name.
        db_path: Path to the SQLite database.
        owner:   Owner identifier. Defaults to ``"default"``.

    Returns:
        Dict with keys ``id``, ``name``, ``created_at``, ``stocks``, ``count``,
        or None if the watchlist does not exist for this owner.
    """
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        wl_row = conn.execute(
            "SELECT id, name, created_at FROM watchlists WHERE owner = ? AND name = ?",
            (owner, name),
        ).fetchone()

        if wl_row is None:
            return None

        wl = dict(wl_row)
        stocks = conn.execute(
            """SELECT symbol, added_at FROM watchlist_stocks
               WHERE watchlist_id = ? ORDER BY added_at ASC""",
            (wl["id"],),
        ).fetchall()

        wl["stocks"] = [dict(s) for s in stocks]
        wl["count"] = len(wl["stocks"])
        return wl


def list_watchlists(
    db_path: str,
    owner: str = DEFAULT_OWNER,
) -> list[dict]:
    """
    Return all watchlists for ``owner`` with their stock counts, newest-first.

    Args:
        db_path: Path to the SQLite database.
        owner:   Owner identifier. Defaults to ``"default"``.

    Returns:
        List of dicts with keys ``id``, ``name``, ``created_at``, ``stock_count``.
    """
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT w.id, w.name, w.created_at,
                   COUNT(ws.symbol) AS stock_count
            FROM watchlists w
            LEFT JOIN watchlist_stocks ws ON w.id = ws.watchlist_id
            WHERE w.owner = ?
            GROUP BY w.id
            ORDER BY w.created_at DESC
            """,
            (owner,),
        ).fetchall()
        return [dict(r) for r in rows]


def add_to_watchlist(
    name: str,
    symbol: str,
    db_path: str,
    owner: str = DEFAULT_OWNER,
) -> dict:
    """
    Add a symbol to an existing watchlist.

    Args:
        name:    Watchlist name.
        symbol:  BVC ticker symbol.
        db_path: Path to the SQLite database.
        owner:   Owner identifier. Defaults to ``"default"``.

    Returns:
        Success dict, or an error dict if the watchlist does not exist or
        the symbol is already present.
    """
    sym = symbol.strip().upper()
    now = _utc_now()

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        wl_id = _get_watchlist_id(conn, name, owner)
        if wl_id is None:
            return {"error": f"Watchlist '{name}' not found."}

        existing = conn.execute(
            "SELECT 1 FROM watchlist_stocks WHERE watchlist_id = ? AND symbol = ?",
            (wl_id, sym),
        ).fetchone()
        if existing:
            return {"error": f"Symbol '{sym}' is already in watchlist '{name}'."}

        with conn:
            conn.execute(
                """INSERT INTO watchlist_stocks (watchlist_id, symbol, added_at)
                   VALUES (?, ?, ?)""",
                (wl_id, sym, now),
            )

    return {"success": True, "watchlist": name, "symbol": sym, "added_at": now}


def remove_from_watchlist(
    name: str,
    symbol: str,
    db_path: str,
    owner: str = DEFAULT_OWNER,
) -> dict:
    """
    Remove a symbol from a watchlist.

    Args:
        name:    Watchlist name.
        symbol:  BVC ticker symbol.
        db_path: Path to the SQLite database.
        owner:   Owner identifier. Defaults to ``"default"``.

    Returns:
        Success dict, or an error dict if the watchlist or symbol is not found.
    """
    sym = symbol.strip().upper()

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        wl_id = _get_watchlist_id(conn, name, owner)
        if wl_id is None:
            return {"error": f"Watchlist '{name}' not found."}

        with conn:
            cursor = conn.execute(
                "DELETE FROM watchlist_stocks WHERE watchlist_id = ? AND symbol = ?",
                (wl_id, sym),
            )

        if cursor.rowcount == 0:
            return {"error": f"Symbol '{sym}' not found in watchlist '{name}'."}

    return {"success": True, "watchlist": name, "symbol": sym}


def delete_watchlist(
    name: str,
    db_path: str,
    owner: str = DEFAULT_OWNER,
) -> dict:
    """
    Delete an entire watchlist and all its associated symbols.

    Args:
        name:    Watchlist name.
        db_path: Path to the SQLite database.
        owner:   Owner identifier. Defaults to ``"default"``.

    Returns:
        Success dict, or an error dict if the watchlist does not exist.
    """
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        wl_id = _get_watchlist_id(conn, name, owner)
        if wl_id is None:
            return {"error": f"Watchlist '{name}' not found."}

        with conn:
            # Explicit child delete — does not rely on PRAGMA foreign_keys
            conn.execute(
                "DELETE FROM watchlist_stocks WHERE watchlist_id = ?", (wl_id,)
            )
            conn.execute("DELETE FROM watchlists WHERE id = ?", (wl_id,))

    logger.info("Deleted watchlist '%s'", name)
    return {"success": True, "watchlist": name}


def get_watchlist_symbols(
    name: str,
    db_path: str,
    owner: str = DEFAULT_OWNER,
) -> list[str]:
    """
    Return only the list of ticker symbols in a watchlist.

    Args:
        name:    Watchlist name.
        db_path: Path to the SQLite database.
        owner:   Owner identifier. Defaults to ``"default"``.

    Returns:
        List of uppercase ticker symbols ordered by insertion time.
        Returns an empty list if the watchlist does not exist.
    """
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        wl_id = _get_watchlist_id(conn, name, owner)
        if wl_id is None:
            return []

        rows = conn.execute(
            """SELECT symbol FROM watchlist_stocks
               WHERE watchlist_id = ? ORDER BY added_at ASC""",
            (wl_id,),
        ).fetchall()
        return [r[0] for r in rows]
