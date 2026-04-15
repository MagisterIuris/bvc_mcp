"""
SQLite persistence layer for BVC market data history.

All functions open and close their own connection — no global connection state.
Each connection uses contextlib.closing() to guarantee conn.close() is called
on exit, which is required on Windows to release the file handle immediately
rather than waiting for garbage collection.

The special db_path value ':memory:' is supported for unit tests (skips
the directory creation step).
"""

from __future__ import annotations

import contextlib
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .models import MarketSnapshot

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS snapshots (
    id                INTEGER  PRIMARY KEY AUTOINCREMENT,
    fetched_at        DATETIME NOT NULL,
    market_state      TEXT     NOT NULL,
    total_stocks      INTEGER  NOT NULL,
    tradeable_stocks  INTEGER  NOT NULL,
    source_timestamp  TEXT     NOT NULL
);

CREATE TABLE IF NOT EXISTS stock_prices (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id          INTEGER NOT NULL REFERENCES snapshots(id),
    symbol               TEXT    NOT NULL,
    name                 TEXT    NOT NULL,
    price                REAL,
    variation            REAL,
    open                 REAL,
    high                 REAL,
    low                  REAL,
    volume_mad           REAL,
    quantity_traded      INTEGER,
    best_bid             REAL,
    best_ask             REAL,
    reference_price      REAL,
    last_trade_datetime  TEXT,
    segment_code         TEXT
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol
    ON stock_prices(symbol);

CREATE INDEX IF NOT EXISTS idx_stock_prices_snapshot_id
    ON stock_prices(snapshot_id);

CREATE INDEX IF NOT EXISTS idx_snapshots_fetched_at
    ON snapshots(fetched_at);

CREATE TABLE IF NOT EXISTS watchlists (
    id         INTEGER  PRIMARY KEY AUTOINCREMENT,
    owner      TEXT     NOT NULL DEFAULT 'default',
    name       TEXT     NOT NULL,
    created_at DATETIME NOT NULL,
    UNIQUE(owner, name)
);

CREATE INDEX IF NOT EXISTS idx_watchlists_owner
    ON watchlists(owner);

CREATE TABLE IF NOT EXISTS watchlist_stocks (
    watchlist_id INTEGER NOT NULL REFERENCES watchlists(id) ON DELETE CASCADE,
    symbol       TEXT    NOT NULL,
    added_at     DATETIME NOT NULL,
    PRIMARY KEY (watchlist_id, symbol)
);
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def init_db(db_path: str) -> None:
    """
    Create the database schema if it does not already exist.

    Also ensures the parent directory is created when using a file-based path.
    Uses contextlib.closing() to guarantee the connection is closed immediately
    after the DDL, which matters on Windows where open handles block file ops.

    Args:
        db_path: Path to the SQLite file, or ':memory:' for an in-memory DB.
    """
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.executescript(_DDL)

    logger.debug("Database initialised at %s", db_path)


def save_snapshot(
    snapshot: MarketSnapshot,
    db_path: str,
    fetched_at: Optional[str] = None,
) -> int:
    """
    Persist a full market snapshot (header + all tradeable stock prices) to the DB.

    Only tradeable stocks (price is not None) are written to stock_prices.

    Args:
        snapshot:   The parsed market snapshot to save.
        db_path:    Path to the SQLite database file.
        fetched_at: Optional UTC datetime string (``YYYY-MM-DD HH:MM:SS``).
                    Defaults to the current UTC time when omitted.
                    Passing an explicit value is useful in tests to guarantee
                    deterministic ordering without relying on wall-clock time.

    Returns:
        The auto-generated ``snapshot_id`` of the inserted row.
    """
    if fetched_at is None:
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    rows = [
        (
            None,  # placeholder, replaced after snapshot insert
            stock.symbol,
            stock.name,
            stock.price,
            stock.variation,
            stock.open,
            stock.high,
            stock.low,
            stock.volume_mad,
            stock.quantity_traded,
            stock.best_bid,
            stock.best_ask,
            stock.reference_price,
            stock.last_trade_datetime.isoformat() if stock.last_trade_datetime else None,
            stock.segment_code,
        )
        for stock in snapshot.tradeable_stocks
    ]

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        with conn:  # transaction: auto-commit on success, rollback on exception
            cursor = conn.execute(
                """
                INSERT INTO snapshots
                    (fetched_at, market_state, total_stocks, tradeable_stocks, source_timestamp)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    fetched_at,
                    snapshot.market_state,
                    len(snapshot.stocks),
                    len(snapshot.tradeable_stocks),
                    snapshot.timestamp,
                ),
            )
            snapshot_id: int = cursor.lastrowid  # type: ignore[assignment]

            # Patch the snapshot_id placeholder in each stock row
            stock_rows = [(snapshot_id,) + row[1:] for row in rows]

            conn.executemany(
                """
                INSERT INTO stock_prices
                    (snapshot_id, symbol, name, price, variation, open, high, low,
                     volume_mad, quantity_traded, best_bid, best_ask, reference_price,
                     last_trade_datetime, segment_code)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                stock_rows,
            )

    logger.info(
        "Snapshot #%d saved: %d tradeable stocks at %s",
        snapshot_id,
        len(stock_rows),
        fetched_at,
    )
    return snapshot_id


def get_history(symbol: str, limit: int, db_path: str) -> list[dict]:
    """
    Return the price history of a single stock across the last N snapshots.

    Results are ordered newest-first so that ``history[0]`` is the most recent
    data point.

    Args:
        symbol: BVC ticker symbol (case-insensitive).
        limit:  Maximum number of rows to return.
        db_path: Path to the SQLite database file.

    Returns:
        A list of dicts with keys:
        ``fetched_at``, ``price``, ``variation``, ``open``, ``high``, ``low``,
        ``volume_mad``, ``quantity_traded``.
        Returns an empty list if the symbol is unknown or the DB has no data.
    """
    sym = symbol.strip().upper()
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            SELECT
                s.fetched_at,
                sp.price,
                sp.variation,
                sp.open,
                sp.high,
                sp.low,
                sp.volume_mad,
                sp.quantity_traded
            FROM stock_prices sp
            JOIN snapshots s ON sp.snapshot_id = s.id
            WHERE sp.symbol = ?
            ORDER BY s.fetched_at DESC
            LIMIT ?
            """,
            (sym, limit),
        )
        return [dict(row) for row in cursor.fetchall()]


def get_price_in_range(
    symbol: str, from_date: str, to_date: str, db_path: str
) -> list[dict]:
    """
    Return price data points for a stock within a calendar date range.

    Results are ordered oldest-first so that ``data[0]`` is the start price
    and ``data[-1]`` is the end price for variation calculation.

    Args:
        symbol:    BVC ticker symbol (case-insensitive).
        from_date: Start date inclusive, format ``YYYY-MM-DD``.
        to_date:   End date inclusive, format ``YYYY-MM-DD``.
        db_path:   Path to the SQLite database file.

    Returns:
        A list of dicts with keys: ``fetched_at``, ``price``, ``variation``,
        ``high``, ``low``, ``volume_mad``.
    """
    sym = symbol.strip().upper()
    from_dt = f"{from_date} 00:00:00"
    to_dt = f"{to_date} 23:59:59"

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            SELECT
                s.fetched_at,
                sp.price,
                sp.variation,
                sp.high,
                sp.low,
                sp.volume_mad
            FROM stock_prices sp
            JOIN snapshots s ON sp.snapshot_id = s.id
            WHERE sp.symbol = ?
              AND s.fetched_at >= ?
              AND s.fetched_at <= ?
            ORDER BY s.fetched_at ASC
            """,
            (sym, from_dt, to_dt),
        )
        return [dict(row) for row in cursor.fetchall()]


def get_snapshots_list(limit: int, db_path: str) -> list[dict]:
    """
    Return a list of the most recent snapshot metadata records.

    Args:
        limit:   Maximum number of snapshots to return (newest first).
        db_path: Path to the SQLite database file.

    Returns:
        A list of dicts with keys: ``id``, ``fetched_at``, ``market_state``,
        ``tradeable_stocks``, ``total_stocks``, ``source_timestamp``.
    """
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            SELECT id, fetched_at, market_state, tradeable_stocks,
                   total_stocks, source_timestamp
            FROM snapshots
            ORDER BY fetched_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cursor.fetchall()]


def get_latest_snapshot_id(db_path: str) -> Optional[int]:
    """
    Return the id of the most recently saved snapshot, or None if the DB is empty.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        Integer snapshot id, or None.
    """
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        cursor = conn.execute(
            "SELECT id FROM snapshots ORDER BY fetched_at DESC LIMIT 1"
        )
        row = cursor.fetchone()
        return row[0] if row else None


def get_period_performance(
    from_date: str, to_date: str, db_path: str
) -> list[dict]:
    """
    Return the first and last known price for every symbol within a date range.

    Uses SQLite window functions (available since SQLite 3.25, 2018) to compute
    first/last prices in a single query — no per-symbol round-trips.

    Args:
        from_date: Start date inclusive, format ``YYYY-MM-DD``.
        to_date:   End date inclusive, format ``YYYY-MM-DD``.
        db_path:   Path to the SQLite database file.

    Returns:
        List of dicts with keys: ``symbol``, ``name``, ``start_price``,
        ``end_price``, ``start_at``, ``end_at``.
        Only symbols with at least one valid (non-NULL) price in the range
        and a positive start price are included.
    """
    from_dt = f"{from_date} 00:00:00"
    to_dt = f"{to_date} 23:59:59"

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    sp.symbol,
                    sp.name,
                    sp.price,
                    s.fetched_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY sp.symbol ORDER BY s.fetched_at ASC
                    ) AS rn_asc,
                    ROW_NUMBER() OVER (
                        PARTITION BY sp.symbol ORDER BY s.fetched_at DESC
                    ) AS rn_desc
                FROM stock_prices sp
                JOIN snapshots s ON sp.snapshot_id = s.id
                WHERE s.fetched_at >= ? AND s.fetched_at <= ?
                  AND sp.price IS NOT NULL
            )
            SELECT
                f.symbol,
                f.name,
                f.price   AS start_price,
                f.fetched_at AS start_at,
                l.price   AS end_price,
                l.fetched_at AS end_at
            FROM ranked f
            JOIN ranked l ON f.symbol = l.symbol AND l.rn_desc = 1
            WHERE f.rn_asc = 1 AND f.price > 0
            """,
            (from_dt, to_dt),
        )
        return [dict(row) for row in cursor.fetchall()]


def get_avg_volumes(db_path: str, limit: int = 20) -> dict:
    """
    Return the average trading volume (volume_mad) per symbol across the last
    ``limit`` snapshots. Uses a single SQL query for efficiency.

    Args:
        db_path: Path to the SQLite database file.
        limit:   Number of most-recent snapshots to include in the average.

    Returns:
        Dict mapping symbol → average volume (float). Symbols with no volume
        data in the window are omitted.
    """
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            WITH recent_snaps AS (
                SELECT id FROM snapshots
                ORDER BY fetched_at DESC
                LIMIT ?
            )
            SELECT sp.symbol, AVG(sp.volume_mad) AS avg_volume
            FROM stock_prices sp
            WHERE sp.snapshot_id IN (SELECT id FROM recent_snaps)
              AND sp.volume_mad IS NOT NULL
            GROUP BY sp.symbol
            """,
            (limit,),
        )
        return {row["symbol"]: row["avg_volume"] for row in cursor.fetchall()}


def get_symbols_ma_status(period: int, db_path: str) -> list[dict]:
    """
    For every symbol that has at least ``period`` data points, return the
    current price, the SMA(period), and whether the current price is above it.

    Uses a single SQL query via window functions for efficiency.

    Args:
        period:  Moving-average window (e.g. 20 for MA20).
        db_path: Path to the SQLite database file.

    Returns:
        List of dicts with keys: ``symbol``, ``price``, ``ma_value``,
        ``above_ma`` (1 or 0).
    """
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            WITH recent AS (
                SELECT
                    sp.symbol,
                    sp.price,
                    s.fetched_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY sp.symbol ORDER BY s.fetched_at DESC
                    ) AS rn
                FROM stock_prices sp
                JOIN snapshots s ON sp.snapshot_id = s.id
                WHERE sp.price IS NOT NULL
            ),
            ma_calc AS (
                SELECT
                    symbol,
                    AVG(price) AS ma_value,
                    COUNT(*)   AS cnt
                FROM recent
                WHERE rn <= ?
                GROUP BY symbol
                HAVING cnt >= ?
            ),
            current_price AS (
                SELECT symbol, price FROM recent WHERE rn = 1
            )
            SELECT
                cp.symbol,
                cp.price,
                mc.ma_value,
                CASE WHEN cp.price > mc.ma_value THEN 1 ELSE 0 END AS above_ma
            FROM current_price cp
            JOIN ma_calc mc ON cp.symbol = mc.symbol
            """,
            (period, period),
        )
        return [dict(row) for row in cursor.fetchall()]


def get_all_symbols_recent_prices(limit: int, db_path: str) -> dict:
    """
    Return the last ``limit`` prices for every symbol in a single query.

    Used for batch analytics (e.g. computing market-wide volatility) without
    issuing one query per symbol.

    Args:
        limit:   Maximum number of most-recent prices per symbol.
        db_path: Path to the SQLite database file.

    Returns:
        Dict mapping symbol → list[float] of prices in **chronological order**
        (oldest first). Symbols with no price data are omitted.
    """
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            WITH recent AS (
                SELECT
                    sp.symbol,
                    sp.price,
                    s.fetched_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY sp.symbol ORDER BY s.fetched_at DESC
                    ) AS rn
                FROM stock_prices sp
                JOIN snapshots s ON sp.snapshot_id = s.id
                WHERE sp.price IS NOT NULL
            )
            SELECT symbol, price, fetched_at
            FROM recent
            WHERE rn <= ?
            ORDER BY symbol, fetched_at ASC
            """,
            (limit + 1,),
        )
        result: dict = {}
        for row in cursor.fetchall():
            sym = row["symbol"]
            if sym not in result:
                result[sym] = []
            result[sym].append(row["price"])
        return result
