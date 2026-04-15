"""
Unit tests for database.py.

All tests that need multiple DB operations use temporary files on disk
(not ':memory:') because each sqlite3.connect(':memory:') call creates an
independent, empty database — schemas created in one connection are invisible
to another.

Windows note: every direct sqlite3.connect() call in test helper code uses
contextlib.closing() so the file handle is released immediately, allowing
os.unlink() to succeed in the cleanup block.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import tempfile
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bvc_mcp.models import MarketSnapshot, Stock

# ---------------------------------------------------------------------------
# Sample raw API payloads
# ---------------------------------------------------------------------------

RAW_STOCK_A = {
    "Symbol": "ATW",
    "Libelle": "ATTIJARIWAFA BANK",
    "Cours": 667.9,
    "Variation": -4.74,
    "Ouverture": 681.0,
    "PlusHaut": 690.0,
    "PlusBas": 667.9,
    "Volumes": 32684179.2,
    "QteEchangee": 48497,
    "MeilleurDemande": 666.7,
    "QteAchat": 15,
    "MeilleurOffre": 689.9,
    "QteVente": 1000,
    "CoursDeReferance": 701.1,
    "DateDernierCours": "09/03/2026 13:35:29",
    "Etat": "Market Close",
    "CodeSegment": "01",
    "IdTypeValeur": 1,
}

RAW_STOCK_B = {
    "Symbol": "IAM",
    "Libelle": "ITISSALAT AL-MAGHRIB",
    "Cours": 180.5,
    "Variation": 3.66,
    "Ouverture": 174.0,
    "PlusHaut": 181.0,
    "PlusBas": 173.5,
    "Volumes": 5200000.0,
    "QteEchangee": 29000,
    "MeilleurDemande": 180.0,
    "QteAchat": 200,
    "MeilleurOffre": 181.0,
    "QteVente": 500,
    "CoursDeReferance": 174.2,
    "DateDernierCours": "09/03/2026 14:00:00",
    "Etat": "Market Close",
    "CodeSegment": "01",
    "IdTypeValeur": 1,
}

RAW_STOCK_SUSPENDED = {
    "Symbol": "XYZ",
    "Libelle": "SUSPENDED CO",
    "Cours": "",
    "Variation": "",
    "Ouverture": "",
    "PlusHaut": "",
    "PlusBas": "",
    "Volumes": "",
    "QteEchangee": "",
    "MeilleurDemande": "",
    "QteAchat": "",
    "MeilleurOffre": "",
    "QteVente": "",
    "CoursDeReferance": 50.0,
    "DateDernierCours": "",
    "Etat": "Market Close",
    "CodeSegment": "02",
    "IdTypeValeur": 1,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_snapshot(timestamp: str = "2026-03-09 15:54:02") -> MarketSnapshot:
    """Build a MarketSnapshot with two tradeable stocks and one suspended stock."""
    stocks = [
        Stock.model_validate(RAW_STOCK_A),
        Stock.model_validate(RAW_STOCK_B),
        Stock.model_validate(RAW_STOCK_SUSPENDED),
    ]
    return MarketSnapshot(
        success=True,
        lastModified=1773071642,
        timestamp=timestamp,
        timestampFrench="lundi 9 mars 2026 16:54:02",
        stocks=stocks,
    )


def make_temp_db() -> str:
    """Create a temporary file and return its path (caller must unlink it)."""
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    return f.name


# ---------------------------------------------------------------------------
# init_db
# ---------------------------------------------------------------------------


class TestInitDb:
    def test_creates_tables(self):
        """init_db on ':memory:' should not raise."""
        from bvc_mcp.database import init_db

        init_db(":memory:")

    def test_idempotent(self):
        """Calling init_db twice on the same path should not raise."""
        from bvc_mcp.database import init_db

        db_path = make_temp_db()
        try:
            init_db(db_path)
            init_db(db_path)  # CREATE TABLE IF NOT EXISTS — no error
        finally:
            os.unlink(db_path)

    def test_creates_snapshots_and_stock_prices_tables(self):
        """Both tables must exist after init_db."""
        from bvc_mcp.database import init_db

        db_path = make_temp_db()
        try:
            init_db(db_path)
            with contextlib.closing(sqlite3.connect(db_path)) as conn:
                tables = {
                    r[0]
                    for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
            assert "snapshots" in tables
            assert "stock_prices" in tables
        finally:
            os.unlink(db_path)


# ---------------------------------------------------------------------------
# save_snapshot
# ---------------------------------------------------------------------------


class TestSaveSnapshot:
    def test_returns_positive_integer(self):
        """save_snapshot should return the auto-incremented snapshot id."""
        from bvc_mcp.database import init_db, save_snapshot

        db_path = make_temp_db()
        try:
            init_db(db_path)
            snapshot_id = save_snapshot(make_snapshot(), db_path)
            assert isinstance(snapshot_id, int)
            assert snapshot_id >= 1
        finally:
            os.unlink(db_path)

    def test_second_snapshot_gets_higher_id(self):
        """Two consecutive saves should produce monotonically increasing ids."""
        from bvc_mcp.database import init_db, save_snapshot

        db_path = make_temp_db()
        try:
            init_db(db_path)
            id1 = save_snapshot(make_snapshot(), db_path, fetched_at="2026-03-09 09:00:00")
            id2 = save_snapshot(make_snapshot(), db_path, fetched_at="2026-03-09 10:00:00")
            assert id2 > id1
        finally:
            os.unlink(db_path)

    def test_only_tradeable_stocks_saved(self):
        """Suspended stocks (price=None) must NOT appear in stock_prices."""
        from bvc_mcp.database import init_db, save_snapshot

        db_path = make_temp_db()
        try:
            init_db(db_path)
            save_snapshot(make_snapshot(), db_path)
            with contextlib.closing(sqlite3.connect(db_path)) as conn:
                rows = conn.execute("SELECT symbol FROM stock_prices").fetchall()
            symbols = {r[0] for r in rows}
            assert "XYZ" not in symbols
            assert "ATW" in symbols
            assert "IAM" in symbols
        finally:
            os.unlink(db_path)

    def test_snapshot_row_inserted(self):
        """The snapshots table should have exactly one row after one save."""
        from bvc_mcp.database import init_db, save_snapshot

        db_path = make_temp_db()
        try:
            init_db(db_path)
            save_snapshot(make_snapshot(), db_path)
            with contextlib.closing(sqlite3.connect(db_path)) as conn:
                count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            assert count == 1
        finally:
            os.unlink(db_path)

    def test_explicit_fetched_at_is_stored(self):
        """Passing an explicit fetched_at should be stored verbatim in the DB."""
        from bvc_mcp.database import init_db, save_snapshot

        db_path = make_temp_db()
        try:
            init_db(db_path)
            save_snapshot(make_snapshot(), db_path, fetched_at="2026-01-15 12:30:00")
            with contextlib.closing(sqlite3.connect(db_path)) as conn:
                row = conn.execute("SELECT fetched_at FROM snapshots").fetchone()
            assert row[0] == "2026-01-15 12:30:00"
        finally:
            os.unlink(db_path)


# ---------------------------------------------------------------------------
# get_history
# ---------------------------------------------------------------------------


class TestGetHistory:
    def _setup_db(self) -> str:
        """Create a temp DB with two snapshots at different times."""
        from bvc_mcp.database import init_db, save_snapshot

        db_path = make_temp_db()
        init_db(db_path)
        save_snapshot(make_snapshot(), db_path, fetched_at="2026-03-09 09:00:00")
        save_snapshot(make_snapshot(), db_path, fetched_at="2026-03-09 10:00:00")
        return db_path

    def test_returns_history_for_known_symbol(self):
        """get_history should return rows for a symbol that has been saved."""
        from bvc_mcp.database import get_history

        db_path = self._setup_db()
        try:
            rows = get_history("ATW", 10, db_path)
            assert len(rows) == 2
        finally:
            os.unlink(db_path)

    def test_history_ordered_newest_first(self):
        """Results should be ordered descending by fetched_at."""
        from bvc_mcp.database import get_history

        db_path = self._setup_db()
        try:
            rows = get_history("ATW", 10, db_path)
            assert rows[0]["fetched_at"] > rows[1]["fetched_at"]
        finally:
            os.unlink(db_path)

    def test_limit_respected(self):
        """get_history with limit=1 should return at most 1 row."""
        from bvc_mcp.database import get_history

        db_path = self._setup_db()
        try:
            rows = get_history("ATW", 1, db_path)
            assert len(rows) == 1
        finally:
            os.unlink(db_path)

    def test_unknown_symbol_returns_empty_list(self):
        """get_history for a symbol not in the DB should return []."""
        from bvc_mcp.database import get_history

        db_path = self._setup_db()
        try:
            rows = get_history("UNKNOWN", 10, db_path)
            assert rows == []
        finally:
            os.unlink(db_path)

    def test_case_insensitive_symbol(self):
        """Symbol lookup should normalise to uppercase before querying."""
        from bvc_mcp.database import get_history

        db_path = self._setup_db()
        try:
            rows_upper = get_history("ATW", 10, db_path)
            rows_lower = get_history("atw", 10, db_path)
            assert len(rows_upper) == len(rows_lower)
        finally:
            os.unlink(db_path)

    def test_row_contains_expected_keys(self):
        """Each history row should have all expected field keys."""
        from bvc_mcp.database import get_history

        db_path = self._setup_db()
        try:
            rows = get_history("ATW", 10, db_path)
            expected = {
                "fetched_at", "price", "variation", "open",
                "high", "low", "volume_mad", "quantity_traded",
            }
            assert expected.issubset(rows[0].keys())
        finally:
            os.unlink(db_path)


# ---------------------------------------------------------------------------
# get_price_in_range
# ---------------------------------------------------------------------------


class TestGetPriceInRange:
    def _setup_db(self) -> str:
        """Create a temp DB with three snapshots on three consecutive days."""
        from bvc_mcp.database import init_db, save_snapshot

        db_path = make_temp_db()
        init_db(db_path)
        save_snapshot(make_snapshot(), db_path, fetched_at="2026-03-09 09:00:00")
        save_snapshot(make_snapshot(), db_path, fetched_at="2026-03-10 09:00:00")
        save_snapshot(make_snapshot(), db_path, fetched_at="2026-03-11 09:00:00")
        return db_path

    def test_returns_rows_within_range(self):
        """Query for 2 days should return exactly the snapshots within that window."""
        from bvc_mcp.database import get_price_in_range

        db_path = self._setup_db()
        try:
            rows = get_price_in_range("ATW", "2026-03-09", "2026-03-10", db_path)
            assert len(rows) == 2
        finally:
            os.unlink(db_path)

    def test_excludes_rows_outside_range(self):
        """Query for a single day should only return the snapshot for that day."""
        from bvc_mcp.database import get_price_in_range

        db_path = self._setup_db()
        try:
            rows = get_price_in_range("ATW", "2026-03-11", "2026-03-11", db_path)
            assert len(rows) == 1
        finally:
            os.unlink(db_path)

    def test_ordered_ascending(self):
        """Results must be chronological (oldest first) for evolution calculations."""
        from bvc_mcp.database import get_price_in_range

        db_path = self._setup_db()
        try:
            rows = get_price_in_range("ATW", "2026-03-09", "2026-03-11", db_path)
            assert rows[0]["fetched_at"] < rows[-1]["fetched_at"]
        finally:
            os.unlink(db_path)

    def test_empty_range_returns_empty(self):
        """A date range with no data should return an empty list."""
        from bvc_mcp.database import get_price_in_range

        db_path = self._setup_db()
        try:
            rows = get_price_in_range("ATW", "2025-01-01", "2025-01-02", db_path)
            assert rows == []
        finally:
            os.unlink(db_path)

    def test_row_contains_expected_keys(self):
        """Each range row should have the keys needed for price evolution."""
        from bvc_mcp.database import get_price_in_range

        db_path = self._setup_db()
        try:
            rows = get_price_in_range("ATW", "2026-03-09", "2026-03-11", db_path)
            expected = {"fetched_at", "price", "variation", "high", "low", "volume_mad"}
            assert expected.issubset(rows[0].keys())
        finally:
            os.unlink(db_path)


# ---------------------------------------------------------------------------
# get_snapshots_list
# ---------------------------------------------------------------------------


class TestGetSnapshotsList:
    def _setup_db(self, n: int = 3) -> str:
        from bvc_mcp.database import init_db, save_snapshot

        db_path = make_temp_db()
        init_db(db_path)
        for i in range(n):
            save_snapshot(
                make_snapshot(f"2026-03-{9 + i:02d} 09:00:00"),
                db_path,
                fetched_at=f"2026-03-{9 + i:02d} 09:00:00",
            )
        return db_path

    def test_returns_correct_count(self):
        from bvc_mcp.database import get_snapshots_list

        db_path = self._setup_db(3)
        try:
            rows = get_snapshots_list(10, db_path)
            assert len(rows) == 3
        finally:
            os.unlink(db_path)

    def test_limit_respected(self):
        from bvc_mcp.database import get_snapshots_list

        db_path = self._setup_db(3)
        try:
            rows = get_snapshots_list(2, db_path)
            assert len(rows) == 2
        finally:
            os.unlink(db_path)

    def test_ordered_newest_first(self):
        from bvc_mcp.database import get_snapshots_list

        db_path = self._setup_db(3)
        try:
            rows = get_snapshots_list(10, db_path)
            assert rows[0]["fetched_at"] >= rows[-1]["fetched_at"]
        finally:
            os.unlink(db_path)

    def test_row_contains_expected_keys(self):
        from bvc_mcp.database import get_snapshots_list

        db_path = self._setup_db(1)
        try:
            rows = get_snapshots_list(1, db_path)
            expected = {
                "id", "fetched_at", "market_state",
                "tradeable_stocks", "total_stocks", "source_timestamp",
            }
            assert expected.issubset(rows[0].keys())
        finally:
            os.unlink(db_path)

    def test_empty_db_returns_empty_list(self):
        from bvc_mcp.database import init_db, get_snapshots_list

        db_path = make_temp_db()
        try:
            init_db(db_path)
            rows = get_snapshots_list(10, db_path)
            assert rows == []
        finally:
            os.unlink(db_path)


# ---------------------------------------------------------------------------
# get_latest_snapshot_id
# ---------------------------------------------------------------------------


class TestGetLatestSnapshotId:
    def test_returns_none_on_empty_db(self):
        from bvc_mcp.database import init_db, get_latest_snapshot_id

        db_path = make_temp_db()
        try:
            init_db(db_path)
            assert get_latest_snapshot_id(db_path) is None
        finally:
            os.unlink(db_path)

    def test_returns_id_after_save(self):
        from bvc_mcp.database import init_db, save_snapshot, get_latest_snapshot_id

        db_path = make_temp_db()
        try:
            init_db(db_path)
            saved_id = save_snapshot(make_snapshot(), db_path)
            latest_id = get_latest_snapshot_id(db_path)
            assert latest_id == saved_id
        finally:
            os.unlink(db_path)

    def test_returns_most_recent_id(self):
        """After two saves, should return the id of the second (more recent) save."""
        from bvc_mcp.database import init_db, save_snapshot, get_latest_snapshot_id

        db_path = make_temp_db()
        try:
            init_db(db_path)
            save_snapshot(make_snapshot(), db_path, fetched_at="2026-03-09 09:00:00")
            id2 = save_snapshot(make_snapshot(), db_path, fetched_at="2026-03-09 10:00:00")
            assert get_latest_snapshot_id(db_path) == id2
        finally:
            os.unlink(db_path)


# ---------------------------------------------------------------------------
# Scheduler helper: is_market_open
# ---------------------------------------------------------------------------


class TestIsMarketOpen:
    """Tests for the market-hours check in scheduler.py."""

    def test_returns_bool(self):
        """is_market_open() should always return a bool."""
        from bvc_mcp.scheduler import is_market_open

        result = is_market_open()
        assert isinstance(result, bool)

    def test_weekend_returns_false(self):
        """On a Saturday (weekday=5), is_market_open must return False."""
        from unittest.mock import patch

        import pytz

        from bvc_mcp.scheduler import is_market_open

        tz = pytz.timezone("Africa/Casablanca")
        saturday_noon = datetime(2026, 3, 14, 12, 0, 0, tzinfo=tz)  # Saturday

        with patch("bvc_mcp.scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = saturday_noon
            assert is_market_open() is False

    def test_weekday_during_hours_returns_true(self):
        """On a weekday at 10:00 Casablanca time, is_market_open must return True."""
        from unittest.mock import patch

        import pytz

        from bvc_mcp.scheduler import is_market_open

        tz = pytz.timezone("Africa/Casablanca")
        monday_10am = datetime(2026, 3, 9, 10, 0, 0, tzinfo=tz)  # Monday

        with patch("bvc_mcp.scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = monday_10am
            assert is_market_open() is True

    def test_weekday_before_open_returns_false(self):
        """Before 09:00 on a weekday, market should be closed."""
        from unittest.mock import patch

        import pytz

        from bvc_mcp.scheduler import is_market_open

        tz = pytz.timezone("Africa/Casablanca")
        monday_8am = datetime(2026, 3, 9, 8, 59, 0, tzinfo=tz)

        with patch("bvc_mcp.scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = monday_8am
            assert is_market_open() is False

    def test_weekday_after_close_returns_false(self):
        """After 15:30 on a weekday, market should be closed."""
        from unittest.mock import patch

        import pytz

        from bvc_mcp.scheduler import is_market_open

        tz = pytz.timezone("Africa/Casablanca")
        monday_4pm = datetime(2026, 3, 9, 16, 0, 0, tzinfo=tz)

        with patch("bvc_mcp.scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = monday_4pm
            assert is_market_open() is False

    def test_weekday_exactly_at_close_returns_true(self):
        """At exactly 15:30 on a weekday, market should still be considered open."""
        from unittest.mock import patch

        import pytz

        from bvc_mcp.scheduler import is_market_open

        tz = pytz.timezone("Africa/Casablanca")
        monday_330pm = datetime(2026, 3, 9, 15, 30, 0, tzinfo=tz)

        with patch("bvc_mcp.scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = monday_330pm
            assert is_market_open() is True


class TestSchedulerLifecycle:
    def setup_method(self):
        import bvc_mcp.scheduler as scheduler_module

        scheduler_module._scheduler = None

    def test_collect_snapshot_skips_when_market_closed(self):
        from bvc_mcp import scheduler

        with (
            patch("bvc_mcp.scheduler.is_market_open", return_value=False),
            patch("bvc_mcp.scheduler.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
            patch("bvc_mcp.scheduler.save_snapshot") as mock_save,
        ):
            scheduler.collect_snapshot()

        mock_fetch.assert_not_called()
        mock_save.assert_not_called()

    def test_collect_snapshot_saves_when_market_open(self):
        from bvc_mcp import scheduler

        snapshot = make_snapshot()
        with (
            patch("bvc_mcp.scheduler.is_market_open", return_value=True),
            patch("bvc_mcp.scheduler.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
            patch("bvc_mcp.scheduler.save_snapshot", return_value=123) as mock_save,
        ):
            mock_fetch.return_value = snapshot
            scheduler.collect_snapshot()

        mock_fetch.assert_called_once_with(force_refresh=True)
        mock_save.assert_called_once()

    def test_collect_snapshot_catches_exceptions(self):
        from bvc_mcp import scheduler

        with (
            patch("bvc_mcp.scheduler.is_market_open", return_value=True),
            patch("bvc_mcp.scheduler.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
        ):
            mock_fetch.side_effect = RuntimeError("boom")
            scheduler.collect_snapshot()

    def test_start_scheduler_noop_if_already_running(self):
        from bvc_mcp import scheduler

        scheduler._scheduler = MagicMock(running=True)
        with (
            patch("bvc_mcp.scheduler.init_db") as mock_init_db,
            patch("bvc_mcp.scheduler.BackgroundScheduler") as mock_scheduler_cls,
        ):
            scheduler.start_scheduler()

        mock_init_db.assert_not_called()
        mock_scheduler_cls.assert_not_called()

    def test_start_scheduler_collects_immediately_when_market_open(self):
        from bvc_mcp import scheduler

        fake_scheduler = MagicMock()
        with (
            patch("bvc_mcp.scheduler.init_db") as mock_init_db,
            patch("bvc_mcp.scheduler.BackgroundScheduler", return_value=fake_scheduler),
            patch("bvc_mcp.scheduler.is_market_open", return_value=True),
            patch("bvc_mcp.scheduler.collect_snapshot") as mock_collect,
        ):
            scheduler.start_scheduler()

        mock_init_db.assert_called_once()
        fake_scheduler.add_job.assert_called_once()
        fake_scheduler.start.assert_called_once()
        mock_collect.assert_called_once()

    def test_start_scheduler_waits_when_market_closed(self):
        from bvc_mcp import scheduler

        fake_scheduler = MagicMock()
        with (
            patch("bvc_mcp.scheduler.init_db"),
            patch("bvc_mcp.scheduler.BackgroundScheduler", return_value=fake_scheduler),
            patch("bvc_mcp.scheduler.is_market_open", return_value=False),
            patch("bvc_mcp.scheduler.collect_snapshot") as mock_collect,
        ):
            scheduler.start_scheduler()

        mock_collect.assert_not_called()

    def test_stop_scheduler_shuts_down_running_instance(self):
        from bvc_mcp import scheduler

        fake_scheduler = MagicMock(running=True)
        scheduler._scheduler = fake_scheduler
        scheduler.stop_scheduler()

        fake_scheduler.shutdown.assert_called_once_with(wait=False)
        assert scheduler._scheduler is None

    def test_stop_scheduler_is_safe_when_not_started(self):
        from bvc_mcp import scheduler

        scheduler._scheduler = None
        scheduler.stop_scheduler()
        assert scheduler._scheduler is None
