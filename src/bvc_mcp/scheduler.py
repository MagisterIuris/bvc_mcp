"""
Background scheduler for automatic BVC market data collection.

Uses APScheduler to fire a job every hour at minute=0. The job itself
checks whether the BVC market is currently open before fetching and
persisting data — if the market is closed the job logs a message and exits
immediately without any network or DB activity.

The scheduler runs as a daemon thread so it never blocks the MCP server's
main process from exiting cleanly.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .client import fetch_market_data
from .config import (
    DB_PATH,
    MARKET_CLOSE_HOUR,
    MARKET_CLOSE_MINUTE,
    MARKET_OPEN_HOUR,
    MARKET_OPEN_MINUTE,
    MARKET_TIMEZONE,
    SCHEDULER_INTERVAL_HOURS,
)
from .database import init_db, save_snapshot

logger = logging.getLogger(__name__)

# Module-level scheduler instance — only one should ever be running.
_scheduler: Optional[BackgroundScheduler] = None


# ---------------------------------------------------------------------------
# Market-hours helper
# ---------------------------------------------------------------------------


def is_market_open() -> bool:
    """
    Return True if the BVC market is currently within its official trading hours.

    Trading hours:
    - Days:  Monday to Friday (weekday index 0–4)
    - Hours: 09:00 – 15:30, Africa/Casablanca timezone (UTC+1)

    Returns:
        True if the market is open right now, False otherwise.
    """
    tz = pytz.timezone(MARKET_TIMEZONE)
    now = datetime.now(tz)

    # Weekends: Saturday=5, Sunday=6
    if now.weekday() > 4:
        return False

    # Build open/close datetimes for today in the market timezone
    market_open = now.replace(
        hour=MARKET_OPEN_HOUR,
        minute=MARKET_OPEN_MINUTE,
        second=0,
        microsecond=0,
    )
    market_close = now.replace(
        hour=MARKET_CLOSE_HOUR,
        minute=MARKET_CLOSE_MINUTE,
        second=0,
        microsecond=0,
    )

    return market_open <= now <= market_close


# ---------------------------------------------------------------------------
# Collection job
# ---------------------------------------------------------------------------


def collect_snapshot() -> None:
    """
    Fetch and persist one BVC market snapshot.

    This function is called by APScheduler in a background thread. It:
    1. Checks if the market is currently open — if not, logs and returns.
    2. Fetches fresh data via the HTTP client (bypassing the cache).
    3. Saves the snapshot to the SQLite database.

    Any exception during fetch or DB write is caught and logged so that a
    single failure does not kill the scheduler.
    """
    if not is_market_open():
        logger.info("Market closed, skipping snapshot")
        return

    logger.info("Collecting BVC snapshot...")
    try:
        # fetch_market_data is async; run it in this background thread's own loop.
        snapshot = asyncio.run(fetch_market_data(force_refresh=True))
        snapshot_id = save_snapshot(snapshot, DB_PATH)
        logger.info(
            "Snapshot #%d saved: %d tradeable stocks, API timestamp=%s",
            snapshot_id,
            len(snapshot.tradeable_stocks),
            snapshot.timestamp,
        )
    except Exception as exc:
        logger.error("Snapshot collection failed: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def start_scheduler() -> None:
    """
    Initialise the SQLite database and start the background scheduler.

    The job is registered with a cron trigger that fires every hour at minute=0.
    If the scheduler is already running this function is a no-op.

    Also triggers an immediate snapshot at startup if the market is currently open,
    so the database is not empty after a fresh server start during trading hours.
    """
    global _scheduler

    if _scheduler is not None and _scheduler.running:
        logger.debug("Scheduler already running — skipping start")
        return

    # Ensure the database and its parent directory exist before any job runs.
    init_db(DB_PATH)

    _scheduler = BackgroundScheduler(daemon=True)
    _scheduler.add_job(
        collect_snapshot,
        CronTrigger(minute=0),
        id="hourly_bvc_snapshot",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info(
        "Scheduler started — collecting snapshots every %dh at minute=0",
        SCHEDULER_INTERVAL_HOURS,
    )

    # Collect immediately at startup if the market is open right now.
    if is_market_open():
        logger.info("Market is open at startup — collecting initial snapshot now")
        collect_snapshot()
    else:
        logger.info("Market is closed at startup — waiting for next scheduled window")


def stop_scheduler() -> None:
    """
    Gracefully shut down the background scheduler.

    Safe to call even if the scheduler was never started.
    """
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
    _scheduler = None
