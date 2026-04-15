#!/usr/bin/env python3
"""
Standalone script to manually trigger an immediate BVC market snapshot.

Useful for:
- Populating the database outside normal trading hours (with --force)
- Testing the data pipeline end-to-end
- Backfilling a gap after a server downtime

Usage:
    python scripts/collect_now.py           # respects market-hours check
    python scripts/collect_now.py --force   # ignores market-hours check
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup — allows running the script directly without installing the package.
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT / "src"))

# ---------------------------------------------------------------------------
# Imports (after path setup)
# ---------------------------------------------------------------------------

from bvc_mcp.client import fetch_market_data
from bvc_mcp.config import DB_PATH
from bvc_mcp.database import init_db, save_snapshot
from bvc_mcp.scheduler import is_market_open

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


async def collect(force: bool = False) -> None:
    """
    Fetch a fresh BVC snapshot and persist it to the database.

    Args:
        force: If True, skip the market-hours check and collect regardless.
    """
    if not force and not is_market_open():
        logger.warning(
            "The BVC market is currently closed. "
            "Use --force to collect a snapshot outside trading hours."
        )
        sys.exit(0)

    logger.info("Initialising database at %s", DB_PATH)
    init_db(DB_PATH)

    logger.info("Fetching latest BVC market data...")
    snapshot = await fetch_market_data(force_refresh=True)

    logger.info(
        "Received snapshot: %d total stocks, %d tradeable, API timestamp=%s",
        len(snapshot.stocks),
        len(snapshot.tradeable_stocks),
        snapshot.timestamp,
    )

    snapshot_id = save_snapshot(snapshot, DB_PATH)
    logger.info(
        "Snapshot #%d successfully saved to %s",
        snapshot_id,
        DB_PATH,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Parse CLI arguments and run the collection coroutine."""
    parser = argparse.ArgumentParser(
        description="Manually collect a BVC market data snapshot into the SQLite DB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore the market-hours check and collect regardless of schedule.",
    )
    args = parser.parse_args()
    asyncio.run(collect(force=args.force))


if __name__ == "__main__":
    main()
