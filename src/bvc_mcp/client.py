"""
HTTP client for the Casablanca Stock Exchange (BVC) data endpoint.

Handles fetching, parsing, and in-memory caching of market data.
The cache is shared across all MCP tool calls to avoid redundant HTTP requests.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx

from .models import MarketSnapshot, Stock

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

BVC_ENDPOINT = "https://www.casablancabourse.com/functions/get_latest_data.php"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Cache time-to-live in seconds (5 minutes by default)
CACHE_TTL_SECONDS: int = 300

# HTTP request timeout in seconds
REQUEST_TIMEOUT_SECONDS: float = 15.0


# ---------------------------------------------------------------------------
# Cache container
# ---------------------------------------------------------------------------


@dataclass
class _CacheEntry:
    """Internal container for a cached market snapshot."""

    snapshot: MarketSnapshot
    fetched_at: float = field(default_factory=time.monotonic)

    def is_valid(self, ttl: int = CACHE_TTL_SECONDS) -> bool:
        """Return True if the cache entry is still within its TTL."""
        return (time.monotonic() - self.fetched_at) < ttl


# Module-level cache — shared across all calls within the same process.
_cache: Optional[_CacheEntry] = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def fetch_market_data(force_refresh: bool = False) -> MarketSnapshot:
    """
    Fetch the latest BVC market data, using the in-memory cache when valid.

    The BVC endpoint always returns the most recent trading session data.
    The `t` query parameter is a cache-buster (milliseconds timestamp) and
    does NOT filter by date.

    Args:
        force_refresh: If True, bypass the cache and fetch fresh data.

    Returns:
        A :class:`~bvc_mcp.models.MarketSnapshot` with all listed stocks.

    Raises:
        RuntimeError: If the HTTP request fails and no cached data is available.
    """
    global _cache

    # Return cached data if still valid
    if not force_refresh and _cache is not None and _cache.is_valid():
        logger.debug("Cache hit — returning cached snapshot (age: %.1fs)", _cache_age())
        return _cache.snapshot

    logger.info("Fetching fresh market data from BVC endpoint")
    try:
        snapshot = await _do_fetch()
        _cache = _CacheEntry(snapshot=snapshot)
        logger.info(
            "Market data refreshed: %d stocks, timestamp=%s",
            len(snapshot.stocks),
            snapshot.timestamp,
        )
        return snapshot

    except Exception as exc:
        logger.error("Failed to fetch BVC data: %s", exc)
        if _cache is not None:
            age = _cache_age()
            logger.warning("Serving stale cache (age: %.0fs) due to fetch failure", age)
            return _cache.snapshot
        raise RuntimeError(
            f"BVC endpoint unavailable and no cached data exists. Error: {exc}"
        ) from exc


def get_cache_info() -> dict:
    """
    Return metadata about the current cache state.

    Returns:
        A dictionary with keys: ``has_cache``, ``age_seconds``, ``last_modified``,
        ``stock_count``, ``timestamp``.
    """
    if _cache is None:
        return {"has_cache": False}
    return {
        "has_cache": True,
        "age_seconds": round(_cache_age(), 1),
        "ttl_seconds": CACHE_TTL_SECONDS,
        "is_valid": _cache.is_valid(),
        "last_modified": _cache.snapshot.last_modified,
        "stock_count": len(_cache.snapshot.stocks),
        "timestamp": _cache.snapshot.timestamp,
    }


def invalidate_cache() -> None:
    """Manually invalidate the in-memory cache, forcing a fresh fetch on the next call."""
    global _cache
    _cache = None
    logger.info("Cache invalidated manually")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _do_fetch() -> MarketSnapshot:
    """
    Perform the actual HTTP request to the BVC endpoint and parse the response.

    Returns:
        A parsed :class:`~bvc_mcp.models.MarketSnapshot`.

    Raises:
        httpx.HTTPError: On network or HTTP errors.
        ValueError: If the response JSON is malformed or reports failure.
    """
    timestamp_ms = int(time.time() * 1000)
    url = f"{BVC_ENDPOINT}?t={timestamp_ms}"
    headers = {"User-Agent": USER_AGENT}

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
        logger.debug("GET %s", url)
        response = await client.get(url, headers=headers)
        response.raise_for_status()

    raw = response.json()

    if not raw.get("success", False):
        raise ValueError(f"BVC API returned success=false: {raw}")

    # Parse each stock entry in the `data` array
    stocks: list[Stock] = []
    for item in raw.get("data", []):
        try:
            stocks.append(Stock.model_validate(item))
        except Exception as exc:
            logger.warning("Skipping malformed stock entry %s: %s", item.get("Symbol"), exc)

    snapshot = MarketSnapshot(
        success=raw["success"],
        lastModified=raw["lastModified"],
        timestamp=raw["timestamp"],
        timestampFrench=raw["timestampFrench"],
        stocks=stocks,
    )
    return snapshot


def _cache_age() -> float:
    """Return the age of the current cache entry in seconds, or 0 if no cache."""
    if _cache is None:
        return 0.0
    return time.monotonic() - _cache.fetched_at
