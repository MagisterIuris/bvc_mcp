"""
Utility helpers for the BVC MCP server.

Covers string formatting, number formatting, and JSON serialization helpers.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def format_mad(value: float | None, decimals: int = 2) -> str:
    """
    Format a MAD (Moroccan Dirham) monetary value as a human-readable string.

    Args:
        value: The numeric value to format.
        decimals: Number of decimal places (default 2).

    Returns:
        A formatted string like '32,684,179.20 MAD' or 'N/A' if value is None.
    """
    if value is None:
        return "N/A"
    return f"{value:,.{decimals}f} MAD"


def format_number(value: float | int | None, decimals: int = 0) -> str:
    """
    Format a number with thousands separators.

    Args:
        value: The numeric value to format.
        decimals: Number of decimal places (default 0).

    Returns:
        A formatted string like '48,497' or 'N/A' if value is None.
    """
    if value is None:
        return "N/A"
    return f"{value:,.{decimals}f}"


def format_variation(variation: float | None) -> str:
    """
    Format a percentage variation with sign prefix.

    Args:
        variation: The variation percentage.

    Returns:
        A string like '+3.66%' or '-4.74%' or 'N/A' if None.
    """
    if variation is None:
        return "N/A"
    sign = "+" if variation >= 0 else ""
    return f"{sign}{variation:.2f}%"


def to_json(obj: Any, indent: int = 2) -> str:
    """
    Serialize an object to a JSON string, handling datetime objects.

    Args:
        obj: The object to serialize.
        indent: JSON indentation level (default 2).

    Returns:
        A pretty-printed JSON string.
    """

    def default_serializer(o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

    return json.dumps(obj, indent=indent, default=default_serializer, ensure_ascii=False)


def normalize_symbol(symbol: str) -> str:
    """
    Normalize a stock symbol for comparison (uppercase, stripped).

    Args:
        symbol: Raw symbol string.

    Returns:
        Normalized symbol string.
    """
    return symbol.strip().upper()


def unix_ts_to_iso(unix_ts: int) -> str:
    """
    Convert a Unix timestamp (seconds) to an ISO 8601 string.

    Args:
        unix_ts: Unix timestamp in seconds.

    Returns:
        ISO 8601 datetime string, e.g. '2026-03-09T15:54:02'.
    """
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
