"""
BVC MCP Server — Casablanca Stock Exchange data via the Model Context Protocol.

Run directly:
    python -m bvc_mcp.server
    fastmcp run src/bvc_mcp/server.py

All tools share a single HTTP client with a 5-minute in-memory cache so that
rapid sequential calls from an LLM do not spam the BVC endpoint.

A background scheduler automatically collects and persists hourly snapshots
to the SQLite database while the market is open.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import re
import sqlite3
import sys
from datetime import datetime
from typing import Optional

import fastmcp
from fastmcp.apps.config import AppConfig
from fastmcp.server.dependencies import get_context, get_http_headers
from fastmcp.tools.base import Tool, ToolResult
from mcp.types import ToolAnnotations

from .analytics import (
    calculate_average_volume,
    calculate_bollinger_bands,
    calculate_correlation,
    calculate_momentum,
    calculate_moving_average,
    calculate_rsi,
    calculate_volatility,
    find_support_resistance,
)
from .auth import mask_key, resolve_owner_with_source
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse

from .client import fetch_market_data, get_cache_info
from .config import (
    BVC_UI_DOMAIN,
    DB_PATH,
    HOST,
    OPENAI_APPS_CHALLENGE_TOKEN,
    PORT,
    REQUIRE_WATCHLIST_API_KEY,
)
from .database import (
    get_all_symbols_recent_prices,
    get_avg_volumes,
    get_history as db_get_history,
    get_period_performance,
    get_price_in_range,
    get_snapshots_list as db_get_snapshots_list,
    get_symbols_ma_status,
)
from .models import MarketSnapshot
from .scheduler import start_scheduler
from .utils import format_mad, format_variation, normalize_symbol, to_json
from .watchlist import (
    add_to_watchlist as wl_add,
    create_watchlist as wl_create,
    delete_watchlist as wl_delete,
    get_watchlist as wl_get,
    get_watchlist_symbols,
    list_watchlists as wl_list,
    remove_from_watchlist as wl_remove,
)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FastMCP application
# ---------------------------------------------------------------------------

mcp = fastmcp.FastMCP(
    name="bvc-mcp-server",
    instructions=(
        "Provides real-time and historical market data for the Casablanca Stock Exchange (BVC). "
        "Live data is cached for 5 minutes. Historical data is stored in a local SQLite database "
        "and collected automatically every hour while the market is open. "
        "All monetary values are in MAD (Moroccan Dirham). "
        "Use get_market_status() first to check if the market is open or closed."
    ),
)

WIDGET_URI = "ui://widget/bvc-mcp-dashboard-v2.html"


def _widget_csp() -> dict[str, list[str]]:
    """Return the CSP allowlist for the Apps SDK widget."""
    connect_domains: list[str] = []
    resource_domains: list[str] = ["https://persistent.oaistatic.com"]
    if BVC_UI_DOMAIN:
        connect_domains.append(BVC_UI_DOMAIN)
        resource_domains.append(BVC_UI_DOMAIN)
    return {
        "connectDomains": connect_domains,
        "resourceDomains": resource_domains,
    }


def _widget_app_config() -> AppConfig:
    kwargs: dict[str, object] = {
        "resource_uri": WIDGET_URI,
        "prefers_border": True,
        "csp": _widget_csp(),
    }
    if BVC_UI_DOMAIN:
        kwargs["domain"] = BVC_UI_DOMAIN
    return AppConfig(**kwargs)


def _widget_resource_app_config() -> AppConfig:
    kwargs: dict[str, object] = {
        "prefers_border": True,
        "csp": _widget_csp(),
    }
    if BVC_UI_DOMAIN:
        kwargs["domain"] = BVC_UI_DOMAIN
    return AppConfig(**kwargs)


MODEL_WIDGET_APP = _widget_app_config()
WIDGET_RESOURCE_APP = _widget_resource_app_config()
WIDGET_TOOL_META = {
    "openai/outputTemplate": WIDGET_URI,
    "openai/toolInvocation/invoking": "Loading BVC MCP workspace…",
    "openai/toolInvocation/invoked": "BVC MCP workspace ready.",
}
WIDGET_RESOURCE_META = {
    "ui": {
        "prefersBorder": True,
        "csp": _widget_csp(),
    },
    "openai/widgetDescription": (
        "Interactive BVC MCP dashboard for Casablanca Stock Exchange data, "
        "screeners, indicators, and watchlists."
    ),
    "openai/widgetPrefersBorder": True,
}
if BVC_UI_DOMAIN:
    WIDGET_RESOURCE_META["ui"]["domain"] = BVC_UI_DOMAIN
    WIDGET_RESOURCE_META["openai/widgetDomain"] = BVC_UI_DOMAIN


def _tool_annotations(
    *,
    read_only: bool,
    destructive: bool,
    open_world: bool,
    idempotent: bool,
) -> ToolAnnotations:
    """Return standard MCP tool annotations for marketplace review."""
    return ToolAnnotations(
        readOnlyHint=read_only,
        destructiveHint=destructive,
        openWorldHint=open_world,
        idempotentHint=idempotent,
    )


READ_ONLY_LIVE = _tool_annotations(
    read_only=True,
    destructive=False,
    open_world=True,
    idempotent=True,
)
READ_ONLY_LOCAL = _tool_annotations(
    read_only=True,
    destructive=False,
    open_world=False,
    idempotent=True,
)
WRITE_VALIDATED = _tool_annotations(
    read_only=False,
    destructive=False,
    open_world=True,
    idempotent=False,
)
WRITE_LOCAL = _tool_annotations(
    read_only=False,
    destructive=False,
    open_world=False,
    idempotent=False,
)
DELETE_LOCAL = _tool_annotations(
    read_only=False,
    destructive=True,
    open_world=False,
    idempotent=False,
)

# ---------------------------------------------------------------------------
# Custom HTTP route — /health (used by Railway health-checks and smoke tests)
# ---------------------------------------------------------------------------


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> JSONResponse:
    """Return a simple health-check payload for load-balancers and monitoring."""
    import time

    return JSONResponse({
        "status": "ok",
        "timestamp": time.time(),
        "cache": get_cache_info(),
    })


@mcp.custom_route("/.well-known/openai-apps-challenge", methods=["GET"])
async def openai_apps_challenge(request: Request):
    """Serve the OpenAI Apps domain-verification token when configured."""
    if not OPENAI_APPS_CHALLENGE_TOKEN:
        return PlainTextResponse("OpenAI Apps challenge token not configured.", status_code=404)
    return PlainTextResponse(OPENAI_APPS_CHALLENGE_TOKEN)


def _dashboard_data_from_snapshot(snapshot: MarketSnapshot) -> dict[str, object]:
    """Build a compact bootstrap payload for the interactive BVC MCP widget."""
    tradeable = snapshot.tradeable_stocks
    gainers = [s for s in tradeable if s.variation is not None and s.variation > 0]
    losers = [s for s in tradeable if s.variation is not None and s.variation < 0]
    most_active = max(
        (s for s in tradeable if s.volume_mad is not None),
        key=lambda s: s.volume_mad or 0,
        default=None,
    )
    spotlight = next((s for s in tradeable if s.symbol == "ATW"), None) or (tradeable[0] if tradeable else None)
    total_volume = format_mad(sum(s.volume_mad for s in tradeable if s.volume_mad is not None))
    stock_map = {s.symbol: s for s in snapshot.stocks}

    watchlist_rows: list[dict[str, str]] = []
    for symbol in ["ATW", "BCP", "IAM"]:
        stock = stock_map.get(symbol)
        if stock is None:
            continue
        watchlist_rows.append(
            {
                "symbol": stock.symbol,
                "name": stock.name,
                "price": f"{stock.price} MAD" if stock.price is not None else "N/A",
                "variation": stock.variation_pct_display or "N/A",
            }
        )

    return {
        "kind": "dashboard_bootstrap",
        "status": snapshot.market_state,
        "timestamp": snapshot.timestamp_french,
        "totalVolume": total_volume,
        "breadth": {
            "gainers": len(gainers),
            "losers": len(losers),
            "label": f"{len(gainers)} up / {len(losers)} down",
        },
        "leaders": {
            "gainer": {
                "symbol": gainers[0].symbol if gainers else "N/A",
                "name": gainers[0].name if gainers else "No gainers",
                "variation": gainers[0].variation_pct_display if gainers else "N/A",
            },
            "loser": {
                "symbol": losers[0].symbol if losers else "N/A",
                "name": losers[0].name if losers else "No losers",
                "variation": losers[0].variation_pct_display if losers else "N/A",
            },
            "volume": {
                "symbol": most_active.symbol if most_active else "N/A",
                "name": most_active.name if most_active else "No volume data",
                "value": format_mad(most_active.volume_mad) if most_active else "N/A",
            },
        },
        "spotlight": {
            "symbol": spotlight.symbol if spotlight else "ATW",
            "name": spotlight.name if spotlight else "Attijariwafa Bank",
            "price": f"{spotlight.price}" if spotlight and spotlight.price is not None else "N/A",
            "variation": spotlight.variation_pct_display if spotlight else "N/A",
            "volume": format_mad(spotlight.volume_mad) if spotlight else "N/A",
            "low": f"{spotlight.low}" if spotlight and spotlight.low is not None else "N/A",
            "high": f"{spotlight.high}" if spotlight and spotlight.high is not None else "N/A",
        },
        "watchlist": {
            "name": "Core Morocco",
            "rows": watchlist_rows,
        },
    }


def _dashboard_fallback_data() -> dict[str, object]:
    """Fallback widget bootstrap when live data is unavailable."""
    return {
        "kind": "dashboard_bootstrap",
        "status": "Data unavailable",
        "timestamp": "Fallback snapshot",
        "totalVolume": "N/A",
        "breadth": {"gainers": 0, "losers": 0, "label": "N/A"},
        "leaders": {
            "gainer": {"symbol": "ATW", "name": "Attijariwafa Bank", "variation": "+1.12%"},
            "loser": {"symbol": "SAH", "name": "Sanlam Maroc", "variation": "-5.86%"},
            "volume": {"symbol": "MNG", "name": "Managem", "value": "111M MAD"},
        },
        "spotlight": {
            "symbol": "ATW",
            "name": "Attijariwafa Bank",
            "price": "711",
            "variation": "+1.12%",
            "volume": "23.7M MAD",
            "low": "703",
            "high": "711",
        },
        "watchlist": {
            "name": "Core Morocco",
            "rows": [
                {"symbol": "ATW", "name": "Attijariwafa Bank", "price": "711 MAD", "variation": "+1.12%"},
                {"symbol": "BCP", "name": "Banque Centrale Populaire", "price": "281 MAD", "variation": "+0.72%"},
                {"symbol": "IAM", "name": "Maroc Telecom", "price": "96 MAD", "variation": "+0.63%"},
            ],
        },
    }


_UI_DIR = Path(__file__).with_name("ui")


def _read_widget_asset(name: str) -> str:
    """Read a widget asset from the local UI bundle directory."""
    return (_UI_DIR / name).read_text(encoding="utf-8")


def _bvc_mcp_widget_app_html() -> str:
    """Return the Apps SDK HTML resource rendered inside ChatGPT."""
    html = _read_widget_asset("widget.html")
    css = _read_widget_asset("widget.css")
    js = _read_widget_asset("widget.js")
    return html.replace("__WIDGET_CSS__", css).replace("__WIDGET_JS__", js)


@mcp.resource(
    WIDGET_URI,
    name="BVC MCP Dashboard UI",
    title="BVC MCP Dashboard UI",
    description="Interactive BVC MCP dashboard UI for the BVC MCP server.",
    mime_type="text/html;profile=mcp-app",
    app=WIDGET_RESOURCE_APP,
    meta=WIDGET_RESOURCE_META,
)
def bvc_mcp_dashboard_widget() -> str:
    """Serve the interactive Apps SDK widget resource."""
    return _bvc_mcp_widget_app_html()


@mcp.tool(
    annotations=READ_ONLY_LIVE,
    app=MODEL_WIDGET_APP,
    meta=WIDGET_TOOL_META,
)
async def open_dashboard() -> ToolResult:
    """Open the interactive BVC MCP dashboard UI for BVC market workflows."""
    try:
        snapshot = await fetch_market_data()
        payload = _dashboard_data_from_snapshot(snapshot)
    except RuntimeError:
        payload = _dashboard_fallback_data()

    return ToolResult(
        content="Opened the BVC MCP dashboard.",
        structured_content=payload,
    )


@mcp.tool(annotations=READ_ONLY_LIVE)
async def get_market_status() -> str:
    """
    Return the current market status of the Casablanca Stock Exchange.

    Includes whether the market is open or closed, the timestamp of the last
    available data, and basic statistics about the number of tradeable stocks.

    Returns:
        A JSON string with keys: status, timestamp, timestamp_french,
        last_modified_utc, tradeable_count, total_count, cache_info.
    """
    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return json.dumps({"error": str(exc)})

    tradeable = snapshot.tradeable_stocks
    result = {
        "status": snapshot.market_state,
        "is_open": snapshot.market_state == "Market Open",
        "timestamp": snapshot.timestamp,
        "timestamp_french": snapshot.timestamp_french,
        "last_modified_unix": snapshot.last_modified,
        "tradeable_count": len(tradeable),
        "total_count": len(snapshot.stocks),
        "cache_info": get_cache_info(),
    }
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 2 — All stocks
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def get_all_stocks(include_untradeable: bool = False) -> str:
    """
    Return all stocks listed on the Casablanca Stock Exchange for the latest session.

    Args:
        include_untradeable: If False (default), only return stocks with a valid
            price (traded today). If True, include suspended or non-traded stocks.

    Returns:
        A JSON string containing a list of stock objects with full market data.
    """
    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return json.dumps({"error": str(exc)})

    stocks = snapshot.stocks if include_untradeable else snapshot.tradeable_stocks
    result = {
        "session_timestamp": snapshot.timestamp,
        "count": len(stocks),
        "stocks": [s.to_dict() for s in stocks],
    }
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 3 — Single stock detail
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def get_stock(symbol: str) -> str:
    """
    Return full market data for a single stock identified by its BVC ticker symbol.

    Args:
        symbol: The stock ticker symbol (e.g. 'ATW', 'IAM', 'BCP'). Case-insensitive.

    Returns:
        A JSON string with the stock's complete market data, or an error message
        if the symbol is not found.
    """
    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return json.dumps({"error": str(exc)})

    target = normalize_symbol(symbol)
    for stock in snapshot.stocks:
        if normalize_symbol(stock.symbol) == target:
            result = {
                "session_timestamp": snapshot.timestamp,
                "stock": stock.to_dict(),
            }
            return to_json(result)

    # Symbol not found — provide a helpful error with suggestions
    available = sorted([s.symbol for s in snapshot.stocks])
    return json.dumps(
        {
            "error": f"Symbol '{symbol}' not found on the BVC.",
            "hint": "Use search_stocks() to find valid symbols.",
            "total_symbols_available": len(available),
        }
    )


# ---------------------------------------------------------------------------
# Tool 4 — Top gainers
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def get_top_gainers(limit: int = 10) -> str:
    """
    Return the stocks with the highest positive variation for the current session.

    Only includes tradeable stocks (with a valid price). Sorted by variation
    descending (biggest gain first).

    Args:
        limit: Maximum number of stocks to return (default 10, max 50).

    Returns:
        A JSON string with a ranked list of top-gaining stocks.
    """
    limit = min(max(1, limit), 50)
    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return json.dumps({"error": str(exc)})

    gainers = [
        s for s in snapshot.tradeable_stocks
        if s.variation is not None and s.variation > 0
    ]
    gainers.sort(key=lambda s: s.variation, reverse=True)  # type: ignore[arg-type]
    gainers = gainers[:limit]

    result = {
        "result_type": "top_gainers",
        "session_timestamp": snapshot.timestamp,
        "count": len(gainers),
        "gainers": [
            {
                "rank": i + 1,
                "symbol": s.symbol,
                "name": s.name,
                "price": s.price,
                "variation": s.variation,
                "variation_display": s.variation_pct_display,
                "volume_mad": s.volume_mad,
            }
            for i, s in enumerate(gainers)
        ],
    }
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 5 — Top losers
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def get_top_losers(limit: int = 10) -> str:
    """
    Return the stocks with the largest negative variation for the current session.

    Only includes tradeable stocks (with a valid price). Sorted by variation
    ascending (biggest loss first).

    Args:
        limit: Maximum number of stocks to return (default 10, max 50).

    Returns:
        A JSON string with a ranked list of top-losing stocks.
    """
    limit = min(max(1, limit), 50)
    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return json.dumps({"error": str(exc)})

    losers = [
        s for s in snapshot.tradeable_stocks
        if s.variation is not None and s.variation < 0
    ]
    losers.sort(key=lambda s: s.variation)  # type: ignore[arg-type]
    losers = losers[:limit]

    result = {
        "result_type": "top_losers",
        "session_timestamp": snapshot.timestamp,
        "count": len(losers),
        "losers": [
            {
                "rank": i + 1,
                "symbol": s.symbol,
                "name": s.name,
                "price": s.price,
                "variation": s.variation,
                "variation_display": s.variation_pct_display,
                "volume_mad": s.volume_mad,
            }
            for i, s in enumerate(losers)
        ],
    }
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 6 — Top volume
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def get_top_volume(limit: int = 10) -> str:
    """
    Return the stocks with the highest traded volume (in MAD) for the current session.

    Only includes tradeable stocks that have a valid volume figure. Sorted by
    volume descending.

    Args:
        limit: Maximum number of stocks to return (default 10, max 50).

    Returns:
        A JSON string with a ranked list of most-traded stocks by volume.
    """
    limit = min(max(1, limit), 50)
    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return json.dumps({"error": str(exc)})

    by_volume = [
        s for s in snapshot.tradeable_stocks
        if s.volume_mad is not None
    ]
    by_volume.sort(key=lambda s: s.volume_mad, reverse=True)  # type: ignore[arg-type]
    by_volume = by_volume[:limit]

    result = {
        "result_type": "top_volume",
        "session_timestamp": snapshot.timestamp,
        "count": len(by_volume),
        "top_volume": [
            {
                "rank": i + 1,
                "symbol": s.symbol,
                "name": s.name,
                "price": s.price,
                "variation_display": s.variation_pct_display,
                "volume_mad": s.volume_mad,
                "volume_mad_formatted": format_mad(s.volume_mad),
                "quantity_traded": s.quantity_traded,
            }
            for i, s in enumerate(by_volume)
        ],
    }
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 7 — Search stocks
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def search_stocks(query: str) -> str:
    """
    Search for stocks by ticker symbol or company name (case-insensitive).

    Searches both the Symbol field and the Libelle (full company name) field.
    Whitespace in the query is stripped before matching.

    Args:
        query: The search string (e.g. 'attijar', 'IAM', 'banque').

    Returns:
        A JSON string with matching stocks. Returns all stocks if query is empty.
    """
    # Validate and sanitize query parameter
    query = query.strip()
    if len(query) > 100:
        return json.dumps({"error": "Query parameter exceeds maximum length of 100 characters."})

    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return json.dumps({"error": str(exc)})

    q = query.lower()
    if not q:
        matches = snapshot.stocks
    else:
        matches = [
            s for s in snapshot.stocks
            if q in s.symbol.lower() or q in s.name.lower()
        ]

    result = {
        "result_type": "search_stocks",
        "session_timestamp": snapshot.timestamp,
        "query": query,
        "count": len(matches),
        "results": [s.to_dict() for s in matches],
    }
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 7b — Fuzzy stock search
# ---------------------------------------------------------------------------


def _fuzzy_score(query: str, symbol: str, libelle: str) -> int:
    """Return a relevance score 0-100 for (query, symbol, libelle). Case-insensitive."""
    q = query.lower()
    sym = symbol.lower()
    lib = libelle.lower()

    if q == sym:
        return 100
    if sym.startswith(q):
        return 90
    if q == lib:
        return 85
    if lib.startswith(q):
        return 75
    if q in sym:
        return 60
    if q in lib:
        return 50

    # Token matching: split libelle on spaces and hyphens
    tokens = re.split(r"[\s\-]+", lib)
    if any(t.startswith(q) for t in tokens if t):
        return 40

    # Character-by-character similarity ratio
    max_len = max(len(q), len(sym), len(lib))
    if max_len == 0:
        return 0
    common_sym = sum(c in sym for c in q)
    common_lib = sum(c in lib for c in q)
    ratio = max(common_sym, common_lib) / max_len
    return int(ratio * 35)


@mcp.tool(annotations=READ_ONLY_LIVE)
async def find_stock(query: str, limit: int = 5) -> str:
    """
    Fuzzy search for a BVC stock by symbol or company name.
    Returns the top matching stocks ranked by relevance score.

    Args:
        query: Search string (symbol like 'ATW' or name like 'attijariwafa' or partial like 'banq')
        limit: Maximum number of results to return (default 5, max 20)

    Examples:
        find_stock("attijar") → ATW (Attijariwafa Bank)
        find_stock("telecom") → IAM (Maroc Telecom)
        find_stock("BCP") → BCP (Banque Centrale Populaire)
        find_stock("banque") → ATW, BCP, CIH, BMCI...
    """
    query = query.strip()
    if len(query) > 100:
        return json.dumps({"error": "Query parameter exceeds maximum length of 100 characters."})

    limit = max(1, min(20, limit))

    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return json.dumps({"error": str(exc)})

    scored = []
    for stock in snapshot.stocks:
        score = _fuzzy_score(query, stock.symbol, stock.name)
        if score > 15:
            scored.append((score, stock))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:limit]

    results = [
        {
            "symbol": stock.symbol,
            "name": stock.name,
            "score": score,
            "price": stock.price,
            "variation": stock.variation,
            "volume": stock.volume_mad,
        }
        for score, stock in top
    ]

    return json.dumps({
        "result_type": "find_stock",
        "query": query,
        "count": len(results),
        "results": results,
    })


# ---------------------------------------------------------------------------
# Tool 8 — Market summary
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def get_market_summary() -> str:
    """
    Return a comprehensive statistical summary of the current trading session.

    Aggregates data across all tradeable stocks to produce:
    - Total and tradeable stock counts
    - Number of gainers, losers, and unchanged stocks
    - Total session volume in MAD
    - Top performer (biggest gain)
    - Worst performer (biggest loss)
    - Most active stock by volume

    Returns:
        A JSON string with session-wide market statistics.
    """
    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return json.dumps({"error": str(exc)})

    tradeable = snapshot.tradeable_stocks

    gainers = [s for s in tradeable if s.variation is not None and s.variation > 0]
    losers = [s for s in tradeable if s.variation is not None and s.variation < 0]
    unchanged = [s for s in tradeable if s.variation is not None and s.variation == 0]
    no_variation = [s for s in tradeable if s.variation is None]

    total_volume = sum(s.volume_mad for s in tradeable if s.volume_mad is not None)

    top_gainer = max(gainers, key=lambda s: s.variation, default=None)  # type: ignore[arg-type]
    top_loser = min(losers, key=lambda s: s.variation, default=None)  # type: ignore[arg-type]

    stocks_with_volume = [s for s in tradeable if s.volume_mad is not None]
    top_volume_stock = max(stocks_with_volume, key=lambda s: s.volume_mad, default=None)  # type: ignore[arg-type]

    def stock_summary(s) -> dict | None:
        if s is None:
            return None
        return {
            "symbol": s.symbol,
            "name": s.name,
            "price": s.price,
            "variation": s.variation,
            "variation_display": s.variation_pct_display,
            "volume_mad": s.volume_mad,
        }

    result = {
        "session_timestamp": snapshot.timestamp,
        "session_timestamp_french": snapshot.timestamp_french,
        "market_state": snapshot.market_state,
        "total_listed": len(snapshot.stocks),
        "tradeable": len(tradeable),
        "untradeable": len(snapshot.stocks) - len(tradeable),
        "gainers": len(gainers),
        "losers": len(losers),
        "unchanged": len(unchanged),
        "no_variation_data": len(no_variation),
        "total_volume_mad": total_volume,
        "total_volume_formatted": format_mad(total_volume),
        "top_gainer": stock_summary(top_gainer),
        "top_loser": stock_summary(top_loser),
        "top_volume": stock_summary(top_volume_stock),
    }
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 9 — Stock price history (from DB)
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_stock_history(symbol: str, limit: int = 30) -> str:
    """
    Return the price history of a single stock from the local SQLite database.

    Each data point corresponds to one hourly snapshot collected while the
    market was open. Results are ordered newest-first.

    Args:
        symbol: BVC ticker symbol (e.g. 'ATW', 'IAM'). Case-insensitive.
        limit:  Number of historical data points to return (default 30, max 200).

    Returns:
        A JSON string with keys: symbol, data_points, history.
        Returns an error message if the symbol has no history in the database.
    """
    limit = min(max(1, limit), 200)
    sym = normalize_symbol(symbol)

    try:
        rows = await _run_db_call(db_get_history, sym, limit, DB_PATH)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    if not rows:
        return json.dumps(
            {
                "error": f"No history found for symbol '{symbol}' in the database.",
                "hint": (
                    "Run 'python scripts/collect_now.py --force' to populate the DB, "
                    "or wait for the hourly scheduler to collect data during market hours."
                ),
            }
        )

    result = {
        "result_type": "stock_history",
        "symbol": sym,
        "limit": limit,
        "data_points": len(rows),
        "history": rows,
    }
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 10 — Snapshots list (from DB)
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_snapshots_list(limit: int = 10) -> str:
    """
    Return a list of the most recent market snapshots stored in the database.

    Each entry corresponds to one collection run. Use this to understand the
    database coverage and check when the last snapshot was collected.

    Args:
        limit: Number of snapshots to return (default 10, max 100).

    Returns:
        A JSON string with the snapshot list ordered newest-first.
        Returns an empty list if no snapshots have been collected yet.
    """
    limit = min(max(1, limit), 100)

    try:
        rows = await _run_db_call(db_get_snapshots_list, limit, DB_PATH)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    result = {
        "count": len(rows),
        "snapshots": rows,
    }
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 11 — Price evolution over a date range (from DB)
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_price_evolution(symbol: str, from_date: str, to_date: str) -> str:
    """
    Return the price evolution of a stock between two calendar dates.

    Calculates the total percentage change from the first to the last recorded
    price within the specified date range.

    Args:
        symbol:    BVC ticker symbol (e.g. 'ATW'). Case-insensitive.
        from_date: Start date, inclusive. Format: 'YYYY-MM-DD' (e.g. '2026-03-01').
        to_date:   End date, inclusive.   Format: 'YYYY-MM-DD' (e.g. '2026-03-09').

    Returns:
        A JSON string with: symbol, from, to, start_price, end_price,
        total_variation_pct, total_variation_display, data_points, and a
        chronological data array.
    """
    # Validate date format
    for label, date_str in (("from_date", from_date), ("to_date", to_date)):
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return json.dumps(
                {"error": f"Invalid {label} '{date_str}'. Expected format: YYYY-MM-DD."}
            )

    if from_date > to_date:
        return json.dumps({"error": "from_date must be earlier than or equal to to_date."})

    sym = normalize_symbol(symbol)
    try:
        rows = await _run_db_call(get_price_in_range, sym, from_date, to_date, DB_PATH)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    if not rows:
        return json.dumps(
            {
                "error": (
                    f"No data found for '{symbol}' between {from_date} and {to_date}."
                ),
                "hint": "Check get_snapshots_list() to see what date range is available.",
            }
        )

    start_price: Optional[float] = rows[0]["price"]
    end_price: Optional[float] = rows[-1]["price"]

    if start_price and end_price and start_price != 0:
        total_variation_pct: Optional[float] = round(
            ((end_price - start_price) / start_price) * 100, 2
        )
    else:
        total_variation_pct = None

    result = {
        "result_type": "price_evolution",
        "symbol": sym,
        "from": from_date,
        "to": to_date,
        "start_price": start_price,
        "end_price": end_price,
        "total_variation_pct": total_variation_pct,
        "total_variation_display": (
            format_variation(total_variation_pct)
            if total_variation_pct is not None
            else "N/A"
        ),
        "data_points": len(rows),
        "data": rows,
    }
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 12 — Volume history (from DB)
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_volume_history(symbol: str, limit: int = 30) -> str:
    """
    Return the trading volume history for a single stock from the database.

    Also computes the average volume over the returned period.

    Args:
        symbol: BVC ticker symbol (e.g. 'ATW', 'IAM'). Case-insensitive.
        limit:  Number of historical data points (default 30, max 200).

    Returns:
        A JSON string with: symbol, data_points, average_volume_mad,
        average_volume_formatted, and a history array with per-snapshot volumes.
    """
    limit = min(max(1, limit), 200)
    sym = normalize_symbol(symbol)

    try:
        rows = await _run_db_call(db_get_history, sym, limit, DB_PATH)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    if not rows:
        return json.dumps(
            {
                "error": f"No history found for symbol '{symbol}' in the database.",
                "hint": "Run 'python scripts/collect_now.py --force' to populate the DB.",
            }
        )

    volumes = [r["volume_mad"] for r in rows if r["volume_mad"] is not None]
    avg_volume: Optional[float] = sum(volumes) / len(volumes) if volumes else None

    result = {
        "symbol": sym,
        "data_points": len(rows),
        "average_volume_mad": round(avg_volume, 2) if avg_volume is not None else None,
        "average_volume_formatted": format_mad(avg_volume),
        "history": [
            {
                "fetched_at": r["fetched_at"],
                "volume_mad": r["volume_mad"],
                "volume_formatted": format_mad(r["volume_mad"]),
                "quantity_traded": r["quantity_traded"],
            }
            for r in rows
        ],
    }
    return to_json(result)


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


async def _run_db_call(fn, *args, **kwargs):
    """Run a blocking DB/watchlist function in a worker thread."""
    try:
        return await asyncio.to_thread(fn, *args, **kwargs)
    except sqlite3.Error as exc:
        logger.error("Database operation failed in %s", getattr(fn, "__name__", fn))
        raise RuntimeError("Database operation failed.") from exc


def _resolve_watchlist_owner(api_key: str) -> tuple[Optional[str], Optional[str]]:
    """
    Resolve the owner for watchlist tools, enforcing explicit identity when configured.

    Returns:
        (owner, error_json)
    """
    cleaned_key = api_key.strip()
    authorization = get_http_headers(include={"authorization"}).get("authorization", "")
    try:
        ctx = get_context()
        client_id = getattr(ctx, "client_id", "") or ""
    except RuntimeError:
        client_id = ""
    owner, source = resolve_owner_with_source(
        api_key=cleaned_key,
        authorization=authorization,
        client_id=client_id,
    )
    if REQUIRE_WATCHLIST_API_KEY and source == "default":
        return None, to_json({
            "error": "This watchlist operation requires caller identity in the current deployment.",
            "hint": "Provide an api_key or use a client that forwards a stable caller identity.",
        })
    return owner, None


async def _price_series(symbol: str, limit: int) -> tuple[list[dict], list[float]]:
    """
    Fetch history from DB and return (rows_asc, prices) ready for analytics.

    Rows are returned in chronological order (oldest first) with None-price
    entries already filtered out.

    Args:
        symbol: Normalised BVC ticker symbol.
        limit:  Maximum number of DB rows to fetch.

    Returns:
        Tuple of (rows_ascending, prices_list).
    """
    rows = await _run_db_call(db_get_history, symbol, limit, DB_PATH)
    rows_asc = [r for r in reversed(rows) if r["price"] is not None]
    prices = [r["price"] for r in rows_asc]
    return rows_asc, prices


_NO_HISTORY_HINT = (
    "Run 'python scripts/collect_now.py --force' to populate the database, "
    "or wait for the hourly scheduler during market hours."
)


# ---------------------------------------------------------------------------
# Tool 13 — Moving Average (SMA)
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_moving_average(symbol: str, period: int = 20, limit: int = 100) -> str:
    """
    Calculate the Simple Moving Average (SMA) for a stock from its price history.

    Fetches the last ``limit`` price records from the database and computes
    the SMA with the given window. Returns the data points where the SMA is
    available (i.e. the last len(history) - period + 1 points), along with a
    signal indicating whether the current price is above or below the average.

    Typical periods: 5 (short-term), 20 (medium), 50 (long-term).

    Args:
        symbol: BVC ticker symbol (e.g. 'ATW'). Case-insensitive.
        period: SMA window in number of snapshots (default 20).
        limit:  Number of historical data points to fetch (default 100, max 500).

    Returns:
        JSON with keys: symbol, period, current_price, current_sma, signal, data.
    """
    period = min(max(2, period), 200)
    limit = min(max(period * 2, limit), 500)
    sym = normalize_symbol(symbol)
    try:
        rows_asc, prices = await _price_series(sym, limit)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    if not prices:
        return to_json({"error": f"No history for '{symbol}'.", "hint": _NO_HISTORY_HINT})
    if len(prices) < period:
        return to_json({
            "error": f"Not enough data for MA{period}: need {period} points, have {len(prices)}.",
            "hint": _NO_HISTORY_HINT,
        })

    smas = calculate_moving_average(prices, period)
    data = [
        {"fetched_at": r["fetched_at"], "price": r["price"], "sma": sma}
        for r, sma in zip(rows_asc, smas)
        if sma is not None
    ]

    current_price = prices[-1]
    current_sma = smas[-1]
    signal = "above" if current_price > current_sma else "below"  # type: ignore[operator]

    return to_json({
        "symbol": sym,
        "period": period,
        "current_price": current_price,
        "current_sma": current_sma,
        "signal": f"Price is {signal} MA{period}",
        "data_points": len(data),
        "data": data,
    })


# ---------------------------------------------------------------------------
# Tool 14 — RSI
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_rsi(symbol: str, period: int = 14, limit: int = 100) -> str:
    """
    Calculate the Relative Strength Index (RSI) for a stock.

    Uses Wilder's smoothing method. Returns all data points where RSI is
    computable, plus the current RSI value and its interpretation.

    Interpretation thresholds:
    - RSI < 30 → oversold (potential buy signal)
    - RSI > 70 → overbought (potential sell signal)
    - Otherwise → neutral

    Args:
        symbol: BVC ticker symbol. Case-insensitive.
        period: RSI period in snapshots (default 14).
        limit:  Number of historical data points to fetch (default 100, max 500).

    Returns:
        JSON with keys: symbol, period, current_rsi, interpretation, data.
    """
    period = min(max(2, period), 200)
    limit = min(max(period * 3, limit), 500)
    sym = normalize_symbol(symbol)
    try:
        rows_asc, prices = await _price_series(sym, limit)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    if not prices:
        return to_json({"error": f"No history for '{symbol}'.", "hint": _NO_HISTORY_HINT})

    rsis = calculate_rsi(prices, period)
    data = [
        {"fetched_at": r["fetched_at"], "price": r["price"], "rsi": rsi}
        for r, rsi in zip(rows_asc, rsis)
        if rsi is not None
    ]

    current_rsi = next((v for v in reversed(rsis) if v is not None), None)

    if current_rsi is None:
        interpretation = "insufficient_data"
    elif current_rsi < 30:
        interpretation = "oversold"
    elif current_rsi > 70:
        interpretation = "overbought"
    else:
        interpretation = "neutral"

    return to_json({
        "result_type": "rsi",
        "ui_mode_preference": "fullscreen",
        "symbol": sym,
        "period": period,
        "current_rsi": current_rsi,
        "interpretation": interpretation,
        "data_points": len(data),
        "data": data,
    })


# ---------------------------------------------------------------------------
# Tool 15 — Bollinger Bands
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_bollinger_bands(symbol: str, period: int = 20, limit: int = 100) -> str:
    """
    Calculate Bollinger Bands (middle, upper, lower) for a stock.

    middle = SMA(period)
    upper  = middle + 2 × σ
    lower  = middle - 2 × σ

    Signals returned:
    - "above_upper": price above upper band → potential overbought / sell signal
    - "below_lower": price below lower band → potential oversold / buy signal
    - "inside_bands": price within the bands → no breakout signal

    Args:
        symbol: BVC ticker symbol. Case-insensitive.
        period: Bollinger window (default 20).
        limit:  Number of historical data points (default 100, max 500).

    Returns:
        JSON with keys: symbol, period, current_price, signal, data.
    """
    period = min(max(2, period), 200)
    limit = min(max(period * 2, limit), 500)
    sym = normalize_symbol(symbol)
    try:
        rows_asc, prices = await _price_series(sym, limit)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    if not prices:
        return to_json({"error": f"No history for '{symbol}'.", "hint": _NO_HISTORY_HINT})
    if len(prices) < period:
        return to_json({
            "error": f"Need {period} data points for Bollinger({period}), have {len(prices)}.",
            "hint": _NO_HISTORY_HINT,
        })

    bands = calculate_bollinger_bands(prices, period)
    data = [
        {
            "fetched_at": r["fetched_at"],
            "price": r["price"],
            "upper": b["upper"],
            "middle": b["middle"],
            "lower": b["lower"],
        }
        for r, b in zip(rows_asc, bands)
        if b is not None
    ]

    current_price = prices[-1]
    last_band = next((b for b in reversed(bands) if b is not None), None)

    if last_band is None:
        signal = "insufficient_data"
    elif current_price > last_band["upper"]:
        signal = "above_upper"
    elif current_price < last_band["lower"]:
        signal = "below_lower"
    else:
        signal = "inside_bands"

    return to_json({
        "symbol": sym,
        "period": period,
        "current_price": current_price,
        "current_bands": last_band,
        "signal": signal,
        "data_points": len(data),
        "data": data,
    })


# ---------------------------------------------------------------------------
# Tool 16 — Volatility
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_volatility(symbol: str, period: int = 30) -> str:
    """
    Calculate the annualised historical volatility for a stock and compare it
    to the market average across all BVC stocks with sufficient history.

    Volatility = std(log_returns) × sqrt(252), expressed as a percentage.

    The market average is computed from all symbols that have at least
    ``period`` data points in the database (single SQL query, no per-symbol
    round-trips).

    Args:
        symbol: BVC ticker symbol. Case-insensitive.
        period: Number of price points to use (default 30).

    Returns:
        JSON with keys: symbol, volatility_pct, annualized, vs_market_avg,
        market_avg_pct, data_points.
    """
    period = min(max(2, period), 200)
    sym = normalize_symbol(symbol)

    # Fetch price histories for all symbols in one query
    try:
        all_prices = await _run_db_call(get_all_symbols_recent_prices, period + 1, DB_PATH)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    sym_prices = all_prices.get(sym)
    if not sym_prices:
        return to_json({"error": f"No history for '{symbol}'.", "hint": _NO_HISTORY_HINT})

    vol = calculate_volatility(sym_prices, period)
    if vol is None:
        return to_json({
            "error": f"Insufficient data for volatility (need ≥2 prices, have {len(sym_prices)})."
        })

    # Market average volatility
    market_vols = [
        v for v in (
            calculate_volatility(p, period) for p in all_prices.values()
        )
        if v is not None
    ]
    market_avg: Optional[float] = (
        round(sum(market_vols) / len(market_vols), 4) if market_vols else None
    )

    if market_avg is None:
        vs_market = "unknown"
    elif vol > market_avg * 1.5:
        vs_market = "high"
    elif vol < market_avg * 0.5:
        vs_market = "low"
    else:
        vs_market = "average"

    return to_json({
        "symbol": sym,
        "volatility_pct": vol,
        "annualized": True,
        "period": period,
        "data_points": len(sym_prices),
        "vs_market_avg": vs_market,
        "market_avg_pct": market_avg,
        "market_sample_size": len(market_vols),
    })


# ---------------------------------------------------------------------------
# Tool 17 — Momentum
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_momentum(
    symbol: str, periods: Optional[list[int]] = None
) -> str:
    """
    Calculate price momentum for a stock over multiple lookback periods.

    momentum = (price_now - price_N_snapshots_ago) / price_N_snapshots_ago × 100

    Signals:
    - "bullish" if momentum > 0
    - "bearish" if momentum < 0
    - "neutral" if exactly 0

    Args:
        symbol:  BVC ticker symbol. Case-insensitive.
        periods: List of lookback periods in number of snapshots
                 (default [5, 10, 20]).

    Returns:
        JSON with keys: symbol, current_price, momentum (list of period results).
    """
    if periods is None:
        periods = [5, 10, 20]

    sym = normalize_symbol(symbol)
    max_period = max(periods)
    try:
        rows_asc, prices = await _price_series(sym, max_period + 5)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    if not prices:
        return to_json({"error": f"No history for '{symbol}'.", "hint": _NO_HISTORY_HINT})

    results = []
    for p in sorted(set(periods)):
        mom = calculate_momentum(prices, p)
        if mom is None:
            signal = "insufficient_data"
        elif mom > 0:
            signal = "bullish"
        elif mom < 0:
            signal = "bearish"
        else:
            signal = "neutral"
        results.append({"period": p, "pct": mom, "signal": signal})

    return to_json({
        "symbol": sym,
        "current_price": prices[-1] if prices else None,
        "momentum": results,
    })


# ---------------------------------------------------------------------------
# Tool 18 — Support & Resistance
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_support_resistance(symbol: str, limit: int = 50) -> str:
    """
    Identify support and resistance levels from the last ``limit`` price records.

    Support    = lowest price in the window.
    Resistance = highest price in the window.

    Also returns the current price and the percentage distance from the
    current price to each level.

    Args:
        symbol: BVC ticker symbol. Case-insensitive.
        limit:  Number of historical records to consider (default 50, max 200).

    Returns:
        JSON with keys: symbol, current_price, support, resistance,
        distance_to_support_pct, distance_to_resistance_pct.
    """
    limit = min(max(5, limit), 200)
    sym = normalize_symbol(symbol)
    try:
        rows_asc, prices = await _price_series(sym, limit)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    if not prices:
        return to_json({"error": f"No history for '{symbol}'.", "hint": _NO_HISTORY_HINT})

    levels = find_support_resistance(prices, window=limit)
    current = prices[-1]

    support = levels["support"]
    resistance = levels["resistance"]

    dist_support = (
        round((current - support) / support * 100, 2)
        if support and support != 0 else None
    )
    dist_resistance = (
        round((resistance - current) / current * 100, 2)
        if resistance and current != 0 else None
    )

    return to_json({
        "symbol": sym,
        "current_price": current,
        "support": support,
        "resistance": resistance,
        "distance_to_support_pct": dist_support,
        "distance_to_resistance_pct": dist_resistance,
        "window": limit,
        "data_points": len(prices),
    })


# ---------------------------------------------------------------------------
# Tool 19 — Correlation
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_correlation(symbol1: str, symbol2: str, period: int = 30) -> str:
    """
    Calculate the Pearson correlation between two BVC stocks over recent history.

    Both series are trimmed to the same length (the shorter of the two).

    Interpretation:
    - correlation > 0.7  → strong positive correlation
    - correlation < -0.7 → strong negative correlation
    - otherwise          → weak / no significant correlation

    Args:
        symbol1: First BVC ticker symbol.
        symbol2: Second BVC ticker symbol.
        period:  Number of most-recent data points to use (default 30).

    Returns:
        JSON with keys: symbol1, symbol2, correlation, interpretation,
        data_points.
    """
    period = min(max(2, period), 200)
    sym1 = normalize_symbol(symbol1)
    sym2 = normalize_symbol(symbol2)

    if sym1 == sym2:
        return to_json({
            "symbol1": sym1, "symbol2": sym2,
            "correlation": 1.0,
            "interpretation": "identical symbols",
        })

    try:
        _, prices1 = await _price_series(sym1, period + 5)
        _, prices2 = await _price_series(sym2, period + 5)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    if not prices1:
        return to_json({"error": f"No history for '{symbol1}'.", "hint": _NO_HISTORY_HINT})
    if not prices2:
        return to_json({"error": f"No history for '{symbol2}'.", "hint": _NO_HISTORY_HINT})

    # Align to the same length (use the last N points of each)
    n = min(len(prices1), len(prices2), period)
    p1, p2 = prices1[-n:], prices2[-n:]

    corr = calculate_correlation(p1, p2)

    if corr is None:
        interpretation = "insufficient_data"
    elif corr > 0.7:
        interpretation = "strong positive correlation"
    elif corr < -0.7:
        interpretation = "strong negative correlation"
    elif corr > 0.4:
        interpretation = "moderate positive correlation"
    elif corr < -0.4:
        interpretation = "moderate negative correlation"
    else:
        interpretation = "weak / no significant correlation"

    return to_json({
        "symbol1": sym1,
        "symbol2": sym2,
        "correlation": corr,
        "interpretation": interpretation,
        "data_points": n,
    })


# ---------------------------------------------------------------------------
# Tool 20 — Sector Performance
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def get_sector_performance() -> str:
    """
    Aggregate current session performance by market segment (CodeSegment).

    For each segment: stock count, number of gainers, percentage of gainers,
    average variation, and total volume.

    Common BVC segment codes:
    - "01" → Marché Principal
    - "03" → Marché Développement

    Returns:
        JSON with a list of segments, each with performance statistics.
    """
    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    segments: dict = {}
    for stock in snapshot.tradeable_stocks:
        seg = stock.segment_code or "unknown"
        if seg not in segments:
            segments[seg] = {
                "segment_code": seg,
                "stock_count": 0,
                "gainers": 0,
                "losers": 0,
                "unchanged": 0,
                "variations": [],
                "total_volume_mad": 0.0,
            }
        s = segments[seg]
        s["stock_count"] += 1
        if stock.variation is not None:
            if stock.variation > 0:
                s["gainers"] += 1
            elif stock.variation < 0:
                s["losers"] += 1
            else:
                s["unchanged"] += 1
            s["variations"].append(stock.variation)
        if stock.volume_mad:
            s["total_volume_mad"] += stock.volume_mad

    result_segments = []
    for seg in sorted(segments.values(), key=lambda x: -x["stock_count"]):
        variations = seg.pop("variations")
        seg["avg_variation"] = (
            round(sum(variations) / len(variations), 4) if variations else None
        )
        seg["pct_gainers"] = (
            round(seg["gainers"] / seg["stock_count"] * 100, 1)
            if seg["stock_count"] else None
        )
        seg["total_volume_formatted"] = format_mad(seg["total_volume_mad"])
        result_segments.append(seg)

    return to_json({
        "session_timestamp": snapshot.timestamp,
        "segments": result_segments,
    })


# ---------------------------------------------------------------------------
# Tool 21 — Market Breadth
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def get_market_breadth() -> str:
    """
    Calculate market breadth indicators for the current BVC session.

    Indicators:
    - Advance/Decline ratio (gainers / losers)
    - Volume of advancing stocks vs declining stocks
    - Percentage of tradeable stocks above their MA20 (from DB history)
    - Overall interpretation: "bullish", "bearish", or "neutral"

    The MA20 calculation uses a single SQL query across all symbols.

    Returns:
        JSON with breadth indicators and an overall market interpretation.
    """
    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    tradeable = snapshot.tradeable_stocks
    gainers = [s for s in tradeable if s.variation is not None and s.variation > 0]
    losers = [s for s in tradeable if s.variation is not None and s.variation < 0]

    adv_volume = sum(s.volume_mad for s in gainers if s.volume_mad)
    dec_volume = sum(s.volume_mad for s in losers if s.volume_mad)

    ad_ratio: Optional[float] = (
        round(len(gainers) / len(losers), 2) if losers else None
    )

    # % above MA20 from DB
    try:
        ma_status = await _run_db_call(get_symbols_ma_status, 20, DB_PATH)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    total_with_ma = len(ma_status)
    above_ma20 = sum(1 for row in ma_status if row["above_ma"])
    pct_above_ma20: Optional[float] = (
        round(above_ma20 / total_with_ma * 100, 1) if total_with_ma else None
    )

    # Overall interpretation
    bullish_signals = 0
    bearish_signals = 0
    if ad_ratio is not None:
        if ad_ratio > 1.5:
            bullish_signals += 1
        elif ad_ratio < 0.67:
            bearish_signals += 1
    if pct_above_ma20 is not None:
        if pct_above_ma20 > 60:
            bullish_signals += 1
        elif pct_above_ma20 < 40:
            bearish_signals += 1
    if adv_volume > dec_volume * 1.5:
        bullish_signals += 1
    elif dec_volume > adv_volume * 1.5:
        bearish_signals += 1

    if bullish_signals >= 2:
        interpretation = "bullish"
    elif bearish_signals >= 2:
        interpretation = "bearish"
    else:
        interpretation = "neutral"

    return to_json({
        "session_timestamp": snapshot.timestamp,
        "advance_count": len(gainers),
        "decline_count": len(losers),
        "unchanged_count": len(tradeable) - len(gainers) - len(losers),
        "advance_decline_ratio": ad_ratio,
        "advancing_volume_mad": round(adv_volume, 2),
        "declining_volume_mad": round(dec_volume, 2),
        "advancing_volume_formatted": format_mad(adv_volume),
        "declining_volume_formatted": format_mad(dec_volume),
        "pct_above_ma20": pct_above_ma20,
        "stocks_with_ma20_data": total_with_ma,
        "interpretation": interpretation,
    })


# ---------------------------------------------------------------------------
# Tool 22 — Top Performers (period)
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_top_performers_period(
    from_date: str, to_date: str, limit: int = 10
) -> str:
    """
    Return the best-performing stocks over a custom date range, based on
    the stored price history in the database.

    Performance is measured as the total percentage change from the first
    recorded price to the last recorded price within the range.

    Args:
        from_date: Start date inclusive, format 'YYYY-MM-DD'.
        to_date:   End date inclusive, format 'YYYY-MM-DD'.
        limit:     Number of top performers to return (default 10, max 50).

    Returns:
        JSON with a ranked list of top performers and their total variation.
    """
    limit = min(max(1, limit), 50)
    for label, d in (("from_date", from_date), ("to_date", to_date)):
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            return to_json({"error": f"Invalid {label} '{d}'. Expected YYYY-MM-DD."})
    if from_date > to_date:
        return to_json({"error": "from_date must be ≤ to_date."})

    try:
        rows = await _run_db_call(get_period_performance, from_date, to_date, DB_PATH)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    if not rows:
        return to_json({
            "error": f"No data found between {from_date} and {to_date}.",
            "hint": "Check get_snapshots_list() to see available date range.",
        })

    perf = []
    for r in rows:
        if r["start_price"] and r["end_price"] and r["start_price"] > 0:
            var = round((r["end_price"] - r["start_price"]) / r["start_price"] * 100, 2)
            perf.append({**r, "total_variation_pct": var,
                         "total_variation_display": format_variation(var)})

    perf.sort(key=lambda x: x["total_variation_pct"], reverse=True)
    top = perf[:limit]
    for i, item in enumerate(top):
        item["rank"] = i + 1

    return to_json({
        "from_date": from_date,
        "to_date": to_date,
        "total_symbols": len(perf),
        "count": len(top),
        "top_performers": top,
    })


# ---------------------------------------------------------------------------
# Tool 23 — Worst Performers (period)
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_worst_performers_period(
    from_date: str, to_date: str, limit: int = 10
) -> str:
    """
    Return the worst-performing stocks over a custom date range, based on
    the stored price history in the database.

    Args:
        from_date: Start date inclusive, format 'YYYY-MM-DD'.
        to_date:   End date inclusive, format 'YYYY-MM-DD'.
        limit:     Number of worst performers to return (default 10, max 50).

    Returns:
        JSON with a ranked list of worst performers and their total variation.
    """
    limit = min(max(1, limit), 50)
    for label, d in (("from_date", from_date), ("to_date", to_date)):
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            return to_json({"error": f"Invalid {label} '{d}'. Expected YYYY-MM-DD."})
    if from_date > to_date:
        return to_json({"error": "from_date must be ≤ to_date."})

    try:
        rows = await _run_db_call(get_period_performance, from_date, to_date, DB_PATH)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    if not rows:
        return to_json({
            "error": f"No data found between {from_date} and {to_date}.",
            "hint": "Check get_snapshots_list() to see available date range.",
        })

    perf = []
    for r in rows:
        if r["start_price"] and r["end_price"] and r["start_price"] > 0:
            var = round((r["end_price"] - r["start_price"]) / r["start_price"] * 100, 2)
            perf.append({**r, "total_variation_pct": var,
                         "total_variation_display": format_variation(var)})

    perf.sort(key=lambda x: x["total_variation_pct"])
    worst = perf[:limit]
    for i, item in enumerate(worst):
        item["rank"] = i + 1

    return to_json({
        "from_date": from_date,
        "to_date": to_date,
        "total_symbols": len(perf),
        "count": len(worst),
        "worst_performers": worst,
    })


# ---------------------------------------------------------------------------
# Tool 24 — Stock Screener
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def screen_stocks(
    min_variation: Optional[float] = None,
    max_variation: Optional[float] = None,
    min_volume_mad: Optional[float] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    only_gainers: bool = False,
    only_losers: bool = False,
) -> str:
    """
    Filter live stocks by multiple optional criteria and return matches.

    All parameters are optional — pass only the filters you need. Results
    are sorted by volume descending.

    Args:
        min_variation:  Minimum variation % (e.g. 2.0 for stocks up ≥ 2%).
        max_variation:  Maximum variation % (e.g. -1.0 for stocks down ≤ 1%).
        min_volume_mad: Minimum traded volume in MAD.
        min_price:      Minimum current price in MAD.
        max_price:      Maximum current price in MAD.
        only_gainers:   If True, only return stocks with variation > 0.
        only_losers:    If True, only return stocks with variation < 0.

    Returns:
        JSON with the list of matching stocks sorted by volume.
    """
    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    results = []
    for stock in snapshot.tradeable_stocks:
        if only_gainers and (stock.variation is None or stock.variation <= 0):
            continue
        if only_losers and (stock.variation is None or stock.variation >= 0):
            continue
        if min_variation is not None and (stock.variation is None or stock.variation < min_variation):
            continue
        if max_variation is not None and (stock.variation is None or stock.variation > max_variation):
            continue
        if min_volume_mad is not None and (stock.volume_mad is None or stock.volume_mad < min_volume_mad):
            continue
        if min_price is not None and (stock.price is None or stock.price < min_price):
            continue
        if max_price is not None and (stock.price is None or stock.price > max_price):
            continue
        results.append(stock)

    results.sort(key=lambda s: s.volume_mad or 0, reverse=True)

    return to_json({
        "session_timestamp": snapshot.timestamp,
        "filters": {
            "min_variation": min_variation,
            "max_variation": max_variation,
            "min_volume_mad": min_volume_mad,
            "min_price": min_price,
            "max_price": max_price,
            "only_gainers": only_gainers,
            "only_losers": only_losers,
        },
        "count": len(results),
        "stocks": [s.to_dict() for s in results],
    })


# ---------------------------------------------------------------------------
# Tool 25 — Unusual Volume
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def get_unusual_volume(
    threshold_multiplier: float = 2.0, min_history: int = 10
) -> str:
    """
    Detect stocks whose current session volume significantly exceeds their
    historical average volume.

    Compares each stock's live volume against its average from the last
    ``min_history`` snapshots stored in the database. Returns stocks where:

        current_volume ≥ threshold_multiplier × historical_average

    Args:
        threshold_multiplier: Volume ratio threshold (default 2.0 = 2× average).
        min_history:          Minimum number of historical snapshots for the avg
                              (default 10).

    Returns:
        JSON with detected stocks sorted by volume ratio descending.
    """
    threshold_multiplier = min(max(0.1, threshold_multiplier), 10.0)
    min_history = min(max(1, min_history), 200)
    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    try:
        avg_volumes = await _run_db_call(get_avg_volumes, DB_PATH, limit=min_history)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    unusual = []
    for stock in snapshot.tradeable_stocks:
        if stock.volume_mad is None or stock.symbol not in avg_volumes:
            continue
        avg = avg_volumes[stock.symbol]
        if avg and avg > 0 and stock.volume_mad >= threshold_multiplier * avg:
            unusual.append({
                "symbol": stock.symbol,
                "name": stock.name,
                "current_volume_mad": stock.volume_mad,
                "current_volume_formatted": format_mad(stock.volume_mad),
                "avg_volume_mad": round(avg, 2),
                "ratio": round(stock.volume_mad / avg, 2),
                "price": stock.price,
                "variation_display": stock.variation_pct_display,
            })

    unusual.sort(key=lambda x: x["ratio"], reverse=True)

    return to_json({
        "session_timestamp": snapshot.timestamp,
        "threshold_multiplier": threshold_multiplier,
        "min_history_snapshots": min_history,
        "count": len(unusual),
        "unusual_activity": unusual,
    })


# ---------------------------------------------------------------------------
# Tool 26 — Breakout Candidates
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def get_breakout_candidates(period: int = 20) -> str:
    """
    Find stocks that are currently trading above their MA(period) with a
    positive session variation — a simple technical breakout signal.

    Criteria:
    1. Current price > SMA(period) from historical DB data
    2. Current session variation > 0 (positive day)

    Results are sorted by the distance above the moving average (descending).

    Args:
        period: Moving average period in snapshots (default 20).

    Returns:
        JSON with candidate stocks and their distance above the MA.
    """
    period = min(max(2, period), 200)
    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    try:
        ma_status = {
            row["symbol"]: row
            for row in await _run_db_call(get_symbols_ma_status, period, DB_PATH)
        }
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    candidates = []
    for stock in snapshot.tradeable_stocks:
        if stock.variation is None or stock.variation <= 0:
            continue
        if stock.symbol not in ma_status:
            continue
        row = ma_status[stock.symbol]
        if row["above_ma"] and stock.price:
            dist = round((stock.price - row["ma_value"]) / row["ma_value"] * 100, 2)
            candidates.append({
                "symbol": stock.symbol,
                "name": stock.name,
                "price": stock.price,
                "ma": round(row["ma_value"], 4),
                "distance_above_ma_pct": dist,
                "variation_display": stock.variation_pct_display,
                "volume_mad": stock.volume_mad,
            })

    candidates.sort(key=lambda x: x["distance_above_ma_pct"], reverse=True)

    return to_json({
        "session_timestamp": snapshot.timestamp,
        "ma_period": period,
        "count": len(candidates),
        "breakout_candidates": candidates,
    })


# ---------------------------------------------------------------------------
# Tool 27 — Create Watchlist
# ---------------------------------------------------------------------------


@mcp.tool(annotations=WRITE_VALIDATED)
async def create_watchlist(name: str, symbols: str, api_key: str = "") -> str:
    """
    Create a new named watchlist with an initial set of stocks.

    Each symbol is validated against the live BVC market data before being
    added. Unknown symbols are rejected and reported in the response.

    Args:
        name:    Unique watchlist name (e.g. 'my_banks').
        symbols: Comma-separated BVC ticker symbols (e.g. 'ATW,IAM,BCP').
        api_key: Optional API key for user isolation. Omit for local/single-user use.

    Returns:
        JSON confirming creation with lists of accepted and rejected symbols.
    """
    owner, auth_error = _resolve_watchlist_owner(api_key)
    if auth_error is not None:
        return auth_error

    # Validate watchlist name: alphanumeric, hyphens, underscores, 1–50 chars
    name = name.strip()
    if not re.match(r'^[a-zA-Z0-9_-]{1,50}$', name):
        return to_json({
            "error": (
                "Invalid watchlist name. Use 1–50 characters: letters, digits, "
                "hyphens (-), or underscores (_) only."
            )
        })

    raw_symbols = [s.strip().upper() for s in symbols.split(",") if s.strip()]

    # Validate each symbol: 1–10 uppercase alphanumeric chars
    invalid_symbols = [s for s in raw_symbols if not re.match(r'^[A-Z0-9]{1,10}$', s)]
    if invalid_symbols:
        return to_json({
            "error": f"Invalid symbol(s): {invalid_symbols}. Symbols must be 1–10 uppercase alphanumeric characters."
        })

    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    live_symbols = {normalize_symbol(s.symbol) for s in snapshot.stocks}
    accepted = [s for s in raw_symbols if s in live_symbols]
    rejected = [s for s in raw_symbols if s not in live_symbols]

    if not accepted:
        return to_json({
            "error": "No valid symbols found. None of the provided symbols exist on the BVC.",
            "rejected": rejected,
        })

    try:
        result = await _run_db_call(wl_create, name, accepted, DB_PATH, owner=owner)
    except ValueError as exc:
        return to_json({"error": str(exc)})
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    result["rejected"] = rejected
    result["rejected_count"] = len(rejected)
    result["result_type"] = "watchlist_create"
    result["action"] = "created"
    result["requested_symbols"] = raw_symbols
    result["requested_count"] = len(raw_symbols)
    result["confirmed_symbols"] = accepted
    result["confirmed_count"] = len(accepted)
    result["message"] = (
        "Watchlist created successfully."
        if not rejected
        else "Watchlist created, but some requested symbols were rejected."
    )
    if rejected:
        result["hint"] = "Rejected symbols were not found in the current live market data."
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 28 — Get Watchlist (with live data)
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LIVE)
async def get_watchlist(name: str, api_key: str = "") -> str:
    """
    Return a watchlist with current live market data for each stock.

    Args:
        name:    Watchlist name.
        api_key: Optional API key for user isolation. Omit for local/single-user use.

    Returns:
        JSON with watchlist metadata and live price/variation/volume for each
        stock in the list.
    """
    owner, auth_error = _resolve_watchlist_owner(api_key)
    if auth_error is not None:
        return auth_error
    name = name.strip()
    if not re.match(r'^[a-zA-Z0-9_-]{1,50}$', name):
        return to_json({"error": "Invalid watchlist name format."})
    try:
        wl = await _run_db_call(wl_get, name, DB_PATH, owner=owner)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    if wl is None:
        return to_json({
            "error": f"Watchlist '{name}' not found.",
            "hint": "Use list_watchlists() to see existing watchlists.",
        })

    symbols = [s["symbol"] for s in wl["stocks"]]

    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    stock_map = {s.symbol: s for s in snapshot.stocks}
    live_stocks = []
    for sym in symbols:
        s = stock_map.get(sym)
        if s:
            live_stocks.append({
                "symbol": s.symbol,
                "name": s.name,
                "price": s.price,
                "variation": s.variation,
                "variation_display": s.variation_pct_display,
                "volume_mad": s.volume_mad,
                "is_tradeable": s.is_tradeable,
            })
        else:
            live_stocks.append({"symbol": sym, "error": "Symbol not found in live data"})

    return to_json({
        "result_type": "watchlist_detail",
        "name": wl["name"],
        "created_at": wl["created_at"],
        "count": len(symbols),
        "session_timestamp": snapshot.timestamp,
        "stocks": live_stocks,
    })


# ---------------------------------------------------------------------------
# Tool 29 — Watchlist Performance
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def get_watchlist_performance(
    name: str, from_date: str, to_date: str, api_key: str = ""
) -> str:
    """
    Return the price performance of each stock in a watchlist over a date range.

    Args:
        name:      Watchlist name.
        from_date: Start date, format 'YYYY-MM-DD'.
        to_date:   End date, format 'YYYY-MM-DD'.
        api_key:   Optional API key for user isolation. Omit for local/single-user use.

    Returns:
        JSON with individual stock performances, ranked by total variation,
        and the average portfolio variation.
    """
    owner, auth_error = _resolve_watchlist_owner(api_key)
    if auth_error is not None:
        return auth_error
    name = name.strip()
    if not re.match(r'^[a-zA-Z0-9_-]{1,50}$', name):
        return to_json({"error": "Invalid watchlist name format."})
    for label, d in (("from_date", from_date), ("to_date", to_date)):
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            return to_json({"error": f"Invalid {label} '{d}'. Expected YYYY-MM-DD."})

    try:
        symbols = await _run_db_call(get_watchlist_symbols, name, DB_PATH, owner=owner)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    if not symbols:
        return to_json({
            "error": f"Watchlist '{name}' not found or is empty.",
            "hint": "Use list_watchlists() to see existing watchlists.",
        })

    try:
        all_perf = await _run_db_call(get_period_performance, from_date, to_date, DB_PATH)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    perf_map = {row["symbol"]: row for row in all_perf}

    results = []
    for sym in symbols:
        if sym not in perf_map:
            results.append({"symbol": sym, "error": "No data in the selected date range."})
            continue
        r = perf_map[sym]
        var = None
        if r["start_price"] and r["end_price"] and r["start_price"] > 0:
            var = round((r["end_price"] - r["start_price"]) / r["start_price"] * 100, 2)
        results.append({
            "symbol": sym,
            "name": r["name"],
            "start_price": r["start_price"],
            "end_price": r["end_price"],
            "total_variation_pct": var,
            "total_variation_display": format_variation(var) if var is not None else "N/A",
            "start_at": r["start_at"],
            "end_at": r["end_at"],
        })

    valid_vars = [r["total_variation_pct"] for r in results if r.get("total_variation_pct") is not None]
    avg_var: Optional[float] = (
        round(sum(valid_vars) / len(valid_vars), 2) if valid_vars else None
    )
    results.sort(key=lambda x: x.get("total_variation_pct") or float("-inf"), reverse=True)
    for i, r in enumerate(results):
        r["rank"] = i + 1

    return to_json({
        "watchlist": name,
        "from_date": from_date,
        "to_date": to_date,
        "stock_count": len(symbols),
        "avg_portfolio_variation_pct": avg_var,
        "avg_portfolio_variation_display": format_variation(avg_var) if avg_var is not None else "N/A",
        "performance": results,
    })


# ---------------------------------------------------------------------------
# Tool 30 — List Watchlists
# ---------------------------------------------------------------------------


@mcp.tool(annotations=READ_ONLY_LOCAL)
async def list_watchlists(api_key: str = "") -> str:
    """
    Return all saved watchlists with their stock counts and creation dates.

    Args:
        api_key: Optional API key for user isolation. Omit for local/single-user use.

    Returns:
        JSON with the list of watchlists, ordered by creation date (newest first).
    """
    owner, auth_error = _resolve_watchlist_owner(api_key)
    if auth_error is not None:
        return auth_error
    try:
        watchlists = await _run_db_call(wl_list, DB_PATH, owner=owner)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    return to_json({
        "result_type": "watchlist_list",
        "count": len(watchlists),
        "watchlists": watchlists,
    })


# ---------------------------------------------------------------------------
# Tool 31 — Add to Watchlist
# ---------------------------------------------------------------------------


@mcp.tool(annotations=WRITE_VALIDATED)
async def add_to_watchlist(name: str, symbol: str, api_key: str = "") -> str:
    """
    Add a stock to an existing watchlist.

    The symbol is validated against the live BVC market data before being added.

    Args:
        name:    Watchlist name.
        symbol:  BVC ticker symbol (e.g. 'ATW'). Case-insensitive.
        api_key: Optional API key for user isolation. Omit for local/single-user use.

    Returns:
        JSON confirming success or explaining the error.
    """
    owner, auth_error = _resolve_watchlist_owner(api_key)
    if auth_error is not None:
        return auth_error
    name = name.strip()
    if not re.match(r'^[a-zA-Z0-9_-]{1,50}$', name):
        return to_json({"error": "Invalid watchlist name format."})
    sym = normalize_symbol(symbol)
    if not re.match(r'^[A-Z0-9]{1,10}$', sym):
        return to_json({"error": "Invalid symbol format. Use 1–10 uppercase alphanumeric characters."})

    try:
        snapshot: MarketSnapshot = await fetch_market_data()
    except RuntimeError as exc:
        return to_json({"error": str(exc)})

    live_symbols = {normalize_symbol(s.symbol) for s in snapshot.stocks}
    if sym not in live_symbols:
        return to_json({
            "error": f"Symbol '{sym}' not found on the BVC.",
            "hint": "Use search_stocks() to find valid symbols.",
        })

    try:
        result = await _run_db_call(wl_add, name, sym, DB_PATH, owner=owner)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    if result.get("success"):
        symbols_after = await _run_db_call(get_watchlist_symbols, name, DB_PATH, owner=owner)
        result["result_type"] = "watchlist_add"
        result["action"] = "added_symbol"
        result["confirmed_symbols"] = symbols_after
        result["confirmed_count"] = len(symbols_after)
        result["message"] = f"{sym} was added to '{name}'."
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 32 — Remove from Watchlist
# ---------------------------------------------------------------------------


@mcp.tool(annotations=DELETE_LOCAL)
async def remove_from_watchlist(name: str, symbol: str, api_key: str = "") -> str:
    """
    Remove a stock from a watchlist.

    Args:
        name:    Watchlist name.
        symbol:  BVC ticker symbol. Case-insensitive.
        api_key: Optional API key for user isolation. Omit for local/single-user use.

    Returns:
        JSON confirming success or explaining the error.
    """
    owner, auth_error = _resolve_watchlist_owner(api_key)
    if auth_error is not None:
        return auth_error
    name = name.strip()
    if not re.match(r'^[a-zA-Z0-9_-]{1,50}$', name):
        return to_json({"error": "Invalid watchlist name format."})
    sym = normalize_symbol(symbol)
    if not re.match(r'^[A-Z0-9]{1,10}$', sym):
        return to_json({"error": "Invalid symbol format. Use 1–10 uppercase alphanumeric characters."})
    try:
        result = await _run_db_call(wl_remove, name, sym, DB_PATH, owner=owner)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    if result.get("success"):
        symbols_after = await _run_db_call(get_watchlist_symbols, name, DB_PATH, owner=owner)
        result["result_type"] = "watchlist_remove"
        result["action"] = "removed_symbol"
        result["confirmed_symbols"] = symbols_after
        result["confirmed_count"] = len(symbols_after)
        result["removed_symbol"] = sym
        result["message"] = f"{sym} was removed from '{name}'."
    return to_json(result)


# ---------------------------------------------------------------------------
# Tool 33 — Delete Watchlist
# ---------------------------------------------------------------------------


@mcp.tool(annotations=DELETE_LOCAL)
async def delete_watchlist(name: str, api_key: str = "") -> str:
    """
    Permanently delete a watchlist and all its associated stocks.

    Args:
        name:    Watchlist name to delete.
        api_key: Optional API key for user isolation. Omit for local/single-user use.

    Returns:
        JSON confirming deletion or explaining the error.
    """
    owner, auth_error = _resolve_watchlist_owner(api_key)
    if auth_error is not None:
        return auth_error
    name = name.strip()
    if not re.match(r'^[a-zA-Z0-9_-]{1,50}$', name):
        return to_json({"error": "Invalid watchlist name format."})
    try:
        existing = await _run_db_call(wl_get, name, DB_PATH, owner=owner)
        result = await _run_db_call(wl_delete, name, DB_PATH, owner=owner)
    except RuntimeError as exc:
        return to_json({"error": str(exc)})
    if result.get("success"):
        result["result_type"] = "watchlist_delete"
        result["action"] = "deleted_watchlist"
        result["deleted_count"] = existing["count"] if existing else 0
        result["message"] = f"Watchlist '{name}' was deleted."
    return to_json(result)


# ---------------------------------------------------------------------------
# Apps SDK widget wiring
# ---------------------------------------------------------------------------


def _attach_widget_metadata_to_registered_tools() -> None:
    """Expose the BVC MCP widget on all model-facing tools.

    This keeps the existing tool surface intact while allowing ChatGPT to mount
    the shared dashboard UI for inline, PiP, or fullscreen rendering.
    """
    for component in mcp._local_provider._components.values():
        if not isinstance(component, Tool):
            continue

        meta = dict(component.meta or {})
        ui_meta = dict(meta.get("ui") or {})
        ui_meta.setdefault("resourceUri", WIDGET_URI)
        ui_meta.setdefault("prefersBorder", True)
        if BVC_UI_DOMAIN:
            ui_meta.setdefault("domain", BVC_UI_DOMAIN)
        meta["ui"] = ui_meta
        meta.setdefault("openai/outputTemplate", WIDGET_URI)
        component.meta = meta


_attach_widget_metadata_to_registered_tools()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """
    Start the BVC MCP server.

    When the PORT environment variable is set (e.g. on Railway), the server
    runs in HTTP/SSE mode on the specified host and port.  Without PORT the
    server falls back to stdio mode for local Claude Desktop usage.
    """
    import os

    logger.info("Starting BVC MCP Server")
    start_scheduler()

    if os.environ.get("PORT"):
        # HTTP/SSE mode — used in production (Railway, Docker, etc.)
        logger.info("Running in HTTP/SSE mode on %s:%s", HOST, PORT)
        mcp.run(transport="sse", host=HOST, port=PORT)
    else:
        # stdio mode — used locally with Claude Desktop / Cursor
        logger.info("Running in stdio mode")
        mcp.run()


if __name__ == "__main__":
    main()
