"""
Central configuration for the BVC MCP server.

All tuneable constants live here so that scheduler.py, database.py,
and server.py can share a single source of truth without circular imports.
"""

from __future__ import annotations

import os
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    """Parse a boolean environment variable with a safe fallback."""
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

# Absolute path to the SQLite database file.
# Can be overridden via BVC_DB_PATH environment variable (e.g. for Railway/Docker).
# Default resolves to: <project_root>/data/bvc_history.db
# __file__ = .../bvc-mcp-server/src/bvc_mcp/config.py
#   .parent        → .../src/bvc_mcp/
#   .parent.parent → .../src/
#   .parent×3      → .../bvc-mcp-server/
_DEFAULT_DB_PATH: str = str(
    Path(__file__).parent.parent.parent / "data" / "bvc_history.db"
)
DB_PATH: str = os.environ.get("BVC_DB_PATH", _DEFAULT_DB_PATH)

# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

# Bind address for the HTTP/SSE server (overridable via HOST env var).
HOST: str = os.environ.get("HOST", "0.0.0.0")

# TCP port for the HTTP/SSE server (overridable via PORT env var).
# Railway injects PORT automatically; local dev defaults to 8000.
PORT: int = int(os.environ.get("PORT", "8000"))

# ---------------------------------------------------------------------------
# HTTP cache
# ---------------------------------------------------------------------------

# Time-to-live for the in-memory market data cache (seconds).
CACHE_TTL_SECONDS: int = 300

# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

# How many hours between automatic snapshots (job fires at minute=0 each time).
SCHEDULER_INTERVAL_HOURS: int = 1

# ---------------------------------------------------------------------------
# BVC market hours (Africa/Casablanca = UTC+1, no DST observed in practice)
# ---------------------------------------------------------------------------

MARKET_OPEN_HOUR: int = 9
MARKET_OPEN_MINUTE: int = 0
MARKET_CLOSE_HOUR: int = 15
MARKET_CLOSE_MINUTE: int = 30
MARKET_TIMEZONE: str = "Africa/Casablanca"

# ---------------------------------------------------------------------------
# MCPize / multi-user config
# ---------------------------------------------------------------------------

# Owner identifier used when no api_key or env var is provided.
# Single-user / Claude Desktop deployments always use this value.
DEFAULT_OWNER: str = "default"

# Length (in hex chars) of the sha256-derived owner identifier.
OWNER_ID_LENGTH: int = 16

# Require explicit caller identity for watchlist tools only when explicitly
# enabled. Some MCP clients do not forward a stable user identity across calls,
# so permissive mode remains the safest default for marketplace compatibility.
REQUIRE_WATCHLIST_API_KEY: bool = _env_bool(
    "BVC_REQUIRE_WATCHLIST_API_KEY",
    default=False,
)

# OpenAI Apps domain-verification token served at:
# /.well-known/openai-apps-challenge
OPENAI_APPS_CHALLENGE_TOKEN: str = os.environ.get(
    "OPENAI_APPS_CHALLENGE_TOKEN",
    "",
).strip()

# Dedicated origin advertised to ChatGPT for the widget iframe.
# Required for app submission. Example:
#   https://bvc-mcp-production.up.railway.app
BVC_UI_DOMAIN: str = os.environ.get(
    "BVC_UI_DOMAIN",
    "",
).strip()
