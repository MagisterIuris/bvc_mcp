# BVC MCP Server

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that exposes **real-time and historical** market data from the **Casablanca Stock Exchange (Bourse de Valeurs de Casablanca — BVC)**.

Connect it to Claude Desktop, Cursor, LM Studio, or any MCP-compatible client and ask natural-language questions about Moroccan stocks.

---

## Data Source

- **Endpoint:** `https://www.casablancabourse.com/functions/get_latest_data.php?t={timestamp_ms}`
- **Authentication:** None required
- **Content:** Always returns data for the most recent available trading session
- **Rate:** The server caches results for **5 minutes** — only one HTTP request is made per cache window regardless of how many tools are called

---

## Installation

### With `uv` (recommended)

```bash
# Clone or download the project
cd bvc-mcp-server

# Install dependencies
uv sync

# Install dev/test dependencies
uv sync --extra dev
```

### With `pip`

```bash
cd bvc-mcp-server
pip install -e ".[dev]"
```

### Verify the server starts

```bash
python -m bvc_mcp.server
# or
fastmcp run src/bvc_mcp/server.py
```

---

## Configuration

### Claude Desktop

Add the following to your Claude Desktop config file:

- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "bvc": {
      "command": "python",
      "args": ["-m", "bvc_mcp.server"],
      "cwd": "/absolute/path/to/bvc-mcp-server",
      "env": {
        "PYTHONPATH": "/absolute/path/to/bvc-mcp-server/src"
      }
    }
  }
}
```

Or, if installed via `uv`:

```json
{
  "mcpServers": {
    "bvc": {
      "command": "uv",
      "args": ["run", "--directory", "/absolute/path/to/bvc-mcp-server", "python", "-m", "bvc_mcp.server"]
    }
  }
}
```
### Cursor

Open **Settings → MCP** and add a new server entry:

```json
{
  "bvc": {
    "command": "python",
    "args": ["-m", "bvc_mcp.server"],
    "cwd": "/absolute/path/to/bvc-mcp-server",
    "env": {
      "PYTHONPATH": "/absolute/path/to/bvc-mcp-server/src"
    }
  }
}
```

---

## Available Tools

Tools are split into two groups: **live tools** that query the BVC endpoint (5-minute cache), and **historical tools** that query the local SQLite database populated by the scheduler.

All monetary values are in **MAD (Moroccan Dirham)**.

---

### `get_market_status()`

Returns whether the BVC market is currently open or closed, plus the last data timestamp and stock counts.

**Example questions:**
- *"Is the Casablanca stock market open right now?"*
- *"When was the BVC data last updated?"*

---

### `get_all_stocks(include_untradeable: bool = False)`

Returns all stocks for the latest session.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `include_untradeable` | bool | `False` | Include suspended or non-traded stocks |

**Example questions:**
- *"Give me a list of all BVC stocks with their prices."*
- *"Show me all listed securities including suspended ones."*

---

### `get_stock(symbol: str)`

Returns complete market data for a single stock.

| Parameter | Type | Description |
|---|---|---|
| `symbol` | str | Ticker symbol, e.g. `"ATW"`, `"IAM"`, `"BCP"` |

**Example questions:**
- *"What is the current price of Attijariwafa Bank (ATW)?"*
- *"Show me today's trading data for IAM."*
- *"What is the bid/ask spread for BCP?"*

---

### `get_top_gainers(limit: int = 10)`

Returns the best-performing stocks of the session, sorted by variation descending.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `limit` | int | `10` | Maximum number of results (capped at 50) |

**Example questions:**
- *"What are the top 5 gainers on the BVC today?"*
- *"Which stocks are up the most this session?"*

---

### `get_top_losers(limit: int = 10)`

Returns the worst-performing stocks of the session, sorted by variation ascending.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `limit` | int | `10` | Maximum number of results (capped at 50) |

**Example questions:**
- *"What are the biggest losers on the BVC today?"*
- *"Which stocks fell the most this session?"*

---

### `get_top_volume(limit: int = 10)`

Returns the most-traded stocks by volume (in MAD), sorted descending.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `limit` | int | `10` | Maximum number of results (capped at 50) |

**Example questions:**
- *"Which BVC stocks had the highest trading volume today?"*
- *"Show me the 10 most active stocks by volume."*

---

### `search_stocks(query: str)`

Searches stocks by ticker symbol or company name (case-insensitive).

| Parameter | Type | Description |
|---|---|---|
| `query` | str | Search string, e.g. `"banque"`, `"ATW"`, `"maroc"` |

**Example questions:**
- *"Find all bank stocks on the BVC."*
- *"Search for stocks with 'maroc' in their name."*
- *"Is there a stock with symbol 'CIH'?"*

---

### `get_market_summary()`

Returns a complete statistical overview of the current trading session.

Includes: total stocks, gainers/losers/unchanged counts, total volume, top gainer, top loser, and highest-volume stock.

**Example questions:**
- *"Give me a summary of today's BVC session."*
- *"How many stocks went up vs down today on the Casablanca exchange?"*
- *"What was the total trading volume on the BVC today?"*

---

### `get_stock_history(symbol: str, limit: int = 30)` *(historical)*

Returns the price history of a single stock from the SQLite database. Each data point is one hourly snapshot collected while the market was open. Results are ordered newest-first.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | str | — | BVC ticker symbol, e.g. `"ATW"` |
| `limit` | int | `30` | Number of data points to return (max 200) |

**Example questions:**
- *"Show me the price history of ATW over the last 30 snapshots."*
- *"How has IAM's price evolved over the past week?"*

---

### `get_snapshots_list(limit: int = 10)` *(historical)*

Returns a list of the most recent market snapshots stored in the database. Useful to check what date range is available and when the last collection ran.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `limit` | int | `10` | Number of snapshots to return (max 100) |

**Example questions:**
- *"When was the last time market data was collected?"*
- *"Show me the last 20 database snapshots."*

---

### `get_price_evolution(symbol: str, from_date: str, to_date: str)` *(historical)*

Returns the price evolution of a stock between two calendar dates, including start price, end price, and total percentage change.

| Parameter | Type | Description |
|---|---|---|
| `symbol` | str | BVC ticker symbol |
| `from_date` | str | Start date, format `YYYY-MM-DD` |
| `to_date` | str | End date, format `YYYY-MM-DD` |

**Example questions:**
- *"How much did ATW gain between 2026-03-01 and 2026-03-09?"*
- *"What was the total variation of IAM last week?"*

---

### `get_volume_history(symbol: str, limit: int = 30)` *(historical)*

Returns the trading volume history for a stock, plus the average volume over the returned period.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `symbol` | str | — | BVC ticker symbol |
| `limit` | int | `30` | Number of data points (max 200) |

**Example questions:**
- *"What was the average daily volume of BCP over the last 30 sessions?"*
- *"Show me the volume trend for ATW."*

---

## Historical Data

### How data is collected

The server runs a **background scheduler** (APScheduler) that fires every hour at `HH:00`. The job checks whether the BVC market is currently open before making any network calls:

- **Market open** (Monday–Friday, 09:00–15:30 Casablanca time, UTC+1): fetches a fresh snapshot and saves it to `data/bvc_history.db`
- **Market closed**: logs `"Market closed, skipping snapshot"` and exits immediately

At typical BVC hours, this produces **7 snapshots per trading day** (09:00, 10:00, 11:00, 12:00, 13:00, 14:00, 15:00).

The scheduler also collects one snapshot immediately at server startup if the market is open.

### Database location

```
bvc-mcp-server/data/bvc_history.db
```

The file and its parent directory are created automatically on first run. The database is a standard SQLite file that can be opened with any SQLite client.

### Manual collection with `collect_now.py`

To populate the database outside normal trading hours (for testing, backfilling, etc.):

```bash
# Respects market-hours check — exits with a warning if market is closed
python scripts/collect_now.py

# Ignores market-hours check and collects regardless
python scripts/collect_now.py --force
```

### Timezone

All `fetched_at` datetimes stored in the database are in **UTC**. The market-hours check converts to `Africa/Casablanca` (UTC+1) before comparing.

---

## Financial Analysis Tools

Tools 13–33 provide technical analysis and screening capabilities that work on data already collected in the local SQLite database. No extra dependencies are required — all calculations use pure Python and the standard `math` library.

### Technical Analysis (per symbol)

| Tool | Parameters | What it returns |
|---|---|---|
| `get_stock_ma` | `symbol`, `period` (int) | SMA series: list with `None` for the first `period-1` entries, then the rolling average |
| `get_stock_rsi` | `symbol`, `period` (default 14) | RSI series with Wilder's smoothing. Values in [0, 100]; first `period` entries are `None` |
| `get_stock_bollinger` | `symbol`, `period` (default 20), `std_dev` (default 2.0) | Per-snapshot `{middle, upper, lower}` bands; `None` until enough data |
| `get_stock_volatility` | `symbol`, `period` (default 30) | Single annualised volatility figure (%) based on log returns × √252 |
| `get_stock_momentum` | `symbol`, `period` (default 10) | Rate-of-change vs the price `period` snapshots ago, as a percentage |
| `get_stock_support_resistance` | `symbol`, `window` (default 20) | `{support: min, resistance: max}` of the most recent `window` prices |

**Example questions for an AI assistant:**
- *"What is the RSI for ATW over the last 14 periods?"*
- *"Show me Bollinger Bands for IAM."*
- *"Is BCP above its 20-period moving average?"*
- *"What is the annualised volatility for BMCI?"*

---

### Market Analysis

| Tool | Parameters | What it returns |
|---|---|---|
| `get_market_breadth` | `period` (default 20) | Number/% of stocks above their SMA(`period`). Overall market sentiment gauge. |
| `get_top_performers_period` | `from_date` (YYYY-MM-DD), `to_date`, `limit` (default 10) | Stocks with the highest price change % between the two dates |
| `get_worst_performers_period` | `from_date`, `to_date`, `limit` (default 10) | Stocks with the biggest price decline % over the period |
| `get_stock_correlation` | `symbol_a`, `symbol_b`, `limit` (default 30) | Pearson correlation coefficient [-1, 1] of recent price series |

**Example questions:**
- *"What percentage of BVC stocks are above their 20-day moving average?"*
- *"Which stocks performed best between 2026-01-01 and 2026-03-01?"*
- *"Are ATW and IAM correlated?"*

---

### Stock Screening

| Tool | Parameters | What it returns |
|---|---|---|
| `screen_stocks` | `min_price`, `max_price`, `min_variation`, `max_variation` (all optional floats) | Stocks from the latest snapshot matching the given price/variation range |
| `get_unusual_volume` | `threshold` (default 2.0), `limit` (default 20) | Stocks whose current volume is ≥ `threshold × average` (last 20 snapshots) |
| `get_breakout_candidates` | `period` (default 20), `limit` (default 10) | Stocks whose latest price is at or near their `period`-snapshot high |

**Example questions:**
- *"Screen for stocks priced between 100 and 500 MAD with positive variation."*
- *"Which stocks have unusual trading volume today?"*
- *"List breakout candidates approaching a new 20-period high."*

---

### Watchlist Management

Watchlists are stored in the same SQLite database as market history. Each watchlist has a unique name and can hold any number of BVC ticker symbols.

| Tool | Parameters | What it returns |
|---|---|---|
| `create_watchlist` | `name`, `symbols` (list of strings) | Created watchlist with `{id, name, created_at, symbols, count}` |
| `get_watchlist` | `name` | Watchlist details with the full list of symbols and their `added_at` timestamps |
| `list_watchlists` | *(none)* | All watchlists with name, creation date, and stock count |
| `add_to_watchlist` | `name`, `symbol` | Adds one symbol to an existing watchlist |
| `remove_from_watchlist` | `name`, `symbol` | Removes one symbol from a watchlist |
| `delete_watchlist` | `name` | Deletes the watchlist and all its symbols |
| `get_watchlist_performance` | `name`, `from_date`, `to_date` | Price performance (%) for every symbol in the watchlist over the given date range |

**Example questions:**
- *"Create a watchlist called 'banking' with ATW, BCP, and CIH."*
- *"How did my 'banking' watchlist perform last month?"*
- *"Add IAM to my 'telecom' watchlist."*
- *"List all my watchlists."*

---

## Running Tests

```bash
# With uv
uv run pytest

# With pip
pytest
```

All tests use mocked data — no network access is required.

---

## Architecture

```
bvc-mcp-server/
├── README.md               # This file
├── pyproject.toml          # Dependencies and project metadata
├── .env.example            # Optional environment variable overrides
├── data/
│   └── bvc_history.db      # SQLite database (auto-created on first run)
├── scripts/
│   └── collect_now.py      # CLI tool to manually trigger a snapshot
├── src/
│   └── bvc_mcp/
│       ├── __init__.py     # Package version and metadata
│       ├── config.py       # Central constants (DB path, market hours, TTL)
│       ├── models.py       # Pydantic v2 data models (Stock, MarketSnapshot)
│       ├── client.py       # Async HTTP client + 5-minute in-memory cache
│       ├── database.py     # SQLite read/write functions (no ORM, stdlib only)
│       ├── scheduler.py    # APScheduler background job + market-hours check
│       ├── analytics.py    # Pure-Python financial functions (SMA, RSI, BB, …)
│       ├── watchlist.py    # Watchlist CRUD (SQLite-backed)
│       ├── server.py       # FastMCP app — 33 tool handlers
│       └── utils.py        # Formatting helpers (MAD, %, datetime, JSON)
└── tests/
    ├── test_client.py      # Unit tests: models, cache, live tools (mocked HTTP)
    ├── test_database.py    # Unit tests: DB functions, scheduler market-hours check
    ├── test_analytics.py   # Unit tests: all 8 analytics functions
    └── test_watchlist.py   # Unit tests: full watchlist CRUD lifecycle
```

### File roles

| File | Responsibility |
|---|---|
| `config.py` | Single source of truth for all constants: `DB_PATH`, `CACHE_TTL_SECONDS`, market open/close hours, scheduler interval, timezone. |
| `models.py` | Pydantic models that parse, clean, and validate raw BVC JSON. Handles empty strings, datetime parsing, and computed properties like `is_tradeable`. |
| `client.py` | Single async function `fetch_market_data()` that calls the BVC endpoint and manages the shared in-memory cache. All live tools call this function. |
| `database.py` | Thin SQLite wrapper (stdlib `sqlite3` only). Uses `contextlib.closing()` on every connection for correct file-handle cleanup on Windows. Includes SQL window-function queries for batch analytics (period performance, MA status, bulk price fetch). |
| `scheduler.py` | APScheduler `BackgroundScheduler` (daemon thread). Fires at `HH:00`, checks `is_market_open()`, then calls `save_snapshot()`. Also starts an immediate snapshot at server boot if the market is open. |
| `analytics.py` | Pure-Python financial calculations (no extra dependencies): SMA, RSI (Wilder smoothing), Bollinger Bands, annualised volatility, Pearson correlation, momentum, support/resistance, average volume. |
| `watchlist.py` | Named watchlist CRUD backed by the same SQLite database. Uses `contextlib.closing()` throughout, with explicit child-row deletes to avoid relying on `PRAGMA foreign_keys`. |
| `server.py` | FastMCP application with 33 `@mcp.tool()` async functions. Live tools use `client.py`; historical and analysis tools query `database.py` and `analytics.py`. |
| `utils.py` | Pure helper functions for number formatting, symbol normalization, and JSON serialization with datetime support. |

---

## Extending — Adding a New Tool

Adding a new tool takes three steps:

**Step 1** — Add a new async function in `server.py` decorated with `@mcp.tool()`:

```python
@mcp.tool()
async def get_stocks_by_segment(segment_code: str) -> str:
    """Return all stocks belonging to a given market segment code."""
    try:
        snapshot = await fetch_market_data()
    except RuntimeError as exc:
        return json.dumps({"error": str(exc)})

    filtered = [s for s in snapshot.tradeable_stocks if s.segment_code == segment_code]
    return to_json({"count": len(filtered), "stocks": [s.to_dict() for s in filtered]})
```

**Step 2** — If you need new fields or derived properties, extend the `Stock` model in `models.py` and add the corresponding field or `@property`.

**Step 3** — Add tests for the new tool in `tests/test_client.py` using `patch("bvc_mcp.client.fetch_market_data", ...)` to mock the HTTP layer.

That's it — FastMCP auto-discovers all `@mcp.tool()` functions at startup.

---

## Deployment

### Railway (recommended)

1. Install Railway CLI: `npm install -g @railway/cli`
2. Login: `railway login`
3. Initialise project: `railway init`
4. Add a Volume named `bvc-data` mounted at `/data` in the Railway dashboard
5. Set environment variables in Railway dashboard:
   - `BVC_DB_PATH=/data/bvc_history.db`
   - `HOST=0.0.0.0`
6. Deploy: `railway up`
7. Get your public URL: `railway domain`

### Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `8000` | HTTP port (auto-injected by Railway) |
| `HOST` | `0.0.0.0` | Bind address |
| `BVC_DB_PATH` | `data/bvc_history.db` | SQLite database path |
| `BVC_USER_ID` | *(none)* | Single-user mode owner ID (local dev) |
| `BVC_API_KEY` | *(none)* | Alternative to per-call api_key (local dev) |

### MCPize Publication

1. Deploy to Railway and get your public URL
2. Go to mcprize.io → New Server
3. Enter your Railway URL: `https://your-app.railway.app`
4. MCPize will discover your 33 tools automatically via the MCP protocol
5. Configure pricing tiers

### Docker (local test)

```bash
docker build -t bvc-mcp-server .
docker run -p 8000:8000 -v bvc-data:/data -e PORT=8000 bvc-mcp-server
```

---

## Legal & Best Practices

- Data is sourced from the publicly accessible BVC website. No authentication or scraping is involved — the endpoint is the same one used by the BVC's own web interface.
- This server is intended for **personal, educational, and research use**.
- The 5-minute cache is in place to be a respectful API consumer. Do not lower the TTL unnecessarily.
- Always attribute data to the **Bourse de Casablanca** when displaying it publicly.
- This project is not affiliated with, endorsed by, or sponsored by the Bourse de Casablanca.
