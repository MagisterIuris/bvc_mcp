"""
Unit tests for the BVC MCP server.

Uses mock data to avoid hitting the live BVC endpoint during testing.
Run with: pytest tests/
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
from datetime import datetime
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Fixtures — sample raw API payload
# ---------------------------------------------------------------------------

SAMPLE_STOCK_ACTIVE = {
    "Symbol": "ATW",
    "Libelle": "ATTIJARIWAFA BANK  ",  # trailing spaces — should be stripped
    "Cours": 667.9,
    "Variation": -4.74,
    "Ouverture": 681,
    "PlusHaut": 690,
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

SAMPLE_STOCK_SUSPENDED = {
    "Symbol": "XYZ",
    "Libelle": "SUSPENDED COMPANY",
    "Cours": "",         # no price → untradeable
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

SAMPLE_STOCK_GAINER = {
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

SAMPLE_STOCK_BMCI = {
    "Symbol": "BMCI",
    "Libelle": "BANQUE MAROCAINE POUR LE COMMERCE",
    "Cours": 750.0,
    "Variation": -0.3,
    "Ouverture": 752.0,
    "PlusHaut": 753.0,
    "PlusBas": 749.0,
    "Volumes": 3000000.0,
    "QteEchangee": 4000,
    "MeilleurDemande": 749.0,
    "QteAchat": 50,
    "MeilleurOffre": 751.0,
    "QteVente": 80,
    "CoursDeReferance": 752.3,
    "DateDernierCours": "09/03/2026 14:00:00",
    "Etat": "Market Close",
    "CodeSegment": "01",
    "IdTypeValeur": 1,
}

SAMPLE_STOCK_BCP = {
    "Symbol": "BCP",
    "Libelle": "BANQUE CENTRALE POPULAIRE",
    "Cours": 320.0,
    "Variation": 0.5,
    "Ouverture": 318.0,
    "PlusHaut": 321.0,
    "PlusBas": 317.0,
    "Volumes": 8000000.0,
    "QteEchangee": 25000,
    "MeilleurDemande": 319.0,
    "QteAchat": 100,
    "MeilleurOffre": 320.5,
    "QteVente": 200,
    "CoursDeReferance": 318.4,
    "DateDernierCours": "09/03/2026 14:00:00",
    "Etat": "Market Close",
    "CodeSegment": "01",
    "IdTypeValeur": 1,
}

SAMPLE_API_RESPONSE = {
    "success": True,
    "lastModified": 1773071642,
    "timestamp": "2026-03-09 15:54:02",
    "timestampFrench": "lundi 9 mars 2026 16:54:02",
    "data": [SAMPLE_STOCK_ACTIVE, SAMPLE_STOCK_SUSPENDED, SAMPLE_STOCK_GAINER],
}


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestStockModel:
    """Tests for the Stock Pydantic model."""

    def test_parses_active_stock(self):
        """Active stock with all fields should parse cleanly."""
        from bvc_mcp.models import Stock

        stock = Stock.model_validate(SAMPLE_STOCK_ACTIVE)
        assert stock.symbol == "ATW"
        assert stock.price == 667.9
        assert stock.variation == -4.74
        assert stock.volume_mad == 32684179.2
        assert stock.quantity_traded == 48497

    def test_strips_trailing_spaces_from_name(self):
        """Libelle field should have leading/trailing whitespace removed."""
        from bvc_mcp.models import Stock

        stock = Stock.model_validate(SAMPLE_STOCK_ACTIVE)
        assert stock.name == "ATTIJARIWAFA BANK"
        assert not stock.name.endswith(" ")

    def test_empty_string_price_becomes_none(self):
        """Empty string for Cours should result in price=None."""
        from bvc_mcp.models import Stock

        stock = Stock.model_validate(SAMPLE_STOCK_SUSPENDED)
        assert stock.price is None
        assert stock.variation is None
        assert stock.volume_mad is None

    def test_is_tradeable_true_for_active_stock(self):
        """is_tradeable should be True when price is set."""
        from bvc_mcp.models import Stock

        stock = Stock.model_validate(SAMPLE_STOCK_ACTIVE)
        assert stock.is_tradeable is True

    def test_is_tradeable_false_for_suspended_stock(self):
        """is_tradeable should be False when price is None."""
        from bvc_mcp.models import Stock

        stock = Stock.model_validate(SAMPLE_STOCK_SUSPENDED)
        assert stock.is_tradeable is False

    def test_variation_pct_display_negative(self):
        """Negative variation should display with a minus sign."""
        from bvc_mcp.models import Stock

        stock = Stock.model_validate(SAMPLE_STOCK_ACTIVE)
        assert stock.variation_pct_display == "-4.74%"

    def test_variation_pct_display_positive(self):
        """Positive variation should display with a plus sign."""
        from bvc_mcp.models import Stock

        stock = Stock.model_validate(SAMPLE_STOCK_GAINER)
        assert stock.variation_pct_display == "+3.66%"

    def test_variation_pct_display_none(self):
        """None variation should display as 'N/A'."""
        from bvc_mcp.models import Stock

        stock = Stock.model_validate(SAMPLE_STOCK_SUSPENDED)
        assert stock.variation_pct_display == "N/A"

    def test_parses_last_trade_datetime(self):
        """DateDernierCours should be parsed into a datetime object."""
        from bvc_mcp.models import Stock

        stock = Stock.model_validate(SAMPLE_STOCK_ACTIVE)
        assert isinstance(stock.last_trade_datetime, datetime)
        assert stock.last_trade_datetime.day == 9
        assert stock.last_trade_datetime.month == 3
        assert stock.last_trade_datetime.year == 2026

    def test_empty_date_becomes_none(self):
        """Empty DateDernierCours should result in last_trade_datetime=None."""
        from bvc_mcp.models import Stock

        stock = Stock.model_validate(SAMPLE_STOCK_SUSPENDED)
        assert stock.last_trade_datetime is None

    def test_to_dict_contains_expected_keys(self):
        """to_dict() should return all expected serialization keys."""
        from bvc_mcp.models import Stock

        stock = Stock.model_validate(SAMPLE_STOCK_ACTIVE)
        d = stock.to_dict()
        expected_keys = {
            "symbol", "name", "price", "variation", "variation_display",
            "open", "high", "low", "volume_mad", "quantity_traded",
            "best_bid", "bid_quantity", "best_ask", "ask_quantity",
            "reference_price", "last_trade_datetime", "market_state",
            "segment_code", "is_tradeable",
        }
        assert expected_keys.issubset(d.keys())


class TestMarketSnapshot:
    """Tests for the MarketSnapshot model."""

    def test_tradeable_stocks_filters_correctly(self):
        """tradeable_stocks property should exclude stocks with no price."""
        from bvc_mcp.models import MarketSnapshot, Stock

        stocks = [
            Stock.model_validate(SAMPLE_STOCK_ACTIVE),
            Stock.model_validate(SAMPLE_STOCK_SUSPENDED),
            Stock.model_validate(SAMPLE_STOCK_GAINER),
        ]
        snapshot = MarketSnapshot(
            success=True,
            lastModified=1773071642,
            timestamp="2026-03-09 15:54:02",
            timestampFrench="lundi 9 mars 2026 16:54:02",
            stocks=stocks,
        )
        tradeable = snapshot.tradeable_stocks
        assert len(tradeable) == 2
        symbols = {s.symbol for s in tradeable}
        assert "XYZ" not in symbols

    def test_market_state_from_stock_etat(self):
        """market_state should return the Etat value from the first stock that has one."""
        from bvc_mcp.models import MarketSnapshot, Stock

        stocks = [Stock.model_validate(SAMPLE_STOCK_ACTIVE)]
        snapshot = MarketSnapshot(
            success=True,
            lastModified=1773071642,
            timestamp="2026-03-09 15:54:02",
            timestampFrench="lundi 9 mars 2026 16:54:02",
            stocks=stocks,
        )
        assert snapshot.market_state == "Market Close"


# ---------------------------------------------------------------------------
# Client cache tests
# ---------------------------------------------------------------------------


class TestClientCache:
    """Tests for the in-memory caching logic in client.py."""

    def setup_method(self):
        """Reset module-level cache before each test."""
        import bvc_mcp.client as client_module
        client_module._cache = None

    def _make_snapshot(self):
        """Build a MarketSnapshot from the sample data."""
        from bvc_mcp.models import MarketSnapshot, Stock

        stocks = [Stock.model_validate(SAMPLE_STOCK_ACTIVE)]
        return MarketSnapshot(
            success=True,
            lastModified=1773071642,
            timestamp="2026-03-09 15:54:02",
            timestampFrench="lundi 9 mars 2026 16:54:02",
            stocks=stocks,
        )

    @pytest.mark.asyncio
    async def test_fetch_calls_http_on_cold_cache(self):
        """With no cache, fetch_market_data should call _do_fetch exactly once."""
        snapshot = self._make_snapshot()

        with patch("bvc_mcp.client._do_fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = snapshot
            from bvc_mcp.client import fetch_market_data
            result = await fetch_market_data()
            mock_fetch.assert_called_once()
            assert result.timestamp == "2026-03-09 15:54:02"

    @pytest.mark.asyncio
    async def test_second_fetch_uses_cache(self):
        """Second call within TTL should NOT trigger another HTTP request."""
        snapshot = self._make_snapshot()

        with patch("bvc_mcp.client._do_fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = snapshot
            from bvc_mcp.client import fetch_market_data
            await fetch_market_data()  # primes the cache
            await fetch_market_data()  # should use cache
            mock_fetch.assert_called_once()  # HTTP called only once

    @pytest.mark.asyncio
    async def test_force_refresh_bypasses_cache(self):
        """force_refresh=True should always call _do_fetch."""
        snapshot = self._make_snapshot()

        with patch("bvc_mcp.client._do_fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = snapshot
            from bvc_mcp.client import fetch_market_data
            await fetch_market_data()               # prime cache
            await fetch_market_data(force_refresh=True)  # bypass
            assert mock_fetch.call_count == 2

    @pytest.mark.asyncio
    async def test_stale_cache_served_on_http_error(self):
        """When the HTTP request fails, stale cache should be returned if available."""
        import bvc_mcp.client as client_module
        from bvc_mcp.client import _CacheEntry, fetch_market_data

        snapshot = self._make_snapshot()
        # Inject a stale (expired) cache entry
        entry = _CacheEntry(snapshot=snapshot, fetched_at=time.monotonic() - 9999)
        client_module._cache = entry

        with patch("bvc_mcp.client._do_fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = Exception("Connection refused")
            result = await fetch_market_data()
            assert result.timestamp == "2026-03-09 15:54:02"  # stale cache served

    @pytest.mark.asyncio
    async def test_no_cache_and_http_error_raises(self):
        """With no cache and a failing HTTP request, RuntimeError should be raised."""
        with patch("bvc_mcp.client._do_fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = Exception("DNS failure")
            from bvc_mcp.client import fetch_market_data
            with pytest.raises(RuntimeError, match="BVC endpoint unavailable"):
                await fetch_market_data()

    @pytest.mark.asyncio
    async def test_stale_cache_served_on_timeout(self):
        """A timeout should behave like other fetch failures and serve stale cache."""
        import bvc_mcp.client as client_module
        from bvc_mcp.client import _CacheEntry, fetch_market_data

        snapshot = self._make_snapshot()
        client_module._cache = _CacheEntry(
            snapshot=snapshot,
            fetched_at=time.monotonic() - 9999,
        )

        with patch("bvc_mcp.client._do_fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = TimeoutError("request timed out")
            result = await fetch_market_data()
            assert result.timestamp == "2026-03-09 15:54:02"

    @pytest.mark.asyncio
    async def test_no_cache_and_timeout_raises_runtime_error(self):
        """With no cache, a timeout should surface as a controlled RuntimeError."""
        with patch("bvc_mcp.client._do_fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = TimeoutError("request timed out")
            from bvc_mcp.client import fetch_market_data

            with pytest.raises(RuntimeError, match="BVC endpoint unavailable"):
                await fetch_market_data()

    def test_cache_info_no_cache(self):
        """get_cache_info() with no cache should return has_cache=False."""
        from bvc_mcp.client import get_cache_info
        info = get_cache_info()
        assert info == {"has_cache": False}

    def test_cache_info_with_cache(self):
        import bvc_mcp.client as client_module
        from bvc_mcp.client import _CacheEntry, get_cache_info

        snapshot = self._make_snapshot()
        client_module._cache = _CacheEntry(snapshot=snapshot)
        info = get_cache_info()

        assert info["has_cache"] is True
        assert info["stock_count"] == 1
        assert "ttl_seconds" in info

    def test_invalidate_cache(self):
        """invalidate_cache() should clear the module-level cache."""
        import bvc_mcp.client as client_module
        from bvc_mcp.client import _CacheEntry, invalidate_cache

        snapshot = self._make_snapshot()
        client_module._cache = _CacheEntry(snapshot=snapshot)
        assert client_module._cache is not None

        invalidate_cache()
        assert client_module._cache is None


class TestClientFetchInternals:
    """Direct tests for _do_fetch() and cache helpers."""

    def _mock_response(self, payload: dict):
        response = MagicMock()
        response.json.return_value = payload
        response.raise_for_status.return_value = None
        return response

    @pytest.mark.asyncio
    async def test_do_fetch_parses_valid_payload(self):
        response = self._mock_response(SAMPLE_API_RESPONSE)
        client_ctx = AsyncMock()
        client_ctx.get.return_value = response

        async_client = AsyncMock()
        async_client.__aenter__.return_value = client_ctx
        async_client.__aexit__.return_value = None

        with patch("bvc_mcp.client.httpx.AsyncClient", return_value=async_client):
            from bvc_mcp.client import _do_fetch

            snapshot = await _do_fetch()

        assert snapshot.success is True
        assert snapshot.timestamp == "2026-03-09 15:54:02"
        assert len(snapshot.stocks) == 3

    @pytest.mark.asyncio
    async def test_do_fetch_raises_on_success_false(self):
        payload = dict(SAMPLE_API_RESPONSE)
        payload["success"] = False
        response = self._mock_response(payload)
        client_ctx = AsyncMock()
        client_ctx.get.return_value = response

        async_client = AsyncMock()
        async_client.__aenter__.return_value = client_ctx
        async_client.__aexit__.return_value = None

        with patch("bvc_mcp.client.httpx.AsyncClient", return_value=async_client):
            from bvc_mcp.client import _do_fetch

            with pytest.raises(ValueError, match="success=false"):
                await _do_fetch()

    @pytest.mark.asyncio
    async def test_do_fetch_skips_malformed_stock_rows(self):
        bad_payload = dict(SAMPLE_API_RESPONSE)
        bad_payload["data"] = [SAMPLE_STOCK_ACTIVE, {"Symbol": "BAD"}]
        response = self._mock_response(bad_payload)
        client_ctx = AsyncMock()
        client_ctx.get.return_value = response

        async_client = AsyncMock()
        async_client.__aenter__.return_value = client_ctx
        async_client.__aexit__.return_value = None

        with patch("bvc_mcp.client.httpx.AsyncClient", return_value=async_client):
            from bvc_mcp.client import _do_fetch

            snapshot = await _do_fetch()

        assert len(snapshot.stocks) == 1
        assert snapshot.stocks[0].symbol == "ATW"

    def test_cache_age_returns_zero_without_cache(self):
        import bvc_mcp.client as client_module
        from bvc_mcp.client import _cache_age

        client_module._cache = None
        assert _cache_age() == 0.0


# ---------------------------------------------------------------------------
# Utils tests
# ---------------------------------------------------------------------------


class TestUtils:
    """Tests for utility helper functions."""

    def test_format_mad(self):
        """format_mad should produce a human-readable MAD string."""
        from bvc_mcp.utils import format_mad

        assert format_mad(32684179.2) == "32,684,179.20 MAD"

    def test_format_mad_none(self):
        """format_mad with None should return 'N/A'."""
        from bvc_mcp.utils import format_mad

        assert format_mad(None) == "N/A"

    def test_format_number(self):
        from bvc_mcp.utils import format_number

        assert format_number(48497) == "48,497"

    def test_format_number_none(self):
        from bvc_mcp.utils import format_number

        assert format_number(None) == "N/A"

    def test_format_variation_positive(self):
        """Positive variation should include '+' prefix."""
        from bvc_mcp.utils import format_variation

        assert format_variation(3.66) == "+3.66%"

    def test_format_variation_negative(self):
        """Negative variation should include '-' prefix."""
        from bvc_mcp.utils import format_variation

        assert format_variation(-4.74) == "-4.74%"

    def test_format_variation_none(self):
        """None variation should return 'N/A'."""
        from bvc_mcp.utils import format_variation

        assert format_variation(None) == "N/A"

    def test_normalize_symbol_uppercase(self):
        """normalize_symbol should return uppercase stripped symbol."""
        from bvc_mcp.utils import normalize_symbol

        assert normalize_symbol("  atw  ") == "ATW"
        assert normalize_symbol("iam") == "IAM"

    def test_to_json_serializes_datetime(self):
        """to_json should handle datetime objects without raising."""
        from bvc_mcp.utils import to_json

        obj = {"date": datetime(2026, 3, 9, 15, 54, 2)}
        result = to_json(obj)
        assert "2026-03-09T15:54:02" in result

    def test_to_json_raises_for_non_serializable_object(self):
        from bvc_mcp.utils import to_json

        with pytest.raises(TypeError, match="not JSON serializable"):
            to_json({"x": object()})

    def test_unix_ts_to_iso(self):
        """unix_ts_to_iso should convert a Unix timestamp to ISO 8601 format."""
        from bvc_mcp.utils import unix_ts_to_iso

        result = unix_ts_to_iso(0)
        assert result == "1970-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# Integration-style tool tests (with mocked client)
# ---------------------------------------------------------------------------


class TestMCPTools:
    """Tests for MCP tool functions, with the HTTP client mocked out."""

    def setup_method(self):
        """Reset cache before each test."""
        import bvc_mcp.client as client_module
        client_module._cache = None

    def _make_snapshot(self):
        from bvc_mcp.models import MarketSnapshot, Stock

        stocks = [
            Stock.model_validate(SAMPLE_STOCK_ACTIVE),
            Stock.model_validate(SAMPLE_STOCK_SUSPENDED),
            Stock.model_validate(SAMPLE_STOCK_GAINER),
        ]
        return MarketSnapshot(
            success=True,
            lastModified=1773071642,
            timestamp="2026-03-09 15:54:02",
            timestampFrench="lundi 9 mars 2026 16:54:02",
            stocks=stocks,
        )

    @pytest.mark.asyncio
    async def test_get_market_status_returns_json(self):
        """get_market_status should return parseable JSON with expected keys."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.client.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_market_status
            result = await get_market_status()
            data = json.loads(result)
            assert "status" in data
            assert "tradeable_count" in data
            assert data["tradeable_count"] == 2

    @pytest.mark.asyncio
    async def test_get_stock_found(self):
        """get_stock with a valid symbol should return stock data."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.client.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_stock
            result = await get_stock("ATW")
            data = json.loads(result)
            assert data["stock"]["symbol"] == "ATW"
            assert data["stock"]["price"] == 667.9

    @pytest.mark.asyncio
    async def test_get_stock_not_found(self):
        """get_stock with an unknown symbol should return an error dict."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.client.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_stock
            result = await get_stock("UNKNOWN")
            data = json.loads(result)
            assert "error" in data
            assert "UNKNOWN" in data["error"]

    @pytest.mark.asyncio
    async def test_get_top_gainers(self):
        """get_top_gainers should return stocks sorted by positive variation."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.client.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_top_gainers
            result = await get_top_gainers(limit=5)
            data = json.loads(result)
            assert data["gainers"][0]["symbol"] == "IAM"
            assert data["gainers"][0]["variation"] == 3.66

    @pytest.mark.asyncio
    async def test_get_top_losers(self):
        """get_top_losers should return stocks sorted by negative variation."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.client.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_top_losers
            result = await get_top_losers(limit=5)
            data = json.loads(result)
            assert data["losers"][0]["symbol"] == "ATW"
            assert data["losers"][0]["variation"] == -4.74

    @pytest.mark.asyncio
    async def test_get_top_volume(self):
        """get_top_volume should return stocks sorted by volume descending."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.client.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_top_volume
            result = await get_top_volume(limit=5)
            data = json.loads(result)
            assert data["top_volume"][0]["symbol"] == "ATW"
            assert data["top_volume"][0]["volume_mad"] == 32684179.2

    @pytest.mark.asyncio
    async def test_search_stocks_by_symbol(self):
        """search_stocks should find a stock by symbol substring."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.client.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import search_stocks
            result = await search_stocks("iam")
            data = json.loads(result)
            assert data["count"] == 1
            assert data["results"][0]["symbol"] == "IAM"

    @pytest.mark.asyncio
    async def test_search_stocks_by_name(self):
        """search_stocks should find a stock by company name substring."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.client.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import search_stocks
            result = await search_stocks("attijar")
            data = json.loads(result)
            assert data["count"] == 1
            assert data["results"][0]["symbol"] == "ATW"

    @pytest.mark.asyncio
    async def test_get_market_summary(self):
        """get_market_summary should return aggregated stats with correct counts."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.client.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_market_summary
            result = await get_market_summary()
            data = json.loads(result)
            assert data["tradeable"] == 2
            assert data["gainers"] == 1
            assert data["losers"] == 1
            assert data["top_gainer"]["symbol"] == "IAM"
            assert data["top_loser"]["symbol"] == "ATW"

    @pytest.mark.asyncio
    async def test_get_all_stocks_excludes_untradeable_by_default(self):
        """get_all_stocks should exclude untradeable stocks by default."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.client.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_all_stocks
            result = await get_all_stocks(include_untradeable=False)
            data = json.loads(result)
            symbols = [s["symbol"] for s in data["stocks"]]
            assert "XYZ" not in symbols
            assert data["count"] == 2

    @pytest.mark.asyncio
    async def test_get_all_stocks_includes_untradeable_when_requested(self):
        """get_all_stocks with include_untradeable=True should return all stocks."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.client.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_all_stocks
            result = await get_all_stocks(include_untradeable=True)
            data = json.loads(result)
            assert data["count"] == 3

    @pytest.mark.asyncio
    async def test_get_market_status_handles_fetch_error(self):
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.side_effect = RuntimeError("upstream down")
            from bvc_mcp.server import get_market_status

            result = json.loads(await get_market_status())

        assert result == {"error": "upstream down"}

    @pytest.mark.asyncio
    async def test_get_all_stocks_handles_fetch_error(self):
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.side_effect = RuntimeError("upstream down")
            from bvc_mcp.server import get_all_stocks

            result = json.loads(await get_all_stocks())

        assert result == {"error": "upstream down"}

    @pytest.mark.asyncio
    async def test_get_stock_handles_fetch_error(self):
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.side_effect = RuntimeError("upstream down")
            from bvc_mcp.server import get_stock

            result = json.loads(await get_stock("ATW"))

        assert result == {"error": "upstream down"}

    @pytest.mark.asyncio
    async def test_top_tools_handle_fetch_error(self):
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.side_effect = RuntimeError("upstream down")
            from bvc_mcp.server import get_top_gainers, get_top_losers, get_top_volume

            gainers = json.loads(await get_top_gainers())
            losers = json.loads(await get_top_losers())
            volume = json.loads(await get_top_volume())

        assert gainers == {"error": "upstream down"}
        assert losers == {"error": "upstream down"}
        assert volume == {"error": "upstream down"}

    @pytest.mark.asyncio
    async def test_search_stocks_long_query_returns_error(self):
        from bvc_mcp.server import search_stocks

        result = json.loads(await search_stocks("x" * 101))
        assert "error" in result

    @pytest.mark.asyncio
    async def test_search_stocks_empty_query_returns_all(self):
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import search_stocks

            result = json.loads(await search_stocks("   "))

        assert result["count"] == 3

    @pytest.mark.asyncio
    async def test_search_stocks_handles_fetch_error(self):
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.side_effect = RuntimeError("upstream down")
            from bvc_mcp.server import search_stocks

            result = json.loads(await search_stocks("ATW"))

        assert result == {"error": "upstream down"}

    @pytest.mark.asyncio
    async def test_market_summary_handles_fetch_error(self):
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.side_effect = RuntimeError("upstream down")
            from bvc_mcp.server import get_market_summary

            result = json.loads(await get_market_summary())

        assert result == {"error": "upstream down"}

    @pytest.mark.asyncio
    async def test_market_summary_handles_no_gainers_or_losers(self):
        snapshot = self._make_snapshot()
        for stock in snapshot.stocks:
            if stock.price is not None:
                stock.variation = None
                stock.volume_mad = None
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_market_summary

            result = json.loads(await get_market_summary())

        assert result["top_gainer"] is None
        assert result["top_loser"] is None
        assert result["top_volume"] is None


# ---------------------------------------------------------------------------
# Fuzzy search tool tests
# ---------------------------------------------------------------------------


class TestFindStock:
    """Tests for the find_stock fuzzy search MCP tool."""

    def setup_method(self):
        import bvc_mcp.client as client_module
        client_module._cache = None

    def _make_snapshot(self):
        from bvc_mcp.models import MarketSnapshot, Stock

        stocks = [
            Stock.model_validate(SAMPLE_STOCK_ACTIVE),    # ATW / ATTIJARIWAFA BANK
            Stock.model_validate(SAMPLE_STOCK_SUSPENDED),  # XYZ / SUSPENDED COMPANY
            Stock.model_validate(SAMPLE_STOCK_GAINER),    # IAM / ITISSALAT AL-MAGHRIB
            Stock.model_validate(SAMPLE_STOCK_BCP),        # BCP / BANQUE CENTRALE POPULAIRE
            Stock.model_validate(SAMPLE_STOCK_BMCI),       # BMCI / BANQUE MAROCAINE POUR LE COMMERCE
        ]
        return MarketSnapshot(
            success=True,
            lastModified=1773071642,
            timestamp="2026-03-09 15:54:02",
            timestampFrench="lundi 9 mars 2026 16:54:02",
            stocks=stocks,
        )

    @pytest.mark.asyncio
    async def test_find_stock_exact_symbol_returns_first(self):
        """find_stock('ATW') should return ATW as the top result."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import find_stock
            result = await find_stock("ATW")
            data = json.loads(result)
            assert data["count"] >= 1
            assert data["results"][0]["symbol"] == "ATW"

    @pytest.mark.asyncio
    async def test_find_stock_banque_returns_multiple(self):
        """find_stock('banque') should return at least 2 results (BCP and BMCI)."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import find_stock
            result = await find_stock("banque")
            data = json.loads(result)
            assert data["count"] >= 2

    @pytest.mark.asyncio
    async def test_find_stock_no_match_returns_empty(self):
        """find_stock('xyznotexist') should return an empty results list."""
        snapshot = self._make_snapshot()
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import find_stock
            result = await find_stock("xyznotexist")
            data = json.loads(result)
            assert data["count"] == 0
            assert data["results"] == []

    @pytest.mark.asyncio
    async def test_find_stock_long_query_returns_error(self):
        from bvc_mcp.server import find_stock

        result = json.loads(await find_stock("x" * 101))
        assert "error" in result

    @pytest.mark.asyncio
    async def test_find_stock_handles_fetch_error(self):
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.side_effect = RuntimeError("upstream down")
            from bvc_mcp.server import find_stock

            result = json.loads(await find_stock("ATW"))

        assert result == {"error": "upstream down"}


class TestServerHelpers:
    def test_fuzzy_score_exact_and_prefix_paths(self):
        from bvc_mcp.server import _fuzzy_score

        assert _fuzzy_score("ATW", "ATW", "Attijariwafa Bank") == 100
        assert _fuzzy_score("AT", "ATW", "Attijariwafa Bank") == 90
        assert _fuzzy_score("attijariwafa bank", "ATW", "Attijariwafa Bank") == 85
        assert _fuzzy_score("atti", "ATW", "Attijariwafa Bank") == 75

    def test_fuzzy_score_contains_token_and_empty_paths(self):
        from bvc_mcp.server import _fuzzy_score

        assert _fuzzy_score("tw", "ATW", "Attijariwafa Bank") == 60
        assert _fuzzy_score("bank", "ATW", "Attijariwafa Bank") == 50
        assert _fuzzy_score("ban", "ATW", "Attijariwafa Bank") == 50
        assert _fuzzy_score("", "", "") == 100

# ---------------------------------------------------------------------------
# Watchlist MCP tool tests
# ---------------------------------------------------------------------------


class TestWatchlistMCPTools:
    """Tests for watchlist MCP tools, including no-auth/default-owner behaviour."""

    def _make_snapshot(self):
        from bvc_mcp.models import MarketSnapshot, Stock

        stocks = [
            Stock.model_validate(SAMPLE_STOCK_ACTIVE),
            Stock.model_validate(SAMPLE_STOCK_GAINER),
            Stock.model_validate(SAMPLE_STOCK_BCP),
        ]
        return MarketSnapshot(
            success=True,
            lastModified=1773071642,
            timestamp="2026-03-09 15:54:02",
            timestampFrench="lundi 9 mars 2026 16:54:02",
            stocks=stocks,
        )

    def _make_temp_db(self) -> str:
        from bvc_mcp.database import init_db

        f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        f.close()
        init_db(f.name)
        return f.name

    @pytest.mark.asyncio
    async def test_list_watchlists_without_api_key_returns_empty_list(self):
        """Watchlist tools should work without auth and use the default owner namespace."""
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import list_watchlists

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = await list_watchlists()

            data = json.loads(result)
            assert data["result_type"] == "watchlist_list"
            assert data["count"] == 0
            assert data["watchlists"] == []
        finally:
            os.unlink(db_path)


class TestHistoricalMCPTools:
    """Non-regression tests for DB-backed MCP tools."""

    def _make_temp_db(self) -> str:
        from bvc_mcp.database import init_db

        f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        f.close()
        init_db(f.name)
        return f.name

    def _make_stock(self, raw: dict):
        from bvc_mcp.models import Stock

        return Stock.model_validate(raw)

    def _make_snapshot(self, timestamp: str, atw: float, iam: float, bcp: float):
        from bvc_mcp.models import MarketSnapshot

        atw_raw = dict(SAMPLE_STOCK_ACTIVE)
        atw_raw["Cours"] = atw
        atw_raw["Variation"] = 0.0

        iam_raw = dict(SAMPLE_STOCK_GAINER)
        iam_raw["Cours"] = iam
        iam_raw["Variation"] = 0.0

        bcp_raw = dict(SAMPLE_STOCK_BCP)
        bcp_raw["Cours"] = bcp
        bcp_raw["Variation"] = 0.0

        stocks = [
            self._make_stock(atw_raw),
            self._make_stock(iam_raw),
            self._make_stock(bcp_raw),
        ]
        return MarketSnapshot(
            success=True,
            lastModified=1773071642,
            timestamp=timestamp,
            timestampFrench="lundi 9 mars 2026 16:54:02",
            stocks=stocks,
        )

    def _seed_history_db(self, db_path: str) -> None:
        from bvc_mcp.database import save_snapshot

        snapshots = [
            ("2026-03-07 09:00:00", self._make_snapshot("2026-03-07 09:00:00", 100.0, 50.0, 200.0)),
            ("2026-03-08 09:00:00", self._make_snapshot("2026-03-08 09:00:00", 110.0, 55.0, 198.0)),
            ("2026-03-09 09:00:00", self._make_snapshot("2026-03-09 09:00:00", 121.0, 60.0, 202.0)),
        ]
        for fetched_at, snapshot in snapshots:
            save_snapshot(snapshot, db_path, fetched_at=fetched_at)

    def _seed_long_history_db(self, db_path: str, points: int = 25) -> None:
        from bvc_mcp.database import save_snapshot

        for day in range(1, points + 1):
            fetched_at = f"2026-03-{day:02d} 09:00:00"
            snapshot = self._make_snapshot(
                fetched_at,
                atw=100.0 + day,
                iam=50.0 + (day * 0.5),
                bcp=200.0 - (day * 0.2),
            )
            save_snapshot(snapshot, db_path, fetched_at=fetched_at)

    @pytest.mark.asyncio
    async def test_get_stock_history_returns_db_rows(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_stock_history

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_stock_history("ATW", limit=10))

            assert result["symbol"] == "ATW"
            assert result["data_points"] == 3
            assert result["history"][0]["price"] == 121.0
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_stock_history_returns_not_found_error(self):
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import get_stock_history

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_stock_history("ATW", limit=10))

            assert "error" in result
            assert "No history found" in result["error"]
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_snapshots_list_handles_db_runtime_error(self):
        from bvc_mcp.server import get_snapshots_list

        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.side_effect = RuntimeError("Database operation failed.")
            result = json.loads(await get_snapshots_list())

        assert result == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_snapshots_list_returns_rows_and_clamps_limit(self):
        from bvc_mcp.server import get_snapshots_list

        rows = [{"fetched_at": "2026-03-09 09:00:00"}]
        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.return_value = rows
            result = json.loads(await get_snapshots_list(limit=999))

        awaited_args = mock_db.await_args.args
        assert awaited_args[0].__name__ == "get_snapshots_list"
        assert awaited_args[1] == 100
        assert awaited_args[2] == ANY
        assert result == {"count": 1, "snapshots": rows}

    @pytest.mark.asyncio
    async def test_get_price_evolution_returns_variation(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_price_evolution

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(
                    await get_price_evolution("ATW", "2026-03-07", "2026-03-09")
                )

            assert result["start_price"] == 100.0
            assert result["end_price"] == 121.0
            assert result["total_variation_pct"] == 21.0
            assert result["data_points"] == 3
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_price_evolution_invalid_date_returns_error(self):
        from bvc_mcp.server import get_price_evolution

        result = json.loads(await get_price_evolution("ATW", "2026-99-01", "2026-03-09"))
        assert "error" in result

    @pytest.mark.asyncio
    async def test_get_price_evolution_rejects_reversed_dates(self):
        from bvc_mcp.server import get_price_evolution

        result = json.loads(await get_price_evolution("ATW", "2026-03-10", "2026-03-09"))
        assert "error" in result

    @pytest.mark.asyncio
    async def test_get_price_evolution_handles_empty_range(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_price_evolution

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_price_evolution("ATW", "2027-01-01", "2027-01-02"))

            assert "error" in result
            assert "No data found" in result["error"]
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_price_evolution_handles_db_runtime_error(self):
        from bvc_mcp.server import get_price_evolution

        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.side_effect = RuntimeError("Database operation failed.")
            result = json.loads(await get_price_evolution("ATW", "2026-03-07", "2026-03-09"))

        assert result == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_price_evolution_returns_none_variation_when_start_price_zero(self):
        from bvc_mcp.server import get_price_evolution

        rows = [
            {"price": 0.0, "fetched_at": "2026-03-07 09:00:00"},
            {"price": 10.0, "fetched_at": "2026-03-09 09:00:00"},
        ]
        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.return_value = rows
            result = json.loads(await get_price_evolution("ATW", "2026-03-07", "2026-03-09"))

        assert result["start_price"] == 0.0
        assert result["end_price"] == 10.0
        assert result["total_variation_pct"] is None

    @pytest.mark.asyncio
    async def test_get_rsi_returns_computed_payload(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_rsi

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_rsi("ATW", period=2, limit=10))

            assert result["symbol"] == "ATW"
            assert result["period"] == 2
            assert result["current_rsi"] is not None
            assert result["data_points"] >= 1
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_rsi_returns_no_history_error(self):
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import get_rsi

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_rsi("ATW", period=2, limit=10))

            assert "error" in result
            assert "No history" in result["error"]
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_rsi_can_return_insufficient_data_oversold_and_neutral(self):
        from bvc_mcp.server import get_rsi

        rows = [{"fetched_at": "2026-03-09 09:00:00", "price": 100.0}]
        with (
            patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series,
            patch(
                "bvc_mcp.server.calculate_rsi",
                side_effect=[[None], [10.0], [55.0]],
            ),
        ):
            mock_series.return_value = (rows, [100.0])
            insufficient = json.loads(await get_rsi("ATW", period=2, limit=10))
            oversold = json.loads(await get_rsi("ATW", period=2, limit=10))
            neutral = json.loads(await get_rsi("ATW", period=2, limit=10))

        assert insufficient["interpretation"] == "insufficient_data"
        assert oversold["interpretation"] == "oversold"
        assert neutral["interpretation"] == "neutral"

    @pytest.mark.asyncio
    async def test_get_volume_history_returns_average_and_rows(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_volume_history

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_volume_history("ATW", limit=10))

            assert result["symbol"] == "ATW"
            assert result["data_points"] == 3
            assert result["average_volume_mad"] is not None
            assert len(result["history"]) == 3
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_volume_history_returns_not_found_error(self):
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import get_volume_history

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_volume_history("ATW", limit=10))

            assert "error" in result
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_volume_history_handles_db_runtime_error(self):
        from bvc_mcp.server import get_volume_history

        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.side_effect = RuntimeError("Database operation failed.")
            result = json.loads(await get_volume_history("ATW", limit=10))

        assert result == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_volume_history_handles_missing_volumes(self):
        from bvc_mcp.server import get_volume_history

        rows = [
            {"fetched_at": "2026-03-09 09:00:00", "volume_mad": None, "quantity_traded": 10},
            {"fetched_at": "2026-03-08 09:00:00", "volume_mad": None, "quantity_traded": 5},
        ]
        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.return_value = rows
            result = json.loads(await get_volume_history("ATW", limit=10))

        assert result["average_volume_mad"] is None
        assert result["average_volume_formatted"] == "N/A"

    @pytest.mark.asyncio
    async def test_get_moving_average_returns_payload(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_moving_average

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_moving_average("ATW", period=2, limit=10))

            assert result["symbol"] == "ATW"
            assert result["period"] == 2
            assert result["current_sma"] is not None
            assert result["data_points"] >= 1
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_moving_average_returns_insufficient_data_error(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_moving_average

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_moving_average("ATW", period=10, limit=10))

            assert "error" in result
            assert "Not enough data" in result["error"]
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_moving_average_handles_db_runtime_error(self):
        from bvc_mcp.server import get_moving_average

        with patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series:
            mock_series.side_effect = RuntimeError("Database operation failed.")
            result = json.loads(await get_moving_average("ATW", period=2, limit=10))

        assert result == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_moving_average_handles_no_history(self):
        from bvc_mcp.server import get_moving_average

        with patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series:
            mock_series.return_value = ([], [])
            result = json.loads(await get_moving_average("ATW", period=2, limit=10))

        assert "No history" in result["error"]

    @pytest.mark.asyncio
    async def test_get_moving_average_can_return_below_signal(self):
        from bvc_mcp.server import get_moving_average

        rows = [
            {"fetched_at": "2026-03-07 09:00:00", "price": 10.0},
            {"fetched_at": "2026-03-08 09:00:00", "price": 10.0},
            {"fetched_at": "2026-03-09 09:00:00", "price": 9.0},
        ]
        with patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series:
            mock_series.return_value = (rows, [10.0, 10.0, 9.0])
            result = json.loads(await get_moving_average("ATW", period=2, limit=10))

        assert result["signal"] == "Price is below MA2"

    @pytest.mark.asyncio
    async def test_get_bollinger_bands_returns_payload(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_bollinger_bands

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_bollinger_bands("ATW", period=2, limit=10))

            assert result["symbol"] == "ATW"
            assert result["current_bands"] is not None
            assert result["signal"] in {"above_upper", "below_lower", "inside_bands", "insufficient_data"}
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_bollinger_bands_returns_insufficient_data_error(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_bollinger_bands

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_bollinger_bands("ATW", period=10, limit=10))

            assert "error" in result
            assert "Need 10 data points" in result["error"]
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_bollinger_bands_handles_db_runtime_error(self):
        from bvc_mcp.server import get_bollinger_bands

        with patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series:
            mock_series.side_effect = RuntimeError("Database operation failed.")
            result = json.loads(await get_bollinger_bands("ATW", period=2, limit=10))

        assert result == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_bollinger_bands_handles_no_history(self):
        from bvc_mcp.server import get_bollinger_bands

        with patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series:
            mock_series.return_value = ([], [])
            result = json.loads(await get_bollinger_bands("ATW", period=2, limit=10))

        assert "No history" in result["error"]

    @pytest.mark.asyncio
    async def test_get_bollinger_bands_covers_signal_variants(self):
        from bvc_mcp.server import get_bollinger_bands

        rows = [
            {"fetched_at": "2026-03-08 09:00:00", "price": 100.0},
            {"fetched_at": "2026-03-09 09:00:00", "price": 100.0},
        ]
        with (
            patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series,
            patch(
                "bvc_mcp.server.calculate_bollinger_bands",
                side_effect=[
                    [None, None],
                    [None, {"upper": 90.0, "middle": 80.0, "lower": 70.0}],
                    [None, {"upper": 130.0, "middle": 120.0, "lower": 110.0}],
                ],
            ),
        ):
            mock_series.return_value = (rows, [100.0, 100.0])
            insufficient = json.loads(await get_bollinger_bands("ATW", period=2, limit=10))
            above = json.loads(await get_bollinger_bands("ATW", period=2, limit=10))
            below = json.loads(await get_bollinger_bands("ATW", period=2, limit=10))

        assert insufficient["signal"] == "insufficient_data"
        assert above["signal"] == "above_upper"
        assert below["signal"] == "below_lower"

    @pytest.mark.asyncio
    async def test_get_volatility_returns_market_comparison(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_volatility

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_volatility("ATW", period=3))

            assert result["symbol"] == "ATW"
            assert result["annualized"] is True
            assert result["volatility_pct"] is not None
            assert result["market_sample_size"] >= 1
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_volatility_returns_unknown_when_market_average_missing(self):
        from bvc_mcp.server import get_volatility

        with (
            patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db,
            patch("bvc_mcp.server.calculate_volatility", side_effect=[1.23, None]),
        ):
            mock_db.return_value = {"ATW": [100.0, 101.0, 102.0]}
            result = json.loads(await get_volatility("ATW", period=3))

        assert result["vs_market_avg"] == "unknown"

    @pytest.mark.asyncio
    async def test_get_volatility_handles_db_runtime_error(self):
        from bvc_mcp.server import get_volatility

        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.side_effect = RuntimeError("Database operation failed.")
            result = json.loads(await get_volatility("ATW", period=3))

        assert result == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_volatility_handles_missing_history(self):
        from bvc_mcp.server import get_volatility

        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.return_value = {"IAM": [1.0, 2.0, 3.0]}
            result = json.loads(await get_volatility("ATW", period=3))

        assert "No history" in result["error"]

    @pytest.mark.asyncio
    async def test_get_volatility_handles_insufficient_symbol_data(self):
        from bvc_mcp.server import get_volatility

        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.return_value = {"ATW": [100.0]}
            result = json.loads(await get_volatility("ATW", period=3))

        assert "Insufficient data" in result["error"]

    @pytest.mark.asyncio
    async def test_get_volatility_covers_high_low_and_average_labels(self):
        from bvc_mcp.server import get_volatility

        with (
            patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db,
            patch(
                "bvc_mcp.server.calculate_volatility",
                side_effect=[3.1, 1.0, 1.0, 1.0, 0.4, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            ),
        ):
            mock_db.return_value = {"ATW": [1, 2, 3], "IAM": [1, 2, 3], "BCP": [1, 2, 3]}
            high = json.loads(await get_volatility("ATW", period=3))
            low = json.loads(await get_volatility("ATW", period=3))
            avg = json.loads(await get_volatility("ATW", period=3))

        assert high["vs_market_avg"] == "high"
        assert low["vs_market_avg"] == "low"
        assert avg["vs_market_avg"] == "average"

    @pytest.mark.asyncio
    async def test_get_momentum_returns_multiple_periods(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_momentum

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_momentum("ATW", periods=[1, 2]))

            assert result["symbol"] == "ATW"
            assert len(result["momentum"]) == 2
            assert {item["period"] for item in result["momentum"]} == {1, 2}
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_momentum_default_periods_and_no_history_error(self):
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import get_momentum

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_momentum("ATW"))

            assert "error" in result
            assert "No history" in result["error"]
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_momentum_handles_db_runtime_error(self):
        from bvc_mcp.server import get_momentum

        with patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series:
            mock_series.side_effect = RuntimeError("Database operation failed.")
            result = json.loads(await get_momentum("ATW"))

        assert result == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_momentum_emits_neutral_and_insufficient_signals(self):
        from bvc_mcp.server import get_momentum

        rows = [
            {"fetched_at": "2026-03-07 09:00:00", "price": 100.0},
            {"fetched_at": "2026-03-08 09:00:00", "price": 100.0},
            {"fetched_at": "2026-03-09 09:00:00", "price": 100.0},
        ]
        with patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series:
            mock_series.return_value = (rows, [100.0, 100.0, 100.0])
            result = json.loads(await get_momentum("ATW", periods=[1, 5]))

        by_period = {item["period"]: item for item in result["momentum"]}
        assert by_period[1]["signal"] == "neutral"
        assert by_period[5]["signal"] == "insufficient_data"

    @pytest.mark.asyncio
    async def test_get_momentum_can_emit_bearish_signal(self):
        from bvc_mcp.server import get_momentum

        rows = [
            {"fetched_at": "2026-03-07 09:00:00", "price": 100.0},
            {"fetched_at": "2026-03-08 09:00:00", "price": 90.0},
        ]
        with patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series:
            mock_series.return_value = (rows, [100.0, 90.0])
            result = json.loads(await get_momentum("ATW", periods=[1]))

        assert result["momentum"][0]["signal"] == "bearish"

    @pytest.mark.asyncio
    async def test_get_support_resistance_returns_levels(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_support_resistance

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_support_resistance("ATW", limit=5))

            assert result["symbol"] == "ATW"
            assert result["support"] is not None
            assert result["resistance"] is not None
            assert result["support"] <= result["resistance"]
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_support_resistance_no_history_error(self):
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import get_support_resistance

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_support_resistance("ATW", limit=5))

            assert "error" in result
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_support_resistance_handles_db_runtime_error(self):
        from bvc_mcp.server import get_support_resistance

        with patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series:
            mock_series.side_effect = RuntimeError("Database operation failed.")
            result = json.loads(await get_support_resistance("ATW", limit=5))

        assert result == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_correlation_returns_value(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_correlation

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_correlation("ATW", "IAM", period=3))

            assert result["symbol1"] == "ATW"
            assert result["symbol2"] == "IAM"
            assert result["correlation"] is not None
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_correlation_identical_symbols_short_circuit(self):
        from bvc_mcp.server import get_correlation

        result = json.loads(await get_correlation("ATW", "ATW", period=3))
        assert result["correlation"] == 1.0
        assert result["interpretation"] == "identical symbols"

    @pytest.mark.asyncio
    async def test_get_correlation_missing_history_errors(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_correlation

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(await get_correlation("ATW", "ZZZ", period=3))

            assert "error" in result
            assert "No history" in result["error"]
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_correlation_handles_db_runtime_error(self):
        from bvc_mcp.server import get_correlation

        with patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series:
            mock_series.side_effect = RuntimeError("Database operation failed.")
            result = json.loads(await get_correlation("ATW", "IAM", period=3))

        assert result == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_correlation_interprets_none_and_negative_values(self):
        from bvc_mcp.server import get_correlation

        with (
            patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series,
            patch("bvc_mcp.server.calculate_correlation", side_effect=[None, -0.8]),
        ):
            mock_series.side_effect = [
                ([], [1.0, 2.0, 3.0]),
                ([], [3.0, 2.0, 1.0]),
                ([], [1.0, 2.0, 3.0]),
                ([], [3.0, 2.0, 1.0]),
            ]
            first = json.loads(await get_correlation("ATW", "IAM", period=3))
            second = json.loads(await get_correlation("ATW", "IAM", period=3))

        assert first["interpretation"] == "insufficient_data"
        assert second["interpretation"] == "strong negative correlation"

    @pytest.mark.asyncio
    async def test_get_correlation_covers_remaining_interpretations(self):
        from bvc_mcp.server import get_correlation

        with (
            patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series,
            patch("bvc_mcp.server.calculate_correlation", side_effect=[0.5, -0.5, 0.1]),
        ):
            mock_series.side_effect = [
                ([], [1.0, 2.0, 3.0]), ([], [1.0, 2.0, 3.0]),
                ([], [1.0, 2.0, 3.0]), ([], [1.0, 2.0, 3.0]),
                ([], [1.0, 2.0, 3.0]), ([], [1.0, 2.0, 3.0]),
            ]
            moderate_pos = json.loads(await get_correlation("ATW", "IAM", period=3))
            moderate_neg = json.loads(await get_correlation("ATW", "IAM", period=3))
            weak = json.loads(await get_correlation("ATW", "IAM", period=3))

        assert moderate_pos["interpretation"] == "moderate positive correlation"
        assert moderate_neg["interpretation"] == "moderate negative correlation"
        assert weak["interpretation"] == "weak / no significant correlation"

    @pytest.mark.asyncio
    async def test_get_correlation_reports_missing_history_for_first_symbol(self):
        from bvc_mcp.server import get_correlation

        with patch("bvc_mcp.server._price_series", new_callable=AsyncMock) as mock_series:
            mock_series.side_effect = [([], []), ([], [1.0, 2.0, 3.0])]
            result = json.loads(await get_correlation("ATW", "IAM", period=3))

        assert "No history for 'ATW'" in result["error"]

    @pytest.mark.asyncio
    async def test_get_top_performers_period_returns_ranked_symbols(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_top_performers_period

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(
                    await get_top_performers_period("2026-03-07", "2026-03-09", limit=2)
                )

            assert result["count"] == 2
            assert result["top_performers"][0]["symbol"] == "ATW"
            assert result["top_performers"][0]["rank"] == 1
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_top_performers_period_validation_and_error_paths(self):
        from bvc_mcp.server import get_top_performers_period

        invalid = json.loads(await get_top_performers_period("bad-date", "2026-03-09"))
        reversed_dates = json.loads(await get_top_performers_period("2026-03-10", "2026-03-09"))
        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.side_effect = [RuntimeError("Database operation failed."), []]
            db_error = json.loads(await get_top_performers_period("2026-03-07", "2026-03-09"))
            empty = json.loads(await get_top_performers_period("2026-03-07", "2026-03-09"))

        assert "Invalid from_date" in invalid["error"]
        assert "from_date must be ≤ to_date" in reversed_dates["error"]
        assert db_error == {"error": "Database operation failed."}
        assert "No data found" in empty["error"]

    @pytest.mark.asyncio
    async def test_get_worst_performers_period_returns_ranked_symbols(self):
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import get_worst_performers_period

            with patch("bvc_mcp.server.DB_PATH", db_path):
                result = json.loads(
                    await get_worst_performers_period("2026-03-07", "2026-03-09", limit=2)
                )

            assert result["count"] == 2
            assert result["worst_performers"][0]["symbol"] == "BCP"
            assert result["worst_performers"][0]["rank"] == 1
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_worst_performers_period_validation_and_error_paths(self):
        from bvc_mcp.server import get_worst_performers_period

        invalid = json.loads(await get_worst_performers_period("2026-03-07", "bad-date"))
        reversed_dates = json.loads(await get_worst_performers_period("2026-03-10", "2026-03-09"))
        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.side_effect = [RuntimeError("Database operation failed."), []]
            db_error = json.loads(await get_worst_performers_period("2026-03-07", "2026-03-09"))
            empty = json.loads(await get_worst_performers_period("2026-03-07", "2026-03-09"))

        assert "Invalid to_date" in invalid["error"]
        assert "from_date must be ≤ to_date" in reversed_dates["error"]
        assert db_error == {"error": "Database operation failed."}
        assert "No data found" in empty["error"]

    @pytest.mark.asyncio
    async def test_get_watchlist_performance_returns_ranked_results(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 121.0, 60.0, 202.0)
        db_path = self._make_temp_db()
        try:
            self._seed_history_db(db_path)
            from bvc_mcp.server import create_watchlist, get_watchlist_performance

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.REQUIRE_WATCHLIST_API_KEY", False),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
            ):
                mock_fetch.return_value = snapshot
                await create_watchlist("perf", "ATW,BCP", api_key="user-key")
                result = json.loads(
                    await get_watchlist_performance(
                        "perf", "2026-03-07", "2026-03-09", api_key="user-key"
                    )
                )

            assert result["watchlist"] == "perf"
            assert result["stock_count"] == 2
            assert len(result["performance"]) == 2
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_sector_performance_returns_segments(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_sector_performance

            result = json.loads(await get_sector_performance())

        assert result["session_timestamp"] == "2026-03-09 15:54:02"
        assert len(result["segments"]) >= 1
        assert "segment_code" in result["segments"][0]

    @pytest.mark.asyncio
    async def test_get_sector_performance_handles_fetch_error(self):
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.side_effect = RuntimeError("upstream down")
            from bvc_mcp.server import get_sector_performance

            result = json.loads(await get_sector_performance())

        assert result == {"error": "upstream down"}

    @pytest.mark.asyncio
    async def test_get_sector_performance_counts_losers_and_unchanged(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        for stock in snapshot.stocks:
            if stock.symbol == "ATW":
                stock.variation = -1.0
            elif stock.symbol == "IAM":
                stock.variation = 0.0
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_sector_performance

            result = json.loads(await get_sector_performance())

        segment = result["segments"][0]
        assert segment["losers"] >= 1
        assert segment["unchanged"] >= 1

    @pytest.mark.asyncio
    async def test_get_sector_performance_counts_gainers(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        for stock in snapshot.stocks:
            stock.variation = 1.0
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import get_sector_performance

            result = json.loads(await get_sector_performance())

        assert result["segments"][0]["gainers"] >= 1

    @pytest.mark.asyncio
    async def test_screen_stocks_filters_results(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        for stock in snapshot.stocks:
            if stock.symbol == "IAM":
                stock.variation = 3.66
            else:
                stock.variation = 0.0
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import screen_stocks

            result = json.loads(await screen_stocks(min_variation=1.0, only_gainers=True))

        assert result["count"] == 1
        assert result["stocks"][0]["symbol"] == "IAM"

    @pytest.mark.asyncio
    async def test_screen_stocks_only_losers(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        for stock in snapshot.stocks:
            if stock.symbol == "ATW":
                stock.variation = -4.74
            else:
                stock.variation = 0.5
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import screen_stocks

            result = json.loads(await screen_stocks(only_losers=True))

        assert result["count"] == 1
        assert result["stocks"][0]["symbol"] == "ATW"

    @pytest.mark.asyncio
    async def test_screen_stocks_handles_fetch_error_and_extra_filters(self):
        from bvc_mcp.server import screen_stocks

        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.side_effect = RuntimeError("upstream down")
            fetch_error = json.loads(await screen_stocks())

        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        for stock in snapshot.stocks:
            if stock.symbol == "ATW":
                stock.variation = 2.0
                stock.volume_mad = 1_000_000
            elif stock.symbol == "IAM":
                stock.variation = None
                stock.volume_mad = None
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            filtered = json.loads(
                await screen_stocks(
                    max_variation=2.0,
                    min_volume_mad=100_000,
                    min_price=600,
                    max_price=700,
                )
            )

        assert fetch_error == {"error": "upstream down"}
        assert filtered["count"] == 1
        assert filtered["stocks"][0]["symbol"] == "ATW"

    @pytest.mark.asyncio
    async def test_get_unusual_volume_returns_candidates(self):
        db_path = self._make_temp_db()
        try:
            self._seed_long_history_db(db_path, points=12)
            snapshot = self._make_snapshot("2026-03-12 15:54:02", 150.0, 80.0, 180.0)
            # Force ATW current volume far above its historical average
            for stock in snapshot.stocks:
                if stock.symbol == "ATW":
                    stock.volume_mad = 200_000_000.0

            from bvc_mcp.server import get_unusual_volume

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock,
            ):
                mock.return_value = snapshot
                result = json.loads(await get_unusual_volume(threshold_multiplier=2.0, min_history=5))

            assert result["count"] >= 1
            assert any(item["symbol"] == "ATW" for item in result["unusual_activity"])
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_unusual_volume_and_breakout_cover_skip_paths(self):
        snapshot = self._make_snapshot("2026-03-25 15:54:02", 140.0, 62.0, 195.0)
        snapshot.stocks[0].volume_mad = 50_000
        snapshot.stocks[0].variation = 2.0
        snapshot.stocks[1].variation = 1.0
        snapshot.stocks[2].variation = 1.0

        from bvc_mcp.server import get_breakout_candidates, get_unusual_volume

        with (
            patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
            patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db,
        ):
            mock_fetch.return_value = snapshot
            mock_db.side_effect = [
                {},
                [{"symbol": "ATW", "above_ma": True, "ma_value": 100.0}],
            ]
            unusual = json.loads(await get_unusual_volume())
            breakout = json.loads(await get_breakout_candidates())

        assert unusual["count"] == 0
        assert breakout["count"] == 1
        assert breakout["breakout_candidates"][0]["symbol"] == "ATW"

    @pytest.mark.asyncio
    async def test_get_breakout_candidates_returns_ranked_results(self):
        db_path = self._make_temp_db()
        try:
            self._seed_long_history_db(db_path, points=25)
            snapshot = self._make_snapshot("2026-03-25 15:54:02", 140.0, 62.0, 195.0)
            # Keep ATW positive and above its recent MA
            for stock in snapshot.stocks:
                if stock.symbol == "ATW":
                    stock.variation = 2.5
                elif stock.symbol == "IAM":
                    stock.variation = -0.5

            from bvc_mcp.server import get_breakout_candidates

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock,
            ):
                mock.return_value = snapshot
                result = json.loads(await get_breakout_candidates(period=20))

            assert result["ma_period"] == 20
            assert any(item["symbol"] == "ATW" for item in result["breakout_candidates"])
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_market_breadth_returns_interpretation(self):
        db_path = self._make_temp_db()
        try:
            self._seed_long_history_db(db_path, points=25)
            snapshot = self._make_snapshot("2026-03-25 15:54:02", 140.0, 62.0, 195.0)
            for stock in snapshot.stocks:
                if stock.symbol == "ATW":
                    stock.variation = 2.0
                elif stock.symbol == "IAM":
                    stock.variation = 1.5
                elif stock.symbol == "BCP":
                    stock.variation = -1.0

            from bvc_mcp.server import get_market_breadth

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock,
            ):
                mock.return_value = snapshot
                result = json.loads(await get_market_breadth())

            assert result["session_timestamp"] == "2026-03-25 15:54:02"
            assert result["interpretation"] in {"bullish", "bearish", "neutral"}
            assert "pct_above_ma20" in result
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_market_breadth_handles_fetch_error(self):
        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.side_effect = RuntimeError("upstream down")
            from bvc_mcp.server import get_market_breadth

            result = json.loads(await get_market_breadth())

        assert result == {"error": "upstream down"}

    @pytest.mark.asyncio
    async def test_get_market_breadth_handles_db_runtime_error(self):
        snapshot = self._make_snapshot("2026-03-25 15:54:02", 140.0, 62.0, 195.0)
        with (
            patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
            patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db,
        ):
            mock_fetch.return_value = snapshot
            mock_db.side_effect = RuntimeError("Database operation failed.")
            from bvc_mcp.server import get_market_breadth

            result = json.loads(await get_market_breadth())

        assert result == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_market_breadth_can_be_bearish(self):
        db_path = self._make_temp_db()
        try:
            self._seed_long_history_db(db_path, points=25)
            snapshot = self._make_snapshot("2026-03-25 15:54:02", 140.0, 62.0, 195.0)
            for stock in snapshot.stocks:
                if stock.symbol == "ATW":
                    stock.variation = -2.0
                    stock.volume_mad = 100_000_000.0
                elif stock.symbol == "IAM":
                    stock.variation = -1.5
                    stock.volume_mad = 90_000_000.0
                elif stock.symbol == "BCP":
                    stock.variation = 0.5
                    stock.volume_mad = 1_000.0

            from bvc_mcp.server import get_market_breadth

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock,
                patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db,
            ):
                mock.return_value = snapshot
                mock_db.return_value = [{"symbol": "ATW", "above_ma": False}, {"symbol": "IAM", "above_ma": False}]
                result = json.loads(await get_market_breadth())

            assert result["interpretation"] == "bearish"
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_get_market_breadth_can_be_neutral(self):
        snapshot = self._make_snapshot("2026-03-25 15:54:02", 140.0, 62.0, 195.0)
        for stock in snapshot.stocks:
            stock.variation = 0.0
            stock.volume_mad = 10_000.0

        with (
            patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
            patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db,
        ):
            mock_fetch.return_value = snapshot
            mock_db.return_value = []
            from bvc_mcp.server import get_market_breadth

            result = json.loads(await get_market_breadth())

        assert result["interpretation"] == "neutral"

    @pytest.mark.asyncio
    async def test_get_unusual_volume_handles_fetch_and_db_errors(self):
        from bvc_mcp.server import get_unusual_volume

        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = RuntimeError("upstream down")
            fetch_error = json.loads(await get_unusual_volume())

        snapshot = self._make_snapshot("2026-03-25 15:54:02", 140.0, 62.0, 195.0)
        with (
            patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
            patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db,
        ):
            mock_fetch.return_value = snapshot
            mock_db.side_effect = RuntimeError("Database operation failed.")
            db_error = json.loads(await get_unusual_volume())

        assert fetch_error == {"error": "upstream down"}
        assert db_error == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_breakout_candidates_handles_fetch_and_db_errors(self):
        from bvc_mcp.server import get_breakout_candidates

        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = RuntimeError("upstream down")
            fetch_error = json.loads(await get_breakout_candidates())

        snapshot = self._make_snapshot("2026-03-25 15:54:02", 140.0, 62.0, 195.0)
        with (
            patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
            patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db,
        ):
            mock_fetch.return_value = snapshot
            mock_db.side_effect = RuntimeError("Database operation failed.")
            db_error = json.loads(await get_breakout_candidates())

        assert fetch_error == {"error": "upstream down"}
        assert db_error == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_stock_history_handles_db_runtime_error(self):
        from bvc_mcp.server import get_stock_history

        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.side_effect = RuntimeError("Database operation failed.")
            result = json.loads(await get_stock_history("ATW"))

        assert result == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_get_rsi_handles_db_runtime_error(self):
        from bvc_mcp.server import get_rsi

        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.side_effect = RuntimeError("Database operation failed.")
            result = json.loads(await get_rsi("ATW"))

        assert result == {"error": "Database operation failed."}

    @pytest.mark.asyncio
    async def test_list_watchlists_handles_db_runtime_error(self):
        from bvc_mcp.server import list_watchlists

        with patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db:
            mock_db.side_effect = RuntimeError("Database operation failed.")
            result = json.loads(await list_watchlists())

        assert result == {"error": "Database operation failed."}


class TestServerDbWrapper:
    """Tests for the DB wrapper that prevents raw SQLite exceptions from escaping."""

    def _make_snapshot(self, timestamp: str, atw: float, iam: float, bcp: float):
        from bvc_mcp.models import MarketSnapshot, Stock

        atw_raw = dict(SAMPLE_STOCK_ACTIVE)
        atw_raw["Cours"] = atw
        atw_raw["Variation"] = 0.0

        iam_raw = dict(SAMPLE_STOCK_GAINER)
        iam_raw["Cours"] = iam
        iam_raw["Variation"] = 0.0

        bcp_raw = dict(SAMPLE_STOCK_BCP)
        bcp_raw["Cours"] = bcp
        bcp_raw["Variation"] = 0.0

        stocks = [
            Stock.model_validate(atw_raw),
            Stock.model_validate(iam_raw),
            Stock.model_validate(bcp_raw),
        ]
        return MarketSnapshot(
            success=True,
            lastModified=1773071642,
            timestamp=timestamp,
            timestampFrench="lundi 9 mars 2026 16:54:02",
            stocks=stocks,
        )

    def _make_temp_db(self) -> str:
        from bvc_mcp.database import init_db

        f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        f.close()
        init_db(f.name)
        return f.name

    @pytest.mark.asyncio
    async def test_run_db_call_converts_sqlite_error_to_runtime_error(self):
        from bvc_mcp.server import _run_db_call

        def explode():
            raise sqlite3.OperationalError("disk I/O error")

        with pytest.raises(RuntimeError, match="Database operation failed."):
            await _run_db_call(explode)

    @pytest.mark.asyncio
    async def test_create_and_get_watchlist_without_api_key(self):
        """Creating and reading a watchlist should succeed when api_key is omitted."""
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import create_watchlist, get_watchlist

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
            ):
                mock_fetch.return_value = snapshot

                created = json.loads(await create_watchlist("banks", "ATW,IAM"))
                fetched = json.loads(await get_watchlist("banks"))

            assert created["name"] == "banks"
            assert created["count"] == 2
            assert created["symbols"] == ["ATW", "IAM"]
            assert created["rejected"] == []
            assert fetched["name"] == "banks"
            assert fetched["count"] == 2
            assert {stock["symbol"] for stock in fetched["stocks"]} == {"ATW", "IAM"}
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_watchlist_without_api_key_is_not_an_auth_error(self):
        """Missing api_key should not trigger an auth failure for watchlist tools."""
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import add_to_watchlist, create_watchlist

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
            ):
                mock_fetch.return_value = snapshot

                await create_watchlist("default_ns", "ATW")
                added = json.loads(await add_to_watchlist("default_ns", "BCP"))

            assert added["success"] is True
            assert added["watchlist"] == "default_ns"
            assert added["symbol"] == "BCP"
            assert "error" not in added
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_watchlist_requires_api_key_when_strict_mode_enabled(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import create_watchlist

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.REQUIRE_WATCHLIST_API_KEY", True),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
            ):
                mock_fetch.return_value = snapshot
                result = json.loads(await create_watchlist("banks", "ATW"))

            assert result["error"].startswith("This watchlist operation requires caller identity")
            assert "hint" in result
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_watchlist_accepts_api_key_when_strict_mode_enabled(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import create_watchlist, list_watchlists

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.REQUIRE_WATCHLIST_API_KEY", True),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
            ):
                mock_fetch.return_value = snapshot
                created = json.loads(await create_watchlist("banks", "ATW", api_key="user-key"))
                listed = json.loads(await list_watchlists(api_key="user-key"))

            assert created["name"] == "banks"
            assert listed["count"] == 1
            assert listed["watchlists"][0]["name"] == "banks"
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_watchlist_accepts_authorization_header_when_strict_mode_enabled(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import create_watchlist, list_watchlists

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.REQUIRE_WATCHLIST_API_KEY", True),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
                patch("bvc_mcp.server.get_http_headers", return_value={"authorization": "Bearer session-token"}),
            ):
                mock_fetch.return_value = snapshot
                created = json.loads(await create_watchlist("banks", "ATW"))
                listed = json.loads(await list_watchlists())

            assert created["name"] == "banks"
            assert listed["count"] == 1
            assert listed["watchlists"][0]["name"] == "banks"
        finally:
            os.unlink(db_path)

    async def test_watchlist_uses_stable_client_id_across_different_sessions(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import create_watchlist, list_watchlists

            class CreateContext:
                client_id = "chatgpt-client-123"
                session_id = "session-a"

            class ListContext:
                client_id = "chatgpt-client-123"
                session_id = "session-b"

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.REQUIRE_WATCHLIST_API_KEY", True),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
                patch("bvc_mcp.server.get_context", side_effect=[CreateContext(), ListContext()]),
            ):
                mock_fetch.return_value = snapshot
                created = json.loads(await create_watchlist("banks", "ATW"))
                listed = json.loads(await list_watchlists())

            assert created["name"] == "banks"
            assert listed["count"] == 1
            assert listed["watchlists"][0]["name"] == "banks"
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_watchlist_defaults_to_shared_owner_without_identity(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import create_watchlist, list_watchlists

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.REQUIRE_WATCHLIST_API_KEY", False),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
                patch("bvc_mcp.server.get_context", side_effect=RuntimeError("No active context found.")),
            ):
                mock_fetch.return_value = snapshot
                created = json.loads(await create_watchlist("banks", "ATW"))
                listed = json.loads(await list_watchlists())

            assert created["name"] == "banks"
            assert listed["count"] == 1
            assert listed["watchlists"][0]["name"] == "banks"
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_watchlist_tool_validations_and_runtime_errors(self):
        from bvc_mcp.server import (
            add_to_watchlist,
            create_watchlist,
            delete_watchlist,
            get_watchlist,
            get_watchlist_performance,
            remove_from_watchlist,
        )

        invalid_name = json.loads(await create_watchlist("bad name!", "ATW"))
        invalid_symbol = json.loads(await create_watchlist("banks", "BAD-SYM"))
        invalid_get = json.loads(await get_watchlist("bad name!"))
        invalid_perf = json.loads(
            await get_watchlist_performance("bad name!", "2026-03-07", "2026-03-09")
        )
        invalid_add_name = json.loads(await add_to_watchlist("bad name!", "ATW"))
        invalid_add_symbol = json.loads(await add_to_watchlist("banks", "BAD-SYM"))
        invalid_remove_name = json.loads(await remove_from_watchlist("bad name!", "ATW"))
        invalid_remove_symbol = json.loads(await remove_from_watchlist("banks", "BAD-SYM"))
        invalid_delete = json.loads(await delete_watchlist("bad name!"))

        assert "Invalid watchlist name" in invalid_name["error"]
        assert "Invalid symbol" in invalid_symbol["error"]
        assert "Invalid watchlist name format" in invalid_get["error"]
        assert "Invalid watchlist name format" in invalid_perf["error"]
        assert "Invalid watchlist name format" in invalid_add_name["error"]
        assert "Invalid symbol format" in invalid_add_symbol["error"]
        assert "Invalid watchlist name format" in invalid_remove_name["error"]
        assert "Invalid symbol format" in invalid_remove_symbol["error"]
        assert "Invalid watchlist name format" in invalid_delete["error"]

    @pytest.mark.asyncio
    async def test_watchlist_tools_cover_secondary_error_paths(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import (
                add_to_watchlist,
                create_watchlist,
                delete_watchlist,
                get_watchlist,
                get_watchlist_performance,
                list_watchlists,
                remove_from_watchlist,
            )

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
                patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db,
            ):
                mock_fetch.return_value = snapshot
                mock_db.side_effect = [
                    RuntimeError("Database operation failed."),
                    RuntimeError("Database operation failed."),
                    RuntimeError("Database operation failed."),
                    [],
                    [{"symbol": "ATW"}],
                    RuntimeError("Database operation failed."),
                    RuntimeError("Database operation failed."),
                    RuntimeError("Database operation failed."),
                ]

                create_err = json.loads(await create_watchlist("banks", "ATW"))
                get_err = json.loads(await get_watchlist("banks"))
                add_missing_symbol = json.loads(await add_to_watchlist("banks", "ZZZ"))
                remove_err = json.loads(await remove_from_watchlist("banks", "ATW"))
                perf_empty = json.loads(
                    await get_watchlist_performance("banks", "2026-03-07", "2026-03-09")
                )
                perf_db_err = json.loads(
                    await get_watchlist_performance("banks", "2026-03-07", "2026-03-09")
                )
                list_err = json.loads(await list_watchlists())
                delete_err = json.loads(await delete_watchlist("banks"))

            assert create_err == {"error": "Database operation failed."}
            assert get_err == {"error": "Database operation failed."}
            assert "not found on the BVC" in add_missing_symbol["error"]
            assert remove_err == {"error": "Database operation failed."}
            assert "not found or is empty" in perf_empty["error"]
            assert perf_db_err == {"error": "Database operation failed."}
            assert list_err == {"error": "Database operation failed."}
            assert delete_err == {"error": "Database operation failed."}
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_watchlist_tools_cover_auth_and_live_data_edge_cases(self):
        snapshot = self._make_snapshot("2026-03-09 15:54:02", 667.9, 180.5, 320.0)
        db_path = self._make_temp_db()
        try:
            from bvc_mcp.server import (
                add_to_watchlist,
                create_watchlist,
                delete_watchlist,
                get_watchlist,
                get_watchlist_performance,
                list_watchlists,
                remove_from_watchlist,
            )

            with patch("bvc_mcp.server.REQUIRE_WATCHLIST_API_KEY", True):
                get_auth = json.loads(await get_watchlist("banks"))
                perf_auth = json.loads(
                    await get_watchlist_performance("banks", "2026-03-07", "2026-03-09")
                )
                list_auth = json.loads(await list_watchlists())
                add_auth = json.loads(await add_to_watchlist("banks", "ATW"))
                remove_auth = json.loads(await remove_from_watchlist("banks", "ATW"))
                delete_auth = json.loads(await delete_watchlist("banks"))

            assert "requires caller identity" in get_auth["error"]
            assert "requires caller identity" in perf_auth["error"]
            assert "requires caller identity" in list_auth["error"]
            assert "requires caller identity" in add_auth["error"]
            assert "requires caller identity" in remove_auth["error"]
            assert "requires caller identity" in delete_auth["error"]

            with (
                patch("bvc_mcp.server.DB_PATH", db_path),
                patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock_fetch,
                patch("bvc_mcp.server._run_db_call", new_callable=AsyncMock) as mock_db,
            ):
                mock_fetch.side_effect = [
                    RuntimeError("upstream down"),
                    snapshot,
                    RuntimeError("upstream down"),
                    snapshot,
                ]
                mock_db.side_effect = [
                    {"name": "banks", "created_at": "now", "stocks": [{"symbol": "ATW"}, {"symbol": "MISS"}]},
                    ["ATW", "MISS"],
                    [],
                    ValueError("watchlist exists"),
                ]

                create_fetch_err = json.loads(await create_watchlist("banks", "ATW"))
                create_none_valid = json.loads(await create_watchlist("banks", "ZZZ"))
                get_fetch_err = json.loads(await get_watchlist("banks"))
                perf_missing_data = json.loads(
                    await get_watchlist_performance("banks", "2026-03-07", "2026-03-09")
                )
                create_value_error = json.loads(await create_watchlist("banks", "ATW"))

            assert create_fetch_err == {"error": "upstream down"}
            assert "No valid symbols found" in create_none_valid["error"]
            assert get_fetch_err == {"error": "upstream down"}
            assert perf_missing_data["performance"][0]["symbol"] == "ATW"
            assert perf_missing_data["performance"][0]["error"] == "No data in the selected date range."
            assert perf_missing_data["avg_portfolio_variation_display"] == "N/A"
            assert create_value_error == {"error": "watchlist exists"}
        finally:
            os.unlink(db_path)


class TestServerEntryPoints:
    @pytest.mark.asyncio
    async def test_health_check_returns_ok_payload(self):
        from bvc_mcp.server import health_check

        response = await health_check(MagicMock())
        payload = json.loads(response.body.decode())
        assert payload["status"] == "ok"
        assert "cache" in payload

    @pytest.mark.asyncio
    async def test_openai_apps_challenge_returns_token_when_configured(self):
        from bvc_mcp.server import openai_apps_challenge

        with patch("bvc_mcp.server.OPENAI_APPS_CHALLENGE_TOKEN", "challenge-token-123"):
            response = await openai_apps_challenge(MagicMock())

        assert response.status_code == 200
        assert response.body.decode() == "challenge-token-123"

    @pytest.mark.asyncio
    async def test_openai_apps_challenge_returns_404_when_missing(self):
        from bvc_mcp.server import openai_apps_challenge

        with patch("bvc_mcp.server.OPENAI_APPS_CHALLENGE_TOKEN", ""):
            response = await openai_apps_challenge(MagicMock())

        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_all_tools_expose_mcp_annotations(self):
        from bvc_mcp.server import mcp

        tools = await mcp._local_provider.list_tools()
        tool_map = {tool.name: tool for tool in tools}

        assert all(tool.annotations is not None for tool in tools)

        assert tool_map["get_market_status"].annotations.readOnlyHint is True
        assert tool_map["get_market_status"].annotations.openWorldHint is True
        assert tool_map["get_market_status"].annotations.destructiveHint is False

        assert tool_map["get_stock_history"].annotations.readOnlyHint is True
        assert tool_map["get_stock_history"].annotations.openWorldHint is False
        assert tool_map["get_stock_history"].annotations.destructiveHint is False

        assert tool_map["create_watchlist"].annotations.readOnlyHint is False
        assert tool_map["create_watchlist"].annotations.openWorldHint is True
        assert tool_map["create_watchlist"].annotations.destructiveHint is False

        assert tool_map["remove_from_watchlist"].annotations.readOnlyHint is False
        assert tool_map["remove_from_watchlist"].annotations.openWorldHint is False
        assert tool_map["remove_from_watchlist"].annotations.destructiveHint is True

        assert tool_map["delete_watchlist"].annotations.readOnlyHint is False
        assert tool_map["delete_watchlist"].annotations.openWorldHint is False
        assert tool_map["delete_watchlist"].annotations.destructiveHint is True

    @pytest.mark.asyncio
    async def test_tools_are_wired_to_bvc_mcp_widget_ui_resource(self):
        from bvc_mcp.server import WIDGET_URI, mcp

        tools = await mcp._local_provider.list_tools()
        tool_map = {tool.name: tool for tool in tools}

        assert tool_map["open_dashboard"].meta["ui"]["resourceUri"] == WIDGET_URI
        assert tool_map["get_market_summary"].meta["ui"]["resourceUri"] == WIDGET_URI
        assert tool_map["get_stock"].meta["ui"]["resourceUri"] == WIDGET_URI
        assert tool_map["get_rsi"].meta["ui"]["resourceUri"] == WIDGET_URI
        assert tool_map["create_watchlist"].meta["ui"]["resourceUri"] == WIDGET_URI

    @pytest.mark.asyncio
    async def test_bvc_mcp_widget_resource_is_registered(self):
        from bvc_mcp.server import (
            WIDGET_RESOURCE_APP,
            WIDGET_RESOURCE_META,
            WIDGET_URI,
            bvc_mcp_dashboard_widget,
            mcp,
        )

        resources = await mcp._local_provider.list_resources()
        resource_map = {str(resource.uri): resource for resource in resources}

        assert WIDGET_URI in resource_map
        assert resource_map[WIDGET_URI].mime_type == "text/html;profile=mcp-app"
        assert WIDGET_RESOURCE_APP.prefers_border is True
        assert WIDGET_RESOURCE_APP.csp is not None
        assert "https://persistent.oaistatic.com" in (WIDGET_RESOURCE_APP.csp.resource_domains or [])
        assert WIDGET_RESOURCE_META["ui"]["prefersBorder"] is True
        assert "csp" in WIDGET_RESOURCE_META["ui"]
        assert "connectDomains" in WIDGET_RESOURCE_META["ui"]["csp"]
        assert "resourceDomains" in WIDGET_RESOURCE_META["ui"]["csp"]

        html = bvc_mcp_dashboard_widget()
        assert "window.openai" in html
        assert "Live Casablanca market intelligence rendered natively inside ChatGPT." in html

    @pytest.mark.asyncio
    async def test_open_dashboard_returns_structured_bootstrap(self):
        snapshot = TestMCPTools()._make_snapshot()

        with patch("bvc_mcp.server.fetch_market_data", new_callable=AsyncMock) as mock:
            mock.return_value = snapshot
            from bvc_mcp.server import open_dashboard

            result = await open_dashboard()

        assert result.structured_content["kind"] == "dashboard_bootstrap"
        assert "leaders" in result.structured_content
        assert result.content[0].text == "Opened the BVC MCP dashboard."

    def test_main_runs_stdio_without_port(self):
        from bvc_mcp import server

        with (
            patch.dict(os.environ, {}, clear=False),
            patch("bvc_mcp.server.start_scheduler") as mock_start,
            patch("bvc_mcp.server.mcp.run") as mock_run,
        ):
            os.environ.pop("PORT", None)
            server.main()

        mock_start.assert_called_once()
        mock_run.assert_called_once_with()

    def test_main_runs_sse_with_port(self):
        from bvc_mcp import server

        with (
            patch.dict(os.environ, {"PORT": "8000"}, clear=False),
            patch("bvc_mcp.server.start_scheduler") as mock_start,
            patch("bvc_mcp.server.mcp.run") as mock_run,
        ):
            server.main()

        mock_start.assert_called_once()
        mock_run.assert_called_once()
        _, kwargs = mock_run.call_args
        assert kwargs["transport"] == "sse"
        assert kwargs["host"] == server.HOST
        assert kwargs["port"] == server.PORT


class TestModelsExtra:
    def test_strip_name_returns_non_string_input_unchanged(self):
        from bvc_mcp.models import Stock

        assert Stock.strip_name(123) == 123

    def test_model_validators_handle_bad_numeric_values(self):
        from bvc_mcp.models import Stock

        raw = dict(SAMPLE_STOCK_ACTIVE)
        raw["Cours"] = "not-a-number"
        raw["QteEchangee"] = "not-an-int"
        raw["DateDernierCours"] = "not-a-date"
        stock = Stock.model_validate(raw)

        assert stock.name == SAMPLE_STOCK_ACTIVE["Libelle"].strip()
        assert stock.price is None
        assert stock.quantity_traded is None
        assert stock.last_trade_datetime is None

    def test_numeric_validators_return_none_for_explicit_none(self):
        from bvc_mcp.models import Stock

        assert Stock.empty_string_to_none_float(None) is None
        assert Stock.empty_string_to_none_int(None) is None

    def test_trade_datetime_accepts_datetime_instance_and_market_state_unknown(self):
        from bvc_mcp.models import MarketSnapshot, Stock

        raw = dict(SAMPLE_STOCK_ACTIVE)
        raw["DateDernierCours"] = datetime(2026, 3, 9, 13, 35, 29)
        raw["Etat"] = ""
        stock = Stock.model_validate(raw)
        snapshot = MarketSnapshot(
            success=True,
            lastModified=1773071642,
            timestamp="2026-03-09 15:54:02",
            timestampFrench="lundi 9 mars 2026 16:54:02",
            stocks=[stock],
        )

        assert stock.last_trade_datetime == datetime(2026, 3, 9, 13, 35, 29)
        assert snapshot.market_state == "Unknown"


class TestConfigHelpers:
    def test_env_bool_uses_default_when_missing(self):
        from bvc_mcp.config import _env_bool

        with patch.dict(os.environ, {}, clear=True):
            assert _env_bool("MISSING_FLAG", True) is True
            assert _env_bool("MISSING_FLAG", False) is False

    def test_env_bool_parses_truthy_and_falsey_values(self):
        from bvc_mcp.config import _env_bool

        with patch.dict(os.environ, {"FLAG": " yes "}, clear=True):
            assert _env_bool("FLAG", False) is True
        with patch.dict(os.environ, {"FLAG": "no"}, clear=True):
            assert _env_bool("FLAG", True) is False
