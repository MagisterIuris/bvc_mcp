"""
Unit tests for analytics.py.

All tests operate on synthetic, pre-computed data — no network or DB access.
"""

from __future__ import annotations

import math
from typing import Optional

import pytest

from bvc_mcp.analytics import (
    calculate_average_volume,
    calculate_bollinger_bands,
    calculate_correlation,
    calculate_momentum,
    calculate_moving_average,
    calculate_rsi,
    calculate_volatility,
    find_support_resistance,
)


# ---------------------------------------------------------------------------
# Moving Average
# ---------------------------------------------------------------------------


class TestMovingAverage:
    def test_basic_period_3(self):
        """[1,2,3,4,5] with period=3 → [None, None, 2.0, 3.0, 4.0]."""
        result = calculate_moving_average([1, 2, 3, 4, 5], period=3)
        assert result[0] is None
        assert result[1] is None
        assert result[2] == 2.0
        assert result[3] == 3.0
        assert result[4] == 4.0

    def test_same_length_as_input(self):
        """Output list must have the same length as the input."""
        prices = list(range(10))
        result = calculate_moving_average(prices, period=4)
        assert len(result) == len(prices)

    def test_period_1_equals_original(self):
        """Period=1 SMA equals the original price at every position."""
        prices = [10.0, 20.0, 30.0]
        result = calculate_moving_average(prices, period=1)
        assert result == [10.0, 20.0, 30.0]

    def test_all_none_when_period_exceeds_length(self):
        """When period > len(prices), all values should be None."""
        result = calculate_moving_average([1.0, 2.0], period=5)
        assert all(v is None for v in result)

    def test_period_equals_length(self):
        """With period == len(prices), only the last value is non-None."""
        prices = [2.0, 4.0, 6.0]
        result = calculate_moving_average(prices, period=3)
        assert result[0] is None
        assert result[1] is None
        assert result[2] == 4.0

    def test_constant_prices(self):
        """SMA of a constant series equals that constant."""
        prices = [5.0] * 10
        result = calculate_moving_average(prices, period=5)
        for v in result[4:]:
            assert v == 5.0


# ---------------------------------------------------------------------------
# RSI
# ---------------------------------------------------------------------------


class TestRSI:
    def test_returns_none_for_insufficient_data(self):
        """Fewer than period+1 prices → all None."""
        result = calculate_rsi([100.0] * 10, period=14)
        assert all(v is None for v in result)

    def test_same_length_as_input(self):
        """Output must be same length as input."""
        prices = [float(i) for i in range(1, 30)]
        result = calculate_rsi(prices, period=14)
        assert len(result) == len(prices)

    def test_first_period_values_are_none(self):
        """First `period` entries must be None (index 0 .. period-1)."""
        prices = [float(i) for i in range(1, 30)]
        result = calculate_rsi(prices, period=14)
        for i in range(14):
            assert result[i] is None

    def test_first_non_none_at_period_index(self):
        """First non-None value should appear at index == period."""
        prices = [float(i) for i in range(1, 30)]
        result = calculate_rsi(prices, period=14)
        assert result[14] is not None

    def test_rsi_bounds(self):
        """RSI values must be in [0, 100]."""
        import random
        random.seed(42)
        prices = [100.0 + random.gauss(0, 5) for _ in range(50)]
        result = calculate_rsi(prices, period=14)
        for v in result:
            if v is not None:
                assert 0.0 <= v <= 100.0

    def test_all_gains_rsi_is_100(self):
        """Monotonically increasing prices → RSI should approach 100."""
        prices = [float(i * 10) for i in range(1, 30)]
        result = calculate_rsi(prices, period=14)
        non_none = [v for v in result if v is not None]
        # With all gains, avg_loss stays near 0 → RSI near 100
        assert all(v > 95.0 for v in non_none)

    def test_all_losses_rsi_is_0(self):
        """Monotonically decreasing prices → RSI should approach 0."""
        prices = [float((30 - i) * 10) for i in range(30)]
        result = calculate_rsi(prices, period=14)
        non_none = [v for v in result if v is not None]
        assert all(v < 5.0 for v in non_none)

    def test_empty_input(self):
        """Empty input → empty output."""
        assert calculate_rsi([], period=14) == []

    def test_single_price(self):
        """Single price → all None."""
        assert calculate_rsi([100.0], period=14) == [None]


# ---------------------------------------------------------------------------
# Bollinger Bands
# ---------------------------------------------------------------------------


class TestBollingerBands:
    def test_same_length_as_input(self):
        prices = [float(i) for i in range(1, 25)]
        result = calculate_bollinger_bands(prices, period=20)
        assert len(result) == len(prices)

    def test_first_period_minus_one_are_none(self):
        prices = list(range(1, 25))
        result = calculate_bollinger_bands([float(p) for p in prices], period=20)
        for i in range(19):
            assert result[i] is None

    def test_first_non_none_has_expected_keys(self):
        prices = [float(i) for i in range(1, 25)]
        result = calculate_bollinger_bands(prices, period=20)
        first_valid = next(v for v in result if v is not None)
        assert {"middle", "upper", "lower"} == set(first_valid.keys())

    def test_upper_above_middle_above_lower(self):
        """For non-constant series: upper > middle > lower always."""
        prices = [float(i + (i % 3)) for i in range(1, 30)]
        result = calculate_bollinger_bands(prices, period=10)
        for band in result:
            if band is not None:
                assert band["upper"] >= band["middle"] >= band["lower"]

    def test_constant_prices_zero_bandwidth(self):
        """Constant prices → σ = 0, so upper == middle == lower."""
        prices = [50.0] * 25
        result = calculate_bollinger_bands(prices, period=20)
        for band in result:
            if band is not None:
                assert band["upper"] == band["middle"] == band["lower"] == 50.0

    def test_middle_equals_sma(self):
        """The middle band should equal the SMA for the same window."""
        prices = [float(i) for i in range(1, 26)]
        bb = calculate_bollinger_bands(prices, period=5)
        sma = calculate_moving_average(prices, period=5)
        for b, s in zip(bb, sma):
            if b is not None:
                assert abs(b["middle"] - s) < 1e-6


# ---------------------------------------------------------------------------
# Volatility
# ---------------------------------------------------------------------------


class TestVolatility:
    def test_returns_none_for_single_price(self):
        assert calculate_volatility([100.0]) is None

    def test_returns_none_for_empty(self):
        assert calculate_volatility([]) is None

    def test_returns_float_for_valid_data(self):
        prices = [100.0, 101.0, 99.5, 102.0, 100.5, 103.0]
        result = calculate_volatility(prices)
        assert isinstance(result, float)
        assert result > 0

    def test_constant_prices_zero_volatility(self):
        """Constant prices have no variance → log returns are 0 → volatility = 0."""
        prices = [50.0] * 20
        result = calculate_volatility(prices)
        assert result == 0.0

    def test_higher_variance_higher_volatility(self):
        """More volatile prices should produce a higher annualised volatility."""
        stable = [100.0 + 0.1 * (i % 2) for i in range(20)]
        volatile = [100.0 + 10.0 * (i % 2) for i in range(20)]
        v_stable = calculate_volatility(stable)
        v_volatile = calculate_volatility(volatile)
        assert v_volatile > v_stable

    def test_uses_only_last_period_prices(self):
        """Should use at most `period` prices from the end of the list."""
        # period=3 → last 3 prices: [100.0, 200.0, 100.0] → 2 log returns → variance computable
        long_prices = [100.0] * 100 + [200.0, 100.0]  # only last prices are volatile
        vol = calculate_volatility(long_prices, period=3)
        assert vol is not None and vol > 0


# ---------------------------------------------------------------------------
# Pearson Correlation
# ---------------------------------------------------------------------------


class TestCorrelation:
    def test_identical_series_correlation_is_1(self):
        prices = [10.0, 12.0, 11.0, 13.0, 14.0]
        result = calculate_correlation(prices, prices)
        assert result is not None
        assert abs(result - 1.0) < 1e-6

    def test_perfectly_inverse_series_correlation_is_minus_1(self):
        a = [1.0, 2.0, 3.0, 4.0, 5.0]
        b = [5.0, 4.0, 3.0, 2.0, 1.0]
        result = calculate_correlation(a, b)
        assert result is not None
        assert abs(result - (-1.0)) < 1e-6

    def test_different_lengths_returns_none(self):
        assert calculate_correlation([1.0, 2.0], [1.0, 2.0, 3.0]) is None

    def test_single_point_returns_none(self):
        assert calculate_correlation([1.0], [1.0]) is None

    def test_constant_series_returns_none(self):
        """Constant series has zero variance → correlation undefined → None."""
        a = [5.0, 5.0, 5.0, 5.0]
        b = [1.0, 2.0, 3.0, 4.0]
        assert calculate_correlation(a, b) is None

    def test_result_in_valid_range(self):
        import random
        random.seed(7)
        a = [random.gauss(100, 10) for _ in range(30)]
        b = [random.gauss(200, 5) for _ in range(30)]
        r = calculate_correlation(a, b)
        assert r is not None
        assert -1.0 <= r <= 1.0


# ---------------------------------------------------------------------------
# Momentum
# ---------------------------------------------------------------------------


class TestMomentum:
    def test_returns_none_when_insufficient_data(self):
        assert calculate_momentum([100.0, 105.0], period=3) is None

    def test_positive_momentum(self):
        prices = [100.0, 110.0, 105.0, 115.0]
        result = calculate_momentum(prices, period=3)
        # (115 - 100) / 100 * 100 = 15.0
        assert result == pytest.approx(15.0, rel=1e-4)

    def test_zero_momentum_same_price(self):
        prices = [100.0, 105.0, 100.0]
        result = calculate_momentum(prices, period=2)
        # (100 - 100) / 100 * 100 = 0
        assert result == 0.0

    def test_negative_momentum(self):
        prices = [120.0, 115.0, 110.0, 100.0]
        result = calculate_momentum(prices, period=3)
        # (100 - 120) / 120 * 100 ≈ -16.6667
        assert result is not None
        assert result < 0

    def test_zero_base_price_returns_none(self):
        prices = [0.0, 5.0, 10.0]
        assert calculate_momentum(prices, period=2) is None


# ---------------------------------------------------------------------------
# Support & Resistance
# ---------------------------------------------------------------------------


class TestSupportResistance:
    def test_basic(self):
        prices = [100.0, 105.0, 98.0, 110.0, 102.0]
        result = find_support_resistance(prices, window=5)
        assert result["support"] == 98.0
        assert result["resistance"] == 110.0

    def test_window_limits_to_recent(self):
        """With window=2, only the last 2 prices are considered."""
        prices = [50.0, 200.0, 90.0, 95.0]
        result = find_support_resistance(prices, window=2)
        assert result["support"] == 90.0
        assert result["resistance"] == 95.0

    def test_empty_prices_returns_none(self):
        result = find_support_resistance([], window=5)
        assert result["support"] is None
        assert result["resistance"] is None

    def test_single_price(self):
        result = find_support_resistance([42.0], window=5)
        assert result["support"] == 42.0
        assert result["resistance"] == 42.0

    def test_constant_prices(self):
        result = find_support_resistance([7.5] * 10, window=5)
        assert result["support"] == result["resistance"] == 7.5


# ---------------------------------------------------------------------------
# Average Volume
# ---------------------------------------------------------------------------


class TestAverageVolume:
    def test_basic(self):
        volumes = [100.0, 200.0, 300.0]
        assert calculate_average_volume(volumes) == pytest.approx(200.0)

    def test_ignores_none_values(self):
        volumes = [100.0, None, 300.0]
        assert calculate_average_volume(volumes) == pytest.approx(200.0)

    def test_all_none_returns_none(self):
        assert calculate_average_volume([None, None]) is None

    def test_empty_list_returns_none(self):
        assert calculate_average_volume([]) is None

    def test_single_value(self):
        assert calculate_average_volume([500.0]) == pytest.approx(500.0)

    def test_mixed_zeros_and_values(self):
        volumes = [0.0, 200.0, 400.0]
        assert calculate_average_volume(volumes) == pytest.approx(200.0)
