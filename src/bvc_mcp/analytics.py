"""
Pure-Python financial analytics for BVC market data.

All functions operate on plain lists of floats. No external dependencies —
only the Python standard library (math module) is used.

Conventions:
  - Input price lists are expected in **chronological order** (oldest first).
  - Output lists have the same length as the input list; positions where
    there is insufficient data return None.
  - Monetary values and percentages are rounded to 4 decimal places max.
"""

from __future__ import annotations

import math
from typing import Optional


# ---------------------------------------------------------------------------
# Moving Average
# ---------------------------------------------------------------------------


def calculate_moving_average(prices: list[float], period: int) -> list[Optional[float]]:
    """
    Calculate Simple Moving Average (SMA) for a price series.

    The first (period - 1) values in the output are None because there are
    not enough preceding data points to form a complete window.

    Args:
        prices: Prices in chronological order (oldest first).
        period: Number of periods for the moving average window.

    Returns:
        List of same length as ``prices``. Each value is the SMA at that
        position, or None if insufficient data.

    Example:
        >>> calculate_moving_average([1, 2, 3, 4, 5], 3)
        [None, None, 2.0, 3.0, 4.0]
    """
    result: list[Optional[float]] = []
    for i in range(len(prices)):
        if i < period - 1:
            result.append(None)
        else:
            window = prices[i - period + 1 : i + 1]
            result.append(round(sum(window) / period, 4))
    return result


# ---------------------------------------------------------------------------
# RSI
# ---------------------------------------------------------------------------


def calculate_rsi(
    prices: list[float], period: int = 14
) -> list[Optional[float]]:
    """
    Calculate the Relative Strength Index (RSI) using Wilder's smoothing method.

    RSI = 100 - 100 / (1 + RS)
    RS  = smoothed_avg_gain / smoothed_avg_loss

    The first RSI value appears at index ``period`` (requires ``period + 1``
    prices to produce one change-based average). Subsequent values use
    Wilder's exponential smoothing: avg = (prev_avg * (n-1) + current) / n.

    Args:
        prices: Prices in chronological order (oldest first).
        period: RSI period (default 14).

    Returns:
        List of same length. First ``period`` values are None.
    """
    n = len(prices)
    result: list[Optional[float]] = [None] * n

    if n < 2:
        return result

    changes = [prices[i] - prices[i - 1] for i in range(1, n)]
    gains = [max(c, 0.0) for c in changes]
    losses = [abs(min(c, 0.0)) for c in changes]

    if len(changes) < period:
        return result

    # Seed: simple average of the first `period` changes
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    def _rsi(ag: float, al: float) -> float:
        if al == 0:
            return 100.0
        return round(100.0 - 100.0 / (1.0 + ag / al), 4)

    result[period] = _rsi(avg_gain, avg_loss)

    # Wilder's smoothing for subsequent positions
    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period
        result[i] = _rsi(avg_gain, avg_loss)

    return result


# ---------------------------------------------------------------------------
# Bollinger Bands
# ---------------------------------------------------------------------------


def calculate_bollinger_bands(
    prices: list[float], period: int = 20, std_dev: float = 2.0
) -> list[Optional[dict]]:
    """
    Calculate Bollinger Bands for a price series.

    middle = SMA(period)
    upper  = middle + std_dev * σ   (population std-dev of the window)
    lower  = middle - std_dev * σ

    Args:
        prices:  Prices in chronological order (oldest first).
        period:  SMA window (default 20).
        std_dev: Width in standard deviations (default 2.0).

    Returns:
        List of same length. Each element is either None or a dict with keys
        ``middle``, ``upper``, ``lower``.
    """
    result: list[Optional[dict]] = []
    for i in range(len(prices)):
        if i < period - 1:
            result.append(None)
        else:
            window = prices[i - period + 1 : i + 1]
            middle = sum(window) / period
            variance = sum((p - middle) ** 2 for p in window) / period
            sigma = math.sqrt(variance)
            result.append(
                {
                    "middle": round(middle, 4),
                    "upper": round(middle + std_dev * sigma, 4),
                    "lower": round(middle - std_dev * sigma, 4),
                }
            )
    return result


# ---------------------------------------------------------------------------
# Volatility
# ---------------------------------------------------------------------------


def calculate_volatility(
    prices: list[float], period: int = 30
) -> Optional[float]:
    """
    Calculate annualised historical volatility from logarithmic returns.

    volatility = std(log_returns) * sqrt(252)

    252 trading days per year is the standard convention.
    Sample standard deviation (ddof=1) is used.

    Args:
        prices: Prices in chronological order. The last ``period`` values
                are used; if fewer are available all are used.
        period: Maximum number of prices to include.

    Returns:
        Annualised volatility as a percentage (e.g. 24.5 means 24.5%), or
        None if there are fewer than 2 valid prices.
    """
    subset = prices[-period:] if len(prices) > period else prices
    valid = [p for p in subset if p is not None and p > 0]

    if len(valid) < 2:
        return None

    log_returns = [math.log(valid[i] / valid[i - 1]) for i in range(1, len(valid))]

    if len(log_returns) < 2:
        return None

    n = len(log_returns)
    mean_r = sum(log_returns) / n
    variance = sum((r - mean_r) ** 2 for r in log_returns) / (n - 1)  # sample
    std = math.sqrt(variance)

    return round(std * math.sqrt(252) * 100, 4)  # percent


# ---------------------------------------------------------------------------
# Pearson Correlation
# ---------------------------------------------------------------------------


def calculate_correlation(
    prices_a: list[float], prices_b: list[float]
) -> Optional[float]:
    """
    Calculate the Pearson correlation coefficient between two price series.

    Args:
        prices_a: First price series.
        prices_b: Second price series (must be the same length).

    Returns:
        Pearson r in [-1, 1], or None if the series have different lengths,
        contain fewer than 2 points, or are constant (zero variance).
    """
    if len(prices_a) != len(prices_b) or len(prices_a) < 2:
        return None

    n = len(prices_a)
    mean_a = sum(prices_a) / n
    mean_b = sum(prices_b) / n

    numerator = sum(
        (prices_a[i] - mean_a) * (prices_b[i] - mean_b) for i in range(n)
    )
    denom_a = math.sqrt(sum((x - mean_a) ** 2 for x in prices_a))
    denom_b = math.sqrt(sum((x - mean_b) ** 2 for x in prices_b))

    if denom_a == 0 or denom_b == 0:
        return None

    return round(numerator / (denom_a * denom_b), 6)


# ---------------------------------------------------------------------------
# Momentum
# ---------------------------------------------------------------------------


def calculate_momentum(prices: list[float], period: int) -> Optional[float]:
    """
    Calculate rate-of-change momentum.

    momentum = (price[-1] - price[-1-period]) / price[-1-period] * 100

    Args:
        prices: Prices in chronological order (oldest first).
        period: Lookback period.

    Returns:
        Momentum as a percentage, or None if there are not enough data points
        or if the base price is zero.
    """
    if len(prices) < period + 1:
        return None

    current = prices[-1]
    past = prices[-1 - period]

    if past == 0:
        return None

    return round((current - past) / past * 100, 4)


# ---------------------------------------------------------------------------
# Support & Resistance
# ---------------------------------------------------------------------------


def find_support_resistance(
    prices: list[float], window: int = 5
) -> dict:
    """
    Identify simple support and resistance levels from recent price data.

    Support    = minimum price in the last ``window`` points.
    Resistance = maximum price in the last ``window`` points.

    If fewer than ``window`` prices are available, all available prices are used.

    Args:
        prices: Prices in chronological order (oldest first).
        window: Number of most-recent prices to consider.

    Returns:
        Dict with keys ``support`` (float | None) and ``resistance`` (float | None).
    """
    recent = prices[-window:] if len(prices) >= window else prices

    if not recent:
        return {"support": None, "resistance": None}

    return {
        "support": round(min(recent), 4),
        "resistance": round(max(recent), 4),
    }


# ---------------------------------------------------------------------------
# Average Volume
# ---------------------------------------------------------------------------


def calculate_average_volume(
    volumes: list[Optional[float]],
) -> Optional[float]:
    """
    Calculate the mean of a volume series, ignoring None values.

    Args:
        volumes: List of volume values; may contain None.

    Returns:
        Average volume rounded to 2 decimal places, or None if every value
        in the list is None.
    """
    valid = [v for v in volumes if v is not None]
    if not valid:
        return None
    return round(sum(valid) / len(valid), 2)
