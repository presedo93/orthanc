"""Market feature computation for regime detection and strategies.

Pure functions that compute technical features from OHLCV data.
Each function takes a DataFrame or Series and returns a Series,
making them composable and independently testable.
"""

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Trend features
# ---------------------------------------------------------------------------


def ema(series: pd.Series, period: int) -> pd.Series:  # type: ignore[type-arg]
    """Compute exponential moving average.

    Args:
        series: Input price or indicator series.
        period: EMA lookback period.

    Returns:
        EMA series aligned with input index.
    """
    return series.ewm(span=period, adjust=False).mean()  # type: ignore[return-value]


def ema_slope(series: pd.Series, period: int, slope_window: int = 5) -> pd.Series:  # type: ignore[type-arg]
    """Compute the normalized slope of an EMA.

    Slope is computed as the percentage change over slope_window bars,
    giving a dimensionless trend direction and strength indicator.

    Args:
        series: Input price series (typically close).
        period: EMA lookback period.
        slope_window: Number of bars over which to measure slope.

    Returns:
        Normalized EMA slope series (positive = up, negative = down).
    """
    ema_values = ema(series, period)
    slope = ema_values.pct_change(periods=slope_window)
    return slope.fillna(0.0)


def adx(
    high: pd.Series,  # type: ignore[type-arg]
    low: pd.Series,  # type: ignore[type-arg]
    close: pd.Series,  # type: ignore[type-arg]
    period: int = 14,
) -> pd.Series:  # type: ignore[type-arg]
    """Compute Average Directional Index using Wilder's smoothing.

    ADX measures trend strength regardless of direction. Values above 25
    indicate a strong trend; below 20 indicates a weak or absent trend.

    Args:
        high: High price series.
        low: Low price series.
        close: Close price series.
        period: Smoothing period (default 14).

    Returns:
        ADX series in [0, 100] range.
    """
    # True Range
    tr = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # Directional Movement
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low

    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=high.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=high.index,
    )

    # Wilder's smoothing (EMA with alpha = 1/period)
    alpha = 1.0 / period
    atr_smooth = tr.ewm(alpha=alpha, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(alpha=alpha, adjust=False).mean() / atr_smooth
    minus_di = 100 * minus_dm.ewm(alpha=alpha, adjust=False).mean() / atr_smooth

    # ADX
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    dx = dx.replace([np.inf, -np.inf], 0.0).fillna(0.0)
    adx_values = dx.ewm(alpha=alpha, adjust=False).mean()

    return adx_values.fillna(0.0)


# ---------------------------------------------------------------------------
# Volatility features
# ---------------------------------------------------------------------------


def atr(
    high: pd.Series,  # type: ignore[type-arg]
    low: pd.Series,  # type: ignore[type-arg]
    close: pd.Series,  # type: ignore[type-arg]
    period: int = 14,
) -> pd.Series:  # type: ignore[type-arg]
    """Compute Average True Range.

    Args:
        high: High price series.
        low: Low price series.
        close: Close price series.
        period: Smoothing period.

    Returns:
        ATR series.
    """
    tr = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return tr.ewm(alpha=1.0 / period, adjust=False).mean()  # type: ignore[return-value]


def atr_ratio(
    high: pd.Series,  # type: ignore[type-arg]
    low: pd.Series,  # type: ignore[type-arg]
    close: pd.Series,  # type: ignore[type-arg]
    fast_period: int = 5,
    slow_period: int = 20,
) -> pd.Series:  # type: ignore[type-arg]
    """Compute ratio of short-term to long-term ATR.

    Values > 1 indicate expanding volatility; < 1 indicate contracting.

    Args:
        high: High price series.
        low: Low price series.
        close: Close price series.
        fast_period: Short-term ATR period.
        slow_period: Long-term ATR period.

    Returns:
        ATR ratio series.
    """
    fast_atr = atr(high, low, close, fast_period)
    slow_atr = atr(high, low, close, slow_period)
    ratio = fast_atr / slow_atr
    return ratio.replace([np.inf, -np.inf], 1.0).fillna(1.0)


def bollinger_width(
    close: pd.Series,  # type: ignore[type-arg]
    period: int = 20,
    num_std: float = 2.0,
) -> pd.Series:  # type: ignore[type-arg]
    """Compute Bollinger Band width as a percentage of the middle band.

    Higher values indicate wider bands (more volatility).

    Args:
        close: Close price series.
        period: Moving average period.
        num_std: Number of standard deviations for bands.

    Returns:
        Bollinger width series (dimensionless ratio).
    """
    mid = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()
    width = (num_std * 2 * std) / mid
    return width.replace([np.inf, -np.inf], 0.0).fillna(0.0)


def realized_volatility(
    close: pd.Series,  # type: ignore[type-arg]
    period: int = 20,
) -> pd.Series:  # type: ignore[type-arg]
    """Compute realized volatility as rolling standard deviation of returns.

    Args:
        close: Close price series.
        period: Lookback window.

    Returns:
        Annualized realized volatility series.
    """
    log_returns = np.log(close / close.shift(1))
    return log_returns.rolling(window=period).std().fillna(0.0)


# ---------------------------------------------------------------------------
# Activity features
# ---------------------------------------------------------------------------


def volume_ratio(
    volume: pd.Series,  # type: ignore[type-arg]
    fast_period: int = 5,
    slow_period: int = 20,
) -> pd.Series:  # type: ignore[type-arg]
    """Compute ratio of short-term to long-term average volume.

    Values > 1 indicate above-average activity; < 1 indicate below-average.

    Args:
        volume: Volume series.
        fast_period: Short-term average period.
        slow_period: Long-term average period.

    Returns:
        Volume ratio series.
    """
    fast_vol = volume.rolling(window=fast_period).mean()
    slow_vol = volume.rolling(window=slow_period).mean()
    ratio = fast_vol / slow_vol
    return ratio.replace([np.inf, -np.inf], 1.0).fillna(1.0)  # type: ignore[return-value]


def range_compression(
    high: pd.Series,  # type: ignore[type-arg]
    low: pd.Series,  # type: ignore[type-arg]
    fast_period: int = 5,
    slow_period: int = 20,
) -> pd.Series:  # type: ignore[type-arg]
    """Compute range compression as ratio of short-term to long-term range.

    Values < 1 indicate price compression (narrowing ranges).

    Args:
        high: High price series.
        low: Low price series.
        fast_period: Short-term range period.
        slow_period: Long-term range period.

    Returns:
        Range compression ratio series.
    """
    bar_range = high - low
    fast_range = bar_range.rolling(window=fast_period).mean()
    slow_range = bar_range.rolling(window=slow_period).mean()
    ratio = fast_range / slow_range
    return ratio.replace([np.inf, -np.inf], 1.0).fillna(1.0)


# ---------------------------------------------------------------------------
# Structure features
# ---------------------------------------------------------------------------


def higher_highs_lower_lows(
    high: pd.Series,  # type: ignore[type-arg]
    low: pd.Series,  # type: ignore[type-arg]
    period: int = 10,
) -> pd.Series:  # type: ignore[type-arg]
    """Compute a trend structure score based on higher highs / lower lows.

    Positive values indicate bullish structure (higher highs and higher lows).
    Negative values indicate bearish structure (lower highs and lower lows).
    Near-zero indicates sideways or mixed structure.

    Args:
        high: High price series.
        low: Low price series.
        period: Lookback window for rolling comparison.

    Returns:
        Structure score in [-1, 1] range.
    """
    # Count higher highs and lower lows over lookback
    hh_count = (high > high.shift(1)).rolling(window=period).sum()
    ll_count = (low < low.shift(1)).rolling(window=period).sum()

    # Normalize to [-1, 1]: more HH = bullish, more LL = bearish
    score = (hh_count - ll_count) / period
    return score.fillna(0.0)


# ---------------------------------------------------------------------------
# Bollinger Bands (for mean reversion strategy)
# ---------------------------------------------------------------------------


def bollinger_bands(
    close: pd.Series,  # type: ignore[type-arg]
    period: int = 20,
    num_std: float = 2.0,
) -> tuple[pd.Series, pd.Series, pd.Series]:  # type: ignore[type-arg]
    """Compute Bollinger Bands.

    Args:
        close: Close price series.
        period: Moving average period.
        num_std: Number of standard deviations for bands.

    Returns:
        Tuple of (upper_band, middle_band, lower_band) as Series.
    """
    mid = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()
    upper = mid + num_std * std
    lower = mid - num_std * std
    return upper, mid, lower  # type: ignore[return-value]


def z_score(
    close: pd.Series,  # type: ignore[type-arg]
    period: int = 20,
) -> pd.Series:  # type: ignore[type-arg]
    """Compute z-score of close price relative to rolling mean.

    Args:
        close: Close price series.
        period: Lookback window.

    Returns:
        Z-score series (positive = above mean, negative = below).
    """
    mean = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()
    score = (close - mean) / std
    return score.replace([np.inf, -np.inf], 0.0).fillna(0.0)
