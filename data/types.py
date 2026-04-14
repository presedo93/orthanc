"""Shared type definitions and parsing utilities for data module.

Timestamp parsing, resampling, and shared types.
"""

from datetime import datetime

import pandas as pd

# OHLCV columns expected by resample_ohlcv
_OHLCV_COLUMNS = {'open', 'high', 'low', 'close', 'volume'}

# Standard OHLCV aggregation rules (open=first, high=max, low=min, close=last, volume=sum)
_OHLCV_AGG: dict[str, str] = {
    'open': 'first',
    'high': 'max',
    'low': 'min',
    'close': 'last',
    'volume': 'sum',
}


def parse_timestamp(value: str | datetime | int) -> int:
    """Parse various timestamp formats to milliseconds.

    Args:
        value: Timestamp as ISO string, datetime object, or milliseconds int.

    Returns:
        Timestamp in milliseconds since epoch.

    Raises:
        TypeError: If value type is not supported.

    Examples:
        >>> parse_timestamp("2024-01-01T00:00:00Z")
        1704067200000
        >>> parse_timestamp(datetime(2024, 1, 1))
        1704067200000
        >>> parse_timestamp(1704067200000)
        1704067200000
    """
    if isinstance(value, int):
        return value
    elif isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return int(dt.timestamp() * 1000)


def resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Resample an OHLCV DataFrame to a larger timeframe.

    Takes a DataFrame with a DatetimeIndex and standard OHLCV columns, and
    resamples it to the target timeframe using proper aggregation rules:
    open=first, high=max, low=min, close=last, volume=sum.

    If a ``symbol`` column is present, resampling is performed per symbol
    so multi-symbol DataFrames are handled correctly.

    Args:
        df: OHLCV DataFrame with a DatetimeIndex and columns:
            open, high, low, close, volume (and optionally symbol).
        timeframe: Target pandas-compatible frequency string
            (e.g., '5min', '15min', '1h', '4h', '1D').

    Returns:
        New DataFrame resampled to the target timeframe. Rows where all
        OHLCV values are NaN (empty bars) are dropped.

    Raises:
        ValueError: If the DataFrame index is not a DatetimeIndex or
            required OHLCV columns are missing.

    Examples:
        >>> import pandas as pd
        >>> idx = pd.date_range('2024-01-01', periods=10, freq='1min')
        >>> df = pd.DataFrame({
        ...     'open': range(10), 'high': range(10, 20),
        ...     'low': range(0, 10), 'close': range(5, 15),
        ...     'volume': [100] * 10,
        ... }, index=idx)
        >>> resampled = resample_ohlcv(df, '5min')
        >>> len(resampled)
        2
    """
    _validate_ohlcv(df)

    if 'symbol' in df.columns:
        return (
            df
            .groupby('symbol')
            .apply(
                lambda g: _resample_group(g.drop(columns='symbol'), timeframe),
                include_groups=False,
            )
            .reset_index(level='symbol')
        )

    return _resample_group(df, timeframe)


def _validate_ohlcv(df: pd.DataFrame) -> None:
    """Validate that a DataFrame has the expected OHLCV structure.

    Args:
        df: DataFrame to validate.

    Raises:
        ValueError: If index is not DatetimeIndex or OHLCV columns are missing.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(
            f'Expected DatetimeIndex, got {type(df.index).__name__}. '
            'Use df.set_index(pd.to_datetime(df["timestamp"])) first.'
        )

    missing = _OHLCV_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f'Missing required OHLCV columns: {sorted(missing)}')


def _resample_group(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Resample a single-symbol OHLCV DataFrame.

    Args:
        df: Single-symbol OHLCV DataFrame with DatetimeIndex.
        timeframe: Target pandas frequency string.

    Returns:
        Resampled DataFrame with empty bars dropped.
    """
    resampled = df.resample(timeframe).agg(_OHLCV_AGG)
    assert isinstance(resampled, pd.DataFrame)  # agg with dict always returns DataFrame
    mask = resampled[['open', 'high', 'low', 'close']].notna().all(axis=1)
    result = resampled.loc[mask]
    assert isinstance(result, pd.DataFrame)
    return result
