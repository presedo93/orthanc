"""CCXT exchange wrapper for simplified data fetching."""

from datetime import datetime
from pathlib import Path
from typing import Any

import ccxt
import pandas as pd
import polars as pl

from ..cache import (
    TimeRange,
    find_cached_files,
    find_gaps,
    get_cache_dir,
    load_cached_data,
    merge_and_save,
)
from ..types import parse_timestamp
from .fetcher import fetch_range
from .types import (
    FEED_FUNDING_RATE,
    FEED_LS_RATIO,
    FEED_MARK_OHLCV,
    FEED_OHLCV,
    FEED_OPEN_INTEREST,
    FEED_TRADES,
    DataFeed,
    Exchange,
)


class CCXTHandler:
    """Wrapper over ccxt for simplified exchange data access.

    Encapsulates exchange initialization and provides methods for
    fetching and storing various types of exchange data (OHLCV, open
    interest, funding rates) with intelligent caching.

    Examples:
        >>> handler = CCXTHandler("bybit")
        >>> df = handler.get_ohlcv(
        ...     symbols="BTC/USDT",
        ...     timeframe="1h",
        ...     since="2024-01-01T00:00:00Z",
        ... )
    """

    def __init__(self, exchange_id: str, data_dir: Path | str = 'data') -> None:
        """Initialize the handler with an exchange.

        Args:
            exchange_id: Name of the exchange (e.g., 'bybit', 'binance').
            data_dir: Directory for storing parquet files.

        Raises:
            ValueError: If exchange_id is not supported by ccxt.
        """
        if exchange_id not in ccxt.exchanges:
            raise ValueError(
                f"Unknown exchange '{exchange_id}'. "
                f'Available: {", ".join(ccxt.exchanges[:10])}...'
            )

        exchange_class = getattr(ccxt, exchange_id)
        self._exchange: Exchange = exchange_class({'enableRateLimit': True})
        self._dir = Path(data_dir)
        self._exchange.load_markets()

    @property
    def exchange_id(self) -> str:
        """Return the exchange identifier."""
        return str(self._exchange.id)

    @property
    def timeframes(self) -> dict[str, str]:
        """
        Get available timeframes for OHLCV data on this exchange.

        Returns:
            dictionary mapping timeframe strings to descriptions
            (e.g., {'1m': '1 minute', '1h': '1 hour', '1d': '1 day'})
            Returns empty dict if OHLCV not supported
        """
        if not self._exchange.has.get('fetchOHLCV', False):
            return {}
        return self._exchange.timeframes

    @property
    def rate_limit(self) -> int:
        """
        Get the rate limit for this exchange in milliseconds.

        This is the minimum delay between requests that the exchange
        will tolerate. CCXT's enableRateLimit will automatically
        respect this limit.

        Returns:
            Rate limit in milliseconds
        """
        return self._exchange.rateLimit

    @property
    def markets(self) -> dict[str, Any]:
        """
        Get available trading markets/symbols on this exchange.

        This property loads markets from the exchange if not already loaded.

        Returns:
            dictionary of market information keyed by symbol

        Raises:
            ccxt.ExchangeError: If loading markets fails

        Example:
            >>> handler = CCXTHandler('binance')
            >>> markets = handler.markets
            >>> 'BTC/USDT' in markets
            True
        """
        if not self._exchange.markets:
            self._exchange.load_markets()

        # CCXT markets can be None if not loaded, return empty dict as fallback
        return self._exchange.markets if self._exchange.markets else {}

    @property
    def symbols(self) -> list[str]:
        """
        Get list of available trading symbols on this exchange.

        Returns:
            list of symbol strings (e.g., ['BTC/USDT', 'ETH/USDT', ...])
        """
        return list(self.markets.keys())

    def get_ohlcv(
        self,
        symbols: str | list[str],
        timeframe: str,
        since: str | datetime | int,
        until: str | datetime | int | None = None,
    ) -> pd.DataFrame:
        """Fetch OHLCV data for one or more symbols.

        Uses cached parquet files when available, fetching only missing
        data from the exchange. Data is stored in the structure:
        {dir}/ohlcv/{exchange}/{symbol}/{timeframe}/{since}_{until}.parquet

        Args:
            symbols: Single symbol or list of symbols (e.g., 'BTC/USDT').
            timeframe: Candle timeframe (e.g., '1m', '1h', '1d').
            since: Start time as ISO string, datetime, or milliseconds.
            until: End time (defaults to now if not provided).

        Returns:
            DataFrame with datetime index and columns: symbol, open, high, low, close, volume.

        Examples:
            >>> handler = CCXTHandler("binance")
            >>> df = handler.get_ohlcv(
            ...     symbols=["BTC/USDT", "ETH/USDT"],
            ...     timeframe="1h",
            ...     since="2024-01-01",
            ...     until="2024-01-02",
            ... )
        """
        return self._get_data(symbols, timeframe, since, until, FEED_OHLCV)

    def get_mark_ohlcv(
        self,
        symbols: str | list[str],
        timeframe: str,
        since: str | datetime | int,
        until: str | datetime | int | None = None,
    ) -> pd.DataFrame:
        """Fetch mark price OHLCV data for one or more symbols.

        Mark price OHLCV represents the fair price used for liquidations and
        funding rate calculations in futures markets.

        Uses cached parquet files when available, fetching only missing
        data from the exchange. Data is stored in the structure:
        {dir}/mark_ohlcv/{exchange}/{symbol}/{timeframe}/{since}_{until}.parquet

        Args:
            symbols: Single symbol or list of symbols (e.g., 'BTC/USDT:USDT').
            timeframe: Candle timeframe (e.g., '1m', '1h', '1d').
            since: Start time as ISO string, datetime, or milliseconds.
            until: End time (defaults to now if not provided).

        Returns:
            DataFrame with datetime index and columns: symbol, open, high, low, close, volume.

        Examples:
            >>> handler = CCXTHandler("binance")
            >>> df = handler.get_mark_ohlcv(
            ...     symbols=["BTC/USDT:USDT", "ETH/USDT:USDT"],
            ...     timeframe="1h",
            ...     since="2024-01-01",
            ...     until="2024-01-02",
            ... )
        """
        return self._get_data(symbols, timeframe, since, until, FEED_MARK_OHLCV)

    def get_open_interest(
        self,
        symbols: str | list[str],
        timeframe: str,
        since: str | datetime | int,
        until: str | datetime | int | None = None,
    ) -> pd.DataFrame:
        """Fetch open interest data for one or more symbols.

        Uses cached parquet files when available, fetching only missing
        data from the exchange. Data is stored in the structure:
        {data_dir}/open_interest/{exchange}/{symbol}/{timeframe}/{since}_{until}.parquet

        Args:
            symbols: Single symbol or list of symbols (e.g., 'BTC/USDT:USDT').
            timeframe: Data timeframe (e.g., '1h', '1d').
            since: Start time as ISO string, datetime, or milliseconds.
            until: End time (defaults to now if not provided).

        Returns:
            DataFrame with datetime index and columns: symbol, openInterestAmount, openInterestValue.

        Examples:
            >>> handler = CCXTHandler("bybit")
            >>> df = handler.get_open_interest(
            ...     symbols="BTC/USDT:USDT",
            ...     timeframe="1h",
            ...     since="2024-01-01",
            ...     until="2024-01-02",
            ... )
        """
        return self._get_data(symbols, timeframe, since, until, FEED_OPEN_INTEREST)

    def get_funding_rate(
        self,
        symbols: str | list[str],
        since: str | datetime | int,
        until: str | datetime | int | None = None,
    ) -> pd.DataFrame:
        """Fetch funding rate data for one or more symbols.

        Uses cached parquet files when available, fetching only missing
        data from the exchange. Data is stored in the structure:
        {data_dir}/funding_rate/{exchange}/{symbol}/{timeframe}/{since}_{until}.parquet

        Args:
            symbols: Single symbol or list of symbols (e.g., 'BTC/USDT:USDT').
            timeframe: Data timeframe (e.g., '8h' for 8-hour funding intervals).
            since: Start time as ISO string, datetime, or milliseconds.
            until: End time (defaults to now if not provided).

        Returns:
            DataFrame with datetime index and columns: symbol, fundingRate, fundingTimestamp.

        Examples:
            >>> handler = CCXTHandler("bybit")
            >>> df = handler.get_funding_rate(
            ...     symbols="BTC/USDT:USDT",
            ...     timeframe="8h",
            ...     since="2024-01-01",
            ...     until="2024-01-02",
            ... )
        """
        return self._get_data(symbols, None, since, until, FEED_FUNDING_RATE)

    def get_long_short_ratio(
        self,
        symbols: str | list[str],
        timeframe: str,
        since: str | datetime | int,
        until: str | datetime | int | None = None,
    ) -> pd.DataFrame:
        """Fetch long/short ratio data for one or more symbols.

        Uses cached parquet files when available, fetching only missing
        data from the exchange. Data is stored in the structure:
        {data_dir}/long_short_ratio/{exchange}/{symbol}/{timeframe}/{since}_{until}.parquet

        Args:
            symbols: Single symbol or list of symbols (e.g., 'BTC/USDT:USDT').
            timeframe: Data timeframe (e.g., '1h', '1d').
            since: Start time as ISO string, datetime, or milliseconds.
            until: End time (defaults to now if not provided).

        Returns:
            DataFrame with datetime index and columns: symbol, longShortRatio.

        Examples:
            >>> handler = CCXTHandler("binance")
            >>> df = handler.get_long_short_ratio(
            ...     symbols="BTC/USDT:USDT",
            ...     timeframe="1h",
            ...     since="2024-01-01",
            ...     until="2024-01-02",
            ... )
        """
        return self._get_data(symbols, timeframe, since, until, FEED_LS_RATIO)

    def get_trades(
        self,
        symbols: str | list[str],
        since: str | datetime | int,
        until: str | datetime | int | None = None,
    ) -> pd.DataFrame:
        """Fetch public trades data for one or more symbols.

        Uses cached parquet files when available, fetching only missing
        data from the exchange. Data is stored in the structure:
        {data_dir}/trades/{exchange}/{symbol}/{since}_{until}.parquet

        Args:
            symbols: Single symbol or list of symbols (e.g., 'BTC/USDT').
            since: Start time as ISO string, datetime, or milliseconds.
            until: End time (defaults to now if not provided).

        Returns:
            DataFrame with datetime index and columns: symbol, id, side, price, amount, cost.

        Examples:
            >>> handler = CCXTHandler("binance")
            >>> df = handler.get_trades(
            ...     symbols="BTC/USDT",
            ...     since="2024-01-01T00:00:00Z",
            ...     until="2024-01-01T01:00:00Z",
            ... )
        """
        return self._get_data(symbols, None, since, until, FEED_TRADES)

    def _get_data(
        self,
        symbols: str | list[str],
        timeframe: str | None,
        since: str | datetime | int,
        until: str | datetime | int | None,
        config: DataFeed,
    ) -> pd.DataFrame:
        """Generic method to fetch any type of exchange data with caching.

        Args:
            symbols: Single symbol or list of symbols.
            timeframe: Data timeframe.
            since: Start time.
            until: End time (defaults to now if not provided).
            config: Configuration for the data type to fetch.

        Returns:
            DataFrame with datetime index and data-type-specific columns.
        """
        symbol_list = [symbols] if isinstance(symbols, str) else symbols

        since_ms = parse_timestamp(since)
        until_ms = parse_timestamp(until) if until else self._exchange.milliseconds()

        requested = TimeRange(since_ms, until_ms)
        all_frames: list[pl.DataFrame] = []

        for symbol in symbol_list:
            df = self._get_symbol_data(symbol, timeframe, requested, config)
            if df is not None and len(df) > 0:
                df = df.with_columns(pl.lit(symbol).alias('symbol'))
                all_frames.append(df)

        if not all_frames:
            schema = ['symbol'] + config.columns
            df = pd.DataFrame({col: [] for col in schema})
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df.set_index('timestamp')

        combined = pl.concat(all_frames)
        df = combined.select(['symbol'] + config.columns).to_pandas()

        # Convert timestamp to datetime and set as index
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.set_index('timestamp')

        return df

    def _get_symbol_data(
        self, symbol: str, timeframe: str | None, requested: TimeRange, config: DataFeed
    ) -> pl.DataFrame | None:
        """Fetch data for a single symbol with caching.

        Args:
            symbol: Trading pair symbol.
            timeframe: Data timeframe.
            requested: Time range to fetch.
            config: Configuration for the data type.

        Returns:
            DataFrame with the requested data, or None if no data available.
        """
        dir = get_cache_dir(self._dir, config.name, self.exchange_id, symbol, timeframe)

        # Find gaps that need fetching
        cached_files = find_cached_files(dir, include_checkpoints=True)
        gaps = find_gaps(cached_files, requested)

        # Fetch missing data
        new_data_frames: list[pl.DataFrame] = []
        for gap in gaps:
            records = fetch_range(
                self._exchange, config, symbol, timeframe, gap, cache_dir=dir
            )
            if records:
                # Handle both list and dict formats from CCXT
                if isinstance(records[0], dict):
                    df = pl.DataFrame(records)
                    df = df.select(config.columns)
                else:
                    df = pl.DataFrame(records, schema=config.columns, orient='row')
                new_data_frames.append(df)

        # Combine new data
        new_data = pl.concat(new_data_frames) if new_data_frames else pl.DataFrame()

        # If we fetched new data, merge and save
        if len(new_data) > 0 or gaps:
            final_files = [f for f in cached_files if not f.is_checkpoint]
            merge_and_save(dir, final_files, new_data, requested)

            cached_files = find_cached_files(dir)

        # Load and return the requested range
        return load_cached_data(cached_files, requested)
