"""DataBento handler for simplified futures data fetching.

This module provides the BentoHandler class for fetching OHLCV data from
DataBento with automatic caching. The cache-first architecture ensures
that reading cached data NEVER costs API money.

Continuous Contract Roll Types
------------------------------
- Calendar (c): Roll by expiration date. Example: ES.c.0
- Volume (v): Roll by highest trading volume. Example: MNQ.v.0
- Open Interest (n): Roll by highest open interest. Example: CL.n.0

The number after the roll code is the rank (0 = front month).

Examples:
    >>> handler = BentoHandler(api_key="db-xxx", dataset="GLBX.MDP3")
    >>> df = handler.get_ohlcv(
    ...     symbols="MNQ.v.0",
    ...     schema="ohlcv-1m",
    ...     since="2024-01-01",
    ...     until="2024-03-01",
    ... )
"""

import time
from datetime import datetime
from pathlib import Path

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
from .adapter import BentoAdapter
from .fetcher import fetch_range
from .types import (
    FEED_MAP,
    SCHEMA_MAP,
    SCHEMA_TO_TIMEFRAME,
    BentoFeed,
    Schema,
    SType,
)


class BentoHandler:
    """Handler for DataBento data fetching with intelligent caching.

    Provides a simplified interface for fetching OHLCV data for futures
    with automatic caching. The cache-first architecture ensures:

    - Reading from cache is ALWAYS FREE (no API calls)
    - Only gaps in cached data trigger paid API calls
    - Data is stored in efficient parquet format

    Examples:
        >>> handler = BentoHandler(api_key="db-xxx", dataset="GLBX.MDP3")
        >>> df = handler.get_ohlcv(
        ...     symbols=["MNQ.v.0", "ES.v.0"],
        ...     schema="ohlcv-1m",
        ...     since="2024-01-01",
        ...     until="2024-03-01",
        ... )

    Attributes:
        dataset: The DataBento dataset identifier.
    """

    def __init__(
        self,
        api_key: str,
        dataset: str = 'GLBX.MDP3',
        data_dir: Path | str = 'data',
    ) -> None:
        """Initialize the handler.

        Args:
            api_key: DataBento API key.
            dataset: Dataset identifier (default: 'GLBX.MDP3' for CME futures).
            data_dir: Directory for storing parquet cache files.
        """
        self._adapter = BentoAdapter(api_key=api_key, dataset=dataset)
        self._dataset = dataset
        self._dir = Path(data_dir)

    @property
    def dataset(self) -> str:
        """Return the dataset identifier."""
        return self._dataset

    @property
    def supported_schemas(self) -> list[str]:
        """Return list of supported OHLCV schemas."""
        return list(SCHEMA_MAP.keys())

    def get_ohlcv(
        self,
        symbols: str | list[str],
        schema: str | Schema,
        since: str | datetime | int,
        until: str | datetime | int | None = None,
        stype_in: str | SType = SType.CONTINUOUS,
    ) -> pd.DataFrame:
        """Fetch OHLCV data for one or more symbols.

        Uses cached parquet files when available, fetching only missing
        data from the API. This ensures reading cached data is FREE.

        Args:
            symbols: Single symbol or list of symbols (e.g., 'MNQ.v.0').
            schema: OHLCV schema ('ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d').
            since: Start time as ISO string, datetime, or milliseconds.
            until: End time (defaults to now if not provided).
            stype_in: Symbol type for resolution (default: 'continuous').

        Returns:
            DataFrame with datetime index and columns:
            symbol, open, high, low, close, volume.

        Examples:
            >>> df = handler.get_ohlcv(
            ...     symbols="MNQ.v.0",
            ...     schema="ohlcv-1m",
            ...     since="2024-01-01",
            ...     until="2024-01-31",
            ... )
        """
        # Normalize inputs
        symbol_list = [symbols] if isinstance(symbols, str) else symbols
        schema_enum = self._resolve_schema(schema)
        stype_enum = self._resolve_stype(stype_in)
        config = FEED_MAP[schema_enum]
        timeframe = SCHEMA_TO_TIMEFRAME[schema_enum]

        # Parse timestamps
        since_ms = parse_timestamp(since)
        until_ms = parse_timestamp(until) if until else int(time.time() * 1000)
        requested = TimeRange(since_ms, until_ms)

        # Fetch data for each symbol
        all_frames: list[pl.DataFrame] = []

        for symbol in symbol_list:
            df = self._get_symbol_data(
                symbol=symbol,
                schema=schema_enum,
                timeframe=timeframe,
                requested=requested,
                config=config,
                stype_in=stype_enum,
            )
            if df is not None and len(df) > 0:
                df = df.with_columns(pl.lit(symbol).alias('symbol'))
                all_frames.append(df)

        # Combine and return
        if not all_frames:
            schema_cols = ['symbol'] + config.columns
            df = pd.DataFrame({col: [] for col in schema_cols})
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df.set_index('timestamp')

        combined = pl.concat(all_frames)
        df = combined.select(['symbol'] + config.columns).to_pandas()

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.set_index('timestamp')

        return df

    def get_cost(
        self,
        symbols: str | list[str],
        schema: str | Schema,
        since: str | datetime | int,
        until: str | datetime | int | None = None,
        stype_in: str | SType = SType.CONTINUOUS,
    ) -> float:
        """Estimate cost for a data request before making it.

        Use this to check costs before committing to expensive requests.

        Args:
            symbols: Single symbol or list of symbols.
            schema: OHLCV schema.
            since: Start time.
            until: End time.
            stype_in: Symbol type for resolution.

        Returns:
            Estimated cost in USD.
        """
        symbol_list = [symbols] if isinstance(symbols, str) else symbols
        schema_enum = self._resolve_schema(schema)
        stype_enum = self._resolve_stype(stype_in)

        since_ms = parse_timestamp(since)
        until_ms = parse_timestamp(until) if until else int(time.time() * 1000)

        return self._adapter.get_cost(
            symbols=symbol_list,
            schema=schema_enum,
            start=since_ms,
            end=until_ms,
            stype_in=stype_enum,
        )

    def _get_symbol_data(
        self,
        symbol: str,
        schema: Schema,
        timeframe: str,
        requested: TimeRange,
        config: BentoFeed,
        stype_in: SType,
    ) -> pl.DataFrame | None:
        """Fetch data for a single symbol with caching.

        This is the core cache-first logic:
        1. Find existing cached files
        2. Identify gaps (missing time ranges)
        3. Fetch ONLY the gaps from API (this is where money is spent)
        4. Merge new data with cache
        5. Load and return requested range

        Args:
            symbol: Instrument symbol.
            schema: OHLCV schema.
            timeframe: Timeframe string for cache directory.
            requested: Time range to fetch.
            config: Feed configuration.
            stype_in: Symbol type for resolution.

        Returns:
            DataFrame with the requested data, or None if no data available.
        """
        # Get cache directory: data/bento_ohlcv/GLBX.MDP3/mnq.v.0/1m/
        cache_dir = get_cache_dir(
            self._dir, config.name, self._dataset, symbol, timeframe
        )

        # STEP 1: Find existing cached files (FREE - no API call)
        cached_files = find_cached_files(cache_dir, include_checkpoints=True)

        # STEP 2: Find gaps that need fetching (FREE - pure computation)
        gaps = find_gaps(cached_files, requested)

        # STEP 3: Fetch missing data for each gap (PAID - API calls)
        new_data_frames: list[pl.DataFrame] = []
        for gap in gaps:
            records = fetch_range(
                adapter=self._adapter,
                config=config,
                symbol=symbol,
                schema=schema,
                gap=gap,
                stype_in=stype_in,
                cache_dir=cache_dir,
            )
            if records:
                df = pl.DataFrame(records)
                df = df.select(config.columns)
                new_data_frames.append(df)

        new_data = pl.concat(new_data_frames) if new_data_frames else pl.DataFrame()

        # STEP 4: Merge new data with cache (FREE - local file operations)
        if len(new_data) > 0 or gaps:
            final_files = [f for f in cached_files if not f.is_checkpoint]
            merge_and_save(cache_dir, final_files, new_data, requested)
            cached_files = find_cached_files(cache_dir)

        # STEP 5: Load and return requested range (FREE - local file read)
        return load_cached_data(cached_files, requested)

    def _resolve_schema(self, schema: str | Schema) -> Schema:
        """Resolve schema string or enum to Schema enum."""
        if isinstance(schema, Schema):
            return schema
        if schema in SCHEMA_MAP:
            return SCHEMA_MAP[schema]
        raise ValueError(
            f"Unknown schema '{schema}'. Supported: {list(SCHEMA_MAP.keys())}"
        )

    def _resolve_stype(self, stype: str | SType) -> SType:
        """Resolve stype string or enum to SType enum."""
        if isinstance(stype, SType):
            return stype
        try:
            return SType(stype)
        except ValueError:
            raise ValueError(
                f"Unknown stype '{stype}'. Supported: {[s.value for s in SType]}"
            ) from None

    def close(self) -> None:
        """Close the adapter."""
        self._adapter.close()

    def __enter__(self) -> 'BentoHandler':
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
