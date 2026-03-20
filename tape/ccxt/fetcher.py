"""Generic data fetching with pagination support for different exchanges."""

from pathlib import Path
from typing import Any

import polars as pl

from ..cache import TimeRange, save_checkpoint
from .adapter import ExcAdapter
from .types import DataFeed, Exchange


def fetch_range(
    exchange: Exchange,
    config: DataFeed,
    symbol: str,
    timeframe: str | None,
    gap: TimeRange,
    cache_dir: Path | None = None,
) -> list[list] | list[dict]:
    """Fetch exchange data for a complete time range with pagination.

    Automatically detects pagination direction by checking if the first
    element returned matches 'since' (forward) or 'until' (backward).

    Args:
        exchange: Initialized ccxt exchange instance.
        config: CCXT method config to call (e.g., 'fetch_ohlcv').
        symbol: Trading pair symbol (e.g., 'BTC/USDT').
        timeframe: Candle timeframe (e.g., '1h', '1d'). Can be None for tick data.
        gap: Time range to fetch.
        cache_dir: Directory for checkpoint files (None disables checkpointing).

    Returns:
        List of data records (either lists or dicts), sorted by timestamp ascending.
    """
    # Create exchange-specific adapter
    adapter = ExcAdapter(exchange, config.method_name)
    initial = adapter.method(symbol, timeframe, gap.since, gap.until)

    if not initial:
        return []

    # Extract timestamp from first element (handle both list and dict formats)
    first_ts = _extract_timestamp(initial[0])

    # Calculate step size based on timeframe (default 1000ms for tick data)
    if timeframe:
        step_ms = exchange.parse_timeframe(timeframe) * 1000
    else:
        step_ms = 1000

    # Detect direction: if first element is close to 'since', we're going forward
    if abs(first_ts - gap.since) < step_ms:
        return _fetch_forward(
            adapter, symbol, timeframe, gap, initial, config, step_ms, cache_dir
        )
    return _fetch_backward(adapter, symbol, timeframe, gap, initial, config, cache_dir)


def _extract_timestamp(record: list | dict) -> int:
    """Extract timestamp from a data record (list or dict format)."""
    if isinstance(record, list):
        return record[0]
    elif isinstance(record, dict):
        return record.get('timestamp', 0)


def _fetch_forward(
    adapter: ExcAdapter,
    symbol: str,
    timeframe: str | None,
    gap: TimeRange,
    initial: list[list] | list[dict],
    config: DataFeed,
    step_ms: int,
    cache_dir: Path | None = None,
) -> list[list] | list[dict]:
    """Fetch data moving forward in time (binance-style pagination).

    Implements stale data detection to handle exchanges that return the same
    data repeatedly when reaching their available data limit. Exits early after
    detecting identical timestamps in consecutive responses.
    """
    seen_timestamps: set[int] = set()
    all_data: list[Any] = []
    iteration, stale_count = 0, 0
    prev_last_ts, max_stale_iterations = None, 2

    # Process initial batch with deduplication
    for record in initial:
        ts = _extract_timestamp(record)
        if ts < gap.until and ts not in seen_timestamps:
            seen_timestamps.add(ts)
            all_data.append(record)

    last_ts = _extract_timestamp(initial[-1])
    if last_ts >= gap.until:
        return all_data

    current_since, prev_last_ts = last_ts + step_ms, last_ts
    while current_since < gap.until:
        iteration += 1
        data = adapter.method(symbol, timeframe, current_since, gap.until)

        if not data:
            break

        # Filter and deduplicate records within our range
        for record in data:
            ts = _extract_timestamp(record)
            if ts < gap.until and ts not in seen_timestamps:
                seen_timestamps.add(ts)
                all_data.append(record)

        # Checkpoint if needed
        if iteration % config.checkpoint_interval == 0:
            _checkpoint(cache_dir, all_data, gap, config.columns)

        # Move to next batch
        last_ts = _extract_timestamp(data[-1])

        # Check if we're getting the same data repeatedly (endpoint limit reached)
        if last_ts == prev_last_ts:
            stale_count += 1
            if stale_count >= max_stale_iterations:
                break  # Exit: endpoint returning same data, no more available
        else:
            stale_count, prev_last_ts = 0, last_ts  # Reset counter if we got new data

        current_since = last_ts + step_ms

        # Stop if we've gone past our target
        if last_ts >= gap.until:
            break

    return all_data


def _fetch_backward(
    adapter: ExcAdapter,
    symbol: str,
    timeframe: str | None,
    gap: TimeRange,
    initial: list[list] | list[dict],
    config: DataFeed,
    cache_dir: Path | None = None,
) -> list[list] | list[dict]:
    """Fetch data moving backward in time (bybit-style pagination).

    Implements stale data detection to handle exchanges that return the same
    data repeatedly when reaching their available data limit. Exits early after
    detecting identical timestamps in consecutive responses.
    """
    all_data: dict[int, Any] = {}  # Use dict to dedupe by timestamp
    iteration, stale_count = 0, 0
    prev_first_ts, max_stale_iterations = None, 2

    # Process initial batch
    for record in initial:
        ts = _extract_timestamp(record)
        if gap.since <= ts < gap.until:
            all_data[ts] = record

    first_ts = _extract_timestamp(initial[0])
    if first_ts <= gap.since:
        return [all_data[ts] for ts in sorted(all_data.keys())]

    current_until, prev_first_ts = first_ts, first_ts
    while current_until > gap.since:
        iteration += 1
        data = adapter.method(symbol, timeframe, gap.since, current_until)

        if not data:
            break

        # Filter records within our range and dedupe
        for record in data:
            ts = _extract_timestamp(record)
            if gap.since <= ts < gap.until:
                all_data[ts] = record

        # Checkpoint if needed
        if iteration % config.checkpoint_interval == 0:
            backup_data = [all_data[ts] for ts in sorted(all_data.keys())]
            _checkpoint(cache_dir, backup_data, gap, config.columns)

        # Move backward
        first_ts = _extract_timestamp(data[0])

        # Check if we're getting the same data repeatedly (endpoint limit reached)
        if first_ts == prev_first_ts:
            stale_count += 1
            if stale_count >= max_stale_iterations:
                break  # Exit: endpoint returning same data, no more available
        else:
            stale_count, prev_first_ts = 0, first_ts  # Reset counter if we got new data

        current_until = first_ts

        # Stop if we've gone before our target
        if first_ts <= gap.since:
            break

    # Sort by timestamp ascending
    return [all_data[ts] for ts in sorted(all_data.keys())]


def _checkpoint(
    checkpoint_dir: Path | None,
    data: list[list] | list[dict],
    gap: TimeRange,
    columns: list[str] | None,
) -> None:
    """Convert data to DataFrame and call checkpoint callback."""
    if not data or not checkpoint_dir:
        return

    # Convert to DataFrame
    if isinstance(data[0], dict):
        df = pl.DataFrame(data)
        if columns:
            df = df.select(columns)
    else:
        if not columns:
            raise ValueError('columns required for list-based data')
        df = pl.DataFrame(data, schema=columns, orient='row')

    save_checkpoint(checkpoint_dir, df, gap)
