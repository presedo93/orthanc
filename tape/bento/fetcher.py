"""Data fetching for DataBento API with gap handling.

This module provides the fetch_range function that retrieves OHLCV data
for a specific time gap, with checkpoint support for crash recovery.
"""

import logging
from pathlib import Path
from typing import Any

import polars as pl

from ..cache import TimeRange, save_checkpoint
from .adapter import BentoAdapter
from .types import BentoFeed, Schema, SType

logger = logging.getLogger(__name__)


def fetch_range(
    adapter: BentoAdapter,
    config: BentoFeed,
    symbol: str,
    schema: Schema,
    gap: TimeRange,
    stype_in: SType = SType.CONTINUOUS,
    cache_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Fetch OHLCV data for a time range from DataBento API.

    This function is called for each gap in the cached data. It fetches
    the missing data and returns it as a list of records.

    Unlike CCXT which requires pagination, DataBento returns complete
    results in a single request (up to reasonable limits).

    Args:
        adapter: Initialized BentoAdapter instance.
        config: Feed configuration.
        symbol: Instrument symbol (e.g., 'MNQ.v.0').
        schema: OHLCV schema (ohlcv-1m, ohlcv-1h, etc.).
        gap: Time range to fetch.
        stype_in: Symbol type for input resolution.
        cache_dir: Directory for checkpoint files (None disables checkpointing).

    Returns:
        List of OHLCV records with keys: timestamp, open, high, low, close, volume.
    """
    logger.info(
        'Fetching gap for %s: %d to %d (%s)',
        symbol,
        gap.since,
        gap.until,
        schema.value,
    )

    try:
        records = adapter.fetch_ohlcv(
            symbol=symbol,
            schema=schema,
            start=gap.since,
            end=gap.until,
            stype_in=stype_in,
        )

        if not records:
            logger.debug('No records returned for gap %d-%d', gap.since, gap.until)
            return []

        # Filter to exact requested range (DataBento may return slightly more)
        filtered = _filter_to_range(records, gap)

        logger.info(
            'Fetched %d records for %s (filtered from %d)',
            len(filtered),
            symbol,
            len(records),
        )

        # Save checkpoint for crash recovery if we have data
        if filtered and cache_dir:
            _save_checkpoint(cache_dir, config, filtered, gap)

        return filtered

    except Exception as e:
        logger.error('Failed to fetch gap for %s: %s', symbol, e)
        # Return empty list to allow partial progress
        # The gap will be retried on next request
        return []


def _filter_to_range(
    records: list[dict[str, Any]], gap: TimeRange
) -> list[dict[str, Any]]:
    """Filter records to exact requested time range.

    DataBento may return data slightly outside the requested range,
    so we filter to ensure clean boundaries.

    Args:
        records: List of OHLCV records.
        gap: Requested time range.

    Returns:
        Filtered records within [since, until) range.
    """
    return [r for r in records if gap.since <= r['timestamp'] < gap.until]


def _save_checkpoint(
    cache_dir: Path,
    config: BentoFeed,
    records: list[dict[str, Any]],
    gap: TimeRange,
) -> None:
    """Save checkpoint for crash recovery.

    Converts records to DataFrame and saves using the cache checkpoint system.

    Args:
        cache_dir: Directory for checkpoint files.
        config: Feed configuration.
        records: OHLCV records to checkpoint.
        gap: Original gap being fetched.
    """
    if not records:
        return

    try:
        df = pl.DataFrame(records)
        df = df.select(config.columns)
        save_checkpoint(cache_dir, df, gap)
        logger.debug('Saved checkpoint with %d records', len(records))
    except Exception as e:
        # Checkpoint failure shouldn't stop the fetch
        logger.warning('Failed to save checkpoint: %s', e)
