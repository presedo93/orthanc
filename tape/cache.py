"""Parquet cache management for OHLCV data."""

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl


@dataclass
class TimeRange:
    """Represents a half-open time range [since, until) in milliseconds.

    The range includes `since` but excludes `until`, following Python slice semantics.
    """

    since: int
    until: int

    def overlaps(self, other: 'TimeRange') -> bool:
        """Check if this range overlaps with another."""
        return self.since < other.until and other.since < self.until

    def contains(self, other: 'TimeRange') -> bool:
        """Check if this range fully contains another."""
        return self.since <= other.since and self.until >= other.until


@dataclass
class CachedFile:
    """Represents a cached parquet file with its time range."""

    path: Path
    time_range: TimeRange
    is_checkpoint: bool = False

    @classmethod
    def from_path(cls, path: Path) -> 'CachedFile | None':
        """Parse a parquet file path to extract time range.

        Supports both final and checkpoint files:
        - Final: {since_ms}_{until_ms}.parquet
        - Checkpoint: {since_ms}_{until_ms}.checkpoint.parquet
        """
        # Check for checkpoint file
        checkp_match = re.match(r'(\d+)_(\d+)\.checkpoint\.parquet$', path.name)
        if checkp_match:
            since, until = int(checkp_match.group(1)), int(checkp_match.group(2))
            return cls(path, TimeRange(since, until), True)

        # Check for regular cached file
        match = re.match(r'(\d+)_(\d+)\.parquet$', path.name)
        if not match:
            return None

        since, until = int(match.group(1)), int(match.group(2))
        return cls(path, TimeRange(since, until), False)


@dataclass
class CheckpointMetadata:
    """Metadata for checkpoint files to aid recovery."""

    gap_since: int
    gap_until: int
    data_since: int
    data_until: int
    created_at: int = field(default_factory=lambda: int(time.time() * 1000))
    row_count: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'gap_since': self.gap_since,
            'gap_until': self.gap_until,
            'data_since': self.data_since,
            'data_until': self.data_until,
            'created_at': self.created_at,
            'row_count': self.row_count,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'CheckpointMetadata':
        """Create from dictionary."""
        return cls(**data)


def get_cache_dir(
    base_dir: Path, data: str, exchange_id: str, symbol: str, timeframe: str | None
) -> Path:
    """Get the cache directory path for a symbol and data type.

    Structure: {base_dir}/{data}/{exchange}/{symbol}/{timeframe}/
    Symbol is sanitized: BTC/USDT:USDT -> btc_usdt_usdt

    Args:
        base_dir: Base directory for all cached data.
        data: Type of data (e.g., "ohlcv", "open_interest", "funding_rate").
        exchange_id: Exchange identifier (e.g., "bybit", "binance").
        symbol: Trading pair symbol (e.g., "BTC/USDT:USDT").
        timeframe: Timeframe string (e.g., "1h", "1d").

    Returns:
        Path to the cache directory, created if it doesn't exist.
    """
    safe_symbol = symbol.lower().replace('/', '_').replace(':', '_')

    timeframe = timeframe or ''
    cache_dir = base_dir / data / exchange_id / safe_symbol / timeframe
    cache_dir.mkdir(parents=True, exist_ok=True)

    return cache_dir


def find_cached_files(
    cache_dir: Path, include_checkpoints: bool = False
) -> list[CachedFile]:
    """Find all cached parquet files in a directory, sorted by since time.

    Args:
        cache_dir: Directory to search for cached files.
        include_checkpoints: If True, include checkpoint files in results.
                           If False (default), only return final cached files.

    Returns:
        List of CachedFile objects sorted by time range start.
    """
    files: list[CachedFile] = []
    if not cache_dir.exists():
        return files

    for path in cache_dir.glob('*.parquet'):
        cached = CachedFile.from_path(path)
        if cached:
            # Filter out checkpoints unless explicitly requested
            if include_checkpoints or not cached.is_checkpoint:
                files.append(cached)

    return sorted(files, key=lambda f: f.time_range.since)


def find_gaps(cached_files: list[CachedFile], requested: TimeRange) -> list[TimeRange]:
    """Find gaps in cached data that need to be fetched.

    Returns a list of time ranges that are not covered by cached files.
    """
    if not cached_files:
        return [requested]

    gaps: list[TimeRange] = []
    current_pos = requested.since

    for cached in cached_files:
        # Skip files that end before our current position
        if cached.time_range.until <= current_pos:
            continue
        # Skip files that start after our requested range
        if cached.time_range.since >= requested.until:
            break

        # If there's a gap before this file
        if cached.time_range.since > current_pos:
            gap_end = min(cached.time_range.since, requested.until)
            gaps.append(TimeRange(current_pos, gap_end))

        # Move current position to end of this file
        current_pos = max(current_pos, cached.time_range.until)

    # Check if there's a gap at the end
    if current_pos < requested.until:
        gaps.append(TimeRange(current_pos, requested.until))

    return gaps


def load_cached_data(
    cached_files: list[CachedFile], requested: TimeRange
) -> pl.DataFrame | None:
    """Load and filter cached data for the requested range."""
    relevant_files = [f for f in cached_files if f.time_range.overlaps(requested)]

    if not relevant_files:
        return None

    # Scan all relevant parquet files lazily
    paths = [str(f.path) for f in relevant_files]
    lf = pl.scan_parquet(paths)

    # Filter to requested range and collect
    df = (
        lf
        .filter(
            (pl.col('timestamp') >= requested.since)
            & (pl.col('timestamp') < requested.until)
        )
        .unique(subset=['timestamp'])
        .sort('timestamp')
        .collect()
    )

    return df if len(df) > 0 else None


def merge_and_save(
    cache_dir: Path,
    cached_files: list[CachedFile],
    new_data: pl.DataFrame,
    requested: TimeRange,
    cleanup_checkpoints: bool = True,
) -> Path | None:
    """Merge new data with cached data and save as a consolidated file.

    Creates a new file covering the full requested range and removes
    any overlapping cached files that are now superseded. Optionally
    cleans up checkpoint files for the completed range.

    Args:
        cache_dir: Directory to save the consolidated file.
        cached_files: List of existing cached files (should exclude checkpoints).
        new_data: New data to merge with cached data.
        requested: Time range being consolidated.
        cleanup_checkpoints: If True, remove checkpoint files overlapping the range.

    Returns:
        Path to the consolidated parquet file, or None if no data to save.
    """
    # Load existing cached data for the range (final files only)
    cached_df = load_cached_data(cached_files, requested)

    # Also load checkpoint data if it exists to avoid losing progress
    checkpoint_files = find_cached_files(cache_dir, include_checkpoints=True)
    checkpoints = [f for f in checkpoint_files if f.is_checkpoint]
    checkpoint_df = load_cached_data(checkpoints, requested) if checkpoints else None

    # Combine cached, checkpoint, and new data
    data_frames = []
    if cached_df is not None:
        data_frames.append(cached_df)
    if checkpoint_df is not None:
        data_frames.append(checkpoint_df)
    if len(new_data) > 0:
        data_frames.append(new_data)

    if data_frames:
        combined = pl.concat(data_frames)
        combined = combined.unique(subset=['timestamp']).sort('timestamp')
    else:
        combined = pl.DataFrame()

    # Determine actual range from data
    if len(combined) == 0:
        if cleanup_checkpoints:
            _cleanup_checkpoints(cache_dir, requested)
        return None

    actual_since: int = combined['timestamp'].min()  # type: ignore[assignment]
    actual_until: int = combined['timestamp'].max()  # type: ignore[assignment]

    # Use requested bounds, ensuring file_until is exclusive (covers last timestamp)
    file_since = min(requested.since, actual_since)
    file_until = max(requested.until, actual_until + 1)

    # Write to temporary file first for atomic operation
    temp_path = cache_dir / f'.tmp_{file_since}_{file_until}.parquet'
    combined.write_parquet(temp_path)

    # Create final file path
    final_path = cache_dir / f'{file_since}_{file_until}.parquet'

    # Atomic rename (overwrites if exists)
    temp_path.replace(final_path)

    # Remove old overlapping cached files
    new_range = TimeRange(file_since, file_until)
    for cached in cached_files:
        if new_range.contains(cached.time_range) and cached.path != final_path:
            cached.path.unlink(missing_ok=True)

    # Clean up checkpoints for this range
    if cleanup_checkpoints:
        _cleanup_checkpoints(cache_dir, new_range)

    return final_path


def save_checkpoint(cache_dir: Path, data: pl.DataFrame, gap: TimeRange) -> Path | None:
    """Save checkpoint data incrementally during fetch operations.

    Creates a checkpoint file with metadata for crash recovery. Checkpoint
    files use the naming convention: {data_since}_{data_until}.checkpoint.parquet

    This function performs atomic writes and includes metadata to help with
    recovery scenarios.

    Args:
        cache_dir: Directory to save the checkpoint file.
        data: DataFrame containing the data to checkpoint.
        gap: Time range being fetched (used for metadata).

    Returns:
        Path to the checkpoint file, or None if data is empty.
    """
    if len(data) == 0:
        return None

    # Deduplicate and sort before analyzing
    data = data.unique(subset=['timestamp']).sort('timestamp')

    # Get actual time range from data
    data_since: int = data['timestamp'].min()  # type: ignore[assignment]
    data_until: int = data['timestamp'].max()  # type: ignore[assignment]

    # Create checkpoint file path with special suffix
    checkpoint_path = cache_dir / f'{data_since}_{data_until}.checkpoint.parquet'
    temp_path = cache_dir / f'.tmp_{data_since}_{data_until}.checkpoint.parquet'

    # Create metadata file path
    metadata_path = cache_dir / f'{data_since}_{data_until}.checkpoint.json'

    # Write data atomically
    data.write_parquet(temp_path)
    temp_path.replace(checkpoint_path)

    # Write metadata for recovery
    metadata = CheckpointMetadata(
        gap_since=gap.since,
        gap_until=gap.until,
        data_since=data_since,
        data_until=data_until,
        row_count=len(data),
    )

    metadata_path.write_text(json.dumps(metadata.to_dict(), indent=2))
    return checkpoint_path


def cleanup_old_checkpoints(cache_dir: Path, max_age_hours: int = 24) -> int:
    """Remove old checkpoint files that are likely orphaned.

    This is a maintenance function to clean up checkpoints from failed
    operations. It removes checkpoint files (and their metadata) that
    are older than the specified age.

    Args:
        cache_dir: Directory to search for old checkpoints.
        max_age_hours: Maximum age in hours before considering a checkpoint orphaned.

    Returns:
        Number of checkpoint files removed.
    """
    if not cache_dir.exists():
        return 0

    cutoff_time = time.time() - (max_age_hours * 3600)
    removed_count = 0

    for checkpoint_file in cache_dir.glob('*.checkpoint.parquet'):
        # Check file modification time
        if checkpoint_file.stat().st_mtime < cutoff_time:
            checkpoint_file.unlink(missing_ok=True)
            removed_count += 1

            # Also remove associated metadata
            metadata_file = checkpoint_file.with_name(
                checkpoint_file.name.replace('.checkpoint.parquet', '.checkpoint.json')
            )
            if metadata_file.exists():
                metadata_file.unlink(missing_ok=True)

    return removed_count


def _cleanup_checkpoints(cache_dir: Path, completed_range: TimeRange) -> None:
    """Remove checkpoint files that overlap with a completed range.

    This is an internal helper that removes checkpoints after a successful
    merge operation, as they're no longer needed for recovery.

    Args:
        cache_dir: Directory containing checkpoint files.
        completed_range: Time range that has been successfully consolidated.
    """
    if not cache_dir.exists():
        return

    checkpoint_files = find_cached_files(cache_dir, include_checkpoints=True)
    checkpoints = [f for f in checkpoint_files if f.is_checkpoint]

    for checkpoint in checkpoints:
        # Remove checkpoint if it's fully contained in the completed range
        if completed_range.contains(checkpoint.time_range):
            checkpoint.path.unlink(missing_ok=True)

            # Also remove metadata file
            metadata_path = checkpoint.path.with_name(
                checkpoint.path.name.replace('.checkpoint.parquet', '.checkpoint.json')
            )
            if metadata_path.exists():
                metadata_path.unlink(missing_ok=True)
