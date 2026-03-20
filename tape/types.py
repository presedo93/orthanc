"""Shared type definitions and parsing utilities for tape module."""

from datetime import datetime


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
