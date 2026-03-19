"""DataBento data fetching submodule.

This module provides access to DataBento futures data with intelligent caching.

Continuous Contract Roll Types
------------------------------
DataBento supports three roll rules for continuous contracts. Use these
suffixes when constructing symbols with stype_in='continuous':

- **Calendar (c)**: Roll by expiration date.
  Example: ES.c.0 (E-mini S&P 500, calendar roll, front month)

- **Volume (v)**: Roll by highest trading volume (previous day).
  Example: MNQ.v.0 (Micro Nasdaq-100, volume roll, front month)

- **Open Interest (n)**: Roll by highest open interest (previous day's close).
  Example: CL.n.0 (Crude Oil, open interest roll, front month)

Symbol Format: {SYMBOL}.{ROLL_TYPE}.{RANK}
- SYMBOL: Base futures symbol (ES, MNQ, CL, etc.)
- ROLL_TYPE: c (calendar), v (volume), or n (open interest)
- RANK: 0 = front month, 1 = second month, etc.

Examples:
    >>> from tape import BentoHandler
    >>> handler = BentoHandler(api_key="db-xxx", dataset="GLBX.MDP3")
    >>> df = handler.get_ohlcv(
    ...     symbols="MNQ.v.0",
    ...     schema="ohlcv-1m",
    ...     since="2024-01-01",
    ...     until="2024-03-01",
    ... )
"""

from .adapter import BentoAdapter
from .handler import BentoHandler
from .types import (
    FEED_MAP,
    FEED_OHLCV_1D,
    FEED_OHLCV_1H,
    FEED_OHLCV_1M,
    FEED_OHLCV_1S,
    OHLCV_COLUMNS,
    SCHEMA_MAP,
    SCHEMA_TO_TIMEFRAME,
    BentoFeed,
    RollType,
    Schema,
    SType,
)

__all__ = [
    # Main handler
    'BentoHandler',
    # Adapter (for advanced use)
    'BentoAdapter',
    # Types
    'BentoFeed',
    'Schema',
    'SType',
    'RollType',
    # Constants
    'OHLCV_COLUMNS',
    'SCHEMA_MAP',
    'SCHEMA_TO_TIMEFRAME',
    'FEED_MAP',
    # Pre-configured feeds
    'FEED_OHLCV_1S',
    'FEED_OHLCV_1M',
    'FEED_OHLCV_1H',
    'FEED_OHLCV_1D',
]
