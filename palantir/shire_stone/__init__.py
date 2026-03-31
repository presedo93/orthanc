"""Shire Stone — CCXT exchange data fetching submodule.

Technical name: ccxt/ — CCXT-based exchange data fetching.
"""

from .handler import CCXTHandler
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

__all__ = [
    'CCXTHandler',
    'DataFeed',
    'Exchange',
    'FEED_FUNDING_RATE',
    'FEED_LS_RATIO',
    'FEED_MARK_OHLCV',
    'FEED_OHLCV',
    'FEED_OPEN_INTEREST',
    'FEED_TRADES',
]
