"""CCXT-specific type definitions.

Technical name: ccxt/types.py — exchange types and data feed configs.
"""

from dataclasses import dataclass

import ccxt

Exchange = ccxt.binance | ccxt.binanceus | ccxt.bybit | ccxt.hyperliquid | ccxt.okx

OHLCV_COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
MARK_OHLCV_COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
OPEN_INTEREST_COLUMNS = ['timestamp', 'openInterestAmount', 'openInterestValue']
FUNDING_RATE_COLUMNS = ['timestamp', 'fundingRate']
LS_RATIO_COLUMNS = ['timestamp', 'longShortRatio']
TRADES_COLUMNS = ['timestamp', 'id', 'side', 'price', 'amount', 'cost']


@dataclass
class ShireFeed:
    """Technical name: DataFeed — configuration for a specific exchange data type.

    Defines the metadata needed to fetch, cache, and structure different
    types of exchange data (OHLCV, open interest, funding rates, etc.).

    Attributes:
        name: Identifier for cache directory (e.g., "ohlcv", "open_interest").
        method_name: CCXT method name (e.g., "fetch_ohlcv", "fetch_open_interest_history").
        columns: Column names for the DataFrame schema.
        checkpoint_interval: Number of iterations between checkpoints (default: 50).
    """

    name: str
    method_name: str
    columns: list[str]
    checkpoint_interval: int = 50


FEED_OHLCV = ShireFeed('ohlcv', 'fetch_ohlcv', OHLCV_COLUMNS)

FEED_MARK_OHLCV = ShireFeed('mark_ohlcv', 'fetch_mark_ohlcv', MARK_OHLCV_COLUMNS)

FEED_OPEN_INTEREST = ShireFeed(
    'open_interest', 'fetch_open_interest_history', OPEN_INTEREST_COLUMNS
)

FEED_FUNDING_RATE = ShireFeed(
    'funding_rate', 'fetch_funding_rate_history', FUNDING_RATE_COLUMNS
)

FEED_LS_RATIO = ShireFeed(
    'long_short_ratio', 'fetch_long_short_ratio_history', LS_RATIO_COLUMNS
)

FEED_TRADES = ShireFeed('trades', 'fetch_trades', TRADES_COLUMNS)
