"""CCXT-specific type definitions."""

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
class DataFeed:
    """Configuration for a specific exchange data type.

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


FEED_OHLCV = DataFeed('ohlcv', 'fetch_ohlcv', OHLCV_COLUMNS)

FEED_MARK_OHLCV = DataFeed('mark_ohlcv', 'fetch_mark_ohlcv', MARK_OHLCV_COLUMNS)

FEED_OPEN_INTEREST = DataFeed(
    'open_interest', 'fetch_open_interest_history', OPEN_INTEREST_COLUMNS
)

FEED_FUNDING_RATE = DataFeed(
    'funding_rate', 'fetch_funding_rate_history', FUNDING_RATE_COLUMNS
)

FEED_LS_RATIO = DataFeed(
    'long_short_ratio', 'fetch_long_short_ratio_history', LS_RATIO_COLUMNS
)

FEED_TRADES = DataFeed('trades', 'fetch_trades', TRADES_COLUMNS)
