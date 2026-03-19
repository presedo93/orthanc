"""Exchange-specific method caller for CCXT.

This module provides a clean way to call CCXT methods with exchange-specific
parameter requirements. Different exchanges need different parameters and
even different argument positions.

Example:
    >>> import ccxt
    >>> adapter = ExcAdapter(ccxt.bybit(), 'fetch_ohlcv')
    >>> data = adapter.method("BTC/USDT", "1h", since=1704067200000, until=1704153600000)
"""

import logging
from typing import Any, Callable

from ..errors import ccxt_errors
from .types import Exchange

logger = logging.getLogger(__name__)


class ExcAdapter:
    """Adapter for calling CCXT methods with exchange-specific parameters.

    Handles the complexity of different exchanges requiring different
    parameter names, argument positions, and special handling.

    Attributes:
        exchange: The CCXT exchange instance.
        exchange_id: The exchange identifier (lowercase).
    """

    def __init__(self, exchange: Exchange, name: str) -> None:
        """Initialize the adapter with a CCXT exchange instance.

        Args:
            exchange: Initialized CCXT exchange instance.
            name: Method name from CCXT.
        """
        self._name = name
        self._id = str(exchange.id).lower()
        self._method: Callable = getattr(exchange, name)

    @ccxt_errors()
    def method(
        self,
        symbol: str,
        timeframe: str | None = None,
        since: int | None = None,
        until: int | None = None,
    ) -> Any:
        """Generic method caller that routes to the appropriate call method.

        Automatically handles CCXT errors gracefully, logging them instead of
        raising exceptions. Returns an empty list if data is unavailable.

        Args:
            symbol: Trading pair symbol.
            timeframe: Candle/data timeframe (required for some methods).
            since: Start time in milliseconds.
            until: End time in milliseconds.

        Returns:
            List of records from the exchange, or None on error.

        Examples:
            >>> adapter = ExcAdapter(ccxt.bybit(), 'fetch_ohlcv')
            >>> data = adapter.method("BTC/USDT", "1h", since=1704067200000)
        """
        if self.is_out_of_range(since, until):
            logger.warning(
                f'Time range is out of bounds for {self._id} and {self._name}'
            )
            return None

        match self._id:
            case 'bybit':
                params = {'start': since, 'end': until}

                if timeframe is not None:
                    return self._method(symbol, timeframe, params=params)
                return self._method(symbol, params=params)
            case 'binance':
                params = {'startTime': since, 'endTime': until}

                if timeframe is not None:
                    return self._method(symbol, timeframe, params=params)
                return self._method(symbol, params=params)
            case 'okx':
                params = {'method': 'publicGetMarketHistoryTrades'}
                if timeframe is not None:
                    return self._method(symbol, timeframe, since=since, params=params)
                return self._method(symbol, since=since, params=params)
            case _:
                if timeframe is not None:
                    return self._method(symbol, timeframe, since)
                return self._method(symbol, since)

    def is_out_of_range(self, since: int | None, until: int | None) -> bool:
        binance = self._id == 'binance' and self._name in [
            'fetch_open_interest_history',
            'fetch_long_short_ratio_history',
        ]

        okx = self._id == 'okx' and self._name in [
            'fetch_open_interest_history',
            'fetch_long_short_ratio_history',
        ]

        if since is not None and until is not None:
            bigger = until - since >= 2_592_000_000
            if bigger and (binance or okx):
                return True

        return False
