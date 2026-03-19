"""Error handling utilities for tape module.

This module provides decorators, utilities, and custom exceptions for handling
data fetching errors gracefully with clean logging instead of verbose stack traces.
"""

import functools
import logging
from typing import Any, Callable, ParamSpec, TypeVar

import ccxt

P = ParamSpec('P')
T = TypeVar('T')

logger = logging.getLogger(__name__)


class DataFetchError(Exception):
    """Raised when data fetching fails."""


def ccxt_errors(
    level: int = logging.WARNING,
) -> Callable[[Callable[P, T]], Callable[P, T | Any]]:
    """Decorator to handle CCXT errors gracefully with clean logging.

    Instead of showing the full stack trace, this decorator logs a concise
    error message indicating the issue. Common scenarios include:
    - Exchange doesn't support the requested data type
    - Data not available for the requested date range
    - Rate limiting or network issues
    - Invalid symbols or parameters

    Args:
        level: Logging level to use (default: WARNING).

    Returns:
        Decorated function that handles CCXT errors gracefully.

    Examples:
        >>> @ccxt_errors()
        ... def fetch_data(exchange, symbol):
        ...     return exchange.fetch_ohlcv(symbol, "1h")

        >>> @ccxt_errors(level=logging.ERROR)
        ... def critical_fetch(exchange, symbol):
        ...     return exchange.fetch_ohlcv(symbol, "1h")
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T | Any]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T | Any:
            try:
                return func(*args, **kwargs)
            except ccxt.NotSupported as e:
                logger.log(level, f'Exchange does not support this operation - {e}')
            except ccxt.BadSymbol as e:
                logger.log(level, f'Invalid or unsupported symbol - {e}')
            except ccxt.BadRequest as e:
                logger.log(level, f'Invalid request parameters - {e}')
            except ccxt.ExchangeNotAvailable as e:
                logger.log(level, f'Exchange is unavailable - {e}')
            except ccxt.RateLimitExceeded as e:
                logger.log(level, f'Rate limit exceeded - {e}')
            except ccxt.NetworkError as e:
                logger.log(level, f'Network error occurred - {e}')
            except ccxt.AuthenticationError as e:
                logger.log(level, f'Authentication failed - {e}')
            except ccxt.ExchangeError as e:
                logger.log(level, f'Exchange error - {e}')
            except ccxt.BaseError as e:
                logger.log(level, f'CCXT error - {e}')
            return None

        return wrapper

    return decorator
