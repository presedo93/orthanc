"""SMA crossover strategy.

Goes long when the fast SMA crosses above the slow SMA, exits when
the fast SMA crosses below. A classic trend-following approach.
"""

import pandas as pd

from ..config import Direction, StrategyConfig
from ..registry import Strategy, register_strategy


class SmaCross(Strategy):
    """SMA crossover strategy.

    Args:
        fast_window: Period for the fast moving average.
        slow_window: Period for the slow moving average.
        direction: Trading direction (default: long only).
    """

    def __init__(
        self,
        fast_window: int | float = 10,
        slow_window: int | float = 30,
        direction: Direction = Direction.LONG_ONLY,
        **_kwargs: object,
    ) -> None:
        self.fast_window = int(fast_window)
        self.slow_window = int(slow_window)
        self.direction = direction

    def config(self) -> StrategyConfig:
        """Return strategy configuration from instance state."""
        return StrategyConfig(
            name='sma_cross',
            params={'fast_window': self.fast_window, 'slow_window': self.slow_window},
            direction=self.direction,
        )

    def signals(self, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:  # type: ignore[type-arg]
        """Generate SMA crossover entry/exit signals.

        Args:
            df: OHLCV DataFrame with a 'close' column.

        Returns:
            Tuple of (entries, exits) as boolean Series aligned with df index.
        """
        close = df['close']
        fast_ma = close.rolling(window=self.fast_window).mean()
        slow_ma = close.rolling(window=self.slow_window).mean()

        # Crossover: fast crosses above slow
        entries_raw = (fast_ma > slow_ma) & (fast_ma.shift(1) <= slow_ma.shift(1))  # type: ignore[operator]
        # Crossunder: fast crosses below slow
        exits_raw = (fast_ma < slow_ma) & (fast_ma.shift(1) >= slow_ma.shift(1))  # type: ignore[operator]

        # Fill NaN with False
        entries = entries_raw.fillna(False).astype(bool)  # type: ignore[union-attr]
        exits = exits_raw.fillna(False).astype(bool)  # type: ignore[union-attr]

        return entries, exits


register_strategy('sma_cross', SmaCross)
