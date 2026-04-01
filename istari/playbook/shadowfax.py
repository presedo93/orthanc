"""Volatility Breakout strategy — Shadowfax, lord of horses.

Technical name: Volatility Breakout (volatility_breakout.py).

Enters after volatility expansion from a compression period.
Designed for WAR_OF_THE_RING regimes, especially during transitions
from low to high volatility. Shadowfax bursts forth when the calm
breaks — explosive speed after patient waiting.
"""

import pandas as pd

from ..council_of_wizards import Istari, ordain_istari
from ..lore import atr, bollinger_width, range_compression
from ..scrolls import Direction, Scroll


class Shadowfax(Istari):
    """Volatility Breakout strategy based on range compression and expansion.

    Technical name: VolatilityBreakout — compression/expansion breakout.

    Detects periods of low volatility (compression), then enters when
    price breaks out of the compressed range. Uses ATR for stops and
    momentum fade for exits.

    Args:
        compression_period: Lookback for detecting compression.
        compression_threshold: Range compression ratio below which
            compression is detected.
        atr_multiplier: ATR multiplier for stop distance.
        exit_bars: Maximum bars to hold after breakout.
        direction: Trading direction (default: westward / long only).
    """

    def __init__(
        self,
        compression_period: int | float = 20,
        compression_threshold: float = 0.6,
        atr_multiplier: float = 1.5,
        exit_bars: int | float = 10,
        direction: Direction = Direction.WESTWARD,
        **_kwargs: object,
    ) -> None:
        self.compression_period = int(compression_period)
        self.compression_threshold = float(compression_threshold)
        self.atr_multiplier = float(atr_multiplier)
        self.exit_bars = int(exit_bars)
        self.direction = direction

    def config(self) -> Scroll:
        """Return strategy configuration from instance state."""
        return Scroll(
            name='shadowfax',
            params={
                'compression_period': self.compression_period,
                'compression_threshold': self.compression_threshold,
                'atr_multiplier': self.atr_multiplier,
                'exit_bars': self.exit_bars,
            },
            direction=self.direction,
        )

    def signals(self, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:  # type: ignore[type-arg]
        """Generate volatility breakout entry/exit signals.

        Entry: compression detected followed by range expansion and
        price breaking above/below the compression range.
        Exit: momentum fades (Bollinger width contracts) or ATR stop.

        Args:
            df: OHLCV DataFrame with 'high', 'low', 'close' columns.

        Returns:
            Tuple of (entries, exits) as boolean Series.
        """
        close: pd.Series = df['close']  # type: ignore[assignment]
        high: pd.Series = df['high']  # type: ignore[assignment]
        low: pd.Series = df['low']  # type: ignore[assignment]

        # Detect compression
        rc = range_compression(
            high, low, fast_period=5, slow_period=self.compression_period
        )
        was_compressed = rc.shift(1) < self.compression_threshold

        # Detect expansion (current range expanding from compression)
        is_expanding = rc > 1.0

        # Breakout levels from compression range
        compression_high = high.rolling(window=self.compression_period).max().shift(1)
        compression_low = low.rolling(window=self.compression_period).min().shift(1)

        breakout_up = close > compression_high
        breakout_down = close < compression_low

        # Bollinger width for momentum fade detection
        bw = bollinger_width(close, self.compression_period)
        bw_contracting = bw < bw.shift(1)  # type: ignore[operator]

        # ATR stop
        atr_values = atr(high, low, close)
        stop_long = close - self.atr_multiplier * atr_values
        stop_short = close + self.atr_multiplier * atr_values

        # Time-based exit (count bars since entry approximation)
        # Use a rolling window to detect stale breakouts
        bars_since_breakout_up = breakout_up.rolling(window=self.exit_bars).sum()
        momentum_fading = bw_contracting & (bars_since_breakout_up > 0)  # type: ignore[operator]

        match self.direction:
            case Direction.WESTWARD:
                entries_raw = was_compressed & is_expanding & breakout_up
                exits_raw = momentum_fading | (close < stop_long.shift(1))  # type: ignore[operator]
            case Direction.EASTWARD:
                entries_raw = was_compressed & is_expanding & breakout_down
                exits_raw = momentum_fading | (close > stop_short.shift(1))
            case Direction.ALL_ROADS:
                entries_raw = (
                    was_compressed & is_expanding & (breakout_up | breakout_down)
                )
                exits_raw = momentum_fading | (close < stop_long.shift(1))  # type: ignore[operator]

        entries = entries_raw.fillna(False).astype(bool)  # type: ignore[union-attr]
        exits = exits_raw.fillna(False).astype(bool)  # type: ignore[union-attr]

        return entries, exits


ordain_istari('shadowfax', Shadowfax)
