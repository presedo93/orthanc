"""Trend following strategy.

Enters on breakouts with EMA alignment confirmation. Uses ATR-based
trailing stops and structure breaks for exits. Designed for TREND_UP
and TREND_DOWN regimes.
"""

import pandas as pd

from ..config import Direction, StrategyConfig
from ..features import adx, atr, ema
from ..registry import Strategy, register_strategy


class TrendFollow(Strategy):
    """Trend following strategy using EMA alignment and breakout entries.

    Enters long when price breaks above the recent high with EMA alignment
    (fast > slow) and ADX confirming trend strength. Exits on ATR trailing
    stop or EMA crossunder.

    Args:
        fast_window: Period for the fast EMA.
        slow_window: Period for the slow EMA.
        atr_multiplier: ATR multiplier for trailing stop distance.
        adx_period: ADX lookback period.
        adx_threshold: Minimum ADX to confirm trend strength.
        breakout_period: Lookback for breakout high/low detection.
        direction: Trading direction (default: long only).
    """

    def __init__(
        self,
        fast_window: int | float = 10,
        slow_window: int | float = 30,
        atr_multiplier: float = 2.0,
        adx_period: int | float = 14,
        adx_threshold: float = 20.0,
        breakout_period: int | float = 20,
        direction: Direction = Direction.LONG_ONLY,
        **_kwargs: object,
    ) -> None:
        self.fast_window = int(fast_window)
        self.slow_window = int(slow_window)
        self.atr_multiplier = float(atr_multiplier)
        self.adx_period = int(adx_period)
        self.adx_threshold = float(adx_threshold)
        self.breakout_period = int(breakout_period)
        self.direction = direction

    def config(self) -> StrategyConfig:
        """Return strategy configuration from instance state."""
        return StrategyConfig(
            name='trend_follow',
            params={
                'fast_window': self.fast_window,
                'slow_window': self.slow_window,
                'atr_multiplier': self.atr_multiplier,
                'adx_period': self.adx_period,
                'adx_threshold': self.adx_threshold,
                'breakout_period': self.breakout_period,
            },
            direction=self.direction,
        )

    def signals(self, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:  # type: ignore[type-arg]
        """Generate trend following entry/exit signals.

        Entry: price breaks above previous high with EMA alignment and ADX
        confirmation. Exit: price crosses below fast EMA or ATR trailing
        stop is hit.

        Args:
            df: OHLCV DataFrame with 'open', 'high', 'low', 'close', 'volume'.

        Returns:
            Tuple of (entries, exits) as boolean Series.
        """
        close: pd.Series = df['close']  # type: ignore[assignment]
        high: pd.Series = df['high']  # type: ignore[assignment]
        low: pd.Series = df['low']  # type: ignore[assignment]

        # EMA alignment
        fast_ema = ema(close, self.fast_window)
        slow_ema = ema(close, self.slow_window)
        ema_aligned_long = fast_ema > slow_ema

        # ADX trend strength filter
        adx_values = adx(high, low, close, self.adx_period)
        strong_trend = adx_values > self.adx_threshold

        # Breakout detection
        prev_high = high.rolling(window=self.breakout_period).max().shift(1)
        prev_low = low.rolling(window=self.breakout_period).min().shift(1)
        breakout_long = close > prev_high
        breakout_short = close < prev_low

        # ATR trailing stop
        atr_values = atr(high, low, close)
        trailing_stop_long = close - self.atr_multiplier * atr_values
        trailing_stop_short = close + self.atr_multiplier * atr_values

        # Structure break exit (fast EMA crossunder)
        ema_cross_under = (fast_ema < slow_ema) & (
            fast_ema.shift(1) >= slow_ema.shift(1)
        )  # type: ignore[operator]

        # Combine signals based on direction
        match self.direction:
            case Direction.LONG_ONLY:
                entries_raw = breakout_long & ema_aligned_long & strong_trend
                # Exit on EMA crossunder or price drops below trailing stop
                exits_raw = ema_cross_under | (close < trailing_stop_long.shift(1))  # type: ignore[operator]
            case Direction.SHORT_ONLY:
                ema_aligned_short = fast_ema < slow_ema
                entries_raw = breakout_short & ema_aligned_short & strong_trend
                exits_raw = (
                    (fast_ema > slow_ema) & (fast_ema.shift(1) <= slow_ema.shift(1))  # type: ignore[operator]
                ) | (close > trailing_stop_short.shift(1))
            case Direction.BOTH:
                entries_raw = (breakout_long & ema_aligned_long & strong_trend) | (
                    breakout_short & (fast_ema < slow_ema) & strong_trend
                )
                exits_raw = ema_cross_under | (close < trailing_stop_long.shift(1))  # type: ignore[operator]

        entries = entries_raw.fillna(False).astype(bool)  # type: ignore[union-attr]
        exits = exits_raw.fillna(False).astype(bool)  # type: ignore[union-attr]

        return entries, exits


register_strategy('trend_follow', TrendFollow)
