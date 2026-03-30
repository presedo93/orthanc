"""Mean reversion strategy.

Trades deviations from the mean using Bollinger Bands and z-score.
Designed for SIDEWAYS regimes where price oscillates around a mean.
"""

import pandas as pd

from ..config import Direction, StrategyConfig
from ..features import bollinger_bands, z_score
from ..registry import Strategy, register_strategy


class MeanReversion(Strategy):
    """Mean reversion strategy using Bollinger Bands.

    Enters long at the lower Bollinger Band (oversold), exits at the
    middle band. Enters short at the upper band (overbought), exits
    at the middle band. Uses z-score for signal strength filtering.

    Args:
        period: Bollinger Band / z-score lookback period.
        num_std: Number of standard deviations for bands.
        zscore_entry: Minimum z-score magnitude to trigger entry.
        zscore_exit: Z-score magnitude below which to exit.
        direction: Trading direction (default: long only).
    """

    def __init__(
        self,
        period: int | float = 20,
        num_std: float = 2.0,
        zscore_entry: float = 1.5,
        zscore_exit: float = 0.5,
        direction: Direction = Direction.LONG_ONLY,
        **_kwargs: object,
    ) -> None:
        self.period = int(period)
        self.num_std = float(num_std)
        self.zscore_entry = float(zscore_entry)
        self.zscore_exit = float(zscore_exit)
        self.direction = direction

    def config(self) -> StrategyConfig:
        """Return strategy configuration from instance state."""
        return StrategyConfig(
            name='mean_reversion',
            params={
                'period': self.period,
                'num_std': self.num_std,
                'zscore_entry': self.zscore_entry,
                'zscore_exit': self.zscore_exit,
            },
            direction=self.direction,
        )

    def signals(self, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:  # type: ignore[type-arg]
        """Generate mean reversion entry/exit signals.

        Entry: price touches lower band with z-score confirmation.
        Exit: price returns to middle band.

        Args:
            df: OHLCV DataFrame with 'close' column.

        Returns:
            Tuple of (entries, exits) as boolean Series.
        """
        close: pd.Series = df['close']  # type: ignore[assignment]

        upper, mid, lower = bollinger_bands(close, self.period, self.num_std)
        zs = z_score(close, self.period)

        match self.direction:
            case Direction.LONG_ONLY:
                # Enter long at lower band (oversold)
                entries_raw = (close <= lower) & (zs <= -self.zscore_entry)
                # Exit when price returns to mid band
                exits_raw = (close >= mid) | (zs >= -self.zscore_exit)
            case Direction.SHORT_ONLY:
                # Enter short at upper band (overbought)
                entries_raw = (close >= upper) & (zs >= self.zscore_entry)
                # Exit when price returns to mid band
                exits_raw = (close <= mid) | (zs <= self.zscore_exit)
            case Direction.BOTH:
                # Long at lower, short at upper
                entries_raw = ((close <= lower) & (zs <= -self.zscore_entry)) | (
                    (close >= upper) & (zs >= self.zscore_entry)
                )
                exits_raw = (zs.abs() <= self.zscore_exit) | (
                    (close >= mid) & (close.shift(1) < mid.shift(1))  # type: ignore[operator]
                )

        entries = entries_raw.fillna(False).astype(bool)  # type: ignore[union-attr]
        exits = exits_raw.fillna(False).astype(bool)  # type: ignore[union-attr]

        return entries, exits


register_strategy('mean_reversion', MeanReversion)
