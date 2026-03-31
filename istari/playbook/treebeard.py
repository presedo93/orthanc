"""Mean Reversion strategy — Treebeard the Ent.

Technical name: Mean Reversion (mean_reversion.py).

Trades deviations from the mean using Bollinger Bands and z-score.
Designed for LONG_PEACE regimes where price oscillates around a mean.
Treebeard is patient and always returns to where he started —
like price reverting to the mean.
"""

import pandas as pd

from ..council_of_wizards import Strategy, ordain_istari
from ..lore import bollinger_bands, z_score
from ..scrolls import Direction, StrategyConfig


class Treebeard(Strategy):
    """Mean Reversion strategy using Bollinger Bands.

    Technical name: MeanReversion — Bollinger Bands / mean reversion.

    Enters long at the lower Bollinger Band (oversold), exits at the
    middle band. Enters short at the upper band (overbought), exits
    at the middle band. Uses z-score for signal strength filtering.

    Args:
        period: Bollinger Band / z-score lookback period.
        num_std: Number of standard deviations for bands.
        zscore_entry: Minimum z-score magnitude to trigger entry.
        zscore_exit: Z-score magnitude below which to exit.
        direction: Trading direction (default: westward / long only).
    """

    def __init__(
        self,
        period: int | float = 20,
        num_std: float = 2.0,
        zscore_entry: float = 1.5,
        zscore_exit: float = 0.5,
        direction: Direction = Direction.WESTWARD,
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
            name='treebeard',
            params={
                'period': self.period,
                'num_std': self.num_std,
                'zscore_entry': self.zscore_entry,
                'zscore_exit': self.zscore_exit,
            },
            direction=self.direction,
        )

    def signals(self, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
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
            case Direction.WESTWARD:
                # Enter long at lower band (oversold)
                entries_raw = (close <= lower) & (zs <= -self.zscore_entry)
                # Exit when price returns to mid band
                exits_raw = (close >= mid) | (zs >= -self.zscore_exit)
            case Direction.EASTWARD:
                # Enter short at upper band (overbought)
                entries_raw = (close >= upper) & (zs >= self.zscore_entry)
                # Exit when price returns to mid band
                exits_raw = (close <= mid) | (zs <= self.zscore_exit)
            case Direction.ALL_ROADS:
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


ordain_istari('treebeard', Treebeard)
