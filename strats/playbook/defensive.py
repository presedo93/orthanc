"""Defensive / no-trade strategy.

Reduces exposure and avoids trades during unfavorable conditions.
Designed for LOW_ACTIVITY regimes and unstable transitions.

This strategy intentionally generates no entry signals. When weighted
in a multi-strategy system, it dilutes the overall signal strength,
effectively reducing position sizing and trade frequency.
"""

import pandas as pd

from ..config import Direction, StrategyConfig
from ..registry import Strategy, register_strategy


class Defensive(Strategy):
    """Defensive strategy that generates no entries and always exits.

    When combined with other strategies via the regime switcher,
    its weight represents the proportion of capital that should
    remain uninvested. Higher defensive weight = less trading.

    Args:
        direction: Trading direction (ignored — always generates exits).
    """

    def __init__(
        self,
        direction: Direction = Direction.LONG_ONLY,
        **_kwargs: object,
    ) -> None:
        self.direction = direction

    def config(self) -> StrategyConfig:
        """Return strategy configuration from instance state."""
        return StrategyConfig(
            name='defensive',
            params={},
            direction=self.direction,
        )

    def signals(self, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:  # type: ignore[type-arg]
        """Generate defensive signals: no entries, always exit.

        Args:
            df: OHLCV DataFrame (columns not used).

        Returns:
            Tuple of (entries, exits) where entries are all False
            and exits are all True.
        """
        no_entries = pd.Series(False, index=df.index)
        always_exit = pd.Series(True, index=df.index)
        return no_entries, always_exit


register_strategy('defensive', Defensive)
