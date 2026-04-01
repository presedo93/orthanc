"""Defensive / no-trade strategy — Shelob the spider.

Technical name: Defensive (defensive.py).

Reduces exposure and avoids trades during unfavorable conditions.
Designed for SLEEP_OF_THE_ENTS regimes and unstable transitions.
Shelob lurks in darkness and waits — this strategy intentionally
generates no entry signals, reducing overall exposure.

This strategy intentionally generates no entry signals. When weighted
in a multi-strategy system, it dilutes the overall signal strength,
effectively reducing position sizing and trade frequency.
"""

import pandas as pd

from ..council_of_wizards import Istari, ordain_istari
from ..scrolls import Direction, Scroll


class Shelob(Istari):
    """Defensive strategy that generates no entries and always exits.

    Technical name: Defensive — no-trade / risk-off strategy.

    When combined with other strategies via the regime switcher,
    its weight represents the proportion of capital that should
    remain uninvested. Higher defensive weight = less trading.

    Args:
        direction: Trading direction (ignored — always generates exits).
    """

    def __init__(
        self,
        direction: Direction = Direction.WESTWARD,
        **_kwargs: object,
    ) -> None:
        self.direction = direction

    def config(self) -> Scroll:
        """Return strategy configuration from instance state."""
        return Scroll(
            name='shelob',
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


ordain_istari('shelob', Shelob)
