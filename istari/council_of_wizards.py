"""Strategy base class and registry.

Defines the Strategy abstract base class that all strategies must implement,
and provides a registry for looking up strategies by name.

Technical name: registry.py — strategy ABC and name-based registry.
"""

from abc import ABC, abstractmethod

import pandas as pd

from .scrolls import Scroll

# ---------------------------------------------------------------------------
# Strategy ABC
# ---------------------------------------------------------------------------


class Istari(ABC):
    """Technical name: Strategy — base class for all trading strategies.

    A strategy takes OHLCV data and produces boolean entry/exit signals.
    Configuration is passed at construction time so the strategy instance
    is fully configured and ready to generate signals.
    """

    @abstractmethod
    def signals(self, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:  # type: ignore[type-arg]
        """Generate entry and exit signals.

        Args:
            df: OHLCV DataFrame with DatetimeIndex.

        Returns:
            Tuple of (entries, exits) as boolean pd.Series.
        """
        ...

    @abstractmethod
    def config(self) -> Scroll:
        """Return the strategy's own configuration.

        The strategy instance is the single source of truth for its
        name, parameters, and direction.  This eliminates the need for
        callers to build a separate Scroll manually.

        Returns:
            Scroll populated from the instance's current state.
        """
        ...


# ---------------------------------------------------------------------------
# Registry — The Order of Istari
# ---------------------------------------------------------------------------

ORDER_OF_ISTARI: dict[str, type[Istari]] = {}


def ordain_istari(name: str, cls: type[Istari]) -> None:
    """Register a strategy class under a name.

    Technical name: register_strategy — adds a strategy to the registry.

    Args:
        name: Lookup key for summon_istari.
        cls: Strategy subclass to register.
    """
    ORDER_OF_ISTARI[name] = cls


def summon_istari(config: Scroll) -> Istari:
    """Look up a strategy by name and instantiate it with config params.

    Technical name: build_strategy — instantiates a registered strategy.

    Args:
        config: Strategy configuration with a 'name' field and params.

    Returns:
        An instantiated Istari ready to call generate_signals().

    Raises:
        ValueError: If the strategy name is not registered.
    """
    cls = ORDER_OF_ISTARI.get(config.name)
    if cls is None:
        available = ', '.join(ORDER_OF_ISTARI.keys())
        msg = f"Unknown strategy '{config.name}'. Available: {available}"
        raise ValueError(msg)

    kwargs = {**config.params, 'direction': config.direction}
    return cls(**kwargs)  # type: ignore[misc]
