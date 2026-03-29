"""Strategy base class and registry.

Defines the Strategy abstract base class that all strategies must implement,
and provides a registry for looking up strategies by name.
"""

from abc import ABC, abstractmethod

import pandas as pd

from .config import StrategyConfig

# ---------------------------------------------------------------------------
# Strategy ABC
# ---------------------------------------------------------------------------


class Strategy(ABC):
    """Base class for all trading strategies.

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
    def config(self) -> StrategyConfig:
        """Return the strategy's own configuration.

        The strategy instance is the single source of truth for its
        name, parameters, and direction.  This eliminates the need for
        callers to build a separate StrategyConfig manually.

        Returns:
            StrategyConfig populated from the instance's current state.
        """
        ...


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

STRATEGY_REGISTRY: dict[str, type[Strategy]] = {}


def register_strategy(name: str, cls: type[Strategy]) -> None:
    """Register a strategy class under a name.

    Args:
        name: Lookup key for build_strategy.
        cls: Strategy subclass to register.
    """
    STRATEGY_REGISTRY[name] = cls


def build_strategy(config: StrategyConfig) -> Strategy:
    """Look up a strategy by name and instantiate it with config params.

    Args:
        config: Strategy configuration with a 'name' field and params.

    Returns:
        An instantiated Strategy ready to call generate_signals().

    Raises:
        ValueError: If the strategy name is not registered.
    """
    cls = STRATEGY_REGISTRY.get(config.name)
    if cls is None:
        available = ', '.join(STRATEGY_REGISTRY.keys())
        msg = f"Unknown strategy '{config.name}'. Available: {available}"
        raise ValueError(msg)

    return cls(**config.params)
