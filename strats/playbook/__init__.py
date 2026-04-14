"""Strategy playbook — all available strategy implementations.

Importing this package registers every strategy in the registry.
"""

from .defensive import Defensive
from .mean_reversion import MeanReversion
from .regime_switcher import RegimeSwitcher
from .sma_cross import SmaCross
from .trend_follow import TrendFollow
from .volatility_breakout import VolatilityBreakout

__all__ = [
    'Defensive',
    'MeanReversion',
    'RegimeSwitcher',
    'SmaCross',
    'TrendFollow',
    'VolatilityBreakout',
]
