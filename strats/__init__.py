"""Strategy backtesting engine.

Define trading strategies, run VectorBT backtests, and analyze results
independently of any prop firm rules. Strategies produce entry/exit
signals; the execution engine turns them into equity curves and trade logs.
"""

from .config import (
    Direction,
    ExecutionConfig,
    RegimeConfig,
    RegimeSwitcherConfig,
    SizingMode,
    StrategyConfig,
    WeightAdjustmentConfig,
)
from .execution import ExecutionResult, run_backtest
from .playbook import (
    Defensive,
    MeanReversion,
    RegimeSwitcher,
    SmaCross,
    TrendFollow,
    VolatilityBreakout,
)
from .registry import Strategy, build_strategy

__all__ = [
    # Config
    'Direction',
    'ExecutionConfig',
    'RegimeConfig',
    'RegimeSwitcherConfig',
    'SizingMode',
    'StrategyConfig',
    'WeightAdjustmentConfig',
    # Execution
    'ExecutionResult',
    'run_backtest',
    # Registry
    'Strategy',
    'build_strategy',
    # Playbook
    'Defensive',
    'MeanReversion',
    'RegimeSwitcher',
    'SmaCross',
    'TrendFollow',
    'VolatilityBreakout',
]
