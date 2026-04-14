"""Typed configuration models for strategy and execution.

All configs use Pydantic for validation, serialization, and reproducibility.
Each config covers a single concern: strategy parameters or execution settings.
"""

from enum import Enum

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class Direction(str, Enum):
    """Trading direction — controls long/short/both signal generation."""

    LONG = 'longonly'
    SHORT = 'shortonly'
    BOTH = 'both'


class SizingMode(str, Enum):
    """Position sizing method."""

    PERCENT_OF_BALANCE = 'percent_of_balance'
    PERCENT_OF_EQUITY = 'percent_of_equity'
    FIXED_SIZE = 'fixed_size'
    FIXED_CONTRACTS = 'fixed_contracts'


# ---------------------------------------------------------------------------
# Config models
# ---------------------------------------------------------------------------


class StrategyConfig(BaseModel):
    """Configuration for a trading strategy."""

    name: str
    params: dict[str, float | int | str | bool] = Field(default_factory=dict)
    direction: Direction = Direction.LONG


class ExecutionConfig(BaseModel):
    """Configuration for trade execution."""

    init_cash: float = 50_000.0
    fees: float = 0.001
    fixed_fees: float = 0.0
    slippage: float = 0.0005


# ---------------------------------------------------------------------------
# Regime detection config
# ---------------------------------------------------------------------------


class RegimeConfig(BaseModel):
    """Configuration for HMM-based market regime detection.

    Controls the feature computation periods used to build the
    observation vectors, and the HMM training parameters. The
    model produces continuous [0,1] probabilities per regime
    via forward-backward posterior inference — no hard switches.
    """

    # Feature computation periods
    ema_period: int = 20
    ema_slope_window: int = 5
    adx_period: int = 14
    atr_fast_period: int = 5
    atr_slow_period: int = 20
    bollinger_period: int = 20
    volume_fast_period: int = 5
    volume_slow_period: int = 20
    range_period: int = 10
    structure_period: int = 10

    # HMM parameters
    n_regimes: int = 3
    covariance_type: str = 'full'
    hmm_n_iter: int = 100
    hmm_tol: float = 1e-4
    hmm_n_fits: int = 5
    transition_stickiness: float = 0.95

    # Transition handling
    transition_decay: float = 0.6


class WeightAdjustmentConfig(BaseModel):
    """Controls performance-based and volatility-based weight modifiers.

    Both adjustments are multiplicative factors applied to strategy
    weights after regime-based computation. All factors are clamped
    to [floor, ceiling] to prevent extreme swings.

    Performance adjustment: measures rolling hit rate of each strategy's
    entry signals (was the move directionally correct N bars later?)
    and scales weights up/down accordingly.

    Volatility scaling: compares current realized volatility to a target
    level. High vol → scale down exposure; low vol → scale up.
    """

    # Performance tracking
    perf_lookback: int = 50
    perf_min_signals: int = 5
    perf_horizon: int = 5

    # Volatility scaling
    vol_target: float = 0.02
    vol_period: int = 20

    # Shared clamps
    floor: float = 0.3
    ceiling: float = 1.5


class RegimeSwitcherConfig(BaseModel):
    """Configuration for the regime-based multi-strategy switcher.

    Controls how regime probabilities map to strategy weights and
    how signals from multiple strategies are combined.
    """

    regime: RegimeConfig = Field(default_factory=RegimeConfig)
    weight_adj: WeightAdjustmentConfig = Field(default_factory=WeightAdjustmentConfig)

    # Signal combination threshold — minimum weighted vote to trigger entry
    entry_threshold: float = 0.4
    exit_threshold: float = 0.3

    # Per-strategy parameters
    trend_fast_window: int = 10
    trend_slow_window: int = 30
    trend_atr_multiplier: float = 2.0
    trend_adx_period: int = 14

    mean_rev_period: int = 20
    mean_rev_num_std: float = 2.0

    vol_breakout_compression_period: int = 20
    vol_breakout_atr_multiplier: float = 1.5
