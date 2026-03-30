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
    """Trading direction."""

    LONG_ONLY = 'longonly'
    SHORT_ONLY = 'shortonly'
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
    direction: Direction = Direction.LONG_ONLY


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
    """Configuration for market regime detection.

    Controls the feature computation periods and thresholds used
    by the heuristic regime detector. All thresholds produce
    continuous [0,1] probabilities — no hard switches.
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

    # Trend thresholds
    adx_strong_trend: float = 30.0
    adx_weak_trend: float = 15.0
    ema_slope_threshold: float = 0.005

    # Volatility thresholds
    atr_ratio_high: float = 1.5
    atr_ratio_low: float = 0.7
    bollinger_width_high: float = 0.06
    bollinger_width_low: float = 0.02

    # Activity thresholds
    volume_ratio_low: float = 0.5
    range_compression_low: float = 0.5

    # Smoothing
    regime_ema_span: int = 10
    transition_decay: float = 0.6


class RegimeSwitcherConfig(BaseModel):
    """Configuration for the regime-based multi-strategy switcher.

    Controls how regime probabilities map to strategy weights and
    how signals from multiple strategies are combined.
    """

    regime: RegimeConfig = Field(default_factory=RegimeConfig)

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
