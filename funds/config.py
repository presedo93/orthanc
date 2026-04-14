"""Typed configuration models for the prop firm survival simulator.

All configs use Pydantic for validation, serialization, and reproducibility.
Each config covers a single concern: risk, firm rules, or simulation settings.
"""

from enum import Enum

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class DailyLossReference(str, Enum):
    """How to calculate the daily loss baseline."""

    BALANCE_START_OF_DAY = 'balance_start_of_day'
    EQUITY_START_OF_DAY = 'equity_start_of_day'


class TotalLossReference(str, Enum):
    """How to calculate the total loss baseline."""

    INITIAL_BALANCE = 'initial_balance'
    PEAK_BALANCE = 'peak_balance'
    PEAK_EQUITY = 'peak_equity'


class TrailingDrawdownMode(str, Enum):
    """Trailing drawdown behavior."""

    NONE = 'none'
    BALANCE_TRAILING = 'balance_trailing'
    EQUITY_TRAILING = 'equity_trailing'
    BALANCE_TRAILING_UNTIL_BREAKEVEN = 'balance_trailing_until_breakeven'
    EQUITY_TRAILING_UNTIL_BREAKEVEN = 'equity_trailing_until_breakeven'


class TradingDayDefinition(str, Enum):
    """What counts as a trading day."""

    ANY_CLOSED_TRADE = 'any_closed_trade'
    ANY_OPENED_TRADE = 'any_opened_trade'
    NONZERO_PNL = 'nonzero_pnl'


# ---------------------------------------------------------------------------
# Config models
# ---------------------------------------------------------------------------


class RiskConfig(BaseModel):
    """Configuration for internal risk management."""

    risk_per_trade: float = 0.01
    max_trades_per_day: int = 10
    daily_stop_internal: float | None = None
    daily_take_lock: float | None = None
    consecutive_losses_cutoff: int | None = None


class FirmRuleSet(BaseModel):
    """Rules for a specific prop firm phase.

    Each field maps to a hard constraint the firm applies.
    None means the rule is not active.
    """

    max_daily_loss_pct: float | None = 0.04
    max_daily_loss_abs: float | None = None
    daily_loss_reference: DailyLossReference = DailyLossReference.BALANCE_START_OF_DAY
    daily_loss_includes_unrealized: bool = True

    max_total_loss_pct: float | None = 0.10
    max_total_loss_abs: float | None = None
    total_loss_reference: TotalLossReference = TotalLossReference.INITIAL_BALANCE

    trailing_drawdown_mode: TrailingDrawdownMode = TrailingDrawdownMode.NONE
    trailing_drawdown_pct: float | None = None

    profit_target_pct: float | None = 0.08
    profit_target_abs: float | None = None
    profit_target_includes_unrealized: bool = False

    min_trading_days: int | None = 5
    trading_day_definition: TradingDayDefinition = TradingDayDefinition.ANY_CLOSED_TRADE

    consistency_rule_enabled: bool = False
    consistency_threshold_pct: float = 0.30


class FirmConfig(BaseModel):
    """Configuration for a prop firm account."""

    firm_name: str = 'generic_prop'
    plan_name: str = '50k_single_phase'
    account_size: float = 50_000.0
    evaluation_fee: float = 300.0
    reset_fee: float = 100.0
    activation_fee: float = 0.0
    profit_split_pct: float = 0.80
    rule_set: FirmRuleSet = Field(default_factory=FirmRuleSet)


class SimulationConfig(BaseModel):
    """Configuration for a simulation run."""

    seed: int = 42
    mode: str = 'historical'
    n_runs: int = 1
    horizon_days: int | None = None
