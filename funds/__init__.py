"""Prop firm survival simulator.

Evaluate whether a trading strategy survives under prop trading firm
constraints. Takes execution results from strats and applies firm rules
(daily loss, total loss, trailing drawdown, profit targets) day-by-day
to determine if the account passes, fails, or survives.
"""

from .config import (
    DailyLossReference,
    FirmConfig,
    FirmRuleSet,
    RiskConfig,
    SimulationConfig,
    TotalLossReference,
    TradingDayDefinition,
    TrailingDrawdownMode,
)
from .metrics import (
    AggregateMetrics,
    PerformanceMetrics,
    SurvivalMetrics,
    compute_aggregate,
    compute_performance,
    compute_survival,
    format_summary,
)
from .models import (
    AccountPhase,
    AccountState,
    DailyLedgerRow,
    FailureReason,
    SimulationResult,
    Trade,
)
from .presets import (
    ftmo_like_10k,
    generic_single_phase_eval,
    generic_two_phase_eval,
    topstep_like_50k,
)
from .rules import (
    check_all_rules,
    check_consistency_rule,
    check_daily_loss,
    check_min_trading_days,
    check_profit_target,
    check_total_loss,
    check_trailing_dd,
    update_trailing_dd,
)
from .simulator import simulate_account

__all__ = [
    # Config
    'DailyLossReference',
    'FirmConfig',
    'FirmRuleSet',
    'RiskConfig',
    'SimulationConfig',
    'TotalLossReference',
    'TrailingDrawdownMode',
    'TradingDayDefinition',
    # Metrics
    'AggregateMetrics',
    'PerformanceMetrics',
    'SurvivalMetrics',
    'compute_aggregate',
    'compute_performance',
    'compute_survival',
    'format_summary',
    # Models
    'AccountPhase',
    'AccountState',
    'DailyLedgerRow',
    'FailureReason',
    'SimulationResult',
    'Trade',
    # Presets
    'ftmo_like_10k',
    'generic_single_phase_eval',
    'generic_two_phase_eval',
    'topstep_like_50k',
    # Rules
    'check_all_rules',
    'check_consistency_rule',
    'check_daily_loss',
    'check_min_trading_days',
    'check_profit_target',
    'check_total_loss',
    'check_trailing_dd',
    'update_trailing_dd',
    # Simulator
    'simulate_account',
]
