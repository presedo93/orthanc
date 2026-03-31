"""Mordor — prop firm survival simulator.

Evaluate whether a trading strategy survives under prop trading firm
constraints. Takes execution results from istari and applies firm rules
(daily loss, total loss, trailing drawdown, profit targets) day-by-day
to determine if the account passes, fails, or survives.
"""

from .black_gate import (
    check_consistency_rule,
    enter_mount_doom,
    face_the_gate,
    flee_the_nazgul,
    seize_the_ring,
    summon_balrog,
    update_trailing_dd,
    walk_the_miles,
)
from .dark_lords import (
    mouth_of_sauron,
    sauron_basic,
    sauron_trial,
    witch_king,
)
from .dark_tongue import (
    DailyLossReference,
    FirmConfig,
    FirmRuleSet,
    RiskConfig,
    SimulationConfig,
    TotalLossReference,
    TradingDayDefinition,
    TrailingDrawdownMode,
)
from .eye import (
    AggregateMetrics,
    PerformanceMetrics,
    SurvivalMetrics,
    chronicle_the_age,
    count_the_fallen,
    read_the_scrolls,
    sing_the_tale,
)
from .rings import (
    AccountPhase,
    AccountState,
    DailyLedgerRow,
    FailureReason,
    SimulationResult,
    Trade,
)
from .shadow import cast_into_shadow

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
    'chronicle_the_age',
    'count_the_fallen',
    'read_the_scrolls',
    'sing_the_tale',
    # Models
    'AccountPhase',
    'AccountState',
    'DailyLedgerRow',
    'FailureReason',
    'SimulationResult',
    'Trade',
    # Presets
    'mouth_of_sauron',
    'sauron_basic',
    'sauron_trial',
    'witch_king',
    # Rules
    'check_consistency_rule',
    'face_the_gate',
    'flee_the_nazgul',
    'seize_the_ring',
    'summon_balrog',
    'enter_mount_doom',
    'update_trailing_dd',
    'walk_the_miles',
    # Simulator
    'cast_into_shadow',
]
