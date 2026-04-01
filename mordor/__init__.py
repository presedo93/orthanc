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
    DarkLaw,
    DarkRealmConfig,
    PerilConfig,
    QuestConfig,
    TotalLossReference,
    TradingDayDefinition,
    TrailingDrawdownMode,
)
from .eye import (
    AgeRecord,
    BattleRecord,
    FateRecord,
    chronicle_the_age,
    count_the_fallen,
    read_the_scrolls,
    sing_the_tale,
)
from .rings import (
    AccountPhase,
    DailyChronicle,
    FailureReason,
    QuestResult,
    RealmState,
    Skirmish,
)
from .shadow import cast_into_shadow

__all__ = [
    # Config
    'DailyLossReference',
    'DarkLaw',
    'DarkRealmConfig',
    'PerilConfig',
    'QuestConfig',
    'TotalLossReference',
    'TrailingDrawdownMode',
    'TradingDayDefinition',
    # Metrics
    'AgeRecord',
    'BattleRecord',
    'FateRecord',
    'chronicle_the_age',
    'count_the_fallen',
    'read_the_scrolls',
    'sing_the_tale',
    # Models
    'AccountPhase',
    'DailyChronicle',
    'FailureReason',
    'QuestResult',
    'RealmState',
    'Skirmish',
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
