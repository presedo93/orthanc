"""The Istari — strategy backtesting engine.

Technical name: strats — strategy definition, execution, and registry.

Define trading strategies, run VectorBT backtests, and analyze results
independently of any prop firm rules. Strategies produce entry/exit
signals; the execution engine turns them into equity curves and trade logs.
"""

from .council_of_wizards import Istari, summon_istari
from .forge import ForgeResult, forge_battle
from .playbook import (
    Aragorn,
    Gandalf,
    Saruman,
    Shadowfax,
    Shelob,
    Treebeard,
)
from .scrolls import (
    Direction,
    ForgeConfig,
    RealmConfig,
    SarumanConfig,
    Scroll,
    SizingMode,
    WeightSpell,
)

__all__ = [
    # Config (scrolls)
    'Direction',
    'ForgeConfig',
    'RealmConfig',
    'SarumanConfig',
    'Scroll',
    'SizingMode',
    'WeightSpell',
    # Execution (forge)
    'ForgeResult',
    'forge_battle',
    # Registry (council of wizards)
    'Istari',
    'summon_istari',
    # Playbook (the wizards)
    'Aragorn',
    'Gandalf',
    'Saruman',
    'Shadowfax',
    'Shelob',
    'Treebeard',
]
