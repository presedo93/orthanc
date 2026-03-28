"""Strategy playbook — all available strategy implementations.

Importing this package registers every strategy in the registry.
"""

from .sma_cross import SmaCross

__all__ = [
    'SmaCross',
]
