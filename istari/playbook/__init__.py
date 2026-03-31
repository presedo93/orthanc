"""The Istari playbook — all available strategy implementations.

Importing this package registers every strategy in the Order of Istari.

Technical name: playbook/__init__.py — strategy auto-registration.
"""

from .aragorn import Aragorn
from .gandalf import Gandalf
from .saruman import Saruman
from .shadowfax import Shadowfax
from .shelob import Shelob
from .treebeard import Treebeard

__all__ = [
    'Aragorn',
    'Gandalf',
    'Saruman',
    'Shadowfax',
    'Shelob',
    'Treebeard',
]
