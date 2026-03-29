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
