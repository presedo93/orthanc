"""Domain entities for the prop firm survival simulator.

These dataclasses represent the core domain objects produced during simulation:
trades, daily ledger rows, account state, and simulation results.

Technical name: funds/models.py — Account lifecycle models.
"""

from dataclasses import dataclass, field
from enum import Enum

# ---------------------------------------------------------------------------
# Account lifecycle states
# ---------------------------------------------------------------------------


class AccountPhase(str, Enum):
    """Phase of the account lifecycle.

    Technical mapping:
    - THE_QUEST = 'evaluation'
    - CORONATION = 'funded'
    - RETURN_OF_THE_KING = 'payout_eligible'
    """

    THE_QUEST = 'evaluation'
    CORONATION = 'funded'
    RETURN_OF_THE_KING = 'payout_eligible'


class FailureReason(str, Enum):
    """Reason an account was terminated.

    Technical mapping:
    - BALROG = 'daily_loss_breach'
    - MOUNT_DOOM = 'total_loss_breach'
    - NAZGUL = 'trailing_dd_breach'
    - CURSE_OF_FEANOR = 'consistency_rule_breach'
    - MORGOTH = 'generic_rule_breach'
    """

    BALROG = 'daily_loss_breach'
    MOUNT_DOOM = 'total_loss_breach'
    NAZGUL = 'trailing_dd_breach'
    CURSE_OF_FEANOR = 'consistency_rule_breach'
    MORGOTH = 'generic_rule_breach'


# ---------------------------------------------------------------------------
# Trade
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Skirmish:
    """Technical name: Trade — a closed trade produced by the execution engine."""

    trade_id: int
    symbol: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    size: float
    direction: str
    gross_pnl: float
    fees: float
    net_pnl: float
    exit_reason: str
    day_id: str


# ---------------------------------------------------------------------------
# Daily ledger row
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DailyChronicle:
    """Technical name: DailyLedgerRow — summary of a single trading day."""

    date: str
    start_balance: float
    end_balance: float
    realized_pnl: float
    unrealized_pnl_close: float
    max_intraday_loss: float
    trades_count: int
    wins_count: int
    losses_count: int
    is_green: bool
    is_red: bool
    is_flat: bool


# ---------------------------------------------------------------------------
# Account state
# ---------------------------------------------------------------------------


@dataclass
class RealmState:
    """Technical name: AccountState — mutable account state tracked during simulation.

    Updated bar-by-bar or day-by-day as the simulator processes results.
    """

    starting_balance: float
    current_balance: float
    equity: float
    peak_balance: float
    peak_equity: float
    daily_start_balance: float
    daily_start_equity: float
    daily_realized_pnl: float = 0.0
    daily_unrealized_pnl: float = 0.0
    realized_pnl_total: float = 0.0
    trading_days_count: int = 0
    green_days: int = 0
    red_days: int = 0
    flat_days: int = 0
    consecutive_losses: int = 0
    alive: bool = True
    failure_reason: FailureReason | None = None
    failure_day: str | None = None
    phase: AccountPhase = AccountPhase.THE_QUEST
    evaluation_passed: bool = False
    trailing_dd_floor: float = 0.0


# ---------------------------------------------------------------------------
# Simulation result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QuestResult:
    """Technical name: SimulationResult — full result of a single simulation run."""

    strategy_name: str
    firm_name: str
    symbol: str
    timeframe: str
    survived_to_end: bool
    passed_evaluation: bool
    failure_reason: FailureReason | None
    failure_day: str | None
    days_survived: int
    green_days: int
    red_days: int
    flat_days: int
    net_pnl: float
    gross_pnl: float
    fees_total: float
    max_drawdown_pct: float
    max_daily_loss_seen: float
    profit_factor: float
    win_rate: float
    trade_count: int
    sharpe_ratio: float
    sortino_ratio: float
    daily_ledger: list[DailyChronicle] = field(default_factory=list)
    trade_log: list[Skirmish] = field(default_factory=list)
    equity_curve: list[float] = field(default_factory=list)
    config_snapshot: dict[str, object] = field(default_factory=dict)
