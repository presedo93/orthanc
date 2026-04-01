"""Classical and survival metrics for simulation results — the Eye of Sauron.

Pure functions that compute performance, survival, and robustness metrics
from a SimulationResult or a list of results. No side effects.

Technical name: funds/metrics.py — Performance and survival metrics.

Function mapping:
- read_the_scrolls() = compute_performance()
- count_the_fallen() = compute_survival()
- chronicle_the_age() = compute_aggregate()
- sing_the_tale() = format_summary()
"""

from dataclasses import dataclass

from .rings import DailyChronicle, FailureReason, QuestResult

# ---------------------------------------------------------------------------
# Single-run metrics
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BattleRecord:
    """Technical name: PerformanceMetrics — classical performance metrics for a single simulation."""

    net_pnl: float
    gross_pnl: float
    fees_total: float
    win_rate: float
    profit_factor: float
    sharpe_ratio: float
    sortino_ratio: float
    max_drawdown_pct: float
    trade_count: int
    green_day_ratio: float
    red_day_ratio: float
    flat_day_ratio: float
    avg_daily_pnl: float
    max_daily_loss: float


@dataclass(frozen=True)
class FateRecord:
    """Technical name: SurvivalMetrics — survival metrics for a single simulation."""

    survived: bool
    passed_evaluation: bool
    failure_reason: FailureReason | None
    failure_day: str | None
    days_survived: int
    green_days: int
    red_days: int
    flat_days: int
    trading_days: int


def read_the_scrolls(result: QuestResult) -> BattleRecord:
    """Compute classical performance metrics from a simulation result.

    Technical name: compute_performance — classical performance metrics.

    Args:
        result: A completed simulation result.

    Returns:
        BattleRecord with all classical measures.
    """
    total_days = result.green_days + result.red_days + result.flat_days
    green_ratio = result.green_days / total_days if total_days > 0 else 0.0
    red_ratio = result.red_days / total_days if total_days > 0 else 0.0
    flat_ratio = result.flat_days / total_days if total_days > 0 else 0.0

    daily_pnls = [row.realized_pnl for row in result.daily_ledger]
    avg_daily = sum(daily_pnls) / len(daily_pnls) if daily_pnls else 0.0

    return BattleRecord(
        net_pnl=result.net_pnl,
        gross_pnl=result.gross_pnl,
        fees_total=result.fees_total,
        win_rate=result.win_rate,
        profit_factor=result.profit_factor,
        sharpe_ratio=result.sharpe_ratio,
        sortino_ratio=result.sortino_ratio,
        max_drawdown_pct=result.max_drawdown_pct,
        trade_count=result.trade_count,
        green_day_ratio=green_ratio,
        red_day_ratio=red_ratio,
        flat_day_ratio=flat_ratio,
        avg_daily_pnl=avg_daily,
        max_daily_loss=result.max_daily_loss_seen,
    )


def count_the_fallen(result: QuestResult) -> FateRecord:
    """Compute survival metrics from a simulation result.

    Technical name: compute_survival — survival status and breakdown.

    Args:
        result: A completed simulation result.

    Returns:
        FateRecord with survival status and breakdown.
    """
    trading_days = sum(1 for row in result.daily_ledger if row.trades_count > 0)

    return FateRecord(
        survived=result.survived_to_end,
        passed_evaluation=result.passed_evaluation,
        failure_reason=result.failure_reason,
        failure_day=result.failure_day,
        days_survived=result.days_survived,
        green_days=result.green_days,
        red_days=result.red_days,
        flat_days=result.flat_days,
        trading_days=trading_days,
    )


# ---------------------------------------------------------------------------
# Aggregate metrics (for multiple runs)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AgeRecord:
    """Technical name: AggregateMetrics — aggregated metrics across multiple simulation runs."""

    n_runs: int
    survival_rate: float
    pass_rate: float
    mean_days_survived: float
    median_days_survived: float
    mean_net_pnl: float
    mean_max_drawdown: float
    pct_fail_daily_loss: float
    pct_fail_total_loss: float
    pct_fail_trailing_dd: float
    mean_green_days: float
    mean_trade_count: float


def chronicle_the_age(results: list[QuestResult]) -> AgeRecord:
    """Compute aggregate metrics over many simulation runs.

    Technical name: compute_aggregate — aggregate metrics over runs.

    Args:
        results: List of simulation results from multiple runs.

    Returns:
        AgeRecord summarizing the distribution of outcomes.
    """
    n = len(results)
    if n == 0:
        return AgeRecord(
            n_runs=0,
            survival_rate=0,
            pass_rate=0,
            mean_days_survived=0,
            median_days_survived=0,
            mean_net_pnl=0,
            mean_max_drawdown=0,
            pct_fail_daily_loss=0,
            pct_fail_total_loss=0,
            pct_fail_trailing_dd=0,
            mean_green_days=0,
            mean_trade_count=0,
        )

    survived = sum(1 for r in results if r.survived_to_end)
    passed = sum(1 for r in results if r.passed_evaluation)
    days = sorted(r.days_survived for r in results)
    median_idx = n // 2

    fail_daily = sum(1 for r in results if r.failure_reason == FailureReason.BALROG)
    fail_total = sum(1 for r in results if r.failure_reason == FailureReason.MOUNT_DOOM)
    fail_trailing = sum(1 for r in results if r.failure_reason == FailureReason.NAZGUL)

    return AgeRecord(
        n_runs=n,
        survival_rate=survived / n,
        pass_rate=passed / n,
        mean_days_survived=sum(days) / n,
        median_days_survived=float(days[median_idx]),
        mean_net_pnl=sum(r.net_pnl for r in results) / n,
        mean_max_drawdown=sum(r.max_drawdown_pct for r in results) / n,
        pct_fail_daily_loss=fail_daily / n,
        pct_fail_total_loss=fail_total / n,
        pct_fail_trailing_dd=fail_trailing / n,
        mean_green_days=sum(r.green_days for r in results) / n,
        mean_trade_count=sum(r.trade_count for r in results) / n,
    )


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------


def sing_the_tale(result: QuestResult) -> str:
    """Format a human-readable summary of a simulation result.

    Technical name: format_summary — formatted text report.

    Args:
        result: A completed simulation result.

    Returns:
        Multi-line string summary.
    """
    perf = read_the_scrolls(result)
    surv = count_the_fallen(result)

    lines = [
        f'Strategy: {result.strategy_name}',
        f'Firm: {result.firm_name}',
        f'Symbol: {result.symbol} ({result.timeframe})',
        '',
        '--- Survival ---',
        f'Survived: {surv.survived}',
        f'Passed evaluation: {surv.passed_evaluation}',
        f'Failure reason: {surv.failure_reason.value if surv.failure_reason else "None"}',
        f'Failure day: {surv.failure_day or "N/A"}',
        f'Days survived: {surv.days_survived}',
        f'Trading days: {surv.trading_days}',
        f'Green/Red/Flat: {surv.green_days}/{surv.red_days}/{surv.flat_days}',
        '',
        '--- Performance ---',
        f'Net PnL: {perf.net_pnl:,.2f}',
        f'Gross PnL: {perf.gross_pnl:,.2f}',
        f'Fees: {perf.fees_total:,.2f}',
        f'Win rate: {perf.win_rate:.1%}',
        f'Profit factor: {perf.profit_factor:.2f}',
        f'Sharpe: {perf.sharpe_ratio:.2f}',
        f'Sortino: {perf.sortino_ratio:.2f}',
        f'Max drawdown: {perf.max_drawdown_pct:.1%}',
        f'Max daily loss: {perf.max_daily_loss:,.2f}',
        f'Trades: {perf.trade_count}',
        f'Avg daily PnL: {perf.avg_daily_pnl:,.2f}',
    ]
    return '\n'.join(lines)
