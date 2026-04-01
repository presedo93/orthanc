"""VectorBT execution wrapper.

Converts entry/exit signals into a VectorBT Portfolio with configured
fees, slippage, and sizing. Extracts trade logs, equity curves, and
raw results for downstream processing by the simulator and metrics layers.

Technical name: execution.py — backtest execution via VectorBT.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd
import vectorbt as vbt

from .scrolls import Direction, ForgeConfig


@dataclass(frozen=True)
class ForgeResult:
    """Technical name: ExecutionResult — raw output from the VectorBT execution engine.

    Contains everything needed to evaluate performance, run prop firm
    simulations, or compute standalone strategy metrics.
    """

    equity_curve: pd.Series  # type: ignore[type-arg]
    cash_curve: pd.Series  # type: ignore[type-arg]
    returns: pd.Series  # type: ignore[type-arg]
    daily_returns: pd.Series  # type: ignore[type-arg]
    trades_df: pd.DataFrame
    total_fees: float
    total_return: float
    total_profit: float
    final_value: float
    max_drawdown: float
    sharpe_ratio: float
    sortino_ratio: float
    win_rate: float
    profit_factor: float
    trade_count: int
    portfolio: object  # vbt.Portfolio — kept for further analysis


def forge_battle(
    close: pd.Series,  # type: ignore[type-arg]
    entries: pd.Series,  # type: ignore[type-arg]
    exits: pd.Series,  # type: ignore[type-arg]
    execution_config: ForgeConfig,
    direction: Direction = Direction.WESTWARD,
) -> ForgeResult:
    """Run a backtest using VectorBT and return structured results.

    Technical name: run_backtest — executes VectorBT portfolio simulation.

    Args:
        close: Price series (typically df['close']).
        entries: Boolean series — True where entries fire.
        exits: Boolean series — True where exits fire.
        execution_config: Fees, slippage, initial capital.
        direction: Trading direction (westward, eastward, or all roads).

    Returns:
        ForgeResult with equity curves, trade data, and summary metrics.
    """
    pf = vbt.Portfolio.from_signals(
        close,
        entries,
        exits,
        init_cash=execution_config.init_cash,
        fees=execution_config.fees,
        fixed_fees=execution_config.fixed_fees,
        slippage=execution_config.slippage,
        direction=direction.value,
        freq='h',  # sensible default; VectorBT infers from index if possible
    )

    trades_df = _extract_trades(pf)

    # Safe metric extraction — VectorBT returns NaN for empty portfolios
    # type: ignore comments needed because VectorBT stubs are incomplete
    sharpe = pf.sharpe_ratio()  # type: ignore[attr-defined]
    sortino = pf.sortino_ratio()  # type: ignore[attr-defined]
    trade_count_val = pf.trades.count()  # type: ignore[attr-defined]
    win_rate_val = pf.trades.win_rate() if trade_count_val > 0 else 0.0  # type: ignore[attr-defined]
    pf_val = pf.trades.profit_factor() if trade_count_val > 0 else 0.0  # type: ignore[attr-defined]

    return ForgeResult(
        equity_curve=pf.value(),
        cash_curve=pf.cash(),
        returns=pf.returns(),
        daily_returns=_compute_daily_returns(pf.returns()),
        trades_df=trades_df,
        total_fees=float(pf.orders.fees.sum()),  # type: ignore[attr-defined]
        total_return=float(pf.total_return()),
        total_profit=float(pf.total_profit()),
        final_value=float(pf.final_value()),
        max_drawdown=float(pf.max_drawdown()),  # type: ignore[attr-defined]
        sharpe_ratio=float(sharpe) if not np.isnan(sharpe) else 0.0,
        sortino_ratio=float(sortino) if not np.isnan(sortino) else 0.0,
        win_rate=float(win_rate_val) if not np.isnan(win_rate_val) else 0.0,
        profit_factor=float(pf_val) if not np.isnan(pf_val) else 0.0,
        trade_count=int(trade_count_val),  # type: ignore[arg-type]
        portfolio=pf,
    )


def _compute_daily_returns(returns: pd.Series) -> pd.Series:  # type: ignore[type-arg]
    """Compute daily returns from bar-level returns.

    VectorBT's ``pf.daily_returns()`` calls ``resample(..., axis=0)``
    which was removed in pandas >=2.0.  We replicate the logic manually:
    compound bar returns within each calendar day.

    Args:
        returns: Bar-level percentage returns from ``pf.returns()``.

    Returns:
        Series of daily compounded returns indexed by date.
    """
    if returns.empty:
        return returns

    daily = (1 + returns).groupby(returns.index.date).prod() - 1  # type: ignore[attr-defined]
    daily.index = pd.to_datetime(daily.index)
    return daily


def _extract_trades(pf: object) -> pd.DataFrame:
    """Extract a clean trade log from a VectorBT portfolio.

    Args:
        pf: VectorBT Portfolio object.

    Returns:
        DataFrame with one row per closed trade, including timestamps,
        prices, PnL, fees, direction, and status.
    """
    portfolio = pf  # type: ignore[assignment]
    trades = portfolio.trades  # type: ignore[union-attr]
    if trades.count() == 0:  # type: ignore[union-attr]
        return pd.DataFrame()

    records = trades.records_readable  # type: ignore[union-attr]
    return records.copy()
