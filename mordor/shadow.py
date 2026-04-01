"""Account simulator — walks through VectorBT results day-by-day.

Applies prop firm rules to the equity curve and trade log produced by
the execution engine. Records daily ledger rows, tracks account state,
and determines whether the account survives or fails (and why).

Technical name: funds/simulator.py — Account simulation engine.

Function mapping:
- cast_into_shadow() = simulate_account()
"""

import pandas as pd

from istari import ForgeResult

from .black_gate import (
    face_the_gate,
    seize_the_ring,
    update_trailing_dd,
    walk_the_miles,
)
from .dark_tongue import DarkLaw, DarkRealmConfig
from .rings import (
    DailyChronicle,
    FailureReason,
    QuestResult,
    RealmState,
    Skirmish,
)


def cast_into_shadow(
    result: ForgeResult,
    firm_config: DarkRealmConfig,
    strategy_name: str,
    symbol: str,
    timeframe: str,
) -> QuestResult:
    """Run a prop firm account simulation over execution results.

    Technical name: simulate_account — day-by-day prop firm simulation.

    Walks through the equity curve day by day, applying firm rules at
    end-of-day checkpoints. Records the daily ledger and determines
    whether the account passes, fails, or survives.

    Args:
        result: Output from the VectorBT execution engine.
        firm_config: Firm configuration with account size and rules.
        strategy_name: Name of the strategy used.
        symbol: Instrument symbol.
        timeframe: Data timeframe.

    Returns:
        QuestResult with full simulation output.
    """
    rules = firm_config.rule_set
    equity = result.equity_curve
    trades_df = result.trades_df

    # Build per-day trade PnL from the trades DataFrame
    daily_trade_pnl = _aggregate_daily_trade_pnl(trades_df)
    daily_trade_counts = _aggregate_daily_trade_counts(trades_df)

    # Initialize account state
    init_balance = firm_config.account_size
    state = RealmState(
        starting_balance=init_balance,
        current_balance=init_balance,
        equity=init_balance,
        peak_balance=init_balance,
        peak_equity=init_balance,
        daily_start_balance=init_balance,
        daily_start_equity=init_balance,
        trailing_dd_floor=_initial_trailing_floor(init_balance, rules),
    )

    daily_ledger: list[DailyChronicle] = []
    trade_log: list[Skirmish] = []
    equity_values: list[float] = []
    daily_pnls: list[float] = []

    # Get unique trading dates from equity curve
    if equity.empty:
        return _build_result(
            state,
            result,
            daily_ledger,
            trade_log,
            equity_values,
            strategy_name,
            symbol,
            timeframe,
            firm_config,
        )

    dates = sorted(set(equity.index.date))  # type: ignore[attr-defined]

    for date in dates:
        date_str = str(date)

        # Get equity values for this day
        day_equity = equity.loc[equity.index.date == date]  # type: ignore[comparison-overlap]
        if day_equity.empty:
            continue

        day_end_equity = float(day_equity.iloc[-1])
        day_min_equity = float(day_equity.min())

        # Get trade PnL for this day
        day_realized = daily_trade_pnl.get(date_str, 0.0)
        day_unrealized = day_end_equity - state.daily_start_balance - day_realized
        day_trades = daily_trade_counts.get(date_str, (0, 0, 0))

        # Update state for this day
        state.equity = day_end_equity
        state.daily_realized_pnl = day_realized
        state.daily_unrealized_pnl = day_unrealized
        state.current_balance = state.daily_start_balance + day_realized

        # Track peaks
        state.peak_balance = max(state.peak_balance, state.current_balance)
        state.peak_equity = max(state.peak_equity, day_end_equity)

        # Update trailing drawdown
        state.trailing_dd_floor = update_trailing_dd(state, rules)

        # Compute intraday loss from equity curve
        max_intraday_loss = state.daily_start_balance - day_min_equity

        # Check rules
        breach = face_the_gate(state, rules)
        if breach is not None:
            state.alive = False
            state.failure_reason = breach
            state.failure_day = date_str

            daily_ledger.append(
                _make_ledger_row(
                    date_str,
                    state,
                    day_realized,
                    day_unrealized,
                    max_intraday_loss,
                    day_trades,
                )
            )
            equity_values.extend(day_equity.tolist())
            break

        # Check profit target
        target_hit = seize_the_ring(state, rules)
        min_days_met = walk_the_miles(state, rules)

        if target_hit and min_days_met:
            state.evaluation_passed = True

        # Record daily ledger
        is_green = day_realized > 0
        is_red = day_realized < 0
        is_flat = day_realized == 0

        if day_trades[0] > 0:
            state.trading_days_count += 1

        if is_green:
            state.green_days += 1
        elif is_red:
            state.red_days += 1
        else:
            state.flat_days += 1

        state.realized_pnl_total += day_realized
        daily_pnls.append(day_realized)

        daily_ledger.append(
            _make_ledger_row(
                date_str,
                state,
                day_realized,
                day_unrealized,
                max_intraday_loss,
                day_trades,
            )
        )
        equity_values.extend(day_equity.tolist())

        # Prepare for next day
        state.daily_start_balance = state.current_balance
        state.daily_start_equity = day_end_equity

    # Build trade log from VectorBT trades
    trade_log = _build_trade_log(trades_df, symbol)

    return _build_result(
        state,
        result,
        daily_ledger,
        trade_log,
        equity_values,
        strategy_name,
        symbol,
        timeframe,
        firm_config,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _initial_trailing_floor(balance: float, rules: DarkLaw) -> float:
    """Compute the initial trailing drawdown floor."""
    if rules.trailing_drawdown_pct is not None:
        return balance * (1 - rules.trailing_drawdown_pct)
    return 0.0


def _aggregate_daily_trade_pnl(trades_df: pd.DataFrame) -> dict[str, float]:
    """Sum net PnL per calendar day from the trades DataFrame."""
    if trades_df.empty or 'Exit Timestamp' not in trades_df.columns:
        return {}

    result: dict[str, float] = {}
    for _, row in trades_df.iterrows():
        exit_ts = pd.Timestamp(row['Exit Timestamp'])  # type: ignore[arg-type]
        day = str(exit_ts.date())
        pnl = float(row.get('PnL', 0.0))  # type: ignore[arg-type]
        result[day] = result.get(day, 0.0) + pnl
    return result


def _aggregate_daily_trade_counts(
    trades_df: pd.DataFrame,
) -> dict[str, tuple[int, int, int]]:
    """Count trades, wins, losses per calendar day."""
    if trades_df.empty or 'Exit Timestamp' not in trades_df.columns:
        return {}

    result: dict[str, tuple[int, int, int]] = {}
    for _, row in trades_df.iterrows():
        exit_ts = pd.Timestamp(row['Exit Timestamp'])  # type: ignore[arg-type]
        day = str(exit_ts.date())
        pnl = float(row.get('PnL', 0.0))  # type: ignore[arg-type]
        total, wins, losses = result.get(day, (0, 0, 0))
        total += 1
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        result[day] = (total, wins, losses)
    return result


def _make_ledger_row(
    date_str: str,
    state: RealmState,
    realized: float,
    unrealized: float,
    max_intraday_loss: float,
    trade_counts: tuple[int, int, int],
) -> DailyChronicle:
    """Create a DailyChronicle from current state."""
    total, wins, losses = trade_counts
    return DailyChronicle(
        date=date_str,
        start_balance=state.daily_start_balance,
        end_balance=state.current_balance,
        realized_pnl=realized,
        unrealized_pnl_close=unrealized,
        max_intraday_loss=max_intraday_loss,
        trades_count=total,
        wins_count=wins,
        losses_count=losses,
        is_green=realized > 0,
        is_red=realized < 0,
        is_flat=realized == 0,
    )


def _build_trade_log(trades_df: pd.DataFrame, symbol: str) -> list[Skirmish]:
    """Convert VectorBT trades DataFrame to domain Skirmish objects."""
    if trades_df.empty:
        return []

    trade_log: list[Skirmish] = []
    for idx, row in trades_df.iterrows():
        exit_ts_raw = row.get('Exit Timestamp', '')
        exit_ts = pd.Timestamp(str(exit_ts_raw))
        day_id = str(exit_ts.date()) if pd.notna(exit_ts) else ''

        trade_log.append(
            Skirmish(
                trade_id=int(idx),  # type: ignore[arg-type]
                symbol=symbol,
                entry_time=str(row.get('Entry Timestamp', '')),
                exit_time=str(row.get('Exit Timestamp', '')),
                entry_price=float(row.get('Avg Entry Price', 0.0) or 0.0),  # type: ignore[arg-type]
                exit_price=float(row.get('Avg Exit Price', 0.0) or 0.0),  # type: ignore[arg-type]
                size=float(row.get('Size', 0.0) or 0.0),  # type: ignore[arg-type]
                direction=str(row.get('Direction', 'Long')),
                gross_pnl=float(row.get('PnL', 0.0) or 0.0),  # type: ignore[arg-type]
                fees=float(row.get('Entry Fees', 0.0) or 0.0)
                + float(row.get('Exit Fees', 0.0) or 0.0),  # type: ignore[arg-type]
                net_pnl=float(row.get('PnL', 0.0) or 0.0),  # type: ignore[arg-type]
                exit_reason=str(row.get('Status', 'Closed')),
                day_id=day_id,
            )
        )
    return trade_log


def _build_result(
    state: RealmState,
    exec_result: ForgeResult,
    daily_ledger: list[DailyChronicle],
    trade_log: list[Skirmish],
    equity_values: list[float],
    strategy_name: str,
    symbol: str,
    timeframe: str,
    firm_config: DarkRealmConfig,
) -> QuestResult:
    """Assemble the final QuestResult."""
    max_daily_loss = max((row.max_intraday_loss for row in daily_ledger), default=0.0)

    return QuestResult(
        strategy_name=strategy_name,
        firm_name=firm_config.firm_name,
        symbol=symbol,
        timeframe=timeframe,
        survived_to_end=state.alive,
        passed_evaluation=state.evaluation_passed,
        failure_reason=state.failure_reason,
        failure_day=state.failure_day,
        days_survived=len(daily_ledger),
        green_days=state.green_days,
        red_days=state.red_days,
        flat_days=state.flat_days,
        net_pnl=exec_result.total_profit,
        gross_pnl=exec_result.total_profit + exec_result.total_fees,
        fees_total=exec_result.total_fees,
        max_drawdown_pct=exec_result.max_drawdown,
        max_daily_loss_seen=max_daily_loss,
        profit_factor=exec_result.profit_factor,
        win_rate=exec_result.win_rate,
        trade_count=exec_result.trade_count,
        sharpe_ratio=exec_result.sharpe_ratio,
        sortino_ratio=exec_result.sortino_ratio,
        daily_ledger=daily_ledger,
        trade_log=trade_log,
        equity_curve=equity_values,
        config_snapshot=firm_config.model_dump(),
    )
