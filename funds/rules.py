"""Prop firm rule evaluator.

Pure functions that check whether an account has breached firm constraints.
Each function takes account state and rule configuration, and returns
whether the rule was breached. No side effects, no mutation.
"""

from .config import (
    DailyLossReference,
    FirmRuleSet,
    TotalLossReference,
    TrailingDrawdownMode,
)
from .models import AccountState, FailureReason

# ---------------------------------------------------------------------------
# Daily loss check
# ---------------------------------------------------------------------------


def check_daily_loss(state: AccountState, rules: FirmRuleSet) -> FailureReason | None:
    """Check if the daily loss limit has been breached.

    Evaluates both percentage-based and absolute limits. Returns the failure
    reason if breached, None otherwise.

    Args:
        state: Current account state with daily PnL data.
        rules: Firm rule set with daily loss limits.

    Returns:
        FailureReason.DAILY_LOSS_BREACH if breached, None if within limits.
    """
    if rules.max_daily_loss_pct is None and rules.max_daily_loss_abs is None:
        return None

    # Determine reference price for daily loss
    if rules.daily_loss_reference == DailyLossReference.BALANCE_START_OF_DAY:
        reference = state.daily_start_balance
    else:
        reference = state.daily_start_equity

    # Calculate daily loss (negative = loss)
    if rules.daily_loss_includes_unrealized:
        daily_pnl = state.daily_realized_pnl + state.daily_unrealized_pnl
    else:
        daily_pnl = state.daily_realized_pnl

    # Check percentage limit
    if rules.max_daily_loss_pct is not None and reference > 0:
        max_loss = reference * rules.max_daily_loss_pct
        if daily_pnl <= -max_loss:
            return FailureReason.DAILY_LOSS_BREACH

    # Check absolute limit
    if rules.max_daily_loss_abs is not None:
        if daily_pnl <= -rules.max_daily_loss_abs:
            return FailureReason.DAILY_LOSS_BREACH

    return None


# ---------------------------------------------------------------------------
# Total loss check
# ---------------------------------------------------------------------------


def check_total_loss(state: AccountState, rules: FirmRuleSet) -> FailureReason | None:
    """Check if the total loss limit has been breached.

    Args:
        state: Current account state.
        rules: Firm rule set with total loss limits.

    Returns:
        FailureReason.TOTAL_LOSS_BREACH if breached, None if within limits.
    """
    if rules.max_total_loss_pct is None and rules.max_total_loss_abs is None:
        return None

    # Determine reference for total loss
    if rules.total_loss_reference == TotalLossReference.INITIAL_BALANCE:
        reference = state.starting_balance
    elif rules.total_loss_reference == TotalLossReference.PEAK_BALANCE:
        reference = state.peak_balance
    else:
        reference = state.peak_equity

    total_loss = state.current_balance - state.starting_balance

    # Check percentage limit
    if rules.max_total_loss_pct is not None and reference > 0:
        max_loss = reference * rules.max_total_loss_pct
        if total_loss <= -max_loss:
            return FailureReason.TOTAL_LOSS_BREACH

    # Check absolute limit
    if rules.max_total_loss_abs is not None:
        if total_loss <= -rules.max_total_loss_abs:
            return FailureReason.TOTAL_LOSS_BREACH

    return None


# ---------------------------------------------------------------------------
# Trailing drawdown check
# ---------------------------------------------------------------------------


def update_trailing_dd(state: AccountState, rules: FirmRuleSet) -> float:
    """Compute the updated trailing drawdown floor.

    Args:
        state: Current account state.
        rules: Firm rule set with trailing DD configuration.

    Returns:
        Updated trailing drawdown floor value.
    """
    mode = rules.trailing_drawdown_mode
    pct = rules.trailing_drawdown_pct

    if mode == TrailingDrawdownMode.NONE or pct is None:
        return state.trailing_dd_floor

    # Determine which peak to track
    if mode in (
        TrailingDrawdownMode.BALANCE_TRAILING,
        TrailingDrawdownMode.BALANCE_TRAILING_UNTIL_BREAKEVEN,
    ):
        peak = state.peak_balance
    else:
        peak = state.peak_equity

    new_floor = peak * (1 - pct)

    # For "until breakeven" modes, stop trailing once we hit initial balance
    if mode in (
        TrailingDrawdownMode.BALANCE_TRAILING_UNTIL_BREAKEVEN,
        TrailingDrawdownMode.EQUITY_TRAILING_UNTIL_BREAKEVEN,
    ):
        breakeven_floor = state.starting_balance
        new_floor = min(new_floor, breakeven_floor)

    # Floor can only go up (tighter), never down
    return max(state.trailing_dd_floor, new_floor)


def check_trailing_dd(state: AccountState, rules: FirmRuleSet) -> FailureReason | None:
    """Check if the trailing drawdown has been breached.

    Args:
        state: Current account state with updated trailing_dd_floor.
        rules: Firm rule set.

    Returns:
        FailureReason.TRAILING_DD_BREACH if breached, None if within limits.
    """
    if rules.trailing_drawdown_mode == TrailingDrawdownMode.NONE:
        return None

    if state.trailing_dd_floor <= 0:
        return None

    if state.equity <= state.trailing_dd_floor:
        return FailureReason.TRAILING_DD_BREACH

    return None


# ---------------------------------------------------------------------------
# Profit target check
# ---------------------------------------------------------------------------


def check_profit_target(state: AccountState, rules: FirmRuleSet) -> bool:
    """Check if the profit target has been reached.

    Args:
        state: Current account state.
        rules: Firm rule set with profit target.

    Returns:
        True if profit target reached, False otherwise.
    """
    if rules.profit_target_pct is None and rules.profit_target_abs is None:
        return False

    if rules.profit_target_includes_unrealized:
        profit = state.realized_pnl_total + state.daily_unrealized_pnl
    else:
        profit = state.realized_pnl_total

    # Check percentage target
    if rules.profit_target_pct is not None:
        target = state.starting_balance * rules.profit_target_pct
        if profit >= target:
            return True

    # Check absolute target
    if rules.profit_target_abs is not None:
        if profit >= rules.profit_target_abs:
            return True

    return False


# ---------------------------------------------------------------------------
# Minimum trading days check
# ---------------------------------------------------------------------------


def check_min_trading_days(state: AccountState, rules: FirmRuleSet) -> bool:
    """Check if minimum trading days requirement has been met.

    Args:
        state: Current account state.
        rules: Firm rule set with min trading days.

    Returns:
        True if requirement met, False otherwise.
    """
    if rules.min_trading_days is None:
        return True
    return state.trading_days_count >= rules.min_trading_days


# ---------------------------------------------------------------------------
# Consistency rule check
# ---------------------------------------------------------------------------


def check_consistency_rule(
    daily_pnls: list[float], rules: FirmRuleSet
) -> FailureReason | None:
    """Check if the consistency rule is satisfied.

    The best day's profit must not exceed a threshold of total profit.

    Args:
        daily_pnls: List of daily realized PnL values.
        rules: Firm rule set with consistency configuration.

    Returns:
        FailureReason.CONSISTENCY_BREACH if breached, None if ok.
    """
    if not rules.consistency_rule_enabled:
        return None

    total_profit = sum(p for p in daily_pnls if p > 0)
    if total_profit <= 0:
        return None

    best_day = max(daily_pnls) if daily_pnls else 0.0
    if best_day > rules.consistency_threshold_pct * total_profit:
        return FailureReason.CONSISTENCY_BREACH

    return None


# ---------------------------------------------------------------------------
# Aggregate rule check
# ---------------------------------------------------------------------------


def check_all_rules(state: AccountState, rules: FirmRuleSet) -> FailureReason | None:
    """Run all rule checks and return the first breach found.

    Checks are ordered by severity: daily loss, total loss, trailing DD.

    Args:
        state: Current account state.
        rules: Firm rule set.

    Returns:
        First FailureReason found, or None if all rules pass.
    """
    breach = check_daily_loss(state, rules)
    if breach:
        return breach

    breach = check_total_loss(state, rules)
    if breach:
        return breach

    return check_trailing_dd(state, rules)
