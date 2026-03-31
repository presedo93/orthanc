"""Firm preset configurations — the Dark Lords of Mordor.

Factory functions that return pre-configured FirmConfig instances for
common prop firm profiles. Start with generic profiles; specific
commercial presets can be added later.

Technical name: funds/presets.py — Prop firm presets.

Function mapping:
- witch_king() = ftmo_like_10k()
- mouth_of_sauron() = topstep_like_50k()
- sauron_basic() = generic_single_phase_eval()
- sauron_trial() = generic_two_phase_eval()
"""

from .dark_tongue import (
    DailyLossReference,
    FirmConfig,
    FirmRuleSet,
    TotalLossReference,
    TrailingDrawdownMode,
)

__all__ = [
    'witch_king',
    'sauron_basic',
    'sauron_trial',
    'mouth_of_sauron',
]


def sauron_basic(
    account_size: float = 50_000.0,
) -> FirmConfig:
    """Generic single-phase evaluation account.

    Technical name: generic_single_phase_eval — conservative single-phase eval.

    Conservative defaults: 4% daily loss, 10% total loss, 8% profit
    target, 5 minimum trading days. No trailing drawdown.

    Args:
        account_size: Starting account balance.

    Returns:
        FirmConfig for a single-phase evaluation.
    """
    return FirmConfig(
        firm_name='generic_prop',
        plan_name=f'{int(account_size / 1000)}k_single_phase',
        account_size=account_size,
        evaluation_fee=account_size * 0.006,
        reset_fee=account_size * 0.002,
        activation_fee=0.0,
        profit_split_pct=0.80,
        rule_set=FirmRuleSet(
            max_daily_loss_pct=0.04,
            daily_loss_reference=DailyLossReference.BALANCE_START_OF_DAY,
            daily_loss_includes_unrealized=True,
            max_total_loss_pct=0.10,
            total_loss_reference=TotalLossReference.INITIAL_BALANCE,
            trailing_drawdown_mode=TrailingDrawdownMode.NONE,
            profit_target_pct=0.08,
            profit_target_includes_unrealized=False,
            min_trading_days=5,
            consistency_rule_enabled=False,
        ),
    )


def sauron_trial(
    account_size: float = 100_000.0,
) -> FirmConfig:
    """Generic two-phase evaluation account (phase 1: challenge).

    Technical name: generic_two_phase_eval — tighter challenge phase.

    Tighter profit target (10%) to reflect challenge phase difficulty.
    5% daily loss, 10% total loss, 10 minimum trading days.

    Note: Phase 2 (verification) would use a separate FirmConfig with
    a lower profit target. Multi-phase state machine is Phase 2 work.

    Args:
        account_size: Starting account balance.

    Returns:
        FirmConfig for phase 1 of a two-phase evaluation.
    """
    return FirmConfig(
        firm_name='generic_two_phase',
        plan_name=f'{int(account_size / 1000)}k_phase1',
        account_size=account_size,
        evaluation_fee=account_size * 0.005,
        reset_fee=account_size * 0.002,
        activation_fee=0.0,
        profit_split_pct=0.80,
        rule_set=FirmRuleSet(
            max_daily_loss_pct=0.05,
            daily_loss_reference=DailyLossReference.BALANCE_START_OF_DAY,
            daily_loss_includes_unrealized=True,
            max_total_loss_pct=0.10,
            total_loss_reference=TotalLossReference.INITIAL_BALANCE,
            trailing_drawdown_mode=TrailingDrawdownMode.NONE,
            profit_target_pct=0.10,
            profit_target_includes_unrealized=False,
            min_trading_days=10,
            consistency_rule_enabled=False,
        ),
    )


def witch_king() -> FirmConfig:
    """FTMO-inspired 10k evaluation account.

    Technical name: ftmo_like_10k — FTMO-style rules.

    5% daily loss, 10% total loss, 10% profit target for challenge.
    Includes minimum 4 trading days and no trailing drawdown.

    Returns:
        FirmConfig modeled after FTMO-style rules.
    """
    return FirmConfig(
        firm_name='ftmo_like',
        plan_name='10k_challenge',
        account_size=10_000.0,
        evaluation_fee=155.0,
        reset_fee=99.0,
        activation_fee=0.0,
        profit_split_pct=0.80,
        rule_set=FirmRuleSet(
            max_daily_loss_pct=0.05,
            daily_loss_reference=DailyLossReference.EQUITY_START_OF_DAY,
            daily_loss_includes_unrealized=True,
            max_total_loss_pct=0.10,
            total_loss_reference=TotalLossReference.INITIAL_BALANCE,
            trailing_drawdown_mode=TrailingDrawdownMode.NONE,
            profit_target_pct=0.10,
            profit_target_includes_unrealized=False,
            min_trading_days=4,
            consistency_rule_enabled=False,
        ),
    )


def mouth_of_sauron() -> FirmConfig:
    """Topstep-inspired 50k evaluation account.

    Technical name: topstep_like_50k — Topstep-style rules.

    Features trailing drawdown (equity-based, trails until breakeven)
    rather than a fixed total loss. Profit target of 6%.

    Returns:
        FirmConfig modeled after Topstep-style rules.
    """
    return FirmConfig(
        firm_name='topstep_like',
        plan_name='50k_combine',
        account_size=50_000.0,
        evaluation_fee=165.0,
        reset_fee=99.0,
        activation_fee=150.0,
        profit_split_pct=0.90,
        rule_set=FirmRuleSet(
            max_daily_loss_pct=0.04,
            daily_loss_reference=DailyLossReference.BALANCE_START_OF_DAY,
            daily_loss_includes_unrealized=True,
            max_total_loss_pct=None,
            total_loss_reference=TotalLossReference.INITIAL_BALANCE,
            trailing_drawdown_mode=TrailingDrawdownMode.EQUITY_TRAILING_UNTIL_BREAKEVEN,
            trailing_drawdown_pct=0.04,
            profit_target_pct=0.06,
            profit_target_includes_unrealized=False,
            min_trading_days=5,
            consistency_rule_enabled=False,
        ),
    )
