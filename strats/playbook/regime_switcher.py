"""Regime-based multi-strategy switcher.

Detects the current market regime probabilistically and dynamically
weights sub-strategies accordingly. Implements the full decision flow
from SYSTEM.md: features → regime detection → weight computation →
signal combination → final positions.

Key design principles:
- Probabilistic regimes (no hard switches)
- Weighted strategies (soft allocation)
- Continuous transitions (EMA smoothing + decay)
- Multiple strategies can be active simultaneously
"""

from dataclasses import dataclass
from enum import Enum

import numpy as np
import pandas as pd

from ..config import Direction, RegimeConfig, RegimeSwitcherConfig, StrategyConfig
from ..features import (
    adx,
    atr_ratio,
    bollinger_width,
    ema_slope,
    higher_highs_lower_lows,
    range_compression,
    volume_ratio,
)
from ..registry import Strategy, register_strategy
from .defensive import Defensive
from .mean_reversion import MeanReversion
from .trend_follow import TrendFollow
from .volatility_breakout import VolatilityBreakout

# ---------------------------------------------------------------------------
# Regime types and probabilities
# ---------------------------------------------------------------------------


class RegimeType(str, Enum):
    """Market regime classification."""

    TREND_UP = 'trend_up'
    TREND_DOWN = 'trend_down'
    SIDEWAYS = 'sideways'
    HIGH_VOLATILITY = 'high_volatility'
    LOW_ACTIVITY = 'low_activity'


@dataclass(frozen=True)
class RegimeProbabilities:
    """Probability distribution over market regimes for a single bar.

    All values in [0, 1] and sum to 1.0.
    """

    trend_up: float
    trend_down: float
    sideways: float
    high_volatility: float
    low_activity: float

    def as_dict(self) -> dict[str, float]:
        """Return probabilities as a regime-keyed dictionary."""
        return {
            RegimeType.TREND_UP: self.trend_up,
            RegimeType.TREND_DOWN: self.trend_down,
            RegimeType.SIDEWAYS: self.sideways,
            RegimeType.HIGH_VOLATILITY: self.high_volatility,
            RegimeType.LOW_ACTIVITY: self.low_activity,
        }


# ---------------------------------------------------------------------------
# Regime → strategy mapping matrix (from SYSTEM.md §5.2)
# ---------------------------------------------------------------------------

REGIME_STRATEGY_MATRIX: dict[str, dict[str, float]] = {
    RegimeType.TREND_UP: {
        'trend': 1.0,
        'mean_rev': 0.1,
        'vol_break': 0.3,
        'defensive': 0.0,
    },
    RegimeType.TREND_DOWN: {
        'trend': 1.0,
        'mean_rev': 0.1,
        'vol_break': 0.3,
        'defensive': 0.0,
    },
    RegimeType.SIDEWAYS: {
        'trend': 0.2,
        'mean_rev': 1.0,
        'vol_break': 0.2,
        'defensive': 0.1,
    },
    RegimeType.HIGH_VOLATILITY: {
        'trend': 0.3,
        'mean_rev': 0.2,
        'vol_break': 1.0,
        'defensive': 0.2,
    },
    RegimeType.LOW_ACTIVITY: {
        'trend': 0.0,
        'mean_rev': 0.1,
        'vol_break': 0.0,
        'defensive': 1.0,
    },
}

STRATEGY_KEYS = ('trend', 'mean_rev', 'vol_break', 'defensive')


# ---------------------------------------------------------------------------
# Regime detection — pure functions
# ---------------------------------------------------------------------------


def _sigmoid(x: pd.Series | float, center: float, scale: float) -> pd.Series | float:  # type: ignore[type-arg]
    """Smooth sigmoid mapping from feature value to [0, 1] probability.

    Args:
        x: Input feature values.
        center: Center point of the sigmoid.
        scale: Controls steepness (smaller = steeper).

    Returns:
        Values in [0, 1].
    """
    return 1.0 / (1.0 + np.exp(-(x - center) / max(scale, 1e-10)))


def compute_regime_features(df: pd.DataFrame, cfg: RegimeConfig) -> pd.DataFrame:
    """Compute all features needed for regime detection.

    Args:
        df: OHLCV DataFrame.
        cfg: Regime detection configuration.

    Returns:
        DataFrame with computed feature columns.
    """
    close: pd.Series = df['close']  # type: ignore[assignment]
    high: pd.Series = df['high']  # type: ignore[assignment]
    low: pd.Series = df['low']  # type: ignore[assignment]
    vol: pd.Series = df.get('volume', pd.Series(1.0, index=df.index))  # type: ignore[assignment]

    features = pd.DataFrame(index=df.index)
    features['ema_slope'] = ema_slope(close, cfg.ema_period, cfg.ema_slope_window)
    features['adx'] = adx(high, low, close, cfg.adx_period)
    features['atr_ratio'] = atr_ratio(
        high, low, close, cfg.atr_fast_period, cfg.atr_slow_period
    )
    features['boll_width'] = bollinger_width(close, cfg.bollinger_period)
    features['vol_ratio'] = volume_ratio(
        vol, cfg.volume_fast_period, cfg.volume_slow_period
    )
    features['range_comp'] = range_compression(
        high, low, fast_period=5, slow_period=cfg.range_period
    )
    features['hh_ll'] = higher_highs_lower_lows(high, low, cfg.structure_period)

    return features


def detect_regime_probabilities(
    features: pd.DataFrame,
    cfg: RegimeConfig,
) -> pd.DataFrame:
    """Detect market regime as probability distribution per bar.

    Uses heuristic fuzzy logic on computed features to produce
    continuous [0, 1] probabilities for each regime. All five
    regime probabilities sum to 1.0 per bar.

    Args:
        features: DataFrame from compute_regime_features.
        cfg: Regime detection configuration.

    Returns:
        DataFrame with columns for each RegimeType, values in [0, 1],
        rows summing to 1.0.
    """
    slope: pd.Series = features['ema_slope']  # type: ignore[assignment]
    adx_val: pd.Series = features['adx']  # type: ignore[assignment]
    atr_r: pd.Series = features['atr_ratio']  # type: ignore[assignment]
    bw: pd.Series = features['boll_width']  # type: ignore[assignment]
    vol_r: pd.Series = features['vol_ratio']  # type: ignore[assignment]
    rc: pd.Series = features['range_comp']  # type: ignore[assignment]
    hh_ll: pd.Series = features['hh_ll']  # type: ignore[assignment]

    # --- Trend direction scores ---
    # EMA slope direction (positive = up, negative = down)
    slope_magnitude = slope.abs()
    trend_strength = _sigmoid(
        slope_magnitude, cfg.ema_slope_threshold, cfg.ema_slope_threshold / 2
    )

    # ADX confirms trend existence
    adx_trend = _sigmoid(
        adx_val, cfg.adx_weak_trend, (cfg.adx_strong_trend - cfg.adx_weak_trend) / 3
    )

    # Structure confirmation
    structure_up = _sigmoid(hh_ll, 0.2, 0.15)
    structure_down = _sigmoid(-hh_ll, 0.2, 0.15)

    # Combined trend scores
    trend_up_raw = (
        trend_strength
        * adx_trend
        * _sigmoid(slope, cfg.ema_slope_threshold / 2, cfg.ema_slope_threshold / 3)
        * structure_up
    )
    trend_down_raw = (
        trend_strength
        * adx_trend
        * _sigmoid(-slope, cfg.ema_slope_threshold / 2, cfg.ema_slope_threshold / 3)
        * structure_down
    )

    # --- Volatility score ---
    vol_from_atr = _sigmoid(atr_r, cfg.atr_ratio_high, 0.3)
    vol_from_bw = _sigmoid(bw, cfg.bollinger_width_high, cfg.bollinger_width_high / 3)
    high_vol_raw = (vol_from_atr + vol_from_bw) / 2.0

    # --- Low activity score ---
    low_volume = _sigmoid(-vol_r, -cfg.volume_ratio_low, 0.15)
    low_range = _sigmoid(-rc, -cfg.range_compression_low, 0.15)
    low_activity_raw = (low_volume + low_range) / 2.0

    # --- Sideways: what's left when nothing else is dominant ---
    # Low trend + low volatility + not inactive
    no_trend = 1.0 - (trend_up_raw + trend_down_raw).clip(0, 1)  # type: ignore[union-attr]
    no_extreme_vol = 1.0 - high_vol_raw.clip(0, 1)  # type: ignore[union-attr]
    not_dead = 1.0 - low_activity_raw.clip(0, 1)  # type: ignore[union-attr]
    sideways_raw = no_trend * no_extreme_vol * not_dead

    # --- Normalize to probability distribution ---
    raw = pd.DataFrame(
        {
            RegimeType.TREND_UP: trend_up_raw,
            RegimeType.TREND_DOWN: trend_down_raw,
            RegimeType.SIDEWAYS: sideways_raw,
            RegimeType.HIGH_VOLATILITY: high_vol_raw,
            RegimeType.LOW_ACTIVITY: low_activity_raw,
        },
        index=features.index,
    )

    # Clip negatives, then normalize rows to sum to 1
    raw = raw.clip(lower=0.0)
    row_sums = raw.sum(axis=1).replace(0.0, 1.0)
    normalized = raw.div(row_sums, axis=0)

    return normalized


def smooth_regime_probabilities(
    regime_probs: pd.DataFrame,
    span: int = 10,
) -> pd.DataFrame:
    """Apply EMA smoothing to regime probabilities for transition stability.

    Prevents rapid oscillation between regimes by smoothing the
    probability time series. Re-normalizes after smoothing.

    Args:
        regime_probs: Raw regime probability DataFrame.
        span: EMA span for smoothing.

    Returns:
        Smoothed and re-normalized regime probability DataFrame.
    """
    smoothed = regime_probs.ewm(span=span, adjust=False).mean()

    # Re-normalize after smoothing
    row_sums = smoothed.sum(axis=1).replace(0.0, 1.0)
    return smoothed.div(row_sums, axis=0)  # type: ignore[return-value]


def detect_regime_transition(
    regime_probs: pd.DataFrame,
    threshold: float = 0.15,
) -> pd.Series:  # type: ignore[type-arg]
    """Detect regime transitions for scaling weights during unstable periods.

    A transition is detected when the dominant regime changes or when
    the probability mass shifts significantly between bars.

    Args:
        regime_probs: Smoothed regime probability DataFrame.
        threshold: Minimum probability shift to flag as transition.

    Returns:
        Boolean Series — True where regime transition detected.
    """
    # Compute bar-to-bar change in the full probability vector
    prob_change = regime_probs.diff().abs().sum(axis=1)

    # Also check if the dominant regime changed
    dominant_now = regime_probs.idxmax(axis=1)
    dominant_prev = dominant_now.shift(1)
    regime_changed = dominant_now != dominant_prev

    return (prob_change > threshold) | regime_changed


# ---------------------------------------------------------------------------
# Strategy weight computation — pure functions
# ---------------------------------------------------------------------------


def compute_strategy_weights(
    regime_probs: pd.DataFrame,
    matrix: dict[str, dict[str, float]] | None = None,
) -> pd.DataFrame:
    """Compute strategy weights from regime probabilities.

    Implements the weight formula from SYSTEM.md §5.3:
        strategy_weight = sum_over_regimes(regime_prob * mapping_value)

    Then normalizes weights to sum to 1.0 per bar.

    Args:
        regime_probs: DataFrame with regime probability columns.
        matrix: Regime→strategy mapping matrix. Defaults to REGIME_STRATEGY_MATRIX.

    Returns:
        DataFrame with strategy weight columns, rows summing to 1.0.
    """
    if matrix is None:
        matrix = REGIME_STRATEGY_MATRIX

    weights = pd.DataFrame(0.0, index=regime_probs.index, columns=list(STRATEGY_KEYS))

    for regime_name in regime_probs.columns:
        regime_prob = regime_probs[regime_name]
        strategy_map = matrix.get(regime_name, {})
        for strat_key in STRATEGY_KEYS:
            mapping_value = strategy_map.get(strat_key, 0.0)
            weights[strat_key] += regime_prob * mapping_value

    # Normalize
    row_sums = weights.sum(axis=1).replace(0.0, 1.0)
    return weights.div(row_sums, axis=0)


def apply_transition_scaling(
    weights: pd.DataFrame,
    is_transition: pd.Series,  # type: ignore[type-arg]
    decay: float = 0.6,
) -> pd.DataFrame:
    """Scale down all weights during regime transitions.

    Reduces exposure during unstable periods to minimize
    regime-switch losses.

    Args:
        weights: Strategy weight DataFrame.
        is_transition: Boolean Series flagging transition bars.
        decay: Scale factor applied during transitions (0-1).

    Returns:
        Scaled weight DataFrame.
    """
    scale = pd.Series(1.0, index=weights.index)
    scale = scale.where(~is_transition, decay)
    return weights.mul(scale, axis=0)


# ---------------------------------------------------------------------------
# Regime Switcher — multi-strategy orchestrator
# ---------------------------------------------------------------------------


class RegimeSwitcher(Strategy):
    """Adaptive multi-strategy switcher based on market regime detection.

    Detects the current market regime using heuristic features, computes
    probabilistic weights for each sub-strategy, runs all sub-strategies
    independently, and combines their signals using weighted voting.

    The regime detection is transparent and deterministic — no ML training
    required. The interface is designed so that the detector can be
    upgraded to HMM or GMM later without changing the strategy API.

    Args:
        entry_threshold: Minimum weighted vote to trigger entry.
        exit_threshold: Minimum weighted vote to trigger exit.
        trend_fast_window: Fast EMA period for trend strategy.
        trend_slow_window: Slow EMA period for trend strategy.
        trend_atr_multiplier: ATR multiplier for trend stops.
        trend_adx_period: ADX period for trend strategy.
        mean_rev_period: Bollinger Band period for mean reversion.
        mean_rev_num_std: Std deviations for Bollinger Bands.
        vol_breakout_compression_period: Compression detection period.
        vol_breakout_atr_multiplier: ATR multiplier for vol breakout stops.
        direction: Trading direction.
    """

    def __init__(
        self,
        entry_threshold: float = 0.4,
        exit_threshold: float = 0.3,
        trend_fast_window: int | float = 10,
        trend_slow_window: int | float = 30,
        trend_atr_multiplier: float = 2.0,
        trend_adx_period: int | float = 14,
        mean_rev_period: int | float = 20,
        mean_rev_num_std: float = 2.0,
        vol_breakout_compression_period: int | float = 20,
        vol_breakout_atr_multiplier: float = 1.5,
        direction: Direction = Direction.LONG_ONLY,
        regime_config: RegimeConfig | None = None,
        **_kwargs: object,
    ) -> None:
        self.entry_threshold = float(entry_threshold)
        self.exit_threshold = float(exit_threshold)
        self.direction = direction
        self.regime_config = regime_config or RegimeConfig()

        # Initialize sub-strategies
        self._trend = TrendFollow(
            fast_window=int(trend_fast_window),
            slow_window=int(trend_slow_window),
            atr_multiplier=float(trend_atr_multiplier),
            adx_period=int(trend_adx_period),
            direction=direction,
        )
        self._mean_rev = MeanReversion(
            period=int(mean_rev_period),
            num_std=float(mean_rev_num_std),
            direction=direction,
        )
        self._vol_break = VolatilityBreakout(
            compression_period=int(vol_breakout_compression_period),
            atr_multiplier=float(vol_breakout_atr_multiplier),
            direction=direction,
        )
        self._defensive = Defensive(direction=direction)

    def config(self) -> StrategyConfig:
        """Return strategy configuration from instance state."""
        return StrategyConfig(
            name='regime_switcher',
            params={
                'entry_threshold': self.entry_threshold,
                'exit_threshold': self.exit_threshold,
                'trend_fast_window': self._trend.fast_window,
                'trend_slow_window': self._trend.slow_window,
                'trend_atr_multiplier': self._trend.atr_multiplier,
                'trend_adx_period': self._trend.adx_period,
                'mean_rev_period': self._mean_rev.period,
                'mean_rev_num_std': self._mean_rev.num_std,
                'vol_breakout_compression_period': self._vol_break.compression_period,
                'vol_breakout_atr_multiplier': self._vol_break.atr_multiplier,
            },
            direction=self.direction,
        )

    def detect_regime(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect regime probabilities for the given data.

        Convenience method that runs the full detection pipeline:
        features → raw probabilities → smoothing.

        Args:
            df: OHLCV DataFrame.

        Returns:
            Smoothed regime probability DataFrame.
        """
        features = compute_regime_features(df, self.regime_config)
        raw_probs = detect_regime_probabilities(features, self.regime_config)
        return smooth_regime_probabilities(
            raw_probs, self.regime_config.regime_ema_span
        )

    def signals(self, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:  # type: ignore[type-arg]
        """Generate adaptive multi-strategy entry/exit signals.

        Full decision flow:
        1. Compute market features
        2. Detect regime probabilities (smoothed)
        3. Compute strategy weights from regime
        4. Apply transition scaling
        5. Run all sub-strategies independently
        6. Combine signals using weighted voting
        7. Apply thresholds for final entry/exit

        Args:
            df: OHLCV DataFrame with 'open', 'high', 'low', 'close', 'volume'.

        Returns:
            Tuple of (entries, exits) as boolean Series.
        """
        cfg = self.regime_config

        # 1. Detect regime
        regime_probs = self.detect_regime(df)

        # 2. Compute strategy weights
        weights = compute_strategy_weights(regime_probs)

        # 3. Apply transition scaling
        is_transition = detect_regime_transition(regime_probs)
        weights = apply_transition_scaling(weights, is_transition, cfg.transition_decay)

        # 4. Run all sub-strategies independently
        trend_entries, trend_exits = self._trend.signals(df)
        mr_entries, mr_exits = self._mean_rev.signals(df)
        vb_entries, vb_exits = self._vol_break.signals(df)
        def_entries, def_exits = self._defensive.signals(df)

        # 5. Weighted voting for entries
        entry_vote = (
            weights['trend'] * trend_entries.astype(float)
            + weights['mean_rev'] * mr_entries.astype(float)
            + weights['vol_break'] * vb_entries.astype(float)
            + weights['defensive'] * def_entries.astype(float)
        )

        # 6. Weighted voting for exits
        exit_vote = (
            weights['trend'] * trend_exits.astype(float)
            + weights['mean_rev'] * mr_exits.astype(float)
            + weights['vol_break'] * vb_exits.astype(float)
            + weights['defensive'] * def_exits.astype(float)
        )

        # 7. Apply thresholds
        entries = (entry_vote >= self.entry_threshold).fillna(False).astype(bool)
        exits = (exit_vote >= self.exit_threshold).fillna(False).astype(bool)

        return entries, exits


register_strategy('regime_switcher', RegimeSwitcher)
