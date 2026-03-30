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

from enum import Enum

import numpy as np
import pandas as pd

from ..config import (
    Direction,
    RegimeConfig,
    RegimeSwitcherConfig,
    StrategyConfig,
    WeightAdjustmentConfig,
)
from ..features import (
    adx,
    atr_ratio,
    bollinger_width,
    ema_slope,
    higher_highs_lower_lows,
    range_compression,
    realized_volatility,
    volume_ratio,
)
from ..registry import Strategy, register_strategy
from .defensive import Defensive
from .mean_reversion import MeanReversion
from .trend_follow import TrendFollow
from .volatility_breakout import VolatilityBreakout

# ---------------------------------------------------------------------------
# Regime types
# ---------------------------------------------------------------------------


class RegimeType(str, Enum):
    """Market regime classification."""

    TREND_UP = 'trend_up'
    TREND_DOWN = 'trend_down'
    SIDEWAYS = 'sideways'
    HIGH_VOLATILITY = 'high_volatility'
    LOW_ACTIVITY = 'low_activity'


# ---------------------------------------------------------------------------
# Regime → strategy mapping matrix (from SYSTEM.md §5.2)
# ---------------------------------------------------------------------------

#: Strategy keys used throughout the module — order matters for matrix columns.
STRATEGY_KEYS = ('trend', 'mean_rev', 'vol_break', 'defensive')

#: Mapping matrix as a DataFrame for vectorized weight computation.
#: Rows = regimes, columns = strategy keys.
REGIME_STRATEGY_MATRIX = pd.DataFrame(
    {
        'trend': [1.0, 1.0, 0.2, 0.3, 0.0],
        'mean_rev': [0.1, 0.1, 1.0, 0.2, 0.1],
        'vol_break': [0.3, 0.3, 0.2, 1.0, 0.0],
        'defensive': [0.0, 0.0, 0.1, 0.2, 1.0],
    },
    index=[
        RegimeType.TREND_UP,
        RegimeType.TREND_DOWN,
        RegimeType.SIDEWAYS,
        RegimeType.HIGH_VOLATILITY,
        RegimeType.LOW_ACTIVITY,
    ],
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize each row to sum to 1.0, treating all-zero rows as uniform.

    Args:
        df: DataFrame with non-negative values.

    Returns:
        Row-normalized DataFrame.
    """
    row_sums = df.sum(axis=1).replace(0.0, 1.0)
    return df.div(row_sums, axis=0)


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


def _weighted_vote(
    weights: pd.DataFrame,
    signals: dict[str, pd.Series],  # type: ignore[type-arg]
) -> pd.Series:  # type: ignore[type-arg]
    """Compute weighted vote from strategy weights and boolean signals.

    Args:
        weights: Strategy weight DataFrame (columns = strategy keys).
        signals: Dict mapping strategy key to boolean signal Series.

    Returns:
        Weighted vote Series (float in [0, 1]).
    """
    vote = pd.Series(0.0, index=weights.index)
    for key, signal in signals.items():
        vote += weights[key] * signal.astype(float)
    return vote


# ---------------------------------------------------------------------------
# Regime detection — pure functions
# ---------------------------------------------------------------------------


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

    return _normalize_rows(raw.clip(lower=0.0))


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
    return _normalize_rows(smoothed)  # type: ignore[arg-type]


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
    matrix: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute strategy weights from regime probabilities.

    Implements the weight formula from SYSTEM.md §5.3 as a matrix
    multiply: ``weights = regime_probs @ matrix``, then normalizes.

    Args:
        regime_probs: DataFrame with regime probability columns.
        matrix: Regime→strategy mapping matrix (rows=regimes, cols=strategies).
            Defaults to REGIME_STRATEGY_MATRIX.

    Returns:
        DataFrame with strategy weight columns, rows summing to 1.0.
    """
    if matrix is None:
        matrix = REGIME_STRATEGY_MATRIX

    weights: pd.DataFrame = regime_probs @ matrix  # type: ignore[assignment]
    return _normalize_rows(weights)


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
# Performance & volatility weight adjustments (SYSTEM.md §5.5)
# ---------------------------------------------------------------------------


def compute_performance_factors(
    close: pd.Series,  # type: ignore[type-arg]
    strategy_entries: dict[str, pd.Series],  # type: ignore[type-arg]
    cfg: WeightAdjustmentConfig,
) -> pd.DataFrame:
    """Compute per-strategy performance factors from rolling hit rate.

    For each strategy, measures what fraction of entry signals were
    directionally correct: was close higher ``cfg.perf_horizon`` bars
    after the entry signal? The hit rate is converted to a multiplicative
    factor in [cfg.floor, cfg.ceiling].

    A hit rate of 0.5 (coin flip) maps to factor 1.0 (neutral).
    Above 0.5 scales up; below 0.5 scales down.

    Args:
        close: Close price series.
        strategy_entries: Dict mapping strategy key to boolean entry Series.
        cfg: Weight adjustment configuration.

    Returns:
        DataFrame with strategy keys as columns, performance factors
        as values, clamped to [cfg.floor, cfg.ceiling].
    """
    forward_return = close.shift(-cfg.perf_horizon) - close
    factors = pd.DataFrame(index=close.index, columns=list(strategy_entries.keys()))

    for key, entries in strategy_entries.items():
        # Mark directionally correct signals (price went up after entry)
        hits = (forward_return > 0).astype(float)
        # Only count bars where this strategy actually signaled
        signal_float = entries.astype(float)
        # Rolling hit rate: sum of correct signals / sum of all signals
        rolling_hits = (
            (signal_float * hits).rolling(window=cfg.perf_lookback, min_periods=1).sum()
        )
        rolling_signals = signal_float.rolling(
            window=cfg.perf_lookback, min_periods=1
        ).sum()

        hit_rate = rolling_hits / rolling_signals.where(  # type: ignore[assignment]
            rolling_signals != 0.0, np.nan
        )
        # Not enough signals → neutral factor
        hit_rate = hit_rate.where(rolling_signals >= cfg.perf_min_signals, 0.5)
        hit_rate = hit_rate.fillna(0.5)

        # Map hit rate to factor: 0.5 → 1.0, 0.0 → floor, 1.0 → ceiling
        # Linear mapping: factor = floor + (ceiling - floor) * hit_rate
        factor = cfg.floor + (cfg.ceiling - cfg.floor) * hit_rate
        factors[key] = factor.clip(cfg.floor, cfg.ceiling)

    return factors.astype(float)


def compute_volatility_scaling(
    close: pd.Series,  # type: ignore[type-arg]
    cfg: WeightAdjustmentConfig,
) -> pd.Series:  # type: ignore[type-arg]
    """Compute a scalar volatility adjustment factor per bar.

    Compares current realized volatility to a target level.
    When vol is above target → scale down (reduce exposure).
    When vol is below target → scale up (increase exposure).

    The factor is ``target / current_vol``, clamped to
    [cfg.floor, cfg.ceiling].

    Args:
        close: Close price series.
        cfg: Weight adjustment configuration.

    Returns:
        Series of volatility scaling factors, clamped to
        [cfg.floor, cfg.ceiling].
    """
    current_vol = realized_volatility(close, cfg.vol_period)
    # Avoid division by zero — treat near-zero vol as target (neutral)
    safe_vol = current_vol.replace(0.0, cfg.vol_target).clip(lower=1e-10)
    factor = cfg.vol_target / safe_vol
    return factor.clip(cfg.floor, cfg.ceiling)


def apply_weight_adjustments(
    weights: pd.DataFrame,
    perf_factors: pd.DataFrame,
    vol_scale: pd.Series,  # type: ignore[type-arg]
) -> pd.DataFrame:
    """Apply performance and volatility adjustments to strategy weights.

    Multiplies each strategy weight by its performance factor and the
    global volatility scaling. Re-normalizes so weights sum to 1.0.

    Args:
        weights: Strategy weight DataFrame (columns = strategy keys).
        perf_factors: Per-strategy performance factors DataFrame.
        vol_scale: Scalar volatility adjustment Series.

    Returns:
        Adjusted and re-normalized weight DataFrame.
    """
    adjusted = weights * perf_factors
    adjusted = adjusted.mul(vol_scale, axis=0)
    return _normalize_rows(adjusted)


# ---------------------------------------------------------------------------
# Regime Switcher — multi-strategy orchestrator
# ---------------------------------------------------------------------------

#: Default sub-strategy constructors keyed by strategy key.
#: Used by RegimeSwitcher to build its strategy dict.
_STRATEGY_CONSTRUCTORS: dict[str, type[Strategy]] = {
    'trend': TrendFollow,
    'mean_rev': MeanReversion,
    'vol_break': VolatilityBreakout,
    'defensive': Defensive,
}


class RegimeSwitcher(Strategy):
    """Adaptive multi-strategy switcher based on market regime detection.

    Detects the current market regime using heuristic features, computes
    probabilistic weights for each sub-strategy, runs all sub-strategies
    independently, and combines their signals using weighted voting.

    The regime detection is transparent and deterministic — no ML training
    required. The interface is designed so that the detector can be
    upgraded to HMM or GMM later without changing the strategy API.

    Args:
        switcher_config: Full switcher configuration. Individual params
            below override their corresponding config fields when both
            are provided (for backward compatibility with the registry).
        direction: Trading direction.
    """

    def __init__(
        self,
        switcher_config: RegimeSwitcherConfig | None = None,
        direction: Direction = Direction.LONG_ONLY,
        *,
        # Flat overrides for registry compatibility (build_strategy passes
        # params as kwargs). When switcher_config is provided these are
        # ignored; when it isn't, they seed a new config.
        entry_threshold: float | None = None,
        exit_threshold: float | None = None,
        trend_fast_window: int | float = 10,
        trend_slow_window: int | float = 30,
        trend_atr_multiplier: float = 2.0,
        trend_adx_period: int | float = 14,
        mean_rev_period: int | float = 20,
        mean_rev_num_std: float = 2.0,
        vol_breakout_compression_period: int | float = 20,
        vol_breakout_atr_multiplier: float = 1.5,
        regime_config: RegimeConfig | None = None,
        weight_adj_config: WeightAdjustmentConfig | None = None,
        **_kwargs: object,
    ) -> None:
        # Build config from explicit object or from flat kwargs
        if switcher_config is not None:
            cfg = switcher_config
        else:
            cfg = RegimeSwitcherConfig(
                regime=regime_config or RegimeConfig(),
                weight_adj=weight_adj_config or WeightAdjustmentConfig(),
                entry_threshold=entry_threshold or 0.4,
                exit_threshold=exit_threshold or 0.3,
                trend_fast_window=int(trend_fast_window),
                trend_slow_window=int(trend_slow_window),
                trend_atr_multiplier=float(trend_atr_multiplier),
                trend_adx_period=int(trend_adx_period),
                mean_rev_period=int(mean_rev_period),
                mean_rev_num_std=float(mean_rev_num_std),
                vol_breakout_compression_period=int(vol_breakout_compression_period),
                vol_breakout_atr_multiplier=float(vol_breakout_atr_multiplier),
            )

        self._cfg = cfg
        self.direction = direction

        # Build sub-strategies from config
        self._strategies: dict[str, Strategy] = {
            'trend': TrendFollow(
                fast_window=cfg.trend_fast_window,
                slow_window=cfg.trend_slow_window,
                atr_multiplier=cfg.trend_atr_multiplier,
                adx_period=cfg.trend_adx_period,
                direction=direction,
            ),
            'mean_rev': MeanReversion(
                period=cfg.mean_rev_period,
                num_std=cfg.mean_rev_num_std,
                direction=direction,
            ),
            'vol_break': VolatilityBreakout(
                compression_period=cfg.vol_breakout_compression_period,
                atr_multiplier=cfg.vol_breakout_atr_multiplier,
                direction=direction,
            ),
            'defensive': Defensive(direction=direction),
        }

    def config(self) -> StrategyConfig:
        """Return strategy configuration from instance state."""
        return StrategyConfig(
            name='regime_switcher',
            params={
                'entry_threshold': self._cfg.entry_threshold,
                'exit_threshold': self._cfg.exit_threshold,
                'trend_fast_window': self._cfg.trend_fast_window,
                'trend_slow_window': self._cfg.trend_slow_window,
                'trend_atr_multiplier': self._cfg.trend_atr_multiplier,
                'trend_adx_period': self._cfg.trend_adx_period,
                'mean_rev_period': self._cfg.mean_rev_period,
                'mean_rev_num_std': self._cfg.mean_rev_num_std,
                'vol_breakout_compression_period': self._cfg.vol_breakout_compression_period,
                'vol_breakout_atr_multiplier': self._cfg.vol_breakout_atr_multiplier,
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
        regime_cfg = self._cfg.regime
        features = compute_regime_features(df, regime_cfg)
        raw_probs = detect_regime_probabilities(features, regime_cfg)
        return smooth_regime_probabilities(raw_probs, regime_cfg.regime_ema_span)

    def signals(self, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:  # type: ignore[type-arg]
        """Generate adaptive multi-strategy entry/exit signals.

        Full decision flow:
        1. Detect regime probabilities (smoothed)
        2. Compute strategy weights from regime
        3. Apply transition scaling
        4. Run all sub-strategies independently
        5. Apply performance + volatility weight adjustments (§5.5)
        6. Combine signals via weighted voting + thresholds

        Args:
            df: OHLCV DataFrame with 'open', 'high', 'low', 'close', 'volume'.

        Returns:
            Tuple of (entries, exits) as boolean Series.
        """
        close: pd.Series = df['close']  # type: ignore[assignment]

        # 1. Detect regime
        regime_probs = self.detect_regime(df)

        # 2. Compute strategy weights
        weights = compute_strategy_weights(regime_probs)

        # 3. Apply transition scaling
        is_transition = detect_regime_transition(regime_probs)
        weights = apply_transition_scaling(
            weights, is_transition, self._cfg.regime.transition_decay
        )

        # 4. Run all sub-strategies independently
        all_signals = {
            key: strat.signals(df) for key, strat in self._strategies.items()
        }
        entry_signals: dict[str, pd.Series] = {  # type: ignore[type-arg]
            k: sigs[0] for k, sigs in all_signals.items()
        }
        exit_signals: dict[str, pd.Series] = {  # type: ignore[type-arg]
            k: sigs[1] for k, sigs in all_signals.items()
        }

        # 5. Performance + volatility weight adjustments (SYSTEM.md §5.5)
        wa_cfg = self._cfg.weight_adj
        perf_factors = compute_performance_factors(close, entry_signals, wa_cfg)
        vol_scale = compute_volatility_scaling(close, wa_cfg)
        weights = apply_weight_adjustments(weights, perf_factors, vol_scale)

        # 6. Weighted voting + thresholds
        entry_vote = _weighted_vote(weights, entry_signals)
        exit_vote = _weighted_vote(weights, exit_signals)

        entries = (entry_vote >= self._cfg.entry_threshold).fillna(False).astype(bool)
        exits = (exit_vote >= self._cfg.exit_threshold).fillna(False).astype(bool)

        return entries, exits


register_strategy('regime_switcher', RegimeSwitcher)
