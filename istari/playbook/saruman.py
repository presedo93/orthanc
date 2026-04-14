"""Regime-based multi-strategy switcher — Saruman of Many Colours.

Technical name: Regime Switcher (regime_switcher.py).

Detects the current market regime probabilistically using a Gaussian
Hidden Markov Model and dynamically weights sub-strategies accordingly.
Implements the full decision flow from SYSTEM.md: features → regime
detection → weight computation → signal combination → final positions.
Saruman sees all colours of the spectrum and shifts between them — like
the adaptive regime detector.

Key design principles:
- Probabilistic regimes via HMM posterior inference (no hard switches)
- Weighted strategies (soft allocation)
- Learned transitions (HMM transition matrix captures regime dynamics)
- Multiple strategies can be active simultaneously
"""

from dataclasses import dataclass
from enum import Enum

import numpy as np
import pandas as pd
from hmmlearn.hmm import GaussianHMM  # type: ignore[import-untyped]

from ..council_of_wizards import Istari, ordain_istari
from ..lore import (
    adx,
    atr_ratio,
    bollinger_width,
    ema_slope,
    higher_highs_lower_lows,
    range_compression,
    realized_volatility,
    volume_ratio,
)
from ..scrolls import (
    Direction,
    RealmConfig,
    SarumanConfig,
    Scroll,
    WeightSpell,
)
from .aragorn import Aragorn
from .shadowfax import Shadowfax
from .shelob import Shelob
from .treebeard import Treebeard

# ---------------------------------------------------------------------------
# Regime types
# ---------------------------------------------------------------------------


class RegimeType(str, Enum):
    """Market regime classification.

    Technical names:
    - AGE_OF_KINGS = TRENDING (directional movement, up or down)
    - LONG_PEACE = MEAN_REVERTING (range-bound, oscillating)
    - WAR_OF_THE_RING = VOLATILE (expansion, chaos, reduce exposure)
    """

    AGE_OF_KINGS = 'trending'
    LONG_PEACE = 'mean_reverting'
    WAR_OF_THE_RING = 'volatile'


# ---------------------------------------------------------------------------
# Regime → strategy mapping matrix (from SYSTEM.md §5.2)
# ---------------------------------------------------------------------------

#: Strategy keys used throughout the module — order matters for matrix columns.
FELLOWSHIP_KEYS = ('aragorn', 'treebeard', 'shadowfax', 'shelob')

#: Mapping matrix as a DataFrame for vectorized weight computation.
#: Rows = regimes, columns = strategy keys.
REGIME_STRATEGY_MATRIX = pd.DataFrame(
    {
        'aragorn': [1.0, 0.2, 0.3],
        'treebeard': [0.1, 1.0, 0.2],
        'shadowfax': [0.3, 0.2, 1.0],
        'shelob': [0.0, 0.1, 0.2],
    },
    index=[
        RegimeType.AGE_OF_KINGS,
        RegimeType.LONG_PEACE,
        RegimeType.WAR_OF_THE_RING,
    ],
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FeatureScaler:
    """Z-score scaler for feature standardization.

    Technical name: StandardScaler — replaces sklearn dependency with
    a minimal frozen dataclass holding fitted mean and scale arrays.
    """

    mean: np.ndarray
    scale: np.ndarray

    @classmethod
    def fit(cls, X: np.ndarray) -> FeatureScaler:
        """Fit scaler on observation matrix.

        Args:
            X: Feature matrix of shape (n_samples, n_features).

        Returns:
            Fitted FeatureScaler instance.
        """
        mean = X.mean(axis=0)
        scale = X.std(axis=0)
        # Prevent division by zero for constant features
        scale = np.where(scale == 0.0, 1.0, scale)
        return cls(mean=mean, scale=scale)

    def transform(self, X: np.ndarray) -> np.ndarray:
        """Standardize features using fitted mean and scale.

        Args:
            X: Feature matrix of shape (n_samples, n_features).

        Returns:
            Standardized feature matrix.
        """
        return (X - self.mean) / self.scale


def _normalize_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize each row to sum to 1.0, treating all-zero rows as uniform.

    Args:
        df: DataFrame with non-negative values.

    Returns:
        Row-normalized DataFrame.
    """
    row_sums = df.sum(axis=1).replace(0.0, 1.0)
    return df.div(row_sums, axis=0)


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
# Feature computation — pure functions
# ---------------------------------------------------------------------------


def compute_regime_features(df: pd.DataFrame, cfg: RealmConfig) -> pd.DataFrame:
    """Compute all features needed for regime detection.

    These features become the HMM observation vectors. Each row is a
    multi-dimensional observation at one time step.

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


def _prepare_observations(features: pd.DataFrame) -> tuple[np.ndarray, FeatureScaler]:
    """Standardize feature matrix for HMM fitting.

    Z-score normalizes all features so the Gaussian emission model
    treats each feature dimension equally regardless of scale.

    Args:
        features: Raw feature DataFrame from compute_regime_features.

    Returns:
        Tuple of (standardized observation matrix, fitted scaler).
    """
    clean = features.dropna()
    scaler = FeatureScaler.fit(clean.values)
    X = scaler.transform(clean.values)
    return X, scaler


# ---------------------------------------------------------------------------
# HMM regime detection — pure functions
# ---------------------------------------------------------------------------


def _build_sticky_transmat(n_regimes: int, stickiness: float) -> np.ndarray:
    """Build a transition matrix with high self-transition probability.

    Creates a matrix where each state prefers to stay in the same
    state with probability ``stickiness``, and transitions uniformly
    to other states with the remaining probability mass.

    Args:
        n_regimes: Number of hidden states.
        stickiness: Self-transition probability (e.g. 0.95).

    Returns:
        Transition matrix of shape (n_regimes, n_regimes).
    """
    off_diag = (1.0 - stickiness) / max(n_regimes - 1, 1)
    transmat = np.full((n_regimes, n_regimes), off_diag)
    np.fill_diagonal(transmat, stickiness)
    return transmat


def fit_regime_model(
    features: pd.DataFrame,
    cfg: RealmConfig,
) -> tuple[GaussianHMM, FeatureScaler, dict[int, RegimeType]]:
    """Fit a Gaussian HMM to market feature observations.

    Trains multiple models with different random seeds and selects
    the one with the highest log-likelihood. The transition matrix
    is initialized with high self-transition probability (sticky
    regimes) but is allowed to be refined by EM.

    Also computes the regime label mapping from the fitted model's
    emission means, so it can be cached and reused without
    recomputing on every prediction call.

    Args:
        features: Feature DataFrame from compute_regime_features.
        cfg: Regime detection configuration.

    Returns:
        Tuple of (fitted GaussianHMM, fitted FeatureScaler, label mapping).
    """
    X, scaler = _prepare_observations(features)
    transmat_init = _build_sticky_transmat(cfg.n_regimes, cfg.transition_stickiness)

    best_score = -np.inf
    best_model: GaussianHMM | None = None

    for seed in range(cfg.hmm_n_fits):
        model = GaussianHMM(
            n_components=cfg.n_regimes,
            covariance_type=cfg.covariance_type,
            n_iter=cfg.hmm_n_iter,
            tol=cfg.hmm_tol,
            random_state=seed,
            init_params='smc',
            params='stmc',
            implementation='log',
        )
        # Seed with sticky transition matrix, let EM refine it
        model.transmat_ = transmat_init.copy()

        model.fit(X)
        score = model.score(X)

        if score > best_score:
            best_score = score
            best_model = model

    assert best_model is not None  # noqa: S101 — at least one fit ran

    label_map = _label_regimes(best_model, features)
    return best_model, scaler, label_map


def _label_regimes(
    model: GaussianHMM,
    features: pd.DataFrame,
) -> dict[int, RegimeType]:
    """Map HMM state indices to semantic RegimeType labels.

    The HMM assigns arbitrary integer labels (0, 1, 2) to states.
    This function inspects the learned emission means to assign
    meaningful regime labels based on feature characteristics:

    - TRENDING: highest absolute EMA slope + highest ADX
    - VOLATILE: highest ATR ratio + highest Bollinger width
    - MEAN_REVERTING: whatever is left (low trend, low volatility)

    Args:
        model: Fitted GaussianHMM.
        features: Original feature DataFrame (for column names).

    Returns:
        Dict mapping HMM state index to RegimeType.
    """
    means = model.means_  # shape (n_components, n_features)
    col_names = list(features.dropna().columns)

    slope_idx = col_names.index('ema_slope')
    adx_idx = col_names.index('adx')
    atr_idx = col_names.index('atr_ratio')
    boll_idx = col_names.index('boll_width')

    n_states = means.shape[0]

    # Score each state for "trendiness" and "volatility"
    trend_scores = np.abs(means[:, slope_idx]) + means[:, adx_idx]
    vol_scores = means[:, atr_idx] + means[:, boll_idx]

    # Assign labels greedily: highest trend → TRENDING, highest vol → VOLATILE
    assigned: dict[int, RegimeType] = {}
    remaining = set(range(n_states))

    trend_state = int(np.argmax(trend_scores))
    assigned[trend_state] = RegimeType.AGE_OF_KINGS
    remaining.discard(trend_state)

    # Among remaining, highest volatility → VOLATILE
    vol_candidates = list(remaining)
    vol_state = vol_candidates[int(np.argmax(vol_scores[vol_candidates]))]
    assigned[vol_state] = RegimeType.WAR_OF_THE_RING
    remaining.discard(vol_state)

    # Whatever is left → MEAN_REVERTING
    for s in remaining:
        assigned[s] = RegimeType.LONG_PEACE

    return assigned


def detect_regime_probabilities(
    features: pd.DataFrame,
    model: GaussianHMM,
    scaler: FeatureScaler,
    label_map: dict[int, RegimeType],
) -> pd.DataFrame:
    """Detect market regime as probability distribution per bar.

    Uses the fitted HMM's forward-backward algorithm to compute
    posterior state probabilities. Columns are mapped to semantic
    RegimeType labels using the pre-computed label mapping.

    Args:
        features: DataFrame from compute_regime_features.
        model: Fitted GaussianHMM.
        scaler: Fitted FeatureScaler.
        label_map: Mapping from HMM state index to RegimeType
            (computed once at fit time by fit_regime_model).

    Returns:
        DataFrame with columns for each RegimeType, values in [0, 1],
        rows summing to 1.0. NaN rows (from warmup) get uniform probs.
    """
    clean = features.dropna()
    X = scaler.transform(clean.values)
    posteriors = model.predict_proba(X)

    regime_probs = pd.DataFrame(
        {label_map[i]: posteriors[:, i] for i in range(model.n_components)},
        index=clean.index,
    )

    # Reindex to full original index, fill warmup NaNs with uniform
    regime_probs = regime_probs.reindex(features.index)
    uniform = 1.0 / model.n_components
    regime_probs = regime_probs.fillna(uniform)

    # Ensure consistent column order
    return regime_probs[  # type: ignore[return-value]
        [RegimeType.AGE_OF_KINGS, RegimeType.LONG_PEACE, RegimeType.WAR_OF_THE_RING]
    ]


def detect_regime_transition(
    regime_probs: pd.DataFrame,
    threshold: float = 0.15,
) -> pd.Series:  # type: ignore[type-arg]
    """Detect regime transitions for scaling weights during unstable periods.

    A transition is detected when the dominant regime changes or when
    the probability mass shifts significantly between bars.

    Args:
        regime_probs: Regime probability DataFrame.
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
    cfg: WeightSpell,
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
    cfg: WeightSpell,
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
# Saruman — multi-strategy orchestrator
# ---------------------------------------------------------------------------


class Saruman(Istari):
    """Adaptive multi-strategy switcher based on HMM regime detection.

    Technical name: RegimeSwitcher — regime-based multi-strategy orchestrator.

    Detects the current market regime using a Gaussian Hidden Markov Model
    fitted on technical features, computes probabilistic weights for each
    sub-strategy, runs all sub-strategies independently, and combines their
    signals using weighted voting.

    The HMM must be fitted before signals can be generated. Call
    ``fit(df)`` with historical data, or pass a pre-fitted model via
    ``regime_model`` and ``regime_scaler``.

    Args:
        switcher_config: Full switcher configuration. Individual params
            below override their corresponding config fields when both
            are provided (for backward compatibility with the registry).
        direction: Trading direction.
        regime_model: Pre-fitted GaussianHMM (skip fitting if provided).
        regime_scaler: Pre-fitted FeatureScaler (required with regime_model).
        regime_labels: Pre-computed label mapping (required with regime_model).
    """

    def __init__(
        self,
        switcher_config: SarumanConfig | None = None,
        direction: Direction = Direction.WESTWARD,
        *,
        regime_model: GaussianHMM | None = None,
        regime_scaler: FeatureScaler | None = None,
        regime_labels: dict[int, RegimeType] | None = None,
        # Flat overrides for registry compatibility (summon_istari passes
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
        regime_config: RealmConfig | None = None,
        weight_adj_config: WeightSpell | None = None,
        **_kwargs: object,
    ) -> None:
        # Build config from explicit object or from flat kwargs
        if switcher_config is not None:
            cfg = switcher_config
        else:
            cfg = SarumanConfig(
                regime=regime_config or RealmConfig(),
                weight_adj=weight_adj_config or WeightSpell(),
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
        self._regime_model = regime_model
        self._regime_scaler = regime_scaler
        self._regime_labels = regime_labels

        # Build sub-strategies from config
        self._strategies: dict[str, Istari] = {
            'aragorn': Aragorn(
                fast_window=cfg.trend_fast_window,
                slow_window=cfg.trend_slow_window,
                atr_multiplier=cfg.trend_atr_multiplier,
                adx_period=cfg.trend_adx_period,
                direction=direction,
            ),
            'treebeard': Treebeard(
                period=cfg.mean_rev_period,
                num_std=cfg.mean_rev_num_std,
                direction=direction,
            ),
            'shadowfax': Shadowfax(
                compression_period=cfg.vol_breakout_compression_period,
                atr_multiplier=cfg.vol_breakout_atr_multiplier,
                direction=direction,
            ),
            'shelob': Shelob(direction=direction),
        }

    @property
    def is_fitted(self) -> bool:
        """Whether the HMM regime model has been fitted."""
        return (
            self._regime_model is not None
            and self._regime_scaler is not None
            and self._regime_labels is not None
        )

    def config(self) -> Scroll:
        """Return strategy configuration from instance state."""
        return Scroll(
            name='saruman',
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

    def fit(self, df: pd.DataFrame) -> Saruman:
        """Fit the HMM regime model on historical data.

        Computes features from OHLCV data and trains the Gaussian HMM.
        Must be called before ``signals()`` or ``detect_regime()``
        unless a pre-fitted model was passed to the constructor.

        Args:
            df: OHLCV DataFrame with sufficient history for feature warmup.

        Returns:
            Self, for method chaining.
        """
        features = compute_regime_features(df, self._cfg.regime)
        self._regime_model, self._regime_scaler, self._regime_labels = fit_regime_model(
            features, self._cfg.regime
        )
        return self

    def detect_regime(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect regime probabilities for the given data.

        Convenience method that runs feature computation and HMM
        posterior inference.

        Args:
            df: OHLCV DataFrame.

        Returns:
            Regime probability DataFrame.

        Raises:
            RuntimeError: If the model has not been fitted.
        """
        if not self.is_fitted:
            msg = 'Regime model not fitted. Call fit() first or provide a pre-fitted model.'
            raise RuntimeError(msg)

        assert self._regime_model is not None  # noqa: S101
        assert self._regime_scaler is not None  # noqa: S101
        assert self._regime_labels is not None  # noqa: S101

        features = compute_regime_features(df, self._cfg.regime)
        return detect_regime_probabilities(
            features, self._regime_model, self._regime_scaler, self._regime_labels
        )

    def signals(self, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:  # type: ignore[type-arg]
        """Generate adaptive multi-strategy entry/exit signals.

        Full decision flow:
        1. Fit HMM if not already fitted (auto-fit on first call)
        2. Detect regime probabilities via HMM posterior
        3. Compute strategy weights from regime
        4. Apply transition scaling
        5. Run all sub-strategies independently
        6. Apply performance + volatility weight adjustments (§5.5)
        7. Combine signals via weighted voting + thresholds

        Args:
            df: OHLCV DataFrame with 'open', 'high', 'low', 'close', 'volume'.

        Returns:
            Tuple of (entries, exits) as boolean Series.
        """
        close: pd.Series = df['close']  # type: ignore[assignment]

        # 1. Auto-fit if needed
        if not self.is_fitted:
            self.fit(df)

        # 2. Detect regime
        regime_probs = self.detect_regime(df)

        # 3. Compute strategy weights
        weights = compute_strategy_weights(regime_probs)

        # 4. Apply transition scaling
        is_transition = detect_regime_transition(regime_probs)
        weights = apply_transition_scaling(
            weights, is_transition, self._cfg.regime.transition_decay
        )

        # 5. Run all sub-strategies independently
        all_signals = {
            key: strat.signals(df) for key, strat in self._strategies.items()
        }
        entry_signals: dict[str, pd.Series] = {  # type: ignore[type-arg]
            k: sigs[0] for k, sigs in all_signals.items()
        }
        exit_signals: dict[str, pd.Series] = {  # type: ignore[type-arg]
            k: sigs[1] for k, sigs in all_signals.items()
        }

        # 6. Performance + volatility weight adjustments (SYSTEM.md §5.5)
        wa_cfg = self._cfg.weight_adj
        perf_factors = compute_performance_factors(close, entry_signals, wa_cfg)
        vol_scale = compute_volatility_scaling(close, wa_cfg)
        weights = apply_weight_adjustments(weights, perf_factors, vol_scale)

        # 7. Weighted voting + thresholds
        entry_vote = _weighted_vote(weights, entry_signals)
        exit_vote = _weighted_vote(weights, exit_signals)

        entries = (entry_vote >= self._cfg.entry_threshold).fillna(False).astype(bool)
        exits = (exit_vote >= self._cfg.exit_threshold).fillna(False).astype(bool)

        return entries, exits


ordain_istari('saruman', Saruman)
