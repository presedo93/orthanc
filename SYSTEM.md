# Adaptive Strategy Switching by Market Regime

## Technical Specification (Focused on Regime → Strategy Logic)

---

## 1. Objective

Design a system that dynamically **switches and weights trading strategies based on the current market regime**.

Core goals:

- Detect market regime robustly
- Map regimes to appropriate strategies
- Allocate capital using **probabilistic weighting (not hard switching)**
- Ensure smooth transitions between regimes
- Maximize robustness and survival

---

## 2. Core Principle

Avoid:

- hard switches
- binary logic

Use instead:

- **probabilistic regimes**
- **weighted strategies**
- **continuous transitions**

---

## 3. Regime Model

### 3.1 Regime Set

Initial regimes:

- TREND_UP
- TREND_DOWN
- SIDEWAYS
- HIGH_VOLATILITY
- LOW_ACTIVITY

---

### 3.2 Regime Output

The detector must output probabilities:

```
{
  TREND_UP: 0.60,
  TREND_DOWN: 0.05,
  SIDEWAYS: 0.25,
  HIGH_VOLATILITY: 0.08,
  LOW_ACTIVITY: 0.02
}
```

---

### 3.3 Regime Features

#### Trend

- EMA slope
- ADX
- HH/LL structure

#### Volatility

- ATR
- realized volatility
- Bollinger width

#### Activity

- volume ratio
- range compression

---

## 4. Strategy Definitions

Each strategy must be:

- independent
- specialized
- regime-aware

---

### 4.1 Trend Following Strategy

**Applicable Regimes:**

- TREND_UP
- TREND_DOWN

**Logic:**

- Enter on breakout or pullback
- Use EMA alignment or structure confirmation

**Entry:**

- Break previous high (long)
- Break previous low (short)

**Exit:**

- ATR trailing stop
- structure break

**Strength Signal:**

- distance from breakout
- trend strength (ADX)

---

### 4.2 Mean Reversion Strategy

**Applicable Regimes:**

- SIDEWAYS

**Logic:**

- trade deviations from mean

**Entry:**

- buy lower Bollinger band
- sell upper Bollinger band

**Exit:**

- return to mean
- mid-band

**Strength Signal:**

- z-score
- distance from mean

---

### 4.3 Volatility Breakout Strategy

**Applicable Regimes:**

- HIGH_VOLATILITY (especially transitions)

**Logic:**

- enter after volatility expansion

**Entry:**

- break of compression range

**Exit:**

- short-term momentum fade
- ATR stop

---

### 4.4 Defensive / No-Trade Strategy

**Applicable Regimes:**

- LOW_ACTIVITY
- unstable conditions

**Logic:**

- reduce exposure
- avoid trades

---

## 5. Strategy Weighting Engine

### 5.1 Core Idea

Each strategy gets a weight based on:

- regime probabilities
- strategy suitability
- optional performance

---

### 5.2 Base Mapping Matrix

| Regime           | Trend | MeanRev | VolBreak | Defensive |
|------------------|------|---------|----------|-----------|
| TREND_UP         | 1.0  | 0.1     | 0.3      | 0.0       |
| TREND_DOWN       | 1.0  | 0.1     | 0.3      | 0.0       |
| SIDEWAYS         | 0.2  | 1.0     | 0.2      | 0.1       |
| HIGH_VOLATILITY  | 0.3  | 0.2     | 1.0      | 0.2       |
| LOW_ACTIVITY     | 0.0  | 0.1     | 0.0      | 1.0       |

---

### 5.3 Weight Calculation

```
strategy_weight =
    sum_over_regimes(
        regime_probability * mapping_value
    )
```

Example:

```
trend_weight =
    0.6 * 1.0 +
    0.25 * 0.2 +
    ...
```

---

### 5.4 Normalization

After computing weights:

```
normalize(weights)
```

---

### 5.5 Optional Enhancements

#### Performance Adjustment

```
weight *= performance_factor
```

#### Volatility Scaling

```
weight *= volatility_adjustment
```

---

## 6. Strategy Selection Logic

### 6.1 Soft Allocation

- multiple strategies can be active
- capital distributed proportionally

---

### 6.2 Signal Combination

Option A:

- weighted signals

Option B (recommended):

- independent strategies
- unified risk layer

---

## 7. Regime Transition Handling

### 7.1 Problem

Regime switching causes losses.

---

### 7.2 Solution

#### Hysteresis

- require persistence before switching

#### Smoothing

```
smoothed_regime = EMA(regime_probabilities)
```

#### Transition Mode

- temporarily reduce all weights

```
if regime_change_detected:
    scale_all_weights(0.5)
```

---

## 8. Full Decision Flow

```
features = compute_features(data)

regime_probs = detect_regime(features)

weights = compute_weights(regime_probs)

signals = []
for strategy:
    signals.append(strategy.signal())

final_positions = risk_manager(signals, weights)

execute(final_positions)
```

---

## 9. Key Design Rules

- never hard switch instantly
- always use probabilities
- allow multiple strategies
- explicitly allow "no trade"
- smooth transitions
- prioritize stability over reactivity

---

## 10. Summary

This system:

- detects market regime probabilistically
- maps regimes to strategies
- assigns weights dynamically
- combines strategies under one portfolio
- minimizes regime-switch risk

This is the foundation for a robust adaptive trading system.
