## Purpose of this document

This document defines the core logic, architectural principles, domain concepts, simulation goals, rule modeling approach, metrics, and implementation priorities for a **Python** system intended to be run and explored from **marimo notebooks**, whose main objective is to evaluate and optimize the **survival** of trading strategies under **prop trading firm** constraints.

This version intentionally removes the detailed file and folder layout design and focuses on the **logic and behavior** the system must implement.

The priorities are:

- architectural clarity,
- strict separation of responsibilities,
- extensible code,
- testability,
- reproducibility,
- ease of adding new firms, strategies, rules, instruments, and experiments.

---

# 1. Problem to solve

The problem is not simply to backtest strategies to find which one makes the most money. The real objective is to model the following situation:

- the user buys an evaluation or funded account from a prop firm,
- trades a specific strategy,
- is subject to hard constraints:
  - maximum daily loss,
  - maximum total loss,
  - trailing drawdown,
  - profit targets,
  - minimum trading days,
  - possible consistency restrictions,
  - possible operational restrictions,
- and wants to maximize:
  - the probability of **not breaking rules**,
  - the probability of **passing the evaluation**,
  - the probability of **staying alive while funded**,
  - the expected number of positive days,
  - the expected number of days survived,
  - net profitability adjusted for the real cost of accounts, resets, and payouts.

In other words, the system must not answer only “does this strategy make money?”, but questions like:

- does this strategy survive?
- how long does it survive?
- why does it fail?
- which parameter improves survival?
- which firm is more favorable for this strategy?
- which sizing is best if the main objective is longevity?
- which combination of operational filters improves survival rate without completely destroying edge?
- what is the expected cost of achieving a stable funded account with this strategy?

---

# 2. System philosophy

The system must be built with a **survival-first** philosophy.

## 2.1 What the simulator should not be

It should not be just:

- a classic PnL-oriented backtester,
- a Sharpe optimizer,
- a win-rate comparator,
- a framework coupled to a single firm,
- a giant notebook with scattered logic,
- a set of unstructured scripts.

## 2.2 What it should be

It should be:

- a modular system,
- a constraint-oriented simulator,
- a survival evaluation engine,
- a reproducible experimentation environment,
- an extensible base for real prop firms,
- a framework capable of separating:
  - strategy,
  - execution,
  - risk management,
  - firm rules,
  - simulation,
  - reporting.

## 2.3 Core intuition

In prop trading, most strategies do not fail because they have purely negative expected value, but because:

- the daily limit is too tight,
- an early losing streak destroys the account,
- trailing drawdown makes a high-variance strategy unviable,
- the order of trades matters a lot,
- the strategy may be profitable on average but unviable under hard constraints.

Therefore, the simulator must treat the account as a process with **absorbing barriers**:

- if a rule is violated, the account dies,
- the temporal order of trades is crucial,
- robustness matters more than aggressiveness,
- operational edge includes both the signal and the survival rules.

---

# 3. Functional goals of the system

## 3.1 Main objectives

The system must allow:

1. loading historical data from an existing user module,
2. normalizing those data and exposing them to strategies,
3. executing strategies on those data,
4. converting signals into executed trades,
5. applying costs and slippage,
6. simulating an account under prop firm rules,
7. determining whether the account is still alive or has failed,
8. recording the exact reason for failure,
9. measuring survival metrics and classical metrics,
10. running multiple kinds of simulations:

- historical,
- rolling,
- bootstrap,
- Monte Carlo,
- stress tests,

11. comparing results across:

- strategies,
- parameters,
- firms,
- instruments,
- risk profiles,

12. producing visualizations and summaries usable from marimo.

## 3.2 Secondary objectives

Additionally, the system should be ready to:

- model two-phase evaluations,
- model funded accounts,
- model payouts,
- model real account economics,
- simulate portfolios of accounts,
- support different data types:
  - CFDs,
  - futures,
  - spot,
  - OHLCV,
  - bid/ask if available,
  - multiple bar timeframes,
- incorporate new rules without rewriting the core.

---

# 4. Key use cases

## 4.1 Basic validation of a strategy

The user wants to:

- choose an instrument,
- choose a date range,
- choose a strategy,
- choose a firm rule set,
- and see a single backtest with full traceability.

Expected output:

- trade log,
- equity curve,
- daily ledger,
- exact reason of death if it fails,
- summary metrics.

## 4.2 Evaluate survival under different risk-per-trade settings

The user wants to:

- keep the strategy fixed,
- vary `risk_per_trade`,
- run hundreds or thousands of simulations,
- compare survival probability.

Expected output:

- comparison table,
- heatmap,
- survival curve,
- failure-cause breakdown.

## 4.3 Compare multiple prop firms

The user wants to:

- run the same strategy,
- with the same sizing,
- on the same data,
- but under different firm rules.

Expected output:

- firm ranking,
- survival metrics by firm,
- trailing DD impact,
- minimum trading days impact,
- economics by firm.

## 4.4 Estimate expected cost of getting a stable funded account

The user wants to know:

- how many accounts they can expect to burn,
- the expected total cost incurred,
- how many resets would be expected,
- the net EV of the process.

Expected output:

- expected cost to funded,
- expected cost to first payout,
- EV per euro spent,
- distribution of attempts.

## 4.5 Multi-account simulation

The user wants to run several accounts, in parallel or in sequence, and measure:

- probability that at least one survives,
- probability that several survive,
- expected cost to reach a stable funded account.

---

# 5. Software design principles

## 5.1 Strict layer separation

Each module must have clear responsibilities. The system must not mix:

- strategy with firm rules,
- execution with reporting,
- configuration with logic,
- notebooks with core logic,
- simulation with visualization.

## 5.2 Extensibility

It must be easy to add:

- new strategies,
- new firm profiles,
- new rules,
- new fill models,
- new experiments,
- new reports.

## 5.3 Reproducibility

Every simulation must be repeatable exactly if the following are known:

- the random seed,
- the configuration,
- the code version,
- the dataset,
- the experiment definition.

## 5.4 Testability

Prop firm rules and account logic must be highly testable with deterministic scenarios.

## 5.5 Strategy neutrality

The simulator core must not assume a particular strategy style. It must be able to work with:

- trend following,
- mean reversion,
- breakout,
- scalping,
- session-based,
- formally codified discretionary rule-based trading.

## 5.6 Notebooks as interface, not as core

Marimo notebooks must act as:

- exploration panels,
- orchestrators,
- dashboards,
- experiment comparators,

while the main logic must live in an importable Python package.

---

# 6. Functional view by layer

## 6.1 Data layer

Responsibility:

- connect to the user’s existing module,
- download CFDs, futures, or other instruments,
- normalize them into a uniform structure,
- clean basic issues,
- enrich them with additional information:
  - sessions,
  - spreads,
  - ATR,
  - regime labels,
  - RTH / ETH flags,
- return `DataFrame` objects ready for strategies and simulation.

The data layer must know nothing about:

- prop firm rules,
- sizing,
- Monte Carlo,
- survival metrics.

## 6.2 Strategy layer

Responsibility:

- generate features,
- produce signals,
- propose entries and exits,
- optionally propose stop/take/size hints.

The strategy must know nothing about:

- daily drawdown,
- trailing DD,
- payout rules,
- challenge economics.

## 6.3 Execution layer

Responsibility:

- convert signals into executed trades,
- determine fills,
- apply slippage,
- apply fees,
- maintain open positions,
- close trades by:
  - stop,
  - take,
  - time exit,
  - opposite signal,
  - end of session,
  - operational restriction.

## 6.4 Risk layer

Responsibility:

- apply the trader’s internal controls, independent from the firm:
  - risk per trade,
  - max trades per day,
  - internal daily stop,
  - daily profit lock,
  - cooldown after losses,
  - drawdown-based de-risking,
  - volatility filters,
  - session filters.

## 6.5 Prop firm layer

Responsibility:

- apply external constraints imposed by the firm:
  - daily loss,
  - max loss,
  - trailing DD,
  - target,
  - minimum days,
  - consistency rules,
  - holding restrictions,
  - time restrictions,
  - phase changes between evaluation, funded, and payout.

## 6.6 Simulation layer

Responsibility:

- orchestrate individual and batch runs,
- run single simulations,
- run rolling windows,
- bootstrap,
- Monte Carlo,
- stress tests,
- multi-account simulation,
- aggregate results.

## 6.7 Metrics layer

Responsibility:

- compute classical performance,
- survival metrics,
- robustness metrics,
- prop compliance metrics,
- economic metrics.

## 6.8 Reporting layer

Responsibility:

- tables,
- charts,
- summaries,
- result serialization,
- utilities for marimo visualization.

---

# 7. Domain conceptual model

## 7.1 High-level flow

The full system flow should be:

1. An experiment configuration is defined.
2. Market data are loaded.
3. Features are generated.
4. Signals are generated.
5. The execution engine processes signals and data.
6. Account state is updated bar by bar or event by event.
7. The prop firm rule evaluator checks for breaches.
8. Trades, daily ledger rows, and risk events are recorded.
9. If the account fails, the terminal state is marked.
10. If it does not fail and meets objectives, it may move to the next phase.
11. Metrics are computed.
12. If it is an aggregate simulation, the process is repeated many times.
13. Distributions are aggregated and visualized.

---

# 8. Domain entities

## 8.1 `MarketData`

Represents normalized market data.

Recommended minimum fields:

- `timestamp`
- `symbol`
- `venue`
- `timeframe`
- `open`
- `high`
- `low`
- `close`
- `volume`

Optional fields:

- `bid`
- `ask`
- `mid`
- `spread`
- `open_interest`
- `session_id`
- `is_rth`
- `is_news_window`
- `regime_label`
- `atr`
- `volatility_bucket`

Notes:

- the system must tolerate datasets that only provide OHLCV,
- if `bid/ask` are unavailable, spread may be modeled synthetically,
- if `session_id` is unavailable, it should be derivable.

## 8.2 `Signal`

Represents a trading intent generated by a strategy.

Suggested fields:

- `timestamp`
- `symbol`
- `side` (`long`, `short`, `flat`)
- `entry_type`
- `entry_price_hint`
- `stop_price`
- `take_price`
- `time_exit_bars`
- `confidence`
- `tag`
- `metadata`

## 8.3 `OrderIntent`

Represents an order proposed to the execution engine.

Fields:

- `timestamp`
- `symbol`
- `side`
- `qty_hint`
- `order_type`
- `stop_price`
- `limit_price`
- `reason`
- `strategy_id`

## 8.4 `Position`

State of an open position.

Fields:

- `symbol`
- `side`
- `entry_time`
- `entry_price`
- `qty`
- `stop_price`
- `take_price`
- `time_exit_bars`
- `bars_open`
- `unrealized_pnl`
- `mae`
- `mfe`

## 8.5 `Trade`

Represents a closed trade.

Fields:

- `trade_id`
- `symbol`
- `entry_time`
- `exit_time`
- `entry_price`
- `exit_price`
- `qty`
- `side`
- `gross_pnl`
- `fees`
- `slippage_cost`
- `net_pnl`
- `mae`
- `mfe`
- `holding_bars`
- `exit_reason`
- `strategy_id`
- `session_id`
- `day_id`

## 8.6 `DailyLedgerRow`

Represents the summary of a trading session or day.

Fields:

- `date`
- `start_balance`
- `end_balance`
- `equity_close`
- `realized_pnl`
- `unrealized_pnl_close`
- `max_intraday_loss`
- `max_intraday_drawdown_pct`
- `trades_count`
- `wins_count`
- `losses_count`
- `green_day`
- `red_day`
- `flat_day`
- `near_rule_limit`
- `rule_warning_flags`
- `stopped_by_internal_risk`
- `stopped_by_firm_rule`

## 8.7 `AccountState`

Represents the current account state during a simulation.

Recommended fields:

- `account_id`
- `starting_balance`
- `current_balance`
- `equity`
- `peak_balance`
- `peak_equity`
- `daily_start_balance`
- `daily_start_equity`
- `daily_realized_pnl`
- `daily_unrealized_pnl`
- `daily_total_pnl`
- `realized_pnl_total`
- `unrealized_pnl_total`
- `drawdown_abs`
- `drawdown_pct`
- `open_positions`
- `trading_days_count`
- `green_days_count`
- `red_days_count`
- `flat_days_count`
- `consecutive_losses`
- `consecutive_wins`
- `alive`
- `failure_reason`
- `failure_timestamp`
- `phase`
- `rule_breaches`
- `warnings`
- `eligible_for_payout`
- `evaluation_passed`

## 8.8 `PropFirmRuleSet`

Describes the rules of a specific firm or account plan.

Suggested fields:

- `firm_name`
- `plan_name`
- `account_size`
- `profit_target_abs`
- `profit_target_pct`
- `max_daily_loss_abs`
- `max_daily_loss_pct`
- `max_total_loss_abs`
- `max_total_loss_pct`
- `trailing_drawdown_mode`
- `trailing_drawdown_abs`
- `trailing_drawdown_pct`
- `trailing_stops_at_breakeven`
- `min_trading_days`
- `consistency_rule_enabled`
- `consistency_threshold_pct`
- `overnight_holding_allowed`
- `weekend_holding_allowed`
- `news_trading_allowed`
- `max_position_size`
- `allowed_sessions`
- `phase_type`
- `reset_fee`
- `evaluation_fee`
- `activation_fee`
- `profit_split_pct`
- `payout_waiting_days`
- `payout_buffer_rule`
- `daily_loss_reference_mode`
- `total_loss_reference_mode`

## 8.9 `SimulationResult`

Full result of a single simulation.

Fields:

- `run_id`
- `experiment_id`
- `strategy_name`
- `firm_name`
- `instrument`
- `timeframe`
- `config_snapshot`
- `survived_to_end`
- `passed_evaluation`
- `reached_funded`
- `eligible_for_payout`
- `failure_reason`
- `failure_timestamp`
- `days_survived`
- `calendar_days_elapsed`
- `green_days`
- `red_days`
- `flat_days`
- `net_pnl`
- `gross_pnl`
- `fees_total`
- `slippage_total`
- `max_drawdown_abs`
- `max_drawdown_pct`
- `max_daily_loss_seen`
- `longest_losing_streak`
- `profit_factor`
- `expectancy`
- `trade_count`
- `daily_ledger`
- `trade_log`
- `equity_curve`
- `state_transitions`

## 8.10 `AggregateSimulationResult`

Aggregated result of many runs.

Fields:

- `experiment_id`
- `n_runs`
- `pass_rate`
- `survival_rate_end`
- `survival_probability_day_5`
- `survival_probability_day_10`
- `survival_probability_day_20`
- `survival_probability_day_30`
- `median_days_survived`
- `mean_days_survived`
- `days_survived_p05`
- `days_survived_p95`
- `pct_fail_daily_loss`
- `pct_fail_total_loss`
- `pct_fail_trailing_dd`
- `pct_fail_rule_breach`
- `pct_hit_profit_target`
- `pct_reached_min_trading_days`
- `expected_green_days`
- `expected_red_days`
- `avg_net_pnl`
- `median_net_pnl`
- `p05_net_pnl`
- `p95_net_pnl`
- `avg_max_drawdown`
- `hazard_curve`
- `survival_curve`
- `failure_breakdown`
- `economics_summary`

---

# 9. Prop firm rules: detailed modeling

This part is critical and must be implemented with care.

## 9.1 General principle

Firm rules must be treated as **hard constraints** operating on account state. The simulator must be able to evaluate rules at:

- intrabar level,
- end of bar,
- end of trade,
- end of day,
- phase transition.

## 9.2 Daily loss limit

It must support several real-world modes used by firms.

### Mode A: daily loss based on balance at the start of the day

Example:

- balance at start of day: 50,000
- max daily loss: 2,000
- if equity falls below 48,000 at any point during the day, the account dies.

### Mode B: daily loss based on equity at the start of the day

Similar, but using equity instead of balance.

### Mode C: includes open PnL

It must be possible to evaluate both:

- realized only,
- realized + unrealized.

### Mode D: daily reset at a specific time

The system must support:

- reset at date boundary,
- reset at a configurable session time.

Required fields:

- `daily_loss_reference_mode`
- `daily_loss_includes_unrealized`
- `daily_reset_time`
- `timezone`

## 9.3 Max total loss

It must support at least:

### Reference to initial balance

Maximum loss is calculated from a fixed initial capital.

### Reference to peak balance or peak equity

Some variants may require this.

### Realized-only vs realized+unrealized

This must be configurable.

## 9.4 Trailing drawdown

This is especially important and must be flexible.

Suggested modes:

- `none`
- `balance_trailing`
- `equity_trailing`
- `balance_trailing_until_breakeven`
- `equity_trailing_until_breakeven`
- `trailing_then_fixed`

Configurable behavior:

- whether the trailing threshold rises with each new peak,
- whether it freezes at some threshold,
- whether it never decreases,
- whether it is evaluated with unrealized PnL or realized only.

There must be a clear function that computes at each point in time:

- the current trailing drawdown threshold,
- whether it has been breached.

## 9.5 Profit target

It must be modelable at least as:

- percentage over initial capital,
- absolute value.

It must also be configurable whether it counts:

- realized only,
- realized + unrealized.

## 9.6 Minimum trading days

The system must define precisely what counts as a “trading day”:

- a day with at least one closed trade?
- a day with at least one opened trade?
- a day with non-zero PnL?
- a day with valid activity according to firm rules?

This definition must be configurable.

## 9.7 Consistency rules

Some firms require the best day not to represent more than a certain percentage of total profit, or require profits to be reasonably distributed.

For example, the system should be able to model:

- `best_day_profit <= consistency_threshold_pct * total_profit`
- or equivalent rules on the best trade.

## 9.8 Operational restrictions

The system must be able to configure:

- `overnight_holding_allowed`
- `weekend_holding_allowed`
- `news_trading_allowed`
- `allowed_sessions`
- `forbidden_windows`
- `max_position_size`

## 9.9 Rules by phase

It is very important that rules can change by phase:

- Evaluation Phase 1
- Evaluation Phase 2
- Funded
- Payout eligible

Each phase must be able to have its own `RuleSet`.

---

# 10. State machine

## 10.1 Main states

The system must explicitly model an account state machine.

Suggested states:

- `INIT`
- `EVALUATION_PHASE_1_ACTIVE`
- `EVALUATION_PHASE_1_PASSED`
- `EVALUATION_PHASE_2_ACTIVE`
- `EVALUATION_PHASE_2_PASSED`
- `FUNDED_ACTIVE`
- `PAYOUT_ELIGIBLE`
- `PAYOUT_TAKEN`
- `FAILED_DAILY_LOSS`
- `FAILED_TOTAL_LOSS`
- `FAILED_TRAILING_DD`
- `FAILED_RULE_BREACH`
- `COMPLETED`

## 10.2 Transition events

Examples:

- `target_reached`
- `min_days_reached`
- `phase_passed`
- `daily_loss_breached`
- `total_loss_breached`
- `trailing_dd_breached`
- `forbidden_holding_detected`
- `news_rule_breached`
- `payout_conditions_met`
- `payout_taken`

## 10.3 Advantages of this state machine

It allows:

- modeling one- or two-phase evaluations,
- reusing the same strategy across different firms,
- transitioning from evaluation to funded without hacks,
- measuring metrics separately by phase,
- analyzing in which phase accounts fail the most.

---

# 11. Data layer design

## 11.1 Requirements

The data layer must:

- connect to the existing user module,
- abstract that connection via adapters,
- return consistent `DataFrame` objects,
- support different instruments and timeframes,
- maintain temporal integrity.

## 11.2 Adapter layer

There should be a clear interface such as:

- `MarketDataAdapter`

Suggested methods:

- `fetch_ohlcv(...)`
- `fetch_bid_ask(...)`
- `fetch_metadata(...)`

The objective is to encapsulate the user’s current module without coupling the rest of the system to it.

## 11.3 Normalization

All datasets must be normalized for:

- column names,
- timezone,
- temporal ordering,
- numeric types,
- absence of duplicates,
- consistent time index.

## 11.4 Cleaning

The system must be able to:

- remove duplicates,
- detect gaps,
- flag suspicious bars,
- validate that `low <= min(open, close, high)` and `high >= max(open, close, low)`,
- detect negative spreads or corrupted data.

## 11.5 Resampling

It must support:

- resampling from lower to higher timeframe,
- correct OHLCV aggregation,
- recomputation of derived columns.

## 11.6 Enrichment

Optionally, the data layer should be able to add:

- ATR,
- rolling volatility,
- synthetic spread,
- session open/close flags,
- regime labels.

---

# 12. Strategy design

## 12.1 Principles

The strategy must be modular and must not know firm rules.

It should be limited to answering:

- when to enter,
- when to exit,
- where the stop should be,
- where the take should be,
- optionally a `size_hint`.

## 12.2 Proposed base interface

Each strategy should implement an interface conceptually similar to:

- `prepare_features(df) -> df_features`
- `generate_signals(df_features) -> pd.DataFrame | list[Signal]`
- `update_internal_state(...)`
- `name`
- `version`

## 12.3 Expected outputs

The strategy should be able to produce:

- long entries,
- short entries,
- exits,
- stop/take levels,
- tags or labels for later analysis.

## 12.4 Strategy registry

There should be a registry to load strategies by name:

- `mean_reversion`
- `sma_cross`
- `breakout`
- `opening_range`

This will make configuration-driven experiments easier.

---

# 13. Execution engine design

## 13.1 Recommended initial approach

Implement a **bar-based** engine first.

Reasons:

- simpler,
- enough for an MVP,
- fast to simulate,
- easy to integrate into notebooks,
- scalable to thousands of runs.

Later it may be extended to tick-based if needed.

## 13.2 Engine responsibilities

The engine must:

- iterate through bars in temporal order,
- consult signals,
- open positions,
- update open positions,
- close positions by stop/take/exit,
- apply slippage and fees,
- generate events,
- maintain the trade ledger,
- update account state.

## 13.3 Fill policy

It must be configurable.

Minimum modes:

- `next_bar_open`
- `same_bar_touch`
- `conservative_intrabar`
- `pessimistic_intrabar`
- `optimistic_intrabar`

The goal is to model more or less favorable assumptions.

## 13.4 Intrabar ambiguity

If both stop and take are touched in the same bar, the policy must decide.

Options:

- prioritize stop,
- prioritize take,
- assume worst case,
- use configurable ordering.

For survival analysis, a conservative default is preferable.

## 13.5 Gap handling

The system must support:

- filling at the opening price if the stop is gapped through,
- optional extra slippage for gaps.

---

# 14. Costs, slippage, and frictions

## 14.1 Importance

Costs are crucial, especially in low-edge strategies and prop firms where:

- limits are tight,
- trade count may be high,
- survival depends on avoiding constant erosion.

## 14.2 Minimum costs to support

- commission per contract or per lot,
- fixed commission per trade,
- spread,
- slippage,
- overnight fee if applicable,
- funding if applicable.

## 14.3 Slippage module design

It must be extensible. Recommended initial modes:

- fixed slippage per trade,
- slippage in ticks,
- slippage proportional to spread,
- slippage proportional to ATR,
- volatility-dependent slippage.

## 14.4 Fees module design

It must support:

- fixed fee per entry/exit,
- fee per unit traded,
- round-trip fee,
- symbol-specific configuration if needed.

---

# 15. Internal risk management

Internal risk management is different from firm rules.

## 15.1 Objective

This layer represents the trader’s own logic to protect the account before it reaches firm-imposed limits.

## 15.2 Required controls

### `risk_per_trade`

It should be expressible as:

- percentage of balance,
- percentage of equity,
- fixed size,
- fixed number of contracts.

### `max_trades_per_day`

It should prevent overtrading.

### `daily_stop_internal`

It should be able to stop trading for the day before the firm limit is reached.

### `daily_take_lock`

It should be able to stop the day after reaching a reasonable profit.

### `cooldown_after_losses`

It should be able to block new entries after N consecutive losses.

### `drawdown_based_derisking`

It should reduce size when the account enters drawdown.

### `session_filter`

It should limit trading to certain hours.

### `volatility_filter`

It should prevent trading in extremely adverse conditions.

### `spread_filter`

It should prevent trading when spread is abnormal.

## 15.3 Separation from strategy

The strategy should not directly decide whether to “keep trading today”. That decision should be able to pass through the risk layer.

---

# 16. Historical and sequence-based simulation

## 16.1 Single historical run

A linear simulation over a specific real historical sample.

Usefulness:

- debugging,
- validation,
- traceability,
- understanding exactly why an account fails.

## 16.2 Rolling / walk-forward

Run the same strategy over multiple time windows.

Usefulness:

- temporal robustness,
- avoiding overfitting to a single period,
- studying survival by regime.

## 16.3 Trade bootstrap

Take historical trades and reorder or resample them.

Usefulness:

- trade order matters a lot,
- allows estimation of sensitivity to adverse streaks.

## 16.4 Block bootstrap

Sample blocks of trades or time blocks.

Usefulness:

- preserve local dependence,
- avoid fully destroying clusters of losses/gains.

## 16.5 Parametric Monte Carlo

Generate synthetic sequences from estimated distributions.

Usefulness:

- stress testing beyond observed history,
- heavier tails,
- adverse synthetic scenarios.

## 16.6 Regime-aware simulation

Separate and recombine sequences while respecting regime proportions.

Usefulness:

- identify if a strategy dies in specific environments,
- design regime-based activation filters.

---

# 17. Formal survival analysis

## 17.1 Concept

The account should be treated as a “unit” whose life ends when a rule is violated.

## 17.2 Survival curve

The system should compute a Kaplan-Meier style curve:

- x-axis: time or days,
- y-axis: probability of still being alive.

This makes it very easy to compare:

- strategy A vs B,
- sizing A vs B,
- firm A vs B.

## 17.3 Hazard rate

The system should be able to compute:

- conditional probability of dying on day `t`, given survival until `t-1`.

This is useful to detect:

- very high early mortality,
- stabilization after a phase is passed,
- impact of early rules.

## 17.4 Censoring

If an account is still alive at the end of the observed period, this must be treated as a censored observation when computing certain survival metrics.

---

# 18. System metrics

## 18.1 Classical performance metrics

These should exist, even if they are not the primary focus:

- net PnL,
- gross PnL,
- win rate,
- average win,
- average loss,
- expectancy,
- profit factor,
- Sharpe,
- Sortino,
- max drawdown,
- return over max drawdown.

## 18.2 Survival metrics

These are primary:

- `survival_probability_day_N`
- `survival_probability_end`
- `median_days_survived`
- `mean_days_survived`
- `days_survived_p05`
- `days_survived_p95`
- `hazard_rate_day_N`
- `pass_rate`
- `conditional_survival_after_pass`

## 18.3 Robustness metrics

- `max_losing_streak`
- `avg_losing_streak`
- `p95_daily_loss`
- `p95_trade_loss`
- `drawdown_recovery_time`
- `pct_days_near_daily_limit`
- `pct_runs_breaching_rule_X`

## 18.4 Operational metrics

- `green_day_ratio`
- `red_day_ratio`
- `flat_day_ratio`
- `avg_profit_day`
- `avg_loss_day`
- `trade_frequency`
- `profit_concentration`
- `best_day_share_of_total_profit`

## 18.5 Prop-firm-specific metrics

- `pct_fail_daily_loss`
- `pct_fail_total_loss`
- `pct_fail_trailing_dd`
- `pct_fail_consistency_rule`
- `pct_fail_holding_rule`
- `pct_hit_target_before_failure`
- `pct_hit_min_days_before_failure`
- `pct_reach_funded`
- `pct_reach_payout_eligible`

## 18.6 Economic metrics

- `challenge_fee_total`
- `reset_fee_total`
- `activation_fee_total`
- `expected_cost_to_pass`
- `expected_cost_to_funded`
- `expected_cost_to_first_payout`
- `expected_payout_value`
- `net_ev_after_fees`
- `return_per_euro_spent`

---

# 19. Precise definition of “survival”

There must not be only one notion of survival.

## 19.1 Basic survival

The account has not died by the end of the horizon.

## 19.2 Useful survival

The account has not died and has also generated enough useful activity, for example:

- at least X green days,
- at least Y profit.

## 19.3 Profitable survival

The account has not died and net result exceeds real costs:

- evaluation fee,
- reset fee,
- activation fee.

## 19.4 Payout-eligible survival

The account is still alive and also meets payout conditions.

This is especially important because some strategies may “survive” without being economically useful.

---

# 20. Output and internal artifact design

## 20.1 Trade log

It must be exhaustive and easily exportable to CSV/Parquet.

## 20.2 Daily ledger

It is fundamental for:

- minimum trading days,
- consistency rules,
- green/red day analysis,
- proximity to daily loss.

## 20.3 Equity curve

It must include:

- balance curve,
- equity curve,
- peak equity,
- drawdown curve.

## 20.4 Event log

There must be a log of relevant events:

- trade_opened,
- trade_closed,
- daily_reset,
- internal_stop_triggered,
- firm_warning,
- rule_breach,
- state_transition.

## 20.5 Config snapshot

Each result must save a serializable snapshot of the configuration used.

---

# 21. Typed configuration

## 21.1 Recommendation

Use `pydantic` or `dataclasses` with strong validation.

Primary recommendation: `pydantic`.

## 21.2 Suggested configurations

### `DataConfig`

- symbol
- venue
- timeframe
- start
- end
- timezone
- session_template
- include_extended_hours
- data_source_name
- cache_enabled

### `StrategyConfig`

- strategy_name
- params
- allow_long
- allow_short

### `ExecutionConfig`

- fill_model
- fee_model
- slippage_model
- pyramiding_allowed
- max_open_positions

### `RiskConfig`

- risk_per_trade
- sizing_mode
- max_trades_per_day
- daily_stop_internal
- daily_take_lock
- consecutive_losses_cutoff
- derisking_profile
- session_filter
- volatility_filter

### `FirmConfig`

- firm_name
- plan_name
- phase_structure
- rule_set_reference
- fee_schedule

### `SimulationConfig`

- mode
- n_runs
- bootstrap_method
- block_size
- seed
- horizon_days
- censoring_mode

### `ReportingConfig`

- generate_trade_log
- generate_daily_ledger
- generate_plots
- export_format

## 21.3 Advantages

- early validation,
- reproducibility,
- easy serialization,
- better notebook interface,
- declarative experiments.

---

# 22. Firm preset design

There must be a module with presets.

## 22.1 Conceptual examples

- `generic_single_phase_eval()`
- `generic_two_phase_eval()`
- `ftmo_like_10k()`
- `topstep_like_50k()`
- `generic_cfd_prop_small_account()`

It is not necessary to tie the initial implementation to real commercial names; it can start with generic profiles and later add specific presets.

## 22.2 Recommendation

Each preset should return a clear structure with:

- phases,
- rules per phase,
- economics,
- payout rules.

---

# 23. Economics module design

This is very important given the user’s goal of buying cheap accounts.

## 23.1 What it must model

- initial challenge cost,
- reset cost,
- activation fee,
- payout split,
- minimum payouts,
- expected retry frequency.

## 23.2 What questions it must answer

- which firm offers the best real net EV?
- what is the expected cost of getting a stable funded account?
- what is the expected cost of getting the first payout?
- is it better to buy a cheaper account or a slightly more expensive one with friendlier rules?

## 23.3 Metrics

- `expected_attempts_to_pass`
- `expected_attempts_to_first_funded`
- `expected_cost_to_first_funded`
- `expected_cost_to_first_payout`
- `net_ev_per_cycle`
- `payback_period_in_days`

---

# 24. Multi-account simulation

## 24.1 Motivation

Sometimes the real edge is not in a single account, but in a portfolio of accounts.

## 24.2 Modes

It should support:

- identical parallel accounts,
- accounts with parameter variants,
- accounts with different instruments,
- sequential accounts.

## 24.3 Correlation caution

Perfect independence must not be assumed.

There should be conceptual modes for:

- full correlation,
- partial correlation,
- decorrelation by strategy / instrument / session.

## 24.4 Metrics

- probability that at least one survives,
- probability that at least K survive,
- expected total cost,
- dispersion of results,
- portfolio EV.

---

# 25. Experiments the system must support

## 25.1 `risk_per_trade` sweep

This should be one of the standard experiments.

## 25.2 Internal daily stop sweep

To see whether stopping earlier improves survival.

## 25.3 `max_trades_per_day` sweep

To detect overtrading.

## 25.4 Volatility filter sweep

To evaluate whether avoiding extreme days improves results.

## 25.5 Firm comparison

Same strategy, different rules.

## 25.6 Cost stress test

- double slippage,
- increase commissions,
- worsen spread.

## 25.7 Adverse streak stress test

- biased permutations,
- heavy tails,
- regime lock scenarios.

## 25.8 Instrument comparison

Same strategy, different asset.

---

# 26. Reporting and visualization in marimo

## 26.1 Philosophy

Notebooks should be clean and act as interactive dashboards.

## 26.2 Recommended notebooks behavior

The notebook layer should support:

- loading and inspecting data quality,
- plotting signals over price,
- running a full simulation and inspecting trade logs,
- visualizing daily ledgers and failure reasons,
- running Monte Carlo experiments,
- comparing parameter sweeps,
- comparing firms,
- running stress tests,
- simulating account portfolios,
- analyzing funded-phase survival and economics.

## 26.3 Recommended visualizations

- equity curve,
- balance curve,
- underwater curve,
- histogram of days survived,
- histogram of max drawdown,
- bar chart by failure cause,
- Kaplan-Meier survival curve,
- hazard rate curve,
- heatmap of `risk_per_trade` vs `pass_rate`,
- heatmap of `risk_per_trade` vs `survival_30d`,
- distribution of daily PnL,
- distribution of losing streaks,
- profit concentration chart.

---

# 27. Recommended implementation order

## 27.1 Phase 1: Correct and simple MVP

Implement:

- typed configs,
- data adapter,
- one simple strategy,
- bar-based execution engine,
- daily loss,
- total loss,
- minimum trading days,
- profit target,
- trade log,
- daily ledger,
- basic metrics,
- single-run notebook.

Goal:
have a correct, simple, and testable system.

## 27.2 Phase 2: Advanced prop firm rules

Implement:

- trailing drawdown,
- phases,
- state machine,
- firm presets,
- funded phase.

## 27.3 Phase 3: Survival simulation

Implement:

- simple bootstrap,
- block bootstrap,
- Monte Carlo,
- survival curves,
- hazard rates,
- failure-cause breakdown.

## 27.4 Phase 4: Optimization and comparison

Implement:

- parameter sweeps,
- multi-criteria ranking,
- firm comparison,
- basic economics.

## 27.5 Phase 5: Additional realism

Implement:

- multi-account,
- advanced stress tests,
- account correlation,
- payout modeling,
- news filters,
- more realistic operational rules.

---

# 28. Tests that must exist from the start

## 28.1 Basic rules

There should be deterministic tests for:

- daily loss breach,
- total loss breach,
- trailing DD update,
- trailing DD breach,
- profit target hit,
- minimum trading days count.

## 28.2 Execution engine

- correct fill at open,
- correct stop fill,
- correct take fill,
- correct intrabar policy,
- proper fees and slippage deduction.

## 28.3 Daily ledger

- green / red / flat day,
- correct daily reset,
- correct daily start balance.

## 28.4 State machine

- correct phase transition,
- correct terminal states,
- invalid transitions must not be allowed.

## 28.5 Aggregate simulation

- bootstrap reproducible with seed,
- Monte Carlo reproducible,
- consistent aggregations.

## 28.6 Golden tests

It is advisable to have several small deterministic scenarios where the exact expected result is known.

---

# 29. Anti-patterns Codex must avoid

- mixing strategy with firm rules,
- writing critical logic in notebooks,
- using ambiguous names,
- coupling execution engine and reporting,
- assuming only one firm,
- assuming only one drawdown type,
- ignoring trade order,
- ignoring costs,
- using optimistic defaults,
- overloading a single class with too many responsibilities.

---

# 30. Code style requirements

Codex should generate code with these characteristics:

- modular,
- clear,
- typed,
- testable,
- documented with docstrings,
- explicit names,
- no hidden logic in side effects,
- few unnecessary external dependencies.

Preferences:

- `pandas` for initial data handling,
- `numpy` for calculations,
- `pydantic` for config and validation,
- `matplotlib` / `plotly` only if needed, although the focus of this document is the core, not plotting.

---

# 31. High-level internal API

These conceptual APIs should exist.

## 31.1 Data loading

```python
df = load_market_data(data_config)
```

## 31.2 Strategy initialization

```python
strategy = build_strategy(strategy_config)
```

## 31.3 Single simulation

```python
result = run_single_simulation(
    data=df,
    strategy=strategy,
    execution_config=execution_config,
    risk_config=risk_config,
    firm_config=firm_config,
)
```

## 31.4 Monte Carlo / bootstrap simulation

```python
aggregate = run_survival_simulation(
    result_or_trades=result,
    simulation_config=simulation_config,
)
```

## 31.5 Full experiment

```python
experiment_result = run_experiment(
    data_config=data_config,
    strategy_config=strategy_config,
    execution_config=execution_config,
    risk_config=risk_config,
    firm_config=firm_config,
    simulation_config=simulation_config,
)
```

---

# 32. Multi-objective ranking design

The system must not optimize a single metric.

## 32.1 Recommended approach

Use lexicographic optimization or a configurable weighted score.

## 32.2 Example lexicographic priority

1. maximize `pass_rate`,
2. maximize `survival_probability_day_30`,
3. minimize `pct_fail_daily_loss`,
4. maximize `green_day_ratio`,
5. maximize `avg_net_pnl`.

This fits the survival-first philosophy well.

---

# 33. Questions the system must be able to answer

The system should be designed to answer questions like:

- which `risk_per_trade` maximizes survival at day 30?
- what percentage of deaths comes from daily loss?
- which firm offers better net survival after fees?
- what happens if trading stops after reaching +0.75% for the day?
- what happens if maximum trades per day are reduced?
- what happens if slippage is doubled?
- what happens if trading only happens during certain sessions?
- how many resets should be expected?
- what is the probability that at least one out of 5 accounts survives 60 days?

---

# 34. Reference prompt for Codex

```markdown
I want you to implement a Python package called `prop_survival_sim` designed for marimo notebooks.

The goal of the project is to simulate the survival of trading strategies under prop trading firm rules. The primary focus is not maximizing raw PnL, but evaluating and optimizing:

- survival probability,
- probability of passing evaluation,
- expected number of days survived,
- expected number of green days,
- expected cost of achieving a stable funded account,
- net EV after fees, resets, and payouts.

Mandatory architecture:
- clearly separate logic for:
  - config,
  - data,
  - strategies,
  - execution,
  - risk,
  - prop rules,
  - simulation,
  - metrics,
  - reporting,
  - domain models.
- keep all core logic outside notebooks.
- marimo notebooks should act only as an exploration interface.

I want typed configuration structures using `pydantic` or equivalent, with at least:
- DataConfig
- StrategyConfig
- ExecutionConfig
- RiskConfig
- FirmConfig
- SimulationConfig
- ReportingConfig

I want a modular and extensible system that supports:
- data loading from a configurable adapter,
- interchangeable strategies,
- bar-based execution engine,
- daily loss,
- max total loss,
- configurable trailing drawdown,
- profit target,
- minimum trading days,
- phase state machine,
- evaluation and funded phase,
- trade log,
- daily ledger,
- equity curve,
- event log,
- Monte Carlo,
- trade bootstrap,
- block bootstrap,
- Kaplan-Meier style survival curves,
- hazard rate,
- prop compliance metrics,
- economic metrics,
- multi-account simulation.

Implementation rules:
- the strategy must not know prop firm rules,
- the internal risk layer must be separate from the firm rule layer,
- the data layer must be separate from execution and simulation,
- the engine must support configurable fill models and configurable slippage,
- the system must record exact failure causes,
- every simulation must be reproducible with a fixed seed.

I want unit tests from the beginning for:
- daily loss breach,
- total loss breach,
- trailing drawdown update and breach,
- minimum trading days,
- fills,
- fees,
- reproducible bootstrap,
- reproducible Monte Carlo,
- state machine.

I want the code to be clear, typed, modular, well documented, and easy to extend with new firms and strategies.
```

---

# 35. Minimum deliverables for a first iteration

## 35.1 Functional core

With:

- typed configs,
- domain models,
- loader adapter,
- one example strategy,
- execution engine,
- rule evaluator,
- single-run simulation,
- basic metrics,
- initial notebooks,
- basic tests.

## 35.2 Second iteration

With:

- trailing drawdown,
- state machine,
- firm presets,
- Monte Carlo,
- bootstrap,
- survival curves.

## 35.3 Third iteration

With:

- economics,
- multi-account,
- stress tests,
- multi-objective ranking.

---

# 36. Practical prioritization

If implementation time and complexity must be optimized, the correct order is:

1. core correctness,
2. rule tests,
3. traceable single run,
4. basic Monte Carlo,
5. survival metrics,
6. economics,
7. multi-account,
8. advanced features.

Never the other way around.

---

# 37. Expected final outcome of the system

The final system should make it possible to discover things such as:

- a lower raw-PnL strategy having better survival and higher real net EV,
- a cheap prop firm actually being expensive because of its trailing DD,
- whether an internal daily stop protects the account,
- how to optimize sizing for maximum days survived,
- how many accounts are likely needed before reaching a robust funded one,
- how to compare strategy variants not only by profitability, but also by mortality.

---

# 38. Final executive summary

The core idea of the project is to build a **survival simulator under prop firm constraints**, not a classic backtester.

That means the entire design must revolve around:

- hard constraints,
- trade ordering,
- losing streaks,
- drawdown control,
- phase changes,
- real prop firm business economics.

The architecture must make it possible to evolve the system from a simple MVP into a much more sophisticated platform without rewriting the core.

The real value will not come from finding the strategy with the highest theoretical return, but from finding the combination of:

- strategy,
- sizing,
- filters,
- risk controls,
- and firm,

that maximizes the probability of **staying alive long enough** for the operation to have real economic value.
