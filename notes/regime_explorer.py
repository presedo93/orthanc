"""Regime Switcher explorer — decomposed pipeline notebook."""

import marimo

__generated_with = '0.21.1'
app = marimo.App(width='medium')


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import os

    import altair as alt
    import pandas as pd

    alt.data_transformers.enable('vegafusion')

    import data
    import funds
    import strats
    from strats.playbook.regime_switcher import (
        REGIME_STRATEGY_MATRIX,
        compute_regime_features,
        compute_strategy_weights,
        detect_regime_probabilities,
        detect_regime_transition,
        fit_regime_model,
    )

    return (
        REGIME_STRATEGY_MATRIX,
        alt,
        compute_regime_features,
        compute_strategy_weights,
        data,
        detect_regime_probabilities,
        detect_regime_transition,
        fit_regime_model,
        funds,
        os,
        pd,
        strats,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Regime Switcher Explorer

    Decompose the full decision pipeline of the HMM-based multi-strategy
    switcher: **features → HMM fit → posterior inference → weight
    computation → signal combination → backtest**.

    A Gaussian Hidden Markov Model is trained on technical features to
    discover three latent regimes (trending, mean-reverting, volatile).
    Each stage is inspectable so you can see exactly how the system
    allocates across trend-following, mean-reversion, volatility-breakout,
    and defensive strategies in response to changing market conditions.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Configuration
    """)
    return


@app.cell
def _():
    symbols = {'nasdaq': 'MNQ.v.0', 's&p500': 'MES.v.0', 'russell2000': 'M2K.v.0'}

    timeframes = {'1m': 'ohlcv-1m', '1h': 'ohlcv-1h', '1d': 'ohlcv-1d'}

    sym, trf = 'nasdaq', '1m'
    since, until = '2026-01-01', '2026-01-24'
    return since, sym, symbols, timeframes, trf, until


@app.cell
def _():
    entry_threshold, exit_threshold = 0.4, 0.3
    trend_fast, trend_slow = 10, 30
    return entry_threshold, exit_threshold, trend_fast, trend_slow


@app.cell
def _():
    regime_colors = {
        'trending': '#2ecc71',
        'mean_reverting': '#95a5a6',
        'volatile': '#f39c12',
    }
    return (regime_colors,)


@app.cell
def _(mo, os, data, since, sym, symbols, timeframes, trf, until):
    api_key = os.environ.get('DATABENTO_API_KEY')
    _handler = data.BentoHandler(api_key)

    ohlcv = _handler.get_ohlcv(symbols[sym], timeframes[trf], since, until)

    if 'symbol' in ohlcv.columns:
        ohlcv = ohlcv.drop(columns=['symbol'])

    mo.md(f'Loaded **{len(ohlcv)}** bars of `{sym}` `{trf}` ({since} → {until})')
    return (ohlcv,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 1 · Regime Detection (HMM)
    """)
    return


@app.cell
def _(
    compute_regime_features,
    compute_strategy_weights,
    detect_regime_probabilities,
    detect_regime_transition,
    entry_threshold,
    exit_threshold,
    fit_regime_model,
    strats,
    ohlcv,
    trend_fast,
    trend_slow,
):
    _cfg = strats.RegimeSwitcherConfig(
        entry_threshold=entry_threshold,
        exit_threshold=exit_threshold,
        trend_fast_window=trend_fast,
        trend_slow_window=trend_slow,
    )
    regime_cfg = _cfg.regime

    # Decompose the pipeline step by step
    regime_features = compute_regime_features(ohlcv, regime_cfg)
    regime_model, regime_scaler, regime_label_map = fit_regime_model(
        regime_features, regime_cfg
    )
    regime_probs = detect_regime_probabilities(
        regime_features, regime_model, regime_scaler, regime_label_map
    )
    is_transition = detect_regime_transition(regime_probs)
    strategy_weights = compute_strategy_weights(regime_probs)
    return (
        is_transition,
        regime_cfg,
        regime_features,
        regime_label_map,
        regime_model,
        regime_probs,
        regime_scaler,
        strategy_weights,
    )


@app.cell
def _(
    mo,
    pd,
    regime_cfg,
    regime_features,
    regime_label_map,
    regime_model,
    regime_scaler,
):
    # Log-likelihood of the fitted model
    _clean = regime_features.dropna()
    _score = regime_model.score(regime_scaler.transform(_clean.values))

    # Label mapping
    _label_rows = [
        {'HMM State': k, 'Regime': v.value} for k, v in regime_label_map.items()
    ]

    # Emission means per state (what each regime "looks like")
    _means_df = pd.DataFrame(
        regime_model.means_,
        columns=_clean.columns,
        index=[regime_label_map[i].value for i in range(regime_cfg.n_regimes)],
    ).round(4)

    # Transition matrix
    _trans_df = pd.DataFrame(
        regime_model.transmat_,
        columns=[regime_label_map[i].value for i in range(regime_cfg.n_regimes)],
        index=[regime_label_map[i].value for i in range(regime_cfg.n_regimes)],
    ).round(4)

    mo.vstack([
        mo.md('### HMM Diagnostics'),
        mo.md(f"""
    **Log-likelihood**: `{_score:,.2f}` · **States**: {regime_cfg.n_regimes} · **Covariance**: {regime_cfg.covariance_type}

    #### State → Regime Mapping
        """),
        mo.ui.table(pd.DataFrame(_label_rows)),
        mo.md('#### Learned Transition Matrix'),
        mo.ui.table(_trans_df.reset_index().rename(columns={'index': 'from \\ to'})),
        mo.md('#### Emission Means (feature centroids per regime)'),
        mo.ui.table(_means_df.reset_index().rename(columns={'index': 'regime'})),
    ])
    return


@app.cell
def _(alt, mo, regime_colors, regime_probs):
    _melt = regime_probs.reset_index().melt(
        id_vars='timestamp',
        var_name='regime',
        value_name='probability',
    )

    _chart = (
        alt
        .Chart(_melt)
        .mark_area()
        .encode(
            x=alt.X('timestamp:T', title=''),
            y=alt.Y('probability:Q', stack='normalize', title='Probability'),
            color=alt.Color(
                'regime:N',
                scale=alt.Scale(
                    domain=list(regime_colors.keys()),
                    range=list(regime_colors.values()),
                ),
                title='Regime',
            ),
            tooltip=[
                alt.Tooltip('timestamp:T', title='Time'),
                alt.Tooltip('regime:N', title='Regime'),
                alt.Tooltip('probability:Q', title='Prob', format='.2f'),
            ],
        )
        .properties(width=700, height=280, title='Regime Probability Distribution')
    )

    mo.vstack([
        mo.md('### Regime Probabilities (HMM posteriors)'),
        _chart,
    ])
    return


@app.cell
def _(alt, mo, regime_colors, regime_probs):
    _dominant = regime_probs.idxmax(axis=1).rename('regime').reset_index()

    _dom_chart = (
        alt
        .Chart(_dominant)
        .mark_rect()
        .encode(
            x=alt.X('timestamp:T', title=''),
            color=alt.Color(
                'regime:N',
                scale=alt.Scale(
                    domain=list(regime_colors.keys()),
                    range=list(regime_colors.values()),
                ),
                title='Regime',
            ),
            tooltip=[
                alt.Tooltip('timestamp:T', title='Time'),
                alt.Tooltip('regime:N', title='Dominant'),
            ],
        )
        .properties(width=700, height=40, title='Dominant Regime Timeline')
    )
    mo.vstack([
        mo.md('### Dominant Regime'),
        _dom_chart,
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 2 · Strategy Weights
    """)
    return


@app.cell
def _(alt, mo, strategy_weights):
    _w_melt = strategy_weights.reset_index().melt(
        id_vars='timestamp',
        var_name='strategy',
        value_name='weight',
    )

    _strat_colors = {
        'trend_follow': '#2ecc71',
        'mean_reversion': '#9b59b6',
        'volatility_breakout': '#f39c12',
        'defensive': '#3498db',
    }

    _w_chart = (
        alt
        .Chart(_w_melt)
        .mark_area()
        .encode(
            x=alt.X('timestamp:T', title=''),
            y=alt.Y('weight:Q', stack='normalize', title='Weight'),
            color=alt.Color(
                'strategy:N',
                scale=alt.Scale(
                    domain=list(_strat_colors.keys()),
                    range=list(_strat_colors.values()),
                ),
                title='Strategy',
            ),
            tooltip=[
                alt.Tooltip('timestamp:T', title='Time'),
                alt.Tooltip('strategy:N', title='Strategy'),
                alt.Tooltip('weight:Q', title='Weight', format='.2f'),
            ],
        )
        .properties(width=700, height=280, title='Strategy Weight Allocation')
    )
    mo.vstack([
        mo.md('### Weight Evolution'),
        _w_chart,
    ])
    return


@app.cell
def _(REGIME_STRATEGY_MATRIX, alt, mo):
    _matrix = (
        REGIME_STRATEGY_MATRIX
        .reset_index()
        .melt(
            id_vars='index',
            var_name='strategy',
            value_name='weight',
        )
        .rename(columns={'index': 'regime'})
    )

    _heatmap = (
        alt
        .Chart(_matrix)
        .mark_rect()
        .encode(
            x=alt.X('strategy:N', title='Strategy'),
            y=alt.Y('regime:N', title='Regime'),
            color=alt.Color(
                'weight:Q', scale=alt.Scale(scheme='viridis'), title='Weight'
            ),
            tooltip=[
                alt.Tooltip('regime:N'),
                alt.Tooltip('strategy:N'),
                alt.Tooltip('weight:Q', format='.1f'),
            ],
        )
        .properties(width=300, height=250, title='Static Regime→Strategy Mapping')
    )

    _text = (
        alt
        .Chart(_matrix)
        .mark_text(color='white', fontSize=14, fontWeight='bold')
        .encode(
            x='strategy:N',
            y='regime:N',
            text=alt.Text('weight:Q', format='.1f'),
        )
    )

    mo.vstack([
        mo.md('### Regime → Strategy Matrix'),
        _heatmap + _text,
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 3 · Price & Regime Transitions
    """)
    return


@app.cell
def _(alt, df, is_transition, regime_colors, regime_probs):
    _price = df[['close']].copy().reset_index()

    _dominant = regime_probs.idxmax(axis=1).rename('regime')
    _price = _price.merge(
        _dominant.reset_index(),
        on='timestamp',
        how='left',
    )

    _trans_df = is_transition[is_transition].reset_index()
    _trans_df.columns = ['timestamp', 'transition']
    _trans_df = _trans_df.merge(
        df[['close']].reset_index(),
        on='timestamp',
        how='left',
    )

    _price_line = (
        alt
        .Chart(_price)
        .mark_line(strokeWidth=1.5)
        .encode(
            x=alt.X('timestamp:T', title=''),
            y=alt.Y('close:Q', title='Close', scale=alt.Scale(zero=False)),
            color=alt.Color(
                'regime:N',
                scale=alt.Scale(
                    domain=list(regime_colors.keys()),
                    range=list(regime_colors.values()),
                ),
                title='Regime',
            ),
        )
    )

    _trans_marks = (
        alt
        .Chart(_trans_df)
        .mark_rule(strokeDash=[4, 4], opacity=0.4, color='white')
        .encode(x='timestamp:T')
    )

    _combined = (_price_line + _trans_marks).properties(
        width=700, height=350, title='Price Colored by Dominant Regime'
    )
    _combined
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 4 · Sub-Strategy Signals
    """)
    return


@app.cell
def _(df, strats, mo, pd, regime_label_map, regime_model, regime_scaler):
    _strategy = strats.RegimeSwitcher(
        switcher_config=_cfg,
        regime_model=regime_model,
        regime_scaler=regime_scaler,
        regime_labels=regime_label_map,
    )

    # Run each sub-strategy independently
    _sub_signals = {}
    for _key, _sub in _strategy._strategies.items():
        _ent, _ext = _sub.signals(df)
        _sub_signals[_key] = {'entries': _ent, 'exits': _ext}

    # Build a combined signal overview
    _signal_rows = []
    for _key, _sigs in _sub_signals.items():
        _ent_count = int(_sigs['entries'].sum())
        _ext_count = int(_sigs['exits'].sum())
        _signal_rows.append({
            'Strategy': _key,
            'Entry signals': _ent_count,
            'Exit signals': _ext_count,
        })
    sub_signal_summary = pd.DataFrame(_signal_rows)

    mo.vstack([
        mo.md('### Signal Counts per Sub-Strategy'),
        mo.ui.table(sub_signal_summary),
    ])
    return


@app.cell
def _(alt, df, strats, mo, pd, regime_label_map, regime_model, regime_scaler):
    _strategy2 = strats.RegimeSwitcher(
        switcher_config=_cfg,
        regime_model=regime_model,
        regime_scaler=regime_scaler,
        regime_labels=regime_label_map,
    )
    _rows = []
    for _key, _sub in _strategy2._strategies.items():
        _ent, _ = _sub.signals(df)
        _ts = _ent[_ent].index
        for _t in _ts:
            _rows.append({'timestamp': _t, 'strategy': _key})

    _signal_df = pd.DataFrame(_rows)

    if not _signal_df.empty:
        _strat_colors2 = {
            'trend_follow': '#2ecc71',
            'mean_reversion': '#9b59b6',
            'volatility_breakout': '#f39c12',
            'defensive': '#3498db',
        }

        _sig_chart = (
            alt
            .Chart(_signal_df)
            .mark_tick(thickness=2, size=20)
            .encode(
                x=alt.X('timestamp:T', title=''),
                y=alt.Y('strategy:N', title='Strategy'),
                color=alt.Color(
                    'strategy:N',
                    scale=alt.Scale(
                        domain=list(_strat_colors2.keys()),
                        range=list(_strat_colors2.values()),
                    ),
                    legend=None,
                ),
            )
            .properties(width=700, height=150, title='Entry Signals by Sub-Strategy')
        )
        mo.vstack([
            mo.md('### Entry Signal Timeline'),
            _sig_chart,
        ])
    else:
        mo.vstack([
            mo.md('### Entry Signal Timeline'),
            mo.md('_No entry signals._'),
        ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 5 · Backtest
    """)
    return


@app.cell
def _(df, strats, mo, funds, regime_label_map, regime_model, regime_scaler):
    strategy = strats.RegimeSwitcher(
        switcher_config=_cfg,
        regime_model=regime_model,
        regime_scaler=regime_scaler,
        regime_labels=regime_label_map,
    )
    firm_config = funds.generic_single_phase()
    execution_config = strats.ExecutionConfig(init_cash=firm_config.account_size)

    entries, exits = strategy.signals(df)

    exec_result = strats.run_backtest(
        close=df['close'],
        entries=entries,
        exits=exits,
        execution_config=execution_config,
        direction=strategy.config().direction,
    )

    mo.md(f"""
    ### Performance Summary

    | Metric | Value |
    |--------|-------|
    | Trades | **{exec_result.trade_count}** |
    | Win rate | **{exec_result.win_rate:.1%}** |
    | Profit factor | **{exec_result.profit_factor:.2f}** |
    | Total return | **{exec_result.total_return:.2%}** |
    | Max drawdown | **{exec_result.max_drawdown:.2%}** |
    | Sharpe ratio | **{exec_result.sharpe_ratio:.2f}** |
    | Sortino ratio | **{exec_result.sortino_ratio:.2f}** |
    | Final value | **{exec_result.final_value:,.2f}** |
    """)
    return exec_result, firm_config, strategy


@app.cell
def _(alt, exec_result, mo):
    _eq = exec_result.equity_curve.reset_index()
    _eq.columns = ['timestamp', 'equity']

    _eq_chart = (
        alt
        .Chart(_eq)
        .mark_line(strokeWidth=1.5)
        .encode(
            x=alt.X('timestamp:T', title=''),
            y=alt.Y('equity:Q', title='Equity', scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip('timestamp:T', title='Time'),
                alt.Tooltip('equity:Q', title='Equity', format=',.2f'),
            ],
        )
        .properties(width=700, height=300, title='Equity Curve')
    )
    mo.vstack([
        mo.md('### Equity Curve'),
        _eq_chart,
    ])
    return


@app.cell
def _(alt, exec_result, mo, pd):
    _ret = pd.DataFrame({'return': exec_result.daily_returns.values})

    if not _ret.empty:
        _hist = (
            alt
            .Chart(_ret)
            .mark_bar()
            .encode(
                x=alt.X('return:Q', bin=alt.Bin(maxbins=40), title='Daily Return'),
                y=alt.Y('count()', title='Frequency'),
            )
            .properties(width=700, height=220, title='Daily Returns Distribution')
        )
        mo.vstack([
            mo.md('### Returns Distribution'),
            _hist,
        ])
    else:
        mo.vstack([
            mo.md('### Returns Distribution'),
            mo.md('_No return data._'),
        ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 6 · Prop Firm Simulation
    """)
    return


@app.cell
def _(exec_result, firm_config, mo, funds, strategy, sym, trf):
    sim_result = funds.simulate_account(
        result=exec_result,
        firm_config=firm_config,
        strategy_name=strategy.config().name,
        symbol=sym,
        timeframe=trf,
    )

    _summary = funds.format_summary(sim_result)
    mo.md(f'```\n{_summary}\n```')
    return (sim_result,)


@app.cell
def _(alt, mo, pd, sim_result):
    _eq_sim = pd.DataFrame({
        'bar': range(len(sim_result.equity_curve)),
        'equity': sim_result.equity_curve,
    })

    if not _eq_sim.empty:
        _chart = (
            alt
            .Chart(_eq_sim)
            .mark_line(strokeWidth=1.5)
            .encode(
                x=alt.X('bar:Q', title='Bar'),
                y=alt.Y('equity:Q', title='Equity', scale=alt.Scale(zero=False)),
            )
            .properties(width=700, height=300, title='Simulated Equity Curve')
        )
        mo.vstack([
            mo.md('### Equity Curve (Sim)'),
            _chart,
        ])
    else:
        mo.vstack([
            mo.md('### Equity Curve (Sim)'),
            mo.md('_No equity data._'),
        ])
    return


@app.cell
def _(mo, pd, sim_result):
    if sim_result.daily_ledger:
        _ledger_data = [
            {
                'Date': row.date,
                'Start': f'{row.start_balance:,.2f}',
                'End': f'{row.end_balance:,.2f}',
                'PnL': f'{row.realized_pnl:,.2f}',
                'Unrealized': f'{row.unrealized_pnl_close:,.2f}',
                'Max Intraday Loss': f'{row.max_intraday_loss:,.2f}',
                'Trades': row.trades_count,
                'W/L': f'{row.wins_count}/{row.losses_count}',
                'Status': 'Green'
                if row.is_green
                else ('Red' if row.is_red else 'Flat'),
            }
            for row in sim_result.daily_ledger
        ]
        _ledger_df = pd.DataFrame(_ledger_data)
        mo.vstack([
            mo.md('### Daily Ledger'),
            mo.ui.table(_ledger_df),
        ])
    else:
        mo.vstack([
            mo.md('### Daily Ledger'),
            mo.md('_No daily ledger data._'),
        ])
    return


@app.cell
def _(firm_config, mo):
    _rules = firm_config.rule_set
    mo.md(f"""
    ### Firm Rules: {firm_config.firm_name} ({firm_config.plan_name})

    | Rule | Value |
    |------|-------|
    | Account size | {firm_config.account_size:,.0f} |
    | Daily loss limit | {f'{_rules.max_daily_loss_pct:.0%}' if _rules.max_daily_loss_pct else 'N/A'} |
    | Total loss limit | {f'{_rules.max_total_loss_pct:.0%}' if _rules.max_total_loss_pct else 'N/A'} |
    | Trailing DD | {_rules.trailing_drawdown_mode.value} |
    | Profit target | {f'{_rules.profit_target_pct:.0%}' if _rules.profit_target_pct else 'N/A'} |
    | Min trading days | {_rules.min_trading_days or 'N/A'} |
    | Consistency rule | {'Yes' if _rules.consistency_rule_enabled else 'No'} |
    | Profit split | {firm_config.profit_split_pct:.0%} |
    """)
    return


if __name__ == '__main__':
    app.run()
