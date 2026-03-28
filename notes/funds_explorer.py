"""Prop firm survival simulator — single-run explorer notebook."""

import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import pandas as pd

    import funds
    import strats
    import tape

    return funds, pd, strats, tape


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Prop Firm Survival Simulator

    Single-run explorer: load market data, run a strategy through VectorBT,
    apply prop firm rules day-by-day, and inspect whether the account survives.
    """)
    return


@app.cell
def _():
    symbol = 'BTC/USDT:USDT'

    start, end = '2025-01-01', '2025-06-01'
    timeframes = ['1h', '4h', '15m', '30m', '1d']

    fast_window, slow_window = 10, 30
    return end, fast_window, slow_window, start, symbol


@app.cell
def _(end, mo, start, symbol, tape, timeframe):
    handler = tape.CCXTHandler('bybit')
    df = handler.get_ohlcv(
        symbols=symbol, timeframe=timeframe, since=start, until=end
    )

    # Drop symbol column — we carry it in config
    if 'symbol' in df.columns:
        df = df.drop(columns=['symbol'])

    mo.md(f'Loaded **{len(df)}** bars for `{symbol}` ({start} to {end})')
    return (df,)


@app.cell
def _(df, fast_window, funds, mo, slow_window, strats):
    mo.md('## Backtest execution')

    strategy = strats.SmaCross(
        fast_window=fast_window.value,
        slow_window=slow_window.value,
    )

    firm_config = funds.generic_single_phase_eval()
    execution_config = strats.ExecutionConfig(init_cash=firm_config.account_size)
    strategy_config = strats.StrategyConfig(
        name='sma_cross',
        params={'fast_window': fast_window.value, 'slow_window': slow_window.value},
        direction=strats.Direction.LONG_ONLY,
    )

    entries, exits = strategy.signals(df)

    exec_result = strats.run_backtest(
        close=df['close'],
        entries=entries,
        exits=exits,
        execution_config=execution_config,
        strategy_config=strategy_config,
    )

    mo.md(
        f'VectorBT backtest complete: **{exec_result.trade_count}** trades, '
        f'final value **{exec_result.final_value:,.2f}**'
    )
    return exec_result, firm_config, strategy_config


@app.cell
def _(exec_result, firm_config, funds, strategy_config, symbol, timeframe):
    sim_result = funds.simulate_account(
        result=exec_result,
        firm_config=firm_config,
        strategy_name=strategy_config.name,
        symbol=symbol.value,
        timeframe=timeframe.value,
    )
    return (sim_result,)


@app.cell
def _(funds, mo, sim_result):
    mo.md('## Results')

    _summary = funds.format_summary(sim_result)
    mo.md(f'```\n{_summary}\n```')
    return


@app.cell
def _(mo, pd, sim_result):
    import altair as alt

    mo.md('## Equity Curve')

    _equity_df = pd.DataFrame({
        'bar': range(len(sim_result.equity_curve)),
        'equity': sim_result.equity_curve,
    })

    if not _equity_df.empty:
        _chart = (
            alt.Chart(_equity_df)
            .mark_line()
            .encode(
                x=alt.X('bar:Q', title='Bar'),
                y=alt.Y('equity:Q', title='Equity', scale=alt.Scale(zero=False)),
            )
            .properties(width=700, height=350, title='Equity Curve')
        )
        mo.ui.altair_chart(_chart)
    else:
        mo.md('_No equity data to plot._')
    return (alt,)


@app.cell
def _(mo, pd, sim_result):
    mo.md('## Daily Ledger')

    if sim_result.daily_ledger:
        _ledger_data = [
            {
                'Date': row.date,
                'Start': f'{row.start_balance:,.2f}',
                'End': f'{row.end_balance:,.2f}',
                'Realized PnL': f'{row.realized_pnl:,.2f}',
                'Unrealized': f'{row.unrealized_pnl_close:,.2f}',
                'Max Intraday Loss': f'{row.max_intraday_loss:,.2f}',
                'Trades': row.trades_count,
                'Wins': row.wins_count,
                'Losses': row.losses_count,
                'Status': 'Green'
                if row.is_green
                else ('Red' if row.is_red else 'Flat'),
            }
            for row in sim_result.daily_ledger
        ]
        _ledger_df = pd.DataFrame(_ledger_data)
        mo.ui.table(_ledger_df)
    else:
        mo.md('_No daily ledger data._')
    return


@app.cell
def _(mo, pd, sim_result):
    mo.md('## Trade Log')

    if sim_result.trade_log:
        _trade_data = [
            {
                'ID': t.trade_id,
                'Entry': t.entry_time,
                'Exit': t.exit_time,
                'Direction': t.direction,
                'Entry Price': f'{t.entry_price:,.2f}',
                'Exit Price': f'{t.exit_price:,.2f}',
                'Size': f'{t.size:.6f}',
                'Net PnL': f'{t.net_pnl:,.2f}',
                'Fees': f'{t.fees:,.4f}',
                'Day': t.day_id,
            }
            for t in sim_result.trade_log
        ]
        _trade_df = pd.DataFrame(_trade_data)
        mo.ui.table(_trade_df)
    else:
        mo.md('_No trades._')
    return


@app.cell
def _(alt, mo, pd, sim_result):
    mo.md('## Daily PnL Distribution')

    if sim_result.daily_ledger:
        _pnl_data = pd.DataFrame({
            'pnl': [row.realized_pnl for row in sim_result.daily_ledger]
        })
        _hist = (
            alt.Chart(_pnl_data)
            .mark_bar()
            .encode(
                x=alt.X('pnl:Q', bin=alt.Bin(maxbins=30), title='Daily Realized PnL'),
                y=alt.Y('count()', title='Frequency'),
            )
            .properties(width=700, height=250, title='Daily PnL Distribution')
        )
        mo.ui.altair_chart(_hist)
    else:
        mo.md('_No data._')
    return


@app.cell
def _(firm_config, mo):
    _rules = firm_config.rule_set
    mo.md(
        f"""
        ## Firm Rules: {firm_config.firm_name} ({firm_config.plan_name})

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
        | Eval fee | {firm_config.evaluation_fee:,.0f} |
        """
    )
    return


if __name__ == "__main__":
    app.run()
