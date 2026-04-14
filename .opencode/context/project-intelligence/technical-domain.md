<!-- Context: project-intelligence/technical | Priority: critical | Version: 3.0 | Updated: 2026-04-14 -->

# Technical Domain

**Purpose**: Tech stack, architecture, and development patterns for Orthanc — a prop trading survival simulator.

## Primary Stack

| Layer | Technology | Version | Rationale |
|-------|-----------|---------|-----------|
| Language | Python | >=3.14 | Modern syntax, typing, scientific ecosystem |
| Config/Validation | Pydantic | >=2.12.5 | Typed configs, serialization, reproducibility |
| Backtesting | VectorBT | >=0.28.1 | Vectorized backtesting engine |
| Data (Crypto) | CCXT | >=4.5.22 | Unified exchange API |
| Data (Futures) | Databento | >=0.68.2 | Institutional-grade market data |
| DataFrames | Pandas + Polars + NumPy | Latest | Data manipulation and computation |
| Notebooks | Marimo | >=0.18.1 | Reactive notebooks as exploration interface |
| Visualization | Plotly + Altair | Latest | Interactive charts and dashboards |
| Pkg Manager | uv | Latest | Fast Python package manager |
| Lint/Format | Ruff | >=0.14.6 | Single quotes, preview mode, isort |
| Type Checking | basedpyright | >=1.34.0 | Standard mode type checking |

## Architecture

```
Type: Layered library (not a web service)
Pattern: Data -> Strategy -> Execution -> Simulation -> Metrics
Interface: Marimo notebooks import the library
Naming: Descriptive technical names throughout
```

### Project Structure

```
orthanc/
├── data/              # Data layer — market data fetching and caching
│   ├── ccxt/          # CCXT exchange adapter (crypto)
│   └── bento/         # Databento adapter (futures)
├── strats/            # Strategy layer — signal generation
│   └── playbook/      # Concrete strategy implementations
├── funds/             # Prop firm layer — account simulation and rules
├── archives/          # Raw/cached market data (gitignored)
├── notes/             # Marimo notebooks (exploration interface)
│   └── regime_explorer.py   # Regime switcher pipeline explorer
└── main.py            # Entry point
```

## Code Patterns

### Module API (typed inputs -> pure function -> frozen output)

```python
def run_backtest(
    close: pd.Series, entries: pd.Series, exits: pd.Series,
    execution_config: ExecutionConfig, direction: Direction = Direction.LONG,
) -> ExecutionResult:
    """Run a backtest over the given signals."""
```

### Strategy (ABC + Registry)

```python
class SmaCross(Strategy):
    """SMA Crossover strategy."""
    def __init__(self, fast_window: int | float = 10, ...) -> None: ...
    def config(self) -> StrategyConfig: ...
    def signals(self, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]: ...

register_strategy('sma_cross', SmaCross)
```

### Config (Pydantic BaseModel)

```python
class FirmRuleSet(BaseModel):
    """Prop firm rule constraints."""
    max_daily_loss_pct: float | None = 0.04
    daily_loss_reference: DailyLossReference = DailyLossReference.BALANCE_START_OF_DAY
```

### Domain Model (frozen dataclass)

```python
@dataclass(frozen=True)
class Trade:
    """A single completed trade."""
    trade_id: int
    symbol: str
    net_pnl: float
    exit_reason: str
```

### Rule Logic (pure functions, no side effects)

```python
def check_daily_loss(state: AccountState, rules: FirmRuleSet) -> FailureReason | None:
    """Check daily loss limit."""
```

## Naming Conventions

| Type | Convention | Example |
|------|-----------|---------|
| Files/Modules | `snake_case` | `sma_cross.py`, `config.py` |
| Classes/Configs/Models | `PascalCase` | `SmaCross`, `AccountState`, `CCXTHandler` |
| Functions | `snake_case` | `run_backtest`, `simulate_account` |
| Private helpers | `_prefixed` | `_extract_trades`, `_build_result` |
| Enums/Constants | `PascalCase` / `UPPER_SNAKE_CASE` | `Direction.LONG`, `STRATEGY_REGISTRY` |
| Packages | descriptive `snake_case` | `data/`, `strats/`, `funds/` |

## Code Standards

- **Python >=3.14** — use `X | Y` unions, match/case, modern syntax
- **basedpyright standard** — all code must type-check
- **Ruff format** — single quotes, space indent, preview mode
- **Ruff lint** — isort imports (`select = ["I"]`)
- **Pydantic for configs** — `BaseModel` with typed defaults
- **Frozen dataclasses for domain** — immutable, no side effects
- **Pure functions for rules** — no mutation, return values
- **ABC + Registry for extensible types** — strategy registration pattern
- **Google-style docstrings** — Args, Returns, Raises sections
- **`# type: ignore` only for stubs** — VectorBT, always tagged
- **Layered architecture** — `data/` → `strats/` → `funds/`
- **Notebooks = interface only** — all core logic in importable packages
- **Pre-commit required** — ruff check + ruff format + basedpyright
- **Always `uv run`** — `uv run ruff`, never bare `ruff` (different formatting output)

## Security

- API keys in `.env` only (`.gitignore`d) — never hardcoded
- Input validation via Pydantic at config construction
- No direct user input — library consumed by notebooks

## 📂 Codebase References

| Reference | Path | Description |
|-----------|------|-------------|
| Strategy ABC + registry | `strats/registry.py` | `Strategy` base class + `STRATEGY_REGISTRY` |
| Strategy impl | `strats/playbook/sma_cross.py` | Example strategy (SMA Crossover) |
| Execution | `strats/execution.py` | VectorBT wrapper (`run_backtest` → `ExecutionResult`) |
| Configs (strats) | `strats/config.py` | `StrategyConfig`, `ExecutionConfig`, `RegimeConfig`, `WeightAdjustmentConfig`, `RegimeSwitcherConfig` |
| Configs (funds) | `funds/config.py` | `RiskConfig`, `FirmRuleSet`, `FirmConfig`, `SimulationConfig` |
| Domain models | `funds/models.py` | `Trade`, `DailyLedgerRow`, `AccountState`, `SimulationResult` |
| Metrics | `funds/metrics.py` | `PerformanceMetrics`, `SurvivalMetrics`, `AggregateMetrics` |
| Rules + simulator | `funds/rules.py`, `funds/simulator.py` | Pure rule checks + day-by-day simulation |
| Presets | `funds/presets.py` | Pre-built firm configs (FTMO, TopStep) |
| Data handler (crypto) | `data/ccxt/handler.py` | `CCXTHandler` — CCXT data with caching |
| Data handler (futures) | `data/bento/handler.py` | `BentoHandler` — Databento data with caching |
| Data adapters | `data/ccxt/adapter.py`, `data/bento/adapter.py` | `ExcAdapter`, `BentoAdapter` |
| Data types | `data/ccxt/types.py`, `data/bento/types.py` | `DataFeed`, `BentoFeed` + `FEED_*` |
| Data cache | `data/cache.py` | `TimeRange`, `CachedFile`, `CheckpointMetadata` |
| Error handling | `data/errors.py` | `DataFetchError` — data fetch errors |
| Notebooks | `notes/regime_explorer.py` | Marimo exploration notebook |
| Config | `pyproject.toml`, `.pre-commit-config.yaml` | Dependencies, tools, hooks |

## Class Reference (31 classes)

| Name | Package | File |
|------|---------|------|
| `Strategy` | strats | `registry.py` |
| `StrategyConfig` | strats | `config.py` |
| `ExecutionConfig` | strats | `config.py` |
| `ExecutionResult` | strats | `execution.py` |
| `RegimeConfig` | strats | `config.py` |
| `WeightAdjustmentConfig` | strats | `config.py` |
| `RegimeSwitcherConfig` | strats | `config.py` |
| `Trade` | funds | `models.py` |
| `DailyLedgerRow` | funds | `models.py` |
| `AccountState` | funds | `models.py` |
| `SimulationResult` | funds | `models.py` |
| `RiskConfig` | funds | `config.py` |
| `FirmRuleSet` | funds | `config.py` |
| `FirmConfig` | funds | `config.py` |
| `SimulationConfig` | funds | `config.py` |
| `PerformanceMetrics` | funds | `metrics.py` |
| `SurvivalMetrics` | funds | `metrics.py` |
| `AggregateMetrics` | funds | `metrics.py` |
| `CCXTHandler` | data | `ccxt/handler.py` |
| `BentoHandler` | data | `bento/handler.py` |
| `DataFetchError` | data | `errors.py` |
| `TimeRange` | data | `cache.py` |
| `CachedFile` | data | `cache.py` |
| `CheckpointMetadata` | data | `cache.py` |
| `DataFeed` | data | `ccxt/types.py` |
| `ExcAdapter` | data | `ccxt/adapter.py` |
| `BentoFeed` | data | `bento/types.py` |
| `BentoAdapter` | data | `bento/adapter.py` |
| `SmaCross` | strats | `playbook/sma_cross.py` |
| `TrendFollow` | strats | `playbook/trend_follow.py` |
| `RegimeSwitcher` | strats | `playbook/regime_switcher.py` |
| `VolatilityBreakout` | strats | `playbook/volatility_breakout.py` |
| `Defensive` | strats | `playbook/defensive.py` |
| `MeanReversion` | strats | `playbook/mean_reversion.py` |

## Related Files

- `business-domain.md` — Problem statement, survival-first philosophy
- `decisions-log.md` — Architecture decisions with rationale
- `living-notes.md` — Active issues and open questions
