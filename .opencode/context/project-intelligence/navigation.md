<!-- Context: project-intelligence/nav | Priority: high | Version: 3.0 | Updated: 2026-04-14 -->

# Project Intelligence

> Start here for quick project understanding. Orthanc is a prop trading survival simulator.

## Structure

```
.opencode/context/project-intelligence/
├── navigation.md              # This file — quick overview
├── technical-domain.md        # Stack, architecture, code patterns
├── business-domain.md         # Business context and problem statement
├── business-tech-bridge.md    # How business needs map to solutions
├── decisions-log.md           # Major decisions with rationale
└── living-notes.md            # Active issues, debt, open questions
```

## Quick Routes

| What You Need | File | Priority |
|---------------|------|----------|
| Tech stack and patterns | `technical-domain.md` | critical |
| Problem and philosophy | `business-domain.md` | high |
| Business-tech mapping | `business-tech-bridge.md` | high |
| Decision history | `decisions-log.md` | medium |
| Current state | `living-notes.md` | medium |

## Key Facts

- **Language**: Python >=3.14
- **Architecture**: Layered library (`data/` → `strats/` → `funds/`)
- **Naming**: Descriptive technical names throughout
- **Class Reference**: See `technical-domain.md` → "Class Reference" for full listing (31+ classes)
- **Interface**: Marimo notebooks in `notes/` (`regime_explorer.py`)
- **Data Handlers**: `CCXTHandler` (CCXT/crypto), `BentoHandler` (Databento/futures)
- **Configs**: Pydantic BaseModel (e.g., `FirmRuleSet`, `StrategyConfig`, `ExecutionConfig`)
- **Domain models**: Frozen dataclasses (e.g., `Trade`, `AccountState`, `SimulationResult`)
- **Rules**: Pure functions, no side effects
- **Strategies**: ABC + Registry pattern (`STRATEGY_REGISTRY`)
- **Tooling**: uv + ruff + basedpyright + pre-commit (always `uv run`, never bare `ruff`)

## Maintenance

Update when: tech stack changes, new patterns emerge, architecture decisions made.
Command: `/add-context --update`
