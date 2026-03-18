# Orthanc

TBD

## CI

GitHub Actions runs on every push and PR to `main`. A single job performs three checks in order:

1. **Lint** — `ruff check .`
2. **Format** — `ruff format --check .`
3. **Type check** — `basedpyright`

Dependencies are cached via `setup-uv` and concurrent runs on the same ref are automatically cancelled.

## Pre-commit hooks

The same three checks run locally before each commit via [pre-commit](https://pre-commit.com/).

Setup (once):

```bash
uv sync && uv run pre-commit install
```

After that, `git commit` will run lint, format, and type check automatically. To run manually:

```bash
uv run pre-commit run --all-files
```
