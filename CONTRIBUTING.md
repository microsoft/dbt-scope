# Contributing to dbt-scope

## Prerequisites

Python 3.10+, pip, az CLI (`az login`), git.

## Quick start

```powershell
cp .env.example .env   # fill in your ADLA/storage values
.\.scripts\run.ps1 all # venv, install, unit-test, debug, integration-test
```

## Dev script

```powershell
.\.scripts\run.ps1 <target>

```

| Target             | What it does                                    | Cloud? |
| ------------------ | ----------------------------------------------- | ------ |
| `venv`             | Create fresh venv                               | No     |
| `install`          | `pip install -e ".[dev]"`                       | No     |
| `build`            | Build wheel to `dist/`                          | No     |
| `lint`             | `ruff check + format --check`                   | No     |
| `fix`              | `ruff auto-fix + format`                        | No     |
| `unit-test`        | `pytest tests/unit/` (fast, no credentials)     | No     |
| `debug`            | `dbt debug` against test project                | Yes    |
| `integration-test` | `pytest tests/integration/` (datagen + dbt run) | Yes    |
| `all`              | All of the above in sequence                    | Yes    |

Each target is idempotent — auto-creates venv and installs deps if missing.

## Running tests

```powershell
.\.scripts\run.ps1 unit-test          # fast, no credentials
.\.scripts\run.ps1 integration-test   # generates SS data via datagen, runs dbt, verifies Delta
```

Integration tests are self-contained — they generate their own SS test data on Cosmos via ADLA, run dbt models against it, and verify the resulting Delta tables. The only prerequisites are ADLA + ADLS + `az login`.

## Note

* Test data auto-expires via `STREAMEXPIRY` (7 days). Delta tables are left for manual cleanup.