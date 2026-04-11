# Copilot Instructions — dbt-scope

## Build, test, lint

```bash
uv sync --extra dev                             # create .venv and install dev deps
.scripts/run.sh unit-test                       # fast, no credentials needed
.scripts/run.sh integration-test                # requires ADLA + az login + .env
.scripts/run.sh lint                            # ruff check + format --check
.scripts/run.sh fix                             # ruff auto-fix + format

# Single test file or test
uv run pytest tests/unit/test_script_builder.py -v
uv run pytest tests/unit/test_script_builder.py::TestScriptBuilderFullRefresh::test_generates_create_table -v
```

## Architecture

dbt-scope is a **dbt adapter** that generates [SCOPE](https://azure.microsoft.com/en-us/products/data-lake-analytics) scripts (not SQL) and submits them as ADLA jobs via REST API. Models written as `SELECT ... FROM @data` are compiled into complete SCOPE scripts with `EXTRACT`, `CREATE TABLE`, `DELETE`, and `INSERT INTO` blocks targeting Delta tables on ADLS.

### Adapter layer (`dbt/adapters/scope/`)

| File                | Role                                                                                                                                                                                                                                                                    |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `impl.py`           | `ScopeAdapter(BaseAdapter)` — most introspection methods are no-ops because SCOPE has no catalog. `drop_relation` is a no-op for safety; `rename_relation` raises.                                                                                                      |
| `connections.py`    | REST client for ADLA. `ScopeConnectionHandle` submits jobs via PUT, polls via GET. `ScopeConnectionManager.execute()` passes scripts to `submit_and_wait()`. `SELECT 1` is special-cased to verify auth only.                                                           |
| `script_builder.py` | Core engine. `ScriptBuilder` has static methods (`build_full_refresh`, `build_incremental`, `build_checkpoint`, `build_drop`) that assemble complete SCOPE scripts from a `ScriptConfig` + model SQL. All script generation is pure Python string building — not Jinja. |
| `credentials.py`    | `ScopeCredentials` — maps `profiles.yml` fields. Auth uses `AzureCliCredential` (`az login`).                                                                                                                                                                           |
| `relation.py`       | `ScopeRelation` — extends `BaseRelation`. Adds `delta_location` for ABFSS paths. Identifiers are unquoted.                                                                                                                                                              |
| `column.py`         | `ScopeColumn` — maps agate/dbt types to SCOPE types (e.g., `number → double`, `date → DateTime`).                                                                                                                                                                       |

### Macro layer (`dbt/include/scope/macros/`)

Materializations call helper macros that delegate to `ScriptBuilder` via the adapter. Most dbt DDL/introspection macros are no-ops since SCOPE has no schema catalog. The two key materializations:

- **`table`** — calls `build_full_refresh`: CREATE TABLE + EXTRACT + INSERT.
- **`incremental`** — for microbatch: DELETE partition range + EXTRACT with date filter + INSERT. For append/full-refresh: same as table.

### SCOPE script conventions

- `@target` = Delta table, `@data` = extracted SS rowset, `@batch_data` = user's transformed data.
- Virtual columns `_date`, `_serial`, `_source_file` are added during EXTRACT.
- The partition column (e.g., `event_year_date`) is excluded from EXTRACT and derived from `_date`.
- Incremental mode sets `@@DeltaLakeCommitCondition` (configurable via `delta_lake_commit_condition`, default `FailIfFileConflict`).

## Key conventions

- **Python 3.10+** — use `from __future__ import annotations`, `list[X]` / `dict[X, Y]` (not `List`/`Dict`).
- **Dataclasses** for config/model objects. Use `frozen=True` for immutable types (e.g., `ScopeRelation`).
- **Logging** — `log = logging.getLogger(__name__)` with `%`-style formatting (not f-strings) for lazy evaluation.
- **Exceptions** — `DbtRuntimeError` for config/logic errors, `DbtDatabaseError` for API/job failures. Chain with `from exc`.
- **Ruff** — line length 100, double quotes, rule set: E, W, F, I, N, UP, B, SIM, T20, RUF. `print()` is forbidden in library code (allowed in tests).
- **Tests** — unit tests use `class TestX:` grouping with fixtures from `conftest.py`. Integration tests require `.env` + `az login` and use `@pytest.mark.timeout(3600)`.
- **ScriptBuilder is pure Python** — all SCOPE script generation is in `script_builder.py` using string assembly. Jinja macros only orchestrate; they don't build script text.
