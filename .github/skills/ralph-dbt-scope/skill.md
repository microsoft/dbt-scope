---
name: ralph-dbt-scope
description: "Run the dbt-scope regression testing loop. Iteratively install, lint, build, test, and debug the dbt-scope adapter until all targets pass."
user-invocable: true
---

# dbt-scope Regression Testing Loop

Iterative development loop to make the `dbt-scope` adapter bulletproof against regressions.

---

## CRITICAL: Fix Code First, Never Skip Tests

**NEVER `git add` or `git commit`**, ANY changes you made will be reviewed, committed and pushed by a human.

**When tests fail, they represent REAL logic or integration issues.** The failures are not noise — something is broken and needs a code fix.

**You MUST follow this order:**

1. **Analyze diffs** — understand what changed (Step 0)
2. **Run the full pipeline** — `.\.scripts\run.ps1 all` (Step 1)
3. **Fix failures** — diagnose errors, fix code, re-run failing target (Step 2)
4. **Confirm green** — re-run `all` to verify no regressions (Step 3)
5. **Signal completion** — only after ALL targets are green (Step 4)

**NEVER emit `{ "status": "Succeeded" }` if any target is still failing.** Work through the failures systematically.

---

## The Job

Execute the regression testing loop: analyze diffs → run full pipeline → fix failures → confirm all green → emit completion signal.

---

## Context

- **Project**: `dbt-scope` — a dbt adapter that generates SCOPE scripts and submits them as ADLA jobs via REST API
- **Build system**: `uv` with `pyproject.toml`, PowerShell dev script
- **Python**: 3.10+
- **Auth**: `az login` required for debug/integration targets
- **Config**: `.env` file required (copy from `.env.example`)
- **Logs**: PowerShell transcript + pytest logs in `.logs/`

### Key directories

| Path                        | Purpose                                                                                           |
| --------------------------- | ------------------------------------------------------------------------------------------------- |
| `dbt/adapters/scope/`       | Adapter core (impl.py, connections.py, script_builder.py, credentials.py, relation.py, column.py) |
| `dbt/include/scope/macros/` | Jinja macros for materializations                                                                 |
| `tests/unit/`               | Unit tests (fast, no credentials)                                                                 |
| `tests/integration/`        | Integration tests (ADLA + az login + .env)                                                        |
| `.scripts/run.ps1`          | Dev script — all targets                                                                          |
| `pyproject.toml`            | Project config and dependencies                                                                   |

### Pipeline targets (run by `.\.scripts\run.ps1 all`)

| Target             | What it does                                     | Cloud? |
| ------------------ | ------------------------------------------------ | ------ |
| `venv`             | Create a fresh uv-managed `.venv`                | No     |
| `install`          | `uv sync --extra dev`                            | No     |
| `build`            | Build wheel to `dist/`                           | No     |
| `lint`             | `ruff check + format --check` (auto-fixes first) | No     |
| `unit-test`        | `pytest tests/unit/` (fast, no credentials)      | No     |
| `debug`            | `dbt debug` against test project                 | Yes    |
| `integration-test` | `pytest tests/integration/` (datagen + dbt run)  | Yes    |

---

## Step 0: Analyze Git Diffs

```powershell
cd E:\git\dbt-scope

# Committed changes vs main
git --no-pager diff main...HEAD -- dbt/ tests/ .scripts/ pyproject.toml

# Uncommitted changes (tracked + staged)
git --no-pager diff -- dbt/ tests/ .scripts/ pyproject.toml
git --no-pager diff --cached -- dbt/ tests/ .scripts/ pyproject.toml

# Untracked files (new files not yet in git)
git ls-files --others --exclude-standard -- dbt/ tests/ .scripts/
```

Classify changed files (including untracked):

| Path pattern                 | Classification    | Action required           |
| ---------------------------- | ----------------- | ------------------------- |
| `dbt/adapters/scope/*.py`    | Adapter code      | Full pipeline             |
| `dbt/include/scope/macros/*` | Macros            | Full pipeline             |
| `tests/unit/*`               | Unit tests        | Unit tests + lint         |
| `tests/integration/*`        | Integration tests | Integration tests         |
| `pyproject.toml`             | Dependencies      | Reinstall + full pipeline |
| `.scripts/*`                 | Tooling           | Full pipeline             |

If NO code changes are detected (only docs/images, no untracked code files), skip to the completion signal with Succeeded.

---

## Step 1: Run the Full Pipeline

```powershell
cd E:\git\dbt-scope
.\.scripts\run.ps1 all
```

This runs all targets in sequence: venv → install → build → lint → unit-test → debug → integration-test.

- If it completes with `=== All targets completed. ===`, proceed to Step 4 (completion signal).
- If any target fails, proceed to Step 2.

---

## Step 2: Diagnose and Fix Failures

When a target fails, analyze the error and fix the code.

### If `install` fails

- Check `pyproject.toml` for dependency issues
- Check for import errors in `dbt/adapters/scope/__init__.py`
- Re-run: `.\.scripts\run.ps1 install`

### If `build` fails

- Check `pyproject.toml` build config
- Check for missing `__init__.py` files
- Re-run: `.\.scripts\run.ps1 build`

### If `lint` fails

- The lint target auto-fixes first (`ruff check --fix` + `ruff format`), then verifies
- If unfixable issues remain, manually fix them in the source files
- Ruff config: line length 100, double quotes, rule set: E, W, F, I, N, UP, B, SIM, T20, RUF
- `print()` is forbidden in library code (allowed in tests)
- Re-run: `.\.scripts\run.ps1 lint`

### If `unit-test` fails

1. Read the pytest output to identify which test(s) failed
2. Check the test file in `tests/unit/` and the code under test in `dbt/adapters/scope/`
3. Fix either the implementation or the test (prefer fixing implementation unless the test expectation is wrong)
4. Re-run a single test for fast iteration:
   ```powershell
   uv run pytest tests\unit\test_script_builder.py -v
   uv run pytest tests\unit\test_script_builder.py::TestClass::test_method -v
   ```
5. Once the individual test passes, re-run: `.\.scripts\run.ps1 unit-test`

### If `debug` fails

- Requires `az login` — verify Azure CLI auth
- Requires `.env` with ADLA/storage values
- Check `dbt/adapters/scope/connections.py` for REST API issues
- Check `tests/integration/dbt_project/` for profiles/project config
- Re-run: `.\.scripts\run.ps1 debug`

### If a failure is caused by missing prerequisites (not code)

If `az login` is not authenticated, `.env` is missing required values, or ADLA/ADLS resources are unavailable — these are **environment problems, not code bugs**. Do NOT waste iterations trying to "fix" code for environment issues. Instead:

1. Clearly describe the missing prerequisite in your output
2. Emit `{ "status": "Failed" }` immediately

### If `integration-test` fails

1. Read the pytest output to identify which test(s) failed
2. Integration tests in `tests/integration/test_dbt_scope.py` run the full dbt pipeline: datagen → dbt run → verify Delta tables
3. Check `tests/integration/conftest.py` for fixtures and `tests/integration/datagen.py` for test data generation
4. Fix the adapter code, macros, or test expectations as appropriate
5. Re-run: `.\.scripts\run.ps1 integration-test`

### Key conventions to follow when fixing code

Scan the existing codebase to get an understanding of the coding style, project structure, and testing patterns.

---

## Step 3: Confirm All Green

Once individual targets pass, re-run the full pipeline to confirm no regressions:

```powershell
.\.scripts\run.ps1 all
```

If any target regresses, go back to Step 2 and fix the new failure.

Only proceed to Step 4 when the full pipeline completes successfully.

---

## Step 4: Completion Signal

**CRITICAL: You MUST emit exactly one of these JSON objects as the absolute last line of your output.**

If ALL targets passed (venv + install + build + lint + unit-test + debug + integration-test):

```
{ "status": "Succeeded" }
```

If you were unable to fix all failures after exhausting your approaches:

```
{ "status": "Failed" }
```

**The Ralph loop script parses your final output for this signal.** If neither is found, the loop assumes the task is incomplete and will re-invoke you for another iteration. Always emit a status as a single line of plain text — no code fences, no trailing output after the signal.
