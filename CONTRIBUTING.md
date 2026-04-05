# Contributing to dbt-scope

## Prerequisites

1. Windows pre-reqs

   ```powershell
   winget install -e --id Microsoft.VisualStudioCode
   ```

2. Get a fresh new WSL machine up:

   > ⚠️ Warning: this removes your WSL machine and recreates it fresh.

   ```powershell
   $GIT_ROOT = git rev-parse --show-toplevel
   & "$GIT_ROOT\.scripts\bootstrap-dev-env.ps1"
   ```

3. Clone the repo, and open VSCode in it:

   ```bash
   cd ~/
   
   read -p "Enter your name (e.g. 'FirstName LastName'): " user_name
   read -p "Enter your github email (e.g. 'your-github-alias@blah.com'): " user_email
   read -p "Enter the branch to switch to: (e.g. 'main') " branch_name
    
   git clone https://github.com/microsoft/dbt-scope.git
   
   git config --global user.name "$user_name"
   git config --global user.email "$user_email"
   
   cd dbt-scope/

   git pull origin
   git checkout "$branch_name"
   code .
   ```

4. Run the bootstrapper script from inside the WSL, that installs all tools idempotently:

   ```bash
   GIT_ROOT=$(git rev-parse --show-toplevel)
   chmod +x ${GIT_ROOT}/.scripts/bootstrap-dev-env.sh && ${GIT_ROOT}/contrib/bootstrap-dev-env.sh
   ```

## Quick start

```powershell
winget install -e --id Astral-sh.uv # one-time install for uv
cp .env.example .env   # fill in your ADLA/storage values
.\.scripts\run.ps1 all # uv venv, sync, build, lint, unit-test, debug, integration-test
```

## Dev script

```powershell
.\.scripts\run.ps1 <target>

```

| Target             | What it does                                    | Cloud? |
| ------------------ | ----------------------------------------------- | ------ |
| `venv`             | Create a fresh uv-managed `.venv`               | No     |
| `install`          | `uv sync --extra dev`                           | No     |
| `build`            | Build wheel to `dist/`                          | No     |
| `lint`             | `ruff check + format --check`                   | No     |
| `fix`              | `ruff auto-fix + format`                        | No     |
| `unit-test`        | `pytest tests/unit/` (fast, no credentials)     | No     |
| `debug`            | `dbt debug` against test project                | Yes    |
| `integration-test` | `pytest tests/integration/` (datagen + dbt run) | Yes    |
| `all`              | All of the above in sequence                    | Yes    |

Each target is idempotent — auto-creates the uv-managed `.venv` and syncs deps if missing.

## Running tests

```powershell
.\.scripts\run.ps1 unit-test          # fast, no credentials
.\.scripts\run.ps1 integration-test   # generates SS data via datagen, runs dbt, verifies Delta
```

Integration tests are self-contained — they generate their own SS test data on Cosmos via ADLA, run dbt models against it, and verify the resulting Delta tables. The only prerequisites are ADLA + ADLS + `az login`.

## Note

* Test data auto-expires via `STREAMEXPIRY` (7 days). Delta tables are left for manual cleanup.
