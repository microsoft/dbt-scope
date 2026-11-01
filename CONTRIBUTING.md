# Contributing to dbt-scope

## One-shot Windows setup

Use an elevated PowerShell 7 terminal with Git for Windows (including Git Credential Manager),
VS Code, WSL, and [Task](https://taskfile.dev/installation/) installed. Clone this repo on Windows
and check out a branch that has been pushed to origin. Git must be able to read it without a prompt.

> **Warning:** the default task deletes and recreates **Ubuntu-24.04**. Use `up` to preserve an
> existing workspace. Other installed WSL distros are not unregistered.

```powershell
$GIT_ROOT = git rev-parse --show-toplevel
task --taskfile "$GIT_ROOT\.scripts\bootstrap-dev-env.yaml"
```

This creates the Linux user without first-run prompts, clones the selected Windows branch into
`/workspaces/dbt-scope`, installs the Linux tools and development dependencies, and opens VS Code
in WSL. There is no devcontainer. The PowerShell script also accepts `-Branch` and `-LinuxUser`
when invoked directly; the Taskfile runs the Linux setup afterward.

After a restart, or to refresh dependencies without deleting the checkout:

```powershell
task --taskfile "$GIT_ROOT\.scripts\bootstrap-dev-env.yaml" up
```

`up` does not reset files, switch branches, or reclone the repository.

## Linux and CI setup

The same Linux script works in an existing checkout, including a non-WSL CI runner:

```bash
bash .scripts/bootstrap-dev-env.sh
export PATH="$HOME/.local/bin:$PATH"
```

It installs native Linux Azure CLI and uv as needed and runs `uv sync --extra dev`.
Setup does not log in to Azure, read `.env`, run cloud tests, or publish a wheel.
GitHub Copilot CLI and VS Code extensions are installed when applicable in WSL.

For manual development, sign in with `gh auth login` and authorize the Microsoft organization
before pushing. Copy `.env.example` to `.env` and fill in the values before cloud tests, then
authenticate with Azure CLI. Never commit `.env`.

When driven by repo-bootstrapper, place `.env` in its gitignored `sync/dbt-scope/` directory.
The service syncs it into `/workspaces/dbt-scope/.env` and sets up the configured GitHub identity
after the Taskfile completes. Relay-based Azure authentication, when used, is separate from
bootstrapping, as it is in `.github/actions/auth`.
The dev/test entry point accepts either LF or Windows CRLF line endings in `.env` without
rewriting the file.

## Quick start

```bash
bash .scripts/bootstrap-dev-env.sh
export PATH="$HOME/.local/bin:$PATH"
.scripts/run.sh build
.scripts/run.sh lint
.scripts/run.sh unit-test
```

## Dev script

All development tasks go through a single entry-point — [`.scripts/run.sh`](.scripts/run.sh):

```bash
.scripts/run.sh <target>
```

| Target             | What it does                                    | Cloud? |
| ------------------ | ----------------------------------------------- | ------ |
| `venv`             | Create a fresh uv-managed `.venv`               | No     |
| `install`          | `uv sync --extra dev`                           | No     |
| `build`            | Build wheel to `dist/`                          | No     |
| `upload`           | Build and publish wheel to the existing static site | Yes |
| `lint`             | `ruff check + format --check`                   | No     |
| `fix`              | `ruff auto-fix + format`                        | No     |
| `unit-test`        | `pytest tests/unit/` (fast, no credentials)     | No     |
| `debug`            | `dbt debug` against test project                | Yes    |
| `integration-test` | `pytest tests/integration/` (datagen + dbt run) | Yes    |
| `all`              | Includes upload as well as local and cloud checks | Yes |

Each target is idempotent — auto-creates the uv-managed `.venv` and syncs deps if missing.

Publishing is explicit: `.scripts/run.sh upload` uses Azure CLI authentication to upload the
current version to `arcdataciadomisc/$web/whls/`, overwriting the same wheel filename.
Do not use `all` for a non-publishing validation run: it also invokes `upload`.
Neither bootstrap task publishes.

## Running tests

```bash
.scripts/run.sh unit-test          # fast, no credentials
.scripts/run.sh integration-test   # generates SS data via datagen, runs dbt, verifies Delta
.scripts/run.sh upload             # build and publish wheel
```

Integration tests are self-contained — they generate their own SS test data on Cosmos via ADLA, run dbt models against it, and verify the resulting Delta tables. The only prerequisites are ADLA + ADLS + `az login`.

## Note

* Test data auto-expires via `STREAMEXPIRY` (7 days). Delta tables are left for manual cleanup.
