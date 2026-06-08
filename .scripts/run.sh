#!/bin/bash
#
# dbt-scope dev/test script. Requires uv, Python 3.10+, and az login.
#
# Usage: .scripts/run.sh <target>
#
# Targets: venv | install | build | lint | fix | unit-test | integration-test | debug | all
#
# Each target is idempotent — auto-creates the uv-managed venv if missing
# and syncs deps as needed.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${PROJECT_DIR}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"
VENV_DBT="${VENV_DIR}/bin/dbt"
TEST_PROJECT_DIR="${PROJECT_DIR}/tests/integration/dbt_project"
REQUIRED_PYTHON="3.10"

declare -A TARGETS=(
    ["venv"]="Create a fresh uv-managed .venv"
    ["install"]="uv sync --extra dev"
    ["build"]="Build wheel to dist/"
    ["upload"]="Build wheel and azcopy to static site"
    ["lint"]="ruff check + format --check (auto-fixes first)"
    ["fix"]="ruff auto-fix + format"
    ["unit-test"]="pytest tests/unit/ (fast, no credentials)"
    ["debug"]="dbt debug against test project"
    ["integration-test"]="pytest tests/integration/ (ADLA + az login)"
    ["all"]="Run all targets in sequence"
)
TARGET_ORDER=("venv" "install" "build" "upload" "lint" "unit-test" "debug" "integration-test")

# ── Usage ────────────────────────────────────────────────────────────────────

print_usage() {
    echo
    printf "  %-20s %s\n" "TARGET" "DESCRIPTION"
    printf "  %-20s %s\n" "all" "${TARGETS[all]}"
    for t in "${TARGET_ORDER[@]}"; do printf "  %-20s %s\n" "$t" "${TARGETS[$t]}"; done
    echo
    echo "Usage: .scripts/run.sh <target>"
    echo
}

TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then echo "ERROR: No target provided."; print_usage; exit 1; fi
if [[ "$TARGET" != "all" && "$TARGET" != "fix" && -z "${TARGETS[$TARGET]:-}" ]]; then
    echo "ERROR: Unknown target '$TARGET'"
    print_usage
    exit 1
fi

# ── Load .env ────────────────────────────────────────────────────────────────

load_env_file() {
    local env_file="${PROJECT_DIR}/.env"
    if [[ ! -f "$env_file" ]]; then
        echo -e "\033[33mWARNING: .env file not found. Copy .env.example to .env and fill in values.\033[0m"
        echo -e "\033[33m         cp .env.example .env\033[0m"
        return
    fi
    set -a
    # shellcheck disable=SC1090
    source "$env_file"
    set +a
}

load_env_file

# ── Logging setup ────────────────────────────────────────────────────────────

LOGS_DIR="${PROJECT_DIR}/.logs"
rm -rf "$LOGS_DIR"
mkdir -p "$LOGS_DIR"
TRANSCRIPT_FILE="${LOGS_DIR}/bash_${TARGET}.log"

# ── Assertions ───────────────────────────────────────────────────────────────

assert_env_var() {
    local name="$1"
    local val="${!name:-}"
    if [[ -z "$val" ]]; then
        echo -e "\033[31mERROR: Environment variable $name is not set. Check your .env file.\033[0m"
        exit 1
    fi
    echo "$val"
}

assert_uv() {
    if ! command -v uv &>/dev/null; then
        echo -e "\033[31mERROR: uv is not installed. Install with: curl -LsSf https://astral.sh/uv/install.sh | sh\033[0m"
        exit 1
    fi
}

assert_az() {
    if ! az account show &>/dev/null; then
        echo -e "\033[31mERROR: Not logged into Azure CLI. Run 'az login' first.\033[0m"
        exit 1
    fi
}

# ── Helpers ──────────────────────────────────────────────────────────────────

write_step() {
    echo ""
    echo -e "\033[36m--- $1 ---\033[0m"
}

ensure_venv() {
    assert_uv
    if [[ ! -f "$VENV_PYTHON" ]]; then
        run_venv
    fi
}

ensure_installed() {
    ensure_venv
    if [[ ! -f "$VENV_DBT" ]]; then
        write_step "Installing dbt-scope (dbt CLI not found in venv)"
        run_install
    else
        cd "$PROJECT_DIR"
        uv run --no-sync python -c \
            "from dbt.adapters.scope import Plugin; print(f'  dbt-scope {Plugin.adapter.ConnectionManager.TYPE} adapter loaded')"
    fi
}

# ── Targets ──────────────────────────────────────────────────────────────────

run_venv() {
    write_step "venv: Creating fresh uv-managed virtual environment"
    rm -rf "$VENV_DIR"
    cd "$PROJECT_DIR"
    uv venv "$VENV_DIR" --python "$REQUIRED_PYTHON"
    echo "  venv created at $VENV_DIR"
}

run_install() {
    write_step "install: Syncing dbt-scope environment with uv"
    ensure_venv
    cd "$PROJECT_DIR"
    uv sync --extra dev
    uv run --no-sync python -c \
        "from dbt.adapters.scope import Plugin; print(f'  dbt-scope {Plugin.adapter.ConnectionManager.TYPE} adapter loaded')"
}

run_build() {
    write_step "build: Building wheel to dist/"
    ensure_installed
    local dist_dir="${PROJECT_DIR}/dist"
    rm -rf "$dist_dir"
    cd "$PROJECT_DIR"
    uv build --wheel --out-dir "$dist_dir"
    local whl
    whl=$(ls "$dist_dir"/*.whl 2>/dev/null | head -1)
    local size
    size=$(du -k "$whl" | cut -f1)
    echo "  Built: $(basename "$whl") (${size} KB)"
}

run_upload() {
    write_step "upload: Building wheel and uploading to static storage"
    run_build
    local dist_dir="${PROJECT_DIR}/dist"
    local whl
    whl=$(ls "$dist_dir"/*.whl 2>/dev/null | head -1)
    if [[ -z "$whl" ]]; then
        echo -e "\033[31mERROR: No .whl found in $dist_dir after build.\033[0m"
        exit 1
    fi
    local whl_name
    whl_name=$(basename "$whl")
    echo "  Uploading ${whl_name} → arcdataciadomisc/\$web/whls/${whl_name}"
    az storage blob upload \
        --account-name arcdataciadomisc \
        --container-name '$web' \
        --name "whls/${whl_name}" \
        --file "$whl" \
        --overwrite \
        --auth-mode login
    echo "  Uploaded: https://arcdataciadomisc.z13.web.core.windows.net/whls/${whl_name}"
}

run_lint() {
    write_step "lint: auto-fix + format, then verify"
    ensure_installed
    cd "$PROJECT_DIR"
    uv run --no-sync ruff check --fix dbt/ tests/
    uv run --no-sync ruff format dbt/ tests/

    local check_exit=0 fmt_exit=0
    uv run --no-sync ruff check dbt/ tests/ || check_exit=$?
    uv run --no-sync ruff format --check dbt/ tests/ || fmt_exit=$?

    if [[ $check_exit -ne 0 || $fmt_exit -ne 0 ]]; then
        echo -e "\033[31mLint failed — unfixable issues remain\033[0m"
        return 1
    fi
    echo "  Lint passed."
}

run_fix() {
    write_step "fix: ruff auto-fix + format"
    ensure_installed
    cd "$PROJECT_DIR"
    uv run --no-sync ruff check --fix dbt/ tests/
    uv run --no-sync ruff format dbt/ tests/
    echo "  Fixed."
}

run_unit_test() {
    write_step "unit-test: Running pytest tests/unit/"
    ensure_installed
    cd "$PROJECT_DIR"
    uv run --no-sync pytest "${PROJECT_DIR}/tests/unit" -v
}

run_debug() {
    write_step "debug: Running dbt debug against test project"
    ensure_installed
    assert_az
    cd "$PROJECT_DIR"
    uv run --no-sync dbt debug \
        --project-dir "$TEST_PROJECT_DIR" \
        --profiles-dir "$TEST_PROJECT_DIR"
}

run_integration_test() {
    write_step "integration-test: Running pytest tests/integration/ against ADLA (parallel)"
    ensure_installed
    assert_az

    local start_time=$SECONDS
    local num_cores
    num_cores=$(nproc)
    echo -e "\033[36m  Using $num_cores parallel workers (logical cores)\033[0m"

    cd "$PROJECT_DIR"
    uv run --no-sync pytest "${PROJECT_DIR}/tests/integration" -v -s --timeout=3600 -n "$num_cores"

    local elapsed=$(( SECONDS - start_time ))
    local h=$(( elapsed / 3600 ))
    local m=$(( (elapsed % 3600) / 60 ))
    local s=$(( elapsed % 60 ))
    printf "\n\033[32m  Integration tests completed in %02d:%02d:%02d\033[0m\n" $h $m $s

    if [[ -d "$LOGS_DIR" ]]; then
        local log_count
        log_count=$(find "$LOGS_DIR" -type f | wc -l)
        local log_dirs
        log_dirs=$(find "$LOGS_DIR" -mindepth 1 -type d | wc -l)
        echo -e "\033[36m  Logs: $log_count files across $log_dirs test directories in .logs/\033[0m"
    fi
}

# ── Dispatch ─────────────────────────────────────────────────────────────────

echo -e "\033[32m=== dbt-scope: ${TARGET} ===\033[0m"

# Start transcript (tee to log file)
exec > >(tee -a "$TRANSCRIPT_FILE") 2>&1

if [[ "$TARGET" == "all" ]]; then
    for t in "${TARGET_ORDER[@]}"; do
        "run_${t//-/_}"
    done
    echo ""
    echo -e "\033[32m=== All targets completed. ===\033[0m"
else
    "run_${TARGET//-/_}"
fi
