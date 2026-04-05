#!/bin/bash
#
# Ralph Wiggum — long-running Copilot agent loop for dbt-scope.
#
# Iteratively invokes the Copilot CLI with a prompt file until the agent
# emits a completion signal ({ "status": "Succeeded" } or { "status": "Failed" })
# or the maximum number of iterations is exhausted.
#
# Usage:
#   .scripts/ralph.sh <prompt-file> [--iterations N] [--skip-to "Step X"] [--mcp name]
#
# Examples:
#   .scripts/ralph.sh .github/skills/ralph-dbt-scope/skill.md
#   .scripts/ralph.sh .github/skills/ralph-dbt-scope/skill.md --iterations 10
#   .scripts/ralph.sh .github/skills/ralph-dbt-scope/skill.md --skip-to "Step 3"
#
set -euo pipefail

# ── Parse arguments ──────────────────────────────────────────────────────────

PROMPT_FILE=""
ITERATIONS=30
SKIP_TO=""
MCP_SERVERS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --iterations)
            ITERATIONS="$2"; shift 2 ;;
        --skip-to)
            SKIP_TO="$2"; shift 2 ;;
        --mcp)
            MCP_SERVERS+=("$2"); shift 2 ;;
        -*)
            echo "ERROR: Unknown option '$1'" >&2; exit 1 ;;
        *)
            if [[ -z "$PROMPT_FILE" ]]; then
                PROMPT_FILE="$1"; shift
            else
                echo "ERROR: Unexpected argument '$1'" >&2; exit 1
            fi
            ;;
    esac
done

if [[ -z "$PROMPT_FILE" ]]; then
    echo "ERROR: No prompt file provided." >&2
    echo "Usage: .scripts/ralph.sh <prompt-file> [--iterations N] [--skip-to \"Step X\"] [--mcp name]" >&2
    exit 1
fi

if [[ ! -f "$PROMPT_FILE" ]]; then
    echo "Error: Cannot read '$PROMPT_FILE'" >&2
    exit 1
fi

# ── Read prompt file ─────────────────────────────────────────────────────────

prompt=$(<"$PROMPT_FILE")

if [[ -n "$SKIP_TO" ]]; then
    prompt="**INSTRUCTION: ${SKIP_TO}** — Skip earlier steps and begin from this point.

${prompt}"
fi

# ── Helpers ──────────────────────────────────────────────────────────────────

parse_completion_signal() {
    local output="$1"
    # Look at the last 20 lines for a completion signal
    local signal
    signal=$(echo "$output" | tail -20 | grep -oP '^\s*\{\s*"status"\s*:\s*"(Succeeded|Failed)"\s*\}\s*$' | tail -1 || true)
    if [[ -n "$signal" ]]; then
        echo "$signal" | grep -oP '(Succeeded|Failed)'
    fi
}

SEPARATOR="==============================================================="

# ── Main loop ────────────────────────────────────────────────────────────────

echo "Starting Ralph — Prompt: $PROMPT_FILE — Max iterations: $ITERATIONS"
if [[ -n "$SKIP_TO" ]]; then echo "Skip-to: $SKIP_TO"; fi
if [[ ${#MCP_SERVERS[@]} -gt 0 ]]; then echo "MCP servers: $(IFS=', '; echo "${MCP_SERVERS[*]}")"; fi

for (( i=1; i<=ITERATIONS; i++ )); do
    echo ""
    echo "$SEPARATOR"
    echo "  Ralph Iteration $i of $ITERATIONS"
    echo "$SEPARATOR"

    # Build copilot arguments
    copilot_args=()
    for server in "${MCP_SERVERS[@]}"; do
        copilot_args+=("--mcp" "$server")
    done
    copilot_args+=("-p" "$prompt" "--yolo")

    # Run copilot — stream to terminal and capture output
    output=""
    exit_code=0
    output=$(copilot "${copilot_args[@]}" 2>&1 | tee /dev/stderr) || exit_code=$?

    # Check for completion signal
    signal=$(parse_completion_signal "$output")

    if [[ "$signal" == "Succeeded" ]]; then
        echo ""
        echo "$SEPARATOR"
        echo "  Ralph completed successfully!"
        echo "  Completed at iteration $i of $ITERATIONS"
        echo "$SEPARATOR"
        exit 0
    fi

    if [[ "$signal" == "Failed" ]]; then
        echo ""
        echo "$SEPARATOR"
        echo "  Ralph reported failure."
        echo "  Failed at iteration $i of $ITERATIONS"
        echo "$SEPARATOR"
        exit 1
    fi

    # If copilot itself crashed (non-zero exit, no signal), warn but continue
    if [[ $exit_code -ne 0 ]]; then
        echo -e "\033[33mWARNING: copilot exited with code $exit_code and no completion signal.\033[0m"
    fi

    echo "Iteration $i complete — no completion signal found. Continuing..."
    sleep 2
done

echo ""
echo "Ralph reached max iterations ($ITERATIONS) without completing."
exit 1
