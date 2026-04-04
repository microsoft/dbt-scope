#Requires -Version 5.1
<#
.SYNOPSIS
    Ralph Wiggum — long-running Copilot agent loop for dbt-scope.

.DESCRIPTION
    Iteratively invokes the Copilot CLI with a prompt file until the agent
    emits a completion signal ({ "status": "Succeeded" } or { "status": "Failed" })
    or the maximum number of iterations is exhausted.

.PARAMETER PromptFile
    Path to a markdown file containing the prompt to pipe into copilot.

.PARAMETER Iterations
    Maximum number of iterations (default: 30).

.PARAMETER SkipTo
    Optional instruction prepended to the prompt to skip to a specific step.

.PARAMETER Mcp
    Optional MCP server names to pass to copilot (--mcp).

.EXAMPLE
    .\.scripts\ralph.ps1 .\.github\skills\ralph-dbt-scope\skill.md
    .\.scripts\ralph.ps1 .\.github\skills\ralph-dbt-scope\skill.md -Iterations 10
    .\.scripts\ralph.ps1 .\.github\skills\ralph-dbt-scope\skill.md -SkipTo "Step 3"
#>

param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$PromptFile,

    [Parameter()]
    [int]$Iterations = 30,

    [Parameter()]
    [string]$SkipTo,

    [Parameter()]
    [string[]]$Mcp
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Read prompt file ─────────────────────────────────────────────────────────

if (-not (Test-Path $PromptFile)) {
    Write-Host "Error: Cannot read '$PromptFile'" -ForegroundColor Red
    exit 1
}

$prompt = Get-Content -Path $PromptFile -Raw -Encoding UTF8

if ($SkipTo) {
    $prompt = "**INSTRUCTION: $SkipTo** — Skip earlier steps and begin from this point.`n`n$prompt"
}

# ── Helpers ──────────────────────────────────────────────────────────────────

function Parse-CompletionSignal([string]$output) {
    $lines = ($output.TrimEnd()) -split "`n"
    $start = [Math]::Max(0, $lines.Length - 20)
    for ($i = $lines.Length - 1; $i -ge $start; $i--) {
        $line = $lines[$i].Trim()
        if ($line -match '^\{\s*"status"\s*:\s*"(Succeeded|Failed)"\s*\}$') {
            return $Matches[1]
        }
    }
    return $null
}

$separator = "=" * 63

# ── Main loop ────────────────────────────────────────────────────────────────

Write-Host "Starting Ralph — Prompt: $PromptFile — Max iterations: $Iterations"
if ($SkipTo) { Write-Host "Skip-to: $SkipTo" }
if ($Mcp) { Write-Host "MCP servers: $($Mcp -join ', ')" }

for ($i = 1; $i -le $Iterations; $i++) {
    Write-Host ""
    Write-Host $separator
    Write-Host "  Ralph Iteration $i of $Iterations"
    Write-Host $separator

    # Build copilot arguments
    $copilotArgs = @()
    if ($Mcp) {
        foreach ($server in $Mcp) {
            $copilotArgs += "--mcp"
            $copilotArgs += $server
        }
    }
    $copilotArgs += "-p"
    $copilotArgs += $prompt
    $copilotArgs += "--yolo"

    # Run copilot directly — streams to terminal and captures output
    $sb = [System.Text.StringBuilder]::new()
    & copilot @copilotArgs 2>&1 | ForEach-Object {
        $line = if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.ToString() } else { $_ }
        Write-Host $line
        [void]$sb.AppendLine($line)
    }
    $exitCode = $LASTEXITCODE
    $output = $sb.ToString()

    # Check for completion signal
    $signal = Parse-CompletionSignal $output

    if ($signal -eq "Succeeded") {
        Write-Host ""
        Write-Host $separator
        Write-Host "  Ralph completed successfully!"
        Write-Host "  Completed at iteration $i of $Iterations"
        Write-Host $separator
        exit 0
    }

    if ($signal -eq "Failed") {
        Write-Host ""
        Write-Host $separator
        Write-Host "  Ralph reported failure."
        Write-Host "  Failed at iteration $i of $Iterations"
        Write-Host $separator
        exit 1
    }

    # If copilot itself crashed (non-zero exit, no signal), warn but continue
    if ($exitCode -ne 0) {
        Write-Host "WARNING: copilot exited with code $exitCode and no completion signal." -ForegroundColor Yellow
    }

    Write-Host "Iteration $i complete — no completion signal found. Continuing..."
    Start-Sleep -Seconds 2
}

Write-Host ""
Write-Host "Ralph reached max iterations ($Iterations) without completing."
exit 1
