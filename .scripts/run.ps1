#Requires -Version 5.1
<#
.SYNOPSIS
    dbt-scope dev/test script. Requires uv, Python 3.10+, and az login.

.DESCRIPTION
    Single entry-point for all development tasks. Each target is idempotent —
    auto-creates the uv-managed venv if missing and syncs deps as needed.

.PARAMETER Target
    The task to run: venv | install | unit-test | integration-test | debug | all

.EXAMPLE
    .\.scripts\run.ps1 all
    .\.scripts\run.ps1 unit-test
    .\.scripts\run.ps1 integration-test
#>

param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("venv", "install", "build", "lint", "fix", "unit-test", "integration-test", "debug", "all")]
    [string]$Target
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectDir = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$VenvDir = Join-Path $ProjectDir ".venv"
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
$VenvDbt = Join-Path $VenvDir "Scripts\dbt.exe"
$TestProjectDir = Join-Path $ProjectDir "tests\integration\dbt_project"
$RequiredPython = "3.10"

# ── Load .env ────────────────────────────────────────────────────────────────

function Load-EnvFile {
    $envFile = Join-Path $ProjectDir ".env"
    if (-not (Test-Path $envFile)) {
        Write-Host "WARNING: .env file not found. Copy .env.example to .env and fill in values." -ForegroundColor Yellow
        Write-Host "         cp .env.example .env" -ForegroundColor Yellow
        return
    }
    Get-Content $envFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#")) {
            $parts = $line -split "=", 2
            if ($parts.Count -eq 2) {
                [Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), "Process")
            }
        }
    }
}

Load-EnvFile

# ── Logging setup ────────────────────────────────────────────────────────────

$LogsDir = Join-Path $ProjectDir ".logs"
if (Test-Path $LogsDir) { Remove-Item $LogsDir -Recurse -Force }
New-Item -Path $LogsDir -ItemType Directory -Force | Out-Null
$TranscriptFile = Join-Path $LogsDir "powershell_$Target.log"

function Assert-EnvVar([string]$name) {
    $val = [Environment]::GetEnvironmentVariable($name, "Process")
    if (-not $val) {
        Write-Host "ERROR: Environment variable $name is not set. Check your .env file." -ForegroundColor Red
        exit 1
    }
    return $val
}

function Assert-Uv {
    if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
        Write-Host "ERROR: uv is not installed. Install it with 'winget install -e --id Astral-sh.uv' and rerun this script." -ForegroundColor Red
        exit 1
    }
}

# ── Helpers ──────────────────────────────────────────────────────────────────

function Write-Step([string]$msg) {
    Write-Host ""
    Write-Host "--- $msg ---" -ForegroundColor Cyan
}

function Assert-Az {
    try {
        $null = az account show 2>$null
    }
    catch {
        Write-Host "ERROR: Not logged into Azure CLI. Run 'az login' first." -ForegroundColor Red
        exit 1
    }
}

function Ensure-Venv {
    Assert-Uv
    if (-not (Test-Path $VenvPython)) {
        Invoke-Venv
    }
}

function Ensure-Installed {
    Ensure-Venv
    if (-not (Test-Path $VenvDbt)) {
        Write-Step "Installing dbt-scope (dbt CLI not found in venv)"
        Invoke-Install
    }
    else {
        Push-Location $ProjectDir
        try {
            & uv run --no-sync python -c "from dbt.adapters.scope import Plugin; print(f'  dbt-scope {Plugin.adapter.ConnectionManager.TYPE} adapter loaded')"
        }
        finally {
            Pop-Location
        }
        if ($LASTEXITCODE -ne 0) { throw "Adapter import failed" }
    }
}

# ── Targets ──────────────────────────────────────────────────────────────────

function Invoke-Venv {
    Write-Step "venv: Creating fresh uv-managed virtual environment"
    if (Test-Path $VenvDir) { Remove-Item $VenvDir -Recurse -Force }
    Push-Location $ProjectDir
    try {
        & uv venv $VenvDir --python $RequiredPython
    }
    finally {
        Pop-Location
    }
    if ($LASTEXITCODE -ne 0) { throw "Failed to create venv with uv" }
    Write-Host "  venv created at $VenvDir"
}

function Invoke-Install {
    Write-Step "install: Syncing dbt-scope environment with uv"
    Ensure-Venv
    Push-Location $ProjectDir
    try {
        & uv sync --extra dev
        if ($LASTEXITCODE -ne 0) { throw "uv sync failed" }
        & uv run --no-sync python -c "from dbt.adapters.scope import Plugin; print(f'  dbt-scope {Plugin.adapter.ConnectionManager.TYPE} adapter loaded')"
    }
    finally {
        Pop-Location
    }
    if ($LASTEXITCODE -ne 0) { throw "Adapter import failed" }
}

function Invoke-Build {
    Write-Step "build: Building wheel to dist/"
    Ensure-Installed
    $distDir = Join-Path $ProjectDir "dist"
    if (Test-Path $distDir) { Remove-Item $distDir -Recurse -Force }
    Push-Location $ProjectDir
    try {
        & uv build --wheel --out-dir $distDir
    }
    finally {
        Pop-Location
    }
    if ($LASTEXITCODE -ne 0) { throw "Wheel build failed" }
    $whl = Get-ChildItem $distDir -Filter "*.whl" | Select-Object -First 1
    Write-Host "  Built: $($whl.Name) ($([math]::Round($whl.Length / 1KB, 1)) KB)"
}

function Invoke-Lint {
    Write-Step "lint: auto-fix + format, then verify"
    Ensure-Installed
    Push-Location $ProjectDir
    try {
        & uv run --no-sync ruff check --fix dbt/ tests/
        & uv run --no-sync ruff format dbt/ tests/
        & uv run --no-sync ruff check dbt/ tests/
        $checkExit = $LASTEXITCODE
        & uv run --no-sync ruff format --check dbt/ tests/
        $fmtExit = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }
    if ($checkExit -ne 0 -or $fmtExit -ne 0) { throw "Lint failed — unfixable issues remain" }
    Write-Host "  Lint passed."
}

function Invoke-Fix {
    Write-Step "fix: ruff auto-fix + format"
    Ensure-Installed
    Push-Location $ProjectDir
    try {
        & uv run --no-sync ruff check --fix dbt/ tests/
        & uv run --no-sync ruff format dbt/ tests/
    }
    finally {
        Pop-Location
    }
    Write-Host "  Fixed."
}

function Invoke-UnitTest {
    Write-Step "unit-test: Running pytest tests/unit/"
    Ensure-Installed
    Push-Location $ProjectDir
    try {
        & uv run --no-sync pytest (Join-Path $ProjectDir "tests\unit") -v
    }
    finally {
        Pop-Location
    }
    if ($LASTEXITCODE -ne 0) { throw "Unit tests failed" }
}

function Invoke-Debug {
    Write-Step "debug: Running dbt debug against test project"
    Ensure-Installed
    Assert-Az
    Push-Location $ProjectDir
    try {
        & uv run --no-sync dbt debug `
            --project-dir $TestProjectDir `
            --profiles-dir $TestProjectDir
    }
    finally {
        Pop-Location
    }
    if ($LASTEXITCODE -ne 0) { throw "dbt debug failed" }
}

function Invoke-Integrationtest {
    Write-Step "integration-test: Running pytest tests/integration/ against ADLA (parallel)"
    Ensure-Installed
    Assert-Az

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $numCores = (Get-CimInstance -ClassName Win32_Processor | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum
    Write-Host "  Using $numCores parallel workers (logical cores)" -ForegroundColor Cyan
    Push-Location $ProjectDir
    try {
        & uv run --no-sync pytest (Join-Path $ProjectDir "tests\integration") -v -s --timeout=3600 -n $numCores
    }
    finally {
        Pop-Location
    }
    $sw.Stop()

    Write-Host ""
    Write-Host "  Integration tests completed in $($sw.Elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor Green

    if (Test-Path $LogsDir) {
        $logCount = (Get-ChildItem -Path $LogsDir -Recurse -File | Measure-Object).Count
        $logDirs = (Get-ChildItem -Path $LogsDir -Directory | Measure-Object).Count
        Write-Host "  Logs: $logCount files across $logDirs test directories in .logs/" -ForegroundColor Cyan
    }

    if ($LASTEXITCODE -ne 0) { throw "Integration tests failed" }
}

# ── Dispatch ─────────────────────────────────────────────────────────────────

$targets = @("venv", "install", "build", "lint", "unit-test", "debug", "integration-test")

Write-Host "=== dbt-scope: $Target ===" -ForegroundColor Green

Start-Transcript -Path $TranscriptFile -Force

try {
    if ($Target -eq "all") {
        foreach ($t in $targets) {
            & "Invoke-$($t -replace '-','')" *>&1 | ForEach-Object { $_ }
        }
        Write-Host ""
        Write-Host "=== All targets completed. ===" -ForegroundColor Green
    }
    else {
        $funcName = "Invoke-$($Target -replace '-','')"
        & $funcName
    }
} finally {
    Stop-Transcript
}
