#Requires -Version 5.1
<#
.SYNOPSIS
    dbt-scope dev/test script. Requires Python 3.10+ and az login.

.DESCRIPTION
    Single entry-point for all development tasks. Each target is idempotent —
    auto-creates the venv if missing, installs deps if needed.

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
$VenvPip = Join-Path $VenvDir "Scripts\pip.exe"
$TestProjectDir = Join-Path $ProjectDir "tests\integration\dbt_project"

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

function Assert-EnvVar([string]$name) {
    $val = [Environment]::GetEnvironmentVariable($name, "Process")
    if (-not $val) {
        Write-Host "ERROR: Environment variable $name is not set. Check your .env file." -ForegroundColor Red
        exit 1
    }
    return $val
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
    if (-not (Test-Path $VenvPython)) {
        Invoke-Venv
    }
}

function Ensure-Installed {
    Ensure-Venv
    if (-not (Test-Path (Join-Path $VenvDir "Scripts\dbt.exe"))) {
        Write-Step "Installing dbt-scope (dbt CLI not found in venv)"
        & $VenvPip install -e "$ProjectDir[dev]" --quiet
        if ($LASTEXITCODE -ne 0) { throw "pip install failed" }
        & $VenvPython -c "from dbt.adapters.scope import Plugin; print(f'  dbt-scope {Plugin.adapter.ConnectionManager.TYPE} adapter loaded')"
        if ($LASTEXITCODE -ne 0) { throw "Adapter import failed" }
    }
}

# ── Targets ──────────────────────────────────────────────────────────────────

function Invoke-Venv {
    Write-Step "venv: Creating fresh virtual environment"
    if (Test-Path $VenvDir) { Remove-Item $VenvDir -Recurse -Force }
    & python -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) { throw "Failed to create venv" }
    & $VenvPython -m pip install --upgrade pip --quiet 2>$null
    Write-Host "  venv created at $VenvDir"
}

function Invoke-Install {
    Write-Step "install: Installing dbt-scope in editable mode"
    Ensure-Venv
    & $VenvPip install -e "$ProjectDir[dev]" --quiet
    if ($LASTEXITCODE -ne 0) { throw "pip install failed" }
    & $VenvPython -c "from dbt.adapters.scope import Plugin; print(f'  dbt-scope {Plugin.adapter.ConnectionManager.TYPE} adapter loaded')"
    if ($LASTEXITCODE -ne 0) { throw "Adapter import failed" }
}

function Invoke-Build {
    Write-Step "build: Building wheel to dist/"
    Ensure-Installed
    $distDir = Join-Path $ProjectDir "dist"
    if (Test-Path $distDir) { Remove-Item $distDir -Recurse -Force }
    & $VenvPython -m pip install build --quiet
    & $VenvPython -m build --wheel --outdir $distDir $ProjectDir
    if ($LASTEXITCODE -ne 0) { throw "Wheel build failed" }
    $whl = Get-ChildItem $distDir -Filter "*.whl" | Select-Object -First 1
    Write-Host "  Built: $($whl.Name) ($([math]::Round($whl.Length / 1KB, 1)) KB)"
}

function Invoke-Lint {
    Write-Step "lint: auto-fix + format, then verify"
    Ensure-Installed
    & $VenvPython -m ruff check --fix dbt/ tests/
    & $VenvPython -m ruff format dbt/ tests/
    & $VenvPython -m ruff check dbt/ tests/
    $checkExit = $LASTEXITCODE
    & $VenvPython -m ruff format --check dbt/ tests/
    $fmtExit = $LASTEXITCODE
    if ($checkExit -ne 0 -or $fmtExit -ne 0) { throw "Lint failed — unfixable issues remain" }
    Write-Host "  Lint passed."
}

function Invoke-Fix {
    Write-Step "fix: ruff auto-fix + format"
    Ensure-Installed
    & $VenvPython -m ruff check --fix dbt/ tests/
    & $VenvPython -m ruff format dbt/ tests/
    Write-Host "  Fixed."
}

function Invoke-UnitTest {
    Write-Step "unit-test: Running pytest tests/unit/"
    Ensure-Installed
    & $VenvPython -m pytest (Join-Path $ProjectDir "tests\unit") -v
    if ($LASTEXITCODE -ne 0) { throw "Unit tests failed" }
}

function Invoke-Debug {
    Write-Step "debug: Running dbt debug against test project"
    Ensure-Installed
    Assert-Az
    & (Join-Path $VenvDir "Scripts\dbt.exe") debug `
        --project-dir $TestProjectDir `
        --profiles-dir $TestProjectDir
    if ($LASTEXITCODE -ne 0) { throw "dbt debug failed" }
}

function Invoke-Integrationtest {
    Write-Step "integration-test: Running pytest tests/integration/ against ADLA (parallel)"
    Ensure-Installed
    Assert-Az

    $logsDir = Join-Path $ProjectDir ".logs"
    if (Test-Path $logsDir) { Remove-Item $logsDir -Recurse -Force }
    New-Item -Path $logsDir -ItemType Directory -Force | Out-Null

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    & $VenvPython -m pytest (Join-Path $ProjectDir "tests\integration") -v -s --timeout=3600 -n 4
    $sw.Stop()

    Write-Host ""
    Write-Host "  Integration tests completed in $($sw.Elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor Green

    if (Test-Path $logsDir) {
        $logCount = (Get-ChildItem -Path $logsDir -Recurse -File | Measure-Object).Count
        $logDirs = (Get-ChildItem -Path $logsDir -Directory | Measure-Object).Count
        Write-Host "  Logs: $logCount files across $logDirs test directories in .logs/" -ForegroundColor Cyan
    }

    if ($LASTEXITCODE -ne 0) { throw "Integration tests failed" }
}

# ── Dispatch ─────────────────────────────────────────────────────────────────

$targets = @("venv", "install", "build", "lint", "unit-test", "debug", "integration-test")

Write-Host "=== dbt-scope: $Target ===" -ForegroundColor Green

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
