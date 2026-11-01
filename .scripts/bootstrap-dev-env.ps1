#Requires -Version 7.0
#Requires -RunAsAdministrator

<#
.SYNOPSIS
Recreates Ubuntu-24.04 and clones this checkout's branch into /workspaces/dbt-scope.
.NOTES
Non-interactive. Use the Taskfile's up task to preserve an existing WSL workspace.
#>

[CmdletBinding()]
param(
    [string]$Branch = (git -C $PSScriptRoot rev-parse --abbrev-ref HEAD),
    [ValidatePattern('^[a-z_][a-z0-9_-]{0,31}$')]
    [string]$LinuxUser = ($env:USERNAME.ToLowerInvariant() -replace '[^a-z0-9_-]', '')
)

$ErrorActionPreference = 'Stop'
$distro = 'Ubuntu-24.04'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

function Invoke-Checked {
    param([string]$Step, [scriptblock]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "$Step failed with exit code $LASTEXITCODE." }
}

foreach ($tool in @('git', 'wsl', 'code')) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "Required command '$tool' is missing. Install it before running bootstrap."
    }
}
if ([string]::IsNullOrWhiteSpace($Branch) -or $Branch -eq 'HEAD' -or $Branch.StartsWith('-')) {
    throw 'Select a pushed branch before bootstrapping; detached HEAD is not supported.'
}
Invoke-Checked 'Validate branch' { git check-ref-format --branch $Branch | Out-Null }
$origin = git -C $repoRoot remote get-url origin
if ($LASTEXITCODE -ne 0 -or -not $origin) { throw 'Cannot determine this checkout''s origin.' }
$env:GCM_INTERACTIVE = 'never'
$env:GIT_TERMINAL_PROMPT = '0'
Invoke-Checked 'Check remote branch before recreating WSL' {
    git ls-remote --exit-code --heads $origin "refs/heads/$Branch" | Out-Null
}

$gcmCommand = Get-Command git-credential-manager -ErrorAction SilentlyContinue
$gitExecPath = git --exec-path
if ($LASTEXITCODE -ne 0) { throw 'Cannot locate Git for Windows.' }
$gcmCandidates = @(
    if ($gcmCommand) { $gcmCommand.Source }
    Join-Path (Split-Path (Split-Path $gitExecPath)) 'bin\git-credential-manager.exe'
)
$gcm = $gcmCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if (-not $gcm) {
    throw 'Git Credential Manager is missing. Install Git for Windows with GCM enabled.'
}

Invoke-Checked 'Install VS Code WSL extension' {
    code --install-extension ms-vscode-remote.remote-wsl
}
Invoke-Checked 'Update WSL' { wsl --update }
$previousEncoding = [Console]::OutputEncoding
try {
    [Console]::OutputEncoding = [System.Text.Encoding]::Unicode
    $versionOutput = wsl --version
    if ($LASTEXITCODE -ne 0) { throw 'Could not query the WSL version.' }
} finally {
    [Console]::OutputEncoding = $previousEncoding
}
$versionMatch = [regex]::Match(($versionOutput -join "`n"), '\d+\.\d+\.\d+')
if (-not $versionMatch.Success -or [version]$versionMatch.Value -lt [version]'2.4.4') {
    throw 'WSL 2.4.4 or newer is required for a non-interactive installation with --location.'
}

$drive = Get-CimInstance Win32_LogicalDisk -Filter 'DriveType=3' |
    Where-Object { $_.VolumeName -notmatch 'Temporary' } |
    Sort-Object FreeSpace -Descending | Select-Object -First 1
if (-not $drive) { throw 'No fixed drive is available for the WSL VHD.' }
$wslLocation = Join-Path $drive.DeviceID 'VHD'
Write-Host "Using $wslLocation for Ubuntu-24.04."

# Close stale windows for this distro before replacing its filesystem.
Add-Type -AssemblyName UIAutomationClient
[Windows.Automation.AutomationElement]::RootElement.FindAll('Children', [Windows.Automation.Condition]::TrueCondition) |
    Where-Object { $_.Current.Name -match '\[WSL: Ubuntu-24\.04\].*Visual Studio Code' } |
    ForEach-Object { $_.GetCurrentPattern([Windows.Automation.WindowPattern]::Pattern).Close() }

$distros = @(wsl --list --quiet)
if ($LASTEXITCODE -ne 0) { throw 'Could not list installed WSL distros.' }
$distros = @($distros | ForEach-Object { $_.Replace("`0", '').Trim() } | Where-Object { $_ })
if ($distro -in $distros) {
    Write-Host "Unregistering $distro."
    Invoke-Checked 'Unregister WSL' { wsl --unregister $distro }
}

$memoryGb = [math]::Floor((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB)
$cpu = [Environment]::ProcessorCount
$swapGb = [math]::Floor($memoryGb / 4)
@"
[wsl2]
memory=${memoryGb}GB
processors=$cpu
swap=${swapGb}GB
networkingMode=NAT

[general]
instanceIdleTimeout=-1
"@ | Set-Content -LiteralPath (Join-Path $env:USERPROFILE '.wslconfig')

Invoke-Checked 'Shutdown WSL' { wsl --shutdown }
New-Item -ItemType Directory -Path $wslLocation -Force | Out-Null
Invoke-Checked 'Install Ubuntu' {
    wsl --install -d $distro --location $wslLocation --no-launch --web-download
}
Invoke-Checked 'Create Linux user' {
    wsl -d $distro -u root --exec bash -c 'set -e; useradd -m -s /bin/bash -G sudo "$1"; printf "%s ALL=(ALL) NOPASSWD:ALL\n" "$1" > "/etc/sudoers.d/$1"; chmod 0440 "/etc/sudoers.d/$1"; install -d -o "$1" /workspaces' bash $LinuxUser
}
Invoke-Checked 'Set default Linux user' { wsl --manage $distro --set-default-user $LinuxUser }

$userName = git -C $repoRoot config user.name
$userEmail = git -C $repoRoot config user.email
Write-Host "Cloning $Branch into /workspaces/dbt-scope."
Invoke-Checked 'Clone selected branch into WSL' {
    wsl -d $distro --cd /workspaces --exec bash -c 'set -e; if [ -n "$1" ]; then git config --global user.name "$1"; fi; if [ -n "$2" ]; then git config --global user.email "$2"; fi; git config --global credential.helper "$(printf %q "$(wslpath -u "$5")")"; GIT_TERMINAL_PROMPT=0 git clone --branch "$3" -- "$4" /workspaces/dbt-scope' bash "$userName" "$userEmail" $Branch $origin $gcm
}
