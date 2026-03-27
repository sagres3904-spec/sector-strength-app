param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("0915", "1130", "1530")]
    [string]$Mode,

    [switch]$WriteDrive,

    [switch]$PublishAfterSuccess
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$pythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    $pythonExe = "python"
}

$runnerPath = Join-Path $repoRoot "scripts\run_scheduled_snapshot.py"
if (-not (Test-Path $runnerPath)) {
    throw "Scheduled snapshot runner not found: $runnerPath"
}

$arguments = @($runnerPath, "--mode", $Mode)
if ($WriteDrive) {
    $arguments += "--write-drive"
}
if ($PublishAfterSuccess) {
    $arguments += "--publish-after-success"
}

& $pythonExe @arguments
exit $LASTEXITCODE
