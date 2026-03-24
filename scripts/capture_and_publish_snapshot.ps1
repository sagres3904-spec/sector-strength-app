$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$pythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    $pythonExe = "python"
}

$pollerPath = Join-Path $repoRoot "local_capture_and_publish.py"
if (-not (Test-Path $pollerPath)) {
    throw "Poller script not found: $pollerPath"
}

& $pythonExe $pollerPath @args
exit $LASTEXITCODE
