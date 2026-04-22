$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$repoRoot = [System.IO.Path]::GetFullPath($repoRoot)
Set-Location $repoRoot

$pythonCandidates = @(
    (Join-Path $repoRoot ".venv\Scripts\python.exe"),
    (Join-Path (Split-Path -Parent $repoRoot) ".venv\Scripts\python.exe")
)

$pythonExe = $null
foreach ($candidate in $pythonCandidates) {
    if (Test-Path $candidate) {
        $pythonExe = $candidate
        break
    }
}

if (-not $pythonExe) {
    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCommand) {
        $pythonExe = $pythonCommand.Source
    } else {
        throw "Python executable not found for poller runtime."
    }
}

$pollerPath = Join-Path $repoRoot "local_capture_and_publish.py"
if (-not (Test-Path $pollerPath)) {
    throw "Poller script not found: $pollerPath"
}

Write-Host "[poller-live] repoRoot=$repoRoot"
Write-Host "[poller-live] pythonExe=$pythonExe"
Write-Host "[poller-live] pollerPath=$pollerPath"

& $pythonExe $pollerPath @args
exit $LASTEXITCODE
