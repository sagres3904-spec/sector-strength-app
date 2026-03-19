param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("0915", "1130", "1530")]
    [string]$Mode,

    [switch]$WriteDrive
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    $pythonExe = "python"
}

$runnerPath = Join-Path $repoRoot "scripts\run_scheduled_snapshot.py"
if (-not (Test-Path $runnerPath)) {
    throw "Scheduled snapshot runner not found: $runnerPath"
}

$logDir = Join-Path $repoRoot "data\task_logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logPath = Join-Path $logDir ("scheduled_snapshot_{0}_{1}.log" -f $Mode, $timestamp)

$arguments = @($runnerPath, "--mode", $Mode)
if ($WriteDrive) {
    $arguments += "--write-drive"
}

"[{0}] scheduled snapshot start mode={1}" -f (Get-Date -Format "s"), $Mode | Tee-Object -FilePath $logPath -Append | Out-Null
& $pythonExe @arguments 2>&1 | Tee-Object -FilePath $logPath -Append
$exitCode = $LASTEXITCODE
"[{0}] scheduled snapshot exit_code={1}" -f (Get-Date -Format "s"), $exitCode | Tee-Object -FilePath $logPath -Append | Out-Null
exit $exitCode
