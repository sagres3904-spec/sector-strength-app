param(
    [string]$TaskPrefix = "sector-strength-snapshot",
    [switch]$WriteDrive
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$runnerPath = Join-Path $repoRoot "scripts\run_scheduled_snapshot.ps1"
if (-not (Test-Path $runnerPath)) {
    throw "Scheduled snapshot PowerShell runner not found: $runnerPath"
}

$powershellExe = Join-Path $env:WINDIR "System32\WindowsPowerShell\v1.0\powershell.exe"
if (-not (Test-Path $powershellExe)) {
    $powershellExe = "powershell.exe"
}

$taskSpecs = @(
    @{ Mode = "0915"; Time = "09:15" },
    @{ Mode = "1130"; Time = "11:30" },
    @{ Mode = "1530"; Time = "15:30" }
)

foreach ($spec in $taskSpecs) {
    $taskName = "{0}-{1}" -f $TaskPrefix, $spec.Mode
    $argumentList = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", ('"{0}"' -f $runnerPath),
        "-Mode", $spec.Mode
    )
    if ($WriteDrive) {
        $argumentList += "-WriteDrive"
    }
    $action = New-ScheduledTaskAction -Execute $powershellExe -Argument ($argumentList -join " ")
    $trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At $spec.Time
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Description ("Create local sector snapshot at {0}" -f $spec.Time) -Force | Out-Null
    Write-Host ("Registered task: {0} ({1})" -f $taskName, $spec.Time)
}
