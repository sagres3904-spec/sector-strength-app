param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("0915", "1130", "1530", "now")]
    [string]$Mode
)

$ErrorActionPreference = "Stop"

Set-Location "D:\株アプリ\sector-strength-app-deploy"
& ".\.venv\Scripts\python.exe" ".\local_capture_and_publish.py" --mode $Mode
exit $LASTEXITCODE
