<#
.SYNOPSIS
    Wrapper around run.ps1 that maintains a heartbeat file for Docker HEALTHCHECK.
.DESCRIPTION
    Starts a background heartbeat job that touches /tmp/pixie_microsoft_heartbeat
    every 30 seconds, then runs run.ps1 in the foreground. If run.ps1 exits,
    this wrapper exits with the same code so Docker can restart the container.
#>

$HeartbeatPath = "/tmp/pixie_microsoft_heartbeat"

Write-Host "[healthcheck-wrapper] Starting heartbeat + run.ps1..." -ForegroundColor Cyan

if (-not $env:WORKER_ACTION_LEASE_SECONDS) {
    $env:WORKER_ACTION_LEASE_SECONDS = "7200"
}
if (-not $env:WORKER_ACTION_HEARTBEAT_SECONDS) {
    $env:WORKER_ACTION_HEARTBEAT_SECONDS = "120"
}
if (-not $env:WORKER_ACTION_TYPES) {
    $env:WORKER_ACTION_TYPES = "provision_inbox"
}

# Background job: touch heartbeat file every 30s
$heartbeatJob = Start-Job -ScriptBlock {
    param($path)
    while ($true) {
        Set-Content -Path $path -Value (Get-Date -Format o) -Force
        Start-Sleep -Seconds 30
    }
} -ArgumentList $HeartbeatPath

# Run the actual worker in foreground (streams output naturally)
try {
    & (Join-Path $PSScriptRoot "run.ps1")
    $exitCode = $LASTEXITCODE
} catch {
    Write-Host "[healthcheck-wrapper] run.ps1 threw: $($_.Exception.Message)" -ForegroundColor Red
    $exitCode = 1
} finally {
    Stop-Job $heartbeatJob -ErrorAction SilentlyContinue
    Remove-Job $heartbeatJob -Force -ErrorAction SilentlyContinue
}

Write-Host "[healthcheck-wrapper] run.ps1 exited with code $exitCode" -ForegroundColor Red
exit $exitCode
