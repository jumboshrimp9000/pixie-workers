param(
    [switch]$DryRun
)

. (Join-Path $PSScriptRoot "RecoveryCommon.ps1")

$today = Get-Date
$oddDay = ([int]$today.Day % 2) -eq 1
$allowedPattern = if ($oddDay) { '^[89a-fA-F]' } else { '^[0-7]' }
$nowIso = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")

$result = Invoke-SupabaseApi -Method GET -Table "recovery_pool" -Query "recovery_status=in.(warming,ready)&select=*"
if (-not $result.Success) {
    throw "Failed to load recovery_pool rows: $($result.Error)"
}

$rows = @($result.Data | Where-Object { [string]$_.id -match $allowedPattern })
Write-Log "Recovery health refresh processing $($rows.Count) row(s)" -Level Info

foreach ($row in $rows) {
    $recoveryPoolId = [string]$row.id
    $instantlyAccountId = [string]$row.instantly_account_id
    if (-not $instantlyAccountId) { continue }

    try {
        $account = if ($DryRun) { @{ health_score = 80 } } else { Get-RecoveryInstantlyAccount -InstantlyAccountId $instantlyAccountId }
        $healthScore = Get-RecoveryHealthScoreFromInstantlyAccount -Account $account
        $healthLight = Get-RecoveryHealthLight -Score $healthScore

        $fields = @{
            health_score = $healthScore
            health_light = $healthLight
            health_fetched_at = $nowIso
        }

        $transitioningToGreen = ($healthLight -eq "green" -and -not $row.green_notified_at)
        if ($transitioningToGreen) {
            $fields.recovery_status = "ready"
            $fields.ready_for_reactivation_at = $nowIso
            $fields.green_notified_at = $nowIso
        }

        Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields $fields

        if ($transitioningToGreen -and -not $DryRun) {
            Invoke-RecoveryReadyEmail -RecoveryPoolId $recoveryPoolId
        }
    } catch {
        Write-Log "Recovery health refresh failed for $($row.domain): $($_.Exception.Message)" -Level Warning
        Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{
            last_error = "health_refresh_failed: $($_.Exception.Message)"
        }
    }
}
