<#
.SYNOPSIS
    Requeues Jack replacement actions stranded by Instantly tag mapping lag.
.DESCRIPTION
    Only targets replacement actions (`payload.source =
    jack_threshold_tenant_migration`) that failed on the known Instantly tag
    mapping visibility lag. This is meant for the patched upload worker path
    where account/settings/warmup validation remains strict and tag proof is
    handled separately by the tag auditor.
#>

param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [datetimeoffset]$InProgressUpdatedBefore = ([datetimeoffset]::UtcNow.AddMinutes(-30)),
    [switch]$IncludeProvisionFailures = $true,
    [switch]$RecycleInProgressUploads,
    [switch]$Live
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "config.ps1")

function Normalize-DomainName {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

if (-not (Test-Path $PlanCsv)) { throw "Plan CSV not found: $PlanCsv" }

$plan = @(Import-Csv -Path $PlanCsv)
$domainNames = @($plan | ForEach-Object { Normalize-DomainName ([string]$_.domain) } | Where-Object { $_ } | Sort-Object -Unique)
$domainResult = Invoke-SupabaseApi -Method GET -Table "domains" -Query "select=id,domain&limit=5000"
if (-not $domainResult.Success) { throw $domainResult.Error }
$domains = @($domainResult.Data | Where-Object { $domainNames -contains (Normalize-DomainName ([string]$_.domain)) })
$domainIds = @($domains | ForEach-Object { [string]$_.id })

$allActions = @()
for ($i = 0; $i -lt $domainIds.Count; $i += 8) {
    $last = [Math]::Min($i + 7, $domainIds.Count - 1)
    $chunk = @($domainIds[$i..$last])
    $query = "domain_id=in.($($chunk -join ','))&type=in.(provision_inbox,reupload_inboxes)&select=id,domain_id,type,status,error,attempts,payload,updated_at,started_at,next_retry_at&limit=20000"
    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query $query
    if (-not $result.Success) { throw $result.Error }
    $allActions += @($result.Data)
}

$replacementProvisionIds = @{}
foreach ($action in $allActions) {
    if (
        [string]$action.type -eq "provision_inbox" -and
        $action.payload -and
        [string]$action.payload.source -eq "jack_threshold_tenant_migration"
    ) {
        $replacementProvisionIds[[string]$action.id] = $true
    }
}

$uploadTargets = @($allActions | Where-Object {
    if ([string]$_.type -ne "reupload_inboxes") { return $false }
    if (-not $_.payload) { return $false }
    if (-not $replacementProvisionIds.ContainsKey([string]$_.payload.provision_action_id)) { return $false }
    if ($RecycleInProgressUploads -and [string]$_.status -eq "in_progress") { return $true }
    $errorText = [string]$_.error
    if ([string]$_.status -eq "failed") { return ($errorText -match "tag mapping was not visible") }
    if ([string]$_.status -ne "in_progress") { return $false }
    if (-not $_.updated_at) { return $true }
    return ([datetimeoffset]::Parse([string]$_.updated_at) -lt $InProgressUpdatedBefore)
})

$provisionTargets = @()
if ($IncludeProvisionFailures) {
    $provisionTargets = @($allActions | Where-Object {
        if ([string]$_.type -ne "provision_inbox") { return $false }
        if ([string]$_.status -ne "failed") { return $false }
        if (-not $_.payload -or [string]$_.payload.source -ne "jack_threshold_tenant_migration") { return $false }
        $errorText = [string]$_.error
        return ($errorText -match "Sending-tool upload validation failed" -and $errorText -match "tag mapping was not visible")
    })
}

Write-Host "Matched $($uploadTargets.Count) replacement upload action(s) and $($provisionTargets.Count) provision finalization action(s) to reset. Live=$([bool]$Live) RecycleInProgressUploads=$([bool]$RecycleInProgressUploads)"
foreach ($action in $uploadTargets) {
    $domainName = [string]$action.payload.domain
    Write-Host "upload $($action.id) $domainName status=$($action.status) attempts=$($action.attempts) updated=$($action.updated_at)"
    if (-not $Live) { continue }

    $body = @{
        status = "pending"
        error = $null
        attempts = 0
        started_at = $null
        next_retry_at = $null
    } | ConvertTo-Json -Depth 4 -Compress
    $result = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$($action.id)" -Body $body
    if (-not $result.Success) {
        Write-Warning "Failed to reset $($action.id): $($result.Error)"
    }
}

foreach ($action in $provisionTargets) {
    $domainName = if ($action.payload.domain) { [string]$action.payload.domain } else { [string]$action.domain_id }
    Write-Host "provision $($action.id) $domainName status=$($action.status) attempts=$($action.attempts) updated=$($action.updated_at)"
    if (-not $Live) { continue }

    $body = @{
        status = "pending"
        error = "Requeued after patched Instantly tag mapping lag handling"
        attempts = 0
        started_at = $null
        next_retry_at = (Get-Date).AddSeconds(30).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    } | ConvertTo-Json -Depth 4 -Compress
    $result = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$($action.id)&status=eq.failed" -Body $body
    if (-not $result.Success) {
        Write-Warning "Failed to reset provision $($action.id): $($result.Error)"
    }
}
