<#
.SYNOPSIS
    Requeues Jack replacement provision actions deferred on DKIM/CNAME.
.DESCRIPTION
    Targets only Jack threshold-migration provision_inbox actions that are
    pending because DKIM/CNAME was not accepted yet. This is intended after
    deploying a DKIM CNAME upsert fix, so stale selector records can be
    corrected immediately by the normal worker path.
#>

param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
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
    $query = "domain_id=in.($($chunk -join ','))&type=eq.provision_inbox&status=eq.pending&select=id,domain_id,type,status,error,attempts,payload,updated_at,started_at,next_retry_at&limit=20000"
    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query $query
    if (-not $result.Success) { throw $result.Error }
    $allActions += @($result.Data)
}

$targets = @($allActions | Where-Object {
    if (-not $_.payload -or [string]$_.payload.source -ne "jack_threshold_tenant_migration") { return $false }
    $errorText = [string]$_.error
    return ($errorText -match "DKIM|CNAME|selector")
})

Write-Host "Matched $($targets.Count) Jack DKIM-pending provision action(s) to requeue. Live=$([bool]$Live)"
foreach ($action in $targets) {
    $domainName = if ($action.payload.domain) { [string]$action.payload.domain } else { [string]$action.domain_id }
    Write-Host "provision $($action.id) $domainName attempts=$($action.attempts) next_retry_at=$($action.next_retry_at) error=$($action.error)"
    if (-not $Live) { continue }

    $body = @{
        error = "Requeued after DKIM CNAME upsert fix deployment"
        started_at = $null
        next_retry_at = $null
    } | ConvertTo-Json -Depth 4 -Compress

    $result = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$($action.id)&status=eq.pending" -Body $body
    if (-not $result.Success) {
        Write-Warning "Failed to requeue provision $($action.id): $($result.Error)"
    }
}
