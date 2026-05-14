<#
.SYNOPSIS
    Requeues Jack migration provisions that exhausted Exchange accepted-domain sync waits.
.DESCRIPTION
    Some Microsoft tenants take longer than the normal retry budget to expose a
    newly added accepted domain consistently to Exchange. When that happens, the
    action can remain pending with attempts=max_attempts and be skipped by normal
    workers. This script extends the retry budget only for safe pre-mailbox
    states and only for Jack threshold-migration actions on Active destination
    admins.
#>

param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [int]$ExpectedInboxes = 99,
    [int]$AdditionalAttempts = 6,
    [switch]$Live
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "config.ps1")

function Normalize-DomainName {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

if (-not (Test-Path $PlanCsv)) { throw "Plan CSV not found: $PlanCsv" }

$planDomains = @{}
foreach ($row in @(Import-Csv -Path $PlanCsv)) {
    $domain = Normalize-DomainName ([string]$row.domain)
    if ($domain) { $planDomains[$domain] = $true }
}

$domainRows = @(Invoke-SupabaseApi -Method GET -Table "domains" -Query "select=id,domain,customer_id,status,interim_status,action_history&limit=5000").Data | Where-Object {
    $planDomains.ContainsKey((Normalize-DomainName ([string]$_.domain)))
}

$domainById = @{}
$domainIds = @()
foreach ($domain in $domainRows) {
    $domainById[[string]$domain.id] = $domain
    $domainIds += [string]$domain.id
}

$actions = @()
for ($i = 0; $i -lt $domainIds.Count; $i += 100) {
    $end = [Math]::Min($i + 99, $domainIds.Count - 1)
    $chunk = $domainIds[$i..$end]
    $query = "domain_id=in.($($chunk -join ','))&type=eq.provision_inbox&status=eq.pending&select=id,domain_id,type,status,error,attempts,max_attempts,payload,updated_at,started_at,next_retry_at&limit=20000"
    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query $query
    if ($result.Success) { $actions += @($result.Data) }
}

$targets = @()
foreach ($action in $actions) {
    if (-not $action.payload -or [string]$action.payload.source -ne "jack_threshold_tenant_migration") { continue }
    if ([string]$action.error -notmatch "Exchange sync pending") { continue }
    if ($action.attempts -eq $null -or $action.max_attempts -eq $null) { continue }
    if ([int]$action.attempts -lt [int]$action.max_attempts) { continue }

    $domainId = [string]$action.domain_id
    if (-not $domainById.ContainsKey($domainId)) { continue }
    $domain = $domainById[$domainId]
    if ([string]$domain.interim_status -notin @("Both - DNS Records Added", "Both - DNS Zone Created")) { continue }

    $admin = Get-AssignedAdmin -DomainId $domainId
    if (-not $admin -or [string]$admin.status -ne "Active") { continue }

    $activeCount = @(Get-DomainInboxes -DomainId $domainId -Status "active").Count
    if ($activeCount -ne 0) { continue }

    $pendingCount = @(Get-DomainInboxes -DomainId $domainId -Status "pending").Count
    if ($pendingCount -ne $ExpectedInboxes) { continue }

    $targets += [pscustomobject]@{
        Action = $action
        Domain = $domain
        Admin = $admin
        PendingCount = $pendingCount
    }
}

Write-Host "Matched $($targets.Count) Jack Exchange-sync pending provision action(s). Live=$([bool]$Live)"
foreach ($target in $targets) {
    $action = $target.Action
    $domain = $target.Domain
    $nextMaxAttempts = [int]$action.max_attempts + [Math]::Max(1, $AdditionalAttempts)
    $reason = "Extending retry budget for Exchange accepted-domain sync propagation"
    Write-Host "provision $($action.id) $($domain.domain) attempts=$($action.attempts)/$($action.max_attempts) nextMax=$nextMaxAttempts pending=$($target.PendingCount) admin=$($target.Admin.email)"
    if (-not $Live) { continue }

    $history = if ($domain.action_history) { [string]$domain.action_history } else { "" }
    $history = Add-HistoryEntry -History $history -Entry "RETRY: $reason"
    Update-Domain -DomainId ([string]$domain.id) -Fields @{
        status = "in_progress"
        interim_status = "Both - DNS Records Added"
        action_history = $history
    }

    $body = @{
        max_attempts = $nextMaxAttempts
        started_at = $null
        next_retry_at = $null
        error = $reason
        updated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    }
    $result = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$($action.id)&status=eq.pending" -Body $body
    if (-not $result.Success) {
        Write-Warning "Failed to extend $($action.id): $($result.Error)"
        continue
    }

    Add-ActionLog -ActionId ([string]$action.id) -DomainId ([string]$domain.id) -CustomerId ([string]$domain.customer_id) -EventType "exchange_sync_pending_requeued" -Severity "warn" -Message $reason -Metadata @{
        previous_error = [string]$action.error
        previous_attempts = [int]$action.attempts
        previous_max_attempts = [int]$action.max_attempts
        next_max_attempts = $nextMaxAttempts
        admin_email = [string]$target.Admin.email
    }
}
