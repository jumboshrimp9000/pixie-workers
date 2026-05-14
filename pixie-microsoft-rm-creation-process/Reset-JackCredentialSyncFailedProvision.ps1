param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [int]$ExpectedInboxes = 99,
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
    $query = "domain_id=in.($($chunk -join ','))&type=eq.provision_inbox&status=eq.failed&select=id,domain_id,type,status,error,attempts,max_attempts,payload,updated_at,started_at,next_retry_at&limit=20000"
    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query $query
    if ($result.Success) { $actions += @($result.Data) }
}

$targets = @()
foreach ($action in $actions) {
    if (-not $action.payload -or [string]$action.payload.source -ne "jack_threshold_tenant_migration") { continue }
    $errorText = [string]$action.error
    if ($errorText -notmatch "Bulletproof Check|Password Sync") { continue }
    $domainId = [string]$action.domain_id
    if (-not $domainById.ContainsKey($domainId)) { continue }
    $domain = $domainById[$domainId]
    $admin = Get-AssignedAdmin -DomainId $domainId
    if (-not $admin -or [string]$admin.status -ne "Active") { continue }
    $activeCount = @(Get-DomainInboxes -DomainId $domainId -Status "active").Count
    if ($activeCount -ne $ExpectedInboxes) { continue }
    $targets += [pscustomobject]@{
        Action = $action
        Domain = $domain
        Admin = $admin
        ActiveCount = $activeCount
    }
}

Write-Host "Matched $($targets.Count) Jack credential-sync failed provision action(s) to reset. Live=$([bool]$Live)"
foreach ($target in $targets) {
    $action = $target.Action
    $domain = $target.Domain
    $domainName = [string]$domain.domain
    $attempts = if ($action.attempts -ne $null) { [int]$action.attempts } else { 1 }
    $restoredAttempts = $attempts
    $reason = "Retrying credential/bulletproof validation after password sync failure using normal retry budget"

    Write-Host "provision $($action.id) $domainName attempts=$attempts active=$($target.ActiveCount) admin=$($target.Admin.email) error=$($action.error)"
    if (-not $Live) { continue }

    $history = if ($domain.action_history) { [string]$domain.action_history } else { "" }
    $history = Add-HistoryEntry -History $history -Entry "RETRY: $reason"
    Update-Domain -DomainId ([string]$domain.id) -Fields @{
        status = "in_progress"
        interim_status = "Microsoft - Mailboxes Created"
        action_history = $history
    }

    $body = @{
        status = "pending"
        error = $reason
        attempts = $restoredAttempts
        started_at = $null
        completed_at = $null
        next_retry_at = $null
        updated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    }
    $result = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$($action.id)&status=eq.failed" -Body $body
    if (-not $result.Success) {
        Write-Warning "Failed to reset $($action.id): $($result.Error)"
        continue
    }

    Add-ActionLog -ActionId ([string]$action.id) -DomainId ([string]$domain.id) -CustomerId ([string]$domain.customer_id) -EventType "credential_sync_failed_requeued" -Severity "warn" -Message $reason -Metadata @{
        previous_error = [string]$action.error
        active_inboxes = $target.ActiveCount
        admin_email = [string]$target.Admin.email
    }
}
