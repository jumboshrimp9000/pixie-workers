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

$planDomains = @{}
foreach ($row in @(Import-Csv -Path $PlanCsv)) {
    $domain = Normalize-DomainName ([string]$row.domain)
    if ($domain) { $planDomains[$domain] = $true }
}

$domainRows = @(Invoke-SupabaseApi -Method GET -Table "domains" -Query "select=id,domain,customer_id,action_history&limit=5000").Data | Where-Object {
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
    $query = "domain_id=in.($($chunk -join ','))&type=eq.provision_inbox&status=in.(pending,in_progress)&select=id,domain_id,type,status,error,attempts,payload,updated_at,started_at,next_retry_at&limit=20000"
    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query $query
    if ($result.Success) { $actions += @($result.Data) }
}

$targets = @()
foreach ($action in $actions) {
    if (-not $action.payload -or [string]$action.payload.source -ne "jack_threshold_tenant_migration") { continue }
    $domainId = [string]$action.domain_id
    if (-not $domainById.ContainsKey($domainId)) { continue }
    $domain = $domainById[$domainId]
    $admin = Get-AssignedAdmin -DomainId $domainId
    if (-not $admin -or [string]$admin.status -ne "threshold exceeded") { continue }
    $targets += [pscustomobject]@{
        Action = $action
        Domain = $domain
        Admin = $admin
    }
}

Write-Host "Matched $($targets.Count) premature Jack provision action(s) to cancel. Live=$([bool]$Live)"
foreach ($target in $targets) {
    $action = $target.Action
    $domain = $target.Domain
    $admin = $target.Admin
    $domainName = [string]$domain.domain
    $reason = "Cancelled premature Jack threshold-migration provision action because $domainName is still assigned to threshold tenant $($admin.email); move-first cancellation/reassignment must run."
    Write-Host "cancel $($action.id) $domainName status=$($action.status) admin=$($admin.email)"
    if (-not $Live) { continue }

    $history = if ($domain.action_history) { [string]$domain.action_history } else { "" }
    $history = Add-HistoryEntry -History $history -Entry "Premature provision action cancelled before move: still assigned to threshold tenant"
    Update-Domain -DomainId ([string]$domain.id) -Fields @{ action_history = $history }

    $body = @{
        status = "cancelled"
        error = $reason
        result = @{
            cancelled_reason = "premature_threshold_migration_provision"
            current_admin_email = [string]$admin.email
            current_admin_status = [string]$admin.status
            next_action = "Run threshold migration cancellation/reassignment, then create a fresh replacement provision action."
        }
        started_at = $null
        completed_at = $null
        next_retry_at = $null
        updated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    }
    $result = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$($action.id)&status=in.(pending,in_progress)" -Body $body
    if (-not $result.Success) {
        Write-Warning "Failed to cancel $($action.id): $($result.Error)"
        continue
    }

    Add-ActionLog -ActionId ([string]$action.id) -DomainId ([string]$domain.id) -CustomerId ([string]$domain.customer_id) -EventType "premature_threshold_migration_provision_cancelled" -Severity "warn" -Message $reason -Metadata @{
        current_admin_email = [string]$admin.email
        current_admin_status = [string]$admin.status
    }
}
