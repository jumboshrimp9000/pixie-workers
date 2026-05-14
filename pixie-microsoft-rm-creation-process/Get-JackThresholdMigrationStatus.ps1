<#
.SYNOPSIS
    Repeatable status/proof report for the Jack threshold-tenant migration.
.DESCRIPTION
    Reads the migration plan, current domain assignments, inbox rows, and
    provider/upload actions, then reports only states that matter for deciding
    whether it is safe to widen the run.
#>

param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [int]$ExpectedInboxes = 99,
    [int]$ChunkSize = 4,
    [int]$ApiRetries = 3,
    [switch]$Json
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "config.ps1")

function Normalize-DomainName {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Normalize-Email {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Assert-ApiResult {
    param([hashtable]$Result, [string]$Label)
    if (-not $Result.Success) {
        throw "$Label failed: $($Result.Error)"
    }
    return @($Result.Data)
}

function Invoke-StatusApiRows {
    param(
        [string]$Table,
        [string]$Query,
        [string]$Label
    )

    $attempt = 0
    while ($true) {
        $attempt += 1
        $result = Invoke-SupabaseApi -Method GET -Table $Table -Query $Query
        if ($result.Success) { return @($result.Data) }

        if ($attempt -ge $ApiRetries) {
            throw "$Label failed after $attempt attempt(s): $($result.Error)"
        }

        $sleepSeconds = [Math]::Min(10, 2 * $attempt)
        Write-Warning "$Label failed on attempt $attempt/$ApiRetries; retrying in ${sleepSeconds}s: $($result.Error)"
        Start-Sleep -Seconds $sleepSeconds
    }
}

function Get-RowsByDomainChunks {
    param(
        [string]$Table,
        [string[]]$DomainIds,
        [string]$Select,
        [string]$ExtraQuery = ""
    )

    $rows = [System.Collections.Generic.List[object]]::new()
    if (-not $DomainIds -or $DomainIds.Count -eq 0) { return @() }

    for ($i = 0; $i -lt $DomainIds.Count; $i += $ChunkSize) {
        $last = [Math]::Min($i + $ChunkSize - 1, $DomainIds.Count - 1)
        $chunk = @($DomainIds[$i..$last])
        $query = "domain_id=in.($($chunk -join ','))&select=$Select&limit=20000"
        if ($ExtraQuery) { $query += "&$ExtraQuery" }
        foreach ($row in @(Invoke-StatusApiRows -Table $Table -Query $query -Label "$Table chunk $([int]($i / $ChunkSize) + 1)")) {
            $rows.Add($row) | Out-Null
        }
    }

    return @($rows)
}

if (-not (Test-Path $PlanCsv)) {
    throw "Plan CSV not found: $PlanCsv"
}

$plan = @(Import-Csv -Path $PlanCsv)
$domainNames = @($plan | ForEach-Object { Normalize-DomainName ([string]$_.domain) } | Where-Object { $_ } | Sort-Object -Unique)

$allDomains = @(Invoke-StatusApiRows -Table "domains" -Query "select=id,domain,status,interim_status,provider,customer_id,workspace_id,updated_at&limit=5000" -Label "domains")
$domains = @($allDomains | Where-Object { $domainNames -contains (Normalize-DomainName ([string]$_.domain)) })

$admins = @(Invoke-StatusApiRows -Table "admin_credentials" -Query "provider=eq.microsoft&select=id,email,status,active,locked_by_action_id,locked_domain_id,lock_expires_at&limit=5000" -Label "admin_credentials")
$adminById = @{}
foreach ($admin in $admins) { $adminById[[string]$admin.id] = $admin }

$assignments = @(Invoke-StatusApiRows -Table "domain_admin_assignments" -Query "select=domain_id,admin_cred_id,assigned_at&order=assigned_at.desc&limit=30000" -Label "domain_admin_assignments")

$domainIds = @($domains | ForEach-Object { [string]$_.id })
$inboxes = @(Get-RowsByDomainChunks -Table "inboxes" -DomainIds $domainIds -Select "id,domain_id,email,status,created_at,updated_at")
$actions = @(Get-RowsByDomainChunks -Table "actions" -DomainIds $domainIds -Select "id,domain_id,type,status,error,attempts,max_attempts,payload,result,created_at,updated_at,started_at,next_retry_at" -ExtraQuery "order=updated_at.desc")

$domainById = @{}
foreach ($domain in $domains) { $domainById[[string]$domain.id] = $domain }

$inboxCountsByDomainId = @{}
foreach ($group in @($inboxes | Group-Object domain_id)) {
    $activeCount = @($group.Group | Where-Object { [string]$_.status -eq "active" }).Count
    $deletedCount = @($group.Group | Where-Object { [string]$_.status -eq "deleted" }).Count
    $inboxCountsByDomainId[[string]$group.Name] = [pscustomobject]@{
        Active = $activeCount
        Deleted = $deletedCount
        Total = @($group.Group).Count
    }
}

$planByDomain = @{}
foreach ($row in $plan) {
    $planByDomain[(Normalize-DomainName ([string]$row.domain))] = $row
}

$domainRows = [System.Collections.Generic.List[object]]::new()
foreach ($domain in $domains) {
    $domainName = Normalize-DomainName ([string]$domain.domain)
    $planRow = $planByDomain[$domainName]
    $latestAssignment = $assignments | Where-Object { [string]$_.domain_id -eq [string]$domain.id } | Select-Object -First 1
    $currentAdmin = $null
    if ($latestAssignment -and $adminById.ContainsKey([string]$latestAssignment.admin_cred_id)) {
        $currentAdmin = $adminById[[string]$latestAssignment.admin_cred_id]
    }
    $counts = $inboxCountsByDomainId[[string]$domain.id]
    if (-not $counts) {
        $counts = [pscustomobject]@{ Active = 0; Deleted = 0; Total = 0 }
    }

    $destination = Normalize-Email ([string]$planRow.proposed_destination_admin)
    $current = Normalize-Email ([string]$currentAdmin.email)
    $moved = $current -and $destination -and $current -eq $destination

    $domainRows.Add([pscustomobject]@{
        domain = $domainName
        domain_id = [string]$domain.id
        status = [string]$domain.status
        interim_status = [string]$domain.interim_status
        source_admin = Normalize-Email ([string]$planRow.source_admin)
        destination_admin = $destination
        current_admin = $current
        current_admin_status = [string]$currentAdmin.status
        moved_to_destination = [bool]$moved
        active_inboxes = [int]$counts.Active
        deleted_inboxes = [int]$counts.Deleted
        total_inboxes = [int]$counts.Total
    }) | Out-Null
}

$provisionActions = @($actions | Where-Object {
    [string]$_.type -eq "provision_inbox" -and
    $_.payload -and
    [string]$_.payload.source -eq "jack_threshold_tenant_migration"
})
$provisionActionIds = @{}
foreach ($action in $provisionActions) {
    $provisionActionIds[[string]$action.id] = $true
}
$uploadActions = @($actions | Where-Object {
    [string]$_.type -eq "reupload_inboxes" -and
    $_.payload -and
    [string]$_.payload.provision_action_id -and
    $provisionActionIds.ContainsKey([string]$_.payload.provision_action_id)
})

$completedUploadDomains = @(
    $uploadActions |
        Where-Object { [string]$_.status -eq "completed" } |
        ForEach-Object {
            if ($_.payload.domain) { Normalize-DomainName ([string]$_.payload.domain) }
            elseif ($domainById.ContainsKey([string]$_.domain_id)) { Normalize-DomainName ([string]$domainById[[string]$_.domain_id].domain) }
        } |
        Where-Object { $_ } |
        Sort-Object -Unique
)

$fullyProven = @(
    $domainRows |
        Where-Object {
            $_.moved_to_destination -and
            $_.status -eq "active" -and
            $_.interim_status -eq "Both - Provisioning Complete" -and
            $_.active_inboxes -eq $ExpectedInboxes -and
            $completedUploadDomains -contains $_.domain
        }
)

$report = [pscustomobject]@{
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    plan_domains = $plan.Count
    db_domains = $domains.Count
    moved_to_destination = @($domainRows | Where-Object moved_to_destination).Count
    not_yet_moved = @($domainRows | Where-Object { -not $_.moved_to_destination }).Count
    active_99_moved = @($domainRows | Where-Object { $_.moved_to_destination -and $_.status -eq "active" -and $_.active_inboxes -eq $ExpectedInboxes }).Count
    completed_upload_domains = $completedUploadDomains.Count
    fully_proven_domains = $fullyProven.Count
    total_active_inboxes = @($inboxes | Where-Object { [string]$_.status -eq "active" }).Count
    expected_total_inboxes = $plan.Count * $ExpectedInboxes
    interim_counts = @($domainRows | Group-Object interim_status | Sort-Object Count -Descending | ForEach-Object {
        [pscustomobject]@{ count = $_.Count; interim_status = $_.Name }
    })
    provision_action_counts = @($provisionActions | Group-Object status | Sort-Object Count -Descending | ForEach-Object {
        [pscustomobject]@{ count = $_.Count; status = $_.Name }
    })
    upload_action_counts = @($uploadActions | Group-Object status | Sort-Object Count -Descending | ForEach-Object {
        [pscustomobject]@{ count = $_.Count; status = $_.Name }
    })
    completed_upload_domain_names = $completedUploadDomains
    fully_proven_domain_names = @($fullyProven | Select-Object -ExpandProperty domain | Sort-Object)
    recent_provision_actions = @($provisionActions | Sort-Object updated_at -Descending | Select-Object -First 15 id,status,attempts,error,created_at,updated_at,next_retry_at,@{Name="domain";Expression={$_.payload.domain}})
    recent_upload_actions = @($uploadActions | Sort-Object updated_at -Descending | Select-Object -First 15 id,status,attempts,error,created_at,updated_at,next_retry_at,@{Name="domain";Expression={$_.payload.domain}})
    stuck_or_unproven_samples = @(
        $domainRows |
            Where-Object {
                -not ($_.moved_to_destination -and $_.status -eq "active" -and $_.active_inboxes -eq $ExpectedInboxes -and $completedUploadDomains -contains $_.domain)
            } |
            Sort-Object moved_to_destination, interim_status, domain |
            Select-Object -First 25 domain,current_admin,destination_admin,current_admin_status,status,interim_status,active_inboxes,deleted_inboxes,total_inboxes
    )
}

if ($Json) {
    $report | ConvertTo-Json -Depth 8
} else {
    $report
}
