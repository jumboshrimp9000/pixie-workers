<#
.SYNOPSIS
    Repairs Jack threshold-migration domains that were moved but not queued.
.DESCRIPTION
    Handles the narrow failure mode where source-tenant cancellation completed
    and the domain was reassigned to the clean destination admin, but the lane
    stopped before creating the replacement provision_inbox action. If a domain
    has exactly one or a few extra pending inbox rows, the script only deletes
    extras that are not present in the canonical 99-name set learned from other
    Jack domains, then queues the normal Azure shared-mailbox worker.
#>

param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [int]$ExpectedInboxes = 99,
    [string]$ReferenceDomain = "",
    [switch]$Live,
    [string]$ConfirmText = ""
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "config.ps1")

$expectedConfirm = "REPAIR JACK MOVED PROVISION GAPS"
if ($Live -and $ConfirmText -ne $expectedConfirm) {
    throw "Live repair requires ConfirmText exactly: $expectedConfirm"
}

function Normalize-DomainName {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Normalize-Email {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Get-Username {
    param([object]$Inbox)
    $username = [string]$Inbox.username
    if ($username) { return $username.Trim().ToLowerInvariant() }
    $email = [string]$Inbox.email
    if ($email -and $email.Contains("@")) { return $email.Split("@")[0].Trim().ToLowerInvariant() }
    return ""
}

function Invoke-RepairApiRows {
    param([string]$Table, [string]$Query, [string]$Label)
    $attempt = 0
    while ($true) {
        $attempt += 1
        $result = Invoke-SupabaseApi -Method GET -Table $Table -Query $Query
        if ($result.Success) { return @($result.Data) }
        if ($attempt -ge 3) { throw "$Label failed after $attempt attempt(s): $($result.Error)" }
        Start-Sleep -Seconds (2 * $attempt)
    }
}

function Get-RowsByDomainChunks {
    param([string]$Table, [string[]]$DomainIds, [string]$Select, [string]$ExtraQuery = "")
    $rows = [System.Collections.Generic.List[object]]::new()
    if (-not $DomainIds -or $DomainIds.Count -eq 0) { return @() }
    for ($i = 0; $i -lt $DomainIds.Count; $i += 4) {
        $last = [Math]::Min($i + 3, $DomainIds.Count - 1)
        $chunk = @($DomainIds[$i..$last])
        $query = "domain_id=in.($($chunk -join ','))&select=$Select&limit=20000"
        if ($ExtraQuery) { $query += "&$ExtraQuery" }
        foreach ($row in @(Invoke-RepairApiRows -Table $Table -Query $query -Label "$Table chunk")) {
            $rows.Add($row) | Out-Null
        }
    }
    return @($rows)
}

function Get-JackFulfillmentSettings {
    param([object]$DomainRecord)
    $settings = @{}
    if ($DomainRecord.fulfillment_settings) {
        if ($DomainRecord.fulfillment_settings -is [string]) {
            try { $settings = $DomainRecord.fulfillment_settings | ConvertFrom-Json -Depth 30 -AsHashtable } catch { $settings = @{} }
        } else {
            $json = $DomainRecord.fulfillment_settings | ConvertTo-Json -Depth 30
            try { $settings = $json | ConvertFrom-Json -Depth 30 -AsHashtable } catch { $settings = @{} }
        }
    }
    $settings["tag"] = "Mailboxpro 5/10"
    $settings["tags"] = @("Mailboxpro 5/10")
    $settings["dailyLimit"] = 5
    $settings["sendingGap"] = 30
    $settings["enableWarmup"] = $true
    $settings["sending_tool"] = "instantly"
    $settings["instantlyWarmup"] = @{
        limit = 5
        increment = "disabled"
        reply_rate = 60
    }
    return $settings
}

function New-ProvisionAction {
    param(
        [object]$DomainRecord,
        [object]$PlanRow,
        [string]$CancelActionId,
        [hashtable]$Settings
    )
    $domainName = Normalize-DomainName ([string]$DomainRecord.domain)
    $body = @{
        customer_id = $DomainRecord.customer_id
        domain_id = $DomainRecord.id
        type = "provision_inbox"
        status = "pending"
        attempts = 0
        max_attempts = 12
        payload = @{
            source = "jack_threshold_tenant_migration"
            domain = $domainName
            provider = "azure"
            expected_active_inboxes = $ExpectedInboxes
            source_admin_email = Normalize-Email ([string]$PlanRow.source_admin)
            destination_admin_email = Normalize-Email ([string]$PlanRow.proposed_destination_admin)
            cancel_action_id = $CancelActionId
            sending_tool_settings = $Settings
        }
    }
    return Invoke-SupabaseApi -Method POST -Table "actions" -Body $body
}

if (-not (Test-Path $PlanCsv)) { throw "Plan CSV not found: $PlanCsv" }

$plan = @(Import-Csv -Path $PlanCsv)
$planByDomain = @{}
$domainNames = @()
foreach ($row in $plan) {
    $domainName = Normalize-DomainName ([string]$row.domain)
    if (-not $domainName) { continue }
    $planByDomain[$domainName] = $row
    $domainNames += $domainName
}

$allDomains = @(Invoke-RepairApiRows -Table "domains" -Query "select=id,domain,status,interim_status,customer_id,workspace_id,fulfillment_settings&limit=5000" -Label "domains")
$domains = @($allDomains | Where-Object { $domainNames -contains (Normalize-DomainName ([string]$_.domain)) })
$domainById = @{}
foreach ($domain in $domains) { $domainById[[string]$domain.id] = $domain }
$domainIds = @($domains | ForEach-Object { [string]$_.id })

$admins = @(Invoke-RepairApiRows -Table "admin_credentials" -Query "provider=eq.microsoft&select=id,email,status,active&limit=5000" -Label "admin_credentials")
$adminById = @{}
foreach ($admin in $admins) { $adminById[[string]$admin.id] = $admin }

$assignments = @(Invoke-RepairApiRows -Table "domain_admin_assignments" -Query "select=domain_id,admin_cred_id,assigned_at&order=assigned_at.desc&limit=30000" -Label "domain_admin_assignments")
$inboxes = @(Get-RowsByDomainChunks -Table "inboxes" -DomainIds $domainIds -Select "id,domain_id,email,username,status,created_at,updated_at")
$actions = @(Get-RowsByDomainChunks -Table "actions" -DomainIds $domainIds -Select "id,domain_id,type,status,error,payload,created_at,updated_at" -ExtraQuery "order=updated_at.desc")

$referenceUsernames = @()
if ($ReferenceDomain) {
    $ref = $domains | Where-Object { (Normalize-DomainName ([string]$_.domain)) -eq (Normalize-DomainName $ReferenceDomain) } | Select-Object -First 1
    if (-not $ref) { throw "Reference domain not found in plan: $ReferenceDomain" }
    $referenceUsernames = @($inboxes | Where-Object { [string]$_.domain_id -eq [string]$ref.id -and [string]$_.status -in @("active","pending") } | ForEach-Object { Get-Username $_ } | Where-Object { $_ } | Sort-Object -Unique)
} else {
    foreach ($group in @($inboxes | Group-Object domain_id)) {
        $liveRows = @($group.Group | Where-Object { [string]$_.status -in @("active","pending") })
        if ($liveRows.Count -ne $ExpectedInboxes) { continue }
        $referenceUsernames = @($liveRows | ForEach-Object { Get-Username $_ } | Where-Object { $_ } | Sort-Object -Unique)
        if ($referenceUsernames.Count -eq $ExpectedInboxes) { break }
    }
}
if ($referenceUsernames.Count -ne $ExpectedInboxes) {
    throw "Could not learn canonical $ExpectedInboxes inbox username set from plan domains."
}
$referenceSet = @{}
foreach ($username in $referenceUsernames) { $referenceSet[$username] = $true }

$targets = [System.Collections.Generic.List[object]]::new()
foreach ($domain in $domains) {
    $domainName = Normalize-DomainName ([string]$domain.domain)
    $planRow = $planByDomain[$domainName]
    if (-not $planRow) { continue }

    $latestAssignment = $assignments | Where-Object { [string]$_.domain_id -eq [string]$domain.id } | Select-Object -First 1
    $currentAdmin = if ($latestAssignment -and $adminById.ContainsKey([string]$latestAssignment.admin_cred_id)) { $adminById[[string]$latestAssignment.admin_cred_id] } else { $null }
    $destination = Normalize-Email ([string]$planRow.proposed_destination_admin)
    if ((Normalize-Email ([string]$currentAdmin.email)) -ne $destination) { continue }
    if ([string]$domain.status -ne "in_progress") { continue }
    if ([string]$domain.interim_status -notin @("Both - DNS Zone Created", "Microsoft - Cancellation Complete")) { continue }

    $domainActions = @($actions | Where-Object { [string]$_.domain_id -eq [string]$domain.id })
    $replacementProvision = @($domainActions | Where-Object {
        [string]$_.type -eq "provision_inbox" -and
        $_.payload -and
        [string]$_.payload.source -eq "jack_threshold_tenant_migration"
    })
    if (@($replacementProvision | Where-Object { [string]$_.status -in @("pending","in_progress","completed") }).Count -gt 0) { continue }

    $pending = @($inboxes | Where-Object { [string]$_.domain_id -eq [string]$domain.id -and [string]$_.status -eq "pending" })
    if ($pending.Count -lt $ExpectedInboxes) { continue }

    $cancelAction = @($domainActions | Where-Object {
        [string]$_.type -eq "microsoft_cancel_domain" -and
        $_.payload -and
        [string]$_.payload.source -eq "jack_threshold_tenant_migration"
    } | Sort-Object updated_at -Descending | Select-Object -First 1)

    $targets.Add([pscustomobject]@{
        Domain = $domain
        PlanRow = $planRow
        Pending = $pending
        CancelActionId = if ($cancelAction.Count -gt 0) { [string]$cancelAction[0].id } else { "" }
    }) | Out-Null
}

Write-Host "Matched $($targets.Count) moved domain(s) with missing replacement provision action. Live=$([bool]$Live)"
foreach ($target in $targets) {
    $domain = $target.Domain
    $domainName = Normalize-DomainName ([string]$domain.domain)
    $pending = @($target.Pending)
    $extraCount = $pending.Count - $ExpectedInboxes
    $extras = @()
    if ($extraCount -gt 0) {
        $extras = @($pending | Where-Object {
            $username = Get-Username $_
            -not $referenceSet.ContainsKey($username)
        })
        if ($extras.Count -ne $extraCount) {
            Write-Warning "$domainName has $($pending.Count) pending inboxes, but extras are not provable; skipping."
            continue
        }
    }

    Write-Host "$domainName pending=$($pending.Count) extras=$($extras.Count) cancel=$($target.CancelActionId)"
    foreach ($extra in $extras) {
        Write-Host "  extra pending inbox -> $($extra.email)"
    }
    if (-not $Live) { continue }

    foreach ($extra in $extras) {
        Update-Inbox -InboxId ([string]$extra.id) -Fields @{ status = "deleted" }
    }

    $settings = Get-JackFulfillmentSettings -DomainRecord $domain
    $history = if ($domain.action_history) { [string]$domain.action_history } else { "" }
    $history = Add-HistoryEntry -History $history -Entry "Threshold migration repair: queued replacement provision after moved-domain gap"
    Update-Domain -DomainId ([string]$domain.id) -Fields @{
        status = "in_progress"
        interim_status = "Both - DNS Zone Created"
        fulfillment_settings = $settings
        action_history = $history
    }

    $result = New-ProvisionAction -DomainRecord $domain -PlanRow $target.PlanRow -CancelActionId ([string]$target.CancelActionId) -Settings $settings
    if (-not $result.Success) {
        Write-Warning "Failed to create provision action for ${domainName}: $($result.Error)"
    } else {
        $actionId = if ($result.Data -and $result.Data.Count -gt 0) { [string]$result.Data[0].id } else { "" }
        Write-Host "  queued provision action $actionId"
    }
}
