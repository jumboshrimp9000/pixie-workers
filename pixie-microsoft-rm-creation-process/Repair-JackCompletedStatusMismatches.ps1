<#
.SYNOPSIS
    Repairs Jack migration domain rows left in a failed/in-progress UI state.
.DESCRIPTION
    Some long-running replacement provisions can complete the Microsoft and
    Instantly work but leave the domain row stale, usually after an earlier
    retry path wrote Both - Failed. This script only repairs rows that have:

      - current admin equal to the migration plan destination
      - exactly the expected active inbox count
      - a completed jack_threshold_tenant_migration provision action
      - a completed reupload_inboxes action linked to that provision action

    It does not create mailboxes, upload inboxes, or bypass provider proof.
#>

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

function Normalize-Email {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

if (-not (Test-Path $PlanCsv)) { throw "Plan CSV not found: $PlanCsv" }

$planByDomain = @{}
foreach ($row in @(Import-Csv -Path $PlanCsv)) {
    $domain = Normalize-DomainName ([string]$row.domain)
    if ($domain) { $planByDomain[$domain] = $row }
}

$domainRows = @((Invoke-SupabaseApi -Method GET -Table "domains" -Query "select=id,domain,customer_id,status,interim_status,action_history&limit=5000").Data | Where-Object {
    $planByDomain.ContainsKey((Normalize-DomainName ([string]$_.domain)))
})

$targets = [System.Collections.Generic.List[object]]::new()
foreach ($domain in $domainRows) {
    $domainId = [string]$domain.id
    $domainName = Normalize-DomainName ([string]$domain.domain)
    $planRow = $planByDomain[$domainName]
    $destination = Normalize-Email ([string]$planRow.proposed_destination_admin)
    if ([string]$domain.status -eq "active" -and [string]$domain.interim_status -eq "Both - Provisioning Complete") { continue }

    $currentAdmin = Get-AssignedAdmin -DomainId $domainId
    $currentAdminEmail = Normalize-Email ([string]$currentAdmin.email)
    if (-not $currentAdminEmail -or $currentAdminEmail -ne $destination) { continue }

    $activeCount = @(Get-DomainInboxes -DomainId $domainId -Status "active").Count
    if ($activeCount -ne $ExpectedInboxes) { continue }

    $actionResult = Invoke-SupabaseApi -Method GET -Table "actions" -Query "domain_id=eq.$domainId&type=in.(provision_inbox,reupload_inboxes)&order=updated_at.desc&limit=50&select=id,type,status,error,payload,updated_at"
    if (-not $actionResult.Success) { continue }
    $actions = @($actionResult.Data)
    $provision = @($actions | Where-Object {
        [string]$_.type -eq "provision_inbox" -and
        [string]$_.status -eq "completed" -and
        $_.payload -and
        [string]$_.payload.source -eq "jack_threshold_tenant_migration"
    } | Sort-Object updated_at -Descending | Select-Object -First 1)
    if ($provision.Count -eq 0) { continue }

    $provisionId = [string]$provision[0].id
    $upload = @($actions | Where-Object {
        [string]$_.type -eq "reupload_inboxes" -and
        [string]$_.status -eq "completed" -and
        $_.payload -and
        [string]$_.payload.provision_action_id -eq $provisionId
    } | Sort-Object updated_at -Descending | Select-Object -First 1)
    if ($upload.Count -eq 0) { continue }

    $targets.Add([pscustomobject]@{
        Domain = $domain
        DomainName = $domainName
        ActiveCount = $activeCount
        ProvisionActionId = $provisionId
        UploadActionId = [string]$upload[0].id
        CurrentStatus = [string]$domain.status
        CurrentInterim = [string]$domain.interim_status
    }) | Out-Null
}

Write-Host "Matched $($targets.Count) completed Jack status mismatch domain(s). Live=$([bool]$Live)"
foreach ($target in $targets) {
    Write-Host "$($target.DomainName) status=$($target.CurrentStatus) interim=$($target.CurrentInterim) active=$($target.ActiveCount) provision=$($target.ProvisionActionId) upload=$($target.UploadActionId)"
    if (-not $Live) { continue }

    $history = if ($target.Domain.action_history) { [string]$target.Domain.action_history } else { "" }
    $history = Add-HistoryEntry -History $history -Entry "REPAIR: Marked complete after linked replacement provision and upload were already completed."
    Update-Domain -DomainId ([string]$target.Domain.id) -Fields @{
        status = "active"
        interim_status = "Both - Provisioning Complete"
        action_history = $history
    }
    Add-ActionLog -ActionId $target.UploadActionId -DomainId ([string]$target.Domain.id) -CustomerId ([string]$target.Domain.customer_id) -EventType "completed_status_mismatch_repaired" -Severity "info" -Message "Domain row marked complete after linked replacement provision/upload proof." -Metadata @{
        provision_action_id = $target.ProvisionActionId
        active_inboxes = $target.ActiveCount
        previous_status = $target.CurrentStatus
        previous_interim_status = $target.CurrentInterim
    }
}
