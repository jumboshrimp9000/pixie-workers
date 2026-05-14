<#
.SYNOPSIS
    Repairs Jack migration domains stuck in final verification despite direct proof.
.DESCRIPTION
    Some replacement provision actions can remain open even after the linked
    Instantly upload completed and the domain verifies cleanly through the
    direct domain verifier. This script only repairs domains that have:

      - current admin equal to the migration plan destination
      - exactly the expected active inbox count
      - a completed reupload_inboxes action
      - an in-progress/pending jack_threshold_tenant_migration provision action
      - a successful direct Instantly proof for count, tag, and settings

    It does not create mailboxes or upload inboxes.
#>

param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [int]$ExpectedInboxes = 99,
    [string[]]$Domains = @(),
    [string]$DomainsFile = "",
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

function Get-DomainInputs {
    param(
        [string[]]$InlineDomains,
        [string]$FilePath
    )

    $values = [System.Collections.Generic.List[string]]::new()
    foreach ($domain in @($InlineDomains)) {
        if (-not [string]::IsNullOrWhiteSpace([string]$domain)) {
            $values.Add([string]$domain) | Out-Null
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($FilePath)) {
        if (-not (Test-Path $FilePath)) {
            throw "Domains file not found: $FilePath"
        }

        if ([System.IO.Path]::GetExtension($FilePath).ToLowerInvariant() -eq ".csv") {
            foreach ($row in @(Import-Csv -Path $FilePath)) {
                foreach ($column in @("domain", "Domain", "new_domain", "replacement_domain")) {
                    $candidate = [string]$row.$column
                    if (-not [string]::IsNullOrWhiteSpace($candidate)) {
                        $values.Add($candidate) | Out-Null
                        break
                    }
                }
            }
        } else {
            foreach ($line in @(Get-Content -Path $FilePath)) {
                if (-not [string]::IsNullOrWhiteSpace([string]$line)) {
                    $values.Add([string]$line) | Out-Null
                }
            }
        }
    }

    return @($values)
}

if (-not (Test-Path $PlanCsv)) { throw "Plan CSV not found: $PlanCsv" }

$verifyScript = Join-Path $PSScriptRoot "verify-profitpath-instantly-domain.ps1"
if (-not (Test-Path $verifyScript)) { throw "Verifier not found: $verifyScript" }

$planByDomain = @{}
foreach ($row in @(Import-Csv -Path $PlanCsv)) {
    $domain = Normalize-DomainName ([string]$row.domain)
    if ($domain) { $planByDomain[$domain] = $row }
}

$domainFilter = @{}
foreach ($domain in @(Get-DomainInputs -InlineDomains $Domains -FilePath $DomainsFile)) {
    $normalized = Normalize-DomainName $domain
    if ($normalized) { $domainFilter[$normalized] = $true }
}

$domainRows = @((Invoke-SupabaseApi -Method GET -Table "domains" -Query "select=id,domain,customer_id,status,interim_status,action_history&limit=5000").Data | Where-Object {
    $planByDomain.ContainsKey((Normalize-DomainName ([string]$_.domain)))
})

$targets = [System.Collections.Generic.List[object]]::new()
foreach ($domain in $domainRows) {
    $domainId = [string]$domain.id
    $domainName = Normalize-DomainName ([string]$domain.domain)
    if ($domainFilter.Count -gt 0 -and -not $domainFilter.ContainsKey($domainName)) { continue }
    $planRow = $planByDomain[$domainName]
    $destination = Normalize-Email ([string]$planRow.proposed_destination_admin)
    if ([string]$domain.status -eq "active" -and [string]$domain.interim_status -eq "Both - Provisioning Complete") { continue }

    $currentAdmin = Get-AssignedAdmin -DomainId $domainId
    $currentAdminEmail = Normalize-Email ([string]$currentAdmin.email)
    if (-not $currentAdminEmail -or $currentAdminEmail -ne $destination) { continue }

    $activeCount = @(Get-DomainInboxes -DomainId $domainId -Status "active").Count
    if ($activeCount -ne $ExpectedInboxes) { continue }

    $actionResult = Invoke-SupabaseApi -Method GET -Table "actions" -Query "domain_id=eq.$domainId&type=in.(provision_inbox,reupload_inboxes)&order=updated_at.desc&limit=50&select=id,type,status,error,payload,updated_at,started_at,attempts,max_attempts"
    if (-not $actionResult.Success) { continue }
    $actions = @($actionResult.Data)

    $provision = @($actions | Where-Object {
        [string]$_.type -eq "provision_inbox" -and
        [string]$_.status -in @("in_progress", "pending") -and
        $_.payload -and
        [string]$_.payload.source -eq "jack_threshold_tenant_migration"
    } | Sort-Object updated_at -Descending | Select-Object -First 1)
    if ($provision.Count -eq 0) { continue }

    $upload = @($actions | Where-Object {
        [string]$_.type -eq "reupload_inboxes" -and
        [string]$_.status -eq "completed"
    } | Sort-Object updated_at -Descending | Select-Object -First 1)
    if ($upload.Count -eq 0) { continue }

    try {
        $proofJson = pwsh -NoProfile -File $verifyScript -Domain $domainName -ExpectedCount $ExpectedInboxes 2>$null | Out-String
        $proof = $proofJson | ConvertFrom-Json
    } catch {
        continue
    }
    if (-not $proof -or -not $proof.ok) { continue }

    $targets.Add([pscustomobject]@{
        Domain = $domain
        DomainName = $domainName
        ActiveCount = $activeCount
        ProvisionAction = $provision[0]
        UploadAction = $upload[0]
        CurrentStatus = [string]$domain.status
        CurrentInterim = [string]$domain.interim_status
        Proof = $proof
    }) | Out-Null
}

Write-Host "Matched $($targets.Count) direct-proof finalization gap domain(s). Live=$([bool]$Live)"
foreach ($target in $targets) {
    Write-Host "$($target.DomainName) status=$($target.CurrentStatus) interim=$($target.CurrentInterim) active=$($target.ActiveCount) provision=$($target.ProvisionAction.id) upload=$($target.UploadAction.id)"
    if (-not $Live) { continue }

    $history = if ($target.Domain.action_history) { [string]$target.Domain.action_history } else { "" }
    $history = Add-HistoryEntry -History $history -Entry "REPAIR: Marked complete after direct Instantly proof confirmed count/tag/settings."
    Update-Domain -DomainId ([string]$target.Domain.id) -Fields @{
        status = "active"
        interim_status = "Both - Provisioning Complete"
        action_history = $history
    }

    Update-ActionStatus -ActionId ([string]$target.ProvisionAction.id) -Status "completed" -Result @{
        domain = $target.DomainName
        active_inboxes_verified = $target.ActiveCount
        upload_action_id = [string]$target.UploadAction.id
        upload_validated = $true
        uploaded = [int]$target.Proof.instantlyChecked
        repaired_via_direct_instantly_proof = $true
        direct_instantly_proof = @{
            tag = [string]$target.Proof.tag
            tag_mapped_count = [int]$target.Proof.tagMappedCount
            db_active_inboxes = [int]$target.Proof.dbActiveInboxes
            instantly_checked = [int]$target.Proof.instantlyChecked
            account_failure_count = [int]$target.Proof.accountFailureCount
        }
    } -Action $target.ProvisionAction | Out-Null

    Add-ActionLog -ActionId ([string]$target.ProvisionAction.id) -DomainId ([string]$target.Domain.id) -CustomerId ([string]$target.Domain.customer_id) -EventType "direct_proof_finalization_repaired" -Severity "info" -Message "Provision action marked complete after direct Instantly proof verified the replacement domain." -Metadata @{
        upload_action_id = [string]$target.UploadAction.id
        active_inboxes = $target.ActiveCount
        previous_status = $target.CurrentStatus
        previous_interim_status = $target.CurrentInterim
        tag_mapped_count = [int]$target.Proof.tagMappedCount
        instantly_checked = [int]$target.Proof.instantlyChecked
    }
}
