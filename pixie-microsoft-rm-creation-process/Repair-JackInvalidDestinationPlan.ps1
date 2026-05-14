<#
.SYNOPSIS
    Repairs Jack threshold migration plan rows whose destination validation failed.
.DESCRIPTION
    The migration controller refuses to move a domain when the planned
    destination admin is not in a clean validation state. This script replaces
    those bad destinations with already-proven, already-validated-clean Active
    Microsoft admins from the same plan, then writes a timestamped backup before
    updating the plan CSV.

    It is intentionally conservative: alternate destinations must be Active,
    unlocked, present in the clean validation CSV, and already attached to a
    fully-proven Jack migration domain.
#>

param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [string]$ValidationCsv = (Join-Path $PSScriptRoot "logs/microsoft-admin-destination-validation-237.csv"),
    [int]$ExpectedInboxes = 99,
    [switch]$Live
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "config.ps1")

$AcceptedValidationStatuses = @(
    "auth_ok_no_recent_external_outbound",
    "auth_ok_no_threshold_evidence"
)

function Normalize-Email {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Normalize-DomainName {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Get-StatusReport {
    $raw = pwsh -NoProfile -File (Join-Path $PSScriptRoot "Get-JackThresholdMigrationStatus.ps1") -PlanCsv $PlanCsv -ExpectedInboxes $ExpectedInboxes -Json 2>$null | Out-String
    $idx = $raw.IndexOf("{")
    if ($idx -lt 0) { throw "Status report did not return JSON: $raw" }
    return $raw.Substring($idx) | ConvertFrom-Json
}

if (-not (Test-Path $PlanCsv)) { throw "Plan CSV not found: $PlanCsv" }
if (-not (Test-Path $ValidationCsv)) { throw "Validation CSV not found: $ValidationCsv" }

$plan = @(Import-Csv -Path $PlanCsv)
$validationRows = @(Import-Csv -Path $ValidationCsv)

$validationByAdmin = @{}
foreach ($row in $validationRows) {
    $admin = Normalize-Email ([string]$row.AdminEmail)
    if ($admin) { $validationByAdmin[$admin] = $row }
}

$cleanValidationByAdmin = @{}
foreach ($row in $validationRows) {
    $admin = Normalize-Email ([string]$row.AdminEmail)
    if (-not $admin) { continue }
    if ($AcceptedValidationStatuses -contains [string]$row.Validation) {
        $cleanValidationByAdmin[$admin] = $row
    }
}

$status = Get-StatusReport
$provenDomains = @{}
foreach ($domain in @($status.fully_proven_domain_names)) {
    $name = Normalize-DomainName ([string]$domain)
    if ($name) { $provenDomains[$name] = $true }
}

$adminsResult = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query "provider=eq.microsoft&select=id,email,status,active,locked_by_action_id,locked_domain_id,usage_count&limit=10000"
if (-not $adminsResult.Success) { throw "Failed to load admin credentials: $($adminsResult.Error)" }

$adminByEmail = @{}
foreach ($admin in @($adminsResult.Data)) {
    $email = Normalize-Email ([string]$admin.email)
    if ($email) { $adminByEmail[$email] = $admin }
}

$badRows = @()
foreach ($row in $plan) {
    $domain = Normalize-DomainName ([string]$row.domain)
    $destination = Normalize-Email ([string]$row.proposed_destination_admin)
    if (-not $domain -or -not $destination) { continue }

    $isNotMoved = $false
    foreach ($sample in @($status.stuck_or_unproven_samples)) {
        if ((Normalize-DomainName ([string]$sample.domain)) -eq $domain -and -not ((Normalize-Email ([string]$sample.current_admin)) -eq $destination)) {
            $isNotMoved = $true
            break
        }
    }

    $validation = if ($validationByAdmin.ContainsKey($destination)) { [string]$validationByAdmin[$destination].Validation } else { "missing" }
    if ($isNotMoved -and ($AcceptedValidationStatuses -notcontains $validation)) {
        $badRows += $row
    }
}

if ($badRows.Count -eq 0) {
    Write-Output "No invalid destination plan rows found from the current status samples."
    return
}

$candidateRows = foreach ($row in $plan) {
    $domain = Normalize-DomainName ([string]$row.domain)
    $destination = Normalize-Email ([string]$row.proposed_destination_admin)
    if (-not $provenDomains.ContainsKey($domain)) { continue }
    if (-not $cleanValidationByAdmin.ContainsKey($destination)) { continue }
    if (-not $adminByEmail.ContainsKey($destination)) { continue }

    $admin = $adminByEmail[$destination]
    if ([string]$admin.status -ne "Active" -or -not [bool]$admin.active) { continue }
    if ($admin.locked_by_action_id) { continue }

    [pscustomobject]@{
        Admin = $destination
        ProvenDomain = $domain
        Usage = if ($admin.usage_count -ne $null) { [int]$admin.usage_count } else { 0 }
        Outbound = if ($cleanValidationByAdmin[$destination].ExternalOutboundCount -ne $null -and [string]$cleanValidationByAdmin[$destination].ExternalOutboundCount -ne "") { [int]$cleanValidationByAdmin[$destination].ExternalOutboundCount } else { 0 }
        Failed = if ($cleanValidationByAdmin[$destination].ExternalFailedCount -ne $null -and [string]$cleanValidationByAdmin[$destination].ExternalFailedCount -ne "") { [int]$cleanValidationByAdmin[$destination].ExternalFailedCount } else { 0 }
    }
}

$candidateRows = @($candidateRows | Sort-Object Usage, Outbound, Failed, Admin)
if ($candidateRows.Count -lt $badRows.Count) {
    throw "Only $($candidateRows.Count) clean proven destination candidate(s) available for $($badRows.Count) invalid destination row(s)."
}

$usedAdmins = @{}
$repairs = [System.Collections.Generic.List[object]]::new()
foreach ($row in $badRows) {
    $domain = Normalize-DomainName ([string]$row.domain)
    $source = Normalize-Email ([string]$row.source_admin)
    $oldDestination = Normalize-Email ([string]$row.proposed_destination_admin)
    $chosen = $null
    foreach ($candidate in $candidateRows) {
        if ($usedAdmins.ContainsKey($candidate.Admin)) { continue }
        if ($candidate.Admin -eq $source -or $candidate.Admin -eq $oldDestination) { continue }
        $chosen = $candidate
        break
    }
    if (-not $chosen) { throw "No clean alternate destination found for $domain" }

    $usedAdmins[$chosen.Admin] = $true
    $repairs.Add([pscustomobject]@{
        Domain = $domain
        OldDestination = $oldDestination
        NewDestination = $chosen.Admin
        NewDestinationProofDomain = $chosen.ProvenDomain
        NewDestinationUsage = $chosen.Usage
    }) | Out-Null
}

$repairs | Format-Table -AutoSize
if (-not $Live) {
    Write-Output "Dry run only. Add -Live to update the plan CSV."
    return
}

$backup = "$PlanCsv.bak-$(Get-Date -Format yyyyMMddHHmmss)"
Copy-Item -Path $PlanCsv -Destination $backup -Force

foreach ($repair in $repairs) {
    foreach ($row in $plan) {
        if ((Normalize-DomainName ([string]$row.domain)) -eq $repair.Domain) {
            $row.proposed_destination_admin = $repair.NewDestination
            $row.destination_candidate_type = "validated_clean_alternate"
        }
    }
}

$plan | Export-Csv -Path $PlanCsv -NoTypeInformation
Write-Output "Updated plan CSV: $PlanCsv"
Write-Output "Backup written: $backup"
