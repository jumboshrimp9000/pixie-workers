<#
.SYNOPSIS
    Batch launcher for Jack/ProfitPath threshold-tenant migrations.
.DESCRIPTION
    Starts multiple one-domain threshold migrations in parallel while relying on
    Microsoft admin locks for safety. The launcher also avoids running two
    domains from the same source tenant in the same wave to reduce lock churn.
#>

param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [string]$ValidationCsv = (Join-Path $PSScriptRoot "logs/microsoft-admin-destination-validation-237.csv"),
    [int]$Limit = 10,
    [int]$MaxParallel = 4,
    [string[]]$Domains = @(),
    [string]$DomainsFile = "",
    [string[]]$SkipDomains = @(),
    [string]$SkipDomainsFile = "",
    [switch]$Live,
    [switch]$EnqueueProvisionOnly,
    [switch]$IncludeAlreadyMoved,
    [switch]$IncludeOpenActions,
    [string]$ConfirmText = "",
    [int]$ExpectedInboxes = 99
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

function Quote-Arg {
    param([string]$Value)
    return '"' + ([string]$Value).Replace('"', '\"') + '"'
}

if (-not (Test-Path $PlanCsv)) { throw "Plan CSV not found: $PlanCsv" }
if (-not (Test-Path $ValidationCsv)) { throw "Validation CSV not found: $ValidationCsv" }
if ($MaxParallel -lt 1) { throw "MaxParallel must be at least 1" }
if ($Limit -lt 1) { throw "Limit must be at least 1" }

$expectedConfirm = "MOVE JACK THRESHOLD DOMAINS"
if ($Live -and $ConfirmText -ne $expectedConfirm) {
    throw "Live batch migration requires ConfirmText exactly: $expectedConfirm"
}

$domainFilter = @{}
foreach ($domain in @(Get-DomainInputs -InlineDomains $Domains -FilePath $DomainsFile)) {
    foreach ($item in ([string]$domain -split ",")) {
        $clean = Normalize-DomainName $item
        if ($clean) { $domainFilter[$clean] = $true }
    }
}

$skipFilter = @{}
foreach ($domain in @(Get-DomainInputs -InlineDomains $SkipDomains -FilePath $SkipDomainsFile)) {
    foreach ($item in ([string]$domain -split ",")) {
        $clean = Normalize-DomainName $item
        if ($clean) { $skipFilter[$clean] = $true }
    }
}

$validationByAdmin = @{}
foreach ($row in @(Import-Csv -Path $ValidationCsv)) {
    $admin = Normalize-Email ([string]$row.AdminEmail)
    if (-not $admin -or $admin -eq "adminemail") { continue }
    $validationByAdmin[$admin] = $row
}

$currentAdminByDomain = @{}
$domainIdByDomain = @{}
$openActionsByDomainId = @{}
try {
    $domainResult = Invoke-SupabaseApi -Method GET -Table "domains" -Query "select=id,domain&limit=5000"
    $adminResult = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query "provider=eq.microsoft&select=id,email,status,active&limit=5000"
    $assignmentResult = Invoke-SupabaseApi -Method GET -Table "domain_admin_assignments" -Query "select=domain_id,admin_cred_id,assigned_at&order=assigned_at.desc&limit=30000"
    $actionResult = Invoke-SupabaseApi -Method GET -Table "actions" -Query "type=in.(microsoft_cancel_domain,provision_inbox)&status=in.(pending,in_progress)&select=id,domain_id,type,status,payload&limit=10000"

    if ($domainResult.Success -and $adminResult.Success -and $assignmentResult.Success) {
        $domainById = @{}
        foreach ($domainRow in @($domainResult.Data)) {
            $name = Normalize-DomainName ([string]$domainRow.domain)
            if (-not $name) { continue }
            $domainById[[string]$domainRow.id] = $name
            $domainIdByDomain[$name] = [string]$domainRow.id
        }

        $adminById = @{}
        foreach ($adminRow in @($adminResult.Data)) {
            $adminById[[string]$adminRow.id] = Normalize-Email ([string]$adminRow.email)
        }

        foreach ($assignment in @($assignmentResult.Data)) {
            $domainId = [string]$assignment.domain_id
            if (-not $domainById.ContainsKey($domainId)) { continue }
            $domainName = $domainById[$domainId]
            if ($currentAdminByDomain.ContainsKey($domainName)) { continue }
            $adminId = [string]$assignment.admin_cred_id
            if ($adminById.ContainsKey($adminId)) {
                $currentAdminByDomain[$domainName] = $adminById[$adminId]
            }
        }
    }

    if ($actionResult.Success) {
        foreach ($action in @($actionResult.Data)) {
            $domainId = [string]$action.domain_id
            if (-not $domainId) { continue }
            $isReusablePendingCancel = (
                [string]$action.type -eq "microsoft_cancel_domain" -and
                [string]$action.status -eq "pending" -and
                $action.payload -and
                [string]$action.payload.source -eq "jack_threshold_tenant_migration"
            )
            if ($isReusablePendingCancel) { continue }
            $openActionsByDomainId[$domainId] = $true
        }
    }
} catch {
    Write-Warning "Live DB skip preflight failed; continuing with CSV/explicit skips only: $($_.Exception.Message)"
}

$rows = @()
foreach ($row in @(Import-Csv -Path $PlanCsv)) {
    $domain = Normalize-DomainName ([string]$row.domain)
    if (-not $domain) { continue }
    if ($domainFilter.Count -gt 0 -and -not $domainFilter.ContainsKey($domain)) { continue }
    if ($skipFilter.ContainsKey($domain)) { continue }

    $destination = Normalize-Email ([string]$row.proposed_destination_admin)
    if (-not $destination -or -not $validationByAdmin.ContainsKey($destination)) { continue }
    $validation = [string]$validationByAdmin[$destination].Validation
    if ($AcceptedValidationStatuses -notcontains $validation) { continue }

    if (-not $IncludeAlreadyMoved -and $currentAdminByDomain.ContainsKey($domain) -and $currentAdminByDomain[$domain] -eq $destination) {
        continue
    }

    if (-not $IncludeOpenActions -and $domainIdByDomain.ContainsKey($domain) -and $openActionsByDomainId.ContainsKey($domainIdByDomain[$domain])) {
        continue
    }

    $rows += [pscustomobject]@{
        Domain = $domain
        SourceAdmin = Normalize-Email ([string]$row.source_admin)
        DestinationAdmin = $destination
        Validation = $validation
    }
}

$rows = @($rows | Select-Object -First $Limit)
if ($rows.Count -eq 0) { throw "No eligible domains found in plan with clean destination validation." }

$singleScript = Join-Path $PSScriptRoot "invoke-jack-threshold-domain-move.ps1"
if (-not (Test-Path $singleScript)) { throw "Single-domain migration script missing: $singleScript" }

$runStamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$logDir = Join-Path $PSScriptRoot "logs/jack-threshold-migration-batch-$runStamp"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

Write-Host "Batch prepared: $($rows.Count) eligible domains, max parallel $MaxParallel, logs $logDir"
if (-not $Live) {
    Write-Host "Dry run only. Add -Live -ConfirmText '$expectedConfirm' to execute child migrations."
    $rows | Select-Object Domain, SourceAdmin, DestinationAdmin, Validation | Format-Table -AutoSize
    return
}

$pending = [System.Collections.Generic.List[object]]::new()
foreach ($row in $rows) { [void]$pending.Add($row) }
$active = [System.Collections.Generic.List[object]]::new()
$completed = [System.Collections.Generic.List[object]]::new()

while ($pending.Count -gt 0 -or $active.Count -gt 0) {
    while ($pending.Count -gt 0 -and $active.Count -lt $MaxParallel) {
        $activeSources = @{}
        foreach ($item in $active) { $activeSources[$item.Row.SourceAdmin] = $true }

        $nextIndex = -1
        for ($i = 0; $i -lt $pending.Count; $i++) {
            if (-not $activeSources.ContainsKey($pending[$i].SourceAdmin)) {
                $nextIndex = $i
                break
            }
        }
        if ($nextIndex -lt 0) { break }

        $row = $pending[$nextIndex]
        $pending.RemoveAt($nextIndex)

        $safeDomain = $row.Domain.Replace("/", "_").Replace(":", "_")
        $stdout = Join-Path $logDir "$safeDomain.out.log"
        $stderr = Join-Path $logDir "$safeDomain.err.log"

        $childConfirm = "MOVE $($row.Domain) TO $($row.DestinationAdmin)"
        $args = @(
            "-NoProfile",
            "-File", (Quote-Arg $singleScript),
            "-Domain", (Quote-Arg $row.Domain),
            "-DestinationAdminEmail", (Quote-Arg $row.DestinationAdmin),
            "-ValidationCsv", (Quote-Arg $ValidationCsv),
            "-ExpectedInboxes", ([string]$ExpectedInboxes)
        )
        if ($Live) {
            $args += @("-Live", "-ConfirmText", (Quote-Arg $childConfirm))
        }
        if ($EnqueueProvisionOnly) {
            $args += @("-EnqueueProvisionOnly")
        }

        $process = Start-Process -FilePath "pwsh" -ArgumentList $args -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
        [void]$active.Add([pscustomobject]@{
            Row = $row
            Process = $process
            Stdout = $stdout
            Stderr = $stderr
            StartedAt = Get-Date
        })
        Write-Host "Started $($row.Domain): source=$($row.SourceAdmin) dest=$($row.DestinationAdmin) pid=$($process.Id)"
    }

    Start-Sleep -Seconds 5

    for ($i = $active.Count - 1; $i -ge 0; $i--) {
        $item = $active[$i]
        if (-not $item.Process.HasExited) { continue }

        $exitCode = $item.Process.ExitCode
        $status = if ($exitCode -eq 0) { "completed" } else { "failed" }
        [void]$completed.Add([pscustomobject]@{
            Domain = $item.Row.Domain
            SourceAdmin = $item.Row.SourceAdmin
            DestinationAdmin = $item.Row.DestinationAdmin
            Status = $status
            ExitCode = $exitCode
            Stdout = $item.Stdout
            Stderr = $item.Stderr
        })
        Write-Host "Finished $($item.Row.Domain): $status exit=$exitCode"
        $active.RemoveAt($i)
    }
}

$summaryPath = Join-Path $logDir "summary.csv"
$completed | Export-Csv -Path $summaryPath -NoTypeInformation
$failures = @($completed | Where-Object { $_.Status -ne "completed" })
Write-Host "Batch finished: completed=$(@($completed | Where-Object Status -eq completed).Count), failed=$($failures.Count), summary=$summaryPath"
if ($failures.Count -gt 0) { exit 1 }
