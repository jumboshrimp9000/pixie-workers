<#
.SYNOPSIS
    Recurring Microsoft tenant threshold monitor.
.DESCRIPTION
    Runs the active tenant threshold probe across active Microsoft admins, marks
    confirmed threshold tenants out of rotation, and writes domain impact reports
    using the latest domain_admin_assignments row as the current admin.
#>

param(
    [int]$ShardCount = 4,
    [int]$TracePolls = 4,
    [int]$TracePollSeconds = 60,
    [string]$Recipient = "leads+99@justresultsagency.com",
    [string]$RunId = "",
    [string]$LogRoot = (Join-Path $PSScriptRoot "logs/tenant-threshold-monitor"),
    [switch]$SkipProbe
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

. (Join-Path $PSScriptRoot "config.ps1")

if (-not $RunId) { $RunId = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ") }
$runDir = Join-Path $LogRoot $RunId
New-Item -ItemType Directory -Force -Path $runDir | Out-Null

function Write-MonitorLog {
    param([string]$Message, [string]$Level = "INFO")
    $line = "[{0}] [{1}] {2}" -f (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss"), $Level, $Message
    Add-Content -Path (Join-Path $runDir "monitor.log") -Value $line
    Write-Host $line
}

function Invoke-MonitorApiRows {
    param(
        [string]$Table,
        [string]$Query,
        [string]$Label
    )

    $result = Invoke-SupabaseApi -Method GET -Table $Table -Query $Query
    if (-not $result.Success) { throw "$Label failed: $($result.Error)" }
    return @($result.Data)
}

function Get-AllMonitorRows {
    param(
        [string]$Table,
        [string]$Select,
        [string]$ExtraQuery = "",
        [int]$PageSize = 1000
    )

    $rows = [System.Collections.Generic.List[object]]::new()
    for ($offset = 0; ; $offset += $PageSize) {
        $query = "select=$Select&limit=$PageSize&offset=$offset"
        if ($ExtraQuery) { $query += "&$ExtraQuery" }
        $page = @(Invoke-MonitorApiRows -Table $Table -Query $query -Label "$Table offset $offset")
        foreach ($row in $page) { $rows.Add($row) | Out-Null }
        if ($page.Count -lt $PageSize) { break }
    }
    return @($rows.ToArray())
}

function Set-AdminStatus {
    param(
        [string]$AdminId,
        [hashtable]$Fields
    )
    $Fields.updated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $result = Invoke-SupabaseApi -Method PATCH -Table "admin_credentials" -Query "id=eq.$AdminId" -Body $Fields
    if (-not $result.Success) { throw "Failed to update admin $AdminId`: $($result.Error)" }
}

function Read-ProbeResults {
    $results = [System.Collections.Generic.List[object]]::new()
    foreach ($file in @(Get-ChildItem -Path $runDir -Filter "shard-*.jsonl" -ErrorAction SilentlyContinue)) {
        foreach ($line in @(Get-Content -Path $file.FullName -ErrorAction SilentlyContinue)) {
            if (-not ([string]$line).Trim()) { continue }
            try {
                $results.Add(($line | ConvertFrom-Json)) | Out-Null
            } catch {
                Write-MonitorLog "Could not parse JSONL row in $($file.Name): $($_.Exception.Message)" "WARN"
            }
        }
    }
    return @($results.ToArray())
}

function Invoke-ThresholdProbeShards {
    $probeScript = Join-Path $PSScriptRoot "Test-JackDestinationTenantActiveThreshold.ps1"
    if (-not (Test-Path $probeScript)) { throw "Missing threshold probe script: $probeScript" }

    Write-MonitorLog "Starting threshold probe: shard_count=$ShardCount recipient=$Recipient run_id=$RunId"
    $processes = [System.Collections.Generic.List[object]]::new()
    for ($i = 0; $i -lt $ShardCount; $i++) {
        $outPath = Join-Path $runDir "shard-$i.out"
        $errPath = Join-Path $runDir "shard-$i.err"
        $jsonlPath = Join-Path $runDir "shard-$i.jsonl"
        $csvPath = Join-Path $runDir "shard-$i.csv"
        $args = @(
            "-NoLogo", "-NoProfile", "-File", $probeScript,
            "-AllActiveMicrosoftTenants",
            "-ShardCount", ([string]$ShardCount),
            "-ShardIndex", ([string]$i),
            "-RunId", $RunId,
            "-Recipient", $Recipient,
            "-TracePolls", ([string]$TracePolls),
            "-TracePollSeconds", ([string]$TracePollSeconds),
            "-OutputJsonl", $jsonlPath,
            "-OutputCsv", $csvPath
        )
        $process = Start-Process -FilePath "pwsh" -ArgumentList $args -PassThru -RedirectStandardOutput $outPath -RedirectStandardError $errPath
        $processes.Add([pscustomobject]@{ Shard = $i; Process = $process; Out = $outPath; Err = $errPath }) | Out-Null
        Write-MonitorLog "Started shard $i pid=$($process.Id)"
    }

    foreach ($entry in @($processes.ToArray())) {
        Wait-Process -Id $entry.Process.Id
        $entry.Process.Refresh()
        Write-MonitorLog "Shard $($entry.Shard) exited code=$($entry.Process.ExitCode)"
    }

    $failed = @($processes | Where-Object { $_.Process.ExitCode -ne 0 })
    return @($failed)
}

function Write-DomainImpactReports {
    param([object[]]$ProbeResults)

    Write-MonitorLog "Loading current admin/domain assignment state"
    $admins = @(Get-AllMonitorRows -Table "admin_credentials" -Select "id,email,provider,status,active,usage_count,last_used" -ExtraQuery "provider=eq.microsoft")
    $domains = @(Get-AllMonitorRows -Table "domains" -Select "id,domain,status,interim_status,provider,customer_id,workspace_id,order_batch_id,updated_at")
    $assignments = @(Get-AllMonitorRows -Table "domain_admin_assignments" -Select "domain_id,admin_cred_id,assigned_at" -ExtraQuery "order=assigned_at.desc")

    $adminById = @{}
    foreach ($admin in $admins) { if ($admin.id) { $adminById[[string]$admin.id] = $admin } }

    $domainById = @{}
    foreach ($domain in $domains) { if ($domain.id) { $domainById[[string]$domain.id] = $domain } }

    $latestAssignmentByDomainId = @{}
    foreach ($assignment in $assignments) {
        $domainId = [string]$assignment.domain_id
        if ($domainId -and -not $latestAssignmentByDomainId.ContainsKey($domainId)) {
            $latestAssignmentByDomainId[$domainId] = $assignment
        }
    }

    $probeByAdminId = @{}
    foreach ($result in $ProbeResults) {
        if ($result.admin_id) { $probeByAdminId[[string]$result.admin_id] = $result }
    }

    $thresholdAdminIds = @{}
    $unknownAdminIds = @{}
    foreach ($admin in $admins) {
        $status = ([string]$admin.status).Trim().ToLowerInvariant()
        if ($status -eq "threshold exceeded") { $thresholdAdminIds[[string]$admin.id] = $true }
    }
    foreach ($result in $ProbeResults) {
        $adminId = [string]$result.admin_id
        if (-not $adminId) { continue }
        if ([string]$result.result -eq "threshold") { $thresholdAdminIds[$adminId] = $true }
        if ([string]$result.result -eq "unknown") { $unknownAdminIds[$adminId] = $true }
    }

    $thresholdDomainRows = [System.Collections.Generic.List[object]]::new()
    $unknownDomainRows = [System.Collections.Generic.List[object]]::new()
    foreach ($domainId in $latestAssignmentByDomainId.Keys) {
        $assignment = $latestAssignmentByDomainId[$domainId]
        $adminId = [string]$assignment.admin_cred_id
        if (-not $adminId -or -not $adminById.ContainsKey($adminId) -or -not $domainById.ContainsKey($domainId)) { continue }

        $admin = $adminById[$adminId]
        $domain = $domainById[$domainId]
        $probe = if ($probeByAdminId.ContainsKey($adminId)) { $probeByAdminId[$adminId] } else { $null }
        $row = [pscustomobject]@{
            domain = ([string]$domain.domain).Trim().ToLowerInvariant()
            domain_id = $domainId
            domain_status = [string]$domain.status
            domain_interim_status = [string]$domain.interim_status
            admin_id = $adminId
            admin_email = ([string]$admin.email).Trim().ToLowerInvariant()
            admin_status = [string]$admin.status
            admin_active = [bool]$admin.active
            latest_assigned_at = [string]$assignment.assigned_at
            latest_probe_result = if ($probe) { [string]$probe.result } else { "" }
            latest_probe_error = if ($probe) { [string]$probe.error } else { "" }
        }

        if ($thresholdAdminIds.ContainsKey($adminId)) {
            $thresholdDomainRows.Add($row) | Out-Null
        } elseif ($unknownAdminIds.ContainsKey($adminId)) {
            $unknownDomainRows.Add($row) | Out-Null
        }
    }

    $adminSummaryRows = [System.Collections.Generic.List[object]]::new()
    foreach ($admin in @($admins | Sort-Object email)) {
        $adminId = [string]$admin.id
        $currentDomainCount = @($latestAssignmentByDomainId.Values | Where-Object { [string]$_.admin_cred_id -eq $adminId }).Count
        $probe = if ($probeByAdminId.ContainsKey($adminId)) { $probeByAdminId[$adminId] } else { $null }
        $adminSummaryRows.Add([pscustomobject]@{
            admin_id = $adminId
            admin_email = ([string]$admin.email).Trim().ToLowerInvariant()
            admin_status = [string]$admin.status
            admin_active = [bool]$admin.active
            current_domain_count = $currentDomainCount
            latest_probe_result = if ($probe) { [string]$probe.result } else { "" }
            latest_probe_error = if ($probe) { [string]$probe.error } else { "" }
        }) | Out-Null
    }

    $thresholdDomainCsv = Join-Path $runDir "threshold-admin-current-domains.csv"
    $unknownDomainCsv = Join-Path $runDir "unknown-admin-current-domains.csv"
    $adminSummaryCsv = Join-Path $runDir "admin-threshold-summary.csv"
    @($thresholdDomainRows.ToArray() | Sort-Object admin_email, domain) | Export-Csv -Path $thresholdDomainCsv -NoTypeInformation
    @($unknownDomainRows.ToArray() | Sort-Object admin_email, domain) | Export-Csv -Path $unknownDomainCsv -NoTypeInformation
    @($adminSummaryRows.ToArray()) | Export-Csv -Path $adminSummaryCsv -NoTypeInformation

    return [pscustomobject]@{
        microsoft_admin_count = $admins.Count
        current_assignment_count = $latestAssignmentByDomainId.Count
        threshold_admin_count = $thresholdAdminIds.Count
        threshold_current_domain_count = $thresholdDomainRows.Count
        unknown_admin_count = $unknownAdminIds.Count
        unknown_current_domain_count = $unknownDomainRows.Count
        threshold_domain_csv = $thresholdDomainCsv
        unknown_domain_csv = $unknownDomainCsv
        admin_summary_csv = $adminSummaryCsv
    }
}

$failedShards = @()
if (-not $SkipProbe) {
    $failedShards = @(Invoke-ThresholdProbeShards)
} else {
    Write-MonitorLog "Skipping probe and generating reports from current database state"
}

$probeResults = @(Read-ProbeResults)
$clean = @($probeResults | Where-Object { [string]$_.result -eq "clean" }).Count
$threshold = @($probeResults | Where-Object { [string]$_.result -eq "threshold" }).Count
$unknown = @($probeResults | Where-Object { [string]$_.result -eq "unknown" }).Count
Write-MonitorLog "Probe aggregate: total=$($probeResults.Count) clean=$clean threshold=$threshold unknown=$unknown"

$adminPatchErrors = [System.Collections.Generic.List[string]]::new()
foreach ($result in $probeResults) {
    $adminId = [string]$result.admin_id
    if (-not $adminId) { continue }
    try {
        if ([string]$result.result -eq "threshold") {
            Set-AdminStatus -AdminId $adminId -Fields @{ active = $false; status = "Threshold Exceeded" }
        } elseif ([string]$result.result -eq "clean") {
            Set-AdminStatus -AdminId $adminId -Fields @{ status = "Active" }
        }
    } catch {
        $adminPatchErrors.Add("$adminId $($_.Exception.Message)") | Out-Null
    }
}

$impact = Write-DomainImpactReports -ProbeResults $probeResults
$summary = [pscustomobject]@{
    run_id = $RunId
    started_from = $PSCommandPath
    probe_skipped = [bool]$SkipProbe
    shard_count = $ShardCount
    shard_failures = $failedShards.Count
    probe_total = $probeResults.Count
    probe_clean = $clean
    probe_threshold = $threshold
    probe_unknown = $unknown
    admin_patch_errors = $adminPatchErrors.Count
    impact = $impact
    completed_at = (Get-Date).ToUniversalTime().ToString("o")
}

$summaryPath = Join-Path $runDir "summary.json"
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8
Write-MonitorLog "Threshold monitor complete: summary=$summaryPath threshold_domains=$($impact.threshold_current_domain_count) unknown_domains=$($impact.unknown_current_domain_count)"

if ($adminPatchErrors.Count -gt 0) {
    $adminPatchErrors | Set-Content -Path (Join-Path $runDir "admin-patch-errors.txt") -Encoding UTF8
    throw "Admin patch errors encountered: $($adminPatchErrors.Count)"
}

if ($failedShards.Count -gt 0) {
    throw "Threshold monitor shard failures: $($failedShards.Count)"
}
