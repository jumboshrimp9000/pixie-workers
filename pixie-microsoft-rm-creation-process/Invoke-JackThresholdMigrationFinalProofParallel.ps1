<#
.SYNOPSIS
    Runs the Jack/ProfitPath final proof in parallel chunks.
.DESCRIPTION
    Splits the migration plan CSV into smaller manifests and launches
    Get-JackThresholdMigrationFinalProof.ps1 in multiple child pwsh processes
    using structured ArgumentList entries instead of inline command text.

    This keeps the verification read-only while reducing wall-clock time for
    large runs such as the 237-domain Jack migration.
#>

param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [int]$ExpectedInboxes = 99,
    [int]$DailyLimit = 5,
    [int]$SendingGapMinutes = 30,
    [int]$WarmupDailyLimit = 5,
    [double]$WarmupReplyRatePercent = 60,
    [string]$Tag = "Mailboxpro 5/10",
    [int]$ChunkSize = 8,
    [int]$MaxParallel = 6,
    [switch]$SkipInstantlyDeepCheck,
    [string]$OutCsv,
    [string]$OutJson
)

$ErrorActionPreference = "Stop"

function Write-Info {
    param([string]$Message)
    Write-Host $Message
}

function New-ChunkFiles {
    param(
        [object[]]$Rows,
        [int]$ChunkSizeValue,
        [string]$WorkingDirectory
    )

    $chunks = [System.Collections.Generic.List[object]]::new()
    $headers = @($Rows[0].PSObject.Properties.Name)
    $chunkIndex = 0

    for ($start = 0; $start -lt $Rows.Count; $start += $ChunkSizeValue) {
        $chunkIndex++
        $end = [Math]::Min($start + $ChunkSizeValue - 1, $Rows.Count - 1)
        $chunkRows = @($Rows[$start..$end])
        $chunkPath = Join-Path $WorkingDirectory ("chunk-{0:d3}.csv" -f $chunkIndex)
        @($chunkRows) | Export-Csv -Path $chunkPath -NoTypeInformation

        $chunks.Add([pscustomobject]@{
            Index = $chunkIndex
            PlanCsv = $chunkPath
            OutCsv = Join-Path $WorkingDirectory ("chunk-{0:d3}.proof.csv" -f $chunkIndex)
            OutJson = Join-Path $WorkingDirectory ("chunk-{0:d3}.proof.json" -f $chunkIndex)
            StdOut = Join-Path $WorkingDirectory ("chunk-{0:d3}.stdout.log" -f $chunkIndex)
            StdErr = Join-Path $WorkingDirectory ("chunk-{0:d3}.stderr.log" -f $chunkIndex)
            Domains = @($chunkRows | ForEach-Object { [string]$_.domain })
            Count = $chunkRows.Count
        }) | Out-Null
    }

    return @($chunks)
}

function Start-ChunkProcess {
    param(
        [object]$Chunk,
        [string]$ProofScriptPath
    )

    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = "pwsh"
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.CreateNoWindow = $true

    [void]$psi.ArgumentList.Add("-NoProfile")
    [void]$psi.ArgumentList.Add("-File")
    [void]$psi.ArgumentList.Add($ProofScriptPath)
    [void]$psi.ArgumentList.Add("-PlanCsv")
    [void]$psi.ArgumentList.Add($Chunk.PlanCsv)
    [void]$psi.ArgumentList.Add("-ExpectedInboxes")
    [void]$psi.ArgumentList.Add([string]$ExpectedInboxes)
    [void]$psi.ArgumentList.Add("-DailyLimit")
    [void]$psi.ArgumentList.Add([string]$DailyLimit)
    [void]$psi.ArgumentList.Add("-SendingGapMinutes")
    [void]$psi.ArgumentList.Add([string]$SendingGapMinutes)
    [void]$psi.ArgumentList.Add("-WarmupDailyLimit")
    [void]$psi.ArgumentList.Add([string]$WarmupDailyLimit)
    [void]$psi.ArgumentList.Add("-WarmupReplyRatePercent")
    [void]$psi.ArgumentList.Add([string]$WarmupReplyRatePercent)
    [void]$psi.ArgumentList.Add("-Tag")
    [void]$psi.ArgumentList.Add($Tag)
    [void]$psi.ArgumentList.Add("-OutCsv")
    [void]$psi.ArgumentList.Add($Chunk.OutCsv)
    [void]$psi.ArgumentList.Add("-OutJson")
    [void]$psi.ArgumentList.Add($Chunk.OutJson)
    if ($SkipInstantlyDeepCheck) {
        [void]$psi.ArgumentList.Add("-SkipInstantlyDeepCheck")
    }

    $process = [System.Diagnostics.Process]::Start($psi)
    return [pscustomobject]@{
        Chunk = $Chunk
        Process = $process
    }
}

if (-not (Test-Path $PlanCsv)) { throw "Plan CSV not found: $PlanCsv" }
if ($ChunkSize -lt 1) { throw "ChunkSize must be at least 1" }
if ($MaxParallel -lt 1) { throw "MaxParallel must be at least 1" }

$proofScript = Join-Path $PSScriptRoot "Get-JackThresholdMigrationFinalProof.ps1"
if (-not (Test-Path $proofScript)) { throw "Proof script not found: $proofScript" }

$planRows = @(Import-Csv -Path $PlanCsv)
if ($planRows.Count -eq 0) { throw "Plan CSV is empty: $PlanCsv" }

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$workDir = Join-Path $PSScriptRoot "logs/jack-threshold-final-proof-parallel-$stamp"
New-Item -ItemType Directory -Path $workDir -Force | Out-Null

if (-not $OutCsv) { $OutCsv = Join-Path $PSScriptRoot "logs/jack-threshold-final-proof-parallel-$stamp.csv" }
if (-not $OutJson) { $OutJson = Join-Path $PSScriptRoot "logs/jack-threshold-final-proof-parallel-$stamp.json" }

$chunks = @(New-ChunkFiles -Rows $planRows -ChunkSizeValue $ChunkSize -WorkingDirectory $workDir)
Write-Info "Prepared $($chunks.Count) proof chunk(s) for $($planRows.Count) domain(s). MaxParallel=$MaxParallel ChunkSize=$ChunkSize"

$pending = [System.Collections.Generic.List[object]]::new()
foreach ($chunk in $chunks) { $pending.Add($chunk) | Out-Null }
$active = [System.Collections.Generic.List[object]]::new()
$completed = [System.Collections.Generic.List[object]]::new()

while ($pending.Count -gt 0 -or $active.Count -gt 0) {
    while ($pending.Count -gt 0 -and $active.Count -lt $MaxParallel) {
        $chunk = $pending[0]
        $pending.RemoveAt(0)
        Write-Info ("Starting chunk {0}/{1} ({2} domain(s))" -f $chunk.Index, $chunks.Count, $chunk.Count)
        $active.Add((Start-ChunkProcess -Chunk $chunk -ProofScriptPath $proofScript)) | Out-Null
    }

    for ($index = $active.Count - 1; $index -ge 0; $index--) {
        $item = $active[$index]
        if (-not $item.Process.HasExited) { continue }

        $stdout = $item.Process.StandardOutput.ReadToEnd()
        $stderr = $item.Process.StandardError.ReadToEnd()
        Set-Content -Path $item.Chunk.StdOut -Value $stdout
        Set-Content -Path $item.Chunk.StdErr -Value $stderr

        $completed.Add([pscustomobject]@{
            Chunk = $item.Chunk
            ExitCode = $item.Process.ExitCode
            StdOut = $stdout
            StdErr = $stderr
        }) | Out-Null

        Write-Info ("Finished chunk {0}/{1} exit={2}" -f $item.Chunk.Index, $chunks.Count, $item.Process.ExitCode)
        $active.RemoveAt($index)
    }

    if ($active.Count -gt 0) {
        Start-Sleep -Seconds 2
    }
}

$failedChunks = @($completed | Where-Object { $_.ExitCode -ne 0 })
if ($failedChunks.Count -gt 0) {
    $sample = $failedChunks[0]
    throw ("Parallel proof failed in chunk {0}. stderr: {1}" -f $sample.Chunk.Index, ([string]$sample.StdErr).Trim())
}

$domainRows = [System.Collections.Generic.List[object]]::new()
$notFinal = [System.Collections.Generic.List[object]]::new()
$endpointStrategies = [System.Collections.Generic.List[string]]::new()
$summary = [ordered]@{
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    plan_csv = $PlanCsv
    plan_domains = 0
    db_domains_found = 0
    domains_on_destination_admin = 0
    domains_with_exact_active_inboxes = 0
    replacement_provision_completed = 0
    linked_reupload_completed = 0
    db_proven_domains = 0
    instantly_deep_check_skipped = [bool]$SkipInstantlyDeepCheck
    instantly_checked_domains = 0
    instantly_proven_domains = 0
    final_proven_domains = 0
    expected_inboxes_per_domain = $ExpectedInboxes
    expected_total_inboxes = 0
    observed_active_inboxes = 0
    expected_daily_limit = $DailyLimit
    expected_sending_gap_minutes = $SendingGapMinutes
    expected_warmup_daily_limit = $WarmupDailyLimit
    expected_warmup_reply_rate_percent = $WarmupReplyRatePercent
    expected_tag = $Tag
}

foreach ($item in @($completed | Sort-Object { $_.Chunk.Index })) {
    $report = Get-Content -Path $item.Chunk.OutJson -Raw | ConvertFrom-Json
    foreach ($row in @($report.domains)) {
        $domainRows.Add($row) | Out-Null
        if (-not $row.final_proven -and $notFinal.Count -lt 25) {
            $notFinal.Add([pscustomobject]@{
                domain = [string]$row.domain
                db_proven = [bool]$row.db_proven
                instantly_check_status = [string]$row.instantly_check_status
                active_inboxes = [int]$row.active_inboxes
                replacement_provision_completed = [bool]$row.replacement_provision_completed
                linked_reupload_completed = [bool]$row.linked_reupload_completed
                failure_reasons = [string]$row.failure_reasons
                instantly_failure_samples = [string]$row.instantly_failure_samples
            }) | Out-Null
        }
    }

    $chunkSummary = $report.summary
    foreach ($field in @(
        "plan_domains",
        "db_domains_found",
        "domains_on_destination_admin",
        "domains_with_exact_active_inboxes",
        "replacement_provision_completed",
        "linked_reupload_completed",
        "db_proven_domains",
        "instantly_checked_domains",
        "instantly_proven_domains",
        "final_proven_domains",
        "expected_total_inboxes",
        "observed_active_inboxes"
    )) {
        $summary[$field] += [double]$chunkSummary.$field
    }

    $strategy = [string]$report.endpoint_strategy.instantly
    if ($strategy) { $endpointStrategies.Add($strategy) | Out-Null }
}

$summary.plan_domains = [int]$summary.plan_domains
$summary.db_domains_found = [int]$summary.db_domains_found
$summary.domains_on_destination_admin = [int]$summary.domains_on_destination_admin
$summary.domains_with_exact_active_inboxes = [int]$summary.domains_with_exact_active_inboxes
$summary.replacement_provision_completed = [int]$summary.replacement_provision_completed
$summary.linked_reupload_completed = [int]$summary.linked_reupload_completed
$summary.db_proven_domains = [int]$summary.db_proven_domains
$summary.instantly_checked_domains = [int]$summary.instantly_checked_domains
$summary.instantly_proven_domains = [int]$summary.instantly_proven_domains
$summary.final_proven_domains = [int]$summary.final_proven_domains
$summary.expected_total_inboxes = [int]$summary.expected_total_inboxes
$summary.observed_active_inboxes = [int]$summary.observed_active_inboxes
$summary.not_final_samples = @($notFinal)

$report = [pscustomobject][ordered]@{
    summary = [pscustomobject]$summary
    domains = @($domainRows)
    endpoint_strategy = [pscustomobject][ordered]@{
        database = "Supabase REST: domains, domain_admin_assignments, inboxes, actions, domain_credentials; actions are scoped to payload.source=jack_threshold_tenant_migration and linked payload.provision_action_id."
        instantly = if ($SkipInstantlyDeepCheck) {
            "Skipped by -SkipInstantlyDeepCheck."
        } else {
            "Parallelized child proof runs call Instantly API v2 list-account, custom-tags, custom-tag-mappings, and direct account fallback only when list-account data is insufficient. Reply-rate values are normalized so 0.6 and 60 both mean 60 percent."
        }
        reply_rate_contract = "Warmup reply_rate is verified as a human percent value: $WarmupReplyRatePercent means $WarmupReplyRatePercent percent, regardless of whether Instantly returns 0.6 or 60."
    }
    chunk_run = [pscustomobject]@{
        work_dir = $workDir
        chunk_size = $ChunkSize
        max_parallel = $MaxParallel
        chunk_count = $chunks.Count
    }
}

@($domainRows) | Export-Csv -Path $OutCsv -NoTypeInformation
$report | ConvertTo-Json -Depth 12 | Set-Content -Path $OutJson

Write-Host ""
Write-Host "Wrote CSV:  $OutCsv"
Write-Host "Wrote JSON: $OutJson"
Write-Host "Parallel proof complete: $($summary.final_proven_domains)/$($summary.plan_domains) final_proven"
