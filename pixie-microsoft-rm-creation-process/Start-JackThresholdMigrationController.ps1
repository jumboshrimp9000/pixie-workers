<#
.SYNOPSIS
    Closed-loop controller for the Jack/ProfitPath threshold-tenant migration.
.DESCRIPTION
    Watches the migration proof report, launches only safe migration waves, and
    records each decision as JSONL so the run can be audited and tuned without
    guessing. This controller does not perform mailbox work itself; it keeps the
    existing cancellation, provision, admin-lock, DKIM, and upload workers fed.
#>

param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [string]$ValidationCsv = (Join-Path $PSScriptRoot "logs/microsoft-admin-destination-validation-237.csv"),
    [int]$ExpectedInboxes = 99,
    [int]$BaseWaveLimit = 10,
    [int]$BaseWaveParallel = 5,
    [int]$MaxProvisionOpen = 36,
    [int]$MaxUploadOpen = 12,
    [int]$LoopSeconds = 120,
    [int]$MaxLoops = 0,
    [int]$FinalProofEveryLoops = 5,
    [int]$MaxAutoWaveLimit = 40,
    [switch]$AggressiveBackfill,
    [switch]$DisableTagBlockedRepair,
    [switch]$DisableInstantlyConflictRepair,
    [switch]$Live,
    [string]$ConfirmText = "",
    [string]$LogDir = (Join-Path $PSScriptRoot "logs/jack-threshold-controller")
)

$ErrorActionPreference = "Stop"

$expectedConfirm = "RUN JACK THRESHOLD MIGRATION CONTROLLER"
if ($Live -and $ConfirmText -ne $expectedConfirm) {
    throw "Live controller requires ConfirmText exactly: $expectedConfirm"
}

if (-not (Test-Path $PlanCsv)) { throw "Plan CSV not found: $PlanCsv" }
if (-not (Test-Path $ValidationCsv)) { throw "Validation CSV not found: $ValidationCsv" }
if ($BaseWaveLimit -lt 1) { throw "BaseWaveLimit must be at least 1" }
if ($BaseWaveParallel -lt 1) { throw "BaseWaveParallel must be at least 1" }
if ($MaxProvisionOpen -lt 1) { throw "MaxProvisionOpen must be at least 1" }
if ($MaxUploadOpen -lt 1) { throw "MaxUploadOpen must be at least 1" }
if ($LoopSeconds -lt 15) { throw "LoopSeconds must be at least 15" }
if ($FinalProofEveryLoops -lt 0) { throw "FinalProofEveryLoops cannot be negative" }
if ($MaxAutoWaveLimit -lt 1) { throw "MaxAutoWaveLimit must be at least 1" }

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
$runStamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$decisionLog = Join-Path $LogDir "decisions-$runStamp.jsonl"
$learningLog = Join-Path $LogDir "learning-$runStamp.jsonl"
$statusScript = Join-Path $PSScriptRoot "Get-JackThresholdMigrationStatus.ps1"
$finalProofScript = Join-Path $PSScriptRoot "Get-JackThresholdMigrationFinalProof.ps1"
$failedActionsScript = Join-Path $PSScriptRoot "Get-JackFailedActions.ps1"
$batchScript = Join-Path $PSScriptRoot "invoke-jack-threshold-domain-move-batch.ps1"
$tagBlockedRepairScript = Join-Path $PSScriptRoot "Reset-JackTagBlockedUploads.ps1"
$instantlyConflictRepairScript = Join-Path $PSScriptRoot "Repair-JackInstantlyCrossWorkspaceConflicts.ps1"
$movedProvisionGapRepairScript = Join-Path $PSScriptRoot "Repair-JackMovedProvisionGaps.ps1"
$credentialSyncRepairScript = Join-Path $PSScriptRoot "Reset-JackCredentialSyncFailedProvision.ps1"
$dkimPendingRepairScript = Join-Path $PSScriptRoot "Reset-JackDkimPendingActions.ps1"
$prematureProvisionRepairScript = Join-Path $PSScriptRoot "Archive-JackPrematureProvisionActions.ps1"
$completedStatusRepairScript = Join-Path $PSScriptRoot "Repair-JackCompletedStatusMismatches.ps1"
$exchangeSyncRepairScript = Join-Path $PSScriptRoot "Reset-JackExchangeSyncPendingProvision.ps1"

function Convert-StatusCounts {
    param([object[]]$Counts)
    $map = @{}
    foreach ($row in @($Counts)) {
        $map[[string]$row.status] = [int]$row.count
    }
    return $map
}

function Get-ControllerStatus {
    $raw = & $statusScript -PlanCsv $PlanCsv -ExpectedInboxes $ExpectedInboxes -Json
    if (-not $raw) { throw "Status report returned no output" }
    return ($raw | ConvertFrom-Json)
}

function Get-ActiveBatchLauncherCount {
    $lines = @(& /bin/ps -axo pid=,command=)
    $selfPid = [string]$PID
    $count = 0
    foreach ($line in $lines) {
        $trimmed = ([string]$line).Trim()
        if (-not $trimmed) { continue }
        if ($trimmed -notmatch "invoke-jack-threshold-domain-move-batch\.ps1") { continue }
        if ($trimmed -notmatch "\s-Live(\s|$)") { continue }
        if ($trimmed.StartsWith($selfPid + " ")) { continue }
        $count += 1
    }
    return $count
}

function Write-Decision {
    param([hashtable]$Decision)
    $Decision.generated_at_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $Decision | ConvertTo-Json -Depth 8 -Compress | Add-Content -Path $decisionLog
}

function Write-Learning {
    param([hashtable]$Learning)
    $Learning.generated_at_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $Learning | ConvertTo-Json -Depth 10 -Compress | Add-Content -Path $learningLog
}

function Get-BlockerClass {
    param([string]$ErrorText)
    if ($ErrorText -match "exists in another workspace|another workspace") { return "instantly_cross_workspace_account_conflict" }
    if ($ErrorText -match "tag mapping was not visible") { return "instantly_tag_mapping_lag" }
    if ($ErrorText -match "Sending-tool upload validation failed") { return "upload_validation_failed" }
    if ($ErrorText -match "Wrong password|invalid password|password") { return "credential_password_sync" }
    if ($ErrorText -match "DKIM|CNAME") { return "dkim_wait_or_failure" }
    if ($ErrorText -match "accepted domain|not an accepted domain|DefaultAcceptedDomain") { return "accepted_domain_not_ready" }
    if ($ErrorText -match "threshold|5\.7\.705|5\.7\.708") { return "source_tenant_threshold" }
    if ($ErrorText -match "timeout|timed out") { return "provider_timeout" }
    if ($ErrorText -match "401|Unauthorized|token") { return "auth_or_token_refresh" }
    if ($ErrorText) { return "unclassified_error" }
    return "unknown"
}

function Get-CurrentFailedActions {
    if (-not (Test-Path $failedActionsScript)) { return @() }
    $output = @(& pwsh -NoProfile -File $failedActionsScript -PlanCsv $PlanCsv -Json 2>&1)
    $text = ($output | ForEach-Object { [string]$_ }) -join "`n"
    if (-not $text.Trim()) { return @() }
    try {
        $parsed = $text | ConvertFrom-Json
        if ($null -eq $parsed) { return @() }
        return @($parsed)
    } catch {
        Write-Learning @{
            event = "failed_action_read_failed"
            error = [string]$_.Exception.Message
            raw = $text
        }
        return @()
    }
}

function Invoke-DbFinalProof {
    if (-not (Test-Path $finalProofScript)) { return $null }
    $output = @(& pwsh -NoProfile -File $finalProofScript -PlanCsv $PlanCsv -ExpectedInboxes $ExpectedInboxes -SkipInstantlyDeepCheck -Json 2>&1)
    $text = ($output | ForEach-Object { [string]$_ }) -join "`n"
    if (-not $text.Trim()) { return $null }
    try {
        $report = $text | ConvertFrom-Json
        return $report.summary
    } catch {
        Write-Learning @{
            event = "db_final_proof_read_failed"
            error = [string]$_.Exception.Message
            raw_sample = if ($text.Length -gt 1500) { $text.Substring(0, 1500) } else { $text }
        }
        return $null
    }
}

function Invoke-DeepFinalProof {
    if (-not (Test-Path $finalProofScript)) { return $null }
    $output = @(& pwsh -NoProfile -File $finalProofScript -PlanCsv $PlanCsv -ExpectedInboxes $ExpectedInboxes -Json 2>&1)
    $text = ($output | ForEach-Object { [string]$_ }) -join "`n"
    if (-not $text.Trim()) { return $null }
    try {
        $report = $text | ConvertFrom-Json
        return $report.summary
    } catch {
        Write-Learning @{
            event = "deep_final_proof_read_failed"
            error = [string]$_.Exception.Message
            raw_sample = if ($text.Length -gt 1500) { $text.Substring(0, 1500) } else { $text }
        }
        return $null
    }
}

function Write-CurrentBlockerLearnings {
    param([object[]]$Rows)
    if (-not $Rows -or $Rows.Count -eq 0) { return }
    $classes = @{}
    foreach ($row in @($Rows)) {
        $class = Get-BlockerClass -ErrorText ([string]$row.error)
        if (-not $classes.ContainsKey($class)) {
            $classes[$class] = [System.Collections.Generic.List[object]]::new()
        }
        $classes[$class].Add($row) | Out-Null
    }
    foreach ($className in $classes.Keys) {
        $items = @($classes[$className])
        $samples = @($items | Select-Object -First 5 domain,type,id,attempts,updated_at,error)
        Write-Learning @{
            event = "current_failed_action_class"
            class = $className
            count = $items.Count
            samples = $samples
        }
    }
}

function Invoke-TagBlockedRepair {
    if ($DisableTagBlockedRepair) { return }
    if (-not (Test-Path $tagBlockedRepairScript)) { return }

    $repairArgs = @(
        "-NoProfile",
        "-File", $tagBlockedRepairScript,
        "-PlanCsv", $PlanCsv
    )
    if ($Live) { $repairArgs += "-Live" }

    try {
        $output = @(& pwsh @repairArgs 2>&1)
        $text = ($output | ForEach-Object { [string]$_ }) -join "`n"
        $matchedLine = @($output | Where-Object { [string]$_ -match "^Matched " } | Select-Object -Last 1)
        if ($matchedLine.Count -gt 0 -and [string]$matchedLine[-1] -notmatch "Matched 0 replacement upload action\(s\) and 0 provision finalization action\(s\)") {
            Write-Host ([string]$matchedLine[-1])
            Write-Decision @{
                action = "repair_tag_blocked_actions"
                reason = "Known Instantly tag mapping visibility failure was requeued"
                live = [bool]$Live
                output = $text
            }
        }
    } catch {
        $message = [string]$_.Exception.Message
        Write-Warning "Tag-blocked repair check failed: $message"
        Write-Decision @{
            action = "repair_check_failed"
            reason = $message
            live = [bool]$Live
        }
    }
}

function Invoke-InstantlyConflictRepair {
    if ($DisableInstantlyConflictRepair) { return }
    if (-not (Test-Path $instantlyConflictRepairScript)) { return }

    $repairArgs = @(
        "-NoProfile",
        "-File", $instantlyConflictRepairScript,
        "-PlanCsv", $PlanCsv,
        "-ExpectedInboxes", ([string]$ExpectedInboxes)
    )
    if ($Live) { $repairArgs += "-Live" }

    try {
        $output = @(& pwsh @repairArgs 2>&1)
        $text = ($output | ForEach-Object { [string]$_ }) -join "`n"
        $matchedLine = @($output | Where-Object { [string]$_ -match "^Matched " } | Select-Object -Last 1)
        if ($matchedLine.Count -gt 0 -and [string]$matchedLine[-1] -notmatch "Matched 0 cross-workspace upload blocker\(s\)") {
            Write-Host ([string]$matchedLine[-1])
            Write-Decision @{
                action = "repair_instantly_cross_workspace_conflict"
                reason = "Queued/recycled mailbox-level repair for Instantly account already in another workspace"
                live = [bool]$Live
                output = $text
            }
        }
    } catch {
        $message = [string]$_.Exception.Message
        Write-Warning "Instantly cross-workspace repair check failed: $message"
        Write-Decision @{
            action = "instantly_cross_workspace_repair_failed"
            reason = $message
            live = [bool]$Live
        }
    }
}

function Invoke-MovedProvisionGapRepair {
    if (-not (Test-Path $movedProvisionGapRepairScript)) { return }

    $repairArgs = @(
        "-NoProfile",
        "-File", $movedProvisionGapRepairScript,
        "-PlanCsv", $PlanCsv,
        "-ExpectedInboxes", ([string]$ExpectedInboxes)
    )
    if ($Live) {
        $repairArgs += @("-Live", "-ConfirmText", "REPAIR JACK MOVED PROVISION GAPS")
    }

    try {
        $output = @(& pwsh @repairArgs 2>&1)
        $text = ($output | ForEach-Object { [string]$_ }) -join "`n"
        $matchedLine = @($output | Where-Object { [string]$_ -match "^Matched " } | Select-Object -Last 1)
        if ($matchedLine.Count -gt 0 -and [string]$matchedLine[-1] -notmatch "Matched 0 moved domain\(s\)") {
            Write-Host ([string]$matchedLine[-1])
            Write-Decision @{
                action = "repair_moved_provision_gap"
                reason = "Moved domain had no replacement provision action queued"
                live = [bool]$Live
                output = $text
            }
        }
    } catch {
        $message = [string]$_.Exception.Message
        Write-Warning "Moved provision-gap repair check failed: $message"
        Write-Decision @{
            action = "moved_provision_gap_repair_failed"
            reason = $message
            live = [bool]$Live
        }
    }
}

function Invoke-CredentialSyncRepair {
    if (-not (Test-Path $credentialSyncRepairScript)) { return }

    $repairArgs = @(
        "-NoProfile",
        "-File", $credentialSyncRepairScript,
        "-PlanCsv", $PlanCsv,
        "-ExpectedInboxes", ([string]$ExpectedInboxes)
    )
    if ($Live) { $repairArgs += "-Live" }

    try {
        $output = @(& pwsh @repairArgs 2>&1)
        $text = ($output | ForEach-Object { [string]$_ }) -join "`n"
        $matchedLine = @($output | Where-Object { [string]$_ -match "^Matched " } | Select-Object -Last 1)
        if ($matchedLine.Count -gt 0 -and [string]$matchedLine[-1] -notmatch "Matched 0 Jack credential-sync failed provision action\(s\)") {
            Write-Host ([string]$matchedLine[-1])
            Write-Decision @{
                action = "repair_credential_sync_failed_provision"
                reason = "Credential/bulletproof sync failure was requeued to rerun mailbox readiness checks"
                live = [bool]$Live
                output = $text
            }
        }
    } catch {
        $message = [string]$_.Exception.Message
        Write-Warning "Credential-sync repair check failed: $message"
        Write-Decision @{
            action = "credential_sync_repair_failed"
            reason = $message
            live = [bool]$Live
        }
    }
}

function Invoke-DkimPendingRepair {
    if (-not (Test-Path $dkimPendingRepairScript)) { return }

    $repairArgs = @(
        "-NoProfile",
        "-File", $dkimPendingRepairScript,
        "-PlanCsv", $PlanCsv
    )
    if ($Live) { $repairArgs += "-Live" }

    try {
        $output = @(& pwsh @repairArgs 2>&1)
        $text = ($output | ForEach-Object { [string]$_ }) -join "`n"
        $matchedLine = @($output | Where-Object { [string]$_ -match "^Matched " } | Select-Object -Last 1)
        if ($matchedLine.Count -gt 0 -and [string]$matchedLine[-1] -notmatch "Matched 0 Jack DKIM-pending provision action\(s\)") {
            Write-Host ([string]$matchedLine[-1])
            Write-Decision @{
                action = "repair_dkim_pending_provision"
                reason = "DKIM CNAMEs are present but Microsoft had not accepted them; requeued for tenant-side enable retry"
                live = [bool]$Live
                output = $text
            }
        }
    } catch {
        $message = [string]$_.Exception.Message
        Write-Warning "DKIM-pending repair check failed: $message"
        Write-Decision @{
            action = "dkim_pending_repair_failed"
            reason = $message
            live = [bool]$Live
        }
    }
}

function Invoke-PrematureProvisionRepair {
    if (-not (Test-Path $prematureProvisionRepairScript)) { return }

    $repairArgs = @(
        "-NoProfile",
        "-File", $prematureProvisionRepairScript,
        "-PlanCsv", $PlanCsv
    )
    if ($Live) { $repairArgs += "-Live" }

    try {
        $output = @(& pwsh @repairArgs 2>&1)
        $text = ($output | ForEach-Object { [string]$_ }) -join "`n"
        $matchedLine = @($output | Where-Object { [string]$_ -match "^Matched " } | Select-Object -Last 1)
        if ($matchedLine.Count -gt 0 -and [string]$matchedLine[-1] -notmatch "Matched 0 premature Jack provision action\(s\)") {
            Write-Host ([string]$matchedLine[-1])
            Write-Decision @{
                action = "repair_premature_threshold_provision"
                reason = "Cancelled replacement provision action that was still pointed at a thresholded tenant so move-first flow can run"
                live = [bool]$Live
                output = $text
            }
        }
    } catch {
        $message = [string]$_.Exception.Message
        Write-Warning "Premature-provision repair check failed: $message"
        Write-Decision @{
            action = "premature_provision_repair_failed"
            reason = $message
            live = [bool]$Live
        }
    }
}

function Invoke-CompletedStatusRepair {
    if (-not (Test-Path $completedStatusRepairScript)) { return }

    $repairArgs = @(
        "-NoProfile",
        "-File", $completedStatusRepairScript,
        "-PlanCsv", $PlanCsv,
        "-ExpectedInboxes", ([string]$ExpectedInboxes)
    )
    if ($Live) { $repairArgs += "-Live" }

    try {
        $output = @(& pwsh @repairArgs 2>&1)
        $text = ($output | ForEach-Object { [string]$_ }) -join "`n"
        $matchedLine = @($output | Where-Object { [string]$_ -match "^Matched " } | Select-Object -Last 1)
        if ($matchedLine.Count -gt 0 -and [string]$matchedLine[-1] -notmatch "Matched 0 completed Jack status mismatch domain\(s\)") {
            Write-Host ([string]$matchedLine[-1])
            Write-Decision @{
                action = "repair_completed_status_mismatch"
                reason = "Domain row was stale after linked replacement provision and upload already completed"
                live = [bool]$Live
                output = $text
            }
        }
    } catch {
        $message = [string]$_.Exception.Message
        Write-Warning "Completed-status repair check failed: $message"
        Write-Decision @{
            action = "completed_status_repair_failed"
            reason = $message
            live = [bool]$Live
        }
    }
}

function Invoke-ExchangeSyncRepair {
    if (-not (Test-Path $exchangeSyncRepairScript)) { return }

    $repairArgs = @(
        "-NoProfile",
        "-File", $exchangeSyncRepairScript,
        "-PlanCsv", $PlanCsv,
        "-ExpectedInboxes", ([string]$ExpectedInboxes)
    )
    if ($Live) { $repairArgs += "-Live" }

    try {
        $output = @(& pwsh @repairArgs 2>&1)
        $text = ($output | ForEach-Object { [string]$_ }) -join "`n"
        $matchedLine = @($output | Where-Object { [string]$_ -match "^Matched " } | Select-Object -Last 1)
        if ($matchedLine.Count -gt 0 -and [string]$matchedLine[-1] -notmatch "Matched 0 Jack Exchange-sync pending provision action\(s\)") {
            Write-Host ([string]$matchedLine[-1])
            Write-Decision @{
                action = "repair_exchange_sync_pending_provision"
                reason = "Accepted-domain sync exceeded retry budget before mailbox creation"
                live = [bool]$Live
                output = $text
            }
        }
    } catch {
        $message = [string]$_.Exception.Message
        Write-Warning "Exchange-sync repair check failed: $message"
        Write-Decision @{
            action = "exchange_sync_repair_failed"
            reason = $message
            live = [bool]$Live
        }
    }
}

function New-Decision {
    param(
        [object]$Status,
        [string]$Action,
        [string]$Reason,
        [int]$WaveLimit = 0,
        [int]$WaveParallel = 0,
        [int]$ProvisionOpen = 0,
        [int]$UploadOpen = 0,
        [int]$LauncherCount = 0
    )

    return @{
        action = $Action
        reason = $Reason
        wave_limit = $WaveLimit
        wave_parallel = $WaveParallel
        launchers_running = $LauncherCount
        provision_open = $ProvisionOpen
        upload_open = $UploadOpen
        fully_proven_domains = [int]$Status.fully_proven_domains
        moved_to_destination = [int]$Status.moved_to_destination
        active_99_moved = [int]$Status.active_99_moved
        not_yet_moved = [int]$Status.not_yet_moved
        completed_upload_domains = [int]$Status.completed_upload_domains
        total_active_inboxes = [int]$Status.total_active_inboxes
        expected_total_inboxes = [int]$Status.expected_total_inboxes
    }
}

Write-Host "Jack threshold migration controller started. Decisions: $decisionLog"
Write-Host "Learning log: $learningLog"
Write-Host "Policy: wave<=${BaseWaveLimit}, auto_wave<=${MaxAutoWaveLimit}, parallel<=${BaseWaveParallel}, provision_open<${MaxProvisionOpen}, upload_open<${MaxUploadOpen}, aggressive=$([bool]$AggressiveBackfill)"

$loop = 0
$lastFullyProven = -1
$lastMoved = -1

while ($true) {
    $loop += 1
    Invoke-InstantlyConflictRepair
    Invoke-TagBlockedRepair
    Invoke-MovedProvisionGapRepair
    Invoke-PrematureProvisionRepair
    Invoke-DkimPendingRepair
    Invoke-CredentialSyncRepair
    Invoke-ExchangeSyncRepair
    Invoke-CompletedStatusRepair
    $status = Get-ControllerStatus
    $provisionCounts = Convert-StatusCounts @($status.provision_action_counts)
    $uploadCounts = Convert-StatusCounts @($status.upload_action_counts)
    $provisionOpen = [int]($provisionCounts["pending"] + $provisionCounts["in_progress"])
    $uploadOpen = [int]($uploadCounts["pending"] + $uploadCounts["in_progress"])
    $launcherCount = Get-ActiveBatchLauncherCount

    $deltaFully = if ($lastFullyProven -ge 0) { [int]$status.fully_proven_domains - $lastFullyProven } else { 0 }
    $deltaMoved = if ($lastMoved -ge 0) { [int]$status.moved_to_destination - $lastMoved } else { 0 }
    $lastFullyProven = [int]$status.fully_proven_domains
    $lastMoved = [int]$status.moved_to_destination
    $failedActions = @(Get-CurrentFailedActions)
    if ($failedActions.Count -gt 0) {
        Write-CurrentBlockerLearnings -Rows $failedActions
        $failedClasses = @(
            $failedActions |
                ForEach-Object { Get-BlockerClass -ErrorText ([string]$_.error) } |
                Group-Object |
                Sort-Object Count -Descending |
                ForEach-Object { "$($_.Name):$($_.Count)" }
        ) -join ", "
        Write-Host "Current blocker classes: $failedClasses"
        Write-Decision @{
            action = "classified_failed_actions"
            reason = "Current replacement-linked failed actions require attention or known repair"
            classes = $failedClasses
            failed_action_count = $failedActions.Count
            live = [bool]$Live
        }
    }

    if ($FinalProofEveryLoops -gt 0 -and (($loop -eq 1) -or ($loop % $FinalProofEveryLoops -eq 0))) {
        $proofSummary = Invoke-DbFinalProof
        if ($null -ne $proofSummary) {
            Write-Host ("DB proof: moved={0}/{1} exactRows={2} provisionDone={3} uploadDone={4} dbProven={5}" -f `
                $proofSummary.domains_on_destination_admin,
                $proofSummary.plan_domains,
                $proofSummary.domains_with_exact_active_inboxes,
                $proofSummary.replacement_provision_completed,
                $proofSummary.linked_reupload_completed,
                $proofSummary.db_proven_domains)
            Write-Learning @{
                event = "db_final_proof_summary"
                domains_on_destination_admin = [int]$proofSummary.domains_on_destination_admin
                domains_with_exact_active_inboxes = [int]$proofSummary.domains_with_exact_active_inboxes
                replacement_provision_completed = [int]$proofSummary.replacement_provision_completed
                linked_reupload_completed = [int]$proofSummary.linked_reupload_completed
                db_proven_domains = [int]$proofSummary.db_proven_domains
                observed_active_inboxes = [int]$proofSummary.observed_active_inboxes
                expected_total_inboxes = [int]$proofSummary.expected_total_inboxes
            }
        }
    }

    Write-Host ("[{0}] proven={1}/{2} moved={3} notMoved={4} provisionOpen={5} uploadOpen={6} deltaProven={7} deltaMoved={8}" -f `
        (Get-Date).ToUniversalTime().ToString("HH:mm:ssZ"),
        $status.fully_proven_domains,
        $status.plan_domains,
        $status.moved_to_destination,
        $status.not_yet_moved,
        $provisionOpen,
        $uploadOpen,
        $deltaFully,
        $deltaMoved)

    if ([int]$status.fully_proven_domains -ge [int]$status.plan_domains) {
        Write-Host "DB proof is complete; running provider-side Instantly final proof before stopping."
        $deepProofSummary = Invoke-DeepFinalProof
        if ($null -ne $deepProofSummary -and [int]$deepProofSummary.final_proven_domains -ge [int]$deepProofSummary.plan_domains) {
            $decision = New-Decision -Status $status -Action "complete" -Reason "All plan domains passed DB and Instantly provider proof" -ProvisionOpen $provisionOpen -UploadOpen $uploadOpen -LauncherCount $launcherCount
            Write-Decision $decision
            Write-Host "All domains are fully proven, including Instantly provider proof. Controller complete."
            break
        }

        Write-Decision @{
            action = "hold"
            reason = "DB proof complete but Instantly provider proof is not complete"
            provider_final_proven = if ($deepProofSummary) { [int]$deepProofSummary.final_proven_domains } else { 0 }
            provider_plan_domains = if ($deepProofSummary) { [int]$deepProofSummary.plan_domains } else { [int]$status.plan_domains }
            live = [bool]$Live
        }
        Write-Host ("Provider proof is not complete yet: final={0}/{1}. Holding." -f `
            $(if ($deepProofSummary) { [int]$deepProofSummary.final_proven_domains } else { 0 }),
            $(if ($deepProofSummary) { [int]$deepProofSummary.plan_domains } else { [int]$status.plan_domains }))
    }

    if ($launcherCount -gt 0) {
        $decision = New-Decision -Status $status -Action "hold" -Reason "A live batch launcher is already running" -ProvisionOpen $provisionOpen -UploadOpen $uploadOpen -LauncherCount $launcherCount
        Write-Decision $decision
    } elseif ($uploadOpen -ge $MaxUploadOpen) {
        $decision = New-Decision -Status $status -Action "hold" -Reason "Upload backlog is at or above the safety ceiling" -ProvisionOpen $provisionOpen -UploadOpen $uploadOpen -LauncherCount $launcherCount
        Write-Decision $decision
    } elseif ($provisionOpen -ge $MaxProvisionOpen) {
        $decision = New-Decision -Status $status -Action "hold" -Reason "Provision backlog is at or above the safety ceiling" -ProvisionOpen $provisionOpen -UploadOpen $uploadOpen -LauncherCount $launcherCount
        Write-Decision $decision
    } elseif ([int]$status.not_yet_moved -le 0) {
        $decision = New-Decision -Status $status -Action "hold" -Reason "All domains are assigned to destination admins; waiting for provisioning/upload proof" -ProvisionOpen $provisionOpen -UploadOpen $uploadOpen -LauncherCount $launcherCount
        Write-Decision $decision
    } else {
        $provisionSlots = [Math]::Max(1, $MaxProvisionOpen - $provisionOpen)
        $uploadSlots = [Math]::Max(1, $MaxUploadOpen - $uploadOpen)
        $waveCeiling = if ($AggressiveBackfill) {
            [Math]::Max($BaseWaveLimit, [Math]::Min($MaxAutoWaveLimit, $provisionSlots))
        } else {
            $BaseWaveLimit
        }
        $waveLimit = [Math]::Min($waveCeiling, [Math]::Min($provisionSlots, [int]$status.not_yet_moved))
        $uploadPressureThreshold = [Math]::Max(1, [int][Math]::Floor($MaxUploadOpen * 0.75))
        if ($uploadOpen -ge $uploadPressureThreshold) {
            $waveLimit = [Math]::Min($waveLimit, [Math]::Max(1, $uploadSlots * 2))
        }
        $waveParallel = [Math]::Min($BaseWaveParallel, $waveLimit)

        $decision = New-Decision -Status $status -Action "launch_wave" -Reason "Capacity available and unmoved threshold domains remain" -WaveLimit $waveLimit -WaveParallel $waveParallel -ProvisionOpen $provisionOpen -UploadOpen $uploadOpen -LauncherCount $launcherCount
        Write-Decision $decision

        $confirm = "MOVE JACK THRESHOLD DOMAINS"
        $args = @(
            "-NoProfile",
            "-File", $batchScript,
            "-PlanCsv", $PlanCsv,
            "-ValidationCsv", $ValidationCsv,
            "-Limit", ([string]$waveLimit),
            "-MaxParallel", ([string]$waveParallel),
            "-ExpectedInboxes", ([string]$ExpectedInboxes),
            "-EnqueueProvisionOnly"
        )
        if ($Live) {
            $args += @("-Live", "-ConfirmText", $confirm)
        }

        Write-Host "Launching wave: limit=$waveLimit parallel=$waveParallel live=$([bool]$Live)"
        & pwsh @args
    }

    if ($MaxLoops -gt 0 -and $loop -ge $MaxLoops) {
        Write-Host "MaxLoops reached; controller exiting."
        break
    }

    Start-Sleep -Seconds $LoopSeconds
}
