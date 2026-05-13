<#
.SYNOPSIS
    Orchestrator: Polls Supabase for Microsoft provisioning, inbox-mutation, and cancellation actions.
.DESCRIPTION
    1. Polls Supabase actions table for pending Microsoft actions
    2. For `provision_inbox`, runs Part 1 (TypeScript - domain setup) then Part 2 (PowerShell - Exchange)
    3. For `microsoft_update_inboxes`, runs the tracked room-mailbox mutation pipeline
    4. For `microsoft_cancel_domain`, runs full Microsoft domain teardown + cancellation finalization
    5. Supports resume via domain.interim_status and action.result step checkpoints
.PARAMETER DryRun
    Run without making actual changes
.PARAMETER MaxDomains
    Limit number of domains to process (0 = all)
.PARAMETER Once
    Process one batch then exit (don't loop)
.EXAMPLE
    pwsh ./run.ps1
    pwsh ./run.ps1 -DryRun
    pwsh ./run.ps1 -MaxDomains 5 -Once
#>

param(
    [switch]$DryRun,
    [int]$MaxDomains = 0,
    [switch]$Once,
    [int]$PollIntervalSeconds = 30
)

# Load shared config
. (Join-Path $PSScriptRoot "config.ps1")

Write-Host ""
Write-Host "=========================================================================" -ForegroundColor Cyan
Write-Host "  PIXIE MICROSOFT ROOM MAILBOX PROVISIONING ORCHESTRATOR" -ForegroundColor Cyan
Write-Host "=========================================================================" -ForegroundColor Cyan
if ($DryRun) { Write-Host "  *** DRY RUN MODE ***" -ForegroundColor Yellow }
Write-Host "  Supabase URL: $($SupabaseConfig.Url)" -ForegroundColor Gray
Write-Host "  Poll interval: ${PollIntervalSeconds}s" -ForegroundColor Gray
Write-Host ""

# Verify Node.js is available for Part 1
$nodeVersion = $null
try { $nodeVersion = node --version 2>$null } catch { }
if (-not $nodeVersion) {
    Write-Log "Node.js not found - Part 1 (TypeScript) will be skipped" -Level Warning
    Write-Log "Install Node.js 20+ to enable domain setup automation" -Level Warning
}

# Install npm deps for Part 1 if needed
$nodeModules = Join-Path $PSScriptRoot "node_modules"
if ($nodeVersion -and -not (Test-Path $nodeModules)) {
    Write-Log "Installing Node.js dependencies..." -Level Info
    Push-Location $PSScriptRoot
    npm install --production 2>&1 | Out-Null
    Pop-Location
}

function Reset-SmtpPlusUnknownProviderFailures {
    if ($DryRun) {
        Write-Log "Dry run: skipping SMTP+ unknown-provider self-heal" -Level Info
        return
    }

    $encodedError = [uri]::EscapeDataString("Unknown provider: smtp_plus")
    $query = "type=eq.provision_inbox&status=eq.failed&error=eq.$encodedError&order=updated_at.desc&limit=100&select=id,domain_id,customer_id,type,status,error,attempts,max_attempts"
    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query $query
    if (-not $result.Success) {
        Write-Log "SMTP+ unknown-provider self-heal could not load failed actions" -Level Warning
        return
    }

    $actions = @($result.Data)
    if ($actions.Count -eq 0) { return }

    $resetCount = 0
    foreach ($failedAction in $actions) {
        if (-not $failedAction.domain_id) { continue }
        $domain = Get-Domain -DomainId ([string]$failedAction.domain_id)
        if (-not $domain) { continue }

        $provider = ([string]$domain.provider).Trim().ToLowerInvariant()
        if ($provider -ne "smtp_plus") { continue }

        $now = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        $update = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$($failedAction.id)&status=eq.failed&error=eq.$encodedError" -Body @{
            status = "pending"
            error = $null
            attempts = 0
            next_retry_at = $null
            started_at = $null
            completed_at = $null
            updated_at = $now
        }

        if ($update.Success -and @($update.Data).Count -gt 0) {
            $resetCount += 1
            Add-ActionLog -ActionId ([string]$failedAction.id) -DomainId ([string]$failedAction.domain_id) -CustomerId ([string]$failedAction.customer_id) -EventType "smtp_plus_provider_self_heal_queued" -Severity "info" -Message "Re-queued SMTP+ provisioning after worker routing fix" -Metadata @{
                previous_error = [string]$failedAction.error
                reset_at = $now
            }
        }
    }

    if ($resetCount -gt 0) {
        Write-Log "Re-queued $resetCount SMTP+ provisioning action(s) that failed with Unknown provider: smtp_plus" -Level Success
    }
}

function Get-RecoveryPoolIdFromActionPayload {
    param([object]$Payload)

    if ($Payload -is [string]) {
        try { $Payload = $Payload | ConvertFrom-Json -Depth 20 } catch { $Payload = $null }
    }
    if (-not $Payload) { return "" }
    return [string]$Payload.recovery_pool_id
}

function Reset-RecoveryInstantlyTimeoutFailures {
    if ($DryRun) {
        Write-Log "Dry run: skipping Recovery Instantly timeout self-heal" -Level Info
        return
    }

    $query = "type=eq.microsoft_recovery_move&status=eq.failed&order=updated_at.desc&limit=100&select=id,domain_id,customer_id,type,status,error,attempts,max_attempts,payload"
    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query $query
    if (-not $result.Success) {
        Write-Log "Recovery Instantly timeout self-heal could not load failed actions" -Level Warning
        return
    }

    $actions = @($result.Data)
    if ($actions.Count -eq 0) { return }

    $resetCount = 0
    foreach ($failedAction in $actions) {
        $errorText = [string]$failedAction.error
        if (-not $errorText) { continue }
        if ($errorText -notmatch "Instantly") { continue }
        if ($errorText -notmatch "504|Gateway Time-out|Gateway Timeout|timed out|operation has timed out|request.*aborted|request.*canceled|account did not appear") { continue }

        $recoveryPoolId = Get-RecoveryPoolIdFromActionPayload -Payload $failedAction.payload
        $now = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        $update = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$($failedAction.id)&status=eq.failed" -Body @{
            status = "pending"
            error = $null
            attempts = 0
            next_retry_at = $null
            started_at = $null
            completed_at = $null
            updated_at = $now
        }

        if ($update.Success -and @($update.Data).Count -gt 0) {
            $resetCount += 1
            if ($recoveryPoolId) {
                Invoke-SupabaseApi -Method PATCH -Table "recovery_pool" -Query "id=eq.$recoveryPoolId&recovery_status=eq.failed" -Body @{
                    recovery_status = "queued_for_recovery"
                    last_error = $null
                    updated_at = $now
                } | Out-Null
            }
            $domainIdForLog = if ($failedAction.domain_id) { [string]$failedAction.domain_id } else { $null }
            $customerIdForLog = if ($failedAction.customer_id) { [string]$failedAction.customer_id } else { $null }
            Add-ActionLog -ActionId ([string]$failedAction.id) -DomainId $domainIdForLog -CustomerId $customerIdForLog -EventType "recovery_instantly_timeout_self_heal_queued" -Severity "info" -Message "Re-queued Recovery Pool move after Instantly timeout handling fix" -Metadata @{
                recovery_pool_id = $recoveryPoolId
                previous_error = $errorText.Substring(0, [Math]::Min(1000, $errorText.Length))
                reset_at = $now
            }
        }
    }

    if ($resetCount -gt 0) {
        Write-Log "Re-queued $resetCount Microsoft recovery move action(s) that failed during Instantly timeout/upload." -Level Success
    }
}

Reset-SmtpPlusUnknownProviderFailures
Reset-RecoveryInstantlyTimeoutFailures

function Process-SingleAction {
    param([object]$Action)

    $actionId = $Action.id
    $domainId = $Action.domain_id
    $actionType = [string]$Action.type

    if ($actionType -in @("microsoft_recovery_move", "microsoft_recovery_reactivate", "microsoft_recovery_purge")) {
        Write-Host ""
        Write-Host "────────────────────────────────────────────────────────────" -ForegroundColor White
        Write-Host "  ACTION: $actionId" -ForegroundColor White
        Write-Host "  TYPE:   $actionType" -ForegroundColor White
        Write-Host "────────────────────────────────────────────────────────────" -ForegroundColor White

        $scriptPath = switch ($actionType) {
            "microsoft_recovery_move" { Join-Path $PSScriptRoot "Part4-MicrosoftDomainRecoveryMove.ps1" }
            "microsoft_recovery_reactivate" { Join-Path $PSScriptRoot "Part5-MicrosoftDomainRecoveryReactivate.ps1" }
            "microsoft_recovery_purge" { Join-Path $PSScriptRoot "Part6-MicrosoftDomainRecoveryPurge.ps1" }
        }

        try {
            $params = @{ Action = $Action }
            if ($DryRun) { $params.DryRun = $true }
            & $scriptPath @params
        } catch {
            Write-Log "$actionType error: $($_.Exception.Message)" -Level Error
            Fail-Action -Action $Action -ErrorMessage "$actionType error: $($_.Exception.Message)"
        }
        return
    }

    if (-not $domainId) {
        Write-Log "Action $actionId has no domain_id, skipping" -Level Warning
        return
    }

    # Fetch domain to check interim_status for resume
    $domain = Get-Domain -DomainId $domainId
    if (-not $domain) {
        Write-Log "Domain $domainId not found, skipping" -Level Warning
        return
    }

    $interimStatus = if ($domain.interim_status) { $domain.interim_status } else { "" }

    Write-Host ""
    Write-Host "────────────────────────────────────────────────────────────" -ForegroundColor White
    Write-Host "  ACTION: $actionId" -ForegroundColor White
    Write-Host "  TYPE:   $actionType" -ForegroundColor White
    Write-Host "  DOMAIN: $($domain.domain) (status: $interimStatus)" -ForegroundColor White
    Write-Host "────────────────────────────────────────────────────────────" -ForegroundColor White

    if ($actionType -eq "microsoft_update_inboxes") {
        if ($domain.provider -ne "microsoft" -and $domain.provider -ne "smtp_plus" -and $domain.provider -ne "azure") {
            Write-Log "Mutation action $actionId is attached to non-Microsoft-backed domain $($domain.domain)" -Level Error
            Fail-Action -Action $Action -ErrorMessage "microsoft_update_inboxes is only valid for Microsoft-backed domains"
            return
        }

        Write-Log "Running Part 2B: Microsoft Inbox Mutations..." -Level Info
        $mutationScript = Join-Path $PSScriptRoot "Part2-MicrosoftInboxMutations.ps1"
        $mutationParams = @{ DomainId = $domainId; ActionId = $actionId }
        if ($DryRun) { $mutationParams.DryRun = $true }

        try {
            & $mutationScript @mutationParams
        } catch {
            Write-Log "Microsoft inbox mutation error: $($_.Exception.Message)" -Level Error
            Fail-Action -Action $Action -ErrorMessage "Microsoft inbox mutation error: $($_.Exception.Message)"
        }
        return
    }

    if ($actionType -eq "microsoft_cancel_domain") {
        if ($domain.provider -ne "microsoft" -and $domain.provider -ne "smtp_plus" -and $domain.provider -ne "azure") {
            Write-Log "Cancellation action $actionId is attached to non-Microsoft-backed domain $($domain.domain)" -Level Error
            Fail-Action -Action $Action -ErrorMessage "microsoft_cancel_domain is only valid for Microsoft-backed domains"
            return
        }

        Write-Log "Running Part 3: Microsoft Domain Cancellation..." -Level Info
        $cancelScript = Join-Path $PSScriptRoot "Part3-MicrosoftDomainCancellation.ps1"
        $cancelParams = @{ DomainId = $domainId; ActionId = $actionId }
        if ($DryRun) { $cancelParams.DryRun = $true }

        try {
            & $cancelScript @cancelParams
        } catch {
            Write-Log "Microsoft cancellation error: $($_.Exception.Message)" -Level Error
            Fail-Action -Action $Action -ErrorMessage "Microsoft cancellation error: $($_.Exception.Message)"
        }
        return
    }

    # Determine which parts need to run based on interim_status
    $part1Statuses = @("", "Both - New Order")  # Part 1 not done yet
    $part2Statuses = @(
        "Both - DNS Zone Created", "Both - NS Migrated",
        "Microsoft - Added to M365", "Both - Verification TXT Added",
        "Both - Domain Verified", "Microsoft - Email Enabled",
        "Both - DNS Records Added", "Microsoft - Exchange Synced",
        "Both - Creating Mailboxes", "Microsoft - Mailboxes Created",
        "Microsoft - Configuring Mailboxes", "Microsoft - SMTP Enabled",
        "Both - DKIM Complete", "Both - Sending Tool Upload Pending",
        "Both - Sending Tool Upload Blocked", "Both - Sending Tool Upload Failed",
        "Both - Failed"
    )

    $needsPart1 = ($interimStatus -in $part1Statuses) -or (-not $domain.cloudflare_zone_id)
    $needsPart2 = ($interimStatus -in $part2Statuses) -or ($interimStatus -in $part1Statuses)

    # ── PART 1: Universal Domain Setup (TypeScript) ──
    if ($needsPart1 -and $nodeVersion) {
        Write-Log "Running Part 1: Universal Domain Setup..." -Level Info

        $tsScript = Join-Path $PSScriptRoot "Part1-UniversalDomainSetup.ts"
        $part1Output = ""

        try {
            $part1Output = & npx tsx $tsScript $domainId $actionId 2>&1 | Out-String
            Write-Host $part1Output

            if ($part1Output -match "PART1_RESULT:FAILED") {
                Write-Log "Part 1 FAILED - check output above" -Level Error
                Fail-Action -Action $Action -ErrorMessage "Part 1 (domain setup) failed"
                return
            }
        } catch {
            Write-Log "Part 1 execution error: $($_.Exception.Message)" -Level Error
            Fail-Action -Action $Action -ErrorMessage "Part 1 execution error: $($_.Exception.Message)"
            return
        }

        Write-Log "Part 1 complete" -Level Success
    } elseif ($needsPart1) {
        Write-Log "Part 1 skipped (Node.js not available). Checking if CF zone exists..." -Level Warning
        if (-not $domain.cloudflare_zone_id) {
            Write-Log "No Cloudflare zone ID - cannot proceed without Part 1" -Level Error
            Fail-Action -Action $Action -ErrorMessage "No Cloudflare zone ID and Node.js not available"
            return
        }
    } else {
        Write-Log "Part 1: Already complete (zone: $($domain.cloudflare_zone_id))" -Level Info
    }

    # ── PART 2: Microsoft Exchange Pipeline (PowerShell) ──
    if ($domain.provider -eq "microsoft" -or $domain.provider -eq "smtp_plus" -or $domain.provider -eq "azure") {
        $part2Label = if ($domain.provider -eq "azure") { "Azure Mailbox Creation" } else { "Microsoft Room Mailbox Creation" }
        Write-Log "Running Part 2: $part2Label..." -Level Info

        $ps1Script = Join-Path $PSScriptRoot "Part2-MicrosoftRoomMailbox.ps1"
        $part2Params = @{ DomainId = $domainId; ActionId = $actionId }
        if ($DryRun) { $part2Params.DryRun = $true }

        try {
            & $ps1Script @part2Params
        } catch {
            Write-Log "Part 2 execution error: $($_.Exception.Message)" -Level Error
            Fail-Action -Action $Action -ErrorMessage "Part 2 execution error: $($_.Exception.Message)"
        }
    } elseif ($domain.provider -eq "google") {
        Write-Log "Google provisioning not yet implemented" -Level Warning
        Fail-Action -Action $Action -ErrorMessage "Google provisioning not implemented"
    } else {
        Write-Log "Unknown provider: $($domain.provider)" -Level Error
        Fail-Action -Action $Action -ErrorMessage "Unknown provider: $($domain.provider)"
    }
}

# ============================================================================
# MAIN LOOP
# ============================================================================
do {
    $actions = Get-PendingActions -ActionTypes @("provision_inbox", "microsoft_update_inboxes", "microsoft_cancel_domain", "microsoft_recovery_move", "microsoft_recovery_reactivate", "microsoft_recovery_purge")

    if ($actions -and $actions.Count -gt 0) {
        $toProcess = $actions
        if ($MaxDomains -gt 0 -and $toProcess.Count -gt $MaxDomains) {
            $toProcess = $toProcess[0..($MaxDomains - 1)]
        }

        Write-Log "Found $($toProcess.Count) pending action(s)" -Level Info

        foreach ($action in $toProcess) {
            $claimedAction = Claim-Action -Action $action
            if (-not $claimedAction) { continue }
            Start-ActionLeaseHeartbeat -Action $claimedAction | Out-Null
            try {
                Process-SingleAction -Action $claimedAction
            } finally {
                Stop-ActionLeaseHeartbeat -Action $claimedAction
            }
        }
    } else {
        if (-not $Once) {
            Write-Host "." -NoNewline -ForegroundColor DarkGray
        }
    }

    if (-not $Once) {
        Start-Sleep -Seconds $PollIntervalSeconds
    }
} while (-not $Once)

if ($Once -and (-not $actions -or $actions.Count -eq 0)) {
    Write-Log "No pending actions found" -Level Info
}

Write-Host ""
Write-Log "Orchestrator finished" -Level Info
