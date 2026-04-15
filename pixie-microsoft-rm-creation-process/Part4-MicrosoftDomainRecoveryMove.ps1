param(
    [Parameter(Mandatory=$true)][object]$Action,
    [switch]$DryRun
)

. (Join-Path $PSScriptRoot "RecoveryCommon.ps1")

Ensure-RecoveryExchangeModule
Add-Type -AssemblyName System.Web

$actionId = [string]$Action.id
$actionRecord = Get-Action -ActionId $actionId
if (-not $actionRecord) { throw "Action not found: $actionId" }

$recoveryPoolId = [string](Get-RecoveryActionPayloadValue -Action $Action -Key "recovery_pool_id")
if (-not $recoveryPoolId) {
    Update-ActionStatus -ActionId $actionId -Status "failed" -Error "Missing recovery_pool_id"
    throw "Missing recovery_pool_id"
}

$recoveryPool = Get-RecoveryPoolRow -RecoveryPoolId $recoveryPoolId
if (-not $recoveryPool) {
    Update-ActionStatus -ActionId $actionId -Status "failed" -Error "Recovery pool row not found"
    throw "Recovery pool row not found: $recoveryPoolId"
}

$domain = [string]$recoveryPool.domain
$customerId = [string]$recoveryPool.original_customer_id
$domainId = if ($recoveryPool.original_domain_id) { [string]$recoveryPool.original_domain_id } else { [string]$Action.domain_id }
$stepMap = Get-RecoveryStepMap -ActionRecord $actionRecord
$summary = [ordered]@{
    source_teardown = "pending"
    recovery_domain_added = $false
    recovery_mailbox = $null
    instantly_account_id = $null
    blacklist_count = 0
}

function Stop-RecoveryMove {
    param(
        [string]$ErrorMessage,
        [string]$StepName = ""
    )

    if ($StepName) {
        Fail-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName $StepName -ErrorMessage $ErrorMessage
    }
    Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{
        recovery_status = "failed"
        last_error = $ErrorMessage
    }
    Fail-Action -Action $actionRecord -ErrorMessage $ErrorMessage -DefaultMaxRetries 5
    throw $ErrorMessage
}

Update-ActionStatus -ActionId $actionId -Status "in_progress"
Add-RecoveryActionLog -Action $actionRecord -DomainId $domainId -CustomerId $customerId -EventType "microsoft_recovery_move_started" -Severity "info" -Message "Recovery move started for $domain"

try {
    $loadStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepName "load_recovery_pool" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$loadStep.status -ne "completed") {
        if (-not $recoveryPool.recovery_tenant_id) {
            $pickedTenant = Pick-RecoveryTenantWithCapacity
            if (-not $pickedTenant -or -not $pickedTenant.id) {
                Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{
                    recovery_status = "failed"
                    last_error = "no_tenant_capacity"
                }
                Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "load_recovery_pool" -Details @{ picked_recovery_tenant = $false }
                Fail-Action -Action $actionRecord -ErrorMessage "no_tenant_capacity" -DefaultMaxRetries 5
                return
            }

            Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{
                recovery_tenant_id = $pickedTenant.id
            }
            $recoveryPool = Get-RecoveryPoolRow -RecoveryPoolId $recoveryPoolId
        }

        Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{
            recovery_status = "moving_in"
            last_error = $null
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "load_recovery_pool" -Details @{
            recovery_tenant_id = $recoveryPool.recovery_tenant_id
            previous_provider = $recoveryPool.previous_provider
        }
    }

    $provider = [string]$recoveryPool.previous_provider
    $sourceTeardownDone = [bool](Get-RecoveryActionPayloadValue -Action $Action -Key "source_teardown_done")

    # SMTP+ domains are hosted in a Microsoft tenant by design ("smtp_plus uses
    # microsoft infra") — treat them exactly like Microsoft for source teardown.
    # Google source teardown runs in the Google Python worker BEFORE this action
    # is enqueued, in which case the backend sets source_teardown_done=true and
    # we skip the teardown entirely.

    $sourceAdminRecord = $null
    $sourceBearer = $null
    $sourceExchangeConnected = $false

    $teardownStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepName "teardown_source_tenant" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$teardownStep.status -ne "completed") {
        if ($sourceTeardownDone) {
            Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "teardown_source_tenant" -Details @{ skipped = $true; reason = "source_teardown_done" }
        } elseif ($provider -eq "microsoft" -or $provider -eq "smtp_plus") {
            $assignedAdmin = if ($domainId) { Get-AssignedAdmin -DomainId $domainId } else { $null }
            $preferredAdminId = if ($assignedAdmin -and $assignedAdmin.id) { [string]$assignedAdmin.id } else { $null }
            $sourceAdminRecord = Acquire-MicrosoftAdminLock -ActionId $actionId -DomainId $domainId -PreferredAdminId $preferredAdminId
            if (-not $sourceAdminRecord) {
                if (Test-ActiveAdminExists -Provider "microsoft") {
                    Requeue-ActionWithoutPenalty -Action $actionRecord -Reason "Waiting for source Microsoft admin lock" -DelaySeconds 60
                    return
                }
                Stop-RecoveryMove -ErrorMessage "No Microsoft admin credentials available for source teardown" -StepName "teardown_source_tenant"
            }

            $sourceTenantId = Get-RecoveryTenantIdFromDomain -Domain (($sourceAdminRecord.email -split '@')[1])
            if (-not $sourceTenantId) {
                Stop-RecoveryMove -ErrorMessage "Could not resolve tenant id for source admin" -StepName "teardown_source_tenant"
            }
            $sourceBearer = Get-RecoveryROPCToken -TenantId $sourceTenantId -Username $sourceAdminRecord.email -Password $sourceAdminRecord.password
            if (-not $sourceBearer) {
                Stop-RecoveryMove -ErrorMessage "Failed to obtain Graph token for source admin" -StepName "teardown_source_tenant"
            }

            if (-not $DryRun) {
                try {
                    Connect-RecoveryExchangeOnline -Email $sourceAdminRecord.email -Password $sourceAdminRecord.password
                    $sourceExchangeConnected = $true
                } catch {
                    Stop-RecoveryMove -ErrorMessage "Source Exchange Online connection failed: $($_.Exception.Message)" -StepName "teardown_source_tenant"
                }
            }

            $deletedMailboxCount = if ($DryRun) { 0 } else { Remove-RecoveryMailboxesByDomain -Domain $domain -Bearer $sourceBearer }
            $graphDeletedCount = if ($DryRun) { 0 } else { Remove-RecoveryGraphUsersByDomain -Bearer $sourceBearer -Domain $domain -AdminEmail $sourceAdminRecord.email }
            $acceptedRemoved = if ($DryRun) { $true } else { Remove-RecoveryAcceptedDomainFromExchange -Domain $domain }
            $graphRemove = if ($DryRun) { @{ Success = $true; Attempts = 0; DryRun = $true } } else { Remove-RecoveryDomainFromGraphWithRetry -Bearer $sourceBearer -Domain $domain -MaxAttempts 3 }
            if (-not $graphRemove.Success) {
                Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{
                    recovery_status = "failed_stuck_source"
                    last_error = "failed_stuck_source"
                }
                Stop-RecoveryMove -ErrorMessage "failed_stuck_source" -StepName "teardown_source_tenant"
            }

            $summary.source_teardown = "completed"
            Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "teardown_source_tenant" -Details @{
                mailboxes_deleted = $deletedMailboxCount
                graph_users_deleted = $graphDeletedCount
                accepted_domain_removed = $acceptedRemoved
                graph_delete_attempts = $graphRemove.Attempts
            }
        } else {
            Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "teardown_source_tenant" -Details @{ skipped = $true; provider = $provider }
        }
    }

    if ($sourceExchangeConnected) {
        try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue } catch { }
        $sourceExchangeConnected = $false
    }
    if ($sourceAdminRecord) {
        Release-MicrosoftAdminLock -ActionId $actionId | Out-Null
        $sourceAdminRecord = $null
    }

    $zoneId = Get-RecoveryPoolZoneId -RecoveryPoolRow $recoveryPool
    if (-not $zoneId) {
        Stop-RecoveryMove -ErrorMessage "Missing Cloudflare zone for recovery move" -StepName "rewrite_cloudflare_dns"
    }

    $recoveryTenant = Get-RecoveryTenant -RecoveryTenantId ([string]$recoveryPool.recovery_tenant_id)
    if (-not $recoveryTenant) {
        Stop-RecoveryMove -ErrorMessage "Recovery tenant credentials not found" -StepName "connect_recovery_tenant"
    }

    $recoveryTenantId = if ($recoveryTenant.tenant_id) { [string]$recoveryTenant.tenant_id } else { Get-RecoveryTenantIdFromDomain -Domain (($recoveryTenant.admin_email -split '@')[1]) }
    if (-not $recoveryTenantId) {
        Stop-RecoveryMove -ErrorMessage "Could not resolve recovery tenant id" -StepName "connect_recovery_tenant"
    }

    $recoveryBearer = Get-RecoveryROPCToken -TenantId $recoveryTenantId -Username $recoveryTenant.admin_email -Password $recoveryTenant.admin_password
    if (-not $recoveryBearer) {
        Stop-RecoveryMove -ErrorMessage "Failed to obtain Graph token for recovery tenant" -StepName "connect_recovery_tenant"
    }

    $recoveryConnectStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepName "connect_recovery_tenant" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$recoveryConnectStep.status -ne "completed") {
        if (-not $DryRun) {
            try {
                Connect-RecoveryExchangeOnline -Email $recoveryTenant.admin_email -Password $recoveryTenant.admin_password
            } catch {
                Stop-RecoveryMove -ErrorMessage "Recovery Exchange Online connection failed: $($_.Exception.Message)" -StepName "connect_recovery_tenant"
            }
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "connect_recovery_tenant" -Details @{ tenant_id = $recoveryTenantId }
    }

    $removeDnsStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepName "remove_old_cloudflare_dns" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$removeDnsStep.status -ne "completed") {
        if (-not $DryRun) {
            Remove-RecoveryManagedDnsRecords -ZoneId $zoneId -Domain $domain
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "remove_old_cloudflare_dns" -Details @{ zone_id = $zoneId }
    }

    $domainStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepName "add_domain_to_recovery_tenant" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$domainStep.status -ne "completed") {
        if (-not $DryRun) {
            $addResult = Add-RecoveryDomainToM365 -Bearer $recoveryBearer -Domain $domain
            if (-not $addResult.Success) {
                Stop-RecoveryMove -ErrorMessage "Failed to add domain to recovery tenant: $($addResult.Error)" -StepName "add_domain_to_recovery_tenant"
            }
            $verificationTxt = Get-RecoveryDomainVerificationRecord -Bearer $recoveryBearer -Domain $domain
            if (-not $verificationTxt) {
                Stop-RecoveryMove -ErrorMessage "Could not fetch recovery verification TXT" -StepName "add_domain_to_recovery_tenant"
            }
            Add-RecoveryCloudflareDnsRecord -ZoneId $zoneId -Type "TXT" -Name "@" -Content $verificationTxt | Out-Null
            $verified = Verify-RecoveryDomain -Bearer $recoveryBearer -Domain $domain -MaxAttempts 20 -WaitSeconds 30
            if (-not $verified) {
                Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{
                    recovery_status = "failed"
                    last_error = "domain_verify_timeout"
                }
                Stop-RecoveryMove -ErrorMessage "domain_verify_timeout" -StepName "add_domain_to_recovery_tenant"
            }
            $emailEnabled = Enable-RecoveryDomainEmailService -Bearer $recoveryBearer -Domain $domain
            if (-not $emailEnabled) {
                Stop-RecoveryMove -ErrorMessage "Failed to enable Email service on recovery tenant" -StepName "add_domain_to_recovery_tenant"
            }
            if (-not (Wait-RecoveryExchangeSync -Domain $domain -MaxWaitSeconds 600)) {
                Stop-RecoveryMove -ErrorMessage "Recovery Exchange sync timeout" -StepName "add_domain_to_recovery_tenant"
            }
            # Now that the domain is verified and Email service is enabled on the recovery
            # tenant, ask MS what DNS records it wants for this specific domain and write
            # them. This is how Part2 does it too — no hardcoded MX host templating.
            Set-RecoveryDnsRecords -ZoneId $zoneId -Domain $domain -Bearer $recoveryBearer
            $dkimConfig = Setup-RecoveryDomainDKIM -Domain $domain
            if ($dkimConfig.Success) {
                Complete-RecoveryDKIMSetup -Domain $domain -ZoneId $zoneId -Selector1CNAME $dkimConfig.Selector1CNAME -Selector2CNAME $dkimConfig.Selector2CNAME | Out-Null
            }
        }

        $summary.recovery_domain_added = $true
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "add_domain_to_recovery_tenant" -Details @{ zone_id = $zoneId }
    }

    $mailboxStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepName "create_recovery_mailbox" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$mailboxStep.status -ne "completed") {
        $mailboxPassword = [string]$env:RECOVERY_MAILBOX_PASSWORD
        if (-not $mailboxPassword) {
            Stop-RecoveryMove -ErrorMessage "RECOVERY_MAILBOX_PASSWORD is not configured" -StepName "create_recovery_mailbox"
        }

        if (-not $DryRun) {
            Enable-RecoveryTenantSMTPAuth | Out-Null
            $roomResults = New-RecoveryRoomMailboxBulk -Domain $domain -Inboxes @(@{
                id = $recoveryPoolId
                username = "info"
                first_name = $domain
                last_name = "Recovery"
            }) -Password $mailboxPassword -Bearer $recoveryBearer

            if (@($roomResults.Created).Count -eq 0) {
                Stop-RecoveryMove -ErrorMessage "Failed to create recovery room mailbox" -StepName "create_recovery_mailbox"
            }
        }

        $recoveryMailbox = "info@$domain"
        $externalId = $null
        if (-not $DryRun) {
            $mailbox = Get-Mailbox -Identity $recoveryMailbox -ErrorAction SilentlyContinue
            $externalId = if ($mailbox) { [string]$mailbox.ExternalDirectoryObjectId } else { $recoveryMailbox }
        } else {
            $externalId = $recoveryMailbox
        }

        Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{
            recovery_mailbox = $recoveryMailbox
            recovery_mailbox_external_id = $externalId
        }
        $summary.recovery_mailbox = $recoveryMailbox
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "create_recovery_mailbox" -Details @{ email = $recoveryMailbox; external_id = $externalId }
    }

    $instantlyStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepName "attach_recovery_mailbox_to_instantly" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$instantlyStep.status -ne "completed") {
        $recoveryMailbox = "info@$domain"
        $instantlyAccountId = if ($DryRun) { $recoveryMailbox } else { Add-RecoveryMailboxToInstantly -Email $recoveryMailbox -Password $env:RECOVERY_MAILBOX_PASSWORD }
        if (-not $DryRun) {
            Enable-RecoveryInstantlyWarmup -Email $recoveryMailbox | Out-Null
        }
        Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{
            instantly_account_id = $instantlyAccountId
        }
        $summary.instantly_account_id = $instantlyAccountId
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "attach_recovery_mailbox_to_instantly" -Details @{ instantly_account_id = $instantlyAccountId }
    }

    $dnsblStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepName "dnsbl_check" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$dnsblStep.status -ne "completed") {
        $flags = if ($DryRun) { @() } else { @(Test-RecoveryDnsblListings -Domain $domain) }
        Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{
            blacklist_flags = $flags
            blacklist_checked_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
        }
        $summary.blacklist_count = @($flags).Count
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "dnsbl_check" -Details @{ listings = @($flags).Count }
    }

    $finalizeStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepName "finalize_recovery_move" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$finalizeStep.status -ne "completed") {
        if (-not $DryRun) {
            Remove-RecoveryPoolOriginalState -RecoveryPoolRow $recoveryPool
        }
        Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{
            recovery_status = "warming"
            last_error = $null
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_move" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "finalize_recovery_move" -Details @{ status = "warming" }
    }

    Update-ActionStatus -ActionId $actionId -Status "completed" -Result @{
        recovery_pool_id = $recoveryPoolId
        domain = $domain
        status = "warming"
    }
} catch {
    if ([string]$_.Exception.Message -ne "Waiting for source Microsoft admin lock") {
        throw
    }
} finally {
    try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue } catch { }
    Release-MicrosoftAdminLock -ActionId $actionId | Out-Null
}
