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
$targetDomainId = [string](Get-RecoveryActionPayloadValue -Action $Action -Key "target_domain_id")
$config = Get-RecoveryActionPayloadValue -Action $Action -Key "config"

if (-not $recoveryPoolId -or -not $targetDomainId) {
    Update-ActionStatus -ActionId $actionId -Status "failed" -Error "Missing recovery_pool_id or target_domain_id"
    throw "Missing recovery payload"
}

$recoveryPool = Get-RecoveryPoolRow -RecoveryPoolId $recoveryPoolId
$targetDomain = Get-Domain -DomainId $targetDomainId
if (-not $recoveryPool -or -not $targetDomain) {
    Update-ActionStatus -ActionId $actionId -Status "failed" -Error "Recovery pool or target domain not found"
    throw "Recovery pool or target domain not found"
}

$domain = [string]$recoveryPool.domain
$customerId = [string]$recoveryPool.original_customer_id
$stepMap = Get-RecoveryStepMap -ActionRecord $actionRecord
$summary = [ordered]@{
    target_domain_id = $targetDomainId
    instantly_removed = $false
    mailbox_count = 0
}

function Stop-RecoveryReactivate {
    param([string]$ErrorMessage, [string]$StepName = "")
    if ($StepName) {
        Fail-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepMap $stepMap -Summary $summary -StepName $StepName -ErrorMessage $ErrorMessage
    }
    Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{ last_error = $ErrorMessage }
    Fail-Action -Action $actionRecord -ErrorMessage $ErrorMessage -DefaultMaxRetries 5
    throw $ErrorMessage
}

if ([string]$recoveryPool.recovery_status -ne "reactivating") {
    Stop-RecoveryReactivate -ErrorMessage "Recovery pool status must be reactivating" -StepName "load_recovery_reactivate_context"
}

Update-ActionStatus -ActionId $actionId -Status "in_progress"

$loadStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepName "load_recovery_reactivate_context" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
if ([string]$loadStep.status -ne "completed") {
    Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "load_recovery_reactivate_context" -Details @{
        recovery_pool_id = $recoveryPoolId
        target_domain_id = $targetDomainId
    }
}

$recoveryTenant = Get-RecoveryTenant -RecoveryTenantId ([string]$recoveryPool.recovery_tenant_id)
if (-not $recoveryTenant) {
    Stop-RecoveryReactivate -ErrorMessage "Recovery tenant credentials not found" -StepName "connect_recovery_tenant"
}

$recoveryTenantId = if ($recoveryTenant.tenant_id) { [string]$recoveryTenant.tenant_id } else { Get-RecoveryTenantIdFromDomain -Domain (($recoveryTenant.admin_email -split '@')[1]) }
$recoveryBearer = Get-RecoveryROPCToken -TenantId $recoveryTenantId -Username $recoveryTenant.admin_email -Password $recoveryTenant.admin_password
if (-not $recoveryBearer) {
    Stop-RecoveryReactivate -ErrorMessage "Failed to obtain recovery Graph token" -StepName "connect_recovery_tenant"
}

$zoneId = if ($targetDomain.cloudflare_zone_id) { [string]$targetDomain.cloudflare_zone_id } else { Get-RecoveryPoolZoneId -RecoveryPoolRow $recoveryPool }
if (-not $zoneId) {
    Stop-RecoveryReactivate -ErrorMessage "Missing Cloudflare zone for target domain" -StepName "repoint_dns_to_target_tenant"
}

try {
    $disconnectStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepName "disconnect_recovery_mailbox_from_instantly" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$disconnectStep.status -ne "completed") {
        if (-not $DryRun) {
            Remove-RecoveryInstantlyAccount -InstantlyAccountId ([string]$recoveryPool.instantly_account_id)
        }
        $summary.instantly_removed = $true
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "disconnect_recovery_mailbox_from_instantly" -Details @{ instantly_account_id = $recoveryPool.instantly_account_id }
    }

    $recoveryConnectStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepName "connect_recovery_tenant" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$recoveryConnectStep.status -ne "completed") {
        if (-not $DryRun) {
            Connect-RecoveryExchangeOnline -Email $recoveryTenant.admin_email -Password $recoveryTenant.admin_password
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "connect_recovery_tenant" -Details @{ tenant_id = $recoveryTenantId }
    }

    $cleanupStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepName "remove_from_recovery_tenant" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$cleanupStep.status -ne "completed") {
        if (-not $DryRun) {
            $mailboxEmail = if ($recoveryPool.recovery_mailbox) { [string]$recoveryPool.recovery_mailbox } else { "info@$domain" }
            try { Remove-Mailbox -Identity $mailboxEmail -Confirm:$false -ErrorAction Stop } catch { }
            Remove-RecoveryAcceptedDomainFromExchange -Domain $domain | Out-Null
            $graphRemove = Remove-RecoveryDomainFromGraphWithRetry -Bearer $recoveryBearer -Domain $domain -MaxAttempts 3
            if (-not $graphRemove.Success) {
                Stop-RecoveryReactivate -ErrorMessage "Failed to remove domain from recovery tenant: $($graphRemove.Error)" -StepName "remove_from_recovery_tenant"
            }
            Remove-RecoveryTenantCapacityCount -TenantRow $recoveryTenant
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "remove_from_recovery_tenant" -Details @{ removed_domain = $domain }
    }

    try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue } catch { }

    $assignedAdmin = Get-AssignedAdmin -DomainId $targetDomainId
    $preferredAdminId = if ($assignedAdmin -and $assignedAdmin.id) { [string]$assignedAdmin.id } else { $null }
    $targetAdmin = Acquire-MicrosoftAdminLock -ActionId $actionId -DomainId $targetDomainId -PreferredAdminId $preferredAdminId
    if (-not $targetAdmin) {
        if (Test-ActiveAdminExists -Provider "microsoft") {
            Requeue-ActionWithoutPenalty -Action $actionRecord -Reason "Waiting for target Microsoft admin lock" -DelaySeconds 60
            return
        }
        Stop-RecoveryReactivate -ErrorMessage "No Microsoft admin credentials available for reactivation" -StepName "connect_target_tenant"
    }
    Ensure-DomainAdminAssignment -DomainId $targetDomainId -AdminCredId $targetAdmin.id

    $targetTenantId = Get-RecoveryTenantIdFromDomain -Domain (($targetAdmin.email -split '@')[1])
    $targetBearer = Get-RecoveryROPCToken -TenantId $targetTenantId -Username $targetAdmin.email -Password $targetAdmin.password
    if (-not $targetBearer) {
        Stop-RecoveryReactivate -ErrorMessage "Failed to obtain target Graph token" -StepName "connect_target_tenant"
    }

    $targetConnectStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepName "connect_target_tenant" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$targetConnectStep.status -ne "completed") {
        if (-not $DryRun) {
            Connect-RecoveryExchangeOnline -Email $targetAdmin.email -Password $targetAdmin.password
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "connect_target_tenant" -Details @{ tenant_id = $targetTenantId }
    }

    $removeDnsStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepName "remove_old_cloudflare_dns" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$removeDnsStep.status -ne "completed") {
        if (-not $DryRun) {
            Remove-RecoveryManagedDnsRecords -ZoneId $zoneId -Domain $domain
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "remove_old_cloudflare_dns" -Details @{ zone_id = $zoneId }
    }

    $domainStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepName "add_domain_to_target_tenant" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$domainStep.status -ne "completed") {
        if (-not $DryRun) {
            $addResult = Add-RecoveryDomainToM365 -Bearer $targetBearer -Domain $domain
            if (-not $addResult.Success) {
                Stop-RecoveryReactivate -ErrorMessage "Failed to add domain to target tenant: $($addResult.Error)" -StepName "add_domain_to_target_tenant"
            }
            $verificationTxt = Get-RecoveryDomainVerificationRecord -Bearer $targetBearer -Domain $domain
            if (-not $verificationTxt) {
                Stop-RecoveryReactivate -ErrorMessage "Could not fetch target verification TXT" -StepName "add_domain_to_target_tenant"
            }
            Add-RecoveryCloudflareDnsRecord -ZoneId $zoneId -Type "TXT" -Name "@" -Content $verificationTxt | Out-Null
            if (-not (Verify-RecoveryDomain -Bearer $targetBearer -Domain $domain -MaxAttempts 20 -WaitSeconds 30)) {
                Stop-RecoveryReactivate -ErrorMessage "Target domain verify timeout" -StepName "add_domain_to_target_tenant"
            }
            if (-not (Enable-RecoveryDomainEmailService -Bearer $targetBearer -Domain $domain)) {
                Stop-RecoveryReactivate -ErrorMessage "Failed to enable Email service on target tenant" -StepName "add_domain_to_target_tenant"
            }
            if (-not (Wait-RecoveryExchangeSync -Domain $domain -MaxWaitSeconds 600)) {
                Stop-RecoveryReactivate -ErrorMessage "Target Exchange sync timeout" -StepName "add_domain_to_target_tenant"
            }
            # Pull the real service configuration records for this domain from the target
            # tenant's Graph — same pattern Part2 uses.
            Set-RecoveryDnsRecords -ZoneId $zoneId -Domain $domain -Bearer $targetBearer
            $dkimConfig = Setup-RecoveryDomainDKIM -Domain $domain
            if ($dkimConfig.Success) {
                Complete-RecoveryDKIMSetup -Domain $domain -ZoneId $zoneId -Selector1CNAME $dkimConfig.Selector1CNAME -Selector2CNAME $dkimConfig.Selector2CNAME | Out-Null
            }
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "add_domain_to_target_tenant" -Details @{ target_domain_id = $targetDomainId }
    }

    $mailboxStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepName "create_target_mailboxes" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$mailboxStep.status -ne "completed") {
        $pendingInboxes = @(Get-DomainInboxes -DomainId $targetDomainId -Status "pending")
        if (@($pendingInboxes).Count -eq 0) {
            $pendingInboxes = @(Get-DomainInboxes -DomainId $targetDomainId -Status "active")
        }
        $inboxPassword = -join ((65..90) + (97..122) + (48..57) | Get-Random -Count 14 | ForEach-Object { [char]$_ })
        $inboxPassword = "A" + $inboxPassword.Substring(1, 12) + "1!"
        if (-not $DryRun) {
            Enable-RecoveryTenantSMTPAuth | Out-Null
            $mailboxes = New-RecoveryRoomMailboxBulk -Domain $domain -Inboxes $pendingInboxes -Password $inboxPassword -Bearer $targetBearer
            if (@($mailboxes.Created).Count -eq 0) {
                Stop-RecoveryReactivate -ErrorMessage "No mailboxes created on target tenant" -StepName "create_target_mailboxes"
            }
            foreach ($mb in @($mailboxes.Created)) {
                Update-Inbox -InboxId $mb.InboxId -Fields @{
                    email = $mb.Email
                    password = $inboxPassword
                    status = "active"
                }
            }
        }

        $summary.mailbox_count = @((Get-DomainInboxes -DomainId $targetDomainId -Status "active")).Count
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "create_target_mailboxes" -Details @{ target_domain_id = $targetDomainId; mailbox_count = $summary.mailbox_count }
    }

    $finalizeStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepName "finalize_reactivation" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$finalizeStep.status -ne "completed") {
        Update-Domain -DomainId $targetDomainId -Fields @{
            status = "active"
            interim_status = "Both - Provisioning Complete"
        }
        Invoke-SupabaseApi -Method PATCH -Table "inboxes" -Query "domain_id=eq.$targetDomainId" -Body @{
            status = "active"
            updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
        } | Out-Null
        if (-not $DryRun) {
            Remove-RecoveryPoolRow -RecoveryPoolId $recoveryPoolId
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_reactivate" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "finalize_reactivation" -Details @{ deleted_recovery_pool = (-not $DryRun) }
    }

    Update-ActionStatus -ActionId $actionId -Status "completed" -Result @{
        recovery_pool_id = $recoveryPoolId
        target_domain_id = $targetDomainId
        status = "active"
    }
} finally {
    try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue } catch { }
    Release-MicrosoftAdminLock -ActionId $actionId | Out-Null
}
