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
$restoreToCustomer = [bool](Get-RecoveryActionPayloadValue -Action $Action -Key "restore_to_customer")
$restoredDomainId = [string](Get-RecoveryActionPayloadValue -Action $Action -Key "restored_domain_id")

if (-not $recoveryPoolId) {
    Update-ActionStatus -ActionId $actionId -Status "failed" -Error "Missing recovery_pool_id"
    throw "Missing recovery_pool_id"
}

$recoveryPool = Get-RecoveryPoolRow -RecoveryPoolId $recoveryPoolId
if (-not $recoveryPool) {
    Update-ActionStatus -ActionId $actionId -Status "failed" -Error "Recovery pool row not found"
    throw "Recovery pool row not found"
}

$domain = [string]$recoveryPool.domain
$customerId = [string]$recoveryPool.original_customer_id
$stepMap = Get-RecoveryStepMap -ActionRecord $actionRecord
$summary = [ordered]@{
    instantly_removed = $false
    restore_to_customer = $restoreToCustomer
}

function Stop-RecoveryPurge {
    param([string]$ErrorMessage, [string]$StepName = "")
    if ($StepName) {
        Fail-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepMap $stepMap -Summary $summary -StepName $StepName -ErrorMessage $ErrorMessage
    }
    Update-RecoveryPool -RecoveryPoolId $recoveryPoolId -Fields @{ last_error = $ErrorMessage }
    Fail-Action -Action $actionRecord -ErrorMessage $ErrorMessage -DefaultMaxRetries 5
    throw $ErrorMessage
}

Update-ActionStatus -ActionId $actionId -Status "in_progress"

$loadStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepName "load_recovery_purge_context" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
if ([string]$loadStep.status -ne "completed") {
    Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "load_recovery_purge_context" -Details @{
        recovery_pool_id = $recoveryPoolId
        restore_to_customer = $restoreToCustomer
        restored_domain_id = $restoredDomainId
    }
}

$recoveryTenant = Get-RecoveryTenant -RecoveryTenantId ([string]$recoveryPool.recovery_tenant_id)
if (-not $recoveryTenant) {
    if (-not $recoveryPool.recovery_tenant_id) {
        $dnsCleanupStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepName "cleanup_cloudflare_zone" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
        if ([string]$dnsCleanupStep.status -ne "completed") {
            if (-not $restoreToCustomer) {
                $zoneId = Get-RecoveryPoolZoneId -RecoveryPoolRow $recoveryPool
                if ($zoneId -and -not $DryRun) {
                    foreach ($record in @(Get-CloudflareDnsRecords -ZoneId $zoneId)) {
                        if (-not $record.id) { continue }
                        try { Remove-CloudflareDnsRecord -ZoneId $zoneId -RecordId $record.id } catch { }
                    }
                }
            }
            Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "cleanup_cloudflare_zone" -Details @{ skipped = $restoreToCustomer; no_recovery_tenant = $true }
        }

        $finalizeStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepName "finalize_recovery_purge" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
        if ([string]$finalizeStep.status -ne "completed") {
            if (-not $DryRun) {
                Remove-RecoveryPoolRow -RecoveryPoolId $recoveryPoolId
            }
            Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "finalize_recovery_purge" -Details @{ deleted_recovery_pool = (-not $DryRun); no_recovery_tenant = $true }
        }

        Update-ActionStatus -ActionId $actionId -Status "completed" -Result @{
            recovery_pool_id = $recoveryPoolId
            restore_to_customer = $restoreToCustomer
            restored_domain_id = $restoredDomainId
            no_recovery_tenant = $true
        }
        return
    }
    Stop-RecoveryPurge -ErrorMessage "Recovery tenant credentials not found" -StepName "connect_recovery_tenant"
}

$recoveryTenantId = if ($recoveryTenant.tenant_id) { [string]$recoveryTenant.tenant_id } else { Get-RecoveryTenantIdFromDomain -Domain (($recoveryTenant.admin_email -split '@')[1]) }
$recoveryBearer = Get-RecoveryROPCToken -TenantId $recoveryTenantId -Username $recoveryTenant.admin_email -Password $recoveryTenant.admin_password
if (-not $recoveryBearer) {
    Stop-RecoveryPurge -ErrorMessage "Failed to obtain recovery Graph token" -StepName "connect_recovery_tenant"
}

try {
    $instantlyStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepName "disconnect_recovery_mailbox_from_instantly" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$instantlyStep.status -ne "completed") {
        if (-not $DryRun) {
            Remove-RecoveryInstantlyAccount -InstantlyAccountId ([string]$recoveryPool.instantly_account_id)
        }
        $summary.instantly_removed = $true
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "disconnect_recovery_mailbox_from_instantly" -Details @{ instantly_account_id = $recoveryPool.instantly_account_id }
    }

    $connectStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepName "connect_recovery_tenant" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$connectStep.status -ne "completed") {
        if (-not $DryRun) {
            Connect-RecoveryExchangeOnline -Email $recoveryTenant.admin_email -Password $recoveryTenant.admin_password
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "connect_recovery_tenant" -Details @{ tenant_id = $recoveryTenantId }
    }

    $tenantCleanupStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepName "remove_recovery_tenant_domain" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$tenantCleanupStep.status -ne "completed") {
        if (-not $DryRun) {
            $mailboxEmail = if ($recoveryPool.recovery_mailbox) { [string]$recoveryPool.recovery_mailbox } else { "postmaster@$domain" }
            try { Remove-Mailbox -Identity $mailboxEmail -Confirm:$false -ErrorAction Stop } catch { }
            Remove-RecoveryAcceptedDomainFromExchange -Domain $domain | Out-Null
            $graphRemove = Remove-RecoveryDomainFromGraphWithRetry -Bearer $recoveryBearer -Domain $domain -MaxAttempts 3
            if (-not $graphRemove.Success) {
                Stop-RecoveryPurge -ErrorMessage "Failed to remove recovery domain: $($graphRemove.Error)" -StepName "remove_recovery_tenant_domain"
            }
            Remove-RecoveryTenantCapacityCount -TenantRow $recoveryTenant
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "remove_recovery_tenant_domain" -Details @{ removed_domain = $domain }
    }

    $dnsCleanupStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepName "cleanup_cloudflare_zone" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$dnsCleanupStep.status -ne "completed") {
        if (-not $restoreToCustomer) {
            $zoneId = Get-RecoveryPoolZoneId -RecoveryPoolRow $recoveryPool
            if ($zoneId -and -not $DryRun) {
                foreach ($record in @(Get-CloudflareDnsRecords -ZoneId $zoneId)) {
                    if (-not $record.id) { continue }
                    try { Remove-CloudflareDnsRecord -ZoneId $zoneId -RecordId $record.id } catch { }
                }
            }
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "cleanup_cloudflare_zone" -Details @{ skipped = $restoreToCustomer }
    }

    $finalizeStep = Start-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepName "finalize_recovery_purge" -StepMap $stepMap -Summary $summary -Attempt ([int]$actionRecord.attempts)
    if ([string]$finalizeStep.status -ne "completed") {
        if (-not $DryRun) {
            Remove-RecoveryPoolRow -RecoveryPoolId $recoveryPoolId
        }
        Complete-RecoveryStep -ActionId $actionId -ActionType "microsoft_recovery_purge" -Domain $domain -StepMap $stepMap -Summary $summary -StepName "finalize_recovery_purge" -Details @{ deleted_recovery_pool = (-not $DryRun) }
    }

    Update-ActionStatus -ActionId $actionId -Status "completed" -Result @{
        recovery_pool_id = $recoveryPoolId
        restore_to_customer = $restoreToCustomer
        restored_domain_id = $restoredDomainId
    }
} finally {
    try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue } catch { }
}
