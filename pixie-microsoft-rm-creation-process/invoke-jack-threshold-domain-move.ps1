<#
.SYNOPSIS
    One-off Jack/ProfitPath threshold-tenant migration lane.
.DESCRIPTION
    Moves one already-created Jack Azure domain off its currently assigned
    thresholded Microsoft tenant and onto a clean Active tenant.

    The live flow is deliberately explicit:
      1. Verify current assignment is threshold exceeded.
      2. Verify destination admin is Active and has passed message-trace validation.
      3. Run the existing Microsoft cancellation script against the source tenant.
      4. Reassign the same domain row to the destination admin.
      5. Reset the 99 inbox rows to pending and queue/run Azure provisioning.

    This script does not send test email. Destination cleanliness is based on the
    non-destructive message-trace validator report.
#>

param(
    [Parameter(Mandatory=$true)][string]$Domain,
    [string]$DestinationAdminEmail = "",
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [string]$ValidationCsv = (Join-Path $PSScriptRoot "logs/microsoft-admin-destination-validation-237.csv"),
    [int]$ExpectedInboxes = 99,
    [switch]$Live,
    [switch]$ResumeAfterCancellation,
    [string]$CancelActionId = "",
    [switch]$SkipProvision,
    [switch]$EnqueueProvisionOnly,
    [string]$ConfirmText = ""
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
    return ([string]$Value).Trim().ToLowerInvariant().Replace("https://", "").Replace("http://", "").TrimEnd("/")
}

function Get-DomainByName {
    param([string]$Name)
    $encoded = [uri]::EscapeDataString((Normalize-DomainName $Name))
    $result = Invoke-SupabaseApi -Method GET -Table "domains" -Query "domain=eq.$encoded&select=*&limit=1"
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Get-AdminByEmail {
    param([string]$Email)
    $encoded = [uri]::EscapeDataString((Normalize-Email $Email))
    $result = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query "provider=eq.microsoft&email=eq.$encoded&select=*&limit=1"
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Get-AllDomainInboxes {
    param([string]$DomainId)
    $result = Invoke-SupabaseApi -Method GET -Table "inboxes" -Query "domain_id=eq.$DomainId&order=created_at.asc&select=*"
    if ($result.Success) { return @($result.Data) }
    return @()
}

function Get-OpenDomainActions {
    param([string]$DomainId)
    $query = "domain_id=eq.$DomainId&type=in.(microsoft_cancel_domain,provision_inbox)&status=in.(pending,in_progress)&order=created_at.asc&select=id,type,status,attempts,created_at,started_at,error,payload"
    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query $query
    if ($result.Success) { return @($result.Data) }
    return @()
}

function Get-PlanDestinationAdmin {
    param([string]$DomainName)
    if (-not (Test-Path $PlanCsv)) { return "" }
    $match = Import-Csv -Path $PlanCsv | Where-Object {
        (Normalize-DomainName ([string]$_.domain)) -eq (Normalize-DomainName $DomainName)
    } | Select-Object -First 1
    if ($match -and $match.proposed_destination_admin) { return Normalize-Email ([string]$match.proposed_destination_admin) }
    return ""
}

function Assert-DestinationValidation {
    param([string]$AdminEmail)
    if (-not (Test-Path $ValidationCsv)) {
        throw "Destination validation CSV not found: $ValidationCsv"
    }

    $admin = Normalize-Email $AdminEmail
    $row = Import-Csv -Path $ValidationCsv | Where-Object {
        (Normalize-Email ([string]$_.AdminEmail)) -eq $admin
    } | Select-Object -First 1

    if (-not $row) {
        throw "Destination admin $AdminEmail has not passed message-trace validation yet."
    }

    $validation = [string]$row.Validation
    if ($AcceptedValidationStatuses -notcontains $validation) {
        throw "Destination admin $AdminEmail is not clean enough for migration: validation=$validation error=$($row.Error)"
    }
}

function Add-History {
    param([object]$DomainRecord, [string]$Entry)
    $history = if ($DomainRecord.action_history) { [string]$DomainRecord.action_history } else { "" }
    return Add-HistoryEntry -History $history -Entry $Entry
}

function Get-JackFulfillmentSettings {
    param([object]$DomainRecord)
    $settings = @{}
    if ($DomainRecord.fulfillment_settings) {
        if ($DomainRecord.fulfillment_settings -is [string]) {
            try { $settings = $DomainRecord.fulfillment_settings | ConvertFrom-Json -Depth 30 -AsHashtable } catch { $settings = @{} }
        } else {
            $json = $DomainRecord.fulfillment_settings | ConvertTo-Json -Depth 30
            try { $settings = $json | ConvertFrom-Json -Depth 30 -AsHashtable } catch { $settings = @{} }
        }
    }

    $settings["tag"] = "Mailboxpro 5/10"
    $settings["tags"] = @("Mailboxpro 5/10")
    $settings["dailyLimit"] = 5
    $settings["sendingGap"] = 30
    $settings["enableWarmup"] = $true
    $settings["sending_tool"] = "instantly"
    $settings["instantlyWarmup"] = @{
        limit = 5
        increment = "disabled"
        reply_rate = 60
    }
    return $settings
}

function New-ActionRow {
    param(
        [object]$DomainRecord,
        [string]$Type,
        [hashtable]$Payload,
        [int]$MaxAttempts = 8
    )

    $body = @{
        customer_id = $DomainRecord.customer_id
        domain_id = $DomainRecord.id
        type = $Type
        status = "pending"
        attempts = 0
        max_attempts = $MaxAttempts
        payload = $Payload
    }
    $result = Invoke-SupabaseApi -Method POST -Table "actions" -Body $body
    if (-not $result.Success -or -not $result.Data -or $result.Data.Count -eq 0) {
        throw "Failed to create $Type action: $($result.Error)"
    }
    return $result.Data[0]
}

function Invoke-ClaimedWorkerScript {
    param(
        [object]$Action,
        [string]$ScriptPath,
        [hashtable]$Params
    )

    $fresh = Get-Action -ActionId ([string]$Action.id)
    if (-not $fresh) { throw "Action not found before claim: $($Action.id)" }

    $claimed = Claim-Action -Action $fresh
    if (-not $claimed) { throw "Could not claim action $($Action.id)" }

    Start-ActionLeaseHeartbeat -Action $claimed | Out-Null
    try {
        & $ScriptPath @Params
    } finally {
        Stop-ActionLeaseHeartbeat -Action $claimed
    }

    return Get-Action -ActionId ([string]$Action.id)
}

$domainName = Normalize-DomainName $Domain
$destinationEmail = if ($DestinationAdminEmail) { Normalize-Email $DestinationAdminEmail } else { Get-PlanDestinationAdmin -DomainName $domainName }
if (-not $destinationEmail) { throw "No destination admin provided or found in plan for $domainName" }

$expectedConfirm = "MOVE $domainName TO $destinationEmail"
if ($Live -and $ConfirmText -ne $expectedConfirm) {
    throw "Live migration requires ConfirmText exactly: $expectedConfirm"
}

$domainRecord = Get-DomainByName -Name $domainName
if (-not $domainRecord) { throw "Domain not found in SimpleInboxes: $domainName" }
if ([string]$domainRecord.provider -ne "azure") { throw "$domainName is provider=$($domainRecord.provider), expected azure" }
if (-not $domainRecord.cloudflare_zone_id) { throw "$domainName has no Cloudflare zone id; refusing migration." }

$sourceAdmin = Get-AssignedAdmin -DomainId ([string]$domainRecord.id)
if (-not $sourceAdmin) { throw "$domainName has no assigned source admin." }
if ([string]$sourceAdmin.status -ne "threshold exceeded") {
    throw "$domainName is not currently assigned to a threshold tenant. Current admin=$($sourceAdmin.email), status=$($sourceAdmin.status)"
}

$destinationAdmin = Get-AdminByEmail -Email $destinationEmail
if (-not $destinationAdmin) { throw "Destination admin not found in SI: $destinationEmail" }
if ([string]$destinationAdmin.status -ne "Active" -or -not [bool]$destinationAdmin.active) {
    throw "Destination admin is not Active: $destinationEmail status=$($destinationAdmin.status) active=$($destinationAdmin.active)"
}
if ($destinationAdmin.locked_by_action_id) {
    throw "Destination admin is currently locked by action $($destinationAdmin.locked_by_action_id)"
}
Assert-DestinationValidation -AdminEmail $destinationEmail

$inboxes = @(Get-AllDomainInboxes -DomainId ([string]$domainRecord.id))
$eligibleInboxes = if ($ResumeAfterCancellation) {
    @($inboxes | Where-Object { [string]$_.status -eq "deleted" })
} else {
    @($inboxes | Where-Object { [string]$_.status -in @("pending","provisioning","active","suspended") })
}
$inboxStateLabel = if ($ResumeAfterCancellation) { "deleted" } else { "live" }
if ($eligibleInboxes.Count -ne $ExpectedInboxes) {
    throw "$domainName has $($eligibleInboxes.Count) $inboxStateLabel inbox rows, expected $ExpectedInboxes"
}

$openActions = @(Get-OpenDomainActions -DomainId ([string]$domainRecord.id))
$reusableCancelAction = $null
if (-not $ResumeAfterCancellation) {
    $candidateCancelActions = @($openActions | Where-Object {
        [string]$_.type -eq "microsoft_cancel_domain" -and
        [string]$_.status -eq "pending" -and
        $_.payload -and
        [string]$_.payload.source -eq "jack_threshold_tenant_migration"
    })
    if ($candidateCancelActions.Count -eq 1) {
        $reusableCancelAction = $candidateCancelActions[0]
        $openActions = @($openActions | Where-Object { [string]$_.id -ne [string]$reusableCancelAction.id })
    }
}
if ($ResumeAfterCancellation -and $CancelActionId) {
    $openActions = @($openActions | Where-Object { [string]$_.id -ne [string]$CancelActionId })
}
if ($openActions.Count -gt 0) {
    $actionSummary = ($openActions | ForEach-Object { "$($_.type)/$($_.status)/$($_.id)" }) -join ", "
    throw "$domainName already has open migration/provision action(s): $actionSummary"
}

Write-Log "Prepared threshold migration for ${domainName}: source=$($sourceAdmin.email), destination=$destinationEmail, inboxes=$($eligibleInboxes.Count), resumeAfterCancellation=$ResumeAfterCancellation" -Level Success
if (-not $Live) {
    Write-Log "Dry run only. Add -Live -ConfirmText '$expectedConfirm' to execute." -Level Warning
    return
}

if ($ResumeAfterCancellation) {
    if (-not $CancelActionId) { throw "ResumeAfterCancellation requires CancelActionId" }
    $cancelAction = Get-Action -ActionId $CancelActionId
    if (-not $cancelAction) { throw "Cancellation action not found: $CancelActionId" }
    $domainRecord = Get-Domain -DomainId ([string]$domainRecord.id)
    $deletedCount = @(Get-DomainInboxes -DomainId ([string]$domainRecord.id) -Status "deleted").Count
    if ([string]$domainRecord.status -ne "cancelled" -or [string]$domainRecord.interim_status -ne "Microsoft - Cancellation Complete" -or $deletedCount -ne $ExpectedInboxes) {
        throw "Resume proof failed for ${domainName}: status=$($domainRecord.status), interim=$($domainRecord.interim_status), deletedInboxes=$deletedCount"
    }
    if ([string]$cancelAction.status -ne "completed") {
        Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$([string]$cancelAction.id)" -Body @{
            status = "completed"
            completed_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
            updated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
            error = $null
            result = @{
                message = "Microsoft domain cancellation complete (resumed after lease-fence mismatch)"
                type = "microsoft_cancel_domain"
                domain = $domainName
                summary = @{ deleted_inboxes = $deletedCount; resumed = $true }
            }
        } | Out-Null
        $cancelAction = Get-Action -ActionId $CancelActionId
    }
} else {
    if ($reusableCancelAction) {
        $cancelAction = Get-Action -ActionId ([string]$reusableCancelAction.id)
        if (-not $cancelAction) { throw "Reusable cancellation action disappeared: $($reusableCancelAction.id)" }
        Write-Log "Reusing pending cancellation action $($cancelAction.id) for source tenant teardown." -Level Warning
    } else {
        $cancelPayload = @{
            source = "jack_threshold_tenant_migration"
            payment_status_on_cancel = "paid"
            threshold_migration = $true
            destination_admin_email = $destinationEmail
            source_admin_email = [string]$sourceAdmin.email
            expected_inboxes = $ExpectedInboxes
        }
        $cancelAction = New-ActionRow -DomainRecord $domainRecord -Type "microsoft_cancel_domain" -Payload $cancelPayload -MaxAttempts 5
        Write-Log "Created cancellation action $($cancelAction.id) for source tenant teardown." -Level Info
    }

    $cancelScript = Join-Path $PSScriptRoot "Part3-MicrosoftDomainCancellation.ps1"
    $cancelResult = Invoke-ClaimedWorkerScript -Action $cancelAction -ScriptPath $cancelScript -Params @{
        DomainId = [string]$domainRecord.id
        ActionId = [string]$cancelAction.id
    }
    if (-not $cancelResult -or [string]$cancelResult.status -ne "completed") {
        throw "Cancellation did not complete for $domainName. status=$($cancelResult.status) error=$($cancelResult.error)"
    }
}
Write-Log "Source tenant teardown completed for $domainName." -Level Success

$domainRecord = Get-Domain -DomainId ([string]$domainRecord.id)
$settings = Get-JackFulfillmentSettings -DomainRecord $domainRecord
$history = Add-History -DomainRecord $domainRecord -Entry "Threshold migration: source tenant removed, reassigning to $destinationEmail"

Ensure-DomainAdminAssignment -DomainId ([string]$domainRecord.id) -AdminCredId ([string]$destinationAdmin.id)
Invoke-SupabaseApi -Method PATCH -Table "inboxes" -Query "domain_id=eq.$($domainRecord.id)" -Body @{
    status = "pending"
    updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
} | Out-Null
Update-Domain -DomainId ([string]$domainRecord.id) -Fields @{
    status = "in_progress"
    payment_status = "paid"
    interim_status = "Both - DNS Zone Created"
    cancel_at = $null
    fulfillment_settings = $settings
    action_history = $history
}

$pendingInboxes = @(Get-DomainInboxes -DomainId ([string]$domainRecord.id) -Status "pending")
if ($pendingInboxes.Count -ne $ExpectedInboxes) {
    throw "After reset, $domainName has $($pendingInboxes.Count) pending inboxes, expected $ExpectedInboxes"
}

if ($SkipProvision) {
    Write-Log "Skipping provisioning by request after source teardown/reassignment." -Level Warning
    return
}

$provisionPayload = @{
    source = "jack_threshold_tenant_migration"
    domain = $domainName
    provider = "azure"
    expected_active_inboxes = $ExpectedInboxes
    source_admin_email = [string]$sourceAdmin.email
    destination_admin_email = $destinationEmail
    cancel_action_id = [string]$cancelAction.id
    sending_tool_settings = $settings
}
$provisionAction = New-ActionRow -DomainRecord $domainRecord -Type "provision_inbox" -Payload $provisionPayload -MaxAttempts 12
Write-Log "Created provision action $($provisionAction.id) for $domainName on $destinationEmail." -Level Info

if ($EnqueueProvisionOnly) {
    Write-Log "Provision action queued for worker pickup; not running Part2 inline." -Level Success
    return
}

$provisionScript = Join-Path $PSScriptRoot "Part2-MicrosoftRoomMailbox.ps1"
$provisionResult = Invoke-ClaimedWorkerScript -Action $provisionAction -ScriptPath $provisionScript -Params @{
    DomainId = [string]$domainRecord.id
    ActionId = [string]$provisionAction.id
}

$freshDomain = Get-Domain -DomainId ([string]$domainRecord.id)
$activeCount = @(Get-DomainInboxes -DomainId ([string]$domainRecord.id) -Status "active").Count
Write-Log "Provision lane returned for ${domainName}: action=$($provisionResult.status), domainStatus=$($freshDomain.status), interim=$($freshDomain.interim_status), activeInboxes=$activeCount" -Level Info
