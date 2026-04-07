<#
.SYNOPSIS
    Part 2B: Microsoft inbox identity mutations for existing room mailboxes.
.DESCRIPTION
    Processes Supabase-tracked `microsoft_update_inboxes` actions.

    This script renames existing Microsoft room mailboxes in place instead of
    deleting + recreating them. It updates:
      - Graph user givenName / surname / displayName
      - Graph userPrincipalName
      - Exchange primary SMTP address
      - Exchange alias preservation for the old email
      - Supabase inbox rows
      - Supabase domain_mutation_* tracking tables

    Resume behavior:
      - completed item checkpoints are stored in actions.result.steps[]
      - mutation item status is updated per inbox
      - retries continue remaining inboxes instead of starting over

.PARAMETER DomainId
    Supabase domain UUID
.PARAMETER ActionId
    Supabase action UUID
.PARAMETER DryRun
    Run without making actual changes
#>

param(
    [Parameter(Mandatory=$true)][string]$DomainId,
    [Parameter(Mandatory=$true)][string]$ActionId,
    [switch]$DryRun
)

. (Join-Path $PSScriptRoot "config.ps1")

function Get-TenantIdFromDomain {
    param([string]$Domain)
    try {
        $response = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$Domain/v2.0/.well-known/openid-configuration" -TimeoutSec 30 -ErrorAction Stop
        if ($response.token_endpoint) {
            $parts = $response.token_endpoint -split '/'
            if ($parts.Length -ge 4 -and $parts[3] -and $parts[3].ToLower() -ne "organizations") { return $parts[3] }
        }
    } catch { }
    return $null
}

function Get-ROPCToken {
    param([string]$TenantId, [string]$ClientId, [string]$Username, [string]$Password, [string]$ScopeString = "https://graph.microsoft.com/.default")

    $tokenUrl = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
    $body = @{ grant_type = "password"; client_id = $ClientId; scope = $ScopeString; username = $Username; password = $Password }

    try {
        $response = Invoke-RestMethod -Method POST -Uri $tokenUrl -Body $body -ContentType "application/x-www-form-urlencoded" -TimeoutSec 30 -ErrorAction Stop
        return $response.access_token
    } catch {
        Write-Log "ROPC token failed: $($_.Exception.Message)" -Level Error
        return $null
    }
}

function Invoke-GraphRequest {
    param([string]$Method, [string]$Url, [string]$Bearer, [object]$Body = $null)

    $headers = @{ Authorization = "Bearer $Bearer" }
    $params = @{ Method = $Method; Uri = $Url; Headers = $headers; ContentType = "application/json"; TimeoutSec = 60; ErrorAction = "Stop" }
    if ($Body) { $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 15 } }

    Invoke-RestMethod @params
}

function Test-MailboxHasAlias {
    param(
        [string]$MailboxIdentity,
        [string]$AliasEmail
    )

    $normalized = ("smtp:{0}" -f $AliasEmail.Trim().ToLower())
    try {
        $mailbox = Get-Mailbox -Identity $MailboxIdentity -ErrorAction Stop
        foreach ($address in @($mailbox.EmailAddresses)) {
            if (-not $address) { continue }
            if ($address.ToString().Trim().ToLower() -eq $normalized) { return $true }
        }
    } catch { }
    return $false
}

function Ensure-MailboxAlias {
    param(
        [string]$MailboxIdentity,
        [string]$AliasEmail
    )

    if (Test-MailboxHasAlias -MailboxIdentity $MailboxIdentity -AliasEmail $AliasEmail) {
        return @{ Success = $true; AlreadyPresent = $true }
    }

    try {
        Set-Mailbox -Identity $MailboxIdentity -EmailAddresses @{Add=$AliasEmail} -Confirm:$false -ErrorAction Stop
        return @{ Success = $true; AlreadyPresent = $false }
    } catch {
        return @{ Success = $false; Error = $_.Exception.Message }
    }
}

function Get-GraphUserByEmail {
    param(
        [string]$Bearer,
        [string]$PrimaryEmail,
        [string]$FallbackEmail = $null
    )

    $candidates = @()
    if ($PrimaryEmail) { $candidates += $PrimaryEmail }
    if ($FallbackEmail -and $FallbackEmail -ne $PrimaryEmail) { $candidates += $FallbackEmail }

    foreach ($email in $candidates) {
        try {
            $user = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/users/$email" -Bearer $Bearer
            if ($user) { return $user }
        } catch { }
    }

    return $null
}

function Get-UpdateValue {
    param(
        [object]$Row,
        [string]$PropertyName,
        [string]$Fallback = ""
    )

    if ($null -ne $Row -and $null -ne $Row.$PropertyName) {
        return [string]$Row.$PropertyName
    }
    return $Fallback
}

Add-Type -AssemblyName System.Web

if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
    Write-Host "Installing ExchangeOnlineManagement module..." -ForegroundColor Yellow
    Install-Module -Name ExchangeOnlineManagement -Force -AllowClobber -Scope CurrentUser
}
Import-Module ExchangeOnlineManagement -ErrorAction SilentlyContinue

$actionRecord = Get-Action -ActionId $ActionId
if (-not $actionRecord) {
    throw "Action not found: $ActionId"
}

$payload = $actionRecord.payload
$mutationRequestId = if ($payload -and $payload.mutation_request_id) { [string]$payload.mutation_request_id } else { "" }
$mutationSubmissionId = if ($payload -and $payload.mutation_submission_id) { [string]$payload.mutation_submission_id } else { "" }

$domainRecord = Get-Domain -DomainId $DomainId
if (-not $domainRecord) {
    throw "Domain not found: $DomainId"
}

$domainName = [string]$domainRecord.domain
if ([string]$domainRecord.provider -ne "microsoft") {
    throw "Domain $domainName is not a Microsoft domain"
}

$steps = @()
$stepOrder = New-Object 'System.Collections.Generic.List[string]'
$stepByName = @{}
if ($actionRecord.result -and $actionRecord.result.steps) {
    foreach ($stepRow in @($actionRecord.result.steps)) {
        if (-not $stepRow -or -not $stepRow.step) { continue }
        $stepName = [string]$stepRow.step
        $stepHash = [ordered]@{}
        foreach ($property in $stepRow.PSObject.Properties) {
            $stepHash[$property.Name] = $property.Value
        }
        if (-not $stepByName.ContainsKey($stepName)) {
            $stepOrder.Add($stepName) | Out-Null
        }
        $stepByName[$stepName] = $stepHash
    }
}

$summary = [ordered]@{
    processed = 0
    completed = 0
    skipped = 0
    failed = 0
    alias_active = 0
    failures = @()
}

function Sync-Steps {
    $script:steps = @()
    foreach ($stepName in $script:stepOrder) {
        if ($script:stepByName.ContainsKey($stepName)) {
            $script:steps += $script:stepByName[$stepName]
        }
    }
}

function Get-OrCreate-Step {
    param([string]$StepName)

    if (-not $script:stepByName.ContainsKey($StepName)) {
        $script:stepOrder.Add($StepName) | Out-Null
        $script:stepByName[$StepName] = [ordered]@{
            step = $StepName
        }
    }

    return $script:stepByName[$StepName]
}

function Start-Step {
    param([string]$StepName)

    $step = Get-OrCreate-Step -StepName $StepName
    $step.status = "in_progress"
    $step.startedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    $step.attempt = if ($actionRecord.attempts -ne $null) { [int]$actionRecord.attempts } else { 1 }
    if ($step.Contains("completedAt")) { $step.Remove("completedAt") }
    if ($step.Contains("error")) { $step.Remove("error") }
    if ($step.Contains("details")) { $step.Remove("details") }
    Sync-Steps
    return $step
}

function Complete-Step {
    param([object]$Step, [hashtable]$Details = $null)
    $Step.status = "completed"
    $Step.completedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    if ($Details) { $Step.details = $Details }
    Sync-Steps
}

function Fail-Step {
    param([object]$Step, [string]$ErrorMessage)
    $Step.status = "failed"
    $Step.completedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    $Step.error = $ErrorMessage
    Sync-Steps
}

function Skip-Step {
    param([string]$StepName, [string]$Reason)
    Write-Log "Skipping checkpointed step ${StepName}: $Reason" -Level Info
}

function Persist-Progress {
    param([hashtable]$ExtraResult = $null)

    Sync-Steps
    $result = [ordered]@{
        checkpoint_version = 2
        action_id = $ActionId
        request_id = if ($mutationRequestId) { $mutationRequestId } else { $null }
        submission_id = if ($mutationSubmissionId) { $mutationSubmissionId } else { $null }
        run_attempt = if ($actionRecord.attempts -ne $null) { [int]$actionRecord.attempts } else { 1 }
        type = "microsoft_update_inboxes"
        domain = $domainName
        steps = $script:steps
        summary = $script:summary
        lastUpdated = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    }
    if ($ExtraResult) {
        foreach ($key in $ExtraResult.Keys) { $result[$key] = $ExtraResult[$key] }
    }

    Update-ActionResult -ActionId $ActionId -Result $result
}

function Checkpoint-Step {
    param([string]$StepName)
    if ($script:stepByName.ContainsKey($StepName) -and [string]$script:stepByName[$StepName].status -eq "completed") {
        $details = if ($script:stepByName[$StepName].Contains("details")) { $script:stepByName[$StepName].details } else { $null }
        Skip-Step -StepName $StepName -Reason "Resumed from previous completed step"
        return $details
    }
    return $null
}

function Write-MutationLog {
    param(
        [string]$EventType,
        [string]$Severity,
        [string]$Message,
        [hashtable]$Metadata = $null,
        [string]$ItemId = $null,
        [string]$InboxId = $null
    )

    $logLevel = switch ($Severity) {
        "error" { "Error" }
        "warn" { "Warning" }
        default { "Info" }
    }
    Write-Log $Message -Level $logLevel

    Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $domainRecord.customer_id -EventType $EventType -Severity $Severity -Message $Message -Metadata $Metadata
    if ($mutationRequestId) {
        $eventBody = @{
            submission_id = if ($mutationSubmissionId) { $mutationSubmissionId } else { $null }
            request_id = $mutationRequestId
            item_id = $ItemId
            domain_id = $DomainId
            inbox_id = $InboxId
            event_type = $EventType
            severity = $Severity
            message = $Message
            metadata = if ($Metadata) { $Metadata } else { @{} }
        }
        Add-MutationEvent -Fields $eventBody
    }
}

$mutationItems = if ($mutationRequestId) { @(Get-MutationItems -RequestId $mutationRequestId) } else { @() }
$mutationItemByInboxId = @{}
foreach ($item in $mutationItems) {
    $key = [string]$item.inbox_id
    if ($key) { $mutationItemByInboxId[$key] = $item }
}

$updateRows = @()
if ($payload -and $payload.updates) { $updateRows = @($payload.updates) }
$leasedAdminRecord = $null

try {
    if ($updateRows.Count -eq 0) {
        throw "No updates found in action payload"
    }

    $contextStep = Start-Step -StepName "load_mutation_context"
    $targetInboxIds = @()
    foreach ($row in $updateRows) {
        if ($row.inbox_id) { $targetInboxIds += [string]$row.inbox_id }
    }
    $inboxes = @(Get-InboxesByIds -DomainId $DomainId -InboxIds $targetInboxIds)
    $inboxById = @{}
    foreach ($row in $inboxes) { $inboxById[[string]$row.id] = $row }

    $assignedAdminRecord = Get-AssignedAdmin -DomainId $DomainId
    if (-not $assignedAdminRecord) {
        Write-MutationLog -EventType "admin_fallback" -Severity "warn" -Message "No assigned admin found for $domainName; falling back to the least-used Microsoft admin."
    }

    $preferredAdminId = if ($assignedAdminRecord -and $assignedAdminRecord.id) { [string]$assignedAdminRecord.id } else { $null }
    $leasedAdminRecord = Acquire-MicrosoftAdminLock -ActionId $ActionId -DomainId $DomainId -PreferredAdminId $preferredAdminId
    if (-not $leasedAdminRecord) {
        if (Test-ActiveAdminExists -Provider "microsoft") {
            $waitMessage = if ($preferredAdminId) {
                "Waiting for the assigned Microsoft admin lock"
            } else {
                "Waiting for an available Microsoft admin lock"
            }

            Write-MutationLog -EventType "admin_lock_wait" -Severity "warn" -Message "$waitMessage for $domainName" -Metadata @{
                domain = $domainName
                action_id = $ActionId
            }
            Requeue-ActionWithoutPenalty -Action $actionRecord -Reason $waitMessage -DelaySeconds 60
            return
        }

        throw "No Microsoft admin credentials available"
    }

    Ensure-DomainAdminAssignment -DomainId $DomainId -AdminCredId $leasedAdminRecord.id

    $adminEmail = [string]$leasedAdminRecord.email
    $adminPassword = [string]$leasedAdminRecord.password
    Complete-Step -Step $contextStep -Details @{
        domain = $domainName
        update_count = $updateRows.Count
        target_inbox_count = $inboxes.Count
        admin_email = $adminEmail
    }
    Persist-Progress

    if (-not $DryRun) {
        $connectStep = Start-Step -StepName "connect_exchange_online"
        $securePwd = ConvertTo-SecureString $adminPassword -AsPlainText -Force
        $creds = New-Object System.Management.Automation.PSCredential($adminEmail, $securePwd)
        try {
            Connect-ExchangeOnline -Credential $creds -ShowBanner:$false -ErrorAction Stop
            Complete-Step -Step $connectStep -Details @{ admin_email = $adminEmail }
            Persist-Progress
        } catch {
            $connectError = "Exchange Online connection failed: $($_.Exception.Message)"
            Fail-Step -Step $connectStep -ErrorMessage $connectError
            Persist-Progress
            throw $connectError
        }
    }

    Update-ActionStatus -ActionId $ActionId -Status "in_progress"

    if ($mutationRequestId) {
        $now = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
        Update-MutationRequest -RequestId $mutationRequestId -Fields @{
            status = "processing"
            started_at = $now
            current_step = "microsoft_update_inboxes"
            last_error = $null
            failed_at = $null
        }
        if ($mutationSubmissionId) { Refresh-MutationSubmission -SubmissionId $mutationSubmissionId | Out-Null }
    }

    Write-MutationLog -EventType "action_started" -Severity "info" -Message "Processing microsoft_update_inboxes for $domainName" -Metadata @{
        domain = $domainName
        action_id = $ActionId
        admin_email = $adminEmail
        update_count = $updateRows.Count
    }

    foreach ($update in $updateRows) {
        $inboxId = [string]$update.inbox_id
        $inboxRow = $null
        if ($inboxById.ContainsKey($inboxId)) { $inboxRow = $inboxById[$inboxId] }
        if (-not $inboxRow) { throw "Inbox $inboxId not found on $domainName" }

        $mutationItem = if ($mutationItemByInboxId.ContainsKey($inboxId)) { $mutationItemByInboxId[$inboxId] } else { $null }
        $itemId = if ($mutationItem) { [string]$mutationItem.id } else { $null }
        $stepName = "mutate_$inboxId"
        $script:summary.processed += 1

        if ($mutationItem -and [string]$mutationItem.status -eq "completed") {
            Skip-Step -StepName $stepName -Reason "Mutation item already completed"
            $script:summary.skipped += 1
            continue
        }

        $checkpoint = Checkpoint-Step -StepName $stepName
        if ($checkpoint) {
            if ($itemId) {
                Update-MutationItem -ItemId $itemId -Fields @{
                    status = "completed"
                    completed_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
                    last_error = $null
                }
            }
            $script:summary.skipped += 1
            continue
        }

        $oldEmail = (Get-UpdateValue -Row $mutationItem -PropertyName "old_email" -Fallback ([string]$inboxRow.email)).Trim().ToLower()
        if (-not $oldEmail) {
            $existingUsername = [string]$inboxRow.username
            $oldEmail = ("{0}@{1}" -f $existingUsername, $domainName).Trim().ToLower()
        }

        $newEmail = (Get-UpdateValue -Row $mutationItem -PropertyName "new_email" -Fallback ("{0}@{1}" -f ([string]$update.username), $domainName)).Trim().ToLower()
        $newUsername = (Get-UpdateValue -Row $mutationItem -PropertyName "new_username" -Fallback ([string]$update.username)).Trim().ToLower()
        $newFirstName = (Get-UpdateValue -Row $mutationItem -PropertyName "new_first_name" -Fallback ([string]$update.first_name)).Trim()
        $newLastName = (Get-UpdateValue -Row $mutationItem -PropertyName "new_last_name" -Fallback ([string]$update.last_name)).Trim()
        $newDisplayName = ("{0} {1}" -f $newFirstName, $newLastName).Trim()
        $keepOldAlias = $true
        if ($mutationItem -and $null -ne $mutationItem.keep_old_email_as_alias) {
            if ($mutationItem.keep_old_email_as_alias -is [bool]) {
                $keepOldAlias = [bool]$mutationItem.keep_old_email_as_alias
            } else {
                $keepOldAlias = ([string]$mutationItem.keep_old_email_as_alias).Trim().ToLower() -eq "true"
            }
        }

        if (-not $newUsername) { $newUsername = ($newEmail -split '@')[0] }
        if (-not $newEmail) { $newEmail = ("{0}@{1}" -f $newUsername, $domainName).Trim().ToLower() }

        if ($itemId) {
            Update-MutationItem -ItemId $itemId -Fields @{
                status = "processing"
                started_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
                last_error = $null
                failed_at = $null
            }
        }

        $step = Start-Step -StepName $stepName

        try {
            $user = Get-User -Identity $oldEmail -ErrorAction SilentlyContinue
            if (-not $user -and $newEmail) {
                $user = Get-User -Identity $newEmail -ErrorAction SilentlyContinue
            }
            if (-not $user) {
                throw "User not found by old or new email ($oldEmail / $newEmail)"
            }

            $mailboxIdentity = if ($user.WindowsLiveID) { [string]$user.WindowsLiveID } else { $oldEmail }
            $currentUPN = if ($user.UserPrincipalName) { [string]$user.UserPrincipalName } else { $mailboxIdentity }
            $currentGivenName = [string]$user.FirstName
            $currentSurname = [string]$user.LastName
            $currentDisplayName = [string]$user.displayName
            $mailbox = Get-Mailbox -Identity $mailboxIdentity -ErrorAction Stop
            $currentPrimarySmtp = [string]$mailbox.PrimarySmtpAddress

            $namesMatch = ($currentGivenName -eq $newFirstName) -and ($currentSurname -eq $newLastName) -and ($currentDisplayName -eq $newDisplayName)
            $emailMatch = ($currentUPN.Trim().ToLower() -eq $newEmail) -and ($currentPrimarySmtp.Trim().ToLower() -eq $newEmail)
            $aliasNeeded = $keepOldAlias -and ($oldEmail -ne $newEmail)
            $aliasActive = if ($aliasNeeded -and -not $DryRun) { Test-MailboxHasAlias -MailboxIdentity $mailboxIdentity -AliasEmail $oldEmail } else { -not $aliasNeeded }

            if (-not $DryRun -and -not $namesMatch) {
                Set-User -Identity $mailboxIdentity -DisplayName $newDisplayName -FirstName $newFirstName -LastName $newLastName -Confirm:$false -ErrorAction Stop
                Set-Mailbox -Identity $mailboxIdentity -DisplayName $newDisplayName -Name $newDisplayName -Confirm:$false -ErrorAction Stop
            }

            if (-not $DryRun -and -not $emailMatch) {
                Start-Sleep -Seconds 3
                Set-User -Identity $mailboxIdentity -WindowsEmailAddress $newEmail -Confirm:$false -ErrorAction Stop
                Set-Mailbox -Identity $mailboxIdentity -Alias $newUsername -Confirm:$false -ErrorAction Stop
                Set-Mailbox -Identity $mailboxIdentity -MicrosoftOnlineServicesID $newEmail -WindowsEmailAddress $newEmail -Confirm:$false -ErrorAction Stop
                $mailboxIdentity = $newEmail
            }

            if ($aliasNeeded) {
                if ($DryRun) {
                    $aliasActive = $true
                } elseif (-not $aliasActive) {
                    $aliasResult = Ensure-MailboxAlias -MailboxIdentity $mailboxIdentity -AliasEmail $oldEmail
                    if (-not $aliasResult.Success) {
                        Upsert-InboxEmailAlias -InboxId $inboxId -Email $oldEmail -Status "failed"
                        throw "Renamed mailbox but failed to preserve alias ${oldEmail}: $($aliasResult.Error)"
                    }
                    $aliasActive = $true
                }
                if ($aliasActive) {
                    Upsert-InboxEmailAlias -InboxId $inboxId -Email $oldEmail -Status "active"
                    $script:summary.alias_active += 1
                }
            }

            if (-not $DryRun) {
                Start-Sleep -Seconds 2
                $validatedUser = Get-User -Identity $newEmail -ErrorAction SilentlyContinue
                $validatedMailbox = Get-Mailbox -Identity $newEmail -ErrorAction SilentlyContinue
                if (-not $validatedUser -or -not $validatedMailbox) {
                    throw "Post-update validation failed for $newEmail"
                }
            }

            Update-Inbox -InboxId $inboxId -Fields @{
                username = $newUsername
                email = $newEmail
                first_name = $newFirstName
                last_name = $newLastName
                status = "active"
            }

            if ($itemId) {
                $aliasStatus = if ($aliasNeeded) { "active" } else { "not_needed" }
                Update-MutationItem -ItemId $itemId -Fields @{
                    status = "completed"
                    alias_status = $aliasStatus
                    last_error = $null
                    completed_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
                    failed_at = $null
                }
            }

            $details = @{
                inbox_id = $inboxId
                old_email = $oldEmail
                new_email = $newEmail
                new_display_name = $newDisplayName
                alias_active = $aliasActive
            }
            Complete-Step -Step $step -Details $details
            Persist-Progress

            Write-MutationLog -EventType "mutation_item_completed" -Severity "info" -Message "Updated Microsoft inbox $oldEmail -> $newEmail ($newDisplayName)" -Metadata $details -ItemId $itemId -InboxId $inboxId
            $script:summary.completed += 1
        } catch {
            $errorMessage = $_.Exception.Message
            Fail-Step -Step $step -ErrorMessage $errorMessage
            Persist-Progress

            if ($itemId) {
                Update-MutationItem -ItemId $itemId -Fields @{
                    status = "failed"
                    last_error = $errorMessage
                    failed_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
                }
            }

            $script:summary.failed += 1
            $script:summary.failures += @{
                inbox_id = $inboxId
                old_email = $oldEmail
                new_email = $newEmail
                error = $errorMessage
            }

            Write-MutationLog -EventType "mutation_item_failed" -Severity "error" -Message "Failed to update Microsoft inbox ${oldEmail} -> ${newEmail}: $errorMessage" -Metadata @{
                inbox_id = $inboxId
                old_email = $oldEmail
                new_email = $newEmail
            } -ItemId $itemId -InboxId $inboxId
        }
    }

    $finalizeCheckpoint = Checkpoint-Step -StepName "finalize_mutation"
    if (-not $finalizeCheckpoint) {
        $finalStep = Start-Step -StepName "finalize_mutation"
        $requestStatus = if ($script:summary.failed -gt 0) { "needs_attention" } else { "completed" }

        if ($mutationRequestId) {
            Update-MutationRequest -RequestId $mutationRequestId -Fields @{
                status = $requestStatus
                current_step = if ($requestStatus -eq "completed") { "completed" } else { "microsoft_update_inboxes" }
                last_error = if ($script:summary.failed -gt 0) { "One or more Microsoft inbox mutations failed" } else { $null }
                completed_at = if ($script:summary.failed -eq 0) { (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ") } else { $null }
                failed_at = if ($script:summary.failed -gt 0) { (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ") } else { $null }
                retry_count = [Math]::Max(0, [int]$actionRecord.attempts - 1)
            }
        }

        $finalDetails = @{
            completed = $script:summary.completed
            skipped = $script:summary.skipped
            failed = $script:summary.failed
            alias_active = $script:summary.alias_active
        }
        Complete-Step -Step $finalStep -Details $finalDetails
        Persist-Progress
    }

    if ($mutationSubmissionId) { Refresh-MutationSubmission -SubmissionId $mutationSubmissionId | Out-Null }

    $result = @{
        checkpoint_version = 2
        action_id = $ActionId
        request_id = if ($mutationRequestId) { $mutationRequestId } else { $null }
        submission_id = if ($mutationSubmissionId) { $mutationSubmissionId } else { $null }
        run_attempt = if ($actionRecord.attempts -ne $null) { [int]$actionRecord.attempts } else { 1 }
        type = "microsoft_update_inboxes"
        domain = $domainName
        summary = $script:summary
        steps = $script:steps
    }

    if ($script:summary.failed -gt 0) {
        throw "Microsoft inbox mutation finished with $($script:summary.failed) failed item(s)"
    }

    Update-ActionStatus -ActionId $ActionId -Status "completed" -Result $result
    Write-MutationLog -EventType "action_completed" -Severity "info" -Message "Completed microsoft_update_inboxes for $domainName" -Metadata @{
        completed = $script:summary.completed
        skipped = $script:summary.skipped
    }
} catch {
    $message = $_.Exception.Message
    if ($mutationRequestId) {
        $isFinal = ([int]$actionRecord.attempts -ge [int]$actionRecord.max_attempts)
        $requestStatus = if ($isFinal) { "failed" } else { "needs_attention" }
        Update-MutationRequest -RequestId $mutationRequestId -Fields @{
            status = $requestStatus
            current_step = "microsoft_update_inboxes"
            last_error = $message
            failed_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
            retry_count = [Math]::Max(0, [int]$actionRecord.attempts - 1)
        }
        if ($mutationSubmissionId) { Refresh-MutationSubmission -SubmissionId $mutationSubmissionId | Out-Null }
    }

    Persist-Progress -ExtraResult @{ error = $message }
    Write-MutationLog -EventType "action_failed" -Severity "error" -Message "Microsoft inbox mutation failed for ${domainName}: $message" -Metadata @{ domain = $domainName }
    throw
} finally {
    if (-not $DryRun) {
        try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue } catch { }
    }
    if ($leasedAdminRecord -and $leasedAdminRecord.id) {
        Release-MicrosoftAdminLock -ActionId $ActionId | Out-Null
    }
}
